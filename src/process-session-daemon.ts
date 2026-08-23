import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import {
  chmodSync,
  existsSync,
  rmSync,
} from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { createConnection, createServer, type Server as NetServer, type Socket } from "node:net";
import { fileURLToPath } from "node:url";
import {
  LocalAgentDaemonLock,
  ensureLocalAgentDaemonSecret,
  ensureLocalAgentDaemonStateDir,
  readLocalAgentDaemonSecret,
  type LocalAgentDaemonPaths,
} from "./local-agent-daemon-lifecycle.js";
import {
  ProcessSessionManager,
  type ProcessSessionController,
  type ProcessSnapshot,
  type StartCommandInput,
  type WriteStdinInput,
} from "./process-sessions.js";

const PROCESS_SESSION_DAEMON_PROTOCOL_VERSION = 1;
const MAX_REQUEST_BYTES = 2 * 1024 * 1024;
const DEFAULT_STARTUP_TIMEOUT_MS = 8_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 120_000;
const DEFAULT_REQUEST_READ_TIMEOUT_MS = 5_000;
const RETRY_DELAY_MS = 40;

type ProcessSessionDaemonMethod =
  | "hello"
  | "process.start"
  | "process.write"
  | "daemon.status"
  | "daemon.stop";

type ProcessSessionDaemonRequest =
  | ProcessSessionDaemonRequestBase<"hello", Record<string, never>>
  | ProcessSessionDaemonRequestBase<"process.start", StartCommandInput>
  | ProcessSessionDaemonRequestBase<"process.write", WriteStdinInput>
  | ProcessSessionDaemonRequestBase<"daemon.status", Record<string, never>>
  | ProcessSessionDaemonRequestBase<"daemon.stop", Record<string, never>>;

interface ProcessSessionDaemonRequestBase<M extends ProcessSessionDaemonMethod, P> {
  requestId: string;
  protocolVersion: number;
  authToken: string;
  method: M;
  params: P;
}

type ProcessSessionDaemonResponse =
  | {
      requestId: string;
      protocolVersion: number;
      ok: true;
      result: unknown;
    }
  | {
      requestId: string;
      protocolVersion: number;
      ok: false;
      error: {
        code: string;
        message: string;
        retryable?: boolean;
      };
    };

export interface ProcessSessionDaemonStatus {
  state: "ready" | "stopping";
  protocolVersion: number;
  pid: number;
  endpoint: string;
  runningProcesses: number;
  sessions: number;
  clientConnections: number;
}

export interface ProcessSessionDaemonOptions {
  stateDir: string;
  manager?: ProcessSessionManager;
}

export interface ProcessSessionClientOptions {
  stateDir: string;
  startupTimeoutMs?: number;
  requestTimeoutMs?: number;
  spawnDaemon?: () => void;
}

export class ProcessSessionDaemonError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable = false,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ProcessSessionDaemonError";
  }
}

export class ProcessSessionDaemon {
  readonly paths: LocalAgentDaemonPaths;
  private readonly manager: ProcessSessionManager;
  private readonly lock: LocalAgentDaemonLock;
  private readonly sockets = new Set<Socket>();
  private server?: NetServer;
  private authToken?: string;
  private closePromise?: Promise<void>;
  private stopping = false;

  constructor(options: ProcessSessionDaemonOptions) {
    this.paths = processSessionDaemonPaths(options.stateDir);
    this.manager = options.manager ?? new ProcessSessionManager();
    this.lock = new LocalAgentDaemonLock(this.paths);
  }

  async start(): Promise<ProcessSessionDaemonStatus> {
    if (this.server) return this.status();
    ensureLocalAgentDaemonStateDir(this.paths.stateDir);
    let lockAcquired = false;
    try {
      this.lock.acquire();
      lockAcquired = true;
      this.authToken = ensureLocalAgentDaemonSecret(this.paths);
      if (usesFilesystemSocket(this.paths.endpoint)) rmSync(this.paths.socketPath, { force: true });
      const server = createServer((socket) => this.handleConnection(socket));
      this.server = server;
      await listen(server, this.paths.endpoint);
      if (usesFilesystemSocket(this.paths.endpoint)) chmodSync(this.paths.socketPath, 0o600);
      this.stopping = false;
      return this.status();
    } catch (error) {
      this.server = undefined;
      this.authToken = undefined;
      if (lockAcquired) {
        this.lock.release();
        removeProcessSessionDaemonFiles(this.paths);
      }
      throw error;
    }
  }

  status(): ProcessSessionDaemonStatus {
    if (!this.server) throw new Error("Process session daemon is not started.");
    return {
      state: this.stopping ? "stopping" : "ready",
      protocolVersion: PROCESS_SESSION_DAEMON_PROTOCOL_VERSION,
      pid: process.pid,
      endpoint: this.paths.endpoint,
      runningProcesses: this.manager.runningCount,
      sessions: this.manager.sessionCount,
      clientConnections: this.sockets.size,
    };
  }

  async close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    if (!this.server) return;
    this.stopping = true;
    this.closePromise = (async () => {
      for (const socket of this.sockets) socket.destroy();
      this.sockets.clear();
      await closeServer(this.server);
      this.manager.shutdown();
      removeProcessSessionDaemonFiles(this.paths);
      this.lock.release();
      this.server = undefined;
      this.authToken = undefined;
    })();
    return this.closePromise;
  }

  private handleConnection(socket: Socket): void {
    this.sockets.add(socket);
    socket.setEncoding("utf8");
    let buffer = "";
    let handled = false;
    const requestTimer = setTimeout(() => {
      if (handled) return;
      handled = true;
      this.writeError(socket, "", new ProcessSessionDaemonError(
        "DAEMON_TIMEOUT",
        "Timed out waiting for a complete process session daemon request.",
        true,
      ));
    }, DEFAULT_REQUEST_READ_TIMEOUT_MS);
    requestTimer.unref();
    socket.on("data", (chunk: string | Buffer) => {
      if (handled) return;
      buffer += chunk.toString();
      if (Buffer.byteLength(buffer, "utf8") > MAX_REQUEST_BYTES) {
        handled = true;
        this.writeError(socket, "", new ProcessSessionDaemonError(
          "INVALID_REQUEST",
          "Process session daemon request is too large.",
        ));
        return;
      }
      const newline = buffer.indexOf("\n");
      if (newline === -1) return;
      handled = true;
      clearTimeout(requestTimer);
      void this.handleLine(socket, buffer.slice(0, newline));
    });
    socket.on("error", () => undefined);
    socket.on("close", () => {
      clearTimeout(requestTimer);
      this.sockets.delete(socket);
    });
  }

  private async handleLine(socket: Socket, line: string): Promise<void> {
    let requestId = "";
    try {
      const parsed = JSON.parse(line) as unknown;
      requestId = readRequestId(parsed);
      const request = decodeRequest(parsed);
      if (request.protocolVersion !== PROCESS_SESSION_DAEMON_PROTOCOL_VERSION) {
        throw new ProcessSessionDaemonError(
          "PROTOCOL_MISMATCH",
          `Unsupported process session daemon protocol ${request.protocolVersion}; expected ${PROCESS_SESSION_DAEMON_PROTOCOL_VERSION}.`,
          true,
        );
      }
      this.assertAuthenticated(request.authToken);
      const result = await this.dispatch(request);
      socket.end(encodeResponse({
        requestId: request.requestId,
        protocolVersion: PROCESS_SESSION_DAEMON_PROTOCOL_VERSION,
        ok: true,
        result,
      }));
      if (request.method === "daemon.stop") setImmediate(() => { void this.close(); });
    } catch (error) {
      this.writeError(socket, requestId, toDaemonError(error));
    }
  }

  private async dispatch(request: ProcessSessionDaemonRequest): Promise<unknown> {
    if (this.stopping && request.method !== "hello" && request.method !== "daemon.status") {
      throw new ProcessSessionDaemonError(
        "DAEMON_STOPPING",
        "Process session daemon is stopping.",
        true,
      );
    }
    switch (request.method) {
      case "hello":
      case "daemon.status":
        return this.status();
      case "process.start":
        return this.manager.start(request.params);
      case "process.write":
        return this.manager.write(request.params);
      case "daemon.stop":
        if (this.manager.runningCount > 0) {
          throw new ProcessSessionDaemonError(
            "RUNNING_PROCESSES",
            "Process session daemon has running process sessions and cannot stop.",
            true,
          );
        }
        this.stopping = true;
        return this.status();
    }
  }

  private assertAuthenticated(authToken: string): void {
    const expected = this.authToken;
    if (!expected) throw new ProcessSessionDaemonError("DAEMON_UNAVAILABLE", "Process session daemon is unavailable.", true);
    const actualBytes = Buffer.from(authToken);
    const expectedBytes = Buffer.from(expected);
    if (actualBytes.length !== expectedBytes.length || !timingSafeEqual(actualBytes, expectedBytes)) {
      throw new ProcessSessionDaemonError("UNAUTHORIZED", "Process session daemon authentication failed.");
    }
  }

  private writeError(socket: Socket, requestId: string, error: ProcessSessionDaemonError): void {
    socket.end(encodeResponse({
      requestId,
      protocolVersion: PROCESS_SESSION_DAEMON_PROTOCOL_VERSION,
      ok: false,
      error: {
        code: error.code,
        message: error.message,
        ...(error.retryable ? { retryable: true } : {}),
      },
    }));
  }
}

export class ProcessSessionClient implements ProcessSessionController {
  private readonly paths: LocalAgentDaemonPaths;
  private readonly startupTimeoutMs: number;
  private readonly requestTimeoutMs: number;
  private readonly spawnDaemon: () => void;
  private startupPromise?: Promise<ProcessSessionDaemonStatus>;

  constructor(options: ProcessSessionClientOptions) {
    this.paths = processSessionDaemonPaths(options.stateDir);
    this.startupTimeoutMs = options.startupTimeoutMs ?? DEFAULT_STARTUP_TIMEOUT_MS;
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.spawnDaemon = options.spawnDaemon ?? (() => spawnProcessSessionDaemon(options.stateDir));
  }

  async start(input: StartCommandInput): Promise<ProcessSnapshot> {
    const result = await this.request("process.start", {
      ...input,
      environment: captureProcessEnvironment(),
    });
    return decodeSnapshot(result);
  }

  async write(input: WriteStdinInput): Promise<ProcessSnapshot> {
    return decodeSnapshot(await this.request("process.write", input));
  }

  async status(): Promise<ProcessSessionDaemonStatus> {
    return decodeStatus(await this.requestExisting("daemon.status", {}));
  }

  async stop(): Promise<ProcessSessionDaemonStatus> {
    return decodeStatus(await this.requestExisting("daemon.stop", {}));
  }

  async ensureReady(): Promise<ProcessSessionDaemonStatus> {
    if (this.startupPromise) return this.startupPromise;
    this.startupPromise = this.ensureReadyInternal().finally(() => {
      this.startupPromise = undefined;
    });
    return this.startupPromise;
  }

  private async ensureReadyInternal(): Promise<ProcessSessionDaemonStatus> {
    const existing = await this.tryHello();
    if (existing) return existing;

    let spawnError: unknown;
    try {
      this.spawnDaemon();
    } catch (error) {
      spawnError = error;
    }

    const deadline = Date.now() + this.startupTimeoutMs;
    while (Date.now() < deadline) {
      await delay(RETRY_DELAY_MS);
      const ready = await this.tryHello();
      if (ready) return ready;
    }

    throw new ProcessSessionDaemonError(
      "DAEMON_STARTUP_FAILURE",
      "Unable to start the process session daemon.",
      true,
      spawnError === undefined ? undefined : { cause: spawnError },
    );
  }

  private async tryHello(): Promise<ProcessSessionDaemonStatus | undefined> {
    const authToken = readLocalAgentDaemonSecret(this.paths);
    if (!authToken) return undefined;
    try {
      const response = await sendRequest(this.paths.endpoint, {
        requestId: randomUUID(),
        protocolVersion: PROCESS_SESSION_DAEMON_PROTOCOL_VERSION,
        authToken,
        method: "hello",
        params: {},
      }, Math.min(this.requestTimeoutMs, 1_000));
      if (!response.ok) {
        if (response.error.code === "DAEMON_UNAVAILABLE") return undefined;
        throw remoteError(response.error);
      }
      return decodeStatus(response.result);
    } catch (error) {
      if (error instanceof ProcessSessionDaemonError && error.code === "DAEMON_UNAVAILABLE") return undefined;
      throw error;
    }
  }

  private async request<M extends ProcessSessionDaemonMethod>(
    method: M,
    params: Extract<ProcessSessionDaemonRequest, { method: M }>["params"],
  ): Promise<unknown> {
    await this.ensureReady();
    return this.requestExisting(method, params);
  }

  private async requestExisting(
    method: ProcessSessionDaemonMethod,
    params: StartCommandInput | WriteStdinInput | Record<string, never>,
  ): Promise<unknown> {
    const authToken = readLocalAgentDaemonSecret(this.paths);
    if (!authToken) {
      throw new ProcessSessionDaemonError("DAEMON_UNAVAILABLE", "Process session daemon credentials are unavailable.", true);
    }
    const response = await sendRequest(this.paths.endpoint, {
      requestId: randomUUID(),
      protocolVersion: PROCESS_SESSION_DAEMON_PROTOCOL_VERSION,
      authToken,
      method,
      params,
    } as ProcessSessionDaemonRequest, this.requestTimeoutMs);
    if (!response.ok) throw remoteError(response.error);
    return response.result;
  }
}

export function processSessionDaemonPaths(
  stateDir: string,
  platform: NodeJS.Platform = process.platform,
): LocalAgentDaemonPaths {
  const daemonStateDir = resolve(stateDir, "process-session-daemon");
  const socketPath = join(daemonStateDir, "processd.sock");
  return {
    stateDir: daemonStateDir,
    socketPath,
    pidPath: join(daemonStateDir, "processd.pid"),
    lockPath: join(daemonStateDir, "processd.lock"),
    secretPath: join(daemonStateDir, "processd.secret"),
    logPath: join(daemonStateDir, "processd.log"),
    endpoint: platform === "win32"
      ? `\\\\.\\pipe\\devspace-processd-${hashStateDir(daemonStateDir)}`
      : platform === "linux"
        ? `\0devspace-processd-${hashStateDir(daemonStateDir)}`
        : socketPath,
  };
}

export function spawnProcessSessionDaemon(stateDir: string): void {
  const entrypoint = resolveProcessSessionDaemonEntrypoint();
  const execArgv = processSessionDaemonExecArgv(process.execArgv);
  const userSystemd = linuxUserSystemdRuntime();
  if (userSystemd) {
    const unit = `devspace-process-session-${hashStateDir(resolve(stateDir)).slice(0, 16)}`;
    const result = spawnSync(userSystemd.systemdRun, [
      "--user",
      "--quiet",
      "--collect",
      `--unit=${unit}`,
      "--property=Type=exec",
      `--setenv=DEVSPACE_STATE_DIR=${resolve(stateDir)}`,
      `--setenv=HOME=${homedir()}`,
      process.execPath,
      ...execArgv,
      entrypoint,
    ], {
      env: {
        ...process.env,
        XDG_RUNTIME_DIR: userSystemd.runtimeDir,
        DBUS_SESSION_BUS_ADDRESS: `unix:path=${userSystemd.busPath}`,
      },
      encoding: "utf8",
      windowsHide: true,
    });
    if (result.error || result.status !== 0) {
      throw new ProcessSessionDaemonError(
        "DAEMON_STARTUP_FAILURE",
        `Unable to start process session daemon through the user service manager: ${result.error?.message ?? (result.stderr.trim() || `exit ${result.status}`)}`,
        true,
      );
    }
    return;
  }

  if (process.platform === "linux" && process.env.INVOCATION_ID) {
    throw new ProcessSessionDaemonError(
      "DAEMON_STARTUP_FAILURE",
      "DevSpace is running under systemd but the user service manager is unavailable; refusing to keep process sessions inside the DevSpace service cgroup.",
      true,
    );
  }

  const child = spawn(process.execPath, [...execArgv, entrypoint], {
    detached: true,
    stdio: "ignore",
    windowsHide: true,
    env: { ...process.env, DEVSPACE_STATE_DIR: resolve(stateDir) },
  });
  child.unref();
}

export function resolveProcessSessionDaemonEntrypoint(): string {
  const compiled = fileURLToPath(new URL("./process-session-daemon-main.js", import.meta.url));
  if (existsSync(compiled)) return compiled;
  return fileURLToPath(new URL("./process-session-daemon-main.ts", import.meta.url));
}

function linuxUserSystemdRuntime(): {
  systemdRun: string;
  runtimeDir: string;
  busPath: string;
} | undefined {
  if (process.platform !== "linux" || typeof process.getuid !== "function") return undefined;
  const uid = process.getuid();
  const runtimeDir = process.env.XDG_RUNTIME_DIR || `/run/user/${uid}`;
  const busPath = join(runtimeDir, "bus");
  const systemdRun = ["/usr/bin/systemd-run", "/bin/systemd-run"].find(existsSync);
  if (!systemdRun || !existsSync(busPath)) return undefined;
  return { systemdRun, runtimeDir, busPath };
}

export function processSessionDaemonExecArgv(execArgv: readonly string[]): string[] {
  const result: string[] = [];
  for (let index = 0; index < execArgv.length; index += 1) {
    const argument = execArgv[index]!;
    if (/^--inspect(?:-brk|-wait)?(?:=.*)?$/.test(argument)) continue;
    if (argument === "--inspect-port") {
      index += 1;
      continue;
    }
    if (argument.startsWith("--inspect-port=")) continue;
    if (argument === "-e" || argument === "--eval" || argument === "-p" || argument === "--print") {
      index += 1;
      continue;
    }
    if (
      argument.startsWith("--eval=")
      || argument.startsWith("--print=")
      || argument === "-c"
      || argument === "--check"
    ) {
      continue;
    }
    if (argument === "--input-type") {
      index += 1;
      continue;
    }
    if (argument.startsWith("--input-type=")) continue;
    result.push(argument);
  }
  return result;
}

function captureProcessEnvironment(): Record<string, string> {
  return Object.fromEntries(
    Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
  );
}

async function sendRequest(
  endpoint: string,
  request: ProcessSessionDaemonRequest,
  timeoutMs: number,
): Promise<ProcessSessionDaemonResponse> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(endpoint);
    let buffer = "";
    let settled = false;
    const timer = setTimeout(() => finishError(new ProcessSessionDaemonError(
      "DAEMON_TIMEOUT",
      "Timed out waiting for the process session daemon.",
      true,
    )), timeoutMs);

    const finish = (response: ProcessSessionDaemonResponse) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.end();
      resolve(response);
    };
    const finishError = (error: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      reject(error);
    };

    socket.setEncoding("utf8");
    socket.on("data", (chunk: string | Buffer) => {
      buffer += chunk.toString();
      const newline = buffer.indexOf("\n");
      if (newline === -1) return;
      try {
        const response = decodeResponse(JSON.parse(buffer.slice(0, newline)) as unknown);
        if (response.requestId !== request.requestId) {
          throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon response request id did not match.");
        }
        finish(response);
      } catch (error) {
        finishError(error instanceof Error ? error : new Error(String(error)));
      }
    });
    socket.once("error", (cause) => finishError(new ProcessSessionDaemonError(
      "DAEMON_UNAVAILABLE",
      "Process session daemon is unavailable.",
      true,
      { cause },
    )));
    socket.once("close", () => {
      if (!settled) {
        finishError(new ProcessSessionDaemonError(
          "DAEMON_UNAVAILABLE",
          "Process session daemon closed the connection.",
          true,
        ));
      }
    });
    socket.once("connect", () => socket.write(`${JSON.stringify(request)}\n`));
  });
}

function decodeRequest(value: unknown): ProcessSessionDaemonRequest {
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_REQUEST", "Process session daemon request must be an object.");
  const requestId = requiredString(record.requestId, "requestId");
  const protocolVersion = requiredInteger(record.protocolVersion, "protocolVersion");
  const authToken = requiredString(record.authToken, "authToken");
  const method = requiredString(record.method, "method") as ProcessSessionDaemonMethod;
  switch (method) {
    case "hello":
    case "daemon.status":
    case "daemon.stop":
      return { requestId, protocolVersion, authToken, method, params: decodeEmptyParams(record.params) } as ProcessSessionDaemonRequest;
    case "process.start":
      return { requestId, protocolVersion, authToken, method, params: decodeStartInput(record.params) };
    case "process.write":
      return { requestId, protocolVersion, authToken, method, params: decodeWriteInput(record.params) };
    default:
      throw new ProcessSessionDaemonError("UNKNOWN_METHOD", `Unknown process session daemon method: ${method}`);
  }
}

function decodeResponse(value: unknown): ProcessSessionDaemonResponse {
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon response must be an object.");
  const requestId = requiredString(record.requestId, "requestId");
  const protocolVersion = requiredInteger(record.protocolVersion, "protocolVersion");
  if (record.ok === true) return { requestId, protocolVersion, ok: true, result: record.result };
  if (record.ok === false) {
    const error = asRecord(record.error);
    if (!error) throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon error payload is invalid.");
    return {
      requestId,
      protocolVersion,
      ok: false,
      error: {
        code: requiredString(error.code, "error.code"),
        message: requiredString(error.message, "error.message"),
        ...(typeof error.retryable === "boolean" ? { retryable: error.retryable } : {}),
      },
    };
  }
  throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon response is invalid.");
}

function decodeSnapshot(value: unknown): ProcessSnapshot {
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon returned an invalid process snapshot.");
  const running = requiredBoolean(record.running, "running");
  return {
    sessionId: optionalInteger(record.sessionId, "sessionId"),
    output: typeof record.output === "string" ? record.output : "",
    outputTruncated: requiredBoolean(record.outputTruncated, "outputTruncated"),
    running,
    exitCode: optionalInteger(record.exitCode, "exitCode"),
    signal: optionalString(record.signal),
    wallTimeMs: requiredNonNegativeNumber(record.wallTimeMs, "wallTimeMs"),
  };
}

function decodeStatus(value: unknown): ProcessSessionDaemonStatus {
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon returned an invalid status.");
  const state = requiredString(record.state, "state");
  if (state !== "ready" && state !== "stopping") {
    throw new ProcessSessionDaemonError("INVALID_RESPONSE", "Process session daemon returned an invalid state.");
  }
  return {
    state,
    protocolVersion: requiredInteger(record.protocolVersion, "protocolVersion"),
    pid: requiredInteger(record.pid, "pid"),
    endpoint: requiredString(record.endpoint, "endpoint"),
    runningProcesses: requiredInteger(record.runningProcesses, "runningProcesses"),
    sessions: requiredInteger(record.sessions, "sessions"),
    clientConnections: requiredInteger(record.clientConnections, "clientConnections"),
  };
}

function decodeStartInput(value: unknown): StartCommandInput {
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_REQUEST", "Process start parameters must be an object.");
  return {
    workspaceId: requiredString(record.workspaceId, "workspaceId"),
    command: requiredContentString(record.command, "command"),
    cwd: requiredString(record.cwd, "cwd"),
    workspaceRoot: optionalString(record.workspaceRoot),
    environment: decodeEnvironment(record.environment),
    tty: optionalBoolean(record.tty),
    columns: optionalInteger(record.columns, "columns"),
    rows: optionalInteger(record.rows, "rows"),
    yieldTimeMs: optionalInteger(record.yieldTimeMs, "yieldTimeMs"),
    maxOutputTokens: optionalInteger(record.maxOutputTokens, "maxOutputTokens"),
  };
}

function decodeWriteInput(value: unknown): WriteStdinInput {
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_REQUEST", "Process write parameters must be an object.");
  return {
    workspaceId: requiredString(record.workspaceId, "workspaceId"),
    sessionId: requiredInteger(record.sessionId, "sessionId"),
    chars: optionalContentString(record.chars),
    columns: optionalInteger(record.columns, "columns"),
    rows: optionalInteger(record.rows, "rows"),
    yieldTimeMs: optionalInteger(record.yieldTimeMs, "yieldTimeMs"),
    maxOutputTokens: optionalInteger(record.maxOutputTokens, "maxOutputTokens"),
  };
}

function decodeEnvironment(value: unknown): Record<string, string> | undefined {
  if (value === undefined) return undefined;
  const record = asRecord(value);
  if (!record) throw new ProcessSessionDaemonError("INVALID_REQUEST", "Process environment must be an object.");
  const environment: Record<string, string> = {};
  for (const [key, entry] of Object.entries(record)) {
    if (typeof entry !== "string") {
      throw new ProcessSessionDaemonError("INVALID_REQUEST", `Process environment value for ${key} must be a string.`);
    }
    environment[key] = entry;
  }
  return environment;
}

function decodeEmptyParams(value: unknown): Record<string, never> {
  if (value === undefined) return {};
  const record = asRecord(value);
  if (!record || Object.keys(record).length > 0) {
    throw new ProcessSessionDaemonError("INVALID_REQUEST", "This process session daemon method does not accept parameters.");
  }
  return {};
}

function encodeResponse(response: ProcessSessionDaemonResponse): string {
  return `${JSON.stringify(response)}\n`;
}

function readRequestId(value: unknown): string {
  return optionalString(asRecord(value)?.requestId) ?? "";
}

function toDaemonError(error: unknown): ProcessSessionDaemonError {
  if (error instanceof ProcessSessionDaemonError) return error;
  if (error instanceof SyntaxError) {
    return new ProcessSessionDaemonError("INVALID_REQUEST", "Process session daemon request is not valid JSON.");
  }
  return new ProcessSessionDaemonError(
    "PROCESS_SESSION_ERROR",
    error instanceof Error ? error.message : String(error),
  );
}

function remoteError(error: { code: string; message: string; retryable?: boolean }): ProcessSessionDaemonError {
  return new ProcessSessionDaemonError(error.code, error.message, error.retryable ?? false);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function requiredString(value: unknown, field: string): string {
  const result = optionalString(value);
  if (!result) throw new ProcessSessionDaemonError("INVALID_REQUEST", `Missing ${field}.`);
  return result;
}

function requiredContentString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ProcessSessionDaemonError("INVALID_REQUEST", `Missing ${field}.`);
  }
  return value;
}

function optionalString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed || undefined;
}

function optionalContentString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function requiredInteger(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new ProcessSessionDaemonError("INVALID_REQUEST", `Invalid ${field}.`);
  }
  return value;
}

function optionalInteger(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined;
  return requiredInteger(value, field);
}

function requiredBoolean(value: unknown, field: string): boolean {
  if (typeof value !== "boolean") throw new ProcessSessionDaemonError("INVALID_RESPONSE", `Invalid ${field}.`);
  return value;
}

function optionalBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function requiredNonNegativeNumber(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new ProcessSessionDaemonError("INVALID_RESPONSE", `Invalid ${field}.`);
  }
  return value;
}

function removeProcessSessionDaemonFiles(paths: LocalAgentDaemonPaths): void {
  rmSync(paths.pidPath, { force: true });
  if (usesFilesystemSocket(paths.endpoint)) rmSync(paths.socketPath, { force: true });
}

function usesFilesystemSocket(endpoint: string): boolean {
  return process.platform !== "win32" && !endpoint.startsWith("\0");
}

function hashStateDir(stateDir: string): string {
  return createHash("sha256").update(stateDir).digest("hex").slice(0, 24);
}

function listen(server: NetServer, endpoint: string): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(endpoint, () => {
      server.off("error", reject);
      resolve();
    });
  });
}

function closeServer(server: NetServer | undefined): Promise<void> {
  if (!server) return Promise.resolve();
  return new Promise((resolve) => server.close(() => resolve()));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
