import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  ProcessSessionClient,
  ProcessSessionDaemon,
  processSessionDaemonPaths,
  processSessionDaemonExecArgv,
  spawnProcessSessionDaemon,
} from "./process-session-daemon.js";

const node = process.platform === "win32"
  ? `"${process.execPath}"`
  : JSON.stringify(process.execPath);

test("daemon launch strips parent eval and inspection modes", () => {
  assert.deepEqual(
    processSessionDaemonExecArgv([
      "--max-old-space-size=4096",
      "--inspect=127.0.0.1:9229",
      "--eval",
      "console.log('parent')",
      "--print=1+1",
      "--check",
      "--import",
      "tsx",
    ]),
    ["--max-old-space-size=4096", "--import", "tsx"],
  );
});

test("linux process daemon uses a bounded abstract socket endpoint", () => {
  const stateDir = join("/", "very-long-state-root", "x".repeat(300));
  const paths = processSessionDaemonPaths(stateDir, "linux");
  assert.equal(paths.endpoint.startsWith("\0devspace-processd-"), true);
  assert.equal(Buffer.byteLength(paths.endpoint) < 100, true);
});

test("externally managed process daemon never spawns a fallback", () => {
  assert.throws(
    () => spawnProcessSessionDaemon("/tmp/devspace-managed-processd", {
      DEVSPACE_PROCESS_SESSION_DAEMON_MANAGED: "1",
    }),
    (error: unknown) =>
      error instanceof Error
      && error.message.includes("externally managed"),
  );
});

test("process sessions survive client recreation", async () => {
  const stateDir = await mkdtemp(join(tmpdir(), "devspace-process-daemon-test-"));
  const daemon = new ProcessSessionDaemon({ stateDir });
  await daemon.start();

  try {
    const firstClient = new ProcessSessionClient({ stateDir });
    const started = await firstClient.start({
      workspaceId: "workspace-a",
      cwd: process.cwd(),
      command: `${node} -e "setTimeout(() => console.log('survived'), 150)"`,
      yieldTimeMs: 5,
    });
    assert.equal(started.running, true);
    assert.ok(started.sessionId);

    const secondClient = new ProcessSessionClient({ stateDir });
    const completed = await secondClient.write({
      workspaceId: "workspace-a",
      sessionId: started.sessionId,
      yieldTimeMs: 2_000,
    });

    assert.equal(completed.running, false);
    assert.equal(completed.exitCode, 0);
    assert.match(completed.output, /survived/);
  } finally {
    await daemon.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});

test("workspace release guard blocks process starts until released", async () => {
  const stateDir = await mkdtemp(join(tmpdir(), "devspace-process-daemon-release-guard-test-"));
  const daemon = new ProcessSessionDaemon({ stateDir });
  await daemon.start();

  try {
    const client = new ProcessSessionClient({ stateDir });
    const guard = await client.acquireWorkspaceReleaseGuard("workspace-a");

    await assert.rejects(
      () => client.start({
        workspaceId: "workspace-a",
        cwd: process.cwd(),
        command: `${node} -e "console.log('must-not-start')"`,
        yieldTimeMs: 2_000,
      }),
      /being released/,
    );

    const otherWorkspace = await client.start({
      workspaceId: "workspace-b",
      cwd: process.cwd(),
      command: `${node} -e "console.log('other-workspace')"`,
      yieldTimeMs: 2_000,
    });
    assert.equal(otherWorkspace.exitCode, 0);

    await guard.release();
    const releasedWorkspace = await client.start({
      workspaceId: "workspace-a",
      cwd: process.cwd(),
      command: `${node} -e "console.log('released')"`,
      yieldTimeMs: 2_000,
    });
    assert.equal(releasedWorkspace.exitCode, 0);
  } finally {
    await daemon.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});

test("process daemon preserves the calling environment", async () => {
  const stateDir = await mkdtemp(join(tmpdir(), "devspace-process-daemon-env-test-"));
  const daemon = new ProcessSessionDaemon({ stateDir });
  await daemon.start();
  const previous = process.env.DEVSPACE_PROCESS_DAEMON_TEST;
  process.env.DEVSPACE_PROCESS_DAEMON_TEST = "preserved";

  try {
    const client = new ProcessSessionClient({ stateDir });
    const completed = await client.start({
      workspaceId: "workspace-a",
      cwd: process.cwd(),
      command: `${node} -e "console.log(process.env.DEVSPACE_PROCESS_DAEMON_TEST)"`,
      yieldTimeMs: 2_000,
    });
    assert.equal(completed.exitCode, 0);
    assert.match(completed.output, /preserved/);
  } finally {
    if (previous === undefined) delete process.env.DEVSPACE_PROCESS_DAEMON_TEST;
    else process.env.DEVSPACE_PROCESS_DAEMON_TEST = previous;
    await daemon.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});

test("process daemon refuses shutdown while a command is running", async () => {
  const stateDir = await mkdtemp(join(tmpdir(), "devspace-process-daemon-stop-test-"));
  const daemon = new ProcessSessionDaemon({ stateDir });
  await daemon.start();

  try {
    const client = new ProcessSessionClient({ stateDir });
    const started = await client.start({
      workspaceId: "workspace-a",
      cwd: process.cwd(),
      command: `${node} -e "setTimeout(() => {}, 500)"`,
      yieldTimeMs: 5,
    });
    assert.equal(started.running, true);
    assert.ok(started.sessionId);

    await assert.rejects(
      () => client.acquireWorkspaceReleaseGuard("workspace-a"),
      /running process session/,
    );

    await assert.rejects(() => client.stop(), /running process sessions/);

    await client.write({
      workspaceId: "workspace-a",
      sessionId: started.sessionId,
      chars: "\u0003",
      yieldTimeMs: 2_000,
    });
  } finally {
    await daemon.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});
