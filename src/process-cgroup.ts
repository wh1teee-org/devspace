import {
  accessSync,
  constants,
  lstatSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  rmdirSync,
  writeFileSync,
} from "node:fs";
import { join, posix, sep } from "node:path";

const CGROUP_FS = "/sys/fs/cgroup";
const PROC_SELF_CGROUP = "/proc/self/cgroup";
const SESSION_NAME = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const DEFAULT_RETIRE_TIMEOUT_MS = 2_000;
const RETIRE_POLL_MS = 10;

export interface CgroupCommandInvocation {
  executable: string;
  args: string[];
}

export function parseUnifiedCgroupPath(raw: string): string {
  const matches = raw
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => line.split(":"))
    .filter((parts) => parts.length === 3 && parts[0] === "0" && parts[1] === "");
  if (matches.length !== 1) {
    throw new Error("Process-session isolation requires exactly one unified cgroup v2 membership.");
  }
  const value = matches[0]![2]!;
  if (!value.startsWith("/") || posix.normalize(value) !== value || value.includes("\0")) {
    throw new Error("Process-session unified cgroup v2 path is invalid.");
  }
  return value;
}

export function parseCgroupPopulated(raw: string): boolean {
  const values = raw
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => line.trim().split(/\s+/))
    .filter((parts) => parts[0] === "populated");
  if (values.length !== 1 || !["0", "1"].includes(values[0]?.[1] ?? "")) {
    throw new Error("Process-session cgroup.events has no valid populated state.");
  }
  return values[0]![1] === "1";
}

export function cgroupEnterInvocation(
  cgroupPath: string,
  executable: string,
  args: string[],
): CgroupCommandInvocation {
  return {
    executable: "/bin/sh",
    args: [
      "-c",
      "cgroup_procs=$1; shift; printf '%s\\n' \"$$\" > \"$cgroup_procs\" || exit 126; exec \"$@\"",
      "devspace-cgroup-enter",
      join(cgroupPath, "cgroup.procs"),
      executable,
      ...args,
    ],
  };
}

export class ProcessSessionCgroup {
  constructor(readonly path: string) {}

  async retire(timeoutMs = DEFAULT_RETIRE_TIMEOUT_MS): Promise<void> {
    const events = join(this.path, "cgroup.events");
    if (parseCgroupPopulated(readFileSync(events, "utf8"))) {
      writeFileSync(join(this.path, "cgroup.kill"), "1\n", "utf8");
    }

    const deadline = Date.now() + timeoutMs;
    while (parseCgroupPopulated(readFileSync(events, "utf8"))) {
      if (Date.now() >= deadline) {
        throw new Error(`Process-session cgroup did not become empty: ${this.path}`);
      }
      await new Promise((resolve) => setTimeout(resolve, RETIRE_POLL_MS));
    }
    rmdirSync(this.path);
  }

  disposeEmpty(): void {
    if (parseCgroupPopulated(readFileSync(join(this.path, "cgroup.events"), "utf8"))) {
      throw new Error(`Cannot dispose populated process-session cgroup: ${this.path}`);
    }
    rmdirSync(this.path);
  }
}

export function createProcessSessionCgroup(
  name: string,
  options: {
    cgroupFs?: string;
    procSelfCgroup?: string;
    uid?: number;
  } = {},
): ProcessSessionCgroup {
  if (process.platform !== "linux" || typeof process.getuid !== "function") {
    throw new Error("Process-session cgroup isolation requires Linux cgroup v2.");
  }
  if (!SESSION_NAME.test(name)) {
    throw new Error("Process-session cgroup name is invalid.");
  }

  const uid = options.uid ?? process.getuid();
  const cgroupFs = realpathSync(options.cgroupFs ?? CGROUP_FS);
  const membership = parseUnifiedCgroupPath(
    readFileSync(options.procSelfCgroup ?? PROC_SELF_CGROUP, "utf8"),
  );
  const parent = realpathSync(`${cgroupFs}${membership}`);
  const prefix = cgroupFs.endsWith(sep) ? cgroupFs : `${cgroupFs}${sep}`;
  if (parent !== cgroupFs && !parent.startsWith(prefix)) {
    throw new Error("Process-session cgroup membership escaped cgroupfs.");
  }
  const parentMetadata = lstatSync(parent);
  if (!parentMetadata.isDirectory() || parentMetadata.isSymbolicLink() || parentMetadata.uid !== uid) {
    throw new Error(
      "Process-session cgroup isolation requires delegated ownership of the broker cgroup.",
    );
  }

  const path = join(parent, name);
  mkdirSync(path);
  try {
    const metadata = lstatSync(path);
    if (!metadata.isDirectory() || metadata.isSymbolicLink() || metadata.uid !== uid) {
      throw new Error("Process-session child cgroup metadata is unsafe.");
    }
    accessSync(join(path, "cgroup.procs"), constants.W_OK);
    accessSync(join(path, "cgroup.events"), constants.R_OK);
    accessSync(join(path, "cgroup.kill"), constants.W_OK);
    return new ProcessSessionCgroup(path);
  } catch (error) {
    try {
      rmdirSync(path);
    } catch {
      // Preserve the original validation error; a non-empty cgroup remains visible for diagnosis.
    }
    throw error;
  }
}
