import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { ProcessSessionManager } from "./process-sessions.js";

const enabled =
  process.platform === "linux"
  && process.env.DEVSPACE_TEST_PROCESS_CGROUP === "1";

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

test("delegated Linux process session retires hidden background descendants", { skip: !enabled }, async () => {
  const root = await mkdtemp(join(tmpdir(), "devspace-cgroup-session-"));
  const pidFile = join(root, "background.pid");
  const previous = process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
  process.env.DEVSPACE_PROCESS_SESSION_CGROUP = "1";
  const manager = new ProcessSessionManager({ completedSessionTtlMs: 100 });
  let backgroundPid: number | undefined;

  try {
    const result = await manager.start({
      workspaceId: "systemd-session",
      cwd: root,
      command: `sleep 20 >/dev/null 2>&1 & echo $! > ${JSON.stringify(pidFile)}`,
      yieldTimeMs: 5_000,
    });
    assert.equal(result.running, false);
    assert.equal(result.exitCode, 0);

    backgroundPid = Number.parseInt((await readFile(pidFile, "utf8")).trim(), 10);
    assert.ok(Number.isInteger(backgroundPid) && backgroundPid > 1);
    assert.equal(processExists(backgroundPid), false);
  } finally {
    manager.shutdown();
    if (backgroundPid && processExists(backgroundPid)) {
      try {
        process.kill(backgroundPid, "SIGKILL");
      } catch {
        // Best-effort fixture cleanup if the lifecycle assertion fails.
      }
    }
    if (previous === undefined) delete process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
    else process.env.DEVSPACE_PROCESS_SESSION_CGROUP = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test("delegated Linux process session preserves interrupt semantics", { skip: !enabled }, async () => {
  const root = await mkdtemp(join(tmpdir(), "devspace-cgroup-interrupt-"));
  const previous = process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
  process.env.DEVSPACE_PROCESS_SESSION_CGROUP = "1";
  const manager = new ProcessSessionManager({ completedSessionTtlMs: 100 });
  let foregroundPid: number | undefined;

  try {
    const started = await manager.start({
      workspaceId: "systemd-interrupt",
      cwd: root,
      command: `${JSON.stringify(process.execPath)} -e "console.log(process.pid); setInterval(() => {}, 1000)"`,
      yieldTimeMs: 50,
    });
    assert.equal(started.running, true);
    assert.ok(started.sessionId);

    const ready = started.output.trim()
      ? started
      : await manager.write({
          workspaceId: "systemd-interrupt",
          sessionId: started.sessionId,
          yieldTimeMs: 5_000,
        });
    const pidMatch = ready.output.match(/(?:^|\n)(\d+)(?:\n|$)/);
    assert.ok(pidMatch, `expected foreground pid in output, got: ${JSON.stringify(ready.output)}`);
    foregroundPid = Number.parseInt(pidMatch[1], 10);
    assert.equal(processExists(foregroundPid), true);

    const interrupted = await manager.write({
      workspaceId: "systemd-interrupt",
      sessionId: ready.sessionId ?? started.sessionId,
      chars: "\u0003",
      yieldTimeMs: 2_000,
    });
    assert.equal(interrupted.running, false);
    assert.equal(interrupted.signal, "SIGINT");
    assert.equal(interrupted.exitCode, undefined);
    assert.equal(processExists(foregroundPid), false);
  } finally {
    manager.shutdown();
    if (foregroundPid && processExists(foregroundPid)) {
      try {
        process.kill(foregroundPid, "SIGKILL");
      } catch {
        // Best-effort fixture cleanup if the lifecycle assertion fails.
      }
    }
    if (previous === undefined) delete process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
    else process.env.DEVSPACE_PROCESS_SESSION_CGROUP = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test("delegated Linux PTY session retires detached descendants", { skip: !enabled }, async () => {
  const root = await mkdtemp(join(tmpdir(), "devspace-cgroup-pty-"));
  const previous = process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
  process.env.DEVSPACE_PROCESS_SESSION_CGROUP = "1";
  const manager = new ProcessSessionManager({ completedSessionTtlMs: 100 });
  let backgroundPid: number | undefined;

  try {
    const result = await manager.start({
      workspaceId: "systemd-pty",
      cwd: root,
      command:
        `${JSON.stringify(process.execPath)} -e "const { spawn } = require('node:child_process'); `
        + "const child = spawn('sleep', ['20'], { detached: true, stdio: 'ignore' }); "
        + "child.unref(); console.log(child.pid)\"",
      tty: true,
      columns: 80,
      rows: 24,
      yieldTimeMs: 5_000,
    });
    assert.equal(result.running, false);
    assert.equal(result.exitCode, 0);
    const pidMatch = result.output.match(/(?:^|\r?\n)(\d+)(?:\r?\n|$)/);
    assert.ok(pidMatch, `expected background pid in PTY output, got: ${JSON.stringify(result.output)}`);
    backgroundPid = Number.parseInt(pidMatch[1], 10);
    assert.equal(processExists(backgroundPid), false);
  } finally {
    manager.shutdown();
    if (backgroundPid && processExists(backgroundPid)) {
      try {
        process.kill(backgroundPid, "SIGKILL");
      } catch {
        // Best-effort fixture cleanup if the lifecycle assertion fails.
      }
    }
    if (previous === undefined) delete process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
    else process.env.DEVSPACE_PROCESS_SESSION_CGROUP = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test("delegated Linux pipe session preserves stdin interaction", { skip: !enabled }, async () => {
  const root = await mkdtemp(join(tmpdir(), "devspace-cgroup-stdin-"));
  const previous = process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
  process.env.DEVSPACE_PROCESS_SESSION_CGROUP = "1";
  const manager = new ProcessSessionManager({ completedSessionTtlMs: 100 });

  try {
    const started = await manager.start({
      workspaceId: "systemd-stdin",
      cwd: root,
      command:
        `${JSON.stringify(process.execPath)} -e "process.stdin.once('data', data => { `
        + "console.log('managed-input:' + data.toString().trim()); process.exit(0); })\"",
      yieldTimeMs: 50,
    });
    assert.equal(started.running, true);
    assert.ok(started.sessionId);

    const completed = await manager.write({
      workspaceId: "systemd-stdin",
      sessionId: started.sessionId,
      chars: "hello\n",
      yieldTimeMs: 5_000,
    });
    assert.equal(completed.running, false);
    assert.equal(completed.exitCode, 0);
    assert.match(completed.output, /managed-input:hello/);
  } finally {
    manager.shutdown();
    if (previous === undefined) delete process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
    else process.env.DEVSPACE_PROCESS_SESSION_CGROUP = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test("delegated Linux PTY session preserves resize interaction", { skip: !enabled }, async () => {
  const root = await mkdtemp(join(tmpdir(), "devspace-cgroup-resize-"));
  const previous = process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
  process.env.DEVSPACE_PROCESS_SESSION_CGROUP = "1";
  const manager = new ProcessSessionManager({ completedSessionTtlMs: 100 });

  try {
    const started = await manager.start({
      workspaceId: "systemd-resize",
      cwd: root,
      command:
        `${JSON.stringify(process.execPath)} -e "setTimeout(() => `
        + "console.log('managed-columns:' + process.stdout.columns), 500)\"",
      tty: true,
      columns: 80,
      rows: 24,
      yieldTimeMs: 50,
    });
    assert.equal(started.running, true);
    assert.ok(started.sessionId);

    const resized = await manager.write({
      workspaceId: "systemd-resize",
      sessionId: started.sessionId,
      columns: 120,
      rows: 30,
      yieldTimeMs: 5_000,
    });
    assert.equal(resized.running, false);
    assert.equal(resized.exitCode, 0);
    assert.match(resized.output, /managed-columns:120/);
  } finally {
    manager.shutdown();
    if (previous === undefined) delete process.env.DEVSPACE_PROCESS_SESSION_CGROUP;
    else process.env.DEVSPACE_PROCESS_SESSION_CGROUP = previous;
    await rm(root, { recursive: true, force: true });
  }
});
