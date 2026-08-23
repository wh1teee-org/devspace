#!/usr/bin/env node
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import {
  ProcessSessionDaemon,
} from "./process-session-daemon.js";
import { LocalAgentDaemonAlreadyRunningError } from "./local-agent-daemon-lifecycle.js";

const stateDir = resolve(
  process.env.DEVSPACE_STATE_DIR ?? join(homedir(), ".local", "share", "devspace"),
);
const daemon = new ProcessSessionDaemon({ stateDir });
let shuttingDown = false;

const shutdown = () => {
  if (shuttingDown) return;
  shuttingDown = true;
  void daemon.close().finally(() => process.exit(0));
};

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

try {
  await daemon.start();
} catch (error) {
  if (error instanceof LocalAgentDaemonAlreadyRunningError) process.exit(0);
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
