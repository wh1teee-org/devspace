import assert from "node:assert/strict";
import {
  cgroupEnterInvocation,
  parseCgroupPopulated,
  parseUnifiedCgroupPath,
} from "./process-cgroup.js";

assert.equal(
  parseUnifiedCgroupPath("0::/ci.slice/ci-workload.slice/devspace-process-session-daemon.service\n"),
  "/ci.slice/ci-workload.slice/devspace-process-session-daemon.service",
);
assert.throws(
  () => parseUnifiedCgroupPath("1:name=/legacy\n"),
  /unified cgroup v2/,
);
assert.throws(
  () => parseUnifiedCgroupPath("0::/first\n0::/second\n"),
  /exactly one unified cgroup/,
);

assert.equal(parseCgroupPopulated("populated 1\nfrozen 0\n"), true);
assert.equal(parseCgroupPopulated("populated 0\nfrozen 0\n"), false);
assert.throws(() => parseCgroupPopulated("frozen 0\n"), /populated/);

assert.deepEqual(
  cgroupEnterInvocation(
    "/sys/fs/cgroup/ci.slice/ci-workload.slice/devspace.service/session-7",
    "/bin/bash",
    ["-c", "echo ok"],
  ),
  {
    executable: "/bin/sh",
    args: [
      "-c",
      "cgroup_procs=$1; shift; printf '%s\\n' \"$$\" > \"$cgroup_procs\" || exit 126; exec \"$@\"",
      "devspace-cgroup-enter",
      "/sys/fs/cgroup/ci.slice/ci-workload.slice/devspace.service/session-7/cgroup.procs",
      "/bin/bash",
      "-c",
      "echo ok",
    ],
  },
);
