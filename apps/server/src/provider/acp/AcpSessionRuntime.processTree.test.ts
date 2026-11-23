// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";

import { describe, expect, it, vi } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";

const identity = (
  pid: number,
  ppid: number,
  pgid: number,
  sid: number,
  startTime = String(pid),
  state = "S",
): AcpSessionRuntime.AcpPosixProcessIdentity => ({
  executable: `/proc/${pid}/exe`,
  pgid,
  pid,
  ppid,
  sid,
  startTime,
  state,
});

function makeController(input: {
  readonly processes: ReadonlyArray<AcpSessionRuntime.AcpPosixProcessIdentity>;
  readonly onProcess?: (
    processes: Map<number, AcpSessionRuntime.AcpPosixProcessIdentity>,
    pid: number,
    signal: NodeJS.Signals,
  ) => void;
}) {
  const processes = new Map(input.processes.map((process) => [process.pid, process]));
  const signals: Array<string> = [];
  const controller: AcpSessionRuntime.AcpPosixProcessTreeController = {
    childPidsOf: (pid) =>
      [...processes.values()]
        .filter((process) => process.ppid === pid)
        .map((process) => process.pid),
    childrenOf: (pid) => [...processes.values()].filter((process) => process.ppid === pid),
    identity: (pid) => processes.get(pid),
    snapshot: () => [...processes.values()],
    signalProcess: (pid, signal) => {
      signals.push(`process:${pid}:${signal}`);
      if (input.onProcess) return input.onProcess(processes, pid, signal);
      processes.delete(pid);
    },
  };
  return { controller, processes, signals };
}

const server = () => identity(process.pid, 1, process.pid, process.pid, "server");

describe("terminatePosixOwnedProcessTree", () => {
  it("parses unified cgroup paths and escaped cgroup2 mountinfo", () => {
    expect(AcpSessionRuntime.parseUnifiedCgroupPath("0::/user.slice/app.scope\n")).toBe(
      "/user.slice/app.scope",
    );
    expect(AcpSessionRuntime.parseUnifiedCgroupPath("0::/one\n0::/two\n")).toBeUndefined();
    expect(AcpSessionRuntime.isSafeCgroupPath("/user.slice/app.scope")).toBe(true);
    expect(AcpSessionRuntime.isSafeCgroupPath("/user.slice/../escape")).toBe(false);
    expect(AcpSessionRuntime.isSafeCgroupPath("/user.slice/app.scope (deleted)")).toBe(false);
    expect(
      AcpSessionRuntime.parseCgroup2Mounts(
        "42 31 0:37 /user.slice /sys/fs/cgroup/user\\040mount rw - cgroup2 cgroup2 rw\n",
      )[0],
    ).toEqual({ mountPoint: "/sys/fs/cgroup/user mount", root: "/user.slice" });
    expect(
      AcpSessionRuntime.parseCgroup2Mounts("42 31 0:37 / /sys/fs/cgroup rw - tmpfs tmpfs rw\n")[0],
    ).toBeUndefined();
    expect(
      AcpSessionRuntime.parseCgroup2Mounts(
        "41 31 0:36 /other /sys/fs/cgroup/other rw - cgroup2 cgroup2 rw\n" +
          "42 31 0:37 /user.slice /sys/fs/cgroup/user rw - cgroup2 cgroup2 rw\n",
      ),
    ).toHaveLength(2);
  });

  it.live("sweeps empty t3-acp sibling leases owned by dead pids only", () =>
    Effect.sync(() => {
      const scratchRoot = NodePath.join(process.cwd(), "tmp");
      NodeFS.mkdirSync(scratchRoot, { recursive: true });
      const parent = NodeFS.mkdtempSync(NodePath.join(scratchRoot, "acp-cgroup-stale-siblings-"));
      // Unlike cgroupfs, temp directories cannot rmdir with control files; inject populated/remove.
      const populatedByPath = new Map<string, "0" | "1">();
      const writeSibling = (name: string, populated: "0" | "1" | null) => {
        const path = NodePath.join(parent, name);
        NodeFS.mkdirSync(path);
        if (populated !== null) populatedByPath.set(path, populated);
        return path;
      };
      const deadPid = 2_147_483_646;
      const liveForeignPid = 2_147_483_645;
      const staleEmpty = writeSibling(`t3-acp-${deadPid}-aaaa`, "0");
      const currentPidSibling = writeSibling(`t3-acp-${process.pid}-bbbb`, "0");
      const liveForeignSibling = writeSibling(`t3-acp-${liveForeignPid}-cccc`, "0");
      const stalePopulated = writeSibling(`t3-acp-${deadPid}-dddd`, "1");
      const unrelated = writeSibling("other-lease", "0");
      const isProcessAlive = (pid: number) => pid === process.pid || pid === liveForeignPid;
      try {
        AcpSessionRuntime.sweepStaleLinuxCgroupSiblings(parent, {
          isProcessAlive,
          readPopulated: (path) => populatedByPath.get(path),
          remove: (path) => {
            populatedByPath.delete(path);
            NodeFS.rmdirSync(path);
          },
        });
        expect(NodeFS.existsSync(staleEmpty)).toBe(false);
        expect(NodeFS.existsSync(currentPidSibling)).toBe(true);
        expect(NodeFS.existsSync(liveForeignSibling)).toBe(true);
        expect(NodeFS.existsSync(stalePopulated)).toBe(true);
        expect(NodeFS.existsSync(unrelated)).toBe(true);
        writeSibling(`t3-acp-${deadPid}-eeee`, null);
        AcpSessionRuntime.sweepStaleLinuxCgroupSiblings(parent, {
          isProcessAlive,
          readPopulated: (path) => populatedByPath.get(path),
          remove: (path) => {
            populatedByPath.delete(path);
            NodeFS.rmdirSync(path);
          },
        });
        expect(NodeFS.existsSync(NodePath.join(parent, `t3-acp-${deadPid}-eeee`))).toBe(true);
        expect(NodeFS.existsSync(currentPidSibling)).toBe(true);
      } finally {
        NodeFS.rmSync(parent, { recursive: true, force: true });
      }
    }),
  );

  it.live("preserves exact argv and strips wrapper-only environment before exec", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) !== "linux") return;
      const lease = AcpSessionRuntime.makeLinuxCgroupController().create();
      if (lease === undefined) return;
      const scratchRoot = NodePath.join(process.cwd(), "tmp");
      NodeFS.mkdirSync(scratchRoot, { recursive: true });
      const scratch = NodeFS.mkdtempSync(NodePath.join(scratchRoot, "acp-cgroup-wrapper-"));
      const linkedNode = NodePath.join(scratch, "T3 Code AppImage 'quoted'");
      const bareGrok = NodePath.join(scratch, "grok");
      const relativeBin = NodePath.join(scratch, "relative-bin");
      const directoryBin = NodePath.join(scratch, "directory-bin");
      const validBin = NodePath.join(scratch, "valid-bin");
      const outputPath = NodePath.join(scratch, "argv.json");
      NodeFS.mkdirSync(relativeBin);
      NodeFS.mkdirSync(NodePath.join(directoryBin, "grok"), { recursive: true });
      NodeFS.mkdirSync(validBin);
      NodeFS.symlinkSync(process.execPath, linkedNode);
      NodeFS.symlinkSync(process.execPath, bareGrok);
      NodeFS.symlinkSync(process.execPath, NodePath.join(relativeBin, "grok"));
      NodeFS.symlinkSync(process.execPath, NodePath.join(validBin, "grok"));
      expect(
        AcpSessionRuntime.resolveLinuxCgroupTargetCommand("grok", scratch, { PATH: scratch }),
      ).toBe(bareGrok);
      expect(AcpSessionRuntime.resolveLinuxCgroupTargetCommand("grok", scratch, { PATH: "" })).toBe(
        bareGrok,
      );
      // Undefined PATH searches /usr/bin:/bin; the bareGrok fixture lives only in cwd.
      expect(
        AcpSessionRuntime.resolveLinuxCgroupTargetCommand("grok", scratch, { PATH: undefined }),
      ).toBeUndefined();
      expect(
        AcpSessionRuntime.resolveLinuxCgroupTargetCommand("sh", scratch, { PATH: undefined }),
      ).toBeDefined();
      expect(
        AcpSessionRuntime.resolveLinuxCgroupTargetCommand("grok", scratch, {
          PATH: "relative-bin",
        }),
      ).toBe(NodePath.join(relativeBin, "grok"));
      expect(
        AcpSessionRuntime.resolveLinuxCgroupTargetCommand("grok", scratch, {
          PATH: `directory-bin${NodePath.delimiter}valid-bin`,
        }),
      ).toBe(NodePath.join(validBin, "grok"));
      const target = [
        'const fs = require("node:fs");',
        "fs.writeFileSync(process.argv[1], JSON.stringify({",
        "  args: process.argv.slice(2),",
        "  electron: process.env.ELECTRON_RUN_AS_NODE,",
        "  wrapper: process.env.T3_ACP_CGROUP_WRAPPER,",
        "}));",
      ].join("\n");
      const wrapped = AcpSessionRuntime.wrapCommandForLinuxCgroup(lease, linkedNode, [
        "-e",
        target,
        outputPath,
        "space value",
        "single'quote",
        'double"quote',
      ]);
      try {
        const result = NodeChildProcess.spawnSync(wrapped.command, wrapped.args, {
          encoding: "utf8",
          env: {
            ...process.env,
            ELECTRON_RUN_AS_NODE: "1",
            T3_ACP_CGROUP_WRAPPER: "1",
          },
        });
        expect(result.status, result.stderr).toBe(0);
        expect(NodeFS.readFileSync(outputPath, "utf8")).toBe(
          '{"args":["space value","single\'quote","double\\"quote"]}',
        );
      } finally {
        yield* AcpSessionRuntime.terminateLinuxCgroupLease(lease).pipe(Effect.ignore);
        NodeFS.rmSync(scratch, { recursive: true, force: true });
      }
    }),
  );

  it("contains a packaged-runtime command without requiring cgroup delegation", () => {
    if (HostProcess.Platform.defaultValue() !== "linux") return;
    const scratchRoot = NodePath.join(process.cwd(), "tmp");
    NodeFS.mkdirSync(scratchRoot, { recursive: true });
    const scratch = NodeFS.mkdtempSync(NodePath.join(scratchRoot, "acp-cgroup-wrapper-fake-"));
    const leasePath = NodePath.join(scratch, "lease with spaces");
    const outputPath = NodePath.join(scratch, "argv output");
    NodeFS.mkdirSync(leasePath);
    NodeFS.writeFileSync(NodePath.join(leasePath, "cgroup.procs"), "");
    const relativePath = AcpSessionRuntime.parseUnifiedCgroupPath(
      NodeFS.readFileSync("/proc/self/cgroup", "utf8"),
    );
    expect(relativePath).toBeDefined();
    const lease: AcpSessionRuntime.AcpLinuxCgroupLease = {
      contains: () => false,
      exists: () => true,
      path: leasePath,
      relativePath: relativePath!,
      kill: () => undefined,
      populated: () => false,
      remove: () => undefined,
    };
    const packagedExecPath = vi.spyOn(process, "execPath", "get").mockReturnValue("/bin/sh");
    try {
      const wrapped = AcpSessionRuntime.wrapCommandForLinuxCgroup(lease, "/bin/sh", [
        "-c",
        'printf "%s\\n" "$0" "$1" "$2" "$3" "${ELECTRON_RUN_AS_NODE-}" "${T3_ACP_CGROUP_WRAPPER-}" > "$4"',
        "packaged-target",
        "space value",
        "single'quote",
        'double"quote',
        outputPath,
      ]);
      const result = NodeChildProcess.spawnSync(wrapped.command, wrapped.args, {
        encoding: "utf8",
        env: {
          ...process.env,
          ELECTRON_RUN_AS_NODE: "1",
          T3_ACP_CGROUP_WRAPPER: "1",
        },
      });
      expect(result.status, result.stderr).toBe(0);
      expect(NodeFS.readFileSync(outputPath, "utf8")).toBe(
        "packaged-target\nspace value\nsingle'quote\ndouble\"quote\n\n\n",
      );
      expect(NodeFS.readFileSync(NodePath.join(leasePath, "cgroup.procs"), "utf8")).toMatch(
        /^\d+\n$/,
      );

      const mismatch = AcpSessionRuntime.wrapCommandForLinuxCgroup(
        { ...lease, relativePath: "/not-the-current-cgroup" },
        "/bin/sh",
        ["-c", 'printf executed > "$0"', outputPath],
      );
      NodeFS.rmSync(outputPath);
      const mismatchResult = NodeChildProcess.spawnSync(mismatch.command, mismatch.args, {
        encoding: "utf8",
      });
      expect(mismatchResult.status, mismatchResult.stderr).toBe(126);
      expect(NodeFS.existsSync(outputPath)).toBe(false);

      const missingTarget = AcpSessionRuntime.wrapCommandForLinuxCgroup(
        lease,
        "/nonexistent-t3-probe",
        [],
      );
      const missingTargetResult = NodeChildProcess.spawnSync(
        missingTarget.command,
        missingTarget.args,
        {
          encoding: "utf8",
        },
      );
      expect(missingTargetResult.status, missingTargetResult.stderr).toBe(125);
      // dash runs EXIT traps after failed exec; bash does not, so exercise both.
      const missingTargetBash = NodeChildProcess.spawnSync("bash", missingTarget.args, {
        encoding: "utf8",
      });
      if ((missingTargetBash.error as NodeJS.ErrnoException | undefined)?.code !== "ENOENT") {
        expect(missingTargetBash.status, missingTargetBash.stderr).toBe(125);
        expect(missingTargetBash.stderr).toContain("/nonexistent-t3-probe");
      }
    } finally {
      packagedExecPath.mockRestore();
      NodeFS.rmSync(scratch, { recursive: true, force: true });
    }
  });

  it("sends TERM only to the exact captured root while it remains in the child cgroup", () => {
    const fixture = makeController({
      processes: [server(), identity(100, process.pid, 100, 100, "owned")],
    });
    const ownedRoot: AcpSessionRuntime.AcpOwnedPosixProcess = {
      ...identity(100, process.pid, 100, 100, "owned"),
      parentExecutable: server().executable,
      parentStartTime: server().startTime,
    };
    const root: AcpSessionRuntime.AcpPosixOwnershipRoot = {
      captureAttempted: true,
      value: ownedRoot,
    };
    const lease: AcpSessionRuntime.AcpLinuxCgroupLease = {
      contains: () => true,
      exists: () => true,
      path: "/test/t3-acp-root",
      relativePath: "/test/t3-acp-root",
      kill: () => undefined,
      populated: () => true,
      remove: () => undefined,
    };
    AcpSessionRuntime.signalLinuxCgroupRootTerm({ controller: fixture.controller, lease, root });
    expect(fixture.signals).toEqual(["process:100:SIGTERM"]);

    const reused = makeController({
      processes: [server(), identity(100, process.pid, 100, 100, "reused")],
    });
    AcpSessionRuntime.signalLinuxCgroupRootTerm({ controller: reused.controller, lease, root });
    expect(reused.signals).toEqual([]);

    const migrated = makeController({
      processes: [server(), identity(100, process.pid, 100, 100, "owned")],
    });
    AcpSessionRuntime.signalLinuxCgroupRootTerm({
      controller: migrated.controller,
      lease: { ...lease, contains: () => false },
      root,
    });
    expect(migrated.signals).toEqual([]);
  });

  it.live("keeps ownership across a fork-before-exec executable transition", () =>
    Effect.gen(function* () {
      const beforeExec = { ...identity(110, 100, 110, 110), executable: "/provider/fork" };
      const fixture = makeController({
        processes: [server(), identity(100, process.pid, 100, 100), beforeExec],
      });
      const frontier = new Map<number, AcpSessionRuntime.AcpOwnedPosixProcess>();
      const childQueues = new Map<number, Array<number>>();
      const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
      const root: AcpSessionRuntime.AcpPosixOwnershipRoot = { value: undefined };
      AcpSessionRuntime.observePosixOwnershipLedger({
        childQueues,
        controller: fixture.controller,
        frontier,
        ledger,
        root,
        rootPid: 100,
      });
      fixture.processes.set(110, { ...beforeExec, executable: "/usr/bin/bash" });
      AcpSessionRuntime.observePosixOwnershipLedger({
        childQueues,
        controller: fixture.controller,
        frontier,
        ledger,
        root,
        rootPid: 100,
      });

      expect([...ledger.values()].find((process) => process.pid === 110)?.executable).toBe(
        "/usr/bin/bash",
      );
      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller: fixture.controller,
        grace: 0,
        ledger,
        rootPid: 100,
      });
      expect(fixture.signals).toContain("process:110:SIGTERM");
      expect(fixture.processes.has(110)).toBe(false);
    }),
  );

  it("never adopts a replacement PID after initial root capture failed", () => {
    const fixture = makeController({ processes: [server()] });
    const frontier = new Map<number, AcpSessionRuntime.AcpOwnedPosixProcess>();
    const childQueues = new Map<number, Array<number>>();
    const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
    const root: AcpSessionRuntime.AcpPosixOwnershipRoot = { value: undefined };
    const capture = () => {
      try {
        AcpSessionRuntime.observePosixOwnershipLedger({
          childQueues,
          controller: fixture.controller,
          frontier,
          ledger,
          root,
          rootPid: 100,
        });
        return undefined;
      } catch (error) {
        return error;
      }
    };
    expect(capture()).toMatchObject({
      detail: "ACP process 100 exited before its ownership ledger was captured",
    });
    fixture.processes.set(100, identity(100, process.pid, 100, 100, "reused"));
    expect(capture()).toMatchObject({
      detail: "ACP process 100 exited before its ownership ledger was captured",
    });
    expect(ledger.size).toBe(0);
  });

  it.live("rotates more than 64 live parents without scanning retained tombstones", () =>
    Effect.gen(function* () {
      // Use a different synthetic parent group; the fixture shares the runner's real group, which cleanup skips.
      const parentPidBase = process.pid < 500_000 ? 500_000 : 1_000;
      const parents = Array.from({ length: 130 }, (_, index) =>
        identity(parentPidBase + index, 100, parentPidBase + index, parentPidBase + index),
      );
      let childListReads = 0;
      let identityCalls = 0;
      let snapshotCalls = 0;
      const fixture = makeController({
        processes: [server(), identity(100, process.pid, 100, 100)],
      });
      const controller: AcpSessionRuntime.AcpPosixProcessTreeController = {
        ...fixture.controller,
        childPidsOf: (pid) => {
          childListReads += 1;
          return fixture.controller.childPidsOf(pid);
        },
        identity: (pid) => {
          identityCalls += 1;
          return fixture.controller.identity(pid);
        },
        snapshot: () => {
          snapshotCalls += 1;
          return fixture.controller.snapshot();
        },
      };
      const frontier = new Map<number, AcpSessionRuntime.AcpOwnedPosixProcess>();
      const childQueues = new Map<number, Array<number>>();
      const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
      const root: AcpSessionRuntime.AcpPosixOwnershipRoot = { value: undefined };
      for (let index = 0; index < 5_000; index += 1) {
        const tombstone = identity(100_000 + index, 1, 100_000 + index, 100_000 + index);
        ledger.set(`${tombstone.pid}:${tombstone.startTime}`, {
          ...tombstone,
          parentExecutable: undefined,
          parentStartTime: "",
        });
      }
      AcpSessionRuntime.observePosixOwnershipLedger({
        childQueues,
        controller,
        frontier,
        ledger,
        root,
        rootPid: 100,
      });
      for (const parent of parents) {
        fixture.processes.set(parent.pid, parent);
        fixture.processes.set(
          parent.pid + 10_000,
          identity(parent.pid + 10_000, parent.pid, parent.pgid, parent.sid),
        );
      }
      let passes = 0;
      while (
        !parents.every((parent) =>
          [...ledger.values()].some((owned) => owned.pid === parent.pid + 10_000),
        ) &&
        passes < 40
      ) {
        childListReads = 0;
        identityCalls = 0;
        AcpSessionRuntime.observePosixOwnershipLedger({
          childQueues,
          controller,
          frontier,
          ledger,
          maxProcesses: 64,
          root,
          rootPid: 100,
        });
        expect(identityCalls + childListReads).toBeLessThanOrEqual(64);
        passes += 1;
      }

      const missing = parents.filter(
        (parent) => ![...ledger.values()].some((owned) => owned.pid === parent.pid + 10_000),
      );
      expect(missing, `missing after ${passes} passes`).toEqual([]);
      expect(passes).toBeGreaterThan(2);
      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller,
        grace: 0,
        ledger,
        rootPid: 100,
      });
      expect(snapshotCalls).toBe(10);
      expect(fixture.processes.size).toBe(1);
    }),
  );

  it.live("terminates nested owned groups bottom-up and catches a TERM fork race", () =>
    Effect.gen(function* () {
      let forked = false;
      const fixture = makeController({
        processes: [
          server(),
          identity(100, process.pid, 100, 100),
          identity(110, 100, 110, 110),
          identity(115, 110, 115, 110),
          identity(120, 110, 120, 120),
          identity(121, 120, 120, 120),
        ],
        onProcess: (processes, pid, signal) => {
          if (signal === "SIGTERM" && pid === 121 && !forked) {
            forked = true;
            processes.set(122, identity(122, 110, 122, 122));
          }
          processes.delete(pid);
        },
      });

      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller: fixture.controller,
        grace: 0,
        rootPid: 100,
      });

      expect(fixture.processes.has(100)).toBe(false);
      expect(fixture.processes.has(122)).toBe(false);
      expect(fixture.signals.indexOf("process:121:SIGTERM")).toBeLessThan(
        fixture.signals.indexOf("process:120:SIGTERM"),
      );
      expect(fixture.signals.indexOf("process:115:SIGTERM")).toBeLessThan(
        fixture.signals.indexOf("process:110:SIGTERM"),
      );
    }),
  );

  it.live("falls back to exact PIDs for unknown group members and a missing leader", () =>
    Effect.gen(function* () {
      const unrelated = identity(999, 1, 110, 110, "unrelated");
      const fixture = makeController({
        processes: [
          server(),
          identity(100, process.pid, 100, 100),
          identity(110, 100, 110, 110),
          identity(111, 110, 110, 110),
          unrelated,
        ],
        onProcess: (processes, pid) => {
          processes.delete(pid);
          if (pid === 111) processes.delete(110);
        },
      });

      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller: fixture.controller,
        grace: 0,
        rootPid: 100,
      });

      expect(fixture.signals).toContain("process:111:SIGTERM");
      expect(fixture.processes.get(999)).toEqual(unrelated);
    }),
  );

  it.live("re-admits a still-owned child after PID reuse and never signals the T3 session", () =>
    Effect.gen(function* () {
      const reused = identity(110, 100, 110, 110, "reused");
      const fixture = makeController({
        processes: [
          server(),
          identity(100, process.pid, 100, 100),
          identity(110, 100, 110, 110, "owned"),
          identity(120, 100, 120, process.pid),
        ],
        onProcess: (processes, pid) => {
          if (pid === 110) processes.set(110, reused);
          else processes.delete(pid);
        },
      });

      const result = yield* Effect.exit(
        AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller: fixture.controller,
          grace: 0,
          rootPid: 100,
        }),
      );

      expect(Exit.isFailure(result)).toBe(true);
      expect(fixture.processes.get(110)).toEqual(reused);
      expect(
        fixture.signals.filter((entry) => entry.startsWith("process:110:")).length,
      ).toBeGreaterThan(0);
      expect(fixture.signals.some((entry) => entry.includes(":120:"))).toBe(false);
    }),
  );

  it.live("does not treat zombie residual entries as teardown survivors", () =>
    Effect.gen(function* () {
      expect(AcpSessionRuntime.posixProcessIsZombie({ ...identity(1, 0, 1, 1), state: "Z" })).toBe(
        true,
      );
      expect(AcpSessionRuntime.posixProcessIsZombie({ ...identity(1, 0, 1, 1), state: "Z+" })).toBe(
        true,
      );
      expect(AcpSessionRuntime.posixProcessIsZombie({ ...identity(1, 0, 1, 1), state: "ZN" })).toBe(
        true,
      );
      expect(AcpSessionRuntime.posixProcessIsZombie({ ...identity(1, 0, 1, 1), state: "Zs" })).toBe(
        true,
      );
      expect(AcpSessionRuntime.posixProcessIsZombie({ ...identity(1, 0, 1, 1), state: "S" })).toBe(
        false,
      );
      expect(AcpSessionRuntime.posixProcessIsZombie({ ...identity(1, 0, 1, 1), state: "R+" })).toBe(
        false,
      );
      const withoutState: AcpSessionRuntime.AcpPosixProcessIdentity = {
        executable: "/proc/1/exe",
        pgid: 1,
        pid: 1,
        ppid: 0,
        sid: 1,
        startTime: "1",
      };
      expect(AcpSessionRuntime.posixProcessIsZombie(withoutState)).toBe(false);

      const root = identity(100, process.pid, 100, 100);
      const zombieChild = identity(110, 100, 110, 110, "110", "Z+");
      const fixture = makeController({
        processes: [server(), root, zombieChild],
        onProcess: (processes, pid) => {
          // Kill leaves a zombie with the same pid/starttime until reaped.
          const current = processes.get(pid);
          if (current === undefined) return;
          processes.set(pid, { ...current, state: pid === 100 ? "Z" : "Z+" });
        },
      });
      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller: fixture.controller,
        grace: 0,
        rootPid: 100,
      });
      expect(fixture.processes.get(100)?.state).toBe("Z");
      expect(fixture.processes.get(110)?.state).toBe("Z+");
    }),
  );

  it("admits a live child after a free PID was previously reserved by a dead entry", () => {
    const staleChild: AcpSessionRuntime.AcpOwnedPosixProcess = {
      ...identity(200, 100, 200, 200, "stale-child"),
      parentExecutable: `/proc/100/exe`,
      parentStartTime: "100",
    };
    const liveChild = identity(200, 100, 200, 200, "live-child");
    const root = identity(100, process.pid, 100, 100);
    const ownedRoot: AcpSessionRuntime.AcpOwnedPosixProcess = {
      ...root,
      parentExecutable: `/proc/${process.pid}/exe`,
      parentStartTime: "server",
    };
    const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>([
      [`100:${root.startTime}`, ownedRoot],
      [`200:${staleChild.startTime}`, staleChild],
    ]);
    const controller = makeController({
      processes: [server(), root],
    }).controller;

    AcpSessionRuntime.capturePosixOwnershipLedger({
      controller,
      ledger,
      rootPid: 100,
      table: [root],
    });
    expect([...ledger.values()].some((entry) => entry.pid === 200)).toBe(false);

    AcpSessionRuntime.capturePosixOwnershipLedger({
      controller: makeController({ processes: [server(), root, liveChild] }).controller,
      ledger,
      rootPid: 100,
      table: [root, liveChild],
    });

    const admitted = [...ledger.values()].find((entry) => entry.pid === 200);
    expect(admitted?.startTime).toBe("live-child");
  });

  it.live("signals children forked under a still-live parent during the same pass", () =>
    Effect.gen(function* () {
      let forked = false;
      const fixture = makeController({
        processes: [
          server(),
          identity(100, process.pid, 100, 100),
          identity(110, 100, 110, 110),
          identity(111, 110, 111, 111),
        ],
        onProcess: (processes, pid) => {
          if (pid === 111 && !forked) {
            forked = true;
            processes.set(112, identity(112, 110, 112, 112));
          }
          processes.delete(pid);
        },
      });

      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller: fixture.controller,
        grace: 0,
        rootPid: 100,
      });

      expect(forked).toBe(true);
      expect(fixture.processes.has(112)).toBe(false);
      expect(fixture.signals.some((entry) => entry.startsWith("process:112:"))).toBe(true);
    }),
  );

  it.live("detects an owned child first observed in the final survivor snapshot", () =>
    Effect.gen(function* () {
      const lateChild = identity(111, 110, 111, 111);
      const fixture = makeController({
        processes: [server(), identity(100, process.pid, 100, 100), identity(110, 100, 110, 110)],
        onProcess: (processes, pid) => {
          if (pid !== 110) processes.delete(pid);
        },
      });
      let snapshotCalls = 0;
      const controller: AcpSessionRuntime.AcpPosixProcessTreeController = {
        ...fixture.controller,
        snapshot: () => {
          snapshotCalls += 1;
          if (snapshotCalls === 6) fixture.processes.set(lateChild.pid, lateChild);
          return fixture.controller.snapshot();
        },
      };

      const error = yield* Effect.flip(
        AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller,
          discoveryPasses: 2,
          grace: 0,
          rootPid: 100,
        }),
      );

      expect(snapshotCalls).toBe(6);
      expect(error.detail).toContain("110, 111");
    }),
  );

  it.live("retains the ledger across root exit and an explicit failure for finalizer retry", () =>
    Effect.gen(function* () {
      let ignoreSignals = true;
      const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
      const frontier = new Map<number, AcpSessionRuntime.AcpOwnedPosixProcess>();
      const childQueues = new Map<number, Array<number>>();
      const root: AcpSessionRuntime.AcpPosixOwnershipRoot = { value: undefined };
      const fixture = makeController({
        processes: [
          server(),
          identity(100, process.pid, 100, 100),
          identity(110, 100, 110, 110),
          identity(111, 110, 110, 110),
        ],
        onProcess: (processes, pid) => {
          if (ignoreSignals && pid === 100) processes.delete(100);
          if (!ignoreSignals) processes.delete(pid);
        },
      });
      AcpSessionRuntime.observePosixOwnershipLedger({
        childQueues,
        controller: fixture.controller,
        frontier,
        ledger,
        root,
        rootPid: 100,
      });

      const explicit = yield* Effect.exit(
        AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller: fixture.controller,
          grace: 0,
          ledger,
          rootPid: 100,
        }),
      );
      expect(Exit.isFailure(explicit)).toBe(true);
      expect(fixture.processes.has(100)).toBe(false);
      expect(fixture.processes.has(111)).toBe(true);

      fixture.processes.set(112, identity(112, 110, 112, 112));
      AcpSessionRuntime.observePosixOwnershipLedger({
        childQueues,
        controller: fixture.controller,
        frontier,
        ledger,
        root,
        rootPid: 100,
      });
      ignoreSignals = false;
      yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
        controller: fixture.controller,
        grace: 0,
        ledger,
        rootPid: 100,
      });
      expect(fixture.processes.has(110)).toBe(false);
      expect(fixture.processes.has(111)).toBe(false);
      expect(fixture.processes.has(112)).toBe(false);
    }),
  );

  it.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
    "snapshots /proc when a process exits while its entry is being typed",
    () => {
      const fs = NodeModule.createRequire(import.meta.url)("node:fs") as typeof NodeFS;
      const realReaddirSync = fs.readdirSync;
      // Node lstat()s a dirent of unknown type; a process that exits meanwhile makes it throw.
      fs.readdirSync = ((path: NodeFS.PathLike, options?: unknown) => {
        if (path === "/proc" && typeof options === "object" && options !== null) {
          throw Object.assign(new Error("ENOENT: no such file or directory, lstat '/proc/4563'"), {
            code: "ENOENT",
            syscall: "lstat",
            path: "/proc/4563",
          });
        }
        return Reflect.apply(realReaddirSync, fs, [path, options]);
      }) as typeof NodeFS.readdirSync;
      NodeModule.syncBuiltinESMExports();
      try {
        const snapshot = AcpSessionRuntime.makePosixProcessTreeController("linux").snapshot();
        expect(snapshot.some((entry) => entry.pid === process.pid)).toBe(true);
      } finally {
        fs.readdirSync = realReaddirSync;
        NodeModule.syncBuiltinESMExports();
      }
    },
  );

  it("parses Darwin ps output including masked or hex sess, BSD states, and malformed rows", () => {
    const stdout = [
      "",
      "  101     1   101        0 R+   Mon Nov  3 10:50:16 2026",
      "  102   101   102 1a2b3c4d Z+   Mon Nov 23 10:50:17 2026",
      "  103   101   103      103 SN   Mon Nov 23 10:50:18 2026\r",
      "  104   103   104 99999999999999999999 ZN Mon Nov 23 10:50:19 2026",
      "    0     0     0        0 S    Mon Nov 23 10:50:16 2026",
      "  -5     1     1        0 S    Mon Nov 23 10:50:16 2026",
      "  abc    1     1        0 S    Mon Nov 23 10:50:16 2026",
      "  105   101   105        0 S    Mon Nov 23 10:50:16",
      "  106   101  bad         0 S    Mon Nov 23 10:50:16 2026",
    ].join("\n");

    expect(AcpSessionRuntime.parseDarwinPsProcessTable(stdout)).toEqual([
      {
        executable: undefined,
        pgid: 101,
        pid: 101,
        ppid: 1,
        sid: 0,
        startTime: "Mon Nov 3 10:50:16 2026",
        state: "R",
      },
      {
        executable: undefined,
        pgid: 102,
        pid: 102,
        ppid: 101,
        sid: 0,
        startTime: "Mon Nov 23 10:50:17 2026",
        state: "Z",
      },
      {
        executable: undefined,
        pgid: 103,
        pid: 103,
        ppid: 101,
        sid: 103,
        startTime: "Mon Nov 23 10:50:18 2026",
        state: "S",
      },
      {
        executable: undefined,
        pgid: 104,
        pid: 104,
        ppid: 103,
        sid: 0,
        startTime: "Mon Nov 23 10:50:19 2026",
        state: "Z",
      },
    ]);
  });

  it.live(
    "terminates detached Darwin descendants when sess is masked to 0 while protecting current pgid",
    () =>
      Effect.gen(function* () {
        const currentPid = process.pid;
        const rootPid = currentPid + 1000;
        const detachedPid = currentPid + 1001;
        const zombiePid = currentPid + 1003;
        const liveRows = new Map<number, string>([
          [currentPid, `${currentPid} 1 ${currentPid} 0 S+ Mon Nov 23 10:50:00 2026`],
          [rootPid, `${rootPid} ${currentPid} ${rootPid} 0 SN Mon Nov 23 10:50:01 2026`],
          [detachedPid, `${detachedPid} ${rootPid} ${detachedPid} 0 R+ Mon Nov 23 10:50:02 2026`],
          [zombiePid, `${zombiePid} ${rootPid} ${zombiePid} 0 Z+ Mon Nov 23 10:50:04 2026`],
        ]);
        const signals: Array<string> = [];
        const controller = AcpSessionRuntime.makeDarwinProcessTreeController({
          readPsTable: () => [...liveRows.values()].join("\n"),
          signalProcess: (pid, signal) => {
            signals.push(`${pid}:${signal}`);
            if (pid !== zombiePid) liveRows.delete(pid);
          },
        });

        yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller,
          grace: 0,
          rootPid,
        });

        expect(signals).toContain(`${detachedPid}:SIGTERM`);
        expect(signals).toContain(`${rootPid}:SIGTERM`);
        expect(signals.some((entry) => entry.startsWith(`${currentPid}:`))).toBe(false);
        expect(liveRows.has(detachedPid)).toBe(false);
        expect(liveRows.has(rootPid)).toBe(false);

        const sameGroupPid = currentPid + 1002;
        const protectedRows = new Map<number, string>([
          [currentPid, `${currentPid} 1 ${currentPid} 0 S+ Mon Nov 23 10:50:00 2026`],
          [rootPid, `${rootPid} ${currentPid} ${rootPid} 0 SN Mon Nov 23 10:50:01 2026`],
          [sameGroupPid, `${sameGroupPid} ${rootPid} ${currentPid} 0 S Mon Nov 23 10:50:03 2026`],
        ]);
        const protectedSignals: Array<string> = [];
        const protectedController = AcpSessionRuntime.makeDarwinProcessTreeController({
          readPsTable: () => [...protectedRows.values()].join("\n"),
          signalProcess: (pid, signal) => {
            protectedSignals.push(`${pid}:${signal}`);
            protectedRows.delete(pid);
          },
        });

        const protectedResult = yield* Effect.exit(
          AcpSessionRuntime.terminatePosixOwnedProcessTree({
            controller: protectedController,
            grace: 0,
            rootPid,
          }),
        );

        expect(Exit.isFailure(protectedResult)).toBe(true);
        expect(protectedSignals).toContain(`${rootPid}:SIGTERM`);
        expect(protectedSignals.some((entry) => entry.startsWith(`${currentPid}:`))).toBe(false);
        expect(protectedSignals.some((entry) => entry.startsWith(`${sameGroupPid}:`))).toBe(false);
        expect(protectedRows.has(sameGroupPid)).toBe(true);
      }),
  );

  it("caches Darwin ps reads within the TTL and refreshes on snapshot or signalProcess", () => {
    let clockMs = 100;
    let reads = 0;
    let tableText = [
      "100 1 100 0 S Mon Nov 23 10:50:00 2026",
      "101 100 101 0 S Mon Nov 23 10:50:01 2026",
    ].join("\n");
    const sent: Array<string> = [];
    const controller = AcpSessionRuntime.makeDarwinProcessTreeController({
      cacheTtlMs: 15,
      now: () => clockMs,
      readPsTable: () => {
        reads += 1;
        return tableText;
      },
      signalProcess: (pid, signal) => {
        sent.push(`${pid}:${signal}`);
        if (pid === 999) {
          throw Object.assign(new Error("ESRCH"), { code: "ESRCH" });
        }
        if (pid === 888) {
          throw Object.assign(new Error("EPERM"), { code: "EPERM" });
        }
      },
    });

    expect(controller.identity(100)?.pid).toBe(100);
    expect(controller.childPidsOf(100)).toEqual([101]);
    expect(controller.childrenOf(100).map((entry) => entry.pid)).toEqual([101]);
    expect(reads).toBe(1);

    clockMs += 10;
    expect(controller.identity(101)?.pid).toBe(101);
    expect(reads).toBe(1);

    clockMs += 5;
    expect(controller.identity(101)?.pid).toBe(101);
    expect(reads).toBe(2);

    tableText = "100 1 100 0 S Mon Nov 23 10:50:00 2026";
    expect(controller.snapshot().map((entry) => entry.pid)).toEqual([100]);
    expect(reads).toBe(3);
    expect(controller.identity(101)).toBeUndefined();
    expect(reads).toBe(3);

    controller.signalProcess(999, "SIGTERM");
    expect(controller.identity(100)?.pid).toBe(100);
    expect(reads).toBe(4);

    expect(() => controller.signalProcess(888, "SIGTERM")).toThrow("EPERM");
    expect(controller.identity(100)?.pid).toBe(100);
    expect(reads).toBe(5);
    expect(sent).toEqual(["999:SIGTERM", "888:SIGTERM"]);
  });

  it("throws on transient Darwin ps errors without wiping the ownership frontier", () => {
    let clockMs = 100;
    let failRead = false;
    let tableText = "100 1 100 0 S Mon Nov 23 10:50:00 2026";
    const controller = AcpSessionRuntime.makeDarwinProcessTreeController({
      cacheTtlMs: 15,
      now: () => clockMs,
      readPsTable: () => {
        if (failRead) throw new Error("ps timed out");
        return tableText;
      },
    });
    const frontier = new Map<number, AcpSessionRuntime.AcpOwnedPosixProcess>();
    const childQueues = new Map<number, Array<number>>();
    const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
    const root: AcpSessionRuntime.AcpPosixOwnershipRoot = { value: undefined };

    AcpSessionRuntime.observePosixOwnershipLedger({
      childQueues,
      controller,
      frontier,
      ledger,
      root,
      rootPid: 100,
    });
    expect(frontier.has(100)).toBe(true);
    expect(ledger.size).toBe(1);

    clockMs += 20;
    failRead = true;
    expect(() => controller.identity(100)).toThrow(
      AcpSessionRuntime.AcpProcessGroupTerminationError,
    );
    expect(() =>
      AcpSessionRuntime.observePosixOwnershipLedger({
        childQueues,
        controller,
        frontier,
        ledger,
        root,
        rootPid: 100,
      }),
    ).toThrow(AcpSessionRuntime.AcpProcessGroupTerminationError);
    expect(frontier.has(100)).toBe(true);

    failRead = false;
    tableText = [
      "100 1 100 0 S Mon Nov 23 10:50:00 2026",
      "101 100 101 0 S Mon Nov 23 10:50:01 2026",
    ].join("\n");
    AcpSessionRuntime.observePosixOwnershipLedger({
      childQueues,
      controller,
      frontier,
      ledger,
      root,
      rootPid: 100,
    });
    expect(frontier.has(100)).toBe(true);
    expect(frontier.has(101)).toBe(true);
    expect([...ledger.values()].map((entry) => entry.pid).toSorted((a, b) => a - b)).toEqual([
      100, 101,
    ]);
  });

  it.effect(
    "populates Darwin process cache via refreshAsync without invoking sync readPsTable",
    () =>
      Effect.gen(function* () {
        let clockMs = 100;
        let syncReads = 0;
        let asyncReads = 0;
        let failAsync = false;
        const tableText = [
          "100 1 100 0 S Mon Nov 23 10:50:00 2026",
          "101 100 101 0 S Mon Nov 23 10:50:01 2026",
        ].join("\n");
        const controller = AcpSessionRuntime.makeDarwinProcessTreeController({
          cacheTtlMs: 15,
          now: () => clockMs,
          readPsTable: () => {
            syncReads += 1;
            return tableText;
          },
          readPsTableAsync: () =>
            Effect.suspend(() => {
              asyncReads += 1;
              return failAsync
                ? Effect.fail(
                    new AcpSessionRuntime.AcpProcessGroupTerminationError({ detail: "ps failed" }),
                  )
                : Effect.succeed(tableText);
            }),
        });

        expect(controller.observeIntervalMs?.(1)).toBeGreaterThanOrEqual(250);
        expect(controller.observeIntervalMs?.(32)).toBeGreaterThanOrEqual(250);
        expect(controller.observeIntervalMs?.(64)).toBeGreaterThanOrEqual(500);
        expect(controller.observeIntervalMs?.(200)).toBeGreaterThanOrEqual(1000);
        expect(controller.refreshAsync).toBeDefined();

        yield* controller.refreshAsync!();
        expect(asyncReads).toBe(1);
        expect(syncReads).toBe(0);

        clockMs += 100;
        expect(controller.identity(100)?.pid).toBe(100);
        expect(controller.childrenOf(100).map((entry) => entry.pid)).toEqual([101]);
        expect(syncReads).toBe(0);

        failAsync = true;
        const failure = yield* Effect.flip(controller.refreshAsync!());
        expect(failure).toBeInstanceOf(AcpSessionRuntime.AcpProcessGroupTerminationError);
        expect(syncReads).toBe(0);
      }),
  );

  it.live("terminates a real detached descendant process via the Darwin ps controller", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) === "win32") return;
      const rootChild = NodeChildProcess.spawn(
        process.execPath,
        [
          "-e",
          [
            'const { spawn } = require("node:child_process");',
            'const grandchild = spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], { detached: true, stdio: "ignore" });',
            "grandchild.unref();",
            "process.stdout.write(`${grandchild.pid}\\n`);",
            "setTimeout(() => {}, 30000);",
          ].join(" "),
        ],
        { detached: true, stdio: ["ignore", "pipe", "ignore"] },
      );
      let grandchildPid: number | undefined;
      const forceKill = (pid: number | undefined) => {
        if (pid === undefined || pid <= 0) return;
        try {
          process.kill(pid, "SIGKILL");
        } catch {}
      };
      yield* Effect.gen(function* () {
        const rootPid = rootChild.pid;
        expect(rootPid).toBeDefined();
        grandchildPid = yield* Effect.promise(
          () =>
            new Promise<number>((resolve, reject) => {
              let buffer = "";
              rootChild.stdout.setEncoding("utf8");
              rootChild.stdout.on("data", (chunk: string) => {
                buffer += chunk;
                const newline = buffer.indexOf("\n");
                if (newline !== -1) {
                  resolve(Number(buffer.slice(0, newline).trim()));
                }
              });
              rootChild.once("error", reject);
              rootChild.once("exit", () => reject(new Error("root exited before printing pid")));
            }),
        );
        expect(Number.isSafeInteger(grandchildPid) && grandchildPid > 0).toBe(true);

        const controller = AcpSessionRuntime.makePosixProcessTreeController("darwin");
        expect(controller.identity(grandchildPid)).toBeDefined();

        yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller,
          grace: 0,
          rootPid: rootPid!,
        });

        const remainingGrandchild = controller.identity(grandchildPid);
        expect(
          remainingGrandchild === undefined ||
            AcpSessionRuntime.posixProcessIsZombie(remainingGrandchild),
        ).toBe(true);
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            forceKill(grandchildPid);
            forceKill(rootChild.pid);
          }),
        ),
      );
    }),
  );

  it("fails closed where a stable POSIX identity provider is unavailable", () => {
    try {
      AcpSessionRuntime.makePosixProcessTreeController("freebsd");
      throw new Error("Expected unsupported POSIX process ownership to fail closed");
    } catch (error) {
      expect(error).toMatchObject({
        detail: "Detached ACP descendant ownership is unsupported on freebsd",
      });
    }
  });

  it.live(
    "uses async snapshots with zero sync ps reads and skips zombies across large Darwin trees (R3-CF-10)",
    () =>
      Effect.gen(function* () {
        const currentPid = process.pid;
        for (const childCount of [20, 50]) {
          for (const mode of ["cooperative-zombie", "ignore-term"] as const) {
            const rootPid = currentPid + 20_000;
            const liveRows = new Map<
              number,
              { pid: number; ppid: number; pgid: number; sid: number; state: string; start: string }
            >([
              [
                currentPid,
                {
                  pid: currentPid,
                  ppid: 1,
                  pgid: currentPid,
                  sid: 0,
                  state: "S+",
                  start: "Mon Nov 23 10:50:00 2026",
                },
              ],
              [
                rootPid,
                {
                  pid: rootPid,
                  ppid: currentPid,
                  pgid: rootPid,
                  sid: 0,
                  state: "SN",
                  start: "Mon Nov 23 10:50:01 2026",
                },
              ],
            ]);
            for (let i = 1; i <= childCount; i += 1) {
              const pid = rootPid + i;
              liveRows.set(pid, {
                pid,
                ppid: rootPid,
                pgid: pid,
                sid: 0,
                state: "S",
                start: "Mon Nov 23 10:50:02 2026",
              });
            }
            const formatTable = () =>
              [...liveRows.values()]
                .map((r) => `${r.pid} ${r.ppid} ${r.pgid} ${r.sid} ${r.state} ${r.start}`)
                .join("\n");
            let syncReads = 0;
            let asyncReads = 0;
            const signals: Array<string> = [];
            const controller = AcpSessionRuntime.makeDarwinProcessTreeController({
              readPsTable: () => {
                syncReads += 1;
                return formatTable();
              },
              readPsTableAsync: () =>
                Effect.sync(() => {
                  asyncReads += 1;
                  return formatTable();
                }),
              signalProcess: (pid, signal) => {
                signals.push(`${pid}:${signal}`);
                const row = liveRows.get(pid);
                if (row === undefined) return;
                if (mode === "cooperative-zombie") {
                  if (pid === rootPid) {
                    liveRows.delete(rootPid);
                    for (const [id, entry] of liveRows.entries()) {
                      if (entry.state.startsWith("Z")) liveRows.delete(id);
                    }
                  } else {
                    liveRows.set(pid, { ...row, state: "Z+" });
                  }
                } else if (signal === "SIGKILL") {
                  liveRows.delete(pid);
                }
              },
            });

            yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
              controller,
              grace: 0,
              rootPid,
            });

            expect(syncReads).toBe(0);
            expect(asyncReads).toBeLessThanOrEqual(10);
            expect(liveRows.has(rootPid)).toBe(false);
            for (let i = 1; i <= childCount; i += 1) {
              expect(liveRows.has(rootPid + i)).toBe(false);
            }
            if (mode === "cooperative-zombie") {
              // Each cooperative child becomes Z+ on pass 1 and must not be re-signaled on passes 2-5.
              for (let i = 1; i <= childCount; i += 1) {
                const pidSignals = signals.filter((entry) => entry.startsWith(`${rootPid + i}:`));
                expect(pidSignals).toEqual([`${rootPid + i}:SIGTERM`]);
              }
            }
          }
        }
      }),
  );

  it.live("wraps final snapshot failures as typed AcpProcessGroupTerminationError (R3-CF-10)", () =>
    Effect.gen(function* () {
      const fixture = makeController({
        processes: [server(), identity(100, process.pid, 100, 100)],
      });
      let snapshotCalls = 0;
      const controller: AcpSessionRuntime.AcpPosixProcessTreeController = {
        ...fixture.controller,
        snapshot: () => {
          snapshotCalls += 1;
          if (snapshotCalls === 6) {
            throw new Error("final snapshot boom");
          }
          return fixture.controller.snapshot();
        },
      };

      const error = yield* Effect.flip(
        AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller,
          discoveryPasses: 2,
          grace: 0,
          rootPid: 100,
        }),
      );

      expect(error).toBeInstanceOf(AcpSessionRuntime.AcpProcessGroupTerminationError);
      expect(error.detail).toContain("Failed to snapshot ACP process tree 100");
    }),
  );

  it.live(
    "admits and terminates same-PGID reparented orphans on Darwin with masked sess=0 while protecting current group/session (R3-CF-12)",
    () =>
      Effect.gen(function* () {
        const currentPid = process.pid;
        const rootPid = currentPid + 30_000;
        const plainOrphanPid = rootPid + 1;
        const trapTermOrphanPid = rootPid + 2;
        const orphanGrandchildPid = rootPid + 3;
        const sameCurrentPgidPid = rootPid + 4;
        const liveRows = new Map<number, string>([
          [1, "1 0 1 0 S Mon Nov 23 10:50:00 2026"],
          [currentPid, `${currentPid} 1 ${currentPid} 0 S+ Mon Nov 23 10:50:00 2026`],
          [rootPid, `${rootPid} ${currentPid} ${rootPid} 0 SN Mon Nov 23 10:50:01 2026`],
          // Reparented to PID 1, still in rootPid's detached PGID, with masked sess=0.
          [plainOrphanPid, `${plainOrphanPid} 1 ${rootPid} 0 S Mon Nov 23 10:50:02 2026`],
          [trapTermOrphanPid, `${trapTermOrphanPid} 1 ${rootPid} 0 S Mon Nov 23 10:50:03 2026`],
          // Detached grandchild spawned by the reparented orphan.
          [
            orphanGrandchildPid,
            `${orphanGrandchildPid} ${trapTermOrphanPid} ${orphanGrandchildPid} 0 S Mon Nov 23 10:50:04 2026`,
          ],
          // Unrelated process sharing current T3 PGID must never be admitted or signaled.
          [
            sameCurrentPgidPid,
            `${sameCurrentPgidPid} 1 ${currentPid} 0 S Mon Nov 23 10:50:05 2026`,
          ],
        ]);
        let syncReads = 0;
        let asyncReads = 0;
        const signals: Array<string> = [];
        const controller = AcpSessionRuntime.makeDarwinProcessTreeController({
          readPsTable: () => {
            syncReads += 1;
            return [...liveRows.values()].join("\n");
          },
          readPsTableAsync: () =>
            Effect.sync(() => {
              asyncReads += 1;
              return [...liveRows.values()].join("\n");
            }),
          signalProcess: (pid, signal) => {
            signals.push(`${pid}:${signal}`);
            if (pid === trapTermOrphanPid && signal === "SIGTERM") return;
            liveRows.delete(pid);
          },
        });

        // Continuous observer with cached Darwin snapshot discovers reparented orphans without sync ps.
        const frontier = new Map<number, AcpSessionRuntime.AcpOwnedPosixProcess>();
        const childQueues = new Map<number, Array<number>>();
        const ledger = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
        const root: AcpSessionRuntime.AcpPosixOwnershipRoot = { value: undefined };
        yield* controller.refreshAsync!();
        AcpSessionRuntime.observePosixOwnershipLedger({
          childQueues,
          controller,
          frontier,
          ledger,
          root,
          rootPid,
        });
        expect([...ledger.values()].map((entry) => entry.pid)).toContain(plainOrphanPid);
        expect([...ledger.values()].map((entry) => entry.pid)).toContain(trapTermOrphanPid);
        expect([...ledger.values()].map((entry) => entry.pid)).toContain(orphanGrandchildPid);
        expect([...ledger.values()].map((entry) => entry.pid)).not.toContain(1);
        expect([...ledger.values()].map((entry) => entry.pid)).not.toContain(currentPid);
        expect([...ledger.values()].map((entry) => entry.pid)).not.toContain(sameCurrentPgidPid);
        expect(syncReads).toBe(0);

        yield* AcpSessionRuntime.terminatePosixOwnedProcessTree({
          controller,
          grace: 0,
          ledger,
          root,
          rootPid,
        });

        expect(syncReads).toBe(0);
        expect(asyncReads).toBeLessThanOrEqual(11);
        expect(signals).toContain(`${plainOrphanPid}:SIGTERM`);
        expect(signals).toContain(`${trapTermOrphanPid}:SIGTERM`);
        expect(signals).toContain(`${trapTermOrphanPid}:SIGKILL`);
        expect(signals).toContain(`${orphanGrandchildPid}:SIGTERM`);
        expect(signals.some((entry) => entry.startsWith("1:"))).toBe(false);
        expect(signals.some((entry) => entry.startsWith(`${currentPid}:`))).toBe(false);
        expect(signals.some((entry) => entry.startsWith(`${sameCurrentPgidPid}:`))).toBe(false);
        expect(liveRows.has(plainOrphanPid)).toBe(false);
        expect(liveRows.has(trapTermOrphanPid)).toBe(false);
        expect(liveRows.has(orphanGrandchildPid)).toBe(false);
        expect(liveRows.has(sameCurrentPgidPid)).toBe(true);
      }),
  );

  it("admits same-PGID orphans after root exit unless root PID was reused and guards current SID (R3-CF-12)", () => {
    const rootIdentity = identity(40_100, process.pid, 40_100, 40_100, "root-orig");
    const orphan = identity(40_101, 1, 40_100, 40_100, "orphan");
    const sameCurrentSid = identity(40_102, 1, 40_100, process.pid, "current-sid");

    // Root exited (not in table), PID not reused -> orphan admitted, current-SID entry rejected.
    const ledgerAfterExit = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
    const rootAfterExit: AcpSessionRuntime.AcpPosixOwnershipRoot = {
      captureAttempted: true,
      value: {
        ...rootIdentity,
        parentExecutable: server().executable,
        parentStartTime: server().startTime,
      },
    };
    const fixtureAfterExit = makeController({
      processes: [server(), orphan, sameCurrentSid],
    });
    AcpSessionRuntime.capturePosixOwnershipLedger({
      controller: fixtureAfterExit.controller,
      ledger: ledgerAfterExit,
      root: rootAfterExit,
      rootPid: rootIdentity.pid,
    });
    expect([...ledgerAfterExit.values()].map((entry) => entry.pid)).toContain(orphan.pid);
    expect([...ledgerAfterExit.values()].map((entry) => entry.pid)).not.toContain(
      sameCurrentSid.pid,
    );

    // Root PID reused with a different startTime -> same-PGID entries must NOT be admitted.
    const reusedRoot = identity(40_100, 1, 40_100, 40_100, "root-reused");
    const unrelatedChild = identity(40_103, 1, 40_100, 40_100, "unrelated-reused-pgid");
    const ledgerReused = new Map<string, AcpSessionRuntime.AcpOwnedPosixProcess>();
    const rootReused: AcpSessionRuntime.AcpPosixOwnershipRoot = {
      captureAttempted: true,
      value: {
        ...rootIdentity,
        parentExecutable: server().executable,
        parentStartTime: server().startTime,
      },
    };
    const fixtureReused = makeController({
      processes: [server(), reusedRoot, unrelatedChild],
    });
    AcpSessionRuntime.capturePosixOwnershipLedger({
      controller: fixtureReused.controller,
      ledger: ledgerReused,
      root: rootReused,
      rootPid: rootIdentity.pid,
    });
    expect([...ledgerReused.values()].map((entry) => entry.pid)).not.toContain(unrelatedChild.pid);
  });
});
