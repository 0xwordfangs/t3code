// @effect-diagnostics nodeBuiltinImport:off -- fake systemd binaries live in a temp dir.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import { AgentScope, AgentScopeTerminationError } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import { makeCodexAppServerSpawnCommand } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ProviderProcesses from "./providerProcesses.ts";
import { classifyScope, make, ownedAgentSlice, type ScopeState } from "./agentScope.ts";

const GiB = 1024 ** 3;
const memory = vi.hoisted(() => ({ total: 16 * 1024 ** 3 }));
vi.mock("node:os", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:os")>()),
  totalmem: () => memory.total,
}));

const running: ScopeState = {
  loadState: "loaded",
  activeState: "active",
  result: "success",
  oomKills: 0,
  populated: true,
};

describe("classifyScope", () => {
  it("reports an OOM kill only once systemd records it", () => {
    expect(classifyScope({ ...running, activeState: "failed", result: "oom-kill" })).toBe(
      "oom-killed",
    );
    expect(classifyScope({ ...running, oomKills: 1 })).toBe("stopping");
    expect(classifyScope({ ...running, populated: false })).toBe("stopping");
    expect(classifyScope({ ...running, activeState: "deactivating" })).toBe("stopping");
  });
});

interface FakeUnit {
  activeState: string;
  result: string;
  controlGroup?: string;
}

const withFakeSystemd = <A, E>(
  body: (input: {
    readonly scope: Effect.Success<typeof make>;
    readonly units: Map<string, FakeUnit>;
    readonly calls: ReadonlyArray<ReadonlyArray<string>>;
    readonly callTimeouts: ReadonlyArray<{
      readonly args: ReadonlyArray<string>;
      readonly timeoutMs: number | undefined;
    }>;
    readonly failingVerbs: Set<string>;
    readonly stateDir: string;
    readonly cgroupRoot: string;
  }) => Effect.Effect<A, E>,
  // A user manager older than systemd 253 rejects OOMPolicy on scopes.
  {
    oomPolicy = true,
    stopFails = false,
    leftovers = false,
    liveOwner = false,
    failCgroupKill = false,
    onWriteCgroupKill,
    onStopOrKill,
    onShow,
  }: {
    readonly oomPolicy?: boolean;
    readonly stopFails?: boolean;
    readonly leftovers?: boolean;
    readonly liveOwner?: boolean;
    readonly failCgroupKill?: boolean;
    readonly onWriteCgroupKill?: (
      path: string,
      data: string,
      units: Map<string, FakeUnit>,
      cgroupRoot: string,
    ) => void;
    readonly onStopOrKill?: (
      verb: string,
      unit: string,
      units: Map<string, FakeUnit>,
      cgroupRoot: string,
    ) => void;
    readonly onShow?: (unit: string, units: Map<string, FakeUnit>, cgroupRoot: string) => void;
  } = {},
) =>
  Effect.gen(function* () {
    const bin = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-agent-scope-"));
    for (const name of ["systemd-run", "systemctl", "agent"]) {
      NodeFS.writeFileSync(NodePath.join(bin, name), "", { mode: 0o755 });
    }
    const cgroupRoot = NodePath.join(bin, "cgroup");
    const units = new Map<string, FakeUnit>();
    const calls: Array<ReadonlyArray<string>> = [];
    const callTimeouts: Array<{
      readonly args: ReadonlyArray<string>;
      readonly timeoutMs: number | undefined;
    }> = [];
    const failingVerbs = new Set<string>();
    const output = (stdout: string, code = 0): ProcessRunner.ProcessRunOutput => ({
      stdout,
      stderr: "",
      code: ChildProcessSpawner.ExitCode(code),
      timedOut: false,
      stdoutTruncated: false,
      stderrTruncated: false,
      stdoutInvalidUtf8: false,
      stderrInvalidUtf8: false,
    });
    const runner = ProcessRunner.ProcessRunner.of({
      run: ({ args, timeout }) =>
        Effect.sync(() => {
          calls.push(args);
          callTimeouts.push({
            args,
            timeoutMs:
              timeout === undefined
                ? undefined
                : Duration.toMillis(Duration.fromInputUnsafe(timeout)),
          });
          if (!oomPolicy && args.some((arg) => arg.startsWith("--property=OOMPolicy="))) {
            return output("", 1);
          }
          const [, verb, unit = ""] = args;
          if (failingVerbs.has(verb!)) return output("", 1);
          if (stopFails && verb === "stop") {
            return {
              ...output(""),
              stderr: `Failed to stop ${args.at(-1)}: Unit not loaded.`,
              code: ChildProcessSpawner.ExitCode(5),
            };
          }
          if (verb === "reset-failed") units.delete(unit);
          if (verb === "kill" || verb === "stop") {
            onStopOrKill?.(verb, args.at(-1) ?? "", units, cgroupRoot);
          }
          if (verb !== "show") return output("");
          onShow?.(unit, units, cgroupRoot);
          const state = units.get(unit);
          return output(
            state === undefined
              ? "LoadState=not-found\nActiveState=inactive\nResult=success\n"
              : `LoadState=loaded\nActiveState=${state.activeState}\nResult=${state.result}\n` +
                  `ControlGroup=${state.controlGroup ?? ""}\n`,
          );
        }),
    });
    const fileSystem = yield* FileSystem.FileSystem;
    const { stateDir } = yield* ServerConfig.ServerConfig;
    if (leftovers) {
      units.set(ownedAgentSlice(stateDir), { activeState: "active", result: "success" });
    }
    const scope = yield* make.pipe(
      Effect.provideService(ProviderProcesses.ProviderProcesses, { hasOtherLiveOwner: liveOwner }),
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.provideService(HostProcess.Platform, "linux"),
      Effect.provideService(HostProcess.Environment, { PATH: bin, XDG_RUNTIME_DIR: "/run/user/1" }),
      Effect.provideService(FileSystem.FileSystem, {
        ...fileSystem,
        readFileString: (path, encoding) =>
          fileSystem.readFileString(path.replace("/sys/fs/cgroup", cgroupRoot), encoding),
        writeFileString: (path, data, options) => {
          if (failCgroupKill && path.endsWith("/cgroup.kill")) {
            return fileSystem.writeFileString(
              NodePath.join(cgroupRoot, "nonexistent-dir", "cgroup.kill"),
              data,
              options,
            );
          }
          return fileSystem
            .writeFileString(path.replace("/sys/fs/cgroup", cgroupRoot), data, options)
            .pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  if (path.endsWith("/cgroup.kill")) {
                    onWriteCgroupKill?.(path, data, units, cgroupRoot);
                  }
                }),
              ),
            );
        },
      }),
    );
    return yield* body({
      scope,
      units,
      calls,
      callTimeouts,
      failingVerbs,
      stateDir,
      cgroupRoot,
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(bin, { recursive: true, force: true }))),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-agent-scope-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  );

const unitOf = (launch: Pick<ChildProcess.StandardCommand, "args">) =>
  launch.args
    .map(String)
    .find((arg) => arg.startsWith("--unit="))
    ?.slice("--unit=".length) ?? "";

const sliceOf = (launch: { readonly args: ReadonlyArray<string> }) =>
  launch.args.find((arg) => arg.startsWith("--slice="))?.slice("--slice=".length) ?? "";

/** An active scope as systemd and the kernel report it, with the agent itself gone. */
const exitedAgent = (
  units: Map<string, FakeUnit>,
  cgroupRoot: string,
  unit: string,
  { populated, oomKills }: { readonly populated: boolean; readonly oomKills: number | undefined },
) => {
  const controlGroup = `/${unit}`;
  NodeFS.mkdirSync(NodePath.join(cgroupRoot, controlGroup), { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(cgroupRoot, controlGroup, "cgroup.events"),
    `populated ${populated ? 1 : 0}\nfrozen 0\n`,
  );
  // An undefined count leaves memory.events unreadable.
  if (oomKills !== undefined) {
    NodeFS.writeFileSync(
      NodePath.join(cgroupRoot, controlGroup, "memory.events"),
      `low 0\nhigh 0\nmax 0\noom ${oomKills}\noom_kill ${oomKills}\noom_group_kill 0\n`,
    );
  }
  units.set(unit, { activeState: "active", result: "success", controlGroup });
};

const stopsOf = (calls: ReadonlyArray<ReadonlyArray<string>>) =>
  calls.filter(([, verb]) => verb === "stop");
const launchCodex = (scope: Effect.Success<typeof make>, threadId: string) =>
  makeCodexAppServerSpawnCommand({ command: "agent", args: ["app-server"], threadId }).pipe(
    Effect.provideService(AgentScope, scope),
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.map((command) => {
      assert(ChildProcess.isStandardCommand(command));
      return command;
    }),
  );

describe("AgentScope service", () => {
  it.effect("labels a thread from its newest scope until its next session clears it", () =>
    withFakeSystemd(({ scope, units }) =>
      Effect.gen(function* () {
        const first = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        units.set(unitOf(first), { activeState: "failed", result: "oom-kill" });
        const second = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        units.set(unitOf(second), { activeState: "active", result: "success" });
        expect(yield* scope.oomKilled("a")).toBe(false);

        units.set(unitOf(second), { activeState: "failed", result: "oom-kill" });
        expect(yield* scope.oomKilled("a")).toBe(true);
        units.delete(unitOf(second));
        expect(yield* scope.oomKilled("a")).toBe(true);

        yield* scope.clear("a");
        expect(yield* scope.oomKilled("a")).toBe(false);
      }),
    ),
  );

  it.effect(
    "keeps shared Codex scopes running without attributing their OOM to the launch thread",
    () =>
      withFakeSystemd(({ scope, units, calls }) =>
        Effect.gen(function* () {
          const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          expect(agent.args).toContain("--property=OOMPolicy=stop");
          units.set(unitOf(agent), { activeState: "failed", result: "oom-kill" });
          expect(yield* scope.oomKilled("a")).toBe(true);

          const codex = yield* launchCodex(scope, "a");
          expect(codex.args).toContain("--property=OOMPolicy=continue");
          expect(codex.args).not.toContain("--property=OOMPolicy=stop");
          expect(codex.args).toContain("--property=TimeoutStopSec=10s");
          expect(codex.args).toContain("--collect");
          units.set(unitOf(codex), { activeState: "active", result: "success" });
          expect(yield* scope.oomKilled("a")).toBe(false);
          units.set(unitOf(codex), { activeState: "failed", result: "oom-kill" });
          expect(yield* scope.oomKilled("a")).toBe(false);
          expect(
            calls.filter(([, verb, unit]) => verb === "show" && unit === unitOf(codex)),
          ).toEqual([]);
        }),
      ),
  );

  it.effect.each([{ oomPolicy: true }, { oomPolicy: false }])(
    "Force stop kills a thread's own scope but never a shared Codex scope (OOM policy $oomPolicy)",
    ({ oomPolicy }) =>
      withFakeSystemd(
        ({ scope, units, calls, cgroupRoot }) =>
          Effect.gen(function* () {
            const agent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "t",
              threadId: "a",
            });
            exitedAgent(units, cgroupRoot, unitOf(agent), { populated: true, oomKills: 0 });
            const codex = yield* launchCodex(scope, "b");
            exitedAgent(units, cgroupRoot, unitOf(codex), { populated: true, oomKills: 0 });

            expect(yield* scope.kill("a")).toBe(true);
            expect(
              NodeFS.readFileSync(NodePath.join(cgroupRoot, unitOf(agent), "cgroup.kill"), "utf8"),
            ).toBe("1");
            expect(yield* scope.kill("b")).toBe(false);
            expect(NodeFS.existsSync(NodePath.join(cgroupRoot, unitOf(codex), "cgroup.kill"))).toBe(
              false,
            );
            expect(calls.filter(([, verb]) => verb === "kill")).toEqual([]);
          }),
        { oomPolicy },
      ),
  );

  it.effect("keeps a running scope while hundreds of other threads launch", () =>
    withFakeSystemd(({ scope, units }) =>
      Effect.gen(function* () {
        const live = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "live" });
        units.set(unitOf(live), { activeState: "active", result: "success" });
        for (let index = 0; index < 600; index++) {
          yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: `other-${index}` });
        }
        units.set(unitOf(live), { activeState: "failed", result: "oom-kill" });
        expect(yield* scope.oomKilled("live")).toBe(true);
      }),
    ),
  );

  it.effect("delegates every agent scope so rootless containers can manage their cgroups", () =>
    withFakeSystemd(({ scope }) =>
      Effect.gen(function* () {
        const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const codex = yield* launchCodex(scope, "a");
        const terminal = yield* scope.wrap({
          command: "agent",
          args: [],
          name: "terminal",
          survivesRestart: true,
        });
        for (const launch of [agent, codex, terminal]) {
          expect(launch.args).toContain("--property=Delegate=yes");
        }
      }),
    ),
  );

  it.effect("caps agent memory and yields CPU, disk, and tasks to the server", () =>
    withFakeSystemd(({ scope, calls }) =>
      Effect.gen(function* () {
        yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const setProperties = calls.filter(([, verb]) => verb === "set-property");
        expect(setProperties).toEqual([
          [
            "--user",
            "set-property",
            "--runtime",
            "app-t3code-agents.slice",
            expect.stringMatching(/^MemoryHigh=\d+$/),
            expect.stringMatching(/^MemoryMax=\d+$/),
          ],
          [
            "--user",
            "set-property",
            "--runtime",
            "app-t3code.slice",
            "CPUWeight=10",
            "IOWeight=10",
            "TasksMax=25%",
          ],
        ]);
      }),
    ),
  );

  it.effect("follows a live memory change at the next launch", () => {
    memory.total = 16 * GiB;
    return withFakeSystemd(({ scope, calls, failingVerbs }) =>
      Effect.gen(function* () {
        const launch = scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const memoryLimits = () =>
          calls
            .filter(
              ([, verb, , slice]) => verb === "set-property" && slice === "app-t3code-agents.slice",
            )
            .map((args) => args.slice(4));
        yield* launch;
        yield* launch;
        expect(memoryLimits()).toEqual([[`MemoryHigh=${10 * GiB}`, `MemoryMax=${12 * GiB}`]]);

        // A VM balloon or `pct set --memory` grows RAM while the server keeps running.
        memory.total = 22 * GiB;
        failingVerbs.add("set-property");
        yield* launch;
        yield* launch;
        failingVerbs.delete("set-property");
        yield* launch;
        yield* launch;
        const grown = [`MemoryHigh=${16 * GiB}`, `MemoryMax=${18 * GiB}`];
        expect(memoryLimits()).toEqual([
          [`MemoryHigh=${10 * GiB}`, `MemoryMax=${12 * GiB}`],
          grown,
          grown,
          grown,
        ]);
      }),
    );
  });

  it.effect("stops agents a killed server left before startup completes", () =>
    withFakeSystemd(
      ({ scope, calls, stateDir }) =>
        Effect.gen(function* () {
          expect(stopsOf(calls)).toEqual([["--user", "stop", ownedAgentSlice(stateDir)]]);
          const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          expect(sliceOf(agent)).toBe(ownedAgentSlice(stateDir));
          expect(agent.args).toContain("--property=TimeoutStopSec=10s");
          const terminal = yield* scope.wrap({
            command: "agent",
            args: [],
            name: "terminal",
            survivesRestart: true,
          });
          expect(sliceOf(terminal)).toBe("app-t3code-agents.slice");
          expect(stopsOf(calls)).toHaveLength(1);
        }),
      { leftovers: true },
    ),
  );

  it.effect("leaves the slice to another live server sharing this home", () =>
    withFakeSystemd(({ calls }) => Effect.sync(() => expect(stopsOf(calls)).toEqual([])), {
      leftovers: true,
      liveOwner: true,
    }),
  );

  it.effect("stops leftover processes only in an active agent scope", () =>
    withFakeSystemd(({ scope, units, calls, cgroupRoot }) =>
      Effect.gen(function* () {
        const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const unit = unitOf(agent);
        expect(agent.unit).toBe(unit);

        yield* scope.stop("session-2.scope");
        expect(calls.some((args) => args.includes("session-2.scope"))).toBe(false);
        yield* scope.stop(unit);
        exitedAgent(units, cgroupRoot, unit, { populated: false, oomKills: 0 });
        yield* scope.stop(unit);
        exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 0 });
        units.get(unit)!.activeState = "deactivating";
        yield* scope.stop(unit);
        expect(stopsOf(calls)).toEqual([]);

        units.get(unit)!.activeState = "active";
        yield* scope.stop(unit);
        expect(stopsOf(calls)).toEqual([["--user", "stop", "--no-block", unit]]);
        expect(yield* scope.oomKilled("a")).toBe(false);
      }),
    ),
  );

  it.effect.each([true, false])(
    "cleans up an OOM exit without losing its diagnosis (OOMPolicy=%s)",
    (oomPolicy) =>
      withFakeSystemd(
        ({ scope, units, calls, cgroupRoot }) =>
          Effect.gen(function* () {
            const agent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "t",
              threadId: "a",
            });
            const unit = unitOf(agent);
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: undefined });
            yield* scope.stop(unit);
            expect(stopsOf(calls)).toEqual(
              oomPolicy ? [] : [["--user", "stop", "--no-block", unit]],
            );

            // The kernel records the kill before systemd records the OOM result.
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 1 });
            yield* scope.stop(unit);
            expect(stopsOf(calls)).toHaveLength(oomPolicy ? 0 : 2);

            if (oomPolicy) {
              units.set(unit, { activeState: "failed", result: "oom-kill" });
              expect(yield* scope.oomKilled("a")).toBe(true);
              units.delete(unit);
              expect(yield* scope.oomKilled("a")).toBe(true);
            }
          }),
        { oomPolicy },
      ),
  );

  it.effect("reports failed daemon cleanup without failing session teardown", () => {
    const warnings: Array<unknown> = [];
    const logger = Logger.make(({ message }) => {
      warnings.push(message);
    });
    return withFakeSystemd(
      ({ scope, units, cgroupRoot }) =>
        Effect.gen(function* () {
          const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          exitedAgent(units, cgroupRoot, unitOf(agent), { populated: true, oomKills: 0 });
          yield* scope.stop(unitOf(agent));
          expect(warnings.flat()).toContain("Could not stop an exited agent's scope");
        }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false }))),
      { stopFails: true },
    );
  });

  it.effect("leaves a quiet slice alone", () =>
    withFakeSystemd(({ scope, calls }) =>
      Effect.gen(function* () {
        yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        expect(calls.filter(([, verb]) => verb === "stop")).toEqual([]);
      }),
    ),
  );

  it.effect(
    "terminateUnit kills via cgroup.kill regardless of OOM-diagnosis state and ignores non-t3code units",
    () =>
      withFakeSystemd(
        ({ scope, units, calls, cgroupRoot }) =>
          Effect.gen(function* () {
            yield* scope.terminateUnit("session-2.scope");
            expect(calls.some((args) => args.includes("session-2.scope"))).toBe(false);

            const agent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "acp",
              threadId: "a",
            });
            const unit = unitOf(agent);

            // Already unloaded or empty scope returns cleanly without writing cgroup.kill.
            yield* scope.terminateUnit(unit);
            exitedAgent(units, cgroupRoot, unit, { populated: false, oomKills: 0 });
            yield* scope.terminateUnit(unit);
            expect(NodeFS.existsSync(NodePath.join(cgroupRoot, unit, "cgroup.kill"))).toBe(false);

            // Bypass OOM-policy skip when memory.events is absent (oomKills: undefined).
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: undefined });
            yield* scope.terminateUnit(unit);
            expect(
              NodeFS.readFileSync(NodePath.join(cgroupRoot, unit, "cgroup.kill"), "utf8"),
            ).toBe("1\n");

            // Bypass OOM-policy skip when oom_kill > 0.
            NodeFS.rmSync(NodePath.join(cgroupRoot, unit, "cgroup.kill"));
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 1 });
            yield* scope.terminateUnit(unit);
            expect(
              NodeFS.readFileSync(NodePath.join(cgroupRoot, unit, "cgroup.kill"), "utf8"),
            ).toBe("1\n");
            expect(calls.filter(([, verb]) => verb === "kill" || verb === "stop")).toEqual([]);
          }),
        {
          oomPolicy: true,
          onWriteCgroupKill: (writePath, _data, _units, cgroupRoot) => {
            const eventsPath = NodePath.join(
              NodePath.dirname(writePath.replace("/sys/fs/cgroup", cgroupRoot)),
              "cgroup.events",
            );
            NodeFS.writeFileSync(eventsPath, "populated 0\nfrozen 0\n");
          },
        },
      ),
  );

  it.effect(
    "terminateUnit falls back to blocking systemctl kill and stop when cgroup.kill is unavailable",
    () =>
      withFakeSystemd(
        ({ scope, units, calls, cgroupRoot }) =>
          Effect.gen(function* () {
            const agent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "acp",
              threadId: "a",
            });
            const unit = unitOf(agent);
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 1 });

            yield* scope.terminateUnit(unit);

            expect(calls.filter(([, verb]) => verb === "kill")).toEqual([
              ["--user", "kill", "--signal=SIGKILL", unit],
            ]);
            expect(calls.filter(([, verb]) => verb === "stop")).toEqual([["--user", "stop", unit]]);
          }),
        {
          oomPolicy: true,
          failCgroupKill: true,
          onStopOrKill: (verb, unit, units) => {
            if (verb === "stop") {
              const entry = units.get(unit);
              if (entry !== undefined) entry.activeState = "inactive";
            }
          },
        },
      ),
  );

  it.effect("terminateUnit fails closed when systemctl show fails or scope remains populated", () =>
    withFakeSystemd(({ scope, units, failingVerbs, cgroupRoot }) =>
      Effect.gen(function* () {
        const agent = yield* scope.wrap({
          command: "agent",
          args: [],
          name: "acp",
          threadId: "a",
        });
        const unit = unitOf(agent);
        exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 0 });

        failingVerbs.add("show");
        const showError = yield* Effect.flip(scope.terminateUnit(unit));
        expect(showError).toBeInstanceOf(Error);
        expect(showError).toBeInstanceOf(AgentScopeTerminationError);
        expect(showError.message).toContain(`Failed to inspect ACP scope ${unit}`);

        failingVerbs.delete("show");
        // cgroup.kill writes to disk but nothing clears populated 1 -> must fail closed.
        const populatedError = yield* Effect.flip(scope.terminateUnit(unit));
        expect(populatedError).toBeInstanceOf(Error);
        expect(populatedError).toBeInstanceOf(AgentScopeTerminationError);
        expect(populatedError.message).toContain(
          `ACP scope ${unit} remained populated after forced termination`,
        );
      }),
    ),
  );

  it.effect(
    "terminateUnit bounds per-step systemctl timeouts within the overall wall-clock budget and preserves OOM diagnosis on unplanned exit",
    () => {
      let oomShowCount = 0;
      return withFakeSystemd(
        ({ scope, units, calls, callTimeouts, cgroupRoot }) =>
          Effect.gen(function* () {
            const oomAgent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "acp",
              threadId: "oom-thread",
            });
            const oomUnit = unitOf(oomAgent);
            exitedAgent(units, cgroupRoot, oomUnit, { populated: true, oomKills: 1 });

            // Unplanned exit with oom_kill > 0 waits for systemd to record Result=oom-kill
            // without issuing cgroup.kill or manual systemctl stop.
            yield* scope.terminateUnit(oomUnit, { preserveOomDiagnosis: true });
            expect(NodeFS.existsSync(NodePath.join(cgroupRoot, oomUnit, "cgroup.kill"))).toBe(
              false,
            );
            expect(calls.filter((args) => args.includes(oomUnit) && args[1] === "stop")).toEqual(
              [],
            );
            expect(yield* scope.oomKilled("oom-thread")).toBe(true);

            // Unplanned self-exit with oom_kill === 0 still terminates lingering orphans via fallback
            // with bounded per-step timeouts (<= 2000 ms, never 15 s).
            const selfExitAgent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "acp",
              threadId: "self-exit-thread",
            });
            const selfExitUnit = unitOf(selfExitAgent);
            exitedAgent(units, cgroupRoot, selfExitUnit, { populated: true, oomKills: 0 });
            yield* scope.terminateUnit(selfExitUnit, { preserveOomDiagnosis: true });

            const selfExitTimeouts = callTimeouts.filter((entry) =>
              entry.args.includes(selfExitUnit),
            );
            expect(selfExitTimeouts.length).toBeGreaterThan(0);
            for (const entry of selfExitTimeouts) {
              expect(entry.timeoutMs).toBeDefined();
              expect(entry.timeoutMs!).toBeLessThanOrEqual(2_000);
            }
          }),
        {
          oomPolicy: true,
          failCgroupKill: true,
          onShow: (unit, units) => {
            const entry = units.get(unit);
            if (entry === undefined || entry.result === "oom-kill") return;
            if (unit.startsWith("t3code-acp-") && oomShowCount < 2) {
              oomShowCount += 1;
              if (oomShowCount === 2) {
                entry.result = "oom-kill";
                entry.activeState = "failed";
              }
            }
          },
          onStopOrKill: (verb, unit, units) => {
            if (verb === "stop") {
              const entry = units.get(unit);
              if (entry !== undefined) entry.activeState = "inactive";
            }
          },
        },
      );
    },
  );
});
