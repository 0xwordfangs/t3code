import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { AgentScope, agentScopeCommand } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";

import {
  makePosixProcessTreeController,
  posixProcessIsZombie,
  type AcpLinuxCgroupController,
} from "./AcpSessionRuntime.ts";
import {
  makeAcpClientTerminals,
  resolveEmbeddedTerminalContent,
  type AcpClientTerminals,
} from "./clientTerminals.ts";

const withTerminals = <A, E>(use: (terminals: AcpClientTerminals) => Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const terminals = yield* makeAcpClientTerminals({
      defaultCwd: process.cwd(),
    });
    return yield* use(terminals).pipe(Effect.ensuring(terminals.disposeAll));
  }).pipe(Effect.provide(NodeServices.layer));

describe("AcpClientTerminals", () => {
  it.live("runs a command, buffers output, and reports the exit status", () =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const created = yield* terminals.create({
          sessionId: "session",
          command: process.execPath,
          args: ["-e", "console.log('hello from acp'); process.exit(3);"],
        });

        const exit = yield* terminals.waitForExit({
          sessionId: "session",
          terminalId: created.terminalId,
        });
        expect(exit.exitCode).toBe(3);

        const output = yield* terminals.output({
          sessionId: "session",
          terminalId: created.terminalId,
        });
        expect(output.output).toContain("hello from acp");
        expect(output.truncated).toBe(false);
        expect(output.exitStatus?.exitCode).toBe(3);
      }),
    ),
  );

  it.effect("preserves UTF-8 when stderr interleaves a split stdout character", () =>
    Effect.gen(function* () {
      const stdoutStarted = yield* Deferred.make<void>();
      const stderrWritten = yield* Deferred.make<void>();
      const child = ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        stdin: Sink.drain,
        stdout: Stream.make(new Uint8Array([0xe2])).pipe(
          Stream.concat(Stream.fromEffectDrain(Deferred.succeed(stdoutStarted, undefined))),
          Stream.concat(Stream.fromEffectDrain(Deferred.await(stderrWritten))),
          Stream.concat(Stream.make(new Uint8Array([0x82, 0xac]))),
        ),
        stderr: Stream.fromEffectDrain(Deferred.await(stdoutStarted)).pipe(
          Stream.concat(Stream.make(new Uint8Array([0x58]))),
          Stream.concat(Stream.fromEffectDrain(Deferred.succeed(stderrWritten, undefined))),
        ),
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      });
      const terminals = yield* makeAcpClientTerminals({ defaultCwd: process.cwd() }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.succeed(child)),
        ),
      );
      yield* Effect.addFinalizer(() => terminals.disposeAll);
      const terminal = yield* terminals.create({ sessionId: "session", command: "fixture" });
      const request = { sessionId: "session", ...terminal };
      yield* terminals.waitForExit(request);

      const output = yield* terminals.output(request);
      expect(output.output).toContain("€");
      expect(output.output).toContain("X");
      expect(output.output).not.toContain("\uFFFD");
      expect(output.truncated).toBe(false);
      expect(terminals.readOutputSnapshot("session", terminal.terminalId)?.output).toBe(
        output.output,
      );
    }),
  );

  it.live("resolves terminal environment from the requesting ACP session", () =>
    Effect.gen(function* () {
      const inheritedEnvironment = yield* HostProcess.Environment;
      const terminals = yield* makeAcpClientTerminals({
        defaultCwd: process.cwd(),
        environmentForSession: (sessionId) =>
          sessionId === "unknown"
            ? undefined
            : {
                T3_ACP_MCP_AUTHORIZATION: sessionId === "session-a" ? "token-a" : "token-b",
                T3CODE_THREAD_ID: sessionId,
              },
      });
      yield* Effect.addFinalizer(() => terminals.disposeAll);
      inheritedEnvironment.T3_TEST_INHERITED = "refreshed";

      for (const [sessionId, expected] of [
        ["session-a", "token-a||session-a|refreshed|T3_TEST_OVERRIDE=request"],
        ["session-b", "token-b||session-b|refreshed|T3_TEST_OVERRIDE=request"],
        ["unknown", "|||refreshed|T3_TEST_OVERRIDE=inherited"],
      ] as const) {
        const created = yield* terminals.create({
          sessionId,
          command: process.execPath,
          args: [
            "-e",
            "const env = process.env; process.stdout.write([env.T3_ACP_MCP_AUTHORIZATION ?? '', env.t3_Acp_Mcp_Endpoint ?? '', env.T3CODE_THREAD_ID ?? '', env.T3_TEST_INHERITED, ...Object.keys(env).filter((name) => name.toUpperCase() === 'T3_TEST_OVERRIDE').map((name) => name + '=' + env[name])].join('|'))",
          ],
          ...(sessionId === "unknown"
            ? {}
            : {
                env: [
                  { name: "T3_TEST_OVERRIDE", value: "first" },
                  { name: "t3_Test_Override", value: "second" },
                  { name: "T3_TEST_OVERRIDE", value: "request" },
                ],
              }),
        });
        yield* terminals.waitForExit({ sessionId, terminalId: created.terminalId });
        const output = yield* terminals.output({ sessionId, terminalId: created.terminalId });
        expect(output.output).toBe(expected);
      }
    }).pipe(
      Effect.provideService(HostProcess.Environment, {
        ...process.env,
        T3_ACP_MCP_AUTHORIZATION: "launching-thread-token",
        t3_Acp_Mcp_Endpoint: "launching-thread-endpoint",
        T3CODE_THREAD_ID: "launching-thread",
        T3_TEST_INHERITED: "kept",
        T3_TEST_OVERRIDE: "inherited",
      }),
      // Exercise Windows' case-insensitive variable names on every OS.
      Effect.provideService(HostProcess.Platform, "win32"),
      Effect.provide(NodeServices.layer),
    ),
  );

  it.live("runs Devin shell source with arguments, pipes, and session fallback variables", () =>
    Effect.gen(function* () {
      const terminals = yield* makeAcpClientTerminals({
        defaultCwd: process.cwd(),
        shellCommands: true,
        environmentForSession: () => ({ T3_ACP_MCP_NODE: "mailbox-probe" }),
      });
      yield* Effect.addFinalizer(() => terminals.disposeAll);
      const terminal = yield* terminals.create({
        sessionId: "devin",
        command: 'printf "%s\\n" "$T3_ACP_MCP_NODE" | tr a-z A-Z',
      });
      const exit = yield* terminals.waitForExit({ sessionId: "devin", ...terminal });
      const output = yield* terminals.output({ sessionId: "devin", ...terminal });
      expect(exit.exitCode).toBe(0);
      expect(output.output.trim()).toBe("MAILBOX-PROBE");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live.each([
    { text: "a".repeat(64) + "TAIL", limit: 16 },
    { text: "€".repeat(16) + "TAIL", limit: 6 },
  ])("truncates buffered output from the beginning at the $limit byte limit", ({ text, limit }) =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const created = yield* terminals.create({
          sessionId: "session",
          command: process.execPath,
          args: ["-e", `process.stdout.write(${JSON.stringify(text)});`],
          outputByteLimit: limit,
        });
        yield* terminals.waitForExit({ sessionId: "session", terminalId: created.terminalId });

        const output = yield* terminals.output({
          sessionId: "session",
          terminalId: created.terminalId,
        });
        expect(output.truncated).toBe(true);
        expect(output.output.endsWith("TAIL")).toBe(true);
        expect(Buffer.byteLength(output.output)).toBeLessThanOrEqual(limit);
        expect(output.output).not.toContain("\uFFFD");
      }),
    ),
  );

  it.live("kills long-running commands and rejects released terminal handles", () =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const created = yield* terminals.create({
          sessionId: "session",
          command: process.execPath,
          args: ["-e", "setInterval(() => {}, 1000);"],
        });
        yield* terminals.kill({ sessionId: "session", terminalId: created.terminalId });
        const exit = yield* terminals.waitForExit({
          sessionId: "session",
          terminalId: created.terminalId,
        });
        expect(exit.exitCode === 0 ? null : exit.exitCode).not.toBe(0);
        // Windows has no signals: a killed process only reports an exit code.
        if (yield* HostProcess.isWindows) {
          expect(exit.signal).toBeNull();
        } else {
          expect(exit.signal).toBe("SIGTERM");
        }

        yield* terminals.release({ sessionId: "session", terminalId: created.terminalId });
        const rejected = yield* Effect.flip(
          terminals.output({ sessionId: "session", terminalId: created.terminalId }),
        );
        expect(rejected.message).toContain("unknown terminal ID");

        expect(terminals.readOutputSnapshot("session", created.terminalId)).toBeDefined();
      }),
    ),
  );

  it.effect("answers wait_for_exit when a background child keeps the output pipes open", () =>
    Effect.gen(function* () {
      // Models `sh -c "setsid server & echo done"`: sh exits, the orphan inherits stdout/stderr.
      const orphanExited = yield* Deferred.make<void>();
      const heldOpen = Stream.fromEffectDrain(Deferred.await(orphanExited));
      const child = ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        stdin: Sink.drain,
        stdout: Stream.make(new TextEncoder().encode("done\n")).pipe(Stream.concat(heldOpen)),
        stderr: heldOpen,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      });
      const terminals = yield* makeAcpClientTerminals({ defaultCwd: process.cwd() }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.succeed(child)),
        ),
      );
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(orphanExited, undefined).pipe(Effect.andThen(terminals.disposeAll)),
      );
      const terminal = yield* terminals.create({ sessionId: "session", command: "fixture" });
      const request = { sessionId: "session", ...terminal };
      const waiting = yield* terminals.waitForExit(request).pipe(Effect.forkChild);

      yield* TestClock.adjust("5 seconds");
      expect(waiting.pollUnsafe()).toBeDefined();
      expect(yield* Fiber.join(waiting)).toEqual({ exitCode: 0, signal: null });
      const output = yield* terminals.output(request);
      expect(output.output).toBe("done\n");
      expect(output.exitStatus).toEqual({ exitCode: 0, signal: null });
    }),
  );

  it.effect.each([
    { deadline: "default", forceKillAfter: undefined, deadlineMillis: 5000 },
    { deadline: "configured", forceKillAfter: "250 millis" as const, deadlineMillis: 250 },
  ])(
    "waits for the $deadline deadline before force-killing a SIGTERM-ignoring terminal",
    ({ forceKillAfter, deadlineMillis }) =>
      Effect.gen(function* () {
        const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        const child = ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Deferred.await(exited),
          isRunning: Deferred.isDone(exited).pipe(Effect.map((done) => !done)),
          // Model a process that only exits when the spawner escalates after its deadline.
          kill: (options) =>
            options?.forceKillAfter === undefined
              ? Deferred.await(exited).pipe(Effect.asVoid)
              : Effect.sleep(options.forceKillAfter).pipe(
                  Effect.andThen(Deferred.succeed(exited, ChildProcessSpawner.ExitCode(137))),
                  Effect.asVoid,
                ),
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
        const terminals = yield* makeAcpClientTerminals({
          defaultCwd: process.cwd(),
          ...(forceKillAfter === undefined ? {} : { forceKillAfter }),
        }).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() => Effect.succeed(child)),
          ),
        );
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(exited, ChildProcessSpawner.ExitCode(137)).pipe(
            Effect.andThen(terminals.disposeAll),
          ),
        );
        const terminal = yield* terminals.create({
          sessionId: "session",
          command: "ignores-SIGTERM",
        });
        const request = { sessionId: "session", ...terminal };
        const killing = yield* terminals.kill(request).pipe(Effect.forkChild);
        const waiting = yield* terminals.waitForExit(request).pipe(Effect.forkChild);

        yield* TestClock.adjust(deadlineMillis - 1);
        expect(yield* child.isRunning).toBe(true);
        expect(killing.pollUnsafe()).toBeUndefined();
        expect(waiting.pollUnsafe()).toBeUndefined();
        yield* TestClock.adjust("1 millis");
        expect(yield* child.isRunning).toBe(false);
        yield* Fiber.join(killing);
        expect(yield* Fiber.join(waiting)).toEqual({ exitCode: 137, signal: null });
      }),
  );

  it.live("force-kills commands that ignore SIGTERM and resolves wait_for_exit", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const ready = yield* Deferred.make<void>();
      const spawned = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>();
      let stdout = "";
      const terminals = yield* makeAcpClientTerminals({
        defaultCwd: process.cwd(),
        forceKillAfter: "50 millis",
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
          ...spawner,
          spawn: (command) =>
            spawner.spawn(command).pipe(
              Effect.tap((child) => Deferred.succeed(spawned, child)),
              Effect.map((child) => ({
                ...child,
                stdout: child.stdout.pipe(
                  Stream.tap((chunk) => {
                    stdout += Buffer.from(chunk).toString("utf8");
                    return stdout.includes("ready")
                      ? Deferred.succeed(ready, undefined)
                      : Effect.void;
                  }),
                ),
              })),
            ),
        }),
      );
      yield* Effect.addFinalizer(() => terminals.disposeAll);
      const terminal = yield* terminals.create({
        sessionId: "session",
        command: process.execPath,
        args: [
          "-e",
          "process.on('SIGTERM', () => {}); console.log('ready'); setInterval(() => {}, 1000);",
        ],
      });
      const child = yield* Deferred.await(spawned);
      // Reap only this test's captured child, including when the regression fails.
      yield* Effect.addFinalizer(() => child.kill({ killSignal: "SIGKILL" }).pipe(Effect.ignore));
      yield* Deferred.await(ready);

      const request = { sessionId: "session", ...terminal };
      yield* terminals.kill(request);
      const exit = yield* terminals.waitForExit(request);
      // Windows has no signals: the command is terminated without a signal name.
      if (!(yield* HostProcess.isWindows)) {
        expect(exit).toEqual({ exitCode: null, signal: "SIGKILL" });
      }
      expect(yield* child.isRunning).toBe(false);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reserves terminal capacity across concurrent creates", () =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const results = yield* Effect.all(
          Array.from({ length: 17 }, () =>
            terminals
              .create({
                sessionId: "session",
                command: process.execPath,
                args: ["-e", "setInterval(() => {}, 1000);"],
              })
              .pipe(Effect.result),
          ),
          { concurrency: "unbounded" },
        );

        expect(results.filter(Result.isSuccess)).toHaveLength(16);
        expect(results.filter(Result.isFailure)).toHaveLength(1);
      }),
    ),
  );

  it.live("bounds unreleased terminal handles without invalidating existing ids", () =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const terminalIds: string[] = [];
        for (let index = 0; index < 64; index += 1) {
          const created = yield* terminals.create({
            sessionId: "session",
            command: process.execPath,
            args: ["-e", `process.stdout.write('${index}');`],
          });
          yield* terminals.waitForExit({
            sessionId: "session",
            terminalId: created.terminalId,
          });
          terminalIds.push(created.terminalId);
        }

        const rejected = yield* terminals
          .create({
            sessionId: "session",
            command: process.execPath,
            args: ["-e", ""],
          })
          .pipe(Effect.result);
        expect(Result.isFailure(rejected)).toBe(true);
        expect(terminals.readOutputSnapshot("session", terminalIds[0]!)).toBeDefined();
        expect(terminals.readOutputSnapshot("session", terminalIds.at(-1)!)).toBeDefined();
      }),
    ),
  );

  it.live("bounds aggregate output retained across unreleased terminals", () =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const terminalIds: string[] = [];
        for (let index = 0; index < 3; index += 1) {
          const created = yield* terminals.create({
            sessionId: "session",
            command: process.execPath,
            args: ["-e", "process.stdout.write('x'.repeat(8 * 1024 * 1024));"],
            outputByteLimit: 8 * 1024 * 1024,
          });
          yield* terminals.waitForExit({
            sessionId: "session",
            terminalId: created.terminalId,
          });
          terminalIds.push(created.terminalId);
        }

        const snapshots = terminalIds.map((terminalId) =>
          terminals.readOutputSnapshot("session", terminalId)!,
        );
        const retainedBytes = snapshots.reduce(
          (total, snapshot) => total + Buffer.byteLength(snapshot.output),
          0,
        );
        expect(retainedBytes).toBeLessThanOrEqual(16 * 1024 * 1024);
        expect(snapshots.some((snapshot) => snapshot.truncated)).toBe(true);
      }),
    ),
  );

  it.live("rejects terminal handles from a different ACP session", () =>
    withTerminals((terminals) =>
      Effect.gen(function* () {
        const created = yield* terminals.create({
          sessionId: "owner-session",
          command: process.execPath,
          args: ["-e", "process.stdout.write('owned');"],
        });
        const foreignRequest = {
          sessionId: "foreign-session",
          terminalId: created.terminalId,
        };
        const results = yield* Effect.forEach(
          [
            terminals.output(foreignRequest),
            terminals.waitForExit(foreignRequest),
            terminals.kill(foreignRequest),
            terminals.release(foreignRequest),
          ],
          (effect) => effect.pipe(Effect.result),
          { concurrency: "unbounded" },
        );

        expect(results.every(Result.isFailure)).toBe(true);
        expect(terminals.readOutputSnapshot("foreign-session", created.terminalId)).toBeUndefined();
        expect(terminals.readOutputSnapshot("owner-session", created.terminalId)).toBeDefined();
      }),
    ),
  );

  const isPosixProcessAlive = (pid: number, platform: NodeJS.Platform): boolean => {
    try {
      process.kill(pid, 0);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException | undefined)?.code === "ESRCH") {
        return false;
      }
    }
    if (platform === "linux" || platform === "darwin") {
      const identity = makePosixProcessTreeController(platform).identity(pid);
      if (identity === undefined || posixProcessIsZombie(identity)) {
        return false;
      }
    }
    return true;
  };

  const forceKillPid = (pid: number | undefined): void => {
    if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid) return;
    try {
      process.kill(pid, "SIGKILL");
    } catch {}
  };

  const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

  const spawnSetsidTrackedTerminal = (input: {
    readonly shellCommands: boolean;
    readonly nodeCode: string;
    readonly trackedPids: Array<number>;
    readonly linuxCgroupController?: AcpLinuxCgroupController | null | undefined;
  }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const pidsReady = yield* Deferred.make<{
        readonly leaderPid: number;
        readonly childPid: number;
      }>();
      let stdout = "";
      const terminals = yield* makeAcpClientTerminals({
        defaultCwd: process.cwd(),
        shellCommands: input.shellCommands,
        forceKillAfter: "250 millis",
        ...(input.linuxCgroupController === undefined
          ? {}
          : { linuxCgroupController: input.linuxCgroupController }),
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
          ...spawner,
          spawn: (command) =>
            spawner.spawn(command).pipe(
              Effect.map((child) => ({
                ...child,
                stdout: child.stdout.pipe(
                  Stream.tap((chunk) => {
                    stdout += Buffer.from(chunk).toString("utf8");
                    const leaderMatch = /LEADER_PID=(\d+)/.exec(stdout);
                    const childMatch = /CHILD_PID=(\d+)/.exec(stdout);
                    if (leaderMatch === null || childMatch === null) return Effect.void;
                    const leaderPid = Number(leaderMatch[1]);
                    const childPid = Number(childMatch[1]);
                    input.trackedPids.push(leaderPid, childPid);
                    return Deferred.succeed(pidsReady, { leaderPid, childPid });
                  }),
                ),
              })),
            ),
        }),
      );
      const created = yield* terminals.create(
        input.shellCommands
          ? {
              sessionId: "session",
              command: `exec ${shellQuote(process.execPath)} -e ${shellQuote(input.nodeCode)}`,
            }
          : { sessionId: "session", command: process.execPath, args: ["-e", input.nodeCode] },
      );
      const pids = yield* Deferred.await(pidsReady);
      return { terminals, terminalId: created.terminalId, ...pids };
    });

  const DETACHED_RUNNING_LEADER_NODE_CODE = [
    'const { spawn } = require("node:child_process");',
    'const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 20000)"], { detached: true, stdio: "ignore" });',
    "child.unref();",
    "console.log(`LEADER_PID=${process.pid}`);",
    "console.log(`CHILD_PID=${child.pid}`);",
    "setTimeout(() => {}, 20000);",
  ].join(" ");

  const DETACHED_EXITED_LEADER_NODE_CODE = [
    'const { spawn } = require("node:child_process");',
    'const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 20000)"], { detached: true, stdio: "ignore" });',
    "child.unref();",
    "console.log(`LEADER_PID=${process.pid}`);",
    "console.log(`CHILD_PID=${child.pid}`);",
    // Stay alive longer than Darwin's 250 ms process-tree observe interval before exiting cleanly.
    "setTimeout(() => process.exit(0), 400);",
  ].join(" ");

  it.live.each([
    { mode: "default-cgroup", linuxCgroupController: undefined },
    { mode: "posix-ledger", linuxCgroupController: null },
  ])(
    "terminates leader and setsid descendant on kill, release, and disposeAll with shellCommands: true ($mode)",
    ({ linuxCgroupController }) =>
      Effect.gen(function* () {
        const platform = yield* HostProcess.Platform;
        if (platform === "win32") return;

        // 1) terminal/kill terminates both leader and setsid descendant and resolves waitForExit.
        {
          const trackedPids: Array<number> = [];
          yield* Effect.gen(function* () {
            const spawned = yield* spawnSetsidTrackedTerminal({
              shellCommands: true,
              nodeCode: DETACHED_RUNNING_LEADER_NODE_CODE,
              trackedPids,
              linuxCgroupController,
            });
            yield* Effect.addFinalizer(() => spawned.terminals.disposeAll);
            expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(true);
            expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(true);

            yield* spawned.terminals.kill({
              sessionId: "session",
              terminalId: spawned.terminalId,
            });
            const exit = yield* spawned.terminals.waitForExit({
              sessionId: "session",
              terminalId: spawned.terminalId,
            });
            expect(exit.signal).not.toBeNull();
            expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(false);
            expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(false);
          }).pipe(
            Effect.scoped,
            Effect.ensuring(
              Effect.sync(() => {
                for (const pid of trackedPids) forceKillPid(pid);
              }),
            ),
          );
        }

        // 2) terminal/release terminates a setsid descendant even after the leader has already exited.
        {
          const trackedPids: Array<number> = [];
          yield* Effect.gen(function* () {
            const spawned = yield* spawnSetsidTrackedTerminal({
              shellCommands: true,
              nodeCode: DETACHED_EXITED_LEADER_NODE_CODE,
              trackedPids,
              linuxCgroupController,
            });
            yield* Effect.addFinalizer(() => spawned.terminals.disposeAll);

            const exit = yield* spawned.terminals.waitForExit({
              sessionId: "session",
              terminalId: spawned.terminalId,
            });
            expect(exit.exitCode).toBe(0);
            expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(true);

            yield* spawned.terminals.release({
              sessionId: "session",
              terminalId: spawned.terminalId,
            });
            expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(false);
            expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(false);
          }).pipe(
            Effect.scoped,
            Effect.ensuring(
              Effect.sync(() => {
                for (const pid of trackedPids) forceKillPid(pid);
              }),
            ),
          );
        }

        // 3) disposeAll terminates both leader and setsid descendant.
        {
          const trackedPids: Array<number> = [];
          yield* Effect.gen(function* () {
            const spawned = yield* spawnSetsidTrackedTerminal({
              shellCommands: true,
              nodeCode: DETACHED_RUNNING_LEADER_NODE_CODE,
              trackedPids,
              linuxCgroupController,
            });
            expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(true);
            expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(true);

            yield* spawned.terminals.disposeAll;
            expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(false);
            expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(false);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                for (const pid of trackedPids) forceKillPid(pid);
              }),
            ),
          );
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("terminates setsid descendant on disposeAll with shellCommands: false", () =>
    Effect.gen(function* () {
      const platform = yield* HostProcess.Platform;
      if (platform === "win32") return;

      const trackedPids: Array<number> = [];
      yield* Effect.gen(function* () {
        const spawned = yield* spawnSetsidTrackedTerminal({
          shellCommands: false,
          nodeCode: DETACHED_RUNNING_LEADER_NODE_CODE,
          trackedPids,
          linuxCgroupController: null,
        });
        expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(true);
        expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(true);

        yield* spawned.terminals.disposeAll;
        expect(isPosixProcessAlive(spawned.leaderPid, platform)).toBe(false);
        expect(isPosixProcessAlive(spawned.childPid, platform)).toBe(false);
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            for (const pid of trackedPids) forceKillPid(pid);
          }),
        ),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live(
    "prefers AgentScope auxiliary units (including cwd-relative executables) and calls terminateUnit on kill, release, and disposeAll",
    () =>
      Effect.gen(function* () {
        const platform = yield* HostProcess.Platform;
        if (platform === "win32") return;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-client-term-cwd-" });
        const relativeScript = path.join(workDir, "local-helper.sh");
        yield* fs.writeFileString(relativeScript, "#!/bin/sh\nsleep 20\n");
        yield* fs.chmod(relativeScript, 0o755);

        const wrapCalls: Array<{
          readonly command: string;
          readonly args: ReadonlyArray<string>;
          readonly name: string;
          readonly threadId: string | undefined;
          readonly auxiliary: boolean | undefined;
        }> = [];
        const terminatedUnits: Array<string> = [];
        let nextUnit = 1;

        const terminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          shellCommands: true,
          threadId: "thread-terminal-scope",
          forceKillAfter: "250 millis",
        }).pipe(
          Effect.provideService(AgentScope, {
            ...AgentScope.defaultValue(),
            wrap: ({ command, args, name, threadId, auxiliary }) =>
              Effect.sync(() => {
                wrapCalls.push({ command, args, name, threadId, auxiliary });
                const unit = `t3code-${name}-${nextUnit}.scope`;
                nextUnit += 1;
                return { command, args, unit };
              }),
            terminateUnit: (unit) =>
              Effect.sync(() => {
                terminatedUnits.push(unit);
              }),
          }),
        );
        yield* Effect.addFinalizer(() => terminals.disposeAll);

        // 1) kill -> terminateUnit
        const t1 = yield* terminals.create({ sessionId: "session", command: "sleep 20" });
        yield* terminals.kill({ sessionId: "session", terminalId: t1.terminalId });
        expect(terminatedUnits).toEqual(["t3code-acp-terminal-1.scope"]);

        // 2) release -> terminateUnit (cwd-relative executable with args bypasses shell and resolves against cwd)
        const t2 = yield* terminals.create({
          sessionId: "session",
          command: "./local-helper.sh",
          args: ["arg1"],
        });
        yield* terminals.release({ sessionId: "session", terminalId: t2.terminalId });
        expect(terminatedUnits).toEqual([
          "t3code-acp-terminal-1.scope",
          "t3code-acp-terminal-2.scope",
        ]);

        // 3) disposeAll -> terminateUnit
        yield* terminals.create({ sessionId: "session", command: "sleep 20" });
        yield* terminals.disposeAll;
        expect(terminatedUnits).toEqual([
          "t3code-acp-terminal-1.scope",
          "t3code-acp-terminal-2.scope",
          "t3code-acp-terminal-3.scope",
        ]);

        expect(wrapCalls).toHaveLength(3);
        expect(wrapCalls[0]!.command).toBe("/bin/sh");
        expect(wrapCalls[0]!.args).toEqual(["-c", "sleep 20"]);
        expect(wrapCalls[1]!.command).toBe(relativeScript);
        expect(wrapCalls[1]!.args).toEqual(["arg1"]);
        expect(wrapCalls[2]!.command).toBe("/bin/sh");
        expect(wrapCalls[2]!.args).toEqual(["-c", "sleep 20"]);
        for (const call of wrapCalls) {
          expect(call.name).toBe("acp-terminal");
          expect(call.threadId).toBe("thread-terminal-scope");
          expect(call.auxiliary).toBe(true);
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "invokes windowsProcessTreeTerminator at most once while running and skips already-exited terminals",
    () =>
      Effect.gen(function* () {
        const terminatedPids: Array<number> = [];
        const terminals = yield* makeAcpClientTerminals({
          defaultCwd: process.cwd(),
          forceKillAfter: "250 millis",
          windowsProcessTreeTerminator: (pid) =>
            Effect.sync(() => {
              terminatedPids.push(pid);
            }),
        }).pipe(Effect.provideService(HostProcess.Platform, "win32"));
        yield* Effect.addFinalizer(() => terminals.disposeAll);

        // 1) Already-exited short-lived command must never trigger taskkill on release or disposeAll.
        const shortTerm = yield* terminals.create({
          sessionId: "session",
          command: process.execPath,
          args: ["-e", "process.exit(0)"],
        });
        expect(
          yield* terminals.waitForExit({ sessionId: "session", terminalId: shortTerm.terminalId }),
        ).toEqual({ exitCode: 0, signal: null });
        yield* terminals.release({ sessionId: "session", terminalId: shortTerm.terminalId });
        yield* terminals.disposeAll;
        expect(terminatedPids).toEqual([]);

        // 2) Live command invokes terminator once on kill and never again on release or disposeAll.
        const liveTerm = yield* terminals.create({
          sessionId: "session",
          command: process.execPath,
          args: ["-e", "setTimeout(() => {}, 20000)"],
        });
        yield* terminals.kill({ sessionId: "session", terminalId: liveTerm.terminalId });
        expect(terminatedPids).toHaveLength(1);
        yield* terminals.release({ sessionId: "session", terminalId: liveTerm.terminalId });
        yield* terminals.disposeAll;
        expect(terminatedPids).toHaveLength(1);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "isolates AgentScope pre-containment wrappers from untrusted request.env while delivering request.env post-containment (R4-CF-10)",
    () =>
      Effect.gen(function* () {
        const platform = yield* HostProcess.Platform;
        if (platform !== "linux") return;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-r4-cf10-scope-" });
        const hostRuntimeDir = path.join(workDir, "host-xdg");
        yield* fs.makeDirectory(hostRuntimeDir, { recursive: true });
        const customBinDir = path.join(workDir, "custom-bin");
        yield* fs.makeDirectory(customBinDir, { recursive: true });
        const systemdEnvFile = path.join(workDir, "systemd-run-env.json");
        const targetEnvFile = path.join(workDir, "target-env.json");
        const poisonedEnvRanFile = path.join(workDir, "poisoned-env-ran");

        // Malicious `env` inside request.env PATH must NEVER be selected as envBin.
        const poisonedEnvBin = path.join(customBinDir, "env");
        yield* fs.writeFileString(
          poisonedEnvBin,
          `#!/bin/sh\necho poisoned > ${shellQuote(poisonedEnvRanFile)}\nexit 99\n`,
        );
        yield* fs.chmod(poisonedEnvBin, 0o755);

        const stubSystemdRun = path.join(workDir, "stub-systemd-run");
        yield* fs.writeFileString(
          stubSystemdRun,
          [
            "#!/bin/sh",
            `${shellQuote(process.execPath)} -e 'const fs=require("node:fs"); fs.writeFileSync(process.argv[1], JSON.stringify(process.env));' ${shellQuote(systemdEnvFile)}`,
            'while [ "$#" -gt 0 ]; do',
            '  if [ "$1" = "--" ]; then shift; break; fi',
            "  shift",
            "done",
            'exec "$@"',
          ].join("\n") + "\n",
        );
        yield* fs.chmod(stubSystemdRun, 0o755);

        const targetScript =
          [
            "#!/bin/sh",
            `${shellQuote(process.execPath)} -e 'const fs=require("node:fs"); fs.writeFileSync(process.argv[1], JSON.stringify({ env: process.env, args: process.argv.slice(2) }));' ${shellQuote(targetEnvFile)} "$@"`,
          ].join("\n") + "\n";
        const myTool = path.join(customBinDir, "my-tool");
        yield* fs.writeFileString(myTool, targetScript);
        yield* fs.chmod(myTool, 0o755);

        const eqTool = path.join(customBinDir, "eq=tool");
        yield* fs.writeFileString(eqTool, targetScript);
        yield* fs.chmod(eqTool, 0o755);

        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const hostEnv = yield* HostProcess.Environment;
        const hostPath = hostEnv.PATH ?? "/usr/bin:/bin";
        const spawnEnvs: Array<Record<string, unknown>> = [];
        const wrapEnvs: Array<NodeJS.ProcessEnv | undefined> = [];
        let nextUnit = 1;

        const terminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          forceKillAfter: "250 millis",
          environment: {
            PATH: hostPath,
            XDG_RUNTIME_DIR: hostRuntimeDir,
            DBUS_SESSION_BUS_ADDRESS: "unix:path=/tmp/host-dbus",
            LD_PRELOAD: "/tmp/host-preload.so",
            LD_AUDIT: "/tmp/host-audit.so",
            GCONV_PATH: "/tmp/host-gconv",
            BASH_ENV: "/tmp/host-bash-env",
            ENV: "/tmp/host-env",
            IFS: ":",
            HOST_KEPT: "yes",
          },
        }).pipe(
          Effect.provideService(AgentScope, {
            ...AgentScope.defaultValue(),
            wrap: ({ command, args, name, env }) =>
              Effect.sync(() => {
                wrapEnvs.push(env);
                const unit = `t3code-${name}-${nextUnit}.scope`;
                nextUnit += 1;
                return {
                  ...agentScopeCommand({
                    command,
                    args,
                    scope: {
                      systemdRun: stubSystemdRun,
                      slice: "app-t3code-agents-test.slice",
                      unit,
                      runtimeDir: hostRuntimeDir,
                      options: ["--collect"],
                    },
                  }),
                  unit,
                };
              }),
            terminateUnit: () => Effect.void,
          }),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
            ...spawner,
            spawn: (command) => {
              if (ChildProcess.isStandardCommand(command)) {
                spawnEnvs.push({ ...command.options.env });
              }
              return spawner.spawn(command);
            },
          }),
        );
        yield* Effect.addFinalizer(() => terminals.disposeAll);

        // 1) Invalid request.env entries are rejected cleanly.
        for (const badEnv of [
          [{ name: "", value: "x" }],
          [{ name: "BAD=NAME", value: "x" }],
          [{ name: "BAD\0NAME", value: "x" }],
          [{ name: "GOOD", value: "bad\0val" }],
        ]) {
          const rejected = yield* Effect.exit(
            terminals.create({
              sessionId: "session",
              command: "/bin/true",
              env: badEnv,
            }),
          );
          expect(Exit.isFailure(rejected)).toBe(true);
        }

        // 2) Command resolved via request.env PATH runs post-containment with request.env,
        //    while wrapper spawn env and systemd-run NEVER see request.env or stripped vars.
        const created = yield* terminals.create({
          sessionId: "session",
          command: "my-tool",
          args: ["arg-one", "arg-two"],
          env: [
            { name: "XDG_RUNTIME_DIR", value: "/tmp/attacker-xdg" },
            { name: "DBUS_SESSION_BUS_ADDRESS", value: "unix:path=/tmp/attacker-bus" },
            { name: "LD_PRELOAD", value: "/tmp/nonexistent-attacker-preload.so" },
            { name: "LD_AUDIT", value: "/tmp/nonexistent-attacker-audit.so" },
            { name: "BASH_ENV", value: "/tmp/attacker-bash-env" },
            { name: "ENV", value: "/tmp/attacker-sh-env" },
            { name: "IFS", value: ";" },
            { name: "GCONV_PATH", value: "/tmp/attacker-gconv" },
            { name: "PATH", value: customBinDir },
            { name: "SAFE_VAR", value: "first" },
            { name: "SAFE_VAR", value: "last-wins" },
          ],
        });
        const exit = yield* terminals.waitForExit({
          sessionId: "session",
          terminalId: created.terminalId,
        });
        expect(exit.exitCode).toBe(0);
        expect(yield* fs.exists(poisonedEnvRanFile)).toBe(false);

        const wrapEnv = wrapEnvs[0]!;
        const spawnEnv = spawnEnvs[0]!;
        const systemdRunEnv = JSON.parse(yield* fs.readFileString(systemdEnvFile)) as Record<
          string,
          string | undefined
        >;
        for (const preEnv of [wrapEnv, spawnEnv, systemdRunEnv]) {
          expect(preEnv.XDG_RUNTIME_DIR).toBe(hostRuntimeDir);
          expect(preEnv.DBUS_SESSION_BUS_ADDRESS).toBeUndefined();
          expect(preEnv.LD_PRELOAD).toBeUndefined();
          expect(preEnv.LD_AUDIT).toBeUndefined();
          expect(preEnv.BASH_ENV).toBeUndefined();
          expect(preEnv.ENV).toBeUndefined();
          expect(preEnv.IFS).toBeUndefined();
          expect(preEnv.GCONV_PATH).toBeUndefined();
          expect(preEnv.SAFE_VAR).toBeUndefined();
          expect(preEnv.PATH).not.toBe(customBinDir);
          expect(preEnv.HOST_KEPT).toBe("yes");
        }

        const targetResult = JSON.parse(yield* fs.readFileString(targetEnvFile)) as {
          readonly env: Record<string, string | undefined>;
          readonly args: ReadonlyArray<string>;
        };
        expect(targetResult.args).toEqual(["arg-one", "arg-two"]);
        expect(targetResult.env.SAFE_VAR).toBe("last-wins");
        expect(targetResult.env.XDG_RUNTIME_DIR).toBe("/tmp/attacker-xdg");
        expect(targetResult.env.DBUS_SESSION_BUS_ADDRESS).toBe("unix:path=/tmp/attacker-bus");
        expect(targetResult.env.BASH_ENV).toBe("/tmp/attacker-bash-env");
        expect(targetResult.env.GCONV_PATH).toBe("/tmp/attacker-gconv");
        expect(targetResult.env.PATH).toBe(customBinDir);
        expect(targetResult.env.HOST_KEPT).toBe("yes");

        // 3) Executable path containing '=' is invoked safely past `env` assignments.
        const eqCreated = yield* terminals.create({
          sessionId: "session",
          command: "eq=tool",
          args: ["eq-arg"],
          env: [
            { name: "PATH", value: customBinDir },
            { name: "EQ_TEST", value: "ok" },
          ],
        });
        const eqExit = yield* terminals.waitForExit({
          sessionId: "session",
          terminalId: eqCreated.terminalId,
        });
        expect(eqExit.exitCode).toBe(0);
        const eqResult = JSON.parse(yield* fs.readFileString(targetEnvFile)) as {
          readonly env: Record<string, string | undefined>;
          readonly args: ReadonlyArray<string>;
        };
        expect(eqResult.args).toEqual(["eq-arg"]);
        expect(eqResult.env.EQ_TEST).toBe("ok");

        // 4) Unresolved command under request.env.PATH does NOT fall back to host PATH via AgentScope
        // (prevents running wrong host binary and dropping request.env).
        const emptyPathDir = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-r4-cf10-empty-path-",
        });
        let unresolvedWrapCalls = 0;
        const unresolvedTerminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          environment: {
            PATH: hostPath,
            XDG_RUNTIME_DIR: hostRuntimeDir,
          },
          agentScope: {
            ...AgentScope.defaultValue(),
            wrap: ({ command, args }) =>
              Effect.sync(() => {
                unresolvedWrapCalls += 1;
                return {
                  command,
                  args,
                  unit: "t3code-acp-terminal-unresolved.scope",
                };
              }),
            terminateUnit: () => Effect.void,
          },
        });
        yield* Effect.addFinalizer(() => unresolvedTerminals.disposeAll);
        const unresolvedExit = yield* unresolvedTerminals
          .create({
            sessionId: "session",
            command: "true",
            env: [
              { name: "PATH", value: emptyPathDir },
              { name: "LD_PRELOAD", value: "/tmp/evil.so" },
            ],
          })
          .pipe(Effect.exit);
        expect(Exit.isFailure(unresolvedExit)).toBe(true);
        expect(unresolvedWrapCalls).toBe(0);

        // 5) Fallback when explicitCgroupController.create() returns undefined preserves request.env.
        const fallbackTerminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          environment: {
            PATH: hostPath,
            XDG_RUNTIME_DIR: hostRuntimeDir,
          },
          linuxCgroupController: {
            create: () => undefined,
          },
        });
        yield* Effect.addFinalizer(() => fallbackTerminals.disposeAll);
        const fallbackTerm = yield* fallbackTerminals.create({
          sessionId: "session",
          command: myTool,
          args: ["fallback-arg"],
          env: [{ name: "FALLBACK_KEPT", value: "yes" }],
        });
        const fallbackExit = yield* fallbackTerminals.waitForExit({
          sessionId: "session",
          terminalId: fallbackTerm.terminalId,
        });
        expect(fallbackExit.exitCode).toBe(0);
        const fallbackResult = JSON.parse(yield* fs.readFileString(targetEnvFile)) as {
          readonly env: Record<string, string | undefined>;
          readonly args: ReadonlyArray<string>;
        };
        expect(fallbackResult.args).toEqual(["fallback-arg"]);
        expect(fallbackResult.env.FALLBACK_KEPT).toBe("yes");

        // 6) Trusted host LD_LIBRARY_PATH is stripped from wrapperEnvironment and restored post-containment,
        // and an executable `./env` inside request.cwd cannot hijack host env resolution when PATH has an empty segment.
        const fakeCwdEnv = path.join(workDir, "env");
        yield* fs.writeFileString(fakeCwdEnv, "#!/bin/sh\necho HIJACKED >&2\nexit 99\n");
        yield* fs.chmod(fakeCwdEnv, 0o755);
        const hostLdTerminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          environment: {
            PATH: `:${hostPath}`,
            XDG_RUNTIME_DIR: hostRuntimeDir,
            LD_LIBRARY_PATH: "/opt/trusted-host/lib",
          },
          agentScope: {
            ...AgentScope.defaultValue(),
            wrap: ({ command, args }) =>
              Effect.succeed({
                ...agentScopeCommand({
                  command,
                  args,
                  scope: {
                    systemdRun: stubSystemdRun,
                    slice: "t3code-test.slice",
                    unit: "t3code-acp-terminal-host-ld.scope",
                    runtimeDir: hostRuntimeDir,
                    options: [],
                  },
                }),
                unit: "t3code-acp-terminal-host-ld.scope",
              }),
            terminateUnit: () => Effect.void,
          },
        });
        yield* Effect.addFinalizer(() => hostLdTerminals.disposeAll);
        const hostLdTerm = yield* hostLdTerminals.create({
          sessionId: "session",
          command: myTool,
          args: ["host-ld-arg"],
        });
        const hostLdExit = yield* hostLdTerminals.waitForExit({
          sessionId: "session",
          terminalId: hostLdTerm.terminalId,
        });
        expect(hostLdExit.exitCode).toBe(0);
        const hostLdWrapperEnv = JSON.parse(yield* fs.readFileString(systemdEnvFile)) as Record<
          string,
          string | undefined
        >;
        const hostLdTargetResult = JSON.parse(yield* fs.readFileString(targetEnvFile)) as {
          readonly env: Record<string, string | undefined>;
        };
        expect(hostLdWrapperEnv.LD_LIBRARY_PATH).toBeUndefined();
        expect(hostLdTargetResult.env.LD_LIBRARY_PATH).toBe("/opt/trusted-host/lib");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "isolates Linux cgroup /bin/sh wrapper from request.env on both migration failure (exit 125) and success (R4-CF-10)",
    () =>
      Effect.gen(function* () {
        const platform = yield* HostProcess.Platform;
        if (platform !== "linux") return;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-r4-cf10-cgroup-" });
        const hookSentinel = path.join(workDir, "hook-executed");
        const hookScript = path.join(workDir, "hook.sh");
        yield* fs.writeFileString(hookScript, `echo ran > ${shellQuote(hookSentinel)}\n`);
        yield* fs.chmod(hookScript, 0o755);

        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const spawnEnvs: Array<Record<string, unknown>> = [];
        const recordingSpawner = {
          ...spawner,
          spawn: (command: ChildProcess.Command) => {
            if (ChildProcess.isStandardCommand(command)) {
              spawnEnvs.push({ ...command.options.env });
            }
            return spawner.spawn(command);
          },
        };

        const hostileEnv = [
          { name: "LD_PRELOAD", value: "/tmp/nonexistent-cgroup-preload.so" },
          { name: "BASH_ENV", value: hookScript },
          { name: "ENV", value: hookScript },
          { name: "IFS", value: ":" },
          { name: "DBUS_SESSION_BUS_ADDRESS", value: "unix:path=/tmp/attacker-bus" },
          { name: "TARGET_ONLY_SECRET", value: "delivered-after-cgroup" },
        ];

        // 1) Migration fails with exit 125 before exec: wrapper env has none of request.env, and hook never runs.
        const failingController: AcpLinuxCgroupController = {
          create: () => ({
            path: path.join(workDir, "missing-cgroup-dir"),
            relativePath: "/missing",
            contains: () => false,
            exists: () => false,
            kill: () => {},
            populated: () => false,
            remove: () => {},
          }),
        };
        const failingTerminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          linuxCgroupController: failingController,
          forceKillAfter: "250 millis",
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, recordingSpawner));
        yield* Effect.addFinalizer(() => failingTerminals.disposeAll);

        const failedTerm = yield* failingTerminals.create({
          sessionId: "session",
          command: "/bin/true",
          env: hostileEnv,
        });
        const failedExit = yield* failingTerminals.waitForExit({
          sessionId: "session",
          terminalId: failedTerm.terminalId,
        });
        expect(failedExit.exitCode).toBe(125);
        expect(yield* fs.exists(hookSentinel)).toBe(false);
        expect(spawnEnvs[0]!.LD_PRELOAD).toBeUndefined();
        expect(spawnEnvs[0]!.BASH_ENV).toBeUndefined();
        expect(spawnEnvs[0]!.ENV).toBeUndefined();
        expect(spawnEnvs[0]!.IFS).toBeUndefined();
        expect(spawnEnvs[0]!.DBUS_SESSION_BUS_ADDRESS).toBeUndefined();
        expect(spawnEnvs[0]!.TARGET_ONLY_SECRET).toBeUndefined();

        // 2) Migration succeeds: wrapper env stays clean, while target receives request.env after cgroup migration.
        const procSelfCgroup = yield* fs
          .readFileString("/proc/self/cgroup")
          .pipe(Effect.orElseSucceed(() => ""));
        const cgroupV2Line = procSelfCgroup.split("\n").find((line) => line.startsWith("0::"));
        if (cgroupV2Line === undefined) return;
        const currentRelativePath = cgroupV2Line.slice("0::".length);
        const fakeCgroupDir = path.join(workDir, "fake-cgroup");
        yield* fs.makeDirectory(fakeCgroupDir, { recursive: true });
        yield* fs.writeFileString(path.join(fakeCgroupDir, "cgroup.procs"), "");

        const succeedingController: AcpLinuxCgroupController = {
          create: () => ({
            path: fakeCgroupDir,
            relativePath: currentRelativePath,
            contains: () => false,
            exists: () => false,
            kill: () => {},
            populated: () => false,
            remove: () => {},
          }),
        };
        const targetEnvFile = path.join(workDir, "cgroup-target-env.json");
        const succeedingTerminals = yield* makeAcpClientTerminals({
          defaultCwd: workDir,
          linuxCgroupController: succeedingController,
          forceKillAfter: "250 millis",
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, recordingSpawner));
        yield* Effect.addFinalizer(() => succeedingTerminals.disposeAll);

        const okTerm = yield* succeedingTerminals.create({
          sessionId: "session",
          command: process.execPath,
          args: [
            "-e",
            `require("node:fs").writeFileSync(${JSON.stringify(targetEnvFile)}, JSON.stringify(process.env))`,
          ],
          env: hostileEnv,
        });
        const okExit = yield* succeedingTerminals.waitForExit({
          sessionId: "session",
          terminalId: okTerm.terminalId,
        });
        expect(okExit.exitCode).toBe(0);
        expect(yield* fs.exists(hookSentinel)).toBe(false);
        expect(spawnEnvs[1]!.LD_PRELOAD).toBeUndefined();
        expect(spawnEnvs[1]!.BASH_ENV).toBeUndefined();
        expect(spawnEnvs[1]!.ENV).toBeUndefined();
        expect(spawnEnvs[1]!.IFS).toBeUndefined();
        expect(spawnEnvs[1]!.DBUS_SESSION_BUS_ADDRESS).toBeUndefined();
        expect(spawnEnvs[1]!.TARGET_ONLY_SECRET).toBeUndefined();

        const deliveredEnv = JSON.parse(yield* fs.readFileString(targetEnvFile)) as Record<
          string,
          string | undefined
        >;
        expect(deliveredEnv.TARGET_ONLY_SECRET).toBe("delivered-after-cgroup");
        expect(deliveredEnv.DBUS_SESSION_BUS_ADDRESS).toBe("unix:path=/tmp/attacker-bus");
        expect(deliveredEnv.BASH_ENV).toBe(hookScript);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "fences disposeAll against in-flight create() suspended in agentScope.wrap, cleans up scopeUnit, and never spawns after disposeAll (R4-CF-11)",
    () =>
      Effect.gen(function* () {
        const wrapEntered = yield* Deferred.make<void>();
        const releaseWrap = yield* Deferred.make<void>();
        const terminatedUnits: Array<string> = [];
        let spawnCount = 0;

        const makeHandle = () =>
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(1),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            stdin: Sink.drain,
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
            unref: Effect.succeed(Effect.void),
          });

        let wrapOrdinal = 0;
        const terminals = yield* makeAcpClientTerminals({
          defaultCwd: process.cwd(),
          linuxCgroupController: null,
          agentScope: {
            ...AgentScope.defaultValue(),
            wrap: ({ command, args, name }) =>
              Effect.gen(function* () {
                wrapOrdinal += 1;
                const unit = `t3code-${name}-${wrapOrdinal}.scope`;
                if (wrapOrdinal === 1) {
                  yield* Deferred.succeed(wrapEntered, undefined);
                  yield* Deferred.await(releaseWrap);
                }
                return { command, args, unit };
              }),
            terminateUnit: (unit) =>
              Effect.sync(() => {
                terminatedUnits.push(unit);
              }),
          },
        }).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.sync(() => {
                spawnCount += 1;
                return makeHandle();
              }),
            ),
          ),
        );

        const createdIds: Array<string> = [];
        const createFiber = yield* terminals
          .create(
            { sessionId: "session", command: process.execPath },
            {
              onCreated: (terminalId) => {
                createdIds.push(terminalId);
              },
            },
          )
          .pipe(Effect.forkChild);

        yield* Deferred.await(wrapEntered);

        // disposeAll advances disposalGeneration immediately, then waits for creationPermit.
        const disposeFiber = yield* terminals.disposeAll.pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        expect(disposeFiber.pollUnsafe()).toBeUndefined();

        // Letting wrap finish must see the bumped disposalGeneration, clean up its scopeUnit,
        // abort before spawner.spawn, and let disposeAll complete cleanly.
        yield* Deferred.succeed(releaseWrap, undefined);
        const createExit = yield* Fiber.await(createFiber);
        yield* Fiber.join(disposeFiber);

        expect(Exit.isFailure(createExit)).toBe(true);
        expect(spawnCount).toBe(0);
        expect(createdIds).toEqual([]);
        expect(terminatedUnits).toEqual(["t3code-acp-terminal-1.scope"]);

        // A new create() started AFTER disposeAll succeeds and invokes onCreated synchronously upon registration.
        let callbackTerminalId: string | undefined;
        let sawCallbackBeforeResolve = false;
        const gen1Created = yield* terminals
          .create(
            { sessionId: "session", command: process.execPath },
            {
              ownerGeneration: 1,
              onCreated: (terminalId) => {
                callbackTerminalId = terminalId;
              },
            },
          )
          .pipe(
            Effect.tap((response) =>
              Effect.sync(() => {
                sawCallbackBeforeResolve = callbackTerminalId === response.terminalId;
              }),
            ),
          );
        expect(spawnCount).toBe(1);
        expect(callbackTerminalId).toBe(gen1Created.terminalId);
        expect(sawCallbackBeforeResolve).toBe(true);

        // disposeGeneration(1) disposes generation 1 without touching generation 2's terminals.
        const gen2Created = yield* terminals.create(
          { sessionId: "session", command: process.execPath },
          { ownerGeneration: 2 },
        );
        yield* terminals.disposeGeneration(1);
        const gen1Out = yield* terminals
          .output({ sessionId: "session", terminalId: gen1Created.terminalId })
          .pipe(Effect.exit);
        expect(Exit.isFailure(gen1Out)).toBe(true);
        const gen2Out = yield* terminals.output({
          sessionId: "session",
          terminalId: gen2Created.terminalId,
        });
        expect(gen2Out.output).toBe("");
        expect(terminatedUnits).toEqual([
          "t3code-acp-terminal-1.scope",
          "t3code-acp-terminal-2.scope",
        ]);
        yield* terminals.disposeAll;
        expect(terminatedUnits).toEqual([
          "t3code-acp-terminal-1.scope",
          "t3code-acp-terminal-2.scope",
          "t3code-acp-terminal-3.scope",
        ]);
      }),
  );
});

describe("resolveEmbeddedTerminalContent", () => {
  it("rewrites terminal content into text from the buffered snapshot", () => {
    const notification = {
      sessionId: "session",
      update: {
        sessionUpdate: "tool_call_update" as const,
        toolCallId: "call-1",
        content: [
          { type: "content" as const, content: { type: "text" as const, text: "before" } },
          { type: "terminal" as const, terminalId: "t3-term-1" },
          { type: "terminal" as const, terminalId: "t3-term-unknown" },
        ],
      },
    };

    const resolved = resolveEmbeddedTerminalContent(notification, (sessionId, terminalId) =>
      sessionId === "session" && terminalId === "t3-term-1"
        ? { output: "compiled 3 files", truncated: false, exitStatus: undefined }
        : undefined,
    );

    expect(resolved.update).toMatchObject({
      content: [
        { type: "content", content: { type: "text", text: "before" } },
        { type: "content", content: { type: "text", text: "compiled 3 files" } },
        { type: "content", content: { type: "text", text: "[terminal t3-term-unknown]" } },
      ],
    });
  });
});
