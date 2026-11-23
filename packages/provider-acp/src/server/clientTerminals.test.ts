import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
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
