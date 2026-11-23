import * as Cause from "effect/Cause";
import * as Channel from "effect/Channel";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as NodeBuffer from "node:buffer";
import { AgentScope, AgentScopeThreadId, type AgentScopeShape } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  capturePosixOwnershipLedger,
  makeLinuxCgroupController,
  makePosixProcessTreeController,
  observePosixOwnershipLedger,
  observePosixOwnershipLedgerContinuously,
  resolveLinuxCgroupTargetCommand,
  terminateLinuxCgroupLease,
  terminatePosixOwnedProcessTree,
  terminateWindowsProcessTreeWithTaskkill,
  wrapCommandForLinuxCgroup,
  type AcpLinuxCgroupController,
  type AcpLinuxCgroupLease,
  type AcpOwnedPosixProcess,
  type AcpPosixOwnershipRoot,
  type AcpPosixProcessTreeController,
  type AcpProcessGroupTerminationError,
} from "./AcpSessionRuntime.ts";

const MAX_LIVE_TERMINALS = 16;
const MAX_UNRELEASED_TERMINALS = 64;
const MAX_RETAINED_TERMINALS = 32;
const MAX_RETAINED_OUTPUT_BYTES = 16 * 1024 * 1024;
const DEFAULT_OUTPUT_BYTE_LIMIT = 4 * 1024 * 1024;
const MAX_OUTPUT_BYTE_LIMIT = 8 * 1024 * 1024;
const OUTPUT_DRAIN_GRACE = "1 second";

const terminalRequestError = (message: string, cause?: unknown) =>
  EffectAcpErrors.AcpRequestError.internalError(
    message,
    undefined,
    cause === undefined ? {} : { cause },
  );

interface OutputBufferState {
  chunks: Array<Uint8Array>;
  bytes: number;
  truncated: boolean;
  readonly limit: number;
}

function trimOutputStart(state: OutputBufferState, bytes: number): void {
  let remaining = Math.min(bytes, state.bytes);
  while (remaining > 0) {
    const head = state.chunks[0]!;
    if (head.byteLength <= remaining) {
      state.chunks.shift();
      state.bytes -= head.byteLength;
      remaining -= head.byteLength;
    } else {
      state.chunks[0] = head.subarray(remaining);
      state.bytes -= remaining;
      remaining = 0;
    }
    state.truncated = true;
  }
}

function appendOutput(state: OutputBufferState, chunk: Uint8Array): void {
  state.chunks.push(chunk);
  state.bytes += chunk.byteLength;
  // The spec requires truncation from the beginning of the output.
  if (state.bytes > state.limit) trimOutputStart(state, state.bytes - state.limit);
}

function readOutput(state: OutputBufferState): string {
  let combined: Uint8Array = NodeBuffer.Buffer.concat(state.chunks, state.bytes);
  if (state.truncated) {
    // The spec requires truncation at a UTF-8 character boundary.
    let start = 0;
    while (start < combined.byteLength && (combined[start]! & 0b1100_0000) === 0b1000_0000) {
      start += 1;
    }
    combined = combined.subarray(start);
  }
  return NodeBuffer.Buffer.from(combined).toString("utf8");
}

function terminalExitSignal(error: unknown): string | null {
  const cause = error instanceof Error ? error.cause : undefined;
  const message =
    cause instanceof Error ? cause.message : error instanceof Error ? error.message : "";
  return message.match(/signal: '([^']+)'/u)?.[1] ?? null;
}

export interface AcpTerminalOutputSnapshot {
  readonly output: string;
  readonly truncated: boolean;
  readonly exitStatus: EffectAcpSchema.TerminalExitStatus | undefined;
}

interface ManagedAcpTerminal {
  readonly terminalId: string;
  readonly sessionId: string;
  readonly commandLine: string;
  readonly buffer: OutputBufferState;
  readonly exit: Deferred.Deferred<EffectAcpSchema.WaitForTerminalExitResponse>;
  readonly kill: Effect.Effect<void>;
  readonly dispose: Effect.Effect<void>;
  exitStatus: EffectAcpSchema.TerminalExitStatus | undefined;
  released: boolean;
}

export interface AcpClientTerminals {
  readonly create: (
    request: EffectAcpSchema.CreateTerminalRequest,
  ) => Effect.Effect<EffectAcpSchema.CreateTerminalResponse, EffectAcpErrors.AcpRequestError>;
  readonly output: (
    request: EffectAcpSchema.TerminalOutputRequest,
  ) => Effect.Effect<EffectAcpSchema.TerminalOutputResponse, EffectAcpErrors.AcpRequestError>;
  readonly waitForExit: (
    request: EffectAcpSchema.WaitForTerminalExitRequest,
  ) => Effect.Effect<EffectAcpSchema.WaitForTerminalExitResponse, EffectAcpErrors.AcpRequestError>;
  readonly kill: (
    request: EffectAcpSchema.KillTerminalRequest,
  ) => Effect.Effect<EffectAcpSchema.KillTerminalResponse, EffectAcpErrors.AcpRequestError>;
  readonly release: (
    request: EffectAcpSchema.ReleaseTerminalRequest,
  ) => Effect.Effect<EffectAcpSchema.ReleaseTerminalResponse, EffectAcpErrors.AcpRequestError>;
  /** Buffered output for embedded tool-call content; survives release. */
  readonly readOutputSnapshot: (
    sessionId: string,
    terminalId: string,
  ) => AcpTerminalOutputSnapshot | undefined;
  readonly readCommandLine: (terminalId: string) => string | undefined;
  readonly disposeAll: Effect.Effect<void>;
}

export interface AcpClientTerminalsOptions {
  /** Devin sends shell source in command, rather than an executable plus args. */
  readonly shellCommands?: boolean | undefined;
  readonly defaultCwd: string;
  readonly environment?: NodeJS.ProcessEnv | undefined;
  readonly environmentForSession?:
    | ((sessionId: string) => NodeJS.ProcessEnv | undefined)
    | undefined;
  readonly forceKillAfter?: Duration.Input | undefined;
  readonly threadId?: string | undefined;
  readonly agentScope?: AgentScopeShape | undefined;
  readonly linuxCgroupController?: AcpLinuxCgroupController | null | undefined;
  readonly posixProcessTreeController?: AcpPosixProcessTreeController | undefined;
  readonly windowsProcessTreeTerminator?:
    | ((pid: number) => Effect.Effect<void, AcpProcessGroupTerminationError>)
    | undefined;
}

function materializeAcpTerminalCommand(input: {
  readonly request: EffectAcpSchema.CreateTerminalRequest;
  readonly shellCommands?: boolean | undefined;
  readonly isWindows: boolean;
}): {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly shell: boolean;
} {
  const useShell = input.shellCommands === true && (input.request.args?.length ?? 0) === 0;
  if (useShell && !input.isWindows) {
    // Materialize POSIX shell invocation before cgroup or AgentScope wrapping so
    // raw shell source is never passed to executable resolution or `shell: true`.
    return {
      command: "/bin/sh",
      args: ["-c", input.request.command],
      shell: false,
    };
  }
  return {
    command: input.request.command,
    args: input.request.args ?? [],
    shell: useShell,
  };
}

// Windows variable names are case-insensitive and Node passes only one spelling to the child,
// so a later layer's `Path` must replace an earlier `PATH` rather than sit beside it.
function mergeEnvironments(
  isWindows: boolean,
  layers: ReadonlyArray<Iterable<readonly [string, string | undefined]>>,
) {
  const variables = new Map<string, readonly [string, string | undefined]>();
  for (const layer of layers) {
    for (const [name, value] of layer) {
      variables.set(isWindows ? name.toUpperCase() : name, [name, value]);
    }
  }
  return Object.fromEntries(variables.values());
}

export const makeAcpClientTerminals = Effect.fn("makeAcpClientTerminals")(function* (
  options: AcpClientTerminalsOptions,
): Effect.fn.Return<AcpClientTerminals, never, ChildProcessSpawner.ChildProcessSpawner> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const forceKillAfter = options.forceKillAfter ?? "5 seconds";
  const platform = yield* HostProcess.Platform;
  const isWindows = platform === "win32";
  const agentScope = options.agentScope ?? (yield* AgentScope);
  const threadId = options.threadId ?? (yield* AgentScopeThreadId);
  const creationPermit = yield* Semaphore.make(1);
  const terminals = new Map<string, ManagedAcpTerminal>();
  let nextTerminalNumber = 1;

  const enforceTotalOutputLimit = (): void => {
    let excess =
      Array.from(terminals.values()).reduce((total, terminal) => total + terminal.buffer.bytes, 0) -
      MAX_RETAINED_OUTPUT_BYTES;
    if (excess <= 0) return;
    for (const terminal of terminals.values()) {
      const before = terminal.buffer.bytes;
      trimOutputStart(terminal.buffer, excess);
      excess -= before - terminal.buffer.bytes;
      if (excess <= 0) return;
    }
  };

  const pruneReleasedSnapshots = (): void => {
    const released = Array.from(terminals.values()).filter((terminal) => terminal.released);
    let retainedBytes = released.reduce((total, terminal) => total + terminal.buffer.bytes, 0);
    while (released.length > MAX_RETAINED_TERMINALS || retainedBytes > MAX_RETAINED_OUTPUT_BYTES) {
      const terminal = released.shift();
      if (terminal === undefined) break;
      retainedBytes -= terminal.buffer.bytes;
      terminals.delete(terminal.terminalId);
    }
  };

  const requireTerminal = (
    sessionId: string,
    terminalId: string,
    operation: string,
  ): Effect.Effect<ManagedAcpTerminal, EffectAcpErrors.AcpRequestError> => {
    const terminal = terminals.get(terminalId);
    if (terminal === undefined || terminal.sessionId !== sessionId || terminal.released) {
      return Effect.fail(
        terminalRequestError(`ACP ${operation} received an unknown terminal ID '${terminalId}'.`),
      );
    }
    return Effect.succeed(terminal);
  };

  const create: AcpClientTerminals["create"] = (request) =>
    creationPermit.withPermit(
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const liveCount = Array.from(terminals.values()).filter(
            (terminal) => !terminal.released && terminal.exitStatus === undefined,
          ).length;
          const unreleasedCount = Array.from(terminals.values()).filter(
            (terminal) => !terminal.released,
          ).length;
          if (unreleasedCount >= MAX_UNRELEASED_TERMINALS) {
            return yield* terminalRequestError(
              `ACP terminal/create exceeded the limit of ${MAX_UNRELEASED_TERMINALS} unreleased terminals.`,
            );
          }
          if (liveCount >= MAX_LIVE_TERMINALS) {
            return yield* terminalRequestError(
              `ACP terminal/create exceeded the limit of ${MAX_LIVE_TERMINALS} concurrent terminals.`,
            );
          }
          // Thread credentials and identity must come from the requesting session.
          const inheritedEnvironment = Object.fromEntries(
            Object.entries(options.environment ?? (yield* HostProcess.Environment)).filter(
              ([name]) =>
                !name.toUpperCase().startsWith("T3_ACP_MCP_") &&
                name.toUpperCase() !== "T3CODE_THREAD_ID",
            ),
          );
          const sessionEnvironment = options.environmentForSession?.(request.sessionId);
          const environment = mergeEnvironments(isWindows, [
            Object.entries(inheritedEnvironment),
            Object.entries(sessionEnvironment ?? {}),
            (request.env ?? []).map((variable) => [variable.name, variable.value] as const),
          ]);
          const cwd = request.cwd ?? options.defaultCwd;
          const baseCommand = materializeAcpTerminalCommand({
            request,
            shellCommands: options.shellCommands,
            isWindows,
          });
          const effectiveThreadId = threadId ?? sessionEnvironment?.T3CODE_THREAD_ID;
          const resolvedTargetCommand =
            baseCommand.shell || isWindows
              ? undefined
              : resolveLinuxCgroupTargetCommand(baseCommand.command, cwd, environment);
          const explicitCgroupController =
            platform === "linux" &&
            options.linuxCgroupController !== undefined &&
            options.linuxCgroupController !== null
              ? options.linuxCgroupController
              : undefined;
          // Prefer AgentScope first on Linux so terminals join ownedSlice (Force Stop,
          // stopLeftoverAgents, and memory/task limits); fall back to a delegated cgroup
          // leaf when systemd user scopes are unavailable (or when injected explicitly).
          const scopedLaunch =
            baseCommand.shell || explicitCgroupController !== undefined
              ? undefined
              : yield* agentScope.wrap({
                  command: resolvedTargetCommand ?? baseCommand.command,
                  args: baseCommand.args,
                  name: "acp-terminal",
                  ...(effectiveThreadId === undefined ? {} : { threadId: effectiveThreadId }),
                  env: environment,
                  auxiliary: true,
                });
          const scopeUnit = scopedLaunch?.unit;
          const linuxCgroupLease: AcpLinuxCgroupLease | undefined =
            platform === "linux" &&
            scopeUnit === undefined &&
            options.linuxCgroupController !== null &&
            resolvedTargetCommand !== undefined
              ? yield* Effect.sync(() => {
                  try {
                    return (explicitCgroupController ?? makeLinuxCgroupController()).create();
                  } catch {
                    return undefined;
                  }
                })
              : undefined;
          const launchCommand =
            linuxCgroupLease !== undefined && resolvedTargetCommand !== undefined
              ? {
                  ...wrapCommandForLinuxCgroup(
                    linuxCgroupLease,
                    resolvedTargetCommand,
                    baseCommand.args,
                  ),
                  shell: false,
                }
              : scopedLaunch !== undefined
                ? { command: scopedLaunch.command, args: scopedLaunch.args, shell: false }
                : baseCommand;

          const terminalScope = yield* Scope.make();
          const cleanupFailedSpawn = Effect.gen(function* () {
            yield* Scope.close(terminalScope, Exit.void).pipe(Effect.ignore);
            if (linuxCgroupLease !== undefined) {
              yield* terminateLinuxCgroupLease(linuxCgroupLease).pipe(
                Effect.ignoreCause({ log: true }),
              );
            }
            if (scopeUnit !== undefined) {
              yield* agentScope.terminateUnit(scopeUnit).pipe(Effect.ignoreCause({ log: true }));
            }
          });
          const child = yield* restore(
            spawner
              .spawn(
                ChildProcess.make(launchCommand.command, launchCommand.args, {
                  cwd,
                  env: environment,
                  shell: launchCommand.shell,
                  detached: !isWindows,
                  forceKillAfter,
                }),
              )
              .pipe(
                Effect.provideService(Scope.Scope, terminalScope),
                Effect.mapError((cause) =>
                  terminalRequestError(
                    `Could not start terminal command '${request.command}'.`,
                    cause,
                  ),
                ),
              ),
          ).pipe(
            Effect.onExit((exit) => (Exit.isFailure(exit) ? cleanupFailedSpawn : Effect.void)),
          );

          const terminalId = `t3-term-${nextTerminalNumber}`;
          nextTerminalNumber += 1;

          const rootPid = Number(child.pid);
          const hasTrackablePid =
            Number.isSafeInteger(rootPid) && rootPid > 1 && rootPid !== process.pid;
          const posixOwnershipLedger = new Map<string, AcpOwnedPosixProcess>();
          const posixOwnershipFrontier = new Map<number, AcpOwnedPosixProcess>();
          const posixOwnershipChildQueues = new Map<number, Array<number>>();
          const posixOwnershipRoot: AcpPosixOwnershipRoot = { value: undefined };
          const posixController: AcpPosixProcessTreeController | undefined =
            !isWindows && linuxCgroupLease === undefined && hasTrackablePid
              ? (() => {
                  if (options.posixProcessTreeController !== undefined) {
                    return options.posixProcessTreeController;
                  }
                  try {
                    return makePosixProcessTreeController(platform);
                  } catch {
                    return undefined;
                  }
                })()
              : undefined;

          let observerFiber: Fiber.Fiber<never> | undefined;
          if (posixController !== undefined) {
            yield* Effect.sync(() => {
              try {
                observePosixOwnershipLedger({
                  childQueues: posixOwnershipChildQueues,
                  controller: posixController,
                  frontier: posixOwnershipFrontier,
                  ledger: posixOwnershipLedger,
                  root: posixOwnershipRoot,
                  rootPid,
                });
              } catch {}
            });
            if (posixOwnershipRoot.value !== undefined) {
              observerFiber = yield* observePosixOwnershipLedgerContinuously({
                childQueues: posixOwnershipChildQueues,
                controller: posixController,
                frontier: posixOwnershipFrontier,
                ledger: posixOwnershipLedger,
                root: posixOwnershipRoot,
                rootPid,
              }).pipe(Effect.forkIn(terminalScope));
            }
          }

          const interruptObserver = Effect.gen(function* () {
            const fiber = observerFiber;
            if (fiber === undefined) return;
            observerFiber = undefined;
            yield* Fiber.interrupt(fiber).pipe(Effect.ignore);
          });

          const snapshotPosixLedger =
            posixController === undefined || linuxCgroupLease !== undefined
              ? Effect.void
              : Effect.gen(function* () {
                  if (posixTreeTerminated || posixOwnershipRoot.value === undefined) return;
                  const table =
                    posixController.snapshotAsync !== undefined
                      ? yield* posixController.snapshotAsync().pipe(Effect.option)
                      : Option.none();
                  yield* Effect.sync(() => {
                    try {
                      capturePosixOwnershipLedger({
                        controller: posixController,
                        ledger: posixOwnershipLedger,
                        root: posixOwnershipRoot,
                        rootPid,
                        ...(Option.isSome(table) ? { table: table.value } : {}),
                      });
                    } catch {}
                  });
                });

          const containmentSemaphore = yield* Semaphore.make(1);
          let cgroupTerminated = false;
          let posixTreeTerminated = false;
          let scopeUnitTerminated = false;
          let windowsTreeTerminated = false;

          const terminateWindowsTreeIfRunning =
            !isWindows ||
            !hasTrackablePid ||
            (options.windowsProcessTreeTerminator === undefined &&
              HostProcess.Platform.defaultValue() !== "win32")
              ? Effect.void
              : Effect.gen(function* () {
                  if (windowsTreeTerminated) return;
                  const running = yield* child.isRunning.pipe(Effect.orElseSucceed(() => false));
                  windowsTreeTerminated = true;
                  if (!running) return;
                  const terminateWindows =
                    options.windowsProcessTreeTerminator ??
                    ((pid: number) =>
                      terminateWindowsProcessTreeWithTaskkill(pid).pipe(
                        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                      ));
                  yield* terminateWindows(rootPid).pipe(Effect.ignore);
                });

          const terminateContainment = (mode: "kill" | "dispose") =>
            containmentSemaphore.withPermit(
              Effect.uninterruptible(
                Effect.gen(function* () {
                  yield* terminateWindowsTreeIfRunning;
                  yield* snapshotPosixLedger;
                  yield* interruptObserver;

                  if (linuxCgroupLease !== undefined && !cgroupTerminated) {
                    const exit = yield* Effect.exit(terminateLinuxCgroupLease(linuxCgroupLease));
                    if (Exit.isSuccess(exit)) {
                      cgroupTerminated = true;
                    } else {
                      yield* Effect.logWarning("Failed to terminate ACP client terminal cgroup", {
                        terminalId,
                        path: linuxCgroupLease.path,
                        cause: Cause.pretty(exit.cause),
                      });
                    }
                  }

                  if (
                    posixController !== undefined &&
                    linuxCgroupLease === undefined &&
                    posixOwnershipRoot.value !== undefined &&
                    !posixTreeTerminated
                  ) {
                    const exit = yield* Effect.exit(
                      terminatePosixOwnedProcessTree({
                        controller: posixController,
                        grace: 0,
                        ledger: posixOwnershipLedger,
                        root: posixOwnershipRoot,
                        rootPid,
                      }),
                    );
                    if (Exit.isSuccess(exit)) {
                      posixTreeTerminated = true;
                    } else {
                      yield* Effect.logWarning(
                        "Failed to terminate ACP client terminal process tree",
                        {
                          terminalId,
                          rootPid,
                          cause: Cause.pretty(exit.cause),
                        },
                      );
                    }
                  }

                  if (scopeUnit !== undefined && !scopeUnitTerminated) {
                    const exit = yield* Effect.exit(agentScope.terminateUnit(scopeUnit));
                    if (Exit.isSuccess(exit)) {
                      scopeUnitTerminated = true;
                    } else {
                      yield* Effect.logWarning(
                        "Failed to terminate ACP client terminal scope unit",
                        {
                          terminalId,
                          unit: scopeUnit,
                          cause: Cause.pretty(exit.cause),
                        },
                      );
                      if (mode === "dispose") {
                        yield* agentScope.stop(scopeUnit).pipe(Effect.ignoreCause({ log: true }));
                      }
                    }
                  }
                }),
              ),
            );

          const stopObserverIfIdle = Effect.gen(function* () {
            if (posixController === undefined || observerFiber === undefined) return;
            if (posixController.refreshAsync !== undefined) {
              yield* posixController.refreshAsync().pipe(Effect.ignore);
            }
            yield* Effect.sync(() => {
              try {
                observePosixOwnershipLedger({
                  childQueues: posixOwnershipChildQueues,
                  controller: posixController,
                  frontier: posixOwnershipFrontier,
                  ledger: posixOwnershipLedger,
                  root: posixOwnershipRoot,
                  rootPid,
                });
              } catch {}
            });
            if (posixOwnershipFrontier.size === 0) {
              yield* interruptObserver;
            }
          });

          const record: ManagedAcpTerminal = {
            terminalId,
            sessionId: request.sessionId,
            commandLine: [request.command, ...(request.args ?? [])].join(" "),
            buffer: {
              chunks: [],
              bytes: 0,
              truncated: false,
              limit: Math.min(
                request.outputByteLimit ?? DEFAULT_OUTPUT_BYTE_LIMIT,
                MAX_OUTPUT_BYTE_LIMIT,
              ),
            },
            exit: yield* Deferred.make<EffectAcpSchema.WaitForTerminalExitResponse>(),
            // Snapshot live descendants (and on Windows kill live tree before releasing the handle)
            // so freshly forked setsid children cannot reparent unseen, then reap containment layers.
            kill: snapshotPosixLedger.pipe(
              Effect.andThen(
                containmentSemaphore.withPermit(
                  Effect.uninterruptible(terminateWindowsTreeIfRunning),
                ),
              ),
              Effect.andThen(child.kill({ forceKillAfter }).pipe(Effect.ignore)),
              Effect.andThen(terminateContainment("kill")),
            ),
            // Terminate containment BEFORE closing terminalScope so observerFiber stays alive
            // through the final snapshot and SIGTERM-ignoring leaders do not stall release/disposeAll.
            dispose: terminateContainment("dispose").pipe(
              Effect.andThen(Scope.close(terminalScope, Exit.void).pipe(Effect.ignore)),
            ),
            exitStatus: undefined,
            released: false,
          };
          terminals.set(terminalId, record);

          const pump = (stream: typeof child.stdout) =>
            stream.pipe(
              // Keep each pipe's UTF-8 decoder independent and preserve literal BOMs.
              Stream.pipeThroughChannel(Channel.decodeText("utf-8", { ignoreBOM: true })),
              Stream.encodeText,
              Stream.runForEach((chunk) =>
                Effect.sync(() => {
                  appendOutput(record.buffer, chunk);
                  enforceTotalOutputLimit();
                }),
              ),
              Effect.ignore,
            );
          const stdoutPump = yield* Effect.forkDetach(pump(child.stdout));
          const stderrPump = yield* Effect.forkDetach(pump(child.stderr));
          yield* Effect.forkDetach(
            child.exitCode.pipe(
              Effect.match({
                onFailure: (error) => ({ exitCode: null, signal: terminalExitSignal(error) }),
                onSuccess: (code) => ({ exitCode: Number(code), signal: null }),
              }),
              Effect.tap(() => stopObserverIfIdle),
              Effect.flatMap((status) =>
                Effect.all([Fiber.join(stdoutPump), Fiber.join(stderrPump)], {
                  discard: true,
                }).pipe(
                  // A background child can inherit the pipes and hold them open indefinitely.
                  Effect.timeoutOption(OUTPUT_DRAIN_GRACE),
                  Effect.andThen(
                    Effect.sync(() => {
                      record.exitStatus = status;
                    }),
                  ),
                  Effect.andThen(Deferred.succeed(record.exit, status)),
                ),
              ),
            ),
          );
          return { terminalId };
        }),
      ),
    );

  const output: AcpClientTerminals["output"] = (request) =>
    requireTerminal(request.sessionId, request.terminalId, "terminal/output").pipe(
      Effect.map((terminal) => ({
        output: readOutput(terminal.buffer),
        truncated: terminal.buffer.truncated,
        ...(terminal.exitStatus === undefined ? {} : { exitStatus: terminal.exitStatus }),
      })),
    );

  const waitForExit: AcpClientTerminals["waitForExit"] = (request) =>
    requireTerminal(request.sessionId, request.terminalId, "terminal/wait_for_exit").pipe(
      Effect.flatMap((terminal) => Deferred.await(terminal.exit)),
    );

  const kill: AcpClientTerminals["kill"] = (request) =>
    requireTerminal(request.sessionId, request.terminalId, "terminal/kill").pipe(
      Effect.flatMap((terminal) => terminal.kill),
      Effect.as({}),
    );

  const release: AcpClientTerminals["release"] = (request) =>
    Effect.uninterruptible(
      requireTerminal(request.sessionId, request.terminalId, "terminal/release").pipe(
        Effect.flatMap((terminal) =>
          Effect.sync(() => {
            terminal.released = true;
          }).pipe(
            Effect.andThen(terminal.dispose),
            Effect.andThen(Deferred.await(terminal.exit)),
            Effect.andThen(Effect.sync(pruneReleasedSnapshots)),
          ),
        ),
        Effect.as({}),
      ),
    );

  const readOutputSnapshot: AcpClientTerminals["readOutputSnapshot"] = (sessionId, terminalId) => {
    const terminal = terminals.get(terminalId);
    if (terminal === undefined || terminal.sessionId !== sessionId) return undefined;
    return {
      output: readOutput(terminal.buffer),
      truncated: terminal.buffer.truncated,
      exitStatus: terminal.exitStatus,
    };
  };

  const readCommandLine: AcpClientTerminals["readCommandLine"] = (terminalId) =>
    terminals.get(terminalId)?.commandLine;

  const disposeAll: AcpClientTerminals["disposeAll"] = Effect.uninterruptible(
    Effect.suspend(() =>
      Effect.forEach(
        Array.from(terminals.values()),
        (terminal) =>
          Effect.sync(() => {
            terminal.released = true;
          }).pipe(Effect.andThen(terminal.dispose), Effect.andThen(Deferred.await(terminal.exit))),
        { concurrency: "unbounded", discard: true },
      ).pipe(Effect.andThen(Effect.sync(pruneReleasedSnapshots))),
    ),
  );

  const logged =
    <Request, Response>(
      operation: string,
      handler: (request: Request) => Effect.Effect<Response, EffectAcpErrors.AcpRequestError>,
    ) =>
    (request: Request) =>
      handler(request).pipe(
        Effect.tapError((error) =>
          Effect.logWarning("ACP client terminal operation failed", {
            operation,
            detail: error.message,
          }),
        ),
      );

  return {
    create: logged("terminal/create", create),
    output: logged("terminal/output", output),
    waitForExit: logged("terminal/wait_for_exit", waitForExit),
    kill: logged("terminal/kill", kill),
    release: logged("terminal/release", release),
    readOutputSnapshot,
    readCommandLine,
    disposeAll,
  };
});

export function resolveEmbeddedTerminalContent(
  notification: EffectAcpSchema.SessionNotification,
  readOutputSnapshot: AcpClientTerminals["readOutputSnapshot"],
): EffectAcpSchema.SessionNotification {
  const update = notification.update;
  if (update.sessionUpdate !== "tool_call" && update.sessionUpdate !== "tool_call_update") {
    return notification;
  }
  const content = update.content;
  if (!content || !content.some((entry) => entry.type === "terminal")) {
    return notification;
  }
  const mapped = content.map((entry): EffectAcpSchema.ToolCallContent => {
    if (entry.type !== "terminal") return entry;
    const snapshot = readOutputSnapshot(notification.sessionId, entry.terminalId);
    return {
      type: "content",
      content: {
        type: "text",
        text: snapshot === undefined ? `[terminal ${entry.terminalId}]` : snapshot.output,
      },
    };
  });
  return { ...notification, update: { ...update, content: mapped } };
}
