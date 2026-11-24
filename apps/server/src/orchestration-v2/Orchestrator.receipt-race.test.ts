import { vi } from "vite-plus/test";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
// Holds model-selection planning until both racing commands have read no receipt.
let capabilityGate: Effect.Effect<void> | undefined;
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () =>
    (capabilityGate ?? Effect.void).pipe(Effect.as(CodexProviderCapabilitiesV2)),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata controls"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase)),
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "receipt-race" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

it.effect("a command id reused concurrently for two threads succeeds for only one", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadIds = [ThreadId.make("thread:race-a"), ThreadId.make("thread:race-b")];
    for (const threadId of threadIds) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${threadId}`),
        threadId,
        projectId: ProjectId.make("project:receipt-race"),
        title: threadId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
    }

    const arrived = yield* Ref.make(0);
    const bothArrived = yield* Deferred.make<void>();
    capabilityGate = Effect.gen(function* () {
      if ((yield* Ref.updateAndGet(arrived, (count) => count + 1)) === 2) {
        yield* Deferred.succeed(bothArrived, undefined);
      }
      yield* Deferred.await(bothArrived);
    });
    const setModel = (threadId: ThreadId) =>
      orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("command:shared"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-6" },
      });
    const exits = yield* Effect.forEach(threadIds, (threadId) => Effect.exit(setModel(threadId)), {
      concurrency: "unbounded",
    }).pipe(Effect.ensuring(Effect.sync(() => (capabilityGate = undefined))));

    const winner = exits.findIndex(Exit.isSuccess);
    const loser = exits.findIndex(Exit.isFailure);
    assert.isAtLeast(winner, 0);
    assert.isAtLeast(loser, 0);
    const winnerExit = exits[winner]!;
    const loserExit = exits[loser]!;
    assert.isTrue(Exit.isSuccess(winnerExit) && Exit.isFailure(loserExit));
    if (Exit.isFailure(loserExit)) {
      const error = Cause.findErrorOption(loserExit.cause);
      assert.isTrue(error._tag === "Some");
      if (error._tag === "Some") {
        assert.equal(error.value._tag, "OrchestratorCommandIdConflictError");
        if (error.value._tag === "OrchestratorCommandIdConflictError") {
          assert.equal(error.value.receiptThreadId, threadIds[winner]);
          assert.equal(error.value.commandThreadId, threadIds[loser]);
          assert.equal(error.value.receiptCommandType, "thread.model-selection.set");
          assert.equal(
            error.value.message,
            `Command command:shared was already handled for thread ${threadIds[winner]} and cannot be replayed for ${threadIds[loser]}.`,
          );
        }
      }
    }

    const models = yield* Effect.forEach(threadIds, (threadId) =>
      projections.getThreadShell(threadId).pipe(Effect.map((shell) => shell?.modelSelection.model)),
    );
    assert.equal(models[winner], "gpt-6");
    assert.equal(models[loser], "gpt-5.1-codex");

    const noOpArrived = yield* Ref.make(0);
    const bothNoOpsArrived = yield* Deferred.make<void>();
    const getThreadRecords = projections.getThreadRecords;
    using _records = vi.spyOn(projections, "getThreadRecords").mockImplementation((...args) =>
      Effect.gen(function* () {
        if ((yield* Ref.updateAndGet(noOpArrived, (count) => count + 1)) === 2) {
          yield* Deferred.succeed(bothNoOpsArrived, undefined);
        }
        yield* Deferred.await(bothNoOpsArrived);
        return yield* getThreadRecords(...args);
      }),
    );
    const closeShare = (threadId: ThreadId) =>
      orchestrator.dispatch({
        type: "thread.guest-share.close",
        commandId: CommandId.make("command:shared-noop"),
        threadId,
      });
    const noOpExits = yield* Effect.forEach(
      threadIds,
      (threadId) => Effect.exit(closeShare(threadId)),
      {
        concurrency: "unbounded",
      },
    );
    const noOpWinner = noOpExits.findIndex(Exit.isSuccess);
    const noOpLoser = noOpExits.findIndex(Exit.isFailure);
    assert.isAtLeast(noOpWinner, 0);
    assert.isAtLeast(noOpLoser, 0);
    const noOpFailure = noOpExits[noOpLoser]!;
    if (Exit.isFailure(noOpFailure)) {
      const error = Cause.findErrorOption(noOpFailure.cause);
      assert.isTrue(error._tag === "Some");
      if (error._tag === "Some") {
        assert.equal(error.value._tag, "OrchestratorCommandIdConflictError");
      }
    }
    const noOpReplay = yield* closeShare(threadIds[noOpWinner]!);
    const noOpSuccess = noOpExits[noOpWinner]!;
    if (Exit.isSuccess(noOpSuccess)) {
      assert.equal(noOpReplay.sequence, noOpSuccess.value.sequence);
      assert.deepEqual(noOpReplay.storedEvents, []);
    }

    // The winner's own thread still replays its receipt.
    const replay = yield* setModel(threadIds[winner]!);
    if (Exit.isSuccess(winnerExit)) {
      assert.equal(replay.sequence, winnerExit.value.sequence);
      assert.isAbove(replay.storedEvents.length, 0);
      assert.deepEqual(
        replay.storedEvents.map((stored) => stored.sequence),
        winnerExit.value.storedEvents.map((stored) => stored.sequence),
      );
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "rejects commandId reuse across different command types on the same thread in replay and concurrent commit",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const threadId = ThreadId.make("thread:same-thread-type-conflict");

      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command:type-conflict:create"),
        threadId,
        projectId: ProjectId.make("project:receipt-race"),
        title: "Type conflict thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });

      // 1. Sequential replayCommandReceipt rejects cross-command-type reuse on the same thread.
      const deferCommandId = CommandId.make("command:type-conflict:defer-logout");
      const deferDispatch = {
        type: "message.dispatch" as const,
        commandId: deferCommandId,
        threadId,
        messageId: MessageId.make("message:type-conflict:defer-logout"),
        text: "/logout",
        attachments: [],
        dispatchMode: {
          type: "defer_start" as const,
          workspaceStrategy: { type: "root" as const },
        },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      const original = yield* orchestrator.dispatch(deferDispatch);

      const sequentialConflict = yield* orchestrator
        .dispatch({
          type: "thread.create",
          commandId: deferCommandId,
          threadId,
          projectId: ProjectId.make("project:receipt-race"),
          title: "Replay with wrong command type",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        })
        .pipe(Effect.flip);

      assert.equal(sequentialConflict._tag, "OrchestratorCommandIdConflictError");
      if (sequentialConflict._tag === "OrchestratorCommandIdConflictError") {
        assert.equal(sequentialConflict.commandId, deferCommandId);
        assert.equal(sequentialConflict.commandType, "thread.create");
        assert.equal(sequentialConflict.receiptCommandType, "message.dispatch");
        assert.equal(sequentialConflict.receiptThreadId, threadId);
        assert.equal(sequentialConflict.commandThreadId, threadId);
        assert.equal(
          sequentialConflict.message,
          `Command ${deferCommandId} was already handled as message.dispatch on thread ${threadId} and cannot be replayed as thread.create.`,
        );
      }

      // Same commandId with the matching commandType still replays cleanly.
      const matchingReplay = yield* orchestrator.dispatch(deferDispatch);
      assert.equal(matchingReplay.sequence, original.sequence);
      assert.isTrue(matchingReplay.replayed);

      // 2. Concurrent commit race in dispatchWithReceiptEffect rejects cross-command-type conflict on the same thread.
      const raceCommandId = CommandId.make("command:type-conflict:concurrent-commit");
      capabilityGate = Effect.gen(function* () {
        yield* receipts
          .insertIfAbsent({
            commandId: raceCommandId,
            threadId,
            commandType: "message.dispatch",
            acceptedAt: yield* DateTime.now,
            resultSequence: original.sequence,
            status: "accepted",
            error: null,
          })
          .pipe(Effect.orDie);
      });
      const concurrentConflict = yield* orchestrator
        .dispatch({
          type: "thread.model-selection.set",
          commandId: raceCommandId,
          threadId,
          modelSelection: { ...modelSelection, model: "gpt-6" },
        })
        .pipe(Effect.ensuring(Effect.sync(() => (capabilityGate = undefined))), Effect.flip);

      assert.equal(concurrentConflict._tag, "OrchestratorCommandIdConflictError");
      if (concurrentConflict._tag === "OrchestratorCommandIdConflictError") {
        assert.equal(concurrentConflict.commandId, raceCommandId);
        assert.equal(concurrentConflict.commandType, "thread.model-selection.set");
        assert.equal(concurrentConflict.receiptCommandType, "message.dispatch");
        assert.equal(concurrentConflict.receiptThreadId, threadId);
        assert.equal(concurrentConflict.commandThreadId, threadId);
        assert.equal(
          concurrentConflict.message,
          `Command ${raceCommandId} was already handled as message.dispatch on thread ${threadId} and cannot be replayed as thread.model-selection.set.`,
        );
      }

      // The conflicting concurrent command must not have mutated thread state.
      const shell = yield* projections.getThreadShell(threadId);
      assert.equal(shell?.modelSelection.model, "gpt-5.1-codex");

      // 3. Zero-event no-op concurrent race also rejects cross-command-type conflict via replayCommandReceipt.
      const noOpRaceCommandId = CommandId.make("command:type-conflict:concurrent-noop");
      const getThreadRecords = projections.getThreadRecords;
      let injectedNoOpReceipt = false;
      using _records = vi.spyOn(projections, "getThreadRecords").mockImplementation((...args) =>
        Effect.gen(function* () {
          if (!injectedNoOpReceipt) {
            injectedNoOpReceipt = true;
            yield* receipts
              .insertIfAbsent({
                commandId: noOpRaceCommandId,
                threadId,
                commandType: "message.dispatch",
                acceptedAt: yield* DateTime.now,
                resultSequence: original.sequence,
                status: "accepted",
                error: null,
              })
              .pipe(Effect.orDie);
          }
          return yield* getThreadRecords(...args);
        }),
      );
      const noOpConflict = yield* orchestrator
        .dispatch({
          type: "thread.guest-share.close",
          commandId: noOpRaceCommandId,
          threadId,
        })
        .pipe(Effect.flip);

      assert.equal(noOpConflict._tag, "OrchestratorCommandIdConflictError");
      if (noOpConflict._tag === "OrchestratorCommandIdConflictError") {
        assert.equal(noOpConflict.commandId, noOpRaceCommandId);
        assert.equal(noOpConflict.commandType, "thread.guest-share.close");
        assert.equal(noOpConflict.receiptCommandType, "message.dispatch");
        assert.equal(noOpConflict.receiptThreadId, threadId);
        assert.equal(noOpConflict.commandThreadId, threadId);
      }
    }).pipe(Effect.provide(layerTest)),
);
