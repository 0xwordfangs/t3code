import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

const stopEarlierBackgroundWork = ({
  wakeAfterStop = false,
  failedStart = false,
  stopWithQueue,
  olderStart = false,
  backgroundWorkAfterStop,
  stalledRun,
  workflowMember = false,
}: {
  readonly wakeAfterStop?: boolean;
  readonly failedStart?: boolean;
  readonly backgroundWorkAfterStop?: "running" | "unknown";
  readonly stopWithQueue?: "thread.stop" | "run.interrupt";
  readonly olderStart?: boolean;
  readonly workflowMember?: boolean;
  readonly stalledRun?: "missing-session-terminal" | "returned-interrupt" | "superseded-attempt";
}) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("background-work-stop");
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const interrupts: ProviderAdapter.ProviderAdapterV2InterruptInput[] = [];
    const interruptCalls = yield* Queue.unbounded<void>();
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ...(failedStart
              ? {
                  hasPendingBackgroundWorkForThread: (providerThread) =>
                    Effect.gen(function* () {
                      if (
                        !interrupts.some((input) => input.providerThread.id === providerThread.id)
                      ) {
                        return false;
                      }
                      if (backgroundWorkAfterStop === "unknown") {
                        return yield* Effect.die("Background work probe failed");
                      }
                      return backgroundWorkAfterStop === "running";
                    }),
                }
              : {}),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                started.push(turn);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: () => Effect.die("unused"),
            interruptTurn: (interrupt) =>
              Effect.sync(() => {
                interrupts.push(interrupt);
              }).pipe(Effect.andThen(Queue.offer(interruptCalls, undefined))),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:background-work-stop");
      // This provider never confirms Stop, so Stop gives up waiting at its bound.
      const drainUnconfirmedStop = Effect.gen(function* () {
        const drained = yield* Effect.forkChild(worker.drain());
        yield* Queue.take(interruptCalls);
        yield* TestClock.adjust("2 seconds");
        yield* Fiber.join(drained);
      });
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:background-work-stop"),
        title: "Background work stop",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("start-dev-server"),
        threadId,
        messageId: MessageId.make("message:start-dev-server"),
        text: "Start the dev server",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(running);
      if (wakeAfterStop) {
        const send = (id: string, provider = false) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(id),
            threadId,
            messageId: MessageId.make(`message:${id}`),
            text: id,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: provider ? "agent" : "user",
            creationSource: provider ? "provider" : "web",
          });
        yield* send("follow-up");
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop"),
          threadId,
        });
        // Stop is committed, but its provider interrupt has not run yet.
        yield* send("Background task completed.", true);
        const [stopped, followUp, wake] = (yield* orchestrator.getThreadProjection(threadId)).runs;
        assert.equal(stopped?.status, "running");
        assert.isTrue(followUp?.queueHeld);
        assert.isTrue(wake?.queueHeld);

        yield* drainUnconfirmedStop;
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs.map((run) => run.status),
          ["interrupted", "queued", "queued"],
        );
        assert.isTrue(after.runs.find((run) => run.id === followUp?.id)?.queueHeld);
        assert.isTrue(after.runs.find((run) => run.id === wake?.id)?.queueHeld);
        assert.lengthOf(started, 1);
        return;
      }
      const first = started[0]!;
      const codexTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const devServerId = TurnItemId.make("turn-item:dev-server");
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("dev-server"),
            type: "turn-item.updated",
            threadId,
            runId: first.runId,
            occurredAt: now,
            payload: {
              id: devServerId,
              threadId,
              runId: first.runId,
              nodeId: first.rootNodeId,
              providerThreadId: codexTurn.providerThreadId,
              providerTurnId: codexTurn.id,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 100,
              status: "running",
              title: null,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              type: "command_execution",
              input: "vp run dev --share",
            },
          },
        ],
      });
      if (stalledRun !== undefined) {
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const node = before.nodes.find((candidate) => candidate.id === run.rootNodeId)!;
        const attempt = before.attempts[0]!;
        if (stalledRun === "missing-session-terminal") {
          const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
          const failed = yield* watch(
            (event) => event.type === "run.updated" && event.payload.status === "failed",
          );
          yield* sessions.release({
            providerSessionId: first.providerThread.providerSessionId!,
            reason: "runtime_error",
          });
          yield* Fiber.join(failed);
          yield* sink.write({
            events: [
              {
                id: EventId.make("stale-run"),
                type: "run.updated",
                threadId,
                occurredAt: now,
                payload: run,
              },
              {
                id: EventId.make("stale-node"),
                type: "node.updated",
                threadId,
                occurredAt: now,
                payload: node,
              },
              {
                id: EventId.make("stale-attempt"),
                type: "run-attempt.updated",
                threadId,
                occurredAt: now,
                payload: attempt,
              },
            ],
          });
        }
        const messageId = MessageId.make("partial-output");
        const item = before.turnItems.find((candidate) => candidate.id === devServerId)!;
        assert.ok(item.type === "command_execution");
        const terminalProviderTurn = stalledRun.endsWith("-terminal");
        if (terminalProviderTurn) {
          yield* sink.write({
            events: [
              {
                id: EventId.make("terminal-provider-turn"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: now,
                payload: { ...codexTurn, status: "completed", completedAt: now },
              },
              ...(stalledRun === "missing-session-terminal"
                ? [
                    {
                      id: EventId.make("completed-dev-server"),
                      type: "turn-item.updated" as const,
                      threadId,
                      occurredAt: now,
                      payload: { ...item, status: "completed" as const, completedAt: now },
                    },
                  ]
                : []),
            ],
          });
        }
        yield* sink.write({
          events: [
            {
              id: EventId.make("partial-message"),
              type: "message.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: {
                id: messageId,
                threadId,
                runId: run.id,
                nodeId: node.id,
                role: "assistant",
                text: "Partial output",
                attachments: [],
                streaming: true,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: now,
                updatedAt: now,
              },
            },
            {
              id: EventId.make("partial-item"),
              type: "turn-item.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: {
                ...item,
                providerThreadId: codexTurn.providerThreadId,
                providerTurnId: codexTurn.id,
                id: TurnItemId.make("partial-output"),
                type: "assistant_message",
                messageId,
                text: "Partial output",
                streaming: true,
              },
            },
          ],
        });
        // Shaped like CodexAdapterV2's buildApprovalRequestArtifacts output.
        const approvalRequestId = RuntimeRequestId.make("request:stalled-approval");
        const approvalNodeId = NodeId.make("node:stalled-approval");
        const approvalItemId = TurnItemId.make("turn-item:stalled-approval");
        if (stalledRun === "returned-interrupt") {
          yield* sink.write({
            events: [
              {
                id: EventId.make("stalled-approval-node"),
                type: "node.updated",
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: {
                  ...node,
                  id: approvalNodeId,
                  parentNodeId: node.id,
                  kind: "approval_request",
                  status: "waiting",
                  countsForRun: false,
                  runtimeRequestId: approvalRequestId,
                },
              },
              {
                id: EventId.make("stalled-approval-request"),
                type: "runtime-request.updated",
                threadId,
                occurredAt: now,
                payload: {
                  id: approvalRequestId,
                  nodeId: approvalNodeId,
                  providerTurnId: codexTurn.id,
                  nativeRequestRef: { driver, nativeId: "approval", strength: "strong" },
                  kind: "command",
                  status: "pending",
                  responseCapability: {
                    type: "live",
                    providerSessionId: first.providerThread.providerSessionId!,
                  },
                  createdAt: now,
                  resolvedAt: null,
                },
              },
              {
                id: EventId.make("stalled-approval-item"),
                type: "turn-item.updated",
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: {
                  id: approvalItemId,
                  threadId,
                  runId: run.id,
                  nodeId: approvalNodeId,
                  providerThreadId: codexTurn.providerThreadId,
                  providerTurnId: codexTurn.id,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 101,
                  status: "waiting",
                  title: null,
                  startedAt: now,
                  completedAt: null,
                  updatedAt: now,
                  type: "approval_request",
                  requestId: approvalRequestId,
                  requestKind: "command",
                  prompt: "rm -rf build",
                },
              },
            ],
          });
        }
        if (stalledRun === "superseded-attempt") {
          yield* sink.write({
            events: [
              {
                id: EventId.make("new-attempt"),
                type: "run.updated",
                threadId,
                occurredAt: now,
                payload: { ...run, activeAttemptId: RunAttemptId.make("new-attempt") },
              },
            ],
          });
        }
        yield* orchestrator.dispatch(
          stalledRun === "superseded-attempt"
            ? {
                type: "thread.background-work.settle",
                commandId: CommandId.make("late-settle"),
                threadId,
                providerThreadId: codexTurn.providerThreadId,
                providerTurnId: codexTurn.id,
              }
            : {
                type: "thread.stop",
                commandId: CommandId.make("stop-stalled-run"),
                threadId,
              },
        );
        yield* stalledRun === "returned-interrupt" ? drainUnconfirmedStop : worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        const interrupted = stalledRun !== "superseded-attempt";
        assert.equal(after.runs[0]?.status, interrupted ? "interrupted" : "running");
        assert.equal(after.attempts[0]?.status, interrupted ? "interrupted" : "running");
        assert.equal(
          after.providerTurns[0]?.status,
          terminalProviderTurn ? "completed" : interrupted ? "interrupted" : "running",
        );
        assert.equal(
          after.nodes.find((candidate) => candidate.id === node.id)?.status,
          interrupted ? "interrupted" : "running",
        );
        const output = after.messages.find((message) => message.id === messageId)!;
        assert.equal(output.streaming, !interrupted);
        assert.equal(output.text, "Partial output");
        assert.equal(
          after.turnItems.find((candidate) => candidate.id === devServerId)?.status,
          stalledRun === "missing-session-terminal"
            ? "completed"
            : interrupted
              ? "interrupted"
              : "running",
        );
        assert.equal(
          after.turnItems.find((candidate) => candidate.type === "assistant_message")?.status,
          interrupted ? "interrupted" : "running",
        );
        assert.equal(
          after.turnItems.filter((candidate) => candidate.type === "run_interrupt_result").length,
          interrupted ? 1 : 0,
        );
        assert.isEmpty(after.runs.filter((candidate) => candidate.status === "waiting"));
        if (stalledRun === "returned-interrupt") {
          assert.deepEqual(
            [
              after.runtimeRequests.find((candidate) => candidate.id === approvalRequestId)?.status,
              after.nodes.find((candidate) => candidate.id === approvalNodeId)?.status,
              after.turnItems.find((candidate) => candidate.id === approvalItemId)?.status,
            ],
            ["cancelled", "interrupted", "interrupted"],
          );
        }
        return;
      }
      const settled = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === first.runId &&
          event.payload.status === "waiting",
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...codexTurn, status: "completed", completedAt: now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: codexTurn.providerThreadId,
        providerTurnId: codexTurn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();

      const codexProviderThread = (yield* orchestrator.getThreadProjection(threadId))
        .providerThreads[0]!;
      const backgroundTasks = [
        { taskId: "sleep", description: "Wait before the recheck", kind: "command" as const },
      ];
      const otherBackgroundTasks = [{ ...backgroundTasks[0]!, taskId: "other-sleep" }];
      const subagentThread = (name: string) =>
        Effect.gen(function* () {
          const subagentThreadId = ThreadId.make(`thread:${name}`);
          const rootId = NodeId.make(`node:${name}-root`);
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create-${name}`),
            threadId: subagentThreadId,
            projectId: ProjectId.make("project:background-work-stop"),
            title: name,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "agent",
            creationSource: "provider",
          });
          yield* sink.write({
            events: [
              {
                id: EventId.make(`${name}-root`),
                type: "node.updated",
                threadId: subagentThreadId,
                occurredAt: now,
                payload: {
                  id: rootId,
                  threadId: subagentThreadId,
                  runId: null,
                  parentNodeId: null,
                  rootNodeId: rootId,
                  kind: "root_turn",
                  status: "running",
                  countsForRun: false,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  runtimeRequestId: null,
                  checkpointScopeId: null,
                  startedAt: now,
                  completedAt: null,
                },
              },
            ],
          });
          return { threadId: subagentThreadId, rootId };
        });
      const { threadId: reviewerThreadId, rootId: reviewerRootId } =
        yield* subagentThread("reviewer");
      const tester = yield* subagentThread("tester");
      const nestedSubagentId = NodeId.make("subagent:nested");
      const nestedItemId = TurnItemId.make("turn-item:nested-subagent");
      const nested = {
        threadId: reviewerThreadId,
        runId: null,
        origin: "provider_native",
        driver,
        providerInstanceId: instanceId,
        providerThreadId: null,
        childThreadId: tester.threadId,
        prompt: "Check the tests",
        title: null,
        status: "running",
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      } as const;
      yield* sink.write({
        events: [
          ...(workflowMember
            ? []
            : [
                {
                  id: EventId.make("nested-subagent-item"),
                  type: "turn-item.updated",
                  threadId: reviewerThreadId,
                  occurredAt: now,
                  payload: {
                    ...nested,
                    id: nestedItemId,
                    nodeId: nestedSubagentId,
                    providerTurnId: null,
                    nativeItemRef: null,
                    parentItemId: null,
                    ordinal: 1,
                    type: "subagent",
                    subagentId: nestedSubagentId,
                  },
                } as const,
              ]),
          {
            id: EventId.make("nested-subagent"),
            type: "subagent.updated",
            threadId: reviewerThreadId,
            occurredAt: now,
            payload: {
              ...nested,
              id: nestedSubagentId,
              parentNodeId: reviewerRootId,
              createdBy: "agent",
              nativeTaskRef: workflowMember
                ? { driver, nativeId: "workflow:member", strength: "strong" }
                : null,
              model: null,
            },
          },
        ],
      });
      if (workflowMember) {
        const memberRoot = (yield* orchestrator.getThreadProjection(tester.threadId)).nodes[0]!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("workflow-member-node"),
              type: "node.updated",
              threadId: reviewerThreadId,
              occurredAt: now,
              payload: {
                ...memberRoot,
                id: nestedSubagentId,
                threadId: reviewerThreadId,
                parentNodeId: reviewerRootId,
                rootNodeId: reviewerRootId,
                kind: "subagent",
              },
            },
          ],
        });
      }
      const settledRun = (input: {
        readonly ordinal: number;
        readonly providerThreadId: ProviderThreadId;
        readonly runningItem?: {
          readonly id: TurnItemId;
          readonly kind: "command" | "subagent";
        };
      }) => {
        const runId = RunId.make(`run:${input.ordinal}`);
        const attemptId = RunAttemptId.make(`attempt:${input.ordinal}`);
        const nodeId = NodeId.make(`node:${input.ordinal}`);
        const providerTurnId = ProviderTurnId.make(`provider-turn:${input.ordinal}`);
        const events: Array<OrchestrationV2DomainEvent> = [
          {
            id: EventId.make(`run:${input.ordinal}`),
            type: "run.created",
            threadId,
            occurredAt: now,
            payload: {
              id: runId,
              threadId,
              ordinal: input.ordinal,
              providerInstanceId: instanceId,
              modelSelection,
              providerThreadId: input.providerThreadId,
              userMessageId: MessageId.make(`message:${input.ordinal}`),
              rootNodeId: nodeId,
              activeAttemptId: attemptId,
              status: "completed",
              requestedAt: now,
              startedAt: now,
              completedAt: now,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
          {
            id: EventId.make(`node:${input.ordinal}`),
            type: "node.updated",
            threadId,
            runId,
            occurredAt: now,
            payload: {
              id: nodeId,
              threadId,
              runId,
              parentNodeId: null,
              rootNodeId: nodeId,
              kind: "root_turn",
              status: "completed",
              countsForRun: true,
              providerThreadId: input.providerThreadId,
              providerTurnId,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: now,
              completedAt: now,
            },
          },
          {
            id: EventId.make(`attempt:${input.ordinal}`),
            type: "run-attempt.created",
            threadId,
            occurredAt: now,
            payload: {
              id: attemptId,
              runId,
              attemptOrdinal: 1,
              rootNodeId: nodeId,
              providerInstanceId: instanceId,
              providerThreadId: input.providerThreadId,
              providerTurnId,
              reason: "initial",
              status: "completed",
              startedAt: now,
              completedAt: now,
            },
          },
          {
            id: EventId.make(`provider-turn:${input.ordinal}`),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: providerTurnId,
              providerThreadId: input.providerThreadId,
              nodeId,
              runAttemptId: attemptId,
              nativeTurnRef: null,
              ordinal: input.ordinal,
              status: "completed",
              startedAt: now,
              completedAt: now,
            },
          },
        ];
        const item = input.runningItem;
        if (item !== undefined) {
          const base = {
            id: item.id,
            threadId,
            runId,
            nodeId,
            providerThreadId: input.providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: input.ordinal * 100,
            status: "running",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
          } as const;
          events.push({
            id: EventId.make(`item:${input.ordinal}`),
            type: "turn-item.updated",
            threadId,
            runId,
            occurredAt: now,
            payload:
              item.kind === "command"
                ? { ...base, type: "command_execution", input: "vp run test --watch" }
                : {
                    ...base,
                    providerThreadId: ProviderThreadId.make("provider-thread:codex-subagent"),
                    type: "subagent",
                    subagentId: NodeId.make(`subagent:${input.ordinal}`),
                    origin: "provider_native",
                    driver,
                    providerInstanceId: instanceId,
                    childThreadId: reviewerThreadId,
                    prompt: "Review the change",
                    result: null,
                  },
          });
          if (item.kind === "subagent") {
            events.push({
              id: EventId.make(`subagent:${input.ordinal}`),
              type: "subagent.updated",
              threadId,
              runId,
              occurredAt: now,
              payload: {
                ...base,
                id: NodeId.make(`subagent:${input.ordinal}`),
                parentNodeId: nodeId,
                origin: "provider_native",
                createdBy: "agent",
                driver,
                providerInstanceId: instanceId,
                providerThreadId: null,
                childThreadId: reviewerThreadId,
                nativeTaskRef: null,
                prompt: "Review the change",
                model: null,
                result: null,
              },
            });
          }
        }
        return { runId, providerTurnId, events };
      };

      const watcherId = TurnItemId.make("turn-item:watcher");
      const otherProviderThreadId = ProviderThreadId.make("provider-thread:other");
      const watcherRun = settledRun({
        ordinal: 2,
        providerThreadId: olderStart ? otherProviderThreadId : codexProviderThread.id,
        ...(olderStart ? {} : { runningItem: { id: watcherId, kind: "command" as const } }),
      });
      const reviewerId = TurnItemId.make("turn-item:reviewer");
      const reviewerRun = settledRun({
        ordinal: 3,
        providerThreadId: codexProviderThread.id,
        runningItem: { id: reviewerId, kind: "subagent" },
      });
      const latestRun = settledRun({ ordinal: 4, providerThreadId: otherProviderThreadId });
      yield* sink.write({
        events: [
          ...watcherRun.events,
          ...reviewerRun.events,
          {
            id: EventId.make("provider-thread:other"),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: {
              ...codexProviderThread,
              id: otherProviderThreadId,
              firstRunOrdinal: 4,
              lastRunOrdinal: 4,
              ...(failedStart ? { pendingBackgroundTasks: otherBackgroundTasks } : {}),
            },
          },
          ...latestRun.events,
        ],
      });

      const failedRun = settledRun({ ordinal: 5, providerThreadId: codexProviderThread.id });
      if (failedStart) {
        yield* sink.write({
          events: [
            {
              id: EventId.make("failed-start-roster"),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...codexProviderThread,
                lastRunOrdinal: 5,
                pendingBackgroundTasks: backgroundTasks,
              },
            },
            ...failedRun.events.flatMap((event): Array<OrchestrationV2DomainEvent> => {
              switch (event.type) {
                case "provider-turn.updated":
                  return [];
                case "run.created":
                  return [{ ...event, payload: { ...event.payload, status: "failed" } }];
                case "node.updated":
                  return [
                    {
                      ...event,
                      payload: { ...event.payload, status: "failed", providerTurnId: null },
                    },
                  ];
                case "run-attempt.created":
                  return [
                    {
                      ...event,
                      payload: { ...event.payload, status: "failed", providerTurnId: null },
                    },
                  ];
                default:
                  return [event];
              }
            }),
          ],
        });
        // The older turn reports an empty roster, but the refused run owns the row now.
        const clear = yield* sink.writeIfProviderThreadOwner({
          providerThreadId: codexProviderThread.id,
          runId: reviewerRun.runId,
          activeAttemptId: RunAttemptId.make("attempt:3"),
          expectedLastRunOrdinal: 3,
          events: [
            {
              id: EventId.make("dropped-roster-clear"),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: { ...codexProviderThread, pendingBackgroundTasks: [] },
            },
          ],
        });
        assert.isFalse(clear.committed);
        const beforeStop = (yield* orchestrator.getThreadProjection(threadId)).providerThreads.find(
          (thread) => thread.id === codexProviderThread.id,
        )!;
        assert.equal(beforeStop.lastRunOrdinal, 5);
        assert.deepEqual(beforeStop.pendingBackgroundTasks, backgroundTasks);
      }

      if (stopWithQueue !== undefined) {
        const owner = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.id === latestRun.runId,
        )!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("latest-run-waiting"),
              type: "run.updated",
              threadId,
              runId: owner.id,
              occurredAt: now,
              payload: { ...owner, status: "waiting", completedAt: null },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queue-follow-up"),
          threadId,
          messageId: MessageId.make("message:queue-follow-up"),
          text: "Follow up after the background work",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
        assert.equal(queued.status, "queued");
        yield* sink.write({
          events: [
            {
              id: EventId.make("queue-held"),
              type: "run.updated",
              threadId,
              runId: queued.id,
              occurredAt: now,
              payload: { ...queued, queueHeld: true },
            },
            {
              id: EventId.make("latest-run-settled"),
              type: "run.updated",
              threadId,
              runId: owner.id,
              occurredAt: now,
              payload: owner,
            },
          ],
        });
      }

      if (olderStart) {
        const older = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.id === watcherRun.runId,
        )!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("older-run-starting"),
              type: "run.updated",
              threadId,
              runId: older.id,
              occurredAt: now,
              payload: { ...older, status: "starting", startedAt: null, completedAt: null },
            },
          ],
        });
      }

      yield* orchestrator.dispatch(
        stopWithQueue === "thread.stop"
          ? {
              type: "thread.stop",
              commandId: CommandId.make("stop-background-work"),
              threadId,
            }
          : {
              type: "run.interrupt",
              commandId: CommandId.make("stop-background-work"),
              threadId,
              runId: olderStart
                ? watcherRun.runId
                : failedStart
                  ? failedRun.runId
                  : latestRun.runId,
              ...(stopWithQueue === undefined ? {} : { holdQueue: true }),
            },
      );
      yield* worker.drain();

      assert.sameDeepMembers(
        interrupts.map((interrupt) => [interrupt.providerThread.id, interrupt.providerTurnId]),
        [
          ...(olderStart || failedStart ? [] : [[otherProviderThreadId, latestRun.providerTurnId]]),
          [codexProviderThread.id, reviewerRun.providerTurnId],
        ],
      );
      const after = yield* orchestrator.getThreadProjection(threadId);
      if (olderStart) {
        assert.equal(after.runs.find((run) => run.id === watcherRun.runId)?.status, "interrupted");
      }
      if (stopWithQueue !== undefined) {
        assert.equal(after.runs.at(-1)?.status, "queued");
        assert.equal(after.runs.at(-1)?.queueHeld, true);
        assert.lengthOf(started, 1);
      }
      assert.deepEqual(
        [devServerId, ...(olderStart ? [] : [watcherId]), reviewerId].map(
          (id) => after.turnItems.find((item) => item.id === id)?.status,
        ),
        olderStart ? ["interrupted", "interrupted"] : ["interrupted", "interrupted", "interrupted"],
      );
      assert.equal(after.subagents[0]?.status, "interrupted");
      const reviewerThread = yield* orchestrator.getThreadProjection(reviewerThreadId);
      assert.equal(reviewerThread.nodes[0]?.status, "interrupted");
      assert.equal(reviewerThread.subagents[0]?.status, "interrupted");
      if (workflowMember) {
        assert.isUndefined(reviewerThread.turnItems.find((item) => item.id === nestedItemId));
        assert.equal(
          reviewerThread.nodes.find((node) => node.id === nestedSubagentId)?.status,
          "interrupted",
        );
      } else {
        assert.equal(
          reviewerThread.turnItems.find((item) => item.id === nestedItemId)?.status,
          "interrupted",
        );
      }
      const testerThread = yield* orchestrator.getThreadProjection(tester.threadId);
      assert.equal(testerThread.nodes[0]?.status, "interrupted");
      if (failedStart) {
        assert.deepEqual(
          after.providerThreads.find((candidate) => candidate.id === codexProviderThread.id)
            ?.pendingBackgroundTasks,
          backgroundWorkAfterStop === undefined ? [] : backgroundTasks,
        );
        assert.deepEqual(
          after.providerThreads.find((candidate) => candidate.id === otherProviderThreadId)
            ?.pendingBackgroundTasks,
          otherBackgroundTasks,
        );
      }
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: "background-work-stop" },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  });

it.effect("Stop reaches background work an earlier provider thread still runs", () =>
  stopEarlierBackgroundWork({}),
);

it.effect("Stop settles workflow members without subagent turn items", () =>
  stopEarlierBackgroundWork({ workflowMember: true }),
);

it.effect(
  "Stop clears a stale roster after a refused run takes ownership of the provider thread",
  () => stopEarlierBackgroundWork({ failedStart: true }),
);

it.effect("Stop preserves the roster when the background work probe fails", () =>
  stopEarlierBackgroundWork({ failedStart: true, backgroundWorkAfterStop: "unknown" }),
);

it.effect.each(["thread.stop", "run.interrupt"] as const)(
  "%s stops background work when a later message is queued",
  (stopType) => stopEarlierBackgroundWork({ stopWithQueue: stopType }),
);

it.effect.each(["thread.stop", "run.interrupt"] as const)(
  "%s reaches later background work when an older run is starting",
  (stopType) => stopEarlierBackgroundWork({ stopWithQueue: stopType, olderStart: true }),
);

it.effect.each(["missing-session-terminal", "returned-interrupt", "superseded-attempt"] as const)(
  "Stop recovers a stalled run after %s without changing a newer attempt",
  (stalledRun) => stopEarlierBackgroundWork({ stalledRun }),
);

it.effect("Stop holds a provider wake that queues before the stopped turn ends", () =>
  stopEarlierBackgroundWork({ wakeAfterStop: true }),
);

const stopPreProviderStartFollowUp = ({
  run2Status,
  stopType,
  backgroundKind,
}: {
  readonly run2Status: "preparing" | "starting" | "running";
  readonly stopType: "thread.stop" | "run.interrupt";
  readonly backgroundKind: "turn-item" | "roster-only";
}) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("background-work-stop-prestart");
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const interrupts: ProviderAdapter.ProviderAdapterV2InterruptInput[] = [];
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                started.push(turn);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: () => Effect.die("unused"),
            interruptTurn: (interrupt) =>
              Effect.sync(() => {
                interrupts.push(interrupt);
              }),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };

    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make(`thread:prestart-${run2Status}-${stopType}-${backgroundKind}`);
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );

      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-prestart"),
        threadId,
        projectId: ProjectId.make("project:prestart"),
        title: "Pre-start background work stop",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const run1Running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("run-1"),
        threadId,
        messageId: MessageId.make("message:run-1"),
        text: "Launch background work",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(run1Running);

      const first = started[0]!;
      const run1Turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const devServerId = TurnItemId.make("turn-item:prestart-dev-server");
      const now = yield* DateTime.now;
      if (backgroundKind === "turn-item") {
        yield* sink.write({
          events: [
            {
              id: EventId.make("prestart-dev-server"),
              type: "turn-item.updated",
              threadId,
              runId: first.runId,
              occurredAt: now,
              payload: {
                id: devServerId,
                threadId,
                runId: first.runId,
                nodeId: first.rootNodeId,
                providerThreadId: run1Turn.providerThreadId,
                providerTurnId: run1Turn.id,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 100,
                status: "running",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                type: "command_execution",
                input: "vp run dev",
              },
            },
          ],
        });
      }

      const settled = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === first.runId &&
          (event.payload.status === "completed" || event.payload.status === "waiting"),
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...run1Turn, status: "completed", completedAt: now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: run1Turn.providerThreadId,
        providerTurnId: run1Turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();

      const ownProviderThread = (yield* orchestrator.getThreadProjection(threadId))
        .providerThreads[0]!;
      const ownRoster = [
        { taskId: "own-bg-1", description: "Own background task", kind: "command" as const },
      ];
      const otherRoster = [
        { taskId: "other-bg-1", description: "Other thread task", kind: "command" as const },
      ];
      const otherProviderThreadId = ProviderThreadId.make("provider-thread:other-roster");
      const otherRunId = RunId.make("run:other-roster");
      const otherAttemptId = RunAttemptId.make("attempt:other-roster");
      const otherNodeId = NodeId.make("node:other-roster");
      const otherTurnId = ProviderTurnId.make("provider-turn:other-roster");

      if (backgroundKind === "roster-only") {
        yield* sink.write({
          events: [
            {
              id: EventId.make("own-roster"),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...ownProviderThread,
                lastRunOrdinal: 1,
                pendingBackgroundTasks: ownRoster,
              },
            },
          ],
        });
      }

      // Dispatch follow-up Run 2 on the same providerThread without draining the effect worker,
      // leaving it pre-provider-start (providerTurn === undefined).
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("run-2"),
        threadId,
        messageId: MessageId.make("message:run-2"),
        text: "Follow up on same provider thread",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const beforeOther = yield* orchestrator.getThreadProjection(threadId);
      const run2 = beforeOther.runs.at(-1)!;
      assert.equal(run2.providerThreadId, ownProviderThread.id);
      assert.equal(run2.status, "starting");
      assert.isUndefined(
        beforeOther.providerTurns.find((turn) => turn.runAttemptId === run2.activeAttemptId),
      );

      yield* sink.write({
        events: [
          {
            id: EventId.make("other-provider-thread"),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: {
              ...ownProviderThread,
              id: otherProviderThreadId,
              firstRunOrdinal: 3,
              lastRunOrdinal: 3,
              pendingBackgroundTasks: otherRoster,
            },
          },
          {
            id: EventId.make("other-run"),
            type: "run.created",
            threadId,
            occurredAt: now,
            payload: {
              id: otherRunId,
              threadId,
              ordinal: 3,
              providerInstanceId: instanceId,
              modelSelection,
              providerThreadId: otherProviderThreadId,
              userMessageId: MessageId.make("message:other-roster"),
              rootNodeId: otherNodeId,
              activeAttemptId: otherAttemptId,
              status: "completed",
              requestedAt: now,
              startedAt: now,
              completedAt: now,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
          {
            id: EventId.make("other-node"),
            type: "node.updated",
            threadId,
            runId: otherRunId,
            occurredAt: now,
            payload: {
              id: otherNodeId,
              threadId,
              runId: otherRunId,
              parentNodeId: null,
              rootNodeId: otherNodeId,
              kind: "root_turn",
              status: "completed",
              countsForRun: true,
              providerThreadId: otherProviderThreadId,
              providerTurnId: otherTurnId,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: now,
              completedAt: now,
            },
          },
          {
            id: EventId.make("other-attempt"),
            type: "run-attempt.created",
            threadId,
            occurredAt: now,
            payload: {
              id: otherAttemptId,
              runId: otherRunId,
              attemptOrdinal: 1,
              rootNodeId: otherNodeId,
              providerInstanceId: instanceId,
              providerThreadId: otherProviderThreadId,
              providerTurnId: otherTurnId,
              reason: "initial",
              status: "completed",
              startedAt: now,
              completedAt: now,
            },
          },
          {
            id: EventId.make("other-turn"),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: otherTurnId,
              providerThreadId: otherProviderThreadId,
              nodeId: otherNodeId,
              runAttemptId: otherAttemptId,
              nativeTurnRef: null,
              ordinal: 3,
              status: "completed",
              startedAt: now,
              completedAt: now,
            },
          },
          ...(run2Status === "starting"
            ? []
            : [
                {
                  id: EventId.make(`run-2-${run2Status}`),
                  type: "run.updated" as const,
                  threadId,
                  runId: run2.id,
                  occurredAt: now,
                  payload: { ...run2, status: run2Status },
                },
              ]),
        ],
      });

      yield* orchestrator.dispatch(
        stopType === "thread.stop"
          ? {
              type: "thread.stop",
              commandId: CommandId.make("stop-prestart"),
              threadId,
            }
          : {
              type: "run.interrupt",
              commandId: CommandId.make("stop-prestart"),
              threadId,
              runId: run2.id,
            },
      );
      yield* worker.drain();

      assert.lengthOf(started, 1);
      assert.sameDeepMembers(
        interrupts.map((interrupt) => [interrupt.providerThread.id, interrupt.providerTurnId]),
        stopType === "thread.stop"
          ? [
              [ownProviderThread.id, run1Turn.id],
              [otherProviderThreadId, otherTurnId],
            ]
          : [[ownProviderThread.id, run1Turn.id]],
      );

      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs.find((candidate) => candidate.id === run2.id)?.status, "interrupted");
      if (backgroundKind === "turn-item") {
        assert.equal(
          after.turnItems.find((candidate) => candidate.id === devServerId)?.status,
          "interrupted",
        );
      } else {
        assert.deepEqual(
          after.providerThreads.find((candidate) => candidate.id === ownProviderThread.id)
            ?.pendingBackgroundTasks,
          [],
        );
      }
      assert.deepEqual(
        after.providerThreads.find((candidate) => candidate.id === otherProviderThreadId)
          ?.pendingBackgroundTasks,
        stopType === "thread.stop" ? [] : otherRoster,
      );
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: `prestart-${run2Status}-${stopType}-${backgroundKind}` },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  });

it.effect.each([
  ["preparing", "thread.stop", "turn-item"],
  ["starting", "thread.stop", "turn-item"],
  ["running", "thread.stop", "turn-item"],
  ["preparing", "run.interrupt", "turn-item"],
  ["starting", "run.interrupt", "turn-item"],
  ["running", "run.interrupt", "turn-item"],
  ["preparing", "thread.stop", "roster-only"],
  ["starting", "thread.stop", "roster-only"],
  ["running", "thread.stop", "roster-only"],
  ["preparing", "run.interrupt", "roster-only"],
  ["starting", "run.interrupt", "roster-only"],
  ["running", "run.interrupt", "roster-only"],
] as const)(
  "R4-CF-01: %s pre-start Run 2 stopped via %s interrupts prior Run 1 %s background work on the same providerThread",
  ([run2Status, stopType, backgroundKind]) =>
    stopPreProviderStartFollowUp({ run2Status, stopType, backgroundKind }),
);

it.effect.each(["thread.stop", "run.interrupt"] as const)(
  "R4-CF-01: %s deduplicates supersededTurn against same-providerThread background interrupts",
  (stopType) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("background-work-stop-superseded-dedupe");
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const interrupts: ProviderAdapter.ProviderAdapterV2InterruptInput[] = [];
      const interruptCalls = yield* Queue.unbounded<void>();
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        instanceId,
        driver,
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            return {
              instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver,
                providerInstanceId: instanceId,
                status: "ready",
                cwd,
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.fromQueue(events),
              ensureThread: ({ threadId }) =>
                Effect.succeed({
                  id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                  driver,
                  providerInstanceId: instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                  nativeConversationHeadRef: null,
                  status: "idle",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                }),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: (turn) =>
                Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                }).pipe(Effect.asVoid),
              steerTurn: () => Effect.die("unused"),
              interruptTurn: (interrupt) =>
                Effect.sync(() => {
                  interrupts.push(interrupt);
                }).pipe(Effect.andThen(Queue.offer(interruptCalls, undefined))),
              respondToRuntimeRequest: () => Effect.die("unused"),
              readThreadSnapshot: () => Effect.die("unused"),
              rollbackThread: () => Effect.die("unused"),
              forkThread: () => Effect.die("unused"),
            };
          }),
      };

      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`thread:superseded-dedupe-${stopType}`);
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );

        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create-dedupe"),
          threadId,
          projectId: ProjectId.make("project:dedupe"),
          title: "Superseded dedupe",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const run1Running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-1-dedupe"),
          threadId,
          messageId: MessageId.make("message:run-1-dedupe"),
          text: "Start background process",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(run1Running);

        const afterRun1Start = yield* orchestrator.getThreadProjection(threadId);
        const run1 = afterRun1Start.runs[0]!;
        const run1Turn = afterRun1Start.providerTurns[0]!;
        const devServerId = TurnItemId.make("turn-item:dedupe-dev-server");
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("dedupe-dev-server"),
              type: "turn-item.updated",
              threadId,
              runId: run1.id,
              occurredAt: now,
              payload: {
                id: devServerId,
                threadId,
                runId: run1.id,
                nodeId: run1.rootNodeId!,
                providerThreadId: run1Turn.providerThreadId,
                providerTurnId: run1Turn.id,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 100,
                status: "running",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                type: "command_execution",
                input: "vp run dev",
              },
            },
          ],
        });
        const settled = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === run1.id &&
            (event.payload.status === "completed" || event.payload.status === "waiting"),
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...run1Turn, status: "completed", completedAt: now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: run1Turn.providerThreadId,
          providerTurnId: run1Turn.id,
          runOrdinal: run1.ordinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();

        // Dispatch Run 2 on the same providerThread; attach a still-running Attempt 1 turn
        // superseded by an active Attempt 2 that has no providerTurn yet.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-2-dedupe"),
          threadId,
          messageId: MessageId.make("message:run-2-dedupe"),
          text: "Follow up with restart",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const beforeStop = yield* orchestrator.getThreadProjection(threadId);
        const run2 = beforeStop.runs.at(-1)!;
        const attempt1 = beforeStop.attempts.find(
          (candidate) => candidate.id === run2.activeAttemptId,
        )!;
        const attempt2Id = RunAttemptId.make("attempt:run2-restart-2");
        const supersededTurnId = ProviderTurnId.make("provider-turn:run2-attempt1-superseded");
        yield* sink.write({
          events: [
            {
              id: EventId.make("superseded-turn-running"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: supersededTurnId,
                providerThreadId: run1Turn.providerThreadId,
                nodeId: run2.rootNodeId!,
                runAttemptId: attempt1.id,
                nativeTurnRef: null,
                ordinal: 2,
                status: "running",
                startedAt: now,
                completedAt: null,
              },
            },
            {
              id: EventId.make("attempt-2-created"),
              type: "run-attempt.created",
              threadId,
              occurredAt: now,
              payload: {
                ...attempt1,
                id: attempt2Id,
                attemptOrdinal: 2,
                providerTurnId: null,
                reason: "steering_restart",
                status: "pending",
              },
            },
            {
              id: EventId.make("run-2-active-attempt-2"),
              type: "run.updated",
              threadId,
              runId: run2.id,
              occurredAt: now,
              payload: { ...run2, activeAttemptId: attempt2Id, status: "starting" },
            },
          ],
        });

        yield* orchestrator.dispatch(
          stopType === "thread.stop"
            ? { type: "thread.stop", commandId: CommandId.make("stop-dedupe"), threadId }
            : {
                type: "run.interrupt",
                commandId: CommandId.make("stop-dedupe"),
                threadId,
                runId: run2.id,
              },
        );
        const drained = yield* Effect.forkChild(worker.drain());
        yield* Queue.take(interruptCalls);
        yield* TestClock.adjust("2 seconds");
        yield* Fiber.join(drained);

        assert.deepEqual(
          interrupts.map((interrupt) => [interrupt.providerThread.id, interrupt.providerTurnId]),
          [[run1Turn.providerThreadId, supersededTurnId]],
        );
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          after.runs.find((candidate) => candidate.id === run2.id)?.status,
          "interrupted",
        );
        assert.equal(
          after.turnItems.find((candidate) => candidate.id === devServerId)?.status,
          "interrupted",
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `superseded-dedupe-${stopType}` },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
);

it.effect("R4-CF-01: supersededTurn beats same-ordinal background turn on multi-attempt run", () =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("background-work-stop-same-ordinal-superseded");
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const interrupts: ProviderAdapter.ProviderAdapterV2InterruptInput[] = [];
    const interruptCalls = yield* Queue.unbounded<void>();
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Queue.offer(events, {
                type: "provider_turn.updated",
                driver,
                providerTurn: {
                  id: ProviderTurnId.make(`provider-turn:same-ord:${turn.attemptId}`),
                  providerThreadId: turn.providerThread.id,
                  nodeId: turn.rootNodeId,
                  runAttemptId: turn.attemptId,
                  nativeTurnRef: {
                    driver,
                    nativeId: `native:${turn.attemptId}`,
                    strength: "strong",
                  },
                  ordinal: turn.providerTurnOrdinal,
                  status: "running",
                  startedAt: now,
                  completedAt: null,
                },
              }).pipe(Effect.asVoid),
            steerTurn: () => Effect.void,
            interruptTurn: (interrupt) =>
              Effect.sync(() => {
                interrupts.push(interrupt);
              }).pipe(Effect.andThen(Queue.offer(interruptCalls, undefined)), Effect.asVoid),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };

    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:r4-cf01-same-ordinal-superseded");

      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-same-ordinal"),
        threadId,
        projectId: ProjectId.make("project:r4-cf01-same-ordinal"),
        title: "Same-ordinal superseded dedupe",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });

      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      const run1Running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("run-1-same-ordinal"),
        threadId,
        messageId: MessageId.make("message:run-1-same-ordinal"),
        text: "Start run with multi-attempt restarts",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(run1Running);

      const liveProjection = yield* orchestrator.getThreadProjection(threadId);
      const run1 = liveProjection.runs[0]!;
      const attempt1Turn = liveProjection.providerTurns[0]!;
      const providerThreadId = attempt1Turn.providerThreadId;

      // Finish Run 1 cleanly so a live session exists on providerThreadId.
      const run1Settled = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === run1.id &&
          (event.payload.status === "completed" || event.payload.status === "waiting"),
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...attempt1Turn, status: "completed", completedAt: now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId,
        providerTurnId: attempt1Turn.id,
        runOrdinal: run1.ordinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(run1Settled);
      yield* worker.drain();

      // Dispatch Run 2 on the same providerThread; its Attempt 1 left an open background item
      // (same run.ordinal === 2), Attempt 2 is a still-running supersededTurn, and Attempt 3 is pre-start.
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("run-2-same-ordinal"),
        threadId,
        messageId: MessageId.make("message:run-2-same-ordinal"),
        text: "Multi-attempt run 2",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const beforeStop = yield* orchestrator.getThreadProjection(threadId);
      const run2 = beforeStop.runs.at(-1)!;
      const run2Attempt1 = beforeStop.attempts.find(
        (candidate) => candidate.id === run2.activeAttemptId,
      )!;
      const run2Attempt1TurnId = ProviderTurnId.make("provider-turn:same-ordinal-run2-attempt1");
      const run2Attempt2Id = RunAttemptId.make("attempt:same-ordinal-run2-2");
      const attempt2SupersededTurnId = ProviderTurnId.make("provider-turn:same-ordinal-attempt2");
      const run2Attempt3Id = RunAttemptId.make("attempt:same-ordinal-run2-3");
      const bgItemId = TurnItemId.make("turn-item:same-ordinal-bg");

      yield* sink.write({
        events: [
          {
            id: EventId.make("same-ord-r2-t1"),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: run2Attempt1TurnId,
              providerThreadId,
              nodeId: run2.rootNodeId!,
              runAttemptId: run2Attempt1.id,
              nativeTurnRef: null,
              ordinal: 2,
              status: "completed",
              startedAt: now,
              completedAt: now,
            },
          },
          {
            id: EventId.make("same-ord-bg"),
            type: "turn-item.updated",
            threadId,
            runId: run2.id,
            occurredAt: now,
            payload: {
              id: bgItemId,
              threadId,
              runId: run2.id,
              nodeId: run2.rootNodeId!,
              providerThreadId,
              providerTurnId: run2Attempt1TurnId,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 50,
              status: "running",
              title: null,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              type: "command_execution",
              input: "sleep 60",
            },
          },
          {
            id: EventId.make("same-ord-a2"),
            type: "run-attempt.created",
            threadId,
            occurredAt: now,
            payload: {
              ...run2Attempt1,
              id: run2Attempt2Id,
              attemptOrdinal: 2,
              providerTurnId: attempt2SupersededTurnId,
              reason: "steering_restart",
              status: "running",
            },
          },
          {
            id: EventId.make("same-ord-t2"),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: attempt2SupersededTurnId,
              providerThreadId,
              nodeId: run2.rootNodeId!,
              runAttemptId: run2Attempt2Id,
              nativeTurnRef: null,
              ordinal: 3,
              status: "running",
              startedAt: now,
              completedAt: null,
            },
          },
          {
            id: EventId.make("same-ord-a3"),
            type: "run-attempt.created",
            threadId,
            occurredAt: now,
            payload: {
              ...run2Attempt1,
              id: run2Attempt3Id,
              attemptOrdinal: 3,
              providerTurnId: null,
              reason: "steering_restart",
              status: "pending",
            },
          },
          {
            id: EventId.make("same-ord-run-a3"),
            type: "run.updated",
            threadId,
            runId: run2.id,
            occurredAt: now,
            payload: { ...run2, activeAttemptId: run2Attempt3Id, status: "starting" },
          },
        ],
      });

      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-same-ordinal"),
        threadId,
      });
      const drained = yield* Effect.forkChild(worker.drain());
      yield* Queue.take(interruptCalls);
      yield* TestClock.adjust("2 seconds");
      yield* Fiber.join(drained);

      assert.deepEqual(
        interrupts.map((interrupt) => [interrupt.providerThread.id, interrupt.providerTurnId]),
        [[providerThreadId, attempt2SupersededTurnId]],
      );
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: "same-ordinal-superseded" },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  }),
);

it.effect(
  "R4-CF-01: pre-provider-start Stop settles background work immediately when provider session is missing",
  () =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("background-work-stop-dead-session");
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        instanceId,
        driver,
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            return {
              instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver,
                providerInstanceId: instanceId,
                status: "ready",
                cwd,
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.fromQueue(events),
              ensureThread: ({ threadId }) =>
                Effect.succeed({
                  id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                  driver,
                  providerInstanceId: instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                  nativeConversationHeadRef: null,
                  status: "idle",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                }),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: () => Effect.void,
              steerTurn: () => Effect.void,
              interruptTurn: () => Effect.void,
              respondToRuntimeRequest: () => Effect.die("unused"),
              readThreadSnapshot: () => Effect.die("unused"),
              rollbackThread: () => Effect.die("unused"),
              forkThread: () => Effect.die("unused"),
            };
          }),
      };

      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread:r4-cf01-dead-session");

        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create-dead-session"),
          threadId,
          projectId: ProjectId.make("project:r4-cf01-dead-session"),
          title: "Dead session pre-start Stop",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-1-dead"),
          threadId,
          messageId: MessageId.make("message:run-1-dead"),
          text: "Run 1",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const afterRun1 = yield* orchestrator.getThreadProjection(threadId);
        const run1 = afterRun1.runs[0]!;
        const providerThread = afterRun1.providerThreads[0]!;
        const run1TurnId = ProviderTurnId.make("provider-turn:dead-run-1");
        const bgItemId = TurnItemId.make("turn-item:dead-bg");

        // Mark Run 1 completed with background work & roster on a detached/dead providerSessionId.
        yield* sink.write({
          events: [
            {
              id: EventId.make("dead-pt"),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...providerThread,
                providerSessionId: null,
                lastRunOrdinal: 1,
                pendingBackgroundTasks: [
                  {
                    taskId: "bg-dead-1",
                    description: "Dead session background task",
                    kind: "command" as const,
                  },
                ],
              },
            },
            {
              id: EventId.make("dead-turn-1"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: run1TurnId,
                providerThreadId: providerThread.id,
                nodeId: run1.rootNodeId!,
                runAttemptId: run1.activeAttemptId!,
                nativeTurnRef: null,
                ordinal: 1,
                status: "completed",
                startedAt: now,
                completedAt: now,
              },
            },
            {
              id: EventId.make("dead-bg-item"),
              type: "turn-item.updated",
              threadId,
              runId: run1.id,
              occurredAt: now,
              payload: {
                id: bgItemId,
                threadId,
                runId: run1.id,
                nodeId: run1.rootNodeId!,
                providerThreadId: providerThread.id,
                providerTurnId: run1TurnId,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 20,
                status: "running",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                type: "command_execution",
                input: "sleep 60",
              },
            },
            {
              id: EventId.make("dead-run-1-done"),
              type: "run.updated",
              threadId,
              runId: run1.id,
              occurredAt: now,
              payload: { ...run1, status: "completed", completedAt: now },
            },
          ],
        });

        // Dispatch Run 2 (pre-provider-start "starting") and stop it immediately.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-2-dead"),
          threadId,
          messageId: MessageId.make("message:run-2-dead"),
          text: "Run 2",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop-dead-session"),
          threadId,
        });

        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs.at(-1)?.status, "interrupted");
        assert.equal(
          after.turnItems.find((candidate) => candidate.id === bgItemId)?.status,
          "interrupted",
        );
        assert.deepEqual(
          after.providerThreads.find((candidate) => candidate.id === providerThread.id)
            ?.pendingBackgroundTasks,
          [],
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "dead-session-prestart-stop" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
);

it.effect(
  "R4-CF-01: resolvable providerTurn beats unresolvable runAttemptId:null turn even with higher lastRunOrdinal, and app_owned subagent turnItem is not corrupted by pre-start run.interrupt",
  () =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("background-work-stop-resolvable-and-app-owned");
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const interrupts: ProviderAdapter.ProviderAdapterV2InterruptInput[] = [];
      const interruptCalls = yield* Queue.unbounded<void>();
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        instanceId,
        driver,
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            return {
              instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver,
                providerInstanceId: instanceId,
                status: "ready",
                cwd,
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.fromQueue(events),
              ensureThread: ({ threadId }) =>
                Effect.succeed({
                  id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                  driver,
                  providerInstanceId: instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                  nativeConversationHeadRef: null,
                  status: "idle",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                }),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: (turn) =>
                Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:res:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                }).pipe(Effect.asVoid),
              steerTurn: () => Effect.void,
              interruptTurn: (interrupt) =>
                Effect.sync(() => {
                  interrupts.push(interrupt);
                }).pipe(Effect.andThen(Queue.offer(interruptCalls, undefined)), Effect.asVoid),
              respondToRuntimeRequest: () => Effect.die("unused"),
              readThreadSnapshot: () => Effect.die("unused"),
              rollbackThread: () => Effect.die("unused"),
              forkThread: () => Effect.die("unused"),
            };
          }),
      };

      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread:r4-cf01-resolvable-and-app-owned");

        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create-res"),
          threadId,
          projectId: ProjectId.make("project:r4-cf01-res"),
          title: "Resolvable and app_owned check",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });

        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        const run1Running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-1-res"),
          threadId,
          messageId: MessageId.make("message:run-1-res"),
          text: "Run 1",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(run1Running);

        const afterStart = yield* orchestrator.getThreadProjection(threadId);
        const run1 = afterStart.runs[0]!;
        const run1Turn = afterStart.providerTurns[0]!;
        const providerThread = afterStart.providerThreads[0]!;
        const run1Settled = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === run1.id &&
            (event.payload.status === "completed" || event.payload.status === "waiting"),
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...run1Turn, status: "completed", completedAt: now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: run1Turn.providerThreadId,
          providerTurnId: run1Turn.id,
          runOrdinal: run1.ordinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(run1Settled);
        yield* worker.drain();

        // Part A: Only an app_owned delegated subagent turn item is running from Run 1.
        // Pre-start run.interrupt without holdQueue must NOT emit a provider interrupt or corrupt it.
        const appOwnedItemId = TurnItemId.make("turn-item:app-owned-subagent-1");
        yield* sink.write({
          events: [
            {
              id: EventId.make("app-owned-item"),
              type: "turn-item.updated",
              threadId,
              runId: run1.id,
              occurredAt: now,
              payload: {
                id: appOwnedItemId,
                threadId,
                runId: run1.id,
                nodeId: run1.rootNodeId!,
                providerThreadId: providerThread.id,
                providerTurnId: run1Turn.id,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 30,
                status: "running",
                title: "Delegated child",
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                type: "subagent",
                subagentId: NodeId.make("node:app-owned-subagent-1"),
                origin: "app_owned",
                driver,
                providerInstanceId: instanceId,
                childThreadId: ThreadId.make("thread:child-app-owned"),
                prompt: "Work",
                result: null,
              },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-2-app-owned"),
          threadId,
          messageId: MessageId.make("message:run-2-app-owned"),
          text: "Run 2 pre-start",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const run2 = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("interrupt-run-2-app-owned"),
          threadId,
          runId: run2.id,
        });
        yield* worker.drain();
        assert.lengthOf(interrupts, 0);
        const afterRun2Stop = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterRun2Stop.turnItems.find((candidate) => candidate.id === appOwnedItemId)?.status,
          "running",
        );

        // Part B: Add an unresolvable (runAttemptId: null) turn alongside roster tasks with
        // higher lastRunOrdinal (3 > 1). Resolvable run1Turn must still win over runlessTurn.
        const runlessTurnId = ProviderTurnId.make("provider-turn:runless-higher-last-ord");
        yield* sink.write({
          events: [
            {
              id: EventId.make("res-pt-roster"),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...afterRun2Stop.providerThreads[0]!,
                lastRunOrdinal: 3,
                pendingBackgroundTasks: [
                  {
                    taskId: "bg-res-1",
                    description: "Background command",
                    kind: "command" as const,
                  },
                ],
              },
            },
            {
              id: EventId.make("res-runless-turn"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: runlessTurnId,
                providerThreadId: providerThread.id,
                nodeId: run1.rootNodeId!,
                runAttemptId: null,
                nativeTurnRef: null,
                ordinal: 99,
                status: "completed",
                startedAt: now,
                completedAt: now,
              },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("run-3-res"),
          threadId,
          messageId: MessageId.make("message:run-3-res"),
          text: "Run 3 pre-start",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop-run-3-res"),
          threadId,
        });
        const drained = yield* Effect.forkChild(worker.drain());
        yield* Queue.take(interruptCalls);
        yield* TestClock.adjust("2 seconds");
        yield* Fiber.join(drained);

        assert.deepEqual(
          interrupts.map((interrupt) => [interrupt.providerThread.id, interrupt.providerTurnId]),
          [[providerThread.id, run1Turn.id]],
        );
        const finalProjection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          finalProjection.providerThreads.find((candidate) => candidate.id === providerThread.id)
            ?.pendingBackgroundTasks,
          [],
        );
        // app_owned subagent turn item still not corrupted by settleBackgroundWork.
        assert.equal(
          finalProjection.turnItems.find((candidate) => candidate.id === appOwnedItemId)?.status,
          "running",
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "resolvable-and-app-owned" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
);
