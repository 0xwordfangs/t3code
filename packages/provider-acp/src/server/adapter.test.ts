import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as EffectAcpErrors from "effect-acp/errors";

import type * as AcpSessionRuntime from "./AcpSessionRuntime.ts";
import { AcpProviderCapabilitiesV2, makeAcpAdapterV2 } from "./adapter.ts";

const TEST_DRIVER = ProviderDriverKind.make("acp");

const testProviderHostLayer = Layer.succeed(ProviderHost.ProviderHost, {
  paths: {
    cwd: process.cwd(),
    baseDir: process.cwd(),
    stateDir: process.cwd(),
    providerStatusCacheDir: process.cwd(),
    attachmentsDir: process.cwd(),
  },
  settings: {
    get: Effect.succeed(DEFAULT_SERVER_SETTINGS),
    withSnapshot: (use) => use(DEFAULT_SERVER_SETTINGS),
    changes: Stream.empty,
    subscribe: Effect.succeed(Stream.empty),
  },
  shouldRunBackgroundWork: () => Effect.succeed(false),
  resolveAttachmentPath: () => null,
  credentials: (namespace, bindingId) =>
    Effect.succeed({
      binding: { owner: "t3", key: `${namespace}:${bindingId}` },
      get: Effect.succeedNone,
      set: () => Effect.void,
      remove: Effect.void,
    }),
});

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  NodeCrypto.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  testProviderHostLayer,
);

const makeHarness = Effect.fnUntraced(function* (options: {
  readonly name: string;
  readonly runtimeMode: RuntimeMode;
  readonly approvalPolicy?: "never" | "on-request" | "untrusted";
  readonly enablePostSettleContinuation?: boolean;
  readonly deferFinalizeForBackgroundWork?: boolean;
  readonly onUrlElicitation?: (input: {
    readonly elicitationId: string;
    readonly url: string;
    readonly message: string;
  }) => Effect.Effect<boolean>;
}) {
  type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
  const sessionId = `session-${options.name}`;
  let permissionHandler: Parameters<RuntimeService["handleRequestPermission"]>[0] | undefined;
  let elicitationHandler: Parameters<RuntimeService["handleElicitation"]>[0] | undefined;
  let onOutgoingResponse: ((requestId: string) => Effect.Effect<void>) | undefined;
  let promptStarted = yield* Deferred.make<void>();
  let promptGate = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
  let cancelCalled = yield* Deferred.make<void>();
  const continuationOffered =
    yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();

  const startResult: AcpSessionRuntime.AcpSessionRuntimeStartResult = {
    sessionId,
    initializeResult: {
      protocolVersion: 1,
      agentCapabilities: {},
    },
    sessionSetupResult: {},
    modelConfigId: undefined,
  };

  const makeMockRuntime = (
    runtimeInput: Parameters<Parameters<typeof makeAcpAdapterV2>[0]["flavor"]["makeRuntime"]>[0],
  ): Effect.Effect<RuntimeService, EffectAcpErrors.AcpError> => {
    onOutgoingResponse = runtimeInput.onOutgoingResponse;
    const unsupported = (method: string) =>
      Effect.fail(EffectAcpErrors.AcpRequestError.methodNotFound(method));
    return Effect.succeed({
      handleRequestPermission: (handler) =>
        Effect.sync(() => {
          permissionHandler = handler;
        }),
      handleElicitation: (handler) =>
        Effect.sync(() => {
          elicitationHandler = handler;
        }),
      handleMcpConnect: () => Effect.void,
      handleMcpMessage: () => Effect.void,
      handleMcpDisconnect: () => Effect.void,
      handleMcpNotification: () => Effect.void,
      handleReadTextFile: () => Effect.void,
      handleWriteTextFile: () => Effect.void,
      handleCreateTerminal: () => Effect.void,
      handleTerminalOutput: () => Effect.void,
      handleTerminalWaitForExit: () => Effect.void,
      handleTerminalKill: () => Effect.void,
      handleTerminalRelease: () => Effect.void,
      handleSessionUpdate: () => Effect.void,
      handleElicitationComplete: () => Effect.void,
      handleUnknownExtRequest: () => Effect.void,
      handleUnknownExtNotification: () => Effect.void,
      handleExtRequest: () => Effect.void,
      handleExtNotification: () => Effect.void,
      initialize: () => Effect.succeed(startResult.initializeResult),
      start: () => Effect.succeed(startResult),
      getEvents: () => Stream.empty,
      drainEvents: Effect.void,
      getModeState: Effect.undefined,
      getConfigOptions: Effect.succeed([]),
      configOptionChanges: Stream.empty,
      loadSession: () => Effect.succeed(startResult),
      resumeSession: () => Effect.succeed(startResult),
      forkSession: () => Effect.succeed(startResult),
      listSessions: () => unsupported("session/list"),
      closeSession: () => Effect.succeed({}),
      deleteSession: () => unsupported("session/delete"),
      listProviders: unsupported("provider/list"),
      setProvider: () => unsupported("provider/set"),
      disableProvider: () => unsupported("provider/disable"),
      logout: unsupported("logout"),
      prompt: (_payload, promptOptions) =>
        Effect.gen(function* () {
          if (promptOptions?.dispatched !== undefined) {
            yield* Deferred.succeed(promptOptions.dispatched, undefined);
          }
          yield* Deferred.succeed(promptStarted, undefined);
          return yield* Deferred.await(promptGate);
        }),
      cancel: Effect.suspend(() => Deferred.succeed(cancelCalled, undefined).pipe(Effect.asVoid)),
      setMode: () => Effect.succeed({}),
      setSessionModel: () => Effect.succeed({}),
      setConfigOption: () => Effect.succeed({ configOptions: [] }),
      setModel: () => Effect.void,
      request: (method) => unsupported(method),
      notify: () => Effect.void,
    });
  };

  const instanceId = ProviderInstanceId.make(`acp-adapter-${options.name}`);
  const adapter = yield* makeAcpAdapterV2({
    instanceId,
    flavor: {
      driver: TEST_DRIVER,
      capabilities: AcpProviderCapabilitiesV2,
      makeRuntime: makeMockRuntime,
      ...(options.enablePostSettleContinuation === undefined
        ? {}
        : { enablePostSettleContinuation: options.enablePostSettleContinuation }),
      ...(options.deferFinalizeForBackgroundWork === undefined
        ? {}
        : { deferFinalizeForBackgroundWork: options.deferFinalizeForBackgroundWork }),
      ...(options.onUrlElicitation === undefined
        ? {}
        : { onUrlElicitation: options.onUrlElicitation }),
    },
    selfInvocation: { command: process.execPath, entrypoint: undefined },
    ...(options.enablePostSettleContinuation === true
      ? {
          continuationRequests: {
            offer: (request) => Queue.offer(continuationOffered, request).pipe(Effect.asVoid),
          },
        }
      : {}),
  });

  const threadId = ThreadId.make(`thread-acp-${options.name}`);
  const policyFor = (runtimeMode: RuntimeMode, includeInitialApprovalPolicy = false) =>
    ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
      runtimeMode,
      interactionMode: "default",
      cwd: process.cwd(),
      ...(includeInitialApprovalPolicy && options.approvalPolicy !== undefined
        ? { approvalPolicy: options.approvalPolicy }
        : {}),
    });
  const initialPolicy = policyFor(options.runtimeMode, true);
  const modelSelection = { instanceId, model: "default" } as const;
  const session = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make(`provider-session-acp-${options.name}`),
    modelSelection,
    runtimePolicy: initialPolicy,
  });
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* session.events.pipe(
    Stream.runForEach((event) => Queue.offer(events, event)),
    Effect.forkScoped,
  );
  const providerThread = yield* session.ensureThread({
    threadId,
    modelSelection,
    runtimePolicy: initialPolicy,
  });
  const now = yield* DateTime.now;
  const makeTurn = (
    ordinal: number,
    turnOptions?: {
      readonly continuation?: boolean;
      readonly runtimeMode?: RuntimeMode;
      readonly text?: string;
    },
  ): ProviderAdapter.ProviderAdapterV2TurnInput => {
    const mode = turnOptions?.runtimeMode ?? options.runtimeMode;
    return {
      appThread: {
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId: ProjectId.make(`project-acp-${options.name}`),
        title: `ACP ${options.name}`,
        providerInstanceId: instanceId,
        modelSelection,
        runtimeMode: mode,
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: providerThread.id,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
      threadId,
      runId: RunId.make(`run-acp-${options.name}-${ordinal}`),
      runOrdinal: ordinal,
      providerTurnOrdinal: ordinal,
      attemptId: RunAttemptId.make(`attempt-acp-${options.name}-${ordinal}`),
      rootNodeId: NodeId.make(`node-acp-${options.name}-${ordinal}`),
      providerThread,
      message: {
        createdBy: turnOptions?.continuation ? "agent" : "user",
        creationSource: turnOptions?.continuation ? "provider" : "web",
        messageId: MessageId.make(`message-acp-${options.name}-${ordinal}`),
        text: turnOptions?.text ?? (turnOptions?.continuation ? "Continuation wake" : "run"),
        attachments: [],
      },
      modelSelection,
      runtimePolicy: policyFor(mode, turnOptions?.runtimeMode === undefined),
    };
  };

  const settleTurn1 = Effect.gen(function* () {
    yield* session.startTurn(makeTurn(1)).pipe(Effect.forkScoped);
    yield* Deferred.await(promptStarted);
    yield* Deferred.succeed(promptGate, { stopReason: "end_turn" });
    while ((yield* Queue.take(events)).type !== "turn.terminal") {
      // drain Turn 1 events
    }
  });

  const requestPermission = (
    toolCallId: string,
    requestId = `perm-${toolCallId}`,
  ): Effect.Effect<EffectAcpSchema.RequestPermissionResponse, EffectAcpErrors.AcpError> =>
    permissionHandler!(
      {
        sessionId,
        toolCall: { toolCallId, kind: "execute", title: "Run shell command" },
        options: [
          { optionId: "allow_once", name: "Allow", kind: "allow_once" },
          { optionId: "deny", name: "Deny", kind: "reject_once" },
        ],
      },
      { requestId, method: "session/request_permission" },
    ).pipe(Effect.tap(() => onOutgoingResponse?.(requestId) ?? Effect.void));

  const elicitMcpApproval = (requestId: string) =>
    elicitationHandler!(
      {
        sessionId,
        mode: "form",
        message: "Approve MCP tool call?",
        requestedSchema: {
          type: "object",
          properties: {
            approved: { type: "boolean", title: "Approved" },
          },
          required: ["approved"],
        },
        _meta: { codex_approval_kind: "mcp_tool_call" },
      },
      { requestId, method: "elicitation/create" },
    ).pipe(Effect.tap(() => onOutgoingResponse?.(requestId) ?? Effect.void));

  const elicitFormQuestion = (requestId: string) =>
    elicitationHandler!(
      {
        sessionId,
        mode: "form",
        message: "Pick a target",
        requestedSchema: {
          type: "object",
          properties: {
            target: { type: "string", enum: ["staging", "prod"], title: "Target" },
          },
          required: ["target"],
        },
      },
      { requestId, method: "elicitation/create" },
    ).pipe(Effect.tap(() => onOutgoingResponse?.(requestId) ?? Effect.void));

  const elicitUrl = (requestId: string) =>
    elicitationHandler!(
      {
        sessionId,
        mode: "url",
        elicitationId: `url-${requestId}`,
        url: "https://example.com/authorize",
        message: "Authorize access",
      },
      { requestId, method: "elicitation/create" },
    ).pipe(Effect.tap(() => onOutgoingResponse?.(requestId) ?? Effect.void));

  return {
    session,
    providerThread,
    events,
    continuationOffered,
    makeTurn,
    settleTurn1,
    requestPermission,
    elicitMcpApproval,
    elicitFormQuestion,
    elicitUrl,
    get promptStarted() {
      return promptStarted;
    },
    get promptGate() {
      return promptGate;
    },
    get cancelCalled() {
      return cancelCalled;
    },
    resetPrompt: Effect.gen(function* () {
      promptStarted = yield* Deferred.make<void>();
      promptGate = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
      cancelCalled = yield* Deferred.make<void>();
    }),
  };
});

const awaitPendingRuntimeRequest = Effect.fnUntraced(function* (
  events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>,
) {
  while (true) {
    const event = yield* Queue.take(events);
    if (event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending") {
      return event.runtimeRequest;
    }
  }
});

describe("AcpAdapterV2 post-settle policy admission (CF-02)", () => {
  it.effect(
    "holds post-settle full-access permission and MCP elicitation until continuation admission and cancels when declined",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf02-decline-full-access",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;

        const permFiber = yield* harness
          .requestPermission("call_perm_declined")
          .pipe(Effect.forkScoped);
        const permOffer = yield* Queue.take(harness.continuationOffered);
        assert.isUndefined(permFiber.pollUnsafe());
        yield* permOffer.clearIfCurrent!();
        assert.deepEqual(yield* Fiber.join(permFiber), { outcome: { outcome: "cancelled" } });

        const mcpFiber = yield* harness
          .elicitMcpApproval("mcp_tool_call_approval_declined")
          .pipe(Effect.forkScoped);
        const mcpOffer = yield* Queue.take(harness.continuationOffered);
        assert.isUndefined(mcpFiber.pollUnsafe());
        yield* mcpOffer.clearIfCurrent!();
        assert.deepEqual(yield* Fiber.join(mcpFiber), { action: "cancel" });
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "prompts instead of auto-approving when a post-settle full-access permission or MCP elicitation wakes into downgraded approval-required",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf02-downgraded-continuation",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;

        const permFiber = yield* harness
          .requestPermission("call_perm_downgraded")
          .pipe(Effect.forkScoped);
        const mcpFiber = yield* harness
          .elicitMcpApproval("mcp_tool_call_approval_downgraded")
          .pipe(Effect.forkScoped);
        const offered = yield* Queue.take(harness.continuationOffered);
        assert.isUndefined(permFiber.pollUnsafe());
        assert.isUndefined(mcpFiber.pollUnsafe());

        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(
          harness.makeTurn(2, { continuation: true, runtimeMode: "approval-required" }),
        );

        const firstRequest = yield* awaitPendingRuntimeRequest(harness.events);
        const secondRequest = yield* awaitPendingRuntimeRequest(harness.events);
        const approvalRequest = [firstRequest, secondRequest].find(
          (request) => request.kind !== "user_input",
        );
        const mcpInputRequest = [firstRequest, secondRequest].find(
          (request) => request.kind === "user_input",
        );
        assert.isDefined(approvalRequest);
        assert.isDefined(mcpInputRequest);
        assert.isUndefined(permFiber.pollUnsafe());
        assert.isUndefined(mcpFiber.pollUnsafe());

        yield* harness.session.respondToRuntimeRequest({
          requestId: approvalRequest!.id,
          decision: "decline",
        });
        yield* harness.session.respondToRuntimeRequest({
          requestId: mcpInputRequest!.id,
          answers: { approved: "false" },
        });
        assert.deepEqual(yield* Fiber.join(permFiber), {
          outcome: { outcome: "selected", optionId: "deny" },
        });
        assert.deepEqual(yield* Fiber.join(mcpFiber), {
          action: "accept",
          content: { approved: false },
        });
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "pins requiredAsk for post-settle approval-required permission and MCP elicitation even if Turn 2 starts in full-access",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf02-pin-required-ask",
          runtimeMode: "approval-required",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;

        const permFiber = yield* harness
          .requestPermission("call_perm_pinned")
          .pipe(Effect.forkScoped);
        const mcpFiber = yield* harness
          .elicitMcpApproval("mcp_tool_call_approval_pinned")
          .pipe(Effect.forkScoped);
        const offered = yield* Queue.take(harness.continuationOffered);
        assert.isUndefined(permFiber.pollUnsafe());
        assert.isUndefined(mcpFiber.pollUnsafe());

        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(
          harness.makeTurn(2, { continuation: true, runtimeMode: "full-access" }),
        );

        const firstRequest = yield* awaitPendingRuntimeRequest(harness.events);
        const secondRequest = yield* awaitPendingRuntimeRequest(harness.events);
        const approvalRequest = [firstRequest, secondRequest].find(
          (request) => request.kind !== "user_input",
        );
        const mcpInputRequest = [firstRequest, secondRequest].find(
          (request) => request.kind === "user_input",
        );
        assert.isDefined(approvalRequest);
        assert.isDefined(mcpInputRequest);
        assert.isUndefined(permFiber.pollUnsafe());
        assert.isUndefined(mcpFiber.pollUnsafe());

        yield* harness.session.respondToRuntimeRequest({
          requestId: approvalRequest!.id,
          decision: "accept",
        });
        yield* harness.session.respondToRuntimeRequest({
          requestId: mcpInputRequest!.id,
          answers: { approved: "true" },
        });
        assert.deepEqual(yield* Fiber.join(permFiber), {
          outcome: { outcome: "selected", optionId: "allow_once" },
        });
        assert.deepEqual(yield* Fiber.join(mcpFiber), {
          action: "accept",
          content: { approved: true },
        });
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "pins deny disposition for post-settle permission even if Turn 2 starts in full-access",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf02-pin-deny",
          runtimeMode: "approval-required",
          approvalPolicy: "never",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;

        const permFiber = yield* harness
          .requestPermission("call_perm_denied_pinned")
          .pipe(Effect.forkScoped);
        const offered = yield* Queue.take(harness.continuationOffered);
        assert.isUndefined(permFiber.pollUnsafe());

        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(
          harness.makeTurn(2, { continuation: true, runtimeMode: "full-access" }),
        );

        assert.deepEqual(yield* Fiber.join(permFiber), {
          outcome: { outcome: "selected", optionId: "deny" },
        });
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "unblocks awaitRequestTurn and cancels pending permission and MCP elicitation when a queued continuation ends without starting",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf02-on-run-ended-without-starting",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;

        const permFiber = yield* harness
          .requestPermission("call_cancelled_queue")
          .pipe(Effect.forkScoped);
        const mcpFiber = yield* harness
          .elicitMcpApproval("mcp_tool_call_approval_cancelled_queue")
          .pipe(Effect.forkScoped);
        const offered = yield* Queue.take(harness.continuationOffered);
        assert.isDefined(offered.onRunEndedWithoutStarting);
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* offered.onRunEndedWithoutStarting!();

        assert.deepEqual(yield* Fiber.join(permFiber), { outcome: { outcome: "cancelled" } });
        assert.deepEqual(yield* Fiber.join(mcpFiber), { action: "cancel" });
      }).pipe(Effect.provide(layerTest)),
  );
});

describe("AcpAdapterV2 stop and interrupt cancellation (CF-12)", () => {
  it.effect.each([
    { label: "hard Stop in full-access", runtimeMode: "full-access", hardStop: true },
    { label: "soft interrupt in full-access", runtimeMode: "full-access", hardStop: false },
    { label: "hard Stop in approval-required", runtimeMode: "approval-required", hardStop: true },
    {
      label: "soft interrupt in approval-required",
      runtimeMode: "approval-required",
      hardStop: false,
    },
  ] as const)(
    "immediately cancels incoming permission, MCP, URL, and user-input requests after $label on an unsettled turn",
    ({ label, runtimeMode, hardStop }) =>
      Effect.gen(function* () {
        let urlElicitationCalls = 0;
        const harness = yield* makeHarness({
          name: `cf12-${runtimeMode}-${hardStop ? "hard" : "soft"}`,
          runtimeMode,
          onUrlElicitation: () =>
            Effect.sync(() => {
              urlElicitationCalls += 1;
              return true;
            }),
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        let providerTurnId:
          | Extract<
              ProviderAdapter.ProviderAdapterV2Event,
              { type: "provider_turn.updated" }
            >["providerTurn"]["id"]
          | undefined;
        while (providerTurnId === undefined) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "provider_turn.updated" && event.providerTurn.status === "running") {
            providerTurnId = event.providerTurn.id;
          }
        }

        const interruptFiber = yield* harness.session
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId,
            ...(hardStop ? { requestRuntimeRestart: true } : {}),
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.cancelCalled);

        const permOutcome = yield* harness.requestPermission("call_after_interrupt");
        const mcpOutcome = yield* harness.elicitMcpApproval(
          "mcp_tool_call_approval_after_interrupt",
        );
        const urlOutcome = yield* harness.elicitUrl("url_after_interrupt");
        const formOutcome = yield* harness.elicitFormQuestion("form_after_interrupt");

        assert.deepEqual(permOutcome, { outcome: { outcome: "cancelled" } }, label);
        assert.deepEqual(mcpOutcome, { action: "cancel" }, label);
        assert.deepEqual(urlOutcome, { action: "cancel" }, label);
        assert.deepEqual(formOutcome, { action: "cancel" }, label);
        assert.equal(urlElicitationCalls, 0, label);

        yield* Deferred.succeed(harness.promptGate, { stopReason: "cancelled" });
        yield* Fiber.join(interruptFiber);

        const remainingEvents: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        while (true) {
          const event = yield* Queue.take(harness.events);
          remainingEvents.push(event);
          if (event.type === "turn.terminal") break;
        }
        assert.isFalse(
          remainingEvents.some((event) => event.type === "runtime_request.updated"),
          `${label} must not emit pending runtime request cards`,
        );
      }).pipe(Effect.provide(layerTest)),
  );
});
