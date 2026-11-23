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
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as EffectAcpErrors from "effect-acp/errors";

import type * as AcpSessionRuntime from "./AcpSessionRuntime.ts";
import { AcpProviderCapabilitiesV2, makeAcpAdapterV2, type AcpAdapterV2Flavor } from "./adapter.ts";

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
  readonly sandboxPolicy?: unknown;
  readonly cwd?: string;
  readonly enablePostSettleContinuation?: boolean;
  readonly deferFinalizeForBackgroundWork?: boolean;
  readonly detachOpenBackgroundCommandsOnSettle?: boolean;
  readonly isBackgroundCommandTool?: AcpAdapterV2Flavor["isBackgroundCommandTool"];
  readonly cancelEffect?: Effect.Effect<void, EffectAcpErrors.AcpError>;
  readonly cancelResolvesPrompt?: boolean;
  readonly loadSession?: boolean;
  readonly canReadThreadSnapshot?: boolean;
  readonly restartRuntimeAfterInterrupt?: boolean;
  readonly clientFileSystem?: AcpAdapterV2Flavor["clientFileSystem"];
  readonly clientTerminals?: {
    readonly environment?: NodeJS.ProcessEnv;
    readonly shellCommands?: boolean;
  };
  readonly loadSessionStarted?: Deferred.Deferred<void>;
  readonly loadSessionGate?: Deferred.Deferred<void>;
  readonly loadSessionError?: EffectAcpErrors.AcpError;
  readonly sessionScope?: Scope.Scope;
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
  let sessionUpdateHandler: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
  let readTextFileHandler: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
  let writeTextFileHandler: Parameters<RuntimeService["handleWriteTextFile"]>[0] | undefined;
  let createTerminalHandler: Parameters<RuntimeService["handleCreateTerminal"]>[0] | undefined;
  let terminalOutputHandler: Parameters<RuntimeService["handleTerminalOutput"]>[0] | undefined;
  let terminalWaitForExitHandler:
    | Parameters<RuntimeService["handleTerminalWaitForExit"]>[0]
    | undefined;
  let onOutgoingResponse: ((requestId: string) => Effect.Effect<void>) | undefined;
  let promptStarted = yield* Deferred.make<void>();
  let promptGate = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
  let cancelCalled = yield* Deferred.make<void>();
  let runtimeScopeCloseCount = 0;
  let runtimeCreateCount = 0;
  const continuationOffered =
    yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();

  const startResult: AcpSessionRuntime.AcpSessionRuntimeStartResult = {
    sessionId,
    initializeResult: {
      protocolVersion: 1,
      agentCapabilities: options.loadSession === true ? { loadSession: true } : {},
    },
    sessionSetupResult: {},
    modelConfigId: undefined,
  };

  const makeMockRuntime = (
    runtimeInput: Parameters<Parameters<typeof makeAcpAdapterV2>[0]["flavor"]["makeRuntime"]>[0],
  ) => {
    runtimeCreateCount += 1;
    onOutgoingResponse = runtimeInput.onOutgoingResponse;
    const unsupported = (method: string) =>
      Effect.fail(EffectAcpErrors.AcpRequestError.methodNotFound(method));
    return Effect.acquireRelease(
      Effect.succeed({
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
        handleReadTextFile: (handler) =>
          Effect.sync(() => {
            readTextFileHandler = handler;
          }),
        handleWriteTextFile: (handler) =>
          Effect.sync(() => {
            writeTextFileHandler = handler;
          }),
        handleCreateTerminal: (handler) =>
          Effect.sync(() => {
            createTerminalHandler = handler;
          }),
        handleTerminalOutput: (handler) =>
          Effect.sync(() => {
            terminalOutputHandler = handler;
          }),
        handleTerminalWaitForExit: (handler) =>
          Effect.sync(() => {
            terminalWaitForExitHandler = handler;
          }),
        handleTerminalKill: () => Effect.void,
        handleTerminalRelease: () => Effect.void,
        handleSessionUpdate: (handler) =>
          Effect.sync(() => {
            sessionUpdateHandler = handler;
          }),
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
        loadSession: (requestedSessionId) =>
          Effect.gen(function* () {
            if (options.loadSessionStarted !== undefined) {
              yield* Deferred.succeed(options.loadSessionStarted, undefined);
            }
            if (options.loadSessionGate !== undefined) {
              yield* Deferred.await(options.loadSessionGate);
            }
            if (options.loadSessionError !== undefined) {
              return yield* options.loadSessionError;
            }
            return { ...startResult, sessionId: requestedSessionId };
          }),
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
        cancel: Effect.suspend(() =>
          Deferred.succeed(cancelCalled, undefined).pipe(
            Effect.andThen(
              options.cancelResolvesPrompt === true
                ? Deferred.succeed(promptGate, { stopReason: "cancelled" }).pipe(Effect.asVoid)
                : Effect.void,
            ),
            Effect.andThen(options.cancelEffect ?? Effect.void),
          ),
        ),
        setMode: () => Effect.succeed({}),
        setSessionModel: () => Effect.succeed({}),
        setConfigOption: () => Effect.succeed({ configOptions: [] }),
        setModel: () => Effect.void,
        request: (method) => unsupported(method),
        notify: () => Effect.void,
      } satisfies RuntimeService),
      () =>
        Effect.sync(() => {
          runtimeScopeCloseCount += 1;
        }),
    );
  };

  const instanceId = ProviderInstanceId.make(`acp-adapter-${options.name}`);
  const adapter = yield* makeAcpAdapterV2({
    instanceId,
    flavor: {
      driver: TEST_DRIVER,
      capabilities:
        options.canReadThreadSnapshot === true
          ? {
              ...AcpProviderCapabilitiesV2,
              threads: {
                ...AcpProviderCapabilitiesV2.threads,
                canReadThreadSnapshot: true,
              },
            }
          : AcpProviderCapabilitiesV2,
      makeRuntime: makeMockRuntime,
      ...(options.enablePostSettleContinuation === undefined
        ? {}
        : { enablePostSettleContinuation: options.enablePostSettleContinuation }),
      ...(options.deferFinalizeForBackgroundWork === undefined
        ? {}
        : { deferFinalizeForBackgroundWork: options.deferFinalizeForBackgroundWork }),
      ...(options.detachOpenBackgroundCommandsOnSettle === undefined
        ? {}
        : {
            detachOpenBackgroundCommandsOnSettle: options.detachOpenBackgroundCommandsOnSettle,
          }),
      ...(options.isBackgroundCommandTool === undefined
        ? {}
        : { isBackgroundCommandTool: options.isBackgroundCommandTool }),
      ...(options.restartRuntimeAfterInterrupt === undefined
        ? {}
        : { restartRuntimeAfterInterrupt: options.restartRuntimeAfterInterrupt }),
      ...(options.clientFileSystem === undefined
        ? {}
        : { clientFileSystem: options.clientFileSystem }),
      ...(options.onUrlElicitation === undefined
        ? {}
        : { onUrlElicitation: options.onUrlElicitation }),
    },
    selfInvocation: { command: process.execPath, entrypoint: undefined },
    ...(options.clientTerminals === undefined ? {} : { clientTerminals: options.clientTerminals }),
    ...(options.enablePostSettleContinuation === true
      ? {
          continuationRequests: {
            offer: (request) => Queue.offer(continuationOffered, request).pipe(Effect.asVoid),
          },
        }
      : {}),
  });

  const threadId = ThreadId.make(`thread-acp-${options.name}`);
  const policyFor = (
    runtimeMode: RuntimeMode,
    includeInitialApprovalPolicy = false,
    policyOverrides?: {
      readonly approvalPolicy?: "never" | "on-request" | "untrusted";
      readonly sandboxPolicy?: unknown;
      readonly cwd?: string;
    },
  ) =>
    ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
      runtimeMode,
      interactionMode: "default",
      cwd: policyOverrides?.cwd ?? options.cwd ?? process.cwd(),
      ...((policyOverrides?.approvalPolicy ??
        (includeInitialApprovalPolicy ? options.approvalPolicy : undefined)) !== undefined
        ? {
            approvalPolicy:
              policyOverrides?.approvalPolicy ??
              (includeInitialApprovalPolicy ? options.approvalPolicy : undefined),
          }
        : {}),
      ...((policyOverrides?.sandboxPolicy ??
        (includeInitialApprovalPolicy ? options.sandboxPolicy : undefined)) !== undefined
        ? {
            sandboxPolicy:
              policyOverrides?.sandboxPolicy ??
              (includeInitialApprovalPolicy ? options.sandboxPolicy : undefined),
          }
        : {}),
    });
  const initialPolicy = policyFor(options.runtimeMode, true);
  const modelSelection = { instanceId, model: "default" } as const;
  const openSessionEffect = adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make(`provider-session-acp-${options.name}`),
    modelSelection,
    runtimePolicy: initialPolicy,
  });
  const session =
    options.sessionScope === undefined
      ? yield* openSessionEffect
      : yield* openSessionEffect.pipe(Effect.provideService(Scope.Scope, options.sessionScope));
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
      readonly approvalPolicy?: "never" | "on-request" | "untrusted";
      readonly sandboxPolicy?: unknown;
      readonly cwd?: string;
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
      runtimePolicy: policyFor(mode, turnOptions?.runtimeMode === undefined, {
        ...(turnOptions?.approvalPolicy === undefined
          ? {}
          : { approvalPolicy: turnOptions.approvalPolicy }),
        ...(turnOptions?.sandboxPolicy === undefined
          ? {}
          : { sandboxPolicy: turnOptions.sandboxPolicy }),
        ...(turnOptions?.cwd === undefined ? {} : { cwd: turnOptions.cwd }),
      }),
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
    sessionId,
    threadId,
    providerThread,
    modelSelection,
    initialPolicy,
    events,
    continuationOffered,
    makeTurn,
    settleTurn1,
    requestPermission,
    elicitMcpApproval,
    elicitFormQuestion,
    elicitUrl,
    readTextFile: (filePath: string, reqSessionId = sessionId) =>
      readTextFileHandler!(
        { sessionId: reqSessionId, path: filePath },
        { requestId: `read-${filePath}`, method: "fs/read_text_file" },
      ),
    writeTextFile: (filePath: string, content = "edited", reqSessionId = sessionId) =>
      writeTextFileHandler!(
        { sessionId: reqSessionId, path: filePath, content },
        { requestId: `write-${filePath}`, method: "fs/write_text_file" },
      ),
    createTerminal: (
      command = "echo",
      args: ReadonlyArray<string> = [],
      reqSessionId = sessionId,
    ) =>
      createTerminalHandler!(
        { sessionId: reqSessionId, command, args: [...args] },
        { requestId: `term-create-${command}`, method: "terminal/create" },
      ),
    terminalOutput: (terminalId: string, reqSessionId = sessionId) =>
      terminalOutputHandler!(
        { sessionId: reqSessionId, terminalId },
        { requestId: `term-out-${terminalId}`, method: "terminal/output" },
      ),
    waitForTerminalExit: (terminalId: string, reqSessionId = sessionId) =>
      terminalWaitForExitHandler!(
        { sessionId: reqSessionId, terminalId },
        { requestId: `term-wait-${terminalId}`, method: "terminal/wait_for_exit" },
      ),
    get readTextFileHandler() {
      return readTextFileHandler;
    },
    get writeTextFileHandler() {
      return writeTextFileHandler;
    },
    get createTerminalHandler() {
      return createTerminalHandler;
    },
    emitUpdate: (update: EffectAcpSchema.SessionUpdate) =>
      sessionUpdateHandler!({ sessionId, update }),
    get promptStarted() {
      return promptStarted;
    },
    get promptGate() {
      return promptGate;
    },
    get cancelCalled() {
      return cancelCalled;
    },
    get runtimeScopeCloseCount() {
      return runtimeScopeCloseCount;
    },
    get runtimeCreateCount() {
      return runtimeCreateCount;
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

  it.effect(
    "immediately cancels late full-access permission requests after non-hard-restart Stop when stoppedRunQuarantine is active",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf12-non-hard-stop-quarantine",
          runtimeMode: "full-access",
          cancelResolvesPrompt: true,
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }
        yield* Queue.clear(harness.events);

        const postStopPerm = yield* harness.requestPermission("call_after_non_hard_stop");
        assert.deepEqual(postStopPerm, { outcome: { outcome: "cancelled" } });
        const trailing = yield* Queue.clear(harness.events);
        assert.isFalse(trailing.some((event) => event.type === "runtime_request.updated"));
      }).pipe(Effect.provide(layerTest)),
  );
});

const awaitRunningProviderTurnId = Effect.fnUntraced(function* (
  events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>,
) {
  while (true) {
    const event = yield* Queue.take(events);
    if (event.type === "provider_turn.updated" && event.providerTurn.status === "running") {
      return event.providerTurn.id;
    }
  }
});

describe("AcpAdapterV2 shouldCloseRuntimeOnStop (CF-13)", () => {
  it.effect(
    "closes runtimeScope and finalizes an unsettled turn with an open background command when Stop cancel hangs past 1s",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf13-open-bg-hang",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
          detachOpenBackgroundCommandsOnSettle: true,
          isBackgroundCommandTool: (tool) => tool.kind === "execute",
          cancelEffect: Effect.never,
          loadSession: true,
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_cf13_open_bg",
          title: "sleep 60",
          kind: "execute",
          status: "in_progress",
          rawInput: { CommandLine: "sleep 60", WaitMsBeforeAsync: 5000 },
        });
        yield* Queue.takeAll(harness.events);

        const interruptFiber = yield* harness.session
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: turn1Id,
            requestRuntimeRestart: true,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.cancelCalled);

        yield* TestClock.adjust("1 second");
        assert.isDefined(
          interruptFiber.pollUnsafe(),
          "interruptTurn must settle after the 1s cancel timeout without hitting the 10s interrupt timeout",
        );
        yield* Fiber.join(interruptFiber);

        assert.equal(harness.runtimeScopeCloseCount, 1);
        const stopEvents = yield* Queue.takeAll(harness.events);
        const turn1Terminal = stopEvents.find(
          (event) => event.type === "turn.terminal" && event.providerTurnId === turn1Id,
        );
        assert.equal(turn1Terminal?.type, "turn.terminal");
        if (turn1Terminal?.type === "turn.terminal") {
          assert.equal(turn1Terminal.status, "interrupted");
        }
        const latestThreadUpdate = stopEvents.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(latestThreadUpdate?.type, "provider_thread.updated");
        if (latestThreadUpdate?.type === "provider_thread.updated") {
          assert.deepEqual(latestThreadUpdate.providerThread.pendingBackgroundTasks, []);
        }

        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(2)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        assert.equal(harness.runtimeCreateCount, 2);

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while (true) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "turn.terminal") {
            assert.equal(event.status, "completed");
            break;
          }
        }
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "closes runtimeScope and finalizes an unsettled turn with a prior detached background command when Stop cancel hangs past 1s",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf13-detached-bg-hang",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
          detachOpenBackgroundCommandsOnSettle: true,
          isBackgroundCommandTool: (tool) => tool.kind === "execute",
          cancelEffect: Effect.never,
          loadSession: true,
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_cf13_detached",
          title: "sleep 60",
          kind: "execute",
          status: "in_progress",
          rawInput: { CommandLine: "sleep 60", WaitMsBeforeAsync: 5000 },
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(2)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn2Id = yield* awaitRunningProviderTurnId(harness.events);
        yield* Queue.takeAll(harness.events);

        const interruptFiber = yield* harness.session
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: turn2Id,
            requestRuntimeRestart: true,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.cancelCalled);

        yield* TestClock.adjust("1 second");
        assert.isDefined(
          interruptFiber.pollUnsafe(),
          "interruptTurn must settle after the 1s cancel timeout when detached background commands exist",
        );
        yield* Fiber.join(interruptFiber);

        assert.equal(harness.runtimeScopeCloseCount, 1);
        const stopEvents = yield* Queue.takeAll(harness.events);
        const turn2Terminal = stopEvents.find(
          (event) => event.type === "turn.terminal" && event.providerTurnId === turn2Id,
        );
        assert.equal(turn2Terminal?.type, "turn.terminal");
        if (turn2Terminal?.type === "turn.terminal") {
          assert.equal(turn2Terminal.status, "interrupted");
        }
        const latestThreadUpdate = stopEvents.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(latestThreadUpdate?.type, "provider_thread.updated");
        if (latestThreadUpdate?.type === "provider_thread.updated") {
          assert.deepEqual(latestThreadUpdate.providerThread.pendingBackgroundTasks, []);
        }

        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(3)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        assert.equal(harness.runtimeCreateCount, 2);

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while (true) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "turn.terminal") {
            assert.equal(event.status, "completed");
            break;
          }
        }
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "emits turn.terminal interrupted exactly once when prompt settles within the 1s cancel window",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf13-settles-within-cancel",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
          detachOpenBackgroundCommandsOnSettle: true,
          isBackgroundCommandTool: (tool) => tool.kind === "execute",
          cancelResolvesPrompt: true,
          cancelEffect: Effect.sleep("100 millis"),
          loadSession: true,
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_cf13_fast_settle",
          title: "sleep 60",
          kind: "execute",
          status: "in_progress",
          rawInput: { CommandLine: "sleep 60", WaitMsBeforeAsync: 5000 },
        });
        yield* Queue.takeAll(harness.events);

        const interruptFiber = yield* harness.session
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: turn1Id,
            requestRuntimeRestart: true,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.cancelCalled);
        yield* TestClock.adjust("100 millis");
        yield* Fiber.join(interruptFiber);

        assert.equal(harness.runtimeScopeCloseCount, 1);

        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(2)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        assert.equal(harness.runtimeCreateCount, 2);

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        const trailingEvents: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        while (true) {
          const event = yield* Queue.take(harness.events);
          trailingEvents.push(event);
          if (event.type === "turn.terminal" && event.providerTurnId !== turn1Id) {
            assert.equal(event.status, "completed");
            break;
          }
        }

        const turn1Terminals = trailingEvents.filter(
          (event) => event.type === "turn.terminal" && event.providerTurnId === turn1Id,
        );
        assert.lengthOf(turn1Terminals, 1);
        if (turn1Terminals[0]?.type === "turn.terminal") {
          assert.equal(turn1Terminals[0].status, "interrupted");
        }
      }).pipe(Effect.provide(layerTest)),
  );
});

describe("AcpAdapterV2 bounded wakeBuffer (CF-16)", () => {
  const drainTurnEvents = Effect.fnUntraced(function* (
    events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>,
  ) {
    const collected: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
    while (true) {
      const event = yield* Queue.take(events);
      collected.push(event);
      if (event.type === "turn.terminal") {
        return collected;
      }
    }
  });

  it.effect(
    "bounds 2,000 post-settle thought chunks, keeps hasPendingBackgroundWork false without offers, and replays coalesced reasoning with a later offering message",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf16-thought-flood",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;
        assert.isDefined(harness.session.hasPendingBackgroundWork);

        for (let index = 0; index < 2_000; index += 1) {
          const text = `${String(index).padStart(4, "0")}:${"t".repeat(1_019)}`;
          yield* harness.emitUpdate({
            sessionUpdate: "agent_thought_chunk",
            content: { type: "text", text },
          });
        }

        assert.isTrue(Option.isNone(yield* Queue.poll(harness.continuationOffered)));
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);

        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "FINAL_OFFER_AFTER_FLOOD" },
        });

        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);
        const offered = yield* Queue.take(harness.continuationOffered);
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));
        yield* TestClock.adjust("3 seconds");

        const turn2Events = yield* drainTurnEvents(harness.events);
        const reasoningUpdates = turn2Events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "reasoning"
            ? [event.turnItem]
            : [],
        );
        const assistantUpdates = turn2Events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? [event.turnItem]
            : [],
        );

        assert.isAbove(reasoningUpdates.length, 0);
        const firstReasoning = reasoningUpdates[0]!;
        const finalReasoning = reasoningUpdates[reasoningUpdates.length - 1]!;
        assert.isAbove(firstReasoning.text.length, 1_024);
        assert.isAtMost(firstReasoning.text.length, 65_536);
        assert.isBelow(finalReasoning.text.length, 1_048_576);
        assert.isFalse(finalReasoning.text.includes("0000:"));
        assert.isTrue(finalReasoning.text.includes("1999:"));

        const finalAssistant = assistantUpdates[assistantUpdates.length - 1];
        assert.equal(finalAssistant?.text, "FINAL_OFFER_AFTER_FLOOD");
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "preserves offering tool calls and assistant messages under non-offering thought floods and enforces the 256-entry cap",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf16-offering-preserved",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;
        assert.isDefined(harness.session.hasPendingBackgroundWork);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_cf16_offer_tool",
          title: "Completed background check",
          kind: "read",
          status: "completed",
        });
        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "OFFER_MUST_SURVIVE" },
        });
        const offered = yield* Queue.take(harness.continuationOffered);

        // Distinct messageIds prevent coalescing so the 256-entry bound is also exercised.
        for (let index = 0; index < 300; index += 1) {
          yield* harness.emitUpdate({
            sessionUpdate: "agent_thought_chunk",
            messageId: `thought-${index}`,
            content: { type: "text", text: `entry-${index}:${"z".repeat(4_000)}` },
          });
        }
        yield* harness.emitUpdate({
          sessionUpdate: "agent_thought_chunk",
          messageId: "thought-oversized",
          content: { type: "text", text: "X".repeat(1_200_000) },
        });

        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));
        yield* TestClock.adjust("3 seconds");

        const turn2Events = yield* drainTurnEvents(harness.events);
        const completedToolItems = turn2Events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.nativeItemRef?.nativeId === "call_cf16_offer_tool" &&
          event.turnItem.status === "completed"
            ? [event.turnItem]
            : [],
        );
        const assistantUpdates = turn2Events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? [event.turnItem]
            : [],
        );
        const completedReasoningItems = turn2Events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "reasoning" &&
          event.turnItem.status === "completed"
            ? [event.turnItem]
            : [],
        );

        assert.isAbove(completedToolItems.length, 0);
        assert.equal(assistantUpdates[assistantUpdates.length - 1]?.text, "OFFER_MUST_SURVIVE");
        assert.isAtMost(completedReasoningItems.length, 254);
        assert.isFalse(completedReasoningItems.some((item) => item.text.includes("entry-0:")));
        assert.isTrue(completedReasoningItems.some((item) => item.text.includes("entry-299:")));
        assert.isFalse(completedReasoningItems.some((item) => item.text.includes("XXXX")));
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "splits coalesced text chunks at 65,536 chars so continuation replay never truncates",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "cf16-coalesce-cap",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
        });
        yield* harness.settleTurn1;

        for (let index = 0; index < 100; index += 1) {
          const prefix = index === 0 ? "START_00" : index === 99 ? "END_99" : "";
          const text = `${prefix}${"a".repeat(1_000 - prefix.length)}`;
          yield* harness.emitUpdate({
            sessionUpdate: "agent_message_chunk",
            messageId: "msg-coalesce",
            content: { type: "text", text },
          });
        }

        const offered = yield* Queue.take(harness.continuationOffered);
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));
        yield* TestClock.adjust("3 seconds");

        const turn2Events = yield* drainTurnEvents(harness.events);
        const assistantUpdates = turn2Events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? [event.turnItem]
            : [],
        );

        assert.isAbove(assistantUpdates.length, 0);
        assert.equal(assistantUpdates[0]!.text.length, 65_000);
        const finalAssistant = assistantUpdates[assistantUpdates.length - 1]!;
        assert.equal(finalAssistant.text.length, 100_000);
        assert.isTrue(finalAssistant.text.startsWith("START_00"));
        assert.isTrue(finalAssistant.text.includes("END_99"));
      }).pipe(Effect.provide(layerTest)),
  );
});

const makeControlledSpawnerLayer = () => {
  let childDisposedCount = 0;
  let nextExitDeferred: Deferred.Deferred<ChildProcessSpawner.ExitCode> | undefined;
  const layer = Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make(() =>
      Effect.gen(function* () {
        const exitDeferred =
          nextExitDeferred ?? (yield* Deferred.make<ChildProcessSpawner.ExitCode>());
        nextExitDeferred = undefined;
        const handle = ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(4242),
          exitCode: Deferred.await(exitDeferred),
          isRunning: Deferred.isDone(exitDeferred).pipe(Effect.map((done) => !done)),
          kill: () =>
            Deferred.succeed(exitDeferred, ChildProcessSpawner.ExitCode(143)).pipe(Effect.asVoid),
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            childDisposedCount += 1;
          }).pipe(
            Effect.andThen(
              Deferred.succeed(exitDeferred, ChildProcessSpawner.ExitCode(143)).pipe(Effect.ignore),
            ),
          ),
        );
        return handle;
      }),
    ),
  );
  return {
    layer,
    get childDisposedCount() {
      return childDisposedCount;
    },
    setNextExitDeferred: (deferred: Deferred.Deferred<ChildProcessSpawner.ExitCode>) => {
      nextExitDeferred = deferred;
    },
  };
};

describe("AcpAdapterV2 client policy containment (R2-CF-01)", () => {
  const clientFileSystem: NonNullable<AcpAdapterV2Flavor["clientFileSystem"]> = {
    readTextFile: (request) => Effect.succeed({ content: `read:${request.path}` }),
    writeTextFile: () => Effect.succeed({}),
  };

  it.effect(
    "denies terminal/create, fs/read_text_file, and fs/write_text_file while idle post-settle, during an interrupted turn, after Stop under stoppedRunQuarantine, and from a stale runtime generation",
    () =>
      Effect.gen(function* () {
        const spawner = makeControlledSpawnerLayer();
        const harness = yield* makeHarness({
          name: "r2-cf01-deny-states",
          runtimeMode: "full-access",
          clientFileSystem,
          clientTerminals: {},
        }).pipe(Effect.provide(spawner.layer));

        const assertAllDenied = (
          label: string,
          handlers: {
            readonly createTerminal: NonNullable<typeof harness.createTerminalHandler>;
            readonly readTextFile: NonNullable<typeof harness.readTextFileHandler>;
            readonly writeTextFile: NonNullable<typeof harness.writeTextFileHandler>;
          },
        ) =>
          Effect.gen(function* () {
            const createExit = yield* handlers
              .createTerminal(
                { sessionId: harness.sessionId, command: "echo", args: [] },
                { requestId: `term-${label}`, method: "terminal/create" },
              )
              .pipe(Effect.exit);
            const readExit = yield* handlers
              .readTextFile(
                { sessionId: harness.sessionId, path: "inside.ts" },
                { requestId: `read-${label}`, method: "fs/read_text_file" },
              )
              .pipe(Effect.exit);
            const writeExit = yield* handlers
              .writeTextFile(
                { sessionId: harness.sessionId, path: "inside.ts", content: "x" },
                { requestId: `write-${label}`, method: "fs/write_text_file" },
              )
              .pipe(Effect.exit);
            assert.isTrue(Exit.isFailure(createExit), `${label}: terminal/create denied`);
            assert.isTrue(Exit.isFailure(readExit), `${label}: fs/read_text_file denied`);
            assert.isTrue(Exit.isFailure(writeExit), `${label}: fs/write_text_file denied`);
          });

        const initialHandlers = {
          createTerminal: harness.createTerminalHandler!,
          readTextFile: harness.readTextFileHandler!,
          writeTextFile: harness.writeTextFileHandler!,
        };

        // 1) Idle post-settle.
        yield* harness.settleTurn1;
        yield* assertAllDenied("idle-post-settle", initialHandlers);

        // 2) During an interrupted turn (soft interrupt while prompt is still open).
        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(2)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn2Id = yield* awaitRunningProviderTurnId(harness.events);
        const interruptFiber = yield* harness.session
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: turn2Id,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.cancelCalled);
        yield* assertAllDenied("interrupted-turn", initialHandlers);
        yield* Deferred.succeed(harness.promptGate, { stopReason: "cancelled" });
        yield* Fiber.join(interruptFiber);
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 2
        }

        // 3) After Stop under stoppedRunQuarantine.
        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(3)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn3Id = yield* awaitRunningProviderTurnId(harness.events);
        const stopFiber = yield* harness.session
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: turn3Id,
            requestRuntimeRestart: true,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.cancelCalled);
        yield* Deferred.succeed(harness.promptGate, { stopReason: "cancelled" });
        yield* Fiber.join(stopFiber);
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 3
        }
        yield* assertAllDenied("stopped-run-quarantine", initialHandlers);

        // 4) From a stale runtime generation even while a new turn is active on the replacement runtime.
        yield* harness.session.ensureThread({
          threadId: harness.threadId,
          modelSelection: harness.modelSelection,
          runtimePolicy: harness.initialPolicy,
          forceNewNativeThread: true,
        });
        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(4)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        yield* assertAllDenied("stale-generation", initialHandlers);

        const freshRead = yield* harness.readTextFile("inside.ts");
        assert.equal(freshRead.content, "read:inside.ts");

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 4
        }
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "disposes running client terminals on Stop and revokes acceptForSession execute grants",
    () =>
      Effect.gen(function* () {
        const spawner = makeControlledSpawnerLayer();
        const harness = yield* makeHarness({
          name: "r2-cf01-stop-revoke",
          runtimeMode: "approval-required",
          clientTerminals: {},
          cancelResolvesPrompt: true,
        }).pipe(Effect.provide(spawner.layer));

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        const deniedBeforeGrant = yield* harness.createTerminal("sleep").pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(deniedBeforeGrant));

        const permFiber = yield* harness
          .requestPermission("call_grant_exec")
          .pipe(Effect.forkScoped);
        const pending = yield* awaitPendingRuntimeRequest(harness.events);
        yield* harness.session.respondToRuntimeRequest({
          requestId: pending.id,
          decision: "acceptForSession",
        });
        yield* Fiber.join(permFiber);

        const childExit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        spawner.setNextExitDeferred(childExit);
        const created = yield* harness.createTerminal("long-task");
        assert.equal(spawner.childDisposedCount, 0);

        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }
        assert.equal(spawner.childDisposedCount, 1);

        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(2)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        const oldOutputExit = yield* harness.terminalOutput(created.terminalId).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(oldOutputExit));

        const deniedAfterStop = yield* harness.createTerminal("echo").pipe(Effect.flip);
        assert.include(deniedAfterStop.message, "requires approval");

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 2
        }
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "denies fs/write_text_file under approval-required and readOnly during an active turn while allowing it under full-access",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          name: "r2-cf01-write-policy",
          runtimeMode: "approval-required",
          clientFileSystem,
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const askDenied = yield* harness.writeTextFile("src/a.ts", "one").pipe(Effect.flip);
        assert.include(askDenied.message, "requires approval");
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        yield* harness.resetPrompt;
        yield* harness.session
          .startTurn(
            harness.makeTurn(2, {
              runtimeMode: "full-access",
              approvalPolicy: "never",
              sandboxPolicy: { type: "readOnly" },
            }),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const readOnlyDenied = yield* harness.writeTextFile("src/a.ts", "two").pipe(Effect.flip);
        assert.include(readOnlyDenied.message, "does not allow fs/write_text_file");
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 2
        }

        yield* harness.resetPrompt;
        yield* harness.session
          .startTurn(
            harness.makeTurn(3, {
              runtimeMode: "full-access",
              approvalPolicy: "never",
              sandboxPolicy: { type: "dangerFullAccess" },
            }),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const fullAccessWrite = yield* harness.writeTextFile("src/a.ts", "three");
        assert.deepEqual(fullAccessWrite, {});
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 3
        }
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "allows fs/read_text_file while loading an unquarantined session and denies it once idle",
    () =>
      Effect.gen(function* () {
        const loadSessionStarted = yield* Deferred.make<void>();
        const loadSessionGate = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          name: "r2-cf01-load-session-read",
          runtimeMode: "full-access",
          loadSession: true,
          clientFileSystem,
          loadSessionStarted,
          loadSessionGate,
        });

        const resumeFiber = yield* harness.session
          .resumeThread({
            providerThread: {
              ...harness.providerThread,
              nativeThreadRef: {
                driver: TEST_DRIVER,
                nativeId: "loaded-session-r2-cf01",
                strength: "strong",
              },
            },
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(loadSessionStarted);

        const duringLoad = yield* harness.readTextFile("src/loaded.ts", "loaded-session-r2-cf01");
        assert.equal(duringLoad.content, "read:src/loaded.ts");

        yield* Deferred.succeed(loadSessionGate, undefined);
        yield* Fiber.join(resumeFiber);

        const afterLoadIdle = yield* harness
          .readTextFile("src/loaded.ts", "loaded-session-r2-cf01")
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(afterLoadIdle));
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "does not hold runtimeCallbackPermit across terminal/wait_for_exit and fails closed on Stop",
    () =>
      Effect.gen(function* () {
        const spawner = makeControlledSpawnerLayer();
        const harness = yield* makeHarness({
          name: "r2-cf01-wait-permit",
          runtimeMode: "full-access",
          clientTerminals: {},
          cancelResolvesPrompt: true,
        }).pipe(Effect.provide(spawner.layer));

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        const childExit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        spawner.setNextExitDeferred(childExit);
        const created = yield* harness.createTerminal("wait-cmd");
        const waitFiber = yield* harness
          .waitForTerminalExit(created.terminalId)
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        assert.isUndefined(waitFiber.pollUnsafe());

        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "concurrent update while waiting" },
        });
        let sawConcurrentUpdate = false;
        while (!sawConcurrentUpdate) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.text.includes("concurrent update while waiting")
          ) {
            sawConcurrentUpdate = true;
          }
        }

        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        assert.equal(spawner.childDisposedCount, 1);

        const waitExit = yield* Fiber.await(waitFiber);
        assert.isTrue(Exit.isFailure(waitExit));
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "allows post-Stop session/load fs/read_text_file after runtime restart and on non-restarting runtimes while keeping idle reads denied",
    () =>
      Effect.gen(function* () {
        const restartLoadStarted = yield* Deferred.make<void>();
        const restartLoadGate = yield* Deferred.make<void>();
        const restartHarness = yield* makeHarness({
          name: "r2-cf01-post-stop-restart-load",
          runtimeMode: "full-access",
          loadSession: true,
          restartRuntimeAfterInterrupt: true,
          cancelResolvesPrompt: true,
          clientFileSystem,
          loadSessionStarted: restartLoadStarted,
          loadSessionGate: restartLoadGate,
        });

        yield* restartHarness.session.startTurn(restartHarness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(restartHarness.promptStarted);
        const restartTurn1Id = yield* awaitRunningProviderTurnId(restartHarness.events);
        yield* restartHarness.session.interruptTurn({
          providerThread: restartHarness.providerThread,
          providerTurnId: restartTurn1Id,
          requestRuntimeRestart: true,
        });
        while ((yield* Queue.take(restartHarness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        const staleReadHandler = restartHarness.readTextFileHandler!;
        assert.isTrue(
          Exit.isFailure(
            yield* restartHarness.readTextFile("src/quarantined.ts").pipe(Effect.exit),
          ),
        );

        const restartResumeFiber = yield* restartHarness.session
          .resumeThread({ providerThread: restartHarness.providerThread })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(restartLoadStarted);
        assert.equal(restartHarness.runtimeCreateCount, 2);

        const readAfterRestart = yield* restartHarness.readTextFile("src/after-restart.ts");
        assert.equal(readAfterRestart.content, "read:src/after-restart.ts");
        const staleGenReadExit = yield* staleReadHandler(
          { sessionId: restartHarness.sessionId, path: "src/stale.ts" },
          { requestId: "read-stale-gen", method: "fs/read_text_file" },
        ).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(staleGenReadExit));

        yield* Deferred.succeed(restartLoadGate, undefined);
        yield* Fiber.join(restartResumeFiber);
        assert.isTrue(
          Exit.isFailure(
            yield* restartHarness.readTextFile("src/after-restart.ts").pipe(Effect.exit),
          ),
        );

        const nonRestartLoadStarted = yield* Deferred.make<void>();
        const nonRestartLoadGate = yield* Deferred.make<void>();
        const nonRestartHarness = yield* makeHarness({
          name: "r2-cf01-post-stop-no-restart-load",
          runtimeMode: "full-access",
          loadSession: true,
          cancelResolvesPrompt: true,
          clientFileSystem,
          loadSessionStarted: nonRestartLoadStarted,
          loadSessionGate: nonRestartLoadGate,
        });

        yield* nonRestartHarness.session
          .startTurn(nonRestartHarness.makeTurn(1))
          .pipe(Effect.forkScoped);
        yield* Deferred.await(nonRestartHarness.promptStarted);
        const nonRestartTurn1Id = yield* awaitRunningProviderTurnId(nonRestartHarness.events);
        yield* nonRestartHarness.session.interruptTurn({
          providerThread: nonRestartHarness.providerThread,
          providerTurnId: nonRestartTurn1Id,
          requestRuntimeRestart: true,
        });
        while ((yield* Queue.take(nonRestartHarness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        assert.isTrue(
          Exit.isFailure(
            yield* nonRestartHarness.readTextFile("src/quarantined.ts").pipe(Effect.exit),
          ),
        );

        const nonRestartSessionId = "loaded-after-stop-no-restart";
        const nonRestartResumeFiber = yield* nonRestartHarness.session
          .resumeThread({
            providerThread: {
              ...nonRestartHarness.providerThread,
              nativeThreadRef: {
                driver: TEST_DRIVER,
                nativeId: nonRestartSessionId,
                strength: "strong",
              },
            },
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(nonRestartLoadStarted);
        assert.equal(nonRestartHarness.runtimeCreateCount, 1);

        const readDuringNonRestartLoad = yield* nonRestartHarness.readTextFile(
          "src/non-restart-load.ts",
          nonRestartSessionId,
        );
        assert.equal(readDuringNonRestartLoad.content, "read:src/non-restart-load.ts");

        yield* Deferred.succeed(nonRestartLoadGate, undefined);
        yield* Fiber.join(nonRestartResumeFiber);
        assert.isTrue(
          Exit.isFailure(
            yield* nonRestartHarness
              .readTextFile("src/non-restart-load.ts", nonRestartSessionId)
              .pipe(Effect.exit),
          ),
        );
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "disposes client terminals and revokes acceptForSession execute grants when Stop races turn settlement on a non-hard-restart runtime",
    () =>
      Effect.gen(function* () {
        const spawner = makeControlledSpawnerLayer();
        const harness = yield* makeHarness({
          name: "r2-cf01-racing-stop-cleanup",
          runtimeMode: "approval-required",
          clientTerminals: {},
        }).pipe(Effect.provide(spawner.layer));

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        const permFiber = yield* harness
          .requestPermission("call_grant_racing_stop")
          .pipe(Effect.forkScoped);
        const pending = yield* awaitPendingRuntimeRequest(harness.events);
        yield* harness.session.respondToRuntimeRequest({
          requestId: pending.id,
          decision: "acceptForSession",
        });
        yield* Fiber.join(permFiber);

        const childExit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        spawner.setNextExitDeferred(childExit);
        const created = yield* harness.createTerminal("long-task");
        assert.equal(spawner.childDisposedCount, 0);

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }
        assert.equal(spawner.childDisposedCount, 0);

        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        assert.equal(spawner.childDisposedCount, 1);

        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(2)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        assert.isTrue(
          Exit.isFailure(yield* harness.terminalOutput(created.terminalId).pipe(Effect.exit)),
        );
        const deniedAfterStop = yield* harness.createTerminal("echo").pipe(Effect.flip);
        assert.include(deniedAfterStop.message, "requires approval");

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 2
        }
      }).pipe(Effect.provide(layerTest)),
  );
});

describe("AcpAdapterV2 URL elicitation admission and permit release (R2-CF-02)", () => {
  it.effect(
    "cancels idle post-settle URL elicitation on a non-continuation adapter without calling onUrlElicitation",
    () =>
      Effect.gen(function* () {
        let urlCalls = 0;
        const harness = yield* makeHarness({
          name: "r2-cf02-idle-non-cont",
          runtimeMode: "full-access",
          onUrlElicitation: () =>
            Effect.sync(() => {
              urlCalls += 1;
              return true;
            }),
        });
        yield* harness.settleTurn1;

        const outcome = yield* harness.elicitUrl("url-idle-non-cont");
        assert.deepEqual(outcome, { action: "cancel" });
        assert.equal(urlCalls, 0);
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "holds idle post-settle URL elicitation via awaitRequestTurn on a continuation adapter until Turn 2 attaches or Stop cancels",
    () =>
      Effect.gen(function* () {
        let urlCalls = 0;
        const harness = yield* makeHarness({
          name: "r2-cf02-idle-cont",
          runtimeMode: "full-access",
          enablePostSettleContinuation: true,
          deferFinalizeForBackgroundWork: true,
          cancelResolvesPrompt: true,
          onUrlElicitation: () =>
            Effect.sync(() => {
              urlCalls += 1;
              return true;
            }),
        });
        yield* harness.settleTurn1;

        const urlFiber = yield* harness.elicitUrl("url-idle-cont-attach").pipe(Effect.forkScoped);
        const offered = yield* Queue.take(harness.continuationOffered);
        yield* Effect.yieldNow;
        assert.equal(urlCalls, 0);
        assert.isUndefined(urlFiber.pollUnsafe());

        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));
        assert.deepEqual(yield* Fiber.join(urlFiber), { action: "accept" });
        assert.equal(urlCalls, 1);

        yield* TestClock.adjust("3 seconds");
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 2
        }

        // Stop firing while an idle post-settle URL elicitation is awaiting continuation must cancel without calling onUrlElicitation.
        yield* harness.resetPrompt;
        yield* harness.session.startTurn(harness.makeTurn(3)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn3Id = yield* awaitRunningProviderTurnId(harness.events);
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 3
        }

        const urlWhileWaiting = yield* harness
          .elicitUrl("url-idle-cont-stop")
          .pipe(Effect.forkScoped);
        yield* Queue.take(harness.continuationOffered);
        yield* Effect.yieldNow;
        assert.equal(urlCalls, 1);

        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn3Id,
          requestRuntimeRestart: true,
        });
        assert.deepEqual(yield* Fiber.join(urlWhileWaiting), { action: "cancel" });
        assert.equal(urlCalls, 1);
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "releases runtimeCallbackPermit before awaiting active-turn onUrlElicitation and unblocks updates, Stop, and Scope.close",
    () =>
      Effect.gen(function* () {
        const urlStarted = yield* Deferred.make<void>();
        const urlInterrupted = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          name: "r2-cf02-active-stop",
          runtimeMode: "full-access",
          cancelResolvesPrompt: true,
          onUrlElicitation: () =>
            Deferred.succeed(urlStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(urlInterrupted, undefined).pipe(Effect.asVoid),
              ),
            ),
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);

        const urlFiber = yield* harness.elicitUrl("url-active-stop").pipe(Effect.forkScoped);
        yield* Deferred.await(urlStarted);

        // (a) Concurrent session/update proceeds while URL elicitation is pending.
        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "update during url elicitation" },
        });
        let sawUpdate = false;
        while (!sawUpdate) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.text.includes("update during url elicitation")
          ) {
            sawUpdate = true;
          }
        }

        // (b) interruptTurn completes immediately and interrupts onUrlElicitation, returning cancel.
        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        yield* Deferred.await(urlInterrupted);
        assert.deepEqual(yield* Fiber.join(urlFiber), { action: "cancel" });

        // (c) Scope.close(sessionScope) completes immediately and interrupts onUrlElicitation without deadlock.
        const sessionScope = yield* Scope.make();
        const closeUrlStarted = yield* Deferred.make<void>();
        const closeUrlInterrupted = yield* Deferred.make<void>();
        const closeHarness = yield* makeHarness({
          name: "r2-cf02-scope-close",
          runtimeMode: "full-access",
          sessionScope,
          onUrlElicitation: () =>
            Deferred.succeed(closeUrlStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(closeUrlInterrupted, undefined).pipe(Effect.asVoid),
              ),
            ),
        });
        yield* closeHarness.session.startTurn(closeHarness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(closeHarness.promptStarted);

        const closeUrlFiber = yield* closeHarness
          .elicitUrl("url-scope-close")
          .pipe(Effect.forkScoped);
        yield* Deferred.await(closeUrlStarted);

        yield* Scope.close(sessionScope, Exit.void);
        yield* Deferred.await(closeUrlInterrupted);
        yield* Fiber.interrupt(closeUrlFiber);
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "cancels and interrupts an active-turn URL elicitation when the turn settles via finalizeTurn",
    () =>
      Effect.gen(function* () {
        const urlStarted = yield* Deferred.make<void>();
        const urlInterrupted = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          name: "r2-cf02-active-finalize",
          runtimeMode: "full-access",
          onUrlElicitation: () =>
            Deferred.succeed(urlStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(urlInterrupted, undefined).pipe(Effect.asVoid),
              ),
            ),
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        const urlFiber = yield* harness.elicitUrl("url-active-finalize").pipe(Effect.forkScoped);
        yield* Deferred.await(urlStarted);

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        yield* Deferred.await(urlInterrupted);
        assert.deepEqual(yield* Fiber.join(urlFiber), { action: "cancel" });
      }).pipe(Effect.provide(layerTest)),
  );

  it.effect(
    "cancels idle URL elicitation when activeSessionId is null after a failed post-restart activation",
    () =>
      Effect.gen(function* () {
        let urlCalls = 0;
        const loadSessionStarted = yield* Deferred.make<void>();
        const loadSessionGate = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          name: "r2-cf02-failed-activation",
          runtimeMode: "full-access",
          loadSession: true,
          restartRuntimeAfterInterrupt: true,
          cancelResolvesPrompt: true,
          loadSessionStarted,
          loadSessionGate,
          loadSessionError: EffectAcpErrors.AcpRequestError.internalError("load failed"),
          onUrlElicitation: () =>
            Effect.sync(() => {
              urlCalls += 1;
              return true;
            }),
        });

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        const turn1Id = yield* awaitRunningProviderTurnId(harness.events);
        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        const resumeFiber = yield* harness.session
          .resumeThread({ providerThread: harness.providerThread })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(loadSessionStarted);

        assert.deepEqual(yield* harness.elicitUrl("url-during-activation"), { action: "accept" });
        assert.equal(urlCalls, 1);

        yield* Deferred.succeed(loadSessionGate, undefined);
        const resumeExit = yield* Fiber.await(resumeFiber);
        assert.isTrue(Exit.isFailure(resumeExit));

        assert.deepEqual(yield* harness.elicitUrl("url-idle-after-failed-activation"), {
          action: "cancel",
        });
        assert.equal(urlCalls, 1);
      }).pipe(Effect.provide(layerTest)),
  );
});
