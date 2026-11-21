import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ChatFileAttachment,
  GoogleAccountVerificationUrl,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import { htmlRenderFromToolItem } from "@t3tools/shared/toolOutput";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as EffectAcpErrors from "effect-acp/errors";

import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { makeAntigravityAcpRuntime } from "../../provider/acp/AntigravityAcpSupport.ts";
import { normalizeAntigravitySessionUpdate } from "../../provider/acp/AntigravityProtocol.ts";
import { ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE } from "../../provider/antigravityAuthSupport.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import {
  makeAntigravityAcpAdapterFlavor,
  makeAntigravityAdapterV2,
  type AntigravityAdapterV2RuntimeInput,
} from "./AntigravityAdapterV2.ts";

const flavor = makeAntigravityAcpAdapterFlavor({
  instanceId: ProviderInstanceId.make("antigravity-test"),
  random: undefined as never,
  fileSystem: undefined as never,
  path: undefined as never,
  host: undefined as never,
  selfInvocation: undefined as never,
  makeRuntime: () => Effect.die("not spawned in this test"),
  withProcess: (_stop, task) => task,
  defaultModel: Effect.undefined,
});

function permissionRequest(
  toolCallId: string,
  options: ReadonlyArray<EffectAcpSchema.PermissionOption>,
): EffectAcpSchema.RequestPermissionRequest {
  return {
    sessionId: "session-1",
    options,
    toolCall: { toolCallId, title: "Which branch?" },
  };
}

describe("AntigravityAdapterV2 flavor", () => {
  it("keeps a successful subagent launch running until the parent turn becomes idle", () => {
    const batch = flavor.extractSubagentUpdate?.({
      toolCallId: "trajectory:4",
      title: "Running start_subagent",
      kind: "other",
      status: "completed",
      data: { rawOutput: "Started two agents." },
    });
    assert.equal(batch?.status, "running");
    assert.equal(batch?.prompt, "Started two agents.");
    assert.isNull(batch?.result);
    assert.isTrue(flavor.subagentsIdleOnTurnCompletion);

    // Permission prompts carry the launch; later updates for the same call do not.
    const launch = flavor.extractSubagentUpdate?.({
      toolCallId: "35bea692fa95447fbdae6333fec5515f",
      title: "Run invoke_subagent?",
      kind: "other",
      status: "pending",
      data: {
        rawInput: {
          Subagents: [{ TypeName: "research", Role: "Readme Reader", Prompt: "Read README.md" }],
        },
      },
    });
    assert.equal(launch?.title, "Readme Reader");
    assert.equal(launch?.prompt, "Read README.md");
  });

  it("routes interaction_* permission requests to the question card", () => {
    const question = flavor.extractPermissionQuestion?.(
      permissionRequest("interaction_1", [
        { optionId: "main", name: "main", kind: "allow_once" },
        { optionId: "dev", name: "dev", kind: "allow_once" },
      ]),
    );
    assert.isDefined(question);
    assert.deepEqual(
      question?.question.options.map((option) => option.label),
      ["main", "dev"],
    );
    assert.deepEqual(question?.respond({ interaction_1: "dev" }), {
      outcome: { outcome: "selected", optionId: "dev" },
    });
    assert.isUndefined(question?.respond({ interaction_1: "nope" }));
    assert.isUndefined(
      flavor.extractPermissionQuestion?.(
        permissionRequest("tool_1", [{ optionId: "allow", name: "Allow", kind: "allow_once" }]),
      ),
    );
  });

  it("keeps the question card to the agent's own options so a typed answer cannot be sent", () => {
    const question = flavor.extractPermissionQuestion?.(
      permissionRequest("interaction_2", [
        { optionId: "opt-main", name: "main", kind: "allow_once" },
        { optionId: "opt-dev", name: "dev", kind: "allow_once" },
      ]),
    );
    assert.strictEqual(question?.question.allowCustomAnswer, false);
    assert.deepEqual(
      question?.question.options.map((option) => option.value),
      ["opt-main", "opt-dev"],
    );
    assert.deepEqual(question?.respond({ interaction_2: "opt-dev" }), {
      outcome: { outcome: "selected", optionId: "opt-dev" },
    });
  });
});

const layerSession = Layer.mergeAll(
  NodeServices.layer,
  NodeCrypto.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  ),
);

describe("AntigravityAdapterV2 compaction", () => {
  it.live.each([false, true])(
    "negotiates native goals (%s) without enabling manual compaction",
    (nativeGoals) =>
      Effect.gen(function* () {
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const path = yield* Path.Path;
        const crypto = yield* SecureRandom.SecureRandom;
        const mockAgentPath = yield* path.fromFileUrl(
          new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
        );
        const instanceId = ProviderInstanceId.make("antigravity-compaction-test");
        const goalSupport = yield* Ref.make(false);
        const startupMilestone = yield* Deferred.make<void>();
        const consumeStartupCommands = yield* Deferred.make<void>();
        const adapter = yield* makeAntigravityAdapterV2({
          instanceId,
          selfInvocation: yield* resolveSelfInvocation(),
          makeRuntime: (input) =>
            makeAntigravityAcpRuntime({
              ...input,
              childProcessSpawner,
              spawn: {
                command: process.execPath,
                args: [mockAgentPath],
                cwd: input.cwd,
                env: { T3_ACP_ANTIGRAVITY: "1" },
              },
            }).pipe(
              Effect.provideService(SecureRandom.SecureRandom, crypto),
              Effect.map((runtime) => ({
                ...runtime,
                drainEvents: Deferred.succeed(startupMilestone, undefined).pipe(
                  Effect.andThen(runtime.drainEvents),
                ),
              })),
            ),
          withProcess: (_stop, task) => task,
          defaultModel: Effect.undefined,
          supportsNativeGoals: Ref.get(goalSupport).pipe(
            Effect.tap(() => Deferred.succeed(startupMilestone, undefined)),
          ),
          onSessionEvent: (event) =>
            event._tag === "AvailableCommandsUpdated"
              ? Deferred.await(consumeStartupCommands).pipe(
                  Effect.andThen(Ref.set(goalSupport, nativeGoals)),
                )
              : Effect.void,
        });
        const opening = yield* adapter
          .openSession({
            threadId: ThreadId.make("thread-antigravity-compaction"),
            providerSessionId: ProviderSessionId.make("provider-session-antigravity-compaction"),
            modelSelection: { instanceId, model: "gemini-test-low" },
            runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: process.cwd(),
            }),
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(startupMilestone);
        yield* Deferred.succeed(consumeStartupCommands, undefined);
        const session = yield* Fiber.join(opening);
        assert.isUndefined(session.compactThread);
        assert.equal(session.providerSession.capabilities.turns.supportsNativeGoals, nativeGoals);
        assert.equal((yield* adapter.getCapabilities()).turns.supportsNativeGoals, nativeGoals);
      }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 session open", () => {
  it.effect.each([
    {
      name: "browser sign-in",
      cause: new EffectAcpErrors.AcpTransportError({
        detail: ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE,
        cause: undefined,
      }),
      requiresSignIn: true,
    },
    {
      name: "ACP authentication required",
      cause: EffectAcpErrors.AcpRequestError.authRequired(),
      requiresSignIn: true,
    },
    {
      name: "transport failure",
      cause: new EffectAcpErrors.AcpTransportError({
        detail: "Connection closed",
        cause: undefined,
      }),
      requiresSignIn: undefined,
    },
  ] as const)("classifies $name during session open", ({ cause, requiresSignIn }) =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("antigravity-sign-in-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: () => Effect.fail(cause),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const threadId = ThreadId.make("thread-antigravity-sign-in");
      const error = yield* adapter
        .openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-antigravity-sign-in"),
          modelSelection: { instanceId, model: "gemini-test-low" },
          runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
          }),
        })
        .pipe(Effect.flip);

      assert.equal(error._tag, "ProviderAdapterOpenSessionError");
      assert.equal(
        error._tag === "ProviderAdapterOpenSessionError" && error.requiresSignIn,
        requiresSignIn,
      );
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 client file system", () => {
  it.effect("serves confined reads and leaves edits to the agent's diff tools", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      // effect-acp keeps the last handler registered per method; so does this.
      let readTextFile: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
      let writeTextFile: Parameters<RuntimeService["handleWriteTextFile"]>[0] | undefined;
      const logs = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-antigravity-log-" });
      const requestLog = path.join(logs, "requests.ndjson");
      const crypto = yield* SecureRandom.SecureRandom;
      const instanceId = ProviderInstanceId.make("antigravity-containment-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1", T3_ACP_REQUEST_LOG_PATH: requestLog },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              handleReadTextFile: (handler) =>
                Effect.sync(() => {
                  readTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleReadTextFile(handler))),
              handleWriteTextFile: (handler) =>
                Effect.sync(() => {
                  writeTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleWriteTextFile(handler))),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-workspace-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-outside-",
      });
      const outsideFile = path.join(outside, "secret.txt");
      yield* fileSystem.writeFileString(outsideFile, "secret");
      const attachment = path.join(host.paths.attachmentsDir, "pasted.txt");
      yield* fileSystem.writeFileString(attachment, "pasted");

      const threadId = ThreadId.make("thread-antigravity-containment");
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-containment"),
        modelSelection,
        runtimePolicy,
      });
      yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
      const initialize = (yield* fileSystem.readFileString(requestLog))
        .trim()
        .split("\n")
        .map(
          (line) =>
            JSON.parse(line) as {
              method?: string;
              params?: { clientCapabilities?: { fs?: unknown }; clientInfo?: { name?: string } };
            },
        )
        .find((request) => request.method === "initialize");
      assert.deepEqual(initialize?.params?.clientCapabilities?.fs, {
        readTextFile: true,
        writeTextFile: false,
      });
      assert.equal(initialize?.params?.clientInfo?.name, "zed");
      assert.isUndefined(writeTextFile);
      if (readTextFile === undefined) {
        return yield* Effect.die("Antigravity sessions must serve client file reads");
      }
      const context = (method: string) => ({ requestId: `test-${method}`, method });

      const insidePath = path.join(workspace, "src", "inside.ts");
      yield* fileSystem.makeDirectory(path.dirname(insidePath), { recursive: true });
      yield* fileSystem.writeFileString(insidePath, "inside");
      const pasted = yield* readTextFile(
        { sessionId: "mock-session-1", path: attachment },
        context("fs/read_text_file"),
      );
      assert.equal(pasted.content, "pasted");
      yield* fileSystem.writeFileString(insidePath, "\uFEFFfirst\né中🙂\nlast");
      const sliced = yield* readTextFile(
        { sessionId: "mock-session-1", path: insidePath, line: 2, limit: 1 },
        context("fs/read_text_file"),
      );
      assert.equal(sliced.content, "é中🙂");
      yield* fileSystem.writeFileString(insidePath, "inside");
      for (const [name, bytes] of [
        ["image.png", new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])],
        ["data.bin", new Uint8Array([0x61, 0, 0x62])],
      ] as const) {
        const binaryPath = path.join(workspace, name);
        yield* fileSystem.writeFile(binaryPath, bytes);
        const binaryRead = yield* readTextFile(
          { sessionId: "mock-session-1", path: binaryPath },
          context("fs/read_text_file"),
        ).pipe(Effect.flip);
        assert.propertyVal(binaryRead, "code", -32602, `${name} is refused as binary`);
      }

      const outsideRead = yield* readTextFile(
        { sessionId: "mock-session-1", path: outsideFile },
        context("fs/read_text_file"),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(outsideRead));
      const linkPath = path.join(workspace, "linked-secret.txt");
      yield* fileSystem.symlink(outsideFile, linkPath);
      const linkedRead = yield* readTextFile(
        { sessionId: "mock-session-1", path: linkPath },
        context("fs/read_text_file"),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(linkedRead), "a read through an escaping symlink is denied");
      const insideLink = path.join(workspace, "inside-link.ts");
      yield* fileSystem.symlink(insidePath, insideLink);
      const viaInsideLink = yield* readTextFile(
        { sessionId: "mock-session-1", path: insideLink },
        context("fs/read_text_file"),
      );
      assert.equal(viaInsideLink.content, "inside");
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 workspace changes", () => {
  it.effect("confines file requests to the workspace of the turn in progress", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      const promptReceived = yield* Deferred.make<Parameters<RuntimeService["prompt"]>[0]>();
      const host = yield* ProviderHost.ProviderHost;
      const document = ChatFileAttachment.make({
        type: "file",
        id: "native-pdf",
        name: "diagram.pdf",
        mimeType: "application/pdf",
        sizeBytes: 9,
      });
      const documentPath = host.resolveAttachmentPath(document)!;
      yield* fileSystem.writeFileString(documentPath, "%PDF-1.7");
      let readTextFile: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
      const instanceId = ProviderInstanceId.make("antigravity-workspace-change-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1", T3_ACP_HANG_PROMPT_FOREVER: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              prompt: (request) =>
                Deferred.succeed(promptReceived, request).pipe(
                  Effect.andThen(runtime.prompt(request)),
                ),
              start: () =>
                runtime.start().pipe(
                  Effect.map((started) => ({
                    ...started,
                    initializeResult: {
                      ...started.initializeResult,
                      agentCapabilities: {
                        ...started.initializeResult.agentCapabilities,
                        promptCapabilities: { image: true, audio: true, embeddedContext: true },
                      },
                    },
                  })),
                ),
              handleReadTextFile: (handler) =>
                Effect.sync(() => {
                  readTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleReadTextFile(handler))),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const workspaceA = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ag-a-" });
      const workspaceB = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ag-b-" });
      yield* fileSystem.writeFileString(path.join(workspaceA, "a.txt"), "from a");
      yield* fileSystem.writeFileString(path.join(workspaceB, "b.txt"), "from b");
      const policyFor = (cwd: string) =>
        ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd,
        });
      const threadId = ThreadId.make("thread-antigravity-workspace-change");
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-workspace-change"),
        modelSelection,
        runtimePolicy: policyFor(workspaceA),
      });
      const providerThread = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy: policyFor(workspaceA),
      });
      const now = yield* DateTime.now;
      yield* session
        .startTurn({
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: ProjectId.make("project-antigravity-workspace-change"),
            title: "Antigravity workspace change",
            providerInstanceId: instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: workspaceB,
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
          runId: RunId.make("run-antigravity-workspace-change"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("attempt-antigravity-workspace-change"),
          rootNodeId: NodeId.make("node-antigravity-workspace-change"),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("message-antigravity-workspace-change"),
            text: "read b.txt",
            attachments: [document],
          },
          modelSelection,
          runtimePolicy: policyFor(workspaceB),
        })
        .pipe(Effect.forkScoped);
      yield* session.events.pipe(
        Stream.filter((event) => event.type === "provider_turn.updated"),
        Stream.runHead,
      );
      const sent = yield* Deferred.await(promptReceived);
      assert.deepEqual(
        sent.prompt.filter((block) => block.type === "resource_link"),
        [
          {
            type: "resource_link",
            uri: (yield* path.toFileUrl(documentPath)).href,
            name: "diagram.pdf",
            mimeType: "application/pdf",
            size: 8,
          },
        ],
      );
      assert.include(JSON.stringify(sent.prompt), JSON.stringify(documentPath).slice(1, -1));
      if (readTextFile === undefined) {
        return yield* Effect.die("Antigravity sessions must serve client file requests");
      }
      const context = { requestId: "test-read", method: "fs/read_text_file" };
      const fromB = yield* readTextFile(
        { sessionId: "mock-session-1", path: path.join(workspaceB, "b.txt") },
        context,
      );
      assert.equal(fromB.content, "from b");
      const fromA = yield* readTextFile(
        { sessionId: "mock-session-1", path: path.join(workspaceA, "a.txt") },
        context,
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(fromA), "the previous workspace is no longer readable");
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 html_render pages", () => {
  it.effect("shows the page html_render published when the agent reports only a summary", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      let sessionUpdate: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
      const instanceId = ProviderInstanceId.make("antigravity-html-render-test");
      const threadId = ThreadId.make("thread-antigravity-html-render");
      const page = (name: string) => ({
        attachmentId: `${threadId}-${name}-html`,
        title: "Revenue",
        height: 240,
      });
      // Long enough that Antigravity bounds the html the adapter sees.
      const rows = Array.from({ length: 500 }, (_, row) => `<tr><td>Row ${row}</td></tr>`);
      const report = { html: `<table>${rows.join("")}</table>`, title: "Revenue", height: 240 };
      const failed = { ...report, html: "<p>Revenue</p>" };
      const errored = { ...report, html: "<p>Revenue error</p>" };
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              handleSessionUpdate: (handler) =>
                Effect.sync(() => {
                  sessionUpdate = handler;
                }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
              // agy_acp_server 1.3/1.4 without a permission prompt: the call is
              // titled "Running <tool>" and completes with the step's summary
              // in place of the MCP result.
              prompt: () =>
                Effect.gen(function* () {
                  const send = (update: EffectAcpSchema.SessionUpdate) =>
                    sessionUpdate === undefined
                      ? Effect.die("the adapter must handle session updates")
                      : sessionUpdate(
                          normalizeAntigravitySessionUpdate({
                            sessionId: "mock-session-1",
                            update,
                          }),
                        );
                  const calls = [
                    ["published", report],
                    ["unpublished", { ...report, html: "<p>Costs</p>" }],
                    ["failed", failed],
                    ["error", errored],
                  ] as const;
                  for (const [toolCallId, rawInput] of calls) {
                    yield* send({
                      sessionUpdate: "tool_call",
                      toolCallId,
                      title: "Running html_render",
                      kind: "other",
                      status: "in_progress",
                      rawInput,
                    });
                  }
                  yield* mcpSessions.recordHtmlRender(threadId, report, page("report"));
                  yield* mcpSessions.recordHtmlRender(threadId, failed, page("failed"));
                  yield* mcpSessions.recordHtmlRender(threadId, errored, page("error"));
                  for (const [toolCallId, status] of [
                    ["unpublished", "completed"],
                    ["failed", "failed"],
                    ["published", "completed"],
                    ["published", "completed"],
                  ] as const) {
                    yield* send({
                      sessionUpdate: "tool_call_update",
                      toolCallId,
                      status,
                      rawOutput: `Summary of ${toolCallId}`,
                    });
                  }
                  yield* send({
                    sessionUpdate: "tool_call_update",
                    toolCallId: "error",
                    status: "completed",
                    rawOutput: { result: { isError: true, content: [] } },
                  });
                  for (const [name, rawInput] of [
                    ["failed", failed],
                    ["error", errored],
                  ] as const) {
                    const toolCallId = `${name}-retry`;
                    yield* send({
                      sessionUpdate: "tool_call",
                      toolCallId,
                      title: "Running html_render",
                      kind: "other",
                      status: "in_progress",
                      rawInput,
                    });
                    yield* mcpSessions.recordHtmlRender(threadId, rawInput, page(toolCallId));
                    yield* send({
                      sessionUpdate: "tool_call_update",
                      toolCallId: name,
                      status: name === "failed" ? "failed" : "completed",
                      rawOutput:
                        name === "failed"
                          ? "Summary of failed"
                          : { result: { isError: true, content: [] } },
                    });
                    yield* send({
                      sessionUpdate: "tool_call_update",
                      toolCallId,
                      status: "completed",
                      rawOutput: `Summary of ${toolCallId}`,
                    });
                  }
                  yield* mcpSessions.recordHtmlRender(threadId, report, page("replacement"));
                  yield* send({
                    sessionUpdate: "tool_call_update",
                    toolCallId: "published",
                    status: "completed",
                    rawOutput: { htmlRender: page("replacement") },
                  });
                  yield* send({
                    sessionUpdate: "tool_call_update",
                    toolCallId: "published",
                    status: "completed",
                    rawOutput: "Summary of published",
                  });
                  return { stopReason: "end_turn" as const };
                }),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-html-render"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* session.startTurn({
        appThread: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: ProjectId.make("project-antigravity-html-render"),
          title: "Antigravity html_render",
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
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
        runId: RunId.make("run-antigravity-html-render"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("attempt-antigravity-html-render"),
        rootNodeId: NodeId.make("node-antigravity-html-render"),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("message-antigravity-html-render"),
          text: "chart the revenue",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      });
      const events = Array.from(
        yield* session.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        ),
      );
      const tool = (nativeId: string) => {
        const event = events.findLast(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === nativeId,
        );
        if (event?.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool") {
          return undefined;
        }
        const item = event.turnItem;
        return {
          status: item.status,
          toolName: item.toolName,
          page: htmlRenderFromToolItem(item),
          output: item.output,
        };
      };

      assert.deepEqual(tool("published"), {
        status: "completed",
        toolName: "t3-code.html_render",
        page: page("replacement"),
        output: { htmlRender: page("replacement") },
      });
      assert.deepEqual(tool("unpublished"), {
        status: "completed",
        toolName: "t3-code.html_render",
        page: undefined,
        output: "Summary of unpublished",
      });
      assert.deepEqual(tool("failed"), {
        status: "failed",
        toolName: "t3-code.html_render",
        page: undefined,
        output: "Summary of failed",
      });
      assert.deepEqual(tool("error"), {
        status: "completed",
        toolName: "t3-code.html_render",
        page: undefined,
        output: [],
      });
      assert.deepEqual(
        events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.nativeItemRef?.nativeId === "published" &&
          event.turnItem.status === "completed"
            ? [htmlRenderFromToolItem(event.turnItem)]
            : [],
        ),
        [page("report"), page("report"), page("replacement"), page("replacement")],
      );
      for (const name of ["failed-retry", "error-retry"]) {
        assert.deepEqual(tool(name), {
          status: "completed",
          toolName: "t3-code.html_render",
          page: page(name),
          output: { htmlRender: page(name) },
        });
      }
      assert.strictEqual(yield* mcpSessions.takeHtmlRender(threadId, report), undefined);
      assert.strictEqual(yield* mcpSessions.takeHtmlRender(threadId, failed), undefined);
      assert.strictEqual(yield* mcpSessions.takeHtmlRender(threadId, errored), undefined);
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 subagent reads", () => {
  it.effect("completes a read T3 served when the agent later fails it with a step error", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-reads-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-reads-outside-",
      });
      const served = path.join(workspace, "served.ts");
      yield* fileSystem.writeFileString(served, "export {};");
      const denied = path.join(outside, "secret.txt");
      yield* fileSystem.writeFileString(denied, "secret");
      const unseen = path.join(workspace, "unseen.ts");
      const retried = path.join(workspace, "retried.ts");
      const stepError = 'Model produced invalid output. ("model output error")';
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      let sessionUpdate: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
      let readTextFile: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
      const instanceId = ProviderInstanceId.make("antigravity-subagent-reads-test");
      const threadId = ThreadId.make("thread-antigravity-subagent-reads");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              handleSessionUpdate: (handler) =>
                Effect.sync(() => {
                  sessionUpdate = handler;
                }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
              handleReadTextFile: (handler) =>
                Effect.sync(() => {
                  readTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleReadTextFile(handler))),
              // agy_acp_server announces a subagent's client_view_file on the parent session,
              // never completes it, then fails every open call with the next step error.
              prompt: () =>
                Effect.gen(function* () {
                  if (sessionUpdate === undefined || readTextFile === undefined) {
                    return yield* Effect.die("the adapter must handle updates and reads");
                  }
                  const send = sessionUpdate;
                  const handleRead = readTextFile;
                  const update = (update: EffectAcpSchema.SessionUpdate) =>
                    send(
                      normalizeAntigravitySessionUpdate({ sessionId: "mock-session-1", update }),
                    );
                  // The agent's own view_file never reaches T3, so it must not take a served read.
                  const reads = [
                    ["call_native", served, "Running view_file"],
                    ["call_served", served, "Running client_view_file"],
                    ["call_missing", retried, "Running client_view_file"],
                    ["call_retried", retried, "Running client_view_file"],
                    ["call_denied", denied, "Running client_view_file"],
                    ["call_unseen", unseen, "Running client_view_file"],
                  ] as const;
                  for (const [toolCallId, target, title] of reads) {
                    yield* update({
                      sessionUpdate: "tool_call",
                      toolCallId,
                      title,
                      kind: "read",
                      status: "in_progress",
                      rawInput: { absolute_path: target },
                      locations: [{ path: target }],
                    });
                  }
                  const serve = (target: string) =>
                    handleRead(
                      { sessionId: "subagent-conversation", path: target },
                      { requestId: `read-${target}`, method: "fs/read_text_file" },
                    ).pipe(Effect.exit);
                  yield* serve(served);
                  yield* serve(retried);
                  yield* fileSystem.writeFileString(retried, "export {};").pipe(Effect.orDie);
                  yield* serve(retried);
                  yield* serve(denied);
                  for (const [toolCallId] of reads) {
                    yield* update({
                      sessionUpdate: "tool_call_update",
                      toolCallId,
                      status: "failed",
                      rawOutput: stepError,
                    });
                  }
                  return { stopReason: "end_turn" as const };
                }),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-subagent-reads"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* session.startTurn({
        appThread: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: ProjectId.make("project-antigravity-subagent-reads"),
          title: "Antigravity subagent reads",
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: workspace,
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
        runId: RunId.make("run-antigravity-subagent-reads"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("attempt-antigravity-subagent-reads"),
        rootNodeId: NodeId.make("node-antigravity-subagent-reads"),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("message-antigravity-subagent-reads"),
          text: "review the codebase",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      });
      const events = Array.from(
        yield* session.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        ),
      );
      const read = (nativeId: string) => {
        const event = events.findLast(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === nativeId,
        );
        return event?.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"
          ? { status: event.turnItem.status, output: event.turnItem.output }
          : undefined;
      };

      const firstEvent = (nativeId: string, status: string) =>
        events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === nativeId &&
            event.turnItem.status === status,
        );
      // T3 completes a served read itself instead of leaving it running until the flush.
      assert.isAbove(firstEvent("call_served", "completed"), -1);
      assert.isBelow(firstEvent("call_served", "completed"), firstEvent("call_native", "failed"));
      const failed = { status: "failed" as const, output: stepError };
      assert.deepEqual(read("call_served"), { status: "completed", output: undefined });
      assert.deepEqual(read("call_retried"), { status: "completed", output: undefined });
      assert.deepEqual(read("call_native"), failed);
      assert.deepEqual(read("call_missing"), failed);
      assert.deepEqual(read("call_denied"), failed);
      assert.deepEqual(read("call_unseen"), failed);
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 runtime mode ceiling", () => {
  const modesSetFor = (pickedMode: string) =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      const modesSet: Array<string> = [];
      const instanceId = ProviderInstanceId.make("antigravity-ceiling-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              setMode: (modeId) =>
                Effect.sync(() => {
                  modesSet.push(modeId);
                }).pipe(Effect.andThen(runtime.setMode(modeId))),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const threadId = ThreadId.make("thread-antigravity-ceiling");
      yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-ceiling"),
        modelSelection: {
          instanceId,
          model: "gemini-test-low",
          options: [{ id: "_t3/session-mode", value: pickedMode }],
        },
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "approval-required",
          interactionMode: "default",
          cwd: process.cwd(),
          maxRuntimeMode: "auto-accept-edits",
        }),
      });
      return modesSet;
    }).pipe(Effect.provide(layerSession), Effect.scoped);

  it.effect("does not apply a picked session mode above the policy ceiling", () =>
    Effect.gen(function* () {
      assert.deepEqual(yield* modesSetFor("yolo"), ["default"]);
      // A mode under the ceiling is still the user's to pick.
      assert.deepEqual(yield* modesSetFor("auto_edit"), ["default", "auto_edit"]);
    }),
  );
});

describe("AntigravityAdapterV2 session system prompt", () => {
  it.effect.each(["1.4.5", "1.4.1"])(
    "carries PR linking once in the %s session setup or once per turn",
    (agentVersion) =>
      Effect.gen(function* () {
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const crypto = yield* SecureRandom.SecureRandom;
        const mockAgentPath = yield* path.fromFileUrl(
          new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
        );
        const logs = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-antigravity-log-" });
        const requestLog = path.join(logs, "requests.ndjson");
        type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
        const promptReceived = yield* Deferred.make<Parameters<RuntimeService["prompt"]>[0]>();
        const instanceId = ProviderInstanceId.make("antigravity-system-prompt-test");
        const adapter = yield* makeAntigravityAdapterV2({
          instanceId,
          selfInvocation: yield* resolveSelfInvocation(),
          makeRuntime: (input) =>
            makeAntigravityAcpRuntime({
              ...input,
              childProcessSpawner,
              spawn: {
                command: process.execPath,
                args: [mockAgentPath],
                cwd: input.cwd,
                env: {
                  T3_ACP_ANTIGRAVITY: "1",
                  T3_ACP_AGENT_VERSION: agentVersion,
                  T3_ACP_REQUEST_LOG_PATH: requestLog,
                  T3_ACP_HANG_PROMPT_FOREVER: "1",
                },
              },
            }).pipe(
              Effect.provideService(SecureRandom.SecureRandom, crypto),
              Effect.map((runtime): RuntimeService => ({
                ...runtime,
                prompt: (request) =>
                  Deferred.succeed(promptReceived, request).pipe(
                    Effect.andThen(runtime.prompt(request)),
                  ),
              })),
            ),
          withProcess: (_stop, task) => task,
          defaultModel: Effect.undefined,
        });
        const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ag-sp-" });
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: workspace,
        });
        const threadId = ThreadId.make("thread-antigravity-system-prompt");
        const modelSelection = { instanceId, model: "gemini-test-low" } as const;
        const session = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-antigravity-system-prompt"),
          modelSelection,
          runtimePolicy,
        });
        const providerThread = yield* session.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const now = yield* DateTime.now;
        yield* session
          .startTurn({
            appThread: {
              createdBy: "user",
              creationSource: "web",
              id: threadId,
              projectId: ProjectId.make("project-antigravity-system-prompt"),
              title: "Antigravity system prompt",
              providerInstanceId: instanceId,
              modelSelection,
              runtimeMode: "full-access",
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
            runId: RunId.make("run-antigravity-system-prompt"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("attempt-antigravity-system-prompt"),
            rootNodeId: NodeId.make("node-antigravity-system-prompt"),
            providerThread,
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: MessageId.make("message-antigravity-system-prompt"),
              text: "open a PR",
              attachments: [],
            },
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.forkScoped);
        const sent = JSON.stringify((yield* Deferred.await(promptReceived)).prompt);
        const sessionNew = (yield* fileSystem.readFileString(requestLog))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line) as { method?: string; params?: { _meta?: unknown } })
          .find((request) => request.method === "session/new");
        const append = JSON.stringify(sessionNew?.params?._meta ?? null);
        // Identity stays per turn either way.
        assert.include(sent, "through the Antigravity harness, as gemini-test-low");
        if (agentVersion === "1.4.5") {
          assert.include(append, "pull_request_linking");
          assert.notInclude(sent, "<pull_request_linking>");
        } else {
          assert.notInclude(append, "systemPrompt");
          assert.include(sent, "<pull_request_linking>");
        }
      }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 background commands and post-settle wakeup", () => {
  const makeHarness = Effect.fnUntraced(function* (options: {
    readonly threadName: string;
    readonly runtimeMode: "full-access" | "approval-required";
    readonly profileDirectory?: string;
  }) {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const path = yield* Path.Path;
    const crypto = yield* SecureRandom.SecureRandom;
    const mockAgentPath = yield* path.fromFileUrl(
      new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
    );
    type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
    let sessionUpdateHandler: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
    let permissionHandler: Parameters<RuntimeService["handleRequestPermission"]>[0] | undefined;
    let onOutgoingResponse: AntigravityAdapterV2RuntimeInput["onOutgoingResponse"];
    let onTermination: AntigravityAdapterV2RuntimeInput["onTermination"];
    let cancelCount = 0;
    let promptStarted = yield* Deferred.make<void>();
    let promptGate = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
    const continuationRequests: Array<ProviderContinuationRequests.ProviderContinuationRequest> =
      [];
    const continuationOffered =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    const instanceId = ProviderInstanceId.make(`antigravity-bg-${options.threadName}`);
    const adapter = yield* makeAntigravityAdapterV2({
      instanceId,
      selfInvocation: yield* resolveSelfInvocation(),
      ...(options.profileDirectory === undefined
        ? {}
        : { profileDirectory: options.profileDirectory }),
      makeRuntime: (input) => {
        onOutgoingResponse = input.onOutgoingResponse;
        onTermination = input.onTermination;
        return makeAntigravityAcpRuntime({
          ...input,
          childProcessSpawner,
          spawn: {
            command: process.execPath,
            args: [mockAgentPath],
            cwd: input.cwd,
            env: { T3_ACP_ANTIGRAVITY: "1", T3_ACP_HANG_PROMPT_FOREVER: "1" },
          },
        }).pipe(
          Effect.provideService(SecureRandom.SecureRandom, crypto),
          Effect.map((runtime): RuntimeService => ({
            ...runtime,
            cancel: Effect.sync(() => {
              cancelCount += 1;
            }).pipe(Effect.andThen(runtime.cancel)),
            handleSessionUpdate: (handler) =>
              Effect.sync(() => {
                sessionUpdateHandler = handler;
              }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
            handleRequestPermission: (handler) =>
              Effect.sync(() => {
                permissionHandler = handler;
              }).pipe(Effect.andThen(runtime.handleRequestPermission(handler))),
            prompt: () =>
              Deferred.succeed(promptStarted, undefined).pipe(
                Effect.andThen(Deferred.await(promptGate)),
              ),
          })),
        );
      },
      withProcess: (_stop, task) => task,
      defaultModel: Effect.undefined,
      continuationRequests: {
        offer: (request) =>
          Effect.sync(() => {
            continuationRequests.push(request);
          }).pipe(Effect.andThen(Queue.offer(continuationOffered, request)), Effect.asVoid),
      },
    });
    const threadId = ThreadId.make(`thread-antigravity-bg-${options.threadName}`);
    const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
      runtimeMode: options.runtimeMode,
      interactionMode: "default",
      cwd: process.cwd(),
    });
    const modelSelection = { instanceId, model: "gemini-test-low" } as const;
    const session = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make(`provider-session-bg-${options.threadName}`),
      modelSelection,
      runtimePolicy,
    });
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    yield* session.events.pipe(
      Stream.runForEach((event) => Queue.offer(events, event)),
      Effect.forkScoped,
    );
    const providerThread = yield* session.ensureThread({
      threadId,
      modelSelection,
      runtimePolicy,
    });
    const sessionId = providerThread.nativeThreadRef?.nativeId ?? "mock-session-1";
    const now = yield* DateTime.now;
    const makeTurn = (
      ordinal: number,
      turnOptions?: { readonly continuation?: boolean; readonly text?: string },
    ): ProviderAdapter.ProviderAdapterV2TurnInput => ({
      appThread: {
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId: ProjectId.make(`project-bg-${options.threadName}`),
        title: `Antigravity background ${options.threadName}`,
        providerInstanceId: instanceId,
        modelSelection,
        runtimeMode: options.runtimeMode,
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
      runId: RunId.make(`run-bg-${options.threadName}-${ordinal}`),
      runOrdinal: ordinal,
      providerTurnOrdinal: ordinal,
      attemptId: RunAttemptId.make(`attempt-bg-${options.threadName}-${ordinal}`),
      rootNodeId: NodeId.make(`node-bg-${options.threadName}-${ordinal}`),
      providerThread,
      message: {
        createdBy: turnOptions?.continuation ? "agent" : "user",
        creationSource: turnOptions?.continuation ? "provider" : "web",
        messageId: MessageId.make(`message-bg-${options.threadName}-${ordinal}`),
        text:
          turnOptions?.text ??
          (turnOptions?.continuation ? "Background task completed." : "run build"),
        attachments: [],
      },
      modelSelection,
      runtimePolicy,
    });
    return {
      session,
      sessionId,
      providerThread,
      events,
      get promptStarted() {
        return promptStarted;
      },
      get promptGate() {
        return promptGate;
      },
      get cancelCount() {
        return cancelCount;
      },
      resetPrompt: Effect.gen(function* () {
        promptStarted = yield* Deferred.make<void>();
        promptGate = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
      }),
      terminateRuntime: (detail = "Antigravity process exited") =>
        onTermination?.(new EffectAcpErrors.AcpTransportError({ detail, cause: detail })) ??
        Effect.void,
      continuationRequests,
      continuationOffered,
      makeTurn,
      emitUpdate: (update: EffectAcpSchema.SessionUpdate) =>
        sessionUpdateHandler!({ sessionId, update }),
      requestPermission: (
        params: Omit<EffectAcpSchema.RequestPermissionRequest, "sessionId">,
        requestId = "perm-1",
      ) =>
        permissionHandler!(
          { ...params, sessionId },
          { requestId, method: "session/request_permission" },
        ).pipe(Effect.tap(() => onOutgoingResponse?.(requestId) ?? Effect.void)),
    };
  });

  it.effect(
    "keeps a backgrounded run_command awaiting completion, updates it on exit, and continues on post-settle wakeup with permission",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "full-lifecycle",
          runtimeMode: "full-access",
        });
        assert.isDefined(harness.session.hasPendingBackgroundWork);
        assert.isDefined(harness.session.awaitBackgroundTasks);

        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        // Long run_command stays in_progress when Antigravity moves it to background.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_bg_build",
          title: "Running run_command",
          kind: "execute",
          status: "in_progress",
          rawInput: { CommandLine: "vp test && build", WaitMsBeforeAsync: 10000 },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Build is running in background." },
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });

        const turn1Events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        while (true) {
          const event = yield* Queue.take(harness.events);
          turn1Events.push(event);
          if (event.type === "turn.terminal") break;
        }
        const bgItemAtSettle = turn1Events.findLast(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "call_bg_build",
        );
        assert.equal(bgItemAtSettle?.type, "turn_item.updated");
        if (bgItemAtSettle?.type === "turn_item.updated") {
          assert.equal(bgItemAtSettle.turnItem.status, "running");
        }
        const threadAtSettle = turn1Events.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(threadAtSettle?.type, "provider_thread.updated");
        if (threadAtSettle?.type === "provider_thread.updated") {
          assert.deepEqual(threadAtSettle.providerThread.pendingBackgroundTasks, [
            {
              taskId: "call_bg_build",
              kind: "command",
              description: "vp test && build",
              awaited: true,
            },
          ]);
        }
        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);

        // Background command finishes post-settle.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_bg_build",
          status: "completed",
          rawOutput: { combinedOutput: "BUILD OK\n", exitCode: 0 },
        });
        let bgCompleted = false;
        let clearedPending = false;
        while (!bgCompleted || !clearedPending) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "call_bg_build" &&
            event.turnItem.status === "completed"
          ) {
            bgCompleted = true;
            assert.equal(event.turnItem.type, "command_execution");
            if (event.turnItem.type === "command_execution") {
              assert.equal(event.turnItem.output, "BUILD OK\n");
              assert.equal(event.turnItem.exitCode, 0);
            }
          }
          if (
            event.type === "provider_thread.updated" &&
            (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 0
          ) {
            clearedPending = true;
          }
        }
        assert.lengthOf(harness.continuationRequests, 0);

        // Antigravity wakes up: thought -> tool_call (pending) -> permission -> tool completes -> reply.
        yield* harness.emitUpdate({
          sessionUpdate: "agent_thought_chunk",
          content: { type: "text", text: "Build passed, checking git status." },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_wake_git",
          title: "Running run_command",
          kind: "execute",
          status: "pending",
          rawInput: { CommandLine: "git status -s" },
        });
        const offered = yield* Queue.take(harness.continuationOffered);
        assert.lengthOf(harness.continuationRequests, 1);

        // Permission arrives while activeTurn is still null (before Turn 2 attaches).
        const permissionOutcome = yield* harness.requestPermission({
          toolCall: { toolCallId: "call_wake_git", kind: "execute", title: "Run command" },
          options: [
            { optionId: "allow_once", name: "Allow", kind: "allow_once" },
            { optionId: "deny", name: "Deny", kind: "reject_once" },
          ],
        });
        assert.deepEqual(permissionOutcome, {
          outcome: { outcome: "selected", optionId: "allow_once" },
        });

        // Start Turn 2 continuation.
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));

        // Tool runs for 6s (> 3s debounce); Turn 2 must NOT prematurely finalize while tool runs.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_wake_git",
          status: "in_progress",
        });
        yield* TestClock.adjust("6 seconds");
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_wake_git",
          status: "completed",
          rawOutput: { combinedOutput: "clean\n", exitCode: 0 },
        });
        // Model thinks for 6s after tool completion (< 15s post-tool debounce) before replying.
        yield* TestClock.adjust("6 seconds");
        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Build succeeded and git tree is clean." },
        });
        yield* TestClock.adjust("4 seconds");

        const turn2Events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        while (true) {
          const event = yield* Queue.take(harness.events);
          turn2Events.push(event);
          if (event.type === "turn.terminal") break;
        }
        assert.include(JSON.stringify(turn2Events), "Build succeeded and git tree is clean.");
        assert.include(JSON.stringify(turn2Events.at(-1)), '"status":"completed"');
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "does not hold completion for daemon commands, supports t3_background_wait, and wakes while a daemon runs",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "daemon-and-wait",
          runtimeMode: "full-access",
        });
        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_daemon",
          title: "Running run_command",
          kind: "execute",
          status: "in_progress",
          rawInput: { CommandLine: "vp dev", IsDaemon: true },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_tests",
          title: "Running run_command",
          kind: "execute",
          status: "in_progress",
          rawInput: { CommandLine: "vp test", IsDaemon: false },
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });

        let settleTasks: ReadonlyArray<unknown> | undefined;
        while (true) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "provider_thread.updated") {
            settleTasks = event.providerThread.pendingBackgroundTasks;
          }
          if (event.type === "turn.terminal") break;
        }
        assert.deepEqual(settleTasks, [
          { taskId: "call_daemon", kind: "command", description: "vp dev" },
          { taskId: "call_tests", kind: "command", description: "vp test", awaited: true },
        ]);

        // Toggle awaited flags via t3_background_wait (`awaitBackgroundTasks`).
        const cleared = yield* harness.session.awaitBackgroundTasks!({
          providerThread: harness.providerThread,
          taskIds: ["call_tests"],
          awaited: false,
        });
        assert.deepEqual(cleared, { matched: ["call_tests"] });
        const afterClear = yield* Queue.take(harness.events);
        assert.equal(afterClear.type, "provider_thread.updated");
        if (afterClear.type === "provider_thread.updated") {
          assert.deepEqual(afterClear.providerThread.pendingBackgroundTasks, [
            { taskId: "call_daemon", kind: "command", description: "vp dev" },
            { taskId: "call_tests", kind: "command", description: "vp test" },
          ]);
        }

        // Tests finish while daemon keeps running; wake must still offer continuation immediately.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_tests",
          status: "completed",
          rawOutput: { combinedOutput: "ALL PASS\n", exitCode: 0 },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Tests passed while dev server stays up." },
        });
        const offered = yield* Queue.take(harness.continuationOffered);
        assert.isDefined(offered);
        assert.lengthOf(harness.continuationRequests, 1);
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "awaits a continuation turn for approval-required post-settle permission requests without failing",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "approval-wake",
          runtimeMode: "approval-required",
        });
        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        // Antigravity wakes up in approval-required mode and immediately requests permission.
        const permissionFiber = yield* harness
          .requestPermission({
            toolCall: { toolCallId: "call_wake_perm", kind: "execute", title: "Run git status" },
            options: [
              { optionId: "allow_once", name: "Allow", kind: "allow_once" },
              { optionId: "deny", name: "Deny", kind: "reject_once" },
            ],
          })
          .pipe(Effect.forkScoped);

        const offered = yield* Queue.take(harness.continuationOffered);
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));

        let approvalRequestId:
          | Extract<
              ProviderAdapter.ProviderAdapterV2Event,
              { type: "runtime_request.updated" }
            >["runtimeRequest"]["id"]
          | undefined;
        while (approvalRequestId === undefined) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.status === "pending"
          ) {
            approvalRequestId = event.runtimeRequest.id;
          }
        }

        // Wait 6s (> 3s debounce) while user considers approval; Turn 2 must stay open.
        yield* TestClock.adjust("6 seconds");
        yield* harness.session.respondToRuntimeRequest({
          requestId: approvalRequestId,
          decision: "accept",
        });
        const outcome = yield* Fiber.join(permissionFiber);
        assert.deepEqual(outcome, {
          outcome: { outcome: "selected", optionId: "allow_once" },
        });

        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Approved wake finished." },
        });
        yield* TestClock.adjust("4 seconds");
        let terminalStatus: string | undefined;
        while (terminalStatus === undefined) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "turn.terminal") terminalStatus = event.status;
        }
        assert.equal(terminalStatus, "completed");
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "settles cleanly when a turn invokes a native Antigravity subagent without hanging",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "native-subagent-settle",
          runtimeMode: "full-access",
        });
        assert.isTrue(harness.session.backgroundTasksEndWithProcess);
        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "subagent-call-1",
          kind: "other",
          title: "Running invoke_subagent",
          status: "in_progress",
          rawInput: {
            Subagents: [{ Role: "Codebase Researcher", Prompt: "Inspect adapters" }],
          },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "subagent-call-1",
          status: "completed",
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });

        let terminalStatus: string | undefined;
        let sawIdleSubagent = false;
        while (terminalStatus === undefined) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.type === "subagent" &&
            event.turnItem.status === "idle"
          ) {
            sawIdleSubagent = true;
          }
          if (event.type === "turn.terminal") terminalStatus = event.status;
        }
        assert.equal(terminalStatus, "completed");
        assert.isTrue(sawIdleSubagent);
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "stops detached background commands on post-settle Stop, blocks post-Stop auto-approve and late wakes",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "post-settle-stop",
          runtimeMode: "full-access",
        });
        const turn1 = harness.makeTurn(1);
        yield* harness.session.startTurn(turn1).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_stop_bg",
          kind: "execute",
          title: "sleep 60",
          status: "in_progress",
          rawInput: { CommandLine: "sleep 60", WaitMsBeforeAsync: 5000 },
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });

        let turn1Id:
          | Extract<
              ProviderAdapter.ProviderAdapterV2Event,
              { type: "turn.terminal" }
            >["providerTurnId"]
          | undefined;
        while (turn1Id === undefined) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "turn.terminal") turn1Id = event.providerTurnId;
        }
        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);

        // User clicks Stop after Turn 1 settled while background command still runs.
        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });

        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);
        assert.isAtLeast(harness.cancelCount, 1);

        const drained = yield* Queue.takeAll(harness.events);
        const interruptedCmd = drained.find(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted",
        );
        assert.isDefined(interruptedCmd);
        const clearedThread = drained.findLast((event) => event.type === "provider_thread.updated");
        assert.equal(clearedThread?.type, "provider_thread.updated");
        if (clearedThread?.type === "provider_thread.updated") {
          assert.deepEqual(clearedThread.providerThread.pendingBackgroundTasks, []);
          assert.isFalse(clearedThread.pendingBackgroundWork);
        }

        // Late exit + assistant message from the stopped runtime must be quarantined and not wake Turn 2.
        yield* harness.emitUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Late post-stop wake" },
        });
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "cancels post-Stop permission requests in full-access mode when stoppedRunQuarantine is active",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "post-stop-perm-cancel",
          runtimeMode: "full-access",
        });
        const turn1 = harness.makeTurn(1);
        yield* harness.session.startTurn(turn1).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        let turn1Id:
          | Extract<
              ProviderAdapter.ProviderAdapterV2Event,
              { type: "provider_turn.updated" }
            >["providerTurn"]["id"]
          | undefined;
        while (turn1Id === undefined) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "provider_turn.updated" && event.providerTurn.status === "running") {
            turn1Id = event.providerTurn.id;
          }
        }
        yield* Deferred.succeed(harness.promptGate, { stopReason: "cancelled" });
        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn1Id,
          requestRuntimeRestart: true,
        });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        // Permission request arriving while stoppedRunQuarantine is active must cancel, not auto-approve.
        const postStopPerm = yield* harness.requestPermission({
          toolCall: { toolCallId: "call_after_stop", kind: "execute", title: "echo nope" },
          options: [{ optionId: "allow_once", name: "Allow", kind: "allow_once" }],
        });
        assert.deepEqual(postStopPerm, { outcome: { outcome: "cancelled" } });
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "terminalizes Turn 2 as failed when the Antigravity process crashes mid-continuation and preserves exitCode on duplicate updates",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "crash-and-exitcode",
          runtimeMode: "full-access",
        });
        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_exitcode_bg",
          kind: "execute",
          title: "run test",
          status: "in_progress",
          rawInput: { CommandLine: "exit 7", WaitMsBeforeAsync: 5000 },
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        // Background command exits with exitCode 7, then a duplicate completion update arrives without rawOutput.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_exitcode_bg",
          status: "failed",
          rawOutput: { combinedOutput: "boom", exitCode: 7 },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_exitcode_bg",
          status: "failed",
        });

        const exitEvents = yield* Queue.takeAll(harness.events);
        const lastCommandItem = exitEvents.findLast(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.equal(lastCommandItem?.type, "turn_item.updated");
        if (
          lastCommandItem?.type === "turn_item.updated" &&
          lastCommandItem.turnItem.type === "command_execution"
        ) {
          assert.equal(lastCommandItem.turnItem.status, "failed");
          assert.equal(lastCommandItem.turnItem.exitCode, 7);
        }

        // Agent wakes up into Turn 2 and starts a tool, then antigravity-acp crashes.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_turn2_open",
          kind: "execute",
          title: "echo recovery",
          status: "in_progress",
          rawInput: { CommandLine: "echo recovery" },
        });
        const offered = yield* Queue.take(harness.continuationOffered);
        yield* offered.dispatchIfCurrent!(Effect.void);
        yield* harness.session.startTurn(harness.makeTurn(2, { continuation: true }));

        yield* harness.terminateRuntime("antigravity-acp crashed");

        let turn2TerminalStatus: string | undefined;
        while (turn2TerminalStatus === undefined) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "turn.terminal") turn2TerminalStatus = event.status;
        }
        assert.equal(turn2TerminalStatus, "failed");
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "unblocks awaitRequestTurn when continuation is declined via clearIfCurrent and keeps soft-interrupted detached daemons",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "decline-and-soft-interrupt",
          runtimeMode: "approval-required",
        });
        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_daemon_keep",
          kind: "execute",
          title: "npm run dev",
          status: "in_progress",
          rawInput: { CommandLine: "npm run dev", IsDaemon: true },
        });
        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }

        // Mid-turn soft interrupt (requestRuntimeRestart omitted) on Turn 2 must NOT drop Turn 1's daemon.
        yield* harness.resetPrompt;
        const turn2 = harness.makeTurn(2, { text: "next prompt" });
        yield* harness.session.startTurn(turn2).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);
        let turn2Id:
          | Extract<
              ProviderAdapter.ProviderAdapterV2Event,
              { type: "provider_turn.updated" }
            >["providerTurn"]["id"]
          | undefined;
        while (turn2Id === undefined) {
          const event = yield* Queue.take(harness.events);
          if (event.type === "provider_turn.updated" && event.providerTurn.status === "running") {
            turn2Id = event.providerTurn.id;
          }
        }
        yield* Deferred.succeed(harness.promptGate, { stopReason: "cancelled" });
        yield* harness.session.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: turn2Id,
        });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 2
        }
        // Detached daemon is still tracked after soft interrupt.
        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);

        // Post-settle permission request whose continuation offer is declined via clearIfCurrent must cancel promptly.
        const permFiber = yield* harness
          .requestPermission({
            toolCall: { toolCallId: "call_declined_wake", kind: "execute", title: "echo hi" },
            options: [{ optionId: "allow_once", name: "Allow", kind: "allow_once" }],
          })
          .pipe(Effect.forkScoped);
        const declined = yield* Queue.take(harness.continuationOffered);
        yield* declined.clearIfCurrent!();
        const outcome = yield* Fiber.join(permFiber);
        assert.deepEqual(outcome, { outcome: { outcome: "cancelled" } });
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "matches only running commands in mid-turn awaitBackgroundTasks, publishes awaited state, and clears on completion",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          threadName: "midturn-await-bg",
          runtimeMode: "full-access",
        });
        yield* harness.session.startTurn(harness.makeTurn(1)).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.promptStarted);

        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_running_1",
          kind: "execute",
          title: "pnpm build",
          status: "in_progress",
          rawInput: { CommandLine: "pnpm build", IsDaemon: true },
        });
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "call_done_1",
          kind: "execute",
          title: "echo ok",
          status: "completed",
          rawInput: { CommandLine: "echo ok" },
        });
        yield* Queue.takeAll(harness.events);

        const waitResult = yield* harness.session.awaitBackgroundTasks!({
          providerThread: harness.providerThread,
          taskIds: ["call_running_1", "call_done_1"],
          awaited: true,
        });
        assert.deepEqual(waitResult, { matched: ["call_running_1"] });

        const afterAwait = yield* Queue.takeAll(harness.events);
        const awaitedThread = afterAwait.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(awaitedThread?.type, "provider_thread.updated");
        if (awaitedThread?.type === "provider_thread.updated") {
          assert.deepEqual(awaitedThread.providerThread.pendingBackgroundTasks, [
            {
              taskId: "call_running_1",
              kind: "command",
              description: "pnpm build",
              awaited: true,
            },
          ]);
        }

        // Finishing the command mid-turn clears its override and updates pendingBackgroundTasks.
        yield* harness.emitUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "call_running_1",
          status: "completed",
          rawOutput: { combinedOutput: "built", exitCode: 0 },
        });
        const afterFinish = yield* Queue.takeAll(harness.events);
        const clearedThread = afterFinish.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(clearedThread?.type, "provider_thread.updated");
        if (clearedThread?.type === "provider_thread.updated") {
          assert.deepEqual(clearedThread.providerThread.pendingBackgroundTasks, []);
        }

        yield* Deferred.succeed(harness.promptGate, { stopReason: "end_turn" });
        while ((yield* Queue.take(harness.events)).type !== "turn.terminal") {
          // drain Turn 1
        }
      }).pipe(Effect.provide(layerSession)),
  );

  it("classifies Antigravity RPC quota errors via flavor.promptFailure and excludes VALIDATION_REQUIRED", () => {
    const quota = flavor.promptFailure?.(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Individual quota reached",
      }),
    );
    assert.equal(quota?.class, "usage_limit");

    const permanent = flavor.promptFailure?.(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "VALIDATION_REQUIRED",
      }),
    );
    assert.equal(permanent?.class, "permission_error");
    assert.equal(permanent?.retryable, false);
  });

  it.effect(
    "invokes onVerificationRequired with validated verificationUrl when a turn fails with VALIDATION_REQUIRED",
    () =>
      Effect.gen(function* () {
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const path = yield* Path.Path;
        const crypto = yield* SecureRandom.SecureRandom;
        const mockAgentPath = yield* path.fromFileUrl(
          new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
        );
        const verificationUrl = GoogleAccountVerificationUrl.make(
          "https://accounts.google.com/v3/signin/continue?plt=adapter-turn-1",
        );
        const notified = yield* Deferred.make<{
          readonly verificationUrl?: GoogleAccountVerificationUrl;
          readonly message?: string;
        }>();
        const instanceId = ProviderInstanceId.make("antigravity-verification-test");
        const adapter = yield* makeAntigravityAdapterV2({
          instanceId,
          selfInvocation: yield* resolveSelfInvocation(),
          onVerificationRequired: (input) => Deferred.succeed(notified, input).pipe(Effect.asVoid),
          makeRuntime: (input) =>
            makeAntigravityAcpRuntime({
              ...input,
              childProcessSpawner,
              spawn: {
                command: process.execPath,
                args: [mockAgentPath],
                cwd: input.cwd,
                env: { T3_ACP_ANTIGRAVITY: "1" },
              },
            }).pipe(
              Effect.provideService(SecureRandom.SecureRandom, crypto),
              Effect.map((runtime) => ({
                ...runtime,
                prompt: () =>
                  Effect.fail(
                    new EffectAcpErrors.AcpRequestError({
                      code: -32603,
                      errorMessage: `PERMISSION_DENIED (code 403): ${JSON.stringify({
                        error: {
                          code: 403,
                          status: "PERMISSION_DENIED",
                          details: [
                            {
                              "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                              reason: "VALIDATION_REQUIRED",
                              metadata: {
                                validation_url: verificationUrl,
                                validation_error_message:
                                  "Verify your Google account to continue using Antigravity.",
                              },
                            },
                          ],
                        },
                      })}`,
                    }),
                  ),
              })),
            ),
          withProcess: (_stop, task) => task,
          defaultModel: Effect.undefined,
        });
        const threadId = ThreadId.make("thread-antigravity-verification");
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
        });
        const modelSelection = { instanceId, model: "gemini-test-low" } as const;
        const session = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-antigravity-verification"),
          modelSelection,
          runtimePolicy,
        });
        const providerThread = yield* session.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const now = yield* DateTime.now;
        const terminalFiber = yield* session.events.pipe(
          Stream.filter((event) => event.type === "turn.terminal"),
          Stream.runHead,
          Effect.forkScoped,
        );
        yield* session.startTurn({
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: ProjectId.make("project-antigravity-verification"),
            title: "Antigravity verification",
            providerInstanceId: instanceId,
            modelSelection,
            runtimeMode: "full-access",
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
          runId: RunId.make("run-antigravity-verification"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("attempt-antigravity-verification"),
          rootNodeId: NodeId.make("node-antigravity-verification"),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("message-antigravity-verification"),
            text: "hello",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        const notice = yield* Deferred.await(notified);
        assert.deepEqual(notice, {
          verificationUrl,
          message: "Verify your Google account to continue using Antigravity.",
        });
        const terminal = yield* Fiber.join(terminalFiber);
        assert.equal(terminal._tag, "Some");
        if (terminal._tag === "Some" && terminal.value.type === "turn.terminal") {
          assert.equal(terminal.value.status, "failed");
          assert.equal(terminal.value.failure?.class, "permission_error");
          assert.equal(terminal.value.failure?.code, "VALIDATION_REQUIRED");
          assert.equal(terminal.value.failure?.retryable, false);
          assert.equal(terminal.value.failure?.verificationUrl, verificationUrl);
        }
      }).pipe(Effect.provide(layerSession)),
  );
});
