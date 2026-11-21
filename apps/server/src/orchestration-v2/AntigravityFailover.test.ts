import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  GoogleAccountVerificationUrl,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { createModelCapabilities } from "@t3tools/shared/model";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettingsService from "../serverSettings.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import * as AntigravityFailover from "./AntigravityFailover.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { AcpProviderCapabilitiesV2 } from "@t3tools/provider-acp/server/adapter";

const ANTIGRAVITY_DRIVER = ProviderDriverKind.make("antigravity");
const NOW = Date.parse("2026-11-21T12:00:00.000Z");
const FUTURE_RESET = "2099-01-01T00:00:00.000Z";
const PROJECT_ID = ProjectId.make("project:antigravity-failover");

const antigravityCapabilities = {
  ...AcpProviderCapabilitiesV2,
  sessions: {
    ...AcpProviderCapabilitiesV2.sessions,
    supportsModelSwitchInSession: true,
    supportsRuntimeModeSwitchInSession: true,
  },
  tools: {
    ...AcpProviderCapabilitiesV2.tools,
    supportsMcpTools: true,
  },
  subagents: {
    ...AcpProviderCapabilitiesV2.subagents,
    supportsSubagents: true,
  },
} as const;

function makeAntigravityModel(
  slug: string,
  thoughtLevels: ReadonlyArray<string> = ["low", "medium", "high"],
  optionId = "thought_level",
): ServerProvider["models"][number] {
  return {
    slug,
    name: slug,
    isCustom: false,
    capabilities: createModelCapabilities({
      optionDescriptors: [
        {
          id: optionId,
          label: "Thought level",
          type: "select",
          role: "reasoning",
          options: thoughtLevels.map((id) => ({ id, label: id })),
        },
      ],
    }),
  };
}

function makeAntigravityProvider(
  id: string,
  overrides: Partial<ServerProvider> = {},
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: ANTIGRAVITY_DRIVER,
    displayName: id,
    enabled: true,
    installed: true,
    version: "1.5.0",
    status: "ready",
    auth: { status: "authenticated", type: "oauth-personal", email: `${id}@example.com` },
    checkedAt: "2026-11-21T11:55:00.000Z",
    supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
    models: [
      makeAntigravityModel("gemini-4-neon", ["low", "medium", "high", "xhigh"]),
      makeAntigravityModel("gemini-4-argon", ["low", "medium", "high"]),
      makeAntigravityModel("claude-opus-5-5", ["low", "medium", "high"]),
    ],
    slashCommands: [],
    skills: [],
    ...overrides,
  };
}

function makeLimits(
  windows: ReadonlyArray<{
    readonly id: string;
    readonly usedPercent: number;
    readonly resetsAt?: string;
  }>,
  checkedAt = "2026-11-21T11:55:00.000Z",
): NonNullable<ServerProvider["usageLimits"]> {
  return {
    checkedAt,
    windows: windows.map((window) => ({
      id: window.id,
      kind: window.id.includes("weekly") ? "weekly" : "session",
      label: window.id,
      usedPercent: window.usedPercent,
      resetsAt: window.resetsAt ?? FUTURE_RESET,
    })),
  };
}

function settingsForProviders(
  providers: ReadonlyArray<ServerProvider>,
  overrides: Partial<ServerSettings> = {},
): ServerSettings {
  const providerInstances: Record<
    string,
    { readonly driver: ProviderDriverKind; readonly enabled: boolean }
  > = {};
  for (const provider of providers) {
    providerInstances[provider.instanceId] = {
      driver: provider.driver,
      enabled: provider.enabled,
    };
  }
  return {
    ...DEFAULT_SERVER_SETTINGS,
    ...overrides,
    providerInstances: {
      ...providerInstances,
      ...overrides.providerInstances,
    },
  };
}

describe("selectAntigravityFailoverCandidate", () => {
  it("picks the account with the lowest usedPercent across 15 accounts and keeps probeFailed idle accounts eligible", () => {
    const accounts: Array<ServerProvider> = [
      makeAntigravityProvider("antigravity", {
        usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 100 }]),
      }),
      makeAntigravityProvider("antigravity_2", {
        usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 85 }]),
      }),
      makeAntigravityProvider("antigravity_3", {
        usageLimits: {
          checkedAt: "2026-11-21T11:55:00.000Z",
          windows: [],
          unavailable: {
            reason: "probeFailed",
            message: "Antigravity's saved sign-in has expired.",
          },
        },
      }),
      makeAntigravityProvider("antigravity_4", {
        usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 100 }]),
      }),
      ...Array.from({ length: 11 }, (_, idx) => {
        const n = idx + 5;
        return makeAntigravityProvider(`antigravity_${n}`, {
          usageLimits: makeLimits([
            { id: "3p-5h", usedPercent: n === 12 ? 15 : 40 + idx },
            { id: "gemini-5h", usedPercent: 100 },
          ]),
        });
      }),
    ];

    const best = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: {
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "claude-opus-5-5",
      },
      providers: accounts,
      settings: settingsForProviders(accounts),
      nowMs: NOW,
    });
    assert.equal(best?.toInstanceId, ProviderInstanceId.make("antigravity_12"));

    // When all confirmed-quota accounts are exhausted, the idle >1h probeFailed account is chosen.
    const allConfirmedExhausted = accounts.map((account) =>
      account.instanceId === ProviderInstanceId.make("antigravity_3")
        ? account
        : { ...account, usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 100 }]) },
    );
    const idleFallback = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: {
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "claude-opus-5-5",
      },
      providers: allConfirmedExhausted,
      settings: settingsForProviders(allConfirmedExhausted),
      nowMs: NOW,
    });
    assert.equal(idleFallback?.toInstanceId, ProviderInstanceId.make("antigravity_3"));
  });

  it("respects global disable, per-project disable, unauthenticated status, and model family isolation", () => {
    const providers = [
      makeAntigravityProvider("antigravity", {
        usageLimits: makeLimits([
          { id: "gemini-argon-wtu-weekly", usedPercent: 100 },
          { id: "gemini-5h", usedPercent: 20 },
        ]),
      }),
      makeAntigravityProvider("antigravity_disabled", { enabled: false }),
      makeAntigravityProvider("antigravity_project_disabled", {
        usageLimits: makeLimits([{ id: "gemini-argon-wtu-weekly", usedPercent: 0 }]),
      }),
      makeAntigravityProvider("antigravity_unauth", {
        auth: { status: "unauthenticated" },
      }),
      makeAntigravityProvider("antigravity_ok", {
        usageLimits: makeLimits([{ id: "gemini-argon-wtu-weekly", usedPercent: 10 }]),
      }),
    ];
    const settings: ServerSettings = settingsForProviders(providers, {
      projectSettingsOverrides: {
        [PROJECT_ID]: {
          providerInstances: {
            [ProviderInstanceId.make("antigravity_project_disabled")]: { enabled: false },
          },
        },
      },
    });

    // gemini-4-neon uses the default gemini bucket (20% used), so preflight stays on antigravity.
    const neonDecision = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: {
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "gemini-4-neon",
      },
      providers,
      settings,
      nowMs: NOW,
    });
    assert.isNull(neonDecision);

    // gemini-4-argon uses gemini-argon (100% used), so preflight skips disabled/unauth accounts and picks antigravity_ok.
    const argonDecision = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: {
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "gemini-4-argon",
        options: [{ id: "thought_level", value: "xhigh" }],
      },
      providers,
      settings,
      nowMs: NOW,
    });
    assert.equal(argonDecision?.toInstanceId, ProviderInstanceId.make("antigravity_ok"));
    // xhigh is downgraded to high because antigravity_ok's gemini-4-argon only advertises low/medium/high.
    assert.deepEqual(argonDecision?.modelSelection.options, [
      { id: "thought_level", value: "high" },
    ]);

    // Also normalizes ACP-native `thinking_level` options identified by `role: "reasoning"`.
    const thinkingLevelTarget = makeAntigravityProvider("antigravity_acp_opt", {
      models: [makeAntigravityModel("gemini-4-argon", ["low", "medium", "high"], "thinking_level")],
      usageLimits: makeLimits([{ id: "gemini-argon-wtu-weekly", usedPercent: 5 }]),
    });
    const acpNormalized = AntigravityFailover.normalizeAntigravityOptionsForCandidate(
      [{ id: "thinking_level", value: "xhigh" }],
      thinkingLevelTarget,
      "gemini-4-argon",
    );
    assert.deepEqual(acpNormalized, [{ id: "thinking_level", value: "high" }]);
  });

  it("tracks runtime cooldown per model family without immediate refresh self-cancellation", () => {
    const providerA = makeAntigravityProvider("antigravity");
    const providerB = makeAntigravityProvider("antigravity_2");
    const cooldowns = new Map([
      [
        AntigravityFailover.antigravityCooldownKey(providerA.instanceId, "claude-opus-5-5"),
        { recordedAtMs: NOW - 10_000, untilMs: NOW + 60_000 },
      ],
    ]);

    const cooldownSettings = settingsForProviders([providerA, providerB]);
    // Claude family is in cooldown on antigravity -> preflight switches to antigravity_2.
    const claudeDecision = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: { instanceId: providerA.instanceId, model: "claude-opus-5-5" },
      providers: [providerA, providerB],
      settings: cooldownSettings,
      nowMs: NOW,
      cooldowns,
    });
    assert.equal(claudeDecision?.toInstanceId, providerB.instanceId);

    // Gemini family is unaffected by Claude cooldown -> stays on antigravity.
    const geminiDecision = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: { instanceId: providerA.instanceId, model: "gemini-4-neon" },
      providers: [providerA, providerB],
      settings: cooldownSettings,
      nowMs: NOW,
      cooldowns,
    });
    assert.isNull(geminiDecision);

    // An immediate refresh (2s after error) reporting <100% does NOT cancel the cooldown.
    const immediateRefreshA = makeAntigravityProvider("antigravity", {
      usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 10 }], "2026-11-21T11:59:52.000Z"),
    });
    const afterImmediateRefresh = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: { instanceId: immediateRefreshA.instanceId, model: "claude-opus-5-5" },
      providers: [immediateRefreshA, providerB],
      settings: cooldownSettings,
      nowMs: NOW,
      cooldowns,
    });
    assert.equal(afterImmediateRefresh?.toInstanceId, providerB.instanceId);

    // A probe after the grace period showing a 0% reset window clears the cooldown early.
    const resetCooldowns = new Map([
      [
        AntigravityFailover.antigravityCooldownKey(providerA.instanceId, "claude-opus-5-5"),
        { recordedAtMs: NOW - 120_000, untilMs: NOW + 300_000 },
      ],
    ]);
    const resetProbeA = makeAntigravityProvider("antigravity", {
      usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 0 }], "2026-11-21T11:59:59.000Z"),
    });
    const afterResetProbe = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: { instanceId: resetProbeA.instanceId, model: "claude-opus-5-5" },
      providers: [resetProbeA, providerB],
      settings: cooldownSettings,
      nowMs: NOW,
      cooldowns: resetCooldowns,
    });
    assert.isNull(afterResetProbe);
  });

  it("excludes candidates with auth.verificationUrl in both preflight and failover modes", () => {
    const verificationUrl = GoogleAccountVerificationUrl.make(
      "https://accounts.google.com/v3/signin/continue?plt=failover-exclude",
    );
    const currentExhausted = makeAntigravityProvider("antigravity", {
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
    });
    const unverifiedAccount = makeAntigravityProvider("antigravity_2", {
      status: "warning",
      auth: {
        status: "authenticated",
        type: "oauth-personal",
        email: "b@example.com",
        verificationUrl,
      },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });
    const unverifiedWithoutUrl = makeAntigravityProvider("antigravity_4", {
      status: "warning",
      auth: {
        status: "authenticated",
        type: "oauth-personal",
        email: "d@example.com",
        verificationRequired: true,
      },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });
    const healthyAccount = makeAntigravityProvider("antigravity_3", {
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 30 }]),
    });

    const providers = [currentExhausted, unverifiedAccount, unverifiedWithoutUrl, healthyAccount];
    const settings = settingsForProviders(providers);

    const preflightDecision = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "preflight",
      projectId: PROJECT_ID,
      selection: { instanceId: currentExhausted.instanceId, model: "gemini-4-neon" },
      providers,
      settings,
      nowMs: NOW,
    });
    assert.equal(preflightDecision?.toInstanceId, healthyAccount.instanceId);

    const failoverDecision = AntigravityFailover.selectAntigravityFailoverCandidate({
      mode: "failover",
      projectId: PROJECT_ID,
      selection: { instanceId: currentExhausted.instanceId, model: "gemini-4-neon" },
      providers,
      settings,
      nowMs: NOW,
    });
    assert.equal(failoverDecision?.toInstanceId, healthyAccount.instanceId);

    // When only accounts needing verification remain, neither mode routes to them.
    const onlyUnverified = [currentExhausted, unverifiedAccount, unverifiedWithoutUrl];
    const onlyUnverifiedSettings = settingsForProviders(onlyUnverified);
    assert.isNull(
      AntigravityFailover.selectAntigravityFailoverCandidate({
        mode: "preflight",
        projectId: PROJECT_ID,
        selection: { instanceId: currentExhausted.instanceId, model: "gemini-4-neon" },
        providers: onlyUnverified,
        settings: onlyUnverifiedSettings,
        nowMs: NOW,
      }),
    );
    assert.isNull(
      AntigravityFailover.selectAntigravityFailoverCandidate({
        mode: "failover",
        projectId: PROJECT_ID,
        selection: { instanceId: currentExhausted.instanceId, model: "gemini-4-neon" },
        providers: onlyUnverified,
        settings: onlyUnverifiedSettings,
        nowMs: NOW,
      }),
    );
  });

  it("enforces personal OAuth tenant and billing boundaries in preflight and failover modes", () => {
    const businessCurrent = makeAntigravityProvider("antigravity_biz_1", {
      auth: { status: "authenticated", type: "oauth-business", email: "biz1@corp.example.com" },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
    });
    const businessCandidate = makeAntigravityProvider("antigravity_biz_2", {
      auth: { status: "authenticated", type: "oauth-business", email: "biz2@corp.example.com" },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });
    const personalCurrent = makeAntigravityProvider("antigravity", {
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
    });
    const personalHealthy = makeAntigravityProvider("antigravity_personal_2", {
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 25 }]),
    });
    const apiKeyCandidate = makeAntigravityProvider("antigravity_api_key", {
      auth: { status: "authenticated", type: "gemini-api-key" },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });
    const vertexCandidate = makeAntigravityProvider("antigravity_vertex", {
      auth: { status: "authenticated", type: "agent-platform" },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });
    const unsupportedLimitsCandidate = makeAntigravityProvider("antigravity_unsupported", {
      usageLimits: {
        checkedAt: "2026-11-21T11:55:00.000Z",
        windows: [],
        unavailable: { reason: "unsupported", message: "Usage limits are not supported." },
      },
    });
    const untypedAuthCandidate = makeAntigravityProvider("antigravity_untyped", {
      auth: { status: "authenticated", email: "untyped@example.com" },
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });
    const configMismatchCandidate = makeAntigravityProvider("antigravity_cfg_biz", {
      usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
    });

    const allProviders = [
      businessCurrent,
      businessCandidate,
      personalCurrent,
      personalHealthy,
      apiKeyCandidate,
      vertexCandidate,
      unsupportedLimitsCandidate,
      untypedAuthCandidate,
      configMismatchCandidate,
    ];
    const baseSettings = settingsForProviders(allProviders);
    const settingsWithConfigOverride: ServerSettings = {
      ...baseSettings,
      providerInstances: {
        ...baseSettings.providerInstances,
        [configMismatchCandidate.instanceId]: {
          driver: ANTIGRAVITY_DRIVER,
          enabled: true,
          config: { authMethod: "oauth-business" },
        },
      },
    };

    for (const mode of ["preflight", "failover"] as const) {
      // oauth-business never fails over to oauth-personal or another oauth-business.
      assert.isNull(
        AntigravityFailover.selectAntigravityFailoverCandidate({
          mode,
          projectId: PROJECT_ID,
          selection: { instanceId: businessCurrent.instanceId, model: "gemini-4-neon" },
          providers: [businessCurrent, businessCandidate, personalHealthy],
          settings: settingsWithConfigOverride,
          nowMs: NOW,
        }),
      );

      // oauth-personal never fails over to non-personal, untyped, config-mismatched, or unsupported candidates.
      assert.isNull(
        AntigravityFailover.selectAntigravityFailoverCandidate({
          mode,
          projectId: PROJECT_ID,
          selection: { instanceId: personalCurrent.instanceId, model: "gemini-4-neon" },
          providers: [
            personalCurrent,
            businessCandidate,
            apiKeyCandidate,
            vertexCandidate,
            unsupportedLimitsCandidate,
            untypedAuthCandidate,
            configMismatchCandidate,
          ],
          settings: settingsWithConfigOverride,
          nowMs: NOW,
        }),
      );

      // When a valid oauth-personal candidate exists alongside excluded accounts, only it is selected.
      const decision = AntigravityFailover.selectAntigravityFailoverCandidate({
        mode,
        projectId: PROJECT_ID,
        selection: { instanceId: personalCurrent.instanceId, model: "gemini-4-neon" },
        providers: allProviders,
        settings: settingsWithConfigOverride,
        nowMs: NOW,
      });
      assert.equal(decision?.toInstanceId, personalHealthy.instanceId);

      // Current instance whose config authMethod is non-personal also refuses failover.
      const currentConfigMismatchSettings: ServerSettings = {
        ...settingsWithConfigOverride,
        providerInstances: {
          ...settingsWithConfigOverride.providerInstances,
          [personalCurrent.instanceId]: {
            driver: ANTIGRAVITY_DRIVER,
            enabled: true,
            config: { authMethod: "gemini-api-key" },
          },
        },
      };
      assert.isNull(
        AntigravityFailover.selectAntigravityFailoverCandidate({
          mode,
          projectId: PROJECT_ID,
          selection: { instanceId: personalCurrent.instanceId, model: "gemini-4-neon" },
          providers: [personalCurrent, personalHealthy],
          settings: currentConfigMismatchSettings,
          nowMs: NOW,
        }),
      );
    }
  });
});

interface MockTurnOutcome {
  readonly behavior: "succeed" | "usage_limit" | "validation_required";
  readonly assistantText?: string;
  readonly verificationUrl?: GoogleAccountVerificationUrl;
}

function makeFakeAntigravityAdapter(input: {
  readonly instanceId: ProviderInstanceId;
  readonly cwd: string;
  readonly outcomeForTurn: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) => MockTurnOutcome;
  readonly recordedTurns: Array<{
    readonly instanceId: ProviderInstanceId;
    readonly threadId: ThreadId;
    readonly text: string;
    readonly model: string;
  }>;
}): ProviderAdapter.ProviderAdapterV2["Service"] {
  const { instanceId, cwd, outcomeForTurn, recordedTurns } = input;
  return {
    instanceId,
    driver: ANTIGRAVITY_DRIVER,
    getCapabilities: () => Effect.succeed(antigravityCapabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (openInput) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        return {
          instanceId,
          driver: ANTIGRAVITY_DRIVER,
          providerSessionId: openInput.providerSessionId,
          providerSession: {
            id: openInput.providerSessionId,
            driver: ANTIGRAVITY_DRIVER,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: "gemini-4-neon",
            capabilities: antigravityCapabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ensureThread: ({ threadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${instanceId}:${threadId}`),
              driver: ANTIGRAVITY_DRIVER,
              providerInstanceId: instanceId,
              providerSessionId: openInput.providerSessionId,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: {
                driver: ANTIGRAVITY_DRIVER,
                nativeId: `native:${instanceId}:${threadId}`,
                strength: "strong",
              },
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
              recordedTurns.push({
                instanceId,
                threadId: turn.threadId,
                text: turn.message.text,
                model: turn.modelSelection.model,
              });
              const turnId = ProviderTurnId.make(`turn:${instanceId}:${turn.attemptId}`);
              yield* Queue.offer(events, {
                type: "provider_turn.updated",
                driver: ANTIGRAVITY_DRIVER,
                providerTurn: {
                  id: turnId,
                  providerThreadId: turn.providerThread.id,
                  nodeId: turn.rootNodeId,
                  runAttemptId: turn.attemptId,
                  nativeTurnRef: null,
                  ordinal: turn.providerTurnOrdinal,
                  status: "running",
                  startedAt: now,
                  completedAt: null,
                },
              });
              const outcome = outcomeForTurn(turn);
              if (outcome.behavior === "succeed") {
                const reply = outcome.assistantText ?? `Reply from ${instanceId}`;
                yield* Queue.offer(events, {
                  type: "turn_item.updated",
                  driver: ANTIGRAVITY_DRIVER,
                  turnItem: {
                    id: `item:assistant:${instanceId}:${turn.attemptId}` as never,
                    threadId: turn.threadId,
                    runId: turn.runId,
                    nodeId: turn.rootNodeId,
                    providerThreadId: turn.providerThread.id,
                    providerTurnId: turnId,
                    nativeItemRef: null,
                    parentItemId: null,
                    ordinal: turn.runOrdinal * 100 + 2,
                    status: "completed",
                    title: null,
                    startedAt: now,
                    completedAt: now,
                    updatedAt: now,
                    type: "assistant_message",
                    messageId: MessageId.make(`assistant:${instanceId}:${turn.attemptId}`),
                    text: reply,
                    streaming: false,
                  },
                });
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver: ANTIGRAVITY_DRIVER,
                  providerTurn: {
                    id: turnId,
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: null,
                    ordinal: turn.providerTurnOrdinal,
                    status: "completed",
                    startedAt: now,
                    completedAt: now,
                  },
                });
                yield* Queue.offer(events, {
                  type: "turn.terminal",
                  driver: ANTIGRAVITY_DRIVER,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turnId,
                  runOrdinal: turn.runOrdinal,
                  status: "completed",
                  failure: null,
                  threadDisposition: "reusable",
                });
                return;
              }
              if (outcome.behavior === "usage_limit") {
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver: ANTIGRAVITY_DRIVER,
                  providerTurn: {
                    id: turnId,
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: null,
                    ordinal: turn.providerTurnOrdinal,
                    status: "failed",
                    startedAt: now,
                    completedAt: now,
                  },
                });
                yield* Queue.offer(events, {
                  type: "turn.terminal",
                  driver: ANTIGRAVITY_DRIVER,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turnId,
                  runOrdinal: turn.runOrdinal,
                  status: "failed",
                  threadDisposition: "reusable",
                  failureItemOrdinal: turn.runOrdinal * 100 + 5,
                  failure: makeProviderFailure({
                    class: "usage_limit",
                    message: "Individual quota reached",
                    code: "QUOTA_EXHAUSTED",
                    retryable: true,
                    resetAt: FUTURE_RESET,
                  }),
                });
                return;
              }
              yield* Queue.offer(events, {
                type: "provider_turn.updated",
                driver: ANTIGRAVITY_DRIVER,
                providerTurn: {
                  id: turnId,
                  providerThreadId: turn.providerThread.id,
                  nodeId: turn.rootNodeId,
                  runAttemptId: turn.attemptId,
                  nativeTurnRef: null,
                  ordinal: turn.providerTurnOrdinal,
                  status: "failed",
                  startedAt: now,
                  completedAt: now,
                },
              });
              yield* Queue.offer(events, {
                type: "turn.terminal",
                driver: ANTIGRAVITY_DRIVER,
                providerThreadId: turn.providerThread.id,
                providerTurnId: turnId,
                runOrdinal: turn.runOrdinal,
                status: "failed",
                threadDisposition: "reusable",
                failureItemOrdinal: turn.runOrdinal * 100 + 5,
                failure: makeProviderFailure({
                  class: "permission_error",
                  message: "Verify your Google account to continue using Antigravity.",
                  code: "VALIDATION_REQUIRED",
                  retryable: false,
                  ...(outcome.verificationUrl === undefined
                    ? {}
                    : { verificationUrl: outcome.verificationUrl }),
                }),
              });
            }),
          steerTurn: () => Effect.die("unused"),
          interruptTurn: () => Effect.void,
          respondToRuntimeRequest: () => Effect.die("unused"),
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
}

function makeIntegrationLayer(input: {
  readonly name: string;
  readonly cwd: string;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly settings?: Partial<ServerSettings>;
  readonly outcomeFor: (
    instanceId: ProviderInstanceId,
    turn: ProviderAdapter.ProviderAdapterV2TurnInput,
  ) => MockTurnOutcome;
  readonly recordedTurns: Array<{
    readonly instanceId: ProviderInstanceId;
    readonly threadId: ThreadId;
    readonly text: string;
    readonly model: string;
  }>;
}) {
  const providersRef = Ref.makeUnsafe<ReadonlyArray<ServerProvider>>(input.providers);
  const adapterMap = new Map(
    input.providers.map((provider) => [
      provider.instanceId,
      makeFakeAntigravityAdapter({
        instanceId: provider.instanceId,
        cwd: input.cwd,
        outcomeForTurn: (turn) => input.outcomeFor(provider.instanceId, turn),
        recordedTurns: input.recordedTurns,
      }),
    ]),
  );
  const layerAdapters = ProviderAdapterRegistry.layerFromAdapters([...adapterMap.values()]);
  const layerProviderRegistry = Layer.succeed(ProviderRegistry.ProviderRegistry, {
    getProviders: Ref.get(providersRef),
    refresh: () => Ref.get(providersRef),
    refreshInstance: () => Ref.get(providersRef),
    reloadSecretsAndRefresh: () => Ref.get(providersRef),
    applySecretAndRefresh: () => Ref.get(providersRef),
    refreshAfterUpdate: () => Ref.get(providersRef),
    refreshWorkspaceSnapshot: () => Ref.get(providersRef),
    invalidateModelCaches: () => Effect.void,
    refreshModelCatalog: Effect.void,
    getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
    setProviderMaintenanceActionState: () => Ref.get(providersRef),
    streamChanges: Stream.empty,
  });
  const layerSettings = ServerSettingsService.layerTest(
    settingsForProviders(input.providers, input.settings),
  );
  const layerFailover = AntigravityFailover.layer.pipe(
    Layer.provide(Layer.mergeAll(layerProviderRegistry, layerSettings)),
  );
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: input.name },
    layerAdapters,
    { runEffectWorker: true },
  ).pipe(Layer.provide(layerFailover));
  const layerThreadMgmt = Layer.merge(
    layerOrchestrator,
    ThreadManagementService.layer.pipe(
      Layer.provide(layerOrchestrator),
      Layer.provide(layerSettings),
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
    ),
  );
  return OrchestratorMcpService.layer.pipe(
    Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provideMerge(layerThreadMgmt),
    Layer.provideMerge(layerFailover),
    Layer.provide(
      Layer.mergeAll(
        layerAdapters,
        layerProviderRegistry,
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        layerSettings,
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        NodeServices.layer,
        NodeCrypto.layer,
      ),
    ),
  );
}

describe("Antigravity auto-failover integration", () => {
  it.live(
    "chains multi-account failover on usage_limit with context handoff and without duplicating user messages",
    () =>
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("antigravity-failover-chain");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 40 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 50 }]),
          }),
          makeAntigravityProvider("antigravity_3", {
            displayName: "AGY-3",
            // Idle >1h expired token (`probeFailed`), still a valid failover candidate.
            usageLimits: {
              checkedAt: "2026-11-21T11:55:00.000Z",
              windows: [],
              unavailable: { reason: "probeFailed", message: "Expired access token" },
            },
          }),
        ];
        let turn1Done = false;
        const layer = makeIntegrationLayer({
          name: "antigravity-failover-chain",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: (instanceId) => {
            if (!turn1Done) {
              turn1Done = true;
              return { behavior: "succeed", assistantText: "Initial context established." };
            }
            if (instanceId === ProviderInstanceId.make("antigravity")) {
              return { behavior: "usage_limit" };
            }
            if (instanceId === ProviderInstanceId.make("antigravity_2")) {
              return { behavior: "usage_limit" };
            }
            return { behavior: "succeed", assistantText: "Completed on AGY-3." };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadId = ThreadId.make("thread:antigravity-chain");

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-chain"),
            threadId,
            projectId: PROJECT_ID,
            title: "Chain test",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });

          const waitForEvent = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );

          // Turn 1 succeeds on antigravity so the thread has conversation history.
          const waitRun1 = yield* waitForEvent(
            (event) =>
              event.type === "run.updated" &&
              event.threadId === threadId &&
              event.payload.ordinal === 1 &&
              event.payload.status === "completed",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-1"),
            threadId,
            messageId: MessageId.make("message:1"),
            text: "Remember the secret keyword PHOENIX.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(waitRun1);

          // Turn 2 hits usage_limit on antigravity -> fails over to antigravity_2 -> hits usage_limit -> succeeds on antigravity_3.
          const waitRun2 = yield* waitForEvent(
            (event) =>
              event.type === "run.updated" &&
              event.threadId === threadId &&
              event.payload.ordinal === 2 &&
              event.payload.status === "completed",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-2"),
            threadId,
            messageId: MessageId.make("message:2"),
            text: "Continue using the keyword.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(waitRun2);

          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            projection.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity_3"),
          );
          const run2 = projection.runs.find((run) => run.ordinal === 2)!;
          assert.equal(run2.status, "completed");
          // User messages were never duplicated: exactly 2 user messages total (msg-1 and msg-2).
          assert.equal(projection.messages.filter((message) => message.role === "user").length, 2);
          // All 3 attempts are recorded persistently on run 2.
          const run2Attempts = projection.attempts
            .filter((attempt) => attempt.runId === run2.id)
            .map((attempt) => attempt.providerInstanceId);
          assert.deepEqual(run2Attempts, [
            ProviderInstanceId.make("antigravity"),
            ProviderInstanceId.make("antigravity_2"),
            ProviderInstanceId.make("antigravity_3"),
          ]);
          // The final turn on antigravity_3 received context handoff containing Turn 1's keyword.
          const finalTurn = recordedTurns.findLast(
            (turn) => turn.instanceId === ProviderInstanceId.make("antigravity_3"),
          );
          assert.isDefined(finalTurn);
          assert.include(finalTurn!.text, "PHOENIX");
          assert.include(finalTurn!.text, "Continue using the keyword.");
          // Earlier attempt's handoff was superseded so attempt 3 does not receive duplicate handoffs.
          const run2Handoffs = projection.contextHandoffs.filter(
            (handoff) => handoff.targetRunId === run2.id,
          );
          assert.isAtLeast(run2Handoffs.length, 2);
          assert.isTrue(run2Handoffs.some((handoff) => handoff.status === "superseded"));
        }).pipe(Effect.provide(layer));
      }),
  );

  it.live(
    "stops cleanly without infinite loop when all accounts exhaust quota or hit VALIDATION_REQUIRED",
    () =>
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("antigravity-failover-bounds");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", { displayName: "AGY-1" }),
          makeAntigravityProvider("antigravity_2", { displayName: "AGY-2" }),
        ];

        // Case 1: All accounts hit usage_limit -> tries each once, then stops failed with notice.
        const exhaustedLayer = makeIntegrationLayer({
          name: "antigravity-all-exhausted",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: () => ({ behavior: "usage_limit" }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadId = ThreadId.make("thread:all-exhausted");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-exhausted"),
            threadId,
            projectId: PROJECT_ID,
            title: "All exhausted",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const waitExhaustedNotice = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.threadId === threadId &&
                event.payload.type === "system_notice" &&
                event.payload.message.includes(
                  "All available Antigravity accounts reached quota limits",
                ),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-exhausted"),
            threadId,
            messageId: MessageId.make("message:exhausted"),
            text: "Do work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(waitExhaustedNotice);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(projection.runs[0]?.status, "failed");
          assert.deepEqual(
            projection.attempts.map((attempt) => attempt.providerInstanceId),
            [ProviderInstanceId.make("antigravity"), ProviderInstanceId.make("antigravity_2")],
          );
          assert.isTrue(
            projection.turnItems.some(
              (item) =>
                item.type === "system_notice" &&
                item.message.includes("All available Antigravity accounts reached quota limits"),
            ),
          );
        }).pipe(Effect.provide(exhaustedLayer));

        // Case 2: Non-quota permanent error (VALIDATION_REQUIRED) -> stops immediately on attempt 1 and surfaces verificationUrl.
        const permanentTurns: typeof recordedTurns = [];
        const verificationUrl = GoogleAccountVerificationUrl.make(
          "https://accounts.google.com/v3/signin/continue?plt=failover-case2",
        );
        const permanentLayer = makeIntegrationLayer({
          name: "antigravity-validation-required",
          cwd,
          providers,
          recordedTurns: permanentTurns,
          outcomeFor: () => ({ behavior: "validation_required", verificationUrl }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadId = ThreadId.make("thread:validation-required");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-validation"),
            threadId,
            projectId: PROJECT_ID,
            title: "Validation required",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const waitFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === threadId &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-validation"),
            threadId,
            messageId: MessageId.make("message:validation"),
            text: "Do work",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(waitFailed);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(projection.runs[0]?.status, "failed");
          assert.deepEqual(
            projection.attempts.map((attempt) => attempt.providerInstanceId),
            [ProviderInstanceId.make("antigravity")],
          );
          assert.isFalse(
            projection.turnItems.some(
              (item) =>
                item.type === "system_notice" &&
                item.message.includes("All available Antigravity accounts reached quota limits"),
            ),
          );
          const errorItem = projection.turnItems.find((item) => item.type === "error");
          assert.equal(errorItem?.type, "error");
          if (errorItem?.type === "error") {
            assert.equal(errorItem.title, "Verification required");
            assert.equal(errorItem.failure?.class, "permission_error");
            assert.equal(errorItem.failure?.code, "VALIDATION_REQUIRED");
            assert.equal(errorItem.failure?.retryable, false);
            assert.equal(errorItem.failure?.verificationUrl, verificationUrl);
          }
        }).pipe(Effect.provide(permanentLayer));
      }),
  );

  it.live(
    "applies pre-flight routing and runtime failover to delegated subagents without failing the parent task prematurely",
    () =>
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("antigravity-subagent-failover");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            // 3p (Claude) is 100% exhausted on AGY-1; Gemini is still available.
            usageLimits: makeLimits([
              { id: "3p-5h", usedPercent: 100 },
              { id: "gemini-5h", usedPercent: 10 },
            ]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 20 }]),
          }),
          makeAntigravityProvider("antigravity_3", {
            displayName: "AGY-3",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 50 }]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:parent-design-failover");
        const layer = makeIntegrationLayer({
          name: "antigravity-subagent-failover",
          cwd,
          providers,
          settings: {
            agentRoleModels: {
              design: {
                instanceId: ProviderInstanceId.make("antigravity"),
                model: "claude-opus-5-5",
              },
            },
          },
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return { behavior: "succeed", assistantText: "Parent running." };
            }
            // Pre-flight should skip exhausted AGY-1 and start on AGY-2; AGY-2 then hits runtime quota -> fails over to AGY-3.
            if (instanceId === ProviderInstanceId.make("antigravity_2")) {
              return { behavior: "usage_limit" };
            }
            return { behavior: "succeed", assistantText: "Design completed on AGY-3." };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          // Put parent in an active run so delegateTask can attach to it.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-parent"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent"),
            text: "Delegate design",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:antigravity-failover"),
            requestNamespace: "provider-session:antigravity-failover",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:antigravity-failover",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };
          const waitSubagentDone = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "subagent.updated" &&
                event.threadId === parentThreadId &&
                (event.payload.status === "completed" || event.payload.status === "failed"),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const delegated = yield* mcp.delegateTask(scope, {
            role: "design",
            task: "Design the failover card.",
            mode: "async",
            clientRequestId: "req-design-1",
          });
          // Pre-flight routed the child directly away from exhausted AGY-1 to AGY-2.
          assert.equal(delegated.providerInstanceId, ProviderInstanceId.make("antigravity_2"));
          assert.isNotNull(delegated.childThreadId);
          const childThreadId = delegated.childThreadId!;

          yield* Fiber.join(waitSubagentDone);

          const childProjection = yield* orchestrator.getThreadProjection(childThreadId);
          assert.equal(childProjection.runs[0]?.status, "completed");
          assert.deepEqual(
            childProjection.attempts.map((attempt) => attempt.providerInstanceId),
            [ProviderInstanceId.make("antigravity_2"), ProviderInstanceId.make("antigravity_3")],
          );
          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const subagent = parentProjection.subagents.find((task) => task.id === delegated.taskId);
          assert.equal(subagent?.status, "completed");
          assert.include(subagent?.result ?? "", "Design completed on AGY-3.");
        }).pipe(Effect.provide(layer));
      }),
  );

  it.live(
    "pre-flight routes main thread messages directly to an available Antigravity account when the selected one is 100% exhausted",
    () =>
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("antigravity-main-preflight");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 15 }]),
          }),
        ];
        const layer = makeIntegrationLayer({
          name: "antigravity-main-preflight",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: () => ({ behavior: "succeed", assistantText: "Handled on AGY-2." }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadId = ThreadId.make("thread:main-preflight");

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-preflight"),
            threadId,
            projectId: PROJECT_ID,
            title: "Main preflight",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const waitCompleted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === threadId &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-preflight"),
            threadId,
            messageId: MessageId.make("message:preflight"),
            text: "Run immediately on backup",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(waitCompleted);

          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(projection.runs[0]?.status, "completed");
          assert.equal(
            projection.runs[0]?.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );
          assert.equal(recordedTurns.length, 1);
          assert.equal(recordedTurns[0]?.instanceId, ProviderInstanceId.make("antigravity_2"));
        }).pipe(Effect.provide(layer));
      }),
  );

  it.live(
    "holds delegate_task(mode: wait) open during runtime failover and does not let autoArmLimitRecovery snooze the switched thread",
    () =>
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("antigravity-wait-failover");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 20 }]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:parent-wait");
        const layer = makeIntegrationLayer({
          name: "antigravity-wait-failover",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return { behavior: "succeed", assistantText: "Parent running." };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "Wait-mode child completed on AGY-2." };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent-wait"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent wait",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-parent-wait"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent-wait"),
            text: "Delegate wait child",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:antigravity-wait-failover"),
            requestNamespace: "provider-session:antigravity-wait-failover",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:antigravity-wait-failover",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          const waited = yield* mcp.delegateTask(scope, {
            task: "Finish in wait mode across failover.",
            mode: "wait",
            timeoutMs: 15_000,
            clientRequestId: "req-wait-failover",
          });
          assert.equal(waited.status, "completed");
          assert.equal(waited.workState, "result_available");
          assert.include(waited.summary ?? "", "Wait-mode child completed on AGY-2.");
          assert.isNotNull(waited.childThreadId);

          const childProjection = yield* orchestrator.getThreadProjection(waited.childThreadId!);
          const childRun = childProjection.runs[0]!;
          assert.equal(childRun.status, "completed");
          assert.deepEqual(
            childProjection.attempts.map((attempt) => attempt.providerInstanceId),
            [ProviderInstanceId.make("antigravity"), ProviderInstanceId.make("antigravity_2")],
          );

          // A late autoArmLimitRecovery call for the failed-over run must be ignored.
          yield* orchestrator.autoArmLimitRecovery({
            commandId: CommandId.make("late-auto-arm"),
            threadId: waited.childThreadId!,
            limitRecovery: {
              runId: childRun.id,
              resetAt: FUTURE_RESET,
              autoResume: true,
              snooze: true,
            },
          });
          const afterLateArm = yield* orchestrator.getThreadProjection(waited.childThreadId!);
          assert.isNull(afterLateArm.thread.limitRecovery ?? null);
          assert.isNull(afterLateArm.thread.snoozedUntil ?? null);
        }).pipe(Effect.provide(layer));
      }),
  );

  it.live(
    "skips pre-flight failover when target.providerInstanceId is pinned while keeping role-routed pre-flight active",
    () =>
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("antigravity-explicit-pin");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([
              { id: "3p-5h", usedPercent: 100 },
              { id: "gemini-5h", usedPercent: 10 },
            ]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 15 }]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:parent-explicit-pin");
        const layer = makeIntegrationLayer({
          name: "antigravity-explicit-pin",
          cwd,
          providers,
          settings: {
            agentRoleModels: {
              design: {
                instanceId: ProviderInstanceId.make("antigravity"),
                model: "claude-opus-5-5",
              },
            },
          },
          recordedTurns,
          outcomeFor: () => ({ behavior: "succeed", assistantText: "Done." }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent-pin"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent pin",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-parent-pin"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent-pin"),
            text: "Start parent",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:antigravity-explicit-pin"),
            requestNamespace: "provider-session:antigravity-explicit-pin",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:antigravity-explicit-pin",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          const pinnedDelegation = yield* mcp.delegateTask(scope, {
            target: {
              providerInstanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            task: "Stay on explicitly pinned instance.",
            mode: "async",
            clientRequestId: "req-pinned-delegate",
          });
          assert.equal(pinnedDelegation.providerInstanceId, ProviderInstanceId.make("antigravity"));

          const pinnedThread = yield* mcp.createThreads(scope, {
            threads: [
              {
                title: "Pinned thread",
                prompt: "Stay on pinned instance during initial dispatch.",
                target: {
                  providerInstanceId: ProviderInstanceId.make("antigravity"),
                  model: "claude-opus-5-5",
                },
              },
            ],
            clientRequestId: "req-pinned-create-thread",
          });
          assert.equal(
            pinnedThread.threads[0]?.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          const pinnedThreadProjection = yield* orchestrator.getThreadProjection(
            pinnedThread.threads[0]!.threadId,
          );
          assert.equal(
            pinnedThreadProjection.runs[0]?.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );

          const roleDelegation = yield* mcp.delegateTask(scope, {
            role: "design",
            task: "Role-routed work still pre-flight fails over.",
            mode: "async",
            clientRequestId: "req-role-delegate",
          });
          assert.equal(roleDelegation.providerInstanceId, ProviderInstanceId.make("antigravity_2"));
        }).pipe(Effect.provide(layer));
      }),
  );
});
