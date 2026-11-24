import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  EventId,
  GoogleAccountVerificationUrl,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ScheduledTaskId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { createModelCapabilities } from "@t3tools/shared/model";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettingsService from "../serverSettings.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import * as AntigravityFailover from "./AntigravityFailover.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as RestartContinuation from "./RestartContinuation.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as UsageLimitRecoveryWorker from "./UsageLimitRecoveryWorker.ts";
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
  readonly pauseBeforeRunning?: {
    readonly onBeforeRunning: Deferred.Deferred<void>;
    readonly releaseRunning: Deferred.Deferred<void>;
  };
  readonly pause?: {
    readonly onStarted: Deferred.Deferred<void>;
    readonly release: Deferred.Deferred<void>;
  };
}

interface RecordedSessionOp {
  readonly op: "openSession" | "ensureThread" | "resumeThread";
  readonly instanceId: ProviderInstanceId;
  readonly initialNativeThreadId?: string | undefined;
  readonly nativeThreadId?: string | null | undefined;
  readonly providerThreadOwnerInstanceId?: ProviderInstanceId | undefined;
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
  readonly recordedSessionOps?: Array<RecordedSessionOp>;
}): ProviderAdapter.ProviderAdapterV2["Service"] {
  const { instanceId, cwd, outcomeForTurn, recordedTurns, recordedSessionOps } = input;
  return {
    instanceId,
    driver: ANTIGRAVITY_DRIVER,
    getCapabilities: () => Effect.succeed(antigravityCapabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (openInput) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const sessionScope = yield* Effect.scope;
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        recordedSessionOps?.push({
          op: "openSession",
          instanceId,
          initialNativeThreadId: openInput.initialNativeThreadId,
        });
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
            Effect.sync(() => {
              const nativeId = `native:${instanceId}:${threadId}`;
              recordedSessionOps?.push({
                op: "ensureThread",
                instanceId,
                nativeThreadId: nativeId,
              });
              return {
                id: ProviderThreadId.make(`provider-thread:${instanceId}:${threadId}`),
                driver: ANTIGRAVITY_DRIVER,
                providerInstanceId: instanceId,
                providerSessionId: openInput.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: ANTIGRAVITY_DRIVER,
                  nativeId,
                  strength: "strong" as const,
                },
                nativeConversationHeadRef: null,
                status: "idle" as const,
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              };
            }),
          resumeThread: ({ providerThread }) =>
            Effect.sync(() => {
              recordedSessionOps?.push({
                op: "resumeThread",
                instanceId,
                nativeThreadId: providerThread.nativeThreadRef?.nativeId,
                providerThreadOwnerInstanceId: providerThread.providerInstanceId,
              });
              return providerThread;
            }),
          startTurn: (turn) =>
            Effect.gen(function* () {
              recordedTurns.push({
                instanceId,
                threadId: turn.threadId,
                text: turn.message.text,
                model: turn.modelSelection.model,
              });
              const turnId = ProviderTurnId.make(`turn:${instanceId}:${turn.attemptId}`);
              const outcome = outcomeForTurn(turn);
              const emitRunningAndOutcome = Effect.gen(function* () {
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
                if (outcome.behavior === "succeed") {
                  const reply = outcome.assistantText ?? `Reply from ${instanceId}`;
                  const emitSuccess = Effect.gen(function* () {
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
                  });
                  if (outcome.pause !== undefined) {
                    const { onStarted, release } = outcome.pause;
                    yield* Deferred.succeed(onStarted, undefined);
                    yield* Deferred.await(release).pipe(
                      Effect.andThen(emitSuccess),
                      Effect.forkIn(sessionScope),
                    );
                  } else {
                    yield* emitSuccess;
                  }
                  return;
                }
                if (outcome.behavior === "usage_limit") {
                  const emitUsageLimit = Effect.gen(function* () {
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
                  });
                  if (outcome.pause !== undefined) {
                    const { onStarted, release } = outcome.pause;
                    yield* Deferred.succeed(onStarted, undefined);
                    yield* Deferred.await(release).pipe(
                      Effect.andThen(emitUsageLimit),
                      Effect.forkIn(sessionScope),
                    );
                  } else {
                    yield* emitUsageLimit;
                  }
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
              });
              if (outcome.pauseBeforeRunning !== undefined) {
                const { onBeforeRunning, releaseRunning } = outcome.pauseBeforeRunning;
                yield* Deferred.succeed(onBeforeRunning, undefined);
                yield* Deferred.await(releaseRunning).pipe(
                  Effect.andThen(emitRunningAndOutcome),
                  Effect.forkIn(sessionScope),
                );
                return;
              }
              yield* emitRunningAndOutcome;
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
  readonly providersRef?: Ref.Ref<ReadonlyArray<ServerProvider>>;
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
  readonly recordedSessionOps?: Array<RecordedSessionOp>;
  readonly databaseLayer?: NonNullable<
    Parameters<typeof ProviderReplayHarness.layerWithRegistry>[2]
  >["databaseLayer"];
  readonly wrapFailover?: (
    inner: AntigravityFailover.AntigravityFailoverShape,
  ) => AntigravityFailover.AntigravityFailoverShape;
  readonly wrapProjectionStore?: (
    store: ProjectionStore.ProjectionStoreV2Shape,
  ) => ProjectionStore.ProjectionStoreV2Shape;
  readonly runContinuationWorker?: boolean;
  readonly sweeps?: Map<string, Effect.Effect<void>>;
}) {
  const providersRef =
    input.providersRef ?? Ref.makeUnsafe<ReadonlyArray<ServerProvider>>(input.providers);
  const adapterMap = new Map(
    input.providers.map((provider) => [
      provider.instanceId,
      makeFakeAntigravityAdapter({
        instanceId: provider.instanceId,
        cwd: input.cwd,
        outcomeForTurn: (turn) => input.outcomeFor(provider.instanceId, turn),
        recordedTurns: input.recordedTurns,
        ...(input.recordedSessionOps === undefined
          ? {}
          : { recordedSessionOps: input.recordedSessionOps }),
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
  const baseFailover = AntigravityFailover.layer.pipe(
    Layer.provide(Layer.mergeAll(layerProviderRegistry, layerSettings)),
  );
  const layerFailover =
    input.wrapFailover === undefined
      ? baseFailover
      : Layer.effect(
          AntigravityFailover.AntigravityFailover,
          Effect.map(AntigravityFailover.AntigravityFailover, input.wrapFailover),
        ).pipe(Layer.provide(baseFailover));
  const layerDatabase = input.databaseLayer ?? SqlitePersistence.layerMemory;
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: input.name },
    layerAdapters,
    {
      runEffectWorker: true,
      ...(input.runContinuationWorker === undefined
        ? {}
        : { runContinuationWorker: input.runContinuationWorker }),
      ...(input.databaseLayer === undefined ? {} : { databaseLayer: input.databaseLayer }),
      ...(input.wrapProjectionStore === undefined
        ? {}
        : { wrapProjectionStore: input.wrapProjectionStore }),
    },
  ).pipe(Layer.provide(layerFailover));
  const layerThreadMgmt = Layer.merge(
    layerOrchestrator,
    ThreadManagementService.layer.pipe(
      Layer.provide(layerOrchestrator),
      Layer.provide(layerSettings),
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
    ),
  );
  const layerMcp = OrchestratorMcpService.layer.pipe(
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
  if (input.sweeps === undefined) {
    return layerMcp;
  }
  const sweeps = input.sweeps;
  const layerScheduler = Layer.succeed(Scheduler.Scheduler, {
    register: <E, R>(name: string, work: Effect.Effect<void, E, R>) =>
      Effect.gen(function* () {
        const context = yield* Effect.context<R>();
        sweeps.set(name, work.pipe(Effect.provideContext(context), Effect.orDie));
      }),
  });
  const layerRecovery = UsageLimitRecoveryWorker.layer.pipe(
    Layer.provide(layerScheduler),
    Layer.provide(ProjectionStore.layer.pipe(Layer.provide(layerDatabase))),
    Layer.provide(layerSettings),
    Layer.provide(layerThreadMgmt),
  );
  return Layer.merge(layerMcp, layerRecovery);
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

  it.effect(
    "allocates an isolated providerThread and handoff when startNextQueuedRun pre-flight fails over a queued non-fresh follow-up",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-cf06-queued-preflight");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const recordedSessionOps: Array<RecordedSessionOp> = [];
        const initialProviders = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
          }),
        ];
        const providersRef = Ref.makeUnsafe<ReadonlyArray<ServerProvider>>(initialProviders);
        const run1Started = yield* Deferred.make<void>();
        const releaseRun1 = yield* Deferred.make<void>();
        const layer = makeIntegrationLayer({
          name: "antigravity-cf06-queued-preflight",
          cwd,
          providers: initialProviders,
          providersRef,
          recordedTurns,
          recordedSessionOps,
          outcomeFor: (_instanceId, turn) =>
            turn.runOrdinal === 1
              ? {
                  behavior: "succeed",
                  assistantText: "Run 1 context PHOENIX_QUEUED.",
                  pause: { onStarted: run1Started, release: releaseRun1 },
                }
              : {
                  behavior: "succeed",
                  assistantText: "Run 2 completed on AGY-2.",
                },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadId = ThreadId.make("thread:cf06-queued-preflight");

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-cf06"),
            threadId,
            projectId: PROJECT_ID,
            title: "CF-06 queued preflight",
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

          const waitRun1Running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === threadId &&
                event.payload.ordinal === 1 &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-cf06-1"),
            threadId,
            messageId: MessageId.make("message:cf06-1"),
            text: "Remember PHOENIX_QUEUED.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(run1Started);
          yield* Fiber.join(waitRun1Running);

          // Queue Run 2 as a normal non-fresh follow-up while Run 1 is active on Account A.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("msg-cf06-2"),
            threadId,
            messageId: MessageId.make("message:cf06-2"),
            text: "Follow up on the queued turn.",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });

          // Exhaust Account A before Run 1 completes so startNextQueuedRun pre-flight fails over Run 2.
          yield* Ref.set(providersRef, [
            makeAntigravityProvider("antigravity", {
              displayName: "AGY-1",
              usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
            }),
            makeAntigravityProvider("antigravity_2", {
              displayName: "AGY-2",
              usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
            }),
          ]);

          const waitRun2Completed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === threadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Deferred.succeed(releaseRun1, undefined);
          yield* Fiber.join(waitRun2Completed);

          const agy2 = ProviderInstanceId.make("antigravity_2");
          const agy1 = ProviderInstanceId.make("antigravity");
          const agy2Ops = recordedSessionOps.filter((op) => op.instanceId === agy2);
          assert.isTrue(agy2Ops.some((op) => op.op === "ensureThread"));
          assert.isFalse(agy2Ops.some((op) => op.op === "resumeThread"));
          const agy2OpenOps = agy2Ops.filter((op) => op.op === "openSession");
          assert.isNotEmpty(agy2OpenOps);
          for (const op of agy2OpenOps) {
            assert.isUndefined(op.initialNativeThreadId);
          }

          const projection = yield* orchestrator.getThreadProjection(threadId);
          const run1 = projection.runs.find((run) => run.ordinal === 1)!;
          const run2 = projection.runs.find((run) => run.ordinal === 2)!;
          assert.equal(run2.providerInstanceId, agy2);
          assert.notEqual(run2.providerThreadId, run1.providerThreadId);

          const pt1 = projection.providerThreads.find((pt) => pt.id === run1.providerThreadId);
          const pt2 = projection.providerThreads.find((pt) => pt.id === run2.providerThreadId);
          assert.equal(pt1?.providerInstanceId, agy1);
          assert.equal(pt2?.providerInstanceId, agy2);

          const handoffForRun2 = projection.contextHandoffs.find(
            (handoff) => handoff.targetRunId === run2.id,
          );
          assert.isDefined(handoffForRun2);
          assert.include(handoffForRun2!.fromProviderThreadIds, run1.providerThreadId!);
          assert.equal(handoffForRun2?.toProviderThreadId, run2.providerThreadId);
          assert.isAtMost(handoffForRun2!.coveredRunOrdinals.from, 1);
          assert.isAtLeast(handoffForRun2!.coveredRunOrdinals.to, 1);
          const handoffTurnItem = projection.turnItems.find(
            (item) => item.type === "handoff" && item.contextHandoffId === handoffForRun2?.id,
          );
          assert.isDefined(handoffTurnItem);
          if (handoffTurnItem?.type === "handoff") {
            assert.deepEqual(handoffTurnItem.fromProviderInstanceIds, [agy1]);
            assert.equal(handoffTurnItem.toProviderInstanceId, agy2);
          }
        }).pipe(
          Effect.ensuring(Deferred.succeed(releaseRun1, undefined).pipe(Effect.ignore)),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "does not keep declined Antigravity failover pending across server restarts and lets UsageLimitRecoveryWorker and waitForThread settle",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-antigravity-cf07-restart-",
        });
        const dbPath = path.join(tempDir, "state.sqlite");
        const cwd = yield* checkpointWorkspace("antigravity-cf07-restart");
        const threadId = ThreadId.make("thread:cf07-restart");

        // Instance 1: Run 1 fails with usage_limit on AGY-1 while AGY-2 is exhausted (100%).
        const instance1Providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
          }),
        ];
        const failoverDeclined = yield* Deferred.make<void>();
        const dbLayer1 = SqlitePersistence.layerFromPath(dbPath).pipe(
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        );
        const layer1 = makeIntegrationLayer({
          name: "antigravity-cf07-instance1",
          cwd,
          providers: instance1Providers,
          recordedTurns: [],
          outcomeFor: () => ({ behavior: "usage_limit" }),
          databaseLayer: dbLayer1,
          wrapFailover: (inner) => ({
            ...inner,
            nextFailoverSelection: (input) =>
              inner
                .nextFailoverSelection(input)
                .pipe(
                  Effect.tap((decision) =>
                    decision === null
                      ? Deferred.succeed(failoverDeclined, undefined).pipe(Effect.asVoid)
                      : Effect.void,
                  ),
                ),
          }),
        });

        const failedRun = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-cf07-restart"),
            threadId,
            projectId: PROJECT_ID,
            title: "CF-07 restart test",
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
            commandId: CommandId.make("msg-cf07-restart"),
            threadId,
            messageId: MessageId.make("message:cf07-restart"),
            text: "Hit quota when backup is exhausted.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(waitFailed);
          yield* Deferred.await(failoverDeclined);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const run = projection.runs[0]!;
          assert.equal(run.status, "failed");
          assert.isFalse(yield* orchestrator.restartContinuationPending(run));
          return run;
        }).pipe(Effect.provide(layer1));

        // Instance 2: Reboot against the same SQLite DB with AGY-2 now healthy (0%).
        const instance2Providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
          }),
        ];
        const sweeps = new Map<string, Effect.Effect<void>>();
        let nextFailoverSelectionCalls = 0;
        const dbLayer2 = SqlitePersistence.layerFromPath(dbPath).pipe(
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        );
        const layer2 = makeIntegrationLayer({
          name: "antigravity-cf07-instance2",
          cwd,
          providers: instance2Providers,
          settings: { autoResumeLimitedThreads: true },
          recordedTurns: [],
          outcomeFor: () => ({ behavior: "succeed" }),
          databaseLayer: dbLayer2,
          sweeps,
          wrapFailover: (inner) => ({
            ...inner,
            nextFailoverSelection: (input) =>
              Effect.sync(() => {
                nextFailoverSelectionCalls += 1;
              }).pipe(Effect.andThen(inner.nextFailoverSelection(input))),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadManagement = yield* ThreadManagementService.ThreadManagementService;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sweep = Effect.andThen(
            Effect.suspend(() => sweeps.get("usage-limit-recovery")!),
            worker.drain(),
          );

          assert.isFalse(yield* orchestrator.restartContinuationPending(failedRun));

          yield* sweep;
          yield* sweep;
          yield* sweep;

          const afterSweeps = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(afterSweeps.thread.limitRecovery?.runId, failedRun.id);
          assert.equal(nextFailoverSelectionCalls, 0);

          const waitFiber = yield* threadManagement
            .waitForThread({
              projectId: PROJECT_ID,
              threadId,
              runId: failedRun.id,
              timeoutMs: 5_000,
            })
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* TestClock.adjust("5 seconds");
          const waited = yield* Fiber.join(waitFiber);
          assert.isFalse(waited.timedOut);
          assert.equal(waited.run?.id, failedRun.id);
        }).pipe(Effect.provide(layer2));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "clears stale Attempt-1 failover state once Attempt 2 completes and handles interrupting a blocked awaitRestartContinuation fiber",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-cf07-stale-waiter");
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
          }),
          makeAntigravityProvider("antigravity_3", {
            displayName: "AGY-3",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 0 }]),
          }),
        ];
        const gateEntered = yield* Deferred.make<void>();
        const releaseGate = yield* Deferred.make<void>();
        let gatedOnce = false;
        const layer = makeIntegrationLayer({
          name: "antigravity-cf07-stale-waiter",
          cwd,
          providers,
          recordedTurns: [],
          outcomeFor: (instanceId) =>
            instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "Completed on AGY-2." },
          wrapFailover: (inner) => ({
            ...inner,
            recordQuotaExhausted: (input) =>
              Effect.gen(function* () {
                if (!gatedOnce) {
                  gatedOnce = true;
                  yield* Deferred.succeed(gateEntered, undefined);
                  yield* Deferred.await(releaseGate);
                }
                yield* inner.recordQuotaExhausted(input);
              }),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadId = ThreadId.make("thread:cf07-stale-waiter");

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-cf07-stale"),
            threadId,
            projectId: PROJECT_ID,
            title: "CF-07 stale waiter",
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
            commandId: CommandId.make("msg-cf07-stale"),
            threadId,
            messageId: MessageId.make("message:cf07-stale"),
            text: "Fail over cleanly.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });

          yield* Deferred.await(gateEntered);
          const projectionWhileGated = yield* orchestrator.getThreadProjection(threadId);
          const staleAttempt1Run = projectionWhileGated.runs[0]!;
          assert.equal(staleAttempt1Run.status, "failed");
          assert.equal(staleAttempt1Run.providerInstanceId, ProviderInstanceId.make("antigravity"));

          assert.isTrue(yield* orchestrator.restartContinuationPending(staleAttempt1Run));
          const blockedFiber = yield* orchestrator
            .awaitRestartContinuation(staleAttempt1Run)
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* TestClock.adjust("1 second");
          assert.isTrue(Option.isNone(Option.fromUndefinedOr(blockedFiber.pollUnsafe())));
          yield* Fiber.interrupt(blockedFiber);
          const interruptExit = yield* Fiber.await(blockedFiber);
          assert.isTrue(Exit.isFailure(interruptExit));
          if (Exit.isFailure(interruptExit)) {
            assert.isTrue(Cause.hasInterruptsOnly(interruptExit.cause));
          }

          yield* Deferred.succeed(releaseGate, undefined);
          yield* Fiber.join(waitCompleted);

          const finished = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(finished.runs[0]?.status, "completed");
          assert.deepEqual(
            finished.attempts.map((attempt) => attempt.providerInstanceId),
            [ProviderInstanceId.make("antigravity"), ProviderInstanceId.make("antigravity_2")],
          );

          assert.isFalse(yield* orchestrator.restartContinuationPending(staleAttempt1Run));
          const staleAwaitFiber = yield* orchestrator
            .awaitRestartContinuation(staleAttempt1Run)
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* TestClock.adjust("1 second");
          const stalePolled = Option.fromUndefinedOr(staleAwaitFiber.pollUnsafe());
          assert.isTrue(Option.isSome(stalePolled));
          if (Option.isSome(stalePolled)) {
            assert.isTrue(Exit.isSuccess(stalePolled.value));
          }
        }).pipe(
          Effect.ensuring(Deferred.succeed(releaseGate, undefined).pipe(Effect.ignore)),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "does not prematurely fail a delegated subagent via recoverDelegatedTask while Antigravity failover is gated",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-cf08-recover-race");
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
        const parentThreadId = ThreadId.make("thread:parent-cf08-recover");
        const gateEntered = yield* Deferred.make<void>();
        const releaseGate = yield* Deferred.make<void>();
        let gatedOnce = false;
        const layer = makeIntegrationLayer({
          name: "antigravity-cf08-recover-race",
          cwd,
          providers,
          recordedTurns: [],
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return { behavior: "succeed", assistantText: "Parent running." };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "AUTHORITATIVE_RESULT_FROM_AGY2" };
          },
          wrapFailover: (inner) => ({
            ...inner,
            recordQuotaExhausted: (input) =>
              Effect.gen(function* () {
                if (!gatedOnce && input.instanceId === ProviderInstanceId.make("antigravity")) {
                  gatedOnce = true;
                  yield* Deferred.succeed(gateEntered, undefined);
                  yield* Deferred.await(releaseGate);
                }
                yield* inner.recordQuotaExhausted(input);
              }),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent-cf08-recover"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent CF-08 recover",
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
            commandId: CommandId.make("start-parent-cf08-recover"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent-cf08-recover"),
            text: "Delegate task",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:antigravity-cf08-recover"),
            requestNamespace: "provider-session:antigravity-cf08-recover",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:antigravity-cf08-recover",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          const delegated = yield* mcp.delegateTask(scope, {
            task: "Complete on backup after quota failure.",
            mode: "async",
            clientRequestId: "req-cf08-recover",
          });
          const childThreadId = delegated.childThreadId!;

          yield* Deferred.await(gateEntered);
          // Same parent-locked finalizeAppOwnedSubagent path as settlesChildWork.
          const recoverFiber = yield* orchestrator
            .recoverDelegatedTask(childThreadId)
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* TestClock.adjust("1 second");

          const gatedParent = yield* orchestrator.getThreadProjection(parentThreadId);
          const gatedTask = gatedParent.subagents.find((task) => task.id === delegated.taskId);
          assert.isDefined(gatedTask);
          assert.notEqual(gatedTask?.status, "failed");
          assert.isNull(gatedTask?.result ?? null);
          assert.isDefined(recoverFiber.pollUnsafe());
          yield* Fiber.join(recoverFiber);

          const waitSubagentDone = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "subagent.updated" &&
                event.threadId === parentThreadId &&
                event.payload.id === delegated.taskId &&
                (event.payload.status === "completed" || event.payload.status === "failed"),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Deferred.succeed(releaseGate, undefined);
          yield* Fiber.join(waitSubagentDone);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const subagent = parentProjection.subagents.find((task) => task.id === delegated.taskId);
          assert.equal(subagent?.status, "completed");
          assert.include(subagent?.result ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");

          const status = yield* mcp.taskStatus(scope, delegated.taskId);
          assert.equal(status.status, "completed");
          assert.include(status.summary ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
        }).pipe(
          Effect.ensuring(Deferred.succeed(releaseGate, undefined).pipe(Effect.ignore)),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "aborts pending child Antigravity failover when cancelDelegatedTask is invoked while failover is gated",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-cf08-cancel-race");
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
        const parentThreadId = ThreadId.make("thread:parent-cf08-cancel");
        const gateEntered = yield* Deferred.make<void>();
        const releaseGate = yield* Deferred.make<void>();
        const cancelEntered = yield* Deferred.make<void>();
        let gatedOnce = false;
        let cancelGateArmed = false;
        const layer = makeIntegrationLayer({
          name: "antigravity-cf08-cancel-race",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return { behavior: "succeed", assistantText: "Parent running." };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "Should never start on AGY-2." };
          },
          wrapFailover: (inner) => ({
            ...inner,
            recordQuotaExhausted: (input) =>
              Effect.gen(function* () {
                if (!gatedOnce && input.instanceId === ProviderInstanceId.make("antigravity")) {
                  gatedOnce = true;
                  yield* Deferred.succeed(gateEntered, undefined);
                  yield* Deferred.await(releaseGate);
                }
                yield* inner.recordQuotaExhausted(input);
              }),
          }),
          wrapProjectionStore: (store) => ({
            ...store,
            getThreadRecords: ((
              threadId: ThreadId,
              fields: ReadonlyArray<string>,
              filter?: unknown,
            ) =>
              store
                .getThreadRecords(threadId, fields as never, filter as never)
                .pipe(
                  Effect.tap(() =>
                    cancelGateArmed && threadId === parentThreadId && fields.includes("subagents")
                      ? Deferred.succeed(cancelEntered, undefined).pipe(Effect.asVoid)
                      : Effect.void,
                  ),
                )) as typeof store.getThreadRecords,
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent-cf08-cancel"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent CF-08 cancel",
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
            commandId: CommandId.make("start-parent-cf08-cancel"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent-cf08-cancel"),
            text: "Delegate and cancel",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:antigravity-cf08-cancel"),
            requestNamespace: "provider-session:antigravity-cf08-cancel",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:antigravity-cf08-cancel",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          const delegated = yield* mcp.delegateTask(scope, {
            task: "Cancel while failover is gated.",
            mode: "async",
            clientRequestId: "req-cf08-cancel",
          });
          const childThreadId = delegated.childThreadId!;

          yield* Deferred.await(gateEntered);
          cancelGateArmed = true;
          const cancelFiber = yield* orchestrator
            .cancelDelegatedTask({
              parentThreadId,
              taskId: delegated.taskId,
              command: {
                type: "thread.stop",
                commandId: CommandId.make("stop-cf08-cancel"),
                threadId: childThreadId,
                stopDelegatedTasks: false,
                reason: "Cancelled while failover was pending.",
              },
              stopDescendants: () => Effect.void,
            })
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(cancelEntered);
          yield* Deferred.succeed(releaseGate, undefined);
          yield* Fiber.join(cancelFiber);
          yield* worker.drain();

          const childTurns = recordedTurns.filter((turn) => turn.threadId === childThreadId);
          assert.deepEqual(
            childTurns.map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity")],
          );
          const childProjection = yield* orchestrator.getThreadProjection(childThreadId);
          assert.equal(childProjection.attempts.length, 1);
          assert.equal(
            childProjection.attempts[0]?.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const subagent = parentProjection.subagents.find((task) => task.id === delegated.taskId);
          assert.isDefined(subagent);
          assert.equal(subagent?.status, "interrupted");
          assert.equal(subagent?.completionDelivery?.state, "disposed");
        }).pipe(
          Effect.ensuring(Deferred.succeed(releaseGate, undefined).pipe(Effect.ignore)),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R2-CF-06: suppresses reactive usage_limit failover for pinned delegate_task (async and wait) while unpinned control still fails over",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r2-cf06-delegate");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 100 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 15 }]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:parent-r2-cf06-delegate");
        const releaseParent = yield* Deferred.make<void>();
        const parentStarted = yield* Deferred.make<void>();
        const layer = makeIntegrationLayer({
          name: "antigravity-r2-cf06-delegate",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return {
                behavior: "succeed",
                assistantText: "Parent done.",
                pause: { onStarted: parentStarted, release: releaseParent },
              };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "BACKUP_AGY2_RESULT" };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent-r2-cf06-delegate"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent R2-CF-06 delegate",
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
            commandId: CommandId.make("start-parent-r2-cf06-delegate"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent-r2-cf06-delegate"),
            text: "Keep parent active.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(parentStarted);

          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:r2-cf06-delegate"),
            requestNamespace: "provider-session:r2-cf06-delegate",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:r2-cf06-delegate",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          // 1A: Pinned async delegate_task stays on antigravity and fails cleanly on usage_limit.
          const waitAsyncFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "subagent.updated" &&
                event.threadId === parentThreadId &&
                (event.payload.status === "failed" || event.payload.status === "completed"),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const asyncPinned = yield* mcp.delegateTask(scope, {
            target: {
              providerInstanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            task: "PINNED_ASYNC_TASK",
            mode: "async",
            clientRequestId: "r2-cf06-async",
          });
          assert.equal(asyncPinned.providerInstanceId, ProviderInstanceId.make("antigravity"));
          yield* Fiber.join(waitAsyncFailed);
          yield* worker.drain();

          const asyncChild = yield* orchestrator.getThreadProjection(asyncPinned.childThreadId!);
          assert.isTrue(asyncChild.thread.antigravityFailoverPinned);
          assert.isTrue(asyncChild.runs[0]?.antigravityFailoverPinned);
          assert.equal(
            asyncChild.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(asyncChild.runs[0]?.status, "failed");
          assert.equal(asyncChild.attempts.length, 1);
          assert.deepEqual(
            recordedTurns
              .filter((turn) => turn.threadId === asyncPinned.childThreadId)
              .map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity")],
          );

          // 1B: Pinned wait delegate_task also stays on antigravity and returns failure without failover.
          const waitPinned = yield* mcp.delegateTask(scope, {
            target: {
              providerInstanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            task: "PINNED_WAIT_TASK",
            mode: "wait",
            timeoutMs: 10_000,
            clientRequestId: "r2-cf06-wait",
          });
          assert.equal(waitPinned.status, "failed");
          assert.equal(waitPinned.providerInstanceId, ProviderInstanceId.make("antigravity"));
          assert.deepEqual(
            recordedTurns
              .filter((turn) => turn.threadId === waitPinned.childThreadId)
              .map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity")],
          );

          // Unpinned control (omits providerInstanceId): still fails over to antigravity_2.
          const unpinnedControl = yield* mcp.delegateTask(scope, {
            target: { model: "claude-opus-5-5" },
            task: "UNPINNED_CONTROL_TASK",
            mode: "wait",
            timeoutMs: 10_000,
            clientRequestId: "r2-cf06-unpinned-control",
          });
          assert.equal(unpinnedControl.status, "completed");
          assert.equal(
            unpinnedControl.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );
          assert.include(unpinnedControl.summary ?? "", "BACKUP_AGY2_RESULT");
        }).pipe(
          Effect.ensuring(Deferred.succeed(releaseParent, undefined).pipe(Effect.ignore)),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R2-CF-06: preserves pinning across idle create_threads, t3_thread_launch, post-configure send, queued follow-up promotion, and clears pin on unpinned switch",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r2-cf06-followups");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 100 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "3p-5h", usedPercent: 15 }]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:parent-r2-cf06-followups");
        const parentStarted = yield* Deferred.make<void>();
        const releaseParent = yield* Deferred.make<void>();
        const queuedRun1Started = yield* Deferred.make<void>();
        const releaseQueuedRun1 = yield* Deferred.make<void>();
        const layer = makeIntegrationLayer({
          name: "antigravity-r2-cf06-followups",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return {
                behavior: "succeed",
                assistantText: "Parent running.",
                pause: { onStarted: parentStarted, release: releaseParent },
              };
            }
            if (turn.message.text === "QUEUED_RUN1_CONTEXT") {
              return {
                behavior: "succeed",
                assistantText: "Run 1 context stayed on AGY-1.",
                pause: { onStarted: queuedRun1Started, release: releaseQueuedRun1 },
              };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "Executed on AGY-2." };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-parent-r2-cf06-followups"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Parent R2-CF-06 followups",
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
            commandId: CommandId.make("start-parent-r2-cf06-followups"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:parent-r2-cf06-followups"),
            text: "Keep parent active.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(parentStarted);

          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:r2-cf06-followups"),
            requestNamespace: "provider-session:r2-cf06-followups",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:r2-cf06-followups",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          // 1. Idle pinned create_threads + t3_thread_send stays on antigravity.
          const idleCreated = yield* mcp.createThreads(scope, {
            threads: [
              {
                title: "Idle Pinned",
                target: {
                  providerInstanceId: ProviderInstanceId.make("antigravity"),
                  model: "claude-opus-5-5",
                },
              },
            ],
            clientRequestId: "r2-cf06-idle-create",
          });
          const idleThreadId = idleCreated.threads[0]!.threadId;
          const waitIdleFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === idleThreadId &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* mcp.sendToThread(scope, {
            threadId: idleThreadId,
            message: "IDLE_PINNED_SEND",
            clientRequestId: "r2-cf06-idle-send",
          });
          yield* Fiber.join(waitIdleFailed);
          yield* worker.drain();
          assert.deepEqual(
            recordedTurns
              .filter((turn) => turn.threadId === idleThreadId)
              .map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity")],
          );

          // 2. Idle thread created with antigravityFailoverPinned (matching t3_thread_launch) + sendToThread.
          const launchedThreadId = ThreadId.make("thread:r2-cf06-launched");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-r2-cf06-launched"),
            threadId: launchedThreadId,
            projectId: PROJECT_ID,
            title: "Launched Pinned",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: true,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "agent",
            creationSource: "mcp",
          });
          const waitLaunchedFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === launchedThreadId &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* mcp.sendToThread(scope, {
            threadId: launchedThreadId,
            message: "LAUNCHED_PINNED_SEND",
            clientRequestId: "r2-cf06-launched-send",
          });
          yield* Fiber.join(waitLaunchedFailed);
          yield* worker.drain();
          assert.deepEqual(
            recordedTurns
              .filter((turn) => turn.threadId === launchedThreadId)
              .map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity")],
          );

          // 3. Re-pin via provider.switch (matching t3_thread_configure), verify pinned send,
          //    then unpin via thread.model-selection.set without antigravityFailoverPinned and verify failover resumes.
          yield* orchestrator.dispatch({
            type: "provider.switch",
            commandId: CommandId.make("configure-pin-r2-cf06"),
            threadId: idleThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: true,
          });
          const waitPostConfigureFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === idleThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* mcp.sendToThread(scope, {
            threadId: idleThreadId,
            message: "POST_CONFIGURE_PINNED_SEND",
            clientRequestId: "r2-cf06-post-configure-send",
          });
          yield* Fiber.join(waitPostConfigureFailed);
          yield* worker.drain();
          assert.deepEqual(
            recordedTurns
              .filter((turn) => turn.threadId === idleThreadId)
              .map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity"), ProviderInstanceId.make("antigravity")],
          );

          // Unpin cleanly via thread.model-selection.set without antigravityFailoverPinned.
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("unpin-r2-cf06"),
            threadId: idleThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
          });
          const afterUnpin = yield* orchestrator.getThreadProjection(idleThreadId);
          assert.notEqual(afterUnpin.thread.antigravityFailoverPinned, true);
          const waitUnpinnedCompleted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === idleThreadId &&
                event.payload.ordinal === 3 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* mcp.sendToThread(scope, {
            threadId: idleThreadId,
            message: "POST_UNPIN_SEND",
            clientRequestId: "r2-cf06-post-unpin-send",
          });
          yield* Fiber.join(waitUnpinnedCompleted);
          const afterUnpinnedSend = yield* orchestrator.getThreadProjection(idleThreadId);
          assert.equal(
            afterUnpinnedSend.runs[2]?.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );

          // 4. Queued follow-up promotion on a pinned thread stays on antigravity with no ContextHandoff to AGY-2.
          const queuedCreated = yield* mcp.createThreads(scope, {
            threads: [
              {
                title: "Queued Pinned",
                prompt: "QUEUED_RUN1_CONTEXT",
                target: {
                  providerInstanceId: ProviderInstanceId.make("antigravity"),
                  model: "claude-opus-5-5",
                },
              },
            ],
            clientRequestId: "r2-cf06-queued-create",
          });
          const queuedThreadId = queuedCreated.threads[0]!.threadId;
          yield* Deferred.await(queuedRun1Started);
          yield* mcp.sendToThread(scope, {
            threadId: queuedThreadId,
            message: "QUEUED_FOLLOWUP_RUN2",
            mode: "queue",
            clientRequestId: "r2-cf06-queued-run2",
          });
          const waitQueuedRun2Failed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === queuedThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Deferred.succeed(releaseQueuedRun1, undefined);
          yield* Fiber.join(waitQueuedRun2Failed);
          yield* worker.drain();

          const queuedProjection = yield* orchestrator.getThreadProjection(queuedThreadId);
          assert.equal(
            queuedProjection.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(
            queuedProjection.runs[1]?.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(queuedProjection.contextHandoffs.length, 0);
          assert.deepEqual(
            recordedTurns
              .filter((turn) => turn.threadId === queuedThreadId)
              .map((turn) => turn.instanceId),
            [ProviderInstanceId.make("antigravity"), ProviderInstanceId.make("antigravity")],
          );
        }).pipe(
          Effect.ensuring(
            Effect.all([
              Deferred.succeed(releaseParent, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseQueuedRun1, undefined).pipe(Effect.ignore),
            ]),
          ),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R2-CF-07: preserves Attempt 2 authoritative result and parent wake across TOCTOU races in recoverDelegatedTask, taskStatus, and delegateTask(mode=wait)",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r2-cf07-toctou");
        const isInspectFailoverRead = (fields: ReadonlyArray<string>) =>
          fields.length === 6 &&
          fields.includes("runs") &&
          fields.includes("attempts") &&
          fields.includes("providerSessions") &&
          fields.includes("providerThreads") &&
          fields.includes("runtimeRequests") &&
          fields.includes("turnItems");

        const runToctouScenario = <E, R>(
          name: string,
          scenario: (ctx: {
            readonly orchestrator: Orchestrator.OrchestratorV2["Service"];
            readonly mcp: OrchestratorMcpService.OrchestratorMcpService["Service"];
            readonly worker: EffectWorker.OrchestrationEffectWorkerV2["Service"];
            readonly parentThreadId: ThreadId;
            readonly scope: McpInvocationScope;
            readonly releaseParent: Deferred.Deferred<void>;
            readonly failoverEntered: Deferred.Deferred<void>;
            readonly releaseFailover: Deferred.Deferred<void>;
            readonly attempt2Started: Deferred.Deferred<void>;
            readonly releaseAttempt2: Deferred.Deferred<void>;
            readonly arm: (childThreadId: ThreadId | "*") => void;
            readonly wasTriggered: () => boolean;
          }) => Effect.Effect<void, E, R>,
        ) =>
          Effect.gen(function* () {
            const providers = [
              makeAntigravityProvider("antigravity", {
                displayName: "AGY-1",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
              }),
              makeAntigravityProvider("antigravity_2", {
                displayName: "AGY-2",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 15 }]),
              }),
            ];
            const parentThreadId = ThreadId.make(`thread:parent-${name}`);
            const parentStarted = yield* Deferred.make<void>();
            const releaseParent = yield* Deferred.make<void>();
            const failoverEntered = yield* Deferred.make<void>();
            const releaseFailover = yield* Deferred.make<void>();
            const attempt2Started = yield* Deferred.make<void>();
            const releaseAttempt2 = yield* Deferred.make<void>();
            let armedChildThreadId: ThreadId | "*" | null = null;
            let triggered = false;
            let failoverGated = false;

            const layer = makeIntegrationLayer({
              name,
              cwd,
              providers,
              recordedTurns: [],
              runContinuationWorker: true,
              outcomeFor: (instanceId, turn) => {
                if (turn.threadId === parentThreadId) {
                  if (turn.runOrdinal === 1) {
                    return {
                      behavior: "succeed",
                      assistantText: "Parent turn 1 done.",
                      pause: { onStarted: parentStarted, release: releaseParent },
                    };
                  }
                  return { behavior: "succeed", assistantText: "Parent woke on Attempt 2." };
                }
                if (instanceId === ProviderInstanceId.make("antigravity")) {
                  return { behavior: "usage_limit" };
                }
                return {
                  behavior: "succeed",
                  assistantText: "AUTHORITATIVE_RESULT_FROM_AGY2",
                  pause: { onStarted: attempt2Started, release: releaseAttempt2 },
                };
              },
              wrapFailover: (inner) => ({
                ...inner,
                recordQuotaExhausted: (input) =>
                  Effect.gen(function* () {
                    if (
                      !failoverGated &&
                      input.instanceId === ProviderInstanceId.make("antigravity")
                    ) {
                      failoverGated = true;
                      yield* Deferred.succeed(failoverEntered, undefined);
                      yield* Deferred.await(releaseFailover);
                    }
                    yield* inner.recordQuotaExhausted(input);
                  }),
              }),
              wrapProjectionStore: (store) => ({
                ...store,
                getThreadRecords: ((
                  threadId: ThreadId,
                  fields: ReadonlyArray<string>,
                  filter?: unknown,
                ) =>
                  Effect.gen(function* () {
                    const matchesTarget =
                      armedChildThreadId === "*"
                        ? threadId !== parentThreadId
                        : armedChildThreadId !== null && threadId === armedChildThreadId;
                    if (matchesTarget && !triggered && isInspectFailoverRead(fields)) {
                      triggered = true;
                      yield* Deferred.await(failoverEntered);
                      yield* Deferred.succeed(releaseFailover, undefined);
                      yield* Deferred.await(attempt2Started);
                    }
                    return yield* store.getThreadRecords(
                      threadId,
                      fields as never,
                      filter as never,
                    );
                  })) as typeof store.getThreadRecords,
              }),
            });

            yield* Effect.gen(function* () {
              const orchestrator = yield* Orchestrator.OrchestratorV2;
              const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
              const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

              yield* orchestrator.dispatch({
                type: "thread.create",
                commandId: CommandId.make(`create-${name}`),
                threadId: parentThreadId,
                projectId: PROJECT_ID,
                title: name,
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
                commandId: CommandId.make(`start-${name}`),
                threadId: parentThreadId,
                messageId: MessageId.make(`message:${name}`),
                text: "Run TOCTOU check.",
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              yield* Deferred.await(parentStarted);

              const scope: McpInvocationScope = {
                environmentId: EnvironmentId.make(`environment:${name}`),
                requestNamespace: `provider-session:${name}`,
                thread: {
                  threadId: parentThreadId,
                  providerSessionId: `provider-session:${name}`,
                  providerInstanceId: ProviderInstanceId.make("antigravity"),
                },
                client: undefined,
                capabilities: new Set(["orchestration"]),
                issuedAt: 1,
              };

              yield* scenario({
                orchestrator,
                mcp,
                worker,
                parentThreadId,
                scope,
                releaseParent,
                failoverEntered,
                releaseFailover,
                attempt2Started,
                releaseAttempt2,
                arm: (childThreadId) => {
                  armedChildThreadId = childThreadId;
                },
                wasTriggered: () => triggered,
              });
            }).pipe(
              Effect.ensuring(
                Effect.all([
                  Deferred.succeed(releaseParent, undefined).pipe(Effect.ignore),
                  Deferred.succeed(releaseFailover, undefined).pipe(Effect.ignore),
                  Deferred.succeed(releaseAttempt2, undefined).pipe(Effect.ignore),
                ]),
              ),
              Effect.provide(layer),
            );
          });

        // Path 1A: recoverDelegatedTask -> finalizeAppOwnedSubagent racing Attempt 2 commit.
        yield* runToctouScenario("r2-cf07-1a", (ctx) =>
          Effect.gen(function* () {
            const task1A = yield* ctx.mcp.delegateTask(ctx.scope, {
              task: "Check 1A recoverDelegatedTask TOCTOU.",
              mode: "async",
              clientRequestId: "r2-cf07-1a",
            });
            yield* Deferred.await(ctx.failoverEntered);
            ctx.arm(task1A.childThreadId!);
            yield* ctx.orchestrator.recoverDelegatedTask(task1A.childThreadId!);
            assert.isTrue(ctx.wasTriggered());

            const parentMid1A = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            const subagentMid1A = parentMid1A.subagents.find((task) => task.id === task1A.taskId);
            assert.equal(subagentMid1A?.status, "running");
            assert.isNull(subagentMid1A?.result ?? null);

            const wait1ADone = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "subagent.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.id === task1A.taskId &&
                  (event.payload.status === "completed" || event.payload.status === "failed"),
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseAttempt2, undefined);
            yield* Fiber.join(wait1ADone);
            yield* ctx.orchestrator.recoverDelegatedTask(task1A.childThreadId!);

            const parentFinal1A = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            const subagentFinal1A = parentFinal1A.subagents.find(
              (task) => task.id === task1A.taskId,
            );
            assert.equal(subagentFinal1A?.status, "completed");
            assert.include(subagentFinal1A?.result ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
          }),
        );

        // Path 1B: mcp.taskStatus + defense-in-depth acknowledge guard racing Attempt 2 commit,
        // then verify parent wakes when Attempt 2 completes.
        yield* runToctouScenario("r2-cf07-1b", (ctx) =>
          Effect.gen(function* () {
            const task1B = yield* ctx.mcp.delegateTask(ctx.scope, {
              task: "Check 1B taskStatus TOCTOU and parent wake.",
              mode: "async",
              clientRequestId: "r2-cf07-1b",
            });
            yield* Deferred.await(ctx.failoverEntered);
            ctx.arm(task1B.childThreadId!);
            const statusDuringRace = yield* ctx.mcp.taskStatus(ctx.scope, task1B.taskId);
            assert.isTrue(ctx.wasTriggered());
            assert.notEqual(statusDuringRace.status, "failed");
            assert.equal(statusDuringRace.workState, "working");

            // Defense-in-depth: even a direct acknowledge command while Attempt 2 runs must not write acknowledged.
            const parentRun1 = (yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId))
              .runs[0]!;
            yield* ctx.orchestrator.dispatch({
              type: "delegated_task.completion-delivery.acknowledge",
              commandId: CommandId.make("ack-r2-cf07-1b-premature"),
              parentThreadId: ctx.parentThreadId,
              taskId: task1B.taskId,
              observedByRunId: parentRun1.id,
            });
            const parentMid1B = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            const subagentMid1B = parentMid1B.subagents.find((task) => task.id === task1B.taskId);
            assert.notEqual(subagentMid1B?.completionDelivery?.state, "acknowledged");

            const waitParentRun1Done = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.id === parentRun1.id &&
                  event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseParent, undefined);
            yield* Fiber.join(waitParentRun1Done);

            const waitParentWake = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.ordinal === 2 &&
                  event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseAttempt2, undefined);
            yield* Fiber.join(waitParentWake);
            yield* ctx.worker.drain();

            const parentAfterWake = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            assert.isAtLeast(parentAfterWake.runs.length, 2);
            const final1B = parentAfterWake.subagents.find((task) => task.id === task1B.taskId);
            assert.equal(final1B?.status, "completed");
            assert.include(final1B?.result ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
          }),
        );

        // Path 1C: delegateTask(mode="wait") racing Attempt 2 commit on waitForTask's automatic readTask.
        yield* runToctouScenario("r2-cf07-1c", (ctx) =>
          Effect.gen(function* () {
            ctx.arm("*");
            const waitTaskFiber = yield* ctx.mcp
              .delegateTask(ctx.scope, {
                task: "Check 1C delegate_task(mode=wait) TOCTOU.",
                mode: "wait",
                timeoutMs: 10_000,
                clientRequestId: "r2-cf07-1c",
              })
              .pipe(Effect.forkChild({ startImmediately: true }));
            yield* Deferred.await(ctx.attempt2Started);
            yield* Effect.yieldNow;
            assert.isTrue(ctx.wasTriggered());
            assert.isUndefined(waitTaskFiber.pollUnsafe());
            yield* Deferred.succeed(ctx.releaseAttempt2, undefined);
            const result1C = yield* Fiber.join(waitTaskFiber);
            assert.equal(result1C.status, "completed");
            assert.equal(result1C.providerInstanceId, ProviderInstanceId.make("antigravity_2"));
            assert.include(result1C.summary ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
          }),
        );
      }),
  );

  it.effect(
    "R2-CF-08: waitForThread started during a running turn wakes promptly on mid-wait declined failover (backup exhaustion, >10s failover, and mid-failover thread.stop)",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r2-cf08-midwait");
        const runMidWaitScenario = <E, R>(
          name: string,
          decline: (ctx: {
            readonly orchestrator: Orchestrator.OrchestratorV2["Service"];
            readonly providersRef: Ref.Ref<ReadonlyArray<ServerProvider>>;
            readonly threadId: ThreadId;
            readonly waitFiber: Fiber.Fiber<
              ThreadManagementService.ThreadManagementWaitResult,
              ThreadManagementService.ThreadManagementError
            >;
            readonly releaseFailover: Deferred.Deferred<void>;
          }) => Effect.Effect<void, E, R>,
        ) =>
          Effect.gen(function* () {
            const initialProviders = [
              makeAntigravityProvider("antigravity", {
                displayName: "AGY-1",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
              }),
              makeAntigravityProvider("antigravity_2", {
                displayName: "AGY-2",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
              }),
            ];
            const providersRef = yield* Ref.make<ReadonlyArray<ServerProvider>>(initialProviders);
            const threadId = ThreadId.make(`thread:${name}`);
            const runStarted = yield* Deferred.make<void>();
            const releaseRun = yield* Deferred.make<void>();
            const failoverEntered = yield* Deferred.make<void>();
            const releaseFailover = yield* Deferred.make<void>();
            let gated = false;

            const layer = makeIntegrationLayer({
              name,
              cwd,
              providers: initialProviders,
              providersRef,
              recordedTurns: [],
              outcomeFor: () => ({
                behavior: "usage_limit",
                pause: { onStarted: runStarted, release: releaseRun },
              }),
              wrapFailover: (inner) => ({
                ...inner,
                recordQuotaExhausted: (input) =>
                  Effect.gen(function* () {
                    if (!gated) {
                      gated = true;
                      yield* Deferred.succeed(failoverEntered, undefined);
                      yield* Deferred.await(releaseFailover);
                    }
                    yield* inner.recordQuotaExhausted(input);
                  }),
              }),
            });

            yield* Effect.gen(function* () {
              const orchestrator = yield* Orchestrator.OrchestratorV2;
              const threadManagement = yield* ThreadManagementService.ThreadManagementService;

              yield* orchestrator.dispatch({
                type: "thread.create",
                commandId: CommandId.make(`${name}-create`),
                threadId,
                projectId: PROJECT_ID,
                title: name,
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
                commandId: CommandId.make(`${name}-msg`),
                threadId,
                messageId: MessageId.make(`message:${name}`),
                text: "Start running then fail with usage_limit.",
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              yield* Deferred.await(runStarted);
              const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;

              // Start waitForThread while the run is actively running (initial.pending === false).
              const waitFiber = yield* threadManagement
                .waitForThread({
                  projectId: PROJECT_ID,
                  threadId,
                  runId: run.id,
                  timeoutMs: 24 * 60 * 60 * 1_000,
                })
                .pipe(Effect.forkChild({ startImmediately: true }));
              yield* Effect.yieldNow;
              yield* Deferred.succeed(releaseRun, undefined);
              yield* Deferred.await(failoverEntered);
              // Give waitForThread a turn to observe run.updated(failed) and register on awaitRestartContinuation.
              yield* Effect.yieldNow;
              yield* Effect.yieldNow;

              yield* decline({
                orchestrator,
                providersRef,
                threadId,
                waitFiber,
                releaseFailover,
              });

              const result = yield* Fiber.join(waitFiber);
              assert.isFalse(result.timedOut);
              assert.equal(result.run?.id, run.id);
              assert.equal(result.run?.status, "failed");
            }).pipe(
              Effect.ensuring(
                Effect.all([
                  Deferred.succeed(releaseRun, undefined).pipe(Effect.ignore),
                  Deferred.succeed(releaseFailover, undefined).pipe(Effect.ignore),
                ]),
              ),
              Effect.provide(layer),
            );
          });

        // Scenario 1: Mid-wait backup exhaustion declines failover without run.updated.
        yield* runMidWaitScenario("r2-cf08-exhaust", (ctx) =>
          Effect.gen(function* () {
            yield* Ref.set(ctx.providersRef, [
              makeAntigravityProvider("antigravity", {
                displayName: "AGY-1",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
              }),
              makeAntigravityProvider("antigravity_2", {
                displayName: "AGY-2",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
              }),
            ]);
            yield* Deferred.succeed(ctx.releaseFailover, undefined);
          }),
        );

        // Scenario 2: Mid-wait failover takes >10s (past awaitRestartContinuation's 10s cap) before declining.
        yield* runMidWaitScenario("r2-cf08-slow", (ctx) =>
          Effect.gen(function* () {
            yield* TestClock.adjust("15 seconds");
            yield* Effect.yieldNow;
            assert.isUndefined(ctx.waitFiber.pollUnsafe());
            yield* Ref.set(ctx.providersRef, [
              makeAntigravityProvider("antigravity", {
                displayName: "AGY-1",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
              }),
              makeAntigravityProvider("antigravity_2", {
                displayName: "AGY-2",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 100 }]),
              }),
            ]);
            yield* Deferred.succeed(ctx.releaseFailover, undefined);
          }),
        );

        // Scenario 3: Mid-failover thread.stop aborts failover without run.updated.
        yield* runMidWaitScenario("r2-cf08-stop", (ctx) =>
          Effect.gen(function* () {
            const stopFiber = yield* ctx.orchestrator
              .dispatch({
                type: "thread.stop",
                commandId: CommandId.make("r2-cf08-stop-cmd"),
                threadId: ctx.threadId,
                reason: "User stopped during failover",
              })
              .pipe(Effect.forkChild({ startImmediately: true }));
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseFailover, undefined);
            yield* Fiber.join(stopFiber);
          }),
        );
      }),
  );

  it.effect(
    "R2-CF-06 review regressions: thread shell pin visibility, unpinned delegate_task from pinned parent, steering pin sync, and queued pin handoff",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r2-cf06-review");
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
              { id: "gemini-5h", usedPercent: 90 },
              { id: "3p-5h", usedPercent: 90 },
            ]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:r2-cf06-review-parent");
        const steerThreadId = ThreadId.make("thread:r2-cf06-review-steer");
        const queueSwitchThreadId = ThreadId.make("thread:r2-cf06-review-queue");
        const parentStarted = yield* Deferred.make<void>();
        const releaseParent = yield* Deferred.make<void>();
        const steerRun1Started = yield* Deferred.make<void>();
        const releaseSteerRun1 = yield* Deferred.make<void>();
        const steerPinnedStarted = yield* Deferred.make<void>();
        const releaseSteerPinned = yield* Deferred.make<void>();
        const queueRun1Started = yield* Deferred.make<void>();
        const releaseQueueRun1 = yield* Deferred.make<void>();

        const layer = makeIntegrationLayer({
          name: "antigravity-r2-cf06-review",
          cwd,
          providers,
          recordedTurns,
          runContinuationWorker: true,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              return {
                behavior: "succeed",
                assistantText: "Pinned parent active.",
                pause: { onStarted: parentStarted, release: releaseParent },
              };
            }
            if (turn.message.text === "STEER_INITIAL_UNPINNED") {
              return {
                behavior: "succeed",
                assistantText: "Steer initial running.",
                pause: { onStarted: steerRun1Started, release: releaseSteerRun1 },
              };
            }
            if (turn.message.text === "STEER_PINNED_ACTIVE") {
              return {
                behavior: "succeed",
                assistantText: "Steer pinned active.",
                pause: { onStarted: steerPinnedStarted, release: releaseSteerPinned },
              };
            }
            if (turn.message.text === "QUEUE_RUN1_PINNED") {
              return {
                behavior: "succeed",
                assistantText: "Queue run 1 pinned.",
                pause: { onStarted: queueRun1Started, release: releaseQueueRun1 },
              };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "SUCCEEDED_ON_AGY2" };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          // 1. Pinned parent exposes antigravityFailoverPinned in getThreadShell, while an
          //    unpinned delegate_task from it does NOT inherit the pin and fails over to AGY-2.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-r2-cf06-review-parent"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "Pinned Parent",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: true,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const parentShell = yield* orchestrator.getThreadShell(parentThreadId);
          assert.equal(parentShell?.antigravityFailoverPinned, true);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-r2-cf06-review-parent"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:r2-cf06-review-parent"),
            text: "Keep pinned parent active.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(parentStarted);

          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:r2-cf06-review"),
            requestNamespace: "provider-session:r2-cf06-review",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:r2-cf06-review",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          const unpinnedChild = yield* mcp.delegateTask(scope, {
            task: "Unpinned child from pinned parent.",
            mode: "wait",
            timeoutMs: 10_000,
            clientRequestId: "r2-cf06-review-unpinned-child",
          });
          assert.equal(unpinnedChild.status, "completed");
          assert.equal(unpinnedChild.providerInstanceId, ProviderInstanceId.make("antigravity_2"));
          assert.include(unpinnedChild.summary ?? "", "SUCCEEDED_ON_AGY2");
          const childProjection = yield* orchestrator.getThreadProjection(
            unpinnedChild.childThreadId!,
          );
          assert.notEqual(childProjection.thread.antigravityFailoverPinned, true);
          assert.notEqual(childProjection.runs[0]?.antigravityFailoverPinned, true);

          // 2. Steering/restarting an active unpinned run on AGY-2 with an explicit pinned
          //    selection sets antigravityFailoverPinned on both thread and restartedRun, keeping
          //    it on AGY-1 without failover.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-r2-cf06-review-steer"),
            threadId: steerThreadId,
            projectId: PROJECT_ID,
            title: "Steer Pin Thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity_2"),
              model: "claude-opus-5-5",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const waitSteerRun1Running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.threadId === steerThreadId &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-r2-cf06-review-steer-1"),
            threadId: steerThreadId,
            messageId: MessageId.make("message:r2-cf06-review-steer-1"),
            text: "STEER_INITIAL_UNPINNED",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(steerRun1Started);
          yield* Fiber.join(waitSteerRun1Running);

          const steerTargetRun1Id = (yield* orchestrator.getThreadProjection(steerThreadId))
            .runs[0]!.id;
          const waitSteerFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === steerThreadId &&
                event.payload.id === steerTargetRun1Id &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("pin-steer-r2-cf06-review-steer"),
            threadId: steerThreadId,
            messageId: MessageId.make("message:r2-cf06-review-steer-pin"),
            text: "STEER_WITH_EXPLICIT_PIN",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: true,
            dispatchMode: { type: "restart_active", targetRunId: steerTargetRun1Id },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.succeed(releaseSteerRun1, undefined);
          yield* Fiber.join(waitSteerFailed);
          yield* worker.drain();
          const afterPinnedSteer = yield* orchestrator.getThreadProjection(steerThreadId);
          assert.equal(afterPinnedSteer.thread.antigravityFailoverPinned, true);
          assert.equal(afterPinnedSteer.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(
            afterPinnedSteer.runs[0]?.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );

          // Steering a pinned active run without modelSelection preserves the pin, while steering
          // with a new unpinned modelSelection clears the pin on both thread and restartedRun.
          const waitSteerPinnedRunning = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.threadId === steerThreadId &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-r2-cf06-review-steer-pinned"),
            threadId: steerThreadId,
            messageId: MessageId.make("message:r2-cf06-review-steer-pinned"),
            text: "STEER_PINNED_ACTIVE",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(steerPinnedStarted);
          yield* Fiber.join(waitSteerPinnedRunning);
          const pinnedSteerTargetRunId = (yield* orchestrator.getThreadProjection(steerThreadId))
            .runs[1]!.id;
          const waitUnpinnedSteerCompleted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === steerThreadId &&
                event.payload.id === pinnedSteerTargetRunId &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("unpin-steer-r2-cf06-review"),
            threadId: steerThreadId,
            messageId: MessageId.make("message:r2-cf06-review-unpin-steer"),
            text: "STEER_UNPIN_SWITCH",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            dispatchMode: { type: "restart_active", targetRunId: pinnedSteerTargetRunId },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.succeed(releaseSteerPinned, undefined);
          yield* Fiber.join(waitUnpinnedSteerCompleted);
          const afterUnpinnedSteer = yield* orchestrator.getThreadProjection(steerThreadId);
          assert.notEqual(afterUnpinnedSteer.thread.antigravityFailoverPinned, true);
          assert.notEqual(afterUnpinnedSteer.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(
            afterUnpinnedSteer.runs[1]?.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );

          // 3. Queuing an unpinned switch behind a pinned active run clears the thread pin when
          //    the queued run starts so preflight/reactive failover applies cleanly to Run 2.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-r2-cf06-review-queue"),
            threadId: queueSwitchThreadId,
            projectId: PROJECT_ID,
            title: "Queue Unpin Switch",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: true,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-r2-cf06-review-queue-1"),
            threadId: queueSwitchThreadId,
            messageId: MessageId.make("message:r2-cf06-review-queue-1"),
            text: "QUEUE_RUN1_PINNED",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(queueRun1Started);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queue-r2-cf06-review-unpin-2"),
            threadId: queueSwitchThreadId,
            messageId: MessageId.make("message:r2-cf06-review-queue-2"),
            text: "QUEUE_RUN2_UNPINNED",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: false,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const waitQueuedRun2Completed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === queueSwitchThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseQueueRun1, undefined);
          yield* Fiber.join(waitQueuedRun2Completed);
          const afterQueueSwitch = yield* orchestrator.getThreadProjection(queueSwitchThreadId);
          assert.notEqual(afterQueueSwitch.thread.antigravityFailoverPinned, true);
          assert.notEqual(afterQueueSwitch.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(
            afterQueueSwitch.runs[1]?.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );
          const queueShell = yield* orchestrator.getThreadShell(queueSwitchThreadId);
          assert.notEqual(queueShell?.antigravityFailoverPinned, true);
        }).pipe(
          Effect.ensuring(
            Effect.all([
              Deferred.succeed(releaseParent, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseSteerRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseSteerPinned, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseQueueRun1, undefined).pipe(Effect.ignore),
            ]),
          ),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R3-CF-08: queued-message.promote-to-steer and deliverDeferredSteers preserve persisted antigravityFailoverPinned while explicit unpin still unpins",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r3-cf08-pin-preserve");
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
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
          }),
        ];

        const pathAThreadId = ThreadId.make("thread:r3-cf08-path-a");
        const pathAPlainThreadId = ThreadId.make("thread:r3-cf08-path-a-plain");
        const pathBThreadId = ThreadId.make("thread:r3-cf08-path-b");
        const unpinThreadId = ThreadId.make("thread:r3-cf08-explicit-unpin");

        const pathARun1Started = yield* Deferred.make<void>();
        const releasePathARun1 = yield* Deferred.make<void>();
        const pathAPlainRun1Started = yield* Deferred.make<void>();
        const releasePathAPlainRun1 = yield* Deferred.make<void>();
        const pathBRun1BeforeRunning = yield* Deferred.make<void>();
        const releasePathBRun1Running = yield* Deferred.make<void>();
        const pathBRun1Started = yield* Deferred.make<void>();
        const releasePathBRun1 = yield* Deferred.make<void>();
        const unpinRun1BeforeRunning = yield* Deferred.make<void>();
        const releaseUnpinRun1Running = yield* Deferred.make<void>();
        const unpinRun1Started = yield* Deferred.make<void>();
        const releaseUnpinRun1 = yield* Deferred.make<void>();

        const layer = makeIntegrationLayer({
          name: "antigravity-r3-cf08",
          cwd,
          providers,
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.message.text === "PATH_A_INITIAL_RUN") {
              return {
                behavior: "succeed",
                assistantText: "PATH_A_SENSITIVE_CONTEXT",
                pause: { onStarted: pathARun1Started, release: releasePathARun1 },
              };
            }
            if (turn.message.text === "PATH_A_PLAIN_INITIAL_RUN") {
              return {
                behavior: "succeed",
                assistantText: "PATH_A_PLAIN_SENSITIVE_CONTEXT",
                pause: { onStarted: pathAPlainRun1Started, release: releasePathAPlainRun1 },
              };
            }
            if (turn.message.text === "PATH_B_STARTING_RUN") {
              return {
                behavior: "succeed",
                assistantText: "PATH_B_SENSITIVE_CONTEXT",
                pauseBeforeRunning: {
                  onBeforeRunning: pathBRun1BeforeRunning,
                  releaseRunning: releasePathBRun1Running,
                },
                pause: { onStarted: pathBRun1Started, release: releasePathBRun1 },
              };
            }
            if (turn.message.text === "UNPIN_STARTING_RUN") {
              return {
                behavior: "succeed",
                assistantText: "UNPIN_INITIAL_CONTEXT",
                pauseBeforeRunning: {
                  onBeforeRunning: unpinRun1BeforeRunning,
                  releaseRunning: releaseUnpinRun1Running,
                },
                pause: { onStarted: unpinRun1Started, release: releaseUnpinRun1 },
              };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "FAILOVER_TO_AGY2" };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          // Path A: queued followsThreadSelection scheduled message promoted to steer after pin.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf08-a-create"),
            threadId: pathAThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-08 Path A",
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
          const waitPathARunning = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.threadId === pathAThreadId &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-a-run1"),
            threadId: pathAThreadId,
            messageId: MessageId.make("message:r3-cf08-a-1"),
            text: "PATH_A_INITIAL_RUN",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(pathARun1Started);
          yield* Fiber.join(waitPathARunning);

          // Queue a bound scheduled task (followsThreadSelection: true) before pinning.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-a-sched"),
            threadId: pathAThreadId,
            messageId: MessageId.make("message:r3-cf08-a-sched"),
            text: "PATH_A_SCHEDULED_PROMPT",
            attachments: [],
            scheduledTaskId: ScheduledTaskId.make("scheduled-task:r3-cf08-a"),
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "server",
          });
          const pathABeforePin = yield* orchestrator.getThreadProjection(pathAThreadId);
          const pathATargetRunId = pathABeforePin.runs[0]!.id;
          const pathAQueuedRunId = pathABeforePin.runs[1]!.id;
          assert.isTrue(
            pathABeforePin.messages.find(
              (msg) => msg.id === MessageId.make("message:r3-cf08-a-sched"),
            )?.followsThreadSelection,
          );

          // Pin the thread to antigravity.
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r3-cf08-a-pin"),
            threadId: pathAThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });

          const waitPathAFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === pathAThreadId &&
                event.payload.id === pathATargetRunId &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make("r3-cf08-a-promote"),
            threadId: pathAThreadId,
            queuedRunId: pathAQueuedRunId,
            targetRunId: pathATargetRunId,
          });
          yield* Deferred.succeed(releasePathARun1, undefined);
          yield* Fiber.join(waitPathAFailed);
          yield* worker.drain();

          const pathAFinal = yield* orchestrator.getThreadProjection(pathAThreadId);
          assert.equal(pathAFinal.thread.antigravityFailoverPinned, true);
          assert.equal(pathAFinal.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(
            pathAFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(pathAFinal.runs[0]?.status, "failed");
          assert.equal(pathAFinal.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === pathAThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Path A2: plain user queued message (followsThreadSelection: false, steerWhenReadyRunId: undefined)
          // promoted to steer while Run 1 is running after thread pin preserves antigravityFailoverPinned.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf08-a2-create"),
            threadId: pathAPlainThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-08 Path A2 Plain Queue",
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
          const waitPathAPlainRunning = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.threadId === pathAPlainThreadId &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-a2-run1"),
            threadId: pathAPlainThreadId,
            messageId: MessageId.make("message:r3-cf08-a2-1"),
            text: "PATH_A_PLAIN_INITIAL_RUN",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(pathAPlainRun1Started);
          yield* Fiber.join(waitPathAPlainRunning);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-a2-queue"),
            threadId: pathAPlainThreadId,
            messageId: MessageId.make("message:r3-cf08-a2-queued"),
            text: "PATH_A_PLAIN_QUEUED_PROMPT",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const pathAPlainBeforePin = yield* orchestrator.getThreadProjection(pathAPlainThreadId);
          const pathAPlainTargetRunId = pathAPlainBeforePin.runs[0]!.id;
          const pathAPlainQueuedRunId = pathAPlainBeforePin.runs[1]!.id;
          assert.isUndefined(pathAPlainBeforePin.runs[1]?.steerWhenReadyRunId);
          assert.isUndefined(pathAPlainBeforePin.runs[1]?.antigravityFailoverPinned);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r3-cf08-a2-pin"),
            threadId: pathAPlainThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });

          const waitPathAPlainFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === pathAPlainThreadId &&
                event.payload.id === pathAPlainTargetRunId &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make("r3-cf08-a2-promote"),
            threadId: pathAPlainThreadId,
            queuedRunId: pathAPlainQueuedRunId,
            targetRunId: pathAPlainTargetRunId,
          });
          yield* Deferred.succeed(releasePathAPlainRun1, undefined);
          yield* Fiber.join(waitPathAPlainFailed);
          yield* worker.drain();

          const pathAPlainFinal = yield* orchestrator.getThreadProjection(pathAPlainThreadId);
          assert.equal(pathAPlainFinal.thread.antigravityFailoverPinned, true);
          assert.equal(pathAPlainFinal.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(
            pathAPlainFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(pathAPlainFinal.runs[0]?.status, "failed");
          assert.equal(pathAPlainFinal.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === pathAPlainThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Path B: deferred steer queued before provider_turn.updated(running) + pin + deliverDeferredSteers.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf08-b-create"),
            threadId: pathBThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-08 Path B",
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
            commandId: CommandId.make("r3-cf08-b-run1"),
            threadId: pathBThreadId,
            messageId: MessageId.make("message:r3-cf08-b-1"),
            text: "PATH_B_STARTING_RUN",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(pathBRun1BeforeRunning);

          // Dispatch steer while Run 1 has no running provider turn -> queued with steerWhenReadyRunId.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-b-deferred-steer"),
            threadId: pathBThreadId,
            messageId: MessageId.make("message:r3-cf08-b-steer"),
            text: "PATH_B_DEFERRED_STEER_PROMPT",
            attachments: [],
            deliveryIntent: "steer",
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const pathBQueued = yield* orchestrator.getThreadProjection(pathBThreadId);
          assert.equal(pathBQueued.runs[1]?.status, "queued");
          assert.equal(pathBQueued.runs[1]?.steerWhenReadyRunId, pathBQueued.runs[0]!.id);
          assert.isUndefined(pathBQueued.runs[1]?.antigravityFailoverPinned);

          // Pin the thread on the same instance before provider_turn.updated(running) arrives.
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r3-cf08-b-pin"),
            threadId: pathBThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });

          const waitPathBSteerPromoted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === pathBThreadId &&
                event.payload.id === pathBQueued.runs[1]!.id &&
                event.payload.status === "cancelled",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          const waitPathBFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === pathBThreadId &&
                event.payload.id === pathBQueued.runs[0]!.id &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          // Release provider_turn.updated(running) and await deliverDeferredSteers promotion before releasing Run 1.
          yield* Deferred.succeed(releasePathBRun1Running, undefined);
          yield* Deferred.await(pathBRun1Started);
          yield* Fiber.join(waitPathBSteerPromoted);
          yield* Deferred.succeed(releasePathBRun1, undefined);
          yield* Fiber.join(waitPathBFailed);
          yield* worker.drain();

          const pathBFinal = yield* orchestrator.getThreadProjection(pathBThreadId);
          assert.equal(pathBFinal.thread.antigravityFailoverPinned, true);
          assert.equal(pathBFinal.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(
            pathBFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(pathBFinal.runs[0]?.status, "failed");
          assert.equal(pathBFinal.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === pathBThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Control: deferred steer with explicit antigravityFailoverPinned: false still unpins and fails over.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf08-unpin-create"),
            threadId: unpinThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-08 Explicit Unpin Control",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-unpin-run1"),
            threadId: unpinThreadId,
            messageId: MessageId.make("message:r3-cf08-unpin-1"),
            text: "UNPIN_STARTING_RUN",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(unpinRun1BeforeRunning);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf08-unpin-steer"),
            threadId: unpinThreadId,
            messageId: MessageId.make("message:r3-cf08-unpin-steer"),
            text: "EXPLICIT_UNPIN_DEFERRED_STEER",
            attachments: [],
            antigravityFailoverPinned: false,
            deliveryIntent: "steer",
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const unpinQueued = yield* orchestrator.getThreadProjection(unpinThreadId);
          assert.equal(unpinQueued.runs[1]?.status, "queued");
          assert.equal(unpinQueued.runs[1]?.steerWhenReadyRunId, unpinQueued.runs[0]!.id);
          assert.equal(unpinQueued.runs[1]?.antigravityFailoverPinned, false);

          const waitUnpinSteerPromoted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === unpinThreadId &&
                event.payload.id === unpinQueued.runs[1]!.id &&
                event.payload.status === "cancelled",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          const waitUnpinCompleted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === unpinThreadId &&
                event.payload.id === unpinQueued.runs[0]!.id &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseUnpinRun1Running, undefined);
          yield* Deferred.await(unpinRun1Started);
          yield* Fiber.join(waitUnpinSteerPromoted);
          yield* Deferred.succeed(releaseUnpinRun1, undefined);
          yield* Fiber.join(waitUnpinCompleted);
          yield* worker.drain();

          const unpinFinal = yield* orchestrator.getThreadProjection(unpinThreadId);
          assert.notEqual(unpinFinal.thread.antigravityFailoverPinned, true);
          assert.notEqual(unpinFinal.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(
            unpinFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );
          assert.equal(unpinFinal.runs[0]?.status, "completed");
        }).pipe(
          Effect.ensuring(
            Effect.all([
              Deferred.succeed(releasePathARun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releasePathAPlainRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releasePathBRun1Running, undefined).pipe(Effect.ignore),
              Deferred.succeed(releasePathBRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseUnpinRun1Running, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseUnpinRun1, undefined).pipe(Effect.ignore),
            ]),
          ),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R3-CF-07: post-selection pre-commit sibling cooldown keeps inspectAntigravityFailoverState pending while still allowing pre-commit thread.stop",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r3-cf07-selected-window");
        const isContextHandoffsRead = (fields: ReadonlyArray<string>) =>
          fields.length === 1 && fields[0] === "contextHandoffs";

        // Scenario 1: Sibling thread exhausts antigravity_2 strictly after candidate selection
        // and before Attempt 2 commit -> taskStatus, waitForThread, and recoverDelegatedTask
        // observe pending (no premature failure / no premature acknowledged), then Attempt 2
        // completes and wakes the parent with the authoritative result.
        const providers = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
          }),
        ];
        const parentThreadId = ThreadId.make("thread:r3-cf07-parent");
        const siblingThreadId = ThreadId.make("thread:r3-cf07-sibling");
        const stopThreadId = ThreadId.make("thread:r3-cf07-stop");

        const parentStarted = yield* Deferred.make<void>();
        const releaseParent = yield* Deferred.make<void>();
        const handoffWindowEntered = yield* Deferred.make<void>();
        const releaseHandoffWindow = yield* Deferred.make<void>();
        const siblingCooldownRecorded = yield* Deferred.make<void>();
        const stopHandoffWindowEntered = yield* Deferred.make<void>();
        const releaseStopHandoffWindow = yield* Deferred.make<void>();
        let gatedChild = false;
        let gatedStop = false;

        const layer = makeIntegrationLayer({
          name: "antigravity-r3-cf07",
          cwd,
          providers,
          recordedTurns: [],
          runContinuationWorker: true,
          outcomeFor: (instanceId, turn) => {
            if (turn.threadId === parentThreadId) {
              if (turn.runOrdinal === 1) {
                return {
                  behavior: "succeed",
                  assistantText: "Parent turn 1 done.",
                  pause: { onStarted: parentStarted, release: releaseParent },
                };
              }
              return { behavior: "succeed", assistantText: "Parent woke on Attempt 2." };
            }
            if (turn.threadId === siblingThreadId) {
              return { behavior: "usage_limit" };
            }
            if (instanceId === ProviderInstanceId.make("antigravity")) {
              return { behavior: "usage_limit" };
            }
            return {
              behavior: "succeed",
              assistantText: "AUTHORITATIVE_RESULT_FROM_AGY2",
            };
          },
          wrapFailover: (inner) => ({
            ...inner,
            recordQuotaExhausted: (input) =>
              Effect.gen(function* () {
                yield* inner.recordQuotaExhausted(input);
                if (input.instanceId === ProviderInstanceId.make("antigravity_2")) {
                  yield* Deferred.succeed(siblingCooldownRecorded, undefined).pipe(Effect.ignore);
                }
              }),
          }),
          wrapProjectionStore: (store) => ({
            ...store,
            getThreadRecords: ((
              threadId: ThreadId,
              fields: ReadonlyArray<string>,
              filter?: unknown,
            ) =>
              Effect.gen(function* () {
                if (
                  threadId !== parentThreadId &&
                  threadId !== siblingThreadId &&
                  threadId !== stopThreadId &&
                  !gatedChild &&
                  isContextHandoffsRead(fields)
                ) {
                  gatedChild = true;
                  yield* Deferred.succeed(handoffWindowEntered, undefined);
                  yield* Deferred.await(releaseHandoffWindow);
                }
                if (threadId === stopThreadId && !gatedStop && isContextHandoffsRead(fields)) {
                  gatedStop = true;
                  yield* Deferred.succeed(stopHandoffWindowEntered, undefined);
                  yield* Deferred.await(releaseStopHandoffWindow);
                }
                return yield* store.getThreadRecords(threadId, fields as never, filter as never);
              })) as typeof store.getThreadRecords,
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threadManagement = yield* ThreadManagementService.ThreadManagementService;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf07-create-parent"),
            threadId: parentThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-07 Parent",
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
            commandId: CommandId.make("r3-cf07-start-parent"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:r3-cf07-parent-1"),
            text: "Start parent for R3-CF-07.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(parentStarted);

          const scope: McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:r3-cf07"),
            requestNamespace: "provider-session:r3-cf07",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "provider-session:r3-cf07",
              providerInstanceId: ProviderInstanceId.make("antigravity"),
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };

          const task = yield* mcp.delegateTask(scope, {
            task: "Child task hitting post-selection pre-commit window.",
            mode: "async",
            clientRequestId: "r3-cf07-child",
          });
          yield* Deferred.await(handoffWindowEntered);

          // Concurrent sibling thread organically exhausts antigravity_2 while child is inside
          // the post-selection pre-commit window.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf07-create-sibling"),
            threadId: siblingThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-07 Sibling",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity_2"),
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
            commandId: CommandId.make("r3-cf07-start-sibling"),
            threadId: siblingThreadId,
            messageId: MessageId.make("message:r3-cf07-sibling-1"),
            text: "Exhaust AGY-2.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(siblingCooldownRecorded);

          // All three observers must see pending during the window (no premature failure / ack).
          const statusDuringWindow = yield* mcp.taskStatus(scope, task.taskId);
          assert.equal(statusDuringWindow.status, "running");
          assert.equal(statusDuringWindow.workState, "working");
          assert.isNull(statusDuringWindow.summary);

          const waitFiber = yield* threadManagement
            .waitForThread({
              projectId: PROJECT_ID,
              threadId: task.childThreadId!,
              timeoutMs: 25,
            })
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Effect.yieldNow;
          yield* TestClock.adjust("50 millis");
          const waitDuringWindow = yield* Fiber.join(waitFiber);
          assert.isTrue(waitDuringWindow.timedOut);

          yield* orchestrator.recoverDelegatedTask(task.childThreadId!);
          const parentMid = yield* orchestrator.getThreadProjection(parentThreadId);
          const subagentMid = parentMid.subagents.find((entry) => entry.id === task.taskId);
          assert.equal(subagentMid?.status, "running");
          assert.isNull(subagentMid?.result ?? null);
          assert.notEqual(subagentMid?.completionDelivery?.state, "acknowledged");

          // Finish parent Run 1, then release the child's handoff window so Attempt 2 commits,
          // completes on antigravity_2, and wakes the parent.
          const parentRun1Id = parentMid.runs[0]!.id;
          const waitParentRun1Done = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === parentThreadId &&
                event.payload.id === parentRun1Id &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseParent, undefined);
          yield* Fiber.join(waitParentRun1Done);

          const waitParentWake = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === parentThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          const waitSubagentDelivered = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "subagent.updated" &&
                event.threadId === parentThreadId &&
                event.payload.id === task.taskId &&
                event.payload.completionDelivery?.state === "delivered",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseHandoffWindow, undefined);
          yield* Fiber.join(waitParentWake);
          yield* Fiber.join(waitSubagentDelivered);
          yield* worker.drain();

          const parentFinal = yield* orchestrator.getThreadProjection(parentThreadId);
          assert.isAtLeast(parentFinal.runs.length, 2);
          const subagentFinal = parentFinal.subagents.find((entry) => entry.id === task.taskId);
          assert.equal(subagentFinal?.status, "completed");
          assert.include(subagentFinal?.result ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
          assert.equal(subagentFinal?.completionDelivery?.state, "delivered");

          // Control: pre-commit thread.stop during selectedAntigravityFailoverAttempts still aborts failover.
          // Advance clock past the 24h max cooldown (FUTURE_RESET) so antigravity and antigravity_2 are selectable again.
          yield* TestClock.adjust("25 hours");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r3-cf07-create-stop"),
            threadId: stopThreadId,
            projectId: PROJECT_ID,
            title: "R3-CF-07 Stop Control",
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
            commandId: CommandId.make("r3-cf07-start-stop"),
            threadId: stopThreadId,
            messageId: MessageId.make("message:r3-cf07-stop-1"),
            text: "Trigger failover then stop pre-commit.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(stopHandoffWindowEntered);

          const stopFiber = yield* orchestrator
            .dispatch({
              type: "thread.stop",
              commandId: CommandId.make("r3-cf07-stop-cmd"),
              threadId: stopThreadId,
              reason: "User stopped during post-selection window",
            })
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseStopHandoffWindow, undefined);
          yield* Fiber.join(stopFiber);
          yield* worker.drain();

          const stopFinal = yield* orchestrator.getThreadProjection(stopThreadId);
          assert.equal(stopFinal.runs[0]?.status, "failed");
          assert.equal(stopFinal.attempts.length, 1);
          assert.equal(stopFinal.thread.providerInstanceId, ProviderInstanceId.make("antigravity"));
        }).pipe(
          Effect.ensuring(
            Effect.all([
              Deferred.succeed(releaseParent, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseHandoffWindow, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseStopHandoffWindow, undefined).pipe(Effect.ignore),
            ]),
          ),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R3-CF-17: RestartContinuation preserves antigravityFailoverPinned on same-instance different-model pins (pre-restart in-flight, post-restart, defense-in-depth, and unpinned control)",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r3-cf17-restart-pin");
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
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
        ];

        const postRestartThreadId = ThreadId.make("thread:r3-cf17-post-restart");
        const inflightPinThreadId = ThreadId.make("thread:r3-cf17-inflight-pin");
        const unpinnedControlThreadId = ThreadId.make("thread:r3-cf17-unpinned-control");
        const defenseThreadId = ThreadId.make("thread:r3-cf17-defense-in-depth");

        const startedByThread = new Map<ThreadId, Deferred.Deferred<void>>([
          [postRestartThreadId, yield* Deferred.make<void>()],
          [inflightPinThreadId, yield* Deferred.make<void>()],
          [unpinnedControlThreadId, yield* Deferred.make<void>()],
          [defenseThreadId, yield* Deferred.make<void>()],
        ]);
        const releaseByThread = new Map<ThreadId, Deferred.Deferred<void>>([
          [postRestartThreadId, yield* Deferred.make<void>()],
          [inflightPinThreadId, yield* Deferred.make<void>()],
          [unpinnedControlThreadId, yield* Deferred.make<void>()],
          [defenseThreadId, yield* Deferred.make<void>()],
        ]);

        const layer = makeIntegrationLayer({
          name: "antigravity-r3-cf17",
          cwd,
          providers,
          recordedTurns,
          settings: { continueThreadsAfterServerUpdate: true },
          outcomeFor: (instanceId, turn) => {
            if (turn.runOrdinal === 1) {
              return {
                behavior: "succeed",
                assistantText: `Run 1 context for ${turn.threadId}`,
                pause: {
                  onStarted: startedByThread.get(turn.threadId)!,
                  release: releaseByThread.get(turn.threadId)!,
                },
              };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "FAILOVER_TO_AGY2" };
          },
        });

        const settingsLayer = ServerSettingsService.layerTest(
          settingsForProviders(providers, { continueThreadsAfterServerUpdate: true }),
        );

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const eventSink = yield* EventSink.EventSinkV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          const startAndCancelForRestart = (input: {
            readonly threadId: ThreadId;
            readonly initialPinned?: boolean;
            readonly pinInFlightBeforeCancel?: boolean;
          }) =>
            Effect.gen(function* () {
              yield* orchestrator.dispatch({
                type: "thread.create",
                commandId: CommandId.make(`create:${input.threadId}`),
                threadId: input.threadId,
                projectId: PROJECT_ID,
                title: String(input.threadId),
                modelSelection: {
                  instanceId: ProviderInstanceId.make("antigravity"),
                  model: "gemini-4-neon",
                },
                ...(input.initialPinned === true ? { antigravityFailoverPinned: true } : {}),
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: null,
                worktreePath: cwd,
                createdBy: "user",
                creationSource: "web",
              });
              const waitRunning = yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "provider-turn.updated" &&
                    event.threadId === input.threadId &&
                    event.payload.status === "running",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped({ startImmediately: true }),
              );
              yield* Effect.yieldNow;
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(`run1:${input.threadId}`),
                threadId: input.threadId,
                messageId: MessageId.make(`message:${input.threadId}:1`),
                text: `SENSITIVE_PRE_RESTART_${input.threadId}`,
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              yield* Deferred.await(startedByThread.get(input.threadId)!);
              yield* Fiber.join(waitRunning);

              if (input.pinInFlightBeforeCancel === true) {
                yield* orchestrator.dispatch({
                  type: "thread.model-selection.set",
                  commandId: CommandId.make(`inflight-pin:${input.threadId}`),
                  threadId: input.threadId,
                  modelSelection: {
                    instanceId: ProviderInstanceId.make("antigravity"),
                    model: "claude-opus-5-5",
                  },
                  antigravityFailoverPinned: true,
                });
              }

              const liveProjection = yield* orchestrator.getThreadProjection(input.threadId);
              const now = yield* DateTime.now;
              assert.equal(
                RestartContinuation.restartContinuationRun(liveProjection, now)?.id,
                liveProjection.runs[0]!.id,
              );

              // Simulate server-restart cancellation of the active run.
              const run1 = liveProjection.runs[0]!;
              const attempt1 = liveProjection.attempts.find(
                (entry) => entry.id === run1.activeAttemptId,
              )!;
              const node1 = liveProjection.nodes.find((entry) => entry.id === run1.rootNodeId)!;
              const turn1 = liveProjection.providerTurns.find(
                (entry) => entry.runAttemptId === run1.activeAttemptId,
              )!;
              yield* eventSink.write({
                events: [
                  {
                    id: EventId.make(`event:runtime-reconcile:${input.threadId}:turn`),
                    type: "provider-turn.updated",
                    threadId: input.threadId,
                    runId: run1.id,
                    nodeId: node1.id,
                    providerInstanceId: run1.providerInstanceId,
                    occurredAt: now,
                    payload: { ...turn1, status: "cancelled", completedAt: now },
                  },
                  {
                    id: EventId.make(`event:runtime-reconcile:${input.threadId}:node`),
                    type: "node.updated",
                    threadId: input.threadId,
                    runId: run1.id,
                    nodeId: node1.id,
                    providerInstanceId: run1.providerInstanceId,
                    occurredAt: now,
                    payload: { ...node1, status: "cancelled", completedAt: now },
                  },
                  {
                    id: EventId.make(`event:runtime-reconcile:${input.threadId}:attempt`),
                    type: "run-attempt.updated",
                    threadId: input.threadId,
                    runId: run1.id,
                    nodeId: node1.id,
                    providerInstanceId: run1.providerInstanceId,
                    occurredAt: now,
                    payload: { ...attempt1, status: "cancelled", completedAt: now },
                  },
                  {
                    id: EventId.make(`event:runtime-reconcile:${input.threadId}:run`),
                    type: "run.updated",
                    threadId: input.threadId,
                    runId: run1.id,
                    nodeId: node1.id,
                    providerInstanceId: run1.providerInstanceId,
                    occurredAt: now,
                    payload: { ...run1, status: "cancelled", completedAt: now },
                  },
                ],
              });
              return run1.id;
            });

          // Scenario A: post-restart same-instance different-model pin.
          const postRestartRun1Id = yield* startAndCancelForRestart({
            threadId: postRestartThreadId,
          });
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r3-cf17-post-pin"),
            threadId: postRestartThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: true,
          });
          const waitPostRestartFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === postRestartThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* RestartContinuation.continueRestartedRun({
            threadId: postRestartThreadId,
            sourceRunId: postRestartRun1Id,
          }).pipe(Effect.provide(settingsLayer));
          yield* Fiber.join(waitPostRestartFailed);
          yield* worker.drain();

          const postRestartFinal = yield* orchestrator.getThreadProjection(postRestartThreadId);
          assert.equal(postRestartFinal.thread.antigravityFailoverPinned, true);
          assert.equal(postRestartFinal.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(
            postRestartFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(postRestartFinal.runs[1]?.status, "failed");
          assert.equal(postRestartFinal.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === postRestartThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Scenario B: pre-restart in-flight same-instance different-model pin.
          const inflightRun1Id = yield* startAndCancelForRestart({
            threadId: inflightPinThreadId,
            pinInFlightBeforeCancel: true,
          });
          const waitInflightFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === inflightPinThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* RestartContinuation.continueRestartedRun({
            threadId: inflightPinThreadId,
            sourceRunId: inflightRun1Id,
          }).pipe(Effect.provide(settingsLayer));
          yield* Fiber.join(waitInflightFailed);
          yield* worker.drain();

          const inflightFinal = yield* orchestrator.getThreadProjection(inflightPinThreadId);
          assert.equal(inflightFinal.thread.antigravityFailoverPinned, true);
          assert.equal(inflightFinal.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(
            inflightFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(inflightFinal.runs[1]?.status, "failed");
          assert.equal(inflightFinal.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === inflightPinThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Scenario C (defense-in-depth): direct message.dispatch with restartContinuationOfRunId
          // omitting antigravityFailoverPinned still preserves same-instance pin in Orchestrator.
          const defenseRun1Id = yield* startAndCancelForRestart({
            threadId: defenseThreadId,
            pinInFlightBeforeCancel: true,
          });
          const waitDefenseFailed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === defenseThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r3-cf17-defense-dispatch"),
            threadId: defenseThreadId,
            messageId: MessageId.make("message:r3-cf17-defense:2"),
            text: "Continue where you left off.",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            dispatchMode: { type: "start_immediately" },
            createdBy: "agent",
            creationSource: "server",
            restartContinuationOfRunId: defenseRun1Id,
          });
          yield* Fiber.join(waitDefenseFailed);
          yield* worker.drain();

          const defenseFinal = yield* orchestrator.getThreadProjection(defenseThreadId);
          assert.equal(defenseFinal.thread.antigravityFailoverPinned, true);
          assert.equal(defenseFinal.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(
            defenseFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity"),
          );
          assert.equal(defenseFinal.runs[1]?.status, "failed");
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === defenseThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Scenario D (control): explicitly unpinned thread after restart stays unpinned and fails over.
          const unpinnedRun1Id = yield* startAndCancelForRestart({
            threadId: unpinnedControlThreadId,
            initialPinned: true,
          });
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r3-cf17-unpin"),
            threadId: unpinnedControlThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
          });
          const waitUnpinnedCompleted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === unpinnedControlThreadId &&
                event.payload.ordinal === 2 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* RestartContinuation.continueRestartedRun({
            threadId: unpinnedControlThreadId,
            sourceRunId: unpinnedRun1Id,
          }).pipe(Effect.provide(settingsLayer));
          yield* Fiber.join(waitUnpinnedCompleted);
          yield* worker.drain();

          const unpinnedFinal = yield* orchestrator.getThreadProjection(unpinnedControlThreadId);
          assert.notEqual(unpinnedFinal.thread.antigravityFailoverPinned, true);
          assert.notEqual(unpinnedFinal.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(
            unpinnedFinal.thread.providerInstanceId,
            ProviderInstanceId.make("antigravity_2"),
          );
          assert.equal(unpinnedFinal.runs[1]?.status, "completed");
        }).pipe(
          Effect.ensuring(
            Effect.forEach([...releaseByThread.values()], (deferred) =>
              Deferred.succeed(deferred, undefined).pipe(Effect.ignore),
            ),
          ),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R4-CF-06: unpinned queued messages and deferred steers with same-instance model or option changes preserve later thread pins across start, preflight, deferred steer, and promote-to-steer",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r4-cf06-unpinned-queue-pin");
        const recordedTurns: Array<{
          readonly instanceId: ProviderInstanceId;
          readonly threadId: ThreadId;
          readonly text: string;
          readonly model: string;
        }> = [];
        const makeDefaultProviders = () => [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
        ];
        const initialProviders = makeDefaultProviders();
        const providersRef = yield* Ref.make<ReadonlyArray<ServerProvider>>(initialProviders);

        const runtimeQueueThreadId = ThreadId.make("thread:r4-cf06-runtime-queue");
        const preflightQueueThreadId = ThreadId.make("thread:r4-cf06-preflight-queue");
        const deferredSteerThreadId = ThreadId.make("thread:r4-cf06-deferred-steer");
        const manualPromoteThreadId = ThreadId.make("thread:r4-cf06-manual-promote");
        const explicitUnpinThreadId = ThreadId.make("thread:r4-cf06-explicit-unpin");
        const crossInstanceThreadId = ThreadId.make("thread:r4-cf06-cross-instance");

        const runtimeRun1Started = yield* Deferred.make<void>();
        const releaseRuntimeRun1 = yield* Deferred.make<void>();
        const preflightRun1Started = yield* Deferred.make<void>();
        const releasePreflightRun1 = yield* Deferred.make<void>();
        const deferredBeforeRunning = yield* Deferred.make<void>();
        const releaseDeferredRunning = yield* Deferred.make<void>();
        const deferredRun1Started = yield* Deferred.make<void>();
        const releaseDeferredRun1 = yield* Deferred.make<void>();
        const manualRun1Started = yield* Deferred.make<void>();
        const releaseManualRun1 = yield* Deferred.make<void>();
        const unpinRun1Started = yield* Deferred.make<void>();
        const releaseUnpinRun1 = yield* Deferred.make<void>();
        const crossRun1Started = yield* Deferred.make<void>();
        const releaseCrossRun1 = yield* Deferred.make<void>();

        const layer = makeIntegrationLayer({
          name: "antigravity-r4-cf06",
          cwd,
          providers: initialProviders,
          providersRef,
          recordedTurns,
          outcomeFor: (instanceId, turn) => {
            if (turn.message.text === "R4_RUNTIME_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "R4_RUNTIME_SENSITIVE_CONTEXT",
                pause: { onStarted: runtimeRun1Started, release: releaseRuntimeRun1 },
              };
            }
            if (turn.message.text === "R4_PREFLIGHT_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "R4_PREFLIGHT_SENSITIVE_CONTEXT",
                pause: { onStarted: preflightRun1Started, release: releasePreflightRun1 },
              };
            }
            if (turn.message.text === "R4_PREFLIGHT_QUEUED_CLAUDE") {
              return {
                behavior: "succeed",
                assistantText: `PREFLIGHT_RAN_ON_${instanceId}`,
              };
            }
            if (turn.message.text === "R4_DEFERRED_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "R4_DEFERRED_SENSITIVE_CONTEXT",
                pauseBeforeRunning: {
                  onBeforeRunning: deferredBeforeRunning,
                  releaseRunning: releaseDeferredRunning,
                },
                pause: { onStarted: deferredRun1Started, release: releaseDeferredRun1 },
              };
            }
            if (turn.message.text === "R4_MANUAL_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "R4_MANUAL_SENSITIVE_CONTEXT",
                pause: { onStarted: manualRun1Started, release: releaseManualRun1 },
              };
            }
            if (turn.message.text === "R4_UNPIN_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "R4_UNPIN_INITIAL_CONTEXT",
                pause: { onStarted: unpinRun1Started, release: releaseUnpinRun1 },
              };
            }
            if (turn.message.text === "R4_CROSS_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "R4_CROSS_INITIAL_CONTEXT",
                pause: { onStarted: crossRun1Started, release: releaseCrossRun1 },
              };
            }
            if (turn.message.text === "R4_CROSS_QUEUED_AGY2") {
              return {
                behavior: "succeed",
                assistantText: `CROSS_RAN_ON_${instanceId}`,
              };
            }
            return instanceId === ProviderInstanceId.make("antigravity")
              ? { behavior: "usage_limit" }
              : { behavior: "succeed", assistantText: "FAILOVER_TO_AGY2" };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

          // Scenario 1: queue_after_active with different same-instance model on unpinned thread
          // -> pin thread -> Run 1 ends -> Run 2 hits usage_limit (runtime failover blocked, pin preserved).
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r4-cf06-s1-create"),
            threadId: runtimeQueueThreadId,
            projectId: PROJECT_ID,
            title: "R4-CF-06 Runtime Queue",
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
            commandId: CommandId.make("r4-cf06-s1-run1"),
            threadId: runtimeQueueThreadId,
            messageId: MessageId.make("message:r4-cf06-s1-1"),
            text: "R4_RUNTIME_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(runtimeRun1Started);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s1-queue"),
            threadId: runtimeQueueThreadId,
            messageId: MessageId.make("message:r4-cf06-s1-2"),
            text: "R4_RUNTIME_QUEUED_CLAUDE",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const s1Queued = yield* orchestrator.getThreadProjection(runtimeQueueThreadId);
          assert.isUndefined(s1Queued.runs[1]?.antigravityFailoverPinned);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r4-cf06-s1-pin"),
            threadId: runtimeQueueThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          const waitS1Failed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === runtimeQueueThreadId &&
                event.payload.id === s1Queued.runs[1]!.id &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseRuntimeRun1, undefined);
          yield* Fiber.join(waitS1Failed);
          yield* worker.drain();

          const s1Final = yield* orchestrator.getThreadProjection(runtimeQueueThreadId);
          assert.equal(s1Final.thread.antigravityFailoverPinned, true);
          assert.equal(s1Final.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(s1Final.thread.providerInstanceId, ProviderInstanceId.make("antigravity"));
          assert.equal(s1Final.thread.modelSelection.model, "claude-opus-5-5");
          assert.equal(s1Final.runs[1]?.status, "failed");
          assert.equal(s1Final.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === runtimeQueueThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          // Expire runtime cooldown from Scenario 1 before Scenario 2.
          yield* TestClock.adjust("25 hours");

          // Scenario 2: same flow with 100% quota on antigravity (5% on antigravity_2) ->
          // immediate preflight failover blocked and pin preserved.
          yield* Ref.set(providersRef, [
            makeAntigravityProvider("antigravity", {
              displayName: "AGY-1",
              usageLimits: makeLimits([
                { id: "gemini-5h", usedPercent: 10 },
                { id: "3p-5h", usedPercent: 100 },
              ]),
            }),
            makeAntigravityProvider("antigravity_2", {
              displayName: "AGY-2",
              usageLimits: makeLimits([
                { id: "gemini-5h", usedPercent: 10 },
                { id: "3p-5h", usedPercent: 5 },
              ]),
            }),
          ]);
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r4-cf06-s2-create"),
            threadId: preflightQueueThreadId,
            projectId: PROJECT_ID,
            title: "R4-CF-06 Preflight Queue",
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
            commandId: CommandId.make("r4-cf06-s2-run1"),
            threadId: preflightQueueThreadId,
            messageId: MessageId.make("message:r4-cf06-s2-1"),
            text: "R4_PREFLIGHT_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(preflightRun1Started);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s2-queue"),
            threadId: preflightQueueThreadId,
            messageId: MessageId.make("message:r4-cf06-s2-2"),
            text: "R4_PREFLIGHT_QUEUED_CLAUDE",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const s2Queued = yield* orchestrator.getThreadProjection(preflightQueueThreadId);
          assert.isUndefined(s2Queued.runs[1]?.antigravityFailoverPinned);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r4-cf06-s2-pin"),
            threadId: preflightQueueThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          const waitS2Done = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === preflightQueueThreadId &&
                event.payload.id === s2Queued.runs[1]!.id &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releasePreflightRun1, undefined);
          yield* Fiber.join(waitS2Done);
          yield* worker.drain();

          const s2Final = yield* orchestrator.getThreadProjection(preflightQueueThreadId);
          assert.equal(s2Final.thread.antigravityFailoverPinned, true);
          assert.equal(s2Final.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(s2Final.thread.providerInstanceId, ProviderInstanceId.make("antigravity"));
          assert.equal(s2Final.thread.modelSelection.model, "claude-opus-5-5");
          assert.equal(s2Final.runs[1]?.providerInstanceId, ProviderInstanceId.make("antigravity"));
          assert.equal(s2Final.runs[1]?.status, "completed");
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === preflightQueueThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          yield* Ref.set(providersRef, makeDefaultProviders());

          // Scenario 3: deferred steer while Run 1 is starting with option-only change
          // (thought_level: "xhigh") on unpinned thread -> pin thread -> deliverDeferredSteers
          // promotes steer -> pin preserved on usage_limit.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r4-cf06-s3-create"),
            threadId: deferredSteerThreadId,
            projectId: PROJECT_ID,
            title: "R4-CF-06 Deferred Steer Option Change",
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
            commandId: CommandId.make("r4-cf06-s3-run1"),
            threadId: deferredSteerThreadId,
            messageId: MessageId.make("message:r4-cf06-s3-1"),
            text: "R4_DEFERRED_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(deferredBeforeRunning);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s3-steer"),
            threadId: deferredSteerThreadId,
            messageId: MessageId.make("message:r4-cf06-s3-steer"),
            text: "R4_DEFERRED_STEER_XHIGH",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
              options: [{ id: "thought_level", value: "xhigh" }],
            },
            deliveryIntent: "steer",
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const s3Queued = yield* orchestrator.getThreadProjection(deferredSteerThreadId);
          assert.equal(s3Queued.runs[1]?.status, "queued");
          assert.equal(s3Queued.runs[1]?.steerWhenReadyRunId, s3Queued.runs[0]!.id);
          assert.isUndefined(s3Queued.runs[1]?.antigravityFailoverPinned);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r4-cf06-s3-pin"),
            threadId: deferredSteerThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          const waitS3Promoted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === deferredSteerThreadId &&
                event.payload.id === s3Queued.runs[1]!.id &&
                event.payload.status === "cancelled",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          const waitS3Failed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === deferredSteerThreadId &&
                event.payload.id === s3Queued.runs[0]!.id &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseDeferredRunning, undefined);
          yield* Deferred.await(deferredRun1Started);
          yield* Fiber.join(waitS3Promoted);
          yield* Deferred.succeed(releaseDeferredRun1, undefined);
          yield* Fiber.join(waitS3Failed);
          yield* worker.drain();

          const s3Final = yield* orchestrator.getThreadProjection(deferredSteerThreadId);
          assert.equal(s3Final.thread.antigravityFailoverPinned, true);
          assert.equal(s3Final.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(s3Final.thread.providerInstanceId, ProviderInstanceId.make("antigravity"));
          assert.deepEqual(s3Final.thread.modelSelection.options, [
            { id: "thought_level", value: "xhigh" },
          ]);
          assert.equal(s3Final.runs[0]?.status, "failed");
          assert.equal(s3Final.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === deferredSteerThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          yield* TestClock.adjust("25 hours");

          // Scenario 4: manual queued-message.promote-to-steer after pinning with different
          // same-instance model preserves pin.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r4-cf06-s4-create"),
            threadId: manualPromoteThreadId,
            projectId: PROJECT_ID,
            title: "R4-CF-06 Manual Promote",
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
          const waitS4Running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.threadId === manualPromoteThreadId &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s4-run1"),
            threadId: manualPromoteThreadId,
            messageId: MessageId.make("message:r4-cf06-s4-1"),
            text: "R4_MANUAL_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(manualRun1Started);
          yield* Fiber.join(waitS4Running);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s4-queue"),
            threadId: manualPromoteThreadId,
            messageId: MessageId.make("message:r4-cf06-s4-2"),
            text: "R4_MANUAL_PROMOTE_CLAUDE",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const s4Queued = yield* orchestrator.getThreadProjection(manualPromoteThreadId);
          assert.isUndefined(s4Queued.runs[1]?.antigravityFailoverPinned);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r4-cf06-s4-pin"),
            threadId: manualPromoteThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          const waitS4Failed = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === manualPromoteThreadId &&
                event.payload.id === s4Queued.runs[0]!.id &&
                event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* orchestrator.dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make("r4-cf06-s4-promote"),
            threadId: manualPromoteThreadId,
            queuedRunId: s4Queued.runs[1]!.id,
            targetRunId: s4Queued.runs[0]!.id,
          });
          yield* Deferred.succeed(releaseManualRun1, undefined);
          yield* Fiber.join(waitS4Failed);
          yield* worker.drain();

          const s4Final = yield* orchestrator.getThreadProjection(manualPromoteThreadId);
          assert.equal(s4Final.thread.antigravityFailoverPinned, true);
          assert.equal(s4Final.runs[0]?.antigravityFailoverPinned, true);
          assert.equal(s4Final.thread.providerInstanceId, ProviderInstanceId.make("antigravity"));
          assert.equal(s4Final.thread.modelSelection.model, "claude-opus-5-5");
          assert.equal(s4Final.runs[0]?.status, "failed");
          assert.equal(s4Final.contextHandoffs.length, 0);
          assert.isEmpty(
            recordedTurns.filter(
              (turn) =>
                turn.threadId === manualPromoteThreadId &&
                turn.instanceId === ProviderInstanceId.make("antigravity_2"),
            ),
          );

          yield* TestClock.adjust("25 hours");

          // Scenario 5 (control): explicit antigravityFailoverPinned: false on queued message
          // still explicitly unpins even when queued on an unpinned thread that is later pinned.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r4-cf06-s5-create"),
            threadId: explicitUnpinThreadId,
            projectId: PROJECT_ID,
            title: "R4-CF-06 Explicit Unpin Control",
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
            commandId: CommandId.make("r4-cf06-s5-run1"),
            threadId: explicitUnpinThreadId,
            messageId: MessageId.make("message:r4-cf06-s5-1"),
            text: "R4_UNPIN_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(unpinRun1Started);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s5-queue"),
            threadId: explicitUnpinThreadId,
            messageId: MessageId.make("message:r4-cf06-s5-2"),
            text: "R4_EXPLICIT_UNPIN_QUEUED",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            antigravityFailoverPinned: false,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const s5Queued = yield* orchestrator.getThreadProjection(explicitUnpinThreadId);
          assert.equal(s5Queued.runs[1]?.antigravityFailoverPinned, false);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r4-cf06-s5-pin"),
            threadId: explicitUnpinThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          const waitS5Done = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === explicitUnpinThreadId &&
                event.payload.id === s5Queued.runs[1]!.id &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseUnpinRun1, undefined);
          yield* Fiber.join(waitS5Done);
          yield* worker.drain();

          const s5Final = yield* orchestrator.getThreadProjection(explicitUnpinThreadId);
          assert.notEqual(s5Final.thread.antigravityFailoverPinned, true);
          assert.notEqual(s5Final.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(s5Final.thread.providerInstanceId, ProviderInstanceId.make("antigravity_2"));
          assert.equal(s5Final.runs[1]?.status, "completed");

          yield* TestClock.adjust("25 hours");

          // Scenario 6 (control): cross-instance queued run on unpinned thread does not leak
          // a later pin across Antigravity instances.
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("r4-cf06-s6-create"),
            threadId: crossInstanceThreadId,
            projectId: PROJECT_ID,
            title: "R4-CF-06 Cross-Instance Control",
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
            commandId: CommandId.make("r4-cf06-s6-run1"),
            threadId: crossInstanceThreadId,
            messageId: MessageId.make("message:r4-cf06-s6-1"),
            text: "R4_CROSS_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* Deferred.await(crossRun1Started);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-cf06-s6-queue"),
            threadId: crossInstanceThreadId,
            messageId: MessageId.make("message:r4-cf06-s6-2"),
            text: "R4_CROSS_QUEUED_AGY2",
            attachments: [],
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity_2"),
              model: "gemini-4-neon",
            },
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const s6Queued = yield* orchestrator.getThreadProjection(crossInstanceThreadId);
          assert.isUndefined(s6Queued.runs[1]?.antigravityFailoverPinned);

          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("r4-cf06-s6-pin"),
            threadId: crossInstanceThreadId,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          const waitS6Done = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.threadId === crossInstanceThreadId &&
                event.payload.id === s6Queued.runs[1]!.id &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          yield* Deferred.succeed(releaseCrossRun1, undefined);
          yield* Fiber.join(waitS6Done);
          yield* worker.drain();

          const s6Final = yield* orchestrator.getThreadProjection(crossInstanceThreadId);
          assert.equal(s6Final.thread.providerInstanceId, ProviderInstanceId.make("antigravity_2"));
          assert.notEqual(s6Final.thread.antigravityFailoverPinned, true);
          assert.notEqual(s6Final.runs[1]?.antigravityFailoverPinned, true);
          assert.equal(s6Final.runs[1]?.status, "completed");
        }).pipe(
          Effect.ensuring(
            Effect.all([
              Deferred.succeed(releaseRuntimeRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releasePreflightRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseDeferredRunning, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseDeferredRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseManualRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseUnpinRun1, undefined).pipe(Effect.ignore),
              Deferred.succeed(releaseCrossRun1, undefined).pipe(Effect.ignore),
            ]),
          ),
          Effect.provide(layer),
        );
      }),
  );

  it.effect(
    "R4-CF-07: stale pre-commit WAL read overlap in inspectAntigravityFailoverState does not prematurely fail or acknowledge mid-Attempt-2 while exhausted failover still settles cleanly",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r4-cf07-stale-wal-toctou");
        const isInspectFailoverRead = (fields: ReadonlyArray<string>) =>
          fields.includes("runs") &&
          fields.includes("attempts") &&
          fields.includes("providerSessions") &&
          fields.includes("providerThreads") &&
          fields.includes("runtimeRequests") &&
          fields.includes("turnItems");
        const isRunsRecheckRead = (fields: ReadonlyArray<string>) =>
          fields.length === 1 && fields[0] === "runs";

        const runStaleSnapshotScenario = <E, R>(
          name: string,
          options: { readonly exhaustAllAccounts?: boolean },
          scenario: (ctx: {
            readonly orchestrator: Orchestrator.OrchestratorV2["Service"];
            readonly mcp: OrchestratorMcpService.OrchestratorMcpService["Service"];
            readonly worker: EffectWorker.OrchestrationEffectWorkerV2["Service"];
            readonly parentThreadId: ThreadId;
            readonly scope: McpInvocationScope;
            readonly releaseParent: Deferred.Deferred<void>;
            readonly failoverEntered: Deferred.Deferred<void>;
            readonly releaseFailover: Deferred.Deferred<void>;
            readonly attempt2Started: Deferred.Deferred<void>;
            readonly releaseAttempt2: Deferred.Deferred<void>;
            readonly arm: (childThreadId: ThreadId) => void;
            readonly wasTriggered: () => boolean;
            readonly recheckCount: () => number;
          }) => Effect.Effect<void, E, R>,
        ) =>
          Effect.gen(function* () {
            const providers = [
              makeAntigravityProvider("antigravity", {
                displayName: "AGY-1",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
              }),
              makeAntigravityProvider("antigravity_2", {
                displayName: "AGY-2",
                usageLimits: makeLimits([{ id: "gemini-5h", usedPercent: 10 }]),
              }),
            ];
            const parentThreadId = ThreadId.make(`thread:${name}-parent`);
            const parentStarted = yield* Deferred.make<void>();
            const releaseParent = yield* Deferred.make<void>();
            const failoverEntered = yield* Deferred.make<void>();
            const releaseFailover = yield* Deferred.make<void>();
            const attempt2Started = yield* Deferred.make<void>();
            const releaseAttempt2 = yield* Deferred.make<void>();
            let failoverGated = false;
            let armedChildThreadId: ThreadId | null = null;
            let triggered = false;
            let runsRecheckCount = 0;

            const layer = makeIntegrationLayer({
              name,
              cwd,
              providers,
              recordedTurns: [],
              runContinuationWorker: true,
              outcomeFor: (instanceId, turn) => {
                if (turn.threadId === parentThreadId) {
                  if (turn.runOrdinal === 1) {
                    return {
                      behavior: "succeed",
                      assistantText: "Parent turn 1 done.",
                      pause: { onStarted: parentStarted, release: releaseParent },
                    };
                  }
                  return { behavior: "succeed", assistantText: "Parent woke after child settled." };
                }
                if (options.exhaustAllAccounts === true) {
                  return { behavior: "usage_limit" };
                }
                if (instanceId === ProviderInstanceId.make("antigravity")) {
                  return { behavior: "usage_limit" };
                }
                return {
                  behavior: "succeed",
                  assistantText: "AUTHORITATIVE_RESULT_FROM_AGY2",
                  pause: { onStarted: attempt2Started, release: releaseAttempt2 },
                };
              },
              wrapFailover: (inner) => ({
                ...inner,
                recordQuotaExhausted: (input) =>
                  Effect.gen(function* () {
                    if (
                      options.exhaustAllAccounts !== true &&
                      !failoverGated &&
                      input.instanceId === ProviderInstanceId.make("antigravity")
                    ) {
                      failoverGated = true;
                      yield* Deferred.succeed(failoverEntered, undefined);
                      yield* Deferred.await(releaseFailover);
                    }
                    yield* inner.recordQuotaExhausted(input);
                  }),
              }),
              wrapProjectionStore: (store) => ({
                ...store,
                getThreadRecords: ((
                  threadId: ThreadId,
                  fields: ReadonlyArray<string>,
                  filter?: unknown,
                ) =>
                  Effect.gen(function* () {
                    const matchesTarget =
                      armedChildThreadId !== null && threadId === armedChildThreadId;
                    if (matchesTarget && triggered && isRunsRecheckRead(fields)) {
                      runsRecheckCount += 1;
                    }
                    if (matchesTarget && !triggered && isInspectFailoverRead(fields)) {
                      triggered = true;
                      yield* Deferred.await(failoverEntered);
                      // Capture the pre-commit Attempt 1 snapshot (simulating an unlocked WAL read)
                      // BEFORE Attempt 2 commits and clears all in-memory tracking sets.
                      const stalePreCommitSnapshot = yield* store.getThreadRecords(
                        threadId,
                        fields as never,
                        filter as never,
                      );
                      yield* Deferred.succeed(releaseFailover, undefined);
                      yield* Deferred.await(attempt2Started);
                      yield* Effect.yieldNow;
                      yield* Effect.yieldNow;
                      return stalePreCommitSnapshot;
                    }
                    return yield* store.getThreadRecords(
                      threadId,
                      fields as never,
                      filter as never,
                    );
                  })) as typeof store.getThreadRecords,
              }),
            });

            yield* Effect.gen(function* () {
              const orchestrator = yield* Orchestrator.OrchestratorV2;
              const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
              const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;

              yield* orchestrator.dispatch({
                type: "thread.create",
                commandId: CommandId.make(`create-${name}`),
                threadId: parentThreadId,
                projectId: PROJECT_ID,
                title: name,
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
                commandId: CommandId.make(`start-${name}`),
                threadId: parentThreadId,
                messageId: MessageId.make(`message:${name}`),
                text: "Run R4-CF-07 stale WAL TOCTOU check.",
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              yield* Deferred.await(parentStarted);

              const scope: McpInvocationScope = {
                environmentId: EnvironmentId.make(`environment:${name}`),
                requestNamespace: `provider-session:${name}`,
                thread: {
                  threadId: parentThreadId,
                  providerSessionId: `provider-session:${name}`,
                  providerInstanceId: ProviderInstanceId.make("antigravity"),
                },
                client: undefined,
                capabilities: new Set(["orchestration"]),
                issuedAt: 1,
              };

              yield* scenario({
                orchestrator,
                mcp,
                worker,
                parentThreadId,
                scope,
                releaseParent,
                failoverEntered,
                releaseFailover,
                attempt2Started,
                releaseAttempt2,
                arm: (childThreadId) => {
                  armedChildThreadId = childThreadId;
                },
                wasTriggered: () => triggered,
                recheckCount: () => runsRecheckCount,
              });
            }).pipe(
              Effect.ensuring(
                Effect.all([
                  Deferred.succeed(releaseParent, undefined).pipe(Effect.ignore),
                  Deferred.succeed(releaseFailover, undefined).pipe(Effect.ignore),
                  Deferred.succeed(releaseAttempt2, undefined).pipe(Effect.ignore),
                ]),
              ),
              Effect.provide(layer),
            );
          });

        // Path 1: recoverDelegatedTask / finalizeAppOwnedSubagentTask with stale pre-commit
        // snapshot overlapping Attempt 2 writeSystemEvents + Effect.ensuring + releasePendingOnce
        // (zero sibling threads) -> Attempt 1 is NOT finalized as failed, Attempt 2 delivers.
        yield* runStaleSnapshotScenario("r4-cf07-path1-recover", {}, (ctx) =>
          Effect.gen(function* () {
            const task = yield* ctx.mcp.delegateTask(ctx.scope, {
              task: "Path 1 stale WAL snapshot during recoverDelegatedTask.",
              mode: "async",
              clientRequestId: "r4-cf07-path1",
            });
            const childThreadId = task.childThreadId!;
            yield* Deferred.await(ctx.failoverEntered);
            ctx.arm(childThreadId);

            yield* ctx.orchestrator.recoverDelegatedTask(childThreadId);
            assert.isTrue(ctx.wasTriggered());
            assert.isAtLeast(ctx.recheckCount(), 1);

            const parentMid = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            const subagentMid = parentMid.subagents.find((entry) => entry.id === task.taskId);
            assert.equal(subagentMid?.status, "running");
            assert.isNull(subagentMid?.result ?? null);

            const parentRun1Id = parentMid.runs[0]!.id;
            const waitParentRun1Done = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.id === parentRun1Id &&
                  event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseParent, undefined);
            yield* Fiber.join(waitParentRun1Done);

            const waitParentWake = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.ordinal === 2 &&
                  event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            const waitDelivered = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "subagent.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.id === task.taskId &&
                  event.payload.completionDelivery?.state === "delivered",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseAttempt2, undefined);
            yield* Fiber.join(waitParentWake);
            yield* Fiber.join(waitDelivered);
            yield* ctx.worker.drain();

            const parentFinal = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            assert.isAtLeast(parentFinal.runs.length, 2);
            const subagentFinal = parentFinal.subagents.find((entry) => entry.id === task.taskId);
            assert.equal(subagentFinal?.status, "completed");
            assert.include(subagentFinal?.result ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
            assert.equal(subagentFinal?.completionDelivery?.state, "delivered");
          }),
        );

        // Path 2: delegated_task.completion-delivery.acknowledge with identical stale pre-commit
        // WAL snapshot overlap -> delegatedTaskResultPending sees "superseded", does NOT commit
        // acknowledged mid-Attempt-2, and parent wakes when Attempt 2 completes.
        yield* runStaleSnapshotScenario("r4-cf07-path2-ack", {}, (ctx) =>
          Effect.gen(function* () {
            const task = yield* ctx.mcp.delegateTask(ctx.scope, {
              task: "Path 2 stale WAL snapshot during acknowledge.",
              mode: "async",
              clientRequestId: "r4-cf07-path2",
            });
            const childThreadId = task.childThreadId!;
            yield* Deferred.await(ctx.failoverEntered);
            ctx.arm(childThreadId);

            const parentBeforeAck = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            const parentRun1Id = parentBeforeAck.runs[0]!.id;
            yield* ctx.orchestrator.dispatch({
              type: "delegated_task.completion-delivery.acknowledge",
              commandId: CommandId.make("r4-cf07-path2-ack-cmd"),
              parentThreadId: ctx.parentThreadId,
              taskId: task.taskId,
              observedByRunId: parentRun1Id,
            });
            assert.isTrue(ctx.wasTriggered());
            assert.isAtLeast(ctx.recheckCount(), 1);

            const parentMid = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            const subagentMid = parentMid.subagents.find((entry) => entry.id === task.taskId);
            assert.equal(subagentMid?.status, "running");
            assert.notEqual(subagentMid?.completionDelivery?.state, "acknowledged");

            const waitParentRun1Done = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.id === parentRun1Id &&
                  event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseParent, undefined);
            yield* Fiber.join(waitParentRun1Done);

            const waitParentWake = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.ordinal === 2 &&
                  event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            const waitDelivered = yield* ctx.orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "subagent.updated" &&
                  event.threadId === ctx.parentThreadId &&
                  event.payload.id === task.taskId &&
                  event.payload.completionDelivery?.state === "delivered",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(ctx.releaseAttempt2, undefined);
            yield* Fiber.join(waitParentWake);
            yield* Fiber.join(waitDelivered);
            yield* ctx.worker.drain();

            const parentFinal = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
            assert.isAtLeast(parentFinal.runs.length, 2);
            const subagentFinal = parentFinal.subagents.find((entry) => entry.id === task.taskId);
            assert.equal(subagentFinal?.status, "completed");
            assert.include(subagentFinal?.result ?? "", "AUTHORITATIVE_RESULT_FROM_AGY2");
            assert.equal(subagentFinal?.completionDelivery?.state, "delivered");
          }),
        );

        // Path 3 (control): genuinely exhausted failover across all accounts still returns
        // "settled" and finalizes cleanly.
        yield* runStaleSnapshotScenario(
          "r4-cf07-path3-exhausted",
          { exhaustAllAccounts: true },
          (ctx) =>
            Effect.gen(function* () {
              const waitSubagentFailed = yield* ctx.orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "subagent.updated" &&
                    event.threadId === ctx.parentThreadId &&
                    event.payload.status === "failed",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped({ startImmediately: true }),
              );
              yield* Effect.yieldNow;
              const task = yield* ctx.mcp.delegateTask(ctx.scope, {
                task: "Path 3 control: all Antigravity accounts exhausted.",
                mode: "async",
                clientRequestId: "r4-cf07-path3",
              });
              const childThreadId = task.childThreadId!;
              yield* Fiber.join(waitSubagentFailed);
              yield* ctx.worker.drain();

              const pendingAfterExhaustion =
                yield* ctx.orchestrator.delegatedTaskResultPending(childThreadId);
              assert.isFalse(pendingAfterExhaustion);

              const parentMid = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
              const parentRun1Id = parentMid.runs[0]!.id;
              const waitParentWake = yield* ctx.orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "run.updated" &&
                    event.threadId === ctx.parentThreadId &&
                    event.payload.ordinal === 2 &&
                    event.payload.status === "completed",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped({ startImmediately: true }),
              );
              const waitDelivered = yield* ctx.orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "subagent.updated" &&
                    event.threadId === ctx.parentThreadId &&
                    event.payload.id === task.taskId &&
                    event.payload.completionDelivery?.state === "delivered",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped({ startImmediately: true }),
              );
              const waitParentRun1Done = yield* ctx.orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "run.updated" &&
                    event.threadId === ctx.parentThreadId &&
                    event.payload.id === parentRun1Id &&
                    event.payload.status === "completed",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped({ startImmediately: true }),
              );
              yield* Effect.yieldNow;
              yield* Deferred.succeed(ctx.releaseParent, undefined);
              yield* Fiber.join(waitParentRun1Done);
              yield* Fiber.join(waitParentWake);
              yield* Fiber.join(waitDelivered);
              yield* ctx.worker.drain();

              const childFinal = yield* ctx.orchestrator.getThreadProjection(childThreadId);
              assert.equal(childFinal.attempts.length, 2);
              assert.equal(childFinal.runs[0]?.status, "failed");

              const parentFinal = yield* ctx.orchestrator.getThreadProjection(ctx.parentThreadId);
              const subagentFinal = parentFinal.subagents.find((entry) => entry.id === task.taskId);
              assert.equal(subagentFinal?.status, "failed");
              assert.equal(subagentFinal?.completionDelivery?.state, "delivered");
            }),
        );
      }),
  );

  it.effect(
    "R4-CF-06: chained queued runs resolve effectiveBasePinned across prior queued runs (pin-after-Q1 unpins on Q2 model change, and Q2 behind unpinned Q1 preserves unpin)",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const cwd = yield* checkpointWorkspace("antigravity-r4-cf06-chained-queue");
        const initialProviders = [
          makeAntigravityProvider("antigravity", {
            displayName: "AGY-1",
            usageLimits: makeLimits([
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
          makeAntigravityProvider("antigravity_2", {
            displayName: "AGY-2",
            usageLimits: makeLimits([
              { id: "gemini-5h", usedPercent: 10 },
              { id: "3p-5h", usedPercent: 10 },
            ]),
          }),
        ];
        const run1StartedA = yield* Deferred.make<void>();
        const releaseRun1A = yield* Deferred.make<void>();
        const run1StartedB = yield* Deferred.make<void>();
        const releaseRun1B = yield* Deferred.make<void>();

        const layer = makeIntegrationLayer({
          name: "antigravity-r4-cf06-chained",
          cwd,
          providers: initialProviders,
          recordedTurns: [],
          outcomeFor: (_instanceId, turn) => {
            if (turn.message.text === "CHAIN_A_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "ok",
                pause: { onStarted: run1StartedA, release: releaseRun1A },
              };
            }
            if (turn.message.text === "CHAIN_B_RUN1") {
              return {
                behavior: "succeed",
                assistantText: "ok",
                pause: { onStarted: run1StartedB, release: releaseRun1B },
              };
            }
            return { behavior: "succeed", assistantText: "ok" };
          },
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const projectId = ProjectId.make("project:r4-cf06-chained");

          // Case A (unpinned -> Q1 same-instance -> pin thread -> Q2 changes model omitting pin):
          // Effective base at Q1 is pinned=true, so Q2 changing model must record explicit unpin (false).
          const threadA = ThreadId.make("thread:r4-cf06-chain-a");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("chain-a-create"),
            threadId: threadA,
            projectId,
            title: "Chain A",
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
            commandId: CommandId.make("chain-a-run1"),
            threadId: threadA,
            messageId: MessageId.make("message:chain-a-run1"),
            text: "CHAIN_A_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const drainA = yield* Effect.forkScoped(worker.drain());
          yield* Deferred.await(run1StartedA);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("chain-a-q1"),
            threadId: threadA,
            messageId: MessageId.make("message:chain-a-q1"),
            text: "CHAIN_A_Q1",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("chain-a-pin"),
            threadId: threadA,
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("chain-a-q2"),
            threadId: threadA,
            messageId: MessageId.make("message:chain-a-q2"),
            text: "CHAIN_A_Q2",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            createdBy: "user",
            creationSource: "web",
          });
          const projA = yield* orchestrator.getThreadProjection(threadA);
          assert.isUndefined(projA.runs[1]?.antigravityFailoverPinned);
          assert.equal(projA.runs[2]?.antigravityFailoverPinned, false);

          yield* Deferred.succeed(releaseRun1A, undefined);
          yield* Fiber.join(drainA);

          // Case B (pinned -> Q1 changes model -> unpins (false) -> Q2 behind Q1 omits pin):
          // Effective base at Q1 is false, so Q2 must also persist false (never resurrect pin if Q1 is cancelled).
          const threadB = ThreadId.make("thread:r4-cf06-chain-b");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("chain-b-create"),
            threadId: threadB,
            projectId,
            title: "Chain B",
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "gemini-4-neon",
            },
            antigravityFailoverPinned: true,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("chain-b-run1"),
            threadId: threadB,
            messageId: MessageId.make("message:chain-b-run1"),
            text: "CHAIN_B_RUN1",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const drainB = yield* Effect.forkScoped(worker.drain());
          yield* Deferred.await(run1StartedB);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("chain-b-q1"),
            threadId: threadB,
            messageId: MessageId.make("message:chain-b-q1"),
            text: "CHAIN_B_Q1",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            modelSelection: {
              instanceId: ProviderInstanceId.make("antigravity"),
              model: "claude-opus-5-5",
            },
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("chain-b-q2"),
            threadId: threadB,
            messageId: MessageId.make("message:chain-b-q2"),
            text: "CHAIN_B_Q2",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const projB = yield* orchestrator.getThreadProjection(threadB);
          assert.equal(projB.runs[1]?.antigravityFailoverPinned, false);
          assert.equal(projB.runs[2]?.antigravityFailoverPinned, false);

          yield* Deferred.succeed(releaseRun1B, undefined);
          yield* Fiber.join(drainB);
        }).pipe(Effect.provide(layer));
      }),
  );
});
