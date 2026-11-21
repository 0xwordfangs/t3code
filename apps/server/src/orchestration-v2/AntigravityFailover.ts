import {
  ANTIGRAVITY_DEFAULT_MODEL,
  isProviderAvailable,
  resolveProjectProviderInstanceEnabled,
  type ModelSelection,
  type ProjectId,
  type ProviderInstanceId,
  type ProviderOptionSelection,
  type ServerProvider,
  type ServerProviderModel,
  type ServerSettings,
} from "@t3tools/contracts";
import {
  antigravityModelQuotaFamily,
  antigravityModelUsedPercent,
  exhaustedUsageWindow,
} from "@t3tools/shared/usageLimits";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettingsService from "../serverSettings.ts";

const DEFAULT_ANTIGRAVITY_QUOTA_COOLDOWN_MS = 5 * 60 * 1_000;
const ANTIGRAVITY_COOLDOWN_EARLY_CLEAR_GRACE_MS = 60 * 1_000;
const MAX_ANTIGRAVITY_QUOTA_COOLDOWN_MS = 24 * 60 * 60 * 1_000;

export interface AntigravityQuotaCooldownEntry {
  readonly untilMs: number;
  readonly recordedAtMs: number;
}

export interface AntigravityFailoverDecision {
  readonly fromInstanceId: ProviderInstanceId;
  readonly toInstanceId: ProviderInstanceId;
  readonly fromDisplayName: string;
  readonly toDisplayName: string;
  readonly candidate: ServerProvider;
  readonly modelSelection: ModelSelection;
}

export interface AntigravityFailoverShape {
  readonly isAntigravityInstance: (instanceId: ProviderInstanceId) => Effect.Effect<boolean>;
  readonly preflightSelection: (input: {
    readonly projectId: ProjectId;
    readonly selection: ModelSelection;
    readonly providers?: ReadonlyArray<ServerProvider>;
    readonly settings?: ServerSettings;
    readonly orchestrationCapableInstanceIds?: ReadonlySet<ProviderInstanceId>;
  }) => Effect.Effect<AntigravityFailoverDecision | null>;
  readonly nextFailoverSelection: (input: {
    readonly projectId: ProjectId;
    readonly selection: ModelSelection;
    readonly triedInstanceIds: ReadonlySet<ProviderInstanceId>;
    readonly providers?: ReadonlyArray<ServerProvider>;
    readonly settings?: ServerSettings;
    readonly orchestrationCapableInstanceIds?: ReadonlySet<ProviderInstanceId>;
  }) => Effect.Effect<AntigravityFailoverDecision | null>;
  readonly recordQuotaExhausted: (input: {
    readonly instanceId: ProviderInstanceId;
    readonly model: string;
    readonly resetAt?: string | null;
  }) => Effect.Effect<void>;
  readonly getDisplayName: (instanceId: ProviderInstanceId) => Effect.Effect<string>;
}

const noopFailover: AntigravityFailoverShape = {
  isAntigravityInstance: () => Effect.succeed(false),
  preflightSelection: () => Effect.succeed(null),
  nextFailoverSelection: () => Effect.succeed(null),
  recordQuotaExhausted: () => Effect.void,
  getDisplayName: (instanceId) => Effect.succeed(instanceId),
};

export class AntigravityFailover extends Context.Reference<AntigravityFailoverShape>(
  "t3/orchestration-v2/AntigravityFailover",
  { defaultValue: () => noopFailover },
) {}

export function antigravityCooldownKey(instanceId: ProviderInstanceId, model: string): string {
  return `${instanceId}\0${antigravityModelQuotaFamily(model) ?? "*"}`;
}

function isAntigravityCooldownActive(input: {
  readonly provider: ServerProvider;
  readonly model: string;
  readonly entry: AntigravityQuotaCooldownEntry | undefined;
  readonly nowMs: number;
}): boolean {
  const { provider, model, entry, nowMs } = input;
  if (entry === undefined || entry.untilMs <= nowMs) return false;
  const limits = provider.usageLimits;
  if (limits !== undefined && !limits.unavailable) {
    const checkedAtMs = Date.parse(limits.checkedAt);
    const usedPercent = antigravityModelUsedPercent(provider, nowMs, model);
    // Require both a grace window (so the immediate refresh triggered by `recordQuotaExhausted`
    // cannot cancel its own cooldown on a stale/burst summary) and a matching 0% reset window.
    if (
      Number.isFinite(checkedAtMs) &&
      checkedAtMs >= entry.recordedAtMs + ANTIGRAVITY_COOLDOWN_EARLY_CLEAR_GRACE_MS &&
      usedPercent === 0 &&
      exhaustedUsageWindow(provider, nowMs, model) === undefined
    ) {
      return false;
    }
  }
  return true;
}

function advertisedAntigravityModel(
  provider: ServerProvider,
  model: string,
): ServerProviderModel | undefined {
  return (
    provider.models.find((candidate) => candidate.slug === model) ??
    provider.models.find((candidate) =>
      candidate.aliases?.some((alias) => alias.toLowerCase() === model.toLowerCase()),
    )
  );
}

/**
 * Normalizes model options for the target Antigravity instance so small catalog differences
 * across accounts (such as `thought_level: "xhigh"` vs `"high"`) do not fail a failover turn.
 */
export function normalizeAntigravityOptionsForCandidate(
  options: ReadonlyArray<ProviderOptionSelection> | undefined,
  candidate: ServerProvider,
  model: string,
): ReadonlyArray<ProviderOptionSelection> | undefined {
  if (options === undefined) return undefined;
  const advertised = advertisedAntigravityModel(candidate, model);
  const descriptors = advertised?.capabilities?.optionDescriptors;
  if (descriptors === undefined) return options;
  const normalized: Array<ProviderOptionSelection> = [];
  for (const option of options) {
    const descriptor = descriptors.find((entry) => entry.id === option.id);
    if (descriptor === undefined) continue;
    if (descriptor.type === "boolean") {
      if (typeof option.value === "boolean") normalized.push(option);
      continue;
    }
    if (descriptor.options.some((choice) => choice.id === option.value)) {
      normalized.push(option);
      continue;
    }
    if (
      (descriptor.role === "reasoning" || option.id === "thought_level") &&
      option.value === "xhigh" &&
      descriptor.options.some((choice) => choice.id === "high")
    ) {
      normalized.push({ id: option.id, value: "high" });
    }
  }
  return normalized.length === 0 ? undefined : normalized;
}

function providerLabel(provider: ServerProvider | undefined, fallbackId: ProviderInstanceId) {
  const name = provider?.displayName?.trim();
  return name && name.length > 0 ? name : fallbackId;
}

function supportsRequestedModel(provider: ServerProvider, model: string): boolean {
  if (provider.models.length === 0 || model === ANTIGRAVITY_DEFAULT_MODEL) return true;
  return advertisedAntigravityModel(provider, model) !== undefined;
}

function isEligibleAntigravityCandidate(input: {
  readonly candidate: ServerProvider;
  readonly model: string;
  readonly projectId: ProjectId;
  readonly settings: ServerSettings;
  readonly nowMs: number;
  readonly excludedInstanceIds: ReadonlySet<ProviderInstanceId>;
  readonly orchestrationCapableInstanceIds?: ReadonlySet<ProviderInstanceId>;
  readonly cooldowns?: ReadonlyMap<string, AntigravityQuotaCooldownEntry>;
}): boolean {
  const { candidate, model, projectId, settings, nowMs } = input;
  if (candidate.driver !== "antigravity") return false;
  if (input.excludedInstanceIds.has(candidate.instanceId)) return false;
  if (
    input.orchestrationCapableInstanceIds !== undefined &&
    !input.orchestrationCapableInstanceIds.has(candidate.instanceId)
  ) {
    return false;
  }
  if (!candidate.enabled || !candidate.installed || !isProviderAvailable(candidate)) return false;
  if (candidate.status === "error" || candidate.status === "disabled") return false;
  if (candidate.auth.status === "unauthenticated") return false;
  if (!resolveProjectProviderInstanceEnabled(settings, projectId, candidate.instanceId)) {
    return false;
  }
  if (!supportsRequestedModel(candidate, model)) return false;
  if (exhaustedUsageWindow(candidate, nowMs, model) !== undefined) return false;
  const cooldownEntry = input.cooldowns?.get(antigravityCooldownKey(candidate.instanceId, model));
  if (isAntigravityCooldownActive({ provider: candidate, model, entry: cooldownEntry, nowMs })) {
    return false;
  }
  return true;
}

function candidateRank(
  candidate: ServerProvider,
  nowMs: number,
  model: string,
): { readonly tier: number; readonly usedPercent: number } {
  const usedPercent = antigravityModelUsedPercent(candidate, nowMs, model);
  if (candidate.auth.status === "authenticated") {
    if (usedPercent !== null) {
      return { tier: 0, usedPercent };
    }
    // Idle >1h accounts commonly report `probeFailed` due to an expired access token while their
    // refresh token is still valid and quota is untouched; keep them eligible right after
    // confirmed-quota accounts.
    return { tier: 1, usedPercent: 0 };
  }
  return { tier: 2, usedPercent: usedPercent ?? 0 };
}

export function selectAntigravityFailoverCandidate(input: {
  readonly mode: "preflight" | "failover";
  readonly projectId: ProjectId;
  readonly selection: ModelSelection;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly settings: ServerSettings;
  readonly nowMs: number;
  readonly triedInstanceIds?: ReadonlySet<ProviderInstanceId>;
  readonly orchestrationCapableInstanceIds?: ReadonlySet<ProviderInstanceId>;
  readonly cooldowns?: ReadonlyMap<string, AntigravityQuotaCooldownEntry>;
}): AntigravityFailoverDecision | null {
  const current = input.providers.find(
    (provider) => provider.instanceId === input.selection.instanceId,
  );
  if (current === undefined || current.driver !== "antigravity") return null;

  if (input.mode === "preflight") {
    const currentExhausted =
      exhaustedUsageWindow(current, input.nowMs, input.selection.model) !== undefined ||
      isAntigravityCooldownActive({
        provider: current,
        model: input.selection.model,
        entry: input.cooldowns?.get(
          antigravityCooldownKey(current.instanceId, input.selection.model),
        ),
        nowMs: input.nowMs,
      });
    if (!currentExhausted) return null;
  }

  const excludedInstanceIds = new Set<ProviderInstanceId>(input.triedInstanceIds ?? []);
  excludedInstanceIds.add(input.selection.instanceId);

  const candidates = input.providers
    .filter((candidate) =>
      isEligibleAntigravityCandidate({
        candidate,
        model: input.selection.model,
        projectId: input.projectId,
        settings: input.settings,
        nowMs: input.nowMs,
        excludedInstanceIds,
        ...(input.orchestrationCapableInstanceIds === undefined
          ? {}
          : { orchestrationCapableInstanceIds: input.orchestrationCapableInstanceIds }),
        ...(input.cooldowns === undefined ? {} : { cooldowns: input.cooldowns }),
      }),
    )
    .map((candidate) => ({
      candidate,
      rank: candidateRank(candidate, input.nowMs, input.selection.model),
    }))
    .toSorted(
      (left, right) =>
        left.rank.tier - right.rank.tier ||
        left.rank.usedPercent - right.rank.usedPercent ||
        left.candidate.instanceId.localeCompare(right.candidate.instanceId),
    );

  const chosen = candidates[0]?.candidate;
  if (chosen === undefined) return null;

  const normalizedOptions = normalizeAntigravityOptionsForCandidate(
    input.selection.options,
    chosen,
    input.selection.model,
  );
  const modelSelection: ModelSelection = {
    instanceId: chosen.instanceId,
    model: input.selection.model,
    ...(normalizedOptions === undefined ? {} : { options: normalizedOptions }),
  };
  return {
    fromInstanceId: current.instanceId,
    toInstanceId: chosen.instanceId,
    fromDisplayName: providerLabel(current, current.instanceId),
    toDisplayName: providerLabel(chosen, chosen.instanceId),
    candidate: chosen,
    modelSelection,
  };
}

export const layer = Layer.effect(
  AntigravityFailover,
  Effect.gen(function* () {
    const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
    const serverSettings = yield* ServerSettingsService.ServerSettingsService;
    const scope = yield* Effect.scope;
    const cooldownsRef = yield* Ref.make<ReadonlyMap<string, AntigravityQuotaCooldownEntry>>(
      new Map(),
    );

    const readInputs = Effect.fn("AntigravityFailover.readInputs")(function* (input: {
      readonly providers?: ReadonlyArray<ServerProvider>;
      readonly settings?: ServerSettings;
    }) {
      return yield* Effect.all({
        providers:
          input.providers !== undefined
            ? Effect.succeed(input.providers)
            : providerRegistry.getProviders,
        settings:
          input.settings !== undefined
            ? Effect.succeed(input.settings)
            : serverSettings.getSettings.pipe(Effect.orDie),
        cooldowns: Ref.get(cooldownsRef),
        nowMs: Clock.currentTimeMillis,
      });
    });

    return {
      isAntigravityInstance: Effect.fn("AntigravityFailover.isAntigravityInstance")(
        function* (instanceId) {
          const providers = yield* providerRegistry.getProviders;
          return (
            providers.find((provider) => provider.instanceId === instanceId)?.driver ===
            "antigravity"
          );
        },
      ),
      preflightSelection: Effect.fn("AntigravityFailover.preflightSelection")(function* (input) {
        const { providers, settings, cooldowns, nowMs } = yield* readInputs(input);
        return selectAntigravityFailoverCandidate({
          mode: "preflight",
          projectId: input.projectId,
          selection: input.selection,
          providers,
          settings,
          nowMs,
          cooldowns,
          ...(input.orchestrationCapableInstanceIds === undefined
            ? {}
            : { orchestrationCapableInstanceIds: input.orchestrationCapableInstanceIds }),
        });
      }),
      nextFailoverSelection: Effect.fn("AntigravityFailover.nextFailoverSelection")(
        function* (input) {
          const { providers, settings, cooldowns, nowMs } = yield* readInputs(input);
          return selectAntigravityFailoverCandidate({
            mode: "failover",
            projectId: input.projectId,
            selection: input.selection,
            providers,
            settings,
            nowMs,
            triedInstanceIds: input.triedInstanceIds,
            cooldowns,
            ...(input.orchestrationCapableInstanceIds === undefined
              ? {}
              : { orchestrationCapableInstanceIds: input.orchestrationCapableInstanceIds }),
          });
        },
      ),
      recordQuotaExhausted: Effect.fn("AntigravityFailover.recordQuotaExhausted")(
        function* (input) {
          const nowMs = yield* Clock.currentTimeMillis;
          const parsedReset = input.resetAt ? Date.parse(input.resetAt) : Number.NaN;
          const untilMs =
            Number.isFinite(parsedReset) && parsedReset > nowMs
              ? Math.min(parsedReset, nowMs + MAX_ANTIGRAVITY_QUOTA_COOLDOWN_MS)
              : nowMs + DEFAULT_ANTIGRAVITY_QUOTA_COOLDOWN_MS;
          const key = antigravityCooldownKey(input.instanceId, input.model);
          yield* Ref.update(cooldownsRef, (current) => {
            const next = new Map<string, AntigravityQuotaCooldownEntry>();
            for (const [existingKey, existingEntry] of current) {
              if (existingEntry.untilMs > nowMs) {
                next.set(existingKey, existingEntry);
              }
            }
            next.set(key, { untilMs, recordedAtMs: nowMs });
            return next;
          });
          yield* providerRegistry
            .refreshInstance(input.instanceId)
            .pipe(Effect.ignoreCause({ log: false }), Effect.forkIn(scope));
        },
      ),
      getDisplayName: Effect.fn("AntigravityFailover.getDisplayName")(function* (instanceId) {
        const providers = yield* providerRegistry.getProviders;
        return providerLabel(
          providers.find((provider) => provider.instanceId === instanceId),
          instanceId,
        );
      }),
    };
  }),
);
