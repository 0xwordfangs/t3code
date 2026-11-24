import type { EnvironmentId } from "@t3tools/contracts";
import { errorDisplayMessage } from "@t3tools/client-runtime/errors";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { msg } from "gt-react-native";

import { m } from "../../i18n/messages";

type RefreshProvidersTarget = {
  readonly environmentId: EnvironmentId;
  readonly input: { readonly refreshModels?: true };
};

export function createProviderCatalogRefreshRunner<Result>(
  refreshProviders: (target: RefreshProvidersTarget) => Promise<Result>,
) {
  const pending = new Map<string, Promise<Result>>();

  return (
    environmentId: EnvironmentId,
    options?: { readonly refreshModels?: boolean },
  ): Promise<Result> => {
    const refreshModels = options?.refreshModels !== false;
    const key = `${environmentId}:${refreshModels ? "1" : "0"}`;
    const existing = pending.get(key);
    if (existing) return existing;
    const input = refreshModels ? { refreshModels: true as const } : {};
    const next = refreshProviders({ environmentId, input }).finally(() => {
      pending.delete(key);
    });
    pending.set(key, next);
    return next;
  };
}

export function providerCatalogRefreshError(
  result: AtomCommandResult<unknown, unknown>,
): string | null {
  if (result._tag !== "Failure" || isAtomCommandInterrupted(result)) return null;
  const error = squashAtomCommandFailure(result);
  return errorDisplayMessage(error, m) ?? m(msg("Provider discovery failed."));
}
