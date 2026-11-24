import { EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  createProviderCatalogRefreshRunner,
  providerCatalogRefreshError,
} from "./provider-catalog-refresh";

describe("mobile provider catalog refresh", () => {
  it("requests model discovery for the selected environment and deduplicates pending taps", async () => {
    let resolveRefresh: ((value: "refreshed") => void) | undefined;
    const refreshProviders = vi.fn(
      () =>
        new Promise<"refreshed">((resolve) => {
          resolveRefresh = resolve;
        }),
    );
    const refresh = createProviderCatalogRefreshRunner(refreshProviders);
    const environmentId = EnvironmentId.make("environment-mobile");

    const first = refresh(environmentId);
    const second = refresh(environmentId);

    expect(second).toBe(first);
    expect(refreshProviders).toHaveBeenCalledOnce();
    expect(refreshProviders).toHaveBeenCalledWith({
      environmentId,
      input: { refreshModels: true },
    });

    resolveRefresh?.("refreshed");
    await expect(first).resolves.toBe("refreshed");
  });

  it("reports a discovery error and allows retry after the failed command settles", async () => {
    const failure = AsyncResult.failure<never, Error>(Cause.fail(new Error("discovery failed")));
    const success = AsyncResult.success("refreshed");
    let callCount = 0;
    const refreshProviders = vi.fn(async () => (callCount++ === 0 ? failure : success));
    const refresh = createProviderCatalogRefreshRunner(refreshProviders);
    const environmentId = EnvironmentId.make("environment-mobile");

    expect(providerCatalogRefreshError(await refresh(environmentId))).toBe("discovery failed");
    expect(providerCatalogRefreshError(await refresh(environmentId))).toBeNull();
    expect(refreshProviders).toHaveBeenCalledTimes(2);
  });

  it("sends an empty input when refreshModels is disabled for read-only callers and keeps distinct keys separate", async () => {
    const refreshProviders = vi.fn(async () => "refreshed");
    const refresh = createProviderCatalogRefreshRunner(refreshProviders);
    const environmentId = EnvironmentId.make("environment-mobile-readonly");

    const readOnlyPromise = refresh(environmentId, { refreshModels: false });
    const privilegedPromise = refresh(environmentId, { refreshModels: true });
    expect(privilegedPromise).not.toBe(readOnlyPromise);
    await expect(readOnlyPromise).resolves.toBe("refreshed");
    await expect(privilegedPromise).resolves.toBe("refreshed");
    expect(refreshProviders).toHaveBeenCalledTimes(2);
    expect(refreshProviders).toHaveBeenNthCalledWith(1, {
      environmentId,
      input: {},
    });
    expect(refreshProviders).toHaveBeenNthCalledWith(2, {
      environmentId,
      input: { refreshModels: true },
    });
  });
});
