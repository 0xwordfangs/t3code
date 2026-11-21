import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Layer from "effect/Layer";
import { describe, expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";

import { GoogleAccountVerificationUrl } from "@t3tools/contracts";
import {
  AntigravityKeyringError,
  decodeGoKeyringSecret,
  makeAntigravityUsageProbe,
} from "./antigravityUsageLimits.ts";

const summary = {
  groups: [
    {
      displayName: "Gemini Models",
      buckets: [
        {
          bucketId: "gemini-weekly",
          window: "weekly",
          remainingFraction: 0.96,
          resetTime: "2026-10-12T19:41:43Z",
        },
        {
          bucketId: "gemini-5h",
          window: "5h",
          remainingFraction: 0.91,
          resetTime: "2026-10-09T16:06:01Z",
        },
      ],
    },
    {
      displayName: "Claude and GPT models",
      buckets: [
        {
          bucketId: "3p-weekly",
          window: "weekly",
          remainingFraction: 0,
          resetTime: "2026-10-10T15:28:22Z",
        },
        {
          bucketId: "3p-5h",
          window: "5h",
          remainingFraction: 0.44,
          resetTime: "2026-10-09T16:06:30Z",
        },
      ],
    },
  ],
};

const cliToken = (
  refreshToken: string,
  options: { readonly expiry?: string | undefined; readonly accessToken?: string } = {},
) =>
  JSON.stringify({
    token: {
      access_token: options.accessToken ?? "access-token",
      token_type: "Bearer",
      refresh_token: refreshToken,
      ...("expiry" in options
        ? options.expiry === undefined
          ? {}
          : { expiry: options.expiry }
        : { expiry: "2099-01-01T00:00:00Z" }),
      expires_in: 3599,
    },
    auth_method: "consumer",
    id_token_email: "someone@example.com",
  });

const fixture = Effect.fn("AntigravityUsageTest.fixture")(function* (
  options: {
    summary?: unknown;
    /** Per-host quota summaries; hosts not listed fall back to `summary`. */
    summaryByHost?: Record<string, unknown>;
    models?: unknown;
    summaryStatus?: number;
    /** Per-host summary statuses; hosts not listed fall back to `summaryStatus`. */
    summaryStatusByHost?: Record<string, number>;
    refreshToken?: string;
    accessToken?: string;
    expiry?: string | undefined;
    userId?: string;
    userInfoAt?: number;
    /** The CLI is signed out: no antigravity-oauth-token file. */
    missingToken?: boolean;
    /** An acp_token.json left in the profile by an older runtime. */
    leftoverAcpToken?: boolean;
    authMethod?: "oauth-personal" | "oauth-business" | "gemini-api-key";
    keyringSecret?: string;
    /** Overrides the OS keyring read instead of `keyringSecret`. */
    readKeyringSecret?: Effect.Effect<string | undefined, AntigravityKeyringError>;
  } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profileDirectory = yield* fs.makeTempDirectoryScoped();
  yield* fs.makeDirectory(path.join(profileDirectory, "antigravity-acp"));
  if (options.leftoverAcpToken) {
    yield* fs.writeFileString(
      path.join(profileDirectory, "antigravity-acp", "acp_token.json"),
      JSON.stringify({
        client_id: "client-id",
        client_secret: "client-secret",
        refresh_token: "stale-refresh",
        project_id: "project-id",
      }),
    );
  }
  let expectedAccessToken = options.accessToken ?? "access-token";
  const cliTokenPath = path.join(profileDirectory, "antigravity-acp", "antigravity-oauth-token");
  const writeCliToken = (
    refreshToken: string,
    tokenOptions: { readonly expiry?: string | undefined; readonly accessToken?: string } = {},
  ) => {
    const resolvedAccessToken = tokenOptions.accessToken ?? options.accessToken ?? "access-token";
    expectedAccessToken = resolvedAccessToken;
    return fs.writeFileString(
      cliTokenPath,
      cliToken(refreshToken, {
        ...("expiry" in options ? { expiry: options.expiry } : {}),
        ...tokenOptions,
        accessToken: resolvedAccessToken,
      }),
    );
  };
  if (!options.missingToken) {
    yield* writeCliToken(options.refreshToken ?? "refresh-a");
  }
  const requests: Array<{ url: string; body: string }> = [];
  const http = HttpClient.make((request, url, _signal, fiber) =>
    Effect.sync(() => {
      const init = Context.getOrUndefined(fiber.context, FetchHttpClient.RequestInit);
      expect(init?.redirect).toBe("manual");
      const body =
        request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "";
      requests.push({ url: request.url, body });
      expect(request.url).not.toMatch(/\/token$/);
      expect(request.headers.authorization).toBe(`Bearer ${expectedAccessToken}`);
      if (request.url.endsWith("/userinfo")) {
        return HttpClientResponse.fromWeb(
          request,
          options.userId
            ? Response.json({ id: options.userId, email: "someone@example.com" })
            : Response.json({}, { status: 403 }),
        );
      }
      expect(JSON.parse(body)).toEqual({});
      expect(request.headers["user-agent"]).toMatch(/^antigravity\/hub\/2\.24\.0 /);
      return HttpClientResponse.fromWeb(
        request,
        request.url.endsWith(":retrieveUserQuotaSummary")
          ? Response.json(options.summaryByHost?.[url.host] ?? options.summary ?? summary, {
              status: options.summaryStatusByHost?.[url.host] ?? options.summaryStatus ?? 200,
            })
          : Response.json(options.models ?? { models: {} }),
      );
    }).pipe(
      Effect.tap(() =>
        request.url.endsWith("/userinfo") && options.userInfoAt !== undefined
          ? TestClock.setTime(options.userInfoAt)
          : Effect.void,
      ),
    ),
  );
  const usage = yield* makeAntigravityUsageProbe({
    profileDirectory,
    authMethod: options.authMethod ?? "oauth-personal",
    readKeyringSecret: options.readKeyringSecret ?? Effect.succeed(options.keyringSecret),
  }).pipe(Effect.provideService(HttpClient.HttpClient, http));
  return { ...usage, requests, writeCliToken, cliTokenPath };
});

describe("Antigravity subscription limits", () => {
  // Oracle: the 10:16 UTC payload and its narrow Workaround rule.
  it.effect("omits rolling resets for untouched summary buckets and keeps used Gemini resets", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:04Z")));
      const { probe } = yield* fixture({
        summary: {
          groups: [
            {
              buckets: [
                {
                  bucketId: "3p-5h",
                  window: "5h",
                  remainingFraction: 1,
                  resetTime: "2026-10-21T15:16:05Z",
                },
                {
                  bucketId: "3p-weekly",
                  window: "weekly",
                  remainingFraction: 1,
                  resetTime: "2026-10-28T10:16:05Z",
                },
                {
                  bucketId: "gemini-5h",
                  window: "5h",
                  remainingFraction: 0.848104,
                  resetTime: "2026-10-21T13:29:53Z",
                  description:
                    "You have used some of your 5-hour limit, it will fully refresh in 3 hours, 13 minutes.",
                },
                {
                  bucketId: "gemini-weekly",
                  window: "weekly",
                  remainingFraction: 0.9620177,
                  resetTime: "2026-10-28T08:29:53Z",
                  description:
                    "You have used some of your weekly limit, it will fully refresh in 6 days, 22 hours.",
                },
              ],
            },
          ],
        },
      });
      const limits = yield* probe;
      const byId = new Map(limits.windows.map((window) => [window.id, window]));
      expect(byId.get("3p-5h")).toEqual({
        id: "3p-5h",
        label: "Claude & GPT (shared) · 5-hour",
        kind: "session",
        usedPercent: 0,
        windowDurationMins: 300,
      });
      expect(byId.get("3p-weekly")).toEqual({
        id: "3p-weekly",
        label: "Claude & GPT (shared) · Weekly",
        kind: "weekly",
        usedPercent: 0,
        windowDurationMins: 10080,
      });
      expect(byId.get("gemini-5h")?.resetsAt).toBe("2026-10-21T13:29:53.000Z");
      expect(byId.get("gemini-weekly")?.resetsAt).toBe("2026-10-28T08:29:53.000Z");
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Oracle: measure the tolerance from the summary request, after auth.
  it.effect("uses summary request time after authentication for the reset tolerance", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:02Z")));
      const { probe } = yield* fixture({
        userInfoAt: DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:04Z")),
        summary: {
          buckets: [
            {
              bucketId: "3p-5h",
              window: "5h",
              remainingFraction: 1,
              resetTime: "2026-10-21T15:17:03Z",
            },
          ],
        },
      });
      expect((yield* probe).windows[0]?.resetsAt).toBeUndefined();
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Oracle: keep descriptions, measured resets, and spent buckets.
  it.effect("keeps summary resets with a description, a distant reset, or no quota left", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:04Z")));
      const { probe } = yield* fixture({
        summary: {
          buckets: [
            {
              bucketId: "described",
              window: "5h",
              remainingFraction: 1,
              description: "Reported reset",
              resetTime: "2026-10-21T15:16:04Z",
            },
            {
              bucketId: "distant",
              window: "weekly",
              remainingFraction: 1,
              resetTime: "2026-10-28T12:16:04Z",
            },
            {
              bucketId: "spent",
              window: "5h",
              remainingFraction: 0,
              resetTime: "2026-10-21T15:16:04Z",
            },
            {
              bucketId: "overfull",
              window: "5h",
              remainingFraction: 1.01,
              resetTime: "2026-10-21T15:16:04Z",
            },
            {
              bucketId: "empty-description",
              window: "5h",
              remainingFraction: 1,
              description: "",
              resetTime: "2026-10-21T15:16:04Z",
            },
          ],
        },
      });
      expect((yield* probe).windows.map(({ id, resetsAt }) => ({ id, resetsAt }))).toEqual([
        { id: "described", resetsAt: "2026-10-21T15:16:04.000Z" },
        { id: "empty-description", resetsAt: "2026-10-21T15:16:04.000Z" },
        { id: "overfull", resetsAt: "2026-10-21T15:16:04.000Z" },
        { id: "spent", resetsAt: "2026-10-21T15:16:04.000Z" },
        { id: "distant", resetsAt: "2026-10-28T12:16:04.000Z" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Oracle: the 60-second tolerance applies to both known window lengths.
  it.effect("omits summary resets 59 seconds away but keeps resets 61 seconds away", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:04Z")));
      const { probe } = yield* fixture({
        summary: {
          buckets: [
            {
              bucketId: "5h-before",
              window: "5h",
              remainingFraction: 1,
              resetTime: "2026-10-21T15:15:05Z",
            },
            {
              bucketId: "5h-after",
              window: "5h",
              remainingFraction: 1,
              resetTime: "2026-10-21T15:17:05Z",
            },
            {
              bucketId: "5h-edge",
              window: "5h",
              remainingFraction: 1,
              resetTime: "2026-10-21T15:17:04Z",
            },
            {
              bucketId: "weekly-after",
              window: "weekly",
              remainingFraction: 1,
              resetTime: "2026-10-28T10:17:03Z",
            },
            {
              bucketId: "weekly-before",
              window: "weekly",
              remainingFraction: 1,
              resetTime: "2026-10-28T10:15:03Z",
            },
            {
              bucketId: "weekly-edge",
              window: "weekly",
              remainingFraction: 1,
              resetTime: "2026-10-28T10:15:04Z",
            },
          ],
        },
      });
      expect((yield* probe).windows.map(({ id, resetsAt }) => ({ id, resetsAt }))).toEqual([
        { id: "5h-after", resetsAt: "2026-10-21T15:17:05.000Z" },
        { id: "5h-before", resetsAt: undefined },
        { id: "5h-edge", resetsAt: undefined },
        { id: "weekly-after", resetsAt: undefined },
        { id: "weekly-before", resetsAt: "2026-10-28T10:15:03.000Z" },
        { id: "weekly-edge", resetsAt: undefined },
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Oracle: workspace sign-ins are metered on cloudcode-pa, so its reading wins even when less used.
  it.effect("reports a business account's quota from the endpoint it is metered on", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:04Z")));
      const { probe } = yield* fixture({
        authMethod: "oauth-business",
        summaryByHost: {
          "daily-cloudcode-pa.googleapis.com": {
            buckets: [
              {
                bucketId: "3p-5h",
                window: "5h",
                remainingFraction: 1,
                resetTime: "2026-10-21T15:16:04Z",
              },
              {
                bucketId: "3p-weekly",
                window: "weekly",
                remainingFraction: 0.5,
                resetTime: "2026-10-27T10:16:04Z",
              },
            ],
          },
          "cloudcode-pa.googleapis.com": {
            buckets: [
              {
                bucketId: "3p-5h",
                window: "5h",
                remainingFraction: 0.5,
                resetTime: "2026-10-21T13:16:04Z",
              },
              {
                bucketId: "3p-weekly",
                window: "weekly",
                remainingFraction: 1,
                resetTime: "2026-10-28T10:16:04Z",
              },
            ],
          },
        },
      });
      expect((yield* probe).windows).toEqual([
        {
          id: "3p-5h",
          label: "Claude & GPT (shared) · 5-hour",
          kind: "session",
          usedPercent: 50,
          windowDurationMins: 300,
          resetsAt: "2026-10-21T13:16:04.000Z",
        },
        {
          id: "3p-weekly",
          label: "Claude & GPT (shared) · Weekly",
          kind: "weekly",
          usedPercent: 0,
          windowDurationMins: 10080,
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Oracle: model quotas have no evidence for the summary-only rule.
  it.effect("keeps full model fallback resets even at a full window from the request", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-21T10:16:04Z")));
      const { probe } = yield* fixture({
        summaryStatus: 404,
        models: {
          models: {
            "gemini-a": {
              quotaInfo: { remainingFraction: 1, resetTime: "2026-10-21T15:16:04Z" },
              weeklyQuotaInfo: { remainingFraction: 1, resetTime: "2026-10-28T10:16:04Z" },
            },
          },
        },
      });
      expect(
        (yield* probe).windows.map(({ id, resetsAt, windowDurationMins }) => ({
          id,
          resetsAt,
          windowDurationMins,
        })),
      ).toEqual([
        { id: "gemini-weekly", resetsAt: "2026-10-28T10:16:04.000Z", windowDurationMins: 10080 },
        { id: "gemini-quota", resetsAt: "2026-10-21T15:16:04.000Z", windowDurationMins: undefined },
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("reports separate Gemini and shared Claude/GPT five-hour and weekly limits", () =>
    Effect.gen(function* () {
      const { probe, requests } = yield* fixture();
      const limits = yield* probe;
      expect(limits.windows).toHaveLength(4);
      expect(limits.unavailable).toBeUndefined();
      expect(limits.credentialFingerprint).toMatch(/^[0-9a-f]{64}$/);
      const byId = new Map(limits.windows.map((window) => [window.id, window]));
      expect(byId.get("gemini-weekly")?.usedPercent).toBeCloseTo(4);
      expect(byId.get("gemini-5h")?.usedPercent).toBeCloseTo(9);
      expect(byId.get("3p-weekly")).toMatchObject({
        label: "Claude & GPT (shared) · Weekly",
        kind: "weekly",
        usedPercent: 100,
        windowDurationMins: 10080,
        resetsAt: "2026-10-10T15:28:22.000Z",
      });
      expect(byId.get("3p-5h")).toMatchObject({ kind: "session", windowDurationMins: 300 });
      expect(byId.get("3p-5h")?.usedPercent).toBeCloseTo(56);
      yield* probe;
      expect(
        requests.filter((request) => request.url.endsWith(":retrieveUserQuotaSummary")),
      ).toHaveLength(4);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("merges quota groups that only one endpoint reports", () =>
    Effect.gen(function* () {
      const { probe } = yield* fixture({
        summaryByHost: {
          // The daily endpoint can omit Gemini while the other endpoint returns all groups.
          "daily-cloudcode-pa.googleapis.com": {
            groups: [
              {
                displayName: "Claude and GPT models",
                buckets: [
                  { bucketId: "3p-weekly", window: "weekly", remainingFraction: 0.5 },
                  { bucketId: "3p-5h", window: "5h", remainingFraction: 0.3 },
                ],
              },
            ],
          },
        },
      });
      const limits = yield* probe;
      expect(limits.windows.map((window) => window.id).toSorted()).toEqual([
        "3p-5h",
        "3p-weekly",
        "gemini-5h",
        "gemini-weekly",
      ]);
      const byId = new Map(limits.windows.map((window) => [window.id, window]));
      expect(byId.get("gemini-weekly")?.label).toBe("Gemini · Weekly");
      expect(byId.get("3p-5h")?.usedPercent).toBeCloseTo(70);
      expect(byId.get("3p-weekly")?.usedPercent).toBe(50);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Oracle: reported payloads for a personal sign-in, whose runtime meters on the daily endpoint.
  it.effect("reports a personal account's quota from the daily endpoint it is metered on", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-11-07T12:00:00Z")));
      const argon = (remainingFraction: number, resetTime: string) => ({
        groups: [
          {
            displayName: "Gemini 4 Argon",
            buckets: [
              {
                bucketId: "gemini-argon-wtu-weekly",
                window: "weekly",
                remainingFraction,
                resetTime,
              },
            ],
          },
        ],
      });
      const { probe } = yield* fixture({
        authMethod: "oauth-personal",
        summaryByHost: {
          "daily-cloudcode-pa.googleapis.com": argon(0.8623059, "2026-11-14T08:41:35Z"),
          "cloudcode-pa.googleapis.com": argon(0, "2026-11-08T18:27:14Z"),
        },
      });
      expect((yield* probe).windows).toEqual([
        expect.objectContaining({
          id: "gemini-argon-wtu-weekly",
          usedPercent: expect.closeTo(13.77, 2),
          resetsAt: "2026-11-14T08:41:35.000Z",
        }),
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("labels Argon and Carbon quotas apart from the default Gemini quota", () =>
    Effect.gen(function* () {
      const { probe } = yield* fixture({
        summary: {
          groups: [
            ...summary.groups,
            {
              displayName: "Gemini 4 Argon",
              buckets: [
                { bucketId: "gemini-argon-wtu-weekly", window: "weekly", remainingFraction: 0.6 },
              ],
            },
            {
              displayName: "Gemini 4 Carbon",
              buckets: [
                { bucketId: "gemini-carbon-wtu-weekly", window: "weekly", remainingFraction: 0.4 },
              ],
            },
            {
              displayName: " ",
              buckets: [{ bucketId: "gemini-carbon-5h", window: "5h", remainingFraction: 0.8 }],
            },
          ],
        },
      });
      const limits = yield* probe;
      const byId = new Map(limits.windows.map((window) => [window.id, window]));
      expect(byId.get("gemini-weekly")?.label).toBe("Gemini · Weekly");
      expect(byId.get("gemini-argon-wtu-weekly")?.label).toBe("Gemini 4 Argon · Weekly");
      expect(byId.get("gemini-carbon-wtu-weekly")?.label).toBe("Gemini 4 Carbon · Weekly");
      expect(byId.get("gemini-carbon-5h")?.label).toBe("Gemini Carbon · 5-hour");
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("keeps one endpoint's quotas when the other fails", () =>
    Effect.gen(function* () {
      const { probe } = yield* fixture({
        summaryStatusByHost: { "daily-cloudcode-pa.googleapis.com": 503 },
      });
      const limits = yield* probe;
      expect(limits.unavailable).toBeUndefined();
      expect(limits.windows).toHaveLength(4);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("re-reads a changed sign-in on every probe and returns unsupported after removal", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const { probe, writeCliToken, cliTokenPath } = yield* fixture();
      const first = yield* probe;
      yield* writeCliToken("refresh-b");
      const second = yield* probe;
      expect(second.credentialFingerprint).not.toBe(first.credentialFingerprint);
      yield* fs.remove(cliTokenPath);
      expect((yield* probe).unavailable?.reason).toBe("unsupported");
      yield* writeCliToken("refresh-b");
      const restored = yield* probe;
      expect(restored.unavailable).toBeUndefined();
      expect(restored.credentialFingerprint).toBe(second.credentialFingerprint);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("names one Google account the same way from every sign-in", () =>
    Effect.gen(function* () {
      const read = (refreshToken: string, userId: string) =>
        fixture({ refreshToken, userId }).pipe(Effect.flatMap((usage) => usage.probe));
      const machineA = yield* read("refresh-a", "google-user-1");
      const machineB = yield* read("refresh-b", "google-user-1");
      const otherAccount = yield* read("refresh-c", "google-user-2");
      expect(machineA.credentialFingerprint).toMatch(/^[0-9a-f]{64}$/);
      expect(machineB.credentialFingerprint).toBe(machineA.credentialFingerprint);
      expect(otherAccount.credentialFingerprint).not.toBe(machineA.credentialFingerprint);
      expect(JSON.stringify(machineA)).not.toMatch(/google-user-1|someone@example.com/);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("falls back to model quotas without inventing a window duration", () =>
    Effect.gen(function* () {
      const { probe } = yield* fixture({
        summaryStatus: 404,
        models: {
          models: {
            "gemini-a": {
              modelProvider: "MODEL_PROVIDER_GOOGLE",
              quotaInfo: { remainingFraction: 0.8, resetTime: "2026-10-10T12:00:00Z" },
            },
            "gemini-b": {
              modelProvider: "MODEL_PROVIDER_GOOGLE",
              quotaInfo: { remainingFraction: 0.6, resetTime: "2026-10-10T12:00:00Z" },
            },
            "claude-a": { quotaInfo: { remainingFraction: 0.2 } },
          },
        },
      });
      const limits = yield* probe;
      expect(limits.windows).toHaveLength(2);
      expect(limits.windows.find((window) => window.id === "gemini-quota")?.usedPercent).toBe(40);
      expect(
        limits.windows.every(
          (window) => window.kind === "other" && window.windowDurationMins === undefined,
        ),
      ).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("skips disabled and missing fractions and clamps valid percentages", () =>
    Effect.gen(function* () {
      const { probe } = yield* fixture({
        summary: {
          buckets: [
            { bucketId: "disabled", remainingFraction: 0, disabled: true },
            { bucketId: "missing", resetTime: "2026-10-10T12:00:00Z" },
            { bucketId: "gemini-5h", window: "5h", remainingFraction: 2, resetTime: "invalid" },
          ],
        },
      });
      expect((yield* probe).windows).toEqual([
        {
          id: "gemini-5h",
          kind: "session",
          label: "Gemini · 5-hour",
          usedPercent: 0,
          windowDurationMins: 300,
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("does not make network requests for API keys or missing sign-ins", () =>
    Effect.gen(function* () {
      for (const options of [
        { authMethod: "gemini-api-key" as const },
        { missingToken: true },
        { missingToken: true, leftoverAcpToken: true },
      ]) {
        const { probe, requests } = yield* fixture(options);
        expect((yield* probe).unavailable?.reason).toBe("unsupported");
        expect(requests).toEqual([]);
      }
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("reads 1.5.x CLI keyring sign-ins only for Gemini Enterprise without refreshing", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-09T00:00:00Z")));
      // Google account instances keep their own sign-in; the shared entry belongs to another app.
      let personalKeyringReads = 0;
      const personal = yield* fixture({
        authMethod: "oauth-personal",
        missingToken: true,
        readKeyringSecret: Effect.suspend(() => {
          personalKeyringReads += 1;
          return Effect.succeed(
            cliToken("cli-refresh", {
              expiry: "2026-10-09T12:00:00.123456789+11:00",
            }),
          );
        }),
      });
      expect((yield* personal.probe).unavailable?.message).toBe(
        "Sign in with Google to see Antigravity limits.",
      );
      expect(personal.requests).toEqual([]);
      expect(personalKeyringReads).toBe(0);

      const { probe, requests } = yield* fixture({
        authMethod: "oauth-business",
        missingToken: true,
        userId: "google-user-1",
        keyringSecret: cliToken("cli-refresh", {
          expiry: "2026-10-09T12:00:00.123456789+11:00",
        }),
      });
      const limits = yield* probe;
      expect(limits.unavailable).toBeUndefined();
      expect(limits.windows).toHaveLength(4);
      expect(limits.credentialFingerprint).toMatch(/^[0-9a-f]{64}$/);
      expect(requests.some((request) => request.url.endsWith("/token"))).toBe(false);
      expect(JSON.stringify(limits)).not.toMatch(/cli-refresh|access-token|someone@example.com/);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("prefers antigravity-oauth-token over the OS keyring", () =>
    Effect.gen(function* () {
      let keyringReads = 0;
      const { probe } = yield* fixture({
        authMethod: "oauth-business",
        readKeyringSecret: Effect.suspend(() => {
          keyringReads += 1;
          return Effect.die("the keyring must not be read when antigravity-oauth-token exists");
        }),
      });
      const limits = yield* probe;
      expect(limits.unavailable).toBeUndefined();
      expect(limits.windows).toHaveLength(4);
      expect(keyringReads).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("ignores leftover acp_token.json across file, keyring, and signed-out states", () =>
    Effect.gen(function* () {
      const fromFile = yield* fixture({
        leftoverAcpToken: true,
        refreshToken: "refresh-b",
      });
      const fileLimits = yield* fromFile.probe;
      expect(fileLimits.unavailable).toBeUndefined();
      expect(fileLimits.windows).toHaveLength(4);
      expect(fromFile.requests.map((request) => request.body).join("\n")).not.toContain(
        "stale-refresh",
      );

      const fromKeyring = yield* fixture({
        authMethod: "oauth-business",
        leftoverAcpToken: true,
        missingToken: true,
        keyringSecret: cliToken("refresh-b"),
      });
      const keyringLimits = yield* fromKeyring.probe;
      expect(keyringLimits.unavailable).toBeUndefined();
      expect(keyringLimits.windows).toHaveLength(4);
      expect(fromKeyring.requests.map((request) => request.body).join("\n")).not.toContain(
        "stale-refresh",
      );

      const personalSignedOut = yield* fixture({
        authMethod: "oauth-personal",
        leftoverAcpToken: true,
        missingToken: true,
        keyringSecret: cliToken("refresh-b"),
      });
      expect((yield* personalSignedOut.probe).unavailable?.reason).toBe("unsupported");
      expect(personalSignedOut.requests).toEqual([]);

      const signedOut = yield* fixture({
        leftoverAcpToken: true,
        missingToken: true,
      });
      expect((yield* signedOut.probe).unavailable?.reason).toBe("unsupported");
      expect(signedOut.requests).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("returns probeFailed with zero network requests for expired or missing expiry", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-09T12:00:00Z")));
      for (const expiry of [
        "2026-10-09T11:59:00Z",
        "2026-10-09T12:00:30Z",
        "2026-10-09T12:01:00Z",
        "not-a-timestamp",
        undefined,
      ]) {
        const fromKeyring = yield* fixture({
          authMethod: "oauth-business",
          missingToken: true,
          keyringSecret: cliToken("cli-refresh", { expiry }),
        });
        const keyringLimits = yield* fromKeyring.probe;
        expect(keyringLimits.windows).toEqual([]);
        expect(keyringLimits.unavailable).toEqual({
          reason: "probeFailed",
          message:
            "Antigravity's saved sign-in has expired. Start an Antigravity turn to renew it, then refresh limits.",
        });
        expect(fromKeyring.requests).toEqual([]);

        const fromFile = yield* fixture({ expiry });
        const fileLimits = yield* fromFile.probe;
        expect(fileLimits.windows).toEqual([]);
        expect(fileLimits.unavailable?.reason).toBe("probeFailed");
        expect(fromFile.requests).toEqual([]);
      }

      const validPastMargin = yield* fixture({
        authMethod: "oauth-business",
        missingToken: true,
        keyringSecret: cliToken("cli-refresh", { expiry: "2026-10-09T12:01:01Z" }),
      });
      expect((yield* validPastMargin.probe).unavailable).toBeUndefined();
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect("returns a safe failure with zero network requests when the keyring read fails", () =>
    Effect.gen(function* () {
      const { probe, requests } = yield* fixture({
        authMethod: "oauth-business",
        missingToken: true,
        readKeyringSecret: Effect.fail(
          new AntigravityKeyringError({ cause: "secret-service-locked-super-secret-detail" }),
        ),
      });
      const limits = yield* probe;
      expect(limits.windows).toEqual([]);
      expect(limits.unavailable?.reason).toBe("probeFailed");
      expect(requests).toEqual([]);
      expect(JSON.stringify(limits)).not.toContain("secret-service-locked-super-secret-detail");
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect(
    "returns a safe failure without leaking contents for invalid JSON in CLI token file or keyring",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const fromFile = yield* fixture({ missingToken: true });
        yield* fs.writeFileString(fromFile.cliTokenPath, "{corrupt-file-secret-value");
        const fileLimits = yield* fromFile.probe;
        expect(fileLimits.windows).toEqual([]);
        expect(fileLimits.unavailable?.reason).toBe("probeFailed");
        expect(fromFile.requests).toEqual([]);
        expect(JSON.stringify(fileLimits)).not.toContain("corrupt-file-secret-value");

        for (const keyringSecret of [
          "{corrupt-keyring-secret-value",
          JSON.stringify({ token: { access_token: "", refresh_token: "leaked-keyring-refresh" } }),
        ]) {
          const fromKeyring = yield* fixture({
            authMethod: "oauth-business",
            missingToken: true,
            keyringSecret,
          });
          const keyringLimits = yield* fromKeyring.probe;
          expect(keyringLimits.windows).toEqual([]);
          expect(keyringLimits.unavailable?.reason).toBe("probeFailed");
          expect(fromKeyring.requests).toEqual([]);
          expect(JSON.stringify(keyringLimits)).not.toMatch(
            /corrupt-keyring-secret-value|leaked-keyring-refresh/,
          );
        }
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect(
    "falls back to access_token fingerprinting when userinfo fails and refresh_token is omitted",
    () =>
      Effect.gen(function* () {
        const noRefresh = (accessToken: string) =>
          JSON.stringify({
            token: {
              access_token: accessToken,
              expiry: "2099-01-01T00:00:00Z",
            },
          });
        const first = yield* fixture({
          authMethod: "oauth-business",
          missingToken: true,
          accessToken: "token-a",
          keyringSecret: noRefresh("token-a"),
        });
        const second = yield* fixture({
          authMethod: "oauth-business",
          missingToken: true,
          accessToken: "token-b",
          keyringSecret: noRefresh("token-b"),
        });
        const firstLimits = yield* first.probe;
        const secondLimits = yield* second.probe;
        expect(firstLimits.credentialFingerprint).toMatch(/^[0-9a-f]{64}$/);
        expect(secondLimits.credentialFingerprint).toMatch(/^[0-9a-f]{64}$/);
        expect(secondLimits.credentialFingerprint).not.toBe(firstLimits.credentialFingerprint);
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it("decodes go-keyring's UTF-8, base64, and hex encodings and rejects malformed input", () => {
    const bytes = (text: string) => Array.from(new TextEncoder().encode(text));
    const value = '{"token":{"note":"café 🔑"}}';
    expect(decodeGoKeyringSecret(bytes(value))).toBe(value);
    expect(decodeGoKeyringSecret(new TextEncoder().encode(value))).toBe(value);
    expect(decodeGoKeyringSecret([])).toBe("");
    const encodedBase64 = Buffer.from(value, "utf8").toString("base64");
    expect(decodeGoKeyringSecret(bytes(`go-keyring-base64:${encodedBase64}`))).toBe(value);
    const hex = Buffer.from(value, "utf8").toString("hex");
    expect(decodeGoKeyringSecret(bytes(`go-keyring-encoded:${hex}`))).toBe(value);
    expect(() => decodeGoKeyringSecret(bytes("go-keyring-base64:!!!invalid!!!"))).toThrow();
    expect(() => decodeGoKeyringSecret(bytes("go-keyring-encoded:7b7dzz"))).toThrow();
    expect(() => decodeGoKeyringSecret(bytes("go-keyring-encoded:7b7d0"))).toThrow();
    expect(() => decodeGoKeyringSecret([0xff, 0xfe])).toThrow();
  });

  it.effect(
    "returns verificationRequired on HTTP 403 VALIDATION_REQUIRED without calling fetchAvailableModels",
    () =>
      Effect.gen(function* () {
        const validUrl =
          "https://accounts.google.com/v3/signin/continue?plt=quota-check&continue=https%3A%2F%2Fantigravity.google%2F";
        const { probeDetailed, requests } = yield* fixture({
          summaryStatus: 403,
          summary: {
            error: {
              code: 403,
              message: "Verify your account to continue.",
              status: "PERMISSION_DENIED",
              details: [
                {
                  "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                  reason: "VALIDATION_REQUIRED",
                  metadata: {
                    validation_url: validUrl,
                    validation_error_message: "Verify your Google account to use Antigravity.",
                  },
                },
              ],
            },
          },
        });
        const result = yield* probeDetailed;
        expect(result.usageLimits.windows).toEqual([]);
        expect(result.usageLimits.unavailable).toEqual({
          reason: "probeFailed",
          message: "Verify your Google account to use Antigravity.",
        });
        expect(result.verificationRequired).toEqual({
          message: "Verify your Google account to use Antigravity.",
          verificationUrl: GoogleAccountVerificationUrl.make(validUrl),
        });
        expect(requests.some((request) => request.url.includes("fetchAvailableModels"))).toBe(
          false,
        );
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect(
    "blocks fetchAvailableModels and drops non-Google validation_url while falling back to google.rpc.Help links",
    () =>
      Effect.gen(function* () {
        const invalidUrlProbe = yield* fixture({
          summaryStatus: 403,
          summary: {
            error: {
              code: 403,
              message: "Account verification required.",
              status: "PERMISSION_DENIED",
              details: [
                {
                  "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                  reason: "VALIDATION_REQUIRED",
                  metadata: {
                    validation_url: "https://evil.example.com/phish",
                  },
                },
              ],
            },
          },
        });
        const invalidResult = yield* invalidUrlProbe.probeDetailed;
        expect(invalidResult.usageLimits.unavailable?.reason).toBe("probeFailed");
        expect(invalidResult.verificationRequired).toEqual({
          message: "Account verification required.",
        });
        expect(invalidResult.verificationRequired?.verificationUrl).toBeUndefined();
        expect(
          invalidUrlProbe.requests.some((request) => request.url.includes("fetchAvailableModels")),
        ).toBe(false);

        const helpUrl = "https://accounts.google.com/v3/signin/continue?plt=help-link";
        for (const metadata of [{}, { validation_url: "https://evil.example.com/phish" }]) {
          const helpProbe = yield* fixture({
            summaryStatus: 403,
            summary: {
              error: {
                code: 403,
                message: "Account verification required.",
                status: "PERMISSION_DENIED",
                details: [
                  {
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    reason: "VALIDATION_REQUIRED",
                    metadata,
                  },
                  {
                    "@type": "type.googleapis.com/google.rpc.Help",
                    links: [
                      { description: "Ignore", url: "https://evil.example.com/ignore" },
                      { description: "Verify", url: helpUrl },
                    ],
                  },
                ],
              },
            },
          });
          const helpResult = yield* helpProbe.probeDetailed;
          expect(helpResult.verificationRequired).toEqual({
            message: "Account verification required.",
            verificationUrl: GoogleAccountVerificationUrl.make(helpUrl),
          });
          expect(
            helpProbe.requests.some((request) => request.url.includes("fetchAvailableModels")),
          ).toBe(false);
        }
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});
