// @effect-diagnostics nodeBuiltinImport:off -- node:sqlite and lstat read the native session log.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import type { OrchestrationV2ProviderFailure } from "@t3tools/contracts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as EffectAcpErrors from "effect-acp/errors";

import { isAntigravitySignInRequiredError } from "../antigravityAuthSupport.ts";
import {
  ANTIGRAVITY_VERIFICATION_REQUIRED_CODE,
  extractAntigravityVerificationFromText,
  extractAntigravityVerificationRequired,
  hasConflictingNon403ApiError,
  type AntigravityVerificationRequired,
} from "../antigravityVerification.ts";
import { antigravityManagedPath, antigravityProfileRoot } from "./AntigravityProfilePaths.ts";

// CORTEX_STEP_TYPE_ERROR_MESSAGE in the native trajectory.
const ERROR_STEP_TYPE = 17;
// The harness error text leads the payload; a Go stack trace follows it.
const MAX_ERROR_PAYLOAD_BYTES = 16_384;
const MAX_ACP_ERROR_SCAN_CHARS = 32_768;
const MAX_ACP_DATA_CANDIDATES = 8;
const MAX_ACP_DATA_PROPERTIES = 64;
const MAX_SERIALIZE_DEPTH = 16;
// The Go harness's rendering of a Google API error, as the agent's api_errors.py reads it.
const API_ERROR = /Error (\d{3}), Message: (.{1,4096}?), Status: ([A-Z_]+)/s;
const QUOTA_EXHAUSTED = /\breason:QUOTA_EXHAUSTED\b/;
const RESOURCE_EXHAUSTED = /\bRESOURCE_EXHAUSTED\b/;
const HTTP_429 = /\bHTTP\s*429\b|\bstatus(?:\s*code)?\s*[:=]?\s*429\b/i;
const QUOTA_MESSAGE =
  /\b(?:individual quota reached|quota exhausted|usage limit reached|quota limit reached)\b/i;
// Permanent account/auth/policy blocks must never be classified as quota limits.
const NON_QUOTA_PERMANENT =
  /\b(?:VALIDATION_REQUIRED|PERMISSION_DENIED|UNAUTHENTICATED|ACCOUNT_RESTRICTED)\b/i;
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);
// Claude rejects a saved thinking block without a valid signature. It sits in the native
// history, so every later prompt to that session fails the same way.
const INVALID_THINKING_SIGNATURE =
  /\bmessages\.\d+\.content\.\d+(?:\.thinking\.signature: Field required|: Invalid `signature` in `thinking` block)/;
const RESET_TIMESTAMP = /\bquotaResetTimeStamp:([^\s\]]+)/;
const RESET_DELAY = /\bquotaResetDelay:((?:\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|h|m|s))+)(?=[\s\]])/;
// google.rpc.RetryInfo, which plain rate limits carry instead of a quota reset.
const RETRY_DELAY = /\bretryDelay:((?:\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|h|m|s))+)(?=[\s\]])/;
const GO_DURATION_PART = /(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|h|m|s)/g;
const GO_DURATION_UNIT_MS: Record<string, number> = {
  ns: 1e-6,
  us: 1e-3,
  µs: 1e-3,
  μs: 1e-3,
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
};
const MAX_RESET_DELAY_MS = 365 * 24 * 60 * 60 * 1_000;
const NATIVE_SESSION_ID = /^[\w-]+$/;

interface LastStep {
  readonly type: number;
  readonly payload: Uint8Array | null;
}

class AntigravitySessionLogError extends Schema.TaggedError<AntigravitySessionLogError>()(
  "AntigravitySessionLogError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not read the Antigravity session log.";
  }
}

const sessionLogError = (cause: unknown) => new AntigravitySessionLogError({ cause });

const readSteps = <A>(file: string, read: (database: NodeSqlite.DatabaseSync) => A) =>
  Effect.try({
    try: () => {
      const database = new NodeSqlite.DatabaseSync(file, { readOnly: true });
      try {
        database.exec("PRAGMA busy_timeout = 100");
        const hasSteps =
          database
            .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'steps'")
            .get() !== undefined;
        return hasSteps ? read(database) : undefined;
      } finally {
        database.close();
      }
    },
    catch: sessionLogError,
  });

const lastStepIndex = (database: NodeSqlite.DatabaseSync) => {
  const index = database.prepare("SELECT max(idx) AS idx FROM steps").get()?.idx;
  return typeof index === "number" ? index : -1;
};

const lastStepAfter =
  (index: number) =>
  (database: NodeSqlite.DatabaseSync): LastStep | undefined => {
    const row = database
      .prepare(
        `SELECT step_type AS type, substr(step_payload, 1, ?) AS payload
         FROM steps WHERE idx > ? ORDER BY idx DESC LIMIT 1`,
      )
      .get(MAX_ERROR_PAYLOAD_BYTES, index);
    if (typeof row?.type !== "number") return undefined;
    return { type: row.type, payload: row.payload instanceof Uint8Array ? row.payload : null };
  };

function resetAtFrom(text: string, now: DateTime.Utc): string | undefined {
  // makeProviderFailure drops a timestamp that does not parse, so fall back to the delay.
  const timestamp = RESET_TIMESTAMP.exec(text)?.[1];
  if (timestamp !== undefined && Option.isSome(DateTime.make(timestamp))) return timestamp;
  const delay = RESET_DELAY.exec(text)?.[1] ?? RETRY_DELAY.exec(text)?.[1];
  if (delay === undefined) return undefined;
  let delayMs = 0;
  for (const [, amount, unit] of delay.matchAll(GO_DURATION_PART)) {
    delayMs += Number(amount) * (GO_DURATION_UNIT_MS[unit!] ?? 0);
    if (!Number.isFinite(delayMs) || delayMs > MAX_RESET_DELAY_MS) return undefined;
  }
  if (!Number.isFinite(delayMs) || delayMs <= 0 || delayMs > MAX_RESET_DELAY_MS) return undefined;
  const target = DateTime.make(DateTime.toEpochMillis(now) + delayMs);
  return Option.isSome(target) ? DateTime.formatIso(target.value) : undefined;
}

function turnFailure(
  stopReason: EffectAcpSchema.StopReason,
  step: LastStep | undefined,
  now: DateTime.Utc,
):
  | {
      readonly failure: OrchestrationV2ProviderFailure;
      readonly threadDisposition: "reusable" | "broken";
    }
  | undefined {
  const errorText =
    step?.type === ERROR_STEP_TYPE
      ? new TextDecoder().decode(step.payload ?? new Uint8Array())
      : undefined;
  // The agent maps only the SDK's QUOTA_EXHAUSTED stop reason to `refusal`.
  if (errorText === undefined && stopReason !== "refusal") return undefined;
  const text = errorText ?? "";
  const reusable = (failure: OrchestrationV2ProviderFailure) =>
    ({ failure, threadDisposition: "reusable" }) as const;
  const verification = extractAntigravityVerificationFromText(text);
  if (verification !== undefined) {
    return reusable(
      makeProviderFailure({
        class: "permission_error",
        code: ANTIGRAVITY_VERIFICATION_REQUIRED_CODE,
        message: verification.message,
        retryable: false,
        ...(verification.verificationUrl === undefined
          ? {}
          : { verificationUrl: verification.verificationUrl }),
      }),
    );
  }
  const apiError = API_ERROR.exec(text);
  const nonQuotaPermanent = NON_QUOTA_PERMANENT.test(text);
  if (!nonQuotaPermanent && (stopReason === "refusal" || QUOTA_EXHAUSTED.test(text))) {
    return reusable(
      makeProviderFailure({
        class: "usage_limit",
        message: "You have reached your Antigravity quota for this period.",
        code: apiError?.[3] ?? "QUOTA_EXHAUSTED",
        retryable: true,
        resetAt: resetAtFrom(text, now) ?? null,
      }),
    );
  }
  if (INVALID_THINKING_SIGNATURE.test(text)) {
    return {
      failure: makeProviderFailure({
        class: "provider_error",
        message:
          "Antigravity's saved conversation can no longer be sent to the model. The next message starts a fresh Antigravity session with this thread's context.",
        code: "INVALID_THINKING_SIGNATURE",
        retryable: true,
      }),
      threadDisposition: "broken",
    };
  }
  if (apiError === null) {
    if (!nonQuotaPermanent && (QUOTA_MESSAGE.test(text) || RESOURCE_EXHAUSTED.test(text))) {
      return reusable(
        makeProviderFailure({
          class: "usage_limit",
          message: "You have reached your Antigravity quota for this period.",
          code: RESOURCE_EXHAUSTED.test(text) ? "RESOURCE_EXHAUSTED" : "QUOTA_EXHAUSTED",
          retryable: true,
          resetAt: resetAtFrom(text, now) ?? null,
        }),
      );
    }
    return reusable(
      makeProviderFailure({
        class: "provider_error",
        message: "Antigravity stopped this turn because of an error.",
      }),
    );
  }
  const [, httpStatus, message, status] = apiError;
  const rateLimited =
    !nonQuotaPermanent &&
    (httpStatus === "429" || status === "RESOURCE_EXHAUSTED" || QUOTA_MESSAGE.test(message ?? ""));
  return reusable(
    makeProviderFailure({
      class: rateLimited ? "usage_limit" : "provider_error",
      message: message!.trim(),
      code: status ?? null,
      retryable: rateLimited || (!nonQuotaPermanent && Number(httpStatus) >= 500),
      resetAt: rateLimited ? (resetAtFrom(text, now) ?? null) : null,
    }),
  );
}

function boundScanText(text: string, maxChars = MAX_ACP_ERROR_SCAN_CHARS): string {
  return text.length > maxChars ? text.slice(0, maxChars) : text;
}

function serializeErrorData(data: unknown, maxChars = MAX_ACP_ERROR_SCAN_CHARS): string {
  if (data === undefined || data === null) return "";
  if (typeof data === "string") return boundScanText(data, maxChars);
  const chunks: Array<string> = [];
  let length = 0;
  const seen = new Set<object>();

  const push = (chunk: string): boolean => {
    if (length >= maxChars) return false;
    const remaining = maxChars - length;
    if (chunk.length <= remaining) {
      chunks.push(chunk);
      length += chunk.length;
      return length < maxChars;
    }
    chunks.push(chunk.slice(0, remaining));
    length = maxChars;
    return false;
  };

  const visit = (value: unknown, depth: number): boolean => {
    if (length >= maxChars) return false;
    if (value === null) return push("null");
    if (typeof value === "boolean") return push(value ? "true" : "false");
    if (typeof value === "number") {
      return push(Number.isFinite(value) ? String(value) : "null");
    }
    if (typeof value === "string") {
      return push(JSON.stringify(boundScanText(value, maxChars - length + 2)));
    }
    if (typeof value === "bigint") return push(JSON.stringify(String(value)));
    if (typeof value !== "object") return push("null");
    if (value instanceof Date) return push(JSON.stringify(value.toISOString()));
    if (depth >= MAX_SERIALIZE_DEPTH || seen.has(value)) return push('"[truncated]"');
    seen.add(value);
    try {
      if (Array.isArray(value)) {
        if (!push("[")) return false;
        for (let index = 0; index < value.length; index++) {
          if (index > 0 && !push(",")) return false;
          const item = value[index];
          if (item === undefined || typeof item === "function" || typeof item === "symbol") {
            if (!push("null")) return false;
          } else if (!visit(item, depth + 1)) {
            return false;
          }
        }
        return push("]");
      }
      if (!push("{")) return false;
      let written = 0;
      const record = value as Record<string, unknown>;
      for (const key in record) {
        if (!Object.hasOwn(record, key)) continue;
        const entry = record[key];
        if (entry === undefined || typeof entry === "function" || typeof entry === "symbol") {
          continue;
        }
        if (written > 0 && !push(",")) return false;
        written += 1;
        if (!push(`${JSON.stringify(boundScanText(key, maxChars - length + 2))}:`)) {
          return false;
        }
        if (!visit(entry, depth + 1)) return false;
      }
      return push("}");
    } finally {
      seen.delete(value);
    }
  };

  try {
    visit(data, 0);
    return chunks.join("");
  } catch {
    return "";
  }
}

function makeLazySerializedErrorData(data: unknown): () => string {
  let cached: string | undefined;
  return () => {
    if (cached === undefined) {
      cached = serializeErrorData(data);
    }
    return cached;
  };
}

function extractVerificationFromAcpError(
  cause: EffectAcpErrors.AcpRequestError,
  boundedErrorMessage: string,
  readSerializedData: () => string,
): AntigravityVerificationRequired | undefined {
  if (cause.code === -32602 || hasConflictingNon403ApiError(boundedErrorMessage)) {
    return undefined;
  }
  const fromData = extractAntigravityVerificationRequired(cause.data);
  if (fromData?.verificationUrl !== undefined) return fromData;
  let fallback = fromData;

  const fromMessage = extractAntigravityVerificationFromText(boundedErrorMessage);
  if (fromMessage !== undefined) {
    if (fromMessage.verificationUrl !== undefined) return fromMessage;
    fallback ??= fromMessage;
  }

  if (typeof cause.data === "string") {
    const parsed = extractAntigravityVerificationFromText(boundScanText(cause.data));
    if (parsed !== undefined) {
      if (parsed.verificationUrl !== undefined) return parsed;
      fallback ??= parsed;
    }
    return fallback;
  }

  if (typeof cause.data === "object" && cause.data !== null) {
    const record = cause.data as Record<string, unknown>;
    let inspectedCandidates = 0;
    let visitedProperties = 0;
    for (const key in record) {
      if (!Object.hasOwn(record, key)) continue;
      if (
        inspectedCandidates >= MAX_ACP_DATA_CANDIDATES ||
        visitedProperties >= MAX_ACP_DATA_PROPERTIES
      ) {
        break;
      }
      visitedProperties += 1;
      const value = record[key];
      if (typeof value !== "string") continue;
      inspectedCandidates += 1;
      const parsed = extractAntigravityVerificationFromText(boundScanText(value));
      if (parsed === undefined) continue;
      if (parsed.verificationUrl !== undefined) return parsed;
      fallback ??= parsed;
    }
  }

  return fallback ?? extractAntigravityVerificationFromText(readSerializedData());
}

/**
 * Classifies ACP RPC errors returned directly by `prompt()` or idle `stopReason: "error"`.
 * Permanent non-quota failures stay non-retryable and never trigger quota failover.
 */
export function classifyAntigravityPromptFailure(
  cause: unknown,
  now: DateTime.Utc = DateTime.nowUnsafe(),
): OrchestrationV2ProviderFailure {
  if (isAntigravitySignInRequiredError(cause)) {
    return makeProviderFailure({ cause, class: "provider_error", retryable: false });
  }
  if (!isAcpRequestError(cause)) {
    return makeProviderFailure({ cause, class: "provider_error" });
  }
  const boundedErrorMessage = boundScanText(cause.errorMessage);
  const readSerializedData = makeLazySerializedErrorData(cause.data);
  const verification = extractVerificationFromAcpError(
    cause,
    boundedErrorMessage,
    readSerializedData,
  );
  if (verification !== undefined) {
    return makeProviderFailure({
      cause,
      class: "permission_error",
      code: ANTIGRAVITY_VERIFICATION_REQUIRED_CODE,
      message: verification.message,
      retryable: false,
      ...(verification.verificationUrl === undefined
        ? {}
        : { verificationUrl: verification.verificationUrl }),
    });
  }
  const dataText = readSerializedData();
  const combined = `${boundedErrorMessage}\n${dataText}`;
  const nonQuotaMatch = NON_QUOTA_PERMANENT.exec(combined);
  if (nonQuotaMatch !== null) {
    return makeProviderFailure({
      cause,
      message: boundedErrorMessage.trim() || undefined,
      code: nonQuotaMatch[0].toUpperCase(),
      class: "provider_error",
      retryable: false,
    });
  }
  const apiError = API_ERROR.exec(combined);
  const isQuotaError =
    QUOTA_EXHAUSTED.test(combined) ||
    QUOTA_MESSAGE.test(combined) ||
    RESOURCE_EXHAUSTED.test(combined) ||
    HTTP_429.test(combined) ||
    apiError?.[1] === "429" ||
    apiError?.[3] === "RESOURCE_EXHAUSTED";
  if (isQuotaError) {
    const trimmed = boundedErrorMessage.trim();
    return makeProviderFailure({
      cause,
      class: "usage_limit",
      message:
        apiError?.[2]?.trim() ||
        trimmed ||
        "You have reached your Antigravity quota for this period.",
      code:
        apiError?.[3] ??
        (RESOURCE_EXHAUSTED.test(combined) ? "RESOURCE_EXHAUSTED" : "QUOTA_EXHAUSTED"),
      retryable: true,
      resetAt: resetAtFrom(combined, now) ?? null,
    });
  }
  return makeProviderFailure({
    cause,
    message: boundedErrorMessage.trim() || undefined,
    code: String(cause.code),
    class: "provider_error",
  });
}

const conversationSegments = ["antigravity-acp", "conversations"];

/**
 * Antigravity reports an upstream API failure as a chat message and `end_turn`. Its
 * session log ends that turn with an error step, so the check reads only steps the
 * current prompt added, read-only, from this session's own unlinked database. Without
 * that evidence a typed `refusal` still fails the turn.
 */
export const watchAntigravityPromptOutcome = Effect.fn("AntigravityTurnOutcome.watch")(function* (
  profileDirectory: string,
  sessionId: string,
) {
  const typedOnly = (result: EffectAcpSchema.PromptResponse) =>
    DateTime.now.pipe(Effect.map((now) => turnFailure(result.stopReason, undefined, now)));
  if (!NATIVE_SESSION_ID.test(sessionId)) return typedOnly;
  const segments = [...conversationSegments, `${sessionId}.db`];
  const before = yield* Effect.tryPromise({
    try: async () => {
      const root = await antigravityProfileRoot(profileDirectory);
      if (root === undefined) return undefined;
      if ((await antigravityManagedPath(root, conversationSegments, "directory")) === undefined) {
        return undefined;
      }
      const database = await antigravityManagedPath(root, segments, "file");
      // A path that exists but fails the proof is a link or other foreign entry.
      if (database === undefined) {
        const present = await NodeFSP.lstat(NodePath.join(root, ...segments)).then(
          () => true,
          () => false,
        );
        return present ? undefined : { root, identity: undefined, startIndex: -1 };
      }
      return { root, identity: database.info, startIndex: undefined };
    },
    catch: sessionLogError,
  }).pipe(Effect.orElseSucceed(() => undefined));
  if (before === undefined) {
    yield* Effect.logWarning("The Antigravity session log is not this session's own file.");
    return typedOnly;
  }
  const startIndex =
    before.startIndex ??
    (yield* readSteps(NodePath.join(before.root, ...segments), lastStepIndex).pipe(
      Effect.map((index) => index ?? -1),
      Effect.orElseSucceed(() => undefined),
    ));
  if (startIndex === undefined) {
    yield* Effect.logWarning("Could not read the Antigravity session log before the prompt.");
    return typedOnly;
  }
  return Effect.fn("AntigravityTurnOutcome.check")(function* (
    result: EffectAcpSchema.PromptResponse,
  ) {
    // The same file as before the prompt, or one this prompt created, still unlinked.
    const after = yield* Effect.tryPromise({
      try: () => antigravityManagedPath(before.root, segments, "file"),
      catch: sessionLogError,
    }).pipe(Effect.orElseSucceed(() => undefined));
    const sameFile =
      after !== undefined &&
      (before.identity === undefined ||
        (after.info.dev === before.identity.dev && after.info.ino === before.identity.ino));
    const step = sameFile
      ? yield* readSteps(after.path, lastStepAfter(startIndex)).pipe(
          Effect.tapError(() =>
            Effect.logWarning("Could not read the Antigravity session log after the prompt."),
          ),
          Effect.orElseSucceed(() => undefined),
        )
      : undefined;
    return turnFailure(result.stopReason, step, yield* DateTime.now);
  });
});
