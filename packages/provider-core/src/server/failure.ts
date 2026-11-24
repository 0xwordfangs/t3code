import {
  GoogleAccountVerificationUrl,
  type NodeId,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderFailureClass,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2TurnItem,
  type ProviderDriverKind,
  type ProviderThreadId,
  type ProviderTurnId,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import { redactUrlCredentials } from "@t3tools/shared/observability";
import * as DateTime from "effect/DateTime";
import * as Cause from "effect/Cause";

import * as IdAllocator from "./IdAllocator.ts";
import type { ProviderAdapterFailureReason } from "./ProviderAdapter.ts";
import * as Schema from "effect/Schema";

export class ContextHandoffBudgetError extends Schema.TaggedError<ContextHandoffBudgetError>()(
  "ContextHandoffBudgetError",
  {},
) {
  override get message() {
    return "The request leaves too little context for a provider handoff. Shorten the request or use a larger-context model; the current request has not been truncated.";
  }
}

export class ContextHandoffBackgroundWorkError extends Schema.TaggedError<ContextHandoffBackgroundWorkError>()(
  "ContextHandoffBackgroundWorkError",
  {},
) {
  override get message() {
    return "The target provider still has background work running. Wait for it to finish or stop it before retrying the handoff.";
  }
}

export const MAX_PROVIDER_FAILURE_MESSAGE_LENGTH = 4_096;
const MAX_PROVIDER_FAILURE_CODE_LENGTH = 128;

const DEFAULT_PROVIDER_FAILURE_MESSAGE = "Provider turn failed.";

// Gateways answer an oversized body with a 413 page; resending the same history fails identically.
const REQUEST_TOO_LARGE_STATUS = /^\s*(?:(?:API Error|Error|HTTP|status)(?:\s*:)?\s*)?413\b/iu;
const REQUEST_TOO_LARGE_REASON = /\b(?:Request Entity|Payload|Content) Too Large\b/u;
const REQUEST_TOO_LARGE_MESSAGE =
  "This thread's request is too large for the provider (HTTP 413), so retrying fails the same way. Start a fresh session with /clear, or continue in a new thread.";

const PROVIDER_FAILURE_CATEGORIES: Partial<
  Record<string, { readonly summary: string; readonly guidance: string }>
> = {
  ProviderAdapterTurnStartError: {
    summary: "The provider could not start this turn",
    guidance: "Retry the turn; if it keeps failing, check the provider setup and server logs.",
  },
  ProviderAdapterEventStreamError: {
    summary: "The provider event stream closed unexpectedly",
    guidance: "Retry the turn; if it keeps failing, check the provider and server logs.",
  },
  ProviderAdapterOpenSessionError: {
    summary: "The provider session could not be opened",
    guidance: "Check that the provider is installed and signed in, then retry the turn.",
  },
  ProviderAdapterResumeThreadError: {
    summary: "The provider conversation could not be resumed",
    guidance: "Retry the turn; if it keeps failing, check the provider and server logs.",
  },
};

// codex_protocol::protocol::validate_thread_goal_objective
const CODEX_GOAL_VALIDATION_MESSAGE =
  /^goal objective must (?:not be empty|be at most \d{1,6} characters)$/u;

// codex_config::ConstraintError::InvalidValue for a managed requirements.toml or cloud policy.
const CODEX_ACCESS_REQUIREMENT_REJECTION =
  /invalid value for `(approval_policy|sandbox_mode)`: `([A-Za-z]{1,32})` is not in the allowed set /u;

const PROVIDER_FAILURE_REASONS: Record<ProviderAdapterFailureReason, string> = {
  previous_turn_active: "the previous turn is still running",
  attachment_unreadable: "an attachment could not be read",
};

/**
 * Walks a cause chain for curated text without reading arbitrary provider
 * defect text. `known` is the message of a domain error T3 raised itself;
 * `category` is the generic message of the innermost provider wrapper seen.
 */
function curatedCauseMessages(cause: unknown): {
  readonly known: string | undefined;
  readonly category: string | undefined;
} {
  const seen = new Set<unknown>();
  let category: (typeof PROVIDER_FAILURE_CATEGORIES)[string];
  let reason: string | undefined;
  const categoryMessage = () => {
    if (category === undefined) return undefined;
    return reason
      ? `${category.summary}: ${reason}. ${category.guidance}`
      : `${category.summary}. ${category.guidance}`;
  };
  for (let depth = 0; depth < 16 && cause != null && !seen.has(cause); depth++) {
    seen.add(cause);
    try {
      if (Cause.isCause(cause)) {
        cause = Cause.squash(cause);
        continue;
      }
      if (typeof cause !== "object") break;
      const tag = (cause as Record<string, unknown>)._tag;
      switch (tag) {
        case "ContextHandoffBudgetError":
          return { known: new ContextHandoffBudgetError().message, category: categoryMessage() };
        case "ContextHandoffBackgroundWorkError":
          return {
            known: new ContextHandoffBackgroundWorkError().message,
            category: categoryMessage(),
          };
        case "AcpModelSelectionError":
        case "ClaudeBackgroundWorkBlocksQueryReplacementError":
        case "ClaudeAppendSystemPromptFileError":
        case "ClaudeAppendSystemPromptMissingValueError":
        case "ClaudeSettingsLaunchArgError":
        case "ClaudeMcpRegistrationTimeoutError":
        case "ClaudeCodeRootRefusalError":
        case "ClaudeConversationNotFoundError":
        case "CursorModelUnavailableError":
        case "CodexGoalObjectiveTooLongError":
        case "ProviderSetupError":
        case "ProviderAdapterThreadInUseError":
        case "ProviderCompactionUnsupportedError":
          return { known: stringField(cause, "message"), category: categoryMessage() };
        case "CodexAppServerRequestError": {
          // Only Codex's fixed-shape goal validation and managed-requirements
          // text is actionable and safe to show; other request error text stays hidden.
          const rejection = stringField(cause, "message");
          if (
            (cause as Record<string, unknown>).method === "thread/goal/set" &&
            rejection !== undefined &&
            CODEX_GOAL_VALIDATION_MESSAGE.test(rejection)
          ) {
            return { known: `Codex rejected the goal: ${rejection}.`, category: categoryMessage() };
          }
          const access =
            rejection === undefined ? null : CODEX_ACCESS_REQUIREMENT_REJECTION.exec(rejection);
          if (access !== null) {
            return {
              known: `Codex's managed requirements do not allow \`${access[1]}\` \`${access[2]}\`, which this thread's access mode uses. Switch the thread to an access mode the policy allows.`,
              category: categoryMessage(),
            };
          }
          break;
        }
        case "AcpProcessExitedError": {
          const exit = `provider process exited${processExitStatus(cause)}`;
          const stage = category === undefined ? `The ${exit}` : `${category.summary}: the ${exit}`;
          return {
            known: `${stage}. Check the server logs for the provider's output, then retry the turn.`,
            category: categoryMessage(),
          };
        }
        // ACP reserves -32000 for auth_required; the agent's own text is not shown.
        case "AcpRequestError":
          if ((cause as Record<string, unknown>).code !== -32000) break;
          return {
            known:
              "The provider needs you to sign in. Sign in from its provider settings or with its own CLI or app, then retry the turn.",
            category: categoryMessage(),
          };
        case "ContextHandoffDeliveryUncertainError":
          return {
            known:
              "T3 could not confirm whether conversation history reached the provider. Retry the turn to recover the session.",
            category: categoryMessage(),
          };
        case "ProviderAdapterProtocolError": {
          const typedReason = (cause as Record<string, unknown>).reason;
          if (
            category !== undefined &&
            typeof typedReason === "string" &&
            Object.hasOwn(PROVIDER_FAILURE_REASONS, typedReason)
          ) {
            reason ??= PROVIDER_FAILURE_REASONS[typedReason as ProviderAdapterFailureReason];
          }
          break;
        }
      }
      if (typeof tag === "string" && Object.hasOwn(PROVIDER_FAILURE_CATEGORIES, tag)) {
        category = PROVIDER_FAILURE_CATEGORIES[tag];
        reason = undefined;
      }
      cause = (cause as Record<string, unknown>).cause;
    } catch {
      break;
    }
  }
  return { known: undefined, category: categoryMessage() };
}

// Only the structured status; stderr and the spawner cause can carry provider output.
function processExitStatus(exited: object): string {
  const { signal, code } = exited as Record<string, unknown>;
  if (typeof signal === "string" && /^SIG[A-Z0-9]{1,16}$/u.test(signal)) {
    return ` after signal ${signal}`;
  }
  return typeof code === "number" && Number.isSafeInteger(code) ? ` with code ${code}` : "";
}

/**
 * The curated message of a domain error in a failure's cause chain, if any.
 * Callers that pass their own `message` to `makeProviderFailure` check it
 * first, so a known reason is not hidden behind a wrapper's text.
 */
export function knownProviderFailureMessage(cause: unknown): string | undefined {
  return curatedCauseMessages(cause).known;
}

function stringField(value: unknown, key: "message" | "code"): string | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  try {
    const candidate = (value as Record<string, unknown>)[key];
    return typeof candidate === "string" ? candidate : undefined;
  } catch {
    return undefined;
  }
}

const TRAILING_URL_PUNCTUATION = new Set([")", ",", ".", ";", "!", "?", "…"]);
const MAX_REDACTION_INPUT_MULTIPLIER = 8;
const MIN_REDACTION_INPUT_CAP = 32_768;

function splitTrailingUrlPunctuation(match: string): {
  readonly candidate: string;
  readonly trailing: string;
} {
  let end = match.length;
  while (end > 0 && TRAILING_URL_PUNCTUATION.has(match[end - 1]!)) {
    end -= 1;
  }
  return end === match.length
    ? { candidate: match, trailing: "" }
    : { candidate: match.slice(0, end), trailing: match.slice(end) };
}

const EMBEDDED_URL_IN_PATH = /https?(?::|%(?:25){0,3}3[aA])(?:\/|%(?:25){0,3}2[fF]){2}/iu;
const ENCODED_QUERY_OR_HASH = /%(?:25){0,3}(?:3[fF]|23)/u;
const JSON_ESCAPED_QUERY_OR_HASH = /\\{1,2}u00(?:3[fF]|23|26)/u;
const TAIL_PATH_OR_QUERY_STOP = /[/?#]|\\{1,2}u002[fF]/u;
const ENCODED_OR_LITERAL_AT = /@|\\{1,2}u0040|%(?:25){0,3}40/u;
const ENCODED_OR_LITERAL_AT_GLOBAL = /@|\\{1,2}u0040|%(?:25){0,3}40/gu;
const PARTIAL_SCHEME_PREFIX = /^https?(?::|%(?:25){0,3}3[aA])(?:\/|%(?:25){0,3}2[fF]){2}/iu;
const UNIFORMLY_ENCODED_SCHEME_PREFIX =
  /^https?%(?:3[aA]%2[fF]%2[fF]|253[aA]%252[fF]%252[fF]|25253[aA]%25252[fF]%25252[fF]|2525253[aA]%2525252[fF]%2525252[fF])/iu;
const SINGLE_ENCODED_SCHEME_PREFIX = /^https?%3[aA]%2[fF]%2[fF]/iu;
const SINGLE_LAYER_AUTHORITY_DELIM = /[/?#]|%2[fF]|%3[fF]|%23/u;
const LITERAL_URL_PATTERN =
  /(?:(?<![A-Za-z0-9_])|(?<=%(?:25){0,3}[0-9A-Fa-f]{2})|(?<=\\(?:[nrt]|u00(?:20|22|27|3[cCdDeE]))))https?(?::\/\/|%(?:25){0,3}3[aA](?:\/(?:\/|%(?:25){0,3}2[fF])|%(?:25){0,3}2[fF]\/)|:(?:%(?:25){0,3}2[fF](?:\/|%(?:25){0,3}2[fF])|\/%(?:25){0,3}2[fF]))(?:<redacted>|[^\s<>"'\\]|\\(?!["']|\\["']|\\?u00(?:20|22|27|0[aAdD])))+/giu;
const ENCODED_SCHEME_URL_PATTERN =
  /(?:(?<![A-Za-z0-9_])|(?<=%(?:25){0,3}[0-9A-Fa-f]{2})|(?<=\\(?:[nrt]|u00(?:20|22|27|3[cCdDeE]))))https?(?:%3[aA]%2[fF]%2[fF](?:[^\s<>"'%\\]|%(?!(?:20|22|27|0[aAdD]))|\\(?!["']|\\["']|\\?u00(?:20|22|27|0[aAdD])))*|%253[aA]%252[fF]%252[fF](?:[^\s<>"'%\\]|%(?!(?:25)?(?:20|22|27|0[aAdD]))|\\(?!["']|\\["']|\\?u00(?:20|22|27|0[aAdD])))*|%25253[aA]%25252[fF]%25252[fF](?:[^\s<>"'%\\]|%(?!(?:25){0,2}(?:20|22|27|0[aAdD]))|\\(?!["']|\\["']|\\?u00(?:20|22|27|0[aAdD])))*|%2525253[aA]%2525252[fF]%2525252[fF](?:[^\s<>"'%\\]|%(?!(?:25){0,3}(?:20|22|27|0[aAdD]))|\\(?!["']|\\["']|\\?u00(?:20|22|27|0[aAdD])))*|%(?:25){0,3}3[aA](?:%(?:25){0,3}2[fF]){2}(?:[^\s<>"'%\\]|%(?!(?:20|22|27|0[aAdD]))|\\(?!["']|\\["']|\\?u00(?:20|22|27|0[aAdD])))*)/giu;

const ESCAPED_JSON_SECRET_FIELD =
  /(\\"[\w-]*(?:access[_-]?token|api[_-]?key|authorization|credential|password|secret|token)\\"\s*:\s*\\")(?:[^\\"]|\\(?:[^\\"]|$|\\(?:[^\\"]|(?="|$)|\\(?:[\s\S]|$))))*(\\"|(?=")|$)/giu;
const DOUBLE_QUOTED_SECRET_FIELD =
  /(["'][\w-]*(?:access[_-]?token|api[_-]?key|authorization|credential|password|secret|token)["']\s*:\s*")(?:[^\\"]|\\[\s\S]|\\$)*("|$)/giu;
const SINGLE_QUOTED_SECRET_FIELD =
  /(["'][\w-]*(?:access[_-]?token|api[_-]?key|authorization|credential|password|secret|token)["']\s*:\s*')(?:[^\\']|\\[\s\S]|\\$)*('|$)/giu;
const BEARER_OR_BASIC_PREFIX =
  /(?:(?<![A-Za-z0-9_])|(?<=%(?:25){0,3}(?:20|22|27|0[aAdD]))|(?<=\\(?:[nrt]|u00(?:20|22|27|0[aAdD]))))(Bearer|Basic)(?:\s|%(?:25){0,3}20)+/giu;
const SECRET_FIELD_PREFIX =
  /(?:(?<=[?&#]|&[aA][mM][pP];|&#0*38;|&#[xX]0*26;|\\{1,2}u00(?:26|23|3[fF]))()|(?:(?<![\w-])|(?<=%(?:25){0,3}(?:20|22|27|0[aAdD]))|(?<=\\(?:[nrt]|u00(?:20|22|27|0[aAdD]))))(?!u00(?:26|23|3[fF])))([\w-]*(?:access[_-]?token|api[_-]?key|authorization|credential|password|secret|token)\b\s*[:=]\s*)(?:(?:Bearer|Basic)(?:\s|%(?:25){0,3}20)+)?/giu;
const SK_SECRET_PATTERN =
  /(?:(?<![A-Za-z0-9_])|(?<=%(?:25){0,3}(?:20|22|27|0[aAdD]))|(?<=\\(?:[nrt]|u00(?:20|22|27|0[aAdD]))))sk-[A-Za-z0-9_-]{16,}\b/gu;
const TRAILING_TRUNCATED_SK_SECRET =
  /(?:(?<![A-Za-z0-9_])|(?<=%(?:25){0,3}(?:20|22|27|0[aAdD]))|(?<=\\(?:[nrt]|u00(?:20|22|27|0[aAdD]))))sk-[A-Za-z0-9_-]*$/u;
const TRAILING_TRUNCATED_URL_AUTHORITY =
  /(?:(?<![A-Za-z0-9])|(?<=%(?:25){0,3}[0-9A-Fa-f]{2})|(?<=\\(?:[nrt]|u00(?:20|22|27|3[cCdDeE]))))(?:[A-Za-z][A-Za-z0-9+.-]{0,31}:\/\/|https?(?::|%(?:25){0,3}3[aA])(?:\/|%(?:25){0,3}2[fF]){2})[^\s/?#"'<>\\]*$/iu;
const ENCODED_SPACE_DELIMITER_GLOBAL = /%(?:25){0,3}20/gu;
const CLEAN_ENCODED_SPACE_CONNECTORS = ["and", "or", "to", "from", "via"] as const;
const CLEAN_TRUNCATED_URL_PREFIX =
  /^https?:\/\/(?:(?:[A-Za-z0-9.-]*(?::[0-9]*)?)|\[[0-9A-Fa-f:.]*(?:\](?::[0-9]*)?)?)$/iu;
const TRUNCATED_PLACEHOLDER_ELLIPSIS =
  /^(?:<(?:r(?:e(?:d(?:a(?:c(?:t(?:e(?:d(?:>)?)?)?)?)?)?)?)?)?|\[(?:R(?:E(?:D(?:A(?:C(?:T(?:E(?:D(?:\])?)?)?)?)?)?)?)?)?)…$/u;
const SWALLOWED_PREFIX_BEFORE_QUOTE =
  /(?:[\w-]*(?:access[_-]?token|api[_-]?key|authorization|credential|password|secret|token|plt|wsticket|dpop)\b\s*[:=]|\b(?:Bearer|Basic)(?:\s|%(?:25){0,3}20)*)\s*$/iu;
const JSON_ESCAPED_VALUE_STOP = /^\\(?:[nrt]|u00(?:20|22|27|26|23|0[aAdD]))/iu;
const QUERY_PROPERTY_BOUNDARY = /^(?:\\?"|'),\s*(?:\\?"|')/u;
const OBSERVABILITY_URL_CREDENTIAL_KEYS = new Set([
  "wsticket",
  "dpop",
  "plt",
  "access_token",
  "id_token",
  "refresh_token",
]);
const OBSERVABILITY_QUERY_CREDENTIAL_KEYS = new Set([
  ...OBSERVABILITY_URL_CREDENTIAL_KEYS,
  "code",
  "token",
]);

type QuoteDelimiter = '"' | "'" | '\\"';

function isWhitespaceChar(ch: string): boolean {
  return (
    ch === " " ||
    ch === "\t" ||
    ch === "\n" ||
    ch === "\r" ||
    ch === "\f" ||
    ch === "\v" ||
    /\s/u.test(ch)
  );
}

function findLineEnd(source: string, start: number): number {
  let pos = start;
  while (pos < source.length && source[pos] !== "\n" && source[pos] !== "\r") {
    pos += 1;
  }
  return pos;
}

function isAmbiguousQuoteStartChar(ch: string | undefined): boolean {
  return (
    ch === undefined ||
    ch === "," ||
    ch === ";" ||
    ch === "." ||
    ch === "!" ||
    ch === "?" ||
    ch === ":" ||
    ch === ")" ||
    ch === "]" ||
    ch === "}" ||
    ch === "&" ||
    ch === "#" ||
    isWhitespaceChar(ch)
  );
}

function trimTrailingTokenDelimiters(
  rawToken: string,
  options?: {
    readonly preserveTrailingWrappers?: boolean;
    readonly preserveSentencePunctuation?: boolean;
  },
): {
  readonly core: string;
  readonly trailing: string;
} {
  const preserveWrappers = options?.preserveTrailingWrappers ?? true;
  const preserveSentencePunctuation = options?.preserveSentencePunctuation ?? false;
  const minEnd = rawToken.startsWith("[REDACTED]") || rawToken.startsWith("<redacted>") ? 10 : 0;
  let end = rawToken.length;
  while (end > minEnd) {
    const ch = rawToken[end - 1]!;
    if (preserveWrappers && (ch === ")" || ch === "]" || ch === "}")) {
      if (ch === "]" && rawToken.slice(0, end).endsWith("[REDACTED]")) {
        break;
      }
      end -= 1;
      continue;
    }
    if (preserveSentencePunctuation && (ch === "." || ch === "!" || ch === "?" || ch === ":")) {
      end -= 1;
      continue;
    }
    if (preserveWrappers && (ch === "'" || ch === '"')) {
      let backslashes = 0;
      let cursor = end - 2;
      while (cursor >= minEnd && rawToken[cursor] === "\\") {
        backslashes += 1;
        cursor -= 1;
      }
      if (backslashes % 2 === 0) {
        end -= 1;
        continue;
      }
      if (ch === '"' && backslashes % 4 === 1) {
        end -= 2;
        continue;
      }
    }
    break;
  }
  return end === rawToken.length
    ? { core: rawToken, trailing: "" }
    : { core: rawToken.slice(0, end), trailing: rawToken.slice(end) };
}

function scanQuotedValueEnd(
  source: string,
  start: number,
  quote: QuoteDelimiter,
): { readonly end: number; readonly closed: boolean } {
  const lineEnd = findLineEnd(source, start + quote.length);
  let pos = start + quote.length;
  if (quote === '"' || quote === "'") {
    while (pos < lineEnd) {
      const ch = source[pos]!;
      if (ch === "\\") {
        pos += pos + 1 < lineEnd ? 2 : 1;
        continue;
      }
      if (ch === quote) {
        return { end: pos + 1, closed: true };
      }
      pos += 1;
    }
    return { end: lineEnd, closed: false };
  }
  while (pos < lineEnd) {
    const ch = source[pos]!;
    if (ch === '"') {
      return { end: pos, closed: true };
    }
    if (ch === "\\") {
      let bs = 1;
      while (pos + bs < lineEnd && source[pos + bs] === "\\") {
        bs += 1;
      }
      if (pos + bs < lineEnd && source[pos + bs] === '"') {
        if (bs % 4 === 1) {
          return { end: pos + bs + 1, closed: true };
        }
        if (bs % 2 === 0) {
          return { end: pos + bs, closed: true };
        }
        pos += bs + 1;
        continue;
      }
      pos += bs + (bs % 2 === 1 && pos + bs < lineEnd ? 1 : 0);
      continue;
    }
    pos += 1;
  }
  return { end: lineEnd, closed: false };
}

function scanUnquotedEnd(source: string, start: number, isQueryField: boolean): number {
  let pos = start;
  while (pos < source.length) {
    const ch = source[pos]!;
    if (isWhitespaceChar(ch)) break;
    if (isQueryField) {
      if (ch === "&" || ch === "#") break;
      if (
        (ch === '"' || ch === "'" || (ch === "\\" && source[pos + 1] === '"')) &&
        QUERY_PROPERTY_BOUNDARY.test(source.slice(pos, pos + 12))
      ) {
        break;
      }
    } else if (ch === "," || ch === ";") {
      break;
    }
    if (ch === "\\") {
      if (JSON_ESCAPED_VALUE_STOP.test(source.slice(pos, pos + 6))) {
        break;
      }
      if (pos + 1 < source.length && source[pos + 1] !== "\n" && source[pos + 1] !== "\r") {
        pos += 2;
        continue;
      }
    }
    pos += 1;
  }
  return pos;
}

function isObservabilityCredentialField(prefix: string, isQueryField: boolean): boolean {
  const keyMatch = /^([\w-]+)/u.exec(prefix);
  if (keyMatch === null) return false;
  const key = keyMatch[1]!.toLowerCase();
  return isQueryField
    ? OBSERVABILITY_QUERY_CREDENTIAL_KEYS.has(key)
    : OBSERVABILITY_URL_CREDENTIAL_KEYS.has(key);
}

function isBoundedObservabilityRedactedAt(
  source: string,
  start: number,
  isQueryField: boolean,
): number {
  if (isQueryField || !source.startsWith("<redacted>", start)) return -1;
  const end = start + 10;
  const next = source[end];
  return next === "&" || next === "#" ? end : -1;
}

function redactKeyedValues(
  source: string,
  prefixRegex: RegExp,
  options: {
    readonly isBearerPass: boolean;
  },
): string {
  prefixRegex.lastIndex = 0;
  let out = "";
  let cursor = 0;
  let changed = false;
  let match: RegExpExecArray | null;
  while ((match = prefixRegex.exec(source)) !== null) {
    const matchStart = match.index;
    const valueStart = prefixRegex.lastIndex;
    const isQueryField = !options.isBearerPass && match[1] !== undefined;
    const replacementPrefix = options.isBearerPass ? `${match[1]!} ` : match[2]!;

    const ch = source[valueStart];
    if (ch === undefined) continue;
    const quote: QuoteDelimiter | undefined =
      ch === '"'
        ? '"'
        : ch === "'"
          ? "'"
          : ch === "\\" && source[valueStart + 1] === '"'
            ? '\\"'
            : undefined;

    if (quote !== undefined) {
      const afterOpen = source[valueStart + quote.length];
      if (isAmbiguousQuoteStartChar(afterOpen)) {
        // Fail closed to end-of-line on ambiguous quote boundaries instead of guessing quote parity.
        const lineEnd = findLineEnd(source, valueStart);
        out += `${source.slice(cursor, matchStart)}${replacementPrefix}[REDACTED]`;
        cursor = lineEnd;
        prefixRegex.lastIndex = lineEnd;
        changed = true;
        continue;
      }
      const { end: quoteEnd, closed } = scanQuotedValueEnd(source, valueStart, quote);
      const innerEnd =
        closed && quoteEnd >= valueStart + quote.length * 2 ? quoteEnd - quote.length : quoteEnd;
      if (
        !closed ||
        SWALLOWED_PREFIX_BEFORE_QUOTE.test(source.slice(valueStart + quote.length, innerEnd))
      ) {
        const lineEnd = findLineEnd(source, valueStart);
        out += `${source.slice(cursor, matchStart)}${replacementPrefix}[REDACTED]`;
        cursor = lineEnd;
        prefixRegex.lastIndex = lineEnd;
        changed = true;
        continue;
      }
      const tailEnd = scanUnquotedEnd(source, quoteEnd, isQueryField);
      const nextChar = source[tailEnd] ?? "";
      const preserveTrailingWrappers = !isQueryField || (nextChar !== "&" && nextChar !== "#");
      const tail = source.slice(quoteEnd, tailEnd);
      const { trailing } =
        tail.length === 0
          ? { trailing: "" }
          : trimTrailingTokenDelimiters(tail, {
              preserveTrailingWrappers,
              preserveSentencePunctuation: !isQueryField,
            });
      out += `${source.slice(cursor, matchStart)}${replacementPrefix}[REDACTED]${trailing}`;
      cursor = tailEnd;
      prefixRegex.lastIndex = tailEnd;
      changed = true;
      continue;
    }

    if (
      source.length - valueStart <= 11 &&
      source.endsWith("…") &&
      TRUNCATED_PLACEHOLDER_ELLIPSIS.test(source.slice(valueStart))
    ) {
      prefixRegex.lastIndex = source.length;
      continue;
    }

    if (!options.isBearerPass && isObservabilityCredentialField(replacementPrefix, isQueryField)) {
      const redactedEnd = isBoundedObservabilityRedactedAt(source, valueStart, isQueryField);
      if (redactedEnd !== -1) {
        prefixRegex.lastIndex = redactedEnd;
        continue;
      }
    }

    const valEnd = scanUnquotedEnd(source, valueStart, isQueryField);
    if (valEnd === valueStart) {
      prefixRegex.lastIndex = valueStart;
      continue;
    }
    const rawToken = source.slice(valueStart, valEnd);
    if (SWALLOWED_PREFIX_BEFORE_QUOTE.test(rawToken)) {
      const lineEnd = findLineEnd(source, valEnd);
      out += `${source.slice(cursor, matchStart)}${replacementPrefix}[REDACTED]`;
      cursor = lineEnd;
      prefixRegex.lastIndex = lineEnd;
      changed = true;
      continue;
    }
    const nextChar = source[valEnd] ?? "";
    const preserveTrailingWrappers = !isQueryField || (nextChar !== "&" && nextChar !== "#");
    const isPlaceholder = rawToken.startsWith("[REDACTED]") || rawToken.startsWith("<redacted>");
    const { core, trailing } = trimTrailingTokenDelimiters(rawToken, {
      preserveTrailingWrappers,
      preserveSentencePunctuation: !isQueryField && isPlaceholder,
    });
    if (core.length === 0) {
      prefixRegex.lastIndex = valEnd;
      continue;
    }
    const replacementValue =
      !options.isBearerPass && core === "<redacted>" ? "<redacted>" : "[REDACTED]";
    const replacement = `${replacementPrefix}${replacementValue}${trailing}`;
    const original = source.slice(matchStart, valEnd);
    if (replacement !== original) {
      out += `${source.slice(cursor, matchStart)}${replacement}`;
      cursor = valEnd;
      changed = true;
    }
    prefixRegex.lastIndex = valEnd;
  }
  return changed ? out + source.slice(cursor) : source;
}

function cutAtJsonEscapedDelimiter(value: string): {
  readonly prefix: string;
  readonly cutInsideAuthorityUserinfo: boolean;
} {
  const match = JSON_ESCAPED_QUERY_OR_HASH.exec(value);
  if (match === null) {
    return { prefix: value, cutInsideAuthorityUserinfo: false };
  }
  const firstQueryOrHash = value.search(/[?#]/u);
  if (firstQueryOrHash !== -1 && firstQueryOrHash < match.index) {
    return { prefix: value, cutInsideAuthorityUserinfo: false };
  }
  const prefix = value.slice(0, match.index);
  const tail = value.slice(match.index);
  const inAuthority = !/^https?:\/\/[^/]*\//iu.test(prefix);
  const tailStop = tail.search(TAIL_PATH_OR_QUERY_STOP);
  const tailBeforeSlash = tailStop === -1 ? tail : tail.slice(0, tailStop);
  const matchedAmpersand = match[0].endsWith("26");
  const cutInsideAuthorityUserinfo =
    (inAuthority && (matchedAmpersand || ENCODED_OR_LITERAL_AT.test(tailBeforeSlash))) ||
    (matchedAmpersand && ENCODED_OR_LITERAL_AT.test(tailBeforeSlash));
  return { prefix, cutInsideAuthorityUserinfo };
}

function hasRawBackslashBeforeQueryOrHash(value: string): boolean {
  const queryIndex = value.indexOf("?");
  const hashIndex = value.indexOf("#");
  const end =
    queryIndex === -1
      ? hashIndex === -1
        ? value.length
        : hashIndex
      : hashIndex === -1
        ? queryIndex
        : Math.min(queryIndex, hashIndex);
  return value.slice(0, end).includes("\\");
}

function normalizeLiteralOrPartialScheme(candidate: string): string {
  if (/^https?:\/\//iu.test(candidate)) return candidate;
  const scheme = candidate.slice(0, 5).toLowerCase() === "https" ? "https://" : "http://";
  return candidate.replace(PARTIAL_SCHEME_PREFIX, scheme);
}

function isCleanEncodedSpaceUrlList(
  candidate: string,
  options?: { readonly allowTruncatedLast?: boolean },
): boolean {
  const parts = candidate.split(ENCODED_SPACE_DELIMITER_GLOBAL);
  if (parts.length < 2) return false;
  let seenUrl = false;
  for (let index = 0; index < parts.length; index += 1) {
    const part = parts[index]!;
    if (!/^https?:\/\//iu.test(part)) {
      const isTruncatedTail = options?.allowTruncatedLast === true && index === parts.length - 1;
      const isExactConnector = CLEAN_ENCODED_SPACE_CONNECTORS.includes(
        part as (typeof CLEAN_ENCODED_SPACE_CONNECTORS)[number],
      );
      const isTruncatedConnectorOrScheme =
        isTruncatedTail &&
        (part === "" ||
          CLEAN_ENCODED_SPACE_CONNECTORS.some((word) => word.startsWith(part)) ||
          "https://".startsWith(part.toLowerCase()) ||
          "http://".startsWith(part.toLowerCase()));
      if (!isExactConnector && !isTruncatedConnectorOrScheme) {
        return false;
      }
      continue;
    }
    seenUrl = true;
    if (
      options?.allowTruncatedLast === true &&
      index === parts.length - 1 &&
      CLEAN_TRUNCATED_URL_PREFIX.test(part)
    ) {
      continue;
    }
    try {
      const partUrl = new URL(part);
      if (
        partUrl.username !== "" ||
        partUrl.password !== "" ||
        partUrl.search !== "" ||
        partUrl.hash !== "" ||
        EMBEDDED_URL_IN_PATH.test(partUrl.pathname) ||
        ENCODED_QUERY_OR_HASH.test(partUrl.pathname) ||
        hasRawBackslashBeforeQueryOrHash(part)
      ) {
        return false;
      }
    } catch {
      return false;
    }
  }
  return seenUrl;
}

function redactLiteralUrl(match: string): string {
  if (match.endsWith("…")) {
    const beforeEllipsis = match.slice(0, -1);
    if (
      CLEAN_TRUNCATED_URL_PREFIX.test(beforeEllipsis) ||
      isCleanEncodedSpaceUrlList(beforeEllipsis, { allowTruncatedLast: true })
    ) {
      return match;
    }
  }
  const { candidate: rawMatched, trailing } = splitTrailingUrlPunctuation(match);
  const rawCandidate = normalizeLiteralOrPartialScheme(rawMatched);
  const { prefix: candidate, cutInsideAuthorityUserinfo } = cutAtJsonEscapedDelimiter(rawCandidate);
  if (cutInsideAuthorityUserinfo || hasRawBackslashBeforeQueryOrHash(candidate)) {
    return `[REDACTED_URL]${trailing}`;
  }
  try {
    const url = new URL(candidate);
    const hasOuterCredentialsOrQuery =
      url.username !== "" ||
      url.password !== "" ||
      url.search !== "" ||
      url.hash !== "" ||
      ENCODED_QUERY_OR_HASH.test(url.pathname);
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";
    if (EMBEDDED_URL_IN_PATH.test(url.pathname)) {
      if (
        !hasOuterCredentialsOrQuery &&
        candidate === rawCandidate &&
        isCleanEncodedSpaceUrlList(candidate)
      ) {
        return `${candidate}${trailing}`;
      }
      return `[REDACTED_URL]${trailing}`;
    }
    const encodedDelim = ENCODED_QUERY_OR_HASH.exec(url.pathname);
    if (encodedDelim !== null) {
      url.pathname = url.pathname.slice(0, encodedDelim.index);
    }
    return `${url.toString()}${trailing}`;
  } catch {
    return `[REDACTED_URL]${trailing}`;
  }
}

function findLastEncodedOrLiteralAt(value: string): number {
  ENCODED_OR_LITERAL_AT_GLOBAL.lastIndex = 0;
  let lastIndex = -1;
  let match: RegExpExecArray | null;
  while ((match = ENCODED_OR_LITERAL_AT_GLOBAL.exec(value)) !== null) {
    lastIndex = match.index;
  }
  return lastIndex;
}

function redactEncodedSchemeUrl(match: string): string {
  const { candidate, trailing } = splitTrailingUrlPunctuation(match);
  if (!UNIFORMLY_ENCODED_SCHEME_PREFIX.test(candidate)) {
    return `[REDACTED_URL]${trailing}`;
  }
  let decoded = candidate;
  for (let i = 0; i < 4 && !/^https?:\/\//iu.test(decoded) && decoded.includes("%"); i += 1) {
    const singleSchemeMatch = SINGLE_ENCODED_SCHEME_PREFIX.exec(decoded);
    if (singleSchemeMatch !== null) {
      const afterScheme = decoded.slice(singleSchemeMatch[0].length);
      const lastAt = findLastEncodedOrLiteralAt(afterScheme);
      if (lastAt !== -1 && SINGLE_LAYER_AUTHORITY_DELIM.test(afterScheme.slice(0, lastAt))) {
        return `[REDACTED_URL]${trailing}`;
      }
    }
    try {
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
    } catch {
      return `[REDACTED_URL]${trailing}`;
    }
  }
  const decodedCut = cutAtJsonEscapedDelimiter(decoded);
  if (
    decodedCut.cutInsideAuthorityUserinfo ||
    hasRawBackslashBeforeQueryOrHash(decodedCut.prefix)
  ) {
    return `[REDACTED_URL]${trailing}`;
  }
  decoded = decodedCut.prefix;
  try {
    const url = new URL(decoded);
    if (
      /[\s<>"']/u.test(decoded) ||
      EMBEDDED_URL_IN_PATH.test(url.pathname) ||
      ENCODED_QUERY_OR_HASH.test(url.pathname)
    ) {
      return `[REDACTED_URL]${trailing}`;
    }
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";
    return `${url.toString()}${trailing}`;
  } catch {
    return `[REDACTED_URL]${trailing}`;
  }
}

function replaceUnsafeControlCharacters(value: string): string {
  const sanitized: Array<string> = [];
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    sanitized.push(
      codePoint <= 0x08 ||
        (codePoint >= 0x0b && codePoint <= 0x0c) ||
        (codePoint >= 0x0e && codePoint <= 0x1f) ||
        codePoint === 0x7f
        ? " "
        : character,
    );
  }
  return sanitized.join("");
}

function applySecretFieldRedactions(value: string): string {
  const afterJson = value
    .replace(ESCAPED_JSON_SECRET_FIELD, "$1[REDACTED]$2")
    .replace(DOUBLE_QUOTED_SECRET_FIELD, "$1[REDACTED]$2")
    .replace(SINGLE_QUOTED_SECRET_FIELD, "$1[REDACTED]$2");
  const afterBearer = redactKeyedValues(afterJson, BEARER_OR_BASIC_PREFIX, { isBearerPass: true });
  const afterFields = redactKeyedValues(afterBearer, SECRET_FIELD_PREFIX, { isBearerPass: false });
  return afterFields.replace(SK_SECRET_PATTERN, "[REDACTED]");
}

function redactProviderFailureTextOnce(value: string): string {
  const urlsRedacted = value
    .replace(ENCODED_SCHEME_URL_PATTERN, redactEncodedSchemeUrl)
    .replace(LITERAL_URL_PATTERN, redactLiteralUrl);
  const base = applySecretFieldRedactions(urlsRedacted);
  // Run after URL stripping so `<redacted>` cannot split a literal URL before its fragment.
  return redactUrlCredentials(base).trim();
}

function redactProviderFailureText(value: string): string {
  let current = replaceUnsafeControlCharacters(value).trim();
  for (let pass = 0; pass < 3; pass += 1) {
    const next = redactProviderFailureTextOnce(current);
    if (next === current) {
      break;
    }
    current = next;
  }
  return current;
}

function boundedText(value: string, maxLength: number): string {
  const preRedactionLimit = Math.max(
    maxLength * MAX_REDACTION_INPUT_MULTIPLIER,
    MIN_REDACTION_INPUT_CAP,
  );
  let capped = value;
  if (value.length > preRedactionLimit) {
    let cut = preRedactionLimit;
    const lastCodeUnit = value.charCodeAt(cut - 1);
    if (lastCodeUnit >= 0xd800 && lastCodeUnit <= 0xdbff) {
      cut -= 1;
    }
    capped = value
      .slice(0, cut)
      .replace(TRAILING_TRUNCATED_SK_SECRET, "[REDACTED]")
      .replace(TRAILING_TRUNCATED_URL_AUTHORITY, "[REDACTED_URL]");
  }
  const redacted = redactProviderFailureText(capped);
  if (redacted.length <= maxLength) return redacted;
  let end = Math.max(0, maxLength - 1);
  const finalCodeUnit = redacted.charCodeAt(end - 1);
  if (finalCodeUnit >= 0xd800 && finalCodeUnit <= 0xdbff) {
    end -= 1;
  }
  return `${redacted.slice(0, end)}…`;
}

export function sanitizeProviderFailureMessage(value: string): string {
  return boundedText(value, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
}

const isGoogleAccountVerificationUrl = Schema.is(GoogleAccountVerificationUrl);

function providerFailureTitle(failure: OrchestrationV2ProviderFailure): string {
  if (failure.code === "VALIDATION_REQUIRED") return "Verification required";
  return failure.class === "usage_limit" ? "Usage limit reached" : "Provider error";
}

export function makeProviderFailure(input: {
  readonly cause?: unknown;
  readonly message?: string | undefined;
  readonly code?: string | null | undefined;
  readonly class?: OrchestrationV2ProviderFailureClass;
  readonly retryable?: boolean | null;
  readonly resetAt?: string | null;
  readonly verificationUrl?: string | undefined;
}): OrchestrationV2ProviderFailure {
  const curated = curatedCauseMessages(input.cause);
  const requestTooLarge =
    input.message !== undefined &&
    (REQUEST_TOO_LARGE_STATUS.test(input.message) || REQUEST_TOO_LARGE_REASON.test(input.message));
  const rawMessage = requestTooLarge
    ? REQUEST_TOO_LARGE_MESSAGE
    : (input.message ?? curated.known ?? curated.category ?? DEFAULT_PROVIDER_FAILURE_MESSAGE);
  const message = sanitizeProviderFailureMessage(rawMessage);
  const rawCode = input.code ?? stringField(input.cause, "code") ?? null;
  const code =
    rawCode === null ? null : boundedText(rawCode, MAX_PROVIDER_FAILURE_CODE_LENGTH) || null;
  // Validated Google account verification links require query params (e.g. `?plt=...`), so skip URL redaction.
  const verificationUrl =
    input.verificationUrl !== undefined && isGoogleAccountVerificationUrl(input.verificationUrl)
      ? input.verificationUrl
      : undefined;

  return {
    class: input.class ?? "unknown",
    message: message || DEFAULT_PROVIDER_FAILURE_MESSAGE,
    code,
    retryable: requestTooLarge ? false : (input.retryable ?? null),
    ...(input.class === "usage_limit" &&
    input.resetAt != null &&
    Number.isFinite(Date.parse(input.resetAt))
      ? { resetAt: DateTime.formatIso(DateTime.makeUnsafe(input.resetAt)) }
      : {}),
    ...(verificationUrl === undefined ? {} : { verificationUrl }),
  };
}

export function makeProviderFailureTurnItem(input: {
  readonly driver: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly nodeId: NodeId | null;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  readonly itemOrdinal: number;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly retry?: OrchestrationV2ProviderRetry;
  readonly retryStartedAt?: DateTime.Utc;
  readonly occurredAt: DateTime.Utc;
}): Extract<OrchestrationV2TurnItem, { readonly type: "error" }> {
  return {
    id: IdAllocator.derive.turnItemFromProviderItem({
      driver: input.driver,
      nativeItemId: `terminal-failure:${input.providerTurnId}`,
    }),
    threadId: input.threadId,
    runId: input.runId,
    nodeId: input.nodeId,
    providerThreadId: input.providerThreadId,
    providerTurnId: input.providerTurnId,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: input.itemOrdinal,
    status: "failed",
    title: providerFailureTitle(input.failure),
    startedAt: input.retryStartedAt ?? input.occurredAt,
    completedAt: input.occurredAt,
    updatedAt: input.occurredAt,
    type: "error",
    failure: input.failure,
    ...(input.retry === undefined ? {} : { retry: input.retry }),
  };
}

/**
 * Longest a turn may stay in provider retries before the adapter stops it as
 * failed. Codex waits for the network without limit and a hung Claude request
 * can take many minutes per attempt, so counting attempts alone never ends them.
 */
export const PROVIDER_RETRY_BUDGET_MS = 10 * 60 * 1000;

export function makeProviderRetryTurnItem(input: {
  readonly driver: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly nodeId: NodeId | null;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  readonly itemOrdinal: number;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly retry: OrchestrationV2ProviderRetry;
  readonly status: Extract<
    OrchestrationV2TurnItem["status"],
    "running" | "completed" | "failed" | "interrupted" | "cancelled"
  >;
  readonly startedAt: DateTime.Utc;
  readonly updatedAt: DateTime.Utc;
}): Extract<OrchestrationV2TurnItem, { readonly type: "error" }> {
  const completed = input.status !== "running";
  let title = "Provider retry";
  if (input.status === "completed") {
    title = "Provider recovered";
  } else if (input.status === "failed") {
    title = providerFailureTitle(input.failure);
  } else if (input.status === "interrupted" || input.status === "cancelled") {
    title = "Provider retry stopped";
  }
  return {
    id: IdAllocator.derive.turnItemFromProviderItem({
      driver: input.driver,
      nativeItemId: `terminal-failure:${input.providerTurnId}`,
    }),
    threadId: input.threadId,
    runId: input.runId,
    nodeId: input.nodeId,
    providerThreadId: input.providerThreadId,
    providerTurnId: input.providerTurnId,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: input.itemOrdinal,
    status: input.status,
    title,
    startedAt: input.startedAt,
    completedAt: completed ? input.updatedAt : null,
    updatedAt: input.updatedAt,
    type: "error",
    failure: input.failure,
    retry: input.retry,
  };
}
