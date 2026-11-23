import { GoogleAccountVerificationUrl } from "@t3tools/contracts";
import { sanitizeProviderFailureMessage } from "@t3tools/provider-core/server/failure";
import * as Schema from "effect/Schema";

export const ANTIGRAVITY_VERIFICATION_REQUIRED_CODE = "VALIDATION_REQUIRED";
export const ANTIGRAVITY_VERIFICATION_REQUIRED_MESSAGE =
  "Verify your Google account to use Antigravity.";
const ANTIGRAVITY_TURN_VERIFICATION_REQUIRED_MESSAGE =
  "Verify your Google account to continue using Antigravity.";

export interface AntigravityVerificationRequired {
  readonly message: string;
  readonly verificationUrl?: GoogleAccountVerificationUrl;
}

const isGoogleAccountVerificationUrl = Schema.is(GoogleAccountVerificationUrl);
const EMBEDDED_ERROR_OBJECT_START = /\{\s*"error"\s*:/gu;
const VALIDATION_REQUIRED_WORD = /\bVALIDATION_REQUIRED\b/;
const OUTER_HTTP_STATUS =
  /\b(?:Error|HTTP(?:\/\d(?:\.\d)?)?|status(?:\s*code)?)(?:\s*[:=]\s*|\s+)([1-5]\d{2})\b/gi;
const TRAILING_API_STATUS = /,\s*Status:\s*([A-Z_]+)\b/g;
const OUTER_VERIFICATION_FRAMING =
  /(?:\bError\s+403\b|\bHTTP(?:\/\d(?:\.\d)?)?\s*403\b|\bcode(?:\s*[:=]\s*|\s+)403\b|\bPERMISSION_DENIED\b)/i;
const MAX_VERIFICATION_TEXT_CHARS = 32_768;
const MAX_CANDIDATE_JSON_MATCHES = 8;

interface EmbeddedErrorCandidate {
  readonly startIndex: number;
  readonly endIndex: number;
  readonly slice: string;
}

function textBeforeFirstJsonObject(text: string): string {
  const braceIndex = text.indexOf("{");
  return braceIndex === -1 ? text : text.slice(0, braceIndex);
}

function findEmbeddedErrorObjects(bounded: string): ReadonlyArray<EmbeddedErrorCandidate> {
  const candidates: Array<EmbeddedErrorCandidate> = [];
  let inspected = 0;
  let skipUntil = 0;
  for (const match of bounded.matchAll(EMBEDDED_ERROR_OBJECT_START)) {
    const startIndex = match.index;
    if (startIndex === undefined || startIndex < skipUntil) continue;
    if (inspected >= MAX_CANDIDATE_JSON_MATCHES) break;
    inspected += 1;
    const candidate = sliceBalancedJsonObject(bounded, startIndex);
    if (candidate === undefined) continue;
    skipUntil = candidate.endIndex;
    candidates.push({ startIndex, endIndex: candidate.endIndex, slice: candidate.slice });
  }
  return candidates;
}

function envelopeOutsideRanges(
  text: string,
  ranges: ReadonlyArray<{ readonly startIndex: number; readonly endIndex: number }>,
): string {
  if (ranges.length === 0) return text;
  let result = "";
  let cursor = 0;
  for (const range of ranges) {
    if (range.startIndex > cursor) {
      result += text.slice(cursor, range.startIndex);
    }
    result += "\n";
    cursor = Math.max(cursor, range.endIndex);
  }
  if (cursor < text.length) {
    result += text.slice(cursor);
  }
  return result;
}

function hasConflictingStatusInEnvelope(envelope: string): boolean {
  for (const match of envelope.matchAll(OUTER_HTTP_STATUS)) {
    if (match[1] !== "403") return true;
  }
  for (const match of envelope.matchAll(TRAILING_API_STATUS)) {
    if (match[1] !== "PERMISSION_DENIED" && match[1] !== ANTIGRAVITY_VERIFICATION_REQUIRED_CODE) {
      return true;
    }
  }
  return false;
}

export function hasConflictingNon403ApiError(text: string): boolean {
  const bounded =
    text.length > MAX_VERIFICATION_TEXT_CHARS ? text.slice(0, MAX_VERIFICATION_TEXT_CHARS) : text;
  const envelope = envelopeOutsideRanges(bounded, findEmbeddedErrorObjects(bounded));
  return hasConflictingStatusInEnvelope(envelope);
}

export function hasOuterAntigravityVerificationFraming(text: string): boolean {
  const bounded =
    text.length > MAX_VERIFICATION_TEXT_CHARS ? text.slice(0, MAX_VERIFICATION_TEXT_CHARS) : text;
  const envelope = envelopeOutsideRanges(bounded, findEmbeddedErrorObjects(bounded));
  return OUTER_VERIFICATION_FRAMING.test(envelope);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function parseGoogleAccountVerificationUrl(raw: unknown): GoogleAccountVerificationUrl | undefined {
  if (typeof raw !== "string") return undefined;
  const trimmed = raw.trim();
  return isGoogleAccountVerificationUrl(trimmed) ? trimmed : undefined;
}

export function extractAntigravityVerificationRequired(
  payload: unknown,
): AntigravityVerificationRequired | undefined {
  if (!isRecord(payload) || !isRecord(payload.error)) return undefined;
  const error = payload.error;
  const hasValidCode = error.code === undefined || error.code === 403;
  const hasValidStatus =
    error.status === undefined ||
    error.status === "PERMISSION_DENIED" ||
    error.status === ANTIGRAVITY_VERIFICATION_REQUIRED_CODE;
  const hasVerificationFraming = error.code === 403 || error.status === "PERMISSION_DENIED";
  if (!hasValidCode || !hasValidStatus || !hasVerificationFraming) return undefined;

  const details = Array.isArray(error.details) ? error.details : [];
  let matched = false;
  let validationUrl: GoogleAccountVerificationUrl | undefined;
  let helpUrl: GoogleAccountVerificationUrl | undefined;
  let validationMessage: string | undefined;

  for (const detail of details) {
    if (!isRecord(detail)) continue;
    const type = typeof detail["@type"] === "string" ? detail["@type"] : undefined;
    if (
      detail.reason === ANTIGRAVITY_VERIFICATION_REQUIRED_CODE &&
      type !== undefined &&
      type.endsWith("google.rpc.ErrorInfo")
    ) {
      matched = true;
      if (isRecord(detail.metadata)) {
        validationUrl ??= parseGoogleAccountVerificationUrl(detail.metadata.validation_url);
        validationMessage ??= nonEmptyString(detail.metadata.validation_error_message);
      }
      continue;
    }
    if (type !== undefined && type.endsWith("google.rpc.Help") && Array.isArray(detail.links)) {
      for (const link of detail.links) {
        if (!isRecord(link)) continue;
        helpUrl ??= parseGoogleAccountVerificationUrl(link.url);
      }
    }
  }

  if (!matched) return undefined;
  const verificationUrl = validationUrl ?? helpUrl;
  const rawMessage = validationMessage ?? nonEmptyString(error.message);
  const sanitizedMessage =
    rawMessage !== undefined ? sanitizeProviderFailureMessage(rawMessage) : "";
  const message =
    sanitizedMessage.length > 0 ? sanitizedMessage : ANTIGRAVITY_VERIFICATION_REQUIRED_MESSAGE;
  return {
    message,
    ...(verificationUrl === undefined ? {} : { verificationUrl }),
  };
}

function sliceBalancedJsonObject(
  text: string,
  startIndex: number,
): { readonly slice: string; readonly endIndex: number } | undefined {
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = startIndex; index < text.length; index++) {
    const ch = text[index];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }
    if (ch === '"') {
      inString = true;
    } else if (ch === "{") {
      depth += 1;
    } else if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        const endIndex = index + 1;
        return { slice: text.slice(startIndex, endIndex), endIndex };
      }
    }
  }
  return undefined;
}

export function extractAntigravityVerificationFromText(
  text: string,
  options?: { readonly skipConflictCheck?: boolean },
): AntigravityVerificationRequired | undefined {
  const bounded =
    text.length > MAX_VERIFICATION_TEXT_CHARS ? text.slice(0, MAX_VERIFICATION_TEXT_CHARS) : text;
  if (!VALIDATION_REQUIRED_WORD.test(bounded)) {
    return undefined;
  }
  const candidates = findEmbeddedErrorObjects(bounded);
  const envelope = envelopeOutsideRanges(bounded, candidates);
  if (options?.skipConflictCheck !== true && hasConflictingStatusInEnvelope(envelope)) {
    return undefined;
  }
  const hasValidEnvelopeForJson =
    envelope.trim().length === 0 || OUTER_VERIFICATION_FRAMING.test(envelope);
  let fallback: AntigravityVerificationRequired | undefined;
  if (hasValidEnvelopeForJson) {
    for (const candidate of candidates) {
      try {
        const parsed = extractAntigravityVerificationRequired(JSON.parse(candidate.slice));
        if (parsed === undefined) continue;
        if (parsed.verificationUrl !== undefined) return parsed;
        fallback ??= parsed;
      } catch {
        continue;
      }
    }
  }
  if (fallback !== undefined) return fallback;
  if (envelope.trimStart().startsWith("{")) return undefined;
  const outerText = textBeforeFirstJsonObject(envelope);
  if (!VALIDATION_REQUIRED_WORD.test(outerText) || !OUTER_VERIFICATION_FRAMING.test(outerText)) {
    return undefined;
  }
  return { message: ANTIGRAVITY_TURN_VERIFICATION_REQUIRED_MESSAGE };
}
