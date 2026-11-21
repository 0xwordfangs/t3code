import { GoogleAccountVerificationUrl } from "@t3tools/contracts";
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
const MAX_VERIFICATION_TEXT_CHARS = 32_768;
const MAX_CANDIDATE_JSON_MATCHES = 8;

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
      (type === undefined || type.endsWith("google.rpc.ErrorInfo"))
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
  const message =
    validationMessage ?? nonEmptyString(error.message) ?? ANTIGRAVITY_VERIFICATION_REQUIRED_MESSAGE;
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
): AntigravityVerificationRequired | undefined {
  const bounded =
    text.length > MAX_VERIFICATION_TEXT_CHARS ? text.slice(0, MAX_VERIFICATION_TEXT_CHARS) : text;
  if (!VALIDATION_REQUIRED_WORD.test(bounded)) return undefined;
  let fallback: AntigravityVerificationRequired | undefined;
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
    try {
      const parsed = extractAntigravityVerificationRequired(JSON.parse(candidate.slice));
      if (parsed === undefined) continue;
      if (parsed.verificationUrl !== undefined) return parsed;
      fallback ??= parsed;
    } catch {
      continue;
    }
  }
  return fallback ?? { message: ANTIGRAVITY_TURN_VERIFICATION_REQUIRED_MESSAGE };
}
