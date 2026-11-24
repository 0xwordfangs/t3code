import { assert, describe, it } from "@effect/vitest";

import { extractAntigravityVerificationRequired } from "./antigravityVerification.ts";

describe("antigravityVerification", () => {
  it("extracts verification details in linear time when validation_error_message carries an oversized punctuation URL", () => {
    const payload = {
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: {
              validation_url: "https://accounts.google.com/v3/signin/continue?plt=abc",
              validation_error_message: `https://a.invalid/${".".repeat(128_000)}x`,
            },
          },
        ],
      },
    };

    const extracted = extractAntigravityVerificationRequired(payload);

    assert.isDefined(extracted);
    assert.equal(
      extracted?.verificationUrl,
      "https://accounts.google.com/v3/signin/continue?plt=abc",
    );
    assert.isAtMost(extracted?.message.length ?? 0, 4_096);
  });

  it("caps oversized error.message without splitting UTF-16 surrogate pairs and still redacts trailing secrets", () => {
    const payload = {
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        message: `https://a.invalid/?q=${"a".repeat(32_720)} {"token":"TRUNCATED_VERIFICATION_SECRET_VALUE"}`,
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: {
              validation_url: "https://accounts.google.com/v3/signin/continue?plt=abc",
            },
          },
        ],
      },
    };

    const extracted = extractAntigravityVerificationRequired(payload);
    assert.isDefined(extracted);
    assert.notInclude(extracted?.message ?? "", "TRUNCATED_VERIFICATION_SECRET");
    assert.include(extracted?.message ?? "", '{"token":"[REDACTED]');

    // A UTF-16 surrogate pair straddling the 32 KiB pre-redaction cap must not leave a lone high surrogate.
    const emojiBoundaryPrefix = `https://a.invalid/?q=${"a".repeat(32_740)} bbbbb`;
    assert.equal(emojiBoundaryPrefix.length, 32_767);
    const emojiPayload = {
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        message: `${emojiBoundaryPrefix}🚀tail`,
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: {
              validation_url: "https://accounts.google.com/v3/signin/continue?plt=abc",
            },
          },
        ],
      },
    };
    const emojiExtracted = extractAntigravityVerificationRequired(emojiPayload);
    assert.isDefined(emojiExtracted);
    assert.equal(emojiExtracted?.message, "https://a.invalid/ bbbbb");
    assert.notMatch(emojiExtracted?.message ?? "", /[\uD800-\uDFFF]/u);
  });

  it("redacts boundary-straddling sk- keys plus encoded URL and escaped JSON secrets in validation_error_message", () => {
    const prefix = [
      "continue%3Dhttps://accounts.google.com/v3/signin/continue?plt=SECRET_CONT_PLT",
      "https%3A//example.test/auth?token=SECRET_PART_TOK",
      '{\\"token\\":\\"SECRET_ESC_JSON\\",\\"reason\\":\\"verify\\"}',
      "https://example.test/p\\u003fcode=SECRET_JSON_Q",
    ].join(" ");
    const fillerUrl = `https://a.invalid/?q=${"a".repeat(32_768 - prefix.length - 24 - 11)}`;
    const validationErrorMessage = `${prefix} ${fillerUrl} sk-1234567890abcdef1234567890`;
    assert.isAbove(validationErrorMessage.length, 32_768);

    const payload = {
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: {
              validation_url: "https://accounts.google.com/v3/signin/continue?plt=abc",
              validation_error_message: validationErrorMessage,
            },
          },
        ],
      },
    };

    const extracted = extractAntigravityVerificationRequired(payload);
    assert.isDefined(extracted);
    assert.equal(
      extracted?.verificationUrl,
      "https://accounts.google.com/v3/signin/continue?plt=abc",
    );
    const message = extracted?.message ?? "";
    assert.notInclude(message, "SECRET_CONT_PLT");
    assert.notInclude(message, "SECRET_PART_TOK");
    assert.notInclude(message, "SECRET_ESC_JSON");
    assert.notInclude(message, "SECRET_JSON_Q");
    assert.notInclude(message, "sk-1234");
    assert.include(message, "continue%3Dhttps://accounts.google.com/v3/signin/continue");
    assert.include(message, "https://example.test/auth");
    assert.include(message, '{\\"token\\":\\"[REDACTED]\\",\\"reason\\":\\"verify\\"}');
    assert.include(message, "https://example.test/p");
    assert.include(message, "https://a.invalid/ [REDACTED]");
  });
});
