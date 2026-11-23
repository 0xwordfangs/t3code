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
  });
});
