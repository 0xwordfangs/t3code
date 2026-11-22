import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as EffectAcpErrors from "effect-acp/errors";
import {
  GoogleAccountVerificationUrl,
  type OrchestrationV2ProviderFailure,
} from "@t3tools/contracts";

import {
  extractAntigravityVerificationFromText,
  extractAntigravityVerificationRequired,
} from "../antigravityVerification.ts";
import {
  classifyAntigravityPromptFailure,
  watchAntigravityPromptOutcome,
} from "./AntigravityTurnOutcome.ts";

const USER_INPUT = 14;
const PLANNER_RESPONSE = 15;
const TOOL = 8;
const ERROR_MESSAGE = 17;

// Error-step payloads trimmed from Antigravity 1.4.5 against a local Gemini endpoint.
const errorPayload = (error: string) =>
  Buffer.concat([
    Buffer.from([0x0a, 0x24]),
    Buffer.from(
      `Agent execution terminated due to error.\u0012agent executor error: ${error}: ${error}\n(1) attached stack trace\n  -- stack trace:`,
    ),
  ]);
// An unparsable reset timestamp must not hide the reset delay, nor a retry delay outrank it.
const QUOTA_ERROR = errorPayload(
  "Error 429, Message: Quota exhausted for test model., Status: RESOURCE_EXHAUSTED, Details: [map[@type:type.googleapis.com/google.rpc.RetryInfo retryDelay:42s] map[@type:type.googleapis.com/google.rpc.ErrorInfo domain:googleapis.com metadata:map[quotaResetDelay:3600s quotaResetTimeStamp:soon] reason:QUOTA_EXHAUSTED]]",
);
const RATE_LIMIT_ERROR = errorPayload(
  "Error 429, Message: Resource has been exhausted (e.g. check quota)., Status: RESOURCE_EXHAUSTED, Details: [map[@type:type.googleapis.com/google.rpc.ErrorInfo domain:cloudcode-pa.googleapis.com reason:RATE_LIMIT_EXCEEDED] map[@type:type.googleapis.com/google.rpc.RetryInfo retryDelay:42s]]",
);
const INVALID_ARGUMENT_ERROR = errorPayload(
  "Error 400, Message: Request contains an invalid argument., Status: INVALID_ARGUMENT, Details: []",
);
// Vertex Claude rejects a resumed history whose saved thinking block lost its signature.
const MISSING_SIGNATURE_ERROR = errorPayload(
  'Error 400, Message: {"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0.thinking.signature: Field required"},"request_id":"req_vrtx_test"}, Status: INVALID_ARGUMENT, Details: []',
);
const INVALID_SIGNATURE_ERROR = errorPayload(
  'Error 400, Message: {"type":"error","error":{"type":"invalid_request_error","message":"messages.3.content.0: Invalid `signature` in `thinking` block"}}, Status: INVALID_ARGUMENT, Details: []',
);
// The model's reply quoting the agent's quota notice is an ordinary planner response.
const QUOTED_NOTICE = Buffer.from(
  "Usage Limit Reached\n\nYou have reached your current quota for this period. Your limit will reset in 1 hour.",
);
const reusable = (failure: OrchestrationV2ProviderFailure) =>
  ({ failure, threadDisposition: "reusable" }) as const;

describe("watchAntigravityPromptOutcome", () => {
  it.effect("fails only turns whose own steps end in an upstream error", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const profileDirectory = yield* fs.makeTempDirectoryScoped();
      const sessionId = "e6aba774-5472-4277-b6ee-27b2e05570a7";
      const conversations = path.join(profileDirectory, "antigravity-acp", "conversations");
      yield* fs.makeDirectory(conversations, { recursive: true });
      let nextIndex = 0;
      const appendSteps = (steps: ReadonlyArray<readonly [number, Uint8Array]>) => {
        const database = new NodeSqlite.DatabaseSync(path.join(conversations, `${sessionId}.db`));
        try {
          database.exec(
            "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)",
          );
          const insert = database.prepare("INSERT INTO steps VALUES (?, ?, ?)");
          for (const [type, payload] of steps) insert.run(nextIndex++, type, payload);
        } finally {
          database.close();
        }
      };
      const turn = (
        steps: ReadonlyArray<readonly [number, Uint8Array]>,
        result: EffectAcpSchema.PromptResponse = { stopReason: "end_turn" },
      ) =>
        Effect.gen(function* () {
          const check = yield* watchAntigravityPromptOutcome(profileDirectory, sessionId);
          appendSteps(steps);
          return yield* check(result);
        });
      const input = Buffer.from("Run the fixture.");
      const reply = Buffer.from("FIXTURE_OK");

      // The first prompt creates the session log.
      assert.isUndefined(
        yield* turn([
          [USER_INPUT, input],
          [PLANNER_RESPONSE, reply],
        ]),
      );
      assert.deepEqual(
        yield* turn([
          [USER_INPUT, input],
          [ERROR_MESSAGE, QUOTA_ERROR],
        ]),
        reusable({
          class: "usage_limit",
          message: "You have reached your Antigravity quota for this period.",
          code: "RESOURCE_EXHAUSTED",
          retryable: true,
          resetAt: "1970-01-01T01:00:00.000Z",
        }),
      );
      // The earlier error step is outside this turn.
      assert.isUndefined(
        yield* turn(
          [
            [USER_INPUT, input],
            [PLANNER_RESPONSE, QUOTED_NOTICE],
          ],
          {
            stopReason: "end_turn",
            usage: { inputTokens: 42, outputTokens: 3, totalTokens: 54 },
          },
        ),
      );
      assert.deepEqual(
        yield* turn([
          [USER_INPUT, input],
          [ERROR_MESSAGE, RATE_LIMIT_ERROR],
        ]),
        reusable({
          class: "usage_limit",
          message: "Resource has been exhausted (e.g. check quota).",
          code: "RESOURCE_EXHAUSTED",
          retryable: true,
          resetAt: "1970-01-01T00:00:42.000Z",
        }),
      );
      assert.deepEqual(
        yield* turn([
          [USER_INPUT, input],
          [ERROR_MESSAGE, INVALID_ARGUMENT_ERROR],
        ]),
        reusable({
          class: "provider_error",
          message: "Request contains an invalid argument.",
          code: "INVALID_ARGUMENT",
          retryable: false,
        }),
      );
      // A saved thinking block without a valid signature fails every later prompt.
      for (const error of [MISSING_SIGNATURE_ERROR, INVALID_SIGNATURE_ERROR]) {
        const outcome = yield* turn([
          [USER_INPUT, input],
          [ERROR_MESSAGE, error],
        ]);
        assert.equal(outcome?.failure.code, "INVALID_THINKING_SIGNATURE");
        assert.equal(outcome?.threadDisposition, "broken");
      }
      // A completed tool keeps its usage, and the turn still fails.
      assert.equal(
        (yield* turn(
          [
            [USER_INPUT, input],
            [PLANNER_RESPONSE, reply],
            [TOOL, reply],
            [ERROR_MESSAGE, QUOTA_ERROR],
          ],
          {
            stopReason: "end_turn",
            usage: { inputTokens: 40, outputTokens: 5, totalTokens: 45 },
          },
        ))?.failure.class,
        "usage_limit",
      );
      assert.isUndefined(
        yield* turn([
          [USER_INPUT, input],
          [PLANNER_RESPONSE, reply],
        ]),
      );
      // The agent maps the SDK's QUOTA_EXHAUSTED stop reason to `refusal`.
      assert.deepEqual(
        yield* turn([], { stopReason: "refusal" }),
        reusable({
          class: "usage_limit",
          message: "You have reached your Antigravity quota for this period.",
          code: "QUOTA_EXHAUSTED",
          retryable: true,
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reads only this session's own unlinked log, and keeps a typed refusal", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const profileDirectory = yield* fs.makeTempDirectoryScoped();
      const conversations = path.join(profileDirectory, "antigravity-acp", "conversations");
      yield* fs.makeDirectory(conversations, { recursive: true });
      const own = "e6aba774-5472-4277-b6ee-27b2e05570a7";
      const peer = "0d9c2b4e-1111-4222-8333-944455556666";
      const append = (sessionId: string, type: number, payload: Uint8Array) => {
        const database = new NodeSqlite.DatabaseSync(path.join(conversations, `${sessionId}.db`));
        try {
          database.exec(
            "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)",
          );
          database
            .prepare("INSERT INTO steps (step_type, step_payload) VALUES (?, ?)")
            .run(type, payload);
        } finally {
          database.close();
        }
      };

      // own.db links to a peer session; the peer's new error step is not this turn's.
      append(peer, USER_INPUT, Buffer.from("peer"));
      yield* fs.symlink(
        path.join(conversations, `${peer}.db`),
        path.join(conversations, `${own}.db`),
      );
      const linked = yield* watchAntigravityPromptOutcome(profileDirectory, own);
      append(peer, ERROR_MESSAGE, QUOTA_ERROR);
      assert.isUndefined(yield* linked({ stopReason: "end_turn" }));
      assert.equal((yield* linked({ stopReason: "refusal" }))?.failure.class, "usage_limit");

      // An unreadable log gives no evidence, but the agent's typed refusal still fails the turn.
      yield* fs.remove(path.join(conversations, `${own}.db`));
      yield* fs.writeFileString(path.join(conversations, `${own}.db`), "not a database");
      const corrupt = yield* watchAntigravityPromptOutcome(profileDirectory, own);
      assert.isUndefined(yield* corrupt({ stopReason: "end_turn" }));
      assert.deepEqual(
        yield* corrupt({ stopReason: "refusal" }),
        reusable({
          class: "usage_limit",
          message: "You have reached your Antigravity quota for this period.",
          code: "QUOTA_EXHAUSTED",
          retryable: true,
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "classifies Individual quota reached as usage_limit and keeps VALIDATION_REQUIRED permanent",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profileDirectory = yield* fs.makeTempDirectoryScoped();
        const conversations = path.join(profileDirectory, "antigravity-acp", "conversations");
        yield* fs.makeDirectory(conversations, { recursive: true });
        const sessionId = "a1b2c3d4-1111-4222-8333-444455556666";
        let nextIndex = 0;
        const appendSteps = (steps: ReadonlyArray<readonly [number, Uint8Array]>) => {
          const database = new NodeSqlite.DatabaseSync(path.join(conversations, `${sessionId}.db`));
          try {
            database.exec(
              "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)",
            );
            const insert = database.prepare("INSERT INTO steps VALUES (?, ?, ?)");
            for (const [type, payload] of steps) insert.run(nextIndex++, type, payload);
          } finally {
            database.close();
          }
        };
        const turn = (steps: ReadonlyArray<readonly [number, Uint8Array]>) =>
          Effect.gen(function* () {
            const check = yield* watchAntigravityPromptOutcome(profileDirectory, sessionId);
            appendSteps(steps);
            return yield* check({ stopReason: "end_turn" });
          });

        const individualQuota = errorPayload("Individual quota reached for claude-opus-5-5.");
        const validationRequired = errorPayload(
          "Error 403, Message: Account requires verification., Status: VALIDATION_REQUIRED, Details: []",
        );

        const quotaOutcome = yield* turn([
          [USER_INPUT, Buffer.from("design")],
          [ERROR_MESSAGE, individualQuota],
        ]);
        assert.equal(quotaOutcome?.failure.class, "usage_limit");
        assert.equal(quotaOutcome?.failure.code, "QUOTA_EXHAUSTED");

        const validationOutcome = yield* turn([
          [USER_INPUT, Buffer.from("design")],
          [ERROR_MESSAGE, validationRequired],
        ]);
        assert.equal(validationOutcome?.failure.class, "permission_error");
        assert.equal(validationOutcome?.failure.code, "VALIDATION_REQUIRED");
        assert.equal(validationOutcome?.failure.retryable, false);

        const bareResourceExhausted = errorPayload("Upstream returned RESOURCE_EXHAUSTED.");
        const resourceExhaustedOutcome = yield* turn([
          [USER_INPUT, Buffer.from("design")],
          [ERROR_MESSAGE, bareResourceExhausted],
        ]);
        assert.equal(resourceExhaustedOutcome?.failure.class, "usage_limit");
        assert.equal(resourceExhaustedOutcome?.failure.code, "RESOURCE_EXHAUSTED");

        // Go 1.5.x trajectory payload with PERMISSION_DENIED (code 403) + pretty-printed JSON VALIDATION_REQUIRED.
        const validUrl =
          "https://accounts.google.com/v3/signin/continue?plt=traj-1&continue=https%3A%2F%2Fantigravity.google%2F";
        const prettyValidationJson = JSON.stringify(
          {
            error: {
              code: 403,
              message: "Verify your Google account to continue.",
              status: "PERMISSION_DENIED",
              details: [
                {
                  "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                  reason: "VALIDATION_REQUIRED",
                  metadata: {
                    validation_url: validUrl,
                    validation_error_message: "Verify your Google account to continue.",
                  },
                },
              ],
            },
          },
          null,
          2,
        );
        const go15Validation = errorPayload(
          `PERMISSION_DENIED (code 403): upstream error:\n${prettyValidationJson}`,
        );
        const go15Outcome = yield* turn([
          [USER_INPUT, Buffer.from("design")],
          [ERROR_MESSAGE, go15Validation],
        ]);
        assert.deepEqual(
          go15Outcome,
          reusable({
            class: "permission_error",
            code: "VALIDATION_REQUIRED",
            message: "Verify your Google account to continue.",
            retryable: false,
            verificationUrl: GoogleAccountVerificationUrl.make(validUrl),
          }),
        );

        const nonGoogleValidationJson = JSON.stringify(
          {
            error: {
              code: 403,
              message: "Verify your Google account to continue.",
              status: "PERMISSION_DENIED",
              details: [
                {
                  "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                  reason: "VALIDATION_REQUIRED",
                  metadata: {
                    validation_url: "https://evil.example.com/verify",
                  },
                },
              ],
            },
          },
          null,
          2,
        );
        const nonGoogleOutcome = yield* turn([
          [USER_INPUT, Buffer.from("design")],
          [ERROR_MESSAGE, errorPayload(`PERMISSION_DENIED (code 403): ${nonGoogleValidationJson}`)],
        ]);
        assert.equal(nonGoogleOutcome?.failure.class, "permission_error");
        assert.equal(nonGoogleOutcome?.failure.code, "VALIDATION_REQUIRED");
        assert.equal(nonGoogleOutcome?.failure.retryable, false);
        assert.isUndefined(nonGoogleOutcome?.failure.verificationUrl);

        // Oversized durations must not throw and must drop resetAt.
        for (const oversized of [
          errorPayload(
            "Error 429, Message: Rate limited., Status: RESOURCE_EXHAUSTED, Details: [map[@type:type.googleapis.com/google.rpc.RetryInfo retryDelay:99999999999999999h ]]",
          ),
          errorPayload(
            "Error 429, Message: Quota exhausted., Status: RESOURCE_EXHAUSTED, Details: [map[@type:type.googleapis.com/google.rpc.ErrorInfo metadata:map[quotaResetDelay:99999999999999999h ] reason:QUOTA_EXHAUSTED]]",
          ),
        ]) {
          const outcome = yield* turn([
            [USER_INPUT, Buffer.from("design")],
            [ERROR_MESSAGE, oversized],
          ]);
          assert.equal(outcome?.failure.class, "usage_limit");
          assert.isUndefined(outcome?.failure.resetAt);
        }

        // HTTP 400 INVALID_ARGUMENT with embedded VALIDATION_REQUIRED JSON and bare text never promote to permission_error.
        const spoofed400WithType = errorPayload(
          `Error 400, Message: ${prettyValidationJson}, Status: INVALID_ARGUMENT, Details: []`,
        );
        const spoofed400WithoutType = errorPayload(
          `Error 400, Message: ${JSON.stringify({
            error: {
              code: 403,
              status: "PERMISSION_DENIED",
              details: [{ reason: "VALIDATION_REQUIRED", metadata: { validation_url: validUrl } }],
            },
          })}, Status: INVALID_ARGUMENT, Details: []`,
        );
        for (const nonVerification of [
          spoofed400WithType,
          spoofed400WithoutType,
          errorPayload("VALIDATION_REQUIRED"),
        ]) {
          const outcome = yield* turn([
            [USER_INPUT, Buffer.from("design")],
            [ERROR_MESSAGE, nonVerification],
          ]);
          assert.notEqual(outcome?.failure.class, "permission_error");
          assert.isUndefined(outcome?.failure.verificationUrl);
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("classifyAntigravityPromptFailure", () => {
  it("classifies ACP RPC quota errors as usage_limit and permanent blocks as provider_error", () => {
    const quota = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Individual quota reached for model claude-opus-5-5",
      }),
    );
    assert.equal(quota.class, "usage_limit");
    assert.equal(quota.retryable, true);

    const validation = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Error 403: VALIDATION_REQUIRED (account verification needed)",
      }),
    );
    assert.equal(validation.class, "permission_error");
    assert.equal(validation.code, "VALIDATION_REQUIRED");
    assert.equal(validation.retryable, false);

    const rpcValidationUrl =
      "https://accounts.google.com/v3/signin/continue?plt=rpc-1&continue=https%3A%2F%2Fantigravity.google%2F";
    const rpcValidation = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `PERMISSION_DENIED (code 403): ${JSON.stringify({
          error: {
            code: 403,
            status: "PERMISSION_DENIED",
            details: [
              {
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                reason: "VALIDATION_REQUIRED",
                metadata: {
                  validation_url: rpcValidationUrl,
                  validation_error_message: "Verify your Google account.",
                },
              },
            ],
          },
        })}`,
      }),
    );
    assert.equal(rpcValidation.class, "permission_error");
    assert.equal(rpcValidation.code, "VALIDATION_REQUIRED");
    assert.equal(rpcValidation.retryable, false);
    assert.equal(rpcValidation.verificationUrl, rpcValidationUrl);

    const rpcInvalidUrl = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Error 403: VALIDATION_REQUIRED",
        data: {
          error: {
            code: 403,
            details: [
              {
                reason: "VALIDATION_REQUIRED",
                metadata: { validation_url: "https://evil.example.com/verify" },
              },
            ],
          },
        },
      }),
    );
    assert.equal(rpcInvalidUrl.class, "permission_error");
    assert.equal(rpcInvalidUrl.code, "VALIDATION_REQUIRED");
    assert.equal(rpcInvalidUrl.retryable, false);
    assert.isUndefined(rpcInvalidUrl.verificationUrl);

    const authRequired = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32000,
        errorMessage: "Sign in required",
      }),
    );
    assert.equal(authRequired.class, "provider_error");
    assert.equal(authRequired.retryable, false);

    const http429 = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "HTTP 429 Too Many Requests",
      }),
    );
    assert.equal(http429.class, "usage_limit");
    assert.equal(http429.retryable, true);

    for (const oversizedMessage of [
      "HTTP 429 retryDelay:99999999999999999h ",
      "HTTP 429 quotaResetDelay:99999999999999999h ",
    ]) {
      const oversized = classifyAntigravityPromptFailure(
        new EffectAcpErrors.AcpRequestError({
          code: -32603,
          errorMessage: oversizedMessage,
        }),
      );
      assert.equal(oversized.class, "usage_limit");
      assert.isUndefined(oversized.resetAt);
    }

    const spoofedPayload = {
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: { validation_url: rpcValidationUrl },
          },
        ],
      },
    };
    const missingTypePayload = {
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        details: [
          { reason: "VALIDATION_REQUIRED", metadata: { validation_url: rpcValidationUrl } },
        ],
      },
    };
    const missingFramingPayload = {
      error: {
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: { validation_url: rpcValidationUrl },
          },
        ],
      },
    };
    assert.isUndefined(extractAntigravityVerificationRequired(missingTypePayload));
    assert.isUndefined(extractAntigravityVerificationRequired(missingFramingPayload));
    assert.isUndefined(extractAntigravityVerificationFromText(JSON.stringify(missingTypePayload)));
    assert.isUndefined(
      extractAntigravityVerificationFromText(JSON.stringify(missingFramingPayload)),
    );
    assert.isUndefined(extractAntigravityVerificationFromText("VALIDATION_REQUIRED"));
    assert.isUndefined(
      extractAntigravityVerificationFromText(
        `VALIDATION_REQUIRED ${JSON.stringify(missingTypePayload)}`,
      ),
    );
    assert.isUndefined(
      extractAntigravityVerificationFromText(
        '{"error":{"code":403,"status":"PERMISSION_DENIED","details":[{"reason":"VALIDATION_REQUIRED"}]},}',
      ),
    );
    assert.isUndefined(
      extractAntigravityVerificationFromText(
        '{"error":{"code":403,"status":"PERMISSION_DENIED","details":[{"reason":"VALIDATION_REQUIRED"',
      ),
    );

    const longValid403 = extractAntigravityVerificationFromText(
      `Error 403, Message: ${"x".repeat(5_000)} ${JSON.stringify(spoofedPayload)}, Status: PERMISSION_DENIED`,
    );
    assert.equal(longValid403?.verificationUrl, rpcValidationUrl);

    for (const nonVerificationError of [
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `Error 400, Message: ${JSON.stringify(spoofedPayload)}, Status: INVALID_ARGUMENT`,
        data: spoofedPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `Error 400, Message: bad arg, Status: PERMISSION_DENIED ${JSON.stringify(spoofedPayload)}, Status: INVALID_ARGUMENT, Details: []`,
        data: spoofedPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `Error 400, Message: ${"a".repeat(5_000)} ${JSON.stringify(spoofedPayload)}, Status: INVALID_ARGUMENT`,
        data: spoofedPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `HTTP 400 Bad Request: ${JSON.stringify(spoofedPayload)}`,
        data: spoofedPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `Error 500: ${JSON.stringify(spoofedPayload)}`,
        data: spoofedPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `Error 400, Message: ${JSON.stringify(missingTypePayload)}, Status: INVALID_ARGUMENT`,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32602,
        errorMessage: "Invalid params",
        data: spoofedPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Request failed",
        data: missingTypePayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Request failed",
        data: missingFramingPayload,
      }),
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "VALIDATION_REQUIRED",
      }),
    ]) {
      const classified = classifyAntigravityPromptFailure(nonVerificationError);
      assert.notEqual(classified.class, "permission_error");
      assert.isUndefined(classified.verificationUrl);
    }
  });

  it("bounds verification scanning while extracting valid JSON nested inside unclosed wrappers", () => {
    const adversarial = `Error 403: VALIDATION_REQUIRED ${'{"error":'.repeat(8_000)}`;
    const fallback = extractAntigravityVerificationFromText(adversarial);
    assert.deepEqual(fallback, {
      message: "Verify your Google account to continue using Antigravity.",
    });

    const nestedUrl =
      "https://accounts.google.com/v3/signin/continue?plt=nested-1&continue=https%3A%2F%2Fantigravity.google%2F";
    const innerJson = JSON.stringify({
      error: {
        code: 403,
        status: "PERMISSION_DENIED",
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "VALIDATION_REQUIRED",
            metadata: {
              validation_url: nestedUrl,
              validation_error_message: "Nested verification prompt.",
            },
          },
        ],
      },
    });
    const unclosedOuter = `{"error": {"unclosed": ${innerJson}`;
    const extracted = extractAntigravityVerificationFromText(unclosedOuter);
    assert.deepEqual(extracted, {
      message: "Nested verification prompt.",
      verificationUrl: GoogleAccountVerificationUrl.make(nestedUrl),
    });
  });

  it("bounds oversized AcpRequestError.data scanning, serialization, and repeated API_ERROR segments", () => {
    // Early-stops string candidate scanning once verificationUrl is found without reading later fields.
    const validUrl =
      "https://accounts.google.com/v3/signin/continue?plt=early-stop&continue=https%3A%2F%2Fantigravity.google%2F";
    const earlyReads: Record<string, number> = {};
    const earlyStopData: Record<string, unknown> = {};
    Object.defineProperty(earlyStopData, "first", {
      enumerable: true,
      get() {
        earlyReads.first = (earlyReads.first ?? 0) + 1;
        return JSON.stringify({
          error: {
            code: 403,
            details: [
              {
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                reason: "VALIDATION_REQUIRED",
                metadata: { validation_url: validUrl },
              },
            ],
          },
        });
      },
    });
    for (let index = 0; index < 20; index++) {
      const key = `extra_${index}`;
      Object.defineProperty(earlyStopData, key, {
        enumerable: true,
        get() {
          earlyReads[key] = (earlyReads[key] ?? 0) + 1;
          return "unused";
        },
      });
    }
    const earlyResult = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Permission denied",
        data: earlyStopData,
      }),
    );
    assert.equal(earlyResult.class, "permission_error");
    assert.equal(earlyResult.code, "VALIDATION_REQUIRED");
    assert.equal(earlyResult.verificationUrl, validUrl);
    assert.equal(earlyReads.first, 1);
    assert.isUndefined(earlyReads.extra_0);

    // Caps candidate string scans, serializes cause.data at most once, and stops at the byte budget.
    const largeReads: Record<string, number> = {};
    const hugeData: Record<string, unknown> = {};
    const chunk = "x".repeat(4_096);
    for (let index = 0; index < 40; index++) {
      const key = `field_${index}`;
      Object.defineProperty(hugeData, key, {
        enumerable: true,
        get() {
          largeReads[key] = (largeReads[key] ?? 0) + 1;
          return index === 0 ? `HTTP 429 ${chunk}` : chunk;
        },
      });
    }
    const largeResult = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Request failed",
        data: hugeData,
      }),
    );
    assert.equal(largeResult.class, "usage_limit");
    assert.equal(largeResult.retryable, true);
    // Field 0 is inspected once as a verification candidate and once during the single bounded serialization.
    assert.equal(largeReads.field_0, 2);
    // Fields well past the 8-candidate and 32 KiB serialization budget are never touched.
    assert.isUndefined(largeReads.field_20);
    assert.isUndefined(largeReads.field_39);

    // Repeated `Error 429, Message: ` segments without a closing Status classify cleanly with bounded message.
    const repeatedSegments = "Error 429, Message: ".repeat(1_500);
    const repeatedResult = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: `${repeatedSegments}Quota exhausted`,
      }),
    );
    assert.equal(repeatedResult.class, "usage_limit");
    assert.equal(repeatedResult.retryable, true);
    assert.isAtMost(repeatedResult.message.length, 32_768);

    // Oversized message before `, Status: ` does not match past the 4096-char regex cap.
    const overlongApiMessage = `Error 500, Message: ${"a".repeat(5_000)}, Status: INTERNAL`;
    const overlongResult = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: overlongApiMessage,
      }),
    );
    assert.equal(overlongResult.class, "provider_error");
    assert.equal(overlongResult.code, "-32603");

    // A 40 KiB errorMessage does not truncate cause.data out of combined classification.
    const longMessageWithQuotaData = classifyAntigravityPromptFailure(
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "z".repeat(40_000),
        data: { details: "Error 429, Message: Quota exhausted, Status: RESOURCE_EXHAUSTED" },
      }),
    );
    assert.equal(longMessageWithQuotaData.class, "usage_limit");
    assert.equal(longMessageWithQuotaData.retryable, true);
  });
});
