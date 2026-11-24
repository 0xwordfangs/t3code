import { assert, it } from "@effect/vitest";
import {
  GoogleAccountVerificationUrl,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderSetupError,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import {
  knownProviderFailureMessage,
  makeProviderFailure,
  makeProviderFailureTurnItem,
  MAX_PROVIDER_FAILURE_MESSAGE_LENGTH,
  sanitizeProviderFailureMessage,
  ContextHandoffBudgetError,
  ContextHandoffBackgroundWorkError,
} from "./failure.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

it("redacts credentials and URL secrets from provider failures", () => {
  const failure = makeProviderFailure({
    message:
      'request failed: Authorization: Bearer bearer-secret https://user:pass@example.test/path?access_token=url-secret#fragment {"token":"json-secret"} api_key=key-secret sk-abcdefghijklmnop',
    code: "provider_rejected",
    class: "provider_error",
  });

  assert.equal(failure.class, "provider_error");
  assert.equal(failure.code, "provider_rejected");
  assert.include(failure.message, "[REDACTED]");
  assert.include(failure.message, "https://example.test/path");
  assert.notInclude(failure.message, "bearer-secret");
  assert.notInclude(failure.message, "user:pass");
  assert.notInclude(failure.message, "url-secret");
  assert.notInclude(failure.message, "json-secret");
  assert.notInclude(failure.message, "key-secret");
  assert.notInclude(failure.message, "sk-abcdefghijklmnop");
});

it("redacts credentials whose key names end in a credential word", () => {
  const failure = makeProviderFailure({
    code: '{"refresh_token":"code value"}',
    message:
      'oauth refresh failed: client_secret=client-value refresh_token=refresh-value {"client_secret":"json-value"} clientSecret: camel-value X-Auth-Token: header-value max_tokens=4096',
  });

  assert.equal(failure.code, '{"refresh_token":"[REDACTED]"}');
  assert.equal(
    failure.message,
    'oauth refresh failed: client_secret=[REDACTED] refresh_token=[REDACTED] {"client_secret":"[REDACTED]"} clientSecret: [REDACTED] X-Auth-Token: [REDACTED] max_tokens=4096',
  );
});

it("reports an oversized provider request as not retryable instead of the gateway's error page", () => {
  for (const message of [
    "413 <html>\n<head><title>413 Request Entity Too Large</title></head>\n<body>\n<center><h1>413 Request Entity Too Large</h1></center>\n<hr><center>nginx</center>\n</body>\n</html>",
    "unexpected status 413 Payload Too Large: request body exceeds the limit",
    'API Error: 413 {"type":"error","error":{"type":"request_too_large"}}',
  ]) {
    const failure = makeProviderFailure({ message, class: "provider_error", retryable: true });

    assert.equal(failure.class, "provider_error");
    assert.equal(failure.retryable, false);
    assert.equal(
      failure.message,
      "This thread's request is too large for the provider (HTTP 413), so retrying fails the same way. Start a fresh session with /clear, or continue in a new thread.",
    );
  }
  for (const message of [
    "Request too large for gpt-4o on tokens per min (TPM): Limit 30000, Requested 41300.",
    "Rate limited after 413 requests.",
    "Tool result content too large to return.",
  ]) {
    const failure = makeProviderFailure({ message, retryable: true });
    assert.equal(failure.message, message);
    assert.equal(failure.retryable, true);
  }
});

it("does not split a surrogate pair at the truncation boundary", () => {
  const failure = makeProviderFailure({
    message: `${"a".repeat(MAX_PROVIDER_FAILURE_MESSAGE_LENGTH - 2)}🚀tail`,
  });

  assert.equal(failure.message.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH - 1);
  assert.equal(failure.message.at(-1), "…");
  assert.notMatch(failure.message.slice(0, -1), /[\uD800-\uDBFF]$/u);
});

it("does not expose arbitrary cause messages and preserves explicit messages", () => {
  const cause = new Error("Adapter failed", {
    cause: new Error("Session expired. Sign in again."),
  });
  for (const value of [
    cause,
    Cause.fail(cause),
    "Session expired. Sign in again.",
    { message: "Session expired. Sign in again." },
  ]) {
    assert.equal(makeProviderFailure({ cause: value }).message, "Provider turn failed.");
    assert.equal(
      makeProviderFailure({ cause: value, message: "Provider connection closed." }).message,
      "Provider connection closed.",
    );
  }
});

it("preserves actionable T3 errors wrapped by turn startup", () => {
  for (const error of [
    new ContextHandoffBudgetError(),
    new ContextHandoffBackgroundWorkError(),
    new ProviderSetupError({
      instanceId: ProviderInstanceId.make("codex"),
      operation: "runtime",
      detail: "Codex 0.144.3 does not work with this T3 Code release.",
    }),
  ]) {
    const cause = new ProviderAdapter.ProviderAdapterTurnStartError({
      driver: ProviderDriverKind.make("codex"),
      threadId: ThreadId.make("thread:handoff-error"),
      providerThreadId: ProviderThreadId.make("provider-thread:handoff-error"),
      runId: RunId.make("run:handoff-error"),
      cause: error,
    });
    assert.equal(makeProviderFailure({ cause: Cause.fail(cause) }).message, error.message);
  }
});

it("names how the provider process exited and the stage it failed in, without its output", () => {
  for (const [exit, status] of [
    [{ signal: "SIGILL" }, " after signal SIGILL"],
    [{ code: 134 }, " with code 134"],
    [{}, ""],
    [{ signal: "SIGILL Bearer signal-secret" }, ""],
  ] as const) {
    const sessionOpenFailure = {
      _tag: "ProviderSessionOpenError",
      cause: new ProviderAdapter.ProviderAdapterOpenSessionError({
        driver: ProviderDriverKind.make("antigravity"),
        providerSessionId: ProviderSessionId.make("provider-session:private-id"),
        cause: {
          _tag: "AcpProcessExitedError",
          ...exit,
          pid: 4242,
          stderr: "*** SIGILL *** token=stderr-secret",
          cause: new Error("Process interrupted with private-detail"),
        },
      }),
    };
    const expected = `The provider session could not be opened: the provider process exited${status}. Check the server logs for the provider's output, then retry the turn.`;
    assert.equal(knownProviderFailureMessage(sessionOpenFailure), expected);
    assert.equal(makeProviderFailure({ cause: Cause.fail(sessionOpenFailure) }).message, expected);
  }
  assert.equal(
    makeProviderFailure({ cause: { _tag: "AcpProcessExitedError", code: 1 } }).message,
    "The provider process exited with code 1. Check the server logs for the provider's output, then retry the turn.",
  );
});

it("tells the user to sign in only when an ACP agent answers auth_required", () => {
  const providerSessionId = ProviderSessionId.make("provider-session:private-id");
  const sessionOpenFailure = (code: number) => ({
    _tag: "ProviderSessionOpenError",
    cause: new ProviderAdapter.ProviderAdapterOpenSessionError({
      driver: ProviderDriverKind.make("acpRegistry"),
      providerSessionId,
      cause: Cause.fail({ _tag: "AcpRequestError", code, errorMessage: "Private agent text" }),
    }),
  });

  assert.equal(
    makeProviderFailure({ cause: Cause.fail(sessionOpenFailure(-32000)) }).message,
    "The provider needs you to sign in. Sign in from its provider settings or with its own CLI or app, then retry the turn.",
  );
  assert.isUndefined(knownProviderFailureMessage(sessionOpenFailure(-32603)));
});

it("keeps unrecognized reasons and provider text out of category messages", () => {
  for (const cause of [
    "Private launch command",
    { _tag: "ProviderAdapterProtocolError", detail: "Pi model 'private-config' is invalid" },
    { _tag: "ProviderAdapterProtocolError", reason: "private-config" },
    { _tag: "ProviderAdapterProtocolError", reason: "constructor" },
    { _tag: "constructor", cause: "private-config" },
    {
      _tag: "CodexAppServerRequestError",
      method: "thread/goal/set",
      message: "thread not found: private-thread",
    },
    {
      _tag: "CodexAppServerRequestError",
      method: "turn/start",
      message: "goal objective must be at most 4000 characters",
    },
  ]) {
    assert.equal(
      makeProviderFailure({ cause: { _tag: "ProviderAdapterTurnStartError", cause } }).message,
      "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs.",
    );
  }
  assert.equal(
    makeProviderFailure({
      cause: { _tag: "ProviderAdapterProtocolError", reason: "previous_turn_active" },
    }).message,
    "Provider turn failed.",
  );
});

it("names a managed Codex policy that refuses the thread's access mode", () => {
  const message = makeProviderFailure({
    cause: {
      _tag: "ProviderAdapterTurnStartError",
      cause: {
        _tag: "CodexAppServerRequestError",
        method: "turn/start",
        code: -32600,
        message:
          "invalid thread settings override: invalid value for `sandbox_mode`: `DangerFullAccess` is not in the allowed set [ReadOnly, WorkspaceWrite] (set by /etc/codex/requirements.toml)",
      },
    },
  }).message;
  assert.notInclude(message, "Retry the turn");
  assert.match(message, /requirements|policy/iu);
});

it("uses the innermost category without carrying an outer reason into it", () => {
  assert.equal(
    makeProviderFailure({
      cause: {
        _tag: "ProviderAdapterTurnStartError",
        cause: {
          _tag: "ProviderAdapterProtocolError",
          reason: "previous_turn_active",
          cause: Cause.fail({ _tag: "ProviderAdapterOpenSessionError" }),
        },
      },
    }).message,
    "The provider session could not be opened. Check that the provider is installed and signed in, then retry the turn.",
  );
});

it("explains a native thread another client is writing to", () => {
  const inUse = new ProviderAdapter.ProviderAdapterThreadInUseError({
    driver: ProviderDriverKind.make("codex"),
    providerThreadId: ProviderThreadId.make("provider-thread:in-use"),
    detail: "This conversation is open in another Codex client.",
    cause: new Error("thread provider-thread:in-use already has an active writer"),
  });
  const cause = new ProviderAdapter.ProviderAdapterTurnStartError({
    driver: ProviderDriverKind.make("codex"),
    threadId: ThreadId.make("thread:in-use"),
    providerThreadId: ProviderThreadId.make("provider-thread:in-use"),
    runId: RunId.make("run:in-use"),
    cause: inUse,
  });
  assert.equal(makeProviderFailure({ cause: Cause.fail(cause) }).message, inUse.message);
});

it("does not expose defect text nested inside a known error category", () => {
  const failure = makeProviderFailure({
    cause: {
      _tag: "ProviderAdapterEventStreamError",
      cause: new Error(
        `Session rejected: Bearer nested-secret https://user:pass@example.test/path?token=secret ${"x".repeat(5000)}`,
      ),
    },
  });
  assert.include(failure.message, "provider event stream closed unexpectedly");
  assert.notInclude(failure.message, "Session rejected:");
  assert.notInclude(failure.message, "nested-secret");
  assert.notInclude(failure.message, "user:pass");
  assert.notInclude(failure.message, "token=secret");
  assert.isBelow(failure.message.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
});

it("handles cyclic causes and throwing accessors", () => {
  const cyclic: { message: string; cause?: unknown } = {
    message: "Provider disconnected. Retry the turn.",
  };
  cyclic.cause = cyclic;
  assert.equal(makeProviderFailure({ cause: cyclic }).message, "Provider turn failed.");
  assert.equal(
    makeProviderFailure({
      cause: {
        get message() {
          throw new Error("unreadable");
        },
        get cause() {
          throw new Error("unreadable");
        },
      },
    }).message,
    "Provider turn failed.",
  );
});

it("does not serialize arbitrary provider causes", () => {
  const failure = makeProviderFailure({
    cause: {
      payload: { authorization: "Bearer nested-secret" },
      stack: "private provider stack",
    },
    class: "transport_error",
  });

  assert.deepEqual(failure, {
    class: "transport_error",
    message: "Provider turn failed.",
    code: null,
    retryable: null,
  });
});

it.effect("keys terminal failure items by provider turn across retries and fallback paths", () =>
  Effect.gen(function* () {
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const driver = ProviderDriverKind.make("codex");
    const runId = RunId.make("run:provider-failure-id");
    const base = {
      idAllocator,
      driver,
      threadId: ThreadId.make("thread:provider-failure-id"),
      runId,
      nodeId: NodeId.make("node:provider-failure-id"),
      providerThreadId: ProviderThreadId.make("provider-thread:provider-failure-id"),
      itemOrdinal: 101,
      failure: makeProviderFailure({ message: "Provider failed" }),
      occurredAt: DateTime.makeUnsafe("2026-06-22T12:00:00.000Z"),
    } as const;
    const firstTurnId = ProviderTurnId.make("provider-turn:provider-failure-id:first");
    const secondTurnId = ProviderTurnId.make("provider-turn:provider-failure-id:second");

    const firstAttempt = makeProviderFailureTurnItem({
      ...base,
      providerTurnId: firstTurnId,
    });
    const retriedAttempt = makeProviderFailureTurnItem({
      ...base,
      providerTurnId: secondTurnId,
    });
    const ingestorFallback = makeProviderFailureTurnItem({
      ...base,
      runId: null,
      nodeId: null,
      providerTurnId: firstTurnId,
    });

    assert.notEqual(firstAttempt.id, retriedAttempt.id);
    assert.equal(firstAttempt.id, ingestorFallback.id);
    assert.equal(firstAttempt.ordinal, 101);
  }).pipe(Effect.provide(IdAllocator.layer)),
);

it("preserves only valid Google Accounts verification URLs with query params and titles VALIDATION_REQUIRED", () => {
  const validUrl =
    "https://accounts.google.com/v3/signin/continue?plt=abc&continue=https%3A%2F%2Fdevelopers.google.com%2Fprofile";
  const failure = makeProviderFailure({
    class: "permission_error",
    code: "VALIDATION_REQUIRED",
    message: "Verify your Google account to continue using Antigravity.",
    retryable: false,
    verificationUrl: validUrl,
  });

  assert.equal(failure.verificationUrl, GoogleAccountVerificationUrl.make(validUrl));

  for (const invalidUrl of [
    "http://accounts.google.com/v3/signin/continue?plt=abc",
    "https://evil.example.com/v3/signin/continue?plt=abc",
    "https://accounts.google.com@evil.com/v3/signin/continue?plt=1",
    "https://@accounts.google.com/v3/signin/continue?plt=1",
    "https://accounts.google.com/o/oauth2/v2/auth?client_id=123",
    "https://accounts.google.com/Logout",
    "https://accounts.google.com/ServiceLogin?continue=https%3A%2F%2Fgoogle.com",
    "https://accounts.google.com/v3/signin/continue?plt=1#foo",
    "https://accounts.google.com/v3/signin/continue?plt=1&client_id=abc",
    "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fattacker.invalid",
    "javascript:alert(1)",
    "https://accounts.google.com/v3/signin/continue?plt=1\n",
    "https://accounts.google.com/v3/signin/continue?plt=1 extra",
  ]) {
    const rejected = makeProviderFailure({
      class: "permission_error",
      code: "VALIDATION_REQUIRED",
      message: "Verify your Google account.",
      retryable: false,
      verificationUrl: invalidUrl,
    });
    assert.isUndefined(rejected.verificationUrl);
  }

  const item = makeProviderFailureTurnItem({
    driver: ProviderDriverKind.make("antigravity"),
    threadId: ThreadId.make("thread:validation-required"),
    runId: RunId.make("run:validation-required"),
    nodeId: NodeId.make("node:validation-required"),
    providerThreadId: ProviderThreadId.make("provider-thread:validation-required"),
    providerTurnId: ProviderTurnId.make("provider-turn:validation-required"),
    itemOrdinal: 1,
    failure,
    occurredAt: DateTime.makeUnsafe("2026-11-21T12:00:00.000Z"),
  });
  assert.equal(item.title, "Verification required");

  const regularItem = makeProviderFailureTurnItem({
    driver: ProviderDriverKind.make("antigravity"),
    threadId: ThreadId.make("thread:validation-required"),
    runId: RunId.make("run:validation-required"),
    nodeId: NodeId.make("node:validation-required"),
    providerThreadId: ProviderThreadId.make("provider-thread:validation-required"),
    providerTurnId: ProviderTurnId.make("provider-turn:validation-required-other"),
    itemOrdinal: 2,
    failure: makeProviderFailure({ class: "provider_error", message: "Other error" }),
    occurredAt: DateTime.makeUnsafe("2026-11-21T12:00:00.000Z"),
  });
  assert.equal(regularItem.title, "Provider error");
});

it("redacts fully percent-encoded credential URLs in provider failure text", () => {
  const failure = makeProviderFailure({
    message:
      "Redirect failed: https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DDUMMY_FULL_ENC_PLT_007 and double https%253A%252F%252Fuser%253Apass%2540example.test%252Fcb%253Fcode%253DDOUBLE_CODE and malformed https%3A%2F%2Fexample.test%2F%E0%A4%A%3Ftoken%3DBAD_ENC.",
  });

  assert.notInclude(failure.message, "DUMMY_FULL_ENC_PLT_007");
  assert.notInclude(failure.message, "DOUBLE_CODE");
  assert.notInclude(failure.message, "user:pass");
  assert.notInclude(failure.message, "BAD_ENC");
  assert.include(failure.message, "https://accounts.google.com/v3/signin/continue");
  assert.include(failure.message, "https://example.test/cb");
  assert.include(failure.message, "[REDACTED_URL]");

  const delimited = sanitizeProviderFailureMessage(
    [
      "https://proxy.test/r%2Fhttps%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DSECRET_SL",
      "continue%3Dhttps%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DSECRET_EQ",
      "%22https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DSECRET_Q%22",
      "prefix%20https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DSECRET_SP",
      "https%3A%2F%2Fexample.test%2Fdocs%20and%20https%3A%2F%2Fuser%3Apass%40example.test%2Fcb",
    ].join(" | "),
  );
  assert.notInclude(delimited, "SECRET_SL");
  assert.notInclude(delimited, "SECRET_EQ");
  assert.notInclude(delimited, "SECRET_Q");
  assert.notInclude(delimited, "SECRET_SP");
  assert.notInclude(delimited, "user:pass");
  assert.include(delimited, "https://example.test/docs%20and%20https://example.test/cb");
});

it("redacts oversized punctuation-heavy URLs in linear time and keeps truncated trailing secrets redacted", () => {
  // A quadratic trailing-punctuation regex stalls for >6s on 128 KiB of dots + 'x'; the project timeout catches regressions.
  const adversarial64k = `https://a.invalid/${".".repeat(64_000)}x`;
  const adversarial128k = `https://a.invalid/${".".repeat(128_000)}x`;

  const sanitized64k = sanitizeProviderFailureMessage(adversarial64k);
  const sanitized128k = sanitizeProviderFailureMessage(adversarial128k);
  const codeFailure = makeProviderFailure({ message: "failed", code: adversarial64k });

  assert.isAtMost(sanitized64k.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
  assert.isAtMost(sanitized128k.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
  assert.isAtMost(codeFailure.code?.length ?? 0, 128);

  // When a long URL precedes a JSON/key-value secret that gets cut by the 32 KiB pre-redaction cap,
  // unclosed trailing quotes must still be redacted even after URL shortening drops total length <= 4 KiB.
  const truncatedJsonSecret = `https://a.invalid/?q=${"a".repeat(32_720)} {"token":"TRUNCATED_JSON_SECRET_VALUE_EXCEEDING_CAP"}`;
  const redactedJson = sanitizeProviderFailureMessage(truncatedJsonSecret);
  assert.notInclude(redactedJson, "TRUNCATED_JSON_SECRET");
  assert.include(redactedJson, '{"token":"[REDACTED]');

  const truncatedKvSecret = `https://a.invalid/?q=${"a".repeat(32_720)} api_key="TRUNCATED_KV_SECRET_VALUE_EXCEEDING_CAP"`;
  const redactedKv = sanitizeProviderFailureMessage(truncatedKvSecret);
  assert.notInclude(redactedKv, "TRUNCATED_KV_SECRET");
  assert.include(redactedKv, "api_key=[REDACTED]");

  // A `sk-` key straddling the 32 KiB pre-redaction boundary must never leak its truncated prefix.
  const truncatedSkSecret = `https://a.invalid/?q=${"a".repeat(32_730)} sk-1234567890abcdef1234567890`;
  const redactedSk = sanitizeProviderFailureMessage(truncatedSkSecret);
  assert.equal(redactedSk, "https://a.invalid/ [REDACTED]");
  assert.notInclude(redactedSk, "sk-1234");

  const truncatedSkCode = makeProviderFailure({
    message: "failed",
    code: truncatedSkSecret,
  });
  assert.equal(truncatedSkCode.code, "https://a.invalid/ [REDACTED]");
  assert.notInclude(truncatedSkCode.code ?? "", "sk-1234");
});

it("redacts percent-delimited, partially encoded, JSON-escaped, embedded-path, and escaped-JSON secrets idempotently", () => {
  const vectors = [
    [
      "continue%3Dhttps://accounts.google.com/v3/signin/continue?plt=SECRET_PCT_EQ",
      "continue%3Dhttps://accounts.google.com/v3/signin/continue",
      "SECRET_PCT_EQ",
    ],
    [
      "prefix%20https://user:pass@example.test/p?plt=SECRET_PCT_SP#fragSecret",
      "prefix%20https://example.test/p",
      "SECRET_PCT_SP",
    ],
    [
      "%22https://accounts.google.com/v3/signin/continue?plt=SECRET_PCT_QUOT%22",
      "%22https://accounts.google.com/v3/signin/continue",
      "SECRET_PCT_QUOT",
    ],
    [
      "https://s3.amazonaws.com/bucket/my%20file.pdf?X-Amz-Signature=SECRET_S3_SIG",
      "https://s3.amazonaws.com/bucket/my%20file.pdf",
      "SECRET_S3_SIG",
    ],
    [
      "https://alice:my%20secret@example.test/path?sig=SECRET_USERINFO_SP",
      "https://example.test/path",
      "my%20secret",
    ],
    [
      "https://example.test/path?msg=a\\u003cb&sig=SECRET_GO_LT",
      "https://example.test/path",
      "SECRET_GO_LT",
    ],
    [
      "https%3A%2F%2Fghp_SECRETTOKEN\\u003f%40github.com%2Forg%2Frepo",
      "[REDACTED_URL]",
      "ghp_SECRETTOKEN",
    ],
    ["https%3A//example.test/path\\u003femail=a@b.com", "https://example.test/path", "a@b.com"],
    ["https://ghp_SECRET\\u0040github.com/org/repo", "[REDACTED_URL]", "ghp_SECRET"],
    ["https://ghp_SECRET\\u0026@github.com/org/repo", "[REDACTED_URL]", "ghp_SECRET"],
    ["https://user\\secretpass@example.test/path", "[REDACTED_URL]", "secretpass"],
    [
      '{"url":"https://accounts.google.com/v3/signin/continue\\u0026plt=SECRET_ESC_AMP"}',
      '{"url":"https://accounts.google.com/v3/signin/continue"}',
      "SECRET_ESC_AMP",
    ],
    [
      '{"authorization":"Bearer tok1","api_key":"SECRET_SIBLING_JSON"}',
      '{"authorization":"[REDACTED]","api_key":"[REDACTED]"}',
      "SECRET_SIBLING_JSON",
    ],
    [
      '{\\"authorization\\":\\"Bearer tok1\\",\\"api_key\\":\\"SECRET_SIBLING_ESC\\"}',
      '{\\"authorization\\":\\"[REDACTED]\\",\\"api_key\\":\\"[REDACTED]\\"}',
      "SECRET_SIBLING_ESC",
    ],
    [
      "{'authorization':'Bearer tok1','api_key':'SECRET_SIBLING_SINGLE'}",
      "{'authorization':'[REDACTED]','api_key':'[REDACTED]'}",
      "SECRET_SIBLING_SINGLE",
    ],
    [
      '{"note":"Bearer tok1","api_key":"SECRET_NOTE_BEARER"}',
      '{"note":"Bearer [REDACTED]","api_key":"[REDACTED]"}',
      "SECRET_NOTE_BEARER",
    ],
    ["[Bearer tok1]", "[Bearer [REDACTED]]", "tok1"],
    [
      '{"note":"Bearer [REDACTED]LEAK_AFTER_BRACKET"}',
      '{"note":"Bearer [REDACTED]"}',
      "LEAK_AFTER_BRACKET",
    ],
    [
      'Authorization: Bearer "QUOTED_BEARER_SECRET", retry_after=30',
      "Authorization: [REDACTED], retry_after=30",
      "QUOTED_BEARER_SECRET",
    ],
    [
      "access_token=MY_SECRET,retry_after=30",
      "access_token=[REDACTED],retry_after=30",
      "MY_SECRET",
    ],
    ["access_token=MY_SECRET;status=401", "access_token=[REDACTED];status=401", "MY_SECRET"],
    ['refresh_token="MY_SECRET") ok', "refresh_token=[REDACTED]) ok", "MY_SECRET"],
    ["refresh_token=[REDACTED])LEAK_AFTER_PAREN", "refresh_token=[REDACTED]", "LEAK_AFTER_PAREN"],
    [
      "https%3A//accounts.google.com/v3/signin/continue?plt=SECRET_PART_COLON",
      "https://accounts.google.com/v3/signin/continue",
      "SECRET_PART_COLON",
    ],
    [
      "https:%2F%2Faccounts.google.com/v3/signin/continue?plt=SECRET_PART_SLASH",
      "https://accounts.google.com/v3/signin/continue",
      "SECRET_PART_SLASH",
    ],
    [
      "https://accounts.google.com/v3/signin/continue%3Fplt%3DSECRET_ENC_Q",
      "https://accounts.google.com/v3/signin/continue",
      "SECRET_ENC_Q",
    ],
    [
      "https://accounts.google.com/v3/signin/continue%23token%3DSECRET_ENC_HASH",
      "https://accounts.google.com/v3/signin/continue",
      "SECRET_ENC_HASH",
    ],
    [
      '{"url":"https://accounts.google.com/v3/signin/continue\\u003fplt=SECRET_JSON_Q"}',
      '{"url":"https://accounts.google.com/v3/signin/continue"}',
      "SECRET_JSON_Q",
    ],
    [
      '{"nested":"\\"https://accounts.google.com/v3/signin/continue\\\\u003fplt\\\\u003dSECRET_DBL_JSON_Q\\""}',
      '{"nested":"\\"https://accounts.google.com/v3/signin/continue\\""}',
      "SECRET_DBL_JSON_Q",
    ],
    [
      '{"url":"https://accounts.google.com/v3/signin/continue\\u0023token=SECRET_JSON_HASH"}',
      '{"url":"https://accounts.google.com/v3/signin/continue"}',
      "SECRET_JSON_HASH",
    ],
    ["https://alice\\u003f:secret@example.test/path", "[REDACTED_URL]", "alice"],
    [
      "https://proxy.test/r/https://accounts.google.com/v3/signin/continue?plt=SECRET_EMB_LIT",
      "[REDACTED_URL]",
      "SECRET_EMB_LIT",
    ],
    [
      "https://proxy.test/r/https%3A//accounts.google.com/v3/signin/continue?plt=SECRET_EMB_PART",
      "[REDACTED_URL]",
      "SECRET_EMB_PART",
    ],
    [
      "https://proxy.test/r/https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DSECRET_EMB_ENC",
      "[REDACTED_URL]",
      "SECRET_EMB_ENC",
    ],
    [
      "https://example.test/cb?plt=SECRET_BEFORE_FRAG#FRAGMENT_SECRET",
      "https://example.test/cb",
      "FRAGMENT_SECRET",
    ],
    [
      "postgres://alice:dbpass@db.internal:5432/prod ?a=1&amp;plt=SECRET_REL_HTML",
      "postgres://<redacted>@db.internal:5432/prod ?a=1&amp;plt=<redacted>",
      "dbpass",
    ],
    // Encoded URLs with percent-encoded userinfo delimiters (%252F / %253F / %2523) and partially-encoded schemes
    [
      "https%3A%2F%2Fghp_SECRET%252Ftoken%40github.com%2Forg%2Frepo",
      "https://github.com/org/repo",
      "ghp_SECRET",
    ],
    [
      "https%3A%2F%2Fghp_SECRET%253Ftoken%40github.com%2Forg%2Frepo",
      "https://github.com/org/repo",
      "ghp_SECRET",
    ],
    [
      "https%3A%2F%2Fghp_SECRET%2523token%40github.com%2Forg%2Frepo",
      "https://github.com/org/repo",
      "ghp_SECRET",
    ],
    [
      "https%3A//ghp_SECRET%2Ftoken@github.com/org/repo",
      "https://github.com/org/repo",
      "ghp_SECRET",
    ],
    [
      "https%3A%2F%2Fghp_SECRET%2Fpart2\\u0026part3%40github.com%2Forg%2Frepo",
      "[REDACTED_URL]",
      "ghp_SECRET",
    ],
    ["https%3A%2F%2Fghp_SECRET%2Ftoken%40github.com%2Forg%2Frepo", "[REDACTED_URL]", "ghp_SECRET"],
    [
      "https%253A%2F%2Fghp_SECRET%252Ftoken%40github.com%2Forg%2Frepo",
      "[REDACTED_URL]",
      "ghp_SECRET",
    ],
    [
      "https%3A//s3.amazonaws.com/bucket/my%20file.pdf?X-Amz-Signature=SECRET_S3_SIG",
      "https://s3.amazonaws.com/bucket/my%20file.pdf",
      "SECRET_S3_SIG",
    ],
    [
      "https%3A//alice:my%20secret@example.test/path?sig=SECRET_USERINFO_SP",
      "https://example.test/path",
      "my%20secret",
    ],
    [
      "https%3A//example.test/path?msg=a\\u003cb&sig=SECRET_GO_LT",
      "https://example.test/path",
      "SECRET_GO_LT",
    ],
    [
      "https%3A%2F%2Fexample.test%2Fpath%3Fmsg%3Da%3Cb%26sig%3DSECRET_ENC_LT",
      "[REDACTED_URL]",
      "SECRET_ENC_LT",
    ],
    // JSON-escaped \u0040 / %40 userinfo cut bypasses
    ["https://ghp_SECRET\\u0026\\u0040github.com/org/repo", "[REDACTED_URL]", "ghp_SECRET"],
    ["https://alice:12345\\u0026\\u0040github.com/org/repo", "[REDACTED_URL]", "12345"],
    ["https://ghp_SECRET\\u003f\\u0040github.com/org/repo", "[REDACTED_URL]", "ghp_SECRET"],
    ["https://ghp_SECRET\\u003f%40github.com/org/repo", "[REDACTED_URL]", "ghp_SECRET"],
    [
      "https%3A%2F%2Fghp_SECRET\\u0026\\u0040github.com%2Forg%2Frepo",
      "[REDACTED_URL]",
      "ghp_SECRET",
    ],
    // Quoted & escaped Bearer/Basic tokens and placeholder boundary bypasses
    ['header: Bearer "QUOTED_BEARER_SECRET"', "header: Bearer [REDACTED]", "QUOTED_BEARER_SECRET"],
    ["header: Basic 'SINGLE_QUOTED_BASIC'", "header: Basic [REDACTED]", "SINGLE_QUOTED_BASIC"],
    [
      '{"note":"Bearer \\"ESCAPED_BEARER_SECRET\\"","api_key":"SECRET_BEARER_SIBLING"}',
      '{"note":"Bearer [REDACTED]","api_key":"[REDACTED]"}',
      "ESCAPED_BEARER_SECRET",
    ],
    [
      '{"note":"missing Bearer ","api_key":"SECRET_EMPTY_BEARER_SIBLING"}',
      '{"note":"missing Bearer [REDACTED]',
      "SECRET_EMPTY_BEARER_SIBLING",
    ],
    [
      "Bearer [REDACTED]))LEAK_AFTER_BEARER_PARENS",
      "Bearer [REDACTED]",
      "LEAK_AFTER_BEARER_PARENS",
    ],
    [
      "refresh_token=[REDACTED]))LEAK_AFTER_DOUBLE_PAREN",
      "refresh_token=[REDACTED]",
      "LEAK_AFTER_DOUBLE_PAREN",
    ],
    [
      "refresh_token=[REDACTED]])LEAK_AFTER_BRACKET_PAREN",
      "refresh_token=[REDACTED]",
      "LEAK_AFTER_BRACKET_PAREN",
    ],
    [
      "access_token=TOK1,plt=SECRET_CROSS_LAYER_PLT",
      "access_token=[REDACTED],plt=<redacted>",
      "SECRET_CROSS_LAYER_PLT",
    ],
    [
      "postgres://host/db ?access_token=[REDACTED],,REAL_SECRET&state=ok",
      "postgres://host/db ?access_token=[REDACTED]&state=ok",
      "REAL_SECRET",
    ],
    [
      "postgres://host/db?a=1&amp;access_token=[REDACTED],,REAL_SECRET&amp;state=ok",
      "postgres://host/db?a=1&amp;access_token=[REDACTED]&amp;state=ok",
      "REAL_SECRET",
    ],
    [
      "(postgres://host/db?access_token=[REDACTED]) ok",
      "(postgres://host/db?access_token=[REDACTED]) ok",
      "REAL_SECRET",
    ],
    // Hyphen-prefixed and JSON-escaped newline/quote-prefixed URLs
    [
      "foo-https://user:pass@example.test/?sig=SECRET_HYPHEN_PREFIX",
      "foo-https://example.test/",
      "SECRET_HYPHEN_PREFIX",
    ],
    [
      '{"detail":"Error:\\nhttps://s3.amazonaws.com/bucket/file?X-Amz-Signature=SECRET_S3_NL"}',
      '{"detail":"Error:\\nhttps://s3.amazonaws.com/bucket/file"}',
      "SECRET_S3_NL",
    ],
    [
      "\\u0022https://s3.amazonaws.com/bucket/file?X-Amz-Signature=SECRET_U22\\u0022",
      "\\u0022https://s3.amazonaws.com/bucket/file\\u0022",
      "SECRET_U22",
    ],
    // Literal [/?#] before encoded @ on final decode layer, and depth-matched encoded %2520/%2522
    ["https%3A%2F%2Fghp_SECRET/token%40github.com%2Forg%2Frepo", "[REDACTED_URL]", "ghp_SECRET"],
    ["https%3A%2F%2Fghp_SECRET?token%40github.com%2Forg%2Frepo", "[REDACTED_URL]", "ghp_SECRET"],
    [
      "https%253A%252F%252Fghp_SECRET%2Ftoken%2540github.com%252Forg%252Frepo",
      "[REDACTED_URL]",
      "ghp_SECRET",
    ],
    [
      "https%3A%2F%2Fs3.amazonaws.com%2Fbucket%2Fmy%2520file.pdf%3FX-Amz-Signature%3DSECRET_S3_ENC_SP",
      "https://s3.amazonaws.com/bucket/my%20file.pdf",
      "SECRET_S3_ENC_SP",
    ],
    [
      "https%3A%2F%2Falice%3Amy%2520secret%40example.test%2Fpath%3Fsig%3DSECRET_ENC_USER_SP",
      "https://example.test/path",
      "SECRET_ENC_USER_SP",
    ],
    [
      "https%3A%2F%2Fexample.test%2Fpath%3Fmsg%3Da%2522b%26X-Amz-Signature%3DSECRET_ENC_QUOT",
      "https://example.test/path",
      "SECRET_ENC_QUOT",
    ],
    // Mid-token punctuation in Bearer/Basic, trailing tails after quotes, and escaped double-quoted JSON keys
    ["header: Bearer abc)SECRET_TAIL", "header: Bearer [REDACTED]", "SECRET_TAIL"],
    ["header: Bearer abc]SECRET_TAIL", "header: Bearer [REDACTED]", "SECRET_TAIL"],
    ["header: Bearer abc'SECRET_TAIL", "header: Bearer [REDACTED]", "SECRET_TAIL"],
    ["header: Bearer abc\\SECRET_TAIL", "header: Bearer [REDACTED]", "SECRET_TAIL"],
    ['header: Bearer "tok1"SECRET_TAIL', "header: Bearer [REDACTED]", "SECRET_TAIL"],
    [
      '{"note":"Bearer \\"tok1\\"SECRET_TAIL","api_key":"SECRET_SIBLING_ESC_TAIL"}',
      '{"note":"Bearer [REDACTED]","api_key":"[REDACTED]"}',
      "SECRET_TAIL",
    ],
    [
      '\\\\"token":"SECRET_AFTER_TWO_BACKSLASHES"',
      '\\\\"token":"[REDACTED]"',
      "SECRET_AFTER_TWO_BACKSLASHES",
    ],
    ['\\"token":"SECRET_MISMATCHED"', '\\"token":"[REDACTED]"', "SECRET_MISMATCHED"],
    // Fail-closed ambiguous quote boundaries and cross-layer idempotency (<redacted> vs [REDACTED])
    [
      '["missing token: ", "plt=REAL_PLT_SECRET"]',
      '["missing token: [REDACTED]',
      "REAL_PLT_SECRET",
    ],
    [
      '["missing api_key=", "wsTicket=REAL_WSTICKET_SECRET"]',
      '["missing api_key=[REDACTED]',
      "REAL_WSTICKET_SECRET",
    ],
    [
      '{"error":"missing token: ","status":"failed"}',
      '{"error":"missing token: [REDACTED]',
      "UNREACHABLE_SENTINEL",
    ],
    ['(see "?token=") and "plt=REAL_PLT_SECRET"', '(see "?token=[REDACTED]', "REAL_PLT_SECRET"],
    ["access_token=", "access_token=<redacted>", "UNREACHABLE_SENTINEL"],
    ["?access_token=&state=ok", "?access_token=<redacted>&state=ok", "UNREACHABLE_SENTINEL"],
    [
      "access_token=TOK1,error_code=invalid_grant",
      "access_token=[REDACTED],error_code=invalid_grant",
      "TOK1",
    ],
    ["access_token=TOK1,auth_mode=oauth", "access_token=[REDACTED],auth_mode=oauth", "TOK1"],
    [
      "access_token=[REDACTED],code=[REDACTED]REAL_SECRET",
      "access_token=<redacted>",
      "REAL_SECRET",
    ],
    [
      "access_token=[REDACTED],auth_token_v2=[REDACTED]REAL_SECRET",
      "access_token=<redacted>",
      "REAL_SECRET",
    ],
    [
      "access_token=[REDACTED],token=[REDACTED],REAL_SECRET",
      "access_token=<redacted>",
      "REAL_SECRET",
    ],
    ['api_key="MY_SECRET")LEAK', "api_key=[REDACTED]", "LEAK"],
    ['refresh_token="MY_SECRET")LEAK', "refresh_token=[REDACTED]", "LEAK"],
    [
      '{\\"url\\":\\"postgres://host/db?access_token=SECRET_ESC_Q\\",\\"api_key\\":\\"SIBLING_ESC_Q\\"}',
      '{\\"url\\":\\"postgres://host/db?access_token=[REDACTED]\\",\\"api_key\\":\\"[REDACTED]\\"}',
      "SECRET_ESC_Q",
    ],
    // Non-sensitive sibling URLs, quoted secrets with spaces + punctuation/tails, non-HTTP query special chars, and %20 URL lists
    [
      "access_token=TOK1,ws=ws://127.0.0.1:3773/ws?wsTicket=REAL_WSTICKET",
      "access_token=[REDACTED],ws=ws://127.0.0.1:3773/ws?wsTicket=<redacted>",
      "REAL_WSTICKET",
    ],
    [
      "access_token=TOK1,cb=t3://auth/cb?code=REAL_OAUTH_CODE",
      "access_token=[REDACTED],cb=t3://auth/cb?code=<redacted>",
      "REAL_OAUTH_CODE",
    ],
    [
      'Invalid credentials: password="my secret pass".',
      "Invalid credentials: password=[REDACTED].",
      "secret",
    ],
    [
      'Auth failed (api_key="my secret key"): retry later',
      "Auth failed (api_key=[REDACTED]): retry later",
      "secret",
    ],
    ['credential="id=1,SECRET_PART_2".', "credential=[REDACTED].", "SECRET_PART_2"],
    ["password='correct horse battery'!", "password=[REDACTED]!", "horse"],
    ['Authorization: Bearer "my secret token".', "Authorization: [REDACTED].", "secret"],
    ['api_key="MY SECRET")LEAK', "api_key=[REDACTED]", "LEAK"],
    ['Bearer "hello world"SECRET_TAIL', "Bearer [REDACTED]", "SECRET_TAIL"],
    [
      '{"note":"Bearer \'tok 1\'","api_key":"SECRET"}',
      '{"note":"Bearer [REDACTED]","api_key":"[REDACTED]"}',
      "tok",
    ],
    ['{"note":"Bearer \\"my secret\\"."}', '{"note":"Bearer [REDACTED]."}', "secret"],
    ['{"note":"Invalid api_key=\'my secret\'"}', '{"note":"Invalid api_key=[REDACTED]"}', "secret"],
    ['access_token="my secret". Please retry.', "access_token=[REDACTED]. Please retry.", "secret"],
    [
      'Failed (access_token="my secret"): unauthorized',
      "Failed (access_token=[REDACTED]): unauthorized",
      "secret",
    ],
    [
      '{"url":"postgres://host/db?api_key=\\"SECRET_ESC_API_KEY\\"","other":"ok"}',
      '{"url":"postgres://host/db?api_key=[REDACTED]","other":"ok"}',
      "SECRET_ESC_API_KEY",
    ],
    [
      '{"url":"postgres://host/db?token=\\"SECRET_ESC_TOK\\"","other":"ok"}',
      '{"url":"postgres://host/db?token=[REDACTED]","other":"ok"}',
      "SECRET_ESC_TOK",
    ],
    [
      "postgres://host/db?password=abc\\DEF_SECRET&x=1",
      "postgres://host/db?password=[REDACTED]&x=1",
      "DEF_SECRET",
    ],
    [
      'postgres://host/db?password=abc"DEF_SECRET&x=1',
      "postgres://host/db?password=[REDACTED]&x=1",
      "DEF_SECRET",
    ],
    [
      "postgres://host/db?api_key=abc'DEF_SECRET&x=1",
      "postgres://host/db?api_key=[REDACTED]&x=1",
      "DEF_SECRET",
    ],
    [
      'postgres://host/db?api_key="MY_SECRET""LEAK"&state=ok',
      "postgres://host/db?api_key=[REDACTED]&state=ok",
      "LEAK",
    ],
    [
      "https%3A%2F%2Fexample.test%2Fdocs%20and%20https%3A%2F%2Fuser%3Apass%40example.test%2Fcb",
      "https://example.test/docs%20and%20https://example.test/cb",
      "user:pass",
    ],
    [
      "https%253A%252F%252Fexample.test%252Fdocs%2520https%253A%252F%252Fuser%253Apass%2540example.test%252Fcb",
      "https://example.test/docs%2520https://example.test/cb",
      "user:pass",
    ],
    [
      "https%253A%2F%2Fs3.amazonaws.com%2Fbucket%2Fmy%2520file%3FX-Amz-Signature%3DSECRET_MIXED_SIG",
      "[REDACTED_URL]",
      "SECRET_MIXED_SIG",
    ],
    [
      '{"err":"See \\u003chttps://s3.amazonaws.com/bucket/file?X-Amz-Signature=SECRET_GO\\u003e"}',
      '{"err":"See \\u003chttps://s3.amazonaws.com/bucket/file"}',
      "SECRET_GO",
    ],
    ['{"msg":"password=\\"my secret pass\\""}', '{"msg":"password=[REDACTED]"}', "secret"],
    ['access_token=;#"', 'access_token=<redacted>#"', "UNREACHABLE_SENTINEL"],
    // Quoted secrets starting with whitespace or delimiters, non-HTTP query special delimiters, ping-pong, and URL truncation
    [
      'Invalid credentials: password=" my secret pass".',
      "Invalid credentials: password=[REDACTED]",
      "secret",
    ],
    ['Bearer " my secret token"', "Bearer [REDACTED]", "secret"],
    ['password=";mySecretPass"', "password=[REDACTED]", "mySecretPass"],
    ['password=",mySecretPass"', "password=[REDACTED]", "mySecretPass"],
    ['password=")my secret pass"', "password=[REDACTED]", "secret"],
    ['password="&a SECRET999"', "password=[REDACTED]", "SECRET999"],
    ["password=' SECRET999'", "password=[REDACTED]", "SECRET999"],
    ['access_token=" SECRET999"', "access_token=[REDACTED]", "SECRET999"],
    [
      'postgres://host/db?password="&mySecretPass"&x=1',
      "postgres://host/db?password=[REDACTED]",
      "mySecretPass",
    ],
    [
      'postgres://host/db?password=" mySecretPass"&x=1',
      "postgres://host/db?password=[REDACTED]",
      "mySecretPass",
    ],
    [
      'postgres://host/db?api_key=abc")SECRET999&x=1',
      "postgres://host/db?api_key=[REDACTED]&x=1",
      "SECRET999",
    ],
    [
      'postgres://host/db?api_key="MY_SECRET"")LEAK&state=ok',
      "postgres://host/db?api_key=[REDACTED]&state=ok",
      "LEAK",
    ],
    [
      'postgres://host/db?api_key=abc\\")LEAK&x=1',
      "postgres://host/db?api_key=[REDACTED]&x=1",
      "LEAK",
    ],
    [
      "postgres://host/db?api_key=abc')LEAK&x=1",
      "postgres://host/db?api_key=[REDACTED]&x=1",
      "LEAK",
    ],
    [
      'postgres://host/db?password=abc",SECRET999&x=1',
      "postgres://host/db?password=[REDACTED]&x=1",
      "SECRET999",
    ],
    [
      "-postgres://alice:SECRET_PG@db.internal:5432/prod",
      "-postgres://<redacted>@db.internal:5432/prod",
      "SECRET_PG",
    ],
    ['["missing token: ", "api_key=REAL_SECRET"]', '["missing token: [REDACTED]', "REAL_SECRET"],
    ["access_token=;x#;x#;x#;x#SECRET", "access_token=<redacted>#;x#;x#;x#SECRET", "UNREACHABLE"],
    ["refresh_token=,x&,x&,x&,x&SECRET", "refresh_token=<redacted>&,x&,x&,x&SECRET", "UNREACHABLE"],
    [
      `${"a".repeat(4075)} https://example.test/very/long/path`,
      `${"a".repeat(4075)} https://example.tes…`,
      "UNREACHABLE",
    ],
    [
      `${"a".repeat(4086)} https://example.test/very/long/path`,
      `${"a".repeat(4086)} https://…`,
      "UNREACHABLE",
    ],
    // Fail-closed escaped/cross-nested quotes, pre-redacted URL fragments, escaped unquoted tokens, and percent-encoded secrets
    [
      '{"msg":"unexpected \\" char, password=\\" my secret pass\\""}',
      '{"msg":"unexpected \\" char, password=[REDACTED]',
      "my secret pass",
    ],
    ['access_token=abc\\"def SECRET_LEAK', "access_token=[REDACTED] SECRET_LEAK", "abc"],
    ["access_token=foo\\ bar baz", "access_token=[REDACTED] baz", "bar"],
    ["password=<redacted>#SECRET999", "password=[REDACTED]", "SECRET999"],
    ["password=<redacted>&SECRET999", "password=[REDACTED]", "SECRET999"],
    ['password=<redacted>"SECRET999"', 'password=[REDACTED]"', "SECRET999"],
    ['api_key=<redacted>"SECRET999', "api_key=[REDACTED]", "SECRET999"],
    ["api_key=<redacted>\\SECRET999", "api_key=[REDACTED]", "SECRET999"],
    ["api_key=<redacted>%20SECRET999", "api_key=[REDACTED]", "SECRET999"],
    [
      'postgres://host/db?api_key=<redacted>"SECRET999&x=1',
      "postgres://host/db?api_key=[REDACTED]&x=1",
      "SECRET999",
    ],
    [
      'postgres://host/db?access_token=<redacted>"SECRET999&x=1',
      "postgres://host/db?access_token=[REDACTED]&x=1",
      "SECRET999",
    ],
    ["https://example.test?token=<redacted>#REAL_SECRET", "https://example.test/", "REAL_SECRET"],
    [
      '{"hint":"use \' for strings","err":"password=\' my secret pass\'"}',
      '{"hint":"use \' for strings","err":"password=[REDACTED]',
      "my secret pass",
    ],
    [
      '{"hint":"use \' for strings", "dsn":"postgres://host/db?api_key=abc\')SECRET999&x=1"}',
      '{"hint":"use \' for strings", "dsn":"postgres://host/db?api_key=[REDACTED]&x=1"}',
      "SECRET999",
    ],
    [
      '{"url":"postgres://host/db?api_key=abc\\"SECRET999&x=1"}',
      '{"url":"postgres://host/db?api_key=[REDACTED]&x=1"}',
      "SECRET999",
    ],
    [
      '{"msg":"auth failed: password=abc\\"SECRET999"}',
      '{"msg":"auth failed: password=[REDACTED]"}',
      "SECRET999",
    ],
    [
      '{"msg":"Authorization: Bearer abc\\"SECRET999"}',
      '{"msg":"Authorization: [REDACTED]"}',
      "SECRET999",
    ],
    [
      "{'url':'postgres://host/db?api_key=abc\\'SECRET999&x=1'}",
      "{'url':'postgres://host/db?api_key=[REDACTED]&x=1'}",
      "SECRET999",
    ],
    [
      '{"outer":"{\\"msg\\":\\"password=abc\\\\\\"SECRET999\\"}"}',
      '{"outer":"{\\"msg\\":\\"password=[REDACTED]\\"}"}',
      "SECRET999",
    ],
    [
      '{"error":"Unexpected token \' in JSON at position 0; password=\' SECRET999\'"}',
      '{"error":"Unexpected token \' in JSON at position 0; password=[REDACTED]',
      "SECRET999",
    ],
    [
      'Error parsing \'invalid " char\': password=" SECRET999"',
      "Error parsing 'invalid \" char': password=[REDACTED]",
      "SECRET999",
    ],
    [
      'syntax error near "\npassword=" my secret pass"',
      'syntax error near "\npassword=[REDACTED]',
      "my secret pass",
    ],
    [
      'Expected "Bearer ". Header had password=" my secret pass"',
      'Expected "Bearer [REDACTED]',
      "my secret pass",
    ],
    [
      'Expected "token=". Request had api_key=" my secret key"',
      'Expected "token=[REDACTED]',
      "my secret key",
    ],
    [
      'Error: "missing token: "foo password=" SECRET999"',
      'Error: "missing token: [REDACTED]',
      "SECRET999",
    ],
    [
      "wss://example.test/ws?wsticket=part1\\u003cSECRET_PART2&foo=bar",
      "wss://example.test/ws?wsticket=<redacted>&foo=bar",
      "SECRET_PART2",
    ],
    ["error%20Bearer%20SUPERSECRET", "error%20Bearer [REDACTED]", "SUPERSECRET"],
    [
      "https%3A%2F%2Fa.example.test%20Bearer%20SUPERSECRET%20https%3A%2F%2Fb.example.test",
      "[REDACTED_URL]",
      "SUPERSECRET",
    ],
    [
      "https%3A%2F%2Fa.example.test%20token=SUPERSECRET%20https%3A%2F%2Fb.example.test",
      "[REDACTED_URL]",
      "SUPERSECRET",
    ],
    [
      "https://example.test/docs%20sk-1234567890abcdef1234567890%20https://example.test/cb",
      "[REDACTED_URL]",
      "1234567890abcdef",
    ],
    [
      "https://example.test/docs%20token%3DSECRET_TOK%20https://example.test/cb",
      "[REDACTED_URL]",
      "SECRET_TOK",
    ],
    [
      "https://example.test/a%20plt=SECRET_PLT%20https://example.test/b",
      "[REDACTED_URL]",
      "SECRET_PLT",
    ],
    ["error%20sk-ant-api03-1234567890abcdef1234567890abcdef", "error%20[REDACTED]", "sk-ant-api03"],
    // Final review regressions: JSON sibling properties after non-HTTP query params, JSON \n/\r/\t delimiters, and nested kv tails
    [
      '{"url":"postgres://host/db?api_key=SECRET_1","err":"password: SECRET_2"}',
      '{"url":"postgres://host/db?api_key=[REDACTED]","err":"password: [REDACTED]"}',
      "SECRET_2",
    ],
    [
      '{"url":"postgres://host/db?api_key=\\"SECRET_1\\"","err":"password: SECRET_2"}',
      '{"url":"postgres://host/db?api_key=[REDACTED]","err":"password: [REDACTED]"}',
      "SECRET_2",
    ],
    [
      '{"url":"postgres://host/db?password=SECRET_1","err":"api_key=\\"MY SECRET_2\\""}',
      '{"url":"postgres://host/db?password=[REDACTED]","err":"api_key=[REDACTED]"}',
      "SECRET_2",
    ],
    [
      "{'url':'postgres://host/db?api_key=SECRET_1','err':'password: SECRET_2'}",
      "{'url':'postgres://host/db?api_key=[REDACTED]','err':'password: [REDACTED]'}",
      "SECRET_2",
    ],
    [
      '{"err":"Authorization: Bearer TOK1\\nX-Api-Key: SECRET_2"}',
      '{"err":"Authorization: [REDACTED]\\nX-Api-Key: [REDACTED]"}',
      "SECRET_2",
    ],
    [
      '{"err":"api_key=SECRET_1\\npassword: SECRET_2"}',
      '{"err":"api_key=[REDACTED]\\npassword: [REDACTED]"}',
      "SECRET_2",
    ],
    [
      JSON.stringify({ err: "line1\nBearer SUPERSECRET_BEARER" }),
      JSON.stringify({ err: "line1\nBearer [REDACTED]" }),
      "SUPERSECRET_BEARER",
    ],
    [
      JSON.stringify({ err: "line1\nsk-1234567890abcdef1234567890" }),
      JSON.stringify({ err: "line1\n[REDACTED]" }),
      "1234567890abcdef",
    ],
    [
      JSON.stringify({ err: "line1\nplt=SUPERSECRET_PLT\nwsticket=SUPERSECRET_WS" }),
      JSON.stringify({ err: "line1\nplt=<redacted>\nwsticket=<redacted>" }),
      "SUPERSECRET_PLT",
    ],
    [
      "access_token=TOK1,ws=wss://a.test/?foo=1,x=plt=SECRET_NESTED",
      "access_token=<redacted>",
      "SECRET_NESTED",
    ],
    [
      `token=${"x".repeat(32_746)} err%20sk-1234567890abcdef1234567890`,
      "token=[REDACTED] err%20[REDACTED]",
      "sk-123456",
    ],
    [
      `https://a.invalid/?q=${"a".repeat(32_725)} error%20sk-1234567890abcdef1234567890`,
      "https://a.invalid/ error%20[REDACTED]",
      "sk-123456",
    ],
    [
      `token=${"x".repeat(32_725)} {"err":"\\nhttps%3A//glpat-SUPERSECRETTOKEN@gitlab.com/repo"}`,
      'token=[REDACTED] {"err":"\\n[REDACTED_URL]',
      "glpat-",
    ],
  ] as const;

  for (const [input, expected, secret] of vectors) {
    const once = sanitizeProviderFailureMessage(input);
    assert.equal(once, expected);
    assert.notInclude(once, secret);
    assert.equal(sanitizeProviderFailureMessage(once), once);
  }
  assert.notInclude(sanitizeProviderFailureMessage('access_token=abc\\"def SECRET_LEAK'), "def");
  assert.notInclude(sanitizeProviderFailureMessage("access_token=foo\\ bar baz"), "foo");

  const escapedJsonMessage =
    'wrapper: {\\"token\\":\\"ESC_SECRET_1\\",\\"api_key\\":\\"ESC_\\\\\\"QUOTED\\\\\\"_2\\",\\"status\\":\\"ok\\"} {"secret":"inner\\"quote\\\'val","keep":"yes"} {\'password\':\'single\\\'quote"val\',\'keep\':\'yes\'}';
  const escapedJsonCode = '{\\"refresh_token\\":\\"CODE_ESC_SECRET\\",\\"scope\\":\\"read\\"}';
  const failure = makeProviderFailure({
    message: escapedJsonMessage,
    code: escapedJsonCode,
  });

  assert.equal(
    failure.message,
    'wrapper: {\\"token\\":\\"[REDACTED]\\",\\"api_key\\":\\"[REDACTED]\\",\\"status\\":\\"ok\\"} {"secret":"[REDACTED]","keep":"yes"} {\'password\':\'[REDACTED]\',\'keep\':\'yes\'}',
  );
  assert.equal(failure.code, '{\\"refresh_token\\":\\"[REDACTED]\\",\\"scope\\":\\"read\\"}');
  assert.notInclude(failure.message, "ESC_SECRET_1");
  assert.notInclude(failure.message, "QUOTED");
  assert.notInclude(failure.message, "inner");
  assert.notInclude(failure.message, "single");
  assert.notInclude(failure.code ?? "", "CODE_ESC_SECRET");
  assert.equal(sanitizeProviderFailureMessage(failure.message), failure.message);

  // Adversarial linear-time check on long backslash/quote/userinfo sequences; the project timeout catches regressions.
  const adversarial = [
    `{\\"token\\":\\"${"\\".repeat(20_000)}`,
    `Bearer \\"${"\\".repeat(20_000)}`,
    `{"token":"${"\\".repeat(20_000)}`,
    `https://${"a".repeat(20_000)}@`,
  ].join(" ");
  const sanitizedAdversarial = sanitizeProviderFailureMessage(adversarial);
  assert.isAtMost(sanitizedAdversarial.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
});

it("redacts 32 KiB truncated URL userinfo with collapsible prefixes and stays idempotent across 4 KiB truncation boundaries", () => {
  const collapsiblePrefix = `token=${"x".repeat(32_735)} `;
  const truncatedUserinfoCases = [
    [
      `${collapsiblePrefix}https://oauth2:glpat-SUPERSECRETTOKEN@gitlab.com/repo.git`,
      ["oauth2", "glpat", "SUPERSECRETTOKEN"],
    ],
    [
      `${collapsiblePrefix}postgres://admin:supersecret@db.internal:5432/prod`,
      ["admin", "supersecret"],
    ],
  ] as const;
  for (const [input, forbidden] of truncatedUserinfoCases) {
    const out = sanitizeProviderFailureMessage(input);
    for (const secret of forbidden) {
      assert.notInclude(out, secret);
    }
    assert.equal(sanitizeProviderFailureMessage(out), out);
  }

  const boundaryTargets = [
    "https://api.example.test:443/v1/chat",
    "https://[::1]:8443/v1/chat",
    "https://a.example.test/path%20and%20https://b.example.test/path",
    "wss://api.example.test/v1/chat?access_token=SECRET123&ok=1",
    "wss://api.example.test/v1/chat?wsticket=SECRET123&ok=1",
  ];
  const observedSuffixes = new Set<string>();
  for (const target of boundaryTargets) {
    const redactedTarget = sanitizeProviderFailureMessage(target);
    const minPad = Math.max(0, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH - redactedTarget.length - 4);
    for (let pad = minPad; pad < MAX_PROVIDER_FAILURE_MESSAGE_LENGTH; pad += 1) {
      const out = sanitizeProviderFailureMessage(`${"a".repeat(pad)} ${target}`);
      assert.isAtMost(out.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
      assert.notInclude(out, "SECRET123");
      assert.equal(
        sanitizeProviderFailureMessage(out),
        out,
        `non-idempotent at pad=${pad}: ${out.slice(-48)}`,
      );
      for (const suffix of [".…", ":…", "[::1…", "<reda…", "[REDACTED]…", "%20…"]) {
        if (out.endsWith(suffix)) {
          observedSuffixes.add(suffix);
        }
      }
    }
  }
  const truncatedPort = `${"a".repeat(MAX_PROVIDER_FAILURE_MESSAGE_LENGTH - 30)} https://api.example.test:443…`;
  assert.equal(truncatedPort.length, MAX_PROVIDER_FAILURE_MESSAGE_LENGTH);
  assert.equal(sanitizeProviderFailureMessage(truncatedPort), truncatedPort);
  for (const suffix of [".…", ":…", "[::1…", "<reda…", "[REDACTED]…", "%20…"]) {
    assert.isTrue(observedSuffixes.has(suffix), `expected truncation sweep to hit ${suffix}`);
  }
});
