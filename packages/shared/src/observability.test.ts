import { assert, describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import { cast } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Redactable from "effect/Redactable";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import type { OtlpTracer } from "effect/observability";

import {
  causeErrorTag,
  compactTraceAttributes,
  decodeOtlpTraceRecords,
  errorTag,
  makeLocalFileTracer,
  makeTraceSink,
  redactCauseCredentials,
  redactUrlCredentials,
  type EffectTraceRecord,
  type TraceRecord,
  OtlpHeadersFromString,
  truncateTraceAttributes,
  processServiceInstanceId,
} from "./observability.ts";

describe("processServiceInstanceId", () => {
  it("stays the same for the process unless the operator set one", () => {
    expect(processServiceInstanceId()).toBe(processServiceInstanceId({}));
    expect(processServiceInstanceId({ "service.instance.id": "pod-7" })).toBe("pod-7");
  });
});

describe("errorTag", () => {
  it("reports structural tags without retaining arbitrary values", () => {
    assert.equal(errorTag({ _tag: "AcpRequestError" }), "AcpRequestError");
    assert.equal(errorTag(new TypeError("secret-token-value")), "TypeError");
    assert.equal(errorTag({ _tag: "secret token value" }), "TaggedError");
  });
});

describe("causeErrorTag", () => {
  it("reports the tagged failure value instead of the Cause reason wrapper", () => {
    assert.equal(
      causeErrorTag(Cause.fail({ _tag: "ServerAuthInvalidCredentialError" })),
      "ServerAuthInvalidCredentialError",
    );
  });
});

describe("redactUrlCredentials", () => {
  it("redacts URL credential query parameters without corrupting plain error prose", () => {
    assert.equal(
      redactUrlCredentials(
        "https://accounts.google.com/v3/signin/continue?plt=SECRET_PLT_1&continue=https%3A%2F%2Fexample.com&plt=SECRET_PLT_2",
      ),
      "https://accounts.google.com/v3/signin/continue?plt=<redacted>&continue=https%3A%2F%2Fexample.com&plt=<redacted>",
    );
    assert.equal(
      redactUrlCredentials(
        '{"validation_url":"https://accounts.google.com/v3/signin/continue?foo=1\\u0026plt=SECRET_ESCAPED\\u0026token=SECRET_TOK"}',
      ),
      '{"validation_url":"https://accounts.google.com/v3/signin/continue?foo=1\\u0026plt=<redacted>\\u0026token=<redacted>"}',
    );
    assert.equal(
      redactUrlCredentials(
        "https://example.com/cb?access_token=AT_SECRET&id_token=ID_SECRET&refresh_token=RT_SECRET&code=CODE_SECRET&token=TOK_SECRET&state=ok",
      ),
      "https://example.com/cb?access_token=<redacted>&id_token=<redacted>&refresh_token=<redacted>&code=<redacted>&token=<redacted>&state=ok",
    );
    assert.equal(
      redactUrlCredentials("Command failed with exit code=1 and status code=403"),
      "Command failed with exit code=1 and status code=403",
    );
    assert.equal(
      redactUrlCredentials(
        "url: 'https://accounts.google.com/v3/signin/continue?foo=1&plt=SECRET_QUOTED'",
      ),
      "url: 'https://accounts.google.com/v3/signin/continue?foo=1&plt=<redacted>'",
    );
    assert.equal(
      redactUrlCredentials(
        '{"nested":"https://accounts.google.com/v3/signin/continue?foo=1\\\\u0026plt=SECRET_DOUBLE_ESCAPED"}',
      ),
      '{"nested":"https://accounts.google.com/v3/signin/continue?foo=1\\\\u0026plt=<redacted>"}',
    );
  });

  it("redacts percent-encoded keys, nested continue/redirect_uri params, and fully encoded URLs", () => {
    const nestedContinue =
      "https://accounts.google.com/v3/signin/continue?plt=DUMMY_PLT_OUTER_001&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Fplt%3DDUMMY_PLT_INNER_002%26code%3DDUMMY_CODE_003%26token%3DDUMMY_TOKEN_004%26access_token%3DDUMMY_AT_005";
    const redactedNested = redactUrlCredentials(nestedContinue);
    assert.equal(
      redactedNested,
      "https://accounts.google.com/v3/signin/continue?plt=<redacted>&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Fplt%3D<redacted>%26code%3D<redacted>%26token%3D<redacted>%26access_token%3D<redacted>",
    );

    const percentEncodedKeys =
      "https://accounts.google.com/v3/signin/continue?%70lt=DUMMY_PCT_KEY_PLT_006&%77sTicket=DUMMY_WSTICKET_008&%64pop=DUMMY_DPOP_009";
    const redactedKeys = redactUrlCredentials(percentEncodedKeys);
    assert.equal(
      redactedKeys,
      "https://accounts.google.com/v3/signin/continue?%70lt=<redacted>&%77sTicket=<redacted>&%64pop=<redacted>",
    );

    const fullyEncoded =
      "https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DDUMMY_FULL_ENC_PLT_007";
    assert.equal(
      redactUrlCredentials(fullyEncoded),
      "https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3D<redacted>",
    );

    const doubleEncoded =
      "https://accounts.google.com/v3/signin/continue?continue=https%253A%252F%252Fantigravity.google%252Fauth%253Fplt%253DDUMMY_DOUBLE_PLT%2526code%253DDUMMY_DOUBLE_CODE";
    const redactedDouble = redactUrlCredentials(doubleEncoded);
    assert.notInclude(redactedDouble, "DUMMY_DOUBLE_PLT");
    assert.notInclude(redactedDouble, "DUMMY_DOUBLE_CODE");
    assert.include(redactedDouble, "%253Fplt%253D<redacted>%2526code%253D<redacted>");

    const nestedRedirectUri =
      "https://server.invalid/oauth/mcp/authorize?redirect_uri=http%3A%2F%2F127.0.0.1%3A9999%2Fcb%3Fcode%3DDUMMY_HTTP_CODE_010%26token%3DDUMMY_HTTP_TOK_011";
    const redactedRedirect = redactUrlCredentials(nestedRedirectUri);
    assert.notInclude(redactedRedirect, "DUMMY_HTTP_CODE_010");
    assert.notInclude(redactedRedirect, "DUMMY_HTTP_TOK_011");
    assert.include(redactedRedirect, "%3Fcode%3D<redacted>%26token%3D<redacted>");

    const jsonEscaped =
      '{"url":"https://accounts.google.com/v3/signin/continue\\u003fplt\\u003dDUMMY_JSON_PLT\\u0026token\\u003dDUMMY_JSON_TOK"}';
    const redactedJson = redactUrlCredentials(jsonEscaped);
    assert.notInclude(redactedJson, "DUMMY_JSON_PLT");
    assert.notInclude(redactedJson, "DUMMY_JSON_TOK");
    assert.include(redactedJson, "\\u003fplt\\u003d<redacted>\\u0026token\\u003d<redacted>");

    const doubleJsonEscaped =
      '{"nested":"https://accounts.google.com/v3/signin/continue\\\\u003fplt\\\\u003dSECRET_DBL_JSON\\\\u0026code\\\\u003dSECRET_DBL_CODE"}';
    const redactedDoubleJson = redactUrlCredentials(doubleJsonEscaped);
    assert.notInclude(redactedDoubleJson, "SECRET_DBL_JSON");
    assert.notInclude(redactedDoubleJson, "SECRET_DBL_CODE");
    assert.include(
      redactedDoubleJson,
      "\\\\u003fplt\\\\u003d<redacted>\\\\u0026code\\\\u003d<redacted>",
    );

    const clean = "Command failed with exit code=1 (100% done) and status token=ok";
    assert.strictEqual(redactUrlCredentials(clean), clean);
  });

  it("redacts HTML-entity ampersands, JSON-escaped fragments, and URL authority userinfo idempotently", () => {
    const htmlEntities =
      "https://accounts.google.com/v3/signin/continue?foo=1&amp;plt=SECRET_AMP&AMP;token=SECRET_UPPER_AMP&#38;code=SECRET_DEC&#0038;access_token=SECRET_DEC_PAD&#x26;id_token=SECRET_HEX&#X0026;refresh_token=SECRET_HEX_PAD&amp;state=ok";
    const redactedHtml = redactUrlCredentials(htmlEntities);
    assert.equal(
      redactedHtml,
      "https://accounts.google.com/v3/signin/continue?foo=1&amp;plt=<redacted>&AMP;token=<redacted>&#38;code=<redacted>&#0038;access_token=<redacted>&#x26;id_token=<redacted>&#X0026;refresh_token=<redacted>&amp;state=ok",
    );
    assert.equal(redactUrlCredentials(redactedHtml), redactedHtml);

    const escapedFragments = [
      '{"url":"https://example.test/cb\\u0023token=SECRET_FRAG_1\\u0026access_token=SECRET_FRAG_2"}',
      '{"nested":"https://example.test/cb\\\\u0023plt\\\\u003dSECRET_FRAG_3\\\\u0026code\\\\u003dSECRET_FRAG_4"}',
    ];
    for (const input of escapedFragments) {
      const redacted = redactUrlCredentials(input);
      assert.notInclude(redacted, "SECRET_FRAG_");
      assert.include(redacted, "<redacted>");
      assert.equal(redactUrlCredentials(redacted), redacted);
    }
    assert.equal(
      redactUrlCredentials(escapedFragments[0]!),
      '{"url":"https://example.test/cb\\u0023token=<redacted>\\u0026access_token=<redacted>"}',
    );
    assert.equal(
      redactUrlCredentials(escapedFragments[1]!),
      '{"nested":"https://example.test/cb\\\\u0023plt\\\\u003d<redacted>\\\\u0026code\\\\u003d<redacted>"}',
    );

    const userinfoCases = [
      ["https://alice:secret@example.test/path", "https://<redacted>@example.test/path"],
      ["https://TOKEN@example.test/org/repo.git", "https://<redacted>@example.test/org/repo.git"],
      ["https://user@corp:pass@example.test/path", "https://<redacted>@example.test/path"],
      [
        "continue%3Dhttps://user:pass@example.test/p",
        "continue%3Dhttps://<redacted>@example.test/p",
      ],
      [
        "postgres://admin:dbpass@db.internal:5432/app",
        "postgres://<redacted>@db.internal:5432/app",
      ],
      [
        "https://alice:secret@example.test/path?plt=SECRET_PLT&amp;code=SECRET_CODE",
        "https://<redacted>@example.test/path?plt=<redacted>&amp;code=<redacted>",
      ],
      [
        "+https://alice:secret@example.test/org/repo.git",
        "+https://<redacted>@example.test/org/repo.git",
      ],
      [
        "-https://alice:secret@example.test/org/repo.git",
        "-https://<redacted>@example.test/org/repo.git",
      ],
      ["1-https://alice:secret@example.test/path", "1-https://<redacted>@example.test/path"],
      [
        "very-long-prefix-name-https://alice:secret@example.test/path",
        "very-long-prefix-name-https://<redacted>@example.test/path",
      ],
      [
        "postgresql+asyncpg://alice:SECRET@db.internal/app",
        "postgresql+asyncpg://<redacted>@db.internal/app",
      ],
      [
        '{"err":"\\u0022postgres+psycopg2://alice:SECRET@db.internal/app"}',
        '{"err":"\\u0022postgres+psycopg2://<redacted>@db.internal/app"}',
      ],
    ] as const;
    for (const [input, expected] of userinfoCases) {
      const redacted = redactUrlCredentials(input);
      assert.equal(redacted, expected);
      assert.equal(redactUrlCredentials(redacted), redacted);
    }

    const preserved = [
      "Contact alice@example.com for help",
      "https://example.test/@scope/pkg",
      "https://example.test/?email=a@b.com",
      "https://foo@ ",
      "https://foo@",
      "refresh_token=[REDACTED]",
      "access_token=[REDACTED],retry_after=30",
      "access_token=[REDACTED];status=401",
      "refresh_token=[REDACTED]) ok",
      "refresh_token=[REDACTED])) ok",
      "(https://example.test/cb?refresh_token=[REDACTED]) ok",
    ];
    for (const input of preserved) {
      assert.strictEqual(redactUrlCredentials(input), input);
    }
    assert.equal(
      redactUrlCredentials("refresh_token=[REDACTED]LEAK&plt=SECRET_AFTER_PLACEHOLDER"),
      "refresh_token=<redacted>&plt=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("https://example.test/cb?access_token=[REDACTED],REAL_SECRET&state=ok"),
      "https://example.test/cb?access_token=<redacted>&state=ok",
    );
    assert.equal(
      redactUrlCredentials("https://example.test/cb?access_token=[REDACTED],,REAL_SECRET&state=ok"),
      "https://example.test/cb?access_token=<redacted>&state=ok",
    );
    assert.equal(
      redactUrlCredentials("https://example.test/cb?access_token=[REDACTED]);REAL_SECRET&state=ok"),
      "https://example.test/cb?access_token=<redacted>&state=ok",
    );
    assert.equal(
      redactUrlCredentials("https://example.test/cb?access_token=[REDACTED]),REAL_SECRET&state=ok"),
      "https://example.test/cb?access_token=<redacted>&state=ok",
    );
    assert.equal(
      redactUrlCredentials(
        "https://example.test/cb?access_token=[REDACTED],<REAL_SECRET>&state=ok",
      ),
      "https://example.test/cb?access_token=<redacted>&state=ok",
    );
    assert.equal(
      redactUrlCredentials("refresh_token=[REDACTED])LEAK_AFTER_PAREN"),
      "refresh_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("refresh_token=[REDACTED]))LEAK_AFTER_DOUBLE_PAREN"),
      "refresh_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],refresh_token=REAL_SECRET"),
      "access_token=[REDACTED],refresh_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],plt=SECRET_PLT"),
      "access_token=[REDACTED],plt=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],token=REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],api_key=REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],,REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],token=[REDACTED]REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],api_key=[REDACTED]REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],secret=[REDACTED],,REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],token=[REDACTED],REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],token=[REDACTED]"),
      "access_token=[REDACTED],token=[REDACTED]",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],error_code=invalid_grant"),
      "access_token=[REDACTED],error_code=invalid_grant",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],auth_mode=oauth"),
      "access_token=[REDACTED],auth_mode=oauth",
    );
    assert.equal(
      redactUrlCredentials("https://example.test/cb?foo=1&#0000038;code=SECRET_MANY_ZEROS"),
      "https://example.test/cb?foo=1&#0000038;code=<redacted>",
    );
    assert.equal(
      redactUrlCredentials(
        "access_token=[REDACTED],url=https://accounts.google.com/v3/signin/continue?plt=REAL_PLT_SECRET",
      ),
      "access_token=[REDACTED],url=https://accounts.google.com/v3/signin/continue?plt=<redacted>",
    );
    assert.equal(
      redactUrlCredentials(
        "access_token=[REDACTED],continue=https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3DREAL_PLT_ENC",
      ),
      "access_token=[REDACTED],continue=https%3A%2F%2Faccounts.google.com%2Fv3%2Fsignin%2Fcontinue%3Fplt%3D<redacted>",
    );
    assert.equal(
      redactUrlCredentials(
        "access_token=[REDACTED],cb=https://example.test/cb?code=REAL_CODE_SECRET&plt=SECOND_PLT",
      ),
      "access_token=[REDACTED],cb=https://example.test/cb?code=<redacted>&plt=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED],url=https://a.test/?foo=1,api_key=REAL_SECRET"),
      "access_token=<redacted>",
    );
    assert.equal(
      redactUrlCredentials("access_token=[REDACTED]. Please retry."),
      "access_token=[REDACTED]. Please retry.",
    );
    assert.equal(
      redactUrlCredentials("Failed (access_token=[REDACTED]): unauthorized"),
      "Failed (access_token=[REDACTED]): unauthorized",
    );
    assert.equal(redactUrlCredentials("access_token=[REDACTED].LEAK"), "access_token=<redacted>");

    const round6Cases = [
      [
        "wss://example.test/ws?wsticket=part1\\u003cSECRET_PART2&foo=bar",
        "wss://example.test/ws?wsticket=<redacted>&foo=bar",
      ],
      [
        "https://example.test/?plt\\u003dabc\\u003eSECRET_TAIL\\u0026ok=1",
        "https://example.test/?plt\\u003d<redacted>\\u0026ok=1",
      ],
      ["error%20access_token=SUPERSECRET%20ok=1", "error%20access_token=<redacted>%20ok=1"],
      ["error%20plt=SUPERSECRET%20ok=1", "error%20plt=<redacted>%20ok=1"],
      ["error%20code=403", "error%20code=403"],
      ["https://example.test/?token=<reda…", "https://example.test/?token=<reda…"],
      [
        "https://example.test/?access_token=[REDACTED]…",
        "https://example.test/?access_token=[REDACTED]…",
      ],
      ["access_token=FIRST_TOK,plt\\u003dSECRET_PLT", "access_token=<redacted>"],
      [
        '{"url":"wss://example.test/ws?wsticket=part1\\u003dSECRET_PART2"}',
        '{"url":"wss://example.test/ws?wsticket=<redacted>"}',
      ],
      ["access_token=[REDACTED],bearer=SECRET_BEARER", "access_token=<redacted>"],
      ["access_token=[REDACTED],cookie=SECRET_COOKIE", "access_token=<redacted>"],
      ["access_token=[REDACTED],signature=SECRET_SIG", "access_token=<redacted>"],
      ["access_token=[REDACTED],x=plt=SECRET_NESTED", "access_token=<redacted>"],
      [
        "access_token=[REDACTED],url=https://a.test/?foo=1,x=plt=SECRET_NESTED",
        "access_token=<redacted>",
      ],
      ["access_token=[REDACTED],progress=100%,x=plt=SECRET_NESTED", "access_token=<redacted>"],
      ["access_token=[REDACTED],x=a%21plt=SECRET_NESTED", "access_token=<redacted>"],
      [
        "access_token=[REDACTED],url=https://a.test/cb?x=plt=SECRET_NESTED",
        "access_token=<redacted>",
      ],
      ["access_token=[REDACTED],x=plt\\u003dSECRET_NESTED", "access_token=<redacted>"],
      [
        JSON.stringify({ err: "line1\nplt=SUPERSECRET_PLT\nwsticket=SUPERSECRET_WS" }),
        JSON.stringify({ err: "line1\nplt=<redacted>\nwsticket=<redacted>" }),
      ],
      [
        JSON.stringify({ err: "exit code=1\nstatus code=403" }),
        JSON.stringify({ err: "exit code=1\nstatus code=403" }),
      ],
    ] as const;
    for (const [input, expected] of round6Cases) {
      const redacted = redactUrlCredentials(input);
      assert.equal(redacted, expected);
      assert.equal(redactUrlCredentials(redacted), redacted);
    }

    // Nested JSON.stringify (3-backslash \\\" quote) must stay valid JSON and idempotent across sink redaction
    const nestedRecord = JSON.stringify({
      body: JSON.stringify({ url: "https://example.test/cb?plt=NESTED_JSON_SECRET" }),
    });
    const redactedNested = redactUrlCredentials(nestedRecord);
    assert.notInclude(redactedNested, "NESTED_JSON_SECRET");
    assert.equal(redactUrlCredentials(redactedNested), redactedNested);
    const parsedOuter = JSON.parse(redactedNested) as { readonly body: string };
    const parsedInner = JSON.parse(parsedOuter.body) as { readonly url: string };
    assert.equal(parsedInner.url, "https://example.test/cb?plt=<redacted>");
    const doublePassNested = redactUrlCredentials(
      JSON.stringify({
        body: redactUrlCredentials(
          JSON.stringify({ url: "https://example.test/cb?plt=NESTED_JSON_SECRET" }),
        ),
      }),
    );
    assert.equal(redactUrlCredentials(doublePassNested), doublePassNested);
    assert.doesNotThrow(() => JSON.parse(doublePassNested));
  });

  it("stays linear on long runs of backslashes, percent sequences, entities, and userinfo patterns without quadratic backtracking", () => {
    const adversarialBackslashes = `x=1 ${"\\".repeat(200_000)}x`;
    assert.strictEqual(redactUrlCredentials(adversarialBackslashes), adversarialBackslashes);

    const adversarialPercent = `${"%25".repeat(50_000)}&${"a".repeat(50_000)}`;
    assert.strictEqual(redactUrlCredentials(adversarialPercent), adversarialPercent);

    const adversarialUserinfo = `https://${"a".repeat(100_000)}@`;
    assert.strictEqual(redactUrlCredentials(adversarialUserinfo), adversarialUserinfo);
    assert.strictEqual(
      redactUrlCredentials(`https://${"@".repeat(100_000)}`),
      `https://${"@".repeat(100_000)}`,
    );
    const adversarialEntities = `${"&amp;".repeat(50_000)}${"&#000038;".repeat(50_000)}=1`;
    assert.strictEqual(redactUrlCredentials(adversarialEntities), adversarialEntities);

    const repeatedPlaceholderQuery = "?plt=[REDACTED]),".repeat(16_000);
    assert.equal(redactUrlCredentials(repeatedPlaceholderQuery), "?plt=<redacted>");
    const repeatedPlaceholderKv = "plt=[REDACTED],".repeat(16_000);
    assert.strictEqual(redactUrlCredentials(repeatedPlaceholderKv), repeatedPlaceholderKv);
    const repeatedSafeSiblings = `access_token=[REDACTED]${",a=1".repeat(20_000)}`;
    assert.strictEqual(redactUrlCredentials(repeatedSafeSiblings), repeatedSafeSiblings);

    const manyMatches = Array.from({ length: 10_000 }, (_, i) => `plt=secret${i}`).join("&");
    const redactedMany = redactUrlCredentials(manyMatches);
    assert.notInclude(redactedMany, "secret0");
    assert.notInclude(redactedMany, "secret9999");
  });

  it("redacts URL credentials inside Effect Causes including Error subclass data and cause properties while preserving non-secret causes", () => {
    const secretCause = Cause.fail(
      new Error("Verify at https://accounts.google.com/v3/signin/continue?plt=SECRET_CAUSE_PLT"),
    );
    const redactedCause = redactCauseCredentials(secretCause);
    const pretty = Cause.pretty(redactedCause);
    assert.include(pretty, "plt=<redacted>");
    assert.notInclude(pretty, "SECRET_CAUSE_PLT");

    const proseCause = Cause.fail(new Error("Command failed with exit code=1 and status code=403"));
    assert.strictEqual(redactCauseCredentials(proseCause), proseCause);

    class SubclassError extends Schema.TaggedError<SubclassError>()("AcpRequestError", {
      errorMessage: Schema.String,
      data: Schema.Unknown,
      cause: Schema.optionalKey(Schema.Defect()),
    }) {
      override get message() {
        return this.errorMessage;
      }
    }

    const hiddenSecretUrl =
      "https://accounts.google.com/v3/signin/continue?plt=DUMMY_HIDDEN_DATA_PLT_1701";
    const nestedHiddenUrl =
      "https://accounts.google.com/v3/signin/continue?%70lt=DUMMY_NESTED_CAUSE_PLT_1702";
    const errWithHiddenData = new SubclassError({
      errorMessage: "PERMISSION_DENIED (code 403): VALIDATION_REQUIRED",
      data: { validation_url: hiddenSecretUrl },
      cause: {
        code: -32603,
        message: "VALIDATION_REQUIRED",
        data: { validation_url: nestedHiddenUrl },
      },
    });
    const hiddenCause = Cause.fail(errWithHiddenData);
    // Cause.pretty omits `.data` and plain-object `.cause.data`
    assert.notInclude(Cause.pretty(hiddenCause), "DUMMY_HIDDEN_DATA_PLT_1701");
    assert.notInclude(Cause.pretty(hiddenCause), "DUMMY_NESTED_CAUSE_PLT_1702");

    const redactedHidden = redactCauseCredentials(hiddenCause);
    assert.notStrictEqual(redactedHidden, hiddenCause);
    const reason = redactedHidden.reasons[0];
    assert.equal(reason?._tag, "Fail");
    if (reason?._tag === "Fail") {
      const sanitizedErr = reason.error as Error & {
        readonly data?: { readonly validation_url?: string };
        readonly cause?: { readonly data?: { readonly validation_url?: string } };
      };
      assert.equal(sanitizedErr.name, "AcpRequestError");
      assert.equal(
        sanitizedErr.data?.validation_url,
        "https://accounts.google.com/v3/signin/continue?plt=<redacted>",
      );
      assert.equal(
        sanitizedErr.cause?.data?.validation_url,
        "https://accounts.google.com/v3/signin/continue?%70lt=<redacted>",
      );
    }

    const cyclic = new SubclassError({
      errorMessage: "cyclic",
      data: {
        url: "https://accounts.google.com/v3/signin/continue?plt=CYCLIC_PLT",
      },
    });
    Object.defineProperty(cyclic, "cause", {
      value: cyclic,
      writable: true,
      enumerable: false,
      configurable: true,
    });
    const redactedCyclic = redactCauseCredentials(Cause.die(cyclic));
    const cyclicReason = redactedCyclic.reasons[0];
    assert.equal(cyclicReason?._tag, "Die");
    if (cyclicReason?._tag === "Die") {
      const sanitizedCyclic = cyclicReason.defect as Error & {
        readonly data?: { readonly url?: string };
        readonly cause?: unknown;
      };
      assert.strictEqual(sanitizedCyclic.cause, sanitizedCyclic);
      assert.equal(
        sanitizedCyclic.data?.url,
        "https://accounts.google.com/v3/signin/continue?plt=<redacted>",
      );
    }

    const redactableValue = {
      url: "https://accounts.google.com/v3/signin/continue?plt=REDACTABLE_SELF",
      [Redactable.symbolRedactable]() {
        return { url: "<custom-redacted>" };
      },
    };
    const redactableCause = Cause.fail(redactableValue);
    assert.strictEqual(redactCauseCredentials(redactableCause), redactableCause);
  });
});

const TraceRecordLine = Schema.Struct({
  name: Schema.String,
  spanId: Schema.String,
  parentSpanId: Schema.optional(Schema.String),
  attributes: Schema.Record(Schema.String, Schema.Unknown),
  events: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      attributes: Schema.Record(Schema.String, Schema.Unknown),
    }),
  ),
  exit: Schema.optional(
    Schema.Struct({
      _tag: Schema.String,
    }),
  ),
});

const decodeTraceRecordLine = Schema.decodeUnknownSync(Schema.fromJsonString(TraceRecordLine));

const makeRecord = (name: string, suffix = ""): TraceRecord => ({
  type: "effect-span",
  name,
  traceId: `trace-${name}-${suffix}`,
  spanId: `span-${name}-${suffix}`,
  sampled: true,
  kind: "internal",
  startTimeUnixNano: "1",
  endTimeUnixNano: "2",
  durationMs: 1,
  attributes: {
    payload: suffix,
  },
  events: [],
  links: [],
  exit: {
    _tag: "Success",
  },
});

const readTraceRecords = Effect.fn("readTraceRecords")(function* (tracePath: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  return (yield* fileSystem.readFileString(tracePath))
    .trim()
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => decodeTraceRecordLine(line));
});

const layerTest = (tracePath: string) =>
  Layer.mergeAll(
    Layer.effect(
      Tracer.Tracer,
      makeLocalFileTracer({
        filePath: tracePath,
        maxBytes: 1024 * 1024,
        maxFiles: 2,
        batchWindowMs: 10_000,
      }),
    ),
    Logger.layer([Logger.tracerLogger], { mergeWithExisting: false }),
    Layer.succeed(References.MinimumLogLevel, "Info"),
  );

const nodeServicesIt = it.layer(NodeServices.layer);

describe("truncateTraceAttributes", () => {
  it("clamps oversized strings at any depth without mutating the input", () => {
    const stack = "s".repeat(2_000);
    const attributes = {
      "db.query.text": "q".repeat(2_000),
      short: "ok",
      error: { name: "Error", stack, nested: ["a".repeat(2_000)] },
    };
    const truncated = truncateTraceAttributes(attributes);

    assert.equal((truncated["db.query.text"] as string).length, 200 + "…[truncated]".length);
    assert.equal(truncated["short"], "ok");
    const error = truncated["error"] as { stack: string; nested: Array<string> };
    assert.equal(error.stack.length, 500 + "…[truncated]".length);
    assert.equal(error.nested[0]?.length, 500 + "…[truncated]".length);
    assert.equal(attributes.error.stack, stack);
  });
});

describe("observability", () => {
  it.effect(
    "preserves successful results without retaining them in sampled or unsampled spans",
    () =>
      Effect.gen(function* () {
        for (const sampled of [true, false]) {
          const delegates: Array<Tracer.Span> = [];
          const records: Array<EffectTraceRecord> = [];
          const tracer = yield* makeLocalFileTracer({
            filePath: "unused",
            maxBytes: 1024,
            maxFiles: 1,
            batchWindowMs: 10_000,
            sink: {
              filePath: "unused",
              push: (record) => {
                if (record.type === "effect-span") records.push(record);
              },
              flush: Effect.void,
              flushSync: () => {},
              close: () => Effect.void,
            },
            delegate: Tracer.make({
              span: (options) => {
                const span = new Tracer.NativeSpan({ ...options, sampled });
                delegates.push(span);
                return span;
              },
            }),
          });
          const payload = { turnItems: [{ output: [{ text: "synthetic-result" }] }] };
          let span: Tracer.Span | undefined;
          const result = yield* Effect.gen(function* () {
            span = yield* Effect.currentSpan;
            return payload;
          }).pipe(
            Effect.withSpan("read-thread-projection"),
            Effect.provideService(Tracer.Tracer, tracer),
          );
          assert.strictEqual(result, payload);
          assert.isDefined(span);
          for (const completed of [span!, ...delegates]) {
            assert.equal(completed.status._tag, "Ended");
            if (completed.status._tag === "Ended") {
              assert.deepStrictEqual(completed.status.exit, Exit.void);
            }
          }
          assert.deepStrictEqual(
            records.map((record) => record.exit),
            sampled ? [{ _tag: "Success" }] : [],
          );
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("normalizes circular arrays, maps, and sets without recursing forever", () => {
    const array: Array<unknown> = ["alpha"];
    array.push(array);

    const map = new Map<string, unknown>();
    map.set("self", map);

    const set = new Set<unknown>();
    set.add(set);

    assert.deepStrictEqual(
      compactTraceAttributes({
        array,
        map,
        set,
      }),
      {
        array: ["alpha", "[Circular]"],
        map: { self: "[Circular]" },
        set: ["[Circular]"],
      },
    );
  });

  it("keeps an object that appears twice without marking it circular", () => {
    const shared = { cwd: "/repo" };
    const sharedMap = new Map([["cwd", "/repo"]]);
    const sharedSet = new Set(["/repo"]);

    assert.deepStrictEqual(
      compactTraceAttributes({
        ctx: { first: shared, second: shared },
        list: [shared, shared],
        maps: [sharedMap, sharedMap],
        sets: [sharedSet, sharedSet],
      }),
      {
        ctx: { first: { cwd: "/repo" }, second: { cwd: "/repo" } },
        list: [{ cwd: "/repo" }, { cwd: "/repo" }],
        maps: [{ cwd: "/repo" }, { cwd: "/repo" }],
        sets: [["/repo"], ["/repo"]],
      },
    );
  });

  it("caps shared references that would expand exponentially", () => {
    let graph: Record<string, unknown> = { cwd: "/repo" };
    for (let depth = 0; depth < 20; depth++) {
      graph = { left: graph, right: graph };
    }

    const serialized = JSON.stringify(compactTraceAttributes({ graph }));

    assert.include(serialized, '"[Truncated]"');
    assert.isBelow(serialized.length, 1_000_000);
  });

  nodeServicesIt("node services", (it) => {
    it.effect(
      "keeps trace producers responsive during a slow write, bounds the backlog, and drains on close",
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const directory = yield* fs.makeTempDirectoryScoped({
            prefix: "t3-trace-backpressure-",
          });
          const tracePath = path.join(directory, "trace.ndjson");
          const started = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const closed = yield* Ref.make(false);
          const sink = yield* makeTraceSink({
            filePath: tracePath,
            maxBytes: 5_000_000,
            maxFiles: 2,
            batchWindowMs: 10_000,
          }).pipe(
            Effect.provideService(FileSystem.FileSystem, {
              ...fs,
              writeFile: (file, data, options) =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                  yield* fs.writeFile(file, data, options);
                }),
            }),
          );
          sink.push(makeRecord("first"));
          const flushing = yield* Effect.forkChild(sink.flush);
          yield* Deferred.await(started);
          for (let index = 0; index < 2_000; index++) sink.push(makeRecord("small", String(index)));
          for (let index = 0; index < 3; index++)
            sink.push({ ...makeRecord("large"), attributes: { payload: "x".repeat(2_000_000) } });
          const closing = yield* Effect.forkChild(
            sink.close().pipe(Effect.tap(() => Ref.set(closed, true))),
          );
          yield* Effect.yieldNow;
          assert.isFalse(yield* Ref.get(closed));
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(flushing);
          yield* Fiber.join(closing);
          const records = yield* readTraceRecords(tracePath);
          assert.deepStrictEqual(
            records.map((record) => record.name),
            ["first", ...Array.from({ length: 2_000 }, () => "small"), "large", "large"],
          );
          assert.isTrue(yield* Ref.get(closed));
        }),
    );

    it.effect("writes buffered records synchronously for process exit", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-trace-sink-" });
        const tracePath = path.join(tempDir, "exit.trace.ndjson");
        const sink = yield* makeTraceSink({
          filePath: tracePath,
          maxBytes: 1_000_000,
          maxFiles: 2,
          batchWindowMs: 10_000,
        });

        sink.push(makeRecord("before-exit"));
        sink.flushSync();
        sink.flushSync();

        const records = yield* readTraceRecords(tracePath);
        assert.deepStrictEqual(
          records.map((record) => record.name),
          ["before-exit"],
        );
      }),
    );

    it.effect("keeps every trace file within the configured limit for threshold flushes", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-trace-sink-" });
        const tracePath = path.join(tempDir, "shared.trace.ndjson");
        const maxBytes = 1_024;

        const sink = yield* makeTraceSink({
          filePath: tracePath,
          maxBytes,
          maxFiles: 2,
          batchWindowMs: 10_000,
        });

        for (let index = 0; index < 256; index += 1) {
          sink.push(makeRecord("threshold", `${index}-${"x".repeat(48)}`));
        }
        yield* sink.close();

        const matchingFiles = (yield* fileSystem.readDirectory(tempDir)).filter(
          (entry) => entry === "shared.trace.ndjson" || entry.startsWith("shared.trace.ndjson."),
        );
        assert.include(matchingFiles, "shared.trace.ndjson.1");
        for (const entry of matchingFiles) {
          const stat = yield* fileSystem.stat(path.join(tempDir, entry));
          assert.isAtMost(Number(stat.size), maxBytes, entry);
        }
      }),
    );

    it.effect("drops a single trace record that cannot fit within the configured limit", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-trace-sink-" });
        const tracePath = path.join(tempDir, "shared.trace.ndjson");
        const maxBytes = 1_024;

        const sink = yield* makeTraceSink({
          filePath: tracePath,
          maxBytes,
          maxFiles: 2,
          batchWindowMs: 10_000,
        });

        sink.push(makeRecord("oversized", "x".repeat(maxBytes * 2)));
        sink.push(makeRecord("retained"));
        yield* sink.close();

        const records = yield* readTraceRecords(tracePath);
        const stat = yield* fileSystem.stat(tracePath);
        assert.deepEqual(
          records.map((record) => record.name),
          ["retained"],
        );
        assert.isAtMost(Number(stat.size), maxBytes);
      }),
    );

    it.effect("keeps WebSocket credentials out of trace files and exported spans", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-trace-sink-" });
        const tracePath = path.join(tempDir, "shared.trace.ndjson");
        const sink = yield* makeTraceSink({
          filePath: tracePath,
          maxBytes: 1024 * 1024,
          maxFiles: 2,
          batchWindowMs: 10_000,
        });
        const delegates: Array<Tracer.NativeSpan> = [];
        const tracer = yield* makeLocalFileTracer({
          filePath: tracePath,
          maxBytes: 1024 * 1024,
          maxFiles: 2,
          batchWindowMs: 10_000,
          sink,
          delegate: Tracer.make({
            span: (options) => {
              const span = new Tracer.NativeSpan(options);
              delegates.push(span);
              return span;
            },
          }),
        });
        const url = "/ws?wsTicket=ticket-secret&dpop=proof-secret&clientSurface=web";

        yield* Effect.annotateCurrentSpan({
          "url.full": `http://127.0.0.1:3773${url}`,
          "url.query": url.slice(4),
        }).pipe(
          Effect.withSpan("GET", { kind: "server" }),
          Effect.provideService(Tracer.Tracer, tracer),
        );
        yield* Effect.die(new Error(`RequestParseError (GET ${url})`)).pipe(
          Effect.tapCause((cause) => Effect.logError(cause)),
          Effect.withSpan("DeviceHubProxy.proxyWebSocket"),
          Effect.provideService(Tracer.Tracer, tracer),
          Effect.provide(Logger.layer([Logger.tracerLogger], { mergeWithExisting: false })),
          Effect.exit,
        );
        yield* sink.close();

        const written = yield* fileSystem.readFileString(tracePath);
        const exported = delegates.map((span) =>
          JSON.stringify({
            attributes: Object.fromEntries(span.attributes),
            events: span.events.map(([, , attributes]) => attributes),
            exit:
              span.status._tag === "Ended" && Exit.isFailure(span.status.exit)
                ? Cause.pretty(span.status.exit.cause)
                : "",
          }),
        );
        for (const text of [written, ...exported]) {
          assert.notInclude(text, "ticket-secret");
          assert.notInclude(text, "proof-secret");
        }
        assert.include(written, "wsTicket=<redacted>&dpop=<redacted>&clientSurface=web");
        assert.include(exported.join("\n"), "effect.cause");
      }),
    );

    it.effect(
      "redacts plt and OAuth query credentials from trace files and OTLP delegates without corrupting prose",
      () =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-trace-sink-" });
          const tracePath = path.join(tempDir, "plt.trace.ndjson");
          const sink = yield* makeTraceSink({
            filePath: tracePath,
            maxBytes: 1024 * 1024,
            maxFiles: 2,
            batchWindowMs: 10_000,
          });
          const delegates: Array<Tracer.NativeSpan> = [];
          const tracer = yield* makeLocalFileTracer({
            filePath: tracePath,
            maxBytes: 1024 * 1024,
            maxFiles: 2,
            batchWindowMs: 10_000,
            sink,
            delegate: Tracer.make({
              span: (options) => {
                const span = new Tracer.NativeSpan(options);
                delegates.push(span);
                return span;
              },
            }),
          });

          const errorMessage =
            'Validation failed (exit code=1, status code=403): {"validation_url":"https://accounts.google.com/v3/signin/continue?plt=PLT_SECRET_1&access_token=AT_SECRET\\u0026plt=PLT_SECRET_2","cb":"https://example.com/cb?code=CODE_SECRET&token=TOK_SECRET"}';

          yield* Effect.annotateCurrentSpan({
            "validation.url":
              "https://accounts.google.com/v3/signin/continue?foo=1&plt=PLT_ATTR_SECRET",
          }).pipe(
            Effect.andThen(Effect.die(new Error(errorMessage))),
            Effect.tapCause((cause) => Effect.logError(errorMessage, cause)),
            Effect.withSpan("acp.prompt"),
            Effect.provideService(Tracer.Tracer, tracer),
            Effect.provide(Logger.layer([Logger.tracerLogger], { mergeWithExisting: false })),
            Effect.exit,
          );
          yield* sink.close();

          const written = yield* fileSystem.readFileString(tracePath);
          const exported = delegates.map((span) =>
            JSON.stringify({
              attributes: Object.fromEntries(span.attributes),
              events: span.events.map(([name, , attributes]) => ({ name, attributes })),
              exit:
                span.status._tag === "Ended" && Exit.isFailure(span.status.exit)
                  ? Cause.pretty(span.status.exit.cause)
                  : "",
            }),
          );

          for (const text of [written, ...exported]) {
            assert.notInclude(text, "PLT_SECRET_1");
            assert.notInclude(text, "PLT_SECRET_2");
            assert.notInclude(text, "PLT_ATTR_SECRET");
            assert.notInclude(text, "AT_SECRET");
            assert.notInclude(text, "CODE_SECRET");
            assert.notInclude(text, "TOK_SECRET");
            assert.include(text, "exit code=1");
            assert.include(text, "status code=403");
            assert.include(text, "plt=<redacted>");
            assert.include(text, "access_token=<redacted>");
            assert.include(text, "code=<redacted>");
            assert.include(text, "token=<redacted>");
          }
        }),
    );

    it.effect("drops only the invalid trace record when serialization fails", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-trace-sink-" });
        const tracePath = path.join(tempDir, "shared.trace.ndjson");

        const sink = yield* makeTraceSink({
          filePath: tracePath,
          maxBytes: 1024,
          maxFiles: 2,
          batchWindowMs: 10_000,
        });

        const circular: Array<unknown> = [];
        circular.push(circular);

        sink.push(makeRecord("alpha"));
        sink.push({
          ...makeRecord("invalid"),
          attributes: {
            circular,
          },
        } as TraceRecord);
        sink.push(makeRecord("beta"));
        yield* sink.close();

        const lines = yield* readTraceRecords(tracePath);

        assert.deepStrictEqual(
          lines.map((line) => line.name),
          ["alpha", "beta"],
        );
      }),
    );

    it.effect("writes nested spans to disk and captures log messages as span events", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-local-tracer-" });
        const tracePath = path.join(tempDir, "shared.trace.ndjson");

        yield* Effect.scoped(
          Effect.gen(function* () {
            const program = Effect.gen(function* () {
              yield* Effect.annotateCurrentSpan({
                "demo.parent": true,
              });
              yield* Effect.logInfo("parent event");
              yield* Effect.gen(function* () {
                yield* Effect.annotateCurrentSpan({
                  "demo.child": true,
                });
                yield* Effect.logInfo("child event");
              }).pipe(Effect.withSpan("child-span"));
            }).pipe(Effect.withSpan("parent-span"));

            yield* program.pipe(Effect.provide(layerTest(tracePath)));
          }),
        );

        const records = yield* readTraceRecords(tracePath);
        assert.equal(records.length, 2);

        const parent = records.find((record) => record.name === "parent-span");
        const child = records.find((record) => record.name === "child-span");

        assert.notEqual(parent, undefined);
        assert.notEqual(child, undefined);
        if (!parent || !child) {
          return;
        }

        assert.equal(child.parentSpanId, parent.spanId);
        assert.equal(parent.attributes["demo.parent"], true);
        assert.equal(child.attributes["demo.child"], true);
        assert.equal(
          parent.events.some((event) => event.name === "parent event"),
          true,
        );
        assert.equal(
          child.events.some((event) => event.name === "child event"),
          true,
        );
        assert.equal(
          child.events.some((event) => event.attributes["effect.logLevel"] === "INFO"),
          true,
        );
      }),
    );

    it.effect("serializes interrupted spans with an interrupted exit status", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-local-tracer-" });
        const tracePath = path.join(tempDir, "shared.trace.ndjson");

        yield* Effect.scoped(
          Effect.exit(
            Effect.interrupt.pipe(
              Effect.withSpan("interrupt-span"),
              Effect.provide(layerTest(tracePath)),
            ),
          ),
        );

        const records = yield* readTraceRecords(tracePath);
        assert.equal(records.length, 1);
        assert.equal(records[0]?.name, "interrupt-span");
        assert.equal(records[0]?.exit?._tag, "Interrupted");
      }),
    );
  });
});

describe("decodeOtlpTraceRecords", () => {
  it("decodes spans when OTLP/JSON omits unset and empty fields", () => {
    // Protobuf JSON leaves out unset and empty fields, so this batch is valid.
    const payload = cast<unknown, OtlpTracer.TraceData>({
      resourceSpans: [
        {},
        {
          scopeSpans: [
            {},
            {
              spans: [
                {
                  traceId: "trace-1",
                  spanId: "span-1",
                  name: "browser.request",
                  startTimeUnixNano: "1000000",
                  endTimeUnixNano: "3000000",
                  attributes: [{ key: "tags", value: { arrayValue: {} } }],
                },
              ],
            },
          ],
        },
      ],
    });

    assert.deepStrictEqual(decodeOtlpTraceRecords(cast<unknown, OtlpTracer.TraceData>({})), []);
    assert.deepStrictEqual(decodeOtlpTraceRecords(payload), [
      {
        type: "otlp-span",
        name: "browser.request",
        traceId: "trace-1",
        spanId: "span-1",
        sampled: true,
        kind: "internal",
        startTimeUnixNano: "1000000",
        endTimeUnixNano: "3000000",
        durationMs: 2,
        attributes: { tags: [] },
        resourceAttributes: {},
        scope: { attributes: {} },
        events: [],
        links: [],
        status: { code: "0" },
      },
    ]);
  });
});

describe("OtlpHeadersFromString", () => {
  const decode = Schema.decodeUnknownSync(OtlpHeadersFromString);

  it.each([
    {
      name: "decodes percent-encoded values",
      input: "authorization=Basic%20abc%3D%3D,x-tenant=t3",
      expected: { authorization: "Basic abc==", "x-tenant": "t3" },
    },
    {
      name: "ignores whitespace around separators",
      input: "authorization=Basic%20abc%3D%3D, x-tenant = t3 ,",
      expected: { authorization: "Basic abc==", "x-tenant": "t3" },
    },
    {
      name: "keeps literal equals signs inside a value",
      input: "authorization=Bearer abc==",
      expected: { authorization: "Bearer abc==" },
    },
    {
      name: "keeps an empty value",
      input: "x-empty=",
      expected: { "x-empty": "" },
    },
  ])("$name", ({ input, expected }) => {
    expect(decode(input)).toEqual(expected);
  });

  it.each([
    { name: "rejects a pair without a separator", input: "authorization" },
    { name: "rejects a pair without a key", input: "=value" },
    { name: "rejects a malformed percent-encoding", input: "authorization=%E0" },
  ])("$name", ({ input }) => {
    expect(() => decode(input)).toThrow();
  });
});
