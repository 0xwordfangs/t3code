import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ExecutionEnvironmentDescriptor } from "./environment.ts";
import {
  applyServerProviderDeltas,
  formatVerificationUrlDisplay,
  GoogleAccountVerificationUrl,
  OtlpEndpointCheckInput,
  resolveEnvironmentMachineKind,
  ServerConfigProviderStatusDeltasPayload,
  ServerObservability,
  ServerProvider,
  serverProviderDeltas,
  ServerProviders,
} from "./server.ts";
import { ServerSettings } from "./settings.ts";

const decodeServerProvider = Schema.decodeUnknownSync(ServerProvider);
const decodeServerProviders = Schema.decodeUnknownSync(ServerProviders);

const baseProviderSnapshot = {
  instanceId: "codex",
  driver: "codex",
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-04-10T00:00:00.000Z",
  models: [],
};

describe("ServerProvider", () => {
  it("preserves installed-version presence through the provider wire codec", () => {
    const encode = Schema.encodeSync(ServerProvider);
    for (const installation of [{}, { installedVersion: null }, { installedVersion: "1.2.3" }]) {
      const decoded = decodeServerProvider({ ...baseProviderSnapshot, ...installation });
      const wire = JSON.parse(JSON.stringify(encode(decoded)));
      expect(wire).toMatchObject({ version: "1.0.0", ...installation });
      expect(Object.hasOwn(wire, "installedVersion")).toBe(
        Object.hasOwn(installation, "installedVersion"),
      );
      expect(decodeServerProvider(wire)).toEqual(decoded);
    }
  });
});

describe("provider status deltas", () => {
  it("send a provider's lists only when they change and rebuild the full list", () => {
    const codex = decodeServerProvider({
      ...baseProviderSnapshot,
      models: [{ slug: "gpt", name: "GPT", isCustom: false, capabilities: null }],
      slashCommands: [{ name: "compact" }],
      skills: [{ name: "review", path: "/skills/review", enabled: true }],
    });
    const claude = decodeServerProvider({ ...baseProviderSnapshot, instanceId: "claudeAgent" });
    const previous = [codex];
    const next = [
      claude,
      { ...codex, checkedAt: "2026-04-10T00:05:00.000Z", skills: [], message: "Checked" },
    ];

    const deltas = serverProviderDeltas(previous, next);
    // Unchanged lists stay home; a provider the reader never saw travels whole.
    expect(deltas[0]).toEqual(claude);
    expect(deltas[1]).not.toHaveProperty("models");
    expect(deltas[1]).not.toHaveProperty("slashCommands");
    expect(deltas[1]).toMatchObject({ skills: [], message: "Checked" });

    const codec = Schema.toCodecJson(ServerConfigProviderStatusDeltasPayload);
    const wire = JSON.parse(JSON.stringify(Schema.encodeSync(codec)({ providers: deltas })));
    const received = Schema.decodeUnknownSync(codec)(wire).providers;
    // A decoding default would turn the absent slash commands into an empty list.
    expect(received[1]).not.toHaveProperty("slashCommands");
    expect(applyServerProviderDeltas(previous, received)).toEqual(next);
    // Without the provider the delta builds on, its lists are unknown.
    expect(applyServerProviderDeltas([], received)).toEqual([claude]);
  });
});

describe("server config forward compatibility", () => {
  it("drops providers this build cannot decode instead of failing the whole array", () => {
    const decodedBase = decodeServerProvider(baseProviderSnapshot);

    const parsed = decodeServerProviders([
      baseProviderSnapshot,
      { ...baseProviderSnapshot, instanceId: "future", status: "some-future-status" },
    ]);

    expect(parsed).toEqual([decodedBase]);
  });

  it("drops usage windows this build cannot decode instead of failing the provider", () => {
    const parsed = decodeServerProvider({
      ...baseProviderSnapshot,
      usageLimits: {
        checkedAt: "2026-04-10T00:00:00.000Z",
        windows: [
          { id: "primary", kind: "session", label: "Session", usedPercent: 12 },
          { id: "future", kind: "some-future-kind", label: "Future", usedPercent: 1 },
          { id: "bad", kind: "weekly", label: "Weekly", usedPercent: 120 },
        ],
      },
    });

    expect(parsed.usageLimits?.windows).toEqual([
      { id: "primary", kind: "session", label: "Session", usedPercent: 12 },
    ]);
  });
});

describe("ServerObservability", () => {
  const decodeServerObservability = Schema.decodeUnknownSync(ServerObservability);

  it("rejects an unknown endpoint source", () => {
    expect(() =>
      decodeServerObservability({
        logsDirectoryPath: "/tmp/t3/logs",
        localTracingEnabled: true,
        otlpTracesUrl: "https://collector.example.com/v1/traces",
        otlpTracesSource: "cli",
        otlpTracesEnabled: true,
        otlpMetricsEnabled: false,
      }),
    ).toThrow();
  });
});

describe("resolveEnvironmentMachineKind", () => {
  const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
  const decodeSettings = Schema.decodeUnknownSync(ServerSettings);
  const descriptor = (platform: Record<string, unknown>) =>
    decodeDescriptor({
      environmentId: "env-1",
      label: "Box",
      platform: { os: "linux", arch: "x64", ...platform },
      serverVersion: "1.0.0",
      capabilities: {},
    });

  it("prefers the user's pick over what the server detected", () => {
    expect(
      resolveEnvironmentMachineKind({
        environment: descriptor({ machine: "mac-mini" }),
        settings: decodeSettings({ environmentIcon: "laptop" }),
      }),
    ).toBe("laptop");
  });

  it("drops a machine kind this build does not know instead of failing the descriptor", () => {
    const parsed = descriptor({ machine: "toaster" });

    expect(parsed.platform.machine).toBeUndefined();
    expect(
      resolveEnvironmentMachineKind({ environment: parsed, settings: decodeSettings({}) }),
    ).toBe("server");
  });
});

describe("OtlpEndpointCheckInput", () => {
  const decode = Schema.decodeUnknownSync(OtlpEndpointCheckInput);

  it("accepts HTTP receivers only", () => {
    expect(decode({ signal: "traces", url: "https://collector.example.com/v1/traces" }).url).toBe(
      "https://collector.example.com/v1/traces",
    );
    for (const url of ["data:text/plain,ok", "file:///etc/hosts", "collector:4318/v1/traces"]) {
      expect(() => decode({ signal: "traces", url })).toThrow();
    }
  });
});

describe("GoogleAccountVerificationUrl", () => {
  const decode = Schema.decodeUnknownSync(GoogleAccountVerificationUrl);

  it("accepts only Google Accounts signin/continue URLs with allowlisted query keys, non-empty plt, and allowlisted continue targets", () => {
    for (const validUrl of [
      "https://accounts.google.com/v3/signin/continue?plt=abc",
      "https://accounts.google.com/signin/continue?plt=abc",
      "https://accounts.google.com/v3/signin/continue?sarp=1&scc=1&plt=abc&hl=en&osid=1&dsh=S-1&flowName=GlifWebSignIn&flowEntry=ServiceLogin&service=cloudcode",
      "https://accounts.google.com/v3/signin/continue?plt=abc&continue=https%3A%2F%2Fantigravity.google%2F",
      "https://accounts.google.com/v3/signin/continue?plt=abc&continue=https%3A%2F%2Fdevelopers.google.com%2Fprofile",
      "https://accounts.google.com/v3/signin/continue?plt=abc&continueUrl=https%3A%2F%2Fcodeassist.google.com%2F",
      "https://accounts.google.com/v3/signin/continue?plt=abc&continue=https%3A%2F%2Fconsole.cloud.google.com%2F",
      "https://accounts.google.com/v3/signin/continue?plt=abc&continue=https%3A%2F%2Faistudio.google.com%2F",
      "https://accounts.google.com/v3/signin/continue?plt=abc&continue=https%3A%2F%2Fgemini.google.com%2F",
    ]) {
      expect(decode(validUrl)).toBe(validUrl);
    }

    for (const invalidUrl of [
      "https://accounts.google.com/o/oauth2/v2/auth?client_id=123",
      "https://accounts.google.com/Logout",
      "https://accounts.google.com/ServiceLogin?continue=https%3A%2F%2Fgoogle.com",
      "https://@accounts.google.com/v3/signin/continue?plt=1",
      "https://accounts.google.com/v3/signin/continue",
      "https://accounts.google.com/v3/signin/continue?sarp=1&scc=1",
      "https://accounts.google.com/v3/signin/continue?plt=",
      "https://accounts.google.com/v3/signin/continue?plt=%20",
      "https://accounts.google.com/v3/signin/continue?plt=1#foo",
      "https://accounts.google.com/v3/signin/continue?plt=1#",
      "https://accounts.google.com/v3/signin/continue?plt=1&client_id=abc",
      "https://accounts.google.com/v3/signin/continue?plt=1&redirect_uri=https%3A%2F%2Fgoogle.com",
      "https://accounts.google.com/v3/signin/continue?plt=1&response_type=code",
      "https://accounts.google.com/v3/signin/continue?plt=1&scope=openid",
      "https://accounts.google.com/v3/signin/continue?plt=1&followup=https%3A%2F%2Fattacker.invalid%2Fphish",
      "https://accounts.google.com/v3/signin/continue?plt=1&unknown_param=1",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continueUrl=https%3A%2F%2Fevilgoogle.com",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=http%3A%2F%2Fantigravity.google%2F",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2F%40antigravity.google%2F",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2F.google%2F",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F&continue=https%3A%2F%2Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Faccounts.google.com%2Fo%2Foauth2%2Fv2%2Fauth%3Fclient_id%3Dattacker",
      "https://accounts.google.com/v3/signin/continue?plt=1&continueUrl=https%3A%2F%2Faccounts.google.com%2FLogout",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Faccounts.google.com%2FServiceLogin%3Fcontinue%3Dhttps%253A%252F%252Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fwww.google.com%2Furl%3Fq%3Dhttps%253A%252F%252Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fgoogle.com%2F",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fsites.google.com%2Fview%2Fattacker",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fscript.google.com%2Fmacros%2Fs%2Fdummy%2Fexec",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fdocs.google.com%2Fdocument%2Fd%2F1",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fdrive.google.com%2Ffile%2Fd%2F1",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%3A8443%2F",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%23frag",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%23",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%3Fcontinue%3Dhttps%253A%252F%252Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%3Fredirect_uri%3Dhttps%253A%252F%252Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%3Ffollowup%3Dhttps%253A%252F%252Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%3Fq%3Dhttps%253A%252F%252Fattacker.invalid",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2F%3Freturn_to%3D%2Fadmin",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Fplt%3Dnested-secret",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Fcode%3Dnested-code",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Ftoken%3Dnested-token",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Faccess_token%3Dnested-at",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Fid_token%3Dnested-id",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Frefresh_token%3Dnested-rt",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3FwsTicket%3Dnested-ticket",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3Fdpop%3Dnested-dpop",
      "https://accounts.google.com/v3/signin/continue?plt=1&continue=https%3A%2F%2Fantigravity.google%2Fauth%3F%2570lt%3Dnested-pct-plt",
    ]) {
      expect(() => decode(invalidUrl)).toThrow();
    }
  });
});

describe("formatVerificationUrlDisplay", () => {
  it("strips query tokens and fragments while surfacing continue target origin and pathname", () => {
    expect(
      formatVerificationUrlDisplay(
        "https://accounts.google.com/v3/signin/continue?plt=secret-token&sarp=1#step",
      ),
    ).toBe("https://accounts.google.com/v3/signin/continue");

    expect(
      formatVerificationUrlDisplay(
        "https://accounts.google.com/v3/signin/continue?plt=secret-token&continue=https%3A%2F%2Fantigravity.google%2F%3Ftoken%3Dhidden%23frag",
      ),
    ).toBe("https://accounts.google.com/v3/signin/continue → https://antigravity.google/");

    expect(
      formatVerificationUrlDisplay(
        "https://accounts.google.com/v3/signin/continue?plt=secret-token&continueUrl=https%3A%2F%2Fdevelopers.google.com%2Fprofile%3Fsecret%3D1",
      ),
    ).toBe(
      "https://accounts.google.com/v3/signin/continue → https://developers.google.com/profile",
    );

    expect(
      formatVerificationUrlDisplay(
        "https://accounts.google.com/v3/signin/continue?plt=secret-token&continue=javascript%3Aalert(1)",
      ),
    ).toBe("https://accounts.google.com/v3/signin/continue");
  });

  it("falls back to the raw input when URL parsing fails", () => {
    expect(formatVerificationUrlDisplay("not-a-valid-url")).toBe("not-a-valid-url");
  });
});
