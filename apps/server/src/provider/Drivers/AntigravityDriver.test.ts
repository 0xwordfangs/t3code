import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ANTIGRAVITY_DEFAULT_MODEL,
  EnvironmentId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type AntigravitySettings,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { HttpClient } from "effect/http";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AntigravityInstallation from "../AntigravityInstallation.ts";
import {
  ANTIGRAVITY_AUTH_STDOUT_PREFIX,
  antigravityTextGenerationProfileDirectory,
  resolveAntigravityInstanceDirectories,
} from "../antigravityAuthSupport.ts";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ModelManifest from "../ModelManifest.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { AntigravityDriver } from "./AntigravityDriver.ts";
import { AntigravityKeyringSecretOverride } from "../antigravityUsageLimits.ts";
import * as ProviderHostLive from "../ProviderHostLive.ts";
import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";

const hostPlatform = HostProcess.Platform.defaultValue();
const windowsHost = hostPlatform === "win32";
const decodeRequest = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      method: Schema.optional(Schema.String),
      params: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  ),
);
const blockedCredentialKeys = new Set([
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
  "GOOGLE_GENAI_USE_VERTEXAI",
]);

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

const makeHarness = Effect.fn("makeAntigravityDriverHarness")(function* (
  options: {
    readonly config?: Partial<AntigravitySettings>;
    readonly enabled?: boolean;
    /** Extra instance variables, such as ones a parent T3 turn already exported. */
    readonly environment?: ReadonlyArray<{ readonly name: string; readonly value: string }>;
  } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const nodePath = yield* HostProcess.ExecutablePath;
  const baseEnv = yield* HostProcess.Environment;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-antigravity-driver-" });
  const instanceId = ProviderInstanceId.make(path.basename(root));
  const mockAgentPath = yield* path.fromFileUrl(
    new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
  );
  const requestLog = path.join(root, "requests.jsonl");
  const directories = yield* resolveAntigravityInstanceDirectories(config.stateDir, instanceId);
  const profileDirectory = directories.profile;
  const instancePath = `${path.join(root, "instance-bin")}:${baseEnv.PATH ?? ""}`;

  const makeExecutable = Effect.fn("AntigravityDriverTest.makeExecutable")(function* (
    name: string,
    options: {
      readonly loginRequired?: boolean;
      /** `data` of the agent's "Internal error" response to `session/new`. */
      readonly newSessionErrorData?: unknown;
      readonly initialMode?: string;
    } = {},
  ) {
    const directory = path.join(root, name);
    const executablePath = path.join(directory, "agy_acp_server.par");
    const harnessPath = path.join(directory, "localharness_external");
    yield* fs.makeDirectory(directory, { recursive: true });
    const authorizationUrl =
      "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&redirect_uri=http%3A%2F%2F127.0.0.1%3A51234%2F&state=fixture-state";
    yield* fs.writeFileString(
      executablePath,
      [
        "#!/bin/sh",
        ...(options.loginRequired
          ? [`printf '%s\\n' ${shellQuote(ANTIGRAVITY_AUTH_STDOUT_PREFIX + authorizationUrl)}`]
          : []),
        ...(options.newSessionErrorData === undefined
          ? []
          : [
              `export T3_ACP_NEW_SESSION_ERROR_DATA=${shellQuote(JSON.stringify(options.newSessionErrorData))}`,
            ]),
        ...(options.initialMode === undefined
          ? []
          : [`export T3_ACP_INITIAL_MODE=${shellQuote(options.initialMode)}`]),
        `exec ${shellQuote(nodePath)} ${shellQuote(mockAgentPath)} "$@"`,
        "",
      ].join("\n"),
    );
    yield* fs.writeFileString(harnessPath, "#!/bin/sh\nexit 99\n");
    yield* fs.chmod(executablePath, 0o755);
    yield* fs.chmod(harnessPath, 0o755);
    return {
      executablePath,
      harnessPath,
      source: "managed",
      version: name,
      managedVersionDirectory: directory,
    } satisfies AntigravityInstallation.AntigravityExecutable;
  });

  const first = yield* makeExecutable("runtime 'one");
  const second = yield* makeExecutable("runtime two");
  const signedOut = yield* makeExecutable("runtime signed-out", { loginRequired: true });
  const controls: {
    selected: AntigravityInstallation.AntigravityExecutable;
    failResolution: boolean;
    beforeAcquire: Effect.Effect<void>;
  } = { selected: first, failResolution: false, beforeAcquire: Effect.void };
  const acquisitions: Array<{ binaryPath: string | undefined; path: string | undefined }> = [];
  const releases: Array<string | null> = [];
  const launches: Array<{
    command: string;
    args: ReadonlyArray<string>;
    cwd: string | undefined;
    extendEnv: boolean | undefined;
    environment: Record<string, string | undefined>;
    profileDirectory: string | undefined;
    harnessPath: string | undefined;
    forceFileStorage: string | undefined;
    credentialKeys: ReadonlyArray<string>;
    geminiApiKey: string | undefined;
    tempDirectory: string | undefined;
    handle: ChildProcessSpawner.ChildProcessHandle;
  }> = [];

  const layerInstallation = Layer.mock(AntigravityInstallation.AntigravityInstallation)({
    managedDirectory: root,
    resolve: () =>
      Effect.gen(function* () {
        if (controls.failResolution) {
          return yield* new AntigravityInstallation.AntigravityInstallationError({
            operation: "resolve",
            detail: "Fixture resolution failed.",
          });
        }
        return controls.selected;
      }),
    acquire: (binaryPath, environment) =>
      Effect.gen(function* () {
        acquisitions.push({ binaryPath, path: environment?.PATH });
        yield* controls.beforeAcquire;
        if (controls.failResolution) {
          return yield* new AntigravityInstallation.AntigravityInstallationError({
            operation: "resolve",
            detail: "Fixture resolution failed.",
          });
        }
        const selected = controls.selected;
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            releases.push(selected.version);
          }),
        );
        return selected;
      }),
  });
  const observedSpawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (command._tag !== "StandardCommand")
        return yield* Effect.die("Unexpected process pipeline.");
      const handle = yield* spawner.spawn(command);
      const environment = command.options.env ?? {};
      const env = (name: string) => {
        const value = environment[name];
        return Redacted.isRedacted(value) ? Redacted.value(value) : value;
      };
      const rawArgs = command.args.map(String);
      const unwrapped =
        command.command === "/bin/sh" &&
        rawArgs[0] === "-c" &&
        rawArgs[2] === "t3-acp-cgroup-wrapper" &&
        rawArgs[5] !== undefined
          ? { command: rawArgs[5], args: rawArgs.slice(6) }
          : { command: command.command, args: rawArgs };
      launches.push({
        command: unwrapped.command,
        args: unwrapped.args,
        cwd: command.options.cwd,
        extendEnv: command.options.extendEnv,
        environment: Object.fromEntries(Object.keys(environment).map((name) => [name, env(name)])),
        profileDirectory: env("GEMINI_HOME"),
        harnessPath: env("ANTIGRAVITY_HARNESS_PATH"),
        forceFileStorage: env("AGY_ACP_FORCE_FILE_STORAGE"),
        credentialKeys: Object.keys(environment).filter((key) =>
          blockedCredentialKeys.has(key.toUpperCase()),
        ),
        geminiApiKey: env("GEMINI_API_KEY"),
        tempDirectory:
          environment.ANTIGRAVITY_HARNESS_PATH === undefined ? undefined : env("TMPDIR"),
        handle,
      });
      return handle;
    }),
  );
  const instance = yield* AntigravityDriver.create({
    instanceId,
    displayName: "Google test account",
    enabled: options.enabled ?? true,
    config: { ...AntigravityDriver.defaultConfig(), ...options.config },
    environment: [
      { name: "PATH", value: instancePath },
      { name: "T3_ACP_ANTIGRAVITY", value: "1" },
      { name: "T3_ACP_REQUEST_LOG_PATH", value: requestLog },
      { name: "GEMINI_API_KEY", value: "must-not-be-used" },
      { name: "google_api_key", value: "must-not-be-used" },
      { name: "GOOGLE_APPLICATION_CREDENTIALS", value: "/must-not-be-used.json" },
      { name: "GOOGLE_GENAI_USE_VERTEXAI", value: "true" },
      { name: "GEMINI_HOME", value: "/must-not-be-used" },
      { name: "ANTIGRAVITY_HARNESS_PATH", value: "/must-not-be-used" },
      { name: "BROWSER", value: "must-not-run" },
      ...(options.environment ?? []),
    ].map((variable) => ({ ...variable, sensitive: false })),
  }).pipe(
    Effect.provide(layerInstallation),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner),
  );
  const refresh = instance.refreshModels;
  if (!refresh) return yield* Effect.die("Antigravity does not expose model refresh.");
  const readRequests = Effect.gen(function* () {
    if (!(yield* fs.exists(requestLog))) return [];
    const text = yield* fs.readFileString(requestLog);
    return yield* Effect.forEach(text.split(/\r?\n/u).filter(Boolean), (line) =>
      decodeRequest(line),
    );
  });
  const assertClosed = Effect.gen(function* () {
    for (const launch of launches) {
      yield* launch.handle.exitCode.pipe(Effect.ignore);
      expect(yield* launch.handle.isRunning).toBe(false);
      if (launch.cwd) expect(yield* fs.exists(launch.cwd)).toBe(false);
      if (launch.tempDirectory) expect(yield* fs.exists(launch.tempDirectory)).toBe(false);
    }
  });
  return {
    instance,
    refresh,
    makeExecutable,
    fs,
    path,
    profileDirectory,
    directories,
    instancePath,
    first,
    second,
    signedOut,
    controls,
    acquisitions,
    releases,
    launches,
    readRequests,
    assertClosed,
  };
});

const layerDeps = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-antigravity-driver-config-",
}).pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Unexpected quota request")),
    ),
  ),
  Layer.provideMerge(Layer.succeed(AntigravityKeyringSecretOverride, Effect.undefined)),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(
    Layer.succeed(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  ),
  Layer.provideMerge(ModelManifest.layerTest),
  Layer.provideMerge(IdAllocator.layer),
  Layer.provideMerge(McpProviderSessions.layer),
);
const layerTest = ProviderHostLive.layer.pipe(
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provideMerge(layerDeps),
);

it.layer(layerTest)("AntigravityDriver", (it) => {
  it.effect.skipIf(windowsHost)(
    "refreshes the selected executable and limits form capabilities to chat",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        yield* h.refresh();
        const snapshot = yield* h.instance.snapshot.getSnapshot;
        expect(snapshot.status).toBe("ready");
        expect(snapshot.auth.status).toBe("authenticated");
        expect(snapshot.models.map((model) => model.slug)).toEqual([
          "gemini-test-low",
          "gemini-test-high",
        ]);
        expect(snapshot.models[0]?.aliases).toContain(ANTIGRAVITY_DEFAULT_MODEL);
        expect(snapshot.models.every((model) => model.isLegacy !== true)).toBe(true);
        expect(snapshot.slashCommands.map((command) => command.name)).toEqual(["plan", "logout"]);
        expect(snapshot.supportsTextGeneration).toBe(true);
        h.controls.selected = h.second;
        yield* h.refresh();

        expect(h.acquisitions).toEqual([
          { binaryPath: "", path: h.instancePath },
          { binaryPath: "", path: h.instancePath },
        ]);
        const nativeLaunches = h.launches.filter((launch) => launch.harnessPath !== undefined);
        expect(nativeLaunches.map((launch) => launch.command)).toEqual([
          h.first.executablePath,
          h.second.executablePath,
        ]);
        expect(nativeLaunches.map((launch) => launch.harnessPath)).toEqual([
          h.first.harnessPath,
          h.second.harnessPath,
        ]);
        for (const launch of nativeLaunches) {
          expect(launch.args).toEqual(hostPlatform === "linux" ? ["--uid="] : []);
          expect(launch.profileDirectory).toBe(h.profileDirectory);
          expect(launch.forceFileStorage).toBe("1");
          expect(launch.extendEnv).toBe(false);
        }
        for (const launch of h.launches) expect(launch.credentialKeys).toEqual([]);
        expect(h.releases).toEqual([h.first.version, h.second.version]);
        const requests = yield* h.readRequests;
        expect(requests.map((request) => request.method)).toEqual([
          "initialize",
          "auth/login",
          "session/new",
          "initialize",
          "auth/login",
          "session/new",
        ]);
        expect(
          requests
            .filter((request) => request.method === "auth/login")
            .map((request) => request.params?.methodId),
        ).toEqual(["oauth-personal", "oauth-personal"]);
        expect(
          requests
            .filter((request) => request.method === "session/new")
            .map((request) => request.params?.mcpServers),
        ).toEqual([[], []]);
        yield* h.assertClosed;
        yield* h.instance.textGeneration
          .generateThreadTitle({
            cwd: h.profileDirectory,
            message: "Name this thread",
            modelSelection: { instanceId: h.instance.instanceId, model: "gemini-test-low" },
          })
          .pipe(Effect.ignore);
        const workspace = yield* h.fs.makeTempDirectoryScoped({ prefix: "t3-ag-chat-" });
        const threadId = ThreadId.make("thread-antigravity-capabilities");
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "approval-required",
          interactionMode: "default",
          cwd: workspace,
        });
        const modelSelection = { instanceId: h.instance.instanceId, model: "gemini-test-low" };
        const session = yield* h.instance.orchestrationAdapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-antigravity-capabilities"),
          modelSelection,
          runtimePolicy,
        });
        yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });

        const helper = {
          fs: { readTextFile: false, writeTextFile: false },
          terminal: false,
          session: { notices: {}, compaction: {} },
        };
        expect(
          (yield* h.readRequests)
            .filter((request) => request.method === "initialize")
            .map((request) => request.params?.clientCapabilities),
        ).toEqual([
          // Two model refreshes, then thread title generation.
          helper,
          helper,
          helper,
          {
            fs: { readTextFile: true, writeTextFile: false },
            terminal: false,
            session: { notices: {}, compaction: {} },
            elicitation: { form: {} },
          },
        ]);

        // The agy CLI rejects `yolo` and `auto_edit`; it stays in `default` and T3 answers its requests.
        h.controls.selected = {
          ...(yield* h.makeExecutable("runtime cli", { initialMode: "auto_edit" })),
          harnessPath: null,
        };
        for (const runtimeMode of ["full-access", "auto-accept-edits"] as const) {
          const requestsBefore = (yield* h.readRequests).length;
          const cliThreadId = ThreadId.make(`thread-antigravity-cli-${runtimeMode}`);
          const cliPolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
            runtimeMode,
            interactionMode: "default",
            cwd: workspace,
          });
          const cliSession = yield* h.instance.orchestrationAdapter.openSession({
            threadId: cliThreadId,
            providerSessionId: ProviderSessionId.make(`provider-session-cli-${runtimeMode}`),
            modelSelection,
            runtimePolicy: cliPolicy,
          });
          yield* cliSession.ensureThread({
            threadId: cliThreadId,
            modelSelection,
            runtimePolicy: cliPolicy,
          });
          expect(h.launches.at(-1)?.args).toEqual([
            "acp",
            "--gemini-dir",
            h.profileDirectory,
            "--app-data-dir",
            "antigravity-acp",
          ]);
          expect(
            (yield* h.readRequests)
              .slice(requestsBefore)
              .filter(
                (request) =>
                  request.method === "session/set_config_option" &&
                  request.params?.configId === "mode",
              )
              .map((request) => request.params?.value),
          ).toEqual(["default"]);
        }
      }),
  );

  it.effect.skipIf(windowsHost)(
    "gives a thread session its own T3 thread and MCP fallback environment",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness({
          environment: [
            { name: "T3CODE_THREAD_ID", value: "thread-antigravity-ambient" },
            { name: "T3_ACP_MCP_ENDPOINT", value: "http://127.0.0.1:9/ambient" },
            { name: "T3_ACP_MCP_AUTHORIZATION", value: "Bearer ambient-token" },
            { name: "T3_ACP_MCP_NODE", value: "/ambient/node" },
            { name: "T3_ACP_MCP_ENTRYPOINT", value: "/ambient/entrypoint.mjs" },
          ],
        });
        yield* h.refresh();
        const endpoint = "http://127.0.0.1:43126/mcp";
        const threadId = ThreadId.make("thread-antigravity-process-environment");
        yield* (yield* McpProviderSessions.McpProviderSessions).set({
          environmentId: EnvironmentId.make("environment-antigravity-process-environment"),
          threadId,
          providerSessionId: "mcp-session-antigravity-process-environment",
          providerInstanceId: h.instance.instanceId,
          endpoint,
          authorizationHeader: "Bearer antigravity-thread-token",
          browserToolsAvailable: false,
        });
        // Running from source, the fallback entrypoint is this process's script.
        const entrypoint = h.path.resolve(process.argv[1]!);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const workspace = yield* h.fs.makeTempDirectoryScoped({ prefix: "t3-ag-environment-" });
            const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "approval-required",
              interactionMode: "default",
              cwd: workspace,
            });
            const modelSelection = { instanceId: h.instance.instanceId, model: "gemini-test-low" };
            const session = yield* h.instance.orchestrationAdapter.openSession({
              threadId,
              providerSessionId: ProviderSessionId.make(
                "provider-session-antigravity-process-environment",
              ),
              modelSelection,
              runtimePolicy,
            });
            yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
          }),
        );

        // Gemini Enterprise can strip the native MCP servers, and the documented `acp-mcp-call`
        // fallback reads these from the shell.
        const agentLaunches = h.launches.filter(
          (launch) => launch.command === h.first.executablePath,
        );
        expect(agentLaunches).toHaveLength(2);
        expect(agentLaunches[1]?.environment).toMatchObject({
          T3CODE_THREAD_ID: threadId,
          T3_ACP_MCP_ENDPOINT: endpoint,
          T3_ACP_MCP_AUTHORIZATION: "Bearer antigravity-thread-token",
          T3_ACP_MCP_NODE: process.execPath,
          T3_ACP_MCP_ENTRYPOINT: entrypoint,
        });
        // Model refresh and sign-in launches are not part of a thread.
        expect(agentLaunches[0]?.environment).toMatchObject({
          T3CODE_THREAD_ID: "thread-antigravity-ambient",
          T3_ACP_MCP_ENDPOINT: "http://127.0.0.1:9/ambient",
          T3_ACP_MCP_AUTHORIZATION: "Bearer ambient-token",
          T3_ACP_MCP_NODE: "/ambient/node",
          T3_ACP_MCP_ENTRYPOINT: "/ambient/entrypoint.mjs",
        });
        yield* h.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)(
    "closes refresh processes and clears account metadata when Google sign-in is required",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        yield* h.refresh();
        h.controls.selected = h.signedOut;
        const error = yield* h.refresh().pipe(Effect.flip);
        expect(error.detail).toContain("Sign in to Antigravity");
        const snapshot = yield* h.instance.snapshot.getSnapshot;
        expect(snapshot.auth.status).toBe("unauthenticated");
        expect(snapshot.models).toEqual([]);
        expect(snapshot.slashCommands).toEqual([]);
        expect(snapshot.supportsTextGeneration).toBe(false);
        expect(h.acquisitions).toHaveLength(2);
        expect(h.releases).toEqual([h.first.version, h.signedOut.version]);
        yield* h.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)("keeps the previous catalog when executable resolution fails", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.refresh();
      const before = yield* h.instance.snapshot.getSnapshot;
      h.controls.failResolution = true;
      const error = yield* h.refresh().pipe(Effect.flip);
      expect(error.detail).toContain("previous model list is unchanged");
      const after = yield* h.instance.snapshot.getSnapshot;
      expect(after.models).toEqual(before.models);
      expect(after.auth).toEqual(before.auth);
      expect(h.acquisitions).toHaveLength(2);
      expect(h.releases).toEqual([h.first.version]);
      yield* h.assertClosed;
    }),
  );

  it.effect.skipIf(windowsHost)(
    "reports the missing SOCKS dependency when session creation fails and keeps the catalog",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        yield* h.refresh();
        const before = yield* h.instance.snapshot.getSnapshot;
        h.controls.selected = yield* h.makeExecutable("runtime socks", {
          newSessionErrorData: { details: "python-socks is required to use a SOCKS proxy" },
        });
        const error = yield* h.refresh().pipe(Effect.flip);
        expect(error.detail).toBe(
          "Could not refresh Antigravity models. The previous model list is unchanged. Antigravity reported: python-socks is required to use a SOCKS proxy",
        );
        const after = yield* h.instance.snapshot.getSnapshot;
        expect(after.models).toEqual(before.models);
        expect(after.auth).toEqual(before.auth);
        yield* h.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)(
    "never copies unrecognized agent error details into the refresh error",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        yield* h.refresh();
        const before = yield* h.instance.snapshot.getSnapshot;
        const generic =
          "Could not refresh Antigravity models. The previous model list is unchanged.";
        const secret = "https://accounts.example.test/o/oauth2?code=sk-secret-123";
        h.controls.selected = yield* h.makeExecutable("runtime socks with extra", {
          newSessionErrorData: {
            details: `Traceback (most recent call last):\n  proxy ${secret}\npython-socks is required to use a SOCKS proxy`,
          },
        });
        const known = yield* h.refresh().pipe(Effect.flip);
        expect(known.detail).toBe(
          `${generic} Antigravity reported: python-socks is required to use a SOCKS proxy`,
        );

        for (const [name, data] of [
          ["runtime unknown details", { details: `Token refresh failed for ${secret}` }],
          ["runtime object details", { details: { stack: "secret frame", env: { HOME: "/" } } }],
          ["runtime no details", ["python-socks is required to use a SOCKS proxy"]],
        ] as const) {
          h.controls.selected = yield* h.makeExecutable(name, { newSessionErrorData: data });
          const error = yield* h.refresh().pipe(Effect.flip);
          expect(error.detail).toBe(generic);
        }
        const after = yield* h.instance.snapshot.getSnapshot;
        expect(after.models).toEqual(before.models);
        expect(after.auth).toEqual(before.auth);
        expect(JSON.stringify(after)).not.toContain("sk-secret-123");
        yield* h.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)(
    "removes runtime temp directories left by a previous server on create",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const config = yield* ServerConfig.ServerConfig;
        const instanceId = ProviderInstanceId.make("antigravity-orphan-sweep");
        const directories = yield* resolveAntigravityInstanceDirectories(
          config.stateDir,
          instanceId,
        );
        const legacyRoot = path.join(directories.profile, "antigravity-acp", "tmp");
        for (const root of [directories.runtimeTemp, legacyRoot]) {
          const orphan = path.join(root, "run-orphan", "_MEI123", "google3");
          yield* fs.makeDirectory(orphan, { recursive: true });
          yield* fs.writeFileString(path.join(orphan, "payload.bin"), "stale");
        }
        yield* AntigravityDriver.create({
          instanceId,
          displayName: "Sweep",
          enabled: false,
          config: AntigravityDriver.defaultConfig(),
          environment: [],
        }).pipe(
          Effect.provide(
            Layer.mock(AntigravityInstallation.AntigravityInstallation)({
              managedDirectory: config.stateDir,
              resolve: () => Effect.die("unused"),
              acquire: () => Effect.die("unused"),
            }),
          ),
        );
        expect(yield* fs.exists(directories.runtimeTemp)).toBe(false);
        expect(yield* fs.exists(legacyRoot)).toBe(false);
      }),
  );

  it.effect.skipIf(windowsHost)(
    "reports the stored Google email only for personal OAuth instances",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const writeStoredToken = (profileDirectory: string, email: string) =>
          Effect.gen(function* () {
            const acpDir = path.join(profileDirectory, "antigravity-acp");
            yield* fs.makeDirectory(acpDir, { recursive: true });
            yield* fs.writeFileString(
              path.join(acpDir, "antigravity-oauth-token"),
              JSON.stringify({ id_token_email: email }),
            );
          });

        const personal = yield* makeHarness({ config: { authMethod: "oauth-personal" } });
        yield* writeStoredToken(personal.profileDirectory, "personal@example.com");
        expect((yield* personal.instance.snapshot.refresh).auth).toMatchObject({
          status: "unknown",
          type: "oauth-personal",
          email: "personal@example.com",
        });
        yield* personal.refresh();
        expect((yield* personal.instance.snapshot.getSnapshot).auth).toMatchObject({
          status: "authenticated",
          type: "oauth-personal",
          label: "Google account",
          email: "personal@example.com",
        });
        yield* personal.assertClosed;

        const enterprise = yield* makeHarness({
          config: {
            authMethod: "oauth-business",
            gcpProject: "demo-project",
            gcpLocation: "us-central1",
          },
        });
        yield* writeStoredToken(enterprise.profileDirectory, "ignored@example.com");
        const enterpriseAuth = (yield* enterprise.instance.snapshot.refresh).auth;
        expect(enterpriseAuth).toMatchObject({
          status: "unknown",
          type: "oauth-business",
        });
        expect(enterpriseAuth.email).toBeUndefined();
        yield* enterprise.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)(
    "runs native CLI code login in SSH mode without display variables, then authenticates a fresh session",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const h = yield* makeHarness({
          config: { authMethod: "oauth-personal" },
          environment: [
            { name: "DISPLAY", value: ":99" },
            { name: "WAYLAND_DISPLAY", value: "wayland-1" },
          ],
        });
        const cliDir = path.join(path.dirname(h.first.executablePath), "native-cli");
        const cliExecutable = path.join(cliDir, "agy");
        const codeLog = path.join(cliDir, "submitted-code.txt");
        const loginUrl =
          "https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=driver-state";
        yield* fs.makeDirectory(cliDir, { recursive: true });
        yield* fs.writeFileString(
          cliExecutable,
          [
            "#!/bin/sh",
            'case " $* " in',
            '  *" --login "*)',
            `    printf '%s\\n' ${shellQuote(loginUrl)}`,
            "    IFS= read -r code",
            `    printf '%s' "$code" > ${shellQuote(codeLog)}`,
            `    mkdir -p "$GEMINI_HOME/antigravity-acp"`,
            `    printf '{"id_token_email":"isolated@example.com"}' > "$GEMINI_HOME/antigravity-acp/antigravity-oauth-token"`,
            "    exit 0",
            "    ;;",
            "esac",
            `exec ${shellQuote(h.first.executablePath)} "$@"`,
            "",
          ].join("\n"),
        );
        yield* fs.chmod(cliExecutable, 0o755);
        h.controls.selected = {
          executablePath: cliExecutable,
          harnessPath: null,
          source: "path",
          version: "1.5.13",
          managedVersionDirectory: null,
        };

        const auth = h.instance.auth;
        if (!auth?.respond) return yield* Effect.die("Antigravity auth respond is missing.");
        const owner = "driver-code-login-owner";
        const startedFlow = yield* auth.start(owner);
        const waiting = yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "waiting"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect(waiting.authorizationUrl).toBe(loginUrl);
        expect(waiting.interaction?.type).toBe("credentials");

        yield* auth.respond(owner, {
          instanceId: h.instance.instanceId,
          flowId: startedFlow.flowId!,
          interactionId: startedFlow.flowId!,
          response: { type: "credentials", values: { code: "4/0Ab-driver-code" } },
        });
        yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "succeeded"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );

        expect(yield* fs.readFileString(codeLog)).toBe("4/0Ab-driver-code");
        const loginLaunch = h.launches.find((launch) => launch.args.includes("--login"));
        expect(loginLaunch?.environment.SSH_CONNECTION).toBe("127.0.0.1 0 127.0.0.1 0");
        expect(loginLaunch?.environment.DISPLAY).toBeUndefined();
        expect(loginLaunch?.environment.WAYLAND_DISPLAY).toBeUndefined();
        expect(loginLaunch?.profileDirectory).toBeDefined();
        expect(loginLaunch?.profileDirectory).not.toBe(h.profileDirectory);
        expect(yield* fs.exists(loginLaunch!.profileDirectory!)).toBe(false);
        const liveTokenPath = path.join(
          h.profileDirectory,
          "antigravity-acp",
          "antigravity-oauth-token",
        );
        expect(yield* fs.readFileString(liveTokenPath)).toBe(
          '{"id_token_email":"isolated@example.com"}',
        );
        expect((yield* fs.stat(liveTokenPath)).mode & 0o777).toBe(0o600);
        expect((yield* h.instance.snapshot.getSnapshot).auth).toMatchObject({
          status: "authenticated",
          type: "oauth-personal",
          email: "isolated@example.com",
        });
        yield* h.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)(
    "preserves live Account A credentials and cleans up staging on failed or cancelled staged code login",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const nodePath = yield* HostProcess.ExecutablePath;
        const h = yield* makeHarness({ config: { authMethod: "oauth-personal" } });
        const liveAcpDir = path.join(h.profileDirectory, "antigravity-acp");
        const liveTokenPath = path.join(liveAcpDir, "antigravity-oauth-token");
        const accountAToken = JSON.stringify({
          id_token_email: "account-a@example.com",
          token: { refresh_token: "refresh-a" },
        });
        yield* fs.makeDirectory(liveAcpDir, { recursive: true });
        yield* fs.writeFileString(liveTokenPath, accountAToken);
        yield* h.refresh();
        const beforeSnapshot = yield* h.instance.snapshot.getSnapshot;
        expect(beforeSnapshot.auth).toMatchObject({
          status: "authenticated",
          email: "account-a@example.com",
        });
        expect(beforeSnapshot.models.length).toBeGreaterThan(0);

        const cliDir = path.join(path.dirname(h.first.executablePath), "staged-cli");
        yield* fs.makeDirectory(cliDir, { recursive: true });
        const loginUrl =
          "https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=staged-state";
        const subRequiredAgent = path.join(cliDir, "sub-required-agent.mjs");
        yield* fs.writeFileString(
          subRequiredAgent,
          [
            'import * as readline from "node:readline";',
            "const rl = readline.createInterface({ input: process.stdin });",
            'rl.on("line", (line) => {',
            "  const msg = JSON.parse(line);",
            '  if (msg.method === "initialize") {',
            '    process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: { protocolVersion: 1, authMethods: [{ id: "oauth-personal", name: "Google" }] } }) + "\\n");',
            "  } else if (msg.id !== undefined) {",
            '    process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: msg.id, error: { code: -32603, message: "SUBSCRIPTION_REQUIRED" } }) + "\\n");',
            "  }",
            "});",
            "",
          ].join("\n"),
        );

        const writeCli = (
          name: string,
          options: {
            readonly writeBeforeUrl?: boolean;
            readonly failVerify?: boolean;
          },
        ) =>
          Effect.gen(function* () {
            const executablePath = path.join(cliDir, name);
            const writeToken = [
              `mkdir -p "$GEMINI_HOME/antigravity-acp"`,
              `printf '{"id_token_email":"account-b@example.com","token":{"refresh_token":"refresh-b"}}' > "$GEMINI_HOME/antigravity-acp/antigravity-oauth-token"`,
            ];
            yield* fs.writeFileString(
              executablePath,
              [
                "#!/bin/sh",
                'case " $* " in',
                '  *" --login "*)',
                ...(options.writeBeforeUrl
                  ? [
                      ...writeToken,
                      `    printf '%s\\n' ${shellQuote(loginUrl)}`,
                      "    IFS= read -r code",
                    ]
                  : [
                      `    printf '%s\\n' ${shellQuote(loginUrl)}`,
                      "    IFS= read -r code",
                      ...writeToken,
                    ]),
                "    exit 0",
                "    ;;",
                "esac",
                options.failVerify
                  ? `exec ${shellQuote(nodePath)} ${shellQuote(subRequiredAgent)}`
                  : `exec ${shellQuote(h.first.executablePath)} "$@"`,
                "",
              ].join("\n"),
            );
            yield* fs.chmod(executablePath, 0o755);
            return {
              executablePath,
              harnessPath: null,
              source: "path" as const,
              version: "1.5.13",
              managedVersionDirectory: null,
            };
          });

        const auth = h.instance.auth;
        if (!auth?.respond) return yield* Effect.die("Antigravity auth respond is missing.");
        const owner = "staged-owner";

        // 1. Failed post-login verification (SUBSCRIPTION_REQUIRED).
        h.controls.selected = yield* writeCli("agy-sub-required", { failVerify: true });
        const failedFlow = yield* auth.start(owner);
        yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "waiting"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        yield* auth.respond(owner, {
          instanceId: h.instance.instanceId,
          flowId: failedFlow.flowId!,
          interactionId: failedFlow.flowId!,
          response: { type: "credentials", values: { code: "4/0Ab-sub-fail" } },
        });
        const failedState = yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "failed"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect(failedState.message).toContain("eligible Antigravity subscription");
        expect(yield* fs.readFileString(liveTokenPath)).toBe(accountAToken);
        expect(yield* h.instance.snapshot.getSnapshot).toMatchObject({
          auth: { status: "authenticated", email: "account-a@example.com" },
          models: beforeSnapshot.models,
        });

        // 2. Cancelled post-login verification.
        h.controls.selected = yield* writeCli("agy-cancel-verify", {});
        const verifyEntered = yield* Deferred.make<void>();
        let acquireCount = 0;
        h.controls.beforeAcquire = Effect.suspend(() => {
          acquireCount += 1;
          return acquireCount === 2
            ? Deferred.succeed(verifyEntered, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void;
        });
        const cancelVerifyFlow = yield* auth.start(owner);
        yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "waiting"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        yield* auth.respond(owner, {
          instanceId: h.instance.instanceId,
          flowId: cancelVerifyFlow.flowId!,
          interactionId: cancelVerifyFlow.flowId!,
          response: { type: "credentials", values: { code: "4/0Ab-cancel-verify" } },
        });
        yield* Deferred.await(verifyEntered);
        yield* auth.cancel(owner, cancelVerifyFlow.flowId!);
        h.controls.beforeAcquire = Effect.void;
        expect(yield* fs.readFileString(liveTokenPath)).toBe(accountAToken);
        expect((yield* h.instance.snapshot.getSnapshot).auth).toMatchObject({
          status: "authenticated",
          email: "account-a@example.com",
        });

        // 3. Cancelled mid-codeLogin after staging token write.
        h.controls.selected = yield* writeCli("agy-cancel-login", { writeBeforeUrl: true });
        const cancelLoginFlow = yield* auth.start(owner);
        yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "waiting"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        const activeStagingProfile = h.launches.at(-1)?.profileDirectory;
        expect(activeStagingProfile).toBeDefined();
        expect(activeStagingProfile).not.toBe(h.profileDirectory);
        expect(
          yield* fs.exists(
            path.join(activeStagingProfile!, "antigravity-acp", "antigravity-oauth-token"),
          ),
        ).toBe(true);
        yield* auth.cancel(owner, cancelLoginFlow.flowId!);
        expect(yield* fs.exists(activeStagingProfile!)).toBe(false);
        expect(yield* fs.readFileString(liveTokenPath)).toBe(accountAToken);
        expect((yield* h.instance.snapshot.getSnapshot).auth).toMatchObject({
          status: "authenticated",
          email: "account-a@example.com",
        });

        // All staging profile directories under runtimeTemp are cleaned up.
        const tempEntries = yield* fs
          .readDirectory(h.directories.runtimeTemp)
          .pipe(Effect.orElseSucceed(() => []));
        expect(tempEntries.filter((entry) => entry.startsWith("login-profile-"))).toEqual([]);
        yield* h.assertClosed;
      }),
  );

  it.effect.skipIf(windowsHost)(
    "keeps user plugins out of codeLogin staging profiles and isolates text generation into a plugin-free sibling profile",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const fakeHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ag-plugins-home-" });
        const rtkDir = path.join(fakeHome, ".gemini", "config", "plugins", "rtk");
        yield* fs.makeDirectory(rtkDir, { recursive: true });
        yield* fs.writeFileString(
          path.join(rtkDir, "hooks.json"),
          '{"hooks":{"PreInvocation":[{"command":"rtk"}]}}',
        );

        const h = yield* makeHarness({
          config: { authMethod: "oauth-personal" },
          environment: [{ name: "HOME", value: fakeHome }],
        });

        // 1. codeLogin staging profile must not link ~/.gemini/config/plugins during --login or verification.
        const cliDir = path.join(path.dirname(h.first.executablePath), "no-plugins-cli");
        const cliExecutable = path.join(cliDir, "agy");
        const pluginChecksLog = path.join(cliDir, "plugin-checks.log");
        const loginUrl =
          "https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=no-plugins-state";
        yield* fs.makeDirectory(cliDir, { recursive: true });
        yield* fs.writeFileString(
          cliExecutable,
          [
            "#!/bin/sh",
            'plugins_state="absent"',
            'if [ -e "$GEMINI_HOME/config/plugins" ] || [ -L "$GEMINI_HOME/config/plugins" ]; then plugins_state="present"; fi',
            'case " $* " in',
            '  *" --login "*)',
            `    printf 'login:%s\\n' "$plugins_state" >> ${shellQuote(pluginChecksLog)}`,
            `    printf '%s\\n' ${shellQuote(loginUrl)}`,
            "    IFS= read -r code",
            `    mkdir -p "$GEMINI_HOME/antigravity-acp"`,
            `    printf '{"id_token_email":"isolated@example.com"}' > "$GEMINI_HOME/antigravity-acp/antigravity-oauth-token"`,
            "    exit 0",
            "    ;;",
            "esac",
            `printf 'verify:%s\\n' "$plugins_state" >> ${shellQuote(pluginChecksLog)}`,
            `exec ${shellQuote(h.first.executablePath)} "$@"`,
            "",
          ].join("\n"),
        );
        yield* fs.chmod(cliExecutable, 0o755);
        h.controls.selected = {
          executablePath: cliExecutable,
          harnessPath: null,
          source: "path",
          version: "1.5.13",
          managedVersionDirectory: null,
        };

        const auth = h.instance.auth;
        if (!auth?.respond) return yield* Effect.die("Antigravity auth respond is missing.");
        const owner = "no-plugins-owner";
        const startedFlow = yield* auth.start(owner);
        yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "waiting"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        yield* auth.respond(owner, {
          instanceId: h.instance.instanceId,
          flowId: startedFlow.flowId!,
          interactionId: startedFlow.flowId!,
          response: { type: "credentials", values: { code: "4/0Ab-no-plugins-code" } },
        });
        yield* auth.subscribe(owner).pipe(
          Stream.filter((state) => state.phase === "succeeded"),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect((yield* fs.readFileString(pluginChecksLog)).trim().split("\n")).toEqual([
          "login:absent",
          "verify:absent",
        ]);

        // 2. Even with ~/.gemini/config/plugins/rtk/hooks.json active, supportsTextGeneration stays true
        // and text generation runs on `<profile>-textgen` (sharing antigravity-acp, without config/plugins).
        h.controls.selected = h.first;
        yield* h.refresh();
        const readySnapshot = yield* h.instance.snapshot.getSnapshot;
        expect(readySnapshot.supportsTextGeneration).toBe(true);
        expect(yield* fs.readLink(path.join(h.profileDirectory, "config", "plugins"))).toBe(
          path.join(fakeHome, ".gemini", "config", "plugins"),
        );

        const textGenProfile = antigravityTextGenerationProfileDirectory(path, h.profileDirectory);
        const launchesBeforeTextGen = h.launches.length;
        yield* h.instance.textGeneration
          .generateThreadTitle({
            cwd: h.profileDirectory,
            message: "Name this thread",
            modelSelection: { instanceId: h.instance.instanceId, model: "gemini-test-low" },
          })
          .pipe(Effect.ignore);
        const textGenLaunch = h.launches
          .slice(launchesBeforeTextGen)
          .find((launch) => launch.harnessPath !== undefined);
        expect(textGenLaunch?.profileDirectory).toBe(textGenProfile);
        expect(yield* fs.readLink(path.join(textGenProfile, "antigravity-acp"))).toBe(
          path.join(h.profileDirectory, "antigravity-acp"),
        );
        expect(yield* fs.exists(path.join(textGenProfile, "config", "plugins"))).toBe(false);
        expect(
          yield* fs.readLink(path.join(textGenProfile, "config", "plugins")).pipe(
            Effect.as(true),
            Effect.orElseSucceed(() => false),
          ),
        ).toBe(false);

        // 3. Sign-in failure during text generation still notifies provider.onAuthRequired.
        h.controls.selected = h.signedOut;
        const authError = yield* h.instance.textGeneration
          .generateThreadTitle({
            cwd: h.profileDirectory,
            message: "Name this thread",
            modelSelection: { instanceId: h.instance.instanceId, model: "gemini-test-low" },
          })
          .pipe(Effect.flip);
        expect(authError.detail).toContain("Sign in to Antigravity");
        const unauthenticatedSnapshot = yield* h.instance.snapshot.getSnapshot;
        expect(unauthenticatedSnapshot.auth.status).toBe("unauthenticated");
        expect(unauthenticatedSnapshot.models).toEqual([]);
        yield* h.assertClosed;
      }),
  );
});
