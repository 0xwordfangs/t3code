// @effect-diagnostics-next-line nodeBuiltinImport:off - Effect's symlink has no type argument, and Windows needs a junction to link without elevation.
import * as NodeFSP from "node:fs/promises";

import type { AntigravityAuthMethod, ProviderInstanceId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveNodeExecutable, nodeRuntimeUnavailableMessage } from "@t3tools/shared/nodeRuntime";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as AcpErrors from "effect-acp/errors";

import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import {
  antigravityUserSkillDirectories,
  resolveAntigravityUserHome,
} from "./Drivers/AntigravitySkills.ts";

export const ANTIGRAVITY_AUTH_STDOUT_PREFIX =
  "Open the following link to authenticate the ACP server: ";
export const ANTIGRAVITY_AUTH_BROWSER_MARKER = "__T3_ANTIGRAVITY_AUTH_URL__";
export const ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE =
  "Sign in to Antigravity in Settings before you continue.";

const maxAuthorizationUrlLength = 16_384;
const maxBrowserHelperLineLength =
  Math.max(ANTIGRAVITY_AUTH_BROWSER_MARKER.length, ANTIGRAVITY_AUTH_STDOUT_PREFIX.length) +
  maxAuthorizationUrlLength +
  2;
const maxStdoutLineBytes = 16 * 1024 * 1024;
const authPrefixBytes = new TextEncoder().encode(ANTIGRAVITY_AUTH_STDOUT_PREFIX);
const decodeUrl = Schema.decodeUnknownEffect(Schema.URLFromString);
const decodeBrowserHelperUrl = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.String));
const decodeStoredSignInEmail = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id_token_email: Schema.optional(Schema.String),
    }),
  ),
);
const ProfileSettingsFile = Schema.Struct({
  auth: Schema.Struct({ type: Schema.String }),
  gcp: Schema.optional(
    Schema.Struct({
      project: Schema.optional(Schema.String),
      location: Schema.optional(Schema.String),
    }),
  ),
});
const encodeProfileSettings = Schema.encodeSync(Schema.fromJsonString(ProfileSettingsFile));
const isAcpRequestError = Schema.is(AcpErrors.AcpRequestError);
const isAcpTransportError = Schema.is(AcpErrors.AcpTransportError);

// Python splits BROWSER on path separators before parsing quotes. Exit 0 on EPIPE to prevent browser fallback after cancellation.
const browserHelperSource =
  `process.stderr.on("error",()=>process.exit(0)).write(` +
  `"${ANTIGRAVITY_AUTH_BROWSER_MARKER}"+JSON.stringify(process.argv[1])+"\\n",` +
  `()=>process.exit(0))`;
const browserPreflightUrl = "https://example.invalid/t3-antigravity-browser-preflight";

const removedEnvironmentKeys = new Set([
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
  "GOOGLE_CLOUD_PROJECT",
  "GOOGLE_CLOUD_LOCATION",
  "GOOGLE_CLOUD_QUOTA_PROJECT",
  "GOOGLE_GENAI_USE_VERTEXAI",
  "GCLOUD_PROJECT",
  "CLOUDSDK_CORE_PROJECT",
  "AGY_ACP_CCPA_PROJECT",
  "AGY_ACP_ENABLE_OAUTH",
  "GEMINI_HOME",
  "AGY_ACP_FORCE_FILE_STORAGE",
  "ANTIGRAVITY_HARNESS_PATH",
  "BROWSER",
  "PYTHONUNBUFFERED",
  "ELECTRON_RUN_AS_NODE",
  // An inherited pipe whose owner exited makes Electron-as-Node print Crashpad errors to stderr.
  "CHROME_CRASHPAD_PIPE_NAME",
]);

export interface AntigravityProfile {
  readonly platform: NodeJS.Platform;
  readonly geminiHome: string;
  readonly acpDirectory: string;
  readonly tokenPath: string;
  readonly tempDirectory: string;
  readonly browserCommand: string;
  readonly caBundlePath?: string;
}

export interface AntigravityAuthConfig {
  readonly authMethod: AntigravityAuthMethod;
  readonly apiKey: string;
  readonly gcpProject: string;
  readonly gcpLocation: string;
}

const ANTIGRAVITY_PERSONAL_AUTH: AntigravityAuthConfig = {
  authMethod: "oauth-personal",
  apiKey: "",
  gcpProject: "",
  gcpLocation: "",
};

export function antigravityAuthUsesBrowser(authMethod: AntigravityAuthMethod): boolean {
  return authMethod === "oauth-personal" || authMethod === "oauth-business";
}

export function antigravityAuthLabel(authMethod: AntigravityAuthMethod): string {
  switch (authMethod) {
    case "oauth-personal":
      return "Google account";
    case "oauth-business":
      return "Gemini Enterprise";
    case "gemini-api-key":
      return "Gemini API key";
    case "agent-platform":
      return "Agent Platform";
  }
}

export function antigravityAuthConfigIssue(auth: AntigravityAuthConfig): string | null {
  switch (auth.authMethod) {
    case "oauth-personal":
      return null;
    case "oauth-business":
      return auth.gcpProject && auth.gcpLocation
        ? null
        : "Gemini Enterprise needs a GCP project and location in the Antigravity provider settings.";
    case "gemini-api-key":
      return auth.apiKey ? null : "Enter a Gemini API key in the Antigravity provider settings.";
    case "agent-platform":
      return auth.apiKey || (auth.gcpProject && auth.gcpLocation)
        ? null
        : "Agent Platform needs an API key, or a GCP project and location, in the Antigravity provider settings.";
  }
}

/**
 * `auth.type` names the selected method so a native logout clears only that method's credentials
 * instead of every stored token. Never holds a credential.
 */
function antigravityProfileSettings(auth: AntigravityAuthConfig): string {
  const gcp = {
    ...(auth.gcpProject ? { project: auth.gcpProject } : {}),
    ...(auth.gcpLocation ? { location: auth.gcpLocation } : {}),
  };
  return `${encodeProfileSettings({
    auth: { type: auth.authMethod },
    ...(Object.keys(gcp).length > 0 ? { gcp } : {}),
  })}\n`;
}

export interface AntigravityAuthorizationUrl {
  readonly authorizationUrl: string;
  readonly redirectUri: string;
  readonly state: string;
}

function authSupportError(detail: string) {
  return new AcpErrors.AcpTransportError({ detail, cause: undefined });
}

// The 1.5 CLI answers `authenticate` with this internal error when it has no sign-in and no display.
const NATIVE_CLI_NO_BROWSER_PREFIX = "No browser is available here";

export function isAntigravitySignInRequiredError(error: unknown): boolean {
  return (
    (isAcpRequestError(error) &&
      (error.code === -32000 ||
        (error.code === -32603 && error.errorMessage.startsWith(NATIVE_CLI_NO_BROWSER_PREFIX)))) ||
    (isAcpTransportError(error) && error.detail === ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE)
  );
}

export interface AntigravityInstanceDirectories {
  readonly profile: string;
  /** Keep beside the profile: 120-character archive members under its longer name would exceed Windows' 260-character limit. */
  readonly runtimeTemp: string;
}

/** Hashes the instance ID so case-only differences stay separate on case-insensitive filesystems. */
export const resolveAntigravityInstanceDirectories = Effect.fn(
  "resolveAntigravityInstanceDirectories",
)(function* (stateDir: string, instanceId: ProviderInstanceId) {
  const digest = yield* Digest.Digest;
  const path = yield* Path.Path;
  const key = Hex.encode(yield* digest.digest(Digest.sha256, new TextEncoder().encode(instanceId)));
  return {
    profile: path.join(stateDir, "providers", "antigravity", key),
    runtimeTemp: path.join(stateDir, "antigravity-tmp", key.slice(0, 12)),
  } satisfies AntigravityInstanceDirectories;
});

function quoteBrowserArgument(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

function antigravityEnvironment(
  profile: AntigravityProfile,
  baseEnv: NodeJS.ProcessEnv,
  auth: AntigravityAuthConfig,
  runtimeTempDirectory?: string,
) {
  const environment: NodeJS.ProcessEnv = {};
  let existingSshConnection: string | undefined;
  for (const [key, value] of Object.entries(baseEnv)) {
    const upper = key.toUpperCase();
    if (upper === "SSH_CONNECTION") {
      if (value) existingSshConnection = value;
      if (auth.authMethod === "oauth-personal") continue;
    }
    // Windows treats environment keys as case-insensitive. Remove aliases too.
    if (!removedEnvironmentKeys.has(upper)) environment[key] = value;
  }
  // Put the GCP pair in settings.json because GOOGLE_API_KEY takes precedence in the environment.
  const credential =
    auth.authMethod === "gemini-api-key" && auth.apiKey
      ? { GEMINI_API_KEY: auth.apiKey }
      : auth.authMethod === "agent-platform" && auth.apiKey
        ? { GOOGLE_API_KEY: auth.apiKey }
        : {};
  // Each launch unpacks about 1 GB; use T3-owned temp so the driver can reclaim files after force-kill.
  const tempDirectory = runtimeTempDirectory ?? profile.tempDirectory;
  return {
    ...environment,
    ...credential,
    ...(profile.caBundlePath !== undefined &&
    baseEnv.SSL_CERT_FILE === undefined &&
    baseEnv.SSL_CERT_DIR === undefined
      ? { SSL_CERT_FILE: profile.caBundlePath }
      : {}),
    // The 1.5 CLI ignores AGY_ACP_FORCE_FILE_STORAGE and keeps Google sign-ins in one OS keyring
    // entry for every profile, unless it believes it runs over SSH. Sign-in then uses `--login`.
    ...(auth.authMethod === "oauth-personal"
      ? { SSH_CONNECTION: existingSshConnection || "127.0.0.1 0 127.0.0.1 0" }
      : {}),
    GEMINI_HOME: profile.geminiHome,
    AGY_ACP_FORCE_FILE_STORAGE: "1",
    BROWSER: profile.browserCommand,
    PYTHONUNBUFFERED: "1",
    ELECTRON_RUN_AS_NODE: "1",
    ...(profile.platform === "win32"
      ? { TEMP: tempDirectory, TMP: tempDirectory }
      : { TMPDIR: tempDirectory }),
  };
}

const MAX_ANTIGRAVITY_USER_CONFIG_BYTES = 256 * 1024;
const UserConfigObject = Schema.Record(Schema.String, Schema.Unknown);
const decodeUserConfigJson = Schema.decodeEffect(Schema.fromJsonString(UserConfigObject));
const decodeUserConfigObject = Schema.decodeUnknownEffect(UserConfigObject);

class AntigravitySkillLinkError extends Schema.TaggedError<AntigravitySkillLinkError>()(
  "AntigravitySkillLinkError",
  { link: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `Could not link Antigravity user skills at ${this.link}.`;
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isValidPluginEntryName(name: string): boolean {
  return (
    name.length > 0 &&
    name !== "." &&
    name !== ".." &&
    !name.includes("/") &&
    !name.includes("\\") &&
    !name.includes("\0")
  );
}

const textGenerationProfileSemaphore = Semaphore.makeUnsafe(1);
const userPluginsSyncSemaphore = Semaphore.makeUnsafe(1);

function sameNormalizedPath(a: string, b: string, platform: NodeJS.Platform): boolean {
  return platform === "win32" ? a.toLowerCase() === b.toLowerCase() : a === b;
}

function isNodeErrorCode(cause: unknown, code: string): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    "code" in cause &&
    (cause as { readonly code?: unknown }).code === code
  );
}

function isPathInsideDirectory(
  path: Path.Path,
  candidate: string,
  directory: string,
  platform: NodeJS.Platform,
): boolean {
  const normalizedCandidate = platform === "win32" ? candidate.toLowerCase() : candidate;
  const normalizedDirectory = platform === "win32" ? directory.toLowerCase() : directory;
  const relative = path.relative(normalizedDirectory, normalizedCandidate);
  return (
    relative === "" ||
    (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
  );
}

const ensurePrivateDirectory = Effect.fn("ensurePrivateDirectory")(function* (
  directory: string,
  platform: NodeJS.Platform,
) {
  const fs = yield* FileSystem.FileSystem;
  const existing = yield* fs.stat(directory).pipe(Effect.option);
  if (!(Option.isSome(existing) && existing.value.type === "Directory")) {
    yield* fs
      .makeDirectory(directory, { recursive: true, mode: 0o700 })
      .pipe(
        Effect.mapError(() =>
          authSupportError("The Antigravity profile directory could not be created."),
        ),
      );
  }
  if (platform !== "win32") {
    yield* fs
      .chmod(directory, 0o700)
      .pipe(
        Effect.mapError(() =>
          authSupportError("The Antigravity profile directory permissions could not be set."),
        ),
      );
  }
});

/** Sibling profile for text generation so interactive plugins stay out while `antigravity-acp` is shared. */
export function antigravityTextGenerationProfileDirectory(
  path: Path.Path,
  profileDirectory: string,
): string {
  return `${path.resolve(profileDirectory)}-textgen`;
}

/**
 * Prepares the plugin-free sibling profile (`<profile>-textgen`) by sharing only `antigravity-acp`
 * (credentials, settings, and disposable session files) with the main profile.
 */
export const prepareAntigravityTextGenerationProfile = Effect.fn(
  "prepareAntigravityTextGenerationProfile",
)(function* (input: {
  readonly profileDirectory: string;
  readonly platform?: NodeJS.Platform;
}): Effect.fn.Return<string, AcpErrors.AcpError, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = input.platform ?? (yield* HostProcess.Platform);
  const sourceProfile = path.resolve(input.profileDirectory);
  const sourceAcpDirectory = path.join(sourceProfile, "antigravity-acp");
  const textGenerationProfile = antigravityTextGenerationProfileDirectory(path, sourceProfile);
  const targetAcpLink = path.join(textGenerationProfile, "antigravity-acp");

  return yield* textGenerationProfileSemaphore.withPermits(1)(
    Effect.gen(function* () {
      for (const directory of [sourceProfile, sourceAcpDirectory, textGenerationProfile]) {
        yield* ensurePrivateDirectory(directory, platform);
      }

      const linkFailure = () =>
        authSupportError(
          "The Antigravity text-generation profile could not link its credentials directory.",
        );
      const readResolvedTargetLink = () =>
        fs.readLink(targetAcpLink).pipe(
          Effect.map((value) => Option.some(path.resolve(path.dirname(targetAcpLink), value))),
          Effect.catchReason("PlatformError", "NotFound", () =>
            Effect.succeed(Option.none<string>()),
          ),
        );
      const existingLink = yield* readResolvedTargetLink().pipe(
        Effect.catch(() =>
          Effect.tryPromise({
            try: () => NodeFSP.rmdir(targetAcpLink),
            catch: (cause) => new AntigravitySkillLinkError({ link: targetAcpLink, cause }),
          }).pipe(
            Effect.catch((error) =>
              isNodeErrorCode(error.cause, "ENOENT") ? Effect.void : Effect.fail(linkFailure()),
            ),
            Effect.as(Option.none<string>()),
          ),
        ),
      );
      if (
        Option.isSome(existingLink) &&
        sameNormalizedPath(existingLink.value, sourceAcpDirectory, platform)
      ) {
        return textGenerationProfile;
      }
      if (Option.isSome(existingLink)) {
        yield* fs.remove(targetAcpLink, { force: true }).pipe(Effect.mapError(linkFailure));
      }
      yield* Effect.tryPromise({
        try: () =>
          NodeFSP.symlink(
            sourceAcpDirectory,
            targetAcpLink,
            platform === "win32" ? "junction" : "dir",
          ),
        catch: (cause) => new AntigravitySkillLinkError({ link: targetAcpLink, cause }),
      }).pipe(
        Effect.catch((error) =>
          isNodeErrorCode(error.cause, "EEXIST")
            ? readResolvedTargetLink().pipe(
                Effect.mapError(linkFailure),
                Effect.flatMap((racedLink) =>
                  Option.isSome(racedLink) &&
                  sameNormalizedPath(racedLink.value, sourceAcpDirectory, platform)
                    ? Effect.void
                    : Effect.fail(linkFailure()),
                ),
              )
            : Effect.fail(linkFailure()),
        ),
      );
      return textGenerationProfile;
    }),
  );
});

const readDisabledAntigravityPlugins = Effect.fn("readDisabledAntigravityPlugins")(function* (
  userGeminiHome: string,
): Effect.fn.Return<Option.Option<ReadonlySet<string>>, never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const configPath = path.join(userGeminiHome, "config", "config.json");
  const warnAndFailClosed = (details: Record<string, unknown>) =>
    Effect.logWarning(
      "Antigravity user config.json could not be parsed; skipping user plugin linking.",
      { configPath, ...details },
    ).pipe(Effect.as(Option.none<ReadonlySet<string>>()));

  const statResult = yield* fs.stat(configPath).pipe(
    Effect.map((info) => ({ _tag: "Found" as const, info })),
    Effect.catchReason("PlatformError", "NotFound", () =>
      Effect.succeed({ _tag: "NotFound" as const }),
    ),
    Effect.catch((error) => Effect.succeed({ _tag: "Error" as const, error })),
  );
  if (statResult._tag === "NotFound") {
    const isDanglingLink = yield* fs.readLink(configPath).pipe(
      Effect.as(true),
      Effect.orElseSucceed(() => false),
    );
    if (isDanglingLink) {
      return yield* warnAndFailClosed({ reason: "dangling-symlink" });
    }
    return Option.some(new Set<string>());
  }
  if (statResult._tag === "Error") {
    return yield* warnAndFailClosed({ error: statResult.error });
  }
  if (
    statResult.info.type !== "File" ||
    statResult.info.size > BigInt(MAX_ANTIGRAVITY_USER_CONFIG_BYTES)
  ) {
    return yield* warnAndFailClosed({
      type: statResult.info.type,
      size: Number(statResult.info.size),
    });
  }
  const contents = yield* fs.readFileString(configPath).pipe(
    Effect.asSome,
    Effect.catch((error) =>
      Effect.logWarning(
        "Antigravity user config.json could not be read; skipping user plugin linking.",
        { configPath, error },
      ).pipe(Effect.as(Option.none<string>())),
    ),
  );
  if (Option.isNone(contents)) return Option.none();
  const parsed = yield* decodeUserConfigJson(contents.value).pipe(
    Effect.asSome,
    Effect.catch((error) =>
      Effect.logWarning("Antigravity user config.json is invalid; skipping user plugin linking.", {
        configPath,
        error,
      }).pipe(Effect.as(Option.none<Record<string, unknown>>())),
    ),
  );
  if (Option.isNone(parsed) || !isPlainRecord(parsed.value)) {
    return Option.none();
  }
  const rawPlugins = parsed.value.plugins;
  if (rawPlugins === undefined) {
    return Option.some(new Set<string>());
  }
  const plugins = yield* decodeUserConfigObject(rawPlugins).pipe(
    Effect.asSome,
    Effect.catch((error) =>
      Effect.logWarning(
        "Antigravity user config.json plugins field is invalid; skipping user plugin linking.",
        { configPath, error },
      ).pipe(Effect.as(Option.none<Record<string, unknown>>())),
    ),
  );
  if (Option.isNone(plugins) || !isPlainRecord(plugins.value)) {
    return Option.none();
  }
  const disabled = new Set<string>();
  for (const [name, entry] of Object.entries(plugins.value)) {
    if (isPlainRecord(entry) && entry.enabled === false) {
      disabled.add(name);
    }
  }
  return Option.some(disabled);
});

const cleanupAntigravityManagedPlugins = Effect.fn("cleanupAntigravityManagedPlugins")(
  function* (input: {
    readonly profilePlugins: string;
    readonly userPlugins: string;
    readonly platform: NodeJS.Platform;
  }): Effect.fn.Return<
    boolean,
    AntigravitySkillLinkError | PlatformError.PlatformError,
    FileSystem.FileSystem | Path.Path
  > {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const { profilePlugins, userPlugins, platform } = input;
    const isWholeLink = yield* fs.readLink(profilePlugins).pipe(
      Effect.as(true),
      Effect.orElseSucceed(() => false),
    );
    if (isWholeLink) {
      yield* fs.remove(profilePlugins, { force: true });
      return true;
    }
    const info = yield* fs.stat(profilePlugins).pipe(
      Effect.asSome,
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
    );
    if (Option.isNone(info)) return true;
    if (info.value.type !== "Directory") return false;
    const resolvedUserPlugins = path.resolve(userPlugins);
    const entries = yield* fs.readDirectory(profilePlugins);
    const managedLinks: Array<string> = [];
    let hasUnmanagedEntry = false;
    for (const entry of entries) {
      const entryPath = path.join(profilePlugins, entry);
      const linked = yield* fs.readLink(entryPath).pipe(
        Effect.map((value) => Option.some(path.resolve(path.dirname(entryPath), value))),
        Effect.orElseSucceed(() => Option.none<string>()),
      );
      if (
        Option.isSome(linked) &&
        isPathInsideDirectory(path, linked.value, resolvedUserPlugins, platform)
      ) {
        managedLinks.push(entryPath);
      } else {
        hasUnmanagedEntry = true;
      }
    }
    for (const entryPath of managedLinks) {
      yield* fs.remove(entryPath, { force: true });
    }
    if (hasUnmanagedEntry) {
      return false;
    }
    return yield* Effect.tryPromise({
      try: () => NodeFSP.rmdir(profilePlugins),
      catch: (cause) => new AntigravitySkillLinkError({ link: profilePlugins, cause }),
    }).pipe(
      Effect.as(true),
      Effect.catch((error) =>
        isNodeErrorCode(error.cause, "ENOENT")
          ? Effect.succeed(true)
          : isNodeErrorCode(error.cause, "ENOTEMPTY")
            ? Effect.succeed(false)
            : Effect.fail(error),
      ),
    );
  },
);

const syncAntigravityUserPlugins = Effect.fn("syncAntigravityUserPlugins")(function* (input: {
  readonly profileDirectory: string;
  readonly userGeminiHome: string;
  readonly platform: NodeJS.Platform;
  readonly linkUserPlugins: boolean;
}): Effect.fn.Return<void, never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profilePlugins = path.join(input.profileDirectory, "config", "plugins");
  const userPlugins = path.join(input.userGeminiHome, "config", "plugins");

  yield* userPluginsSyncSemaphore
    .withPermits(1)(
      Effect.gen(function* () {
        const resolvedUserPlugins = path.resolve(userPlugins);
        if (sameNormalizedPath(path.resolve(profilePlugins), resolvedUserPlugins, input.platform)) {
          return;
        }
        if (!input.linkUserPlugins) {
          yield* cleanupAntigravityManagedPlugins({
            profilePlugins,
            userPlugins,
            platform: input.platform,
          });
          return;
        }
        const disabledPlugins = yield* readDisabledAntigravityPlugins(input.userGeminiHome);
        if (Option.isNone(disabledPlugins)) {
          yield* cleanupAntigravityManagedPlugins({
            profilePlugins,
            userPlugins,
            platform: input.platform,
          });
          return;
        }
        const userPluginsInfo = yield* fs.stat(userPlugins).pipe(
          Effect.asSome,
          Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
        );
        if (Option.isNone(userPluginsInfo) || userPluginsInfo.value.type !== "Directory") {
          yield* cleanupAntigravityManagedPlugins({
            profilePlugins,
            userPlugins,
            platform: input.platform,
          });
          return;
        }
        if (disabledPlugins.value.size === 0) {
          const existingLink = yield* fs.readLink(profilePlugins).pipe(
            Effect.map((value) => Option.some(path.resolve(path.dirname(profilePlugins), value))),
            Effect.orElseSucceed(() => Option.none<string>()),
          );
          if (
            Option.isSome(existingLink) &&
            sameNormalizedPath(existingLink.value, resolvedUserPlugins, input.platform)
          ) {
            return;
          }
          const cleared = yield* cleanupAntigravityManagedPlugins({
            profilePlugins,
            userPlugins,
            platform: input.platform,
          });
          if (!cleared) return;
          yield* fs.makeDirectory(path.dirname(profilePlugins), { recursive: true });
          yield* Effect.tryPromise({
            try: () =>
              NodeFSP.symlink(
                userPlugins,
                profilePlugins,
                input.platform === "win32" ? "junction" : "dir",
              ),
            catch: (cause) => new AntigravitySkillLinkError({ link: profilePlugins, cause }),
          }).pipe(
            Effect.catchIf(
              (error) => isNodeErrorCode(error.cause, "EEXIST"),
              (error) =>
                fs.readLink(profilePlugins).pipe(
                  Effect.map((value) =>
                    sameNormalizedPath(
                      path.resolve(path.dirname(profilePlugins), value),
                      resolvedUserPlugins,
                      input.platform,
                    ),
                  ),
                  Effect.orElseSucceed(() => false),
                  Effect.flatMap((matches) => (matches ? Effect.void : Effect.fail(error))),
                ),
            ),
          );
          return;
        }

        const isWholeLink = yield* fs.readLink(profilePlugins).pipe(
          Effect.as(true),
          Effect.orElseSucceed(() => false),
        );
        if (isWholeLink) {
          yield* fs.remove(profilePlugins, { force: true });
        }
        const existingProfilePlugins = yield* fs.stat(profilePlugins).pipe(
          Effect.asSome,
          Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
        );
        if (
          Option.isSome(existingProfilePlugins) &&
          existingProfilePlugins.value.type !== "Directory"
        ) {
          return;
        }
        if (Option.isNone(existingProfilePlugins)) {
          yield* fs.makeDirectory(profilePlugins, { recursive: true, mode: 0o700 });
        }
        if (input.platform !== "win32") {
          yield* fs.chmod(profilePlugins, 0o700);
        }

        const enabledTargets = new Map<string, string>();
        for (const name of yield* fs.readDirectory(userPlugins)) {
          if (!isValidPluginEntryName(name) || disabledPlugins.value.has(name)) continue;
          const target = path.join(userPlugins, name);
          const entryInfo = yield* fs.stat(target).pipe(Effect.option);
          if (Option.isNone(entryInfo) || entryInfo.value.type !== "Directory") continue;
          enabledTargets.set(name, path.resolve(target));
        }

        for (const entry of yield* fs.readDirectory(profilePlugins)) {
          const entryPath = path.join(profilePlugins, entry);
          const linked = yield* fs.readLink(entryPath).pipe(
            Effect.map((value) => Option.some(path.resolve(path.dirname(entryPath), value))),
            Effect.orElseSucceed(() => Option.none<string>()),
          );
          if (
            Option.isNone(linked) ||
            !isPathInsideDirectory(path, linked.value, resolvedUserPlugins, input.platform)
          ) {
            continue;
          }
          const expected = enabledTargets.get(entry);
          if (!expected || !sameNormalizedPath(linked.value, expected, input.platform)) {
            yield* fs.remove(entryPath, { force: true });
          }
        }

        for (const [name, target] of enabledTargets) {
          const entryPath = path.join(profilePlugins, name);
          const existingLink = yield* fs.readLink(entryPath).pipe(
            Effect.map((value) => Option.some(path.resolve(path.dirname(entryPath), value))),
            Effect.orElseSucceed(() => Option.none<string>()),
          );
          if (
            Option.isSome(existingLink) &&
            sameNormalizedPath(existingLink.value, target, input.platform)
          ) {
            continue;
          }
          const existingStat = yield* fs.stat(entryPath).pipe(Effect.option);
          if (Option.isSome(existingStat)) continue;
          yield* Effect.tryPromise({
            try: () =>
              NodeFSP.symlink(target, entryPath, input.platform === "win32" ? "junction" : "dir"),
            catch: (cause) => new AntigravitySkillLinkError({ link: entryPath, cause }),
          }).pipe(
            Effect.catch((error) =>
              isNodeErrorCode(error.cause, "EEXIST")
                ? fs.readLink(entryPath).pipe(
                    Effect.map((value) =>
                      sameNormalizedPath(
                        path.resolve(path.dirname(entryPath), value),
                        target,
                        input.platform,
                      ),
                    ),
                    Effect.orElseSucceed(() => false),
                    Effect.flatMap((matches) =>
                      matches
                        ? Effect.void
                        : Effect.logWarning(
                            "Antigravity user plugin is not linked into the profile.",
                            { link: entryPath, target, error },
                          ),
                    ),
                  )
                : Effect.logWarning("Antigravity user plugin is not linked into the profile.", {
                    link: entryPath,
                    target,
                    error,
                  }),
            ),
          );
        }
      }),
    )
    .pipe(
      Effect.catch((error) =>
        Effect.logWarning("Antigravity user plugins are not linked into the profile.", {
          link: profilePlugins,
          target: userPlugins,
          error,
        }),
      ),
    );
});

/** Private GEMINI_HOME isolates credentials/MCP; link global skills and optional enabled plugins from ~/.gemini. */
const linkAntigravityUserSkills = Effect.fn("linkAntigravityUserSkills")(function* (input: {
  readonly profileDirectory: string;
  readonly userHome: string;
  readonly platform: NodeJS.Platform;
  readonly linkUserPlugins?: boolean;
}): Effect.fn.Return<void, never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const userGeminiHome = path.resolve(input.userHome, ".gemini");
  const links = antigravityUserSkillDirectories(path, input.profileDirectory);
  const targets = antigravityUserSkillDirectories(path, userGeminiHome);
  for (const [link, target] of [
    [links[0], targets[0]],
    [links[1], targets[1]],
  ] as const) {
    yield* Effect.gen(function* () {
      if (path.resolve(link) === path.resolve(target)) return;
      // The harness creates these folders itself, and mkdir fails on a dangling link.
      if (!(yield* fs.exists(target))) {
        const linked = yield* fs.readLink(link).pipe(
          Effect.as(true),
          Effect.orElseSucceed(() => false),
        );
        if (linked) yield* fs.remove(link);
        return;
      }
      const existing = yield* fs.readLink(link).pipe(
        Effect.map((value): string | undefined => path.resolve(path.dirname(link), value)),
        Effect.catchReason("PlatformError", "NotFound", () => Effect.undefined),
        // An empty folder is the harness's placeholder from a launch without the link.
        // rmdir refuses a non-empty folder, so content added meanwhile is never deleted.
        Effect.catch((error) =>
          Effect.tryPromise({
            try: () => NodeFSP.rmdir(link),
            catch: (cause) => new AntigravitySkillLinkError({ link, cause }),
          }).pipe(
            Effect.as(undefined),
            Effect.mapError(() => error),
          ),
        ),
      );
      if (existing === target) return;
      if (existing !== undefined) {
        yield* fs.remove(link);
      }
      yield* fs.makeDirectory(path.dirname(link), { recursive: true });
      yield* Effect.tryPromise({
        try: () => NodeFSP.symlink(target, link, input.platform === "win32" ? "junction" : "dir"),
        catch: (cause) => new AntigravitySkillLinkError({ link, cause }),
      });
    }).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Antigravity user skills are not linked into the profile.", {
          link,
          target,
          error,
        }),
      ),
    );
  }
  yield* syncAntigravityUserPlugins({
    profileDirectory: input.profileDirectory,
    userGeminiHome,
    platform: input.platform,
    linkUserPlugins: input.linkUserPlugins ?? true,
  });
});

export const prepareAntigravityProfile = Effect.fn("prepareAntigravityProfile")(function* (input: {
  readonly profileDirectory: string;
  readonly baseEnv?: NodeJS.ProcessEnv;
  readonly runtimeExecutablePath?: string;
  readonly platform?: NodeJS.Platform;
  readonly auth?: AntigravityAuthConfig;
  readonly userHome?: string;
  readonly tempDirectory?: string;
  readonly linkUserPlugins?: boolean;
}) {
  const auth = input.auth ?? ANTIGRAVITY_PERSONAL_AUTH;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = input.platform ?? (yield* HostProcess.Platform);
  const userHome =
    input.userHome ??
    resolveAntigravityUserHome(
      platform,
      input.baseEnv ?? process.env,
      yield* HostProcess.HomeDirectory,
    );
  const runtimeExecutablePath =
    input.runtimeExecutablePath ??
    (yield* resolveNodeExecutable("Antigravity sign-in", input.baseEnv).pipe(
      Effect.mapError(
        (cause) =>
          new AcpErrors.AcpTransportError({
            detail: nodeRuntimeUnavailableMessage("Antigravity sign-in"),
            cause,
          }),
      ),
    ));
  const helperExecutable =
    platform === "win32" ? runtimeExecutablePath.replaceAll("\\", "/") : runtimeExecutablePath;
  const browserArguments = [helperExecutable, "-e", browserHelperSource, "--", "%s"];
  const browserCommand = browserArguments.map(quoteBrowserArgument).join(" ");
  if (
    browserCommand.includes(platform === "win32" ? ";" : ":") ||
    helperExecutable.includes("\r") ||
    helperExecutable.includes("\n") ||
    helperExecutable.includes("\0") ||
    helperExecutable.includes("%s")
  ) {
    return yield* authSupportError(
      "The T3 runtime path cannot be used to suppress Antigravity browser launches.",
    );
  }

  const geminiHome = path.resolve(input.profileDirectory);
  const acpDirectory = path.join(geminiHome, "antigravity-acp");
  const tempDirectory = input.tempDirectory ?? path.join(acpDirectory, "tmp");
  let caBundlePath: string | undefined;
  if (platform === "linux") {
    // Bundled Python may look for certificates at paths absent on this distribution.
    // Relative paths would resolve differently when the agent runs in the project directory.
    for (const candidate of [
      (input.baseEnv ?? process.env).NIX_SSL_CERT_FILE,
      "/etc/ssl/certs/ca-bundle.crt",
      "/etc/ssl/certs/ca-certificates.crt",
      "/etc/pki/tls/certs/ca-bundle.crt",
      "/etc/ssl/cert.pem",
    ]) {
      if (
        candidate &&
        path.isAbsolute(candidate) &&
        (yield* fs.exists(candidate).pipe(Effect.orElseSucceed(() => false)))
      ) {
        caBundlePath = candidate;
        break;
      }
    }
  }
  const profile: AntigravityProfile = {
    platform,
    geminiHome,
    acpDirectory,
    tokenPath: path.join(acpDirectory, "acp_token.json"),
    tempDirectory,
    browserCommand,
    ...(caBundlePath === undefined ? {} : { caBundlePath }),
  };
  const environment = antigravityEnvironment(profile, input.baseEnv ?? process.env, auth);
  yield* Effect.gen(function* () {
    const child = yield* spawner.spawn(
      ChildProcess.make(helperExecutable, ["-e", browserHelperSource, "--", browserPreflightUrl], {
        env: environment,
        extendEnv: false,
        shell: false,
      }),
    );
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        collectUint8StreamText({ stream: child.stdout, maxBytes: 4_096 }),
        collectUint8StreamText({ stream: child.stderr, maxBytes: 4_096 }),
        child.exitCode,
      ],
      { concurrency: "unbounded" },
    );
    if (
      Number(exitCode) !== 0 ||
      stdout.bytes !== 0 ||
      stdout.truncated ||
      stderr.truncated ||
      stderr.text !== `${ANTIGRAVITY_AUTH_BROWSER_MARKER}"${browserPreflightUrl}"\n`
    ) {
      return yield* authSupportError("Antigravity browser suppression could not be verified.");
    }
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: "30 seconds",
      orElse: () =>
        Effect.fail(authSupportError("Antigravity browser suppression verification timed out.")),
    }),
    Effect.mapError((error) =>
      error._tag === "AcpTransportError"
        ? error
        : authSupportError("Antigravity browser suppression could not be verified."),
    ),
  );

  for (const directory of [geminiHome, acpDirectory, tempDirectory]) {
    yield* ensurePrivateDirectory(directory, platform);
  }
  yield* fs
    .writeFileString(path.join(acpDirectory, "settings.json"), antigravityProfileSettings(auth))
    .pipe(
      Effect.mapError(() =>
        authSupportError("The Antigravity profile settings could not be written."),
      ),
    );
  yield* linkAntigravityUserSkills({
    profileDirectory: geminiHome,
    userHome,
    platform,
    ...(input.linkUserPlugins === undefined ? {} : { linkUserPlugins: input.linkUserPlugins }),
  });
  return profile;
});

/** Reads the signed-in Google email from the instance's 1.5.x profile token file, if present. */
export const readAntigravityStoredEmail = Effect.fn("readAntigravityStoredEmail")(function* (
  profileDirectory: string,
): Effect.fn.Return<string | undefined, never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tokenPath = path.join(profileDirectory, "antigravity-acp", "antigravity-oauth-token");
  return yield* fs.readFileString(tokenPath).pipe(
    Effect.flatMap(decodeStoredSignInEmail),
    Effect.map((stored) => {
      const email = stored.id_token_email?.trim();
      return email && email.length > 0 ? email : undefined;
    }),
    Effect.orElseSucceed(() => undefined),
  );
});

export function buildAntigravityAcpSpawnInput(input: {
  readonly installation: {
    readonly executablePath: string;
    readonly harnessPath: string | null;
  };
  readonly profile: AntigravityProfile;
  readonly cwd: string;
  readonly baseEnv?: NodeJS.ProcessEnv;
  readonly auth?: AntigravityAuthConfig;
  readonly runtimeTempDirectory?: string;
}): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: input.installation.executablePath,
    // The agy CLI has no harness sibling and serves ACP through its `acp` subcommand.
    args:
      input.installation.harnessPath === null
        ? [
            "acp",
            "--gemini-dir",
            input.profile.geminiHome,
            "--app-data-dir",
            // The CLI joins this beneath gemini-dir, including when the value is absolute.
            "antigravity-acp",
          ]
        : input.profile.platform === "linux"
          ? ["--uid="]
          : [],
    cwd: input.cwd,
    env: {
      ...antigravityEnvironment(
        input.profile,
        input.baseEnv ?? process.env,
        input.auth ?? ANTIGRAVITY_PERSONAL_AUTH,
        input.runtimeTempDirectory,
      ),
      ...(input.installation.harnessPath === null
        ? {}
        : { ANTIGRAVITY_HARNESS_PATH: input.installation.harnessPath }),
    },
    extendEnv: false,
  };
}

/** Where `agy acp --login` sends Google; the page shows a code the user pastes back. */
const ANTIGRAVITY_CODE_REDIRECT_URI = "https://antigravity.google/oauth-callback";

export const parseAntigravityAuthorizationUrl = Effect.fn("parseAntigravityAuthorizationUrl")(
  function* (
    authorizationUrl: string,
    kind: "loopback" | "code" = "loopback",
  ): Effect.fn.Return<AntigravityAuthorizationUrl, AcpErrors.AcpError> {
    const invalidUrl = () =>
      authSupportError("Antigravity returned an invalid Google sign-in URL.");
    if (authorizationUrl.length > maxAuthorizationUrlLength || /\s/.test(authorizationUrl)) {
      return yield* invalidUrl();
    }
    const url = yield* decodeUrl(authorizationUrl).pipe(Effect.mapError(invalidUrl));
    const state = url.searchParams.get("state");
    const redirectUri = url.searchParams.get("redirect_uri");
    if (
      url.origin !== "https://accounts.google.com" ||
      url.pathname !== (kind === "loopback" ? "/o/oauth2/v2/auth" : "/o/oauth2/auth") ||
      url.username !== "" ||
      url.password !== "" ||
      url.hash !== "" ||
      url.searchParams.getAll("state").length !== 1 ||
      url.searchParams.getAll("redirect_uri").length !== 1 ||
      url.searchParams.getAll("response_type").length !== 1 ||
      url.searchParams.get("response_type") !== "code" ||
      state === null ||
      state.length === 0 ||
      state.length > 512 ||
      /\s/.test(state) ||
      redirectUri === null ||
      (kind === "loopback"
        ? !/^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}\/$/.test(redirectUri)
        : redirectUri !== ANTIGRAVITY_CODE_REDIRECT_URI)
    ) {
      return yield* invalidUrl();
    }
    if (kind === "code") return { authorizationUrl, redirectUri, state };
    const redirect = yield* decodeUrl(redirectUri).pipe(Effect.mapError(invalidUrl));
    if (Number(redirect.port) < 1_024) return yield* invalidUrl();
    return { authorizationUrl, redirectUri, state };
  },
);

/**
 * What `agy acp --login` reads after its link: the code the Antigravity page shows, or that page's
 * URL, which must answer this sign-in. Undefined for anything else, including any line break.
 */
export function antigravityLoginResponse(
  pending: AntigravityAuthorizationUrl,
  response: string,
): string | undefined {
  const value = response.trim();
  if (/^[\w./~-]{1,2048}$/.test(value)) return value;
  if (value.length > maxAuthorizationUrlLength || /\s/.test(value)) return undefined;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return undefined;
  }
  const url = URL.parse(value);
  if (
    url === null ||
    `${url.origin}${url.pathname}` !== pending.redirectUri ||
    url.username !== "" ||
    url.password !== "" ||
    url.hash !== "" ||
    url.searchParams.has("error") ||
    url.searchParams.getAll("state").length !== 1 ||
    url.searchParams.get("state") !== pending.state ||
    url.searchParams.getAll("code").length !== 1 ||
    !url.searchParams.get("code")
  ) {
    return undefined;
  }
  return value;
}

export function makeAntigravityStdoutTransform(
  input: {
    readonly onAuthorizationUrl?: (
      authorizationUrl: string,
    ) => Effect.Effect<void, AcpErrors.AcpError>;
  } = {},
) {
  const handleLine = Effect.fn("antigravityAuthSupport.handleStdoutLine")(function* (
    line: Uint8Array,
  ) {
    if (!authPrefixBytes.every((byte, index) => line[index] === byte)) return [line];
    const message = new TextDecoder().decode(line).replace(/\r?\n$/, "");
    const request = yield* parseAntigravityAuthorizationUrl(
      message.slice(ANTIGRAVITY_AUTH_STDOUT_PREFIX.length),
    );
    if (!input.onAuthorizationUrl) {
      return yield* authSupportError(ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE);
    }
    yield* input.onAuthorizationUrl(request.authorizationUrl);
    return [];
  });

  return (
    stdout: ChildProcessSpawner.ChildProcessHandle["stdout"],
  ): Stream.Stream<Uint8Array, PlatformError.PlatformError | AcpErrors.AcpError> =>
    Stream.suspend(() => {
      let pending: Uint8Array[] = [];
      let pendingBytes = 0;
      const finishLine = () => {
        const line = Buffer.concat(pending, pendingBytes);
        pending = [];
        pendingBytes = 0;
        return line;
      };
      return stdout.pipe(
        Stream.mapEffect(
          Effect.fn("antigravityAuthSupport.splitStdoutLines")(function* (chunk: Uint8Array) {
            const lines: Uint8Array[] = [];
            let offset = 0;
            while (offset < chunk.byteLength) {
              const newline = chunk.indexOf(10, offset);
              const end = newline === -1 ? chunk.byteLength : newline + 1;
              const part = chunk.subarray(offset, end);
              if (pendingBytes + part.byteLength > maxStdoutLineBytes) {
                return yield* authSupportError(
                  "Antigravity sent a protocol line that is too large.",
                );
              }
              pending.push(part);
              pendingBytes += part.byteLength;
              if (newline !== -1) lines.push(finishLine());
              offset = end;
            }
            return lines;
          }),
        ),
        Stream.flatMap(Stream.fromIterable),
        Stream.concat(
          Stream.suspend(() => (pendingBytes > 0 ? Stream.succeed(finishLine()) : Stream.empty)),
        ),
        Stream.mapEffect(handleLine),
        Stream.flatMap(Stream.fromIterable),
      );
    });
}

export function makeAntigravityStderrHandler(
  input: {
    readonly onAuthorizationUrl?: (
      authorizationUrl: string,
    ) => Effect.Effect<void, AcpErrors.AcpError>;
  } = {},
) {
  let pending = "";
  const handleLine = (line: string) => {
    const message = line.endsWith("\r") ? line.slice(0, -1) : line;
    if (message.length > maxBrowserHelperLineLength) {
      return Effect.void;
    }
    const url = message.startsWith(ANTIGRAVITY_AUTH_STDOUT_PREFIX)
      ? Effect.succeed(message.slice(ANTIGRAVITY_AUTH_STDOUT_PREFIX.length))
      : message.startsWith(ANTIGRAVITY_AUTH_BROWSER_MARKER)
        ? decodeBrowserHelperUrl(message.slice(ANTIGRAVITY_AUTH_BROWSER_MARKER.length))
        : undefined;
    if (url === undefined) return Effect.void;
    return url.pipe(
      Effect.flatMap(parseAntigravityAuthorizationUrl),
      Effect.matchEffect({
        onFailure: () => Effect.void,
        onSuccess: (request) =>
          input.onAuthorizationUrl
            ? input.onAuthorizationUrl(request.authorizationUrl)
            : Effect.fail(authSupportError(ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE)),
      }),
    );
  };

  return Effect.fn("antigravityAuthSupport.handleStderr")(function* (text: string) {
    const lines = `${pending}${text}`.split("\n");
    pending = lines.pop() ?? "";
    if (pending.length > maxBrowserHelperLineLength) pending = "";
    yield* Effect.forEach(lines, handleLine, { discard: true });
  });
}
