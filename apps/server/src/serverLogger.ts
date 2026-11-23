import * as HostProcess from "@t3tools/shared/HostProcess";
import * as SharedObservability from "@t3tools/shared/observability";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Redactable from "effect/Redactable";
import * as References from "effect/References";
import * as OtlpExporter from "effect/observability/OtlpExporter";
import * as OtlpLogger from "effect/observability/OtlpLogger";

import * as ServerConfig from "./config.ts";
import * as OtlpExportFailures from "./observability/OtlpExportFailures.ts";

// HTTP errors logged with --log-ws-events carry the request URL and its connection ticket.
const redactingCredentials = <Output>(logger: Logger.Logger<unknown, Output>) =>
  Logger.make<unknown, Output>((options) =>
    logger.log({
      ...options,
      cause: SharedObservability.redactCauseCredentials(options.cause),
      message: redactLogValue(options.message),
    }),
  );

type RedactVisit = { readonly clone: object; referenced: boolean };

function redactLogValue(value: unknown, seen?: WeakMap<object, RedactVisit>): unknown {
  if (typeof value === "string") {
    return SharedObservability.redactUrlCredentials(value);
  }
  if (Cause.isCause(value)) {
    return SharedObservability.redactCauseCredentials(value);
  }
  if (typeof value !== "object" || value === null) return value;
  // A message with its own redaction hook already decides how it prints.
  if (Redactable.isRedactable(value)) return value;
  const visited = seen ?? new WeakMap<object, RedactVisit>();
  const existing = visited.get(value);
  if (existing !== undefined) {
    existing.referenced = true;
    return existing.clone;
  }
  if (value instanceof Error) {
    const redactedMessage = SharedObservability.redactUrlCredentials(value.message);
    const redactedStack =
      typeof value.stack === "string"
        ? SharedObservability.redactUrlCredentials(value.stack)
        : value.stack;
    const sanitized = new Error(redactedMessage);
    const visit: RedactVisit = { clone: sanitized, referenced: false };
    visited.set(value, visit);
    const descriptors = Object.getOwnPropertyDescriptors(value);
    let changed = redactedMessage !== value.message || redactedStack !== value.stack;
    for (const key of Reflect.ownKeys(descriptors)) {
      if (key === "message" || key === "stack") continue;
      const descriptor = descriptors[key as keyof typeof descriptors];
      if (descriptor === undefined || !("value" in descriptor)) continue;
      const next = redactLogValue(descriptor.value, visited);
      if (next !== descriptor.value) {
        descriptor.value = next;
        changed = true;
      }
    }
    const inheritedCause = !("cause" in descriptors) ? value.cause : undefined;
    const redactedInheritedCause =
      inheritedCause !== undefined ? redactLogValue(inheritedCause, visited) : undefined;
    if (redactedInheritedCause !== inheritedCause) {
      changed = true;
    }
    if (!changed && !visit.referenced) {
      visited.set(value, { clone: value, referenced: false });
      return value;
    }
    if (!("name" in descriptors)) {
      sanitized.name = value.name;
    }
    if (redactedStack !== undefined) {
      sanitized.stack = redactedStack;
    }
    for (const key of Reflect.ownKeys(descriptors)) {
      if (key === "message" || key === "stack") continue;
      const descriptor = descriptors[key as keyof typeof descriptors];
      if (descriptor !== undefined && "value" in descriptor) {
        Object.defineProperty(sanitized, key, descriptor);
      }
    }
    if (redactedInheritedCause !== undefined) {
      Object.defineProperty(sanitized, "cause", {
        value: redactedInheritedCause,
        writable: true,
        enumerable: false,
        configurable: true,
      });
    }
    return sanitized;
  }
  if (Array.isArray(value)) {
    const sanitized = new Array<unknown>(value.length);
    const visit: RedactVisit = { clone: sanitized, referenced: false };
    visited.set(value, visit);
    let changed = false;
    for (let index = 0; index < value.length; index += 1) {
      const entry = value[index];
      const next = redactLogValue(entry, visited);
      sanitized[index] = next;
      if (next !== entry) changed = true;
    }
    if (!changed && !visit.referenced) {
      visited.set(value, { clone: value, referenced: false });
      return value;
    }
    return sanitized;
  }
  const prototype: unknown = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) return value;
  const sanitized = Object.create(prototype) as object;
  const visit: RedactVisit = { clone: sanitized, referenced: false };
  visited.set(value, visit);
  // Copy descriptors to preserve symbols without invoking getters.
  const descriptors = Object.getOwnPropertyDescriptors(value);
  let changed = false;
  for (const key of Reflect.ownKeys(descriptors)) {
    const descriptor = descriptors[key as keyof typeof descriptors];
    if (descriptor === undefined || !("value" in descriptor)) continue;
    const next = redactLogValue(descriptor.value, visited);
    if (next !== descriptor.value) {
      descriptor.value = next;
      changed = true;
    }
  }
  if (!changed && !visit.referenced) {
    visited.set(value, { clone: value, referenced: false });
    return value;
  }
  Object.defineProperties(sanitized, descriptors);
  return sanitized;
}

// consolePretty collapses Cause fields via util.inspect; render them with Cause.pretty like standalone arguments.
const consolePretty = (): Logger.Logger<unknown, void> => {
  const pretty = redactingCredentials(Logger.consolePretty());
  return Logger.make((options) =>
    pretty.log({
      ...options,
      message: Array.isArray(options.message)
        ? options.message.map(prettyCauseFields)
        : prettyCauseFields(options.message),
    }),
  );
};

function prettyCauseFields(value: unknown): unknown {
  if (typeof value !== "object" || value === null) return value;
  const prototype: unknown = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) return value;
  // A message with its own redaction hook already decides how it prints.
  if (Redactable.isRedactable(value)) return value;
  // Copy descriptors to preserve symbols without invoking getters.
  const descriptors = Object.getOwnPropertyDescriptors(value);
  let changed = false;
  for (const descriptor of Object.values(descriptors)) {
    if ("value" in descriptor && Cause.isCause(descriptor.value)) {
      descriptor.value = SharedObservability.redactUrlCredentials(
        Cause.pretty(SharedObservability.redactCauseCredentials(descriptor.value)),
      );
      changed = true;
    }
  }
  return changed ? Object.create(prototype, descriptors) : value;
}

export const layer = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const hostName = yield* HostProcess.Hostname;
  const layerMinimumLogLevel = Layer.succeed(References.MinimumLogLevel, config.logLevel);

  const logs = config.otlpLogsExport;
  const otlpLogger =
    config.otlpLogsUrl === undefined
      ? undefined
      : OtlpLogger.make({
          url: config.otlpLogsUrl,
          exportInterval: `${logs.exportIntervalMs} millis`,
          headers: logs.headers,
          resource: ServerConfig.otlpResource(config, hostName),
        }).pipe(Effect.map(redactingCredentials));

  // Logger.layer replaces the whole set; name every logger here.
  // OTLP includes traced messages as log records, so also using tracerLogger would duplicate in-span exports.
  const layerLogger = Logger.layer(
    otlpLogger === undefined
      ? [consolePretty(), redactingCredentials(Logger.tracerLogger)]
      : [consolePretty(), otlpLogger],
    { mergeWithExisting: false },
  ).pipe(
    Layer.provide(OtlpExporter.layerFlusher),
    Layer.provide(OtlpExportFailures.layer("logs")),
    Layer.provide(SharedObservability.layerOtlpSerialization(logs.protocol)),
  );

  return Layer.mergeAll(layerLogger, layerMinimumLogLevel);
}).pipe(Layer.unwrap);
