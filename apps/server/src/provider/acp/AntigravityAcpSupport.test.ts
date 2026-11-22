import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  ANTIGRAVITY_DEFAULT_MODEL,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  type ChatAttachment,
} from "@t3tools/contracts";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/compat";

import * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import {
  applyAntigravityAcpModelSelection,
  antigravityClientInfo,
  antigravitySessionMeta,
  buildAntigravityPrompt,
  isQualifiedAntigravityRelease,
  makeAntigravityAcpRuntime,
} from "./AntigravityAcpSupport.ts";

const modelConfig = {
  id: "model",
  name: "Model",
  type: "select",
  currentValue: "gemini-default",
  options: [
    { value: "gemini-default", name: "Gemini default" },
    { value: "gemini-saved", name: "Gemini saved" },
  ],
} satisfies EffectAcpSchema.SessionConfigOption;

function makeModelRuntime(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> = [modelConfig],
  failure?: EffectAcpErrors.AcpError,
) {
  const selections: string[] = [];
  const setModel = Effect.fn("AntigravityAcpSupportTest.setModel")(function* (model: string) {
    if (failure) return yield* failure;
    selections.push(model);
  });
  const setConfigOption = Effect.fn("AntigravityAcpSupportTest.setConfigOption")(function* (
    configId: string,
    value: string | boolean,
    options?: { readonly allowUnlistedValue?: boolean },
  ) {
    selections.push(
      `${configId}=${String(value)}${options?.allowUnlistedValue === true ? " (unlisted)" : ""}`,
    );
    return { configOptions };
  });
  return {
    runtime: { getConfigOptions: Effect.succeed(configOptions), setModel, setConfigOption },
    selections,
  };
}

describe("Antigravity release gates", () => {
  it.each([
    { version: "1.4.1", filter: false, native: false, ownName: false, advertisedAppend: false },
    { version: "1.4.2", filter: true, native: false, ownName: false, advertisedAppend: false },
    { version: "1.4.4", filter: true, native: false, ownName: false, advertisedAppend: false },
    { version: "1.4.5", filter: true, native: true, ownName: false, advertisedAppend: false },
    { version: "1.4.6", filter: false, native: false, ownName: false, advertisedAppend: false },
    { version: "1.5.0", filter: false, native: false, ownName: false, advertisedAppend: false },
    { version: "1.5.1", filter: false, native: false, ownName: true, advertisedAppend: false },
    { version: "1.5.7", filter: false, native: false, ownName: true, advertisedAppend: true },
    { version: "1.3.0", filter: false, native: false, ownName: false, advertisedAppend: false },
    { version: undefined, filter: false, native: false, ownName: false, advertisedAppend: false },
    { version: "unknown", filter: false, native: false, ownName: false, advertisedAppend: false },
  ])(
    "$version: workflow filter $filter, qualified reads $native, advertised system append $advertisedAppend, own client name $ownName",
    ({ version, filter, native, ownName, advertisedAppend }) => {
      const initialize = {
        protocolVersion: 1,
        ...(advertisedAppend
          ? { agentCapabilities: { _meta: { antigravity: { systemPrompt: { append: true } } } } }
          : {}),
        ...(version === undefined ? {} : { agentInfo: { name: "antigravity-acp", version } }),
      };
      // Helpers send no append, so only the workflow filter can apply.
      expect(antigravitySessionMeta(initialize)).toEqual(
        filter ? { antigravity: { disabledTools: ["run_workflow"] } } : undefined,
      );
      const antigravity = {
        ...(filter ? { disabledTools: ["run_workflow"] } : {}),
        ...(native || advertisedAppend
          ? { systemPrompt: { append: "<pull_request_linking/>" } }
          : {}),
      };
      expect(antigravitySessionMeta(initialize, "<pull_request_linking/>")).toEqual(
        filter || native || advertisedAppend ? { antigravity } : undefined,
      );
      expect(isQualifiedAntigravityRelease(version)).toBe(native);
      expect(antigravityClientInfo(version).name).toBe(ownName ? "t3-code" : "zed");
    },
  );
});

describe("applyAntigravityAcpModelSelection", () => {
  it.effect("restores the saved model instead of the cold-resume default", () =>
    Effect.gen(function* () {
      const { runtime, selections } = makeModelRuntime();
      const model = yield* applyAntigravityAcpModelSelection({
        runtime,
        model: "gemini-saved",
        mapError: (cause) => cause,
      });

      expect(model).toBe("gemini-saved");
      expect(selections).toEqual(["gemini-saved"]);
    }),
  );

  it.effect.each([undefined, null, ANTIGRAVITY_DEFAULT_MODEL])(
    "uses the native default for %s without sending an internal model ID",
    (requestedModel) =>
      Effect.gen(function* () {
        const { runtime, selections } = makeModelRuntime();
        const model = yield* applyAntigravityAcpModelSelection({
          runtime,
          model: requestedModel,
          mapError: (cause) => cause,
        });

        expect(model).toBe("gemini-default");
        expect(selections).toEqual([]);
      }),
  );

  it.effect("selects the manifest default for the alias when the account offers it", () =>
    Effect.gen(function* () {
      const { runtime, selections } = makeModelRuntime();
      const model = yield* applyAntigravityAcpModelSelection({
        runtime,
        model: ANTIGRAVITY_DEFAULT_MODEL,
        defaultModel: "gemini-saved",
        mapError: (cause) => cause,
      });
      expect(model).toBe("gemini-saved");
      expect(selections).toEqual(["gemini-saved"]);

      const { runtime: other, selections: otherSelections } = makeModelRuntime();
      const fallback = yield* applyAntigravityAcpModelSelection({
        runtime: other,
        model: ANTIGRAVITY_DEFAULT_MODEL,
        defaultModel: "gemini-not-offered",
        mapError: (cause) => cause,
      });
      expect(fallback).toBe("gemini-default");
      expect(otherSelections).toEqual([]);
    }),
  );

  it.effect.each(["gemini-removed", "Gemini saved", "gemini-saved[reasoning=high]"])(
    "rejects unavailable or non-native model ID %s without selecting a fallback",
    (model) =>
      Effect.gen(function* () {
        const { runtime, selections } = makeModelRuntime();
        const error = yield* applyAntigravityAcpModelSelection({
          runtime,
          model,
          mapError: (cause) => cause,
        }).pipe(Effect.flip);

        expect(error).toMatchObject({
          _tag: "AcpRequestError",
          code: -32602,
          errorMessage: expect.stringContaining(`'${model}' is unavailable`),
        });
        expect(selections).toEqual([]);
      }),
  );

  it.effect("accepts exact model IDs from grouped native options", () =>
    Effect.gen(function* () {
      const { runtime, selections } = makeModelRuntime([
        {
          ...modelConfig,
          options: [{ groupId: "gemini", name: "Gemini", options: modelConfig.options }],
        },
      ]);
      const model = yield* applyAntigravityAcpModelSelection({
        runtime,
        model: "gemini-saved",
        mapError: (cause) => cause,
      });

      expect(model).toBe("gemini-saved");
      expect(selections).toEqual(["gemini-saved"]);
    }),
  );

  it.effect("passes a model ID saved on runtime 1.3 to the agent as its 1.4 family", () =>
    Effect.gen(function* () {
      // The cold-resume model has no thinking option, so the level is unknown locally.
      const { runtime, selections } = makeModelRuntime([
        {
          ...modelConfig,
          currentValue: "gpt-oss-120b-medium",
          options: [
            { value: "gemini-3.8-flash", name: "Gemini 3.8 Flash" },
            { value: "gpt-oss-120b-medium", name: "GPT-OSS 120B (Medium)" },
          ],
        },
      ]);
      const model = yield* applyAntigravityAcpModelSelection({
        runtime,
        model: "gemini-3.8-flash-low",
        mapError: (cause) => cause,
      });

      expect(model).toBe("gemini-3.8-flash");
      expect(selections).toEqual(["model=gemini-3.8-flash-low (unlisted)"]);
    }),
  );
});

const imageAttachment = {
  type: "image",
  id: "thread-00000000-0000-4000-8000-000000000001",
  name: "screen.png",
  mimeType: "image/png",
  sizeBytes: 1,
} satisfies ChatAttachment;

const textAttachment = {
  type: "file",
  id: "thread-00000000-0000-4000-8000-000000000002-tsx",
  name: "example.tsx",
  mimeType: "application/octet-stream",
  sizeBytes: 1,
} satisfies ChatAttachment;

const pdfAttachment = {
  type: "file",
  id: "thread-00000000-0000-4000-8000-000000000003-pdf",
  name: "report.pdf",
  mimeType: "application/pdf",
  sizeBytes: 1,
} satisfies ChatAttachment;

const makeAttachmentFixture = Effect.fn("AntigravityAcpSupportTest.makeAttachmentFixture")(
  function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const attachmentsDir = yield* fs.makeTempDirectoryScoped({
      prefix: "t3-antigravity-attachments-",
    });
    const write = Effect.fn("AntigravityAcpSupportTest.writeAttachment")(function* (
      attachment: ChatAttachment,
      content: string | Uint8Array,
    ) {
      const filePath = resolveAttachmentPath({ attachmentsDir, attachment });
      if (filePath === null) throw new Error("Invalid test attachment path.");
      if (typeof content === "string") yield* fs.writeFileString(filePath, content);
      else yield* fs.writeFile(filePath, content);
      return { filePath, uri: (yield* path.toFileUrl(filePath)).href };
    });
    const resolve = (attachment: ChatAttachment) =>
      resolveAttachmentPath({ attachmentsDir, attachment });
    return { fs, attachmentsDir, resolve, write };
  },
);

it.layer(NodeServices.layer)("buildAntigravityPrompt", (it) => {
  it.effect("sends image bytes as native image content alongside the user prompt", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const bytes = Buffer.from(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jY9kAAAAASUVORK5CYII=",
        "base64",
      );
      yield* fixture.write(imageAttachment, bytes);
      const prompt = yield* buildAntigravityPrompt({
        input: "  Explain this image.  ",
        attachments: [imageAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      });

      expect(prompt).toEqual([
        { type: "text", text: "Explain this image." },
        { type: "image", data: bytes.toString("base64"), mimeType: "image/png" },
      ]);
    }),
  );

  it.effect("embeds UTF-8 code from the selected environment and keeps the upload in place", () =>
    Effect.gen(function* () {
      const selectedEnvironment = yield* makeAttachmentFixture();
      const otherEnvironment = yield* makeAttachmentFixture();
      const source = '  const name = "caf\u00e9";\n';
      const upload = yield* selectedEnvironment.write(textAttachment, source);
      yield* otherEnvironment.write(textAttachment, "Different environment.");
      const prompt = yield* buildAntigravityPrompt({
        input: undefined,
        attachments: [textAttachment],
        resolveAttachmentPath: selectedEnvironment.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      });

      expect(prompt).toEqual([
        {
          type: "resource",
          resource: { uri: upload.uri, mimeType: "application/octet-stream", text: source },
        },
      ]);
      expect(yield* selectedEnvironment.fs.readFileString(upload.filePath)).toBe(source);
      expect(
        yield* selectedEnvironment.fs.readDirectory(selectedEnvironment.attachmentsDir),
      ).toHaveLength(1);
    }),
  );

  it.effect("keeps folded clipboard text out of native context", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const pastedText = {
        ...textAttachment,
        name: "pasted-text.txt",
        mimeType: "text/plain",
        source: { _tag: "pasted-text" as const },
      } satisfies ChatAttachment;
      yield* fixture.write(pastedText, "A very large crash report");

      const prompt = yield* buildAntigravityPrompt({
        input: "Inspect the pasted text only as needed.",
        attachments: [pastedText],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      });

      expect(prompt).toEqual([{ type: "text", text: "Inspect the pasted text only as needed." }]);
    }),
  );

  it.effect.each([
    { name: "archive.zip", mimeType: "application/zip" },
    { name: "clip.mp4", mimeType: "video/mp4" },
    { name: "recording.aiff", mimeType: "audio/aiff" },
    { name: "large.pdf", mimeType: "application/pdf", sizeBytes: 75_000_000 },
    { name: "large.txt", mimeType: "text/plain", sizeBytes: 1024 * 1024 + 1 },
    { name: "large.wav", mimeType: "audio/wav", sizeBytes: 20 * 1024 * 1024 + 1 },
  ])("keeps $name as a file path without reading or spending the native media budget", (file) =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const attachment = { ...textAttachment, sizeBytes: 50 * 1024 * 1024, ...file };
      const upload = yield* fixture.write(attachment, "");
      yield* fixture.fs.truncate(upload.filePath, attachment.sizeBytes);
      const pdf = yield* fixture.write(pdfAttachment, "");
      yield* fixture.fs.truncate(pdf.filePath, 50 * 1024 * 1024);
      const input = `Inspect the file at ${upload.filePath}`;
      const prompt = yield* buildAntigravityPrompt({
        input,
        attachments: [attachment, pdfAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fixture.fs,
          stream: () => {
            throw new Error("Path attachments must not be read into the prompt");
          },
        }),
      );
      expect(prompt).toEqual([
        { type: "text", text: input },
        {
          type: "resource_link",
          uri: pdf.uri,
          name: "report.pdf",
          mimeType: "application/pdf",
          size: 50 * 1024 * 1024,
        },
      ]);
    }),
  );

  it.effect.each([
    {
      case: "a Gemini model id",
      model: { id: "gemini-3.8-flash", name: "Gemini 3.8 Flash" },
      linked: true,
    },
    {
      case: "an opaque id named Gemini",
      model: { id: "agy-model", name: "Gemini 4 Carbon" },
      linked: true,
    },
    { case: "an unknown model", model: { id: "agy-custom", name: "Custom" }, linked: false },
    { case: "an unlisted opaque id", model: { id: "agy-custom", name: undefined }, linked: false },
    { case: "an unresolved model", model: undefined, linked: false },
    {
      case: "a Claude model",
      model: { id: "claude-opus-5-5", name: "Claude Opus 5.5" },
      linked: false,
    },
    {
      case: "an unqualified runtime",
      agentVersion: "1.4.1",
      model: { id: "gemini-3.8-flash", name: "Gemini 3.8 Flash" },
      linked: false,
    },
    {
      case: "a Windows host",
      platform: "win32" as const,
      model: { id: "gemini-3.8-flash", name: "Gemini 3.8 Flash" },
      linked: false,
    },
    {
      case: "a clip over 20 MiB",
      model: { id: "gemini-3.8-flash", name: "Gemini 3.8 Flash" },
      linked: false,
      sizeBytes: 20 * 1024 * 1024 + 1,
    },
  ])(
    "links an MP4 for $case only when supported",
    ({ agentVersion = "1.4.5", platform = "linux", model, linked, ...size }) =>
      Effect.gen(function* () {
        const fixture = yield* makeAttachmentFixture();
        const clip = {
          ...textAttachment,
          id: "clip",
          name: "clip.mp4",
          mimeType: "video/mp4",
          sizeBytes: 6 * 1024 * 1024,
          ...size,
        };
        const upload = yield* fixture.write(clip, "");
        yield* fixture.fs.truncate(upload.filePath, clip.sizeBytes);
        const prompt = yield* buildAntigravityPrompt({
          input: "What colors appear?",
          attachments: [clip],
          resolveAttachmentPath: fixture.resolve,
          promptCapabilities: { image: true, audio: true, embeddedContext: true },
          agentVersion,
          currentModel: model,
        }).pipe(
          Effect.provideService(HostProcess.Platform, platform),
          Effect.provideService(FileSystem.FileSystem, {
            ...fixture.fs,
            stream: () => Effect.die("Linked video is read by the agent").pipe(Stream.fromEffect),
          }),
        );
        expect(prompt).toEqual([
          { type: "text", text: "What colors appear?" },
          ...(linked
            ? [
                {
                  type: "resource_link",
                  uri: upload.uri,
                  name: "clip.mp4",
                  mimeType: "video/mp4",
                  size: clip.sizeBytes,
                },
              ]
            : []),
        ]);
      }),
  );

  it.effect.each([undefined, {}, { image: false, audio: false, embeddedContext: false }])(
    "keeps attachments path-only when native prompt capabilities are absent: %s",
    (promptCapabilities) =>
      Effect.gen(function* () {
        const fixture = yield* makeAttachmentFixture();
        const audio = {
          ...textAttachment,
          id: "unsupported-audio",
          name: "audio.wav",
          mimeType: "audio/wav",
        };
        const gifAttachment = {
          ...imageAttachment,
          id: "unsupported-gif",
          name: "animation.gif",
          mimeType: "image/gif",
        };
        const gif = yield* fixture.write(gifAttachment, "GIF89a");
        const image = yield* fixture.write(imageAttachment, "pixels");
        const pdf = yield* fixture.write(pdfAttachment, "%PDF-1.7");
        const text = yield* fixture.write(textAttachment, "hello");
        const sound = yield* fixture.write(audio, "RIFF");
        const input = `Read ${image.filePath}, ${gif.filePath}, ${pdf.filePath}, ${text.filePath}, and ${sound.filePath}`;
        const prompt = yield* buildAntigravityPrompt({
          input,
          attachments: [imageAttachment, gifAttachment, pdfAttachment, textAttachment, audio],
          resolveAttachmentPath: fixture.resolve,
          promptCapabilities,
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fixture.fs,
            stream: () =>
              Effect.die("Unnegotiated attachments must not be read").pipe(Stream.fromEffect),
          }),
        );
        expect(prompt).toEqual([{ type: "text", text: input }]);
        expect(
          yield* buildAntigravityPrompt({
            input: "Continue",
            attachments: [],
            resolveAttachmentPath: fixture.resolve,
            promptCapabilities,
          }),
        ).toEqual([{ type: "text", text: "Continue" }]);
      }),
  );

  it.effect("sends supported audio files as native audio content", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const audioAttachment = {
        ...textAttachment,
        id: "recording-1",
        name: "recording.wav",
        mimeType: "audio/wav",
      } satisfies ChatAttachment;
      const bytes = Buffer.from("RIFF....WAVEfmt ", "latin1");
      yield* fixture.write(audioAttachment, bytes);
      const prompt = yield* buildAntigravityPrompt({
        input: "Transcribe this.",
        attachments: [audioAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      });

      expect(prompt).toEqual([
        { type: "text", text: "Transcribe this." },
        { type: "audio", data: bytes.toString("base64"), mimeType: "audio/wav" },
      ]);
    }),
  );

  it.effect.each([{ image: true }])(
    "keeps unsupported image formats path-only with capabilities %s",
    (promptCapabilities) =>
      Effect.gen(function* () {
        const fixture = yield* makeAttachmentFixture();
        const attachment = { ...imageAttachment, name: "animation.gif", mimeType: "image/gif" };
        const upload = yield* fixture.write(attachment, "GIF89a");
        const input = `Analyze the image at ${upload.filePath}`;
        const prompt = yield* buildAntigravityPrompt({
          input,
          attachments: [attachment],
          resolveAttachmentPath: fixture.resolve,
          promptCapabilities,
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fixture.fs,
            stream: () => Effect.die("Unsupported images must not be read").pipe(Stream.fromEffect),
          }),
        );

        expect(prompt).toEqual([{ type: "text", text: input }]);
      }),
  );

  it.effect("rejects oversized images using file size instead of upload metadata", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const upload = yield* fixture.write(imageAttachment, "");
      yield* fixture.fs.truncate(upload.filePath, PROVIDER_SEND_TURN_MAX_IMAGE_BYTES + 1);
      const error = yield* buildAntigravityPrompt({
        input: "Read this attachment.",
        attachments: [imageAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      }).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "AcpRequestError",
        code: -32602,
        errorMessage: expect.stringContaining("'screen.png' is too large"),
      });
    }),
  );

  it.effect("keeps PDF overflow on the file-path route", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const secondAttachment = {
        ...pdfAttachment,
        id: `${pdfAttachment.id}-second`,
        name: "second.pdf",
      };
      const first = yield* fixture.write(pdfAttachment, "");
      const second = yield* fixture.write(secondAttachment, "");
      yield* fixture.fs.truncate(first.filePath, (50 * 1024 * 1024) / 2);
      yield* fixture.fs.truncate(second.filePath, (50 * 1024 * 1024) / 2);
      const input = {
        input: `Read ${first.filePath} and ${second.filePath}`,
        attachments: [pdfAttachment, secondAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      };
      const prompt = yield* buildAntigravityPrompt(input);
      expect(prompt).toEqual([
        { type: "text", text: input.input },
        {
          type: "resource_link",
          uri: first.uri,
          name: "report.pdf",
          mimeType: "application/pdf",
          size: (50 * 1024 * 1024) / 2,
        },
        {
          type: "resource_link",
          uri: second.uri,
          name: "second.pdf",
          mimeType: "application/pdf",
          size: (50 * 1024 * 1024) / 2,
        },
      ]);

      yield* fixture.fs.truncate(second.filePath, (50 * 1024 * 1024) / 2 + 1);
      expect(yield* buildAntigravityPrompt(input)).toEqual([
        { type: "text", text: input.input },
        {
          type: "resource_link",
          uri: first.uri,
          name: "report.pdf",
          mimeType: "application/pdf",
          size: (50 * 1024 * 1024) / 2,
        },
      ]);
    }),
  );

  it.effect("still rejects images when the native budget is full", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const pdf = yield* fixture.write(pdfAttachment, "");
      yield* fixture.fs.truncate(pdf.filePath, 50 * 1024 * 1024);
      yield* fixture.write(imageAttachment, new Uint8Array([1]));
      const error = yield* buildAntigravityPrompt({
        input: "Inspect both attachments.",
        attachments: [pdfAttachment, imageAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      }).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "AcpRequestError",
        errorMessage: expect.stringContaining("'screen.png' is too large"),
      });
    }),
  );

  it.effect("rejects aggregate overflow when a text upload grows after its size check", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const pdf = yield* fixture.write(pdfAttachment, "");
      const text = yield* fixture.write(textAttachment, "a");
      yield* fixture.fs.truncate(pdf.filePath, 50 * 1024 * 1024 - 1);
      const error = yield* buildAntigravityPrompt({
        input: undefined,
        attachments: [pdfAttachment, textAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fixture.fs,
          stat: Effect.fn("AntigravityAcpSupportTest.growAfterStat")(function* (filePath: string) {
            const info = yield* fixture.fs.stat(filePath);
            if (filePath === text.filePath) {
              yield* fixture.fs.writeFileString(filePath, "ab");
            }
            return info;
          }),
        }),
        Effect.flip,
      );

      expect(error).toMatchObject({
        _tag: "AcpRequestError",
        code: -32602,
        errorMessage: "Attachment 'example.tsx' changed while being read and is too large.",
      });
    }),
  );

  it.effect.each([
    { bytes: new Uint8Array([0xff, 0xfe, 0x61]), message: "is not a UTF-8 text file" },
    { bytes: new Uint8Array([0x61, 0, 0x62]), message: "contains binary data" },
  ])("rejects binary data disguised as code: $message", ({ bytes, message }) =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      yield* fixture.write(textAttachment, bytes);
      const error = yield* buildAntigravityPrompt({
        input: undefined,
        attachments: [textAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "AcpRequestError",
        code: -32602,
        errorMessage: expect.stringContaining(message),
      });
    }),
  );

  it.effect("reports a missing upload instead of sending only the remaining text", () =>
    Effect.gen(function* () {
      const fixture = yield* makeAttachmentFixture();
      const error = yield* buildAntigravityPrompt({
        input: "Read this image.",
        attachments: [imageAttachment],
        resolveAttachmentPath: fixture.resolve,
        promptCapabilities: { image: true, audio: true, embeddedContext: true },
      }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "AcpRequestError",
        code: -32602,
        errorMessage: "Could not read attachment 'screen.png'.",
      });
    }),
  );
});

const isProcessAlive = (pid: number, platform: NodeJS.Platform): boolean => {
  try {
    process.kill(pid, 0);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException | undefined)?.code === "ESRCH") {
      return false;
    }
    throw cause;
  }
  if (platform === "linux") {
    const identity = AcpSessionRuntime.makePosixProcessTreeController("linux").identity(pid);
    return identity !== undefined && !AcpSessionRuntime.posixProcessIsZombie(identity);
  }
  return true;
};

const forceKillPid = (pid: number | undefined) => {
  if (pid === undefined || !Number.isInteger(pid) || pid <= 0) return;
  try {
    process.kill(pid, "SIGKILL");
  } catch {}
};

describe("makeAntigravityAcpRuntime process teardown (CF-03)", () => {
  it.live(
    "terminates detached child processes spawned by the agent when runtime scope closes without a systemd scope",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const platform = yield* HostProcess.Platform;
        const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-antigravity-cf03-" });
        const mockAgentPath = path.join(tempDir, "mock-ag-agent.mjs");
        const detachedPidFile = path.join(tempDir, "detached.pid");
        const agentPidFile = path.join(tempDir, "agent.pid");

        yield* fs.writeFileString(
          mockAgentPath,
          [
            'import { spawn } from "node:child_process";',
            'import { writeFileSync } from "node:fs";',
            'import { createInterface } from "node:readline";',
            "const detachedPidFile = process.env.T3_TEST_DETACHED_PID_FILE;",
            "const agentPidFile = process.env.T3_TEST_AGENT_PID_FILE;",
            'if (agentPidFile) writeFileSync(agentPidFile, String(process.pid), "utf8");',
            "const keepAlive = setInterval(() => {}, 60_000);",
            "const reply = (id, result) => {",
            '  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id, result })}\\n`);',
            "};",
            "const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });",
            'rl.on("line", (line) => {',
            "  if (!line.trim()) return;",
            "  const msg = JSON.parse(line);",
            '  if (msg.method === "initialize") {',
            "    reply(msg.id, {",
            "      protocolVersion: 1,",
            "      agentCapabilities: {},",
            '      authMethods: [{ id: "oauth-personal", name: "OAuth" }],',
            "    });",
            '  } else if (msg.method === "authenticate") {',
            "    reply(msg.id, {});",
            '  } else if (msg.method === "session/new") {',
            '    reply(msg.id, { sessionId: "mock-ag-session-1" });',
            '  } else if (msg.method === "session/prompt") {',
            '    const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], {',
            '      detached: process.env.T3_TEST_DETACHED_CHILD === "1",',
            '      stdio: "ignore",',
            "    });",
            "    child.unref();",
            "    if (detachedPidFile && child.pid) {",
            '      writeFileSync(detachedPidFile, String(child.pid), "utf8");',
            "    }",
            "    process.stdout.write(",
            "      `${JSON.stringify({",
            '        jsonrpc: "2.0",',
            '        method: "session/update",',
            "        params: {",
            '          sessionId: "mock-ag-session-1",',
            "          update: {",
            '            sessionUpdate: "tool_call",',
            '            toolCallId: "call_detached_1",',
            '            title: "Running run_command",',
            '            kind: "execute",',
            '            status: "in_progress",',
            '            rawInput: { CommandLine: "sleep 30", WaitMsBeforeAsync: 5000 },',
            "          },",
            "        },",
            "      })}\\n`,",
            "    );",
            '    reply(msg.id, { stopReason: "end_turn" });',
            "  } else if (msg.id !== undefined) {",
            "    reply(msg.id, {});",
            "  }",
            "});",
            'rl.on("close", () => clearInterval(keepAlive));',
          ].join("\n"),
        );

        let detachedPid: number | undefined;
        let agentPid: number | undefined;
        const runtimeScope = yield* Scope.make();

        yield* Effect.gen(function* () {
          const runtime = yield* makeAntigravityAcpRuntime({
            childProcessSpawner,
            cwd: tempDir,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: tempDir,
              env: {
                T3_TEST_DETACHED_PID_FILE: detachedPidFile,
                T3_TEST_AGENT_PID_FILE: agentPidFile,
                T3_TEST_DETACHED_CHILD: platform === "linux" ? "1" : "0",
              },
            },
          }).pipe(
            Effect.provideService(Scope.Scope, runtimeScope),
            Effect.provideService(AgentScope, AgentScope.defaultValue()),
          );

          yield* runtime.start();
          yield* runtime.prompt({
            prompt: [{ type: "text", text: "spawn detached child" }],
          });

          detachedPid = Number((yield* fs.readFileString(detachedPidFile)).trim());
          agentPid = Number((yield* fs.readFileString(agentPidFile)).trim());
          expect(Number.isInteger(detachedPid) && detachedPid > 0).toBe(true);
          expect(isProcessAlive(detachedPid, platform)).toBe(true);

          yield* Scope.close(runtimeScope, Exit.void);
          expect(isProcessAlive(detachedPid, platform)).toBe(false);
        }).pipe(
          Effect.ensuring(
            Scope.close(runtimeScope, Exit.void).pipe(
              Effect.ignore,
              Effect.andThen(
                Effect.sync(() => {
                  forceKillPid(detachedPid);
                  forceKillPid(agentPid);
                }),
              ),
            ),
          ),
        );
      }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});
