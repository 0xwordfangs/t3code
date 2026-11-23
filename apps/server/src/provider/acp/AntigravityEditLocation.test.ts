import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import {
  ANTIGRAVITY_EDIT_DIFF_OMITTED,
  antigravityEditPatch,
  makeScopedAntigravityPreEditReader,
} from "./AntigravityEditLocation.ts";

const path = "/workspace/app.ts";
const file = Array.from({ length: 20 }, (_, i) =>
  i === 9
    ? "const target = 10;"
    : i === 14
      ? "const other = 15;"
      : `const line${i + 1} = ${i + 1};`,
).join("\n");
const replace = { TargetContent: "const target = 10;", ReplacementContent: "const target = 100;" };
const header = `--- ${path}\n+++ ${path}\n`;

describe("antigravityEditPatch", () => {
  it("finds the target inside the announced search range, in either argument casing", () => {
    const expected = `${header}@@ -10,1 +10,1 @@\n-const target = 10;\n+const target = 100;\n`;
    const readPreEditFile = () => file;
    for (const rawInput of [
      { ...replace, StartLine: 5, EndLine: 20 },
      {
        target_content: replace.TargetContent,
        replacement_content: replace.ReplacementContent,
        start_line: 5,
        end_line: 20,
      },
      {
        replacement_chunks: [
          {
            target_content: replace.TargetContent,
            replacement_content: replace.ReplacementContent,
            start_line: 5,
            end_line: 20,
          },
        ],
      },
      // Like the runtime, a present PascalCase value wins, and a 0 line means unspecified.
      { ...replace, target_content: "const other = 15;", StartLine: 5, EndLine: 0 },
    ]) {
      expect(antigravityEditPatch({ path, rawInput, readPreEditFile })).toBe(expected);
    }
    for (const rawInput of [replace, { ReplacementChunks: [replace] }]) {
      expect(
        antigravityEditPatch({ path, rawInput, locations: [{ path, line: 5 }], readPreEditFile }),
      ).toBe(expected);
    }
    // A hunk replaces whole lines, so a match inside a line is not a placement.
    expect(
      antigravityEditPatch({
        path,
        rawInput: { TargetContent: "10", ReplacementContent: "100" },
        readPreEditFile,
      }),
    ).toMatch(/ snippet, line unknown$/m);
    // The range start is not the target's line: without the file, or outside the range, it stays unplaced.
    const snippet = `${header}@@ -1,1 +1,1 @@ snippet, line unknown\n-const target = 10;\n+const target = 100;\n`;
    expect(
      antigravityEditPatch({ path, rawInput: { ...replace, StartLine: 5, EndLine: 20 } }),
    ).toBe(snippet);
    expect(
      antigravityEditPatch({
        path,
        rawInput: { ...replace, StartLine: 11, EndLine: 20 },
        readPreEditFile,
      }),
    ).toBe(snippet);
  });

  it("finds an unannounced snippet in the pre-edit file", () => {
    const expected = `${header}@@ -10,1 +10,1 @@\n-const target = 10;\n+const target = 100;\n`;
    expect(antigravityEditPatch({ path, rawInput: replace, readPreEditFile: () => file })).toBe(
      expected,
    );
    const crlf = file.replaceAll("\n", "\r\n");
    expect(antigravityEditPatch({ path, rawInput: replace, readPreEditFile: () => crlf })).toBe(
      expected,
    );
    // A target written with CRLF previews the same lines, without carriage returns.
    expect(
      antigravityEditPatch({
        path,
        rawInput: {
          TargetContent: "const target = 10;\r\n",
          ReplacementContent: "const target = 100;\r\n",
        },
        readPreEditFile: () => crlf,
      }),
    ).toBe(expected);
    // A change to the line ending alone has no LF hunk to place.
    const endingOnly = {
      TargetContent: "const target = 10;\r\n",
      ReplacementContent: "const target = 10;\n",
    };
    for (const readPreEditFile of [() => crlf, undefined]) {
      expect(antigravityEditPatch({ path, rawInput: endingOnly, readPreEditFile })).toBe(
        ANTIGRAVITY_EDIT_DIFF_OMITTED,
      );
    }
    // A multi-line target written with LF still finds its lines in a CRLF file.
    expect(
      antigravityEditPatch({
        path,
        rawInput: {
          TargetContent: "const line9 = 9;\nconst target = 10;",
          ReplacementContent: "const line9 = 9;\nconst target = 100;",
        },
        readPreEditFile: () => crlf,
      }),
    ).toMatch(/^@@ -9,2 \+9,2 @@$/m);
  });

  it("omits the preview when a chunk adds or removes a line break at its end", () => {
    // Whether that splits or joins lines depends on what follows the match.
    for (const [TargetContent, ReplacementContent] of [
      ["const line3 = 3;", "x\n"],
      ["const line3 = 3;\n", "x"],
      ["const line20 = 20;", "x\n"],
      // Clearing the last line of a file with no final newline removes the line.
      ["const line20 = 20;", ""],
    ]) {
      expect(
        antigravityEditPatch({
          path,
          rawInput: { TargetContent, ReplacementContent },
          readPreEditFile: () => file,
        }),
      ).toBe(ANTIGRAVITY_EDIT_DIFF_OMITTED);
    }
  });

  it("labels repeated or unmatched snippets instead of placing them at line 1", () => {
    const snippet = `${header}@@ -1,1 +1,1 @@ snippet, line unknown\n-const target = 10;\n+const target = 100;\n`;
    for (const readPreEditFile of [
      () => `${file}\n${file}`,
      () => "unrelated\n",
      () => undefined,
      undefined,
    ]) {
      expect(antigravityEditPatch({ path, rawInput: replace, readPreEditFile })).toBe(snippet);
    }
  });

  it("places each replacement chunk and shifts later chunks by earlier line changes", () => {
    const patch = antigravityEditPatch({
      path,
      rawInput: {
        ReplacementChunks: [
          {
            TargetContent: "const other = 15;\n",
            ReplacementContent: "const other = 150;\n",
            StartLine: 15,
            EndLine: 15,
          },
          {
            TargetContent: "const target = 10;\n",
            ReplacementContent: "const target = 100;\nconst added = 1;\n",
            StartLine: 10,
            EndLine: 10,
          },
        ],
      },
      readPreEditFile: () => file,
    });
    expect(patch).toBe(
      [
        header.trimEnd(),
        "@@ -10,1 +10,2 @@",
        "-const target = 10;",
        "+const target = 100;",
        "+const added = 1;",
        "@@ -15,1 +16,1 @@",
        "-const other = 15;",
        "+const other = 150;",
        "",
      ].join("\n"),
    );
  });

  it("omits the preview when only some chunks can be placed", () => {
    const patch = antigravityEditPatch({
      path,
      rawInput: {
        ReplacementChunks: [
          {
            TargetContent: "const target = 10;\n",
            ReplacementContent: "const target = 100;\n",
            StartLine: 10,
            EndLine: 10,
          },
          { TargetContent: "missing", ReplacementContent: "still missing" },
        ],
      },
      readPreEditFile: () => file,
    });
    expect(patch).toBe(ANTIGRAVITY_EDIT_DIFF_OMITTED);
  });

  it("leaves whole-file writes and unknown inputs to the generic diff", () => {
    expect(antigravityEditPatch({ path, rawInput: { CodeContent: "x" } })).toBeUndefined();
    expect(antigravityEditPatch({ path, rawInput: undefined })).toBeUndefined();
  });

  it("omits the preview once an edit exceeds the shared diff budget", () => {
    const lines = (prefix: string, count: number) =>
      Array.from({ length: count }, (_, index) => `${prefix} ${index} ${"x".repeat(8)}`).join("\n");
    // A 7,000-line wholesale replacement, as one chunk.
    expect(
      antigravityEditPatch({
        path,
        rawInput: {
          TargetContent: lines("old", 7_000),
          ReplacementContent: lines("new", 7_000),
          StartLine: 1,
        },
      }),
    ).toBe(ANTIGRAVITY_EDIT_DIFF_OMITTED);
    // Chunks that each fit, but not together.
    const chunk = (start: number) => ({
      TargetContent: lines(`old-${start}`, 300),
      ReplacementContent: lines(`new-${start}`, 300),
      StartLine: start,
    });
    expect(
      antigravityEditPatch({
        path,
        rawInput: { ReplacementChunks: [chunk(1), chunk(1_000)] },
      }),
    ).toBe(ANTIGRAVITY_EDIT_DIFF_OMITTED);
    // Text over the size limit is not diffed at all.
    expect(
      antigravityEditPatch({
        path,
        rawInput: {
          TargetContent: "a".repeat(200_000),
          ReplacementContent: "b".repeat(100_000),
          StartLine: 1,
        },
      }),
    ).toBe(ANTIGRAVITY_EDIT_DIFF_OMITTED);
    expect(antigravityEditPatch({ path, rawInput: { ReplacementChunks: [chunk(1)] } })).toMatch(
      /^@@ -1,300 \+1,300 @@/m,
    );
  });
});

describe("makeScopedAntigravityPreEditReader", () => {
  it.effect("reads only regular UTF-8 text files inside canonical workspace roots", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const pathService = yield* Path.Path;
      const tempRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pre-edit-scope-" });
      const workspace = pathService.join(tempRoot, "workspace");
      const outside = pathService.join(tempRoot, "outside");
      yield* fs.makeDirectory(workspace, { recursive: true });
      yield* fs.makeDirectory(outside, { recursive: true });

      const insideFile = pathService.join(workspace, "app.ts");
      yield* fs.writeFileString(insideFile, "line 1\nconst target = 10;\nline 3\n");
      const outsideFile = pathService.join(outside, "secret.env");
      yield* fs.writeFileString(outsideFile, "SECRET=alpha\n");
      const escapingSymlink = pathService.join(workspace, "escape.env");
      yield* fs.symlink(outsideFile, escapingSymlink);
      const insideSymlink = pathService.join(workspace, "inside-link.ts");
      yield* fs.symlink(insideFile, insideSymlink);
      const binaryFile = pathService.join(workspace, "binary.bin");
      yield* fs.writeFile(binaryFile, new Uint8Array([0x61, 0x00, 0x62]));
      const invalidUtf8File = pathService.join(workspace, "invalid-utf8.bin");
      yield* fs.writeFile(invalidUtf8File, new Uint8Array([0xff, 0xfe, 0xfd]));
      const emptyFile = pathService.join(workspace, "empty.txt");
      yield* fs.writeFileString(emptyFile, "");
      const oversizedFile = pathService.join(workspace, "oversized.txt");
      yield* fs.writeFileString(oversizedFile, "a".repeat(1024 * 1024 + 1));
      const subDir = pathService.join(workspace, "subdir");
      yield* fs.makeDirectory(subDir, { recursive: true });

      const reader = makeScopedAntigravityPreEditReader([workspace]);

      // In-workspace regular file succeeds and places hunk accurately.
      expect(reader(insideFile)).toBe("line 1\nconst target = 10;\nline 3\n");
      expect(reader(insideSymlink)).toBe("line 1\nconst target = 10;\nline 3\n");
      expect(
        antigravityEditPatch({
          path: insideFile,
          rawInput: {
            TargetContent: "const target = 10;",
            ReplacementContent: "const target = 20;",
          },
          readPreEditFile: reader,
        }),
      ).toBe(
        `--- ${insideFile}\n+++ ${insideFile}\n@@ -2,1 +2,1 @@\n-const target = 10;\n+const target = 20;\n`,
      );

      // Out-of-workspace files, traversal, escaping symlinks, relative paths, directories,
      // UNC paths, empty, oversized, binary, FIFO, and /proc fail closed.
      const blockedPaths = [
        "",
        "app.ts",
        workspace,
        subDir,
        outsideFile,
        pathService.join(workspace, "..", "outside", "secret.env"),
        escapingSymlink,
        "\\\\server\\share\\file.txt",
        "//server/share/file.txt",
        binaryFile,
        invalidUtf8File,
        emptyFile,
        oversizedFile,
      ];
      const fifoPath = pathService.join(workspace, "blocking.fifo");
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const mkfifoExit = yield* spawner
        .exitCode(ChildProcess.make("mkfifo", [fifoPath]))
        .pipe(Effect.orElseSucceed(() => -1));
      if (mkfifoExit === 0) {
        blockedPaths.push(fifoPath);
      }
      const hasProcStatus = yield* fs.exists("/proc/self/status");
      if (hasProcStatus) {
        blockedPaths.push("/proc/self/status", "/proc/self/environ");
      }
      for (const blocked of blockedPaths) {
        expect(reader(blocked)).toBeUndefined();
      }
      expect(
        antigravityEditPatch({
          path: outsideFile,
          rawInput: { TargetContent: "SECRET=alpha", ReplacementContent: "SECRET=beta" },
          readPreEditFile: reader,
        }),
      ).toMatch(/ snippet, line unknown$/m);

      // Even if /proc were passed as an allowed root, 0-byte virtual files are rejected via fstatSync.
      if (hasProcStatus) {
        const procReader = makeScopedAntigravityPreEditReader(["/proc/self"]);
        expect(procReader("/proc/self/status")).toBeUndefined();
        expect(procReader("/proc/self/environ")).toBeUndefined();
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
