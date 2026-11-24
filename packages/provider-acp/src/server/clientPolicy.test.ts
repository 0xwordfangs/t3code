// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as NodePath from "node:path";

import {
  acpClientExecuteDisposition,
  acpClientReadDisposition,
  acpClientWriteDisposition,
  acpMcpToolApprovalElicitationDisposition,
  acpPermissionDisposition,
  makeAcpClientPolicyGrants,
  type AcpRuntimePolicy,
} from "./clientPolicy.ts";

function permissionRequest(
  kind: NonNullable<EffectAcpSchema.RequestPermissionRequest["toolCall"]["kind"]>,
  locations?: ReadonlyArray<EffectAcpSchema.ToolCallLocation>,
): EffectAcpSchema.RequestPermissionRequest {
  return {
    options: [],
    sessionId: "permission-session",
    toolCall: {
      kind,
      ...(locations === undefined ? {} : { locations }),
      toolCallId: "permission-tool-call",
    },
  };
}

describe("acpPermissionDisposition", () => {
  const cwd = NodePath.resolve(process.cwd(), "acp-permission-workspace");
  const writableRoot = NodePath.resolve(process.cwd(), "acp-additional-writable-root");
  const policy: AcpRuntimePolicy = {
    runtimeMode: "full-access",
    cwd,
    approvalPolicy: "never",
    sandboxPolicy: {
      type: "workspaceWrite",
      writableRoots: [writableRoot],
      networkAccess: false,
    },
  };

  it("auto-allows mutations only when every location is in cwd or an additional writable root", () => {
    assert.equal(
      acpPermissionDisposition(policy, permissionRequest("edit", [{ path: "src/index.ts" }])),
      "allow",
    );
    assert.equal(
      acpPermissionDisposition(
        policy,
        permissionRequest("delete", [{ path: NodePath.join(writableRoot, "generated.ts") }]),
      ),
      "allow",
    );
    assert.equal(
      acpPermissionDisposition(
        policy,
        permissionRequest("move", [
          { path: NodePath.join(cwd, "from.ts") },
          { path: NodePath.join(writableRoot, "to.ts") },
        ]),
      ),
      "allow",
    );
  });

  it("denies missing or out-of-root mutation locations", () => {
    const outside = NodePath.resolve(process.cwd(), "outside-acp-permission-workspace", "file.ts");
    assert.equal(acpPermissionDisposition(policy, permissionRequest("edit")), "deny");
    assert.equal(
      acpPermissionDisposition(policy, permissionRequest("delete", [{ path: "../escape.ts" }])),
      "deny",
    );
    assert.equal(
      acpPermissionDisposition(
        policy,
        permissionRequest("move", [{ path: NodePath.join(cwd, "inside.ts") }, { path: outside }]),
      ),
      "deny",
    );
  });

  it("keeps non-mutating workspace permissions and denials unchanged", () => {
    assert.equal(acpPermissionDisposition(policy, permissionRequest("read")), "allow");
    assert.equal(acpPermissionDisposition(policy, permissionRequest("execute")), "deny");
  });

  it("auto-accept-edits approves file changes without locations and asks for the rest", () => {
    const autoAcceptEdits: AcpRuntimePolicy = { runtimeMode: "auto-accept-edits", cwd };
    for (const kind of ["edit", "delete", "move"] as const) {
      assert.equal(acpPermissionDisposition(autoAcceptEdits, permissionRequest(kind)), "allow");
    }
    for (const kind of ["execute", "fetch", "other"] as const) {
      assert.equal(acpPermissionDisposition(autoAcceptEdits, permissionRequest(kind)), "ask");
    }
    assert.equal(acpPermissionDisposition(autoAcceptEdits, permissionRequest("read")), "allow");
    assert.equal(
      acpPermissionDisposition(
        { ...autoAcceptEdits, approvalPolicy: "on-request" },
        permissionRequest("edit"),
      ),
      "ask",
    );
  });

  it("auto-allows read-kind permission requests under on-request approval", () => {
    for (const runtimePolicy of [
      { ...policy, approvalPolicy: "on-request" },
      { ...policy, approvalPolicy: "on-request", sandboxPolicy: { type: "readOnly" } },
      { runtimeMode: "approval-required", cwd },
    ] satisfies ReadonlyArray<AcpRuntimePolicy>) {
      for (const kind of ["read", "search", "think"] as const) {
        assert.equal(acpPermissionDisposition(runtimePolicy, permissionRequest(kind)), "allow");
      }
      for (const kind of ["edit", "delete", "move", "execute", "fetch", "other"] as const) {
        assert.equal(acpPermissionDisposition(runtimePolicy, permissionRequest(kind)), "ask");
      }
    }
  });

  it.effect("denies mutations through workspace symlinks that escape the writable roots", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-acp-permission-workspace-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-acp-permission-outside-",
      });
      const outsideFile = path.join(outside, "existing.ts");
      yield* fileSystem.writeFileString(outsideFile, "outside");
      yield* fileSystem.symlink(outsideFile, path.join(workspace, "linked-file.ts"));
      yield* fileSystem.symlink(outside, path.join(workspace, "linked-directory"));

      const realPolicy: AcpRuntimePolicy = {
        runtimeMode: "full-access",
        cwd: workspace,
        approvalPolicy: "never",
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: [],
          networkAccess: false,
        },
      };

      assert.equal(
        acpPermissionDisposition(
          realPolicy,
          permissionRequest("edit", [{ path: "linked-file.ts" }]),
        ),
        "deny",
      );
      assert.equal(
        acpPermissionDisposition(
          realPolicy,
          permissionRequest("edit", [{ path: "linked-directory/new-file.ts" }]),
        ),
        "deny",
        "a missing leaf below an escaping directory symlink must not be auto-approved",
      );
      assert.equal(
        acpPermissionDisposition(
          realPolicy,
          permissionRequest("edit", [{ path: "linked-directory/../escaped-file.ts" }]),
        ),
        "deny",
        "physical symlink traversal must be resolved before parent segments",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("allows existing and new files beneath canonical writable roots", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-acp-permission-workspace-",
      });
      const workspaceLinkParent = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-acp-permission-link-parent-",
      });
      const workspaceLink = path.join(workspaceLinkParent, "workspace-link");
      yield* fileSystem.makeDirectory(path.join(workspace, "src"), { recursive: true });
      yield* fileSystem.writeFileString(path.join(workspace, "src", "existing.ts"), "existing");
      yield* fileSystem.symlink(workspace, workspaceLink);

      const realPolicy: AcpRuntimePolicy = {
        runtimeMode: "full-access",
        cwd: workspaceLink,
        approvalPolicy: "never",
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: [],
          networkAccess: false,
        },
      };

      assert.equal(
        acpPermissionDisposition(
          realPolicy,
          permissionRequest("edit", [{ path: "src/existing.ts" }]),
        ),
        "allow",
      );
      assert.equal(
        acpPermissionDisposition(
          realPolicy,
          permissionRequest("edit", [{ path: "src/generated/new-file.ts" }]),
        ),
        "allow",
        "non-existent descendants of a real in-root ancestor remain writable",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "denies dangling external symlinks with or without trailing separators while keeping legitimate paths writable (R3-CF-06)",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspace = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-dangling-workspace-",
        });
        const outside = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-dangling-outside-",
        });
        yield* fileSystem.makeDirectory(path.join(workspace, "sub"), { recursive: true });
        yield* fileSystem.writeFileString(path.join(workspace, "existing.ts"), "existing");
        yield* fileSystem.symlink(
          path.join(outside, "missing-target"),
          path.join(workspace, "escape"),
        );
        yield* fileSystem.symlink(
          path.join(outside, "missing-sub-target"),
          path.join(workspace, "sub", "escape-sub"),
        );

        const policies: ReadonlyArray<AcpRuntimePolicy> = [
          {
            runtimeMode: "full-access",
            cwd: workspace,
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [],
              networkAccess: false,
            },
          },
          {
            runtimeMode: "auto-accept-edits",
            cwd: workspace,
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [],
              networkAccess: false,
            },
          },
        ];

        const escapingPaths = [
          "escape",
          "escape/",
          "escape///",
          "escape/.",
          "escape/..",
          "escape//nested.txt",
          "sub/escape-sub/",
          "sub//escape-sub/",
          `${path.join(workspace, "escape")}/`,
        ] as const;

        for (const realPolicy of policies) {
          for (const candidate of escapingPaths) {
            assert.equal(
              acpClientWriteDisposition(realPolicy, candidate),
              "deny",
              `expected client write deny for ${candidate}`,
            );
            for (const kind of ["edit", "delete", "move"] as const) {
              assert.equal(
                acpPermissionDisposition(
                  realPolicy,
                  permissionRequest(kind, [{ path: candidate }]),
                ),
                "deny",
                `expected permission ${kind} deny for ${candidate}`,
              );
            }
          }

          assert.equal(acpClientWriteDisposition(realPolicy, "new-dir/"), "allow");
          assert.equal(acpClientWriteDisposition(realPolicy, "sub/new-file.txt"), "allow");
          assert.equal(acpClientWriteDisposition(realPolicy, "existing.ts"), "allow");
          assert.equal(
            acpPermissionDisposition(realPolicy, permissionRequest("edit", [{ path: "new-dir/" }])),
            "allow",
          );
          assert.equal(acpClientWriteDisposition(realPolicy, "/"), "deny");
          assert.equal(acpClientWriteDisposition(realPolicy, "///"), "deny");
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "denies missing-ancestor parent traversal that could collapse onto unprobed symlinks while keeping legitimate paths writable (R4-CF-03)",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspace = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-r4-cf-03-workspace-",
        });
        const outside = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-r4-cf-03-outside-",
        });
        const outsideFile = path.join(outside, "secret.ts");
        yield* fileSystem.writeFileString(outsideFile, "outside");
        yield* fileSystem.writeFileString(path.join(workspace, "existing.ts"), "existing");
        yield* fileSystem.makeDirectory(path.join(workspace, "existing-sub"), { recursive: true });
        yield* fileSystem.symlink(outsideFile, path.join(workspace, "linked-file.ts"));
        yield* fileSystem.symlink(outside, path.join(workspace, "linked-dir"));
        yield* fileSystem.symlink(
          path.join(outside, "missing-target"),
          path.join(workspace, "dangling-link"),
        );
        yield* fileSystem.symlink(outsideFile, path.join(workspace, "existing-sub", "sub-link.ts"));

        const policies: ReadonlyArray<AcpRuntimePolicy> = [
          {
            runtimeMode: "full-access",
            cwd: workspace,
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [],
              networkAccess: false,
            },
          },
          {
            runtimeMode: "auto-accept-edits",
            cwd: workspace,
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [],
              networkAccess: false,
            },
          },
        ];

        const escapingPaths = [
          "missing/../linked-file.ts",
          "missing/../linked-dir/new.txt",
          "missing/../dangling-link",
          "missing/../dangling-link/",
          "a/b/../../linked-dir/new2.txt",
          "existing-sub/missing/../sub-link.ts",
          "missing/..",
          "missing/../",
          "missing/./../linked-file.ts",
          `${workspace}/missing/../linked-file.ts`,
          `/nonexistent-top-xyz/..${workspace}/linked-file.ts`,
        ] as const;

        const legitimatePaths = [
          "existing.ts",
          "src/new-file.ts",
          "existing-sub/../new-file.ts",
          "existing-sub/../existing.ts",
          "new-dir/",
        ] as const;

        for (const realPolicy of policies) {
          for (const candidate of escapingPaths) {
            assert.equal(
              acpClientWriteDisposition(realPolicy, candidate),
              "deny",
              `expected client write deny for ${candidate}`,
            );
            for (const kind of ["edit", "delete", "move"] as const) {
              assert.equal(
                acpPermissionDisposition(
                  realPolicy,
                  permissionRequest(kind, [{ path: candidate }]),
                ),
                "deny",
                `expected permission ${kind} deny for ${candidate}`,
              );
            }
          }

          for (const candidate of legitimatePaths) {
            assert.equal(
              acpClientWriteDisposition(realPolicy, candidate),
              "allow",
              `expected client write allow for ${candidate}`,
            );
            for (const kind of ["edit", "delete", "move"] as const) {
              assert.equal(
                acpPermissionDisposition(
                  realPolicy,
                  permissionRequest(kind, [{ path: candidate }]),
                ),
                "allow",
                `expected permission ${kind} allow for ${candidate}`,
              );
            }
          }
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "denies whitespace-padded paths and escaping symlinks with edge whitespace while preserving trusted roots with trailing whitespace (R4-CF-04)",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspace = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-r4-cf-04-workspace-",
        });
        const outside = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-r4-cf-04-outside-",
        });
        const outsideFile = path.join(outside, "secret.ts");
        yield* fileSystem.writeFileString(outsideFile, "outside");

        const symlinkNames = [
          "ws-trail ",
          "ws-dangling ",
          " ws-lead",
          "ws-dir ",
          "ws-tab\t",
          "ws-nl\n",
        ] as const;

        yield* fileSystem.symlink(outsideFile, path.join(workspace, "ws-trail "));
        yield* fileSystem.symlink(
          path.join(outside, "missing-target"),
          path.join(workspace, "ws-dangling "),
        );
        yield* fileSystem.symlink(outsideFile, path.join(workspace, " ws-lead"));
        yield* fileSystem.symlink(outside, path.join(workspace, "ws-dir "));
        yield* fileSystem.symlink(outsideFile, path.join(workspace, "ws-tab\t"));
        yield* fileSystem.symlink(outsideFile, path.join(workspace, "ws-nl\n"));

        const policies: ReadonlyArray<AcpRuntimePolicy> = [
          {
            runtimeMode: "full-access",
            cwd: workspace,
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [],
              networkAccess: false,
            },
          },
          {
            runtimeMode: "auto-accept-edits",
            cwd: workspace,
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [],
              networkAccess: false,
            },
          },
        ];

        const deniedPaths = [
          ...symlinkNames,
          ...symlinkNames.map((name) => `${workspace}/${name}`),
          "ws-dangling /",
          "ws-dir /new.txt",
          `${workspace}/ws-dir /new.txt`,
          " /etc/passwd",
          "/etc/passwd ",
          " ../outside.txt",
          "../outside.txt ",
          " src/file.ts",
          "src/file.ts ",
        ];

        for (const realPolicy of policies) {
          for (const candidate of deniedPaths) {
            assert.equal(
              acpClientWriteDisposition(realPolicy, candidate),
              "deny",
              `expected client write deny for ${JSON.stringify(candidate)}`,
            );
            for (const kind of ["edit", "delete", "move"] as const) {
              assert.equal(
                acpPermissionDisposition(
                  realPolicy,
                  permissionRequest(kind, [{ path: candidate }]),
                ),
                "deny",
                `expected permission ${kind} deny for ${JSON.stringify(candidate)}`,
              );
            }
          }
        }

        const rootParent = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-acp-r4-cf-04-roots-",
        });
        const workspaceWithTrailingSpace = path.join(rootParent, "workspace ");
        const writableRootWithTrailingSpace = path.join(rootParent, "extra-root ");
        yield* fileSystem.makeDirectory(workspaceWithTrailingSpace, { recursive: true });
        yield* fileSystem.makeDirectory(writableRootWithTrailingSpace, { recursive: true });

        const trailingSpaceRootPolicies: ReadonlyArray<AcpRuntimePolicy> = [
          {
            runtimeMode: "full-access",
            cwd: workspaceWithTrailingSpace,
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [writableRootWithTrailingSpace],
              networkAccess: false,
            },
          },
          {
            runtimeMode: "auto-accept-edits",
            cwd: workspaceWithTrailingSpace,
            sandboxPolicy: {
              type: "workspaceWrite",
              writableRoots: [writableRootWithTrailingSpace],
              networkAccess: false,
            },
          },
        ];

        for (const rootPolicy of trailingSpaceRootPolicies) {
          for (const allowedPath of [
            "src/file.ts",
            `${workspaceWithTrailingSpace}/src/file.ts`,
            `${writableRootWithTrailingSpace}/generated.ts`,
          ]) {
            assert.equal(
              acpClientWriteDisposition(rootPolicy, allowedPath),
              "allow",
              `expected client write allow for ${JSON.stringify(allowedPath)}`,
            );
            for (const kind of ["edit", "delete", "move"] as const) {
              assert.equal(
                acpPermissionDisposition(
                  rootPolicy,
                  permissionRequest(kind, [{ path: allowedPath }]),
                ),
                "allow",
                `expected permission ${kind} allow for ${JSON.stringify(allowedPath)}`,
              );
            }
          }

          for (const paddedPath of [
            " src/file.ts",
            "src/file.ts ",
            "src/file.ts\n",
            workspaceWithTrailingSpace,
            writableRootWithTrailingSpace,
          ]) {
            assert.equal(
              acpClientWriteDisposition(rootPolicy, paddedPath),
              "deny",
              `expected client write deny for ${JSON.stringify(paddedPath)}`,
            );
            for (const kind of ["edit", "delete", "move"] as const) {
              assert.equal(
                acpPermissionDisposition(
                  rootPolicy,
                  permissionRequest(kind, [{ path: paddedPath }]),
                ),
                "deny",
                `expected permission ${kind} deny for ${JSON.stringify(paddedPath)}`,
              );
            }
          }
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("acpMcpToolApprovalElicitationDisposition", () => {
  it("applies runtime policy only to explicitly tagged MCP approval elicitations", () => {
    const fullAccess: AcpRuntimePolicy = {
      runtimeMode: "full-access",
      cwd: process.cwd(),
    };
    const approvalRequired: AcpRuntimePolicy = {
      runtimeMode: "approval-required",
      cwd: process.cwd(),
    };
    const tagged = {
      sessionId: "session-1",
      message: "Approve this request?",
      mode: "form",
      requestedSchema: { type: "object", properties: {} },
      _meta: { codex_approval_kind: "mcp_tool_call" },
    } satisfies EffectAcpSchema.CreateElicitationRequest;

    assert.equal(acpMcpToolApprovalElicitationDisposition(fullAccess, tagged), "allow");
    assert.equal(acpMcpToolApprovalElicitationDisposition(approvalRequired, tagged), "ask");
    assert.equal(
      acpMcpToolApprovalElicitationDisposition(
        { ...fullAccess, approvalPolicy: "on-request" },
        tagged,
      ),
      "ask",
    );
    assert.equal(
      acpMcpToolApprovalElicitationDisposition(
        {
          runtimeMode: "auto-accept-edits",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "workspaceWrite",
            writableRoots: [],
            networkAccess: false,
          },
        },
        tagged,
      ),
      "allow",
    );
    const { _meta: _tag, ...untagged } = tagged;
    assert.equal(
      acpMcpToolApprovalElicitationDisposition(
        fullAccess,
        untagged,
        "mcp_tool_call_approval_exec-123",
      ),
      "allow",
    );
    assert.isUndefined(
      acpMcpToolApprovalElicitationDisposition(fullAccess, {
        ...tagged,
        _meta: { codex_approval_kind: "ordinary_form" },
      }),
    );
    assert.isUndefined(
      acpMcpToolApprovalElicitationDisposition(fullAccess, {
        sessionId: "session-1",
        message: "Authenticate",
        mode: "url",
        elicitationId: "elicitation-1",
        url: "https://example.com/login",
        _meta: { codex_approval_kind: "mcp_tool_call" },
      }),
    );
  });
});

describe("client terminal disposition", () => {
  const cwd = NodePath.resolve(process.cwd(), "acp-client-policy-workspace");

  it("asks in approval-required and auto-accept-edits, allows in auto and full access", () => {
    assert.equal(acpClientExecuteDisposition({ runtimeMode: "approval-required", cwd }), "ask");
    assert.equal(acpClientExecuteDisposition({ runtimeMode: "auto-accept-edits", cwd }), "ask");
    assert.equal(acpClientExecuteDisposition({ runtimeMode: "auto", cwd }), "allow");
    assert.equal(acpClientExecuteDisposition({ runtimeMode: "full-access", cwd }), "allow");
  });

  it("denies terminals under explicit read-only and workspace-write sandboxes", () => {
    for (const sandboxPolicy of [
      { type: "readOnly" },
      { type: "workspaceWrite", writableRoots: [], networkAccess: false },
    ]) {
      assert.equal(
        acpClientExecuteDisposition({
          runtimeMode: "full-access",
          cwd,
          approvalPolicy: "never",
          sandboxPolicy,
        }),
        "deny",
      );
    }
  });
});

describe("client file system disposition", () => {
  const cwd = NodePath.resolve(process.cwd(), "acp-client-fs-workspace");
  const writableRoot = NodePath.resolve(process.cwd(), "acp-client-fs-extra-root");

  it("allows reads across all valid sandbox policies and denies unknown sandbox policies", () => {
    for (const sandboxPolicy of [
      undefined,
      { type: "readOnly" },
      { type: "workspaceWrite", writableRoots: [], networkAccess: false },
      { type: "dangerFullAccess" },
      { type: "externalSandbox" },
    ]) {
      assert.equal(
        acpClientReadDisposition({
          runtimeMode: "approval-required",
          cwd,
          sandboxPolicy,
        }),
        "allow",
      );
      assert.equal(
        acpClientReadDisposition({
          runtimeMode: "full-access",
          cwd,
          sandboxPolicy,
        }),
        "allow",
      );
    }
    assert.equal(
      acpClientReadDisposition({
        runtimeMode: "full-access",
        cwd,
        sandboxPolicy: { type: "unknownSandbox" },
      }),
      "deny",
    );
  });

  it("enforces approval, readOnly, and workspaceWrite containment for client writes", () => {
    const insidePath = NodePath.join(cwd, "src", "file.ts");
    const extraRootPath = NodePath.join(writableRoot, "generated.ts");
    const outsidePath = NodePath.resolve(process.cwd(), "outside-client-fs", "file.ts");

    assert.equal(
      acpClientWriteDisposition(
        {
          runtimeMode: "full-access",
          cwd,
          approvalPolicy: "never",
          sandboxPolicy: { type: "readOnly" },
        },
        insidePath,
      ),
      "deny",
    );

    assert.equal(
      acpClientWriteDisposition({ runtimeMode: "approval-required", cwd }, insidePath),
      "ask",
    );
    assert.equal(
      acpClientWriteDisposition(
        {
          runtimeMode: "full-access",
          cwd,
          approvalPolicy: "on-request",
          sandboxPolicy: { type: "workspaceWrite", writableRoots: [] },
        },
        insidePath,
      ),
      "ask",
    );
    assert.equal(
      acpClientWriteDisposition(
        { runtimeMode: "approval-required", cwd, approvalPolicy: "never" },
        insidePath,
      ),
      "deny",
    );

    for (const runtimeMode of ["auto-accept-edits", "full-access"] as const) {
      const workspaceWritePolicy: AcpRuntimePolicy = {
        runtimeMode,
        cwd,
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: [writableRoot],
          networkAccess: false,
        },
      };
      assert.equal(acpClientWriteDisposition(workspaceWritePolicy, insidePath), "allow");
      assert.equal(acpClientWriteDisposition(workspaceWritePolicy, extraRootPath), "allow");
      assert.equal(acpClientWriteDisposition(workspaceWritePolicy, outsidePath), "deny");
      assert.equal(acpClientWriteDisposition(workspaceWritePolicy, "../escape.ts"), "deny");
    }
  });
});

describe("makeAcpClientPolicyGrants", () => {
  it("grants terminals only from execute approvals, never from any other approved kind", () => {
    const grants = makeAcpClientPolicyGrants();
    for (const toolKind of ["read", "edit", "switch_mode", "other", "unknown", "browse"]) {
      grants.recordApproval({ toolKind, scope: "session", turnKey: "turn-1" });
    }
    assert.isFalse(grants.allowsExecute("turn-1"));
    grants.recordApproval({ toolKind: "execute", scope: "turn", turnKey: "turn-1" });
    assert.isTrue(grants.allowsExecute("turn-1"));
    assert.isFalse(grants.allowsExecute("turn-2"));
    assert.isFalse(grants.allowsExecute(null));
  });

  it("keeps accept-for-session command grants across turns", () => {
    const grants = makeAcpClientPolicyGrants();
    grants.recordApproval({ toolKind: "execute", scope: "session", turnKey: "turn-1" });
    assert.isTrue(grants.allowsExecute("turn-9"));
    assert.isTrue(grants.allowsExecute(null));
  });

  it("drops a turn's command grant when a later turn approves one", () => {
    const grants = makeAcpClientPolicyGrants();
    grants.recordApproval({ toolKind: "execute", scope: "turn", turnKey: "turn-1" });
    grants.recordApproval({ toolKind: "execute", scope: "turn", turnKey: "turn-2" });
    assert.isFalse(grants.allowsExecute("turn-1"));
    assert.isTrue(grants.allowsExecute("turn-2"));
  });

  it("clears both session-scoped and turn-scoped execute grants on reset", () => {
    const grants = makeAcpClientPolicyGrants();
    grants.recordApproval({ toolKind: "execute", scope: "session", turnKey: "turn-1" });
    grants.recordApproval({ toolKind: "execute", scope: "turn", turnKey: "turn-2" });
    assert.isTrue(grants.allowsExecute("turn-1"));
    assert.isTrue(grants.allowsExecute("turn-2"));

    grants.reset();
    assert.isFalse(grants.allowsExecute("turn-1"));
    assert.isFalse(grants.allowsExecute("turn-2"));
    assert.isFalse(grants.allowsExecute(null));
  });
});
