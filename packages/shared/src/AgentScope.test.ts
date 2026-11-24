// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "@effect/vitest";

import { AGENT_OOM_SCORE_ADJ, agentScopeCommand, agentScopeLauncherScript } from "./AgentScope.ts";
import * as HostProcess from "./HostProcess.ts";

describe.skipIf(HostProcess.Platform.defaultValue() !== "linux")("agentScopeCommand", () => {
  it("raises the OOM score and execs the command with its arguments in the same process", () => {
    const launch = agentScopeCommand({
      command: "/bin/sh",
      args: [
        "-c",
        'printf "%s\\n" "$$" "$(cat /proc/self/oom_score_adj)" "$@"',
        "agent",
        "a b",
        "$HOME",
      ],
    });
    const child = NodeChildProcess.spawnSync(launch.command, [...launch.args], {
      encoding: "utf8",
    });
    const [pid, score, ...args] = child.stdout.trimEnd().split("\n");
    expect(Number(pid)).toBe(child.pid);
    expect(Number(score)).toBe(AGENT_OOM_SCORE_ADJ);
    expect(args).toEqual(["a b", "$HOME"]);

    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-agent-launcher-"));
    try {
      const launcher = NodePath.join(directory, "launch's");
      NodeFS.writeFileSync(launcher, agentScopeLauncherScript(launch), { mode: 0o700 });
      const launched = NodeChildProcess.spawnSync(launcher, ["--flag", "it's"], {
        encoding: "utf8",
      });
      const [launchedPid, launchedScore, ...launchedArgs] = launched.stdout.trimEnd().split("\n");
      expect(Number(launchedPid)).toBe(launched.pid);
      expect(Number(launchedScore)).toBe(AGENT_OOM_SCORE_ADJ);
      expect(launchedArgs).toEqual(["a b", "$HOME", "--flag", "it's"]);
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  });

  it("unconditionally pins XDG_RUNTIME_DIR and unsets DBUS_SESSION_BUS_ADDRESS before execing systemd-run", () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-agent-scoped-env-"));
    try {
      const fakeSystemdRun = NodePath.join(directory, "systemd-run");
      NodeFS.writeFileSync(
        fakeSystemdRun,
        [
          "#!/bin/sh",
          'printf "OOM=%s\\n" "$(cat /proc/self/oom_score_adj)"',
          'printf "XDG=%s\\n" "${XDG_RUNTIME_DIR-<unset>}"',
          'printf "DBUS=%s\\n" "${DBUS_SESSION_BUS_ADDRESS-<unset>}"',
          'printf "ARGS=%s\\n" "$*"',
        ].join("\n") + "\n",
        { mode: 0o700 },
      );
      const expectedRuntimeDir = NodePath.join(directory, "host-runtime");
      const launch = agentScopeCommand({
        command: "/bin/echo",
        args: ["hello"],
        scope: {
          systemdRun: fakeSystemdRun,
          slice: "app-t3code-agents-test.slice",
          unit: "t3code-acp-terminal-test.scope",
          runtimeDir: expectedRuntimeDir,
          options: ["--collect"],
        },
      });
      const child = NodeChildProcess.spawnSync(launch.command, [...launch.args], {
        encoding: "utf8",
        env: {
          ...process.env,
          XDG_RUNTIME_DIR: "/tmp/attacker-xdg-runtime",
          DBUS_SESSION_BUS_ADDRESS: "unix:path=/tmp/attacker-dbus/bus",
        },
      });
      expect(child.status).toBe(0);
      const lines = Object.fromEntries(
        child.stdout
          .trimEnd()
          .split("\n")
          .map((line) => {
            const idx = line.indexOf("=");
            return [line.slice(0, idx), line.slice(idx + 1)];
          }),
      );
      expect(Number(lines.OOM)).toBe(AGENT_OOM_SCORE_ADJ);
      expect(lines.XDG).toBe(expectedRuntimeDir);
      expect(lines.DBUS).toBe("<unset>");
      expect(lines.ARGS).toContain("--unit=t3code-acp-terminal-test.scope");
      expect(lines.ARGS).toContain("-- /bin/echo hello");
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  });
});
