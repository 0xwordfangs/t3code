import {
  GoogleAccountVerificationUrl,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceConfig,
  type ServerProvider,
} from "@t3tools/contracts";
import { userEvent } from "vite-plus/test/browser";
import { describe, expect, it, vi } from "vite-plus/test";

import { render } from "../../../test/render";
import { ProviderEnvironmentSection, ProviderInstanceCard } from "./ProviderInstanceCard";

const savedSecret = {
  name: "FAKE_API_KEY",
  value: "",
  sensitive: true,
  valueRedacted: true,
} as const;

describe("ProviderEnvironmentSection saved secrets", () => {
  it("requires a replacement before renaming or unlocking a stored secret", async () => {
    const onChange = vi.fn();
    await using screen = await render(
      <ProviderEnvironmentSection environment={[savedSecret]} onChange={onChange} />,
    );
    const name = screen.getByRole("textbox", { name: "Environment variable name 1" });
    const value = screen.getByLabelText("Environment variable value 1");
    const lock = screen.getByRole("button", {
      name: "Mark environment variable FAKE_API_KEY as sensitive",
    });

    await expect.element(name).toBeDisabled();
    await expect.element(lock).toBeDisabled();
    await userEvent.fill(value, "unused-draft");
    await userEvent.clear(value);
    await userEvent.tab();
    expect(onChange).not.toHaveBeenCalled();
    await expect.element(name).toBeDisabled();
    await expect.element(lock).toBeDisabled();

    await userEvent.fill(value, "fake-replacement");
    await userEvent.tab();
    expect(onChange).toHaveBeenLastCalledWith([
      { ...savedSecret, value: "fake-replacement", valueRedacted: false },
    ]);
    await expect.element(name).toBeEnabled();
    await expect.element(lock).toBeEnabled();
    await userEvent.fill(name, "RENAMED_KEY");
    await userEvent.tab();
    await userEvent.click(
      screen.getByRole("button", { name: "Mark environment variable RENAMED_KEY as sensitive" }),
    );
    expect(onChange).toHaveBeenLastCalledWith([
      { name: "RENAMED_KEY", value: "fake-replacement", sensitive: false, valueRedacted: false },
    ]);
    await userEvent.click(
      screen.getByRole("button", { name: "Remove environment variable RENAMED_KEY" }),
    );
    expect(onChange).toHaveBeenLastCalledWith([]);
  });
});

describe("ProviderEnvironmentSection external settings writes", () => {
  it("keeps a typed unpublished row when an external write redelivers the environment", async () => {
    const onChange = vi.fn();
    await using screen = await render(
      <ProviderEnvironmentSection environment={[]} onChange={onChange} />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Add variable" }));
    await userEvent.type(
      screen.getByRole("textbox", { name: "Environment variable name 1" }),
      "KEEP_ME_TYPED",
    );
    expect(onChange).not.toHaveBeenCalled();

    // A settings write from elsewhere on the instance redelivers a redacted, stored variable.
    await screen.rerender(
      <ProviderEnvironmentSection
        environment={[{ name: "OTHER_VAR", value: "", sensitive: true, valueRedacted: true }]}
        onChange={onChange}
      />,
    );

    await expect
      .element(screen.getByRole("textbox", { name: "Environment variable name 2" }))
      .toHaveValue("KEEP_ME_TYPED");
  });

  it("does not duplicate a redacted published row on the sibling reseed", async () => {
    const onChange = vi.fn();
    await using screen = await render(
      <ProviderEnvironmentSection environment={[]} onChange={onChange} />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Add variable" }));
    await userEvent.fill(
      screen.getByRole("textbox", { name: "Environment variable name 1" }),
      "STORE_ME",
    );
    await userEvent.fill(screen.getByLabelText("Environment variable value 1"), "fake-secret");
    await userEvent.tab();
    expect(onChange).toHaveBeenLastCalledWith([
      { name: "STORE_ME", value: "fake-secret", sensitive: true, valueRedacted: false },
    ]);

    await userEvent.click(screen.getByRole("button", { name: "Add variable" }));
    await userEvent.type(
      screen.getByRole("textbox", { name: "Environment variable name 2" }),
      "UNPUBLISHED_ONE",
    );

    // The stored secret comes back redacted, so the rows reseed without matching the echo.
    await screen.rerender(
      <ProviderEnvironmentSection
        environment={[{ name: "STORE_ME", value: "", sensitive: true, valueRedacted: true }]}
        onChange={onChange}
      />,
    );

    await expect
      .element(screen.getByRole("textbox", { name: "Environment variable name 1" }))
      .toHaveValue("STORE_ME");
    await expect
      .element(screen.getByRole("textbox", { name: "Environment variable name 2" }))
      .toHaveValue("UNPUBLISHED_ONE");
  });

  it("keeps a committed but unpublishable row across an external write", async () => {
    const onChange = vi.fn();
    await using screen = await render(
      <ProviderEnvironmentSection environment={[]} onChange={onChange} />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Add variable" }));
    await userEvent.fill(
      screen.getByRole("textbox", { name: "Environment variable name 1" }),
      "KEEP-ME",
    );
    await userEvent.tab();
    expect(onChange).not.toHaveBeenCalled();

    await screen.rerender(
      <ProviderEnvironmentSection
        environment={[{ name: "OTHER_VAR", value: "", sensitive: true, valueRedacted: true }]}
        onChange={onChange}
      />,
    );
    await expect
      .element(screen.getByRole("textbox", { name: "Environment variable name 2" }))
      .toHaveValue("KEEP-ME");
  });

  it("drops an acknowledged row when an external write removes it", async () => {
    const onChange = vi.fn();
    await using screen = await render(
      <ProviderEnvironmentSection environment={[]} onChange={onChange} />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Add variable" }));
    await userEvent.fill(
      screen.getByRole("textbox", { name: "Environment variable name 1" }),
      "PUBLISH_ME",
    );
    await userEvent.tab();
    expect(onChange).toHaveBeenLastCalledWith([{ name: "PUBLISH_ME", value: "", sensitive: true }]);

    // The server echo acknowledges the publish and keeps the rows; nothing is republished.
    await screen.rerender(
      <ProviderEnvironmentSection
        environment={[{ name: "PUBLISH_ME", value: "", sensitive: true }]}
        onChange={onChange}
      />,
    );
    expect(onChange).toHaveBeenCalledTimes(1);
    const acknowledged = screen.getByRole("textbox", { name: "Environment variable name 1" });

    // Another client deletes the variable. The row must not come back.
    await screen.rerender(<ProviderEnvironmentSection environment={[]} onChange={onChange} />);
    await expect.element(acknowledged).not.toBeInTheDocument();
  });
});

describe("ProviderInstanceCard verification action", () => {
  const verificationUrl = GoogleAccountVerificationUrl.make(
    "https://accounts.google.com/v3/signin/continue?plt=secret-token",
  );
  const instanceId = ProviderInstanceId.make("antigravity");
  const driver = ProviderDriverKind.make("antigravity");
  const instance: ProviderInstanceConfig = {
    driver,
    enabled: true,
    config: {},
  };
  const liveProvider: ServerProvider = {
    instanceId,
    driver,
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "warning",
    auth: {
      status: "unknown",
      type: "oauth-personal",
      verificationRequired: true,
      verificationUrl,
    },
    message: "Verify your Google account to use Antigravity.",
    checkedAt: "2026-10-21T00:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  };

  const renderCard = (readOnly: boolean) => (
    <ProviderInstanceCard
      instanceId={instanceId}
      instance={instance}
      driverOption={undefined}
      liveProvider={liveProvider}
      mode="editor"
      readOnly={readOnly}
      onUpdate={async () => true}
      hiddenModels={[]}
      favoriteModels={[]}
      modelOrder={[]}
      onHiddenModelsChange={() => {}}
      onFavoriteModelsChange={() => {}}
      onToggleFavorite={() => {}}
      onModelOrderChange={() => {}}
    />
  );

  it("shows the compact verification destination when editable and hides the action when read-only", async () => {
    await using screen = await render(renderCard(false));

    await expect.element(screen.getByText("Verification required")).toBeVisible();
    await expect
      .element(screen.getByText("https://accounts.google.com/v3/signin/continue"))
      .toBeVisible();
    expect(screen.container.textContent).not.toContain("plt=secret-token");
    await expect
      .element(screen.getByRole("link", { name: "Verify account" }))
      .toHaveAttribute("href", verificationUrl);

    await screen.rerender(renderCard(true));

    await expect.element(screen.getByText("Verification required")).toBeVisible();
    await expect
      .element(screen.getByRole("link", { name: "Verify account" }))
      .not.toBeInTheDocument();
    await expect
      .element(screen.getByText("https://accounts.google.com/v3/signin/continue"))
      .not.toBeInTheDocument();
  });
});
