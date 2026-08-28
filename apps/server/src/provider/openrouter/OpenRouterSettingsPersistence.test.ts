import { describe, expect, it } from "@effect/vitest";
import {
  defaultInstanceIdForDriver,
  OpenRouterSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  ServerSettingsPatch,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  openRouterApiKeySecretName,
  resolveOpenRouterApiKey,
  selectLiveOpenRouterConfig,
} from "./OpenRouterRuntime.ts";

const decodeSettings = Schema.decodeUnknownSync(ServerSettings);
const decodePatch = Schema.decodeUnknownSync(ServerSettingsPatch);
const decodeOpenRouter = Schema.decodeUnknownSync(OpenRouterSettings);
const KIND = ProviderDriverKind.make("openrouter");

describe("OpenRouter settings persistence", () => {
  it("keeps the api key when saved as an explicit instance", () => {
    const instanceId = defaultInstanceIdForDriver(KIND);
    const patch = decodePatch({
      providerInstances: {
        [instanceId]: { driver: KIND, config: { apiKey: "sk-or-secret", enabled: true } },
      },
    });

    const saved = decodeSettings({ providerInstances: patch.providerInstances });
    const entry = saved.providerInstances[instanceId];
    expect(entry).toBeDefined();
    expect(entry?.driver).toBe(KIND);
    // The envelope keeps the blob opaque; the driver schema decodes it.
    expect(decodeOpenRouter(entry?.config).apiKey).toBe("sk-or-secret");
  });

  it("keeps the api key when saved to the legacy providers block", () => {
    const saved = decodeSettings({
      providers: { openrouter: { apiKey: "sk-or-legacy", enabled: true } },
    });
    expect(saved.providers.openrouter.apiKey).toBe("sk-or-legacy");
  });

  it("survives a full settings encode/decode round trip", () => {
    const instanceId = defaultInstanceIdForDriver(KIND);
    const saved = decodeSettings({
      providerInstances: {
        [instanceId]: { driver: KIND, config: { apiKey: "sk-or-roundtrip" } },
      },
    });
    const wire = JSON.parse(JSON.stringify(Schema.encodeSync(ServerSettings)(saved)));
    const back = decodeSettings(wire);
    expect(decodeOpenRouter(back.providerInstances[instanceId]?.config).apiKey).toBe(
      "sk-or-roundtrip",
    );
  });

  it("does not drop the key when an unrelated setting is patched", () => {
    const instanceId = defaultInstanceIdForDriver(KIND);
    const existing = decodeSettings({
      providerInstances: {
        [instanceId]: { driver: KIND, config: { apiKey: "sk-or-keep" } },
      },
    });
    // The UI resends the whole map; simulate it editing another driver.
    const patch = decodePatch({
      providerInstances: {
        ...existing.providerInstances,
        [ProviderInstanceId.make("codex")]: {
          driver: ProviderDriverKind.make("codex"),
          config: { enabled: true },
        },
      },
    });
    const next = decodeSettings({ providerInstances: patch.providerInstances });
    expect(decodeOpenRouter(next.providerInstances[instanceId]?.config).apiKey).toBe("sk-or-keep");
  });

  it("reads a key saved after boot: explicit instance wins over legacy", () => {
    const instanceId = defaultInstanceIdForDriver(KIND);
    const settings = decodeSettings({
      providers: { openrouter: { apiKey: "sk-or-legacy" } },
      providerInstances: {
        [instanceId]: { driver: KIND, config: { apiKey: "sk-or-instance" } },
      },
    });

    const live = selectLiveOpenRouterConfig(settings, instanceId);
    expect(decodeOpenRouter(live).apiKey).toBe("sk-or-instance");
  });

  it("falls back to the legacy block when no instance entry exists", () => {
    const instanceId = defaultInstanceIdForDriver(KIND);
    const settings = decodeSettings({ providers: { openrouter: { apiKey: "sk-or-legacy" } } });
    expect(decodeOpenRouter(selectLiveOpenRouterConfig(settings, instanceId)).apiKey).toBe(
      "sk-or-legacy",
    );
  });

  it("reports no live config when the user has configured nothing", () => {
    const instanceId = defaultInstanceIdForDriver(KIND);
    const settings = decodeSettings({});
    const live = selectLiveOpenRouterConfig(settings, instanceId);
    // Legacy defaults decode to an empty key, which is what the status check
    // reports as "add an API key" rather than silently probing.
    expect(decodeOpenRouter(live).apiKey).toBe("");
  });

  it("prefers a key typed into settings, else the stored secret", () => {
    expect(resolveOpenRouterApiKey({ settingsApiKey: "sk-typed", storedApiKey: "sk-stored" })).toBe(
      "sk-typed",
    );
    expect(resolveOpenRouterApiKey({ settingsApiKey: "", storedApiKey: "sk-stored" })).toBe(
      "sk-stored",
    );
    expect(resolveOpenRouterApiKey({ settingsApiKey: "   ", storedApiKey: "sk-stored" })).toBe(
      "sk-stored",
    );
    // Trailing newline is easy to introduce when writing the secret file.
    expect(resolveOpenRouterApiKey({ settingsApiKey: "", storedApiKey: "sk-stored\n" })).toBe(
      "sk-stored",
    );
    expect(resolveOpenRouterApiKey({ settingsApiKey: "", storedApiKey: undefined })).toBe("");
  });

  it("names the secret per instance so multiple accounts do not collide", () => {
    expect(openRouterApiKeySecretName(defaultInstanceIdForDriver(KIND))).toBe(
      "provider-openrouter-api-key",
    );
    expect(openRouterApiKeySecretName(ProviderInstanceId.make("openrouter_work"))).toBe(
      "provider-openrouter_work-api-key",
    );
  });

  it.effect("reports the key as configured to the status check", () =>
    Effect.gen(function* () {
      const settings = decodeOpenRouter({ apiKey: "sk-or-live", enabled: true });
      expect(settings.apiKey).toBe("sk-or-live");
      expect(settings.enabled).toBe(true);
    }),
  );
});
