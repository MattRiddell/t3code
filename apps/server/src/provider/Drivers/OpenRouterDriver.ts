/**
 * OpenRouterDriver — first-class `ProviderDriver` for OpenRouter.
 *
 * Uses the Claude Agent SDK/CLI as the agent runtime while owning OpenRouter
 * settings (API key, base URL, attribution) and stamping `driverKind:
 * "openrouter"` on snapshots and sessions.
 *
 * @module provider/Drivers/OpenRouterDriver
 */
import { OpenRouterSettings, type ServerProvider } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerSecretStore } from "../../auth/ServerSecretStore.ts";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { makeClaudeTextGeneration } from "../../textGeneration/ClaudeTextGeneration.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeClaudeAdapter } from "../Layers/ClaudeAdapter.ts";
import {
  checkOpenRouterProviderStatus,
  makePendingOpenRouterProvider,
} from "../Layers/OpenRouterProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  makeStaticProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import {
  buildOpenRouterProcessEnv,
  OPENROUTER_DRIVER_KIND,
  openRouterApiKeySecretName,
  resolveOpenRouterApiKey,
  selectLiveOpenRouterConfig,
  toClaudeSettings,
  withOpenRouterAdapterIdentity,
} from "../openrouter/OpenRouterRuntime.ts";

const decodeOpenRouterSettings = Schema.decodeSync(OpenRouterSettings);
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);
const UPDATE = makeStaticProviderMaintenanceResolver(
  makeManualOnlyProviderMaintenanceCapabilities({
    provider: OPENROUTER_DRIVER_KIND,
    packageName: null,
  }),
);

export type OpenRouterDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: OPENROUTER_DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const OpenRouterDriver: ProviderDriver<OpenRouterSettings, OpenRouterDriverEnv> = {
  driverKind: OPENROUTER_DRIVER_KIND,
  metadata: {
    displayName: "OpenRouter",
    supportsMultipleInstances: true,
  },
  configSchema: OpenRouterSettings,
  defaultConfig: (): OpenRouterSettings => decodeOpenRouterSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const httpClient = yield* HttpClient.HttpClient;
      const serverSettings = yield* ServerSettingsService;
      // Optional so the driver still builds in contexts that do not wire a
      // secret store (tests, minimal embeddings); those simply fall back to
      // whatever key settings carry.
      const secretStore = yield* Effect.serviceOption(ServerSecretStore);
      const eventLoggers = yield* ProviderEventLoggers;
      const baseEnv = mergeProviderInstanceEnvironment(environment);

      // The API key is a credential: prefer the secret store over settings.json.
      // A key typed into settings still wins so an explicit edit applies at once.
      const secretName = openRouterApiKeySecretName(instanceId);
      const withStoredApiKey = (candidate: OpenRouterSettings) =>
        Effect.gen(function* () {
          const stored = Option.isNone(secretStore)
            ? Option.none<Uint8Array>()
            : yield* secretStore.value
                .get(secretName)
                .pipe(Effect.catchCause(() => Effect.succeed(Option.none<Uint8Array>())));
          const storedApiKey = Option.isSome(stored)
            ? new TextDecoder().decode(stored.value)
            : undefined;
          const apiKey = resolveOpenRouterApiKey({
            settingsApiKey: candidate.apiKey,
            storedApiKey,
          });

          // A key typed into settings lands in settings.json as plain text.
          // Copy it into the secret store so the credential has a home that
          // is not part of the settings blob; settings keep working either way.
          if (
            Option.isSome(secretStore) &&
            apiKey.length > 0 &&
            apiKey !== (storedApiKey ?? "").trim()
          ) {
            yield* secretStore.value
              .set(secretName, new TextEncoder().encode(apiKey))
              .pipe(Effect.ignore);
          }

          return { ...candidate, apiKey } satisfies OpenRouterSettings;
        });

      const effectiveConfig = yield* withStoredApiKey({
        ...config,
        enabled,
      } satisfies OpenRouterSettings);
      // Build OpenRouter-owned process env once; pass through to adapter + probes.
      const processEnv = buildOpenRouterProcessEnv(effectiveConfig, baseEnv);
      const claudeSettings = toClaudeSettings(effectiveConfig);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: OPENROUTER_DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const maintenanceCapabilities = yield* resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
        binaryPath: effectiveConfig.binaryPath,
        env: processEnv,
      });

      const adapter = withOpenRouterAdapterIdentity(
        yield* makeClaudeAdapter(claudeSettings, {
          instanceId,
          environment: processEnv,
          ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
        }),
      );
      const textGeneration = yield* makeClaudeTextGeneration(claudeSettings, processEnv);

      // Snapshot settings sources capture the driver config at create time, so
      // every later status check would re-probe with the config this instance
      // was born with. For OpenRouter the API key lives in that config, so a
      // key saved after boot never reached the probe: the provider sat on
      // "add an API key" and the model catalog never refreshed. Re-read the
      // live config on each check instead, and rebuild the OpenRouter-owned
      // env from it.
      const readLiveConfig = Effect.gen(function* () {
        const settings = yield* serverSettings.getSettings.pipe(
          Effect.catchCause(() => Effect.succeed(undefined)),
        );
        if (settings === undefined) {
          return effectiveConfig;
        }
        const raw = selectLiveOpenRouterConfig(settings, instanceId);
        if (raw === undefined) {
          return effectiveConfig;
        }
        const decoded = yield* Schema.decodeUnknownEffect(OpenRouterSettings)(raw).pipe(
          Effect.catchCause(() => Effect.succeed(undefined)),
        );
        if (decoded === undefined) {
          return effectiveConfig;
        }
        // `enabled` stays owned by the registry envelope, not the config blob.
        return yield* withStoredApiKey({ ...decoded, enabled });
      });

      const checkProvider = readLiveConfig.pipe(
        Effect.flatMap((liveConfig) =>
          checkOpenRouterProviderStatus(liveConfig, buildOpenRouterProcessEnv(liveConfig, baseEnv)),
        ),
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(Path.Path, path),
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<OpenRouterSettings>
      >({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingOpenRouterProvider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        refreshInterval: SNAPSHOT_REFRESH_INTERVAL,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: OPENROUTER_DRIVER_KIND,
              instanceId,
              detail: `Failed to build OpenRouter snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: OPENROUTER_DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
