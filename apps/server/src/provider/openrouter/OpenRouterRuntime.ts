import {
  ClaudeSettings,
  type OpenRouterSettings,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderSession,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

export const OPENROUTER_DRIVER_KIND = ProviderDriverKind.make("openrouter");
export const DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api";
export const DEFAULT_OPENROUTER_MODEL = "anthropic/claude-sonnet-4.5";

const decodeClaudeSettings = Schema.decodeSync(ClaudeSettings);

/**
 * Anthropic-compat credential env vars that OpenRouter owns for the Claude
 * Code runtime. Always overwritten (never inherited from the host process).
 */
const OPENROUTER_OWNED_ENV_KEYS = [
  "ANTHROPIC_BASE_URL",
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_AUTH_TOKEN",
  "ANTHROPIC_CUSTOM_HEADERS",
  "OPENROUTER_API_KEY",
  "HTTP_REFERER",
  "X_TITLE",
  // Legacy aliases that must not leak from the host into OpenRouter sessions.
  "OR_SITE_URL",
  "OR_APP_NAME",
] as const;

/**
 * Secret-store key holding an instance's OpenRouter API key.
 *
 * The key is a credential, so the server prefers the secret store (0600 files
 * in the secrets dir) over `settings.json`, which is world-readable on most
 * setups and travels with settings exports.
 */
export function openRouterApiKeySecretName(instanceId: ProviderInstanceId): string {
  return `provider-${instanceId}-api-key`;
}

/**
 * Resolve the effective API key for an instance.
 *
 * A key typed into settings still wins, so an explicit edit takes effect
 * immediately; otherwise the stored secret is used. Both are trimmed, and a
 * blank value on either side falls through to the next source.
 */
export function resolveOpenRouterApiKey(input: {
  readonly settingsApiKey: string;
  readonly storedApiKey: string | undefined;
}): string {
  const fromSettings = input.settingsApiKey.trim();
  if (fromSettings.length > 0) {
    return fromSettings;
  }
  return (input.storedApiKey ?? "").trim();
}

/**
 * Pick the raw OpenRouter config blob the server should treat as current.
 *
 * Mirrors `ProviderInstanceRegistryHydration`: an explicit `providerInstances`
 * entry wins over the legacy `providers.openrouter` block. Returns `undefined`
 * when neither is present so callers can keep their existing config.
 */
export function selectLiveOpenRouterConfig(
  settings: Pick<ServerSettings, "providerInstances" | "providers">,
  instanceId: ProviderInstanceId,
): unknown {
  const explicit = settings.providerInstances[instanceId]?.config;
  if (explicit !== undefined && explicit !== null) {
    return explicit;
  }
  return settings.providers.openrouter;
}

export function normalizeOpenRouterBaseUrl(baseUrl: string): string {
  const trimmed = baseUrl.trim();
  const normalized = (trimmed.length > 0 ? trimmed : DEFAULT_OPENROUTER_BASE_URL).replace(
    /\/+$/,
    "",
  );
  return normalized;
}

export function openRouterModelsUrl(baseUrl: string): string {
  return `${normalizeOpenRouterBaseUrl(baseUrl)}/v1/models?supported_parameters=tools`;
}

/**
 * Narrow Claude runtime config needed by `makeClaudeAdapter` / text generation.
 * OpenRouter does not expose Claude homePath / launchArgs in its settings.
 */
export function toClaudeSettings(settings: OpenRouterSettings): ClaudeSettings {
  return decodeClaudeSettings({
    enabled: settings.enabled,
    binaryPath: settings.binaryPath,
    homePath: "",
    customModels: settings.customModels,
    launchArgs: "",
  });
}

/**
 * Build the process env for OpenRouter-backed Claude Code sessions.
 *
 * Matches OpenRouter's Claude Code contract:
 * - `ANTHROPIC_BASE_URL` → OpenRouter Anthropic skin (`https://openrouter.ai/api`)
 * - `ANTHROPIC_AUTH_TOKEN` → OpenRouter API key
 * - `ANTHROPIC_API_KEY` → always `""` so Claude Code does not prefer a host Anthropic key
 *
 * Owned credential/attribution keys are always cleared first so host values cannot leak
 * when settings omit them.
 */
export function buildOpenRouterProcessEnv(
  settings: OpenRouterSettings,
  baseEnv: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const next: NodeJS.ProcessEnv = { ...baseEnv };
  for (const key of OPENROUTER_OWNED_ENV_KEYS) {
    delete next[key];
  }

  const baseUrl = normalizeOpenRouterBaseUrl(settings.baseUrl);
  const apiKey = settings.apiKey.trim();

  next.ANTHROPIC_BASE_URL = baseUrl;
  // Critical: empty string (not unset) so Claude Code does not fall back to Anthropic auth.
  next.ANTHROPIC_API_KEY = "";

  if (apiKey.length > 0) {
    next.ANTHROPIC_AUTH_TOKEN = apiKey;
    next.OPENROUTER_API_KEY = apiKey;
  } else {
    next.ANTHROPIC_AUTH_TOKEN = "";
    next.OPENROUTER_API_KEY = "";
  }

  const httpReferer = settings.httpReferer.trim();
  if (httpReferer.length > 0) {
    next.HTTP_REFERER = httpReferer;
  }

  const appTitle = settings.appTitle.trim();
  if (appTitle.length > 0) {
    next.X_TITLE = appTitle;
  }

  // Claude Code does not read HTTP_REFERER/X_TITLE — it only forwards extra
  // request headers through ANTHROPIC_CUSTOM_HEADERS (newline-separated
  // "Name: value" pairs). The plain vars above stay for other OpenRouter
  // tooling that does read them.
  const customHeaders: Array<string> = [];
  if (httpReferer.length > 0) {
    customHeaders.push(`HTTP-Referer: ${httpReferer}`);
  }
  if (appTitle.length > 0) {
    customHeaders.push(`X-Title: ${appTitle}`);
  }
  if (customHeaders.length > 0) {
    next.ANTHROPIC_CUSTOM_HEADERS = customHeaders.join("\n");
  }

  // Claude Code defaults to tool search, which omits tools from `tools[]` and
  // refers to them with tool_reference blocks. OpenRouter's Anthropic-compatible
  // endpoint rejects that for every non-Anthropic model ("Deferred custom tools
  // are only supported on Anthropic models"), which kills the turn outright.
  // "false" is Claude Code's own off switch (ENABLE_TOOL_SEARCH => "standard"
  // mode). An explicit host value still wins, for proxies that do forward
  // tool_reference blocks.
  if (next.ENABLE_TOOL_SEARCH === undefined || next.ENABLE_TOOL_SEARCH.trim().length === 0) {
    next.ENABLE_TOOL_SEARCH = "false";
  }

  return next;
}

/**
 * Re-stamp a Claude-runtime adapter with the OpenRouter driver identity.
 *
 * The Claude adapter hardcodes provider "claudeAgent" on its sessions and
 * runtime events. Rather than parameterizing that heavily-churned module (a
 * merge-conflict magnet), OpenRouter decorates the finished adapter: the
 * identity field and every outbound event/session carry "openrouter", while
 * behavior passes through untouched. v2 note: OrchestratorV2 threads
 * instance identity through its own registry, so this decorator retires with
 * the V1 adapter contract.
 */
export function withOpenRouterAdapterIdentity<E>(
  adapter: ProviderAdapterShape<E>,
): ProviderAdapterShape<E> {
  const restampSession = (session: ProviderSession): ProviderSession => ({
    ...session,
    provider: OPENROUTER_DRIVER_KIND,
  });
  return {
    ...adapter,
    provider: OPENROUTER_DRIVER_KIND,
    streamEvents: Stream.map(adapter.streamEvents, (event) => ({
      ...event,
      provider: OPENROUTER_DRIVER_KIND,
    })),
    // Orchestration addresses this adapter as `openrouter`, but the wrapped
    // Claude adapter rejects any `startSession` input whose provider is not
    // its own kind. Translate on the way in, restamp on the way out.
    startSession: (input) =>
      Effect.map(
        adapter.startSession(
          input.provider === undefined ? input : { ...input, provider: adapter.provider },
        ),
        restampSession,
      ),
    listSessions: () =>
      Effect.map(adapter.listSessions(), (sessions) => sessions.map(restampSession)),
  };
}
