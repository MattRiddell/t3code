/**
 * OpenRouterProvider — snapshot/status checks for the OpenRouter driver.
 *
 * OpenRouter rides the Claude Agent CLI as its runtime, so readiness is the
 * conjunction of two probes: the `claude` binary answers `--version` with the
 * OpenRouter-owned environment applied, and the OpenRouter API accepts the
 * configured key (verified by fetching the model catalog). Either failing
 * degrades the snapshot rather than erroring the driver.
 *
 * Turbo seam: v2-safe by design — everything here consumes only the
 * `provider/openrouter` transport module and generic snapshot helpers, none
 * of the V1 adapter contract.
 *
 * @module provider/Layers/OpenRouterProvider
 */
import type { OpenRouterSettings, ServerProviderModel } from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  EMPTY_OPENROUTER_CAPABILITIES,
  FALLBACK_OPENROUTER_MODELS,
  fetchOpenRouterModels,
} from "../openrouter/OpenRouterModels.ts";
import {
  buildOpenRouterProcessEnv,
  normalizeOpenRouterBaseUrl,
} from "../openrouter/OpenRouterRuntime.ts";

const OPENROUTER_PRESENTATION = {
  displayName: "OpenRouter",
  showInteractionModeToggle: true,
} as const;

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

function modelsFromSettings(
  builtIn: ReadonlyArray<ServerProviderModel>,
  customModels: ReadonlyArray<string>,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtIn, customModels, EMPTY_OPENROUTER_CAPABILITIES);
}

interface CliProbeFields {
  readonly installed: boolean;
  readonly version: string | null;
  readonly cliOk: boolean;
  readonly cliMessage: string;
}

/**
 * Probe the Claude Agent CLI with the OpenRouter environment applied. The
 * probe result never fails the effect; failures fold into snapshot fields.
 */
const probeClaudeCliForOpenRouter = Effect.fn("probeClaudeCliForOpenRouter")(function* (
  settings: OpenRouterSettings,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<CliProbeFields, never, ChildProcessSpawner.ChildProcessSpawner> {
  const run = Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(settings.binaryPath, ["--version"], {
      env: environment,
    });
    const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: environment,
      shell: spawnCommand.shell,
    });
    return yield* spawnAndCollect(settings.binaryPath, command);
  });

  const versionProbe = yield* run.pipe(Effect.timeoutOption(DEFAULT_TIMEOUT_MS), Effect.result);

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    if (isCommandMissingCause(error)) {
      return {
        installed: false,
        version: null,
        cliOk: false,
        cliMessage:
          "Claude Agent CLI (`claude`) is not installed or not on PATH. OpenRouter uses Claude Code as its agent runtime.",
      };
    }
    return {
      installed: true,
      version: null,
      cliOk: false,
      cliMessage: "Failed to execute Claude Agent CLI health check for OpenRouter.",
    };
  }

  if (Option.isNone(versionProbe.success)) {
    return {
      installed: true,
      version: null,
      cliOk: false,
      cliMessage: "Claude Agent CLI timed out while running `--version` for OpenRouter.",
    };
  }

  const version = versionProbe.success.value;
  const parsedVersion = parseGenericCliVersion(`${version.stdout}\n${version.stderr}`);
  if (version.code !== 0) {
    return {
      installed: true,
      version: parsedVersion,
      cliOk: false,
      cliMessage: "Claude Agent CLI is installed but failed to run for OpenRouter.",
    };
  }

  return {
    installed: true,
    version: parsedVersion,
    cliOk: true,
    cliMessage: "",
  };
});

/** Full status check: CLI runtime probe plus API-key-validating model fetch. */
export const checkOpenRouterProviderStatus = Effect.fn("checkOpenRouterProviderStatus")(function* (
  settings: OpenRouterSettings,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | HttpClient.HttpClient
> {
  const checkedAt = yield* nowIso;
  const fallbackModels = modelsFromSettings(FALLBACK_OPENROUTER_MODELS, settings.customModels);

  if (!settings.enabled) {
    return buildServerProvider({
      presentation: OPENROUTER_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "OpenRouter is disabled in T3 Turbo settings.",
      },
    });
  }

  // OpenRouter ships enabled so it appears in settings, but it cannot do
  // anything without a key. Short-circuit before the CLI probe so the common
  // "never configured OpenRouter" install pays no startup spawn for it.
  if (settings.apiKey.trim().length === 0) {
    return buildServerProvider({
      presentation: OPENROUTER_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unauthenticated" },
        message: "Add an OpenRouter API key in provider settings.",
      },
    });
  }

  const processEnv = environment ?? buildOpenRouterProcessEnv(settings);
  const cliFields = yield* probeClaudeCliForOpenRouter(settings, processEnv);
  const modelFetch = yield* fetchOpenRouterModels(settings);

  const authOk = modelFetch.ok;
  const auth = authOk
    ? { status: "authenticated" as const }
    : {
        status: modelFetch.authFailed ? ("unauthenticated" as const) : ("unknown" as const),
      };

  const models = authOk
    ? modelsFromSettings(modelFetch.models, settings.customModels)
    : fallbackModels;

  if (cliFields.cliOk && authOk) {
    return buildServerProvider({
      presentation: OPENROUTER_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: cliFields.version,
        status: "ready",
        auth,
        message: `Using ${normalizeOpenRouterBaseUrl(settings.baseUrl)} via the Claude Code runtime.`,
      },
    });
  }

  const messages: Array<string> = [];
  if (!cliFields.cliOk) {
    messages.push(cliFields.cliMessage);
  }
  if (!authOk) {
    messages.push(modelFetch.message);
  }

  // A missing runtime or a rejected key blocks sessions outright; a catalog
  // fetch failing for network reasons only degrades the model list.
  const status =
    !cliFields.cliOk || (modelFetch.ok === false && modelFetch.authFailed) ? "error" : "warning";

  return buildServerProvider({
    presentation: OPENROUTER_PRESENTATION,
    enabled: settings.enabled,
    checkedAt,
    models,
    probe: {
      installed: cliFields.installed,
      version: cliFields.version,
      status,
      auth,
      message: messages.join(" "),
    },
  });
});

/** Instant pre-probe snapshot so the instance renders before the first check. */
export const makePendingOpenRouterProvider = (
  settings: OpenRouterSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* nowIso;
    const models = modelsFromSettings(FALLBACK_OPENROUTER_MODELS, settings.customModels);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: OPENROUTER_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "OpenRouter is disabled in T3 Turbo settings.",
        },
      });
    }

    if (settings.apiKey.trim().length === 0) {
      return buildServerProvider({
        presentation: OPENROUTER_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "error",
          auth: { status: "unauthenticated" },
          message: "Add an OpenRouter API key in provider settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: OPENROUTER_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking OpenRouter…",
      },
    });
  });
