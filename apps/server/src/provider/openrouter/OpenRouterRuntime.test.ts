import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { OpenRouterSettings, ProviderDriverKind } from "@t3tools/contracts";

import {
  buildOpenRouterProcessEnv,
  normalizeOpenRouterBaseUrl,
  openRouterModelsUrl,
  toClaudeSettings,
  OPENROUTER_DRIVER_KIND,
  withOpenRouterAdapterIdentity,
} from "./OpenRouterRuntime.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const decodeOpenRouterSettings = Schema.decodeSync(OpenRouterSettings);

describe("OpenRouterRuntime", () => {
  it("normalizes base URL trailing slashes", () => {
    expect(normalizeOpenRouterBaseUrl("https://openrouter.ai/api/")).toBe(
      "https://openrouter.ai/api",
    );
    expect(normalizeOpenRouterBaseUrl("")).toBe("https://openrouter.ai/api");
  });

  it("builds the tools-capable models URL", () => {
    expect(openRouterModelsUrl("https://openrouter.ai/api")).toBe(
      "https://openrouter.ai/api/v1/models?supported_parameters=tools",
    );
  });

  it("maps settings into Claude settings + OpenRouter-owned process env", () => {
    const settings = decodeOpenRouterSettings({
      apiKey: "sk-or-test",
      baseUrl: "https://openrouter.ai/api/",
      binaryPath: "claude",
      httpReferer: "https://t3.chat",
      appTitle: "T3 Code",
    });

    expect(toClaudeSettings(settings)).toMatchObject({
      enabled: true,
      binaryPath: "claude",
      homePath: "",
      launchArgs: "",
    });

    const env = buildOpenRouterProcessEnv(settings, {
      PATH: "/usr/bin",
      ANTHROPIC_API_KEY: "host-anthropic-key",
      ANTHROPIC_AUTH_TOKEN: "host-token",
      OPENROUTER_API_KEY: "host-openrouter",
      OR_SITE_URL: "https://leaked.example",
      OR_APP_NAME: "Leaked",
    });
    expect(env.ANTHROPIC_BASE_URL).toBe("https://openrouter.ai/api");
    // OpenRouter Claude Code contract: auth token + empty API key.
    expect(env.ANTHROPIC_AUTH_TOKEN).toBe("sk-or-test");
    expect(env.ANTHROPIC_API_KEY).toBe("");
    expect(env.OPENROUTER_API_KEY).toBe("sk-or-test");
    expect(env.HTTP_REFERER).toBe("https://t3.chat");
    expect(env.X_TITLE).toBe("T3 Code");
    // Claude Code only forwards headers through ANTHROPIC_CUSTOM_HEADERS.
    expect(env.ANTHROPIC_CUSTOM_HEADERS).toBe("HTTP-Referer: https://t3.chat\nX-Title: T3 Code");
    // OpenRouter rejects deferred tools for non-Anthropic models.
    expect(env.ENABLE_TOOL_SEARCH).toBe("false");
    expect(env.OR_SITE_URL).toBeUndefined();
    expect(env.OR_APP_NAME).toBeUndefined();
    expect(env.PATH).toBe("/usr/bin");
  });

  it("keeps an explicit host tool-search choice", () => {
    const settings = decodeOpenRouterSettings({ apiKey: "sk-or-test" });
    const env = buildOpenRouterProcessEnv(settings, { ENABLE_TOOL_SEARCH: "true" });
    expect(env.ENABLE_TOOL_SEARCH).toBe("true");

    // A blank host value is not a choice, so the OpenRouter default applies.
    const blank = buildOpenRouterProcessEnv(settings, { ENABLE_TOOL_SEARCH: "  " });
    expect(blank.ENABLE_TOOL_SEARCH).toBe("false");
  });

  it("clears inherited Anthropic credentials when settings apiKey is empty", () => {
    const settings = decodeOpenRouterSettings({
      apiKey: "",
      httpReferer: "",
      appTitle: "",
    });
    const env = buildOpenRouterProcessEnv(settings, {
      ANTHROPIC_API_KEY: "sk-ant-host",
      ANTHROPIC_AUTH_TOKEN: "host-token",
      ANTHROPIC_CUSTOM_HEADERS: "X-Host: leaked",
      OPENROUTER_API_KEY: "sk-or-host",
      HTTP_REFERER: "https://host.example",
      X_TITLE: "Host App",
    });

    expect(env.ANTHROPIC_API_KEY).toBe("");
    expect(env.ANTHROPIC_AUTH_TOKEN).toBe("");
    expect(env.OPENROUTER_API_KEY).toBe("");
    expect(env.HTTP_REFERER).toBeUndefined();
    expect(env.X_TITLE).toBeUndefined();
    expect(env.ANTHROPIC_CUSTOM_HEADERS).toBeUndefined();
    expect(env.ANTHROPIC_BASE_URL).toBe("https://openrouter.ai/api");
  });
});

describe("withOpenRouterAdapterIdentity", () => {
  it.effect("restamps the adapter identity, events, and sessions without touching behavior", () =>
    Effect.gen(function* () {
      const claudeKind = ProviderDriverKind.make("claudeAgent");
      const session = { provider: claudeKind, threadId: "thread-1" };
      const event = { provider: claudeKind, type: "session.started" };
      const base = {
        provider: claudeKind,
        streamEvents: Stream.make(event),
        startSession: () => Effect.succeed(session),
        listSessions: () => Effect.succeed([session]),
        stopSession: () => Effect.void,
      } as unknown as ProviderAdapterShape<never>;

      const decorated = withOpenRouterAdapterIdentity(base);

      expect(decorated.provider).toBe(OPENROUTER_DRIVER_KIND);
      // Untouched members pass through by reference.
      expect(decorated.stopSession).toBe(base.stopSession);

      const events = [
        ...((yield* Stream.runCollect(decorated.streamEvents)) as Iterable<{
          provider: string;
        }>),
      ];
      expect(events.map((entry) => entry.provider)).toEqual([OPENROUTER_DRIVER_KIND]);

      const started = yield* decorated.startSession({} as never) as Effect.Effect<{
        provider: string;
      }>;
      expect(started.provider).toBe(OPENROUTER_DRIVER_KIND);

      const listed = (yield* decorated.listSessions()) as ReadonlyArray<{ provider: string }>;
      expect(listed.map((entry) => entry.provider)).toEqual([OPENROUTER_DRIVER_KIND]);
    }),
  );

  it.effect("hands the wrapped adapter its own provider on startSession input", () =>
    Effect.gen(function* () {
      const claudeKind = ProviderDriverKind.make("claudeAgent");
      const seen: Array<unknown> = [];
      const base = {
        provider: claudeKind,
        streamEvents: Stream.empty,
        startSession: (input: { provider?: string }) => {
          seen.push(input.provider);
          // Mirrors ClaudeAdapter, which rejects a foreign provider outright.
          return input.provider !== undefined && input.provider !== claudeKind
            ? Effect.die(`Expected provider '${claudeKind}' but received '${input.provider}'.`)
            : Effect.succeed({ provider: claudeKind, threadId: "thread-1" });
        },
        listSessions: () => Effect.succeed([]),
      } as unknown as ProviderAdapterShape<never>;

      const decorated = withOpenRouterAdapterIdentity(base);

      const started = yield* decorated.startSession({
        provider: OPENROUTER_DRIVER_KIND,
        threadId: "thread-1",
      } as never) as Effect.Effect<{ provider: string }>;

      expect(seen).toEqual([claudeKind]);
      expect(started.provider).toBe(OPENROUTER_DRIVER_KIND);

      // An input with no provider stays untouched.
      yield* decorated.startSession({ threadId: "thread-2" } as never);
      expect(seen).toEqual([claudeKind, undefined]);
    }),
  );
});
