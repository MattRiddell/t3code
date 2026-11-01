import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { prepareFreshProviderThread } from "./freshProviderContext.ts";

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Renewal must not start a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const layer = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "fresh-context" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

it.effect(
  "renews native context while retaining visible identity, messages, runs and old refs; refuses unsafe twins",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("console");
      const sessionId = ProviderSessionId.make("console-session");
      const providerThreadId = ProviderThreadId.make("old-console-native");
      const now = yield* DateTime.now;
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project"),
        title: "Work With Matt",
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const apply = (type: OrchestrationV2DomainEvent["type"], payload: unknown) =>
        store.apply({
          id: EventId.make(`seed:${type}`),
          type,
          threadId,
          occurredAt: now,
          payload,
        } as OrchestrationV2DomainEvent);
      yield* apply("provider-session.attached", {
        id: sessionId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/repo",
        model: null,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      });
      yield* apply("provider-thread.updated", {
        id: providerThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver: adapter.driver, nativeId: "keep-history", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      });
      yield* apply("run.updated", {
        id: RunId.make("old-run"),
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        providerThreadId,
        userMessageId: MessageId.make("old-message"),
        rootNodeId: null,
        activeAttemptId: null,
        status: "completed",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      });
      yield* apply("message.updated", {
        id: MessageId.make("old-message"),
        threadId,
        runId: RunId.make("old-run"),
        nodeId: null,
        role: "user",
        createdBy: "user",
        creationSource: "web",
        text: "Keep this full original ask in the visible history.",
        attachments: [],
        streaming: false,
        createdAt: now,
        updatedAt: now,
      });
      const before = yield* store.getThreadProjection(threadId);
      const command = {
        type: "provider-session.renew" as const,
        commandId: CommandId.make("renew"),
        threadId,
        providerSessionId: sessionId,
        freshContext: {
          providerThreadId,
          expectedUpdatedAt: before.thread.updatedAt,
          bootstrap: "Role; complete handoff; lessons pointer.",
        },
      };
      assert.equal(
        typeof prepareFreshProviderThread(before, command, now),
        "string",
        "NO: less than 30 minutes",
      );
      yield* TestClock.adjust("31 minutes");
      const later = yield* DateTime.now;
      const refuse = (records: typeof before, cmd = command) =>
        assert.equal(typeof prepareFreshProviderThread(records, cmd, later), "string");
      refuse(before, {
        ...command,
        freshContext: { ...command.freshContext, bootstrap: "😀".repeat(1250) },
      });
      refuse(before, { ...command, freshContext: { ...command.freshContext, bootstrap: " " } });
      refuse(before, {
        ...command,
        freshContext: { ...command.freshContext, expectedUpdatedAt: later },
      });
      refuse(before, { ...command, providerSessionId: ProviderSessionId.make("wrong") });
      for (const status of ["queued", "starting", "running", "waiting"] as const) {
        refuse({ ...before, runs: before.runs.map((row) => ({ ...row, status })) });
      }
      refuse({
        ...before,
        providerThreads: before.providerThreads.map((row) => ({
          ...row,
          pendingBackgroundTasks: [{ taskId: "active-background", kind: "monitor" as const }],
        })),
      });
      yield* orchestrator.dispatch(command);
      const after = yield* store.getThreadProjection(threadId);
      assert.equal(after.thread.id, before.thread.id);
      assert.equal(after.thread.title, before.thread.title);
      assert.deepEqual(after.messages, before.messages);
      assert.deepEqual(after.runs, before.runs);
      assert.deepEqual(after.providerSessions, before.providerSessions);
      // No delayed detach may interrupt a new turn after the renewal commits.
      assert.deepEqual(after.providerTurns, before.providerTurns);
      const old = after.providerThreads.find((row) => row.id === providerThreadId)!;
      assert.equal(old.nativeThreadRef?.nativeId, "keep-history");
      assert.equal(old.status, "archived");
      const fresh = after.providerThreads.find(
        (row) => row.id === after.thread.activeProviderThreadId,
      )!;
      assert.notEqual(fresh.id, providerThreadId);
      assert.equal(fresh.nativeThreadRef, null);
      assert.equal(fresh.freshContext?.bootstrap, command.freshContext.bootstrap);
      refuse(after); // NO: repeated/stale renewal must not reset again.
    }).pipe(Effect.provide(layer)),
);
