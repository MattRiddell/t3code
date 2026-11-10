import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
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

it.effect.each(["none", "background", "turn", "metadata"] as const)(
  "renews native context and preserves concurrent ingestion; refuses unsafe twins (%s)",
  (race) =>
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
      // The idle policy is the client's: a just-completed, unchanged snapshot is accepted.
      assert.equal(
        typeof prepareFreshProviderThread(before, command, now),
        "object",
        "server must not re-impose a thread-updatedAt idle window",
      );
      yield* TestClock.adjust("1 minute");
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
      // A request item its run or runtime request left "waiting" is not live work.
      const oldRun = before.runs.find((row) => row.id === RunId.make("old-run"))!;
      const requestId = RuntimeRequestId.make("old-approval");
      const approval = {
        id: TurnItemId.make("old-approval-item"),
        threadId,
        runId: oldRun.id,
        nodeId: null,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 17,
        status: "waiting",
        title: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "approval_request",
        requestId,
        requestKind: "command",
      } as unknown as OrchestrationV2TurnItem;
      const request = (status: "pending" | "cancelled") =>
        ({
          id: requestId,
          nodeId: NodeId.make("old-approval-node"),
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "command",
          status,
          responseCapability: { type: "not_resumable", reason: "The run was interrupted." },
          createdAt: now,
          resolvedAt: status === "pending" ? null : now,
        }) as unknown as (typeof before.runtimeRequests)[number];
      const withItems = (
        items: ReadonlyArray<OrchestrationV2TurnItem>,
        requests: typeof before.runtimeRequests = [request("cancelled")],
        runs: typeof before.runs = before.runs,
      ) => ({ ...before, turnItems: [...items], runtimeRequests: [...requests], runs: [...runs] });
      const accept = (records: typeof before) =>
        assert.equal(typeof prepareFreshProviderThread(records, command, now), "object");
      // The live shape: an earlier run ended with the item still "waiting", a later run completed.
      for (const status of [
        "completed",
        "failed",
        "cancelled",
        "interrupted",
        "rolled_back",
      ] as const) {
        const ended = [
          { ...oldRun, status },
          { ...oldRun, id: RunId.make("next-run"), ordinal: 2 },
        ];
        accept(withItems([approval], [request("cancelled")], ended));
        // Even with the request row missing, the ended run alone releases the item.
        accept(withItems([approval], [], ended));
      }
      accept(withItems([approval], [request("cancelled")]));
      accept(withItems([{ ...approval, type: "reasoning" } as OrchestrationV2TurnItem], []));
      // A resolved request releases its item even when the item names no run.
      accept(withItems([{ ...approval, runId: null }], [request("cancelled")]));
      // Must say NO: work that can still move.
      refuse(withItems([approval], [request("pending")]));
      refuse(withItems([{ ...approval, runId: null }], []));
      refuse(withItems([{ ...approval, runId: RunId.make("unseen-run") }], []));
      refuse(
        withItems(
          [
            {
              ...approval,
              type: "reasoning",
              runId: RunId.make("unseen-run"),
            } as OrchestrationV2TurnItem,
          ],
          [],
        ),
      );
      for (const type of ["link_request", "secret_request"] as const) {
        refuse(withItems([{ ...approval, type } as unknown as OrchestrationV2TurnItem], []));
      }
      for (const status of ["pending", "running", "waiting"] as const) {
        refuse(
          withItems(
            [
              approval,
              {
                ...approval,
                id: TurnItemId.make("live-item"),
                type: "reasoning",
                runId: RunId.make("unseen-run"),
                status,
              } as OrchestrationV2TurnItem,
            ],
            [request("cancelled")],
          ),
        );
      }
      if (race !== "none") {
        const sink = yield* EventSink.EventSinkV2;
        const commit = sink.commitCommand;
        const planned = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const previous = before.providerThreads.find((row) => row.id === providerThreadId)!;
        const ingested: OrchestrationV2DomainEvent =
          race === "turn"
            ? {
                id: EventId.make("concurrent-native-turn"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: now,
                payload: {
                  id: ProviderTurnId.make("concurrent-turn"),
                  providerThreadId,
                  nodeId: NodeId.make("concurrent-node"),
                  runAttemptId: null,
                  nativeTurnRef: {
                    driver: adapter.driver,
                    nativeId: "concurrent-native-turn",
                    strength: "strong",
                  },
                  ordinal: 1,
                  status: "running",
                  startedAt: later,
                  completedAt: null,
                },
              }
            : {
                id: EventId.make(`concurrent-${race}`),
                type: "provider-thread.updated",
                threadId,
                occurredAt: now,
                payload: {
                  ...previous,
                  // Adapter updates need not advance the row/thread timestamp.
                  ...(race === "background"
                    ? { pendingBackgroundTasks: [{ taskId: "concurrent-task", kind: "monitor" }] }
                    : { nativeMetadata: { title: "Retain this adapter update" } }),
                },
              };
        yield* Effect.acquireUseRelease(
          Effect.sync(() =>
            vi
              .spyOn(sink, "commitCommand")
              .mockImplementationOnce((input) =>
                Deferred.succeed(planned, undefined).pipe(
                  Effect.andThen(Deferred.await(resume)),
                  Effect.andThen(commit(input)),
                ),
              ),
          ),
          () =>
            Effect.gen(function* () {
              const renewal = yield* orchestrator
                .dispatch(command)
                .pipe(Effect.exit, Effect.forkChild);
              yield* Deferred.await(planned);
              // This is the transactional ingestion writer, without the command
              // lock. A delayed event may retain the old snapshot timestamp.
              yield* sink.write({ events: [ingested] });
              const concurrent = yield* store.getThreadProjection(threadId);
              yield* Deferred.succeed(resume, undefined);
              const result = yield* Fiber.join(renewal);
              const afterRace = yield* store.getThreadProjection(threadId);
              if (race === "metadata") {
                assert.isTrue(Exit.isSuccess(result));
                assert.deepEqual(
                  afterRace.providerThreads.find((row) => row.id === providerThreadId)
                    ?.nativeMetadata,
                  { title: "Retain this adapter update" },
                );
                assert.notEqual(afterRace.thread.activeProviderThreadId, providerThreadId);
              } else {
                assert.isTrue(Exit.isFailure(result), "NO: work arrived after renewal planning");
                assert.deepEqual(afterRace, concurrent, "NO: refusal must preserve ingestion");
                assert.isTrue(
                  Exit.isFailure(yield* orchestrator.dispatch(command).pipe(Effect.exit)),
                );
                assert.deepEqual(yield* store.getThreadProjection(threadId), concurrent);
              }
            }),
          (spy) =>
            Deferred.succeed(resume, undefined).pipe(
              Effect.andThen(Effect.sync(() => spy.mockRestore())),
            ),
        );
        return;
      }
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

      // Both reducers must keep a durable receipt when adapters/finalization
      // write snapshots captured before successful bootstrap delivery.
      let memory = after;
      const update = (payload: typeof fresh) =>
        Effect.gen(function* () {
          const event = {
            id: EventId.make(`receipt:${payload.nativeThreadRef?.nativeId}:${payload.updatedAt}`),
            type: "provider-thread.updated" as const,
            threadId,
            occurredAt: later,
            payload,
          };
          memory = ProjectionStore.applyToProjection(memory, event);
          yield* store.apply(event);
          const persisted = yield* store.getThreadProjection(threadId);
          assert.deepEqual(
            persisted.providerThreads.find((row) => row.id === fresh.id)?.freshContext,
            memory.providerThreads.find((row) => row.id === fresh.id)?.freshContext,
          );
          return persisted.providerThreads.find((row) => row.id === fresh.id)!;
        });
      const pending = {
        ...fresh,
        nativeThreadRef: {
          driver: adapter.driver,
          nativeId: "seed-native",
          strength: "strong" as const,
        },
      };
      yield* update({
        ...pending,
        freshContext: { ...fresh.freshContext!, acceptedNativeThreadId: "seed-native" },
      });
      for (const stale of [pending, { ...pending, freshContext: undefined }]) {
        const kept = yield* update(stale);
        assert.equal(kept.freshContext?.acceptedNativeThreadId, "seed-native");
      }
      const replacement = {
        ...pending,
        nativeThreadRef: { ...pending.nativeThreadRef, nativeId: "replacement" },
      };
      const unaccepted = yield* update(replacement);
      assert.notEqual(unaccepted.freshContext?.acceptedNativeThreadId, "replacement");
      yield* update({
        ...replacement,
        freshContext: { ...fresh.freshContext!, acceptedNativeThreadId: "replacement" },
      });
      const retained = yield* update(replacement);
      assert.equal(retained.freshContext?.acceptedNativeThreadId, "replacement");
      const renewed = yield* update({
        ...replacement,
        freshContext: { bootstrap: "New generation", throughRunOrdinal: 2 },
      });
      assert.equal(renewed.freshContext?.acceptedNativeThreadId, undefined);
    }).pipe(Effect.provide(layer)),
);
