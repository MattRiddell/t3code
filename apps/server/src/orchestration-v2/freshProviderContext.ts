import {
  ProviderThreadId,
  isOrchestrationV2WorkActive,
  type OrchestrationV2Command,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type { ProjectionRecords } from "./ProjectionStore.ts";

// Refuse before emitting anything. The command's timestamp is a compare-and-swap
// against the saved handoff's snapshot, including input queued since that snapshot.
export function prepareFreshProviderThread(
  records: ProjectionRecords<
    "providerThreads" | "runs" | "providerTurns" | "runtimeRequests" | "subagents" | "turnItems"
  >,
  command: Extract<OrchestrationV2Command, { type: "provider-session.renew" }>,
  now: DateTime.Utc,
): string | { previous: OrchestrationV2ProviderThread; fresh: OrchestrationV2ProviderThread } {
  const seed = command.freshContext;
  if (seed === undefined) return "Fresh context requires an explicit seed.";
  const previous = records.providerThreads.find((row) => row.id === seed.providerThreadId);
  if (
    previous === undefined ||
    previous.id !== records.thread.activeProviderThreadId ||
    previous.appThreadId !== command.threadId ||
    previous.ownerNodeId !== null ||
    previous.providerSessionId !== command.providerSessionId ||
    previous.nativeThreadRef === null ||
    previous.status === "archived" ||
    previous.status === "closed"
  ) {
    return "Fresh context requires the current root provider thread and native session.";
  }
  if (
    DateTime.toEpochMillis(records.thread.updatedAt) !==
    DateTime.toEpochMillis(seed.expectedUpdatedAt)
  ) {
    return "Thread changed since the handoff snapshot.";
  }
  if (
    DateTime.toEpochMillis(now) - DateTime.toEpochMillis(records.thread.updatedAt) <
    30 * 60_000
  ) {
    return "Thread has not been idle for 30 minutes.";
  }
  if (!seed.bootstrap.trim() || new TextEncoder().encode(seed.bootstrap).length >= 5_000) {
    return "Bootstrap must contain fewer than 5000 UTF-8 bytes.";
  }
  const latest = records.runs.reduce<(typeof records.runs)[number] | undefined>(
    (last, run) => (last === undefined || run.ordinal > last.ordinal ? run : last),
    undefined,
  );
  if (
    latest === undefined ||
    latest.status !== "completed" ||
    latest.completedAt === null ||
    latest.providerThreadId !== previous.id ||
    !Number.isSafeInteger(latest.ordinal + 1) ||
    records.runs.some(
      (run) =>
        !["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(run.status),
    ) ||
    records.providerTurns.some((turn) => turn.status === "pending" || turn.status === "running") ||
    records.runtimeRequests.some((request) => request.status === "pending") ||
    records.subagents.some((task) => isOrchestrationV2WorkActive(task.status)) ||
    records.turnItems.some((item) => isOrchestrationV2WorkActive(item.status)) ||
    records.providerThreads.some((row) => (row.pendingBackgroundTasks?.length ?? 0) > 0)
  ) {
    return "Thread has active, queued, waiting or uncompleted work.";
  }
  return {
    previous: { ...previous, status: "archived", updatedAt: now },
    fresh: {
      ...previous,
      id: ProviderThreadId.make(`fresh:${command.commandId}`),
      providerSessionId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "not_loaded",
      // Non-null distinguishes this deliberate renewal from a queued placeholder.
      firstRunOrdinal: latest.ordinal + 1,
      lastRunOrdinal: latest.ordinal + 1,
      handoffIds: [],
      forkedFrom: null,
      contextUsage: null,
      nativeMetadata: null,
      goal: null,
      pendingBackgroundTasks: [],
      freshContext: { bootstrap: seed.bootstrap, throughRunOrdinal: latest.ordinal },
      createdAt: now,
      updatedAt: now,
    },
  };
}
