/** Runtime evidence for automatic reaping; never changes UI or persisted liveness. */
export interface InactivityEvent {
  readonly type: string;
  readonly turnId?: string;
  readonly itemId?: string;
  readonly requestId?: string;
  readonly payload: unknown;
}

interface Evidence {
  readonly instance: string;
  readonly adapter: object;
  lastActivity: number;
  readonly turns: Set<string>;
  readonly items: Set<string>;
  readonly requests: Set<string>;
  readonly finishedItems: Set<string>;
}

const activityEvents = new Set([
  "session.started",
  "session.exited",
  "runtime.error",
  "runtime.warning",
  "turn.started",
  "turn.completed",
  "turn.aborted",
  "item.started",
  "item.updated",
  "item.completed",
  "request.opened",
  "request.resolved",
  "user-input.requested",
  "user-input.resolved",
  "content.delta",
  "task.started",
  "task.progress",
  "task.updated",
  "task.completed",
]);

/** Clock is process-monotonic receipt time, never a provider-supplied timestamp. */
export function makeInactivityGuard(now: () => number) {
  // null is a compact UNKNOWN tombstone. Never discard it and then mistake a
  // late birth for a new empty history; it retains no adapter or item identifiers.
  const evidence = new Map<string, Evidence | null>();
  const commands = new Map<string, number>();
  const retiredAdapters = new WeakSet<object>();
  const touch = (threadId: string, state: Evidence) => {
    const at = now();
    if (!Number.isFinite(at) || at < state.lastActivity) {
      evidence.set(threadId, null);
      return false;
    }
    state.lastActivity = at;
    return true;
  };
  return {
    observe(
      threadId: string,
      instance: string,
      adapter: object,
      event: InactivityEvent
    ) {
      if (retiredAdapters.has(adapter) || !activityEvents.has(event.type))
        return;
      const payload =
        typeof event.payload === "object" &&
        event.payload !== null &&
        !Array.isArray(event.payload)
          ? (event.payload as Readonly<Record<string, unknown>>)
          : {};
      let state = evidence.get(threadId);
      if (state === null) return;
      if (state === undefined) {
        if (
          event.type !== "session.started" ||
          payload.activityHistory !== "empty" ||
          !Number.isFinite(now())
        ) {
          evidence.set(threadId, null);
          return;
        }
        state = {
          instance,
          adapter,
          lastActivity: now(),
          turns: new Set(),
          items: new Set(),
          requests: new Set(),
          finishedItems: new Set(),
        };
        evidence.set(threadId, state);
        touch(threadId, state);
        return;
      }
      if (
        state.instance !== instance ||
        state.adapter !== adapter ||
        event.type === "session.started" ||
        event.type === "session.exited" ||
        event.type === "runtime.error" ||
        event.type === "runtime.warning" ||
        event.type.startsWith("task.")
      ) {
        // Lost transport history, resume/replacement and background-task snapshots
        // require qualified resynchronization. READY/reconnect is not that proof.
        evidence.set(threadId, null);
        return;
      }
      if (!touch(threadId, state)) return;
      if (event.type === "turn.started") {
        if (event.turnId) state.turns.add(event.turnId);
        else evidence.set(threadId, null);
      } else if (
        event.type === "turn.completed" ||
        event.type === "turn.aborted"
      ) {
        if (event.turnId) state.turns.delete(event.turnId);
        else evidence.set(threadId, null);
        // A projected turn ending does not prove that its tools have ended.
      } else if (
        event.type === "item.started" ||
        event.type === "item.updated" ||
        event.type === "item.completed"
      ) {
        if (!event.itemId) {
          evidence.set(threadId, null);
          return;
        }
        if (
          event.type === "item.completed" ||
          ["completed", "failed", "declined"].includes(
            typeof payload.status === "string" ? payload.status : ""
          )
        ) {
          state.items.delete(event.itemId);
          state.finishedItems.add(event.itemId);
        } else if (!state.finishedItems.has(event.itemId)) {
          if (event.type === "item.started" || payload.status === "inProgress")
            state.items.add(event.itemId);
          else evidence.set(threadId, null);
        }
      } else if (
        event.type === "request.opened" ||
        event.type === "user-input.requested"
      ) {
        if (event.requestId) state.requests.add(event.requestId);
        else evidence.set(threadId, null);
      } else if (
        event.type === "request.resolved" ||
        event.type === "user-input.resolved"
      ) {
        if (event.requestId) state.requests.delete(event.requestId);
        else evidence.set(threadId, null);
      }
      if (
        state.items.size +
          state.finishedItems.size +
          state.requests.size +
          state.turns.size >
        4096
      )
        evidence.set(threadId, null);
    },
    retireAdapter(adapter: object) {
      retiredAdapters.add(adapter);
      for (const [threadId, state] of evidence) {
        if (state?.adapter === adapter) evidence.set(threadId, null);
      }
    },
    beginCommand(threadId: string) {
      commands.set(threadId, (commands.get(threadId) ?? 0) + 1);
      const state = evidence.get(threadId);
      if (state) touch(threadId, state);
    },
    endCommand(threadId: string) {
      const remaining = (commands.get(threadId) ?? 0) - 1;
      if (remaining > 0) commands.set(threadId, remaining);
      else commands.delete(threadId);
      const state = evidence.get(threadId);
      if (state) touch(threadId, state);
    },
    /** Called synchronously at the automatic stop decision, after async routing. */
    commitStop(
      threadId: string,
      instance: string,
      adapter: object,
      threshold: number
    ): boolean {
      const state = evidence.get(threadId);
      const at = now();
      if (
        !state ||
        commands.has(threadId) ||
        state.instance !== instance ||
        state.adapter !== adapter ||
        !Number.isFinite(threshold) ||
        threshold <= 0 ||
        !Number.isFinite(at) ||
        !Number.isFinite(state.lastActivity) ||
        at < state.lastActivity ||
        at - state.lastActivity < threshold ||
        state.turns.size ||
        state.items.size ||
        state.requests.size
      )
        return false;
      // Stop failure remains UNKNOWN too. No automatic retry or strong historical references.
      evidence.set(threadId, null);
      return true;
    },
  };
}
