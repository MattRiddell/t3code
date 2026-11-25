import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";

/**
 * Every server-side pull request poll runs on this tick, so one tick's reads share one GraphQL
 * document per kind. 9.5 minutes leaves the tick's own reads inside the ten-minute badge limit.
 */
export const BACKGROUND_PULL_REQUEST_POLL_INTERVAL = Duration.seconds(570);

/** How old a read a caller nobody is watching may reuse. */
export const BACKGROUND_PULL_REQUEST_MAX_AGE = Duration.minutes(10);

const BACKGROUND_READ_WINDOW_MS = Duration.toMillis(BACKGROUND_PULL_REQUEST_POLL_INTERVAL);

/**
 * Whether a read made at `readAtMs` still serves a background reader at `nowMs`. All background reads
 * expire together at a tick boundary, so the next sweep refills them in one GraphQL document; a read
 * never crosses a boundary, so its age stays under one tick (inside BACKGROUND_PULL_REQUEST_MAX_AGE).
 */
export function isBackgroundReadFresh(readAtMs: number, nowMs: number): boolean {
  return (
    Math.floor(readAtMs / BACKGROUND_READ_WINDOW_MS) ===
    Math.floor(nowMs / BACKGROUND_READ_WINDOW_MS)
  );
}

/** The tick `nowMs` falls in; ticks start on wall-clock multiples of the interval. */
export function backgroundReadTick(nowMs: number): number {
  return Math.floor(nowMs / BACKGROUND_READ_WINDOW_MS);
}

/** Time from `nowMs` to the next tick boundary: a full tick when `nowMs` is on one. */
export function untilNextBackgroundTick(nowMs: number): Duration.Duration {
  const intoTick =
    ((nowMs % BACKGROUND_READ_WINDOW_MS) + BACKGROUND_READ_WINDOW_MS) % BACKGROUND_READ_WINDOW_MS;
  return Duration.millis(BACKGROUND_READ_WINDOW_MS - intoTick);
}

/**
 * Sleeps to the next tick boundary. Every server-side poll wakes there instead of a tick after its
 * own start, so their reads land in the same scheduled-poll document window as the boundary's refills.
 */
export const sleepUntilNextBackgroundTick: Effect.Effect<void> = Effect.flatMap(
  Clock.currentTimeMillis,
  (nowMs) => Effect.sleep(untilNextBackgroundTick(nowMs)),
);

/**
 * A thread with no run, message or wake for this long is idle. Its agent opens no pull request
 * meanwhile, so one opened outside a turn is the only thing a lookup for it can find.
 */
const IDLE_THREAD_AFTER = Duration.hours(2);

/** An idle thread's background lookup is shared across this many ticks (57 minutes). */
export const IDLE_BACKGROUND_READ_TICKS = 6;

const IDLE_BACKGROUND_READ_WINDOW_MS = BACKGROUND_READ_WINDOW_MS * IDLE_BACKGROUND_READ_TICKS;

/** The oldest read an idle background reader reuses; lookup caches hold successes this long. */
export const IDLE_BACKGROUND_READ_MAX_AGE = Duration.millis(IDLE_BACKGROUND_READ_WINDOW_MS);

/**
 * Like isBackgroundReadFresh, on windows of IDLE_BACKGROUND_READ_TICKS ticks. Idle reads expire
 * together at a window boundary, so the one sweep after it refills them in one document.
 */
export function isIdleBackgroundReadFresh(readAtMs: number, nowMs: number): boolean {
  return (
    Math.floor(readAtMs / IDLE_BACKGROUND_READ_WINDOW_MS) ===
    Math.floor(nowMs / IDLE_BACKGROUND_READ_WINDOW_MS)
  );
}

type ActivityAt = DateTime.Utc | null | undefined;

export interface ThreadActivity {
  readonly createdAt: DateTime.Utc;
  readonly latestUserMessageAt?: ActivityAt;
  readonly latestRunRequestedAt?: ActivityAt;
  readonly latestRunStartedAt?: ActivityAt;
  readonly latestRunCompletedAt?: ActivityAt;
  readonly lastSnoozeWakeAt?: ActivityAt;
}

/** Whether the thread's latest activity is older than IDLE_THREAD_AFTER at `nowMs`. */
export function isIdleThread(thread: ThreadActivity, nowMs: number): boolean {
  let latestMs = DateTime.toEpochMillis(thread.createdAt);
  for (const at of [
    thread.latestUserMessageAt,
    thread.latestRunRequestedAt,
    thread.latestRunStartedAt,
    thread.latestRunCompletedAt,
    thread.lastSnoozeWakeAt,
  ]) {
    if (at != null) latestMs = Math.max(latestMs, DateTime.toEpochMillis(at));
  }
  return nowMs - latestMs >= Duration.toMillis(IDLE_THREAD_AFTER);
}
