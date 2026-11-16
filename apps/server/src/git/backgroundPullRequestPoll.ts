import * as Duration from "effect/Duration";

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
