import * as Duration from "effect/Duration";

/**
 * Every server-side pull request poll runs on this tick, so one tick's reads share one GraphQL
 * document per kind. 9.5 minutes leaves the tick's own reads inside the ten-minute badge limit.
 */
export const BACKGROUND_PULL_REQUEST_POLL_INTERVAL = Duration.seconds(570);

/** How old a read a caller nobody is watching may reuse. */
export const BACKGROUND_PULL_REQUEST_MAX_AGE = Duration.minutes(10);
