import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import {
  IDLE_BACKGROUND_READ_TICKS,
  isBackgroundReadFresh,
  isIdleBackgroundReadFresh,
  isIdleThread,
} from "./backgroundPullRequestPoll.ts";

const TICK_MS = 570_000;
const WINDOW_MS = TICK_MS * IDLE_BACKGROUND_READ_TICKS;
const WINDOW_START = WINDOW_MS * 500_000;
const HOUR_MS = 60 * 60_000;
const at = (ms: number) => DateTime.makeUnsafe(ms);

describe("isIdleBackgroundReadFresh", () => {
  it("serves an idle read across tick boundaries inside its window", () => {
    expect(isIdleBackgroundReadFresh(WINDOW_START, WINDOW_START + WINDOW_MS - 1)).toBe(true);
    // The tick reader expires the same read at the first boundary.
    expect(isBackgroundReadFresh(WINDOW_START, WINDOW_START + TICK_MS)).toBe(false);
    expect(isIdleBackgroundReadFresh(WINDOW_START, WINDOW_START + TICK_MS)).toBe(true);
  });

  it("MUST-SAY-NO: expires every idle read at the window boundary, however fresh", () => {
    expect(isIdleBackgroundReadFresh(WINDOW_START + WINDOW_MS - 1, WINDOW_START + WINDOW_MS)).toBe(
      false,
    );
  });
});

describe("isIdleThread", () => {
  const now = WINDOW_START;
  const created = at(now - 30 * HOUR_MS);

  it("calls a thread idle once its latest activity is two hours old", () => {
    expect(isIdleThread({ createdAt: created }, now)).toBe(true);
    expect(
      isIdleThread(
        {
          createdAt: created,
          latestUserMessageAt: at(now - 5 * HOUR_MS),
          latestRunRequestedAt: null,
          latestRunStartedAt: at(now - 3 * HOUR_MS),
          latestRunCompletedAt: at(now - 2 * HOUR_MS),
        },
        now,
      ),
    ).toBe(true);
  });

  it("MUST-SAY-NO: a run, message, request, wake or creation inside two hours is not idle", () => {
    const recent = at(now - 2 * HOUR_MS + 1);
    for (const field of [
      "latestUserMessageAt",
      "latestRunRequestedAt",
      "latestRunStartedAt",
      "latestRunCompletedAt",
      "lastSnoozeWakeAt",
    ] as const) {
      expect(isIdleThread({ createdAt: created, [field]: recent }, now), field).toBe(false);
    }
    expect(isIdleThread({ createdAt: recent }, now)).toBe(false);
  });
});
