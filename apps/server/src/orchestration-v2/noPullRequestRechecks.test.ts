import { describe, expect, it } from "@effect/vitest";

import { makeNoPullRequestRechecks } from "./noPullRequestRechecks.ts";

const NOW_MS = 1_000 * 60 * 60 * 24 * 365;
const MINUTE_MS = 60_000;
const active = (count: number) =>
  Array.from({ length: count }, (_, index) => ({
    key: `branch-${index}`,
    activityAtMs: NOW_MS - (index + 1) * MINUTE_MS,
  }));
const batch = (activeLimit?: number) => ({
  activityCutoffMs: NOW_MS - 120 * MINUTE_MS,
  limit: 25,
  ...(activeLimit === undefined ? {} : { activeLimit }),
});

describe("makeNoPullRequestRechecks", () => {
  it("reads up to the batch's active limit of due active branches in one sweep", () => {
    const rechecks = makeNoPullRequestRechecks();
    const selected = rechecks.select(NOW_MS, active(30), batch(25));
    expect([...selected]).toEqual(active(25).map(({ key }) => key));
  });

  it("MUST-SAY-NO: a sweep outside the batch reads no active branch", () => {
    const rechecks = makeNoPullRequestRechecks();
    expect(rechecks.select(NOW_MS, active(30), { ...batch(0), limit: 0 }).size).toBe(0);
    // Nothing was stamped, so the next batch still reads them all.
    expect(rechecks.select(NOW_MS + MINUTE_MS, active(3), batch(25)).size).toBe(3);
  });

  it("MUST-SAY-NO: without an active limit a sweep keeps the four-branch cap", () => {
    const rechecks = makeNoPullRequestRechecks();
    expect(rechecks.select(NOW_MS, active(30), batch()).size).toBe(4);
  });
});
