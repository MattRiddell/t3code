const RECHECK_MIN_MS = 30 * 60 * 1_000;
const RECHECK_MAX_MS = 24 * 60 * 60 * 1_000;
const RECHECK_IDLE_DIVISOR = 12;
const RECHECKS_PER_SWEEP = 4;

/**
 * Schedules a sweep's branch lookups, so their git work does not grow with the number of idle
 * threads. GitManager's caches cannot: they expire in minutes and evict past a few thousand branches.
 */
export function makeNoPullRequestRechecks() {
  const reads = new Map<string, { readonly readAtMs: number; readonly found: boolean }>();
  return {
    /**
     * The branches a scheduled sweep looks up now: every one that last had a pull request, and
     * the few longest unread that had none and are due. One idle a day is due every two hours.
     */
    select: (
      nowMs: number,
      branches: ReadonlyArray<{ readonly key: string; readonly activityAtMs: number }>,
      idleBatch?: {
        readonly activityCutoffMs: number;
        readonly limit: number;
        /** Due active branches this sweep may read; RECHECKS_PER_SWEEP when unset. */
        readonly activeLimit?: number;
      },
    ): ReadonlySet<string> => {
      const selected = new Set<string>();
      const due: Array<{ key: string; activityAtMs: number; readAtMs: number }> = [];
      for (const branch of branches) {
        const read = reads.get(branch.key);
        if (read?.found === true) {
          selected.add(branch.key);
          continue;
        }
        const delayMs = Math.min(
          Math.max((nowMs - branch.activityAtMs) / RECHECK_IDLE_DIVISOR, RECHECK_MIN_MS),
          RECHECK_MAX_MS,
        );
        if (read !== undefined && nowMs - read.readAtMs < delayMs) continue;
        due.push({ ...branch, readAtMs: read?.readAtMs ?? Number.NEGATIVE_INFINITY });
      }
      due.sort((left, right) =>
        left.readAtMs === right.readAtMs
          ? right.activityAtMs - left.activityAtMs
          : left.readAtMs - right.readAtMs,
      );
      // Stamped before the lookup, so one that keeps failing cannot hold a place in every sweep.
      const active = due
        .filter(
          ({ activityAtMs }) =>
            idleBatch === undefined || activityAtMs > idleBatch.activityCutoffMs,
        )
        .slice(0, idleBatch?.activeLimit ?? RECHECKS_PER_SWEEP);
      const idle =
        idleBatch === undefined || idleBatch.limit === 0
          ? []
          : due
              .filter(({ activityAtMs }) => activityAtMs <= idleBatch.activityCutoffMs)
              .slice(0, Math.max(0, idleBatch.limit - active.length));
      for (const { key } of [...active, ...idle]) {
        selected.add(key);
        reads.set(key, { readAtMs: nowMs, found: false });
      }
      return selected;
    },
    record: (key: string, nowMs: number, found: boolean): void => {
      reads.set(key, { readAtMs: nowMs, found });
    },
    retain: (keys: { readonly has: (key: string) => boolean }): void => {
      for (const key of reads.keys()) {
        if (!keys.has(key)) reads.delete(key);
      }
    },
  };
}
