import {
  CommandId,
  type OrchestrationProjectShell,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadShell,
  type ThreadId,
  type ThreadLinkedPullRequest,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import {
  BACKGROUND_PULL_REQUEST_POLL_INTERVAL,
  isIdleThread,
} from "../git/backgroundPullRequestPoll.ts";
import * as GitManager from "../git/GitManager.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { forkParked } from "../serverActivation.ts";
import { sourceControlDefinitions } from "../sourceControl/sourceControlDefinitions.ts";
import * as Orchestrator from "./Orchestrator.ts";

class ThreadPullRequestServiceV2 extends Context.Service<
  ThreadPullRequestServiceV2,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly drain: Effect.Effect<void>;
  }
>()("t3/orchestration-v2/ThreadPullRequestService/ThreadPullRequestServiceV2") {}

function samePullRequest(
  left: ThreadLinkedPullRequest | null | undefined,
  right: ThreadLinkedPullRequest | null,
): boolean {
  if (left == null || right === null) return left == null && right === null;
  return (
    left.projectId === right.projectId &&
    left.repository.toLowerCase() === right.repository.toLowerCase() &&
    left.number === right.number &&
    left.url === right.url
  );
}

function pullRequestMatchesProject(
  pullRequest: GitManager.GitBranchPullRequest,
  project: OrchestrationProjectShell,
): boolean {
  return (
    pullRequest.repositoryKey !== null &&
    project.repositoryIdentity != null &&
    sourceControlDefinitions.canonicalRepositoryKey(pullRequest.repositoryKey) ===
      sourceControlDefinitions.canonicalRepositoryKey(project.repositoryIdentity.canonicalKey)
  );
}

const resolveProjectForPullRequestDiscovery = Effect.fn(
  "ThreadPullRequestServiceV2.resolveProject",
)(function* (
  project: OrchestrationProjectShell,
  repositoryIdentities: RepositoryIdentityResolver.RepositoryIdentityResolver["Service"],
  options?: { readonly refresh?: boolean },
) {
  // Post-turn discovery refreshes 15-minute identity caches because agents may add remotes.
  const repositoryIdentity = yield* repositoryIdentities.resolve(project.workspaceRoot, {
    refresh: options?.refresh ?? false,
  });
  return {
    project: { ...project, repositoryIdentity },
    repository: sourceControlDefinitions.repositorySelector(repositoryIdentity),
  };
});

export function projectWorkspaceMatchesSnapshot(
  currentProject: Option.Option<Pick<OrchestrationProjectShell, "workspaceRoot">>,
  expectedWorkspaceRoot: string,
): boolean {
  return (
    Option.isSome(currentProject) && currentProject.value.workspaceRoot === expectedWorkspaceRoot
  );
}

const BACKFILL_ATTEMPTS = 5;

interface RefreshRequest {
  readonly threadId: ThreadId | null;
  readonly refresh: boolean;
  readonly backfill?: boolean;
  /** Nobody is watching this read, so it may reuse one up to the background age limit. */
  readonly background?: boolean;
  /** Limits a sweep to branches the filter accepts. */
  readonly branchFilter?: (branchKey: string) => boolean;
}

type RefreshJob =
  | { readonly type: "refresh"; readonly request: RefreshRequest }
  | { readonly type: "activity"; readonly threadId: ThreadId }
  | { readonly type: "poll" };

const branchKeyOf = (
  thread: Pick<OrchestrationV2ThreadShell, "projectId" | "worktreePath" | "branch">,
) => JSON.stringify([thread.projectId, thread.worktreePath, thread.branch]);

export const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const git = yield* GitManager.GitManager;
  const pullRequests = yield* PullRequestService.PullRequestService;
  const repositoryIdentities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const backgroundPolicy = yield* BackgroundPolicy.BackgroundPolicy;
  const pendingBackfill = new Map<ThreadId, number>();

  const finishBackfill = (threads: ReadonlyArray<Pick<OrchestrationV2ThreadShell, "id">>) => {
    for (const thread of threads) pendingBackfill.delete(thread.id);
  };
  const failBackfill = (threads: ReadonlyArray<Pick<OrchestrationV2ThreadShell, "id">>) => {
    for (const thread of threads) {
      const remaining = pendingBackfill.get(thread.id);
      if (remaining === undefined) continue;
      if (remaining <= 1) pendingBackfill.delete(thread.id);
      else pendingBackfill.set(thread.id, remaining - 1);
    }
  };

  const readThreadSnapshot = ({ threadId, backfill }: RefreshRequest) =>
    threadId === null
      ? orchestrator
          .getShellSnapshot({
            location: "active",
            unsettledOnly: !(backfill || pendingBackfill.size > 0),
          })
          .pipe(NodeSqliteClient.readOnly)
      : Effect.gen(function* () {
          // Read sequence first so guarded sync rejects intervening changes.
          const snapshotSequence = yield* orchestrator.getThreadEventSequence(threadId);
          const thread = yield* orchestrator.getThreadShell(threadId);
          return { snapshotSequence, threads: thread === null ? [] : [thread] };
        });

  const synchronize = Effect.fn("ThreadPullRequestServiceV2.synchronize")(function* (
    request: RefreshRequest,
  ) {
    const [threadSnapshot, projectShells] = yield* Effect.all([
      readThreadSnapshot(request),
      projectStore.listShells(),
    ]);
    // Subagent children refresh after their own runs; sweeping them costs a lookup each per minute.
    const snapshotThreads =
      request.threadId === null
        ? threadSnapshot.threads.filter(
            (thread) => thread.lineage.relationshipToParent !== "subagent",
          )
        : threadSnapshot.threads;
    const projects = new Map(projectShells.map((project) => [project.id, project]));
    if (request.backfill) {
      for (const thread of snapshotThreads) {
        if (
          (thread.settledOverride === "settled" || thread.settledAt !== null) &&
          thread.branchPullRequest == null
        ) {
          pendingBackfill.set(thread.id, BACKFILL_ATTEMPTS);
        }
      }
    }
    const visibleThreadIds = new Set(
      snapshotThreads.filter((thread) => thread.branch !== null).map((thread) => thread.id),
    );
    const checkedIds = request.threadId === null ? [...pendingBackfill.keys()] : [request.threadId];
    for (const threadId of checkedIds) {
      if (!visibleThreadIds.has(threadId)) pendingBackfill.delete(threadId);
    }
    const threads = snapshotThreads.filter(
      (thread) =>
        thread.archivedAt === null &&
        ((thread.settledOverride !== "settled" && thread.settledAt === null) ||
          request.threadId !== null ||
          pendingBackfill.has(thread.id)) &&
        (thread.branch !== null || thread.branchPullRequest != null),
    );
    const groups = Map.groupBy(threads, branchKeyOf);

    yield* Effect.forEach(
      [...groups].flatMap(([key, group]) => (request.branchFilter?.(key) === false ? [] : [group])),
      (group) =>
        Effect.gen(function* () {
          const first = group[0]!;
          const project = projects.get(first.projectId);
          if (project === undefined) return finishBackfill(group);
          const { project: resolvedProject, repository } =
            yield* resolveProjectForPullRequestDiscovery(project, repositoryIdentities, {
              refresh: request.refresh,
            });
          if (first.branch !== null && repository === null) return finishBackfill(group);
          const worktreeExists =
            first.worktreePath !== null && (yield* fileSystem.exists(first.worktreePath));
          const cwd =
            worktreeExists && first.worktreePath !== null
              ? first.worktreePath
              : project.workspaceRoot;
          // A background read of a branch whose threads are all idle shares the idle window.
          const nowMs = yield* Clock.currentTimeMillis;
          const background =
            request.background !== true
              ? {}
              : group.every((thread) => isIdleThread(thread, nowMs))
                ? { background: true, idle: true }
                : { background: true };
          const detected =
            first.branch === null
              ? null
              : yield* git.branchPullRequest(
                  { cwd, branch: first.branch },
                  { refresh: request.refresh, ...background },
                );
          if (detected !== null && !pullRequestMatchesProject(detected, resolvedProject)) {
            return finishBackfill(group);
          }
          const detectedReference =
            detected !== null && repository !== null
              ? {
                  projectId: project.id,
                  repository,
                  number: detected.number,
                  url: detected.url,
                }
              : null;

          const plans = yield* Effect.forEach(group, (thread) =>
            Effect.gen(function* () {
              let branchPullRequest = detectedReference;
              if (
                branchPullRequest === null &&
                thread.branch !== null &&
                thread.worktreePath === null &&
                thread.branchPullRequest != null
              ) {
                const previous = yield* pullRequests.summary(thread.branchPullRequest, {
                  recoverTransientFailure: false,
                });
                if (
                  previous.headBranch === thread.branch &&
                  (previous.state === "merged" || previous.state === "closed")
                ) {
                  branchPullRequest = thread.branchPullRequest;
                }
              }

              let replacement: ThreadLinkedPullRequest | undefined;
              if (
                (thread.pullRequests ?? []).length === 0 &&
                thread.linkedPullRequest != null &&
                detected?.state === "open" &&
                detectedReference !== null &&
                !samePullRequest(thread.linkedPullRequest, detectedReference)
              ) {
                const linked = yield* pullRequests.summary(thread.linkedPullRequest, {
                  recoverTransientFailure: false,
                });
                if (linked.state === "merged" || linked.state === "closed") {
                  replacement = detectedReference;
                }
              }
              if (
                samePullRequest(thread.branchPullRequest, branchPullRequest) &&
                replacement === undefined
              ) {
                pendingBackfill.delete(thread.id);
                return null;
              }
              return { thread, branchPullRequest, replacement };
            }).pipe(
              Effect.catchCauseIf(
                (cause) => !Cause.hasInterruptsOnly(cause),
                (cause) =>
                  Effect.logWarning("thread pull request discovery failed", {
                    threadId: thread.id,
                    cause: Cause.pretty(cause),
                  }).pipe(
                    Effect.tap(() => Effect.sync(() => failBackfill([thread]))),
                    Effect.as(null),
                  ),
              ),
            ),
          );
          const updates = plans.filter((plan) => plan !== null);
          if (updates.length === 0) return;

          if (detected !== null && first.branch !== null) {
            const current = yield* git.branchPullRequest(
              { cwd, branch: first.branch },
              { freshGitState: true, ...background },
            );
            const currentIdentity = yield* repositoryIdentities.resolve(project.workspaceRoot, {
              refresh: true,
            });
            if (
              current === null ||
              current.number !== detected.number ||
              current.url !== detected.url ||
              current.state !== detected.state ||
              current.repositoryKey !== detected.repositoryKey ||
              !pullRequestMatchesProject(current, {
                ...project,
                repositoryIdentity: currentIdentity,
              })
            ) {
              return failBackfill(updates.map((update) => update.thread));
            }
          }

          yield* Effect.forEach(
            updates,
            ({ thread, branchPullRequest, replacement }) =>
              Effect.gen(function* () {
                // Discovery yields to network I/O; recheck the project at commit to reject deleted projects or changed checkouts.
                const currentProject = yield* projectStore.getShell(project.id);
                if (!projectWorkspaceMatchesSnapshot(currentProject, project.workspaceRoot)) {
                  return failBackfill([thread]);
                }
                const uuid = yield* crypto.randomUUIDv4();
                yield* orchestrator.dispatch({
                  type: "thread.pull-request.sync",
                  commandId: CommandId.make(`server:thread-pull-request:${thread.id}:${uuid}`),
                  threadId: thread.id,
                  projectId: project.id,
                  snapshotSequence: threadSnapshot.snapshotSequence,
                  expected: {
                    workspaceRoot: project.workspaceRoot,
                    branch: thread.branch,
                    worktreePath: thread.worktreePath,
                    linkedPullRequest: thread.linkedPullRequest ?? null,
                    branchPullRequest: thread.branchPullRequest ?? null,
                  },
                  branchPullRequest,
                  ...(replacement === undefined ? {} : { linkedPullRequest: replacement }),
                });
                pendingBackfill.delete(thread.id);
              }).pipe(
                Effect.catchCauseIf(
                  (cause) => !Cause.hasInterruptsOnly(cause),
                  (cause) =>
                    Effect.logWarning("thread pull request update failed", {
                      threadId: thread.id,
                      cause: Cause.pretty(cause),
                    }).pipe(Effect.tap(() => Effect.sync(() => failBackfill([thread])))),
                ),
              ),
            { discard: true },
          );
        }).pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterruptsOnly(cause),
            (cause) =>
              Effect.logWarning("thread branch pull request lookup failed", {
                threadIds: group.map((thread) => thread.id),
                cause: Cause.pretty(cause),
              }).pipe(Effect.tap(() => Effect.sync(() => failBackfill(group)))),
          ),
        ),
      // Batch concurrent branch lookups into one GitHub GraphQL document.
      { concurrency: 32, discard: true },
    );
  });

  const refresh = (request: RefreshRequest) =>
    synchronize(request).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause),
        (cause) =>
          Effect.logWarning("thread pull request refresh failed", {
            cause: Cause.pretty(cause),
          }),
      ),
    );

  // A thread nobody is viewing waits for the next poll, where its branch shares the poll's
  // document. Thread ids are grouped by branch: one read answers every thread on it.
  const owedRefreshes = new Map<string, Set<ThreadId>>();

  const refreshAfterActivity = Effect.fn("ThreadPullRequestServiceV2.refreshAfterActivity")(
    function* (threadId: ThreadId) {
      const placement = yield* Effect.gen(function* () {
        const thread = yield* orchestrator.getThreadShell(threadId);
        if (thread === null || thread.branch === null) return null;
        const project = yield* projectStore.getShell(thread.projectId);
        if (Option.isNone(project)) return null;
        return {
          branchKey: branchKeyOf(thread),
          cwd: thread.worktreePath ?? project.value.workspaceRoot,
        };
      }).pipe(Effect.orElseSucceed(() => null));
      if (
        placement === null ||
        (yield* backgroundPolicy.shouldRunScopeWork({ type: "vcs-status", cwd: placement.cwd }))
      ) {
        return yield* refresh({ threadId, refresh: true });
      }
      const owed = owedRefreshes.get(placement.branchKey) ?? new Set();
      owedRefreshes.set(placement.branchKey, owed.add(threadId));
    },
  );

  const poll = Effect.fn("ThreadPullRequestServiceV2.poll")(function* () {
    // The sweep only catches pull requests opened outside a turn, which
    // nobody sees without a client. Thread events still refresh headless work.
    const sweeping = yield* backgroundPolicy.shouldRunOpportunisticWork;
    const owed = [...owedRefreshes];
    owedRefreshes.clear();
    const owedBranches = new Set(owed.map(([branchKey]) => branchKey));
    const readers = owed.flatMap(([, threadIds]) => [...threadIds].slice(0, 1));
    const followers = owed.flatMap(([, threadIds]) => [...threadIds].slice(1));
    // Nobody asked for the sweep's reads, so they share the tick's background window with the
    // settlement sweep's lookups of the same branches instead of reading them again.
    const sweep = (branchFilter: (branchKey: string) => boolean) =>
      sweeping ? [refresh({ threadId: null, refresh: false, background: true, branchFilter })] : [];
    yield* Effect.all(
      [
        ...sweep((branchKey) => !owedBranches.has(branchKey)),
        ...readers.map((threadId) => refresh({ threadId, refresh: true })),
      ],
      // One worker job, so the sweep and every owed branch reach the document window together.
      { concurrency: "unbounded", discard: true },
    ).pipe(Effect.provideService(SourceControlRateLimit.ScheduledPoll, true));
    // Everything left on an owed branch reads the answer its reader just cached.
    yield* Effect.all(
      [
        ...sweep((branchKey) => owedBranches.has(branchKey)),
        ...followers.map((threadId) => refresh({ threadId, refresh: false })),
      ],
      { concurrency: 8, discard: true },
    );
  });

  const worker = yield* makeDrainableWorker((job: RefreshJob) => {
    switch (job.type) {
      case "refresh":
        return refresh(job.request);
      case "activity":
        return refreshAfterActivity(job.threadId).pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterruptsOnly(cause),
            (cause) =>
              Effect.logWarning("thread pull request activity refresh failed", {
                threadId: job.threadId,
                cause: Cause.pretty(cause),
              }),
          ),
        );
      case "poll":
        return poll();
    }
  });

  const enqueueActivity = (threadId: ThreadId) => worker.enqueue({ type: "activity", threadId });

  const processEvent = (event: OrchestrationV2DomainEvent) => {
    switch (event.type) {
      case "thread.created":
      case "thread.unarchived":
      case "thread.metadata-updated":
        return worker.enqueue({
          type: "refresh",
          request: { threadId: event.threadId, refresh: false, background: true },
        });
      case "thread.unsettled":
      case "checkpoint.captured":
        return enqueueActivity(event.threadId);
      case "run.updated":
        if (
          event.payload.status === "completed" ||
          event.payload.status === "failed" ||
          event.payload.status === "cancelled" ||
          event.payload.status === "interrupted"
        ) {
          return enqueueActivity(event.threadId);
        }
        break;
    }
    return Effect.void;
  };

  const start: ThreadPullRequestServiceV2["Service"]["start"] = Effect.fn(
    "ThreadPullRequestServiceV2.start",
  )(function* () {
    yield* forkParked(Stream.runForEach(orchestrator.streamDomainEvents, processEvent));
    yield* forkParked(
      Effect.gen(function* () {
        yield* worker.enqueue({
          type: "refresh",
          request: { threadId: null, refresh: false, backfill: true },
        });
        yield* worker.drain;
        yield* Effect.gen(function* () {
          yield* worker.enqueue({ type: "poll" });
          yield* worker.drain;
        }).pipe(
          Effect.repeat(Schedule.spaced(BACKGROUND_PULL_REQUEST_POLL_INTERVAL)),
          Effect.delay(BACKGROUND_PULL_REQUEST_POLL_INTERVAL),
        );
      }).pipe(Effect.asVoid),
    );
  });

  return { start, drain: worker.drain } satisfies ThreadPullRequestServiceV2["Service"];
});
