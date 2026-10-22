import {
  EventId,
  GitManagerError,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationProjectShell,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as GitManager from "../git/GitManager.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadPullRequestService from "./ThreadPullRequestService.ts";

describe("ThreadPullRequestServiceV2 project guard", () => {
  it.effect("discovers a repository from a project shell without enrichment", () =>
    Effect.gen(function* () {
      const project: OrchestrationProjectShell = {
        id: ProjectId.make("project-1"),
        title: "Project",
        workspaceRoot: "/workspace/project",
        defaultModelSelection: null,
        scripts: [],
        repositoryIdentity: null,
        createdAt: "2026-08-01T00:00:00.000Z",
        updatedAt: "2026-08-01T00:00:00.000Z",
      };
      let resolvedRoot: string | null = null;
      const result = yield* ThreadPullRequestService.resolveProjectForPullRequestDiscovery(
        project,
        {
          resolve: (root) => {
            resolvedRoot = root;
            return Effect.succeed({
              canonicalKey: "github.com/pingdotgg/t3code",
              locator: {
                source: "git-remote" as const,
                remoteName: "origin",
                remoteUrl: "git@github.com:pingdotgg/t3code.git",
              },
              provider: "github" as const,
              displayName: "pingdotgg/t3code",
              owner: "pingdotgg",
              name: "t3code",
            });
          },
        },
      );
      expect(resolvedRoot).toBe("/workspace/project");
      expect(result.repository).toBe("pingdotgg/t3code");
      expect(result.project.repositoryIdentity?.canonicalKey).toBe("github.com/pingdotgg/t3code");
    }),
  );

  it.effect("refreshes the cached repository identity only when asked", () =>
    Effect.gen(function* () {
      const project: OrchestrationProjectShell = {
        id: ProjectId.make("project-1"),
        title: "Project",
        workspaceRoot: "/workspace/project",
        defaultModelSelection: null,
        scripts: [],
        repositoryIdentity: null,
        createdAt: "2026-08-01T00:00:00.000Z",
        updatedAt: "2026-08-01T00:00:00.000Z",
      };
      const refreshes: Array<boolean | undefined> = [];
      const resolver = {
        resolve: (_root: string, options?: { readonly refresh?: boolean }) => {
          refreshes.push(options?.refresh);
          return Effect.succeed(null);
        },
      };
      yield* ThreadPullRequestService.resolveProjectForPullRequestDiscovery(project, resolver);
      yield* ThreadPullRequestService.resolveProjectForPullRequestDiscovery(project, resolver, {
        refresh: true,
      });
      expect(refreshes).toEqual([false, true]);
    }),
  );

  it("rejects a pull-request result when the project root changes before dispatch", () => {
    const currentProject = Option.some({
      workspaceRoot: "/workspace/replaced",
    } satisfies Pick<OrchestrationProjectShell, "workspaceRoot">);

    expect(
      ThreadPullRequestService.projectWorkspaceMatchesSnapshot(
        currentProject,
        "/workspace/original",
      ),
    ).toBe(false);
  });

  it("rejects a pull-request result when the project was deleted before dispatch", () => {
    expect(
      ThreadPullRequestService.projectWorkspaceMatchesSnapshot(
        Option.none<Pick<OrchestrationProjectShell, "workspaceRoot">>(),
        "/workspace/original",
      ),
    ).toBe(false);
  });

  it("accepts a pull-request result while the project root is unchanged", () => {
    const currentProject = Option.some({
      workspaceRoot: "/workspace/original",
    } satisfies Pick<OrchestrationProjectShell, "workspaceRoot">);

    expect(
      ThreadPullRequestService.projectWorkspaceMatchesSnapshot(
        currentProject,
        "/workspace/original",
      ),
    ).toBe(true);
  });
});

describe("ThreadPullRequestServiceV2 reads", () => {
  const NOW = DateTime.makeUnsafe("2026-09-20T00:00:00.000Z");
  const threadShell = (id: string): OrchestrationV2ThreadShell => {
    const threadId = ThreadId.make(id);
    return {
      id: threadId,
      projectId: ProjectId.make("project-1"),
      title: id,
      providerInstanceId: ProviderInstanceId.make("codex"),
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdBy: "user",
      creationSource: "web",
      activeRunId: null,
      latestRunId: null,
      status: "idle",
      pendingRuntimeRequest: null,
      latestVisibleMessage: null,
      latestUserMessageAt: null,
      hasActionableProposedPlan: false,
      itemCount: 0,
      visibleItemCount: 0,
      createdAt: NOW,
      updatedAt: NOW,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
  };

  it.effect("an event for one thread reads that thread's shell, not every thread's", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = threadShell("updated-thread");
        const other = threadShell("other-thread");
        const activation = yield* Deferred.make<void>();
        const events = yield* PubSub.unbounded<OrchestrationV2DomainEvent>();
        const reads = yield* Queue.unbounded<
          ThreadId | { readonly location?: string; readonly unsettledOnly?: boolean }
        >();
        const layerDependencies = Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromPubSub(events),
            getShellSnapshot: (options) =>
              Queue.offer(reads, options ?? {}).pipe(
                Effect.as({
                  schemaVersion: 2,
                  snapshotSequence: 1,
                  threads: [thread, other],
                  archivedThreads: [],
                }),
              ),
            getThreadEventSequence: () => Effect.succeed(1),
            getThreadShell: (threadId) =>
              Queue.offer(reads, threadId).pipe(
                Effect.as([thread, other].find((candidate) => candidate.id === threadId) ?? null),
              ),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([]),
          }),
          Layer.mock(GitManager.GitManager)({}),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({}),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            Crypto.Crypto,
            Crypto.make({
              randomBytes: (size) => new Uint8Array(size).fill(1),
              digest: (_algorithm, data) => Effect.succeed(data),
            }),
          ),
          FileSystem.layerNoop({}),
        );

        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);
          expect(yield* Queue.take(reads)).toEqual({ location: "active", unsettledOnly: false });
          yield* service.drain;
          // Fork patch (board #15036): a finished run leaves its branch to the next pass.
          const runId = RunId.make("run:updated-thread:1");
          yield* PubSub.publish(events, {
            type: "run.updated",
            id: EventId.make("event:run-completed"),
            threadId: thread.id,
            runId,
            occurredAt: NOW,
            payload: {
              id: runId,
              threadId: thread.id,
              ordinal: 1,
              providerInstanceId: ProviderInstanceId.make("codex"),
              modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
              providerThreadId: null,
              userMessageId: MessageId.make("message:updated-thread:1"),
              rootNodeId: null,
              activeAttemptId: null,
              status: "completed",
              requestedAt: NOW,
              startedAt: NOW,
              completedAt: NOW,
              checkpointId: null,
              contextHandoffId: null,
            },
          });
          yield* Effect.yieldNow;
          yield* service.drain;
          expect(yield* Queue.size(reads)).toBe(0);
          // Must say no: a metadata update still syncs its thread at once.
          yield* PubSub.publish(events, {
            type: "thread.metadata-updated",
            id: EventId.make("event:metadata"),
            threadId: thread.id,
            occurredAt: NOW,
            payload: {
              createdBy: thread.createdBy,
              creationSource: thread.creationSource,
              id: thread.id,
              projectId: thread.projectId,
              title: thread.title,
              providerInstanceId: thread.providerInstanceId,
              modelSelection: thread.modelSelection,
              runtimeMode: thread.runtimeMode,
              interactionMode: thread.interactionMode,
              branch: thread.branch,
              worktreePath: thread.worktreePath,
              activeProviderThreadId: thread.activeProviderThreadId,
              lineage: thread.lineage,
              forkedFrom: null,
              createdAt: thread.createdAt,
              updatedAt: thread.updatedAt,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            },
          });
          expect(yield* Queue.take(reads)).toBe(thread.id);
          yield* service.drain;
          expect(yield* Queue.size(reads)).toBe(0);
        }).pipe(Effect.provide(layerDependencies));
      }),
    ),
  );

  it("periodic passes skip a branch whose pull request is known and open", () => {
    // Fork patch (board #15036): the pull request sync pass already reads that pull request.
    const link = (number: number, state: "open" | "merged" | "closed" | null) => ({
      host: "github.com",
      repository: "Owner/Repo",
      number,
      url: `https://github.com/Owner/Repo/pull/${number}`,
      source: "created" as const,
      linkedAt: "2026-09-20T00:00:00.000Z",
      snapshot:
        state === null
          ? null
          : {
              state,
              title: `Pull request ${number}`,
              headBranch: "feature/x",
              baseBranch: "main",
              isDraft: false,
              updatedAt: "2026-09-20T00:00:00.000Z",
              syncedAt: "2026-09-20T00:00:00.000Z",
            },
      stack: null,
    });
    const thread = (
      branchNumber: number | null,
      links: ReadonlyArray<ReturnType<typeof link>>,
    ): OrchestrationV2ThreadShell => ({
      ...threadShell("thread"),
      branch: "feature/x",
      branchPullRequest:
        branchNumber === null
          ? null
          : {
              projectId: ProjectId.make("project-1"),
              repository: "owner/repo",
              number: branchNumber,
              url: `https://github.com/Owner/Repo/pull/${branchNumber}`,
            },
      pullRequests: links,
    });
    const skips = ThreadPullRequestService.hasOpenBranchPullRequest;
    expect(skips(thread(7, [link(7, "open")]))).toBe(true);
    // Must say no: anything a new lookup could still change.
    expect(skips(thread(7, [link(7, "merged")]))).toBe(false);
    expect(skips(thread(7, [link(7, "closed")]))).toBe(false);
    expect(skips(thread(7, [link(7, null)]))).toBe(false);
    expect(skips(thread(7, [link(8, "open")]))).toBe(false);
    expect(skips(thread(null, [link(7, "open")]))).toBe(false);
    expect(skips(thread(7, []))).toBe(false);
  });

  it.effect("periodic passes look up only pushed branches; a thread sync still looks up", () =>
    // Fork patch (board #15036): a branch with no pushed head costs no GitHub read per pass.
    Effect.scoped(
      Effect.gen(function* () {
        const withBranch = (id: string, branch: string): OrchestrationV2ThreadShell => ({
          ...threadShell(id),
          branch,
        });
        const pushed = withBranch("pushed-thread", "feature/pushed");
        const local = withBranch("local-thread", "feature/local");
        const unknown = withBranch("unknown-thread", "feature/unknown");
        const project: OrchestrationProjectShell = {
          id: ProjectId.make("project-1"),
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          repositoryIdentity: null,
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        const activation = yield* Deferred.make<void>();
        const events = yield* PubSub.unbounded<OrchestrationV2DomainEvent>();
        const lookups: Array<string> = [];
        const pushedChecks: Array<string> = [];
        const passes = yield* Queue.unbounded<void>();
        const layerDependencies = Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromPubSub(events),
            getShellSnapshot: () =>
              Queue.offer(passes, undefined).pipe(
                Effect.as({
                  schemaVersion: 2,
                  snapshotSequence: 1,
                  threads: [pushed, local, unknown],
                  archivedThreads: [],
                }),
              ),
            getThreadEventSequence: () => Effect.succeed(1),
            getThreadShell: (threadId) =>
              Effect.succeed(
                [pushed, local, unknown].find((candidate) => candidate.id === threadId) ?? null,
              ),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
          }),
          Layer.mock(GitManager.GitManager)({
            branchHasPushedHead: ({ branch }) => {
              pushedChecks.push(branch);
              // Must say yes when the check fails: an unreadable checkout keeps its lookup.
              return branch === "feature/unknown"
                ? Effect.fail(
                    new GitManagerError({
                      operation: "test",
                      cwd: "/workspace/project",
                      detail: "unreadable",
                    }),
                  )
                : Effect.succeed(branch === "feature/pushed");
            },
            branchPullRequest: ({ branch }) => {
              lookups.push(branch);
              return Effect.succeed(null);
            },
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () =>
              Effect.succeed({
                canonicalKey: "github.com/owner/repo",
                locator: {
                  source: "git-remote" as const,
                  remoteName: "origin",
                  remoteUrl: "git@github.com:owner/repo.git",
                },
                provider: "github" as const,
                displayName: "owner/repo",
                owner: "owner",
                name: "repo",
              }),
          }),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            Crypto.Crypto,
            Crypto.make({
              randomBytes: (size) => new Uint8Array(size).fill(1),
              digest: (_algorithm, data) => Effect.succeed(data),
            }),
          ),
          FileSystem.layerNoop({}),
        );

        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);
          yield* Queue.take(passes);
          yield* service.drain;
          expect(pushedChecks.toSorted()).toEqual([
            "feature/local",
            "feature/pushed",
            "feature/unknown",
          ]);
          // Must say no: the pass does not look up the never-pushed branch.
          expect(lookups.toSorted()).toEqual(["feature/pushed", "feature/unknown"]);

          lookups.length = 0;
          pushedChecks.length = 0;
          yield* PubSub.publish(events, {
            type: "thread.metadata-updated",
            id: EventId.make("event:metadata"),
            threadId: local.id,
            occurredAt: NOW,
            payload: {
              createdBy: local.createdBy,
              creationSource: local.creationSource,
              id: local.id,
              projectId: local.projectId,
              title: local.title,
              providerInstanceId: local.providerInstanceId,
              modelSelection: local.modelSelection,
              runtimeMode: local.runtimeMode,
              interactionMode: local.interactionMode,
              branch: local.branch,
              worktreePath: local.worktreePath,
              activeProviderThreadId: local.activeProviderThreadId,
              lineage: local.lineage,
              forkedFrom: null,
              createdAt: local.createdAt,
              updatedAt: local.updatedAt,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            },
          });
          yield* Effect.yieldNow;
          yield* service.drain;
          expect(pushedChecks).toEqual([]);
          expect(lookups).toEqual(["feature/local"]);
        }).pipe(Effect.provide(layerDependencies));
      }),
    ),
  );

  it("periodic passes run one second after each ten-minute tick", () => {
    // Fork patch (board #15036): branch lookups expire on the tick, so the pass reads them together.
    const tick = Date.parse("2026-09-20T00:00:00.000Z");
    const delay = (nowMs: number) =>
      Duration.toMillis(ThreadPullRequestService.nextPeriodicPassDelay(nowMs));
    expect(delay(tick)).toBe(1_000);
    // Must say no: at or after this tick's slot, the next slot is a tick later.
    expect(delay(tick + 1_000)).toBe(10 * 60_000);
    expect(delay(tick + 4 * 60_000)).toBe(6 * 60_000 + 1_000);
  });

  it.effect("periodic sweeps read only unsettled threads once backfill is done", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const settled = {
          ...threadShell("settled-thread"),
          branch: "feature/settled",
          settledOverride: "settled" as const,
          settledAt: NOW,
        };
        const activation = yield* Deferred.make<void>();
        const reads = yield* Queue.unbounded<{
          readonly location?: string;
          readonly unsettledOnly?: boolean;
        }>();
        const layerDependencies = Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.never,
            getShellSnapshot: (options) =>
              Queue.offer(reads, options ?? {}).pipe(
                Effect.as({
                  schemaVersion: 2,
                  snapshotSequence: 1,
                  threads: options?.unsettledOnly ? [] : [settled],
                  archivedThreads: [],
                }),
              ),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([]),
          }),
          Layer.mock(GitManager.GitManager)({}),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({}),
          Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activation)),
          Layer.succeed(
            Crypto.Crypto,
            Crypto.make({
              randomBytes: (size) => new Uint8Array(size).fill(1),
              digest: (_algorithm, data) => Effect.succeed(data),
            }),
          ),
          FileSystem.layerNoop({}),
        );

        yield* Effect.gen(function* () {
          const service = yield* ThreadPullRequestService.make;
          yield* service.start();
          yield* Deferred.succeed(activation, undefined);
          expect(yield* Queue.take(reads)).toEqual({ location: "active", unsettledOnly: false });
          yield* service.drain;
          // Its project is gone, so backfill finishes it on the first pass.
          yield* TestClock.adjust("10 minutes");
          expect(yield* Queue.take(reads)).toEqual({ location: "active", unsettledOnly: true });
          yield* service.drain;
        }).pipe(Effect.provide(layerDependencies));
      }),
    ),
  );
});
