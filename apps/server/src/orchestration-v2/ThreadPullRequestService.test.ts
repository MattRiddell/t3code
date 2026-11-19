import {
  ProjectId,
  ProviderInstanceId,
  SourceControlProviderKind,
  ThreadId,
  type OrchestrationProjectShell,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import { BACKGROUND_PULL_REQUEST_POLL_INTERVAL } from "../git/backgroundPullRequestPoll.ts";
import * as GitManager from "../git/GitManager.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadPullRequestService from "./ThreadPullRequestService.ts";

describe("ThreadPullRequestServiceV2 project guard", () => {
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

  const project: OrchestrationProjectShell = {
    id: ProjectId.make("project-1"),
    title: "Project",
    workspaceRoot: "/workspace/project",
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-08-01T00:00:00.000Z",
    updatedAt: "2026-08-01T00:00:00.000Z",
  };

  const branchThread = (id: string, branch: string): OrchestrationV2ThreadShell => ({
    ...threadShell(id),
    branch,
  });

  interface LookupRecord {
    readonly branch: string;
    readonly refresh: boolean;
    readonly background: boolean;
    readonly idle?: true;
    readonly scheduled: boolean;
  }

  const runDiscovery = (options: {
    readonly threads: ReadonlyArray<OrchestrationV2ThreadShell>;
    readonly viewedCwd?: string;
    readonly clientConnected: boolean;
    readonly scenario: (harness: {
      readonly service: Effect.Success<typeof ThreadPullRequestService.make>;
      readonly lookups: Array<LookupRecord>;
      readonly offer: (event: OrchestrationV2DomainEvent) => Effect.Effect<void>;
      readonly settle: Effect.Effect<void>;
      readonly reachPoll: Effect.Effect<void>;
    }) => Effect.Effect<void>;
  }) =>
    Effect.gen(function* () {
      const activation = yield* Deferred.make<void>();
      const domainEvents = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
      const scopeChecks = yield* Queue.unbounded<void>();
      const startupSweep = yield* Deferred.make<void>();
      const pollLookup = yield* Queue.unbounded<void>();
      const lookups: Array<LookupRecord> = [];
      const dependencies = Layer.mergeAll(
        Layer.mock(Orchestrator.OrchestratorV2)({
          streamDomainEvents: Stream.fromQueue(domainEvents),
          getShellSnapshot: () =>
            Deferred.succeed(startupSweep, undefined).pipe(
              Effect.as({
                schemaVersion: 2 as const,
                snapshotSequence: 1,
                threads: [...options.threads],
                archivedThreads: [],
              }),
            ),
          getThreadEventSequence: () => Effect.succeed(1),
          getThreadShell: (threadId) =>
            Effect.succeed(options.threads.find((thread) => thread.id === threadId) ?? null),
        }),
        Layer.mock(ProjectStore.ProjectStoreV2)({
          listShells: () => Effect.succeed([project]),
          getShell: () => Effect.succeedSome(project),
        }),
        Layer.mock(GitManager.GitManager)({
          branchPullRequest: (input, lookupOptions) =>
            Effect.gen(function* () {
              lookups.push({
                branch: input.branch,
                refresh: lookupOptions?.refresh ?? false,
                background: lookupOptions?.background ?? false,
                ...(lookupOptions?.idle === true ? { idle: true as const } : {}),
                scheduled: yield* SourceControlRateLimit.ScheduledPoll,
              });
              yield* Queue.offer(pollLookup, undefined);
              return null;
            }),
        }),
        Layer.mock(PullRequestService.PullRequestService)({}),
        Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
          resolve: () =>
            Effect.succeed({
              canonicalKey: "github.com/acme/project",
              locator: {
                source: "git-remote" as const,
                remoteName: "origin",
                remoteUrl: "git@github.com:acme/project.git",
              },
              provider: SourceControlProviderKind.make("github"),
              owner: "acme",
              name: "project",
            }),
        }),
        Layer.mock(BackgroundPolicy.BackgroundPolicy)({
          shouldRunScopeWork: (scope) =>
            Queue.offer(scopeChecks, undefined).pipe(
              Effect.as(scope.type === "vcs-status" && scope.cwd === options.viewedCwd),
            ),
          shouldRunOpportunisticWork: Effect.succeed(options.clientConnected),
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
        yield* Deferred.await(startupSweep);
        yield* service.drain;
        yield* Queue.clear(pollLookup);
        lookups.length = 0;
        yield* options.scenario({
          service,
          lookups,
          offer: (event) => Queue.offer(domainEvents, event).pipe(Effect.asVoid),
          // The policy check is the first thing a thread's activity does.
          settle: Queue.take(scopeChecks).pipe(Effect.andThen(service.drain)),
          reachPoll: TestClock.adjust(BACKGROUND_PULL_REQUEST_POLL_INTERVAL).pipe(
            Effect.andThen(Queue.take(pollLookup)),
            Effect.andThen(service.drain),
          ),
        });
      }).pipe(Effect.provide(dependencies));
    }).pipe(Effect.scoped);

  const checkpointCaptured = (thread: OrchestrationV2ThreadShell) =>
    ({ type: "checkpoint.captured", threadId: thread.id }) as OrchestrationV2DomainEvent;

  it.effect(
    "holds a hidden thread's turn for the poll, and sweeps nothing without a client",
    () => {
      const thread = branchThread("idle-branch-thread", "feature/idle");
      return runDiscovery({
        threads: [thread],
        clientConnected: false,
        scenario: ({ lookups, offer, settle, reachPoll, service }) =>
          Effect.gen(function* () {
            yield* offer(checkpointCaptured(thread));
            yield* settle;
            expect(lookups).toEqual([]);

            // A finished turn still discovers the pull request for its branch, unwatched.
            yield* reachPoll;
            expect(lookups).toEqual([
              { branch: "feature/idle", refresh: true, background: false, scheduled: true },
            ]);

            // Polling active branches without a connected client wastes GitHub requests.
            yield* TestClock.adjust("20 minutes");
            yield* service.drain;
            expect(lookups).toHaveLength(1);
          }),
      });
    },
  );

  it.effect("refreshes a viewed thread's turn at once, outside the poll", () => {
    const thread = branchThread("viewed-thread", "feature/viewed");
    return runDiscovery({
      threads: [thread],
      viewedCwd: project.workspaceRoot,
      clientConnected: true,
      scenario: ({ lookups, offer, settle }) =>
        Effect.gen(function* () {
          yield* offer(checkpointCaptured(thread));
          yield* settle;
          expect(lookups).toEqual([
            { branch: "feature/viewed", refresh: true, background: false, scheduled: false },
          ]);
        }),
    });
  });

  it.effect("reads a branch once for every hidden thread waiting on it", () => {
    const first = branchThread("first", "feature/shared");
    const second = branchThread("second", "feature/shared");
    const swept = branchThread("swept", "feature/shared");
    const other = branchThread("other", "feature/other");
    return runDiscovery({
      threads: [first, second, swept, other],
      clientConnected: true,
      scenario: ({ lookups, offer, settle, reachPoll, service }) =>
        Effect.gen(function* () {
          yield* offer(checkpointCaptured(first));
          yield* settle;
          yield* offer(checkpointCaptured(second));
          yield* settle;
          expect(lookups).toEqual([]);

          yield* reachPoll;
          yield* service.drain;
          const byRead = (read: (lookup: LookupRecord) => boolean) =>
            lookups.filter(read).map((lookup) => lookup.branch);
          // The sweep and the owed refresh share the poll's window; one host read per branch.
          expect(byRead((lookup) => lookup.refresh)).toEqual(["feature/shared"]);
          expect(byRead((lookup) => lookup.scheduled).toSorted()).toEqual([
            "feature/other",
            "feature/shared",
          ]);
          // The rest of the shared branch reads what its reader cached, the thread left in
          // the sweep included: second by id, and the sweep's own pass for the whole branch.
          expect(byRead((lookup) => !lookup.refresh && !lookup.scheduled).toSorted()).toEqual([
            "feature/shared",
            "feature/shared",
          ]);
        }),
    });
  });

  // The client-lease sweep read every branch fresh each tick, apart from the settlement sweep's
  // background reads of the same branches.
  it.effect("sweeps on the background read, an idle thread's branch in the idle window", () => {
    const active = branchThread("active", "feature/active");
    const idle = {
      ...branchThread("idle", "feature/idle"),
      createdAt: DateTime.subtract(NOW, { hours: 3 }),
      updatedAt: DateTime.subtract(NOW, { hours: 3 }),
    };
    return TestClock.setTime(DateTime.toEpochMillis(NOW)).pipe(
      Effect.andThen(
        runDiscovery({
          threads: [active, idle],
          clientConnected: true,
          scenario: ({ lookups, reachPoll, service }) =>
            Effect.gen(function* () {
              yield* reachPoll;
              yield* service.drain;
              // MUST-SAY-NO: neither sweep read is a fresh foreground read, and the thread
              // created ten minutes ago is not idle.
              expect(lookups.toSorted((a, b) => a.branch.localeCompare(b.branch))).toEqual([
                { branch: "feature/active", refresh: false, background: true, scheduled: true },
                {
                  branch: "feature/idle",
                  refresh: false,
                  background: true,
                  idle: true,
                  scheduled: true,
                },
              ]);
            }),
        }),
      ),
    );
  });

  it.effect("lets a metadata event reuse a recent read, unlike a finished turn", () => {
    const thread = branchThread("renamed-thread", "feature/renamed");
    return runDiscovery({
      threads: [thread],
      viewedCwd: project.workspaceRoot,
      clientConnected: true,
      scenario: ({ lookups, offer, settle, service }) =>
        Effect.gen(function* () {
          yield* offer({
            type: "thread.metadata-updated",
            threadId: thread.id,
          } as OrchestrationV2DomainEvent);
          yield* offer(checkpointCaptured(thread));
          yield* settle;
          yield* service.drain;
          expect(lookups).toEqual([
            { branch: "feature/renamed", refresh: false, background: true, scheduled: false },
            { branch: "feature/renamed", refresh: true, background: false, scheduled: false },
          ]);
        }),
    });
  });

  it.effect("leaves delegated child threads to their own refresh instead of the sweep", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const parent = {
          ...threadShell("parent-thread"),
          branch: "feature/parent",
          worktreePath: "/workspace/parent",
        };
        const child: OrchestrationV2ThreadShell = {
          ...threadShell("child-thread"),
          branch: "t3/reply-ok",
          worktreePath: "/workspace/child",
          lineage: {
            rootThreadId: parent.id,
            parentThreadId: parent.id,
            relationshipToParent: "subagent",
          },
        };
        const project: OrchestrationProjectShell = {
          id: parent.projectId,
          title: "Project",
          workspaceRoot: "/workspace/project",
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-08-01T00:00:00.000Z",
          updatedAt: "2026-08-01T00:00:00.000Z",
        };
        const activation = yield* Deferred.make<void>();
        const domainEvents = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
        const sweeps = yield* Queue.unbounded<void>();
        const childLookup = yield* Deferred.make<void>();
        const lookups: Array<{ readonly branch: string; readonly refresh: boolean }> = [];
        const dependencies = Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromQueue(domainEvents),
            getShellSnapshot: () =>
              Queue.offer(sweeps, undefined).pipe(
                Effect.as({
                  schemaVersion: 2,
                  snapshotSequence: 1,
                  threads: [parent, child],
                  archivedThreads: [],
                }),
              ),
            getThreadEventSequence: () => Effect.succeed(1),
            getThreadShell: (threadId) =>
              Effect.succeed([parent, child].find((thread) => thread.id === threadId) ?? null),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            listShells: () => Effect.succeed([project]),
            getShell: () => Effect.succeedSome(project),
          }),
          Layer.mock(GitManager.GitManager)({
            branchPullRequest: (input, options) =>
              Effect.gen(function* () {
                const refresh = options?.refresh ?? false;
                lookups.push({ branch: input.branch, refresh });
                if (input.branch === child.branch && refresh) {
                  yield* Deferred.succeed(childLookup, undefined);
                }
                return null;
              }),
          }),
          Layer.mock(PullRequestService.PullRequestService)({}),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () =>
              Effect.succeed({
                canonicalKey: "github.com/acme/project",
                locator: {
                  source: "git-remote" as const,
                  remoteName: "origin",
                  remoteUrl: "git@github.com:acme/project.git",
                },
                provider: SourceControlProviderKind.make("github"),
                owner: "acme",
                name: "project",
              }),
          }),
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            shouldRunScopeWork: () => Effect.succeed(true),
            shouldRunOpportunisticWork: Effect.succeed(true),
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

          // Startup backfill sweep, then one periodic sweep.
          yield* Queue.take(sweeps);
          yield* service.drain;
          yield* TestClock.adjust("569 seconds");
          expect(yield* Queue.size(sweeps)).toBe(0);
          yield* TestClock.adjust("1 second");
          yield* Queue.take(sweeps);
          yield* service.drain;
          const sweepLookups = lookups.map((lookup) => lookup.branch).toSorted();

          // The child's own turn still discovers its pull request.
          yield* Queue.offer(domainEvents, {
            type: "checkpoint.captured",
            threadId: child.id,
          } as OrchestrationV2DomainEvent);
          yield* Deferred.await(childLookup);
          yield* service.drain;

          expect(sweepLookups).toEqual([parent.branch, parent.branch]);
          expect(lookups.slice(sweepLookups.length)).toEqual([
            { branch: child.branch, refresh: true },
          ]);
        }).pipe(Effect.provide(dependencies));
      }),
    ),
  );

  it.effect.each([
    { state: "closed", sameBranch: false },
    { state: "merged", sameBranch: false },
    { state: "closed", sameBranch: true },
    { state: "merged", sameBranch: true },
  ] as const)(
    "restores a $state automatic PR only for its own branch (sameBranch=$sameBranch)",
    ({ state, sameBranch }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const savedReference = {
            projectId: ProjectId.make("project-1"),
            repository: "acme/project",
            number: 42,
            url: "https://github.com/acme/project/pull/42",
          };
          const thread = {
            ...threadShell("branch-pr-thread"),
            branch: "main",
            branchPullRequest: savedReference,
            linkedPullRequest: savedReference,
          };
          const project: OrchestrationProjectShell = {
            id: thread.projectId,
            title: "Project",
            workspaceRoot: "/workspace/project",
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-08-01T00:00:00.000Z",
            updatedAt: "2026-08-01T00:00:00.000Z",
          };
          const activation = yield* Deferred.make<void>();
          const sweepStarted = yield* Deferred.make<void>();
          const commands: Array<OrchestrationV2ServerCommand> = [];
          const dependencies = Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.never,
              getShellSnapshot: () =>
                Deferred.succeed(sweepStarted, undefined).pipe(
                  Effect.as({
                    schemaVersion: 2,
                    snapshotSequence: 1,
                    threads: [thread],
                    archivedThreads: [],
                  }),
                ),
              dispatch: (command) =>
                Effect.sync(() => {
                  commands.push(command);
                  return { sequence: 2, storedEvents: [] };
                }),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([project]),
              getShell: () => Effect.succeedSome(project),
            }),
            Layer.mock(GitManager.GitManager)({
              branchPullRequest: () => Effect.succeed(null),
            }),
            Layer.mock(PullRequestService.PullRequestService)({
              summary: () =>
                Effect.succeed({
                  ...savedReference,
                  provider: SourceControlProviderKind.make("github"),
                  title: "Previous branch work",
                  state,
                  headBranch: sameBranch ? thread.branch : "feature/previous-work",
                  baseBranch: "release",
                  updatedAt: "2026-09-19T00:00:00.000Z",
                }),
            }),
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: () =>
                Effect.succeed({
                  canonicalKey: "github.com/acme/project",
                  locator: {
                    source: "git-remote" as const,
                    remoteName: "origin",
                    remoteUrl: "git@github.com:acme/project.git",
                  },
                  provider: SourceControlProviderKind.make("github"),
                  owner: "acme",
                  name: "project",
                }),
            }),
            Layer.mock(BackgroundPolicy.BackgroundPolicy)({}),
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
            yield* Deferred.await(sweepStarted);
            yield* service.drain;
          }).pipe(Effect.provide(dependencies));

          if (sameBranch) {
            expect(commands).toEqual([]);
          } else {
            expect(commands).toHaveLength(1);
            expect(commands[0]).toMatchObject({
              type: "thread.pull-request.sync",
              threadId: thread.id,
              branchPullRequest: null,
            });
            expect(commands[0]).not.toHaveProperty("linkedPullRequest");
          }
        }),
      ),
  );
});
