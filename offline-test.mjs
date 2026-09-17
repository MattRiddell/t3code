import * as NodeFS from "node:fs";
import * as NodeAssert from "node:assert/strict";
import * as NodeModule from "node:module";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import * as Option from "effect/Option";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Data from "effect/Data";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
const require = NodeModule.createRequire(import.meta.url);
const ts = require("/compiler/lib/typescript.js");
const root = "/workspace";
const read = (p) => NodeFS.readFileSync(`${root}/${p}`, "utf8");
const servicePath = "apps/server/src/provider/Layers/ProviderService.ts";
const reaperPath = "apps/server/src/provider/Layers/ProviderSessionReaper.ts";
const guardPath = "apps/server/src/provider/InactivityGuard.ts";
const service = read(servicePath);
function nodeFor(text, name) {
  const file = ts.createSourceFile("actual.ts", text, ts.ScriptTarget.Latest, true);
  const found = [];
  function walk(n) {
    if (ts.isVariableDeclaration(n) && n.name.getText(file) === name) found.push(n);
    ts.forEachChild(n, walk);
  }
  walk(file);
  NodeAssert.equal(found.length, 1, `unique actual declaration ${name}`);
  return [found[0], file];
}
function declaration(text, name) {
  const [n, f] = nodeFor(text, name);
  return `const ${n.getText(f)};`;
}
function js(text) {
  const r = ts.transpileModule(text, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
    },
    reportDiagnostics: true,
  });
  NodeAssert.equal(
    r.diagnostics?.filter((d) => d.category === ts.DiagnosticCategory.Error).length,
    0,
  );
  return r.outputText;
}
function evaluate(text, env, returns) {
  return new Function(...Object.keys(env), js(text) + `\nreturn (${returns});`)(
    ...Object.values(env),
  );
}
const guardText = read(guardPath)
  .replace("export function", "function")
  .replace("export interface", "interface");
const makeGuard = evaluate(guardText, {}, "makeInactivityGuard");
let count = 0;
async function test(name, fn) {
  await fn();
  count++;
  console.log(`PASS ${count} ${name}`);
}
const run = Effect.runPromise;
const source = (adapter) => ({
  instanceId: "i",
  provider: "opencode",
  adapter,
});
const ev = (type, payload = {}, extra = {}) => ({
  threadId: "t",
  provider: "opencode",
  type,
  payload,
  ...extra,
});
// Object.hasOwn is the discriminator between genuine absence and an invented
// identity: `{ id: undefined }` owns the key, an omitted projection does not.
const identityKeys = ["turnId", "itemId", "requestId"];
const assertIdentityOmitted = (observed) => {
  for (const key of identityKeys)
    NodeAssert.ok(
      Object.hasOwn(observed ?? {}, key) === false,
      `guard must receive omitted ${key}; received ${JSON.stringify(observed?.[key])}`,
    );
};
function fixture({
  fresh = true,
  shell = { session: { activeTurnId: null } },
  provider = "opencode",
  serviceText = service,
  guardFactory = makeGuard,
} = {}) {
  let time = 0,
    stops = 0,
    writes = 0,
    published = 0,
    routeHook = () => Effect.void,
    stopError = false;
  // Records the exact projected event each observe hands to the real guard so
  // controls can assert the absence-preserving shape of the production mapping.
  const guardObservations = [];
  const makeRecordingGuard = (...args) => {
    const guard = guardFactory(...args);
    return {
      observe: (threadId, instance, adapter, event) => {
        guardObservations.push(event);
        return guard.observe(threadId, instance, adapter, event);
      },
      retireAdapter: guard.retireAdapter.bind(guard),
      beginCommand: guard.beginCommand.bind(guard),
      endCommand: guard.endCommand.bind(guard),
      commitStop: guard.commitStop.bind(guard),
    };
  };
  const adapter = {
    provider,
    stopSession: () =>
      Effect.sync(() => {
        stops++;
        if (stopError) throw Error("fixture stop failure");
      }),
  };
  const projection = {
    getThreadShellById: () => Effect.succeed(shell === null ? Option.none() : Option.some(shell)),
  };
  const env = {
    Effect,
    Deferred,
    ProviderAdapterRequestError: class extends Data.TaggedError("ProviderAdapterRequestError") {},
    COMPACTION_COMPLETION_TIMEOUT: "10 minutes",
    McpSessionRegistry: { touchActiveMcpThread: () => Effect.void },
    ProviderInterruptTurnInput: {},
    ProviderRespondToRequestInput: {},
    providerTurnsTotal: {},
    Semaphore,
    Option,
    makeInactivityGuard: makeRecordingGuard,
    performance: { now: () => time },
    projectionQuery: Option.some(projection),
    correlateRuntimeEventWithInstance: (_, e) => e,
    increment: () => Effect.void,
    providerRuntimeEventsTotal: {},
    observeTurnStartedForAnalytics: () => Effect.void,
    observeModelReroutedForAnalytics: () => Effect.void,
    recordTurnCompletedAnalytics: () => Effect.void,
    clearTurnAnalyticsSession: () => Effect.void,
    isCompactedEvent: () => false,
    timedOutNativeCompactions: new Set(),
    pendingCompactions: new Map(),
    publishRuntimeEvent: () =>
      Effect.sync(() => {
        published++;
      }),
    ProviderStopSessionInput: {},
    decodeInputOrValidationError: ({ payload }) => Effect.succeed(payload),
    resolveRoutableSession: () =>
      Effect.gen(function* () {
        yield* routeHook();
        return { adapter, instanceId: "i", threadId: "t", isActive: true };
      }),
    settleCompaction: () => Effect.void,
    clearMcpSession: () => Effect.void,
    directory: {
      upsert: () =>
        Effect.sync(() => {
          writes++;
        }),
    },
    analytics: { record: () => Effect.void },
    withMetrics: () => (x) => x,
    providerSessionsTotal: {},
    providerMetricAttributes: () => ({}),
  };
  env.settleCompaction = evaluate(
    declaration(serviceText, "settleCompaction"),
    { Effect, Deferred, pendingCompactions: env.pendingCompactions },
    "settleCompaction",
  );
  const names = [
    "inactivityGuard",
    "commandPermits",
    "withThreadPermit",
    "protectCommand",
    "processRuntimeEvent",
    "compactThread",
    "interruptTurn",
    "respondToRequest",
    "stopSessionWithGuard",
    "stopSession",
    "stopSessionIfInactive",
  ];
  const f = evaluate(
    names.map((n) => declaration(serviceText, n)).join("\n"),
    env,
    "{inactivityGuard,protectCommand,processRuntimeEvent,stopSession,stopSessionIfInactive,commandPermits,compactThread,interruptTurn,respondToRequest}",
  );
  const admit = (event, src = source(adapter)) => run(f.processRuntimeEvent(src, event));
  const stop = () =>
    run(
      f.stopSessionIfInactive({
        threadId: "t",
        providerInstanceId: "i",
        inactivityThresholdMs: 100,
      }),
    );
  return {
    f,
    adapter,
    admit,
    stop,
    projection,
    env,
    observedCount: () => guardObservations.length,
    observedAt: (index) => guardObservations[index],
    init: () =>
      fresh ? admit(ev("session.started", { activityHistory: "empty" })) : Promise.resolve(),
    time: (n) => {
      time = n;
    },
    route: (fn) => {
      routeHook = fn;
    },
    failStop: () => {
      stopError = true;
    },
    counts: () => ({ stops, writes, published }),
  };
}
await test("namespace excludes live HOME, network and host PID tree", () => {
  NodeAssert.equal(process.env.HOME, "/home/offline");
  NodeAssert.equal(NodeFS.existsSync("/home/matt"), false);
  NodeAssert.equal(NodeFS.existsSync("/home/offline/.t3"), false);
  for (const ns of ["net", "pid", "mnt"])
    NodeAssert.notEqual(
      NodeFS.readlinkSync(`/proc/self/ns/${ns}`),
      process.env[`HOST_${ns.toUpperCase()}_NS`],
    );
  NodeAssert.equal(NodeFS.existsSync("/run/user"), false);
  NodeAssert.equal(NodeFS.existsSync("/home/matt/.local/lib/t3-nightly"), false);
});
await test("actual service: explicit fresh truly idle stops once and records stopped", async () => {
  const f = fixture();
  await f.init();
  f.time(100);
  NodeAssert.equal(await f.stop(), true);
  NodeAssert.deepEqual(f.counts(), { stops: 1, writes: 1, published: 1 });
  NodeAssert.equal(await f.stop(), false);
});
await test("actual publisher explicitly distinguishes newly-created and resumed native session", async () => {
  const text = read("apps/server/src/provider/Layers/OpenCodeAdapter.ts");
  const file = ts.createSourceFile("adapter.ts", text, ts.ScriptTarget.Latest, true);
  let found = [];
  function walk(n) {
    if (
      ts.isExpressionStatement(n) &&
      n.getText(file).includes('message: "OpenCode session started"')
    )
      found.push(n);
    ts.forEachChild(n, walk);
  }
  walk(file);
  NodeAssert.equal(found.length, 1);
  for (const created of [true, false]) {
    let receipt;
    const publish = evaluate(
      `const publish=()=>Effect.gen(function*(){${found[0].getText(file)}});`,
      {
        Effect,
        started: { created, activityHistory: created ? "empty" : "unknown" },
        input: { threadId: "t" },
        buildEventBase: () => Effect.succeed({}),
        emit: (e) =>
          Effect.sync(() => {
            receipt = e;
          }),
      },
      "publish",
    );
    await run(publish());
    NodeAssert.equal(receipt.payload.activityHistory, created ? "empty" : "unknown");
  }
});
for (const [label, payload] of [
  ["legacy absent", {}],
  ["resumed", { activityHistory: "unknown" }],
  ["malformed false", { activityHistory: false }],
  ["malformed null", null],
])
  await test(`unknown history refuses: ${label}`, async () => {
    const f = fixture({ fresh: false });
    await f.admit(ev("session.started", payload));
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
    NodeAssert.equal(f.counts().stops, 0);
  });
await test("cold attachment without birth refuses", async () => {
  const f = fixture({ fresh: false });
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
});
await test("READY hides running tool beyond threshold; actual event caller prevents stop", async () => {
  const f = fixture();
  await f.init();
  await f.admit(ev("item.updated", { status: "inProgress" }, { itemId: "tool" }));
  await f.admit(ev("session.state.changed", { state: "ready" }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
  NodeAssert.equal(f.counts().stops, 0);
});
await test("turn completed cannot clear still-running ordinary tool", async () => {
  const f = fixture();
  await f.init();
  await f.admit(ev("turn.started", {}, { turnId: "turn" }));
  await f.admit(ev("item.started", {}, { itemId: "tool" }));
  await f.admit(ev("turn.completed", {}, { turnId: "turn" }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
});
await test("last completed tool starts full new idle grace", async () => {
  const f = fixture();
  await f.init();
  await f.admit(ev("item.started", {}, { itemId: "tool" }));
  f.time(1000);
  await f.admit(ev("item.completed", { status: "completed" }, { itemId: "tool" }));
  f.time(1099);
  NodeAssert.equal(await f.stop(), false);
  f.time(1100);
  NodeAssert.equal(await f.stop(), true);
});
await test("one of two tools finishing retains the other", async () => {
  const f = fixture();
  await f.init();
  for (const itemId of ["a", "b"]) await f.admit(ev("item.started", {}, { itemId }));
  await f.admit(ev("item.completed", {}, { itemId: "a" }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
});
await test("late update after terminal cannot resurrect finished item", async () => {
  const f = fixture();
  await f.init();
  await f.admit(ev("item.completed", {}, { itemId: "a" }));
  await f.admit(ev("item.updated", { status: "inProgress" }, { itemId: "a" }));
  f.time(100);
  NodeAssert.equal(await f.stop(), true);
});
for (const type of ["runtime.error", "session.exited", "task.started", "task.progress"])
  await test(`${type} is unknown/terminal, never idle evidence`, async () => {
    const f = fixture();
    await f.init();
    await f.admit(ev(type));
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
  });
for (const type of ["item.started", "turn.started", "request.opened"])
  await test(`missing identity on ${type} refuses`, async () => {
    const f = fixture();
    await f.init();
    await f.admit(ev(type));
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
  });
// Boundary for the amended observe mapping: the runtime event union can carry
// an EXPLICIT undefined identity (exactOptionalPropertyTypes rejected it at
// the old direct pass-through). The mapping must omit it so the guard sees
// genuine absence and tombstones, never an invented identity. The refusal
// verdict alone cannot discriminate — an invented identity also stays active
// and refuses — so each control additionally observes the projected event and
// asserts with Object.hasOwn that no identity key was fabricated.
for (const [type, idKey] of [
  ["item.started", "itemId"],
  ["turn.started", "turnId"],
  ["request.opened", "requestId"],
])
  await test(`explicit undefined identity on ${type} is omitted at the guard`, async () => {
    const f = fixture();
    await f.init();
    const before = f.observedCount();
    await f.admit(ev(type, {}, { [idKey]: undefined }));
    NodeAssert.equal(f.observedCount(), before + 1);
    assertIdentityOmitted(f.observedAt(before));
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
    NodeAssert.equal(f.counts().stops, 0);
  });
for (const [open, close] of [
  ["request.opened", "request.resolved"],
  ["user-input.requested", "user-input.resolved"],
])
  await test(`${open} stays active until matching resolution`, async () => {
    const f = fixture();
    await f.init();
    await f.admit(ev(open, {}, { requestId: "r" }));
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
    await f.admit(ev(close, {}, { requestId: "r" }));
    f.time(1100);
    NodeAssert.equal(await f.stop(), true);
  });
await test("content with old provider timestamp uses receipt time", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  await f.admit(ev("content.delta", {}, { createdAt: "1970-01-01T00:00:00Z" }));
  NodeAssert.equal(await f.stop(), false);
});
await test("metadata heartbeat alone does not invent productivity", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  await f.admit(ev("session.state.changed", { state: "ready" }));
  NodeAssert.equal(await f.stop(), true);
});
for (const time of [NaN, Infinity, -1])
  await test(`invalid monotonic time ${time} refuses`, async () => {
    const f = fixture();
    await f.init();
    f.time(time);
    NodeAssert.equal(await f.stop(), false);
  });
await test("new birth cannot erase active tools or uncertified previous lifetime", async () => {
  const f = fixture();
  await f.init();
  await f.admit(ev("item.started", {}, { itemId: "a" }));
  await f.admit(ev("session.started", { activityHistory: "empty" }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
});
await test("work before birth cannot be discarded", async () => {
  const f = fixture({ fresh: false });
  await f.admit(ev("item.started", {}, { itemId: "a" }));
  await f.admit(ev("session.started", { activityHistory: "empty" }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
});
await test("adapter or instance replacement cannot certify old work idle", async () => {
  for (const src of [
    { instanceId: "different", provider: "opencode", adapter: {} },
    { instanceId: "i", provider: "opencode", adapter: {} },
  ]) {
    const f = fixture();
    await f.init();
    await f.admit(ev("session.started", { activityHistory: "empty" }), src);
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
  }
});
for (const [name, shell] of [
  ["missing", null],
  ["active", { session: { activeTurnId: "x" } }],
  ["background", { backgroundLiveness: {} }],
  ["approval", { hasPendingApprovals: true }],
  ["input", { hasPendingUserInput: true }],
])
  await test(`fresh projection ${name} vetoes before adapter stop`, async () => {
    const f = fixture({ shell });
    await f.init();
    f.time(1000);
    NodeAssert.equal(await f.stop(), false);
    NodeAssert.equal(f.counts().writes, 0);
  });
await test("projection read error refuses", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  f.projection.getThreadShellById = () => Effect.fail("private query error");
  NodeAssert.equal(await f.stop(), false);
});
await test("tool admitted during async route vetoes at actual final stop boundary", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  f.route(() =>
    f.f.processRuntimeEvent(source(f.adapter), ev("item.started", {}, { itemId: "race" })),
  );
  NodeAssert.equal(await f.stop(), false);
  NodeAssert.equal(f.counts().stops, 0);
  NodeAssert.equal(f.counts().writes, 0);
});
await test("command permit serializes stop behind a pending real command body", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  const entered = Deferred.makeUnsafe(),
    finish = Deferred.makeUnsafe();
  await run(
    Effect.gen(function* () {
      const command = yield* f.f
        .protectCommand(
          "t",
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(finish);
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const stop = yield* f.f
        .stopSessionIfInactive({
          threadId: "t",
          providerInstanceId: "i",
          inactivityThresholdMs: 100,
        })
        .pipe(Effect.forkChild);
      NodeAssert.equal(f.counts().stops, 0);
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(command);
      NodeAssert.equal(yield* Fiber.join(stop), false);
    }),
  );
});
await test("stop failure does not publish stopped or blindly retry", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  f.failStop();
  await NodeAssert.rejects(f.stop());
  NodeAssert.equal(f.counts().writes, 0);
  NodeAssert.equal(await f.stop(), false);
  NodeAssert.equal(f.counts().stops, 1);
});
await test("explicit stop remains available independently of idle proof", async () => {
  const f = fixture({ fresh: false });
  await run(f.f.stopSession({ threadId: "t" }));
  NodeAssert.equal(f.counts().stops, 1);
});
async function sweep({
  baseline = false,
  provider = "opencode",
  stopped = false,
  invalid = false,
  active = false,
  freshActivity = true,
} = {}) {
  let stoppedCalls = 0,
    guardedCalls = 0;
  const f = fixture();
  await f.init();
  if (freshActivity) await f.admit(ev("item.started", {}, { itemId: "live" }));
  f.time(1000);
  const text = baseline
    ? NodeFS.readFileSync("/baseline/src/provider/Layers/ProviderSessionReaper.ts", "utf8")
    : read(reaperPath);
  const effect = evaluate(
    declaration(text, "sweep"),
    {
      Effect,
      Option,
      Clock: { currentTimeMillis: Effect.succeed(1000) },
      inactivityThresholdMs: 100,
      directory: {
        listBindings: () =>
          Effect.succeed([
            {
              threadId: "t",
              provider,
              providerInstanceId: "i",
              status: stopped ? "stopped" : "ready",
              lastSeenAt: invalid ? "bad" : new Date(0).toISOString(),
            },
          ]),
      },
      projectionSnapshotQuery: {
        getThreadShellById: () =>
          Effect.succeed(Option.some({ session: { activeTurnId: active ? "x" : null } })),
      },
      providerService: {
        stopSession: () =>
          Effect.sync(() => {
            stoppedCalls++;
          }),
        stopSessionIfInactive: (i) => {
          guardedCalls++;
          return f.f.stopSessionIfInactive(i);
        },
      },
    },
    "sweep",
  );
  await run(effect);
  return { stoppedCalls, guardedCalls, actualStops: f.counts().stops };
}
await test("installed actual reaper reproduces active-tool abort; candidate discriminates", async () => {
  NodeAssert.equal((await sweep({ baseline: true })).stoppedCalls, 1);
  NodeAssert.equal((await sweep()).actualStops, 0);
});
await test("candidate actual reaper still reaps proven fresh idle OpenCode", async () => {
  NodeAssert.equal((await sweep({ freshActivity: false })).actualStops, 1);
});
for (const provider of ["codex", "claude", "cursor", "grok", "antigravity"])
  await test(`other-provider installed reaper policy retained: ${provider}`, async () => {
    const x = await sweep({ provider });
    NodeAssert.equal(x.stoppedCalls, 1);
    NodeAssert.equal(x.guardedCalls, 0);
  });
for (const scenario of [{ stopped: true }, { invalid: true }, { active: true }])
  await test(`actual sweep skips ${JSON.stringify(scenario)}`, async () => {
    const x = await sweep(scenario);
    NodeAssert.equal(x.stoppedCalls + x.guardedCalls, 0);
  });
await test("overflow fails closed rather than evicting an active tool", async () => {
  const f = fixture();
  await f.init();
  for (let i = 0; i < 4100; i++) await f.admit(ev("item.completed", {}, { itemId: `${i}` }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), false);
});
await test("actual returned facade protects every mutating command entry", async () => {
  const file = ts.createSourceFile("actual.ts", service, ts.ScriptTarget.Latest, true);
  let found = [];
  function walk(n) {
    if (
      ts.isReturnStatement(n) &&
      n.expression &&
      n.expression.getText(file).includes("startSession: (threadId, input) => protectCommand")
    )
      found.push(n.expression);
    ts.forEachChild(n, walk);
  }
  walk(file);
  NodeAssert.equal(found.length, 1);
  const f = fixture();
  await f.init();
  f.time(1000);
  const calls = [];
  const names = [
    "startSession",
    "sendTurn",
    "compactThread",
    "interruptTurn",
    "respondToRequest",
    "respondToUserInput",
    "rollbackConversation",
    "uploadFeedback",
  ];
  const env = {
    protectCommand: f.f.protectCommand,
    stopSession: f.f.stopSession,
    stopSessionIfInactive: f.f.stopSessionIfInactive,
    listSessions: null,
    getCapabilities: null,
    getInstanceInfo: null,
    assertConversationRollbackSupported: null,
  };
  for (const name of names)
    env[name] = (...args) =>
      Effect.sync(() => {
        calls.push({ name, args });
      });
  const facade = evaluate(`const facade=${found[0].getText(file)};`, env, "facade");
  for (const name of names) {
    f.time(1000 + calls.length * 1000);
    if (name === "startSession") await run(facade[name]("t", { model: "fixture" }));
    else if (name === "compactThread") await run(facade[name]("t", undefined, "request"));
    else await run(facade[name]({ threadId: "t" }));
    NodeAssert.equal(await f.stop(), false);
  }
  NodeAssert.deepEqual(
    calls.map((x) => x.name),
    names,
  );
});
await test("must-fail mutant: removing actual event-caller observation recreates abort", async () => {
  // Anchored to the amended observe block (cycle228): the guard now receives
  // an absence-preserving identity projection of the canonical event.
  const anchor = 'if (source.provider === "opencode") {';
  NodeAssert.equal(service.split(anchor).length, 2);
  const mutant = service.replace(
    anchor,
    'if (source.provider === "opencode" && canonicalEvent.type === "session.started") {',
  );
  const f = fixture({ serviceText: mutant });
  await f.init();
  await f.admit(ev("item.started", {}, { itemId: "live" }));
  f.time(1000);
  NodeAssert.equal(await f.stop(), true, "mutant must violate running-tool veto");
});
await test("must-fail mutant: absent marker accepted recreates unknown-history stop", async () => {
  const anchor = 'payload.activityHistory !== "empty"';
  NodeAssert.equal(guardText.split(anchor).length, 2);
  const mutant = evaluate(
    guardText.replace(anchor, "payload.activityHistory !== undefined"),
    {},
    "makeInactivityGuard",
  );
  const f = fixture({ fresh: false, guardFactory: mutant });
  await f.admit(ev("session.started", {}));
  f.time(1000);
  NodeAssert.equal(await f.stop(), true);
});
await test("must-fail mutant: invented fallback identity defeats the omission discriminator", async () => {
  // Anchored to the exact absence-preserving spread block. The mutant fabricates
  // a fallback id wherever the canonical event carries explicit undefined, which
  // still refuses the automatic stop (the invented identity remains active) yet
  // invents evidence — only the Object.hasOwn omission assertions go red.
  const anchor = [
    "          ...(canonical.turnId !== undefined ? { turnId: canonical.turnId } : {}),",
    "          ...(canonical.itemId !== undefined ? { itemId: canonical.itemId } : {}),",
    "          ...(canonical.requestId !== undefined ? { requestId: canonical.requestId } : {}),",
  ].join("\n");
  NodeAssert.equal(service.split(anchor).length, 2);
  const mutant = service.replace(
    anchor,
    [
      '          ...(canonical.turnId !== undefined ? { turnId: canonical.turnId } : { turnId: "__invented_turn__" }),',
      '          ...(canonical.itemId !== undefined ? { itemId: canonical.itemId } : { itemId: "__invented_item__" }),',
      '          ...(canonical.requestId !== undefined ? { requestId: canonical.requestId } : { requestId: "__invented_request__" }),',
    ].join("\n"),
  );
  const f = fixture({ serviceText: mutant });
  await f.init();
  const before = f.observedCount();
  await f.admit(ev("item.started", {}, { itemId: undefined }));
  NodeAssert.equal(f.observedCount(), before + 1);
  const observed = f.observedAt(before);
  NodeAssert.equal(observed.itemId, "__invented_item__", "mutant must fabricate the identity");
  let red = false;
  try {
    assertIdentityOmitted(observed);
  } catch {
    red = true;
  }
  NodeAssert.equal(red, true, "explicit-undefined controls must fail on invented identities");
  f.time(1000);
  NodeAssert.equal(
    await f.stop(),
    false,
    "mutant still refuses the stop; only the omission discriminator catches it",
  );
});
await test("strict typecheck of real guard and syntax diagnostics for all six source files", () => {
  const program = ts.createProgram([`${root}/${guardPath}`], {
    strict: true,
    noEmit: true,
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
  });
  const diagnostics = ts.getPreEmitDiagnostics(program);
  NodeAssert.equal(
    diagnostics.length,
    0,
    ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCurrentDirectory: () => root,
      getCanonicalFileName: (x) => x,
      getNewLine: () => "\n",
    }),
  );
  const files = [
    guardPath,
    servicePath,
    reaperPath,
    "apps/server/src/provider/Services/ProviderService.ts",
    "apps/server/src/provider/Layers/OpenCodeAdapter.ts",
    "packages/contracts/src/providerRuntime.ts",
  ];
  for (const path of files) js(read(path));
  console.log("GUARD_STRICT_TYPES=PASS; SOURCE_SYNTAX_FILES=6; FULL_MODULE_TYPES=NOT_RUN");
});
await test("actual native create/fork/resume producer never certifies fork history empty", async () => {
  const text = read("apps/server/src/provider/Layers/OpenCodeAdapter.ts");
  for (const scenario of ["fresh", "resume", "fork", "not-found"]) {
    const adopted =
      scenario === "resume"
        ? { id: "old", directory: "requested" }
        : scenario === "fork"
          ? { id: "old", directory: "other" }
          : undefined;
    const calls = [];
    const client = {
      session: {
        get: () => {
          calls.push("get");
          return { data: adopted };
        },
        update: () => ({}),
        fork: () => {
          calls.push("fork");
          return { data: { id: "forked" } };
        },
        create: () => {
          calls.push("create");
          return { data: { id: "new" } };
        },
      },
    };
    const actual = evaluate(
      `const scenario=()=>Effect.gen(function*(){${declaration(
        text,
        "resolved",
      )}return resolved;});`,
      {
        Effect,
        resumeSessionId: scenario === "fresh" ? undefined : "old",
        runOpenCodeSdk: (_, fn) => Effect.sync(fn),
        client,
        sameDirectory: (a, b) => Effect.succeed(a === b),
        directory: "requested",
        input: { runtimeMode: "fixture" },
        buildOpenCodePermissionRules: () => [],
        isOpenCodeNotFound: () => false,
      },
      "scenario()",
    );
    const result = await run(actual);
    NodeAssert.equal(
      result.activityHistory,
      ["fresh", "not-found"].includes(scenario) ? "empty" : "unknown",
    );
    NodeAssert.equal(result.created, scenario !== "resume");
    NodeAssert.deepEqual(
      calls,
      scenario === "fresh"
        ? ["create"]
        : scenario === "resume"
          ? ["get"]
          : scenario === "fork"
            ? ["get", "fork"]
            : ["get", "create"],
    );
  }
});
await test("actual SSE connection-loss warning invalidates idle proof across reconnect", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  await f.admit(
    ev("runtime.warning", {
      message: "OpenCode connection lost. Reconnecting.",
    }),
  );
  await f.admit(ev("session.state.changed", { state: "ready" }));
  f.time(2000);
  NodeAssert.equal(await f.stop(), false);
  NodeAssert.equal(f.counts().stops, 0);
});
await test("actual pending compaction permits interrupt, approval and explicit stop without timeout", async () => {
  const f = fixture();
  await f.init();
  const entered = Deferred.makeUnsafe();
  let interrupts = 0,
    responses = 0;
  f.adapter.compaction = {
    type: "native",
    start: () => Deferred.succeed(entered, undefined).pipe(Effect.asVoid),
  };
  f.adapter.interruptTurn = () =>
    Effect.sync(() => {
      interrupts++;
    });
  f.adapter.respondToRequest = () =>
    Effect.sync(() => {
      responses++;
    });
  const file = ts.createSourceFile("service.ts", service, ts.ScriptTarget.Latest, true);
  let found = [];
  function walk(n) {
    if (
      ts.isReturnStatement(n) &&
      n.expression &&
      n.expression.getText(file).includes("startSession: (threadId, input) => protectCommand")
    )
      found.push(n.expression);
    ts.forEachChild(n, walk);
  }
  walk(file);
  NodeAssert.equal(found.length, 1);
  const facade = evaluate(
    `const facade=${found[0].getText(file)};`,
    {
      ...f.f,
      startSession: () => Effect.void,
      sendTurn: () => Effect.void,
      respondToUserInput: () => Effect.void,
      rollbackConversation: () => Effect.void,
      uploadFeedback: () => Effect.void,
      listSessions: null,
      getCapabilities: null,
      getInstanceInfo: null,
      assertConversationRollbackSupported: null,
    },
    "facade",
  );
  await run(
    Effect.gen(function* () {
      const compaction = yield* facade.compactThread("t").pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(entered);
      f.time(1000);
      NodeAssert.equal(
        yield* f.f.stopSessionIfInactive({
          threadId: "t",
          providerInstanceId: "i",
          inactivityThresholdMs: 100,
        }),
        false,
      );
      yield* facade.interruptTurn({ threadId: "t", turnId: "turn" });
      yield* facade.respondToRequest({
        threadId: "t",
        requestId: "approval",
        decision: "allow",
      });
      NodeAssert.equal(interrupts, 1);
      NodeAssert.equal(responses, 1);
      yield* facade.stopSession({ threadId: "t" });
      const result = yield* Fiber.join(compaction);
      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(f.env.pendingCompactions.size, 0);
      NodeAssert.equal(f.f.commandPermits.size, 0);
      NodeAssert.equal(f.counts().stops, 1);
    }).pipe(Effect.timeout("2 seconds")),
  ); // Failure bound only; success waits on actual Deferred receipts.
});
await test("unknown and committed histories release adapter/item sets into compact tombstones", () => {
  const maps = [];
  class RecordingMap extends Map {
    constructor() {
      super();
      maps.push(this);
    }
  }
  const factory = evaluate(guardText, { Map: RecordingMap }, "makeInactivityGuard");
  let now = 0;
  const g = factory(() => now);
  const adapter = {};
  g.observe("t", "i", adapter, ev("session.started", { activityHistory: "empty" }));
  g.observe("t", "i", adapter, ev("item.started", {}, { itemId: "a" }));
  g.observe("t", "i", adapter, ev("runtime.warning"));
  NodeAssert.equal(maps[0].get("t"), null);
  g.beginCommand("t");
  g.endCommand("t");
  NodeAssert.equal(maps[1].size, 0);
  g.observe("idle", "i", adapter, ev("session.started", { activityHistory: "empty" }));
  now = 100;
  NodeAssert.equal(g.commitStop("idle", "i", adapter, 100), true);
  NodeAssert.equal(maps[0].get("idle"), null);
  g.observe("t", "i", adapter, ev("session.started", { activityHistory: "empty" }));
  now = 1000;
  NodeAssert.equal(g.commitStop("t", "i", adapter, 100), false);
});
await test("permit holders and queued commands keep one lock, then release its map entry", async () => {
  const f = fixture();
  await f.init();
  f.time(1000);
  const entered = Deferred.makeUnsafe(),
    finish = Deferred.makeUnsafe();
  f.route(() =>
    Effect.gen(function* () {
      yield* Deferred.succeed(entered, undefined);
      yield* Deferred.await(finish);
    }),
  );
  await run(
    Effect.gen(function* () {
      const stop = yield* f.f
        .stopSessionIfInactive({
          threadId: "t",
          providerInstanceId: "i",
          inactivityThresholdMs: 100,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const a = yield* f.f.protectCommand("t", Effect.void).pipe(Effect.forkChild);
      const b = yield* f.f.protectCommand("t", Effect.void).pipe(Effect.forkChild);
      yield* Deferred.succeed(finish, undefined);
      NodeAssert.equal(yield* Fiber.join(stop), true);
      yield* Fiber.join(a);
      yield* Fiber.join(b);
      NodeAssert.equal(f.f.commandPermits.size, 0);
    }),
  );
});
await test("actual subscription reconciliation retires removed and replaced adapter histories", async () => {
  for (const replacement of [false, true]) {
    const f = fixture();
    await f.init();
    f.time(1000);
    const nextAdapter = { provider: "opencode", streamEvents: Stream.empty };
    const subscriptions = Ref.makeUnsafe(new Map([["i", f.adapter]]));
    const reconcile = evaluate(
      declaration(service, "reconcileInstanceSubscriptions"),
      {
        Effect,
        Ref,
        Stream,
        Option,
        subscribedAdapters: subscriptions,
        inactivityGuard: f.f.inactivityGuard,
        processRuntimeEvent: f.f.processRuntimeEvent,
        registry: {
          listInstances: () => Effect.succeed(replacement ? ["i"] : []),
          getByInstance: () => Effect.succeed(nextAdapter),
        },
      },
      "reconcileInstanceSubscriptions",
    );
    await run(Effect.scoped(reconcile));
    NodeAssert.equal(await f.stop(), false);
    // Late events from the retired subscription cannot create new idle evidence.
    await f.admit(ev("session.started", { activityHistory: "empty" }, { threadId: "late" }));
    f.time(2000);
    NodeAssert.equal(f.f.inactivityGuard.commitStop("late", "i", f.adapter, 100), false);
    NodeAssert.equal((await run(Ref.get(subscriptions))).size, replacement ? 1 : 0);
  }
});
await test("adapter retirement drops strong history and ignores late birth without a new tombstone", () => {
  const maps = [];
  class RecordingMap extends Map {
    constructor() {
      super();
      maps.push(this);
    }
  }
  const factory = evaluate(guardText, { Map: RecordingMap }, "makeInactivityGuard");
  const g = factory(() => 0),
    adapter = {};
  g.observe("old", "i", adapter, ev("session.started", { activityHistory: "empty" }));
  g.observe("old", "i", adapter, ev("item.started", {}, { itemId: "tool" }));
  g.retireAdapter(adapter);
  NodeAssert.equal(maps[0].get("old"), null);
  g.observe("late", "i", adapter, ev("session.started", { activityHistory: "empty" }));
  NodeAssert.equal(maps[0].has("late"), false);
});
console.log(`CONTROLS_PASS=${count}; actual function extraction, no live services instantiated`);
