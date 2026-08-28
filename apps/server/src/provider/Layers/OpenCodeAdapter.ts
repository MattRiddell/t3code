import {
  EventId,
  type OpenCodeSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  type ToolLifecycleItemType,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { OpencodeClient, Part, PermissionRequest, QuestionRequest } from "@opencode-ai/sdk/v2";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { type OpenCodeAdapterShape } from "../Services/OpenCodeAdapter.ts";
import {
  buildOpenCodePermissionRules,
  OpenCodeRuntime,
  OpenCodeRuntimeError,
  openCodeQuestionId,
  openCodeRuntimeErrorDetail,
  parseOpenCodeModelSlug,
  runOpenCodeSdk,
  toOpenCodeFileParts,
  toOpenCodePermissionReply,
  toOpenCodeQuestionAnswers,
  type OpenCodeServerConnection,
} from "../opencodeRuntime.ts";
import * as Option from "effect/Option";

const PROVIDER = ProviderDriverKind.make("opencode");

/**
 * Version tag stamped into the OpenCode resume cursor. Bump if the cursor
 * shape changes so stale-shaped cursors written by older builds are ignored
 * rather than misread (mirrors GROK_RESUME_VERSION / CURSOR_RESUME_VERSION).
 */
const OPENCODE_RESUME_VERSION = 1 as const;

/**
 * Decode a persisted resume cursor into the upstream `ses_…` id. Anything
 * that isn't a current-version cursor with a non-empty id means "no resume"
 * rather than an error. Re-adopting the session id IS the resume mechanism —
 * OpenCode scopes a conversation's history by session id.
 */
function parseOpenCodeResume(raw: unknown): { readonly sessionId: string } | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return undefined;
  }
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== OPENCODE_RESUME_VERSION) {
    return undefined;
  }
  if (typeof record.sessionId !== "string" || record.sessionId.trim().length === 0) {
    return undefined;
  }
  return { sessionId: record.sessionId.trim() };
}

/**
 * Whether an error definitively reports a missing session. Only a confirmed
 * miss may silently start a fresh session; any other failure (the SDK client
 * is `throwOnError: true`, so `session.get` rejects on every non-2xx) must
 * propagate, or a transient blip resets a live thread to an empty one — the
 * #3604 silent context loss. Decides on structured signals only, never free
 * text: a numeric 404 or the exact `NotFoundError` name, found via a bounded walk
 * over `cause`/`body`/`error`/`data`. An explicit non-404 status seals its
 * subtree so a wrapped "NotFound" name can't reclassify a real failure.
 * Exported for unit testing.
 */
export function isOpenCodeNotFound(cause: unknown): boolean {
  const seen = new Set<unknown>();
  const queue: Array<unknown> = [cause];
  for (let steps = 0; queue.length > 0 && steps < 32; steps += 1) {
    const node = queue.shift();
    if (node === null || typeof node !== "object" || seen.has(node)) {
      continue;
    }
    seen.add(node);
    const record = node as Record<string, unknown>;

    const response = record.response;
    const statuses = [
      record.status,
      record.statusCode,
      response !== null && typeof response === "object"
        ? (response as { readonly status?: unknown }).status
        : undefined,
    ].filter((status): status is number => typeof status === "number");
    if (statuses.includes(404)) {
      return true;
    }
    if (statuses.length > 0) {
      continue;
    }

    const name = record.name;
    if (typeof name === "string" && name.toLowerCase() === "notfounderror") {
      return true;
    }

    for (const key of ["cause", "body", "error", "data"] as const) {
      if (record[key] !== undefined) {
        queue.push(record[key]);
      }
    }
  }
  return false;
}

/**
 * Whether two directory spellings name the same location. Raw string
 * equality misreads a trailing slash, `.`/`..` segment, or symlinked cwd
 * (macOS `/tmp` → `/private/tmp`) as a cwd change, needlessly forking the
 * session on every resume. Lexically equal paths short-circuit; otherwise
 * both sides go through `realPath`, each falling back to its lexical form
 * on failure (deleted directory, external-server path) — so the probe can
 * only widen matches, never split them. Takes the services as arguments so
 * adapter methods stay service-free. Exported for unit testing.
 */
export function isSameOpenCodeDirectory(
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  left: string,
  right: string,
): Effect.Effect<boolean> {
  const lexicalLeft = path.resolve(left);
  const lexicalRight = path.resolve(right);
  if (lexicalLeft === lexicalRight) {
    return Effect.succeed(true);
  }
  const canonicalize = (lexical: string) =>
    fileSystem.realPath(lexical).pipe(Effect.orElseSucceed(() => lexical));
  return Effect.zipWith(
    canonicalize(lexicalLeft),
    canonicalize(lexicalRight),
    (canonicalLeft, canonicalRight) => canonicalLeft === canonicalRight,
  );
}

interface OpenCodeTurnSnapshot {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

type OpenCodeSubscribedEvent =
  Awaited<ReturnType<OpencodeClient["event"]["subscribe"]>> extends {
    readonly stream: AsyncIterable<infer TEvent>;
  }
    ? TEvent
    : never;

type OpenCodeSessionStatusEvent = Extract<
  OpenCodeSubscribedEvent,
  { readonly type: "session.status" }
>;

const OpenCodeSessionStatusMap = Schema.Record(
  Schema.String,
  Schema.Struct({ type: Schema.String }),
);
const decodeOpenCodeSessionStatusMap = Schema.decodeUnknownOption(OpenCodeSessionStatusMap);

interface OpenCodeCancellation {
  readonly turnId: TurnId;
  readonly completion: Deferred.Deferred<void, ProviderAdapterRequestError>;
  deferredIdleEvent?: OpenCodeSessionStatusEvent;
}

interface OpenCodeIdleReconciliation {
  readonly turnId: TurnId;
  readonly promptGeneration: number;
  raw: unknown;
  warned: boolean;
  dirty: boolean;
  fiber?: Fiber.Fiber<void, never>;
}

interface OpenCodePromptAdmission {
  readonly generation: number;
  readonly turnId: TurnId;
  readonly messageId: string;
  readonly priorAwaitingBusy: boolean;
  readonly priorIdle: { readonly turnId: TurnId; readonly raw: unknown } | undefined;
  idleDuringAdmission: { readonly turnId: TurnId; readonly raw: unknown } | undefined;
  idleObservedAfterMessage: boolean;
  messageObserved: boolean;
  accepted: boolean;
  readonly acceptance: Deferred.Deferred<void>;
  recoveryFiber?: Fiber.Fiber<void, never>;
  recoveryRaw: unknown;
}

type OpenCodeTerminalRequestEvent = Extract<
  OpenCodeSubscribedEvent,
  {
    readonly type: "permission.replied" | "question.replied" | "question.rejected";
  }
>;

type OpenCodeAskedRequestEvent = Extract<
  OpenCodeSubscribedEvent,
  { readonly type: "permission.asked" | "question.asked" }
>;

type OpenCodeRoutedRequestEvent = OpenCodeAskedRequestEvent | OpenCodeTerminalRequestEvent;

interface OpenCodeRequestRelationRetry {
  warned: boolean;
  fiber?: Fiber.Fiber<void, never>;
}

interface OpenCodePendingRequestRecovery {
  warned: boolean;
  rerun: boolean;
}

function trimText(value: string | undefined | null): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

function openCodeEventSessionId(event: OpenCodeSubscribedEvent): string | undefined {
  const properties = "properties" in event ? event.properties : undefined;
  if (!properties || typeof properties !== "object") {
    return undefined;
  }

  const sessionID = (properties as { readonly sessionID?: unknown }).sessionID;
  const sessionIDFromProperties = typeof sessionID === "string" ? sessionID : undefined;
  if (sessionIDFromProperties) {
    return sessionIDFromProperties;
  }

  const info = (properties as { readonly info?: { readonly id?: unknown } }).info;
  return info && typeof info.id === "string" ? info.id : undefined;
}

function openCodeEventSessionTitle(event: OpenCodeSubscribedEvent): string | undefined {
  if (event.type !== "session.updated") {
    return undefined;
  }

  const title = trimText(event.properties.info.title);
  // OpenCode mints a placeholder title at session.create when no title was
  // provided, and re-emits it on every `session.updated`. Mirroring it would
  // overwrite the thread's real title (openCodeEventSessionTitle feeds the
  // `thread.metadata.updated` mirror). Ignore OpenCode's auto-generated
  // placeholders so the thread isn't locked onto them.
  if (!title || isOpenCodeDefaultTitle(title)) {
    return undefined;
  }

  return title;
}

function isOpenCodeAbortError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "name" in error &&
    error.name === "MessageAbortedError"
  );
}

function isOpenCodeChildRequestEvent(event: OpenCodeSubscribedEvent): boolean {
  switch (event.type) {
    case "permission.asked":
    case "permission.replied":
    case "question.asked":
    case "question.replied":
    case "question.rejected":
      return true;
    default:
      return false;
  }
}

const OPENCODE_DEFAULT_TITLE_PATTERN =
  /^(New session - |Child session - )\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function isOpenCodeDefaultTitle(title: string): boolean {
  return OPENCODE_DEFAULT_TITLE_PATTERN.test(title);
}

interface OpenCodeSessionContext {
  session: ProviderSession;
  readonly client: OpencodeClient;
  readonly server: OpenCodeServerConnection;
  readonly directory: string;
  readonly openCodeSessionId: string;
  readonly relatedSessionIds: Set<string>;
  readonly resolvedRequestIds: Set<string>;
  readonly emittedTerminalRequestIds: Set<string>;
  readonly requestRelationRetries: Map<string, OpenCodeRequestRelationRetry>;
  readonly pendingPermissions: Map<string, PermissionRequest>;
  readonly pendingQuestions: Map<string, QuestionRequest>;
  readonly messageRoleById: Map<string, "user" | "assistant">;
  readonly partById: Map<string, Part>;
  readonly emittedTextByPartId: Map<string, string>;
  readonly completedAssistantPartIds: Set<string>;
  readonly turns: Array<OpenCodeTurnSnapshot>;
  activeTurnId: TurnId | undefined;
  activeAgent: string | undefined;
  activeVariant: string | undefined;
  cancellation: OpenCodeCancellation | undefined;
  interruptedTurnId: TurnId | undefined;
  reconcileIdleStatus: boolean;
  awaitingBusyAfterInterruption: boolean;
  pendingIdleReconciliation: OpenCodeIdleReconciliation | undefined;
  pendingRequestRecovery: OpenCodePendingRequestRecovery | undefined;
  promptGeneration: number;
  promptAdmission: OpenCodePromptAdmission | undefined;
  readonly firstConnection: Deferred.Deferred<void, ProviderAdapterRequestError>;
  /**
   * One-shot guard flipped by `stopOpenCodeContext` / `emitUnexpectedExit`.
   * The session lifecycle is owned by `sessionScope`; this Ref exists only
   * so concurrent callers can race the transition safely via `getAndSet`.
   */
  readonly stopped: Ref.Ref<boolean>;
  /**
   * Sole lifecycle handle for the session. Closing this scope:
   *   - aborts the `AbortController` registered as a finalizer
   *     (cancels the in-flight `event.subscribe` fetch),
   *   - interrupts the event-pump and server-exit fibers forked
   *     via `Effect.forkIn(sessionScope)`,
   *   - tears down the OpenCode server process for scope-owned servers.
   */
  readonly sessionScope: Scope.Closeable;
}

export interface OpenCodeAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

/**
 * Map a tagged OpenCodeRuntimeError produced by {@link runOpenCodeSdk} into
 * the adapter-boundary `ProviderAdapterRequestError`. SDK-method-level call
 * sites pipe through this in `Effect.mapError` so they never build the error
 * shape by hand.
 */
const toRequestError = (cause: OpenCodeRuntimeError): ProviderAdapterRequestError =>
  new ProviderAdapterRequestError({
    provider: PROVIDER,
    method: cause.operation,
    detail: cause.detail,
    cause: cause.cause,
  });

/**
 * Map a `Cause.squash`-ed failure into a `ProviderAdapterProcessError`. The
 * typed cause is usually an `OpenCodeRuntimeError` (from {@link runOpenCodeSdk}),
 * in which case we preserve its `detail`; otherwise we fall back to
 * {@link openCodeRuntimeErrorDetail} for unknown causes (defects, etc.).
 */
const toProcessError = (threadId: ThreadId, cause: unknown): ProviderAdapterProcessError =>
  new ProviderAdapterProcessError({
    provider: PROVIDER,
    threadId,
    detail: OpenCodeRuntimeError.is(cause) ? cause.detail : openCodeRuntimeErrorDetail(cause),
    cause,
  });

type EventBaseInput = {
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly createdAt?: string | undefined;
  readonly raw?: unknown;
};

function toToolLifecycleItemType(toolName: string): ToolLifecycleItemType {
  const normalized = toolName.toLowerCase();
  if (normalized.includes("bash") || normalized.includes("command")) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("patch") ||
    normalized.includes("multiedit")
  ) {
    return "file_change";
  }
  if (normalized.includes("web")) {
    return "web_search";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  if (
    normalized.includes("task") ||
    normalized.includes("agent") ||
    normalized.includes("subtask")
  ) {
    return "collab_agent_tool_call";
  }
  return "dynamic_tool_call";
}

function mapPermissionToRequestType(
  permission: string,
): "command_execution_approval" | "file_read_approval" | "file_change_approval" | "unknown" {
  switch (permission) {
    case "bash":
      return "command_execution_approval";
    case "read":
      return "file_read_approval";
    case "edit":
      return "file_change_approval";
    default:
      return "unknown";
  }
}

function mapPermissionDecision(reply: "once" | "always" | "reject"): string {
  switch (reply) {
    case "once":
      return "accept";
    case "always":
      return "acceptForSession";
    case "reject":
    default:
      return "decline";
  }
}

function resolveTurnSnapshot(
  context: OpenCodeSessionContext,
  turnId: TurnId,
): OpenCodeTurnSnapshot {
  const existing = context.turns.find((turn) => turn.id === turnId);
  if (existing) {
    return existing;
  }

  const created: OpenCodeTurnSnapshot = { id: turnId, items: [] };
  context.turns.push(created);
  return created;
}

function appendTurnItem(
  context: OpenCodeSessionContext,
  turnId: TurnId | undefined,
  item: unknown,
): void {
  if (!turnId) {
    return;
  }
  resolveTurnSnapshot(context, turnId).items.push(item);
}

const ensureSessionContext = Effect.fn("ensureSessionContext")(function* (
  sessions: ReadonlyMap<ThreadId, OpenCodeSessionContext>,
  threadId: ThreadId,
) {
  const session = sessions.get(threadId);
  if (!session) {
    return yield* new ProviderAdapterSessionNotFoundError({
      provider: PROVIDER,
      threadId,
    });
  }
  if (yield* Ref.get(session.stopped)) {
    return yield* new ProviderAdapterSessionClosedError({
      provider: PROVIDER,
      threadId,
    });
  }
  return session;
});

function normalizeQuestionRequest(request: QuestionRequest): ReadonlyArray<UserInputQuestion> {
  return request.questions.map((question, index) => ({
    id: openCodeQuestionId(index, question),
    header: question.header,
    question: question.question,
    options: question.options.map((option) => ({
      label: option.label,
      description: option.description,
    })),
    ...(question.multiple ? { multiSelect: true } : {}),
  }));
}

function resolveTextStreamKind(part: Part | undefined): "assistant_text" | "reasoning_text" {
  return part?.type === "reasoning" ? "reasoning_text" : "assistant_text";
}

function textFromPart(part: Part): string | undefined {
  switch (part.type) {
    case "text":
    case "reasoning":
      return part.text;
    default:
      return undefined;
  }
}

function commonPrefixLength(left: string, right: string): number {
  let index = 0;
  while (index < left.length && index < right.length && left[index] === right[index]) {
    index += 1;
  }
  return index;
}

function resolveLatestAssistantText(previousText: string | undefined, nextText: string): string {
  if (previousText && previousText.length > nextText.length && previousText.startsWith(nextText)) {
    return previousText;
  }
  return nextText;
}

export function mergeOpenCodeAssistantText(
  previousText: string | undefined,
  nextText: string,
): {
  readonly latestText: string;
  readonly deltaToEmit: string;
} {
  const latestText = resolveLatestAssistantText(previousText, nextText);
  return {
    latestText,
    deltaToEmit: latestText.slice(commonPrefixLength(previousText ?? "", latestText)),
  };
}

export function appendOpenCodeAssistantTextDelta(
  previousText: string,
  delta: string,
): {
  readonly nextText: string;
  readonly deltaToEmit: string;
} {
  return {
    nextText: previousText + delta,
    deltaToEmit: delta,
  };
}

const isoFromEpochMs = (value: number) =>
  DateTime.make(value).pipe(
    Option.match({
      onNone: () => undefined,
      onSome: DateTime.formatIso,
    }),
  );

function messageRoleForPart(
  context: OpenCodeSessionContext,
  part: Pick<Part, "messageID" | "type">,
): "assistant" | "user" | undefined {
  const known = context.messageRoleById.get(part.messageID);
  if (known) {
    return known;
  }
  return part.type === "tool" ? "assistant" : undefined;
}

function detailFromToolPart(part: Extract<Part, { type: "tool" }>): string | undefined {
  switch (part.state.status) {
    case "completed":
      return part.state.output;
    case "error":
      return part.state.error;
    case "running":
      return part.state.title;
    default:
      return undefined;
  }
}

function toolStateCreatedAt(part: Extract<Part, { type: "tool" }>): string | undefined {
  switch (part.state.status) {
    case "running":
      return isoFromEpochMs(part.state.time.start);
    case "completed":
    case "error":
      return isoFromEpochMs(part.state.time.end);
    default:
      return undefined;
  }
}

function sessionErrorMessage(error: unknown): string {
  if (!error || typeof error !== "object") {
    return "OpenCode session failed.";
  }
  const data = "data" in error && error.data && typeof error.data === "object" ? error.data : null;
  const message = data && "message" in data ? data.message : null;
  return typeof message === "string" && message.trim().length > 0
    ? message
    : "OpenCode session failed.";
}

function updateProviderSession(
  context: OpenCodeSessionContext,
  patch: Partial<ProviderSession>,
  options?: {
    readonly clearActiveTurnId?: boolean;
    readonly clearLastError?: boolean;
  },
): Effect.Effect<ProviderSession> {
  return Effect.gen(function* () {
    return applyProviderSessionUpdate(context, patch, options, yield* nowIso);
  });
}

function applyProviderSessionUpdate(
  context: OpenCodeSessionContext,
  patch: Partial<ProviderSession>,
  options:
    | {
        readonly clearActiveTurnId?: boolean;
        readonly clearLastError?: boolean;
      }
    | undefined,
  updatedAt: string,
): ProviderSession {
  const nextSession = {
    ...context.session,
    ...patch,
    updatedAt,
  } as ProviderSession & Record<string, unknown>;
  const mutableSession = nextSession as Record<string, unknown>;
  if (options?.clearActiveTurnId) {
    delete mutableSession.activeTurnId;
  }
  if (options?.clearLastError) {
    delete mutableSession.lastError;
  }
  context.session = nextSession;
  return nextSession;
}

const failPendingOpenCodeCancellation = Effect.fn("failPendingOpenCodeCancellation")(function* (
  context: OpenCodeSessionContext,
  detail: string,
) {
  const cancellation = context.cancellation;
  if (!cancellation) {
    return;
  }
  context.cancellation = undefined;
  yield* Deferred.fail(
    cancellation.completion,
    new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "session.abort",
      detail,
    }),
  ).pipe(Effect.ignore);
});

const abortOpenCodeSessionForTeardown = (context: OpenCodeSessionContext) =>
  runOpenCodeSdk("session.abort", () =>
    context.client.session.abort({ sessionID: context.openCodeSessionId }),
  ).pipe(Effect.timeout("1 second"), Effect.ignore({ log: true }));

const closeUnpublishedOpenCodeContext = Effect.fn("closeUnpublishedOpenCodeContext")(function* (
  context: OpenCodeSessionContext,
  abortRemote: boolean,
) {
  if (yield* Ref.getAndSet(context.stopped, true)) {
    return;
  }
  yield* Deferred.fail(
    context.firstConnection,
    new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "event.subscribe",
      detail: "OpenCode session startup ended before the event stream connected.",
    }),
  ).pipe(Effect.ignore);
  yield* failPendingOpenCodeCancellation(context, "OpenCode session startup was cancelled.");
  context.promptAdmission = undefined;
  if (abortRemote) {
    yield* abortOpenCodeSessionForTeardown(context);
  }
  yield* Scope.close(context.sessionScope, Exit.void).pipe(Effect.ignore);
});

const stopOpenCodeContext = Effect.fn("stopOpenCodeContext")(function* (
  context: OpenCodeSessionContext,
) {
  // Race-safe one-shot: first caller flips the flag, everyone else no-ops.
  if (yield* Ref.getAndSet(context.stopped, true)) {
    return false;
  }
  yield* Deferred.fail(
    context.firstConnection,
    new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "event.subscribe",
      detail: "OpenCode session stopped before the event stream connected.",
    }),
  ).pipe(Effect.ignore);
  yield* failPendingOpenCodeCancellation(context, "OpenCode session stopped during cancellation.");
  context.promptAdmission = undefined;

  // Best-effort remote abort. The scope close below tears down the local
  // handles (event-pump fiber, server-exit fiber, event-subscribe fetch),
  // but we still want to tell OpenCode that this session is done.
  yield* abortOpenCodeSessionForTeardown(context);

  // Closing the session scope interrupts every fiber forked into it and
  // runs each finalizer we registered — the `AbortController.abort()` call,
  // the child-process termination, etc.
  yield* Scope.close(context.sessionScope, Exit.void);
  return true;
});

export function makeOpenCodeAdapter(
  openCodeSettings: OpenCodeSettings,
  options?: OpenCodeAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("opencode");
    const serverConfig = yield* ServerConfig;
    const openCodeRuntime = yield* OpenCodeRuntime;
    const crypto = yield* Crypto.Crypto;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const sameDirectory = (left: string, right: string) =>
      isSameOpenCodeDirectory(fileSystem, path, left, right);
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
            stream: "native",
          })
        : undefined);
    // Only close loggers we created. If the caller passed one in via
    // `options.nativeEventLogger`, they own its lifecycle.
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, OpenCodeSessionContext>();
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate OpenCode runtime identifier.",
            cause,
          }),
      ),
    );
    let messageIdEpochMillis = -1;
    let messageIdCounter = 0;
    // T3 supplies the message ID to match prompt admission events. Keep OpenCode's sortable native shape so equal-time messages retain their upstream order.
    const makeOpenCodeMessageId = Effect.fn("makeOpenCodeMessageId")(function* () {
      const epochMillis = DateTime.toEpochMillis(yield* DateTime.now);
      if (epochMillis !== messageIdEpochMillis) {
        messageIdEpochMillis = epochMillis;
        messageIdCounter = 0;
      }
      messageIdCounter += 1;
      const encodedTime = BigInt.asUintN(
        48,
        BigInt(epochMillis) * 0x1000n + BigInt(messageIdCounter),
      )
        .toString(16)
        .padStart(12, "0");
      const randomBytes = yield* crypto.randomBytes(14).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "crypto/randomBytes",
              detail: "Failed to generate an OpenCode message identifier.",
              cause,
            }),
        ),
      );
      const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
      const random = Array.from(randomBytes, (byte) => alphabet[byte % alphabet.length]).join("");
      return `msg_${encodedTime}${random}`;
    });
    const buildEventBase = (input: EventBaseInput) =>
      Effect.all({
        eventId: randomUUIDv4.pipe(Effect.map(EventId.make)),
        createdAt: input.createdAt === undefined ? nowIso : Effect.succeed(input.createdAt),
      }).pipe(
        Effect.map(({ eventId, createdAt }) => ({
          eventId,
          provider: PROVIDER,
          threadId: input.threadId,
          createdAt,
          ...(input.turnId ? { turnId: input.turnId } : {}),
          ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
          ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
          ...(input.raw !== undefined
            ? {
                raw: {
                  source: "opencode.sdk.event" as const,
                  payload: input.raw,
                },
              }
            : {}),
        })),
      );

    // Layer-level finalizer: when the adapter layer shuts down, stop every
    // session. Each session's `Scope.close` tears down its spawned OpenCode
    // server (via the `ChildProcessSpawner` finalizer installed in
    // `startOpenCodeServerProcess`) and interrupts the forked event/exit
    // fibers. Consumers that can't reason about Effect scopes therefore
    // cannot leak OpenCode child processes by forgetting to call `stopAll`.
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        // `ignoreCause` swallows both typed failures (none here) and defects
        // from throwing scope finalizers so a sibling's death can't interrupt
        // the remaining cleanups.
        yield* Effect.forEach(
          contexts,
          (context) => Effect.ignoreCause(stopOpenCodeContext(context)),
          { concurrency: "unbounded", discard: true },
        );
        // Close the logger AFTER session teardown so any final lifecycle
        // events emitted during shutdown still get written. `close` flushes
        // the `Logger.batched` window and closes each per-thread
        // `RotatingFileSink` handle owned by the logger's internal scope.
        if (managedNativeEventLogger !== undefined) {
          yield* managedNativeEventLogger.close();
        }
      }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
    );

    const emit = (event: ProviderRuntimeEvent) =>
      Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);
    const writeNativeEvent = (
      threadId: ThreadId,
      event: {
        readonly observedAt: string;
        readonly event: Record<string, unknown>;
      },
    ) => (nativeEventLogger ? nativeEventLogger.write(event, threadId) : Effect.void);
    const writeNativeEventBestEffort = (
      threadId: ThreadId,
      event: {
        readonly observedAt: string;
        readonly event: Record<string, unknown>;
      },
    ) => writeNativeEvent(threadId, event).pipe(Effect.catchCause(() => Effect.void));

    const cancelIdleReconciliation = Effect.fn("cancelIdleReconciliation")(function* (
      context: OpenCodeSessionContext,
    ) {
      const pending = context.pendingIdleReconciliation;
      context.pendingIdleReconciliation = undefined;
      if (pending?.fiber) {
        yield* Fiber.interrupt(pending.fiber);
      }
    });

    const completeOpenCodeTurn = Effect.fn("completeOpenCodeTurn")(function* (
      context: OpenCodeSessionContext,
      turnId: TurnId,
      promptGeneration: number,
      raw: unknown,
    ) {
      const updatedAt = yield* nowIso;
      const stopped = yield* Ref.get(context.stopped);
      if (
        stopped ||
        context.activeTurnId !== turnId ||
        context.promptGeneration !== promptGeneration ||
        context.cancellation?.turnId === turnId
      ) {
        return;
      }
      const pendingIdleReconciliation = context.pendingIdleReconciliation;
      if (
        pendingIdleReconciliation?.turnId === turnId &&
        pendingIdleReconciliation.promptGeneration === promptGeneration
      ) {
        context.pendingIdleReconciliation = undefined;
      }
      context.activeTurnId = undefined;
      context.activeAgent = undefined;
      context.activeVariant = undefined;
      context.interruptedTurnId = undefined;
      context.awaitingBusyAfterInterruption = false;
      context.reconcileIdleStatus = false;
      applyProviderSessionUpdate(
        context,
        { status: "ready" },
        { clearActiveTurnId: true },
        updatedAt,
      );
      if (pendingIdleReconciliation?.fiber) {
        yield* Fiber.interrupt(pendingIdleReconciliation.fiber);
      }
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
          raw,
        })),
        type: "turn.completed",
        payload: {
          state: "completed",
        },
      });
    });

    const scheduleIdleReconciliation = Effect.fn("scheduleIdleReconciliation")(function* (
      context: OpenCodeSessionContext,
      turnId: TurnId,
      raw: unknown,
    ) {
      const existing = context.pendingIdleReconciliation;
      if (existing?.turnId === turnId && existing.promptGeneration === context.promptGeneration) {
        existing.raw = raw;
        existing.dirty = true;
        return;
      }
      yield* cancelIdleReconciliation(context);

      const pending: OpenCodeIdleReconciliation = {
        turnId,
        promptGeneration: context.promptGeneration,
        raw,
        warned: false,
        dirty: false,
      };
      context.pendingIdleReconciliation = pending;
      const reconcile = Effect.gen(function* () {
        let retryCount = 0;
        while (context.pendingIdleReconciliation === pending) {
          if (
            context.activeTurnId !== turnId ||
            context.awaitingBusyAfterInterruption ||
            context.promptGeneration !== pending.promptGeneration
          ) {
            context.pendingIdleReconciliation = undefined;
            return;
          }
          const result = yield* runOpenCodeSdk("session.status", () =>
            context.client.session.status(),
          ).pipe(
            Effect.retry({ times: 1 }),
            Effect.match({
              onFailure: (cause) => ({ type: "unknown" as const, cause }),
              onSuccess: (response) => {
                const data = Option.getOrUndefined(decodeOpenCodeSessionStatusMap(response.data));
                if (data === undefined) {
                  return { type: "unknown" as const, cause: undefined };
                }
                const status = data[context.openCodeSessionId];
                if (status === undefined || status.type === "idle") {
                  return { type: "idle" as const };
                }
                if (status.type === "busy" || status.type === "retry") {
                  return { type: "busy" as const };
                }
                return { type: "unknown" as const, cause: undefined };
              },
            }),
          );

          if (
            context.pendingIdleReconciliation !== pending ||
            context.activeTurnId !== turnId ||
            context.promptGeneration !== pending.promptGeneration
          ) {
            return;
          }
          if (result.type === "idle") {
            context.pendingIdleReconciliation = undefined;
            yield* completeOpenCodeTurn(context, turnId, pending.promptGeneration, pending.raw);
            return;
          }
          if (result.type === "busy") {
            if (pending.dirty) {
              pending.dirty = false;
              continue;
            }
            context.pendingIdleReconciliation = undefined;
            return;
          }
          if (!pending.warned) {
            pending.warned = true;
            yield* emit({
              ...(yield* buildEventBase({ threadId: context.session.threadId, turnId })),
              type: "runtime.warning",
              payload: {
                message: "OpenCode turn completion is waiting for session status.",
                detail:
                  result.cause === undefined
                    ? "session.status returned missing or invalid status data."
                    : openCodeRuntimeErrorDetail(result.cause),
              },
            });
          }
          const delayMs = Math.min(250 * 2 ** retryCount, 5_000);
          retryCount += 1;
          yield* Effect.sleep(`${delayMs} millis`);
        }
      }).pipe(
        Effect.catchCause(() => Effect.void),
        Effect.ensuring(
          Effect.sync(() => {
            if (context.pendingIdleReconciliation === pending) {
              context.pendingIdleReconciliation = undefined;
            }
          }),
        ),
      );
      pending.fiber = yield* reconcile.pipe(Effect.forkIn(context.sessionScope));
    });

    const schedulePromptAdmissionRecovery = Effect.fn("schedulePromptAdmissionRecovery")(function* (
      context: OpenCodeSessionContext,
      raw: unknown,
    ) {
      const promptAdmission = context.promptAdmission;
      if (!promptAdmission || promptAdmission.messageObserved) {
        return;
      }
      promptAdmission.recoveryRaw = raw;
      if (promptAdmission.recoveryFiber) {
        return;
      }
      const recover = Effect.gen(function* () {
        yield* Deferred.await(promptAdmission.acceptance);
        let retryCount = 0;
        while (context.promptAdmission === promptAdmission) {
          const response = yield* runOpenCodeSdk("session.message", () =>
            context.client.session.message({
              sessionID: context.openCodeSessionId,
              messageID: promptAdmission.messageId,
            }),
          ).pipe(Effect.option);
          const message = Option.isSome(response) ? response.value.data : undefined;
          if (
            message?.info.id === promptAdmission.messageId &&
            message.info.role === "user" &&
            context.promptAdmission === promptAdmission &&
            context.activeTurnId === promptAdmission.turnId &&
            context.promptGeneration === promptAdmission.generation
          ) {
            promptAdmission.messageObserved = true;
            context.messageRoleById.set(promptAdmission.messageId, "user");
            context.promptAdmission = undefined;
            yield* scheduleIdleReconciliation(
              context,
              promptAdmission.turnId,
              promptAdmission.recoveryRaw,
            );
            return;
          }
          const delayMs = Math.min(250 * 2 ** retryCount, 5_000);
          retryCount += 1;
          yield* Effect.sleep(`${delayMs} millis`);
        }
      }).pipe(
        Effect.catchCause(() => Effect.void),
        Effect.ensuring(
          Effect.sync(() => {
            delete promptAdmission.recoveryFiber;
          }),
        ),
      );
      promptAdmission.recoveryFiber = yield* recover.pipe(Effect.forkIn(context.sessionScope));
    });

    const interruptOpenCodeTurn = Effect.fn("interruptOpenCodeTurn")(function* (
      context: OpenCodeSessionContext,
      turnId: TurnId,
      raw?: unknown,
    ) {
      if (context.interruptedTurnId === turnId) {
        return;
      }
      yield* cancelIdleReconciliation(context);
      context.interruptedTurnId = turnId;
      context.reconcileIdleStatus = true;
      context.awaitingBusyAfterInterruption = false;
      if (context.cancellation?.turnId === turnId) {
        context.cancellation = undefined;
      }
      if (context.activeTurnId === turnId) {
        context.activeTurnId = undefined;
        context.activeAgent = undefined;
        context.activeVariant = undefined;
        yield* updateProviderSession(
          context,
          { status: "ready" },
          { clearActiveTurnId: true, clearLastError: true },
        );
      }
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
          raw,
        })),
        type: "turn.aborted",
        payload: {
          reason: "Interrupted by user.",
        },
      });
    });

    const emitUnexpectedExit = Effect.fn("emitUnexpectedExit")(function* (
      context: OpenCodeSessionContext,
      message: string,
    ) {
      // Atomic one-shot: two fibers can race here (the event-pump on stream
      // failure and the server-exit watcher). `getAndSet` flips the flag in
      // a single step so the loser observes `true` and returns; a plain
      // `Ref.get` would let both racers slip past and emit duplicates.
      if (yield* Ref.getAndSet(context.stopped, true)) {
        return;
      }
      yield* Deferred.fail(
        context.firstConnection,
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "event.subscribe",
          detail: "OpenCode session exited before the event stream connected.",
        }),
      ).pipe(Effect.ignore);
      yield* failPendingOpenCodeCancellation(
        context,
        "OpenCode session exited during cancellation.",
      );
      context.promptAdmission = undefined;
      const turnId = context.activeTurnId;
      sessions.delete(context.session.threadId);
      // Emit lifecycle events BEFORE tearing down the scope. Both call sites
      // run this inside a fiber forked via `Effect.forkIn(context.sessionScope)`;
      // closing that scope triggers the fiber-interrupt finalizer, so any
      // subsequent yield point would unwind and silently drop these emits.
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
        })),
        type: "runtime.error",
        payload: {
          message,
          class: "transport_error",
        },
      }).pipe(Effect.ignore);
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
        })),
        type: "session.exited",
        payload: {
          reason: message,
          recoverable: false,
          exitKind: "error",
        },
      }).pipe(Effect.ignore);
      // Inline the teardown that `stopOpenCodeContext` would do; we can't
      // delegate to it because our `getAndSet` above already flipped the
      // one-shot guard, so the call would no-op.
      yield* abortOpenCodeSessionForTeardown(context);
      yield* Scope.close(context.sessionScope, Exit.void);
    });

    /** Emit content.delta and item.completed events for an assistant text part. */
    const emitAssistantTextDelta = Effect.fn("emitAssistantTextDelta")(function* (
      context: OpenCodeSessionContext,
      part: Part,
      turnId: TurnId | undefined,
      raw: unknown,
    ) {
      const text = textFromPart(part);
      if (text === undefined) {
        return;
      }
      const previousText = context.emittedTextByPartId.get(part.id);
      const { latestText, deltaToEmit } = mergeOpenCodeAssistantText(previousText, text);
      context.emittedTextByPartId.set(part.id, latestText);
      if (latestText !== text) {
        context.partById.set(
          part.id,
          (part.type === "text" || part.type === "reasoning"
            ? { ...part, text: latestText }
            : part) satisfies Part,
        );
      }
      if (deltaToEmit.length > 0) {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: part.id,
            createdAt:
              (part.type === "text" || part.type === "reasoning") && part.time !== undefined
                ? isoFromEpochMs(part.time.start)
                : undefined,
            raw,
          })),
          type: "content.delta",
          payload: {
            streamKind: resolveTextStreamKind(part),
            delta: deltaToEmit,
          },
        });
      }

      if (
        part.type === "text" &&
        part.time?.end !== undefined &&
        !context.completedAssistantPartIds.has(part.id)
      ) {
        context.completedAssistantPartIds.add(part.id);
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: part.id,
            createdAt: isoFromEpochMs(part.time.end),
            raw,
          })),
          type: "item.completed",
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
            ...(latestText.length > 0 ? { detail: latestText } : {}),
          },
        });
      }
    });

    const isRelatedOpenCodeSession = Effect.fn("isRelatedOpenCodeSession")(function* (
      context: OpenCodeSessionContext,
      candidateSessionId: string,
    ) {
      if (context.relatedSessionIds.has(candidateSessionId)) {
        return true;
      }

      const seen = new Set<string>();
      const getSession = (sessionID: string) =>
        runOpenCodeSdk("session.get", () => context.client.session.get({ sessionID })).pipe(
          Effect.catchIf(
            (cause) => isOpenCodeNotFound(cause),
            () => Effect.succeed(undefined),
          ),
        );
      let sessionId: string | undefined = candidateSessionId;
      for (let depth = 0; sessionId !== undefined && depth < 32; depth += 1) {
        if (context.relatedSessionIds.has(sessionId)) {
          context.relatedSessionIds.add(candidateSessionId);
          return true;
        }
        if (seen.has(sessionId)) {
          return false;
        }
        seen.add(sessionId);
        const currentSessionId: string = sessionId;
        const response = yield* getSession(currentSessionId);
        if (response === undefined) {
          return false;
        }
        if (!response.data) {
          return yield* new OpenCodeRuntimeError({
            operation: "session.get",
            detail: `OpenCode session.get returned no session payload for '${currentSessionId}'.`,
          });
        }
        sessionId = response.data.parentID;
      }
      return false;
    });

    const emitPendingOpenCodeRequest = Effect.fn("emitPendingOpenCodeRequest")(function* (
      context: OpenCodeSessionContext,
      event: OpenCodeAskedRequestEvent,
      raw: unknown,
    ) {
      if (context.resolvedRequestIds.has(event.properties.id)) {
        return;
      }
      if (event.type === "permission.asked") {
        const request = event.properties;
        if (context.pendingPermissions.has(request.id)) {
          return;
        }
        context.pendingPermissions.set(request.id, request);
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            requestId: request.id,
            raw,
          })),
          type: "request.opened",
          payload: {
            requestType: mapPermissionToRequestType(request.permission),
            detail: request.patterns.length > 0 ? request.patterns.join("\n") : request.permission,
            args: request.metadata,
          },
        });
        return;
      }

      const request = event.properties;
      if (context.pendingQuestions.has(request.id)) {
        return;
      }
      context.pendingQuestions.set(request.id, request);
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId: context.activeTurnId,
          requestId: request.id,
          raw,
        })),
        type: "user-input.requested",
        payload: { questions: normalizeQuestionRequest(request) },
      });
    });

    const resolvePendingOpenCodeRequest = Effect.fn("resolvePendingOpenCodeRequest")(function* (
      context: OpenCodeSessionContext,
      requestId: string,
    ) {
      context.resolvedRequestIds.add(requestId);
      const retry = context.requestRelationRetries.get(requestId);
      context.requestRelationRetries.delete(requestId);
      if (retry?.fiber) {
        yield* Fiber.interrupt(retry.fiber);
      }
    });

    const emitTerminalOpenCodeRequest = Effect.fn("emitTerminalOpenCodeRequest")(function* (
      context: OpenCodeSessionContext,
      event: OpenCodeTerminalRequestEvent,
    ) {
      const requestId = event.properties.requestID;
      if (context.emittedTerminalRequestIds.has(requestId)) {
        return;
      }
      context.emittedTerminalRequestIds.add(requestId);
      if (event.type === "permission.replied") {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            requestId,
            raw: event,
          })),
          type: "request.resolved",
          payload: {
            requestType: "unknown",
            decision: mapPermissionDecision(event.properties.reply),
          },
        });
        return;
      }

      const request = context.pendingQuestions.get(requestId);
      const answers =
        event.type === "question.replied" && request
          ? Object.fromEntries(
              request.questions.map((question, index) => [
                openCodeQuestionId(index, question),
                event.properties.answers[index]?.join(", ") ?? "",
              ]),
            )
          : {};
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId: context.activeTurnId,
          requestId,
          raw: event,
        })),
        type: "user-input.resolved",
        payload: { answers },
      });
    });

    const scheduleRequestRelationRetry = Effect.fn("scheduleRequestRelationRetry")(function* (
      context: OpenCodeSessionContext,
      event: OpenCodeRoutedRequestEvent,
      raw: unknown = event,
    ) {
      const requestId =
        event.type === "permission.asked" || event.type === "question.asked"
          ? event.properties.id
          : event.properties.requestID;
      if (context.requestRelationRetries.has(requestId)) {
        return;
      }
      if (
        (event.type === "permission.asked" || event.type === "question.asked") &&
        context.resolvedRequestIds.has(requestId)
      ) {
        return;
      }
      const retry: OpenCodeRequestRelationRetry = { warned: false };
      context.requestRelationRetries.set(requestId, retry);
      const run = Effect.gen(function* () {
        let retryCount = 0;
        while (context.requestRelationRetries.get(requestId) === retry) {
          const relation = yield* isRelatedOpenCodeSession(
            context,
            event.properties.sessionID,
          ).pipe(
            Effect.match({
              onFailure: (cause) => ({ type: "unknown" as const, cause }),
              onSuccess: (related) => ({ type: "known" as const, related }),
            }),
          );
          if (context.requestRelationRetries.get(requestId) !== retry) {
            return;
          }
          if (relation.type === "known") {
            context.requestRelationRetries.delete(requestId);
            if (relation.related) {
              if (event.type === "permission.asked" || event.type === "question.asked") {
                yield* emitPendingOpenCodeRequest(context, event, raw);
              } else {
                yield* emitTerminalOpenCodeRequest(context, event);
              }
            }
            return;
          }
          if (!retry.warned) {
            retry.warned = true;
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                requestId,
              })),
              type: "runtime.warning",
              payload: {
                message: "OpenCode request routing is waiting for session ancestry.",
                detail: openCodeRuntimeErrorDetail(relation.cause),
              },
            });
          }
          const delayMs = Math.min(250 * 2 ** retryCount, 5_000);
          retryCount += 1;
          yield* Effect.sleep(`${delayMs} millis`);
        }
      }).pipe(
        Effect.catchCause(() => Effect.void),
        Effect.ensuring(
          Effect.sync(() => {
            if (context.requestRelationRetries.get(requestId) === retry) {
              context.requestRelationRetries.delete(requestId);
            }
          }),
        ),
      );
      retry.fiber = yield* run.pipe(Effect.forkIn(context.sessionScope));
    });

    const schedulePendingRequestRecovery = Effect.fn("schedulePendingRequestRecovery")(function* (
      context: OpenCodeSessionContext,
    ) {
      if (context.pendingRequestRecovery) {
        context.pendingRequestRecovery.rerun = true;
        return;
      }
      const recovery: OpenCodePendingRequestRecovery = { warned: false, rerun: false };
      context.pendingRequestRecovery = recovery;
      const run = Effect.gen(function* () {
        let retryCount = 0;
        while (context.pendingRequestRecovery === recovery) {
          const responses = yield* Effect.all({
            permissions: runOpenCodeSdk("permission.list", () => context.client.permission.list()),
            questions: runOpenCodeSdk("question.list", () => context.client.question.list()),
          }).pipe(
            Effect.match({
              onFailure: (cause) => ({ type: "failure" as const, cause }),
              onSuccess: (value) => ({ type: "success" as const, value }),
            }),
          );
          if (context.pendingRequestRecovery !== recovery) {
            return;
          }
          if (responses.type === "failure") {
            if (!recovery.warned) {
              recovery.warned = true;
              yield* emit({
                ...(yield* buildEventBase({ threadId: context.session.threadId })),
                type: "runtime.warning",
                payload: {
                  message: "OpenCode pending request recovery failed and will retry.",
                  detail: openCodeRuntimeErrorDetail(responses.cause),
                },
              });
            }
            const delayMs = Math.min(250 * 2 ** retryCount, 5_000);
            retryCount += 1;
            yield* Effect.sleep(`${delayMs} millis`);
            continue;
          }
          const permissions = responses.value.permissions.data;
          const questions = responses.value.questions.data;
          if (permissions === undefined || questions === undefined) {
            if (!recovery.warned) {
              recovery.warned = true;
              yield* emit({
                ...(yield* buildEventBase({ threadId: context.session.threadId })),
                type: "runtime.warning",
                payload: {
                  message: "OpenCode pending request recovery returned no data and will retry.",
                },
              });
            }
            const delayMs = Math.min(250 * 2 ** retryCount, 5_000);
            retryCount += 1;
            yield* Effect.sleep(`${delayMs} millis`);
            continue;
          }
          yield* Effect.forEach(
            permissions,
            (request) =>
              scheduleRequestRelationRetry(
                context,
                { id: `recovered:${request.id}`, type: "permission.asked", properties: request },
                { type: "permission.asked", properties: request, recovered: true },
              ),
            { discard: true },
          );
          yield* Effect.forEach(
            questions,
            (request) =>
              scheduleRequestRelationRetry(
                context,
                { id: `recovered:${request.id}`, type: "question.asked", properties: request },
                { type: "question.asked", properties: request, recovered: true },
              ),
            { discard: true },
          );
          if (recovery.rerun) {
            recovery.rerun = false;
            recovery.warned = false;
            continue;
          }
          context.pendingRequestRecovery = undefined;
          return;
        }
      }).pipe(
        Effect.catchCause(() => Effect.void),
        Effect.ensuring(
          Effect.sync(() => {
            if (context.pendingRequestRecovery === recovery) {
              context.pendingRequestRecovery = undefined;
            }
          }),
        ),
      );
      yield* run.pipe(Effect.forkIn(context.sessionScope));
    });

    const handleSubscribedEvent = Effect.fn("handleSubscribedEvent")(function* (
      context: OpenCodeSessionContext,
      event: OpenCodeSubscribedEvent,
    ) {
      if (event.type === "server.connected") {
        const firstConnection = yield* Deferred.succeed(context.firstConnection, undefined);
        yield* schedulePendingRequestRecovery(context);
        if (!firstConnection) {
          yield* schedulePromptAdmissionRecovery(context, event);
        }
        return;
      }
      const terminalRequestId =
        event.type === "permission.replied" ||
        event.type === "question.replied" ||
        event.type === "question.rejected"
          ? event.properties.requestID
          : undefined;
      if (terminalRequestId !== undefined) {
        yield* resolvePendingOpenCodeRequest(context, terminalRequestId);
      }
      if (event.type === "session.created" || event.type === "session.updated") {
        const session = event.properties.info;
        if (session.parentID && context.relatedSessionIds.has(session.parentID)) {
          context.relatedSessionIds.add(session.id);
        }
      } else if (event.type === "session.deleted") {
        context.relatedSessionIds.delete(event.properties.info.id);
      }

      const payloadSessionId = openCodeEventSessionId(event);
      const isParentEvent = payloadSessionId === context.openCodeSessionId;
      let isKnownPendingTerminalEvent = false;
      if (
        payloadSessionId !== undefined &&
        !context.relatedSessionIds.has(payloadSessionId) &&
        isOpenCodeChildRequestEvent(event)
      ) {
        if (event.type === "permission.asked") {
          yield* scheduleRequestRelationRetry(context, event);
        } else if (event.type === "question.asked") {
          yield* scheduleRequestRelationRetry(context, event);
        } else if (
          event.type === "permission.replied" ||
          event.type === "question.replied" ||
          event.type === "question.rejected"
        ) {
          const requestId = event.properties.requestID;
          isKnownPendingTerminalEvent =
            context.pendingPermissions.has(requestId) || context.pendingQuestions.has(requestId);
          if (!isKnownPendingTerminalEvent) {
            yield* scheduleRequestRelationRetry(context, event);
            return;
          }
        }
      }
      const isChildRequestEvent =
        payloadSessionId !== undefined &&
        isOpenCodeChildRequestEvent(event) &&
        (context.relatedSessionIds.has(payloadSessionId) || isKnownPendingTerminalEvent);
      if (!isParentEvent && !isChildRequestEvent) {
        return;
      }

      const turnId = context.activeTurnId;
      yield* writeNativeEventBestEffort(context.session.threadId, {
        observedAt: yield* nowIso,
        event: {
          provider: PROVIDER,
          threadId: context.session.threadId,
          providerThreadId: context.openCodeSessionId,
          type: event.type,
          ...(turnId ? { turnId } : {}),
          ...(!isParentEvent && payloadSessionId ? { childSessionId: payloadSessionId } : {}),
          payload: event,
        },
      });

      switch (event.type) {
        case "session.updated": {
          const title = openCodeEventSessionTitle(event);
          if (title) {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                raw: event,
              })),
              type: "thread.metadata.updated",
              payload: {
                name: title,
                metadata: {
                  sessionID: context.openCodeSessionId,
                },
              },
            });
          }
          break;
        }

        case "message.updated": {
          const promptAdmission = context.promptAdmission;
          if (
            event.properties.info.role === "user" &&
            promptAdmission?.messageId === event.properties.info.id
          ) {
            promptAdmission.messageObserved = true;
            if (promptAdmission.accepted) {
              context.promptAdmission = undefined;
              if (promptAdmission.recoveryFiber) {
                yield* Fiber.interrupt(promptAdmission.recoveryFiber);
              }
            }
          }
          context.messageRoleById.set(event.properties.info.id, event.properties.info.role);
          if (event.properties.info.role === "assistant") {
            for (const part of context.partById.values()) {
              if (part.messageID !== event.properties.info.id) {
                continue;
              }
              yield* emitAssistantTextDelta(context, part, turnId, event);
            }
          }
          break;
        }

        case "message.removed": {
          context.messageRoleById.delete(event.properties.messageID);
          break;
        }

        case "message.part.delta": {
          const existingPart = context.partById.get(event.properties.partID);
          if (!existingPart) {
            break;
          }
          const role = messageRoleForPart(context, existingPart);
          if (role !== "assistant") {
            break;
          }
          const streamKind = resolveTextStreamKind(existingPart);
          const delta = event.properties.delta;
          if (delta.length === 0) {
            break;
          }
          const previousText =
            context.emittedTextByPartId.get(event.properties.partID) ??
            textFromPart(existingPart) ??
            "";
          const { nextText, deltaToEmit } = appendOpenCodeAssistantTextDelta(previousText, delta);
          if (deltaToEmit.length === 0) {
            break;
          }
          context.emittedTextByPartId.set(event.properties.partID, nextText);
          if (existingPart.type === "text" || existingPart.type === "reasoning") {
            context.partById.set(event.properties.partID, {
              ...existingPart,
              text: nextText,
            });
          }
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              itemId: event.properties.partID,
              raw: event,
            })),
            type: "content.delta",
            payload: {
              streamKind,
              delta: deltaToEmit,
            },
          });
          break;
        }

        case "message.part.updated": {
          const part = event.properties.part;
          context.partById.set(part.id, part);
          const messageRole = messageRoleForPart(context, part);

          if (messageRole === "assistant") {
            yield* emitAssistantTextDelta(context, part, turnId, event);
          }

          if (part.type === "tool") {
            const itemType = toToolLifecycleItemType(part.tool);
            const title =
              part.state.status === "running" ? (part.state.title ?? part.tool) : part.tool;
            const detail = detailFromToolPart(part);
            const payload = {
              itemType,
              ...(part.state.status === "error"
                ? { status: "failed" as const }
                : part.state.status === "completed"
                  ? { status: "completed" as const }
                  : { status: "inProgress" as const }),
              ...(title ? { title } : {}),
              ...(detail ? { detail } : {}),
              data: {
                tool: part.tool,
                state: part.state,
              },
            };
            const runtimeEvent: ProviderRuntimeEvent = {
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId,
                itemId: part.callID,
                createdAt: toolStateCreatedAt(part),
                raw: event,
              })),
              type:
                part.state.status === "pending"
                  ? "item.started"
                  : part.state.status === "completed" || part.state.status === "error"
                    ? "item.completed"
                    : "item.updated",
              payload,
            };
            appendTurnItem(context, turnId, part);
            yield* emit(runtimeEvent);
          }
          break;
        }

        case "permission.asked": {
          yield* emitPendingOpenCodeRequest(context, event, event);
          break;
        }

        case "permission.replied": {
          context.pendingPermissions.delete(event.properties.requestID);
          yield* emitTerminalOpenCodeRequest(context, event);
          break;
        }

        case "question.asked": {
          yield* emitPendingOpenCodeRequest(context, event, event);
          break;
        }

        case "question.replied": {
          yield* emitTerminalOpenCodeRequest(context, event);
          context.pendingQuestions.delete(event.properties.requestID);
          break;
        }

        case "question.rejected": {
          context.pendingQuestions.delete(event.properties.requestID);
          yield* emitTerminalOpenCodeRequest(context, event);
          break;
        }

        case "session.status": {
          if (event.properties.status.type === "busy") {
            yield* cancelIdleReconciliation(context);
            if (turnId !== undefined) {
              context.awaitingBusyAfterInterruption = false;
            }
            yield* updateProviderSession(context, {
              status: "running",
              activeTurnId: turnId,
            });
          }

          if (event.properties.status.type === "retry") {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId,
                raw: event,
              })),
              type: "runtime.warning",
              payload: {
                message: event.properties.status.message,
                detail: event.properties.status,
              },
            });
            break;
          }

          if (event.properties.status.type === "idle" && turnId) {
            if (context.cancellation?.turnId === turnId) {
              context.cancellation.deferredIdleEvent = event;
              break;
            }
            if (context.awaitingBusyAfterInterruption) {
              break;
            }
            if (context.promptAdmission?.turnId === turnId) {
              context.promptAdmission.idleDuringAdmission = { turnId, raw: event };
              context.promptAdmission.idleObservedAfterMessage =
                context.promptAdmission.messageObserved;
              break;
            }
            if (context.reconcileIdleStatus) {
              yield* scheduleIdleReconciliation(context, turnId, event);
              break;
            }
            yield* completeOpenCodeTurn(context, turnId, context.promptGeneration, event);
          }
          break;
        }

        case "session.error": {
          const message = sessionErrorMessage(event.properties.error);
          const activeTurnId = context.activeTurnId;
          const cancellation = context.cancellation;
          if (
            isOpenCodeAbortError(event.properties.error) &&
            ((activeTurnId !== undefined && cancellation?.turnId === activeTurnId) ||
              context.interruptedTurnId !== undefined)
          ) {
            if (activeTurnId !== undefined && cancellation?.turnId === activeTurnId) {
              yield* interruptOpenCodeTurn(context, activeTurnId, event);
            }
            break;
          }
          yield* cancelIdleReconciliation(context);
          if (activeTurnId !== undefined && cancellation?.turnId === activeTurnId) {
            context.cancellation = undefined;
          }
          context.activeTurnId = undefined;
          context.activeAgent = undefined;
          context.activeVariant = undefined;
          context.reconcileIdleStatus = false;
          yield* updateProviderSession(
            context,
            {
              status: "error",
              lastError: message,
            },
            { clearActiveTurnId: true },
          );
          if (activeTurnId) {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId: activeTurnId,
                raw: event,
              })),
              type: "turn.completed",
              payload: {
                state: "failed",
                errorMessage: message,
              },
            });
          }
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              raw: event,
            })),
            type: "runtime.error",
            payload: {
              message,
              class: "provider_error",
              detail: event.properties.error,
            },
          });
          break;
        }

        default:
          break;
      }
    });

    const startEventPump = Effect.fn("startEventPump")(function* (context: OpenCodeSessionContext) {
      // One AbortController per session scope. The finalizer fires when
      // the scope closes (explicit stop, unexpected exit, or layer
      // shutdown) and cancels the in-flight `event.subscribe` fetch so
      // the async iterable unwinds cleanly.
      const eventsAbortController = new AbortController();
      yield* Scope.addFinalizer(
        context.sessionScope,
        Effect.sync(() => eventsAbortController.abort()),
      );

      // Fibers forked into `context.sessionScope` are interrupted
      // automatically when the scope closes — no bookkeeping required.
      yield* Effect.flatMap(
        runOpenCodeSdk("event.subscribe", () =>
          context.client.event.subscribe(undefined, {
            signal: eventsAbortController.signal,
          }),
        ),
        (subscription) =>
          Stream.fromAsyncIterable(
            subscription.stream,
            (cause) =>
              new OpenCodeRuntimeError({
                operation: "event.subscribe",
                detail: openCodeRuntimeErrorDetail(cause),
                cause,
              }),
          ).pipe(Stream.runForEach((event) => handleSubscribedEvent(context, event))),
      ).pipe(
        Effect.exit,
        Effect.flatMap((exit) =>
          Effect.gen(function* () {
            // Expected paths: caller aborted the fetch or the session
            // has already been marked stopped. Treat as a clean exit.
            if (eventsAbortController.signal.aborted || (yield* Ref.get(context.stopped))) {
              return;
            }
            if (Exit.isFailure(exit)) {
              yield* emitUnexpectedExit(
                context,
                openCodeRuntimeErrorDetail(Cause.squash(exit.cause)),
              );
            }
          }),
        ),
        Effect.forkIn(context.sessionScope),
      );

      if (!context.server.external && context.server.exitCode !== null) {
        yield* context.server.exitCode.pipe(
          Effect.flatMap((code) =>
            Effect.gen(function* () {
              if (yield* Ref.get(context.stopped)) {
                return;
              }
              yield* emitUnexpectedExit(context, `OpenCode server exited unexpectedly (${code}).`);
            }),
          ),
          Effect.forkIn(context.sessionScope),
        );
      }
    });

    const startSession: OpenCodeAdapterShape["startSession"] = Effect.fn("startSession")(
      function* (input) {
        const binaryPath = openCodeSettings.binaryPath;
        const serverUrl = openCodeSettings.serverUrl;
        const serverPassword = openCodeSettings.serverPassword;
        const directory = input.cwd ?? serverConfig.cwd;
        const resumeSessionId = parseOpenCodeResume(input.resumeCursor)?.sessionId;
        const existing = sessions.get(input.threadId);
        if (existing) {
          yield* stopOpenCodeContext(existing);
          sessions.delete(input.threadId);
        }

        const started = yield* Effect.gen(function* () {
          const sessionScope = yield* Scope.make();
          const startedExit = yield* Effect.exit(
            Effect.gen(function* () {
              // The runtime binds the server's lifetime to the Scope.Scope
              // we provide below — closing `sessionScope` kills the child
              // process automatically. No manual `server.close()` needed.
              const server = yield* openCodeRuntime.connectToOpenCodeServer({
                binaryPath,
                serverUrl,
                ...(options?.environment ? { environment: options.environment } : {}),
              });
              const client = openCodeRuntime.createOpenCodeSdkClient({
                baseUrl: server.url,
                directory,
                ...(serverPassword ? { serverPassword } : {}),
              });
              const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
              if (mcpSession && !server.external) {
                yield* runOpenCodeSdk("mcp.add", () =>
                  client.mcp.add({
                    name: "t3-code",
                    config: {
                      type: "remote",
                      url: mcpSession.endpoint,
                      headers: {
                        Authorization: mcpSession.authorizationHeader,
                      },
                      oauth: false,
                    },
                  }),
                );
              }
              // Resume: re-adopt the session named by the durable cursor —
              // OpenCode scopes history by session id. The probe recovers only
              // a confirmed not-found (start fresh); transport/auth/server
              // errors propagate instead of masking as a new empty session.
              const resolved = yield* Effect.gen(function* () {
                const adopted = resumeSessionId
                  ? yield* runOpenCodeSdk("session.get", () =>
                      client.session.get({ sessionID: resumeSessionId }),
                    ).pipe(
                      Effect.map((response) => response.data),
                      Effect.catchIf(
                        (cause) => isOpenCodeNotFound(cause),
                        () => Effect.void,
                      ),
                    )
                  : undefined;

                // Reuse in place only when the session still matches the
                // requested cwd; on a cwd change it is forked below instead.
                const reusable =
                  adopted &&
                  (!adopted.directory || (yield* sameDirectory(adopted.directory, directory)))
                    ? adopted
                    : undefined;

                if (reusable) {
                  // Resume skips `session.create`, so re-assert the ruleset —
                  // a runtime-mode change would otherwise leave the session on
                  // its original permissions.
                  yield* runOpenCodeSdk("session.update", () =>
                    client.session.update({
                      sessionID: reusable.id,
                      permission: buildOpenCodePermissionRules(input.runtimeMode),
                    }),
                  );
                  return { openCodeSession: reusable, created: false };
                }

                // The session lives under a different cwd (e.g. the thread
                // moved into a git worktree). Fork it into the requested
                // directory instead of minting an empty one — the fork carries
                // the full history, so the follow-up keeps its context (#3604).
                if (adopted) {
                  yield* Effect.logInfo(
                    `OpenCode session '${adopted.id}' was created under a different working directory; forking into '${directory}' to preserve conversation history.`,
                  );
                  const forkedSession = yield* runOpenCodeSdk("session.fork", () =>
                    client.session.fork({ sessionID: adopted.id, directory }),
                  );
                  const forked = forkedSession.data;
                  if (!forked) {
                    return yield* new OpenCodeRuntimeError({
                      operation: "session.fork",
                      detail: "OpenCode session.fork returned no session payload.",
                    });
                  }
                  yield* runOpenCodeSdk("session.update", () =>
                    client.session.update({
                      sessionID: forked.id,
                      permission: buildOpenCodePermissionRules(input.runtimeMode),
                    }),
                  );
                  return { openCodeSession: forked, created: true };
                }

                if (resumeSessionId) {
                  yield* Effect.logWarning(
                    `OpenCode session '${resumeSessionId}' no longer exists; starting a fresh session.`,
                  );
                }
                const createdSession = yield* runOpenCodeSdk("session.create", () =>
                  client.session.create({
                    ...(input.title ? { title: input.title } : {}),
                    permission: buildOpenCodePermissionRules(input.runtimeMode),
                  }),
                );
                if (!createdSession.data) {
                  return yield* new OpenCodeRuntimeError({
                    operation: "session.create",
                    detail: "OpenCode session.create returned no session payload.",
                  });
                }
                return { openCodeSession: createdSession.data, created: true };
              });

              return {
                sessionScope,
                server,
                client,
                openCodeSession: resolved.openCodeSession,
                created: resolved.created,
              };
            }).pipe(Effect.provideService(Scope.Scope, sessionScope)),
          );
          if (Exit.isFailure(startedExit)) {
            yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
            return yield* toProcessError(input.threadId, Cause.squash(startedExit.cause));
          }
          return startedExit.value;
        });

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd: directory,
          ...(input.modelSelection ? { model: input.modelSelection.model } : {}),
          threadId: input.threadId,
          // ProviderService persists this cursor and feeds it back into
          // `startSession` after the in-memory session is lost (reaper /
          // restart), so follow-ups continue the same conversation (#3604).
          resumeCursor: {
            schemaVersion: OPENCODE_RESUME_VERSION,
            sessionId: started.openCodeSession.id,
          },
          createdAt,
          updatedAt: createdAt,
        };

        const context: OpenCodeSessionContext = {
          session,
          client: started.client,
          server: started.server,
          directory,
          openCodeSessionId: started.openCodeSession.id,
          relatedSessionIds: new Set([started.openCodeSession.id]),
          resolvedRequestIds: new Set(),
          emittedTerminalRequestIds: new Set(),
          requestRelationRetries: new Map(),
          pendingPermissions: new Map(),
          pendingQuestions: new Map(),
          partById: new Map(),
          emittedTextByPartId: new Map(),
          messageRoleById: new Map(),
          completedAssistantPartIds: new Set(),
          turns: [],
          activeTurnId: undefined,
          activeAgent: undefined,
          activeVariant: undefined,
          cancellation: undefined,
          interruptedTurnId: undefined,
          reconcileIdleStatus: false,
          awaitingBusyAfterInterruption: false,
          pendingIdleReconciliation: undefined,
          pendingRequestRecovery: undefined,
          promptGeneration: 0,
          promptAdmission: undefined,
          firstConnection: Deferred.makeUnsafe<void, ProviderAdapterRequestError>(),
          stopped: yield* Ref.make(false),
          sessionScope: started.sessionScope,
        };
        yield* startEventPump(context);
        const connectionExit = yield* Deferred.await(context.firstConnection).pipe(
          Effect.timeout("10 seconds"),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "event.subscribe",
                detail: "OpenCode event stream did not connect within 10 seconds.",
                cause,
              }),
          ),
          Effect.onInterrupt(() => closeUnpublishedOpenCodeContext(context, started.created)),
          Effect.exit,
        );
        if (Exit.isFailure(connectionExit)) {
          yield* closeUnpublishedOpenCodeContext(context, started.created);
          return yield* Effect.failCause(connectionExit.cause);
        }
        const connectedRaceWinner = sessions.get(input.threadId);
        if (connectedRaceWinner) {
          // Another start crossed the connection barrier first. A newly
          // created remote session belongs to this loser and must be aborted;
          // a resumed session is shared upstream state and must stay alive.
          yield* closeUnpublishedOpenCodeContext(context, started.created);
          return connectedRaceWinner.session;
        }
        sessions.set(input.threadId, context);
        if (!started.created) {
          yield* schedulePendingRequestRecovery(context);
        }

        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId })),
          type: "session.started",
          payload: {
            message: "OpenCode session started",
          },
        });
        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId })),
          type: "thread.started",
          payload: {
            providerThreadId: started.openCodeSession.id,
          },
        });

        return session;
      },
    );

    const sendTurn: OpenCodeAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
      const context = yield* ensureSessionContext(sessions, input.threadId);
      const modelSelection =
        input.modelSelection ??
        (context.session.model
          ? { instanceId: boundInstanceId, model: context.session.model }
          : undefined);
      if (modelSelection !== undefined && modelSelection.instanceId !== boundInstanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `OpenCode model selection is bound to instance '${modelSelection?.instanceId}', expected '${boundInstanceId}'.`,
        });
      }
      const parsedModel = parseOpenCodeModelSlug(modelSelection?.model);
      if (!parsedModel) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "OpenCode model selection must use the 'provider/model' format.",
        });
      }

      const text = input.input?.trim();
      const fileParts = toOpenCodeFileParts({
        attachments: input.attachments,
        resolveAttachmentPath: (attachment) =>
          resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          }),
      });
      if ((!text || text.length === 0) && fileParts.length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "OpenCode turns require text input or at least one attachment.",
        });
      }

      const freshTurnId = TurnId.make(`opencode-turn-${yield* randomUUIDv4}`);
      const messageId = yield* makeOpenCodeMessageId();
      const pendingCancellation = context.cancellation;
      if (pendingCancellation) {
        yield* Deferred.await(pendingCancellation.completion);
      }
      const currentContext = yield* ensureSessionContext(sessions, input.threadId);
      if (currentContext !== context) {
        return yield* new ProviderAdapterSessionClosedError({
          provider: PROVIDER,
          threadId: input.threadId,
        });
      }
      // A sendTurn while a turn is active is a steer. OpenCode queues the
      // prompt into the running session, so the active turn id is reused.
      const steeringTurnId = context.activeTurnId;
      const turnId = steeringTurnId ?? freshTurnId;
      const agent = getModelSelectionStringOptionValue(modelSelection, "agent");
      const variant = getModelSelectionStringOptionValue(modelSelection, "variant");
      const pendingIdleReconciliation = context.pendingIdleReconciliation;
      const priorAwaitingBusy = context.awaitingBusyAfterInterruption;
      const priorIdleCandidate = pendingIdleReconciliation
        ? {
            turnId: pendingIdleReconciliation.turnId,
            raw: pendingIdleReconciliation.raw,
          }
        : undefined;
      context.pendingIdleReconciliation = undefined;
      const promptGeneration = context.promptGeneration + 1;
      const promptAdmission: OpenCodePromptAdmission = {
        generation: promptGeneration,
        turnId,
        messageId,
        priorAwaitingBusy,
        priorIdle: priorIdleCandidate,
        idleDuringAdmission: undefined,
        idleObservedAfterMessage: false,
        messageObserved: false,
        accepted: false,
        acceptance: Deferred.makeUnsafe<void>(),
        recoveryRaw: undefined,
      };
      context.promptGeneration = promptGeneration;
      context.promptAdmission = promptAdmission;

      context.activeTurnId = turnId;
      context.activeAgent = agent ?? (input.interactionMode === "plan" ? "plan" : undefined);
      context.activeVariant = variant;
      if (steeringTurnId === undefined) {
        context.awaitingBusyAfterInterruption = context.interruptedTurnId !== undefined;
      }
      if (pendingIdleReconciliation?.fiber) {
        yield* Fiber.interrupt(pendingIdleReconciliation.fiber);
      }
      yield* updateProviderSession(
        context,
        {
          status: "running",
          activeTurnId: turnId,
          model: modelSelection?.model ?? context.session.model,
        },
        { clearLastError: true },
      );

      if (steeringTurnId === undefined) {
        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
          type: "turn.started",
          payload: {
            model: modelSelection?.model ?? context.session.model,
            ...(variant ? { effort: variant } : {}),
          },
        });
      }

      yield* runOpenCodeSdk("session.promptAsync", () =>
        context.client.session.promptAsync({
          sessionID: context.openCodeSessionId,
          messageID: messageId,
          model: parsedModel,
          ...(context.activeAgent ? { agent: context.activeAgent } : {}),
          ...(context.activeVariant ? { variant: context.activeVariant } : {}),
          parts: [...(text ? [{ type: "text" as const, text }] : []), ...fileParts],
        }),
      ).pipe(
        Effect.mapError(toRequestError),
        // On failure of a fresh turn: clear active-turn state, flip the
        // session back to ready with lastError set, emit turn.aborted, then
        // let the typed error propagate. We don't need to rebuild the error
        // here — `toRequestError` already produced the right shape. A failed
        // steer leaves the still-running original turn untouched.
        Effect.tapError((requestError) =>
          context.promptAdmission !== promptAdmission || context.activeTurnId !== turnId
            ? Effect.void
            : steeringTurnId !== undefined
              ? Effect.gen(function* () {
                  context.promptAdmission = undefined;
                  context.awaitingBusyAfterInterruption = promptAdmission.priorAwaitingBusy;
                  const idle = promptAdmission.idleDuringAdmission ?? promptAdmission.priorIdle;
                  if (idle) {
                    yield* scheduleIdleReconciliation(context, idle.turnId, idle.raw);
                  }
                })
              : Effect.gen(function* () {
                  context.promptAdmission = undefined;
                  context.activeTurnId = undefined;
                  context.activeAgent = undefined;
                  context.activeVariant = undefined;
                  yield* updateProviderSession(
                    context,
                    {
                      status: "ready",
                      model: modelSelection?.model ?? context.session.model,
                      lastError: requestError.detail,
                    },
                    { clearActiveTurnId: true },
                  );
                  yield* emit({
                    ...(yield* buildEventBase({
                      threadId: input.threadId,
                      turnId,
                    })),
                    type: "turn.aborted",
                    payload: {
                      reason: requestError.detail,
                    },
                  });
                }),
        ),
        Effect.onExit((exit) =>
          Exit.isSuccess(exit)
            ? Effect.void
            : Effect.gen(function* () {
                if (context.promptAdmission === promptAdmission) {
                  context.promptAdmission = undefined;
                }
                yield* Deferred.succeed(promptAdmission.acceptance, undefined).pipe(Effect.ignore);
              }),
        ),
      );
      promptAdmission.accepted = true;
      yield* Deferred.succeed(promptAdmission.acceptance, undefined).pipe(Effect.ignore);
      if (
        context.promptAdmission === promptAdmission &&
        context.activeTurnId === turnId &&
        context.promptGeneration === promptAdmission.generation &&
        promptAdmission.messageObserved
      ) {
        context.promptAdmission = undefined;
        const idle = promptAdmission.idleDuringAdmission;
        if (idle && promptAdmission.idleObservedAfterMessage) {
          yield* scheduleIdleReconciliation(context, turnId, idle.raw);
        }
      }

      return {
        threadId: input.threadId,
        turnId,
        // Re-surface the durable cursor on every turn so the persisted binding
        // is refreshed alongside last-seen/runtime state (mirrors Grok/Codex).
        ...(context.session.resumeCursor !== undefined
          ? { resumeCursor: context.session.resumeCursor }
          : {}),
      };
    });

    const interruptTurn: OpenCodeAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
      function* (threadId, turnId) {
        const context = yield* ensureSessionContext(sessions, threadId);
        const activeTurnId = context.activeTurnId;
        if (
          (turnId !== undefined && activeTurnId !== turnId) ||
          (turnId === undefined && activeTurnId === undefined)
        ) {
          return;
        }
        const interruptedTurnId = turnId ?? activeTurnId;
        yield* cancelIdleReconciliation(context);
        if (interruptedTurnId && context.interruptedTurnId === interruptedTurnId) {
          return;
        }
        const existingCancellation = context.cancellation;
        if (interruptedTurnId && existingCancellation?.turnId === interruptedTurnId) {
          return yield* Deferred.await(existingCancellation.completion);
        }
        const cancellation: OpenCodeCancellation | undefined = interruptedTurnId
          ? {
              turnId: interruptedTurnId,
              completion: Deferred.makeUnsafe<void, ProviderAdapterRequestError>(),
            }
          : undefined;
        if (cancellation) {
          context.cancellation = cancellation;
        }

        const abortExit = yield* Effect.exit(
          runOpenCodeSdk("session.abort", () =>
            context.client.session.abort({ sessionID: context.openCodeSessionId }),
          ).pipe(Effect.mapError(toRequestError)),
        );
        if (Exit.isFailure(abortExit)) {
          if (interruptedTurnId && context.interruptedTurnId === interruptedTurnId) {
            if (cancellation) {
              yield* Deferred.succeed(cancellation.completion, undefined).pipe(Effect.ignore);
            }
            return;
          }
          if (cancellation && context.cancellation === cancellation) {
            context.cancellation = undefined;
            if (cancellation.deferredIdleEvent) {
              yield* scheduleIdleReconciliation(
                context,
                cancellation.turnId,
                cancellation.deferredIdleEvent,
              );
            }
          }
          if (cancellation) {
            yield* Deferred.done(cancellation.completion, abortExit).pipe(Effect.ignore);
          }
          return yield* Effect.failCause(abortExit.cause);
        }

        if (cancellation && context.cancellation === cancellation) {
          yield* interruptOpenCodeTurn(context, cancellation.turnId);
        }
        if (cancellation) {
          yield* Deferred.succeed(cancellation.completion, undefined).pipe(Effect.ignore);
        }
      },
    );

    const respondToRequest: OpenCodeAdapterShape["respondToRequest"] = Effect.fn(
      "respondToRequest",
    )(function* (threadId, requestId, decision) {
      const context = yield* ensureSessionContext(sessions, threadId);
      if (!context.pendingPermissions.has(requestId)) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "permission.reply",
          detail: `Unknown pending permission request: ${requestId}`,
        });
      }

      yield* runOpenCodeSdk("permission.reply", () =>
        context.client.permission.reply({
          requestID: requestId,
          reply: toOpenCodePermissionReply(decision),
        }),
      ).pipe(Effect.mapError(toRequestError));
    });

    const respondToUserInput: OpenCodeAdapterShape["respondToUserInput"] = Effect.fn(
      "respondToUserInput",
    )(function* (threadId, requestId, answers) {
      const context = yield* ensureSessionContext(sessions, threadId);
      const request = context.pendingQuestions.get(requestId);
      if (!request) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "question.reply",
          detail: `Unknown pending user-input request: ${requestId}`,
        });
      }

      yield* runOpenCodeSdk("question.reply", () =>
        context.client.question.reply({
          requestID: requestId,
          answers: toOpenCodeQuestionAnswers(request, answers),
        }),
      ).pipe(Effect.mapError(toRequestError));
    });

    const stopSession: OpenCodeAdapterShape["stopSession"] = Effect.fn("stopSession")(
      function* (threadId) {
        const context = sessions.get(threadId);
        if (!context) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        const stopped = yield* stopOpenCodeContext(context);
        sessions.delete(threadId);
        if (!stopped) {
          return;
        }
        yield* emit({
          ...(yield* buildEventBase({ threadId })),
          type: "session.exited",
          payload: {
            reason: "Session stopped.",
            recoverable: false,
            exitKind: "graceful",
          },
        });
      },
    );

    const listSessions: OpenCodeAdapterShape["listSessions"] = () =>
      Effect.sync(() => [...sessions.values()].map((context) => context.session));

    const hasSession: OpenCodeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId));

    const readThread: OpenCodeAdapterShape["readThread"] = Effect.fn("readThread")(
      function* (threadId) {
        const context = yield* ensureSessionContext(sessions, threadId);
        const messages = yield* runOpenCodeSdk("session.messages", () =>
          context.client.session.messages({
            sessionID: context.openCodeSessionId,
          }),
        ).pipe(Effect.mapError(toRequestError));

        const turns: Array<OpenCodeTurnSnapshot> = [];
        for (const entry of messages.data ?? []) {
          if (entry.info.role === "assistant") {
            turns.push({
              id: TurnId.make(entry.info.id),
              items: [entry.info, ...entry.parts],
            });
          }
        }

        return {
          threadId,
          turns,
        };
      },
    );

    const rollbackThread: OpenCodeAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
      function* (threadId, numTurns) {
        const context = yield* ensureSessionContext(sessions, threadId);
        const messages = yield* runOpenCodeSdk("session.messages", () =>
          context.client.session.messages({
            sessionID: context.openCodeSessionId,
          }),
        ).pipe(Effect.mapError(toRequestError));

        const assistantMessages = (messages.data ?? []).filter(
          (entry) => entry.info.role === "assistant",
        );
        const targetIndex = assistantMessages.length - numTurns - 1;
        const target = targetIndex >= 0 ? assistantMessages[targetIndex] : null;
        yield* runOpenCodeSdk("session.revert", () =>
          context.client.session.revert({
            sessionID: context.openCodeSessionId,
            ...(target ? { messageID: target.info.id } : {}),
          }),
        ).pipe(Effect.mapError(toRequestError));

        return yield* readThread(threadId);
      },
    );

    const stopAll: OpenCodeAdapterShape["stopAll"] = () =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        // `stopOpenCodeContext` is typed as never-failing — SDK aborts are
        // already `Effect.ignore`'d inside it. `ignoreCause` here also
        // swallows defects from throwing finalizers so one bad close can't
        // interrupt the sibling fibers. Same pattern as the layer finalizer.
        yield* Effect.forEach(
          contexts,
          (context) => Effect.ignoreCause(stopOpenCodeContext(context)),
          { concurrency: "unbounded", discard: true },
        );
      });

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      get streamEvents() {
        return Stream.fromQueue(runtimeEvents);
      },
    } satisfies OpenCodeAdapterShape;
  });
}
