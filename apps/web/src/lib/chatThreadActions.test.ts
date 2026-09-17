import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  type ModelSelection,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  resolveThreadActionProjectRef,
  hasExplicitComposerModelSelection,
  resolveNewDraftStartFromOrigin,
  resolveMovedThreadDraftSelection,
  resolveNewThreadModelSelectionOverride,
  startNewThreadFromContext,
  type ChatThreadActionContext,
} from "./chatThreadActions";

const ENVIRONMENT_ID = EnvironmentId.make("environment-1");
const PROJECT_ID = ProjectId.make("project-1");
const FALLBACK_PROJECT_ID = ProjectId.make("project-2");
const PROJECT_DEFAULT_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "project-default",
};
const CARRIED_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "carried-model",
};

function createContext(overrides: Partial<ChatThreadActionContext> = {}): ChatThreadActionContext {
  return {
    activeDraftThread: null,
    activeThread: undefined,
    defaultProjectRef: scopeProjectRef(ENVIRONMENT_ID, FALLBACK_PROJECT_ID),
    handleNewThread: async () => {},
    ...overrides,
  };
}

describe("chatThreadActions", () => {
  it("only treats an active stored selection marked explicit as an explicit pick", () => {
    const draft = {
      activeProvider: PROJECT_DEFAULT_SELECTION.instanceId,
      modelSelectionByProvider: {
        [PROJECT_DEFAULT_SELECTION.instanceId]: PROJECT_DEFAULT_SELECTION,
      },
      modelSelectionExplicit: true,
    };

    expect(hasExplicitComposerModelSelection(draft)).toBe(true);
    expect(hasExplicitComposerModelSelection({ ...draft, modelSelectionExplicit: false })).toBe(
      false,
    );
    expect(hasExplicitComposerModelSelection({ ...draft, activeProvider: null })).toBe(false);
  });

  describe("resolveMovedThreadDraftSelection", () => {
    const OLD_HOME: ModelSelection = {
      instanceId: ProviderInstanceId.make("claude8"),
      model: "claude-opus-5",
    };
    const NEW_HOME: ModelSelection = {
      instanceId: ProviderInstanceId.make("claude7"),
      model: "claude-opus-5",
    };
    const draftOn = (
      selection: ModelSelection,
      extra: { explicit?: boolean; threadInstanceId?: ModelSelection["instanceId"] } = {},
    ) => ({
      activeProvider: selection.instanceId,
      modelSelectionByProvider: { [selection.instanceId]: selection },
      ...(extra.explicit ? { modelSelectionExplicit: true } : {}),
      ...(extra.threadInstanceId ? { modelSelectionThreadInstanceId: extra.threadInstanceId } : {}),
    });

    it("refreshes a draft picked before the thread was moved to another home", () => {
      // Incident shape: picked claude8 while pinned to claude8, then moved to claude7.
      expect(
        resolveMovedThreadDraftSelection({
          draft: draftOn(OLD_HOME, { explicit: true, threadInstanceId: OLD_HOME.instanceId }),
          threadModelSelection: NEW_HOME,
        }),
      ).toBe(NEW_HOME);
    });

    it("refreshes seeded and legacy drafts that name another home", () => {
      expect(
        resolveMovedThreadDraftSelection({
          draft: draftOn(OLD_HOME),
          threadModelSelection: NEW_HOME,
        }),
      ).toBe(NEW_HOME);
      expect(
        resolveMovedThreadDraftSelection({
          draft: draftOn(OLD_HOME, { explicit: true }),
          threadModelSelection: NEW_HOME,
        }),
      ).toBe(NEW_HOME);
    });

    it("keeps a pick the user made after the thread reached its current pin", () => {
      expect(
        resolveMovedThreadDraftSelection({
          draft: draftOn(OLD_HOME, { explicit: true, threadInstanceId: NEW_HOME.instanceId }),
          threadModelSelection: NEW_HOME,
        }),
      ).toBeNull();
    });

    it("leaves matching or empty drafts alone", () => {
      expect(
        resolveMovedThreadDraftSelection({
          draft: draftOn(NEW_HOME),
          threadModelSelection: NEW_HOME,
        }),
      ).toBeNull();
      expect(
        resolveMovedThreadDraftSelection({ draft: null, threadModelSelection: NEW_HOME }),
      ).toBeNull();
      expect(
        resolveMovedThreadDraftSelection({
          draft: { ...draftOn(OLD_HOME), activeProvider: null },
          threadModelSelection: NEW_HOME,
        }),
      ).toBeNull();
    });
  });

  it("does not carry a non-explicit model from the destination draft back into itself", () => {
    expect(
      resolveNewThreadModelSelectionOverride({
        projectDefaultSelection: null,
        carrySelection: CARRIED_SELECTION,
        carrySourceDraftId: "draft-a",
        destinationDraftId: "draft-a",
      }),
    ).toBeNull();
  });

  it("still carries models between different threads when the project has no default", () => {
    expect(
      resolveNewThreadModelSelectionOverride({
        projectDefaultSelection: null,
        carrySelection: CARRIED_SELECTION,
        carrySourceDraftId: "draft-a",
        destinationDraftId: "draft-b",
      }),
    ).toEqual(CARRIED_SELECTION);
  });

  it("keeps the project default above any carried selection", () => {
    expect(
      resolveNewThreadModelSelectionOverride({
        projectDefaultSelection: PROJECT_DEFAULT_SELECTION,
        carrySelection: CARRIED_SELECTION,
        carrySourceDraftId: "draft-a",
        destinationDraftId: "draft-b",
      }),
    ).toEqual(PROJECT_DEFAULT_SELECTION);
  });

  it("only applies the start-from-origin default to new worktree drafts", () => {
    expect(
      resolveNewDraftStartFromOrigin({
        envMode: "worktree",
        newWorktreesStartFromOrigin: true,
      }),
    ).toBe(true);
    expect(
      resolveNewDraftStartFromOrigin({
        envMode: "local",
        newWorktreesStartFromOrigin: true,
      }),
    ).toBe(false);
  });

  it("prefers the active thread project when resolving thread actions", () => {
    const projectRef = resolveThreadActionProjectRef(
      createContext({
        activeThread: {
          environmentId: ENVIRONMENT_ID,
          projectId: PROJECT_ID,
        },
      }),
    );

    expect(projectRef).toEqual(scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID));
  });

  it("falls back to the active draft thread project when there is no active thread", () => {
    const projectRef = resolveThreadActionProjectRef(
      createContext({
        activeDraftThread: {
          environmentId: ENVIRONMENT_ID,
          projectId: PROJECT_ID,
        },
      }),
    );

    expect(projectRef).toEqual(scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID));
  });

  it("falls back to the default project ref when there is no active thread context", () => {
    const projectRef = resolveThreadActionProjectRef(
      createContext({
        defaultProjectRef: scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID),
      }),
    );

    expect(projectRef).toEqual(scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID));
  });

  it("inherits only the project from context, never branch or worktree state", async () => {
    const handleNewThread = vi.fn<ChatThreadActionContext["handleNewThread"]>(async () => {});

    const didStart = await startNewThreadFromContext(
      createContext({
        activeThread: {
          environmentId: ENVIRONMENT_ID,
          projectId: PROJECT_ID,
        },
        handleNewThread,
      }),
    );

    expect(didStart).toBe(true);
    expect(handleNewThread).toHaveBeenCalledWith(scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID));
  });

  it("does not start a thread when there is no project context", async () => {
    const handleNewThread = vi.fn<ChatThreadActionContext["handleNewThread"]>(async () => {});

    const didStart = await startNewThreadFromContext(
      createContext({
        defaultProjectRef: null,
        handleNewThread,
      }),
    );

    expect(didStart).toBe(false);
    expect(handleNewThread).not.toHaveBeenCalled();
  });
});
