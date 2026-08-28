import { beforeEach, describe, expect, it } from "vite-plus/test";

import {
  acquireBrowserSurface,
  acquireBrowserSurfaceClickPresentation,
  resolveBrowserSurfacePanelRect,
  useBrowserSurfaceStore,
} from "./browserSurfaceStore";

describe("browserSurfaceStore", () => {
  beforeEach(() => {
    useBrowserSurfaceStore.setState({ byTabId: {} });
  });

  it("freezes the source content dimensions for a fitted presentation", () => {
    const tabId = "fitted-browser-surface";
    const sourceOwner = Symbol("source");
    const sourceContent = {
      x: 10,
      y: 20,
      width: 1_280,
      height: 720,
      scale: 1,
      scrollLeft: 0,
      scrollTop: 0,
    };
    useBrowserSurfaceStore.getState().claim(tabId, sourceOwner, false);
    useBrowserSurfaceStore.getState().presentContent(tabId, sourceContent);

    const fittedLease = acquireBrowserSurface(tabId, true);
    useBrowserSurfaceStore.getState().presentContent(tabId, {
      ...sourceContent,
      width: 360,
      height: 203,
      scale: 0.28125,
    });

    expect(useBrowserSurfaceStore.getState().byTabId[tabId]?.fittedSourceContent).toEqual(
      sourceContent,
    );
    fittedLease.release();
  });

  it("freezes the first content dimensions when fitting starts before the browser is measured", () => {
    const tabId = "pending-fitted-browser-surface";
    const fittedLease = acquireBrowserSurface(tabId, true);
    const sourceContent = {
      x: 0,
      y: 0,
      width: 1_280,
      height: 720,
      scale: 1,
      scrollLeft: 0,
      scrollTop: 0,
    };

    useBrowserSurfaceStore.getState().presentContent(tabId, sourceContent);
    useBrowserSurfaceStore.getState().presentContent(tabId, {
      ...sourceContent,
      width: 320,
      height: 180,
      scale: 0.25,
    });

    expect(useBrowserSurfaceStore.getState().byTabId[tabId]?.fittedSourceContent).toEqual(
      sourceContent,
    );
    fittedLease.release();
  });

  it("tracks content dimensions for a browser that has never been visible", () => {
    const tabId = "hidden-browser-surface-content-test";
    useBrowserSurfaceStore.getState().presentContent(tabId, {
      x: 0,
      y: 0,
      width: 393,
      height: 852,
      scale: 1,
      scrollLeft: 0,
      scrollTop: 0,
    });

    expect(useBrowserSurfaceStore.getState().byTabId[tabId]).toMatchObject({
      rect: null,
      visible: false,
      content: { width: 393, height: 852 },
    });
  });

  it("keeps a hidden background tab on its own last rect", () => {
    const staleRect = { x: 0, y: 0, width: 500, height: 700 };
    const liveRect = { x: 10, y: 20, width: 900, height: 640 };
    expect(
      resolveBrowserSurfacePanelRect(
        {
          hidden: {
            rect: staleRect,
            visible: false,
            content: null,
            fittedSourceContent: null,
            fitSourceContent: false,
            cornerRadius: 0,
            updatedAt: 1,
            owner: null,
          },
          active: {
            rect: liveRect,
            visible: true,
            content: null,
            fittedSourceContent: null,
            fitSourceContent: false,
            cornerRadius: 0,
            updatedAt: 2,
            owner: null,
          },
        },
        "hidden",
      ),
    ).toEqual(staleRect);
  });

  it("ignores updates and releases from a stale surface lease", () => {
    const tabId = "leased-browser-surface";
    const staleRect = { x: 0, y: 0, width: 500, height: 700 };
    const liveRect = { x: 10, y: 20, width: 900, height: 640 };
    const staleLease = acquireBrowserSurface(tabId);
    staleLease.present(staleRect, true);

    const liveLease = acquireBrowserSurface(tabId);
    liveLease.present(liveRect, true);
    expect(staleLease.present(staleRect, true)).toBe(false);
    staleLease.release();

    expect(useBrowserSurfaceStore.getState().byTabId[tabId]).toMatchObject({
      rect: liveRect,
      visible: true,
    });
  });

  it("hides a surface when its current lease is released", () => {
    const tabId = "released-browser-surface";
    const lease = acquireBrowserSurface(tabId);
    lease.present({ x: 10, y: 20, width: 900, height: 640 }, true);

    lease.release();
    lease.present({ x: 0, y: 0, width: 1, height: 1 }, true);

    expect(useBrowserSurfaceStore.getState().byTabId[tabId]).toMatchObject({
      visible: false,
      owner: null,
    });
  });

  it("pins only visible surfaces through owner changes during a native click", () => {
    const tabId = "click-presentation-surface";
    expect(acquireBrowserSurfaceClickPresentation(tabId)).toBeNull();

    const initialSurface = acquireBrowserSurface(tabId);
    initialSurface.present({ x: 10, y: 20, width: 900, height: 640 }, true);
    const clickPresentation = acquireBrowserSurfaceClickPresentation(tabId);
    expect(clickPresentation).not.toBeNull();
    expect(useBrowserSurfaceStore.getState().byTabId[tabId]?.automationClickHolds).toBe(1);

    acquireBrowserSurface(tabId);
    expect(useBrowserSurfaceStore.getState().byTabId[tabId]).toMatchObject({
      visible: false,
      automationClickHolds: 1,
    });

    clickPresentation?.release();
    expect(useBrowserSurfaceStore.getState().byTabId[tabId]?.automationClickHolds).toBeUndefined();
  });

  it("keeps a switched-away click presentation until native dispatch releases it", () => {
    const clickedTabId = "clicked-browser-surface";
    const selectedTabId = "selected-browser-surface";
    const rect = { x: 10, y: 20, width: 900, height: 640 };
    const clickedSurface = acquireBrowserSurface(clickedTabId);
    clickedSurface.present(rect, true);
    const clickPresentation = acquireBrowserSurfaceClickPresentation(clickedTabId);

    clickedSurface.present(rect, false);
    const selectedSurface = acquireBrowserSurface(selectedTabId);
    selectedSurface.present(rect, true);

    expect(useBrowserSurfaceStore.getState().byTabId[clickedTabId]).toMatchObject({
      visible: false,
      automationClickHolds: 1,
    });
    expect(useBrowserSurfaceStore.getState().byTabId[selectedTabId]?.visible).toBe(true);

    clickPresentation?.release();
    expect(
      useBrowserSurfaceStore.getState().byTabId[clickedTabId]?.automationClickHolds,
    ).toBeUndefined();
  });

  it("rejects concurrent click presentation holds on overlapping tabs", () => {
    const firstTabId = "first-click-browser-surface";
    const secondTabId = "second-click-browser-surface";
    const rect = { x: 10, y: 20, width: 900, height: 640 };
    const firstSurface = acquireBrowserSurface(firstTabId);
    const secondSurface = acquireBrowserSurface(secondTabId);
    firstSurface.present(rect, true);
    secondSurface.present(rect, true);

    const firstClickPresentation = acquireBrowserSurfaceClickPresentation(firstTabId);
    const secondFirstTabClickPresentation = acquireBrowserSurfaceClickPresentation(firstTabId);
    expect(firstClickPresentation).not.toBeNull();
    expect(secondFirstTabClickPresentation).not.toBeNull();
    expect(acquireBrowserSurfaceClickPresentation(secondTabId)).toBeNull();

    firstClickPresentation?.release();
    expect(acquireBrowserSurfaceClickPresentation(secondTabId)).toBeNull();
    expect(useBrowserSurfaceStore.getState().byTabId[firstTabId]?.automationClickHolds).toBe(1);

    secondFirstTabClickPresentation?.release();
    const secondTabClickPresentation = acquireBrowserSurfaceClickPresentation(secondTabId);
    expect(secondTabClickPresentation).not.toBeNull();
    secondTabClickPresentation?.release();
  });

  it("allows concurrent click presentation holds when tab edges only touch", () => {
    const firstTabId = "first-edge-browser-surface";
    const secondTabId = "second-edge-browser-surface";
    const firstSurface = acquireBrowserSurface(firstTabId);
    const secondSurface = acquireBrowserSurface(secondTabId);
    firstSurface.present({ x: 10, y: 20, width: 900, height: 640 }, true);
    secondSurface.present({ x: 910, y: 20, width: 360, height: 203 }, true);

    const firstClickPresentation = acquireBrowserSurfaceClickPresentation(firstTabId);
    const secondClickPresentation = acquireBrowserSurfaceClickPresentation(secondTabId);

    expect(firstClickPresentation).not.toBeNull();
    expect(secondClickPresentation).not.toBeNull();
    firstClickPresentation?.release();
    secondClickPresentation?.release();
  });

  it("allows concurrent click presentation holds on disjoint main and mini-player surfaces", () => {
    const mainTabId = "main-browser-surface";
    const miniPlayerTabId = "mini-player-browser-surface";
    const mainSurface = acquireBrowserSurface(mainTabId);
    const miniPlayerSurface = acquireBrowserSurface(miniPlayerTabId);
    mainSurface.present({ x: 24, y: 80, width: 960, height: 720 }, true);
    miniPlayerSurface.present({ x: 1_100, y: 620, width: 360, height: 203 }, true);

    const mainClickPresentation = acquireBrowserSurfaceClickPresentation(mainTabId);
    const miniPlayerClickPresentation = acquireBrowserSurfaceClickPresentation(miniPlayerTabId);

    expect(mainClickPresentation).not.toBeNull();
    expect(miniPlayerClickPresentation).not.toBeNull();
    mainClickPresentation?.release();
    miniPlayerClickPresentation?.release();
  });

  it("clears fitted presentation state when its lease is released", () => {
    const tabId = "released-fitted-browser-surface";
    const fittedLease = acquireBrowserSurface(tabId, true);
    useBrowserSurfaceStore.getState().presentContent(tabId, {
      x: 0,
      y: 0,
      width: 1_280,
      height: 800,
      scale: 1,
      scrollLeft: 0,
      scrollTop: 0,
    });

    fittedLease.release();

    expect(useBrowserSurfaceStore.getState().byTabId[tabId]).toMatchObject({
      fittedSourceContent: null,
      fitSourceContent: false,
      owner: null,
      visible: false,
    });
  });
});
