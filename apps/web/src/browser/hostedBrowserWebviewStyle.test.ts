import { describe, expect, it } from "vite-plus/test";

import {
  HIDDEN_BROWSER_WEBVIEW_OFFSET,
  resolveHostedBrowserWebviewWrapperStyle,
} from "./hostedBrowserWebviewStyle";

describe("resolveHostedBrowserWebviewWrapperStyle", () => {
  it("places an active webview on its presented surface", () => {
    expect(
      resolveHostedBrowserWebviewWrapperStyle({
        active: true,
        rect: { x: 12, y: 34, width: 800, height: 600 },
        hiddenSize: { width: 1280, height: 800 },
        interactive: true,
      }),
    ).toEqual({
      left: 12,
      top: 34,
      width: 800,
      height: 600,
      zIndex: 30,
      pointerEvents: "auto",
    });
  });

  it("clips a floating webview to the mini-player frame", () => {
    expect(
      resolveHostedBrowserWebviewWrapperStyle({
        active: true,
        cornerRadius: 12,
        rect: { x: 12, y: 34, width: 360, height: 203 },
        hiddenSize: { width: 1280, height: 800 },
        interactive: true,
      }),
    ).toMatchObject({
      left: 12,
      top: 34,
      width: 360,
      height: 203,
      borderRadius: 12,
    });
  });

  it("keeps a click-held webview above a newly selected surface", () => {
    const rect = { x: 12, y: 34, width: 800, height: 600 };
    const hiddenSize = { width: 1280, height: 800 };
    const selectedStyle = resolveHostedBrowserWebviewWrapperStyle({
      active: true,
      rect,
      hiddenSize,
      interactive: true,
    });
    const heldStyle = resolveHostedBrowserWebviewWrapperStyle({
      active: true,
      automationClickHeld: true,
      rect,
      hiddenSize,
      interactive: false,
    });

    expect(heldStyle.zIndex).toBeGreaterThan(selectedStyle.zIndex);
    expect(heldStyle.pointerEvents).toBe("none");
  });

  it("keeps an inactive webview paintable while moving it offscreen", () => {
    const style = resolveHostedBrowserWebviewWrapperStyle({
      active: false,
      rect: { x: 12, y: 34, width: 800, height: 600 },
      hiddenSize: { width: 393, height: 852 },
      interactive: false,
    });

    expect(style).toEqual({
      left: HIDDEN_BROWSER_WEBVIEW_OFFSET,
      top: HIDDEN_BROWSER_WEBVIEW_OFFSET,
      width: 393,
      height: 852,
      zIndex: -1,
      pointerEvents: "none",
      visibility: "visible",
    });
  });
});
