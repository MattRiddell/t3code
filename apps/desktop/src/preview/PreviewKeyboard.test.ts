import { describe, expect, it } from "vite-plus/test";

import { makePreviewAutomationKeySequence } from "./PreviewKeyboard.ts";

describe("preview keyboard packets", () => {
  it("sends Enter as a native down/up pair without a text packet", () => {
    expect(makePreviewAutomationKeySequence({ key: "Enter" })).toEqual({
      keyDown: { type: "rawKeyDown", keyCode: "Enter", modifiers: [] },
      keyUp: { type: "keyUp", keyCode: "Enter", modifiers: [] },
      signal: {
        kind: "key",
        key: "Enter",
        code: "Enter",
        meta: false,
        shift: false,
        control: false,
        alt: false,
      },
    });
  });

  it("separates printable key events from text insertion", () => {
    expect(makePreviewAutomationKeySequence({ key: "z" })).toEqual({
      keyDown: { type: "rawKeyDown", keyCode: "Z", modifiers: [] },
      char: { type: "char", keyCode: "z", modifiers: [] },
      keyUp: { type: "keyUp", keyCode: "Z", modifiers: [] },
      signal: {
        kind: "key",
        key: "z",
        code: "KeyZ",
        meta: false,
        shift: false,
        control: false,
        alt: false,
      },
    });
  });

  it("uses native modifier chords without inserting text", () => {
    expect(makePreviewAutomationKeySequence({ key: "a", modifiers: ["Meta"] })).toEqual({
      keyDown: { type: "rawKeyDown", keyCode: "A", modifiers: ["meta"] },
      keyUp: { type: "keyUp", keyCode: "A", modifiers: ["meta"] },
      signal: {
        kind: "key",
        key: "a",
        code: "KeyA",
        meta: true,
        shift: false,
        control: false,
        alt: false,
      },
    });
  });

  it("keeps editing-chord modifiers on each native packet", () => {
    expect(makePreviewAutomationKeySequence({ key: "z", modifiers: ["Shift", "Meta"] })).toEqual({
      keyDown: { type: "rawKeyDown", keyCode: "Z", modifiers: ["shift", "meta"] },
      keyUp: { type: "keyUp", keyCode: "Z", modifiers: ["shift", "meta"] },
      signal: {
        kind: "key",
        key: "Z",
        code: "KeyZ",
        meta: true,
        shift: true,
        control: false,
        alt: false,
      },
    });
  });

  it("maps shifted printable keys to a base key plus Shift", () => {
    expect(makePreviewAutomationKeySequence({ key: "!" })).toEqual({
      keyDown: { type: "rawKeyDown", keyCode: "1", modifiers: ["shift"] },
      char: { type: "char", keyCode: "!", modifiers: ["shift"] },
      keyUp: { type: "keyUp", keyCode: "1", modifiers: ["shift"] },
      signal: {
        kind: "key",
        key: "!",
        code: "Digit1",
        meta: false,
        shift: true,
        control: false,
        alt: false,
      },
    });
  });

  it("does not insert text for modified shifted keys", () => {
    expect(makePreviewAutomationKeySequence({ key: "1", modifiers: ["Control", "Shift"] })).toEqual(
      {
        keyDown: {
          type: "rawKeyDown",
          keyCode: "1",
          modifiers: ["control", "shift"],
        },
        keyUp: { type: "keyUp", keyCode: "1", modifiers: ["control", "shift"] },
        signal: {
          kind: "key",
          key: "!",
          code: "Digit1",
          meta: false,
          shift: true,
          control: true,
          alt: false,
        },
      },
    );
  });

  it("uses Electron accelerator names for arrows and function keys", () => {
    expect(makePreviewAutomationKeySequence({ key: "ArrowLeft" }).keyDown.keyCode).toBe("Left");
    expect(makePreviewAutomationKeySequence({ key: "F12" }).keyDown.keyCode).toBe("F12");
  });

  it("uses a literal space only for the char packet", () => {
    expect(makePreviewAutomationKeySequence({ key: "Space" })).toEqual({
      keyDown: { type: "rawKeyDown", keyCode: "Space", modifiers: [] },
      char: { type: "char", keyCode: " ", modifiers: [] },
      keyUp: { type: "keyUp", keyCode: "Space", modifiers: [] },
      signal: {
        kind: "key",
        key: " ",
        code: "Space",
        meta: false,
        shift: false,
        control: false,
        alt: false,
      },
    });
  });

  it("does not forward unchecked Unicode keys to Electron", () => {
    expect(() => makePreviewAutomationKeySequence({ key: "\u00e9" } as never)).toThrow(
      "Use preview_type for Unicode text.",
    );
  });
});
