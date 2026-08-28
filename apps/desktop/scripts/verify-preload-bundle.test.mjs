import { assert, describe, it } from "vite-plus/test";

import { verifyPreloadBundle } from "./verify-preload-bundle.mjs";

const validPreload = `
  "desktopBridge getClientPlatform getLocalEnvironmentBootstrap";
  "PICK_FOLDER_CHANNEL __clerk_internal_electron_passkeys";
  require("electron");
`;

describe("desktop preload bundle verifier", () => {
  it("rejects dynamic imports with comments before the opening parenthesis", () => {
    assert.throws(
      () =>
        verifyPreloadBundle(`${validPreload}\nimport /* @vite-ignore */("unsupported-module");`),
      /dynamic import\(\)/,
    );
  });

  it("ignores import-like text in strings", () => {
    assert.doesNotThrow(() =>
      verifyPreloadBundle(`${validPreload}\nconst message = 'import /* comment */("module")';`),
    );
  });

  it("rejects unsupported require calls with comments before the opening parenthesis", () => {
    assert.throws(
      () => verifyPreloadBundle(`${validPreload}\nrequire /* @__PURE__ */ ("node:fs");`),
      /unsupported sandbox imports: node:fs/,
    );
  });
});
