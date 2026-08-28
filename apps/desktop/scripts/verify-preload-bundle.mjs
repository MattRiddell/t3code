import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";
import { parse } from "acorn";

const expectedSymbols = [
  "desktopBridge",
  "getClientPlatform",
  "getLocalEnvironmentBootstrap",
  "PICK_FOLDER_CHANNEL",
  "__clerk_internal_electron_passkeys",
];

const isSyntaxNode = (value) =>
  typeof value === "object" && value !== null && "type" in value && typeof value.type === "string";

const readRuntimeImports = (source) => {
  const runtimeImports = [];
  const visit = (node) => {
    if (node.type === "ImportExpression") {
      throw new Error("Desktop preload bundle contains a dynamic import() call");
    }

    if (node.type === "CallExpression" && node.callee.type === "Identifier") {
      if (node.callee.name === "require") {
        const [argument] = node.arguments;
        if (node.arguments.length !== 1 || argument?.type !== "Literal") {
          throw new Error("Desktop preload bundle contains a dynamic require() call");
        }
        if (typeof argument.value !== "string") {
          throw new Error("Desktop preload bundle contains a dynamic require() call");
        }
        runtimeImports.push(argument.value);
      }
    }

    for (const child of Object.values(node)) {
      if (Array.isArray(child)) {
        for (const item of child) {
          if (isSyntaxNode(item)) visit(item);
        }
      } else if (isSyntaxNode(child)) {
        visit(child);
      }
    }
  };

  visit(parse(source, { ecmaVersion: "latest", sourceType: "script" }));
  return runtimeImports;
};

export const verifyPreloadBundle = (source) => {
  const missingSymbols = expectedSymbols.filter((symbol) => !source.includes(symbol));

  if (missingSymbols.length > 0) {
    throw new Error(`Desktop preload bundle is missing: ${missingSymbols.join(", ")}`);
  }

  const runtimeImports = readRuntimeImports(source);

  const sandboxModules = new Set(["electron", "events", "timers", "url"]);
  const unsupportedImports = [...new Set(runtimeImports)]
    .filter((moduleName) => !sandboxModules.has(moduleName))
    .toSorted();

  if (unsupportedImports.length > 0) {
    throw new Error(
      `Desktop preload bundle contains unsupported sandbox imports: ${unsupportedImports.join(", ")}`,
    );
  }
};

if (process.argv[1] && NodeURL.pathToFileURL(process.argv[1]).href === import.meta.url) {
  const preloadUrl = new URL("../dist-electron/preload.cjs", import.meta.url);
  const source = await NodeFSP.readFile(preloadUrl, "utf8");
  verifyPreloadBundle(source);
}
