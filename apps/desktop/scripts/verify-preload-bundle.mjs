import * as NodeFSP from "node:fs/promises";

const preloadUrl = new URL("../dist-electron/preload.cjs", import.meta.url);
const source = await NodeFSP.readFile(preloadUrl, "utf8");

const expectedSymbols = [
  "desktopBridge",
  "getClientPlatform",
  "getLocalEnvironmentBootstrap",
  "PICK_FOLDER_CHANNEL",
  "__clerk_internal_electron_passkeys",
];
const missingSymbols = expectedSymbols.filter((symbol) => !source.includes(symbol));

if (missingSymbols.length > 0) {
  throw new Error(`Desktop preload bundle is missing: ${missingSymbols.join(", ")}`);
}

const runtimeImportPattern = /\brequire\(\s*(["'])([^"']+)\1\s*\)/g;
const runtimeImports = [...source.matchAll(runtimeImportPattern)].map((match) => match[2]);
const runtimeRequireCount = [...source.matchAll(/\brequire\s*\(/g)].length;

if (runtimeImports.length !== runtimeRequireCount) {
  throw new Error("Desktop preload bundle contains a dynamic require() call");
}

if (/\bimport\s*\(/.test(source)) {
  throw new Error("Desktop preload bundle contains a dynamic import() call");
}

const sandboxModules = new Set(["electron", "events", "timers", "url"]);
const unsupportedImports = [...new Set(runtimeImports)]
  .filter((moduleName) => !sandboxModules.has(moduleName))
  .toSorted();

if (unsupportedImports.length > 0) {
  throw new Error(
    `Desktop preload bundle contains unsupported sandbox imports: ${unsupportedImports.join(", ")}`,
  );
}
