import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";
import { init, parse } from "es-module-lexer";

await init;

const expectedSymbols = [
  "desktopBridge",
  "getClientPlatform",
  "getLocalEnvironmentBootstrap",
  "PICK_FOLDER_CHANNEL",
  "__clerk_internal_electron_passkeys",
];
export const verifyPreloadBundle = (source) => {
  const missingSymbols = expectedSymbols.filter((symbol) => !source.includes(symbol));

  if (missingSymbols.length > 0) {
    throw new Error(`Desktop preload bundle is missing: ${missingSymbols.join(", ")}`);
  }

  const triviaPattern = String.raw`(?:\s|\/\*[\s\S]*?\*\/|\/\/[^\r\n]*(?:\r?\n|$))*`;
  const runtimeImportPattern = new RegExp(
    String.raw`\brequire${triviaPattern}\(${triviaPattern}(["'])([^"']+)\1${triviaPattern}\)`,
    "g",
  );
  const runtimeImports = [...source.matchAll(runtimeImportPattern)].map((match) => match[2]);
  const runtimeRequirePattern = new RegExp(String.raw`\brequire${triviaPattern}\(`, "g");
  const runtimeRequireCount = [...source.matchAll(runtimeRequirePattern)].length;

  if (runtimeImports.length !== runtimeRequireCount) {
    throw new Error("Desktop preload bundle contains a dynamic require() call");
  }

  const [moduleImports] = parse(source);
  if (moduleImports.some((moduleImport) => moduleImport.d >= 0)) {
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
};

if (process.argv[1] && NodeURL.pathToFileURL(process.argv[1]).href === import.meta.url) {
  const preloadUrl = new URL("../dist-electron/preload.cjs", import.meta.url);
  const source = await NodeFSP.readFile(preloadUrl, "utf8");
  verifyPreloadBundle(source);
}
