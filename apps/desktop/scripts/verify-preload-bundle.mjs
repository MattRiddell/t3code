import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";
import { parse } from "acorn";

const expectedDesktopBridgeApis = [
  "getClientPlatform",
  "getLocalEnvironmentBootstraps",
  "pickFolder",
];
const clerkPasskeysGlobal = "__clerk_internal_electron_passkeys";

const isSyntaxNode = (value) =>
  typeof value === "object" && value !== null && "type" in value && typeof value.type === "string";

const readStaticName = (node) => {
  if (node.type === "Identifier") return node.name;
  if (node.type === "Literal" && typeof node.value === "string") return node.value;
  return undefined;
};

const readMemberName = (node) => {
  if (node.type !== "MemberExpression") return undefined;
  if (!node.computed && node.property.type === "Identifier") return node.property.name;
  return readStaticName(node.property);
};

const isContextBridge = (node) =>
  (node.type === "Identifier" && node.name === "contextBridge") ||
  (node.type === "MemberExpression" && readMemberName(node) === "contextBridge");

const inspectBundle = (source) => {
  const runtimeImports = [];
  const exposedGlobals = new Set();
  const desktopBridgeApis = new Set();
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

    if (
      node.type === "CallExpression" &&
      node.callee.type === "MemberExpression" &&
      readMemberName(node.callee) === "exposeInMainWorld" &&
      isContextBridge(node.callee.object)
    ) {
      const [globalName, api] = node.arguments;
      const name = globalName === undefined ? undefined : readStaticName(globalName);
      if (name !== undefined) exposedGlobals.add(name);

      if (name === "desktopBridge" && api?.type === "ObjectExpression") {
        for (const property of api.properties) {
          if (property.type !== "Property") continue;
          const propertyName = readStaticName(property.key);
          if (propertyName !== undefined) desktopBridgeApis.add(propertyName);
        }
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
  return { desktopBridgeApis, exposedGlobals, runtimeImports };
};

export const verifyPreloadBundle = (source) => {
  const { desktopBridgeApis, exposedGlobals, runtimeImports } = inspectBundle(source);
  const missingApis = expectedDesktopBridgeApis.filter((api) => !desktopBridgeApis.has(api));
  if (!exposedGlobals.has("desktopBridge")) missingApis.unshift("desktopBridge exposure");
  if (!exposedGlobals.has(clerkPasskeysGlobal)) missingApis.push(`${clerkPasskeysGlobal} exposure`);

  if (missingApis.length > 0) {
    throw new Error(`Desktop preload bundle is missing executable APIs: ${missingApis.join(", ")}`);
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
