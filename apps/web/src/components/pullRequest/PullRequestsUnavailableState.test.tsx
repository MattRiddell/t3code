import { isValidElement, type ReactElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { PullRequestsUnavailableState } from "./PullRequestsUnavailableState";

function textOf(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textOf).join(" ");
  if (!isValidElement(node)) return "";
  return textOf((node as ReactElement<{ children?: ReactNode }>).props.children);
}

describe("PullRequestsUnavailableState", () => {
  it("can explain an unsupported environment without offering a futile retry", () => {
    const text = textOf(
      PullRequestsUnavailableState({
        title: "Pull requests unavailable",
        error: "Update this environment's T3 Code server to browse pull requests.",
      }),
    );

    expect(text).toContain("Pull requests unavailable");
    expect(text).toContain("Update this environment's T3 Code server");
    expect(text).not.toContain("Retry");
  });

  it("retains the retry for transient load failures", () => {
    const html = renderToStaticMarkup(
      <PullRequestsUnavailableState
        error="GitHub did not answer."
        onRetry={() => {}}
        browserLink={{
          href: "https://github.com/pingdotgg/t3code/pull/42",
          label: "Open on GitHub",
        }}
      />,
    );

    expect(html).toContain("Retry");
    expect(html).toContain("Open on GitHub");
    expect(html).toContain('href="https://github.com/pingdotgg/t3code/pull/42"');
    expect(html).toContain('target="_blank"');
    expect(html).toContain('rel="noopener noreferrer"');
  });

  it("can offer the browser without offering a retry", () => {
    const html = renderToStaticMarkup(
      <PullRequestsUnavailableState
        error="This server cannot read the pull request."
        browserLink={{
          href: "https://gitlab.example.test/group/repo/-/merge_requests/9",
          label: "Open on GitLab",
        }}
      />,
    );

    expect(html).toContain("Open on GitLab");
    expect(html).not.toContain("Retry");
  });

  it("renders no action content without a retry or browser target", () => {
    const html = renderToStaticMarkup(
      <PullRequestsUnavailableState error="This project has no known remote." />,
    );

    expect(html).not.toContain('data-slot="empty-content"');
    expect(html).not.toContain("href=");
  });
});
