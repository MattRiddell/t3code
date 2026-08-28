import { ExternalLinkIcon, GitPullRequestIcon, RefreshCwIcon } from "lucide-react";

import { Button } from "../ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "../ui/empty";

export function PullRequestsUnavailableState({
  title = "Could not load pull requests",
  error,
  onRetry,
  browserLink,
}: {
  title?: string;
  error: string;
  onRetry?: () => void;
  browserLink?: {
    readonly href: string;
    readonly label: string;
  };
}) {
  return (
    <Empty className="px-4 py-16 md:px-4">
      <EmptyMedia variant="icon">
        <GitPullRequestIcon />
      </EmptyMedia>
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        {/* The caller names the fix — update the environment, install gh, sign in — so this
            shows its message rather than trying to infer one from the failure text. */}
        <EmptyDescription>{error}</EmptyDescription>
      </EmptyHeader>
      {onRetry || browserLink ? (
        <EmptyContent className="flex-row flex-wrap justify-center gap-2">
          {onRetry ? (
            <Button size="sm" variant="outline" onClick={onRetry}>
              <RefreshCwIcon className="size-3.5" />
              Retry
            </Button>
          ) : null}
          {browserLink ? (
            <Button
              size="sm"
              variant="outline"
              render={<a href={browserLink.href} target="_blank" rel="noopener noreferrer" />}
            >
              <ExternalLinkIcon aria-hidden className="size-3.5" />
              {browserLink.label}
            </Button>
          ) : null}
        </EmptyContent>
      ) : null}
    </Empty>
  );
}
