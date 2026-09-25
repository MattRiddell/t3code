// Fleet patch (board #13615). The systemd unit raises this server's V8 heap with
// NODE_OPTIONS=--max-old-space-size=N and sets T3_SCRUB_NODE_OPTIONS=1. Node reads
// NODE_OPTIONS once, before any module runs, so deleting it here keeps the raised
// limit for this process only: provider sessions, terminals and every tool they
// spawn from process.env keep their own heap defaults. Imported first by bin.ts.
if (process.env.T3_SCRUB_NODE_OPTIONS === "1") {
  delete process.env.NODE_OPTIONS;
  delete process.env.T3_SCRUB_NODE_OPTIONS;
}

export {};
