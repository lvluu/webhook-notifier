# Animation Sources

This directory contains Mermaid source diagrams for animated walkthroughs of
the Webhook Notifier mechanics. Keep the `.mmd` files as the source of truth;
generated GIFs or videos are presentation artifacts.

## Recommended Workflow

Use fanfa.dev for animated documentation. It is an animated Mermaid editor that
can transform Mermaid diagrams into cinematic animations, shareable links,
embeds, and exported video files.

1. Open https://fanfa.dev/.
2. Paste or submit one `.mmd` file.
3. Pick a theme and animation style that keeps the sequence readable.
4. Export MP4/WebM, or copy the share/embed URL.
5. Save generated files next to the source using the same basename, for example
   `claim-next-batch.mp4`.

Fanfa is useful here because these diagrams are explanatory flow walkthroughs,
not recordings of a terminal session.

## CLI / Automation Options

There is no fanfa.dev CLI documented for rendering these files directly from
the repo. Use one of these paths:

- **fanfa.dev web editor** - preferred path for polished Mermaid animations,
  share links, embeds, and MP4/WebM exports.
- **Mermaid CLI (`mmdc`)** - renders Mermaid to SVG, PNG, or PDF. It can inline
  custom CSS into SVG output, which can be used for simple SVG animation, but it
  does not produce step-through GIF walkthroughs on its own.
- **Manim** - best for fully controlled, code-generated animations. Use when
  Mermaid layout is too limiting or the animation needs exact timing and custom
  visual states.
- **asciinema + agg** - best for terminal demos, not architecture diagrams.
  Record a terminal session with asciinema, then convert the `.cast` file to a
  GIF with `agg`.

## Local Static Preview

For static previews, use Mermaid CLI:

```bash
npm exec --yes @mermaid-js/mermaid-cli -- \
  -i docs/animations/claim-next-batch.mmd \
  -o /tmp/claim-next-batch.svg
```

Static SVGs are not a replacement for the animated GIFs, but they catch Mermaid
syntax errors before exporting animations.

## Diagrams

- `overall-architecture.mmd` - simplified top-level path from Event ingest,
  to Postgres as queue, to worker fan-out and Delivery status.
- `claim-next-batch.mmd` - how multi-Customer claiming amortizes sparse work
  while preserving per-Customer caps.
- `breaker-lifecycle.mmd` - how an unhealthy Webhook moves through
  `closed`, `open`, and `half_open`.
- `retry-policy.mmd` - how retryable and terminal Attempt outcomes move a
  Delivery through retry backoff or final state.
- `host-limiter.mmd` - how host saturation causes short requeue without
  consuming retry budget.

## Embedding

After exporting videos or share links, embed them from `docs/DESIGN.md` or
README pages:

```md
[ClaimNextBatch walkthrough](animations/claim-next-batch.mp4)
```

Avoid duplicating mechanism details in captions; link back to
`docs/DESIGN.md`, ADRs, and benchmark docs for the authoritative text.
