# Maintaining this guide

This guide is a living document. It tracks the code, so it changes when the code changes, and it is
deliberately incomplete in ways worth writing down.

## The rule

**Follow the code, not the design.** Every path, class name, topic, setting and number here was read
out of the source tree. Where the design describes a target the code has not reached, say so in that
section rather than leaving the reader to discover it.

Two consequences that are easy to get wrong:

**Never cite a file that is not in the repository.** The design documents, the ADR files and the
delivery record are untracked by policy. A reader who clones this repository does not have them, so a
citation to one is a dead end. Refer to a decision by its ADR number, which also appears in the source
comments, and cite only paths a reader can open.

**Never promote a target to an achievement.** The architecture states 50,000 events/sec and a
sub-200ms risk budget. No sustained-throughput run has happened, so those are targets everywhere in
this guide and will stay targets until Phase 8 produces a measurement.

## What this guide does not cover yet

Two chapters now cover intent rather than delivered code, and are marked as such:
[What this platform is for](purpose.md) and [Target architecture](architecture.md). They are the only
places in the guide where the design leads and the code follows.

What is still missing gets written as the system grows into it:

**A chapter per phase, as each phase lands.** Deterministic streaming, the durable audit sink, CDC
migration, candidate screening, the agent tool boundary, the precedent graph. Each one becomes a page
written the same way as [Follow one trade](spine.md): one path, traced through real code, with the
test that proves each claim.

**Operations.** Runbooks, what to do when the DLQ fills, how to replay, how to read the dashboards.
This needs services that run somewhere first.

**Measured performance.** The throughput and latency evidence from Phase 8, replacing every target in
this guide with a number and the conditions it was measured under.

Until each of those exists in the code, writing the chapter would mean writing a plan, and the guide
does not carry plans.

## Updating it with a change

Treat the guide as part of the change, not as follow-up work. A pull request that adds a service and
leaves the guide describing the world without it has made the guide wrong, which is worse than leaving
it silent.

**In the same commit as the code:**

1. Update the page that covers the area you touched. If a new service lands, it needs its own section
   and a row in the [delivery state table](index.md#delivery-state-at-a-glance).
2. Add its behaviours to the [Code to proof map](proof.md), naming the real test methods.
3. Move anything it completes out of [Specified, not built](not-built.md).
4. If it changes a topic, a subject, a policy or a partition count, update
   [Topics and schemas](topics.md).
5. If it cost you an hour to work something out, put it in [Gotchas](gotchas.md) while you still
   remember it.
6. Update the diagrams it makes wrong. Sources are in `guide/docs/diagrams/`.

**Then verify:**

```bash
cd guide && mkdocs build --strict     # fails on a broken link or a missing diagram
```

CI runs the same command on every pull request that touches `guide/`, and publishes from `main`.

## Editing a diagram

Each diagram is a `.drawio` source plus an SVG exported from it. The SVG is what the pages embed; the
`.drawio` file is what you change.

```bash
cd guide/docs/diagrams
drawio -x -f svg -e -b 10 --embed-svg-fonts false -o NAME.svg NAME.drawio
```

Four things that are not optional:

`--embed-svg-fonts false`. With font embedding on, draw.io writes a raster PNG fallback for every text
label and the file grows from about 40KB to over 1MB.

**Draw for the content column.** Diagrams are about 980px wide with 14px body text, so they render at
roughly 1:1 rather than being shrunk to fit. A wider diagram is a diagram nobody can read without
clicking it.

**Angle brackets disappear.** draw.io renders labels as HTML, so `List<Grant>` in a label is parsed as
a tag. Write "List of Grant".

**A text cell needs `whiteSpace=wrap` in its style.** Without it a long label renders on one line, and
the export silently widens the whole canvas to fit. A diagram that comes out 2000px wide when you drew
it 980px wide has one of these in it.

## Keeping it honest

The value of this guide is that a reader can check it. Two habits protect that:

Before claiming a test proves something, open the test. Before citing a number, count it.

When a section becomes wrong and you have not got time to fix it properly, delete it. A missing
section is a gap; a wrong one is a trap.
