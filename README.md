# Cursor safety across a source-version upgrade

A from-scratch ingest system for a source whose **sorting convention changed**:
it used to page by record id ascending, and now pages by `updated_at`
descending. Old numeric cursors are still *accepted* by the source — they simply
no longer name the same records. The system treats that difference as the whole
problem.

No third-party services. Python 3.11 standard library only (SQLite, `http.server`,
`subprocess`); the web UI is plain HTML/CSS/JS with no dependencies.

```bash
python3 -m unittest discover -s tests -v     # 28 tests, all scenarios
python3 -m ingest.record_demos               # writes artifacts/*.json
python3 -m ingest.server --port 8765         # review UI at http://127.0.0.1:8765/
python3 -m ingest.server --run late_process  # one scenario, headless JSON
```

## The rule the code enforces

> A cursor is a position *relative to the ordering it was recorded under*.
> Before any cursor is reused, the ordering currently being served must be
> **demonstrated**, not assumed.

Three consequences, each implemented rather than documented:

1. **No version string is trusted.** `ingest/probe.py` fetches two overlapping
   pages at fixed offsets (2 requests, never a walk) and classifies which
   ordering the id and timestamp series are compatible with. Ambiguity is reported
   as `inconclusive` and the harvester refuses to run; a probe that contradicts
   the profile is `mismatch`.
2. **No silent resume.** `Harvester.start()` probes first, then assesses the
   cursor: ordering match, boundary ids still upstream of the offset, revisions
   drifted since the cursor recorded them. Reachability is never enough.
3. **No count-based completeness.** `ingest/verify.py` decides closure per id and
   per unbroken offset run. Equal totals appear in the report only as
   *"corroboration only, not proof"*.

## What is in the box

| Module | Job |
|---|---|
| `ingest/versions.py` | Immutable source profiles; `ordering` is explicit (`key_ascending`, `key_descending`, `updated_desc`). An `updated_desc` profile is forced to be `revisable` — its sort key *is* the revision timestamp. |
| `ingest/upstream.py` | Deterministic in-process source: seeded ids, permuted update times (so the new order is not just descending id), request log with `has_more`, per-request revisions. |
| `ingest/probe.py` | Two-page ordering probe, cursor assessment, cursor with boundary ids + per-id revisions. |
| `ingest/harvester.py` | Probe-then-run; recovery plans with measured cost; rehearsal; execution. |
| `ingest/collector.py` | Retrying edge. Separates "never reached the server" from "response lost after the server served it"; the latter can return a shifted page, which becomes a *fault*. |
| `ingest/journal.py` | Atomic, durable scan journal (observations → then cursor). Cursor file kept separate on purpose. |
| `ingest/verify.py` | Per-record verification, offset-run grouping, SQLite projection with per-record provenance. |
| `ingest/scenario.py` | Scenario as **data** (ops list), so another OS process can rebuild the identical upstream. |
| `ingest/demo.py` | Stepwise sessions the UI drives: partial → upgrade → plan → rehearse → approve. |
| `ingest/server.py`, `web/` | Review UI and JSON API. |

## Recovery plans: overlap vs rebaseline

A plan is **rehearsed** before it can be approved: its pages are fetched from the
live source and counted, but journalled as `rehearsal:*` and excluded from every
verification input. The two strategies, with real numbers from
`artifacts/strategy_compare.json` (14 records, page size 3, cursor at offset 6):

| | Overlap backfill | Rebaseline |
|---|---|---|
| start | offset 3 (one page re-read) | offset 0 |
| pages + probe | 4 + 2 | 5 + 2 |
| naive continuation would be | 3 pages | 3 pages |
| **extra requests** | **+3** | **+4** |
| residual ids it still misses | `#11` | none |
| what it still cannot promise | shifts larger than the re-read window; any revision outside it; concurrent inserts | concurrent writes during the pass itself |

The **residual list is computed**, not hand-written: records no accepted scan has
observed and that the plan will not request. The plan also carries its rehearsal
**state digest**; if the source changes between rehearsal and approval, execution
is refused with *"re-source changed … re-rehearse"* rather than running on stale
numbers. `may_miss` spells out the windows no plan can close.

## The three mandated demonstrations

All are stepwise in the UI and headless in tests. Recorded states are in
`artifacts/*.json`; screenshots in `artifacts/ui-*.png`.

**1. Upgrade mid-harvest** (`mid_harvest_upgrade`, also `strategy_compare`)
Half the source read under `key_ascending`; source switched to `updated_desc`;
the naive resume is refused — *"cursor recorded under key_ascending but server
serves updated_desc; offset 6 names a different record set"*, *"boundary ids
moved across the cursor"*, *"total unchanged at 13 (corroboration only, not
proof)"*. In `mid_harvest_upgrade`, overlap recovery verifies 11/13 and **stays
open** with `#8, #11` enumerated as unobserved. In `strategy_compare` the same
refusal is offered both plans: overlap leaves `#11` residual and stays open,
rebaseline verifies 14/14 and closes.

**2. Repeated revisions across the switch** (`repeated_revisions`)
Record 5 revised three times. Under `updated_desc` the sort key mutates, so the
demo additionally runs a **same-version** resume (scan-c, unchanged ordering) and
it is *also* refused: `1 records revised after the scan recorded them; their new
position is unknown ([5])`. This separates the ordering bug from the revision bug.

**3. Old process wakes up late** (`late_process`)
The partial scan is real; the resume attempt then runs in a **separate OS
process** (`python3 -I`, pid shown in the UI) that rebuilds the source from the
scenario spec and reads the on-disk cursor. It is refused, with the drifted ids
listed. This is a cross-process test, not a second object in the same
interpreter (`tests/test_ingest.py::TestLateProcessRecovery`).

## The web review surface

- **Run** — source state, the frozen partial cursor, the refused naive resume
  with its reasons, then plan cards and a measured plan chart. Approve opens a
  dialog that restates the residual ids and the "cannot promise" list; approval
  is a separate, explicit second call (and API action).
- **Source list** — every record's position under v1 and v2, which moved, which
  were in the partial scan, verification state; click a record for provenance.
- **Verification** — closure badge, coverage bar, contiguous request runs with
  offset ranges and served ordering, gaps, and the full request log with live vs
  rehearsal labels.
- **Scans** — each scan's version, ordering, status, stop reason, and whether it
  counts as evidence (rejected / rehearsal / accepted).

Charts use the validated reference palette (blue / orange / aqua / yellow,
checked with the palette validator for both modes), 2px gaps, direct labels, a
legend, hover + keyboard tooltips, and a table view for every plotted number.
Dark mode is a selected step set, not an inversion.

## Design decisions worth knowing

- **`updated_desc` is not `key_descending`.** Seed update times are a
  deterministic permutation of ids, so the probe cannot pass by accident. The
  probe rejects `updated_desc` when ids descend but timestamps do not.
- **The cursor is written after the observations behind it**, so a crash can
  leave "observations without a cursor" (safe: re-read and dedupe) but never
  "cursor without its pages".
- **A retry is not evidence-neutral.** If the edge dropped a response *after* the
  server served it, the retry may return a different page. That raises
  `page_shifted_between_attempts`, the harvester aborts with `aborted`, and the
  shifted page is not journalled.
- **Rehearsal is registered but never evidence** — it shows up in the scans tab
  labelled `rehearsal (not evidence)`.
- **No lookup-by-id endpoint** exists upstream (`fetch_page_containing` raises),
  so the system cannot cheat around offset semantics.
