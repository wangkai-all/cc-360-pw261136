"""End-to-end tests for version-switchable harvesting.

Run: python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingest.collector import Collector, UpstreamUnavailable  # noqa: E402
from ingest.harvester import (Harvester, STOP_COMPLETE, STOP_PAUSED,  # noqa: E402
                              STOP_REQUIRES_REVIEW)
from ingest.journal import CursorFile, Journal  # noqa: E402
from ingest.probe import (Cursor, CursorAssessment, VERDICT_CONFIRMED,  # noqa: E402
                          VERDICT_INCONCLUSIVE, VERDICT_MISMATCH, assess_cursor,
                          classify, probe_ordering)
from ingest.scenario import ScenarioSpec, build_source  # noqa: E402
from ingest.upstream import UpstreamSource  # noqa: E402
from ingest.verify import IngestStore, verify  # noqa: E402
from ingest.versions import (KEY_ASCENDING, KEY_DESCENDING, UPDATED_DESC,  # noqa: E402
                             ProfileError, SourceProfile)

V1 = dict(ordering=KEY_ASCENDING, page_size=3)
V2 = dict(ordering=UPDATED_DESC, page_size=3)


def profile(version="v1", **kw):
    base = dict(V1 if version == "v1" else V2)
    base.update(kw)
    return SourceProfile("demo", version, **base)


class Sandbox:
    def __init__(self):
        self.dir = Path(tempfile.mkdtemp(prefix="ingest-test-"))
        self.journal = Journal(self.dir / "journal.json", "demo")
        self.cursors = CursorFile(self.dir / "cursor.json")

    def harvester(self, source, version="v1", **kw):
        return Harvester(source, profile(version, **kw),
                         journal=self.journal, cursor_file=self.cursors)


def source_with(n=11):
    s = UpstreamSource("demo", KEY_ASCENDING, seed=7)
    s.seed_records(n)
    return s


# ---------------------------------------------------------------------------
class TestOrderingProbe(unittest.TestCase):
    def test_detects_ascending_and_updated(self):
        s = source_with()
        r = probe_ordering(s, profile(), scan_id="p1", run_id="r")
        self.assertEqual(r.verdict, VERDICT_CONFIRMED)
        self.assertEqual(r.detected_ordering, KEY_ASCENDING)
        s.switch_ordering(UPDATED_DESC)
        r = probe_ordering(s, profile("v2"), scan_id="p2", run_id="r")
        self.assertEqual(r.verdict, VERDICT_CONFIRMED)
        self.assertEqual(r.detected_ordering, UPDATED_DESC)

    def test_detects_descending_vs_updated(self):
        s = source_with()
        s.switch_ordering(KEY_DESCENDING)
        r = probe_ordering(s, profile("v1"), scan_id="p", run_id="r")
        self.assertEqual(r.detected_ordering, KEY_DESCENDING)

    def test_single_record_page_is_inconclusive(self):
        got, evidence = classify([5], [1000])
        self.assertIsNone(got)
        self.assertTrue(any("fewer than two" in e for e in evidence))

    def test_ambiguous_evidence_refuses(self):
        # a single page whose ids ascend and whose update times ascend proves
        # neither ascending-by-key (no) nor updated_desc (no) -> stale ids
        got, _ = classify([9, 4, 7], [1, 2, 3])
        self.assertIsNone(got)

    def test_probe_costs_two_requests(self):
        s = source_with()
        probe_ordering(s, profile(), scan_id="p", run_id="r")
        self.assertEqual(len(s.log.entries), 2)

    def test_probe_mismatch_is_mismatch(self):
        s = source_with()
        s.switch_ordering(UPDATED_DESC)
        r = probe_ordering(s, profile(), scan_id="p", run_id="r")
        self.assertEqual(r.verdict, VERDICT_MISMATCH)
        self.assertFalse(r.usable)


class TestCursorSafety(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox()
        self.source = source_with()
        h = self.box.harvester(self.source)
        out = h.start(scan_id="s1", run_id="r1", max_pages=2)
        self.assertEqual(out.status, STOP_PAUSED)
        self.cursor = self.box.journal.cursor()

    def test_cursor_reachable_but_not_safe_after_reorder(self):
        self.source.switch_ordering(UPDATED_DESC)
        h = self.box.harvester(self.source, "v2")
        out = h.start(scan_id="s1", run_id="r2", cursor=self.cursor)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertFalse(out.assessment.safe_to_resume)
        self.assertTrue(out.assessment.ordering_mismatch)

    def test_cursor_reachable_and_safe_when_ordering_unchanged(self):
        h = self.box.harvester(self.source, "v1")
        out = h.start(scan_id="s1", run_id="r2", cursor=self.cursor)
        self.assertEqual(out.status, STOP_COMPLETE)
        # a safe resume is executed, so the outcome carries the probe but no
        # assessment; the assessment was computed and had to pass
        self.assertEqual(out.probe.verdict, VERDICT_CONFIRMED)
        a = h.assess(self.cursor, "r2")
        self.assertTrue(a.safe_to_resume)

    def test_equal_total_is_not_proof(self):
        """A revision can hide a missing record; the count is identical."""
        before = self.source.size
        # delete-by-revision is not supported upstream, so simulate membership
        # change with an add plus a revision: total changes, but we assert the
        # verifier never uses the total.
        self.source.switch_ordering(UPDATED_DESC)
        self.source.revise(3)
        self.assertEqual(self.source.size, before)
        h = self.box.harvester(self.source, "v2")
        out = h.start(scan_id="s1", run_id="r2", cursor=self.cursor)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertIn("total unchanged", " ".join(out.assessment.reasons))


class TestRecoveryPlans(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox()
        self.source = source_with(12)
        h = self.box.harvester(self.source)
        h.start(scan_id="s1", run_id="r1", max_pages=2)
        self.cursor = self.box.journal.cursor()
        self.source.switch_ordering(UPDATED_DESC)
        self.h2 = self.box.harvester(self.source, "v2")

    def test_plan_shows_measured_extra_requests(self):
        plan = self.h2.plan_recovery(self.cursor, "r2")
        self.assertGreater(plan.baseline_pages, 0)
        rehearsed = self.h2.rehearse(plan, "r2")
        self.assertTrue(rehearsed.rehearsed)
        rehearsal_requests = [e for e in self.source.log.entries if e.rehearsal]
        self.assertEqual(len(rehearsal_requests), rehearsed.planned_pages)
        self.assertEqual(rehearsed.extra_requests,
                         rehearsed.planned_pages + 2 - rehearsed.baseline_pages)

    def test_plan_lists_residual_ids(self):
        plan = self.h2.plan_recovery(self.cursor, "r2", strategy="overlap_backfill",
                                     backfill_pages=1)
        # unread records that now sort before the re-scan start
        self.assertIsInstance(plan.residual_ids, list)
        plan2 = self.h2.plan_recovery(self.cursor, "r3", strategy="rebaseline")
        self.assertEqual(plan2.residual_ids, [])

    def test_plan_refused_if_source_changed_after_rehearsal(self):
        plan = self.h2.plan_recovery(self.cursor, "r2", strategy="rebaseline")
        self.h2.rehearse(plan, "r2")
        plan.approved = True
        self.source.revise(1)          # a write lands between rehearsal and approval
        out = self.h2.start(scan_id="s3", run_id="r2", plan=plan)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertIn("rehearse", out.stop_reason)
        self.assertEqual(out.observed, 0)

    def test_plan_without_rehearsal_is_refused(self):
        plan = self.h2.plan_recovery(self.cursor, "r2", strategy="rebaseline")
        plan.approved = True
        out = self.h2.start(scan_id="s3", run_id="r2", plan=plan)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertIn("never rehearsed", out.stop_reason)

    def test_unapproved_plan_does_not_run(self):
        plan = self.h2.plan_recovery(self.cursor, "r2")
        self.h2.rehearse(plan, "r2")
        out = self.h2.start(scan_id="s1", run_id="r2", plan=plan)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertEqual(out.observed, 0)

    def test_approved_rebaseline_reaches_completion(self):
        plan = self.h2.plan_recovery(self.cursor, "r2", strategy="rebaseline")
        self.h2.rehearse(plan, "r2")
        plan.approved = True
        out = self.h2.start(scan_id="s2", run_id="r2", plan=plan)
        self.assertEqual(out.status, STOP_COMPLETE, out.stop_reason)
        rep = verify(self.box.journal, self.source.snapshot(), profile("v2"))
        self.assertEqual(rep.unobserved, 0)
        self.assertEqual(rep.closure, "closed")

    def test_overlap_backfill_leaves_known_residual(self):
        plan = self.h2.plan_recovery(self.cursor, "r2", strategy="overlap_backfill")
        self.h2.rehearse(plan, "r2")
        plan.approved = True
        self.h2.start(scan_id="s2", run_id="r2", plan=plan)
        rep = verify(self.box.journal, self.source.snapshot(), profile("v2"))
        # The verification is what proves completeness - not the total.  Any
        # record the overlap did not reach must be enumerated.
        self.assertEqual(rep.unobserved, len(plan.residual_ids))
        self.assertNotEqual(rep.closure, "closed")
        self.assertTrue(rep.rationale)


class TestMidHarvestUpgrade(unittest.TestCase):
    def test_upgrade_during_scan_marks_it_incomplete(self):
        box = Sandbox()
        s = source_with(12)
        h = box.harvester(s)
        out = h.start(scan_id="s1", run_id="r1", max_pages=2)
        self.assertEqual(out.observed, 6)
        s.switch_ordering(UPDATED_DESC)
        # a new run under the new version must not continue blindly
        h2 = box.harvester(s, "v2")
        out2 = h2.start(scan_id="s1", run_id="r2", cursor=box.journal.cursor())
        self.assertEqual(out2.status, STOP_REQUIRES_REVIEW)
        rep = verify(box.journal, s.snapshot(), profile("v2"))
        self.assertGreater(rep.unobserved, 0)
        self.assertNotEqual(rep.closure, "closed")


class TestRevisionsDuringUpgrade(unittest.TestCase):
    def test_repeated_revision_moves_record_and_is_flagged(self):
        box = Sandbox()
        s = source_with(12)
        h = box.harvester(s)
        h.start(scan_id="s1", run_id="r1", max_pages=2)
        cursor = box.journal.cursor()
        # record 2 was observed at revision 1 by the first scan
        self.assertEqual(cursor.revisions[2], 1)
        s.switch_ordering(UPDATED_DESC)
        s.revise(2)
        s.revise(2)
        s.revise(2)
        h2 = box.harvester(s, "v2")
        out = h2.start(scan_id="s1", run_id="r2", cursor=cursor)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertIn(2, out.assessment.revisions_drifted)
        self.assertTrue(any("revised after" in r for r in out.assessment.reasons))

    def test_revision_without_upgrade_is_still_costly(self):
        """Even under the same version, a revisable source can drift."""
        box = Sandbox()
        s = source_with(12)
        s.switch_ordering(UPDATED_DESC)
        h = box.harvester(s, "v2")
        h.start(scan_id="s1", run_id="r1", max_pages=2)
        cursor = box.journal.cursor()
        seen_id = sorted(cursor.revisions)[0]
        s.revise(seen_id)
        a = h.assess(cursor, "r2")
        self.assertIn(seen_id, a.revisions_drifted)
        self.assertFalse(a.safe_to_resume)
        out = h.start(scan_id="s1", run_id="r2", cursor=cursor)
        self.assertEqual(out.status, STOP_REQUIRES_REVIEW)
        self.assertIn(seen_id, out.assessment.revisions_drifted)


class TestVerification(unittest.TestCase):
    def test_provenance_per_record(self):
        box = Sandbox()
        s = source_with(8)
        h = box.harvester(s)
        h.start(scan_id="scan-A", run_id="r1")
        rep = verify(box.journal, s.snapshot(), profile())
        self.assertEqual(rep.closure, "closed")
        store = IngestStore(box.dir / "ingest.db")
        store.ingest(rep, box.journal, profile())
        rows = store.record_rows("demo")
        self.assertEqual(len(rows), 8)
        prov = store.provenance("demo", 3)
        self.assertEqual(len(prov), 1)
        self.assertEqual(prov[0]["scan_id"], "scan-A")
        self.assertEqual(prov[0]["ordering"], KEY_ASCENDING)

    def test_rehearsal_requests_never_verify_anything(self):
        box = Sandbox()
        s = source_with(12)
        h = box.harvester(s)
        h.start(scan_id="s1", run_id="r1", max_pages=2)
        s.switch_ordering(UPDATED_DESC)
        h2 = box.harvester(s, "v2")
        plan = h2.plan_recovery(box.journal.cursor(), "r2", strategy="rebaseline")
        h2.rehearse(plan, "r2")
        rep = verify(box.journal, s.snapshot(), profile("v2"))
        # rehearsal fetched every page but journalled nothing
        self.assertEqual(rep.observed_total, 6)
        self.assertGreater(len([e for e in s.log.entries if e.rehearsal]), 0)

    def test_missing_record_is_found_by_id_not_count(self):
        box = Sandbox()
        s = source_with(9)
        h = box.harvester(s)
        # scan pages 0 and 1 only, then add a record that sorts to the front
        h.start(scan_id="s1", run_id="r1", max_pages=2)
        s.switch_ordering(UPDATED_DESC)
        new_id = s.add_record()
        rep = verify(box.journal, s.snapshot(), profile("v2"))
        missing = [r.record_id for r in rep.records if r.state == "unobserved"]
        self.assertIn(new_id, missing)
        self.assertNotEqual(rep.closure, "closed")


class TestCollectorRetry(unittest.TestCase):
    def test_retry_after_503_is_transparent_when_page_stable(self):
        box = Sandbox()
        s = source_with(9)
        col = Collector(s)
        col.inject(1, failures=1)
        page = col.collect(0, 3, scan_id="s1", run_id="r1")
        self.assertIsNone(page.fault)
        self.assertEqual([a.outcome for a in page.attempts], ["unavailable", "ok"])

    def test_shifted_page_between_attempts_is_a_fault(self):
        box = Sandbox()
        s = source_with(9)
        col = Collector(s)
        col.inject_lost_response(1, count=1)
        real_fetch = s.fetch

        def fetch_then_upgrade(offset, limit, **kw):
            resp = real_fetch(offset, limit, **kw)
            s.switch_ordering(UPDATED_DESC)  # deployment lands between attempts
            return resp

        s.fetch = fetch_then_upgrade
        page = col.collect(0, 3, scan_id="s1", run_id="r1")
        self.assertIsNotNone(page.fault)
        self.assertEqual(page.fault.kind, "page_shifted_between_attempts")
        self.assertNotEqual(page.fault.attempts[0].ids, page.fault.attempts[-1].ids)

    def test_harvester_aborts_on_shifted_page_and_journals_nothing(self):
        box = Sandbox()
        s = source_with(9)
        col = Collector(s)
        col.inject_lost_response(1, count=1)

        # the ordering probe reads the source directly, so the collector's first
        # request is the first data page
        col.on_response = lambda request_no: (
            s.switch_ordering(UPDATED_DESC) if request_no == 1 else None)
        h = Harvester(s, profile(), journal=box.journal, collector=col)
        out = h.start(scan_id="s1", run_id="r1")
        self.assertEqual(out.status, "aborted")
        self.assertEqual(out.observed, 0)
        # the shifted page was not journalled
        self.assertEqual(box.journal.observations(), [])

    def test_upstream_unavailable_after_max_attempts(self):
        s = source_with(9)
        col = Collector(s, max_attempts=2)
        col.inject(1, failures=5)
        with self.assertRaises(UpstreamUnavailable):
            col.collect(0, 3, scan_id="s1", run_id="r1")


class TestLateProcessRecovery(unittest.TestCase):
    """A second OS process rebuilds the source and resumes with a stale cursor."""

    def test_cross_process_resume_is_refused(self):
        box = Sandbox()
        s = source_with(12)
        h = box.harvester(s)
        h.start(scan_id="s1", run_id="r1", max_pages=2)
        spec = ScenarioSpec("late", "demo", seed=7, records=12, page_size=3,
                            ops=[{"op": "switch", "ordering": UPDATED_DESC},
                                 {"op": "revise", "id": 2}])
        spec_path = box.dir / "spec.json"
        spec.save(spec_path)
        script = textwrap.dedent(f"""
            import json, sys
            sys.path.insert(0, {str(ROOT)!r})
            from ingest.scenario import ScenarioSpec, build_source
            from ingest.journal import Journal, CursorFile
            from ingest.harvester import Harvester
            from ingest.versions import SourceProfile
            from pathlib import Path
            spec = ScenarioSpec.load(Path({str(spec_path)!r}))
            src = build_source(spec)
            j = Journal(Path({str(box.dir / 'journal.json')!r}), "demo")
            cf = CursorFile(Path({str(box.dir / 'cursor.json')!r}))
            prof = SourceProfile("demo", "v2", "updated_desc", page_size=3)
            out = Harvester(src, prof, journal=j, cursor_file=cf).start(
                scan_id="s1", run_id="late", cursor=cf.read())
            print(json.dumps({{"status": out.status, "reason": out.stop_reason,
                "safe": out.assessment.safe_to_resume if out.assessment else None,
                "detected": out.probe.detected_ordering}}))
        """)
        proc = subprocess.run([sys.executable, "-I", "-c", script],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        got = json.loads(proc.stdout.strip().splitlines()[-1])
        self.assertEqual(got["status"], STOP_REQUIRES_REVIEW)
        self.assertFalse(got["safe"])
        self.assertEqual(got["detected"], UPDATED_DESC)


class TestScenarioDeterminism(unittest.TestCase):
    def test_same_spec_builds_same_source(self):
        spec = ScenarioSpec("x", "demo", seed=11, records=10, page_size=3,
                            ops=[{"op": "revise", "id": 4},
                                 {"op": "switch", "ordering": UPDATED_DESC}])
        a = build_source(spec)
        b = build_source(spec)
        self.assertEqual([r.id for r in a.ordered()], [r.id for r in b.ordered()])
        self.assertEqual([r.updated_at for r in a.ordered()],
                         [r.updated_at for r in b.ordered()])


if __name__ == "__main__":
    unittest.main()
