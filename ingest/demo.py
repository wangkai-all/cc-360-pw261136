"""Stepwise, reproducible demonstrations.

Each scenario is a small state machine so that a UI (or a test) can stop at the
review point, inspect the evidence, and *then* approve a recovery.  Nothing in
here fakes a step: the partial scan is a real scan that stops, the upgrade is a
real change of the served ordering, and the recovery is executed against the
live source with its requests counted.

The four scenarios cover the brief:

``mid_harvest_upgrade``
    source switches ordering after half the records were collected.
``repeated_revisions``
    the same record is revised repeatedly while the upgrade lands.
``late_process``
    the original collector is dead; a *separate OS process* wakes up holding a
    stale cursor and is refused.
``strategy_compare``
    overlap backfill vs rebaseline, with measured request cost and the range
    each still cannot promise.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .collector import Collector
from .harvester import Harvester, RecoveryPlan
from .journal import CursorFile, Journal
from .scenario import ScenarioSpec, build_source
from .upstream import ORDER_FN
from .verify import IngestStore, VerificationReport, verify
from .versions import (KEY_ASCENDING, UPDATED_DESC, SourceProfile)

ROOT = Path(__file__).resolve().parents[1]

SCENARIOS: Dict[str, dict] = {
    "mid_harvest_upgrade": {
        "title": "Ordering upgrade mid-harvest",
        "blurb": ("Half the source is collected under id-ascending. The source is "
                  "deployed on updated_at-descending. The old cursor still resolves; "
                  "it just no longer names the same records."),
        "records": 13, "page_size": 3, "partial_pages": 2,
        "ops_after_partial": [{"op": "switch", "ordering": UPDATED_DESC}],
        "revise_after_partial": [],
        "pre_upgrade_revisions": [],
    },
    "repeated_revisions": {
        "title": "Repeated revisions across the switch",
        "blurb": ("Record 5 is revised three times while the upgrade lands. Under "
                  "updated_desc its position is rewritten each time, so even a "
                  "same-version resume cannot trust an offset."),
        "records": 13, "page_size": 3, "partial_pages": 3,
        "ops_after_partial": [{"op": "switch", "ordering": UPDATED_DESC}],
        "revise_after_partial": [5, 5, 5, 9],
        "pre_upgrade_revisions": [11],
    },
    "late_process": {
        "title": "Old collector wakes up late",
        "blurb": ("The first collector is stopped after two pages. A fresh OS "
                  "process starts later with its on-disk cursor, after the source "
                  "has been upgraded and two records edited."),
        "records": 13, "page_size": 3, "partial_pages": 2,
        "ops_after_partial": [{"op": "switch", "ordering": UPDATED_DESC},
                              {"op": "revise", "id": 3},
                              {"op": "revise", "id": 8}],
        "revise_after_partial": [],
        "pre_upgrade_revisions": [],
    },
    "strategy_compare": {
        "title": "Overlap backfill vs rebaseline",
        "blurb": ("The same interrupted scan is recovered two ways. Both plans are "
                  "rehearsed against the live source; the request difference is "
                  "measured, and each plan states the range it still cannot promise."),
        "records": 14, "page_size": 3, "partial_pages": 2,
        "ops_after_partial": [{"op": "switch", "ordering": UPDATED_DESC}],
        "revise_after_partial": [2],
        "pre_upgrade_revisions": [],
    },
}


@dataclass
class PlanBundle:
    plan: RecoveryPlan
    measured_requests: int
    rehearsal_pages: int


class DemoSession:
    def __init__(self, scenario_id: str, base_dir: Optional[Path] = None) -> None:
        if scenario_id not in SCENARIOS:
            raise KeyError(scenario_id)
        self.scenario_id = scenario_id
        self.meta = SCENARIOS[scenario_id]
        self.base = Path(base_dir) if base_dir else Path(
            tempfile.mkdtemp(prefix=f"demo-{scenario_id}-"))
        self.base.mkdir(parents=True, exist_ok=True)
        # Session artifacts stay with the session's sandbox; the repo's
        # artifacts/ directory is written only by record_demos, deliberately.
        self.artifacts = self.base / "artifacts"
        self.artifacts.mkdir(parents=True, exist_ok=True)

        self.spec = ScenarioSpec(
            scenario_id=scenario_id, source_id="catalog", seed=41,
            records=self.meta["records"], page_size=self.meta["page_size"],
            ops=[])
        self.source = build_source(self.spec)
        # pre-upgrade revisions are applied before the partial scan, so the
        # scan sees revision > 1 for those ids (a live source is never pristine)
        for rid in self.meta["pre_upgrade_revisions"]:
            self.source.revise(rid)

        self.v1 = SourceProfile("catalog", "v1", KEY_ASCENDING,
                                page_size=self.meta["page_size"])
        self.v2 = SourceProfile("catalog", "v2", UPDATED_DESC,
                                page_size=self.meta["page_size"])
        self.journal = Journal(self.base / "journal.json", "catalog")
        self.cursor_file = CursorFile(self.base / "cursor.json")
        self.collector = Collector(self.source)
        self.plans: Dict[str, PlanBundle] = {}
        self.approved: Optional[str] = None
        self.step = "ready"
        self.events: List[dict] = []
        self.partial_outcome = None
        self.resume_outcome = None
        self.verification: Optional[VerificationReport] = None
        self.ingest_stats: Optional[dict] = None
        self.late_process_result: Optional[dict] = None
        self.same_version_outcome = None
        self.partial_cursor = None
        self.store = IngestStore(self.base / "ingest.db")
        self._event("ready", f"source '{self.spec.source_id}' seeded with "
                             f"{self.meta['records']} records under v1/key_ascending")

    # -- helpers ---------------------------------------------------------
    def _event(self, step: str, detail: str, **extra) -> None:
        self.events.append({"step": step, "detail": detail, **extra})

    def _harvester(self, profile: SourceProfile) -> Harvester:
        return Harvester(self.source, profile, journal=self.journal,
                         cursor_file=self.cursor_file)

    def _manifest(self) -> List[dict]:
        """The source list, with each record's position under both orderings.

        Computed from copies, so listing the catalogue never disturbs what the
        live source is serving.
        """
        recs = self.source.records_view()
        old = ORDER_FN[KEY_ASCENDING](recs)
        new = ORDER_FN[UPDATED_DESC](recs)
        pos_old = {r.id: i for i, r in enumerate(old)}
        pos_new = {r.id: i for i, r in enumerate(new)}
        return [{
            "id": r.id, "revision": r.revision, "updated_at": r.updated_at,
            "position_v1": pos_old[r.id],
            "position_v2": pos_new[r.id],
            "moved": pos_old[r.id] != pos_new[r.id],
            "in_partial_scan": pos_old[r.id] < self.meta["partial_pages"] * self.meta["page_size"],
        } for r in new]

    # -- steps -----------------------------------------------------------
    def run_partial(self) -> dict:
        if self.step not in ("ready",):
            raise RuntimeError(f"run_partial not allowed at step {self.step}")
        h = self._harvester(self.v1)
        self.partial_outcome = h.start(scan_id="scan-a", run_id="run-1",
                                       max_pages=self.meta["partial_pages"])
        # Freeze the cursor as the partial scan left it.  The journal's cursor
        # moves on later (a recovery completes it), and the whole point of this
        # panel is the cursor a naive resume would carry.
        self.partial_cursor = self.journal.cursor()
        self.step = "partially_collected"
        self._event("partial", f"scan-a stopped after {self.partial_outcome.observed} "
                               f"records ({self.partial_outcome.stop_reason})",
                    requests=self.partial_outcome.requests)
        self._save("state")
        return self.state()

    def apply_upgrade(self) -> dict:
        if self.step == "ready":
            self.run_partial()
        if self.step != "partially_collected":
            raise RuntimeError(f"apply_upgrade not allowed at step {self.step}")
        for op in self.meta["ops_after_partial"]:
            if op["op"] == "switch":
                self.source.switch_ordering(op["ordering"])
        for rid in self.meta["revise_after_partial"]:
            self.source.revise(rid)
        self.step = "upgraded"
        self._event("upgrade",
                    f"source switched to {self.source.serving_ordering}; "
                    f"revisions applied to {self.meta['revise_after_partial'] or 'none'}")
        # naive resume attempt: deliberately attempt what a naive worker does
        h2 = self._harvester(self.v2)
        cursor = self.cursor_file.read() or self.journal.cursor()
        self.resume_outcome = h2.start(scan_id="scan-a", run_id="run-late",
                                       cursor=cursor)
        self._event("resume_refused",
                    f"naive cursor reuse -> {self.resume_outcome.status} "
                    f"({self.resume_outcome.stop_reason})")
        self._save("state")
        return self.state()

    def same_version_resume(self) -> dict:
        """Resume under the *new* version, then let a revision land mid-flight.

        The ordering is unchanged and the profile is the one the cursor was
        recorded under - so this is not an ordering bug.  It is the revision
        bug: under updated_desc the sort key mutates, so an offset recorded a
        moment ago no longer names the same position.
        """
        if self.scenario_id != "repeated_revisions":
            raise RuntimeError("only the revisions scenario demonstrates this")
        if self.step not in ("upgraded", "planned", "late_process_refused",
                             "same_version_checked"):
            raise RuntimeError(f"same_version_resume not allowed at step {self.step}")
        h2 = self._harvester(self.v2)
        probe_scan = "scan-c"
        partial = h2.start(scan_id=probe_scan, run_id="run-same-version",
                           max_pages=2)
        recorded = self.journal.cursor()
        # A write lands after scan-c recorded its offset, before the resume.
        self.source.revise(5)
        self.source.revise(5)
        outcome = h2.start(scan_id=probe_scan, run_id="run-same-version-2",
                           cursor=recorded)
        self.same_version_outcome = outcome
        self.step = "same_version_checked"
        self._event("same_version_resume",
                    f"resume under unchanged ordering -> {outcome.status} "
                    f"({outcome.stop_reason})")
        self._save("state")
        return self.state()

    def plan(self, strategy: str = "overlap_backfill") -> dict:
        if self.step not in ("upgraded", "planned", "late_process_refused",
                             "same_version_checked", "recovered"):
            raise RuntimeError("plans are only available after the upgrade is applied")
        h2 = self._harvester(self.v2)
        cursor = self.cursor_file.read() or self.journal.cursor()
        plan = h2.plan_recovery(cursor, "run-plan", strategy=strategy)
        before = len(self.source.log.entries)
        h2.rehearse(plan, "run-rehearse")
        measured = len(self.source.log.entries) - before
        self.plans[strategy] = PlanBundle(plan, measured, plan.planned_pages)
        self.step = "planned"
        self._event("plan",
                    f"{strategy}: re-scan from offset {plan.from_offset}, "
                    f"{measured} rehearsal requests, "
                    f"{len(plan.residual_ids)} residual record(s)")
        self._save("state")
        return self.state()

    def approve(self, strategy: str) -> dict:
        if strategy not in self.plans:
            raise KeyError(f"no rehearsed plan {strategy}")
        bundle = self.plans[strategy]
        if bundle.plan.approved:
            return self.state()
        bundle.plan.approved = True
        self.approved = strategy
        h2 = self._harvester(self.v2)
        scan_id = f"scan-b-{strategy}"
        before = len(self.source.log.entries)
        outcome = h2.start(scan_id=scan_id, run_id="run-approved",
                           plan=bundle.plan)
        executed = len(self.source.log.entries) - before
        self.verification = verify(self.journal, self.source.snapshot(), self.v2,
                                   accepted_scans={"scan-a", scan_id})
        self.ingest_stats = self.store.ingest(self.verification, self.journal, self.v2,
                                              rejected={s["scan_id"] for s in self.journal.scans()
                                                        if s["scan_id"] not in ("scan-a", scan_id)
                                                        and not s["scan_id"].startswith("rehearsal:")})
        self.step = "recovered"
        self._event("approved",
                    f"{strategy} approved and executed: {outcome.status}, "
                    f"{executed} live requests, verification "
                    f"{self.verification.closure} "
                    f"({self.verification.verified} verified / "
                    f"{self.verification.unobserved} unobserved)")
        self._save("state")
        self._save("verification", self.verification.to_dict())
        self._save("requests", {"entries": [e.to_dict() for e in self.source.log.entries]})
        return self.state()

    def run_late_process(self) -> dict:
        """Execute the late-resume attempt in a genuinely separate process."""
        if self.step == "ready":
            self.run_partial()
        if self.step == "partially_collected":
            self.apply_upgrade()
        script = textwrap.dedent(f"""
            import json, sys
            from pathlib import Path
            sys.path.insert(0, {str(ROOT)!r})
            from ingest.scenario import ScenarioSpec, build_source
            from ingest.journal import Journal, CursorFile
            from ingest.harvester import Harvester
            from ingest.versions import SourceProfile
            spec = ScenarioSpec(scenario_id="late", source_id="catalog", seed=41,
                                records={self.meta['records']},
                                page_size={self.meta['page_size']},
                                ops=[{{"op": "switch", "ordering": "updated_desc"}},
                                     {{"op": "revise", "id": 3}},
                                     {{"op": "revise", "id": 8}}])
            src = build_source(spec)
            prof = SourceProfile("catalog", "v2", "updated_desc",
                                 page_size={self.meta['page_size']})
            j = Journal(Path({str(self.base / 'journal.json')!r}), "catalog")
            cf = CursorFile(Path({str(self.base / 'cursor.json')!r}))
            out = Harvester(src, prof, journal=j, cursor_file=cf).start(
                scan_id="scan-a", run_id="run-late-process", cursor=cf.read())
            print("RESULT" + json.dumps({{
                "pid": __import__("os").getpid(),
                "status": out.status,
                "reason": out.stop_reason,
                "detected_ordering": out.probe.detected_ordering,
                "safe": out.assessment.safe_to_resume if out.assessment else None,
                "reasons": out.assessment.reasons if out.assessment else [],
                "cursor": cf.read().to_dict() if cf.read() else None,
            }}))
        """)
        proc = subprocess.run([sys.executable, "-I", "-c", script],
                              capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr)
        line = [l for l in proc.stdout.splitlines() if l.startswith("RESULT")][-1]
        self.late_process_result = json.loads(line[len("RESULT"):])
        self.late_process_result["stdout_tail"] = proc.stdout.splitlines()[-3:]
        self.step = "late_process_refused"
        self._event("late_process",
                    f"separate process pid {self.late_process_result['pid']} was "
                    f"{self.late_process_result['status']}")
        self._save("state")
        return self.state()

    # -- output ----------------------------------------------------------
    def state(self) -> dict:
        plans = {k: {**v.plan.to_dict(),
                     "measured_rehearsal_requests": v.measured_requests,
                     "rehearsal_pages": v.rehearsal_pages}
                 for k, v in self.plans.items()}
        accepted_ids = {"scan-a"}
        if self.same_version_outcome is not None:
            accepted_ids.add("scan-c")
        if self.approved:
            accepted_ids.add(f"scan-b-{self.approved}")
        scans = []
        for scan in self.journal.scans():
            row = dict(scan)
            if scan["scan_id"].startswith("rehearsal:"):
                row["accepted"] = False
            elif self.approved:
                row["accepted"] = scan["scan_id"] in accepted_ids
            else:
                row["accepted"] = scan["scan_id"] in accepted_ids
            scans.append(row)
        records = self.store.record_rows("catalog")
        provenance = {}
        if records:
            for row in records:
                provenance[str(row["record_id"])] = self.store.provenance(
                    "catalog", row["record_id"])
        return {
            "scenario_id": self.scenario_id,
            "title": self.meta["title"],
            "blurb": self.meta["blurb"],
            "step": self.step,
            "source": {
                "source_id": self.spec.source_id,
                "serving_ordering": self.source.serving_ordering,
                "size": self.source.size,
                "versions": [self.v1.__dict__, self.v2.__dict__],
                "manifest": self._manifest(),
            },
            "cursor": self.partial_cursor.to_dict() if self.partial_cursor else None,
            "partial": self.partial_outcome.to_dict() if self.partial_outcome else None,
            "resume": self.resume_outcome.to_dict() if self.resume_outcome else None,
            "plans": plans,
            "approved": self.approved,
            "verification": self.verification.to_dict() if self.verification else None,
            "ingest": self.ingest_stats,
            "records": records,
            "provenance": provenance,
            "scans": scans,
            "requests": [e.to_dict() for e in self.source.log.entries],
            "late_process": self.late_process_result,
            "same_version": (self.same_version_outcome.to_dict()
                             if self.same_version_outcome else None),
            "events": self.events,
        }

    def _save(self, name: str, payload: Optional[dict] = None) -> None:
        data = payload if payload is not None else self.state()
        path = self.artifacts / f"{name}.json"
        path.write_text(json.dumps(data, indent=1, sort_keys=True), encoding="utf-8")


def run_full(scenario_id: str) -> dict:
    """Headless run used by the CLI and by tests."""
    session = DemoSession(scenario_id)
    session.run_partial()
    if scenario_id == "late_process":
        session.run_late_process()
        session.plan("overlap_backfill")
        return session.approve("overlap_backfill")
    session.apply_upgrade()
    if scenario_id == "strategy_compare":
        session.plan("overlap_backfill")
        session.plan("rebaseline")
        return session.approve("rebaseline")
    session.plan("overlap_backfill")
    return session.approve("overlap_backfill")
