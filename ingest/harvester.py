"""The harvester.

Two things are deliberately separated:

*Pagination* walks the source under one declared ordering and one profile
version, requesting pages by offset.  It can be interrupted; it journals every
observation and only advances its cursor after the observations behind it are
durable.

*Starting* is never implicit.  :meth:`Harvester.start` probes the live ordering
before it does anything else.  It will not resume a cursor just because the
offset is reachable: it asks the probe, then assesses the cursor against the
live source.  A caller can request a recovery plan instead of a resume, and the
plan can be rehearsed (pages fetched and counted, nothing journalled) before it
is approved.

There is intentionally no "just continue from the cursor" entry point.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .journal import CursorFile, Journal, Observation
from .probe import (Cursor, CursorAssessment, ProbeResult, assess_cursor, probe_ordering)
from .upstream import UpstreamSource
from .versions import ProfileError, SourceProfile

STOP_COMPLETE = "complete"
STOP_PAUSED = "paused"
STOP_INTERRUPTED = "interrupted"
STOP_REQUIRES_REVIEW = "requires_review"
STOP_REFUSED = "refused"
STOP_ABORTED = "aborted"


class PageIntegrityFault(RuntimeError):
    """Raised when an edge retry produced a page that moved under it."""


@dataclass
class ScanOutcome:
    scan_id: str
    run_id: str
    status: str
    stop_reason: Optional[str]
    requests: int
    observed: int
    probe: Optional[ProbeResult] = None
    assessment: Optional[CursorAssessment] = None
    plan: Optional["RecoveryPlan"] = None

    def to_dict(self) -> dict:
        return {
            "scan_id": self.scan_id, "run_id": self.run_id,
            "status": self.status, "stop_reason": self.stop_reason,
            "requests": self.requests, "observed": self.observed,
            "probe": self.probe.to_dict() if self.probe else None,
            "assessment": self.assessment.to_dict() if self.assessment else None,
            "plan": self.plan.to_dict() if self.plan else None,
        }


@dataclass
class RecoveryPlan:
    """What a recovery would request, and what it still cannot promise.

    ``extra_requests`` is the difference between this plan's planned pages
    (plus both ordering probes) and the pages a plain continuation from the
    cursor would have needed.  Both are counted against the live source during
    rehearsal; neither is an estimate of unseen data.

    ``residual_ids`` is the honest part: records that, in the *current* live
    view, would still be missed by this plan.  It is computed, not estimated,
    and it is why a plan is never called "complete" before verification.
    """
    source_id: str
    version: str
    strategy: str                    # "overlap_backfill" | "rebaseline"
    from_offset: int
    page_size: int
    planned_pages: int
    baseline_pages: int
    extra_requests: int
    probe_requests: int
    candidate_ids: List[int]
    already_covered: List[int]
    rechecked_ids: List[int]
    residual_ids: List[int]
    residual_detail: List[dict]
    may_miss: List[dict]
    why_not_safe_to_resume: List[str]
    witnessed_by: List[dict]
    rehearsed: bool = False
    approved: bool = False
    # Digest of the live view at rehearsal time.  Approval is bound to it: if
    # the source changed between rehearsal and approval, the measured numbers
    # are stale and the plan is refused rather than executed.
    state_digest: Optional[str] = None
    rehearsed_at_ordering: Optional[str] = None

    @property
    def may_miss_desc(self) -> str:
        return "; ".join(m["window"] for m in self.may_miss) or "none identified"

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id, "version": self.version,
            "strategy": self.strategy, "from_offset": self.from_offset,
            "page_size": self.page_size, "planned_pages": self.planned_pages,
            "baseline_pages": self.baseline_pages,
            "extra_requests": self.extra_requests,
            "probe_requests": self.probe_requests,
            "candidate_ids": list(self.candidate_ids),
            "already_covered": list(self.already_covered),
            "rechecked_ids": list(self.rechecked_ids),
            "residual_ids": list(self.residual_ids),
            "residual_detail": list(self.residual_detail),
            "may_miss": list(self.may_miss),
            "why_not_safe_to_resume": list(self.why_not_safe_to_resume),
            "witnessed_by": list(self.witnessed_by),
            "rehearsed": self.rehearsed, "approved": self.approved,
            "state_digest": self.state_digest,
            "rehearsed_at_ordering": self.rehearsed_at_ordering,
        }


class Harvester:
    def __init__(self, source: UpstreamSource, profile: SourceProfile, *,
                 journal: Journal, cursor_file: Optional[CursorFile] = None,
                 collector: Optional["Collector"] = None,
                 clock: Optional[Callable[[], None]] = None) -> None:
        self.source = source
        self.profile = profile
        self.journal = journal
        self.cursor_file = cursor_file
        self.collector = collector
        self._pause = threading.Event()
        self._refuse_after: Optional[int] = None

    # -- controls --------------------------------------------------------
    def pause(self) -> None:
        self._pause.set()

    def interrupted_after(self, pages: int) -> None:
        """Demo hook: simulate a process killed after *pages* successful pages."""
        self._refuse_after = pages

    # -- live sort detection --------------------------------------------
    def probe(self, scan_id: str, run_id: str) -> ProbeResult:
        service_scan = f"probe:{scan_id}"
        return probe_ordering(self.source, self.profile,
                              scan_id=service_scan, run_id=run_id)

    def _fetch(self, offset: int, limit: int, *, scan_id: str, run_id: str):
        if self.collector is not None:
            page = self.collector.collect(offset, limit, scan_id=scan_id,
                                          run_id=run_id)
            if page.fault is not None:
                # The retry returned a different page.  Do not journal it: the
                # scan's coverage claim would be built on a page that no longer
                # matched its predecessors.
                raise PageIntegrityFault(page.fault.detail)
            self.journal.note_requests(scan_id, [self.source.log.entries[-1].seq])
            return page.response
        resp = self.source.fetch(offset, limit, scan_id=scan_id, run_id=run_id)
        self.journal.note_requests(scan_id, [self.source.log.entries[-1].seq])
        return resp

    def assess(self, cursor: Cursor, run_id: str) -> CursorAssessment:
        probe = self.probe(cursor.scan_id, run_id)
        return assess_cursor(cursor, self.profile, probe, self.source)

    # -- plans -----------------------------------------------------------
    def plan_recovery(self, cursor: Cursor, run_id: str, *,
                      strategy: str = "overlap_backfill",
                      backfill_pages: int = 1) -> RecoveryPlan:
        if strategy not in ("overlap_backfill", "rebaseline"):
            raise ValueError(f"unknown strategy {strategy!r}")
        probe = self.probe(cursor.scan_id, run_id)
        assessment = assess_cursor(cursor, self.profile, probe, self.source)
        total = self.source.size
        ps = self.profile.page_size

        all_ids = [r.id for r in self.source.ordered()]
        covered = set(cursor.revisions)
        if self.cursor_file is not None and self.journal.cursor() is not None:
            covered |= set(self.journal.cursor().revisions)

        if strategy == "rebaseline":
            from_offset = 0
            planned = (total + ps - 1) // ps
            candidate_ids = all_ids
        else:
            # Re-scan the tail of what was already read, then continue.  The
            # overlap is what re-establishes a trustworthy position under the
            # new ordering; one page is the smallest honest default.
            back = max(1, backfill_pages)
            from_offset = max(0, cursor.offset - back * ps)
            planned = max(0, (total - from_offset + ps - 1) // ps)
            candidate_ids = all_ids[from_offset:]

        # What a naive continuation would have cost: from the cursor, to the end.
        naive_from = min(cursor.offset, total)
        baseline_pages = max(0, (total - naive_from + ps - 1) // ps)
        probe_requests = 2  # two pages fetched by the ordering probe

        already = [i for i in candidate_ids if i in covered]
        # "Already covered" is what the earlier scan had already observed; the
        # overlap deliberately re-requests part of it.  Anything in candidate
        # ids that was covered is therefore a re-read, not a residual.
        rechecked = sorted(set(candidate_ids) & covered)

        may_miss: List[dict] = []
        residual_ids: List[int] = []
        residual_detail: List[dict] = []
        candidate_set = set(candidate_ids)
        if strategy == "overlap_backfill":
            # Anything the *previous* scan never saw and this plan will not
            # request is a residual - the concrete loss the overlap trades
            # against re-reading a page.  In a pure reordering the new order is
            # a permutation of the same records, so only revisions/insertions
            # can create residual; when unread records exist, they are listed.
            residual_ids = [i for i in all_ids if i not in covered and i not in candidate_set]
            for i in residual_ids:
                residual_detail.append({
                    "id": i,
                    "why": "never observed by an accepted scan and lies before the "
                           "plan's re-scan start under the new ordering",
                })
            # Records revised after their observation but outside the overlap
            # are stale, not necessarily missed; the verifier decides.
            may_miss.append({
                "window": (f"positions {from_offset}..{cursor.offset - 1} under "
                           f"{self.profile.ordering} are the only re-read range"),
                "reason": "records that moved from beyond the cursor to inside the "
                          "re-read range are picked up; ones that moved the other "
                          "way are not",
                "bounded_by": (f"re-read covers {cursor.offset - from_offset} records; "
                               f"a shift larger than that is not covered"),
            })
        else:
            may_miss.append({
                "window": "concurrent writes during the rebaseline itself",
                "reason": "a full pass still reads pages across time",
                "bounded_by": "verification by id set, plus the revision check",
            })
        if cursor.ordering != self.profile.ordering:
            may_miss.append({
                "window": (f"records whose position under {self.profile.ordering} "
                           f"is before the new cursor but were never read under "
                           f"{cursor.ordering}"),
                "reason": "a reordering can move an unread record behind the resume point",
                "bounded_by": "only the overlap pages are re-read, so the gap is bounded "
                              "by one page only if the shift is smaller than the overlap",
            })
        if self.profile.revisions == "revisable":
            may_miss.append({
                "window": "records revised between the last observation and the resume",
                "reason": "revision time is the new sort key; a record edited after it was "
                          "read can move to an already-passed offset",
                "bounded_by": "not bounded by the overlap - any record outside the re-read "
                              "range that changed after the snapshot can be skipped",
            })
        may_miss.append({
            "window": "records inserted after the resume while the scan is in flight",
            "reason": "insertions at the head shift every later page by one",
            "bounded_by": "a second pass over the recorded id set, or a full rebaseline",
        })

        why = list(assessment.reasons)
        if assessment.ordering_mismatch:
            why.insert(0, f"cursor offset {cursor.offset} was recorded under "
                          f"{cursor.ordering}; the source now serves "
                          f"{probe.detected_ordering}")

        witnessed = [{"kind": "probe_page", "offset": r.offset, "ids": list(r.ids),
                      "served_ordering": probe.detected_ordering} for r in probe.readings]
        return RecoveryPlan(
            source_id=self.profile.source_id, version=self.profile.version,
            strategy=strategy, from_offset=from_offset, page_size=ps,
            planned_pages=planned, baseline_pages=baseline_pages,
            extra_requests=(planned + probe_requests) - baseline_pages,
            probe_requests=probe_requests, candidate_ids=candidate_ids,
            already_covered=already, rechecked_ids=rechecked,
            residual_ids=residual_ids, residual_detail=residual_detail,
            may_miss=may_miss,
            why_not_safe_to_resume=why, witnessed_by=witnessed)

    def rehearse(self, plan: RecoveryPlan, run_id: str) -> RecoveryPlan:
        """Fetch the plan's pages and count them - with nothing journalled.

        This is what "show the extra requests" means: the plan is executed
        against the live source, so the numbers are measured.  Pages fetched in
        rehearsal are excluded from every verification input.
        """
        seen: List[int] = []
        offset = plan.from_offset
        pages = 0
        scan_id = f"rehearsal:{plan.source_id}"
        # Registered so the UI can show it - and labelled so nothing downstream
        # mistakes it for evidence.  No observations are written.
        self.journal.open_scan(scan_id, run_id, self.profile.version,
                               self.profile.ordering)
        while pages < plan.planned_pages:
            resp = self.source.fetch(offset, plan.page_size,
                                     scan_id=scan_id,
                                     run_id=run_id, rehearsal=True)
            seen.extend(resp.ids())
            pages += 1
            if not resp.has_more:
                break
            offset += plan.page_size
        plan.rehearsed = True
        plan.planned_pages = pages
        plan.candidate_ids = seen
        plan.extra_requests = (pages + plan.probe_requests) - plan.baseline_pages
        self.journal.finish_scan(
            scan_id, "rehearsed",
            f"{pages} pages fetched for plan evaluation; not verification evidence")
        plan.state_digest = self._state_digest()
        plan.rehearsed_at_ordering = self.source.serving_ordering
        return plan

    def _state_digest(self) -> str:
        """A digest of the live view that the plan's numbers were measured on."""
        raw = "|".join(
            f"{r.id}:{r.revision}:{r.updated_at}" for r in self.source.ordered())
        return hashlib.sha256(
            f"{self.source.serving_ordering}|{raw}".encode()).hexdigest()[:16]

    # -- execution -------------------------------------------------------
    def start(self, *, scan_id: str, run_id: str, cursor: Optional[Cursor] = None,
              plan: Optional[RecoveryPlan] = None,
              max_pages: Optional[int] = None) -> ScanOutcome:
        """Probe first, then either continue, execute an approved plan, or stop."""
        probe = self.probe(scan_id, run_id)
        if not probe.usable:
            return ScanOutcome(scan_id, run_id, STOP_REQUIRES_REVIEW,
                               f"ordering probe {probe.verdict}", 0, 0, probe=probe)

        if plan is not None:
            if not plan.approved:
                return ScanOutcome(scan_id, run_id, STOP_REQUIRES_REVIEW,
                                   "recovery plan has not been approved", 0, 0,
                                   probe=probe)
            if not plan.rehearsed:
                return ScanOutcome(scan_id, run_id, STOP_REQUIRES_REVIEW,
                                   "recovery plan was never rehearsed; its cost is unmeasured",
                                   0, 0, probe=probe)
            if plan.state_digest != self._state_digest():
                return ScanOutcome(
                    scan_id, run_id, STOP_REQUIRES_REVIEW,
                    "the source changed after this plan was rehearsed; "
                    "re-rehearse before approving", 0, 0, probe=probe)
            return self._execute(scan_id, run_id, plan.from_offset, probe, plan=plan,
                                 max_pages=max_pages)

        if cursor is None:
            return self._execute(scan_id, run_id, 0, probe, max_pages=max_pages)

        assessment = assess_cursor(cursor, self.profile, probe, self.source)
        if not assessment.safe_to_resume:
            return ScanOutcome(scan_id, run_id, STOP_REQUIRES_REVIEW,
                               "cursor is not safe to resume", 0, 0,
                               probe=probe, assessment=assessment)
        return self._execute(scan_id, run_id, cursor.offset, probe, max_pages=max_pages,
                             resumed_from=cursor)

    def _execute(self, scan_id: str, run_id: str, start_offset: int,
                 probe: ProbeResult, *, plan: Optional[RecoveryPlan] = None,
                 resumed_from: Optional[Cursor] = None,
                 max_pages: Optional[int] = None) -> ScanOutcome:
        self.journal.open_scan(scan_id, run_id, self.profile.version,
                               self.profile.ordering)
        self.journal.reopen_scan_for_progress(scan_id)
        if not probe.usable:
            raise ProfileError("refusing to execute without a confirmed ordering")

        revisions: Dict[int, int] = dict(resumed_from.revisions) if resumed_from else {}
        if plan is not None and self.journal.cursor() is not None:
            revisions.update(self.journal.cursor().revisions)

        offset = start_offset
        page_size = self.profile.page_size
        observed = 0
        pages_done = 0
        boundary: Tuple[int, ...] = ()
        total = self.source.size
        # Every request this execution makes (including the two probe pages) is
        # counted from the shared log, so the number is measured, never tallied
        # by hand.
        requests_before = len(self.source.log.entries) + 1
        probe_scan = f"probe:{scan_id}"
        probe_seqs = [e.seq for e in self.source.log.for_scan(probe_scan)]
        requests_before = min(probe_seqs) if probe_seqs else len(self.source.log.entries) + 1
        self._interruption_pages = pages_done

        def requests_used() -> int:
            return len(self.source.log.entries) - requests_before + 1

        while True:
            if self._refuse_after is not None and pages_done >= self._refuse_after:
                cursor = self._make_cursor(scan_id, offset, boundary, revisions, total)
                self.journal.set_cursor(cursor)
                if self.cursor_file is not None:
                    self.cursor_file.write(cursor)
                self.journal.finish_scan(scan_id, STOP_INTERRUPTED,
                                         "simulated process interruption", cursor)
                return ScanOutcome(scan_id, run_id, STOP_INTERRUPTED,
                                   "simulated process interruption",
                                   requests_used(), observed, probe=probe, plan=plan)
            if max_pages is not None and pages_done >= max_pages:
                cursor = self._make_cursor(scan_id, offset, boundary, revisions, total)
                self.journal.set_cursor(cursor)
                if self.cursor_file is not None:
                    self.cursor_file.write(cursor)
                self.journal.finish_scan(scan_id, STOP_PAUSED, "max_pages reached", cursor)
                return ScanOutcome(scan_id, run_id, STOP_PAUSED, "max_pages reached",
                                   requests_used(), observed, probe=probe, plan=plan)
            if self._pause.is_set():
                cursor = self._make_cursor(scan_id, offset, boundary, revisions, total)
                self.journal.set_cursor(cursor)
                if self.cursor_file is not None:
                    self.cursor_file.write(cursor)
                self.journal.finish_scan(scan_id, STOP_PAUSED, "paused by caller", cursor)
                self._pause.clear()
                return ScanOutcome(scan_id, run_id, STOP_PAUSED, "paused by caller",
                                   requests_used(), observed, probe=probe, plan=plan)

            try:
                resp = self._fetch(offset, page_size, scan_id=scan_id, run_id=run_id)
            except PageIntegrityFault as fault:
                cursor = self._make_cursor(scan_id, offset, boundary, revisions, total)
                self.journal.set_cursor(cursor)
                if self.cursor_file is not None:
                    self.cursor_file.write(cursor)
                self.journal.finish_scan(scan_id, STOP_ABORTED,
                                         f"page integrity fault at offset {offset}", cursor)
                return ScanOutcome(scan_id, run_id, STOP_ABORTED, str(fault),
                                   requests_used(), observed, probe=probe, plan=plan)
            seq = self.source.log.entries[-1].seq
            if resp.ids():
                boundary = tuple(resp.ids()[-min(3, len(resp.ids())):])
            for rec in resp.records:
                self.journal.record(Observation(
                    record_id=rec["id"], scan_id=scan_id, run_id=run_id,
                    version=self.profile.version, ordering=self.profile.ordering,
                    revision=rec["revision"], updated_at=rec["updated_at"],
                    payload=rec["payload"], request_seq=seq,
                    served_ordering=resp.served_ordering,
                    offset=offset, page_size=page_size, has_more=resp.has_more))
                revisions[rec["id"]] = max(revisions.get(rec["id"], 0), rec["revision"])
                observed += 1
            total = resp.total
            pages_done += 1
            if not resp.has_more:
                cursor = self._make_cursor(scan_id, len(self.source.ordered()),
                                           boundary, revisions, total)
                # A complete pass is not the same as a trustworthy cursor: an
                # insertion behind the scan while it was running can be missed.
                self.journal.finish_scan(scan_id, STOP_COMPLETE, None, cursor)
                self.journal.set_cursor(cursor)
                if self.cursor_file is not None:
                    self.cursor_file.write(cursor)
                return ScanOutcome(scan_id, run_id, STOP_COMPLETE, None,
                                   requests_used(), observed, probe=probe, plan=plan)
            offset += page_size

    def _make_cursor(self, scan_id: str, offset: int, boundary: Sequence[int],
                     revisions: Dict[int, int], total: int) -> Cursor:
        return Cursor(
            scan_id=scan_id, source_id=self.profile.source_id,
            version=self.profile.version, ordering=self.profile.ordering,
            offset=offset, page_size=self.profile.page_size,
            total_at_stop=total,
            recorded_at_seq=len(self.source.log.entries),
            boundary_ids=tuple(boundary), revisions=dict(revisions))


# ---------------------------------------------------------------------------
# recovery coverage bookkeeping (used by the plan page and the tests)


def fingerprint(profile: SourceProfile, ordering: str) -> str:
    raw = f"{profile.source_id}|{profile.version}|{ordering}|{profile.page_style}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
