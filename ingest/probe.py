"""Sorting-convention probes and cursor interpretation.

The rule this module exists to enforce:

    A cursor is only a position *relative to the ordering it was recorded
    under*.  Before a cursor is reused, the ordering currently being served
    must be demonstrated, not assumed.

Detection never trusts a version string, a header, or the total count.  It
compares two overlapping pages of live responses and classifies which of the
three orderings the sequence of ids is compatible with.  Because two small
pages can accidentally agree with the wrong convention, a probe reports the
evidence it has and refuses to reach a verdict when the evidence is ambiguous
("inconclusive") - ambiguity is a result, not an excuse to guess.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .upstream import RequestLog, UpstreamSource
from .versions import KEY_ASCENDING, KEY_DESCENDING, ProfileError, SourceProfile, UPDATED_DESC

VERDICT_CONFIRMED = "confirmed"
VERDICT_INCONCLUSIVE = "inconclusive"
VERDICT_MISMATCH = "mismatch"


@dataclass(frozen=True)
class ProbeReading:
    offset: int
    ids: Tuple[int, ...]
    revisions: Tuple[int, ...]
    updated_at: Tuple[int, ...]
    total: int

    def to_dict(self) -> dict:
        return {"offset": self.offset, "ids": list(self.ids),
                "revisions": list(self.revisions),
                "updated_at": list(self.updated_at), "total": self.total}


@dataclass
class ProbeResult:
    source_id: str
    version: str
    expected_ordering: str
    detected_ordering: Optional[str]
    verdict: str
    readings: List[ProbeReading]
    evidence: List[str]
    overlap: List[int]

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id, "version": self.version,
            "expected_ordering": self.expected_ordering,
            "detected_ordering": self.detected_ordering,
            "verdict": self.verdict,
            "readings": [r.to_dict() for r in self.readings],
            "evidence": list(self.evidence),
            "overlap": list(self.overlap),
        }

    @property
    def usable(self) -> bool:
        return self.verdict == VERDICT_CONFIRMED


def _ordered_by(values: Sequence[int], ordering: str) -> bool:
    if len(values) < 2:
        return True
    if ordering == KEY_ASCENDING:
        return all(a < b for a, b in zip(values, values[1:]))
    if ordering == KEY_DESCENDING:
        return all(a > b for a, b in zip(values, values[1:]))
    if ordering == UPDATED_DESC:
        return all(a >= b for a, b in zip(values, values[1:]))
    raise ValueError(ordering)


def classify(ids: Sequence[int], updated_at: Sequence[int]) -> Tuple[Optional[str], List[str]]:
    """Return the ordering(s) compatible with one contiguous run of ids."""
    evidence: List[str] = []
    if len(ids) < 2:
        return None, ["page holds fewer than two records; no ordering evidence"]
    compat = []
    if _ordered_by(ids, KEY_ASCENDING):
        compat.append(KEY_ASCENDING)
    if _ordered_by(ids, KEY_DESCENDING):
        compat.append(KEY_DESCENDING)
    if _ordered_by(updated_at, UPDATED_DESC):
        compat.append(UPDATED_DESC)
    evidence.append(f"ids {list(ids)} compatible with {compat or 'no known ordering'}")
    if KEY_ASCENDING in compat and KEY_DESCENDING in compat:
        evidence.append("a single page cannot distinguish ascending from descending")
        return None, evidence
    if UPDATED_DESC in compat and KEY_ASCENDING in compat:
        evidence.append("ids ascend and update times descend: ordering is ambiguous")
    if len(compat) == 1:
        return compat[0], evidence
    if not compat:
        return None, evidence
    return None, evidence


def probe_ordering(source: UpstreamSource, profile: SourceProfile, *,
                   scan_id: str, run_id: str, log: Optional[RequestLog] = None) -> ProbeResult:
    """Fetch overlapping pages and decide which ordering the server is serving.

    Only two requests, at fixed offsets, so a probe never walks the source.  When
    the two pages overlap, the duplicated ids anchor them to the same snapshot;
    when they do not overlap, each page is classified independently and the
    verdict is ``inconclusive`` unless both agree.
    """
    log = log if log is not None else source.log
    readings: List[ProbeReading] = []
    second_offset = profile.probe_offset + max(1, profile.probe_limit // 2)
    for offset in (profile.probe_offset, second_offset):
        resp = source.fetch(offset, profile.probe_limit, scan_id=scan_id, run_id=run_id)
        readings.append(ProbeReading(
            offset=offset, ids=tuple(resp.ids()),
            revisions=tuple(r["revision"] for r in resp.records),
            updated_at=tuple(r["updated_at"] for r in resp.records),
            total=resp.total))
    first, second = readings
    overlap = sorted(set(first.ids) & set(second.ids))
    evidence: List[str] = []
    if overlap:
        evidence.append(f"pages at offsets {first.offset} and {second.offset} overlap on {overlap}")
    else:
        evidence.append(f"pages at offsets {first.offset} and {second.offset} do not overlap")

    per_page: List[Optional[str]] = []
    for r in readings:
        verdict, ev = classify(r.ids, r.updated_at)
        per_page.append(verdict)
        evidence.extend(ev)

    detected: Optional[str] = None
    if overlap:
        # Overlapping pages anchor both readings to the same moving window.  The
        # ids in the overlap must appear at relative positions consistent with
        # the offsets, and the merged sequence must be compatible with exactly
        # one ordering.
        shift = second.offset - first.offset
        evidence.append(f"overlap {overlap} at relative shift {shift}")
        merged_ids = list(first.ids)
        merged_updated = list(first.updated_at)
        for i, rid in enumerate(second.ids):
            if rid in merged_ids:
                pos = merged_ids.index(rid)
                if pos != i + shift:
                    evidence.append(
                        f"id {rid} appears at relative position {pos} in the first page "
                        f"but {i + shift} from its position in the second")
            else:
                merged_ids.append(rid)
                merged_updated.append(second.updated_at[i])
        detected, ev = classify(merged_ids, merged_updated)
        evidence.extend(ev)
    elif per_page[0] is not None and per_page[0] == per_page[1]:
        detected = per_page[0]
        evidence.append(f"two disjoint pages agree on {detected}")
    else:
        evidence.append("disjoint pages disagree or are individually ambiguous; refusing to guess")

    # A descending page with an ascending update series is not the new ordering.
    if detected == UPDATED_DESC and not _ordered_by(first.updated_at, UPDATED_DESC):
        evidence.append("ids and update times do not both descend; rejecting updated_desc")
        detected = None

    if detected is None:
        verdict = VERDICT_INCONCLUSIVE
    elif detected == profile.ordering:
        verdict = VERDICT_CONFIRMED
    else:
        verdict = VERDICT_MISMATCH
    if verdict == VERDICT_CONFIRMED and not overlap:
        evidence.append("ordering confirmed by disjoint page evidence; overlap would strengthen it")
    return ProbeResult(
        source_id=source.source_id, version=profile.version,
        expected_ordering=profile.ordering, detected_ordering=detected,
        verdict=verdict, readings=readings, evidence=evidence, overlap=overlap)


def require_ordering(source: UpstreamSource, profile: SourceProfile, *,
                     scan_id: str, run_id: str) -> ProbeResult:
    result = probe_ordering(source, profile, scan_id=scan_id, run_id=run_id)
    if not result.usable:
        raise ProfileError(
            f"ordering probe for {profile.source_id}@{profile.version} was "
            f"{result.verdict} (expected {profile.ordering}, "
            f"detected {result.detected_ordering}); refusing to continue")
    return result


@dataclass(frozen=True)
class Cursor:
    scan_id: str
    source_id: str
    version: str
    ordering: str
    offset: int
    page_size: int
    total_at_stop: int
    recorded_at_seq: int
    # ids observed at the page(s) immediately upstream of the cursor
    boundary_ids: Tuple[int, ...]
    # per-id highest revision seen by this scan
    revisions: Dict[int, int]

    def to_dict(self) -> dict:
        return {
            "scan_id": self.scan_id, "source_id": self.source_id,
            "version": self.version, "ordering": self.ordering,
            "offset": self.offset, "page_size": self.page_size,
            "total_at_stop": self.total_at_stop,
            "recorded_at_seq": self.recorded_at_seq,
            "boundary_ids": list(self.boundary_ids),
            "revisions": {str(k): v for k, v in sorted(self.revisions.items())},
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Cursor":
        return cls(
            scan_id=d["scan_id"], source_id=d["source_id"], version=d["version"],
            ordering=d["ordering"], offset=int(d["offset"]),
            page_size=int(d["page_size"]), total_at_stop=int(d["total_at_stop"]),
            recorded_at_seq=int(d["recorded_at_seq"]),
            boundary_ids=tuple(int(i) for i in d.get("boundary_ids", [])),
            revisions={int(k): int(v) for k, v in d.get("revisions", {}).items()})


@dataclass
class CursorAssessment:
    cursor: Cursor
    safe_to_resume: bool
    reasons: List[str]
    ordering_mismatch: bool
    detected_ordering: Optional[str]
    boundary_present: bool
    revisions_drifted: List[int]

    def to_dict(self) -> dict:
        return {
            "cursor": self.cursor.to_dict(),
            "safe_to_resume": self.safe_to_resume,
            "reasons": list(self.reasons),
            "ordering_mismatch": self.ordering_mismatch,
            "detected_ordering": self.detected_ordering,
            "boundary_present": self.boundary_present,
            "revisions_drifted": sorted(self.revisions_drifted),
        }


def assess_cursor(cursor: Cursor, profile: SourceProfile, probe: ProbeResult,
                  source: UpstreamSource) -> CursorAssessment:
    """Decide whether reusing *cursor* against the live source is defensible.

    Resolution is not safety.  A cursor can be *accepted* by the server (its
    offset is in range) while pointing at unrelated records under the new
    ordering.  Every check below is about semantics, not reachability.
    """
    reasons: List[str] = []
    ordering_mismatch = False
    boundary_present = False
    drifted: List[int] = []

    if cursor.source_id != profile.source_id:
        reasons.append(f"cursor belongs to source {cursor.source_id}, not {profile.source_id}")
        return CursorAssessment(cursor, False, reasons, True, probe.detected_ordering,
                                False, [])
    if not probe.usable:
        reasons.append(f"ordering probe was {probe.verdict}; the current convention is unproven")
    if probe.detected_ordering is not None and probe.detected_ordering != cursor.ordering:
        ordering_mismatch = True
        reasons.append(
            f"cursor recorded under {cursor.ordering} but server serves "
            f"{probe.detected_ordering}; offset {cursor.offset} names a different record set")
    elif probe.detected_ordering == cursor.ordering:
        reasons.append(f"ordering {cursor.ordering} still served")

    # Boundary ids are the page(s) immediately before the cursor.  Under a new
    # ordering those ids will not sit at the same offsets.
    if cursor.boundary_ids:
        live = source.ordered()
        positions = {rec.id: idx for idx, rec in enumerate(live)}
        present = [i for i in cursor.boundary_ids if i in positions]
        if not present:
            reasons.append("none of the cursor boundary ids still exist upstream")
            boundary_present = False
        else:
            boundary_present = True
            near = [i for i in present if positions[i] < cursor.offset]
            if len(near) != len(present):
                reasons.append(
                    f"boundary ids moved across the cursor: {sorted(set(present) - set(near))} "
                    f"now sit at or after offset {cursor.offset}")
            else:
                reasons.append(
                    f"boundary ids still upstream of offset {cursor.offset} "
                    f"({len(near)}/{len(present)})")

    if profile.revisions == "revisable":
        for rid, seen in cursor.revisions.items():
            current = source.current_revision(rid)
            if current is not None and current > seen:
                drifted.append(rid)
        if drifted:
            reasons.append(
                f"{len(drifted)} records revised after the scan recorded them; "
                f"their new position is unknown ({sorted(drifted)[:6]}{'...' if len(drifted) > 6 else ''})")

    # The total is only ever corroboration.  Equal totals say nothing about
    # membership: a record can be added while another is revised, or the count
    # endpoint can be cached.  It is never sufficient on its own.
    if source.size == cursor.total_at_stop:
        reasons.append(f"total unchanged at {cursor.total_at_stop} (corroboration only, not proof)")

    safe = (not ordering_mismatch and probe.usable and not drifted
            and boundary_present and not any(
                r.startswith("boundary ids moved") for r in reasons))
    return CursorAssessment(cursor, safe, reasons, ordering_mismatch,
                            probe.detected_ordering, boundary_present, drifted)
