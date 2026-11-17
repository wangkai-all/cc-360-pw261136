"""Verification and ingestion.

Verification does not answer "did the count go up".  It answers, per record:

* was this record *observed* by any accepted scan?
* which scan observed it, at which request, under which upstream ordering?
* has it been revised since that observation?
* is the observation part of a scan that provably covered a contiguous range,
  or is it a lone sighting from a reordering that cannot be placed?

Only then does the service write to SQLite, and it records provenance for every
row so the web page can answer "where did this record come from".
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set

from .journal import Journal, Observation
from .versions import SourceProfile

DONE_COMPLETE = "complete"
DONE_PARTIAL = "partial"
DONE_UNVERIFIED = "unverified"


@dataclass
class RecordReport:
    record_id: int
    state: str                       # verified | stale | unverified | unobserved
    revision_upstream: int
    revision_seen: Optional[int]
    observed_by: List[dict] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"record_id": self.record_id, "state": self.state,
                "revision_upstream": self.revision_upstream,
                "revision_seen": self.revision_seen,
                "observed_by": list(self.observed_by), "notes": list(self.notes)}


@dataclass
class VerificationReport:
    source_id: str
    upstream_total: int
    observed_total: int
    accepted_scans: List[str]
    rejected_scans: List[dict]
    records: List[RecordReport]
    contiguous_ranges: List[dict]
    gaps: List[dict]
    closure: str                     # closed | open | bounded_open
    rationale: List[str]

    @property
    def verified(self) -> int:
        return sum(1 for r in self.records if r.state == "verified")

    @property
    def stale(self) -> int:
        return sum(1 for r in self.records if r.state == "stale")

    @property
    def unverified(self) -> int:
        return sum(1 for r in self.records if r.state == "unverified")

    @property
    def unobserved(self) -> int:
        return sum(1 for r in self.records if r.state == "unobserved")

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "upstream_total": self.upstream_total,
            "observed_total": self.observed_total,
            "accepted_scans": list(self.accepted_scans),
            "rejected_scans": list(self.rejected_scans),
            "records": [r.to_dict() for r in self.records],
            "contiguous_ranges": [dict(r) for r in self.contiguous_ranges],
            "gaps": [dict(g) for g in self.gaps],
            "closure": self.closure,
            "rationale": list(self.rationale),
            "counts": {"verified": self.verified, "stale": self.stale,
                       "unverified": self.unverified, "unobserved": self.unobserved},
        }


def _runs(obs: Sequence[Observation]) -> List[dict]:
    """Group one scan's observations into contiguous offset runs.

    A run is broken when a request sequence is interrupted, when the offset does
    not advance by exactly the page size, or when the ordering the server served
    changes.  Any break is evidence: the pages after it cannot be stitched onto
    the pages before it, so coverage across the break is not claimed.
    """
    ordered = sorted(obs, key=lambda o: (o.request_seq, o.offset))
    runs: List[List[Observation]] = []
    breaks: List[dict] = []
    for o in ordered:
        if not runs:
            runs.append([o])
            continue
        prev = runs[-1][-1]
        same_seq = o.request_seq == prev.request_seq
        if same_seq:
            runs[-1].append(o)
            continue
        expected = prev.offset + (prev.page_size or 0)
        if o.request_seq != prev.request_seq + 1:
            breaks.append({"kind": "request_gap", "after_seq": prev.request_seq,
                           "before_seq": o.request_seq,
                           "detail": "requests are not consecutive in the log"})
            runs.append([o])
        elif o.offset != expected:
            breaks.append({"kind": "offset_gap", "after_seq": prev.request_seq,
                           "expected_offset": expected, "actual_offset": o.offset,
                           "detail": "the scan skipped or repeated offsets"})
            runs.append([o])
        elif o.served_ordering != prev.served_ordering:
            breaks.append({"kind": "ordering_switch", "after_seq": prev.request_seq,
                           "from": prev.served_ordering, "to": o.served_ordering,
                           "detail": "the server's ordering changed mid-scan; the "
                                     "later pages are not continuous with the earlier ones"})
            runs.append([o])
        else:
            runs[-1].append(o)
    out = []
    for run in runs:
        seqs = sorted({o.request_seq for o in run})
        out.append({
            "request_seqs": seqs, "first_seq": seqs[0], "last_seq": seqs[-1],
            "first_offset": min(o.offset for o in run),
            "last_offset": max(o.offset for o in run) + run[0].page_size,
            "ids": [o.record_id for o in run],
            "pages": len(seqs),
            "ordering": run[0].served_ordering,
        })
    return out, breaks


def verify(journal: Journal, source_records: Dict[int, object],
           profile: SourceProfile, *,
           accepted_scans: Optional[Set[str]] = None,
           rejected_scans: Optional[List[dict]] = None,
           rehearsal_request_seqs: Optional[Set[int]] = None) -> VerificationReport:
    """Build a per-record verification report from journalled observations.

    ``source_records`` is the current upstream state (id -> object with
    ``revision``), used only to detect revisions newer than the observation.
    Counts are never used to declare success.
    """
    accepted_scans = accepted_scans or set(journal.scan_ids())
    rejected = list(rejected_scans or [])
    rehearsal_seqs = set(rehearsal_request_seqs or ())

    by_record: Dict[int, List[Observation]] = {}
    accepted_obs: List[Observation] = []
    for obs in journal.observations(include_rehearsal=False):
        if obs.scan_id not in accepted_scans:
            continue
        if obs.request_seq in rehearsal_seqs:
            continue
        accepted_obs.append(obs)
        by_record.setdefault(obs.record_id, []).append(obs)

    observed_ids = set(by_record)
    reports: List[RecordReport] = []
    for rid in sorted(source_records):
        upstream_rev = getattr(source_records[rid], "revision")
        seen = by_record.get(rid, [])
        if not seen:
            reports.append(RecordReport(rid, "unobserved", upstream_rev, None,
                                        [], ["no accepted scan observed this record"]))
            continue
        best = max(seen, key=lambda o: (o.revision, o.request_seq))
        provenance = [{
            "scan_id": o.scan_id, "run_id": o.run_id, "version": o.version,
            "ordering": o.ordering, "served_ordering": o.served_ordering,
            "request_seq": o.request_seq, "revision": o.revision,
            "updated_at": o.updated_at,
        } for o in sorted(seen, key=lambda o: o.request_seq)]
        if best.revision < upstream_rev:
            reports.append(RecordReport(
                rid, "stale", upstream_rev, best.revision, provenance,
                [f"observed at revision {best.revision}, upstream now {upstream_rev}"]))
        else:
            reports.append(RecordReport(rid, "verified", upstream_rev, best.revision,
                                        provenance, []))

    ranges: List[dict] = []
    gaps: List[dict] = []
    for scan_id in sorted(accepted_scans):
        obs = [o for o in accepted_obs if o.scan_id == scan_id]
        if not obs:
            continue
        runs, breaks = _runs(obs)
        for run in runs:
            run["scan_id"] = scan_id
            ranges.append(run)
        for brk in breaks:
            brk["scan_id"] = scan_id
            gaps.append(brk)
    ranges.sort(key=lambda r: (r["scan_id"], r["first_seq"]))

    observed_set = sorted(observed_ids)
    if observed_set:
        id_gap = [i for i in range(observed_set[0], observed_set[-1] + 1)
                  if i not in observed_ids]
        if id_gap:
            gaps.append({
                "kind": "id_gap",
                "detail": "record ids inside the observed span with no observation",
                "ids": id_gap,
            })
    by_scan: Dict[str, List[dict]] = {}
    for r in ranges:
        by_scan.setdefault(r["scan_id"], []).append(r)
    for scan_id, rs in by_scan.items():
        rs.sort(key=lambda r: r["first_offset"])
        for a, b in zip(rs, rs[1:]):
            if a["ordering"] == b["ordering"] and b["first_offset"] > a["last_offset"]:
                gaps.append({
                    "kind": "offset_uncovered", "scan_id": scan_id,
                    "from": a["last_offset"], "to": b["first_offset"],
                    "detail": "no observation covers this offset range under a single "
                              "ordering, so records in it cannot be shown to exist"})

    # Closure is a statement about *evidence*, never about totals.
    #
    # "Uncovered" is a gap in offset coverage under one ordering: a run that
    # jumps forward, or a scan that switched orderings mid-flight.  It is a
    # stronger claim than "an isolated page": with a switch, every record whose
    # new position is inside the skipped span is unaccounted for.
    uncovered = [g for g in gaps if g["kind"] in
                 ("offset_gap", "offset_uncovered", "ordering_switch")]
    isolated = [r for r in ranges if r["pages"] < 2 and len(ranges) > 1]
    totality = len(observed_ids) == len(source_records) and len(source_records) > 0
    if uncovered:
        closure = "bounded_open"
        rationale = [
            f"{len(uncovered)} ordering or offset break(s) leave ranges that no "
            f"accepted scan covered under a single ordering",
            "an id count cannot close this: the records that would fill the range "
            "are exactly the ones never read",
        ]
    elif totality and not isolated:
        closure = "closed"
        rationale = [
            f"every one of the {len(source_records)} upstream records has a journalled "
            f"observation from the accepted scans",
            "each observation sits in an unbroken offset run, and the request log "
            "shows no ordering switch inside a run",
        ]
    elif isolated:
        closure = "bounded_open"
        rationale = [
            f"{len(isolated)} isolated request run(s) cannot be placed relative to the "
            f"rest of the scan; the id set is not provably complete",
            "count equality alone would not close this - an unread record can be offset "
            "by a duplicate or revision",
        ]
    else:
        closure = "open"
        rationale = [
            f"{len(observed_ids)} of {len(source_records)} upstream records observed",
            "missing records are enumerated below rather than inferred from a total",
        ]
    return VerificationReport(
        source_id=profile.source_id, upstream_total=len(source_records),
        observed_total=len(observed_ids), accepted_scans=sorted(accepted_scans),
        rejected_scans=rejected, records=reports, contiguous_ranges=ranges,
        gaps=gaps, closure=closure, rationale=rationale)


def fingerprint_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


class IngestStore:
    """Projection of verified observations into SQLite, with provenance."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
              source_id TEXT NOT NULL,
              record_id INTEGER NOT NULL,
              revision INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              payload_fingerprint TEXT NOT NULL,
              first_seen_scan TEXT,
              last_scan TEXT,
              last_version TEXT,
              last_ordering TEXT,
              last_request_seq INTEGER,
              DISTINCT_OBSERVATIONS INTEGER,
              PRIMARY KEY (source_id, record_id)
            );
            CREATE TABLE IF NOT EXISTS observations (
              source_id TEXT NOT NULL,
              record_id INTEGER NOT NULL,
              scan_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              version TEXT NOT NULL,
              ordering TEXT NOT NULL,
              served_ordering TEXT NOT NULL,
              revision INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              payload_fingerprint TEXT NOT NULL,
              request_seq INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS obs_scan ON observations(scan_id);
            CREATE TABLE IF NOT EXISTS scans (
              scan_id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              version TEXT NOT NULL,
              ordering TEXT NOT NULL,
              status TEXT NOT NULL,
              stop_reason TEXT,
              started_request_seq INTEGER,
              ended_request_seq INTEGER,
              accepted INTEGER NOT NULL,
              note TEXT
            );
            """
        )
        self.conn.commit()

    def ingest(self, report: VerificationReport, journal: Journal,
               profile: SourceProfile, *, scan_notes: Optional[Dict[str, str]] = None,
               rejected: Optional[Set[str]] = None) -> dict:
        rejected = rejected or set()
        scan_notes = scan_notes or {}
        obs = journal.observations(include_rehearsal=False)
        inserted = updated = skipped = 0
        with self.conn:
            for scan in journal.scans():
                self.conn.execute(
                    """INSERT INTO scans(scan_id, source_id, version, ordering, status,
                        stop_reason, started_request_seq, ended_request_seq, accepted, note)
                       VALUES(?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(scan_id) DO UPDATE SET
                         status=excluded.status, stop_reason=excluded.stop_reason,
                         started_request_seq=excluded.started_request_seq,
                         ended_request_seq=excluded.ended_request_seq,
                         accepted=excluded.accepted, note=excluded.note""",
                    (scan["scan_id"], profile.source_id, scan.get("version") or "",
                     scan.get("ordering") or "", scan.get("status") or "",
                     scan.get("stop_reason"), scan.get("started_request_seq"),
                     scan.get("ended_request_seq"),
                     0 if scan["scan_id"] in rejected else 1,
                     scan_notes.get(scan["scan_id"])))
            for o in obs:
                if o.scan_id in rejected:
                    continue
                already = self.conn.execute(
                    "SELECT 1 FROM observations WHERE source_id=? AND record_id=? AND "
                    "scan_id=? AND request_seq=?", 
                    (profile.source_id, o.record_id, o.scan_id, o.request_seq)).fetchone()
                if already:
                    continue
                self.conn.execute(
                    """INSERT INTO observations(source_id, record_id, scan_id, run_id,
                        version, ordering, served_ordering, revision, updated_at,
                        payload_fingerprint, request_seq) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (profile.source_id, o.record_id, o.scan_id, o.run_id, o.version,
                     o.ordering, o.served_ordering, o.revision, o.updated_at,
                     fingerprint_payload(o.payload), o.request_seq))
            for rep in report.records:
                if rep.state != "verified":
                    skipped += 1
                    continue
                best = rep.observed_by[-1]
                row = self.conn.execute(
                    "SELECT record_id FROM records WHERE source_id=? AND record_id=?",
                    (profile.source_id, rep.record_id)).fetchone()
                payload_fp = self.conn.execute(
                    "SELECT payload_fingerprint FROM observations WHERE source_id=? AND "
                    "record_id=? AND request_seq=?",
                    (profile.source_id, rep.record_id, best["request_seq"])).fetchone()
                fp = payload_fp["payload_fingerprint"] if payload_fp else ""
                if row is None:
                    inserted += 1
                    self.conn.execute(
                        """INSERT INTO records(source_id, record_id, revision, updated_at,
                            payload_fingerprint, first_seen_scan, last_scan, last_version,
                            last_ordering, last_request_seq) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (profile.source_id, rep.record_id, rep.revision_upstream,
                         best["updated_at"], fp, best["scan_id"], best["scan_id"],
                         best["version"], best["ordering"], best["request_seq"]))
                else:
                    updated += 1
                    self.conn.execute(
                        """UPDATE records SET revision=?, updated_at=?, payload_fingerprint=?,
                            last_scan=?, last_version=?, last_ordering=?, last_request_seq=?
                           WHERE source_id=? AND record_id=?""",
                        (rep.revision_upstream, best["updated_at"], fp,
                         best["scan_id"], best["version"], best["ordering"],
                         best["request_seq"], profile.source_id, rep.record_id))
        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    def record_rows(self, source_id: str) -> List[dict]:
        rows = self.conn.execute(
            """SELECT r.*, (SELECT COUNT(*) FROM observations o WHERE o.source_id=r.source_id
                 AND o.record_id=r.record_id) AS observation_count
               FROM records r WHERE source_id=? ORDER BY record_id""",
            (source_id,)).fetchall()
        return [dict(r) for r in rows]

    def provenance(self, source_id: str, record_id: int) -> List[dict]:
        rows = self.conn.execute(
            """SELECT scan_id, run_id, version, ordering, served_ordering, revision,
                      updated_at, payload_fingerprint, request_seq
               FROM observations WHERE source_id=? AND record_id=?
               ORDER BY request_seq""", (source_id, record_id)).fetchall()
        return [dict(r) for r in rows]

    def scan_rows(self, source_id: str) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM scans WHERE source_id=? ORDER BY scan_id", (source_id,)).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self.conn.close()
