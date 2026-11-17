"""A local, deterministic stand-in for the upstream source.

No third-party API is involved: the upstream is modelled in-process so that
ordering conventions, revisions and slow scans are reproducible.  The harvester
only ever sees :class:`UpstreamResponse` objects, so a real HTTP adapter could be
dropped in without touching the rest of the code.

Every request is appended to the shared :class:`RequestLog`.  That log is the
accounting unit for "how many extra requests does this recovery plan cost" - the
harvester never estimates when it can count.

The virtual clock advances one tick per request and per revision.  ``updated_at``
is assigned from that clock, which makes ``updated_desc`` a total order
(``(updated_at, id)`` descending) and keeps every demonstration reproducible.
"""

from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .versions import KEY_ASCENDING, KEY_DESCENDING, UPDATED_DESC


@dataclass
class UpstreamRecord:
    id: int
    revision: int
    updated_at: int
    payload: str

    def as_json(self) -> dict:
        return {"id": self.id, "revision": self.revision,
                "updated_at": self.updated_at, "payload": self.payload}


@dataclass
class UpstreamResponse:
    records: List[dict]
    total: int
    has_more: bool
    offset: int
    limit: int
    served_ordering: str

    def ids(self) -> List[int]:
        return [r["id"] for r in self.records]


@dataclass
class RequestEntry:
    seq: int
    scan_id: str
    run_id: str
    offset: int
    limit: int
    served_ordering: str
    ids: List[int]
    total: int
    has_more: bool = True
    rehearsal: bool = False
    # revision values observed in this response, keyed by id
    revisions: Dict[int, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "seq": self.seq, "scan_id": self.scan_id, "run_id": self.run_id,
            "offset": self.offset, "limit": self.limit,
            "served_ordering": self.served_ordering,
            "ids": list(self.ids), "total": self.total,
            "has_more": self.has_more,
            "rehearsal": self.rehearsal,
            "revisions": {str(k): v for k, v in sorted(self.revisions.items())},
        }


class RequestLog:
    def __init__(self) -> None:
        self.entries: List[RequestEntry] = []

    def append(self, entry: RequestEntry) -> None:
        self.entries.append(entry)

    def for_scan(self, scan_id: str) -> List[RequestEntry]:
        return [e for e in self.entries if e.scan_id == scan_id]

    def count(self, scan_id: Optional[str] = None) -> int:
        if scan_id is None:
            return len(self.entries)
        return len(self.for_scan(scan_id))

    def ids_covered(self, scan_id: str) -> set:
        out = set()
        for e in self.for_scan(scan_id):
            out.update(e.ids)
        return out

    def to_dict(self) -> dict:
        return {"entries": [e.to_dict() for e in self.entries]}


ORDER_FN = {
    KEY_ASCENDING: lambda rs: sorted(rs, key=lambda r: (r.id,)),
    KEY_DESCENDING: lambda rs: sorted(rs, key=lambda r: (r.id,), reverse=True),
    UPDATED_DESC: lambda rs: sorted(rs, key=lambda r: (r.updated_at, r.id), reverse=True),
}


class UpstreamSource:
    """Source of record for one ``source_id``.

    ``serving_ordering`` is what the server does *right now* - the upgrade is a
    call to :meth:`switch_ordering`, exactly like a deployment rolling out over
    an environment that clients are still pointed at.
    """

    def __init__(self, source_id: str, serving_ordering: str = KEY_ASCENDING,
                 seed: int = 7, log: Optional[RequestLog] = None) -> None:
        if serving_ordering not in ORDER_FN:
            raise ValueError(f"unknown ordering {serving_ordering!r}")
        self.source_id = source_id
        self.serving_ordering = serving_ordering
        self.log = log if log is not None else RequestLog()
        self._clock = itertools.count(1000, 7)
        self._records: Dict[int, UpstreamRecord] = {}
        self._seed = seed
        self._rng = _Lcg(seed)

    # -- dataset ---------------------------------------------------------
    def seed_records(self, n: int) -> None:
        """Create ``n`` records with ids 1..n.

        Update times are a deterministic *permutation* of the clock, not the
        identity order.  Real sources order by update time for reasons unrelated
        to id, and a demo dataset where ``updated_desc`` looks like
        ``key_descending`` would silently make the ordering probe untestable.
        """
        stamps = [next(self._clock) for _ in range(n)]
        order = list(range(n))
        for i in range(n - 1, 0, -1):          # deterministic Fisher-Yates
            j = self._rng.next_int(i + 1)
            order[i], order[j] = order[j], order[i]
        for idx, rec_id in enumerate(range(1, n + 1)):
            self._records[rec_id] = UpstreamRecord(
                id=rec_id, revision=1, updated_at=stamps[order[idx]],
                payload=f"seed-{rec_id}-v1")
        self._rng = _Lcg(self._seed)

    def add_record(self, payload: Optional[str] = None) -> int:
        next_id = max(self._records, default=0) + 1
        self._records[next_id] = UpstreamRecord(
            id=next_id, revision=1, updated_at=next(self._clock),
            payload=payload or f"new-{next_id}")
        return next_id

    def revise(self, record_id: int, payload: Optional[str] = None) -> int:
        rec = self._records[record_id]
        rec.revision += 1
        rec.updated_at = next(self._clock)
        rec.payload = payload or f"{rec.payload}+r{rec.revision}"
        return rec.revision

    def snapshot(self) -> Dict[int, UpstreamRecord]:
        return copy.deepcopy(self._records)

    def records_view(self) -> List[UpstreamRecord]:
        """Read-only list of current records, independent of served ordering."""
        return list(copy.deepcopy(self._records).values())

    def current_revision(self, record_id: int) -> Optional[int]:
        rec = self._records.get(record_id)
        return rec.revision if rec else None

    def head_ids(self, n: int) -> List[int]:
        return [r.id for r in self.ordered()[:n]]

    @property
    def size(self) -> int:
        return len(self._records)

    def ordered(self) -> List[UpstreamRecord]:
        return ORDER_FN[self.serving_ordering](list(self._records.values()))

    def switch_ordering(self, new_ordering: str) -> None:
        if new_ordering not in ORDER_FN:
            raise ValueError(f"unknown ordering {new_ordering!r}")
        self.serving_ordering = new_ordering

    # -- wire ------------------------------------------------------------
    def fetch(self, offset: int, limit: int, *, scan_id: str, run_id: str,
              rehearsal: bool = False, strict: bool = True) -> UpstreamResponse:
        if offset < 0 or limit < 1:
            raise ValueError("bad offset/limit")
        if strict and self._rng.next_float() < 0.0:  # hook for future faults
            raise RuntimeError("simulated upstream fault")
        ordered = self.ordered()
        page = ordered[offset:offset + limit]
        resp = UpstreamResponse(
            records=[r.as_json() for r in page], total=len(ordered),
            has_more=offset + len(page) < len(ordered),
            offset=offset, limit=limit, served_ordering=self.serving_ordering)
        self.log.append(RequestEntry(
            seq=len(self.log.entries) + 1, scan_id=scan_id, run_id=run_id,
            offset=offset, limit=limit, served_ordering=self.serving_ordering,
            ids=resp.ids(), total=resp.total, has_more=resp.has_more,
            rehearsal=rehearsal,
            revisions={r["id"]: r["revision"] for r in resp.records}))
        return resp

    def fetch_page_containing(self, record_id: int, *, scan_id: str,
                              run_id: str) -> Optional[UpstreamResponse]:
        """Not used by the harvester - kept out of the client surface on purpose.

        The harvester can only page by offset; resolving a record directly would
        hide exactly the ordering instability this project is about.
        """
        raise NotImplementedError("the upstream has no lookup-by-id endpoint")

    def count(self, *, scan_id: str, run_id: str) -> int:
        self.log.append(RequestEntry(
            seq=len(self.log.entries) + 1, scan_id=scan_id, run_id=run_id,
            offset=-1, limit=0, served_ordering=self.serving_ordering,
            ids=[], total=len(self._records)))
        return len(self._records)


class _Lcg:
    """Tiny deterministic PRNG so demos never depend on Python's RNG internals."""

    def __init__(self, seed: int) -> None:
        self.state = seed & 0x7FFFFFFF or 1

    def next_int(self, n: int) -> int:
        self.state = (self.state * 1103515245 + 12345) & 0x7FFFFFFF
        return self.state % n

    def next_float(self) -> float:
        self.state = (self.state * 1103515245 + 12345) & 0x7FFFFFFF
        return self.state / 0x7FFFFFFF


def readings_from_response(resp: UpstreamResponse) -> List[dict]:
    return [{"id": r["id"], "revision": r["revision"],
             "updated_at": r["updated_at"]} for r in resp.records]
