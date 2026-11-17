"""Durable scan journal.

The harvest process can die at any moment; a separate, older process can wake up
holding a cursor it read hours ago.  Both need the same durable record of what
was actually observed.  Everything the verifier later checks is derived from
this journal - it is the write-ahead truth, the SQLite database is a downstream
projection of it.

Writes are atomic (temp file + ``os.replace``) so a crash mid-write cannot leave
a half journal.  The cursor is persisted *after* the observations of the page
that reaches it, so a crash can produce "observations without a cursor" (safe:
the scan simply resumes earlier and dedupes) but never "cursor without the pages
behind it".
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .probe import Cursor


def _atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, sort_keys=True, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


@dataclass
class Observation:
    record_id: int
    scan_id: str
    run_id: str
    version: str
    ordering: str
    revision: int
    updated_at: int
    payload: str
    request_seq: int
    served_ordering: str
    rehearsal: bool = False
    offset: int = 0
    page_size: int = 0
    has_more: bool = True

    def to_dict(self) -> dict:
        return {
            "record_id": self.record_id, "scan_id": self.scan_id,
            "run_id": self.run_id, "version": self.version,
            "ordering": self.ordering, "revision": self.revision,
            "updated_at": self.updated_at, "payload": self.payload,
            "request_seq": self.request_seq,
            "served_ordering": self.served_ordering,
            "rehearsal": self.rehearsal, "offset": self.offset,
            "page_size": self.page_size, "has_more": self.has_more,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Observation":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})


class Journal:
    def __init__(self, path: Path, source_id: str) -> None:
        self.path = Path(path)
        self.source_id = source_id
        self._lock = threading.RLock()
        self._data: dict = {"source_id": source_id, "scans": {},
                            "observations": [], "cursor": None}
        if self.path.exists():
            with self.path.open(encoding="utf-8") as fh:
                self._data = json.load(fh)
        if self._data.get("source_id") != source_id:
            raise ValueError(f"journal at {self.path} belongs to {self._data.get('source_id')!r}")

    # -- scans -----------------------------------------------------------
    def open_scan(self, scan_id: str, run_id: str, version: str, ordering: str) -> dict:
        with self._lock:
            scans = self._data["scans"]
            if scan_id in scans:
                return scans[scan_id]
            scans[scan_id] = {
                "scan_id": scan_id, "run_id": run_id, "version": version,
                "ordering": ordering, "status": "running",
                "request_seqs": [], "stop_reason": None,
                "started_request_seq": None, "ended_request_seq": None,
            }
            self._flush()
            return scans[scan_id]

    def note_requests(self, scan_id: str, seqs: List[int]) -> None:
        with self._lock:
            scan = self._data["scans"][scan_id]
            scan["request_seqs"].extend(seqs)
            if scan["started_request_seq"] is None and seqs:
                scan["started_request_seq"] = min(seqs)
            if seqs:
                scan["ended_request_seq"] = max(seqs)
            self._flush()

    def finish_scan(self, scan_id: str, status: str, stop_reason: Optional[str] = None,
                    cursor: Optional[Cursor] = None) -> None:
        with self._lock:
            scan = self._data["scans"][scan_id]
            scan["status"] = status
            scan["stop_reason"] = stop_reason
            if cursor is not None:
                scan["cursor"] = cursor.to_dict()
            self._flush()

    def set_cursor(self, cursor: Optional[Cursor]) -> None:
        with self._lock:
            self._data["cursor"] = cursor.to_dict() if cursor else None
            self._flush()

    def cursor(self) -> Optional[Cursor]:
        with self._lock:
            raw = self._data.get("cursor")
            return Cursor.from_dict(raw) if raw else None

    def scan(self, scan_id: str) -> dict:
        with self._lock:
            return dict(self._data["scans"][scan_id])

    def scans(self) -> List[dict]:
        with self._lock:
            return [dict(s) for s in self._data["scans"].values()]

    def scan_ids(self) -> List[str]:
        with self._lock:
            return list(self._data["scans"])

    def reopen_scan_for_progress(self, scan_id: str) -> None:
        """A resumed run continues the same logical scan; keep it running."""
        with self._lock:
            scan = self._data["scans"].setdefault(scan_id, {
                "scan_id": scan_id, "run_id": None, "version": None,
                "ordering": None, "status": "running", "request_seqs": [],
                "stop_reason": None, "started_request_seq": None,
                "ended_request_seq": None})
            scan["status"] = "running"
            scan["stop_reason"] = None
            self._flush()

    # -- observations ----------------------------------------------------
    def record(self, obs: Observation) -> None:
        with self._lock:
            self._data["observations"].append(obs.to_dict())
            self._flush()

    def observations(self, scan_id: Optional[str] = None,
                     include_rehearsal: bool = False) -> List[Observation]:
        with self._lock:
            out = []
            for raw in self._data["observations"]:
                if scan_id is not None and raw["scan_id"] != scan_id:
                    continue
                if not include_rehearsal and raw.get("rehearsal"):
                    continue
                out.append(Observation.from_dict(raw))
            return out

    def max_revision(self) -> Dict[int, int]:
        with self._lock:
            out: Dict[int, int] = {}
            for raw in self._data["observations"]:
                if raw.get("rehearsal"):
                    continue
                rid = raw["record_id"]
                out[rid] = max(out.get(rid, 0), raw["revision"])
            return out

    # -- reporting -------------------------------------------------------
    def to_dict(self) -> dict:
        with self._lock:
            return json.loads(json.dumps(self._data))

    def _flush(self) -> None:
        _atomic_write(self.path, self._data)


class CursorFile:
    """Standalone cursor file, written after the journal.

    A cursor on its own - the thing that survives in a monitoring system or gets
    handed to a late-starting worker - is exactly what the upgrade renders
    meaningless.  Keeping it separate from the journal makes that distinction
    explicit instead of hiding it inside one blob.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def read(self) -> Optional[Cursor]:
        if not self.path.exists():
            return None
        with self.path.open(encoding="utf-8") as fh:
            raw = json.load(fh)
        return Cursor.from_dict(raw)

    def write(self, cursor: Cursor) -> None:
        _atomic_write(self.path, cursor.to_dict())
