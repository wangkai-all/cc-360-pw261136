"""Collection path: retry at the edge, and never let the edge's retry hide a loss.

An upstream can answer 503 on the first attempt of any request.  Retrying is
fine; what is *not* fine is treating "the retry succeeded" as proof that nothing
changed between attempts.  Every attempt is logged, and a page whose retry
returned a different id set than the first attempt emits a
``page_shifted_between_attempts`` fault that verification must resolve.

The collector is also where a version switch physically happens: it holds a
:class:`~ingest.versions.SourceProfile` per source and refuses to fetch with a
profile whose ordering disagrees with the last probe for that source.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .upstream import UpstreamResponse, UpstreamSource
from .versions import SourceProfile


class UpstreamUnavailable(RuntimeError):
    pass


@dataclass
class Attempt:
    n: int
    outcome: str
    ids: List[int] = field(default_factory=list)
    served_ordering: Optional[str] = None

    def to_dict(self) -> dict:
        return {"attempt": self.n, "outcome": self.outcome,
                "ids": list(self.ids), "served_ordering": self.served_ordering}


@dataclass
class CollectionFault:
    kind: str
    request_seq: int
    offset: int
    detail: str
    attempts: List[Attempt] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"kind": self.kind, "request_seq": self.request_seq,
                "offset": self.offset, "detail": self.detail,
                "attempts": [a.to_dict() for a in self.attempts]}


@dataclass
class CollectedPage:
    response: UpstreamResponse
    attempts: List[Attempt]
    fault: Optional[CollectionFault] = None


class Collector:
    """Retrying edge.

    Two kinds of retry are distinguished, because only one of them is safe to
    ignore:

    * a request that never reached the source (``inject``) - retry, no evidence
      of change;
    * a request whose response was lost after the source served it
      (``inject_lost_response``) - the server *did* advance its view, and the
      retry may return a different page.  That is a fault, not a clean retry.
    """

    def __init__(self, source: UpstreamSource, max_attempts: int = 3) -> None:
        self.source = source
        self.max_attempts = max_attempts
        self.faults: List[CollectionFault] = []
        self.attempt_log: List[dict] = []
        self.inject_failures: Dict[int, int] = {}
        self.inject_lost: Dict[int, int] = {}
        self.on_response = None      # callable(request_no) after a served response
        self._request_no = 0

    def inject(self, request_no: int, failures: int = 1) -> None:
        """Make the first *failures* attempts fail before reaching the source."""
        self.inject_failures[request_no] = failures

    def inject_lost_response(self, request_no: int, count: int = 1) -> None:
        """Serve the page, then drop the response on the first *count* attempts."""
        self.inject_lost[request_no] = count

    def collect(self, offset: int, limit: int, *, scan_id: str, run_id: str,
                rehearsal: bool = False) -> CollectedPage:
        self._request_no += 1
        request_no = self._request_no
        failures_left = self.inject_failures.pop(request_no, 0)
        lost_left = self.inject_lost.pop(request_no, 0)
        attempts: List[Attempt] = []
        first_ids: Optional[List[int]] = None
        resp: Optional[UpstreamResponse] = None
        for n in range(1, self.max_attempts + 1):
            if failures_left > 0:
                failures_left -= 1
                attempts.append(Attempt(n, "unavailable"))
                self.attempt_log.append({"request_no": request_no, "attempt": n,
                                         "outcome": "unavailable", "offset": offset})
                continue
            try:
                candidate = self.source.fetch(offset, limit, scan_id=scan_id,
                                              run_id=run_id, rehearsal=rehearsal)
            except RuntimeError:
                attempts.append(Attempt(n, "error"))
                continue
            if self.on_response is not None:
                self.on_response(request_no)
            if lost_left > 0:
                lost_left -= 1
                attempts.append(Attempt(n, "timeout_after_body",
                                        candidate.ids(), candidate.served_ordering))
                self.attempt_log.append({"request_no": request_no, "attempt": n,
                                         "outcome": "timeout_after_body",
                                         "offset": offset, "ids": candidate.ids()})
                if first_ids is None:
                    first_ids = candidate.ids()
                continue
            attempts.append(Attempt(n, "ok", candidate.ids(), candidate.served_ordering))
            self.attempt_log.append({"request_no": request_no, "attempt": n,
                                     "outcome": "ok", "offset": offset,
                                     "ids": candidate.ids()})
            resp = candidate
            if first_ids is None:
                first_ids = candidate.ids()
            elif first_ids != candidate.ids():
                fault = CollectionFault(
                    kind="page_shifted_between_attempts",
                    request_seq=self.source.log.entries[-1].seq,
                    offset=offset,
                    detail=(f"first attempt saw {first_ids}, attempt {n} returned "
                            f"{candidate.ids()}; the page moved under the retry"),
                    attempts=attempts)
                self.faults.append(fault)
                return CollectedPage(resp, attempts, fault)
            break
        if resp is None:
            raise UpstreamUnavailable(
                f"offset {offset}: {self.max_attempts} attempts, no usable page")
        return CollectedPage(resp, attempts)

    def fault_report(self) -> List[dict]:
        return [f.to_dict() for f in self.faults]


class SourceRegistry:
    """Version-switchable source profiles with an explicit switch record."""

    def __init__(self, profiles: List[SourceProfile]) -> None:
        self.profiles: Dict[str, SourceProfile] = {}
        for p in profiles:
            self.profiles[p.version] = p
        self.active_version: Optional[str] = None
        self.switches: List[dict] = []

    def activate(self, version: str, *, actor: str, plan: Optional[dict] = None,
                 ordering_probe: Optional[dict] = None) -> dict:
        if version not in self.profiles:
            raise KeyError(f"no profile for version {version}")
        prev = self.active_version
        self.active_version = version
        record = {"from": prev, "to": version, "actor": actor,
                  "profile": self.profile(version).__dict__,
                  "probe": ordering_probe, "plan": plan}
        self.switches.append(record)
        return record

    def profile(self, version: Optional[str] = None) -> SourceProfile:
        v = version or self.active_version
        if v is None:
            raise KeyError("no active version")
        return self.profiles[v]
