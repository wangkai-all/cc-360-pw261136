"""Replayable scenario specifications.

A scenario is data, not a closure: the dataset and every mutation are written as
an operation script, so a *different OS process* can rebuild the identical
upstream (same ids, same update times, same page boundaries).  That is what
makes the "old collection process recovers late" demonstration a real
cross-process test rather than a second object in the same interpreter.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from .upstream import UpstreamSource

SUPPORTED_OPS = ("seed", "revise", "add", "switch")


@dataclass
class ScenarioSpec:
    scenario_id: str
    source_id: str
    seed: int
    records: int
    page_size: int
    ops: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "scenario_id": self.scenario_id, "source_id": self.source_id,
            "seed": self.seed, "records": self.records,
            "page_size": self.page_size, "ops": list(self.ops),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ScenarioSpec":
        return cls(scenario_id=d["scenario_id"], source_id=d["source_id"],
                   seed=int(d["seed"]), records=int(d["records"]),
                   page_size=int(d["page_size"]), ops=list(d.get("ops", [])))

    def save(self, path: Path) -> None:
        path = Path(path)
        path.write_text(json.dumps(self.to_dict(), indent=1, sort_keys=True),
                        encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "ScenarioSpec":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def build_source(spec: ScenarioSpec) -> UpstreamSource:
    """Apply *spec* from scratch.  Pure and deterministic."""
    source = UpstreamSource(spec.source_id, "key_ascending", seed=spec.seed)
    source.seed_records(spec.records)
    for op in spec.ops:
        kind = op.get("op")
        if kind == "seed":
            source.seed_records(int(op["n"]))
        elif kind == "revise":
            source.revise(int(op["id"]), op.get("payload"))
        elif kind == "add":
            source.add_record(op.get("payload"))
        elif kind == "switch":
            source.switch_ordering(op["ordering"])
        else:
            raise ValueError(f"unsupported op {kind!r}")
    return source


def apply_op(source: UpstreamSource, op: dict) -> None:
    kind = op.get("op")
    if kind == "revise":
        source.revise(int(op["id"]), op.get("payload"))
    elif kind == "add":
        source.add_record(op.get("payload"))
    elif kind == "switch":
        source.switch_ordering(op["ordering"])
    else:
        raise ValueError(f"unsupported live op {kind!r}")
