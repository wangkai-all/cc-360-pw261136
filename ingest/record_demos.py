"""Record every demonstration to ``artifacts/`` without a browser.

Each scenario is executed for real (partial scan → upgrade → rehearsal →
approval → verification).  The JSON files are the evidence trail; the UI reads
the same objects from the running server.  Nothing here is a hand-written
transcript.

    python3 -m ingest.record_demos
"""

from __future__ import annotations

import json
from pathlib import Path

from .demo import SCENARIOS, DemoSession

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts"


def run(scenario_id: str, strategy: str) -> dict:
    s = DemoSession(scenario_id)
    s.run_partial()
    if scenario_id == "late_process":
        s.run_late_process()
    else:
        s.apply_upgrade()
    if scenario_id == "repeated_revisions":
        s.same_version_resume()
    if scenario_id == "strategy_compare":
        s.plan("overlap_backfill")
        s.plan("rebaseline")
    else:
        s.plan("overlap_backfill")
    s.approve(strategy)
    return s.state()


def main() -> int:
    OUT.mkdir(exist_ok=True)
    summary = {}
    for sid, meta in SCENARIOS.items():
        strategy = "rebaseline" if sid == "strategy_compare" else "overlap_backfill"
        state = run(sid, strategy)
        payload = {
            "scenario_id": sid,
            "title": meta["title"],
            "approach": strategy,
            "closure": state["verification"]["closure"],
            "counts": state["verification"]["counts"],
            "ingest": state["ingest"],
            "plans": {k: {
                "strategy": k,
                "from_offset": p["from_offset"],
                "planned_pages": p["planned_pages"],
                "baseline_pages": p["baseline_pages"],
                "extra_requests": p["extra_requests"],
                "rechecked_ids": p["rechecked_ids"],
                "residual_ids": p["residual_ids"],
                "may_miss": p["may_miss"],
            } for k, p in state["plans"].items()},
            "late_process": state["late_process"],
            "same_version": state["same_version"],
            "events": state["events"],
        }
        (OUT / f"{sid}.json").write_text(
            json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8")
        summary[sid] = {
            "closure": payload["closure"],
            "verified": payload["counts"]["verified"],
            "unobserved": payload["counts"]["unobserved"],
            "extra_requests": {k: v["extra_requests"] for k, v in payload["plans"].items()},
        }
        print(f"{sid}: {payload['closure']}  "
              f"{payload['counts']['verified']} verified / "
              f"{payload['counts']['unobserved']} unobserved")
    (OUT / "summary.json").write_text(
        json.dumps(summary, indent=1, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
