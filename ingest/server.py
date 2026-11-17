"""HTTP entry point: serves the review UI and a small JSON API.

Standard library only.  The API is intentionally write-with-intent: a recovery
plan is rehearsed, returns its measured cost and residual range, and only an
explicit second call approves it.  That mirrors the CLI and the tests.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

from .demo import SCENARIOS, DemoSession, run_full

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"


class SessionStore:
    def __init__(self) -> None:
        self._sessions: Dict[str, DemoSession] = {}
        self._lock = threading.Lock()

    def create(self, scenario_id: str) -> str:
        if scenario_id not in SCENARIOS:
            raise KeyError(scenario_id)
        sid = uuid.uuid4().hex[:12]
        with self._lock:
            self._sessions[sid] = DemoSession(scenario_id)
        return sid

    def get(self, sid: str) -> DemoSession:
        with self._lock:
            if sid not in self._sessions:
                raise KeyError(sid)
            return self._sessions[sid]


STORE = SessionStore()


class Handler(BaseHTTPRequestHandler):
    server_version = "IngestReview/1.0"

    def log_message(self, fmt, *args):  # quieter default logging
        pass

    # -- helpers ---------------------------------------------------------
    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy",
                         "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                         "script-src 'self'")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json")

    def _error(self, status: int, message: str) -> None:
        self._json(status, {"error": message})

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if len(raw) > 1_000_000:
            raise ValueError("body too large")
        return json.loads(raw.decode("utf-8"))

    def _static(self, path: str) -> None:
        rel = path.lstrip("/")
        target = (WEB / rel).resolve()
        if not str(target).startswith(str(WEB.resolve())) or not target.is_file():
            self._error(HTTPStatus.NOT_FOUND, "not found")
            return
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self._send(HTTPStatus.OK, target.read_bytes(), ctype)

    # -- routes ----------------------------------------------------------
    def do_GET(self) -> None:
        url = urlparse(self.path)
        path = url.path
        try:
            if path == "/":
                self._static("index.html")
            elif path.startswith("/static/"):
                self._static(path[len("/static/"):])
            elif path == "/api/scenarios":
                self._json(OK, {"scenarios": [
                    {"id": k, "title": v["title"], "blurb": v["blurb"]}
                    for k, v in SCENARIOS.items()]})
            elif path.startswith("/api/session/"):
                sid = path.split("/")[3]
                self._json(OK, STORE.get(sid).state())
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")
        except KeyError:
            self._error(HTTPStatus.NOT_FOUND, "no such session or scenario")
        except Exception as exc:  # noqa: BLE001
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def do_POST(self) -> None:
        url = urlparse(self.path)
        parts = [p for p in url.path.split("/") if p]
        try:
            if url.path == "/api/session":
                body = self._body()
                sid = STORE.create(body.get("scenario", ""))
                self._json(HTTPStatus.CREATED, {"session_id": sid,
                                                "state": STORE.get(sid).state()})
                return
            if len(parts) < 4 or parts[0] != "api" or parts[1] != "session":
                self._error(HTTPStatus.NOT_FOUND, "not found")
                return
            sid, action = parts[2], parts[3]
            session = STORE.get(sid)
            body = self._body()
            if action == "run_partial":
                payload = session.run_partial()
            elif action == "upgrade":
                payload = session.apply_upgrade()
            elif action == "plan":
                payload = session.plan(body.get("strategy", "overlap_backfill"))
            elif action == "approve":
                payload = session.approve(body.get("strategy", "overlap_backfill"))
            elif action == "late_process":
                payload = session.run_late_process()
            elif action == "same_version_resume":
                payload = session.same_version_resume()
            else:
                self._error(HTTPStatus.NOT_FOUND, "unknown action")
                return
            self._json(OK, payload)
        except KeyError as exc:
            self._error(HTTPStatus.NOT_FOUND, f"not found: {exc}")
        except (ValueError, RuntimeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        except Exception as exc:  # noqa: BLE001
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))


OK = HTTPStatus.OK


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Version-switchable ingestion review UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--run", choices=sorted(SCENARIOS),
                        help="run one scenario headlessly and print its state")
    args = parser.parse_args(argv)
    if args.run:
        print(json.dumps(run_full(args.run), indent=1, sort_keys=True))
        return 0
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"review UI: http://{args.host}:{args.port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
