"""
ui/server.py
============

Small local dashboard for `la_county_parcel_sync.py`: launch/monitor sync
runs, and review + accept/reject the inserted/updated snapshots a run
produced before they're considered final.

Stdlib-only -- no new dependency beyond what the sync script already needs
(`requests`). The web layer is `http.server`; the review workflow adds one
extra SQLite table (`parcel_reviews`) alongside the sync script's own
tables, created here so the core script stays untouched and independently
testable.

This is a local admin tool: it binds to 127.0.0.1 by default and has no
authentication. Do not expose it on a shared network.

Run from the project root:

    python ui/server.py
    # then open http://127.0.0.1:8765
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sqlite3
import subprocess
import sys
import threading
import uuid
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlsplit

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
SYNC_SCRIPT = PROJECT_ROOT / "la_county_parcel_sync.py"

sys.path.insert(0, str(PROJECT_ROOT))
import la_county_parcel_sync as sync  # noqa: E402  (path must be set up first)

DEFAULT_STATE_DB = PROJECT_ROOT / "state" / "parcels.sqlite3"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"

# Additive schema for the review workflow. Kept separate from
# la_county_parcel_sync.SCHEMA_SQL so the sync engine never needs to know
# the UI exists.
REVIEW_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS parcel_reviews (
    snapshot_id TEXT PRIMARY KEY,
    review_status TEXT NOT NULL DEFAULT 'pending' CHECK (review_status IN ('pending', 'accepted', 'rejected')),
    reviewed_content_hash TEXT,
    reviewed_at TEXT,
    reviewer TEXT,
    note TEXT
);
CREATE INDEX IF NOT EXISTS idx_parcel_reviews_status ON parcel_reviews(review_status);
CREATE INDEX IF NOT EXISTS idx_parcel_snapshots_last_seen_at ON parcel_snapshots(last_seen_at);
"""

RUN_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ApiError(Exception):
    """Raised by a handler to send a specific HTTP status + JSON error body."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


# --------------------------------------------------------------------------
# App / job state
# --------------------------------------------------------------------------


class SyncJob:
    """Tracks one `la_county_parcel_sync.py` subprocess and its live log tail."""

    def __init__(self, job_id: str, args: List[str]):
        self.job_id = job_id
        self.args = args
        self.log: deque = deque(maxlen=4000)
        self.started_at = _utc_now_iso()
        self.finished_at: Optional[str] = None
        self.returncode: Optional[int] = None
        self.process: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self.process = subprocess.Popen(
            [sys.executable, str(SYNC_SCRIPT), *self.args],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _pump(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        for line in self.process.stdout:
            self.log.append(line.rstrip("\n"))
        self.process.wait()
        self.returncode = self.process.returncode
        self.finished_at = _utc_now_iso()

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def to_dict(self, tail: int = 400) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "args": self.args,
            "running": self.running,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "returncode": self.returncode,
            "log": list(self.log)[-tail:],
        }


class AppState:
    """Holds configured paths and the single in-flight sync job (one at a time)."""

    def __init__(self, state_db: Path, output_dir: Path):
        self.state_db = state_db
        self.output_dir = output_dir
        self.lock = threading.Lock()
        self.job: Optional[SyncJob] = None

    def db(self) -> sqlite3.Connection:
        self.state_db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.state_db), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(sync.SCHEMA_SQL)
        conn.executescript(REVIEW_SCHEMA_SQL)
        return conn


APP: AppState


# --------------------------------------------------------------------------
# Review-bucket SQL
# --------------------------------------------------------------------------


def review_bucket_clause(status: str) -> str:
    """
    A snapshot is 'pending' if it was never reviewed, or if it changed again
    (content_hash moved) since it was last accepted/rejected -- so an edit
    to an already-approved parcel automatically falls back into the queue.
    """
    if status == "accepted":
        return "(r.review_status = 'accepted' AND r.reviewed_content_hash = s.content_hash)"
    if status == "rejected":
        return "(r.review_status = 'rejected' AND r.reviewed_content_hash = s.content_hash)"
    if status == "pending":
        return (
            "(r.snapshot_id IS NULL OR r.review_status = 'pending' "
            "OR IFNULL(r.reviewed_content_hash, '') != s.content_hash)"
        )
    return "1=1"


def effective_review_status(row: sqlite3.Row) -> str:
    if row["review_status"] is None:
        return "pending"
    if (row["reviewed_content_hash"] or "") != row["content_hash"]:
        return "pending"
    return row["review_status"]


# --------------------------------------------------------------------------
# GET handlers
# --------------------------------------------------------------------------


def handle_overview(conn: sqlite3.Connection, qs: Dict[str, List[str]]) -> Dict[str, Any]:
    total = conn.execute("SELECT COUNT(*) FROM parcel_snapshots").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM parcel_snapshots WHERE status='active'").fetchone()[0]
    join = "FROM parcel_snapshots s LEFT JOIN parcel_reviews r ON r.snapshot_id = s.snapshot_id WHERE "
    pending = conn.execute(f"SELECT COUNT(*) {join}{review_bucket_clause('pending')}").fetchone()[0]
    accepted = conn.execute(f"SELECT COUNT(*) {join}{review_bucket_clause('accepted')}").fetchone()[0]
    rejected = conn.execute(f"SELECT COUNT(*) {join}{review_bucket_clause('rejected')}").fetchone()[0]
    last_run = conn.execute("SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 1").fetchone()
    runs_count = conn.execute("SELECT COUNT(*) FROM sync_runs").fetchone()[0]
    return {
        "total_snapshots": total,
        "active": active,
        "inactive": total - active,
        "pending_review": pending,
        "accepted": accepted,
        "rejected": rejected,
        "last_run": dict(last_run) if last_run else None,
        "runs_count": runs_count,
    }


def handle_runs(conn: sqlite3.Connection, qs: Dict[str, List[str]]) -> Dict[str, Any]:
    limit = max(1, min(500, int(qs.get("limit", ["50"])[0])))
    rows = conn.execute("SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
    runs = []
    for row in rows:
        d = dict(row)
        errors_path = APP.output_dir / f"errors_{row['run_id']}.ndjson"
        d["has_errors"] = errors_path.exists() and errors_path.stat().st_size > 0
        d["inactivated_count"] = conn.execute(
            "SELECT COUNT(*) FROM parcel_history WHERE run_id = ? AND change_type = 'inactivated'",
            (row["run_id"],),
        ).fetchone()[0]
        runs.append(d)
    return {"runs": runs}


def handle_changes(conn: sqlite3.Connection, qs: Dict[str, List[str]]) -> Dict[str, Any]:
    status = qs.get("status", ["pending"])[0]
    event = qs.get("event", ["all"])[0]
    q = qs.get("q", [""])[0].strip()
    limit = max(1, min(200, int(qs.get("limit", ["25"])[0])))
    offset = max(0, int(qs.get("offset", ["0"])[0]))

    where_parts = [review_bucket_clause(status)]
    params: List[Any] = []

    if event in ("inserted", "updated"):
        where_parts.append("json_extract(s.current_json, '$.event_type') = ?")
        params.append(event)

    if q:
        like = f"%{q}%"
        where_parts.append(
            "(s.property_id LIKE ? OR s.snapshot_id LIKE ? "
            "OR json_extract(s.current_json, '$.normalized.property_location') LIKE ? "
            "OR json_extract(s.current_json, '$.normalized.city') LIKE ?)"
        )
        params.extend([like, like, like, like])

    where_sql = " AND ".join(where_parts)
    base = f"FROM parcel_snapshots s LEFT JOIN parcel_reviews r ON r.snapshot_id = s.snapshot_id WHERE {where_sql}"

    total = conn.execute(f"SELECT COUNT(*) {base}", params).fetchone()[0]
    rows = conn.execute(
        f"""SELECT s.snapshot_id, s.property_id, s.roll_year, s.content_hash,
                   s.status AS parcel_status, s.last_seen_at, s.last_seen_run_id, s.current_json,
                   r.review_status, r.reviewed_at, r.reviewer, r.note, r.reviewed_content_hash
            {base} ORDER BY s.last_seen_at DESC LIMIT ? OFFSET ?""",
        [*params, limit, offset],
    ).fetchall()

    items = []
    for row in rows:
        record = json.loads(row["current_json"])
        items.append(
            {
                "snapshot_id": row["snapshot_id"],
                "property_id": row["property_id"],
                "roll_year": row["roll_year"],
                "content_hash": row["content_hash"],
                "parcel_status": row["parcel_status"],
                "last_seen_at": row["last_seen_at"],
                "last_seen_run_id": row["last_seen_run_id"],
                "event_type": record.get("event_type"),
                "normalized": record.get("normalized"),
                "review": {
                    "status": effective_review_status(row),
                    "reviewed_at": row["reviewed_at"],
                    "reviewer": row["reviewer"],
                    "note": row["note"],
                },
            }
        )

    return {"total": total, "items": items, "limit": limit, "offset": offset}


def handle_snapshot(conn: sqlite3.Connection, qs: Dict[str, List[str]]) -> Dict[str, Any]:
    sid = qs.get("id", [""])[0]
    if not sid:
        raise ApiError(400, "Missing 'id' query parameter")
    row = conn.execute("SELECT * FROM parcel_snapshots WHERE snapshot_id = ?", (sid,)).fetchone()
    if row is None:
        raise ApiError(404, f"Unknown snapshot_id {sid!r}")
    current = json.loads(row["current_json"])

    prev_row = conn.execute(
        """SELECT json_data FROM parcel_history
           WHERE snapshot_id = ? AND change_type = 'updated' ORDER BY id DESC LIMIT 1""",
        (sid,),
    ).fetchone()
    previous = json.loads(prev_row["json_data"]) if prev_row else None

    review_row = conn.execute("SELECT * FROM parcel_reviews WHERE snapshot_id = ?", (sid,)).fetchone()
    review = dict(review_row) if review_row else None
    if review is not None:
        review["effective_status"] = effective_review_status(row)

    return {"current": current, "previous": previous, "review": review}


def handle_errors(conn: sqlite3.Connection, qs: Dict[str, List[str]]) -> Dict[str, Any]:
    run_id = qs.get("run_id", [""])[0]
    limit = max(1, min(1000, int(qs.get("limit", ["200"])[0])))
    if not run_id or not RUN_ID_RE.match(run_id):
        raise ApiError(400, "A valid run_id is required")
    path = APP.output_dir / f"errors_{run_id}.ndjson"
    if not path.exists():
        return {"items": [], "truncated": False, "total_lines": 0}

    items: List[Any] = []
    total_lines = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            if len(items) < limit:
                stripped = line.strip()
                if stripped:
                    try:
                        items.append(json.loads(stripped))
                    except json.JSONDecodeError:
                        continue
    return {"items": items, "truncated": total_lines > limit, "total_lines": total_lines}


def handle_sync_status(conn: sqlite3.Connection, qs: Dict[str, List[str]]) -> Dict[str, Any]:
    tail = max(50, min(4000, int(qs.get("tail", ["400"])[0])))
    with APP.lock:
        if APP.job is None:
            return {"job": None}
        return {"job": APP.job.to_dict(tail=tail)}


# --------------------------------------------------------------------------
# POST handlers
# --------------------------------------------------------------------------


def handle_review_submit(conn: sqlite3.Connection, body: Dict[str, Any]) -> Dict[str, Any]:
    snapshot_ids = body.get("snapshot_ids") or []
    decision = body.get("decision")
    reviewer = (body.get("reviewer") or "").strip() or "anonymous"
    note = (body.get("note") or "").strip()

    if decision not in ("accepted", "rejected"):
        raise ApiError(400, "decision must be 'accepted' or 'rejected'")
    if not snapshot_ids:
        raise ApiError(400, "snapshot_ids is required")

    now = _utc_now_iso()
    updated = 0
    for sid in snapshot_ids:
        row = conn.execute("SELECT content_hash FROM parcel_snapshots WHERE snapshot_id = ?", (sid,)).fetchone()
        if row is None:
            continue
        conn.execute(
            """INSERT INTO parcel_reviews (snapshot_id, review_status, reviewed_content_hash, reviewed_at, reviewer, note)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(snapshot_id) DO UPDATE SET
                   review_status = excluded.review_status,
                   reviewed_content_hash = excluded.reviewed_content_hash,
                   reviewed_at = excluded.reviewed_at,
                   reviewer = excluded.reviewer,
                   note = excluded.note""",
            (sid, decision, row["content_hash"], now, reviewer, note),
        )
        updated += 1
    conn.commit()
    return {"updated": updated}


def handle_review_reset(conn: sqlite3.Connection, body: Dict[str, Any]) -> Dict[str, Any]:
    snapshot_ids = body.get("snapshot_ids") or []
    if not snapshot_ids:
        raise ApiError(400, "snapshot_ids is required")
    conn.executemany("DELETE FROM parcel_reviews WHERE snapshot_id = ?", [(s,) for s in snapshot_ids])
    conn.commit()
    return {"reset": len(snapshot_ids)}


def handle_sync_start(conn: sqlite3.Connection, body: Dict[str, Any]) -> Dict[str, Any]:
    with APP.lock:
        if APP.job is not None and APP.job.running:
            raise ApiError(409, "A sync is already running")

        max_records = body.get("max_records")
        try:
            page_size = int(body.get("page_size") or 1000)
        except (TypeError, ValueError):
            raise ApiError(400, "page_size must be an integer")
        where = (body.get("where") or "1=1").strip() or "1=1"
        resume = bool(body.get("resume"))
        fresh_run = bool(body.get("fresh_run"))
        request_delay = body.get("request_delay")

        args = [
            "--state-db", str(APP.state_db),
            "--output-dir", str(APP.output_dir),
            "--log-level", "INFO",
            "--page-size", str(page_size),
            "--where", where,
        ]
        if max_records in (None, "", "none", "None"):
            args += ["--max-records", "none"]
        else:
            try:
                args += ["--max-records", str(int(max_records))]
            except (TypeError, ValueError):
                raise ApiError(400, "max_records must be an integer or 'none'")
        if request_delay not in (None, ""):
            try:
                args += ["--request-delay", str(float(request_delay))]
            except (TypeError, ValueError):
                raise ApiError(400, "request_delay must be a number")
        if resume:
            args.append("--resume")
        if fresh_run:
            args.append("--fresh-run")

        job = SyncJob(job_id=uuid.uuid4().hex[:12], args=args)
        job.start()
        APP.job = job
        return job.to_dict()


# --------------------------------------------------------------------------
# HTTP plumbing
# --------------------------------------------------------------------------

GET_ROUTES: Dict[str, Callable[[sqlite3.Connection, Dict[str, List[str]]], Dict[str, Any]]] = {
    "/api/overview": handle_overview,
    "/api/runs": handle_runs,
    "/api/changes": handle_changes,
    "/api/snapshot": handle_snapshot,
    "/api/errors": handle_errors,
    "/api/sync/status": handle_sync_status,
}
POST_ROUTES: Dict[str, Callable[[sqlite3.Connection, Dict[str, Any]], Dict[str, Any]]] = {
    "/api/review": handle_review_submit,
    "/api/review/reset": handle_review_reset,
    "/api/sync/start": handle_sync_start,
}


def _stream_export(handler: "Handler") -> None:
    repo = sync.StateRepository(APP.state_db)
    try:
        filename = f"current_parcels_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.ndjson"
        handler.send_response(200)
        handler.send_header("Content-Type", "application/x-ndjson")
        handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        handler.end_headers()
        for line in repo.iter_current():
            handler.wfile.write(line.encode("utf-8"))
            if not line.endswith("\n"):
                handler.wfile.write(b"\n")
    finally:
        repo.close()


class Handler(BaseHTTPRequestHandler):
    server_version = "ParcelSyncUI/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # quieter console
        pass

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError(400, f"Invalid JSON body: {exc}")

    def _dispatch(self, method: str, path: str, qs: Dict[str, List[str]], body: Dict[str, Any]) -> None:
        routes = GET_ROUTES if method == "GET" else POST_ROUTES
        fn = routes.get(path)
        if fn is None:
            self._send_json(404, {"error": f"Unknown endpoint: {method} {path}"})
            return
        try:
            conn = APP.db()
            try:
                result = fn(conn, qs) if method == "GET" else fn(conn, body)
            finally:
                conn.close()
            self._send_json(200, result)
        except ApiError as exc:
            self._send_json(exc.status, {"error": exc.message})
        except Exception as exc:  # noqa: BLE001 - surface as JSON, never crash the server
            self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})

    def do_GET(self) -> None:
        parts = urlsplit(self.path)
        if parts.path == "/api/export":
            try:
                _stream_export(self)
            except Exception as exc:  # noqa: BLE001
                self._send_json(500, {"error": str(exc)})
            return
        if parts.path.startswith("/api/"):
            self._dispatch("GET", parts.path, parse_qs(parts.query), {})
            return
        self._serve_static(parts.path)

    def do_POST(self) -> None:
        parts = urlsplit(self.path)
        try:
            body = self._read_json_body()
        except ApiError as exc:
            self._send_json(exc.status, {"error": exc.message})
            return
        self._dispatch("POST", parts.path, parse_qs(parts.query), body)

    def _serve_static(self, url_path: str) -> None:
        rel = url_path.lstrip("/") or "index.html"
        candidate = (STATIC_DIR / rel).resolve()
        if candidate != STATIC_DIR and STATIC_DIR not in candidate.parents:
            self.send_error(403)
            return
        if not candidate.exists() or not candidate.is_file():
            candidate = STATIC_DIR / "index.html"
            if not candidate.exists():
                self.send_error(404)
                return
        content_type, _ = mimetypes.guess_type(str(candidate))
        data = candidate.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Local dashboard for la_county_parcel_sync.py")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state-db", default=str(DEFAULT_STATE_DB))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    global APP
    APP = AppState(Path(args.state_db), Path(args.output_dir))
    APP.output_dir.mkdir(parents=True, exist_ok=True)
    APP.db().close()  # create/verify schema up front so the first page load is never empty-DB-broken

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"LA Parcel Sync UI running at http://{args.host}:{args.port}")
    print(f"State DB:   {APP.state_db}")
    print(f"Output dir: {APP.output_dir}")
    print("Press Ctrl+C to stop.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
