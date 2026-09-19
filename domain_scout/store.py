"""SQLite persistence: the domain check cache plus immutable run history.

Two concerns share one database file:

* ``cache``       - domain-keyed, TTL'd, disposable. A pure speed optimization.
* ``runs`` /
  ``run_results`` - append-only historical record. The source of truth for the UI.

A cached check still gets written to ``run_results`` (with ``cached = 1``) so that
a warm re-scan produces a complete run record rather than an empty one.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Optional

from .engine import Result

# Bump when the Result dataclass or cache payload shape changes. The cache is
# disposable, so a mismatch just clears it; run history is never destroyed.
SCHEMA_VERSION = 2

RESULT_COLUMNS = [
    "keyword", "tld", "domain", "resolved", "ip_addresses", "active_html",
    "url", "status", "server", "server_family", "provider", "provider_evidence",
    "title", "content_type", "elapsed_ms", "cached", "error",
]

_JSON_COLUMNS = ("ip_addresses", "provider_evidence")
_BOOL_COLUMNS = ("resolved", "active_html", "cached")

ACTIVE_STATUSES = ("queued", "running")


def _row_to_result_dict(row: sqlite3.Row) -> dict:
    """Turn a run_results row back into plain JSON-friendly data."""
    data = {k: row[k] for k in RESULT_COLUMNS}
    for col in _JSON_COLUMNS:
        data[col] = json.loads(data[col] or "[]")
    for col in _BOOL_COLUMNS:
        data[col] = bool(data[col])
    return data


class Store:
    def __init__(self, path: str, cache_ttl: int):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_ttl = cache_ttl
        self.mem: dict[str, Result] = {}
        self._lock = threading.Lock()

        # check_same_thread=False so the web server can touch the connection from
        # the event loop and from worker threads; every write is serialized by
        # self._lock instead.
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        # WAL lets history reads proceed while a scan is writing.
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    # ------------------------------------------------------------------ schema

    def _migrate(self):
        with self._lock:
            self.db.executescript(f"""
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cache (
                    domain     TEXT PRIMARY KEY,
                    checked_at INTEGER NOT NULL,
                    payload    TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    tlds           TEXT    NOT NULL,
                    keywords       TEXT    NOT NULL,
                    status         TEXT    NOT NULL,
                    force          INTEGER NOT NULL DEFAULT 0,
                    total          INTEGER NOT NULL DEFAULT 0,
                    completed      INTEGER NOT NULL DEFAULT 0,
                    resolved_count INTEGER NOT NULL DEFAULT 0,
                    active_count   INTEGER NOT NULL DEFAULT 0,
                    cache_hits     INTEGER NOT NULL DEFAULT 0,
                    error_count    INTEGER NOT NULL DEFAULT 0,
                    created_at     INTEGER NOT NULL,
                    started_at     INTEGER,
                    finished_at    INTEGER,
                    elapsed_ms     INTEGER,
                    error          TEXT,
                    deleted_at     INTEGER
                );

                CREATE TABLE IF NOT EXISTS run_results (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id            INTEGER NOT NULL
                                      REFERENCES runs(id) ON DELETE CASCADE,
                    keyword           TEXT    NOT NULL,
                    tld               TEXT    NOT NULL,
                    domain            TEXT    NOT NULL,
                    resolved          INTEGER NOT NULL,
                    ip_addresses      TEXT    NOT NULL DEFAULT '[]',
                    active_html       INTEGER NOT NULL,
                    url               TEXT,
                    status            INTEGER,
                    server            TEXT,
                    server_family     TEXT,
                    provider          TEXT,
                    provider_evidence TEXT    NOT NULL DEFAULT '[]',
                    title             TEXT,
                    content_type      TEXT,
                    elapsed_ms        INTEGER NOT NULL DEFAULT 0,
                    cached            INTEGER NOT NULL DEFAULT 0,
                    error             TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_run_results_run
                    ON run_results(run_id);
                CREATE INDEX IF NOT EXISTS idx_runs_visible
                    ON runs(deleted_at, id DESC);
            """)

            row = self.db.execute(
                "SELECT value FROM schema_meta WHERE key = 'version'"
            ).fetchone()
            current = int(row["value"]) if row else None

            if current != SCHEMA_VERSION:
                # The cache payload shape is tied to the Result dataclass. Rather
                # than risk Result(**data) raising on an old row, drop the cache;
                # it rebuilds on the next scan. Run history is left untouched.
                self.db.execute("DELETE FROM cache")
                self.db.execute(
                    "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            self.db.commit()

    # ------------------------------------------------------------------- cache

    def cache_get(self, domain: str) -> Optional[Result]:
        if domain in self.mem:
            result = self.mem[domain]
            result.cached = True
            return result

        row = self.db.execute(
            "SELECT checked_at, payload FROM cache WHERE domain = ?", (domain,)
        ).fetchone()
        if not row:
            return None
        if int(time.time()) - row["checked_at"] > self.cache_ttl:
            return None

        try:
            data = json.loads(row["payload"])
            data["cached"] = True
            result = Result(**data)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

        self.mem[domain] = result
        return result

    def cache_put(self, result: Result) -> None:
        result.cached = False
        payload = json.dumps(result.as_dict(), separators=(",", ":"))
        self.mem[result.domain] = result
        with self._lock:
            self.db.execute(
                "INSERT OR REPLACE INTO cache(domain, checked_at, payload) VALUES (?, ?, ?)",
                (result.domain, int(time.time()), payload),
            )
            self.db.commit()

    # -------------------------------------------------------------------- runs

    def create_run(self, tlds: list[str], keywords: list[str], force: bool) -> int:
        total = len(keywords) * len(tlds)
        with self._lock:
            cur = self.db.execute(
                """INSERT INTO runs(tlds, keywords, status, force, total, created_at)
                   VALUES (?, ?, 'queued', ?, ?, ?)""",
                (
                    json.dumps(tlds),
                    json.dumps(keywords),
                    int(force),
                    total,
                    int(time.time()),
                ),
            )
            self.db.commit()
            return int(cur.lastrowid)

    def mark_running(self, run_id: int) -> None:
        with self._lock:
            self.db.execute(
                "UPDATE runs SET status = 'running', started_at = ? WHERE id = ?",
                (int(time.time()), run_id),
            )
            self.db.commit()

    def finish_run(
        self,
        run_id: int,
        status: str,
        elapsed_ms: int,
        counts: Optional[dict] = None,
        error: Optional[str] = None,
    ) -> None:
        counts = counts or {}
        with self._lock:
            self.db.execute(
                """UPDATE runs
                      SET status = ?, finished_at = ?, elapsed_ms = ?, error = ?,
                          resolved_count = ?, active_count = ?, cache_hits = ?,
                          error_count = ?
                    WHERE id = ?""",
                (
                    status,
                    int(time.time()),
                    elapsed_ms,
                    error,
                    counts.get("resolved", 0),
                    counts.get("active", 0),
                    counts.get("cache_hits", 0),
                    counts.get("errors", 0),
                    run_id,
                ),
            )
            self.db.commit()

    def add_results(self, run_id: int, results: Iterable[Result]) -> None:
        rows = []
        for r in results:
            data = r.as_dict()
            rows.append(tuple(
                json.dumps(data[col]) if col in _JSON_COLUMNS
                else int(data[col]) if col in _BOOL_COLUMNS
                else data[col]
                for col in RESULT_COLUMNS
            ) + (run_id,))

        if not rows:
            return

        placeholders = ", ".join("?" for _ in RESULT_COLUMNS)
        with self._lock:
            self.db.executemany(
                f"""INSERT INTO run_results({", ".join(RESULT_COLUMNS)}, run_id)
                    VALUES ({placeholders}, ?)""",
                rows,
            )
            self.db.execute(
                "UPDATE runs SET completed = completed + ? WHERE id = ?",
                (len(rows), run_id),
            )
            self.db.commit()

    def recover_interrupted(self) -> int:
        """A container restart can leave runs stuck in queued/running forever."""
        placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
        with self._lock:
            cur = self.db.execute(
                f"""UPDATE runs
                       SET status = 'interrupted', finished_at = ?,
                           error = 'interrupted by server restart'
                     WHERE status IN ({placeholders})""",
                (int(time.time()), *ACTIVE_STATUSES),
            )
            self.db.commit()
            return cur.rowcount

    def count_runs(self, include_deleted: bool = False) -> int:
        where = "" if include_deleted else "WHERE deleted_at IS NULL"
        return int(self.db.execute(f"SELECT COUNT(*) AS n FROM runs {where}").fetchone()["n"])

    def list_runs(self, limit: int, offset: int = 0,
                  include_deleted: bool = False) -> list[dict]:
        where = "" if include_deleted else "WHERE deleted_at IS NULL"
        rows = self.db.execute(
            f"""SELECT * FROM runs {where} ORDER BY id DESC LIMIT ? OFFSET ?""",
            (limit, offset),
        ).fetchall()
        return [self._run_row_to_dict(r) for r in rows]

    def get_run(self, run_id: int, include_deleted: bool = False) -> Optional[dict]:
        clause = "" if include_deleted else "AND deleted_at IS NULL"
        row = self.db.execute(
            f"SELECT * FROM runs WHERE id = ? {clause}", (run_id,)
        ).fetchone()
        return self._run_row_to_dict(row) if row else None

    def soft_delete_run(self, run_id: int) -> bool:
        """Hide a run from the UI. Rows are retained in the database."""
        with self._lock:
            cur = self.db.execute(
                "UPDATE runs SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL",
                (int(time.time()), run_id),
            )
            self.db.commit()
            return cur.rowcount > 0

    def restore_run(self, run_id: int) -> bool:
        with self._lock:
            cur = self.db.execute(
                "UPDATE runs SET deleted_at = NULL WHERE id = ?", (run_id,)
            )
            self.db.commit()
            return cur.rowcount > 0

    @staticmethod
    def _run_row_to_dict(row: sqlite3.Row) -> dict:
        run = dict(row)
        run["tlds"] = json.loads(run["tlds"] or "[]")
        run["keywords"] = json.loads(run["keywords"] or "[]")
        run["force"] = bool(run["force"])
        run["keyword_count"] = len(run["keywords"])
        run["is_active"] = run["status"] in ACTIVE_STATUSES
        return run

    # ------------------------------------------------------------- run results

    def get_run_results(self, run_id: int, limit: Optional[int] = None,
                        active_first: bool = True) -> list[dict]:
        order = "active_html DESC, resolved DESC, domain ASC" if active_first else "domain ASC"
        sql = f"SELECT * FROM run_results WHERE run_id = ? ORDER BY {order}"
        params: list[Any] = [run_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [_row_to_result_dict(r) for r in self.db.execute(sql, params).fetchall()]

    def iter_run_results(self, run_id: int):
        """Streaming read for CSV export, so a big run never lands in memory at once."""
        cur = self.db.execute(
            "SELECT * FROM run_results WHERE run_id = ? ORDER BY domain ASC", (run_id,)
        )
        for row in cur:
            yield _row_to_result_dict(row)

    def run_breakdown(self, run_id: int) -> dict:
        """Provider / server / TLD aggregates for the summary page, computed in SQL."""
        def group(column: str, active_only: bool) -> dict[str, int]:
            clause = "AND active_html = 1" if active_only else ""
            rows = self.db.execute(
                f"""SELECT COALESCE({column}, 'unknown') AS k, COUNT(*) AS n
                      FROM run_results
                     WHERE run_id = ? {clause}
                     GROUP BY k ORDER BY n DESC, k ASC""",
                (run_id,),
            ).fetchall()
            return {r["k"]: r["n"] for r in rows}

        totals = self.db.execute(
            """SELECT COUNT(*) AS scanned,
                      COALESCE(SUM(resolved), 0) AS resolved,
                      COALESCE(SUM(active_html), 0) AS active,
                      COALESCE(SUM(cached), 0) AS cache_hits,
                      COALESCE(SUM(error IS NOT NULL), 0) AS errors
                 FROM run_results WHERE run_id = ?""",
            (run_id,),
        ).fetchone()

        return {
            **{k: int(totals[k]) for k in totals.keys()},
            "providers": group("provider", active_only=True),
            "server_families": group("server_family", active_only=True),
            "tlds": group("tld", active_only=False),
        }

    def close(self) -> None:
        with self._lock:
            self.db.close()
