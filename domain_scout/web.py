"""Web surface: run submission, run history, per-run summary, CSV export.

Scans never run inside a request. Submitting enqueues a run and redirects to that
run's summary page; a single background worker drains the queue, which is what
makes the long-documented ``job_concurrency: 1`` guarantee real.
"""
from __future__ import annotations

import asyncio
import csv
import io
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from .config import load_config, parse_keywords, parse_tlds, read_keywords
from .engine import Result, scan, summarize
from .store import RESULT_COLUMNS, Store

TEMPLATE_DIR = Path(__file__).parent / "templates"
BATCH_SIZE = 50


# --------------------------------------------------------------- background worker

async def _execute_run(state, run_id: int) -> None:
    store: Store = state.store
    cfg = state.config["scanner"]
    run = store.get_run(run_id, include_deleted=True)
    if not run or run["status"] != "queued":
        return

    store.mark_running(run_id)
    buffer: list[Result] = []

    def on_result(result: Result) -> None:
        buffer.append(result)
        if len(buffer) >= BATCH_SIZE:
            store.add_results(run_id, buffer)
            buffer.clear()

    started = time.perf_counter()
    try:
        results = await scan(
            run["keywords"], run["tlds"], cfg, store, run["force"], on_result
        )
        store.add_results(run_id, buffer)
        store.finish_run(
            run_id, "done", int((time.perf_counter() - started) * 1000),
            summarize(results),
        )
    except asyncio.CancelledError:
        store.add_results(run_id, buffer)
        store.finish_run(
            run_id, "interrupted", int((time.perf_counter() - started) * 1000),
            error="cancelled during shutdown",
        )
        raise
    except Exception as e:
        store.add_results(run_id, buffer)
        store.finish_run(
            run_id, "failed", int((time.perf_counter() - started) * 1000),
            error=f"{type(e).__name__}: {e}",
        )


async def _worker_loop(state) -> None:
    while True:
        run_id = await state.queue.get()
        try:
            state.current_run_id = run_id
            await _execute_run(state, run_id)
        except asyncio.CancelledError:
            raise
        except Exception:  # a worker crash must not kill the queue
            pass
        finally:
            state.current_run_id = None
            state.queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config(app.state.config_path)
    store = Store(
        config["storage"]["sqlite_path"],
        config["scanner"]["cache_ttl_seconds"],
    )
    recovered = store.recover_interrupted()
    if recovered:
        print(f"Marked {recovered} interrupted run(s) from a previous process")

    app.state.config = config
    app.state.store = store
    app.state.queue = asyncio.Queue()
    app.state.current_run_id = None
    app.state.worker = asyncio.create_task(_worker_loop(app.state))
    try:
        yield
    finally:
        app.state.worker.cancel()
        try:
            await app.state.worker
        except asyncio.CancelledError:
            pass
        store.recover_interrupted()
        store.close()


# ------------------------------------------------------------------ app factory

def create_app(config_path: Optional[str] = None) -> FastAPI:
    app = FastAPI(title="Domain Scout", lifespan=lifespan)
    app.state.config_path = config_path
    templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
    templates.env.filters["ts"] = _format_ts
    templates.env.filters["duration"] = _format_duration

    def render(request: Request, name: str, ctx: dict) -> HTMLResponse:
        return templates.TemplateResponse(request, name, ctx)

    # ---------------------------------------------------------------- new run

    @app.get("/", response_class=HTMLResponse)
    async def new_run_form(request: Request):
        config = app.state.config
        keywords_file = config["input"]["keywords_file"]
        try:
            prefill = "\n".join(read_keywords(keywords_file))
        except OSError:
            prefill = ""
        return render(request, "index.html", {
            "prefill_keywords": prefill,
            "keywords_file": keywords_file,
            "web": config["web"],
            "queue_depth": app.state.queue.qsize(),
            "current_run_id": app.state.current_run_id,
        })

    @app.post("/runs")
    async def submit_run(
        keywords: str = Form(""),
        tlds: str = Form(""),
        force: bool = Form(False),
    ):
        run_id = _create_and_enqueue(app, keywords, tlds, force)
        return RedirectResponse(f"/runs/{run_id}", status_code=303)

    # ---------------------------------------------------------------- history

    @app.get("/runs", response_class=HTMLResponse)
    async def run_history(request: Request, page: int = Query(1, ge=1)):
        store: Store = app.state.store
        web = app.state.config["web"]
        page_size = web["history_page_size"]
        total = store.count_runs()
        runs = store.list_runs(limit=page_size, offset=(page - 1) * page_size)

        # Quick summary rows for each expander, rendered server-side.
        for run in runs:
            run["quick_results"] = store.get_run_results(
                run["id"], limit=web["quick_summary_rows"]
            )

        return render(request, "runs.html", {
            "runs": runs,
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_prev": page > 1,
            "has_next": page * page_size < total,
            "quick_rows": web["quick_summary_rows"],
        })

    # ----------------------------------------------------------- run summary

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    async def run_summary(request: Request, run_id: int):
        store: Store = app.state.store
        run = store.get_run(run_id, include_deleted=True)
        if not run:
            raise HTTPException(404, f"Run {run_id} not found")
        return render(request, "run.html", {
            "run": run,
            "results": store.get_run_results(run_id),
            "breakdown": store.run_breakdown(run_id),
            "queue_position": _queue_position(app, run_id),
        })

    @app.get("/runs/{run_id}/export.csv")
    async def export_csv(run_id: int):
        store: Store = app.state.store
        run = store.get_run(run_id, include_deleted=True)
        if not run:
            raise HTTPException(404, f"Run {run_id} not found")

        def rows():
            buf = io.StringIO()
            writer = csv.writer(buf)

            def flush() -> str:
                buf.seek(0)
                chunk = buf.read()
                buf.seek(0)
                buf.truncate(0)
                return chunk

            writer.writerow(RESULT_COLUMNS)
            yield flush()
            for result in store.iter_run_results(run_id):
                writer.writerow([
                    "; ".join(result[col]) if isinstance(result[col], list)
                    else result[col]
                    for col in RESULT_COLUMNS
                ])
                yield flush()

        stamp = datetime.fromtimestamp(run["created_at"], timezone.utc).strftime("%Y%m%d-%H%M%S")
        filename = f"domain-scout-run{run_id}-{stamp}.csv"
        return StreamingResponse(
            rows(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.post("/runs/{run_id}/delete")
    async def delete_run(run_id: int):
        app.state.store.soft_delete_run(run_id)
        return RedirectResponse("/runs", status_code=303)

    @app.post("/runs/{run_id}/restore")
    async def restore_run(run_id: int):
        app.state.store.restore_run(run_id)
        return RedirectResponse(f"/runs/{run_id}", status_code=303)

    # -------------------------------------------------------------------- api

    @app.get("/api/runs")
    async def api_runs(limit: int = Query(25, ge=1, le=200), offset: int = Query(0, ge=0)):
        store: Store = app.state.store
        return {
            "total": store.count_runs(),
            "runs": store.list_runs(limit=limit, offset=offset),
        }

    @app.post("/api/runs", status_code=202)
    async def api_create_run(payload: dict):
        run_id = _create_and_enqueue(
            app,
            payload.get("keywords", ""),
            payload.get("tlds", ""),
            bool(payload.get("force", False)),
        )
        return {"run_id": run_id, "status": "queued", "url": f"/runs/{run_id}"}

    @app.get("/api/runs/{run_id}")
    async def api_run(run_id: int):
        store: Store = app.state.store
        run = store.get_run(run_id, include_deleted=True)
        if not run:
            raise HTTPException(404, f"Run {run_id} not found")
        return {
            "run": run,
            "breakdown": store.run_breakdown(run_id),
            "results": store.get_run_results(run_id),
        }

    @app.get("/api/runs/{run_id}/status")
    async def api_run_status(run_id: int):
        run = app.state.store.get_run(run_id, include_deleted=True)
        if not run:
            raise HTTPException(404, f"Run {run_id} not found")
        return {
            "run_id": run_id,
            "status": run["status"],
            "completed": run["completed"],
            "total": run["total"],
            "queue_position": _queue_position(app, run_id),
        }

    @app.delete("/api/runs/{run_id}")
    async def api_delete_run(run_id: int):
        if not app.state.store.soft_delete_run(run_id):
            raise HTTPException(404, f"Run {run_id} not found or already deleted")
        return {"run_id": run_id, "deleted": True}

    @app.get("/health")
    async def health():
        return JSONResponse({
            "status": "ok",
            "queue_depth": app.state.queue.qsize(),
            "current_run_id": app.state.current_run_id,
        })

    return app


# ---------------------------------------------------------------------- helpers

def _create_and_enqueue(app: FastAPI, keywords_raw, tlds_raw, force: bool) -> int:
    web = app.state.config["web"]
    keywords = (
        parse_keywords(keywords_raw) if isinstance(keywords_raw, str)
        else parse_keywords("\n".join(keywords_raw))
    )
    tlds = parse_tlds(tlds_raw)

    if not keywords:
        raise HTTPException(400, "Provide at least one keyword")
    if not tlds:
        raise HTTPException(400, "Provide at least one TLD")
    if len(keywords) > web["max_keywords_per_run"]:
        raise HTTPException(
            400, f"Too many keywords: {len(keywords)} (max {web['max_keywords_per_run']})"
        )
    if len(tlds) > web["max_tlds_per_run"]:
        raise HTTPException(
            400, f"Too many TLDs: {len(tlds)} (max {web['max_tlds_per_run']})"
        )

    run_id = app.state.store.create_run(tlds, keywords, force)
    app.state.queue.put_nowait(run_id)
    return run_id


def _queue_position(app: FastAPI, run_id: int) -> Optional[int]:
    if app.state.current_run_id == run_id:
        return 0
    pending = list(app.state.queue._queue)  # noqa: SLF001 - read-only peek
    return pending.index(run_id) + 1 if run_id in pending else None


def _format_ts(value: Optional[int]) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_duration(ms: Optional[int]) -> str:
    if ms is None:
        return "-"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.2f}s"
    return f"{int(seconds // 60)}m {seconds % 60:.0f}s"


app = create_app()
