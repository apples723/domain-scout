#!/usr/bin/env python3
"""Command-line surface. Records runs to the same database the web UI reads."""
from __future__ import annotations

import argparse
import asyncio
import time

from .config import load_config, parse_tlds, read_keywords
from .engine import Result, scan, summarize
from .store import Store


def print_results_table(results: list[Result]) -> None:
    """Print every scanned result as a table."""
    if not results:
        return

    def fmt(v):
        if v is None:
            return "-"
        if isinstance(v, bool):
            return "yes" if v else "no"
        if isinstance(v, list):
            return ", ".join(str(x) for x in v) if v else "-"
        return str(v)

    columns = [
        ("keyword", lambda r: r.keyword),
        ("tld", lambda r: r.tld),
        ("domain", lambda r: r.domain),
        ("resolved", lambda r: r.resolved),
        ("active", lambda r: r.active_html),
        ("status", lambda r: r.status),
        ("server", lambda r: r.server),
        ("server_family", lambda r: r.server_family),
        ("provider", lambda r: r.provider),
        ("cached", lambda r: r.cached),
        ("ms", lambda r: r.elapsed_ms),
        ("ips", lambda r: r.ip_addresses),
        ("title", lambda r: r.title),
        ("url", lambda r: r.url),
        ("error", lambda r: r.error),
    ]

    rows = [[fmt(accessor(r)) for _, accessor in columns] for r in results]
    widths = [
        max(len(header), *(len(row[i]) for row in rows))
        for i, (header, _) in enumerate(columns)
    ]

    def render(cells):
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))

    header = render([h for h, _ in columns])
    print(header)
    print("-" * len(header))
    for row in rows:
        print(render(row))


async def run_scan(args) -> int:
    config = load_config(args.config)
    cfg = config["scanner"]
    storage = config["storage"]

    tlds = parse_tlds(args.tld)
    if not tlds:
        raise SystemExit("no valid TLDs provided")

    keywords = read_keywords(args.keywords or config["input"]["keywords_file"])
    if not keywords:
        raise SystemExit("keyword list is empty")

    store = Store(storage["sqlite_path"], cfg["cache_ttl_seconds"])
    run_id = store.create_run(tlds, keywords, args.force)
    store.mark_running(run_id)

    print(f"Run #{run_id}: {len(keywords)} keywords x {len(tlds)} TLD(s) "
          f"= {len(keywords) * len(tlds)} domains")

    buffer: list[Result] = []

    def on_result(result: Result) -> None:
        # Batch inserts: one commit per domain would mean one fsync per domain.
        buffer.append(result)
        if len(buffer) >= 50:
            store.add_results(run_id, buffer)
            buffer.clear()

    started = time.perf_counter()
    try:
        results = await scan(keywords, tlds, cfg, store, args.force, on_result)
    except Exception as e:
        store.add_results(run_id, buffer)
        store.finish_run(run_id, "failed", int((time.perf_counter() - started) * 1000),
                         error=f"{type(e).__name__}: {e}")
        store.close()
        raise

    elapsed = time.perf_counter() - started
    store.add_results(run_id, buffer)
    counts = summarize(results)
    store.finish_run(run_id, "done", int(elapsed * 1000), counts)

    print(f"Scanned:      {counts['scanned']}")
    print(f"Active HTML:  {counts['active']}")
    print(f"nginx:        {counts['nginx']}")
    print(f"Apache:       {counts['apache']}")
    print(f"Cache hits:   {counts['cache_hits']}")
    print(f"Elapsed:      {elapsed:.2f}s")
    print()

    results.sort(key=lambda r: (not r.active_html, r.domain))
    print_results_table(results)
    store.close()
    return run_id


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Keyword + TLD active website scanner")
    p.add_argument(
        "--tld", required=True, action="append",
        help="TLD to test. Repeatable and comma-separated: --tld com,net --tld app",
    )
    p.add_argument("--keywords", help="Override keywords file")
    p.add_argument("--config", default=None, help="Config file (default: config.yaml)")
    p.add_argument("--force", action="store_true", help="Ignore cache")
    args = p.parse_args(argv)
    asyncio.run(run_scan(args))


if __name__ == "__main__":
    main()
