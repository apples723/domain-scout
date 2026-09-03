\
#!/usr/bin/env python3
import argparse
import asyncio
import json
import socket
import sqlite3
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import aiohttp
import yaml


@dataclass
class Result:
    keyword: str
    domain: str
    resolved: bool
    ip_addresses: list[str]
    active_html: bool
    url: Optional[str]
    status: Optional[int]
    server: Optional[str]
    server_family: Optional[str]
    provider: Optional[str]
    provider_evidence: list[str]
    title: Optional[str]
    content_type: Optional[str]
    elapsed_ms: int
    cached: bool = False
    error: Optional[str] = None


class ResultCache:
    def __init__(self, path: str, ttl: int):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.ttl = ttl
        self.mem: dict[str, Result] = {}
        self.db = sqlite3.connect(self.path)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                domain TEXT PRIMARY KEY,
                checked_at INTEGER NOT NULL,
                payload TEXT NOT NULL
            )
        """)
        self.db.commit()

    def get(self, domain: str) -> Optional[Result]:
        now = int(time.time())
        if domain in self.mem:
            r = self.mem[domain]
            r.cached = True
            return r

        row = self.db.execute(
            "SELECT checked_at, payload FROM cache WHERE domain = ?",
            (domain,)
        ).fetchone()

        if not row:
            return None

        checked_at, payload = row
        if now - checked_at > self.ttl:
            return None

        data = json.loads(payload)
        data["cached"] = True
        result = Result(**data)
        self.mem[domain] = result
        return result

    def put(self, result: Result):
        result.cached = False
        payload = json.dumps(asdict(result), separators=(",", ":"))
        self.mem[result.domain] = result
        self.db.execute(
            "INSERT OR REPLACE INTO cache(domain, checked_at, payload) VALUES (?, ?, ?)",
            (result.domain, int(time.time()), payload)
        )
        self.db.commit()


def classify_server(header: Optional[str]) -> Optional[str]:
    if not header:
        return None
    h = header.lower()
    if "nginx" in h:
        return "nginx"
    if "apache" in h:
        return "apache"
    return "other/unknown"


def classify_provider(headers, final_url: str) -> tuple[Optional[str], list[str]]:
    """
    Best-effort infrastructure/edge provider detection from HTTP headers and URL.
    This does NOT guarantee the actual origin host, especially when a CDN/reverse
    proxy intentionally hides it.
    """
    evidence = []
    normalized = {k.lower(): v for k, v in headers.items()}
    joined = "\n".join(f"{k}: {v}" for k, v in normalized.items()).lower()
    url = (final_url or "").lower()

    # Cloudflare
    if "cf-ray" in normalized or "cf-cache-status" in normalized or "__cf_bm" in joined:
        evidence.extend([
            k for k in ("cf-ray", "cf-cache-status", "server")
            if k in normalized
        ])
        return "cloudflare", evidence

    # AWS / CloudFront / ALB / API Gateway hints
    aws_markers = {
        "x-amz-cf-id": "cloudfront",
        "x-amz-cf-pop": "cloudfront",
        "x-amzn-trace-id": "aws",
        "x-amz-apigw-id": "api-gateway",
        "x-amzn-requestid": "aws",
    }
    for header, service in aws_markers.items():
        if header in normalized:
            evidence.append(f"{header}={service}")

    server = normalized.get("server", "").lower()
    via = normalized.get("via", "").lower()

    if "cloudfront" in server or "cloudfront" in via:
        evidence.append("server/via=cloudfront")
    if "awselb" in joined or "elb.amazonaws.com" in url:
        evidence.append("aws-elb")
    if evidence:
        return "aws", evidence

    # Fastly
    if "fastly" in joined or "x-served-by" in normalized or "x-cache-hits" in normalized:
        evidence.extend([
            k for k in ("x-served-by", "x-cache", "x-cache-hits")
            if k in normalized
        ])
        return "fastly", evidence

    # Akamai
    if (
        "akamai" in joined
        or "x-akamai-transformed" in normalized
        or "akamai-grn" in normalized
    ):
        evidence.extend([
            k for k in ("x-akamai-transformed", "akamai-grn")
            if k in normalized
        ])
        return "akamai", evidence

    # Vercel
    if "x-vercel-id" in normalized or "server" in normalized and "vercel" in server:
        evidence.append("x-vercel-id" if "x-vercel-id" in normalized else "server=vercel")
        return "vercel", evidence

    # Netlify
    if "x-nf-request-id" in normalized or "netlify" in server:
        evidence.append("x-nf-request-id" if "x-nf-request-id" in normalized else "server=netlify")
        return "netlify", evidence

    # GitHub Pages
    if "x-github-request-id" in normalized or "github.com" in normalized.get("x-github-backend", "").lower():
        evidence.append("x-github-request-id")
        return "github-pages", evidence

    return None, []


def extract_title(text: str) -> Optional[str]:
    low = text.lower()
    start = low.find("<title")
    if start == -1:
        return None
    start = low.find(">", start)
    if start == -1:
        return None
    end = low.find("</title>", start)
    if end == -1:
        return None
    title = " ".join(text[start + 1:end].split())
    return title[:250] or None


async def resolve_domain(domain: str, timeout: float) -> list[str]:
    loop = asyncio.get_running_loop()

    async def _lookup():
        infos = await loop.getaddrinfo(
            domain,
            443,
            type=socket.SOCK_STREAM,
            family=socket.AF_UNSPEC
        )
        return sorted({info[4][0] for info in infos})

    return await asyncio.wait_for(_lookup(), timeout=timeout)


async def fetch_html(
    session: aiohttp.ClientSession, url: str, max_body: int, allow_redirects: bool
):
    async with session.get(url, allow_redirects=allow_redirects) as resp:
        body = await resp.content.read(max_body)
        ctype = resp.headers.get("Content-Type", "")
        server = resp.headers.get("Server")
        provider, provider_evidence = classify_provider(resp.headers, str(resp.url))
        text = body.decode(resp.charset or "utf-8", errors="replace") if body else ""

        is_html = (
            "text/html" in ctype.lower()
            or text.lstrip().lower().startswith(("<!doctype html", "<html"))
        )
        return {
            "url": str(resp.url),
            "status": resp.status,
            "server": server,
            "server_family": classify_server(server),
            "provider": provider,
            "provider_evidence": provider_evidence,
            "content_type": ctype or None,
            "active_html": bool(is_html and body.strip()),
            "title": extract_title(text) if is_html else None,
        }


async def check_domain(
    keyword: str,
    tld: str,
    session: aiohttp.ClientSession,
    cache: ResultCache,
    cfg: dict,
    force: bool,
) -> Result:
    domain = f"{keyword.strip().lower()}.{tld.strip().lower()}".strip(".")
    start = time.perf_counter()

    if not force:
        cached = cache.get(domain)
        if cached:
            return cached

    try:
        ips = await resolve_domain(domain, cfg["dns_timeout_seconds"])
    except Exception as e:
        result = Result(
            keyword=keyword,
            domain=domain,
            resolved=False,
            ip_addresses=[],
            active_html=False,
            url=None,
            status=None,
            server=None,
            server_family=None,
            provider=None,
            provider_evidence=[],
            title=None,
            content_type=None,
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            error=f"dns: {type(e).__name__}",
        )
        cache.put(result)
        return result

    last_error = None
    for scheme in cfg["schemes"]:
        try:
            info = await fetch_html(
                session,
                f"{scheme}://{domain}/",
                cfg["max_body_bytes"],
                cfg["follow_redirects"],
            )
            result = Result(
                keyword=keyword,
                domain=domain,
                resolved=True,
                ip_addresses=ips,
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                error=None,
                **info,
            )
            cache.put(result)
            return result
        except Exception as e:
            last_error = f"{scheme}: {type(e).__name__}"

    result = Result(
        keyword=keyword,
        domain=domain,
        resolved=True,
        ip_addresses=ips,
        active_html=False,
        url=None,
        status=None,
        server=None,
        server_family=None,
        provider=None,
        provider_evidence=[],
        title=None,
        content_type=None,
        elapsed_ms=int((time.perf_counter() - start) * 1000),
        error=last_error,
    )
    cache.put(result)
    return result


async def run_scan(args):
    with open(args.config, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    cfg = config["scanner"]
    storage = config["storage"]
    input_cfg = config["input"]
    output_cfg = config["output"]

    keyword_path = Path(args.keywords or input_cfg["keywords_file"])
    keywords = [
        line.strip()
        for line in keyword_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    cache = ResultCache(storage["sqlite_path"], cfg["cache_ttl_seconds"])
    output_path = Path(output_cfg["results_file"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    timeout = aiohttp.ClientTimeout(
        total=cfg["request_timeout_seconds"],
        connect=cfg["connect_timeout_seconds"],
    )
    connector = aiohttp.TCPConnector(
        limit=cfg["worker_concurrency"],
        ttl_dns_cache=300,
        # Use the stdlib threaded resolver instead of aiohttp's default aiodns
        # resolver. The aiodns/pycares combination is version-fragile (e.g.
        # aiodns 3.5.0 + pycares 5.x raises a TypeError from Channel.getaddrinfo
        # on Python 3.14), and this app does not need c-ares.
        resolver=aiohttp.ThreadedResolver(),
    )
    headers = {"User-Agent": cfg["user_agent"]}

    sem = asyncio.Semaphore(cfg["worker_concurrency"])

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:

        async def worker(keyword):
            async with sem:
                return await check_domain(
                    keyword, args.tld, session, cache, cfg, args.force
                )

        started = time.perf_counter()
        results = await asyncio.gather(*(worker(k) for k in keywords))
        elapsed = time.perf_counter() - started

    with output_path.open("a", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(asdict(r)) + "\n")

    active = [r for r in results if r.active_html]
    apache = [r for r in active if r.server_family == "apache"]
    nginx = [r for r in active if r.server_family == "nginx"]
    cached = [r for r in results if r.cached]

    print(f"Scanned:      {len(results)}")
    print(f"Active HTML:  {len(active)}")
    print(f"nginx:        {len(nginx)}")
    print(f"Apache:       {len(apache)}")
    print(f"Cache hits:   {len(cached)}")
    print(f"Elapsed:      {elapsed:.2f}s")
    print()

    print_results_table(results)


def print_results_table(results):
    """Print every scanned result (as written to results.jsonl) as a table."""
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

    # (header, accessor) pairs. Columns mirror the fields written to results.jsonl.
    columns = [
        ("keyword", lambda r: r.keyword),
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


def main():
    p = argparse.ArgumentParser(description="Keyword + TLD active website scanner")
    p.add_argument("--tld", required=True, help="TLD to test, e.g. com, net, app")
    p.add_argument("--keywords", help="Override keywords file")
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--force", action="store_true", help="Ignore cache")
    args = p.parse_args()
    asyncio.run(run_scan(args))


if __name__ == "__main__":
    main()
