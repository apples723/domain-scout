"""Scan engine: DNS resolution, HTTP probing, and response classification.

This module is deliberately free of CLI and web concerns. Both surfaces call
``scan()``, which is the single entry point for running a batch of checks.
"""
from __future__ import annotations

import asyncio
import socket
import time
from dataclasses import dataclass, asdict, fields
from typing import Callable, Iterable, Optional, Protocol

import aiohttp


@dataclass
class Result:
    keyword: str
    tld: str
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

    @classmethod
    def field_names(cls) -> list[str]:
        return [f.name for f in fields(cls)]

    def as_dict(self) -> dict:
        return asdict(self)


class CacheLike(Protocol):
    """The slice of the store the engine actually needs."""

    def cache_get(self, domain: str) -> Optional[Result]: ...

    def cache_put(self, result: Result) -> None: ...


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
    evidence: list[str] = []
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
    if "x-vercel-id" in normalized or "vercel" in server:
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


def _failed_result(keyword: str, tld: str, domain: str, resolved: bool,
                   ips: list[str], start: float, error: Optional[str]) -> Result:
    return Result(
        keyword=keyword,
        tld=tld,
        domain=domain,
        resolved=resolved,
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
        error=error,
    )


async def check_domain(
    keyword: str,
    tld: str,
    session: aiohttp.ClientSession,
    cache: CacheLike,
    cfg: dict,
    force: bool,
) -> Result:
    keyword = keyword.strip().lower()
    tld = tld.strip().lower().lstrip(".")
    domain = f"{keyword}.{tld}".strip(".")
    start = time.perf_counter()

    if not force:
        cached = cache.cache_get(domain)
        if cached:
            return cached

    try:
        ips = await resolve_domain(domain, cfg["dns_timeout_seconds"])
    except Exception as e:
        result = _failed_result(
            keyword, tld, domain, False, [], start, f"dns: {type(e).__name__}"
        )
        cache.cache_put(result)
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
                tld=tld,
                domain=domain,
                resolved=True,
                ip_addresses=ips,
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                error=None,
                **info,
            )
            cache.cache_put(result)
            return result
        except Exception as e:
            last_error = f"{scheme}: {type(e).__name__}"

    result = _failed_result(keyword, tld, domain, True, ips, start, last_error)
    cache.cache_put(result)
    return result


def build_pairs(keywords: Iterable[str], tlds: Iterable[str]) -> list[tuple[str, str]]:
    """Cross product of keywords and TLDs, de-duplicated, order preserved."""
    seen: set[tuple[str, str]] = set()
    pairs: list[tuple[str, str]] = []
    for keyword in keywords:
        k = keyword.strip().lower()
        if not k:
            continue
        for tld in tlds:
            t = tld.strip().lower().lstrip(".")
            if not t:
                continue
            if (k, t) in seen:
                continue
            seen.add((k, t))
            pairs.append((k, t))
    return pairs


def make_connector(cfg: dict) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=cfg["worker_concurrency"],
        ttl_dns_cache=300,
        # Use the stdlib threaded resolver instead of aiohttp's default aiodns
        # resolver. The aiodns/pycares combination is version-fragile (e.g.
        # aiodns 3.5.0 + pycares 5.x raises a TypeError from Channel.getaddrinfo
        # on Python 3.14), and this app does not need c-ares.
        resolver=aiohttp.ThreadedResolver(),
    )


async def scan(
    keywords: Iterable[str],
    tlds: Iterable[str],
    cfg: dict,
    cache: CacheLike,
    force: bool = False,
    on_result: Optional[Callable[[Result], None]] = None,
) -> list[Result]:
    """Check every keyword x TLD combination concurrently.

    ``on_result`` is invoked once per completed check, as results land, so callers
    can persist or report progress incrementally instead of waiting for the whole
    batch. Results are returned in completion order.
    """
    pairs = build_pairs(keywords, tlds)
    if not pairs:
        return []

    timeout = aiohttp.ClientTimeout(
        total=cfg["request_timeout_seconds"],
        connect=cfg["connect_timeout_seconds"],
    )
    sem = asyncio.Semaphore(cfg["worker_concurrency"])
    results: list[Result] = []

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=make_connector(cfg),
        headers={"User-Agent": cfg["user_agent"]},
    ) as session:

        async def worker(keyword: str, tld: str) -> Result:
            async with sem:
                return await check_domain(keyword, tld, session, cache, cfg, force)

        tasks = [asyncio.create_task(worker(k, t)) for k, t in pairs]
        try:
            for completed in asyncio.as_completed(tasks):
                result = await completed
                results.append(result)
                if on_result is not None:
                    on_result(result)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    return results


def summarize(results: list[Result]) -> dict:
    """Aggregate counts used by both the CLI summary and the web summary page."""
    active = [r for r in results if r.active_html]
    providers: dict[str, int] = {}
    families: dict[str, int] = {}
    for r in active:
        if r.provider:
            providers[r.provider] = providers.get(r.provider, 0) + 1
        if r.server_family:
            families[r.server_family] = families.get(r.server_family, 0) + 1
    return {
        "scanned": len(results),
        "resolved": sum(1 for r in results if r.resolved),
        "active": len(active),
        "nginx": sum(1 for r in active if r.server_family == "nginx"),
        "apache": sum(1 for r in active if r.server_family == "apache"),
        "cache_hits": sum(1 for r in results if r.cached),
        "errors": sum(1 for r in results if r.error),
        "providers": dict(sorted(providers.items(), key=lambda kv: -kv[1])),
        "server_families": dict(sorted(families.items(), key=lambda kv: -kv[1])),
    }
