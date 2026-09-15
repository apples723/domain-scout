# Domain Scout

Domain Scout takes a list of keywords, pairs each one with every TLD you give it,
and checks whether the resulting domains resolve and serve a live website. It's
built for quickly surveying a batch of candidate names and seeing which ones are
already live, what they're running, and who's fronting them.

It ships two surfaces over one engine:

- a **web UI** for launching scans and browsing past runs
- a **CLI** for scripted and one-off scans

Both write to the same SQLite database, so a CLI scan shows up in the web history
and vice versa.

## Quick start

```bash
docker compose up -d web
# open http://localhost:8000
```

The CLI runs against the same database:

```bash
docker compose run --rm cli --tld com
docker compose run --rm cli --tld com,net --tld app
```

## The web UI

**New scan** (`/`) — paste keywords, list one or more TLDs, optionally bypass the
cache. The keywords box is prefilled from `keywords.txt` as a convenience;
editing it does not write back to that file. Submitting queues the run and takes
you straight to its summary page.

**History** (`/runs`) — a table of every previous run: when it started, which
TLDs, how many domains, how many were active, cache hits, elapsed time, and
status. Each row expands to a quick table of its domains and findings, with a
button through to the full summary.

**Run summary** (`/runs/{id}`) — the complete findings for one run: headline
counts, a breakdown by provider, server family, and TLD, and every domain with
all of its fields. **CSV export lives here.** So does the delete button.

While a run is in flight its summary page reloads on its own until it finishes.
There's no progress bar by design.

## Keywords x TLDs

A run is the cross product of your keywords and your TLDs. Three keywords and two
TLDs is six domain checks:

```text
google.com   google.net
facebook.com facebook.net
reddit.com   reddit.net
```

Every result records which TLD it came from, so a single run can compare `.com`
against `.net` and `.app` side by side.

## What each check reports

For every domain, Domain Scout records:

1. DNS resolution and the resolved IP addresses.
2. HTTPS first, falling back to HTTP.
3. Whether the response is an actual HTML page.
4. HTTP status code and final redirect target.
5. The `Server` response header.
6. Server classification: `nginx`, `apache`, or `other/unknown`.
7. Infrastructure/provider detection for Cloudflare, AWS, Fastly, Akamai, Vercel,
   Netlify, and GitHub Pages, based on HTTP response fingerprints.
8. The evidence behind each provider classification.
9. Page title and content type.

Domain Scout deliberately avoids aggressive fingerprinting. Plenty of production
servers hide or rewrite the `Server` header, so nginx/apache detection is
best-effort by design.

## Provider vs. server

`server_family` and `provider` answer two different questions, and they often
disagree:

```text
server_family: nginx          server_family: other/unknown
provider: aws                 provider: cloudflare
```

Provider detection keys off recognizable edge headers such as `CF-Ray`,
`X-Amz-Cf-Id`, `X-Amzn-Trace-Id`, and `X-Vercel-Id`. A CDN or reverse proxy can
mask the true origin, so detecting Cloudflare confirms the request passes through
Cloudflare, not what runs behind it.

## Performance model

One scan job runs at a time. The web UI enforces this with a single background
worker draining a queue — submit while a scan is running and your run shows up as
`queued` rather than competing for bandwidth.

Inside that job, a bounded worker pool checks domains concurrently:

```yaml
worker_concurrency: 40   # up to 40 domain checks in flight
worker_concurrency: 1    # strictly serial, much slower
```

With serial checks, "hundreds of domains in a few seconds" isn't realistic since
DNS and HTTP latency alone can run into the hundreds of milliseconds per domain.

Caching keeps repeat scans fast:

- An aiohttp DNS cache for repeated lookups.
- A SQLite + in-process cache for completed domain checks.

Warm scans can return large batches of cached results almost instantly. A cached
check is still recorded in the run it belongs to, flagged `cached`, so a warm run
is a complete record rather than a sparse one.

## Storage

Everything lives in one SQLite database (`data/domain-scout.db`, WAL mode):

| Table         | Role |
|---------------|------|
| `cache`       | Domain-keyed, TTL'd, disposable. Purely a speed optimization. |
| `runs`        | One row per scan: TLDs, keywords, status, counts, timings. |
| `run_results` | One row per domain per run. The immutable historical record. |

The cache expires and gets overwritten; run history does not. If the `Result`
shape ever changes, the schema version bumps and the cache is cleared — run
history is never destroyed.

Runs deleted from the UI are **soft deleted**. They disappear from the history
list but their rows stay in the database, and the run's summary page and CSV
export remain reachable by URL with a Restore button.

Results are no longer appended to `data/results.jsonl`; that format is retired in
favour of the run history and CSV export.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# web UI
uvicorn domain_scout.web:app --reload
# -> http://localhost:8000

# CLI
python scanner.py --tld com
python scanner.py --tld com,net --tld app
python scanner.py --tld com --force      # ignore the cache
```

## HTTP API

The UI is server-rendered, but the same operations are available as JSON:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/runs` | List runs (`limit`, `offset`) |
| `POST` | `/api/runs` | Queue a run: `{"keywords": [...], "tlds": "com,net", "force": false}` |
| `GET` | `/api/runs/{id}` | Run, breakdown, and all results |
| `GET` | `/api/runs/{id}/status` | Status and `completed`/`total` |
| `DELETE` | `/api/runs/{id}` | Soft delete |
| `GET` | `/runs/{id}/export.csv` | CSV download |
| `GET` | `/health` | Health check |

## Configuration

`config.yaml` drives both surfaces. Beyond the scanner settings, the `web`
section controls page sizes and the guard rails on submitted work:

```yaml
web:
  history_page_size: 25
  quick_summary_rows: 8      # rows in a history row's expander
  max_keywords_per_run: 1000
  max_tlds_per_run: 20
```

The caps matter because one run fans out to keywords x TLDs concurrent requests.

## A note on access

There is **no authentication**. `docker-compose.yml` publishes the web UI on
`127.0.0.1:8000` for that reason. If you need it reachable from elsewhere, put a
reverse proxy with auth in front of it rather than widening the port binding — a
form that accepts arbitrary hostnames and fans out concurrent requests is not
something to leave open.

## A note on availability

Active and available are different questions. Domain Scout tells you whether a
domain resolves and serves HTML. It does **not** authoritatively determine whether
a domain is available to register. For that, check a registrar or WHOIS/RDAP.
