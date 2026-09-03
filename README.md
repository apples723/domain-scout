# Domain Scout

Domain Scout is an async CLI that takes a list of keywords, pairs each one with a
single TLD, and checks whether the resulting domain resolves and serves a live
website. It's built for quickly surveying a batch of candidate names and seeing
which ones are already live, what they're running, and who's fronting them.

## How it works

Give it a keyword list:

```text
google
facebook
reddit
```

Run it against one TLD:

```bash
python scanner.py --tld com
```

Each keyword becomes a domain, and every domain is checked concurrently:

```text
google.com
facebook.com
reddit.com
```

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

One scan job runs at a time. Inside that job, a bounded worker pool checks domains
concurrently:

```yaml
worker_concurrency: 40   # up to 40 domain checks in flight
worker_concurrency: 1    # strictly serial, much slower
```

With serial checks, "hundreds of domains in a few seconds" isn't realistic since
DNS and HTTP latency alone can run into the hundreds of milliseconds per domain.

Caching keeps repeat scans fast:

- An aiohttp DNS cache for repeated lookups.
- A SQLite + in-process cache for completed domain checks.

Warm scans can return large batches of cached results almost instantly.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scanner.py --tld com
python scanner.py --tld net
python scanner.py --tld app
```

Force a fresh check, bypassing the cache:

```bash
python scanner.py --tld com --force
```

## Docker

```bash
docker compose build
docker compose run --rm domain-scout --tld com
docker compose run --rm domain-scout --tld app
```

## Output

Results print to the terminal as a table and are appended to
`data/results.jsonl`, one JSON object per line:

```json
{
  "keyword": "example",
  "domain": "example.com",
  "resolved": true,
  "ip_addresses": ["93.184.216.34"],
  "active_html": true,
  "url": "https://example.com/",
  "status": 200,
  "server": "cloudflare",
  "server_family": "other/unknown",
  "provider": "cloudflare",
  "provider_evidence": ["cf-ray", "cf-cache-status", "server"],
  "title": "Example Domain",
  "content_type": "text/html",
  "elapsed_ms": 122,
  "cached": false,
  "error": null
}
```

## A note on availability

Active and available are different questions. Domain Scout tells you whether a
domain resolves and serves HTML. It does **not** authoritatively determine whether
a domain is available to register. For that, check a registrar or WHOIS/RDAP.
