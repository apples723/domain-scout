"""Config loading shared by the CLI and the web app."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = os.environ.get("DOMAIN_SCOUT_CONFIG", "config.yaml")

_WEB_DEFAULTS = {
    "host": "0.0.0.0",
    "port": 8000,
    "history_page_size": 25,
    "max_keywords_per_run": 1000,
    "max_tlds_per_run": 20,
    "quick_summary_rows": 8,
}


def load_config(path: str | os.PathLike | None = None) -> dict:
    config_path = Path(path or DEFAULT_CONFIG_PATH)
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    config["web"] = {**_WEB_DEFAULTS, **(config.get("web") or {})}
    return config


def read_keywords(path: str | os.PathLike) -> list[str]:
    """One keyword per line; blank lines and # comments ignored."""
    return parse_keywords(Path(path).read_text(encoding="utf-8"))


def parse_keywords(text: str) -> list[str]:
    keywords: list[str] = []
    seen: set[str] = set()
    for line in text.splitlines():
        keyword = line.strip().lower()
        if not keyword or keyword.startswith("#"):
            continue
        if keyword not in seen:
            seen.add(keyword)
            keywords.append(keyword)
    return keywords


def parse_tlds(raw: str | list[str]) -> list[str]:
    """Accept 'com, net app' or ['com', 'net'] and normalize to a clean list."""
    parts = [raw] if isinstance(raw, str) else list(raw)
    tlds: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for chunk in str(part).replace(",", " ").split():
            tld = chunk.strip().lower().lstrip(".")
            if tld and tld not in seen:
                seen.add(tld)
                tlds.append(tld)
    return tlds
