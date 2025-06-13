# -*- coding: utf-8 -*-
"""Hookdeck events loader with Sentry error reporting and diskcache fallback.

Requirements
------------
    pip install sentry-sdk diskcache requests python-dotenv

Environment variables
---------------------
    HOOKDECK_API_KEY   – your Hookdeck API key
    HOOKDECK_WEBHOOK_ID – webhook ID to pull events for
    SENTRY_DSN         – (optional) Sentry DSN; if absent, Sentry is disabled

Usage
-----
    python hookdeck_with_sentry_and_cache.py  # fetches events and prints parsed results

The script will:
    * Fetch events from Hookdeck REST API (paginated)
    * Cache every raw event body in a local on‑disk cache (./.hookdeck_cache)
    * Parse SMS‑style transaction messages into structured dicts
    * Log and send any parsing/fetching errors to Sentry
    * Skip already‑seen events by their `id`
"""
from __future__ import annotations

import json
import logging
import os
import re
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, List

import requests
from sentry_sdk import capture_exception, init as sentry_init
from sentry_sdk.integrations.logging import LoggingIntegration
import diskcache as dc

# ---------------------------------------------------------------------------
# Sentry setup (optional)
# ---------------------------------------------------------------------------
SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_logging = LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
    sentry_init(dsn=SENTRY_DSN, integrations=[sentry_logging], traces_sample_rate=0.05)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Local cache (diskcache)
# ---------------------------------------------------------------------------
CACHE_DIR = Path(".hookdeck_cache")
cache = dc.Cache(CACHE_DIR)

# ---------------------------------------------------------------------------
# API configuration
# ---------------------------------------------------------------------------
API_KEY = os.getenv("HOOKDECK_API_KEY")
WEBHOOK_ID = os.getenv("HOOKDECK_WEBHOOK_ID")
BASE_URL = "https://api.hookdeck.com/2024-03-01/events"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
}
# ---------------------------------------------------------------------------
# Transaction parser
# ---------------------------------------------------------------------------
TRANSACTION_RE = re.compile(
    r"""
    ^APPROVED\ PURCHASE\ DB\ SALE:\s*
    (?P<merchant>.+?),\s*YEREVAN,\s*
    (?P<address>.*?)(?=,\d{2}[./-]\d{2}[./-]\d{2,4}\s)  # address up to the date
    ,\s*(?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})\s+
    (?P<time>\d{2}:\d{2}),\s*
    card\ \*{3}(?P<card>\d{4})\.\s*
    Amount:(?P<amount>[\d,.]+)\s+
    (?P<currency>\w{3}),\s*
    Balance:(?P<balance>[\d,.]+)
    """,
    re.VERBOSE,
)


def parse_transaction_message(message: str) -> Optional[Dict]:
    """Parse a bank SMS notification into structured data.

    Returns *None* when the message does not match the expected pattern.
    Sends exceptions to Sentry and returns *None* on failure.
    """
    try:
        match = TRANSACTION_RE.match(message)
        if not match:
            return None
        g = match.groupdict()
        amount = Decimal(g["amount"].replace(",", ""))
        balance = Decimal(g["balance"].replace(",", ""))
        return {
            "merchant": g["merchant"].strip(),
            "city": "YEREVAN",
            "address": g["address"].strip() or "N/A",
            "date": g["date"],
            "time": g["time"],
            "card": f"***{g['card']}",
            "amount": float(amount),
            "currency": g["currency"],
            "balance": float(balance),
        }
    except Exception as exc:  # pragma: no cover  – we *want* to see it in Sentry
        logger.exception("Failed to parse message")
        capture_exception(exc)
        return None


# ---------------------------------------------------------------------------
# Hookdeck fetcher with caching and error capture
# ---------------------------------------------------------------------------

def fetch_events(limit: int = 100) -> List[dict]:
    global BASE_URL, HEADERS
    """Fetch new Hookdeck events, caching each raw payload locally.

    The function keeps calling the API until no `next` pagination link is
    returned. Each event's JSON body is stored under its `id` key in the
    local diskcache so re‑runs do not duplicate work.
    """
    if not (API_KEY and WEBHOOK_ID):
        raise RuntimeError("Missing HOOKDECK_API_KEY or HOOKDECK_WEBHOOK_ID env vars")

    events: List[dict] = []
    params = {"webhook_id": WEBHOOK_ID, "limit": limit}

    while True:
        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:  # network issues etc.
            logger.exception("Failed to fetch events from Hookdeck")
            capture_exception(exc)
            break

        batch = payload.get("data") or payload.get("models") or []
        if not batch:
            break

        for ev in batch:
            ev_id = ev.get("id")
            if ev_id in cache:
                logger.debug("Skipping already cached event %s", ev_id)
                continue
            cache[ev_id] = ev  # raw JSON stored for posterity
            events.append(ev)

        # pagination
        next_url = payload.get("next") or payload.get("links", {}).get("next")
        if next_url:
            BASE_URL_PAGE = next_url  # Hookdeck already includes query params
            params = None  # avoid double‑param mixture
            logger.info("Fetching next page: %s", next_url)
            BASE_URL = next_url  # noqa: PLW0127 – reassigned for next loop
        else:
            break

    return events


# ---------------------------------------------------------------------------
# Demo run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        raw_events = fetch_events()
        logger.info("Fetched %d new events", len(raw_events))

        parsed: List[Dict] = []
        for ev in raw_events:
            msg = ev.get("event", {}).get("body", "") or ev.get("body", "")
            doc = parse_transaction_message(msg)
            if doc:
                parsed.append(doc)
            else:
                logger.warning("Unparsed event %s", ev.get("id"))

        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as exc:  # capture any top‑level crash
        logger.exception("Unexpected failure")
        capture_exception(exc)
