# services/pb_writer/pocketbase.py
"""A *very* thin async wrapper around the PocketBase HTTP API.

* Authenticates once with admin e-mail/password (from settings).
* Provides a single public helper – :func:`upsert_parsed_sms` – consumed by the
  PB writer service.  The helper is **idempotent** thanks to a unique
  ``original_key`` field that we store with every record.

Dependencies
------------
Pure-stdlib + ``httpx`` and ``tenacity`` (both already in *poetry* deps).
"""
from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Any, Literal, Mapping

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from libs.config import get_settings
from libs.models import ParsedSMS
from libs.sentry import sentry_capture

__all__ = ["PocketBaseClient", "get_pb_client", "upsert_parsed_sms"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PocketBase client (async)
# ---------------------------------------------------------------------------


class PocketBaseClient:
    """Tiny async-client for the subset of PocketBase endpoints we use."""

    def __init__(self, *, base_url: str, email: str, password: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._email = email
        self._password = password
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=10.0)
        self._token: str | None = None

    # ------------------------------------------------------------------ auth
    async def _ensure_token(self) -> None:
        if self._token is not None:
            return
        resp = await self._client.post(
            "/api/admins/auth-with-password",
            json={"identity": self._email, "password": self._password},
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["token"]
        self._client.headers["Authorization"] = f"Bearer {self._token}"

    # ------------------------------------------------------------- low level
    async def _get(self, path: str, **kwargs: Any) -> httpx.Response:  # noqa: D401
        await self._ensure_token()
        return await self._client.get(path, **kwargs)

    async def _post(self, path: str, **kwargs: Any) -> httpx.Response:  # noqa: D401
        await self._ensure_token()
        return await self._client.post(path, **kwargs)

    async def _patch(self, path: str, **kwargs: Any) -> httpx.Response:  # noqa: D401
        await self._ensure_token()
        return await self._client.patch(path, **kwargs)

    # -------------------------------------------------------------- business

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
    async def upsert(self, collection: str, record: Mapping[str, Any], *, original_key: str) -> None:
        """Create or update a record guaranteeing *idempotency* by *original_key*."""
        # Try read-by-filter first (PocketBase lacks PUT-with-conflict-key)
        filter_param = {
            "filter": f"original_key='{original_key}'",
            "page": 1,
            "perPage": 1,
        }
        try:
            resp = await self._get(f"/api/collections/{collection}/records", params=filter_param)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                rec_id = items[0]["id"]
                # Patch existing
                resp = await self._patch(
                    f"/api/collections/{collection}/records/{rec_id}",
                    json=record,
                )
                resp.raise_for_status()
                logger.info("Patched existing record %s in %s", rec_id, collection)
                return
        except httpx.HTTPStatusError as exc:
            logger.warning("PocketBase search failed: %s", exc)
            raise  # let tenacity retry

        # Not found – create
        try:
            resp = await self._post(f"/api/collections/{collection}/records", json=record)
            resp.raise_for_status()
            logger.info("Inserted new record into %s", collection)
        except httpx.HTTPStatusError as exc:
            logger.error("PocketBase insert failed: %s", exc)
            raise  # trigger retry

    async def close(self) -> None:
        await self._client.aclose()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def get_pb_client() -> PocketBaseClient:
    """Return singleton PocketBaseClient configured from *libs.config*."""
    settings = get_settings()
    return PocketBaseClient(
        base_url=settings.pb_url,
        email=settings.pb_email,
        password=settings.pb_password,
    )


COLLECTION_DEBIT: Literal["sms_data"] = "sms_data"
COLLECTION_CREDIT: Literal["transactions"] = "transactions"


async def upsert_parsed_sms(parsed_sms: ParsedSMS) -> None:  # noqa: D401
    """Upsert *parsed_sms* into the appropriate PocketBase collection."""
    client = get_pb_client()

    record: dict[str, Any] = {
        "original_key": parsed_sms.msg_id,
        "original_body": parsed_sms.raw_body,
        "sender": parsed_sms.sender,
        "datetime": parsed_sms.date.isoformat(),
        "card": parsed_sms.card,
        "amount": str(parsed_sms.amount),
        "currency": parsed_sms.currency,
        "balance": str(parsed_sms.balance) if parsed_sms.balance is not None else None,
        "merchant": parsed_sms.merchant,
        "address": parsed_sms.address,
        "city": parsed_sms.city,
        "txn_type": parsed_sms.txn_type,
    }

    collection = COLLECTION_DEBIT

    try:
        await client.upsert(collection, record, original_key=parsed_sms.msg_id)
    except RetryError as exc:  # after several attempts
        logger.error("PocketBase upsert gave up: %s", exc)
        sentry_capture(exc, extras={"record": record})
        raise
