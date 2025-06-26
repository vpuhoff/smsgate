# libs/sentry.py
"""Thin wrapper around *sentry-sdk* used by every service.

*   **Lazy init** – Sentry initialises **once** via :func:`init_sentry`.
    Without a DSN, the helpers silently no-op – удобно для локальной
    разработки.
*   **Global capture helper** – :func:`sentry_capture` records an exception with
    optional *extras* in a single line of code.

Usage
-----
```python
from libs.sentry import init_sentry, sentry_capture

init_sentry(release="parser@1.0.0", env="prod")
...
try:
    risky_operation()
except Exception as e:
    sentry_capture(e, extras={"user_id": 123})
```
"""
from __future__ import annotations

import os
from functools import lru_cache
from types import TracebackType
from typing import Any, Mapping, MutableMapping, Optional, Type

import sentry_sdk
from sentry_sdk import Hub

from libs.config import get_settings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def init_sentry(*, release: str | None = None, env: str | None = None) -> None:
    """Initialise Sentry SDK once per process.

    If neither *SENTRY_DSN* environment variable nor *settings.sentry_dsn* is
    present, the function becomes a no-op (and :func:`sentry_capture` will do
    nothing as well).
    """
    settings = get_settings()
    dsn = os.getenv("SENTRY_DSN") or getattr(settings, "sentry_dsn", "")
    if not dsn:
        return  # Local run without Sentry – silently skip

    sentry_sdk.init(
        dsn=dsn,
        release=release,
        environment=env or getattr(settings, "env", "local"),
        traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "1.0")),
        profile_session_sample_rate=float(os.getenv("SENTRY_PROFILE_SAMPLE_RATE", "1.0")),
        max_value_length=4_096,  # guard against huge events
    )


def sentry_capture(exc: BaseException, *, extras: Optional[dict[str, Any]] = None) -> None:  # noqa: D401
    """Capture *exc* to Sentry if SDK is initialised.

    Parameters
    ----------
    exc
        The exception object to record.
    extras
        Extra key/value pairs to attach to the event (e.g. raw SMS body).
    """
    client = Hub.current.client
    if client is None:  # SDK not initialised
        return

    with sentry_sdk.push_scope() as scope:  # type: ignore[attr-defined]
        if extras:
            for key, value in extras.items():
                scope.set_extra(key, value)
        sentry_sdk.capture_exception(exc)  # type: ignore[attr-defined]
