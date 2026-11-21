"""Configuration for the row-lock drill app."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - startup misconfiguration
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from exc


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - startup misconfiguration
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    database_url: str
    session_prefix: str
    connect_timeout: int
    # How long to wait for the observed blocking edge to appear.
    block_confirm_seconds: float
    # How long to let the blocked UPDATE finish after a successful terminate.
    release_wait_seconds: float
    # How long to wait for an already-running UPDATE when nothing was terminated.
    refused_wait_seconds: float
    # Abandoned active drills are cleaned up after this many seconds.
    drill_ttl_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.environ.get(
                "DATABASE_URL", "postgresql://drill:drill@localhost:5432/drill"
            ),
            session_prefix=os.environ.get("DRILL_SESSION_PREFIX", "lockdrill"),
            connect_timeout=_int_env("DB_CONNECT_TIMEOUT", 5),
            block_confirm_seconds=_float_env("BLOCK_CONFIRM_SECONDS", 5.0),
            release_wait_seconds=_float_env("RELEASE_WAIT_SECONDS", 10.0),
            refused_wait_seconds=_float_env("REFUSED_WAIT_SECONDS", 2.0),
            drill_ttl_seconds=_float_env("DRILL_TTL_SECONDS", 1800.0),
        )


settings = Settings.from_env()
