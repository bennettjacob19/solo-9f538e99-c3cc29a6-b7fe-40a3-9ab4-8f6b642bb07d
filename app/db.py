"""Database helpers."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import settings

DRILL_ROW_ID = 1


def connect(
    *,
    application_name: str | None = None,
    autocommit: bool = False,
) -> psycopg.Connection[Any]:
    """Open a connection with the drill's standard connection settings.

    Rows are returned as dicts so callers can address columns by name.
    """
    kwargs: dict[str, Any] = {
        "connect_timeout": settings.connect_timeout,
        "autocommit": autocommit,
        "row_factory": dict_row,
    }
    if application_name is not None:
        kwargs["application_name"] = application_name
    return psycopg.connect(settings.database_url, **kwargs)


def ensure_schema() -> None:
    """Create and seed the single-row table used by the drill."""
    with connect(application_name=f"{settings.session_prefix}:setup") as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lock_drill_row (
                id integer PRIMARY KEY,
                value integer NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            INSERT INTO lock_drill_row (id, value)
            VALUES (%s, 0)
            ON CONFLICT (id) DO NOTHING
            """,
            (DRILL_ROW_ID,),
        )
        conn.commit()
