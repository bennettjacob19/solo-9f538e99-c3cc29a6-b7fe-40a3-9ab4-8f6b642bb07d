"""Row-lock drill orchestration.

A drill keeps two real PostgreSQL sessions alive:

* a *holder* (“持锁会话”) connection which opens a transaction and locks the
  drill row with ``SELECT ... FOR UPDATE``;
* a *waiter* (“受阻更新”) connection which runs an ``UPDATE`` of the same row
  and therefore blocks on the holder's row lock.

Every session is identified by the triple ``pid + backend_start +
application_name``.  The application name embeds the drill id, so a reused pid
can never be mistaken for a session this drill created, and a stale page can
never touch a later drill.

Releasing a drill terminates **only** the holder session belonging to that
drill, and only if a single SQL statement can still see that exact session and
that it is still the blocker of this drill's waiter.  Recovery is judged from
the waiter's real ``COMMIT`` result, never from the return value of
``pg_terminate_backend``.
"""

from __future__ import annotations

import datetime as dt
import hmac
import logging
import secrets
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any

import psycopg

from .config import settings
from .db import DRILL_ROW_ID, connect

logger = logging.getLogger("lockdrill")

ACTIVE_STATES = {"starting", "blocked", "releasing"}
RELEASE_STATES = {"recovered", "not_recovered"}
HISTORY_LIMIT = 20


class DrillError(Exception):
    """Base class for drill control errors."""

    status_code = 400


class DrillNotFound(DrillError):
    status_code = 410


class DrillTokenMismatch(DrillError):
    status_code = 403


class DrillNotReleasable(DrillError):
    status_code = 409


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="microseconds")
    return str(value)


def _app_name(drill_id: str, role: str) -> str:
    # PostgreSQL truncates application_name to 63 bytes (NAMEDATALEN - 1).
    # The "drill:" segment lets startup cleanup find sessions left behind by a
    # previous process without touching admin/health connections.
    return f"{settings.session_prefix}:drill:{role}:{drill_id}"[:63]


class Drill:
    """A single drill round and its two PostgreSQL sessions."""

    def __init__(self, drill_id: str, round_no: int, token: str) -> None:
        self.id = drill_id
        self.round = round_no
        self.token = token
        self.created_at = utcnow()
        self.status = "starting"
        self.ended_reason: str | None = None
        self.ended_at: dt.datetime | None = None

        self.holder_app = _app_name(drill_id, "hold")
        self.waiter_app = _app_name(drill_id, "wait")

        self.holder_conn: psycopg.Connection[Any] | None = None
        self.holder_pid: int | None = None
        self.holder_backend_start: dt.datetime | None = None
        self.holder_xact_start: dt.datetime | None = None

        self.waiter_pid: int | None = None
        self.waiter_backend_start: dt.datetime | None = None
        self.waiter_thread: threading.Thread | None = None
        self.waiter_ready = threading.Event()
        self.waiter_done = threading.Event()
        self.waiter_outcome: dict[str, Any] | None = None
        self.waiter_setup_error: str | None = None

        self.release_result: dict[str, Any] | None = None
        self.ending = False
        self.lock = threading.Lock()

    # -- identity --------------------------------------------------------
    def holder_identity(self) -> dict[str, Any]:
        return {
            "pid": self.holder_pid,
            "backend_start": iso(self.holder_backend_start),
            "application_name": self.holder_app,
        }

    def waiter_identity(self) -> dict[str, Any]:
        return {
            "pid": self.waiter_pid,
            "backend_start": iso(self.waiter_backend_start),
            "application_name": self.waiter_app,
        }

    def is_active(self) -> bool:
        return self.status in ACTIVE_STATES

    def age_seconds(self) -> float:
        return (utcnow() - self.created_at).total_seconds()


class DrillStore:
    """In-memory registry of drill rounds.

    The deployment runs a single application process (see docker-compose.yml),
    so the registry is process-local; PostgreSQL itself is the source of truth
    for session state.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._start_lock = threading.Lock()
        self._drills: "OrderedDict[str, Drill]" = OrderedDict()
        self._active_id: str | None = None
        self._round = 0
        self._stopping = False

    # -- registry --------------------------------------------------------
    def get(self, drill_id: str) -> Drill | None:
        with self._lock:
            return self._drills.get(drill_id)

    def require(self, drill_id: str) -> Drill:
        drill = self.get(drill_id)
        if drill is None:
            raise DrillNotFound(
                "该演练记录已不存在（服务可能已重启或记录已清理），未执行任何操作。"
            )
        return drill

    def active(self) -> Drill | None:
        with self._lock:
            if self._active_id is None:
                return None
            return self._drills.get(self._active_id)

    def recent(self, limit: int = 8) -> list[Drill]:
        with self._lock:
            items = list(self._drills.values())
        return items[-limit:][::-1]

    def _register(self, drill: Drill) -> None:
        with self._lock:
            # Forget finished rounds first, oldest first, keeping the limit.
            while len(self._drills) >= HISTORY_LIMIT:
                removable = next(
                    (did for did, item in self._drills.items() if not item.is_active()),
                    None,
                )
                if removable is None:
                    break
                self._drills.pop(removable, None)
            self._drills[drill.id] = drill
            self._active_id = drill.id

    def _set_active(self, drill_id: str | None) -> None:
        with self._lock:
            self._active_id = drill_id

    # -- start -----------------------------------------------------------
    def start(self) -> Drill:
        with self._start_lock:
            if self._stopping:
                raise DrillError("服务正在关闭，无法开始新的演练。")
            previous = self.active()
            if previous is not None and previous.is_active():
                self._end(previous, "开始新一轮演练，上一轮已被结束")
            with self._lock:
                self._round += 1
                round_no = self._round
            drill = Drill(uuid.uuid4().hex, round_no, secrets.token_urlsafe(24))
            self._register(drill)
            try:
                self._setup(drill)
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                with drill.lock:
                    drill.status = "error"
                    drill.ended_reason = f"演练初始化失败：{message}"
                    drill.ended_at = utcnow()
                self._release_local_holder(drill)
                # The waiter thread, if it started, may still be running: its
                # session is identified by app name, so end it too.
                try:
                    with _admin() as admin:
                        admin.execute(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE application_name = %s",
                            (drill.waiter_app,),
                        )
                except Exception:
                    logger.exception(
                        "could not clean up waiter for failed drill %s", drill.id
                    )
                thread = drill.waiter_thread
                if thread is not None and thread is not threading.current_thread():
                    thread.join(timeout=2.0)
                self._set_active(None)
                raise DrillError(f"无法开始演练：{message}") from exc
            return drill

    def _setup(self, drill: Drill) -> None:
        # 1. Holder session: open a transaction and lock the drill row.
        conn = connect(autocommit=False, application_name=drill.holder_app)
        with drill.lock:
            drill.holder_conn = conn
        identity = conn.execute(
            """
            SELECT pg_backend_pid() AS pid, backend_start
            FROM pg_stat_activity
            WHERE pid = pg_backend_pid()
            """
        ).fetchone()
        with drill.lock:
            drill.holder_pid = identity["pid"]
            drill.holder_backend_start = identity["backend_start"]
        conn.execute(
            "SELECT id, value FROM lock_drill_row WHERE id = %s FOR UPDATE",
            (DRILL_ROW_ID,),
        )
        with drill.lock:
            drill.holder_xact_start = conn.execute("SELECT now() AS ts").fetchone()["ts"]

        # 2. Waiter session: a real UPDATE that blocks on the holder's lock.
        thread = threading.Thread(
            target=_waiter_main,
            args=(drill,),
            name=f"drill-waiter-{drill.id[:8]}",
            daemon=True,
        )
        with drill.lock:
            drill.waiter_thread = thread
        thread.start()

        if not drill.waiter_ready.wait(timeout=settings.block_confirm_seconds):
            raise DrillError("受阻更新会话未能在限定时间内建立。")
        if drill.waiter_setup_error:
            raise DrillError(f"受阻更新会话建立失败：{drill.waiter_setup_error}")

        # 3. Confirm from PostgreSQL's own view that the waiter is blocked by
        #    this holder (not merely slow, and not blocked by someone else).
        deadline = time.monotonic() + settings.block_confirm_seconds
        last_seen = "未知"
        while time.monotonic() < deadline:
            if drill.waiter_done.is_set():
                outcome = drill.waiter_outcome or {}
                raise DrillError(
                    "受阻更新没有进入等待状态："
                    + str(outcome.get("error") or outcome.get("status"))
                )
            with _admin() as admin:
                state = _read_sessions(
                    admin, drill.holder_pid, drill.waiter_pid
                )
            holder_row = state.get(drill.holder_pid)
            waiter_row = state.get(drill.waiter_pid)
            if holder_row is not None and waiter_row is not None:
                last_seen = (
                    f"waiter state={waiter_row['state']}, "
                    f"wait={waiter_row['wait_event_type']}/{waiter_row['wait_event']}, "
                    f"blocking={waiter_row['blocking_pids']}"
                )
                if (
                    _row_matches(holder_row, drill.holder_backend_start, drill.holder_app)
                    and _row_matches(waiter_row, drill.waiter_backend_start, drill.waiter_app)
                    and drill.holder_pid in waiter_row["blocking_pids"]
                ):
                    with drill.lock:
                        drill.status = "blocked"
                    return
            time.sleep(0.05)
        raise DrillError(
            "数据库中未观察到预期的阻塞关系"
            f"（持锁 PID={drill.holder_pid}，受阻 PID={drill.waiter_pid}；最后观察：{last_seen}）。"
        )

    # -- release ---------------------------------------------------------
    def release(self, drill_id: str, token: str) -> dict[str, Any]:
        drill = self.require(drill_id)
        if not hmac.compare_digest(token, drill.token):
            raise DrillTokenMismatch(
                "页面令牌与演练会话不匹配（可能来自已结束的旧页面），未执行任何操作。"
            )

        with drill.lock:
            if drill.release_result is not None:
                # Idempotent: repeat submissions return the original verdict.
                return drill.release_result
            if drill.status == "releasing":
                raise DrillNotReleasable("该演练正在执行解锁，请等待本次操作完成。")
            if drill.status != "blocked":
                raise DrillNotReleasable(
                    f"当前演练状态为 {drill.status}，不会终止任何会话。"
                )
            drill.status = "releasing"

        try:
            result = self._do_release(drill)
        except Exception as exc:
            logger.exception("release failed for drill %s", drill.id)
            result = {
                "action": "error",
                "recovered": False,
                "terminated": False,
                "verification": f"解锁过程异常：{type(exc).__name__}: {exc}",
                "finished_at": iso(utcnow()),
                "outcome": drill.waiter_outcome,
            }

        with drill.lock:
            drill.release_result = result
            drill.status = "recovered" if result["recovered"] else "not_recovered"
        # Only drop the local connection object once PostgreSQL confirms the
        # session is gone. Closing a still-live holder would release the lock
        # even when the release was explicitly refused.
        self._close_holder(drill, only_if_dead=True)
        return result

    def _do_release(self, drill: Drill) -> dict[str, Any]:
        params = {
            "holder_pid": drill.holder_pid,
            "holder_backend_start": drill.holder_backend_start,
            "holder_app": drill.holder_app,
            "waiter_pid": drill.waiter_pid,
            "waiter_backend_start": drill.waiter_backend_start,
            "waiter_app": drill.waiter_app,
        }
        result: dict[str, Any] = {
            "action": "refused",
            "terminated": False,
            "terminate_error": None,
            "recovered": False,
            "verification": "",
            "outcome": None,
            "observed_row": None,
            "remaining_blockers": [],
            "identity_checked": True,
            "started_at": iso(utcnow()),
            "finished_at": None,
        }

        try:
            with _admin() as admin:
                row = admin.execute(_TERMINATE_IF_STILL_BLOCKING_SQL, params).fetchone()
        except Exception as exc:
            result["terminate_error"] = f"{type(exc).__name__}: {exc}"
            row = None

        terminated = bool(row and row["terminated"])
        result["terminated"] = terminated
        if terminated:
            result["action"] = "terminate"
        elif row is not None:
            result["action"] = "refused"
        if result["action"] == "refused" and result["terminate_error"] is None:
            result["verification"] = _refusal_reason(drill, params)

        # The verdict comes from the blocked UPDATE itself: wait for its real
        # COMMIT/ROLLBACK result.  If we terminated, that is normally instant.
        wait_seconds = (
            settings.release_wait_seconds if terminated else settings.refused_wait_seconds
        )
        drill.waiter_done.wait(timeout=wait_seconds)
        outcome = drill.waiter_outcome
        result["outcome"] = outcome

        # Independent observation of the row after the fact.
        try:
            with _admin() as admin:
                row_after = admin.execute(
                    "SELECT value, updated_at FROM lock_drill_row WHERE id = %s",
                    (DRILL_ROW_ID,),
                ).fetchone()
                result["observed_row"] = (
                    {
                        "value": row_after["value"],
                        "updated_at": iso(row_after["updated_at"]),
                    }
                    if row_after
                    else None
                )
                if drill.waiter_pid is not None:
                    result["remaining_blockers"] = _blockers_of(admin, drill.waiter_pid)
                result["holder_alive"] = _session_alive(
                    admin,
                    drill.holder_pid,
                    drill.holder_backend_start,
                    drill.holder_app,
                )
        except Exception as exc:
            result["verification"] = (
                result["verification"]
                + f" 事后核查数据库失败：{type(exc).__name__}: {exc}"
            ).strip()
            result["holder_alive"] = None

        result["recovered"] = bool(outcome and outcome.get("status") == "committed")
        result["verification"] = _verdict_text(result, terminated)
        result["finished_at"] = iso(utcnow())
        return result

    # -- end / cleanup ---------------------------------------------------
    def end(
        self, drill_id: str, token: str | None, reason: str = "用户结束了演练"
    ) -> None:
        drill = self.require(drill_id)
        if token is not None and not hmac.compare_digest(token, drill.token):
            raise DrillTokenMismatch(
                "页面令牌与演练会话不匹配（可能来自已结束的旧页面），未执行任何操作。"
            )
        self._end(drill, reason)

    def _end(self, drill: Drill, reason: str) -> None:
        with drill.lock:
            if drill.ending:
                return
            drill.ending = True
        try:
            with _admin() as admin:
                for pid, backend_start, app in (
                    (drill.holder_pid, drill.holder_backend_start, drill.holder_app),
                    (drill.waiter_pid, drill.waiter_backend_start, drill.waiter_app),
                ):
                    if pid is None or backend_start is None:
                        continue
                    admin.execute(
                        """
                        SELECT pg_terminate_backend(a.pid)
                        FROM pg_stat_activity a
                        WHERE a.pid = %s
                          AND a.backend_start = %s
                          AND a.application_name = %s
                        """,
                        (pid, backend_start, app),
                    )
        except Exception:
            logger.exception("cleanup failed for drill %s", drill.id)
        self._release_local_holder(drill)
        thread = drill.waiter_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3.0)
        with drill.lock:
            drill.status = "ended"
            drill.ended_reason = reason
            drill.ended_at = utcnow()
        if self._active_id == drill.id:
            self._set_active(None)

    def _close_holder(self, drill: Drill, only_if_dead: bool = False) -> None:
        """Drop the local holder connection.

        With ``only_if_dead`` the connection is kept open unless PostgreSQL no
        longer has that session, so a refused release cannot accidentally
        release the row lock by closing our own connection.
        """
        with drill.lock:
            conn = drill.holder_conn
            if conn is None:
                return
        if only_if_dead:
            try:
                with _admin() as admin:
                    alive = _backend_alive(
                        admin, drill.holder_pid, drill.holder_backend_start
                    )
            except Exception:
                alive = True  # unknown: do not risk releasing the lock
            if alive:
                return
        with drill.lock:
            if drill.holder_conn is not conn:
                return
            drill.holder_conn = None
        try:
            conn.close()
        except Exception:
            pass

    def _release_local_holder(self, drill: Drill) -> None:
        """Unconditionally close the holder connection (end-of-drill cleanup)."""
        with drill.lock:
            conn = drill.holder_conn
            drill.holder_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def cleanup_expired(self) -> None:
        with self._lock:
            drills = list(self._drills.values())
        for drill in drills:
            if drill.is_active() and drill.age_seconds() > settings.drill_ttl_seconds:
                self._end(drill, "演练超过保留时长，已自动结束")

    def shutdown(self) -> None:
        with self._start_lock:
            self._stopping = True
        with self._lock:
            drills = list(self._drills.values())
        for drill in drills:
            if drill.is_active():
                self._end(drill, "服务关闭，演练会话已结束")

    def reset(self, reason: str = "应用重启，演练记录已丢弃") -> None:
        """End every drill and forget it, as an application restart would.

        Session state in PostgreSQL is finalised first, so no orphaned locks
        are left behind; afterwards the registry is empty and new rounds start
        from scratch.
        """
        with self._lock:
            drills = list(self._drills.values())
        for drill in drills:
            self._end(drill, reason)
        with self._lock:
            self._drills.clear()
            self._active_id = None
            self._round = 0

    # -- observation -----------------------------------------------------
    def snapshot(self, drill: Drill) -> dict[str, Any]:
        snap: dict[str, Any] = {
            "id": drill.id,
            "round": drill.round,
            "status": drill.status,
            "created_at": iso(drill.created_at),
            "ended_at": iso(drill.ended_at),
            "ended_reason": drill.ended_reason,
            "is_active": drill.is_active(),
            "identity": {
                "holder": drill.holder_identity(),
                "waiter": drill.waiter_identity(),
            },
            "holder": None,
            "waiter": None,
            "holder_identity_matches": False,
            "waiter_identity_matches": False,
            "blocking_confirmed": False,
            "blockers": [],
            "locks": {"holder": [], "waiter": []},
            "row": None,
            "db_error": None,
            "release_result": drill.release_result,
            "waiter_outcome": drill.waiter_outcome,
            "can_release": False,
            "can_end": False,
        }
        if drill.holder_pid is None:
            return snap
        try:
            with _admin() as admin:
                sessions = _read_sessions(admin, drill.holder_pid, drill.waiter_pid)
                holder_row = sessions.get(drill.holder_pid)
                waiter_row = sessions.get(drill.waiter_pid)
                snap["holder_identity_matches"] = _row_matches(
                    holder_row, drill.holder_backend_start, drill.holder_app
                )
                snap["waiter_identity_matches"] = _row_matches(
                    waiter_row, drill.waiter_backend_start, drill.waiter_app
                )
                snap["holder"] = _session_payload(holder_row)
                snap["waiter"] = _session_payload(waiter_row)
                blockers = (
                    waiter_row["blocking_pids"] if waiter_row is not None else []
                )
                snap["blockers"] = _blockers_of(admin, drill.waiter_pid)
                snap["blocking_confirmed"] = (
                    drill.holder_pid in blockers
                    and snap["holder_identity_matches"]
                    and snap["waiter_identity_matches"]
                )
                snap["locks"] = {
                    "holder": _locks_of(admin, drill.holder_pid),
                    "waiter": _locks_of(admin, drill.waiter_pid),
                }
                row = admin.execute(
                    "SELECT value, updated_at FROM lock_drill_row WHERE id = %s",
                    (DRILL_ROW_ID,),
                ).fetchone()
                snap["row"] = (
                    {"value": row["value"], "updated_at": iso(row["updated_at"])}
                    if row
                    else None
                )
                snap["db_time"] = iso(admin.execute("SELECT now() AS ts").fetchone()["ts"])
        except Exception as exc:
            snap["db_error"] = f"{type(exc).__name__}: {exc}"

        snap["can_release"] = (
            drill.status == "blocked"
            and snap["blocking_confirmed"]
            and drill.release_result is None
        )
        snap["can_end"] = drill.is_active()
        return snap


# ---------------------------------------------------------------------------
# PostgreSQL observation helpers
# ---------------------------------------------------------------------------


def _admin() -> psycopg.Connection[Any]:
    return connect(application_name=f"{settings.session_prefix}:admin", autocommit=True)


_SESSIONS_SQL = """
SELECT pid, backend_start, xact_start, query_start, state_change, state,
       wait_event_type, wait_event, query, application_name, backend_type,
       usename, client_addr::text AS client_addr,
       pg_blocking_pids(pid) AS blocking_pids
FROM pg_stat_activity
WHERE pid = ANY(%(pids)s)
"""


def _read_sessions(
    conn: psycopg.Connection[Any], *pids: int | None
) -> dict[int, dict[str, Any]]:
    wanted = [pid for pid in pids if pid is not None]
    if not wanted:
        return {}
    rows = conn.execute(_SESSIONS_SQL, {"pids": wanted}).fetchall()
    return {row["pid"]: row for row in rows}


def _blockers_of(
    conn: psycopg.Connection[Any], pid: int | None
) -> list[dict[str, Any]]:
    if pid is None:
        return []
    rows = conn.execute(
        """
        SELECT pid, backend_start, xact_start, state, wait_event_type, wait_event,
               query, application_name, usename, client_addr::text AS client_addr
        FROM pg_stat_activity
        WHERE pid = ANY(pg_blocking_pids(%s))
        """,
        (pid,),
    ).fetchall()
    return [_session_payload(row) for row in rows if row is not None]


def _locks_of(conn: psycopg.Connection[Any], pid: int | None) -> list[dict[str, Any]]:
    if pid is None:
        return []
    rows = conn.execute(
        """
        SELECT l.locktype, l.mode, l.granted,
               c.relnamespace::regnamespace::text || '.' || c.relname AS relation,
               l.page, l.tuple, l.virtualxid::text AS virtualxid,
               l.transactionid::text AS transactionid,
               l.classid::text AS classid, l.objid::text AS objid, l.objsubid
        FROM pg_locks l
        LEFT JOIN pg_class c ON c.oid = l.relation
        WHERE l.pid = %s
        ORDER BY l.granted, l.locktype, relation NULLS LAST
        """,
        (pid,),
    ).fetchall()
    return [dict(row) for row in rows]


def _row_matches(
    row: dict[str, Any] | None,
    backend_start: dt.datetime | None,
    app_name: str,
) -> bool:
    if row is None or backend_start is None:
        return False
    return (
        row["backend_start"] == backend_start
        and row["application_name"] == app_name
    )


def _session_alive(
    conn: psycopg.Connection[Any],
    pid: int | None,
    backend_start: dt.datetime | None,
    app_name: str,
) -> bool:
    """True only for the exact (pid, backend_start, application_name) session."""
    if pid is None:
        return False
    sessions = _read_sessions(conn, pid)
    return _row_matches(sessions.get(pid), backend_start, app_name)


def _backend_alive(
    conn: psycopg.Connection[Any],
    pid: int | None,
    backend_start: dt.datetime | None,
) -> bool:
    """True while the backend for (pid, backend_start) still exists.

    Used only to decide whether it is safe to drop our local connection
    object: closing it while the backend is alive would release its locks.
    """
    if pid is None or backend_start is None:
        return False
    sessions = _read_sessions(conn, pid)
    row = sessions.get(pid)
    return row is not None and row["backend_start"] == backend_start


def _session_payload(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    data = dict(row)
    for key in ("backend_start", "xact_start", "query_start", "state_change"):
        data[key] = iso(data.get(key))
    return data


# ---------------------------------------------------------------------------
# Waiter session (thread)
# ---------------------------------------------------------------------------


def _waiter_main(drill: Drill) -> None:
    started = time.monotonic()
    conn: psycopg.Connection[Any] | None = None
    outcome: dict[str, Any]
    try:
        conn = connect(autocommit=False, application_name=drill.waiter_app)
        identity = conn.execute(
            """
            SELECT pg_backend_pid() AS pid, backend_start
            FROM pg_stat_activity
            WHERE pid = pg_backend_pid()
            """
        ).fetchone()
        with drill.lock:
            drill.waiter_pid = identity["pid"]
            drill.waiter_backend_start = identity["backend_start"]
        drill.waiter_ready.set()

        # Blocks here until the holder's row lock is released.
        cur = conn.execute(
            """
            UPDATE lock_drill_row
            SET value = value + 1, updated_at = now()
            WHERE id = %s
            RETURNING value, updated_at
            """,
            (DRILL_ROW_ID,),
        )
        returned = cur.fetchone()
        if returned is None:
            conn.rollback()
            outcome = {
                "status": "no_row",
                "error": "UPDATE 未匹配到演练行，已回滚",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "finished_at": iso(utcnow()),
            }
        else:
            # The real commit: this is what decides recovery.
            conn.commit()
            outcome = {
                "status": "committed",
                "value": returned["value"],
                "updated_at": iso(returned["updated_at"]),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "finished_at": iso(utcnow()),
            }
    except BaseException as exc:  # KeyboardInterrupt / connection termination
        outcome = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "finished_at": iso(utcnow()),
        }
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        if conn is None:
            drill.waiter_setup_error = outcome["error"]
    finally:
        with drill.lock:
            drill.waiter_outcome = outcome
        drill.waiter_ready.set()
        drill.waiter_done.set()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Release internals
# ---------------------------------------------------------------------------

# Single statement: terminate the holder only if that exact session (pid and
# backend_start and application_name embed the drill id) is still alive and
# still appears in pg_blocking_pids() of this drill's exact waiter session.
_TERMINATE_IF_STILL_BLOCKING_SQL = """
WITH target AS (
    SELECT h.pid
    FROM pg_stat_activity h
    JOIN pg_stat_activity w
      ON w.pid = %(waiter_pid)s
     AND w.backend_start = %(waiter_backend_start)s
     AND w.application_name = %(waiter_app)s
    WHERE h.pid = %(holder_pid)s
      AND h.backend_start = %(holder_backend_start)s
      AND h.application_name = %(holder_app)s
      AND h.pid = ANY (pg_blocking_pids(w.pid))
)
SELECT pid, pg_terminate_backend(pid) AS terminated FROM target
"""


def _refusal_reason(drill: Drill, params: dict[str, Any]) -> str:
    try:
        with _admin() as admin:
            sessions = _read_sessions(admin, drill.holder_pid, drill.waiter_pid)
            holder = sessions.get(drill.holder_pid)
            waiter = sessions.get(drill.waiter_pid)
            if not _row_matches(holder, drill.holder_backend_start, drill.holder_app):
                if holder is None:
                    return (
                        f"未终止任何会话：原持锁会话（PID {drill.holder_pid}）已不存在。"
                    )
                return (
                    f"未终止任何会话：PID {drill.holder_pid} 已被其他会话复用"
                    "（backend_start 或 application_name 不一致）。"
                )
            if not _row_matches(waiter, drill.waiter_backend_start, drill.waiter_app):
                if waiter is None:
                    return (
                        f"未终止任何会话：原受阻更新会话（PID {drill.waiter_pid}）已不存在。"
                    )
                return (
                    f"未终止任何会话：PID {drill.waiter_pid} 已被其他会话复用，"
                    "无法确认原始阻塞关系。"
                )
            if drill.holder_pid not in waiter["blocking_pids"]:
                return (
                    f"未终止任何会话：持锁会话 PID {drill.holder_pid} 已不再是本演练受阻更新"
                    f"（PID {drill.waiter_pid}）的阻塞者。"
                )
            return "未终止任何会话：原子校验未通过（会话或阻塞关系在操作瞬间发生变化）。"
    except Exception as exc:
        return f"未终止任何会话：校验失败（{type(exc).__name__}: {exc}）。"


def _verdict_text(result: dict[str, Any], terminated: bool) -> str:
    outcome = result["outcome"]
    parts: list[str] = []
    if terminated:
        parts.append("已终止本演练持有的持锁会话（PID 与会话启动时间双重校验通过）。")
    elif result["terminate_error"]:
        parts.append(
            f"终止持锁会话失败：{result['terminate_error']}，未终止任何会话。"
        )
    else:
        parts.append(result.get("verification") or "未终止任何会话。")

    if outcome and outcome.get("status") == "committed":
        value = outcome.get("value")
        observed = result.get("observed_row")
        parts.append(
            f"受阻更新已真实提交（COMMIT 成功，UPDATE 返回 value={value}）。恢复判定：已恢复。"
        )
        if observed is not None:
            if observed["value"] == value:
                parts.append(
                    f"独立连接复核当前行 value={observed['value']}，与提交结果一致。"
                )
            else:
                parts.append(
                    f"独立连接复核当前行 value={observed['value']}，与提交值不一致"
                    "（可能已被后续操作修改），请人工核对。"
                )
    elif outcome and outcome.get("status") != "committed":
        parts.append(
            f"受阻更新未提交成功（{outcome.get('status')}：{outcome.get('error')}）。"
            "恢复判定：未恢复。"
        )
    else:
        blockers = result.get("remaining_blockers") or []
        if blockers:
            desc = "、".join(
                f"PID {b['pid']}（{b.get('application_name') or '未知应用'}，"
                f"state={b.get('state')}，wait={b.get('wait_event_type')}/{b.get('wait_event')}）"
                for b in blockers
            )
            parts.append(f"受阻更新仍在等待，当前阻塞者：{desc}。恢复判定：未恢复。")
        else:
            parts.append(
                "受阻更新在等待窗口内仍未返回结果，恢复判定：未恢复，请刷新观察。"
            )

    if result.get("holder_alive"):
        parts.append(
            "注意：终止后持锁会话仍存活，可能仍在持锁，请结束演练或人工处理。"
        )
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Module-level store instance
# ---------------------------------------------------------------------------

store = DrillStore()
