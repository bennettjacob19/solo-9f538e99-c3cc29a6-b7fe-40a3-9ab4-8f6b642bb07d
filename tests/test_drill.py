"""End-to-end tests against a real PostgreSQL.

Requires ``TEST_DATABASE_URL`` (an admin connection able to CREATE ROLE and
CREATE DATABASE). The suite provisions a dedicated role and database, so run it
against a throwaway PostgreSQL instance:

    TEST_DATABASE_URL=postgresql://postgres@localhost/postgres pytest
"""

from __future__ import annotations

import importlib
import os
import re
import sys
import time

import psycopg
import pytest
from fastapi.testclient import TestClient

ADMIN_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not ADMIN_URL, reason="TEST_DATABASE_URL not set; real-database tests skipped"
)

ROLE = "lock_drill_test"
PASSWORD = "lock_drill_test"
DB = "lock_drill_test"


def _provision() -> str:
    assert ADMIN_URL
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
        try:
            conn.execute(f"CREATE ROLE {ROLE} LOGIN PASSWORD '{PASSWORD}'")
        except psycopg.errors.DuplicateObject:
            pass
        conn.execute(f"CREATE DATABASE {DB} OWNER {ROLE}")
        conn.execute(f"GRANT CREATE, USAGE ON SCHEMA public TO {ROLE}")
    return ADMIN_URL.replace("/postgres", f"/{DB}", 1)


@pytest.fixture(scope="session")
def database_url() -> str:
    return _provision()


@pytest.fixture()
def web(database_url, monkeypatch):
    """Fresh app + store, mimicking an application restart per test."""
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("BLOCK_CONFIRM_SECONDS", "5")
    monkeypatch.setenv("RELEASE_WAIT_SECONDS", "10")
    monkeypatch.setenv("REFUSED_WAIT_SECONDS", "1")
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.main

    importlib.reload(app.config)
    importlib.reload(app.db)
    app.drill = importlib.reload(app.drill)
    app.main = importlib.reload(app.main)
    client = TestClient(app.main.app)
    with client:
        yield client, app.drill.store
    app.drill.store.shutdown()


def _start(client) -> str:
    res = client.post("/drill/start", follow_redirects=False)
    assert res.status_code == 303, res.text
    return res.headers["location"].rsplit("/", 1)[-1]


def _state(client, drill_id: str) -> dict:
    return client.get(f"/api/state?drill_id={drill_id}").json()["drill"]


def _token(client, drill_id: str) -> str:
    html = client.get(f"/drill/{drill_id}").text
    return re.search(r'name="token" value="([^"]+)"', html).group(1)


def _sessions(drill_id: str) -> list[tuple[str, str]]:
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        return [
            (row[0], row[1])
            for row in conn.execute(
                "SELECT application_name, state FROM pg_stat_activity "
                "WHERE pid <> pg_backend_pid() AND application_name LIKE %s",
                (f"%{drill_id}",),
            ).fetchall()
        ]


def test_blocking_relation_is_real_and_release_recovers(web):
    client, store = web
    drill_id = _start(client)
    snap = _state(client, drill_id)

    assert snap["status"] == "blocked"
    assert snap["blocking_confirmed"] is True
    holder_pid = snap["identity"]["holder"]["pid"]
    assert [b["pid"] for b in snap["blockers"]] == [holder_pid]
    assert snap["waiter"]["wait_event_type"] == "Lock"
    assert snap["waiter"]["state"] == "active"
    assert snap["holder"]["state"] == "idle in transaction"
    assert snap["identity"]["holder"]["backend_start"]
    assert snap["identity"]["waiter"]["backend_start"]
    assert snap["locks"]["holder"]

    before = snap["row"]["value"]
    res = client.post(
        f"/api/drills/{drill_id}/release", data={"token": _token(client, drill_id)}
    )
    assert res.status_code == 200
    body = res.json()
    release = body["release"]
    assert release["action"] == "terminate"
    assert release["terminated"] is True
    assert release["outcome"]["status"] == "committed"
    assert release["recovered"] is True
    assert release["observed_row"]["value"] == before + 1
    assert release["observed_row"]["value"] == release["outcome"]["value"]
    assert body["drill"]["status"] == "recovered"
    assert _sessions(drill_id) == []
    store.shutdown()


def test_stale_page_after_restart_cannot_touch_new_drill(web):
    client, store = web
    old_id = _start(client)
    old_token = _token(client, old_id)

    # A restart forgets every in-memory drill; leftover sessions (if any) are
    # ended and the page token becomes useless.
    store.reset("simulated restart")
    new_id = _start(client)
    new_token = _token(client, new_id)
    assert new_id != old_id
    new_snap = _state(client, new_id)

    stale = client.post(f"/api/drills/{old_id}/release", data={"token": old_token})
    assert stale.status_code in (403, 410)
    assert stale.json()["ok"] is False

    # An old page's token cannot drive the new drill either.
    wrong = client.post(f"/api/drills/{new_id}/release", data={"token": old_token})
    assert wrong.status_code == 403
    assert wrong.json()["kind"] == "DrillTokenMismatch"

    after = _state(client, new_id)
    assert after["status"] == "blocked"
    assert after["identity"]["holder"]["pid"] == new_snap["identity"]["holder"]["pid"]
    assert (
        after["identity"]["holder"]["backend_start"]
        == new_snap["identity"]["holder"]["backend_start"]
    )
    assert after["identity"]["holder"]["application_name"].endswith(new_id)

    ok = client.post(f"/api/drills/{new_id}/release", data={"token": new_token})
    assert ok.status_code == 200
    assert ok.json()["release"]["recovered"] is True
    store.shutdown()


def test_ended_drill_page_cannot_release_new_session(web):
    client, store = web
    first = _start(client)
    first_token = _token(client, first)
    assert (
        client.post(f"/api/drills/{first}/end", data={"token": first_token}).status_code
        == 200
    )

    second = _start(client)
    blocked = client.post(
        f"/api/drills/{second}/release", data={"token": first_token}
    )
    assert blocked.status_code == 403
    assert _state(client, second)["status"] == "blocked"

    again = client.post(f"/api/drills/{first}/release", data={"token": first_token})
    assert again.status_code == 409
    assert again.json()["kind"] == "DrillNotReleasable"
    store.shutdown()


def test_identity_mismatch_refuses_terminate_and_reports_not_recovered(web):
    """The holder is still alive and still blocking, but its session identity no
    longer matches the one recorded at start: the release must not terminate it,
    and the drill is not reported as recovered while the waiter is still stuck."""
    client, store = web
    drill_id = _start(client)
    snap = _state(client, drill_id)
    drill = store.require(drill_id)

    # Rename the holder session in PostgreSQL itself: pid and blocker edge are
    # unchanged, but application_name no longer matches the drill's identity.
    drill.holder_conn.execute("SET application_name = 'someone-else'")
    time.sleep(0.2)

    res = client.post(
        f"/api/drills/{drill_id}/release", data={"token": _token(client, drill_id)}
    )
    assert res.status_code == 200
    release = res.json()["release"]
    assert release["action"] == "refused"
    assert release["terminated"] is False
    assert release["recovered"] is False
    assert release["outcome"] is None
    assert "未终止任何会话" in release["verification"]
    assert "未恢复" in release["verification"]
    assert [b["pid"] for b in release["remaining_blockers"]] == [
        snap["identity"]["holder"]["pid"]
    ]
    # The holder session we do not own is untouched (it was renamed, so it is
    # looked up by pid).
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        renamed = conn.execute(
            "SELECT application_name, state FROM pg_stat_activity WHERE pid = %s",
            (snap["identity"]["holder"]["pid"],),
        ).fetchone()
    assert renamed is not None
    assert renamed[0] == "someone-else"
    assert renamed[1] == "idle in transaction"
    store.end(drill_id, None, "test cleanup")
    store.shutdown()


def test_successful_terminate_but_waiter_never_finishes_is_not_recovered(web, monkeypatch):
    """Guards against deciding recovery from pg_terminate_backend's return value.

    The terminate succeeds, but no commit result is observed within the window
    (simulated), so the verdict must be "not recovered"."""
    client, store = web
    drill_id = _start(client)
    drill = store.require(drill_id)

    # Simulate a waiter that cannot make progress: no result, event never set.
    with drill.lock:
        drill.waiter_outcome = None
    monkeypatch.setattr(drill.waiter_done, "wait", lambda timeout=None: False)

    res = client.post(
        f"/api/drills/{drill_id}/release", data={"token": _token(client, drill_id)}
    )
    release = res.json()["release"]
    assert release["terminated"] is True
    assert release["outcome"] is None
    assert release["recovered"] is False
    assert "未恢复" in release["verification"]
    assert "COMMIT" not in release["verification"] or "未提交成功" in release["verification"]
    store.end(drill_id, None, "test cleanup")
    store.shutdown()


def test_refused_but_update_had_committed_is_recovered_by_result(web):
    """If the holder is gone by the time release is submitted (session ended
    elsewhere), the blocked UPDATE can still commit. The verdict follows the
    real commit, even though nothing was terminated."""
    client, store = web
    drill_id = _start(client)
    snap = _state(client, drill_id)
    drill = store.require(drill_id)

    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = %s",
            (snap["identity"]["holder"]["pid"],),
        )
    # Wait until the waiter's real commit lands.
    deadline = time.time() + 5
    while drill.waiter_outcome is None and time.time() < deadline:
        time.sleep(0.05)

    # Move the recorded identity so the atomic precondition cannot match.
    with drill.lock:
        drill.holder_backend_start = drill.holder_backend_start.replace(
            year=drill.holder_backend_start.year - 1
        )
    res = client.post(
        f"/api/drills/{drill_id}/release", data={"token": _token(client, drill_id)}
    )
    release = res.json()["release"]
    assert release["action"] == "refused"
    assert release["terminated"] is False
    assert release["outcome"]["status"] == "committed"
    assert release["recovered"] is True
    assert "未终止任何会话" in release["verification"]
    store.shutdown()


def test_release_is_idempotent(web):
    client, store = web
    drill_id = _start(client)
    token = _token(client, drill_id)
    first = client.post(f"/api/drills/{drill_id}/release", data={"token": token})
    second = client.post(f"/api/drills/{drill_id}/release", data={"token": token})
    assert first.status_code == second.status_code == 200
    assert first.json()["release"] == second.json()["release"]
    assert _sessions(drill_id) == []
    store.shutdown()
