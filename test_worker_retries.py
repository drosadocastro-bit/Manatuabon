"""
Test that mast_worker retry logic respects the 3-attempt limit.
Uses an isolated tmp_path DB — no live DB or network required.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


@pytest.fixture()
def mast_db(tmp_path):
    """Create an isolated DB with the mast_queue schema."""
    db_path = tmp_path / "mast_test.db"
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE mast_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_name TEXT NOT NULL,
            queued_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_run TEXT
        );
    """)
    conn.commit()
    conn.close()
    return db_path


# The worker_loop pick-up query (copied from mast_worker.py):
PICK_QUERY = (
    "SELECT * FROM mast_queue "
    "WHERE status='pending' OR (status='failed' AND attempts < 3) "
    "ORDER BY queued_at ASC LIMIT 1"
)


def test_pending_row_is_picked_up(mast_db):
    conn = sqlite3.connect(str(mast_db), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO mast_queue (target_name, queued_at, status, attempts) "
        "VALUES ('Orion', datetime('now'), 'pending', 0)"
    )
    conn.commit()
    row = conn.execute(PICK_QUERY).fetchone()
    conn.close()
    assert row is not None, "pending row should be picked up"
    assert row["target_name"] == "Orion"


def test_failed_row_under_3_attempts_is_retried(mast_db):
    conn = sqlite3.connect(str(mast_db), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO mast_queue (target_name, queued_at, status, attempts) "
        "VALUES ('Orion', datetime('now'), 'failed', 2)"
    )
    conn.commit()
    row = conn.execute(PICK_QUERY).fetchone()
    conn.close()
    assert row is not None, "failed row with attempts < 3 should be retried"


def test_failed_row_at_3_attempts_is_not_retried(mast_db):
    conn = sqlite3.connect(str(mast_db), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO mast_queue (target_name, queued_at, status, attempts) "
        "VALUES ('Orion', datetime('now'), 'failed', 3)"
    )
    conn.commit()
    row = conn.execute(PICK_QUERY).fetchone()
    conn.close()
    assert row is None, "failed row with attempts >= 3 should NOT be retried"


def test_done_row_is_not_picked_up(mast_db):
    conn = sqlite3.connect(str(mast_db), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO mast_queue (target_name, queued_at, status, attempts) "
        "VALUES ('Orion', datetime('now'), 'done', 1)"
    )
    conn.commit()
    row = conn.execute(PICK_QUERY).fetchone()
    conn.close()
    assert row is None, "done row should not be picked up"
