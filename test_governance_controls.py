"""
Tests for governance override controls (MemoryManager.update_hypothesis_status
and build_governance_diagnostics).

Uses an isolated tmp_path DB for MemoryManager tests.
build_governance_diagnostics checks file presence on disk (no DB param),
so it runs against the real workspace — marked @integration.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from manatuabon_agent import MemoryManager

TEST_HYP_ID = "TEST-GOV-OVERRIDE"


@pytest.fixture()
def gov_db(tmp_path):
    """Create an isolated DB with the tables needed for governance tests."""
    from db_init import ensure_runtime_db
    db_path = tmp_path / "gov_test.db"
    conn = ensure_runtime_db(db_path, migrate=False)
    # Seed a hypothesis in 'accepted' state
    conn.execute(
        "INSERT OR REPLACE INTO hypotheses "
        "(id, title, description, status, source, date) "
        "VALUES (?, ?, ?, ?, ?, datetime('now'))",
        (
            TEST_HYP_ID,
            "Governance Override Test",
            "Temporary hypothesis for manual override governance checks.",
            "accepted",
            "test",
        ),
    )
    conn.commit()
    conn.close()
    return db_path


def test_override_accepted_to_held(gov_db):
    memory = MemoryManager(gov_db)
    updated = memory.update_hypothesis_status(
        TEST_HYP_ID,
        "held",
        rationale="Manual downgrade after reviewing unresolved contradictory evidence.",
        actor="test_governance",
    )
    assert updated is not None, "update_hypothesis_status returned None"
    assert updated["previous_status"] == "accepted"


def test_override_summary_contains_latest(gov_db):
    memory = MemoryManager(gov_db)
    memory.update_hypothesis_status(
        TEST_HYP_ID,
        "held",
        rationale="Manual downgrade after reviewing unresolved contradictory evidence.",
        actor="test_governance",
    )
    summary = memory.get_override_summary()
    assert summary["latest"]["hypothesis_id"] == TEST_HYP_ID
    assert "contradictory evidence" in summary["latest"]["rationale"]


def test_governance_diagnostics_flags():
    """Integration test — checks real workspace files. No DB writes."""
    from manatuabon_bridge import build_governance_diagnostics
    diagnostics = build_governance_diagnostics()
    assert diagnostics["override_rationale_required"] is True
    assert diagnostics["charter_present"] is True
    assert diagnostics["decision_policy_present"] is True
    assert diagnostics["change_policy_present"] is True
    assert diagnostics["risk_review_present"] is True