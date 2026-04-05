import json
import sqlite3
from pathlib import Path

from manatuabon_agent import MemoryManager
from manatuabon_bridge import build_governance_diagnostics


DB = Path("d:/Manatuabon/manatuabon.db")
TEST_HYP_ID = "TEST-GOV-OVERRIDE"


def main():
    conn = sqlite3.connect(DB)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO hypotheses (id, title, description, status, source, date) VALUES (?, ?, ?, ?, ?, datetime('now'))",
            (
                TEST_HYP_ID,
                "Governance Override Test",
                "Temporary hypothesis for manual override governance checks.",
                "accepted",
                "test",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    memory = MemoryManager(DB)
    updated = memory.update_hypothesis_status(
        TEST_HYP_ID,
        "held",
        rationale="Manual downgrade after reviewing unresolved contradictory evidence.",
        actor="test_governance",
    )
    summary = memory.get_override_summary()
    diagnostics = build_governance_diagnostics()

    assert updated is not None, updated
    assert updated["previous_status"] == "accepted", updated
    assert summary["latest"]["hypothesis_id"] == TEST_HYP_ID, summary
    assert "contradictory evidence" in summary["latest"]["rationale"], summary
    assert diagnostics["override_rationale_required"] is True, diagnostics
    assert diagnostics["charter_present"] is True, diagnostics
    assert diagnostics["decision_policy_present"] is True, diagnostics
    assert diagnostics["change_policy_present"] is True, diagnostics
    assert diagnostics["risk_review_present"] is True, diagnostics

    conn = sqlite3.connect(DB)
    try:
        conn.execute("DELETE FROM hypothesis_overrides WHERE hypothesis_id=?", (TEST_HYP_ID,))
        conn.execute("DELETE FROM confidence_history WHERE hypothesis_id=?", (TEST_HYP_ID,))
        conn.execute("DELETE FROM hypothesis_decisions WHERE hypothesis_id=?", (TEST_HYP_ID,))
        conn.execute("DELETE FROM hypothesis_reviews WHERE hypothesis_id=?", (TEST_HYP_ID,))
        conn.execute("DELETE FROM hypotheses WHERE id=?", (TEST_HYP_ID,))
        conn.commit()
    finally:
        conn.close()

    print(json.dumps({
        "updated": updated,
        "override_summary": summary,
        "diagnostics": diagnostics,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()