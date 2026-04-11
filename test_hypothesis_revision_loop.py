"""
Tests for hypothesis_revision_loop.py — all offline, no bridge, no external APIs.
"""

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hypothesis_revision_loop import (
    open_db,
    ensure_tracking_table,
    get_needs_revision_candidates,
    get_new_memories,
    get_reflection_guidance,
    get_revision_cycle,
    update_tracking,
    patch_hypothesis_evidence,
    scan_inbox_bundles,
    build_evidence_addendum,
    submit_to_bridge,
    HypothesisRevisionLoop,
    MAX_REVISION_CYCLES,
    COOLDOWN_MINUTES,
)

passed = 0
failed = 0


def check(label: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        print(f"  ✓ {label}")
        passed += 1
    else:
        print(f"  ✗ {label}" + (f" — {detail}" if detail else ""))
        failed += 1


# ── Fixture helpers ────────────────────────────────────────────────────────────

def make_db(tmp: Path):
    """Create a minimal in-memory-style SQLite DB in tmp for testing."""
    import sqlite3
    db_path = tmp / "test.db"
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hypotheses (
            id TEXT PRIMARY KEY, title TEXT, description TEXT,
            evidence TEXT, status TEXT, tags TEXT, source TEXT, date TEXT,
            confidence REAL, created_at TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS hypothesis_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            decision TEXT NOT NULL,
            final_score REAL,
            score_breakdown TEXT,
            merged_with TEXT,
            reasoning TEXT,
            timestamp TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hypothesis_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            verdict TEXT,
            reasoning TEXT,
            objections TEXT,
            score_contributions TEXT,
            review_details TEXT,
            timestamp TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            content TEXT,
            concept_tags TEXT,
            significance INTEGER DEFAULT 1,
            domain_tags TEXT,
            supports_hypothesis TEXT,
            challenges_hypothesis TEXT
        );
    """)
    conn.commit()
    ensure_tracking_table(conn)
    return db_path, conn


def past(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def future(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def seed_hypothesis(conn, hid="H3", decision="needs_revision", decision_minutes_ago=60):
    conn.execute(
        "INSERT OR IGNORE INTO hypotheses (id, title, description, evidence, status) VALUES (?, ?, ?, ?, ?)",
        (hid, f"Test Hypothesis {hid}", f"Desc for {hid}", "Original evidence.", "active")
    )
    conn.execute(
        "INSERT INTO hypothesis_decisions (hypothesis_id, decision, final_score, timestamp) VALUES (?, ?, ?, ?)",
        (hid, decision, 0.62, past(decision_minutes_ago))
    )
    conn.commit()


# ── [1] ensure_tracking_table ──────────────────────────────────────────────────

print("\n[1] ensure_tracking_table")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_db(Path(tmp))
    # Called twice to confirm idempotency
    ensure_tracking_table(conn)
    ensure_tracking_table(conn)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    check("revision_tracking table created", "revision_tracking" in tables)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(revision_tracking)").fetchall()}
    for col in ("hypothesis_id", "revision_cycle", "last_attempted", "last_status", "notes"):
        check(f"column '{col}' exists", col in cols)
    conn.close()


# ── [2] get_needs_revision_candidates ─────────────────────────────────────────

print("\n[2] get_needs_revision_candidates")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_db(Path(tmp))

    # H3: needs_revision, cooldown elapsed (60 min ago) → should appear
    seed_hypothesis(conn, "H3", "needs_revision", decision_minutes_ago=60)
    # H4: accepted → should NOT appear
    seed_hypothesis(conn, "H4", "accepted", decision_minutes_ago=60)
    # H5: needs_revision but cooldown NOT elapsed (5 min ago) → should NOT appear
    seed_hypothesis(conn, "H5", "needs_revision", decision_minutes_ago=5)
    # H6: needs_revision, but MAX cycles already reached
    seed_hypothesis(conn, "H6", "needs_revision", decision_minutes_ago=60)
    conn.execute(
        "INSERT INTO revision_tracking (hypothesis_id, revision_cycle, last_attempted) VALUES (?, ?, ?)",
        ("H6", MAX_REVISION_CYCLES, past(60))
    )
    conn.commit()

    candidates = get_needs_revision_candidates(conn)
    ids = {c["id"] for c in candidates}

    check("H3 (needs_revision, cooldown elapsed) is candidate", "H3" in ids)
    check("H4 (accepted) is NOT a candidate", "H4" not in ids)
    check("H5 (within cooldown window) is NOT a candidate", "H5" not in ids)
    check("H6 (max cycles reached) is NOT a candidate", "H6" not in ids)
    check("candidate has required keys",
          all(k in candidates[0] for k in ("id", "title", "revision_cycle", "last_decision_at"))
          if candidates else False)
    conn.close()


# ── [3] get_new_memories ──────────────────────────────────────────────────────

print("\n[3] get_new_memories")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_db(Path(tmp))
    now_str = datetime.now(timezone.utc).isoformat()
    old_str = past(120)

    # Memory after cutoff, tagged to H3 → should appear
    conn.execute(
        "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
        (now_str, "Sgr A* S2 orbit confirms GR precession.", 5, "H3")
    )
    # Memory before cutoff → should NOT appear
    conn.execute(
        "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
        (old_str, "Old memory that predates last decision.", 5, "H3")
    )
    # Memory for H7, not H3 → should NOT appear
    conn.execute(
        "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
        (now_str, "Unrelated H7 memory.", 5, "H7")
    )
    conn.commit()

    mems = get_new_memories(conn, "H3", since=past(60))
    check("Returns 1 new memory for H3", len(mems) == 1, f"got {len(mems)}")
    check("Memory has expected content", "S2" in (mems[0]["content"] if mems else ""))
    check("Empty result for H7 since recent timestamp",
          len(get_new_memories(conn, "H7", since=future(1))) == 0)
    check("Returns all H3 memories when since=None (epoch)",
          len(get_new_memories(conn, "H3", since=None)) == 2)
    conn.close()


# ── [4] get_reflection_guidance ───────────────────────────────────────────────

print("\n[4] get_reflection_guidance")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_db(Path(tmp))

    details = {
        "concrete_revisions": ["Tighten the Hills mechanism argument", "Cite GRAVITY 2020"],
        "blockers": ["Missing direct Tier A measurement"],
        "evidence_requests": ["Request VLBI proper motion data"],
    }
    conn.execute(
        "INSERT INTO hypothesis_reviews "
        "(hypothesis_id, agent_name, verdict, reasoning, objections, review_details, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("H3", "reflection", "needs_revision", "The argument needs tightening.",
         "Speculative Hills radius", json.dumps(details), datetime.now(timezone.utc).isoformat())
    )
    conn.commit()

    g = get_reflection_guidance(conn, "H3")
    check("concrete_revisions list returned", isinstance(g["concrete_revisions"], list))
    check("2 concrete revisions", len(g["concrete_revisions"]) == 2, f"got {len(g['concrete_revisions'])}")
    check("blockers list returned", isinstance(g["blockers"], list))
    check("evidence_requests returned", len(g["evidence_requests"]) == 1)
    check("reasoning non-empty", len(g["reasoning"]) > 0)
    check("empty dict for unknown hypothesis", get_reflection_guidance(conn, "H999") == {})
    conn.close()


# ── [5] scan_inbox_bundles ────────────────────────────────────────────────────

print("\n[5] scan_inbox_bundles")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    inbox = tmp / "inbox"
    inbox.mkdir()

    # Bundle for H3 → should be found
    bundle_h3 = {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "simulation/orbital_confinement",
        "supports_hypothesis": "H3",
        "summary": "S2 Schwarzschild precession 11.91 arcmin/orbit, consistent with GRAVITY 2020.",
        "significance": 0.85,
        "structured_evidence": {
            "testable_predictions": [
                {"prediction": "GRAVITY instrument should detect S2 precession in next orbit"},
                {"prediction": "Hills radius capture rate measurable with deep IR survey"},
            ]
        }
    }
    (inbox / "simulation_bundle_orbital_confinement_H3_001.json").write_text(json.dumps(bundle_h3))

    # Bundle for H7 → should NOT match H3
    bundle_h7 = {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "simulation/accretion_physics",
        "supports_hypothesis": "H7",
        "summary": "Bondi radius 0.04 arcsec for Sgr A*",
        "significance": 0.7,
        "structured_evidence": {}
    }
    (inbox / "simulation_bundle_accretion_H7_001.json").write_text(json.dumps(bundle_h7))

    # Malformed JSON → should be silently skipped
    (inbox / "simulation_bundle_bad_001.json").write_text("{{BROKEN")

    bundles = scan_inbox_bundles(inbox, "H3")
    check("Finds 1 bundle for H3", len(bundles) == 1, f"found {len(bundles)}")
    check("Bundle has summary", "S2" in (bundles[0]["summary"] if bundles else ""))
    check("Bundle has testable_predictions", len(bundles[0].get("testable_predictions", [])) == 2
          if bundles else False)
    check("H7 bundle NOT included in H3 results", "H7" not in str(bundles))
    check("Empty inbox returns empty list", len(scan_inbox_bundles(tmp / "nonexistent", "H3")) == 0)


# ── [6] build_evidence_addendum ───────────────────────────────────────────────

print("\n[6] build_evidence_addendum")
orig = "S2 orbit data from Gillessen+ 2017."
memories = [
    {"content": "GRAVITY 2020 confirms 12.1 arcmin precession.", "timestamp": "2026-04-10T00:00:00Z"},
    {"content": "Hills mechanism capture at 8.85 AU.", "timestamp": "2026-04-09T00:00:00Z"},
]
sim_bundles = [
    {
        "payload_type": "simulation/orbital_confinement",
        "summary": "r_h = 1.79 pc, precession 11.91 arcmin/orbit",
        "significance": 0.85,
        "testable_predictions": [{"prediction": "Detect S2 in GRAVITY next orbit"}],
    }
]
reflection = {
    "concrete_revisions": ["Cite Schwarzschild precession formula", "Add Hills mechanism calculation"],
    "blockers": [],
}

addendum = build_evidence_addendum(orig, memories, sim_bundles, reflection, revision_cycle=0)

check("Original evidence preserved", orig in addendum)
check("Revision marker present", "[Revision 1" in addendum)
check("Reflection revision included", "Schwarzschild" in addendum)
check("Simulation evidence included", "1.79 pc" in addendum)
check("Memory snippet included", "GRAVITY" in addendum)
check("Addendum is prepended (revision before original)", addendum.index("[Revision") < addendum.index(orig))

# Edge case: no new evidence
bare = build_evidence_addendum(orig, [], [], {}, revision_cycle=2)
check("Bare revision still has marker", "[Revision 3" in bare)
check("Original still present in bare", orig in bare)


# ── [7] update_tracking / get_revision_cycle ──────────────────────────────────

print("\n[7] update_tracking & get_revision_cycle")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_db(Path(tmp))

    check("Initial cycle = 0", get_revision_cycle(conn, "H3") == 0)
    update_tracking(conn, "H3", "submitted", "first pass")
    check("Cycle = 1 after first update", get_revision_cycle(conn, "H3") == 1)
    update_tracking(conn, "H3", "submitted", "second pass")
    check("Cycle = 2 after second update", get_revision_cycle(conn, "H3") == 2)
    update_tracking(conn, "H3", "error", "bridge down")
    check("Cycle = 3 after third update", get_revision_cycle(conn, "H3") == 3)

    row = conn.execute("SELECT * FROM revision_tracking WHERE hypothesis_id = 'H3'").fetchone()
    check("last_status updated to 'error'", row["last_status"] == "error")
    check("last_attempted is recent ISO timestamp", "T" in (row["last_attempted"] or ""))
    conn.close()


# ── [8] patch_hypothesis_evidence ─────────────────────────────────────────────

print("\n[8] patch_hypothesis_evidence")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_db(Path(tmp))
    conn.execute(
        "INSERT INTO hypotheses (id, title, description, evidence, status) VALUES (?, ?, ?, ?, ?)",
        ("H3", "Jailer Hypothesis", "desc", "Old evidence.", "active")
    )
    conn.commit()

    patch_hypothesis_evidence(conn, "H3", "Revised evidence with new data.")
    row = conn.execute("SELECT evidence, updated_at FROM hypotheses WHERE id='H3'").fetchone()
    check("Evidence field updated", "Revised evidence" in (row["evidence"] or ""))
    check("updated_at set", row["updated_at"] is not None and "T" in row["updated_at"])
    conn.close()


# ── [9] submit_to_bridge (unreachable) ────────────────────────────────────────

print("\n[9] submit_to_bridge (bridge unreachable)")
# Use timeout=2 so the test doesn't hang 30 seconds on a refused connection
result = submit_to_bridge("H3", bridge_url="http://127.0.0.1:19999/api/council/reprocess", timeout=2)
check("Returns error dict when bridge unreachable", "error" in result)
check("Error message is non-empty string", isinstance(result["error"], str) and len(result["error"]) > 0)


# ── [10] HypothesisRevisionLoop.run_once (dry_run) ────────────────────────────

print("\n[10] HypothesisRevisionLoop.run_once (dry_run=True)")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    db_path, conn = make_db(tmp)

    # Seed a qualifying hypothesis
    seed_hypothesis(conn, "H3", "needs_revision", decision_minutes_ago=90)

    # Seed a simulation bundle for H3 in inbox
    inbox = tmp / "inbox"
    inbox.mkdir()
    bundle = {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "simulation/orbital_confinement",
        "supports_hypothesis": "H3",
        "summary": "Orbital confinement simulation for Jailer Hypothesis.",
        "significance": 0.87,
        "structured_evidence": {"testable_predictions": []}
    }
    (inbox / "simulation_bundle_orbital_H3_001.json").write_text(json.dumps(bundle))

    # Seed a reflection review for H3
    reflection_details = {
        "concrete_revisions": ["Add quantitative tidal disruption radius"],
        "blockers": [],
        "evidence_requests": [],
    }
    conn.execute(
        "INSERT INTO hypothesis_reviews "
        "(hypothesis_id, agent_name, verdict, reasoning, objections, review_details, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("H3", "reflection", "needs_revision", "Needs tighter numbers.", "",
         json.dumps(reflection_details), datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()

    worker = HypothesisRevisionLoop(
        db_path=db_path,
        inbox_path=inbox,
        dry_run=True,
    )
    results = worker.run_once()

    check("run_once returns 1 result", len(results) == 1, f"got {len(results)}")
    if results:
        r = results[0]
        check("Result hyp_id = H3", r["hyp_id"] == "H3")
        check("Result status = dry_run", r["status"] == "dry_run")
        check("Cycle advanced to 1", r["cycle"] == 1)
        check("sim_bundles = 1", r["sim_bundles"] == 1, f"got {r['sim_bundles']}")
        check("bridge_result has dry_run flag", r["bridge_result"].get("dry_run") is True)


# ── [11] HypothesisRevisionLoop.run_once (no candidates) ─────────────────────

print("\n[11] HypothesisRevisionLoop.run_once (no eligible candidates)")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    db_path, conn = make_db(tmp)

    # Only 'accepted' hypotheses — none eligible
    seed_hypothesis(conn, "H10", "accepted", decision_minutes_ago=60)
    conn.close()

    worker = HypothesisRevisionLoop(db_path=db_path, dry_run=True)
    results = worker.run_once()
    check("Empty results when no candidates", len(results) == 0)


# ── [12] Cooldown enforcement ─────────────────────────────────────────────────

print("\n[12] Cooldown enforcement")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    db_path, conn = make_db(tmp)

    seed_hypothesis(conn, "H3", "needs_revision", decision_minutes_ago=60)
    # Mark H3 as already attempted very recently
    conn.execute(
        "INSERT INTO revision_tracking (hypothesis_id, revision_cycle, last_attempted) VALUES (?, ?, ?)",
        ("H3", 1, datetime.now(timezone.utc).isoformat())   # just now
    )
    conn.commit()
    conn.close()

    worker = HypothesisRevisionLoop(db_path=db_path, dry_run=True)
    results = worker.run_once()
    check("H3 not processed due to cooldown", len(results) == 0, f"got {len(results)}")


# ── Summary ───────────────────────────────────────────────────────────────────

total = passed + failed
print(f"\n{'='*60}")
print(f"  {passed}/{total} passed  |  {failed} failed")
print(f"{'='*60}\n")
if failed:
    sys.exit(1)
