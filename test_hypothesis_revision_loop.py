"""
Tests for hypothesis_revision_loop.py — all offline, no bridge, no external APIs.
"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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


# -- Fixtures ------------------------------------------------------------------


def _past(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _future(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


@pytest.fixture()
def rev_db(tmp_path):
    """Create a minimal SQLite DB for revision-loop testing."""
    db_path = tmp_path / "test.db"
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
    conn.close()
    return db_path


def _seed_hypothesis(db_path, hid="H3", decision="needs_revision", decision_minutes_ago=60):
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT OR IGNORE INTO hypotheses (id, title, description, evidence, status) VALUES (?, ?, ?, ?, ?)",
        (hid, f"Test Hypothesis {hid}", f"Desc for {hid}", "Original evidence.", "active"),
    )
    conn.execute(
        "INSERT INTO hypothesis_decisions (hypothesis_id, decision, final_score, timestamp) VALUES (?, ?, ?, ?)",
        (hid, decision, 0.62, _past(decision_minutes_ago)),
    )
    conn.commit()
    conn.close()


def _open(db_path):
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# -- [1] ensure_tracking_table ------------------------------------------------


class TestEnsureTrackingTable:
    def test_table_created(self, rev_db):
        conn = _open(rev_db)
        ensure_tracking_table(conn)  # idempotent
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "revision_tracking" in tables
        conn.close()

    def test_required_columns_exist(self, rev_db):
        conn = _open(rev_db)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(revision_tracking)").fetchall()}
        for col in ("hypothesis_id", "revision_cycle", "last_attempted", "last_status", "notes"):
            assert col in cols, f"missing column: {col}"
        conn.close()


# -- [2] get_needs_revision_candidates ----------------------------------------


class TestGetNeedsRevisionCandidates:
    def test_needs_revision_cooldown_elapsed(self, rev_db):
        _seed_hypothesis(rev_db, "H3", "needs_revision", decision_minutes_ago=60)
        conn = _open(rev_db)
        ids = {c["id"] for c in get_needs_revision_candidates(conn)}
        assert "H3" in ids
        conn.close()

    def test_accepted_not_candidate(self, rev_db):
        _seed_hypothesis(rev_db, "H4", "accepted", decision_minutes_ago=60)
        conn = _open(rev_db)
        ids = {c["id"] for c in get_needs_revision_candidates(conn)}
        assert "H4" not in ids
        conn.close()

    def test_within_cooldown_not_candidate(self, rev_db):
        _seed_hypothesis(rev_db, "H5", "needs_revision", decision_minutes_ago=5)
        conn = _open(rev_db)
        ids = {c["id"] for c in get_needs_revision_candidates(conn)}
        assert "H5" not in ids
        conn.close()

    def test_max_cycles_reached_not_candidate(self, rev_db):
        _seed_hypothesis(rev_db, "H6", "needs_revision", decision_minutes_ago=60)
        conn = _open(rev_db)
        conn.execute(
            "INSERT INTO revision_tracking (hypothesis_id, revision_cycle, last_attempted) VALUES (?, ?, ?)",
            ("H6", MAX_REVISION_CYCLES, _past(60)),
        )
        conn.commit()
        ids = {c["id"] for c in get_needs_revision_candidates(conn)}
        assert "H6" not in ids
        conn.close()

    def test_candidate_has_required_keys(self, rev_db):
        _seed_hypothesis(rev_db, "H3", "needs_revision", decision_minutes_ago=60)
        conn = _open(rev_db)
        candidates = get_needs_revision_candidates(conn)
        assert candidates
        assert all(k in candidates[0] for k in ("id", "title", "revision_cycle", "last_decision_at"))
        conn.close()


# -- [3] get_new_memories ------------------------------------------------------


class TestGetNewMemories:
    def test_returns_recent_memory_for_hypothesis(self, rev_db):
        conn = _open(rev_db)
        now_str = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
            (now_str, "Sgr A* S2 orbit confirms GR precession.", 5, "H3"),
        )
        conn.commit()
        mems = get_new_memories(conn, "H3", since=_past(60))
        assert len(mems) == 1
        assert "S2" in mems[0]["content"]
        conn.close()

    def test_old_memory_excluded(self, rev_db):
        conn = _open(rev_db)
        conn.execute(
            "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
            (_past(120), "Old memory.", 5, "H3"),
        )
        conn.commit()
        mems = get_new_memories(conn, "H3", since=_past(60))
        assert len(mems) == 0
        conn.close()

    def test_different_hypothesis_excluded(self, rev_db):
        conn = _open(rev_db)
        now_str = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
            (now_str, "Unrelated H7 memory.", 5, "H7"),
        )
        conn.commit()
        mems = get_new_memories(conn, "H3", since=_past(60))
        assert len(mems) == 0
        conn.close()

    def test_since_none_returns_all(self, rev_db):
        conn = _open(rev_db)
        for ts in [datetime.now(timezone.utc).isoformat(), _past(120)]:
            conn.execute(
                "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?, ?, ?, ?)",
                (ts, f"Mem at {ts}", 5, "H3"),
            )
        conn.commit()
        mems = get_new_memories(conn, "H3", since=None)
        assert len(mems) == 2
        conn.close()


# -- [4] get_reflection_guidance -----------------------------------------------


class TestGetReflectionGuidance:
    def _seed_reflection(self, db_path):
        conn = _open(db_path)
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
             "Speculative Hills radius", json.dumps(details), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()

    def test_concrete_revisions_returned(self, rev_db):
        self._seed_reflection(rev_db)
        conn = _open(rev_db)
        g = get_reflection_guidance(conn, "H3")
        assert isinstance(g["concrete_revisions"], list)
        assert len(g["concrete_revisions"]) == 2
        conn.close()

    def test_blockers_and_evidence_requests(self, rev_db):
        self._seed_reflection(rev_db)
        conn = _open(rev_db)
        g = get_reflection_guidance(conn, "H3")
        assert isinstance(g["blockers"], list)
        assert len(g["evidence_requests"]) == 1
        assert len(g["reasoning"]) > 0
        conn.close()

    def test_unknown_hypothesis_returns_empty(self, rev_db):
        conn = _open(rev_db)
        assert get_reflection_guidance(conn, "H999") == {}
        conn.close()


# -- [5] scan_inbox_bundles ----------------------------------------------------


class TestScanInboxBundles:
    def test_finds_matching_bundle(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        bundle_h3 = {
            "manatuabon_schema": "structured_ingest_v1",
            "payload_type": "simulation/orbital_confinement",
            "supports_hypothesis": "H3",
            "summary": "S2 Schwarzschild precession 11.91 arcmin/orbit.",
            "significance": 0.85,
            "structured_evidence": {
                "testable_predictions": [
                    {"prediction": "GRAVITY should detect S2 precession"},
                    {"prediction": "Hills radius capture rate measurable"},
                ]
            },
        }
        (inbox / "simulation_bundle_orbital_confinement_H3_001.json").write_text(
            json.dumps(bundle_h3), encoding="utf-8"
        )
        bundles = scan_inbox_bundles(inbox, "H3")
        assert len(bundles) == 1
        assert "S2" in bundles[0]["summary"]
        assert len(bundles[0].get("testable_predictions", [])) == 2

    def test_other_hypothesis_excluded(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        bundle_h7 = {
            "manatuabon_schema": "structured_ingest_v1",
            "supports_hypothesis": "H7",
            "summary": "unrelated",
            "significance": 0.7,
            "structured_evidence": {},
        }
        (inbox / "sim_h7.json").write_text(json.dumps(bundle_h7), encoding="utf-8")
        bundles = scan_inbox_bundles(inbox, "H3")
        assert len(bundles) == 0

    def test_malformed_json_skipped(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        (inbox / "bad.json").write_text("{{BROKEN", encoding="utf-8")
        assert scan_inbox_bundles(inbox, "H3") == []

    def test_nonexistent_inbox_returns_empty(self, tmp_path):
        assert scan_inbox_bundles(tmp_path / "nonexistent", "H3") == []


# -- [6] build_evidence_addendum -----------------------------------------------


class TestBuildEvidenceAddendum:
    def setup_method(self):
        self.orig = "S2 orbit data from Gillessen+ 2017."
        self.memories = [
            {"content": "GRAVITY 2020 confirms 12.1 arcmin precession.", "timestamp": "2026-04-10T00:00:00Z"},
            {"content": "Hills mechanism capture at 8.85 AU.", "timestamp": "2026-04-09T00:00:00Z"},
        ]
        self.sim_bundles = [
            {
                "payload_type": "simulation/orbital_confinement",
                "summary": "r_h = 1.79 pc, precession 11.91 arcmin/orbit",
                "significance": 0.85,
                "testable_predictions": [{"prediction": "Detect S2 in GRAVITY next orbit"}],
            }
        ]
        self.reflection = {
            "concrete_revisions": ["Cite Schwarzschild precession formula", "Add Hills mechanism calculation"],
            "blockers": [],
        }

    def test_original_evidence_preserved(self):
        a = build_evidence_addendum(self.orig, self.memories, self.sim_bundles, self.reflection, 0)
        assert self.orig in a

    def test_revision_marker_present(self):
        a = build_evidence_addendum(self.orig, self.memories, self.sim_bundles, self.reflection, 0)
        assert "[Revision 1" in a

    def test_reflection_revision_included(self):
        a = build_evidence_addendum(self.orig, self.memories, self.sim_bundles, self.reflection, 0)
        assert "Schwarzschild" in a

    def test_simulation_evidence_included(self):
        a = build_evidence_addendum(self.orig, self.memories, self.sim_bundles, self.reflection, 0)
        assert "1.79 pc" in a

    def test_memory_snippet_included(self):
        a = build_evidence_addendum(self.orig, self.memories, self.sim_bundles, self.reflection, 0)
        assert "GRAVITY" in a

    def test_revision_before_original(self):
        a = build_evidence_addendum(self.orig, self.memories, self.sim_bundles, self.reflection, 0)
        assert a.index("[Revision") < a.index(self.orig)

    def test_bare_revision_with_no_evidence(self):
        bare = build_evidence_addendum(self.orig, [], [], {}, revision_cycle=2)
        assert "[Revision 3" in bare
        assert self.orig in bare


# -- [7] update_tracking / get_revision_cycle ----------------------------------


class TestUpdateTracking:
    def test_initial_cycle_is_zero(self, rev_db):
        conn = _open(rev_db)
        assert get_revision_cycle(conn, "H3") == 0
        conn.close()

    def test_cycle_increments(self, rev_db):
        conn = _open(rev_db)
        update_tracking(conn, "H3", "submitted", "first pass")
        assert get_revision_cycle(conn, "H3") == 1
        update_tracking(conn, "H3", "submitted", "second pass")
        assert get_revision_cycle(conn, "H3") == 2
        conn.close()

    def test_last_status_updated(self, rev_db):
        conn = _open(rev_db)
        update_tracking(conn, "H3", "submitted", "first")
        update_tracking(conn, "H3", "submitted", "second")
        update_tracking(conn, "H3", "error", "bridge down")
        row = conn.execute("SELECT * FROM revision_tracking WHERE hypothesis_id = 'H3'").fetchone()
        assert row["last_status"] == "error"
        assert "T" in (row["last_attempted"] or "")
        conn.close()


# -- [8] patch_hypothesis_evidence ---------------------------------------------


class TestPatchHypothesisEvidence:
    def test_evidence_field_updated(self, rev_db):
        conn = _open(rev_db)
        conn.execute(
            "INSERT INTO hypotheses (id, title, description, evidence, status) VALUES (?, ?, ?, ?, ?)",
            ("H3", "Jailer Hypothesis", "desc", "Old evidence.", "active"),
        )
        conn.commit()
        patch_hypothesis_evidence(conn, "H3", "Revised evidence with new data.")
        row = conn.execute("SELECT evidence, updated_at FROM hypotheses WHERE id='H3'").fetchone()
        assert "Revised evidence" in row["evidence"]
        assert row["updated_at"] is not None and "T" in row["updated_at"]
        conn.close()


# -- [9] submit_to_bridge (unreachable) ----------------------------------------


class TestSubmitToBridge:
    def test_returns_error_when_bridge_unreachable(self):
        result = submit_to_bridge("H3", bridge_url="http://127.0.0.1:19999/api/council/reprocess", timeout=2)
        assert "error" in result
        assert isinstance(result["error"], str) and len(result["error"]) > 0


# -- [10] HypothesisRevisionLoop.run_once (dry_run) ----------------------------


class TestRunOnceDryRun:
    def test_processes_qualifying_hypothesis(self, rev_db, tmp_path):
        _seed_hypothesis(rev_db, "H3", "needs_revision", decision_minutes_ago=90)

        inbox = tmp_path / "inbox"
        inbox.mkdir()
        bundle = {
            "manatuabon_schema": "structured_ingest_v1",
            "payload_type": "simulation/orbital_confinement",
            "supports_hypothesis": "H3",
            "summary": "Orbital confinement simulation for Jailer Hypothesis.",
            "significance": 0.87,
            "structured_evidence": {"testable_predictions": []},
        }
        (inbox / "simulation_bundle_orbital_H3_001.json").write_text(
            json.dumps(bundle), encoding="utf-8"
        )

        conn = _open(rev_db)
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
             json.dumps(reflection_details), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()

        worker = HypothesisRevisionLoop(db_path=rev_db, inbox_path=inbox, dry_run=True)
        results = worker.run_once()

        assert len(results) == 1, f"got {len(results)}"
        r = results[0]
        assert r["hyp_id"] == "H3"
        assert r["status"] == "dry_run"
        assert r["cycle"] == 1
        assert r["sim_bundles"] == 1
        assert r["bridge_result"].get("dry_run") is True


# -- [11] run_once with no candidates -----------------------------------------


class TestRunOnceNoCandidates:
    def test_empty_results_when_no_eligible(self, rev_db):
        _seed_hypothesis(rev_db, "H10", "accepted", decision_minutes_ago=60)
        worker = HypothesisRevisionLoop(db_path=rev_db, dry_run=True)
        assert worker.run_once() == []


# -- [12] Cooldown enforcement ------------------------------------------------


class TestCooldownEnforcement:
    def test_recently_attempted_hypothesis_skipped(self, rev_db):
        _seed_hypothesis(rev_db, "H3", "needs_revision", decision_minutes_ago=60)
        conn = _open(rev_db)
        conn.execute(
            "INSERT INTO revision_tracking (hypothesis_id, revision_cycle, last_attempted) VALUES (?, ?, ?)",
            ("H3", 1, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()

        worker = HypothesisRevisionLoop(db_path=rev_db, dry_run=True)
        assert worker.run_once() == []
