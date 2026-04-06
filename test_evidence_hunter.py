"""Tests for the Evidence Hunter — active evidence-seeking loop."""

import json
import sqlite3
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from evidence_hunter import EvidenceHunter, STALE_DAYS


# ── Fake MemoryManager ────────────────────────────────────────────────

class FakeAgentLog:
    def __init__(self):
        self.events = []

    def add(self, event_type, message, data=None):
        self.events.append((event_type, message, data))


class FakeMemory:
    """Minimal MemoryManager stub with an in-memory SQLite DB."""

    def __init__(self):
        self._db = sqlite3.connect(":memory:")
        self._db.row_factory = sqlite3.Row
        self._init_tables()
        self._next_mem_id = 1

    def _init_tables(self):
        self._db.executescript("""
            CREATE TABLE memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, content TEXT, concept_tags TEXT,
                significance REAL, domain_tags TEXT,
                supports_hypothesis TEXT, challenges_hypothesis TEXT
            );
            CREATE TABLE hypotheses (
                id TEXT PRIMARY KEY, title TEXT, description TEXT,
                status TEXT DEFAULT 'active', confidence REAL,
                evidence TEXT, domain_tags TEXT, origin TEXT
            );
            CREATE TABLE hypothesis_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hypothesis_id TEXT, decision TEXT, final_score REAL,
                reasoning TEXT, timestamp TEXT
            );
            CREATE TABLE evidence_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hypothesis_id TEXT, request_text TEXT,
                priority TEXT DEFAULT 'medium', source_agent TEXT,
                source_context TEXT, status TEXT DEFAULT 'pending',
                triggering_decision TEXT, created_at TEXT, updated_at TEXT,
                resolved_at TEXT, resolution_note TEXT,
                satisfied_by_memory_ids TEXT
            );
        """)

    def _get_conn(self):
        return self._db

    def get_evidence_requests(self, status="pending", hypothesis_id=None, limit=100):
        query = "SELECT er.*, h.title AS hypothesis_title, h.status AS hypothesis_status FROM evidence_requests er LEFT JOIN hypotheses h ON h.id = er.hypothesis_id"
        clauses, params = [], []
        if status and status != "all":
            clauses.append("er.status=?")
            params.append(status)
        if hypothesis_id:
            clauses.append("er.hypothesis_id=?")
            params.append(hypothesis_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY er.id LIMIT ?"
        params.append(limit)
        rows = self._db.execute(query, params).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d["ready_for_rereview"] = False
            d["material_evidence"] = []
            results.append(d)
        return results

    def review_evidence_request(self, request_id, decision, resolution_note="", satisfied_memory_ids=None):
        status = "completed" if decision == "complete" else "dismissed"
        ts = datetime.now().isoformat()
        self._db.execute(
            "UPDATE evidence_requests SET status=?, resolved_at=?, resolution_note=?, satisfied_by_memory_ids=?, updated_at=? WHERE id=?",
            (status, ts, resolution_note, json.dumps(satisfied_memory_ids or []), ts, request_id),
        )
        self._db.commit()
        return {"id": request_id, "status": status}

    def add_memory(self, memory):
        ts = memory.get("timestamp", datetime.now().isoformat())
        cur = self._db.execute(
            "INSERT INTO memories (timestamp, content, concept_tags, significance, domain_tags, supports_hypothesis, challenges_hypothesis) VALUES (?,?,?,?,?,?,?)",
            (ts, memory.get("summary", ""), json.dumps(memory.get("entities", [])), memory.get("confidence", 0.5), json.dumps(memory.get("domain_tags", [])), memory.get("supports_hypothesis"), memory.get("challenges_hypothesis")),
        )
        self._db.commit()
        return cur.lastrowid

    # ── test helpers ──

    def add_held_hypothesis(self, hyp_id, title="Test Hypothesis", days_ago=0):
        self._db.execute(
            "INSERT INTO hypotheses (id, title, description, status, confidence, domain_tags) VALUES (?,?,?,?,?,?)",
            (hyp_id, title, f"Description of {title}", "held", 0.45, '["sgra"]'),
        )
        ts = (datetime.now() - timedelta(days=days_ago)).isoformat()
        self._db.execute(
            "INSERT INTO hypothesis_decisions (hypothesis_id, decision, final_score, reasoning, timestamp) VALUES (?,?,?,?,?)",
            (hyp_id, "held", 0.42, "Evidence too weak", ts),
        )
        self._db.commit()

    def add_pending_request(self, hyp_id, text, priority="high"):
        ts = datetime.now().isoformat()
        cur = self._db.execute(
            "INSERT INTO evidence_requests (hypothesis_id, request_text, priority, source_agent, source_context, status, triggering_decision, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (hyp_id, text, priority, "evidence_reviewer", "{}", "pending", "held", ts, ts),
        )
        self._db.commit()
        return cur.lastrowid

    def add_unlinked_memory(self, content, significance=0.8):
        ts = datetime.now().isoformat()
        cur = self._db.execute(
            "INSERT INTO memories (timestamp, content, concept_tags, significance, domain_tags) VALUES (?,?,?,?,?)",
            (ts, content, "[]", significance, "[]"),
        )
        self._db.commit()
        return cur.lastrowid


# ── Tests ─────────────────────────────────────────────────────────────

class TestEvidenceHunterNoActionable(unittest.TestCase):
    """When there are no pending requests for held hypotheses, do nothing."""

    def test_empty_cycle(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()
        self.assertEqual(result["requests_processed"], 0)
        self.assertEqual(result["requests_satisfied"], 0)


class TestInternalMemorySearch(unittest.TestCase):
    """Evidence Hunter finds matching memories and satisfies requests."""

    def test_finds_tier_a_memory(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-TEST-1", "Sgr A* accretion anomaly")
        req_id = mem.add_pending_request("H-TEST-1", "Provide observed X-ray flux data from MAST catalog")
        # Add an unlinked memory with Tier A content
        mem.add_unlinked_memory(
            "JWST observed X-ray flux near Sgr A* measured at 3.2e-12 erg/s/cm2 from MAST catalog dataset #4521"
        )
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        self.assertEqual(result["requests_processed"], 1)
        self.assertEqual(result["requests_satisfied"], 1)
        self.assertGreater(result["memory_matches"], 0)

        # Verify the request was marked completed
        reqs = mem.get_evidence_requests(status="completed", hypothesis_id="H-TEST-1")
        self.assertEqual(len(reqs), 1)
        self.assertIn("Auto-satisfied", reqs[0]["resolution_note"])

    def test_skips_tier_c_memory(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-TEST-2", "Speculative claim")
        mem.add_pending_request("H-TEST-2", "Provide direct observation data")
        # Only Tier C (speculative) memory available
        mem.add_unlinked_memory(
            "This could perhaps be an analogy for extrapolated conjecture about the hypothetical scenario"
        )
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        self.assertEqual(result["requests_processed"], 1)
        self.assertEqual(result["requests_satisfied"], 0)

    def test_does_not_relink_already_linked_memory(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-TEST-3", "Linked test")
        mem.add_pending_request("H-TEST-3", "Provide JWST observed flux measurement")
        # Add a memory already linked to another hypothesis
        ts = datetime.now().isoformat()
        mem._db.execute(
            "INSERT INTO memories (timestamp, content, concept_tags, significance, domain_tags, supports_hypothesis) VALUES (?,?,?,?,?,?)",
            (ts, "JWST observed flux measurement at 1.5e-11", "[]", 0.9, "[]", "H-OTHER"),
        )
        mem._db.commit()

        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        # Should not find it because the memory is already linked
        self.assertEqual(result["requests_satisfied"], 0)


class TestExternalAPISearch(unittest.TestCase):
    """Evidence Hunter falls back to external APIs when internal search fails."""

    @patch("evidence_hunter.EvidenceHunter._query_arxiv")
    def test_calls_arxiv_when_no_internal_match(self, mock_arxiv):
        mock_arxiv.return_value = [{
            "source": "arxiv:2026.12345",
            "content": "[arXiv:2026.12345] Observed X-ray flare from Sgr A* measured via Chandra at 4.2e-12 erg/s/cm2",
        }]

        mem = FakeMemory()
        mem.add_held_hypothesis("H-TEST-4", "Sgr A* X-ray flare", days_ago=2)
        mem.add_pending_request("H-TEST-4", "Provide observed X-ray flux measurement from Chandra")
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        self.assertTrue(mock_arxiv.called)
        self.assertEqual(result["requests_satisfied"], 1)
        self.assertGreater(result["external_fetches"], 0)

        # Verify new memory was ingested
        rows = mem._db.execute("SELECT * FROM memories WHERE supports_hypothesis='H-TEST-4'").fetchall()
        self.assertGreater(len(rows), 0)

    @patch("evidence_hunter.EvidenceHunter._query_arxiv")
    def test_graceful_api_failure(self, mock_arxiv):
        mock_arxiv.side_effect = Exception("Network timeout")

        mem = FakeMemory()
        mem.add_held_hypothesis("H-TEST-5", "Network failure test")
        mem.add_pending_request("H-TEST-5", "Provide any observational evidence")
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        # Should not crash — just log the error and continue
        self.assertEqual(result["requests_processed"], 1)
        self.assertEqual(result["requests_satisfied"], 0)


class TestStalenessCheck(unittest.TestCase):
    """Auto-reject held hypotheses after STALE_DAYS without satisfied evidence."""

    def test_auto_rejects_stale_hypothesis(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-STALE-1", "Stale hypothesis", days_ago=STALE_DAYS + 1)
        mem.add_pending_request("H-STALE-1", "Provide any Tier A evidence")
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        self.assertEqual(result["hypotheses_auto_rejected"], 1)

        # Verify hypothesis status changed
        row = mem._db.execute("SELECT status FROM hypotheses WHERE id='H-STALE-1'").fetchone()
        self.assertEqual(row["status"], "rejected_insufficient_evidence")

        # Verify decision was recorded
        dec = mem._db.execute(
            "SELECT decision FROM hypothesis_decisions WHERE hypothesis_id='H-STALE-1' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(dec["decision"], "rejected_insufficient_evidence")

        # Verify pending requests were dismissed
        reqs = mem.get_evidence_requests(status="dismissed", hypothesis_id="H-STALE-1")
        self.assertEqual(len(reqs), 1)

    def test_does_not_reject_fresh_hypothesis(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-FRESH-1", "Fresh hypothesis", days_ago=2)
        mem.add_pending_request("H-FRESH-1", "Provide evidence")
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        self.assertEqual(result["hypotheses_auto_rejected"], 0)
        row = mem._db.execute("SELECT status FROM hypotheses WHERE id='H-FRESH-1'").fetchone()
        self.assertEqual(row["status"], "held")

    def test_does_not_reject_if_some_requests_satisfied(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-PARTIAL-1", "Partial evidence", days_ago=STALE_DAYS + 1)
        mem.add_pending_request("H-PARTIAL-1", "First request")
        req2_id = mem.add_pending_request("H-PARTIAL-1", "Second request")
        # Satisfy one request manually
        mem.review_evidence_request(req2_id, "complete", "Manual review", [])
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        result = hunter.hunt()

        # Should NOT reject because one request was completed
        self.assertEqual(result["hypotheses_auto_rejected"], 0)


class TestDomainRouting(unittest.TestCase):
    """Domain detection routes to correct external APIs."""

    def test_sgra_routes_to_arxiv_mast_transient(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        apis = set()
        from evidence_hunter import DOMAIN_API_MAP
        for api in DOMAIN_API_MAP.get("sgra", []):
            apis.add(api)
        self.assertIn("arxiv", apis)
        self.assertIn("mast", apis)
        self.assertIn("transient", apis)

    def test_cosmology_routes_to_arxiv_sdss(self):
        from evidence_hunter import DOMAIN_API_MAP
        apis = set(DOMAIN_API_MAP.get("cosmology", []))
        self.assertIn("arxiv", apis)
        self.assertIn("sdss", apis)
        self.assertNotIn("mast", apis)


class TestTargetExtraction(unittest.TestCase):
    """Astronomical target extraction from hypothesis text."""

    def test_extracts_sgr_a(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        self.assertEqual(hunter._extract_astro_target("Sgr A* accretion anomaly"), "Sgr A*")

    def test_extracts_ngc(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        self.assertEqual(hunter._extract_astro_target("Emission from NGC 4151"), "NGC 4151")

    def test_returns_none_for_generic_text(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        self.assertIsNone(hunter._extract_astro_target("abstract hypothesis about physics"))


class TestCoordinateExtraction(unittest.TestCase):
    """Known coordinates for named targets."""

    def test_sgr_a_coordinates(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        coords = hunter._extract_coordinates("Studies near Sgr A* galactic center")
        self.assertIsNotNone(coords)
        self.assertAlmostEqual(coords["ra"], 266.417, places=1)

    def test_m87_coordinates(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        coords = hunter._extract_coordinates("Jet dynamics of M87")
        self.assertIsNotNone(coords)
        self.assertAlmostEqual(coords["ra"], 187.706, places=1)


class TestAuditTrail(unittest.TestCase):
    """All actions are logged to AgentLog."""

    def test_hunt_logs_start_and_complete(self):
        mem = FakeMemory()
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        hunter.hunt()
        event_types = [e[0] for e in log.events]
        self.assertIn("evidence_hunt_start", event_types)
        self.assertIn("evidence_hunt_complete", event_types)

    def test_satisfied_request_is_logged(self):
        mem = FakeMemory()
        mem.add_held_hypothesis("H-LOG-1", "Sgr A* logging test")
        mem.add_pending_request("H-LOG-1", "Provide JWST observed flux data from catalog")
        mem.add_unlinked_memory(
            "JWST detected flux measurement from catalog survey at 2.1e-12 erg/s/cm2"
        )
        log = FakeAgentLog()
        hunter = EvidenceHunter(mem, log)
        hunter.hunt()
        event_types = [e[0] for e in log.events]
        self.assertIn("evidence_hunt_satisfied", event_types)


if __name__ == "__main__":
    unittest.main()
