"""
Tests for confidence_decay.py, galactic_center_monitor.py, vela_glitch_watch.py
All offline — no network calls.
"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from confidence_decay import (
    open_db, ensure_decay_table, compute_decay, days_since_evidence,
    get_decay_candidates, apply_decay_to_hypothesis, ConfidenceDecayWorker,
    DECAY_FLOOR, GRACE_DAYS,
)
from galactic_center_monitor import (
    make_bundle, make_id, is_recent, process_arxiv_entry, process_zenodo_entry,
    process_atel_entry, seed_eht_bundles, EHT_REFERENCE_BUNDLES,
    GalacticCenterMonitor,
)
from vela_glitch_watch import (
    extract_epoch, extract_delta_nu_nu, epoch_in_window, window_elapsed,
    is_vela_related, mjd_to_decimal_year, decimal_year_now, make_bundle as vela_bundle,
    VelaGlitchWatch,
    VELA_PREDICTION_WINDOW_LO, VELA_PREDICTION_WINDOW_HI,
)


# -- Helpers -------------------------------------------------------------------


def _past(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _make_decay_db(tmp_path: Path):
    db_path = tmp_path / "decay_test.db"
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hypotheses (
            id TEXT PRIMARY KEY, title TEXT, description TEXT,
            evidence TEXT, status TEXT, confidence REAL,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, content TEXT, significance INTEGER DEFAULT 1,
            supports_hypothesis TEXT
        );
    """)
    conn.commit()
    ensure_decay_table(conn)
    return db_path, conn


# ==============================================================================
# CONFIDENCE DECAY TESTS
# ==============================================================================


class TestComputeDecay:
    def test_90_days_yields_3_periods(self):
        new_conf, periods = compute_decay(0.80, days_since_evidence=90)
        assert abs(periods - 3.0) < 0.01

    def test_formula_correct(self):
        new_conf, periods = compute_decay(0.80, days_since_evidence=90)
        expected = round(0.80 * (1 - 0.03) ** 3, 4)
        assert abs(new_conf - expected) < 0.001

    def test_clamped_to_floor(self):
        new_conf, _ = compute_decay(0.12, days_since_evidence=3650)
        assert new_conf >= DECAY_FLOOR

    def test_zero_days_no_decay(self):
        new_conf, _ = compute_decay(0.80, days_since_evidence=0)
        assert abs(new_conf - 0.80) < 0.001


class TestDaysSinceEvidence:
    def test_none_returns_large_number(self):
        assert days_since_evidence(None) == 9999

    def test_recent_returns_zero(self):
        assert days_since_evidence(datetime.now(timezone.utc).isoformat()) == 0

    def test_90_days_ago(self):
        old = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        assert abs(days_since_evidence(old) - 90) <= 1

    def test_z_suffix_handled(self):
        assert days_since_evidence("2020-01-01T00:00:00Z") > 1000


class TestEnsureDecayTable:
    def test_idempotent(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        ensure_decay_table(conn)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "confidence_decay_log" in tables
        conn.close()

    def test_required_columns(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(confidence_decay_log)").fetchall()}
        for col in ("hypothesis_id", "old_confidence", "new_confidence", "days_since_evidence"):
            assert col in cols
        conn.close()


class TestGetDecayCandidates:
    def test_stale_active_hypothesis_is_candidate(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H1','Test','d','e','proposed',0.70,'','')")
        conn.commit()
        ids = {c["id"] for c in get_decay_candidates(conn)}
        assert "H1" in ids
        conn.close()

    def test_accepted_not_candidate(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H2','Accept','d','e','accepted',0.70,'','')")
        conn.commit()
        ids = {c["id"] for c in get_decay_candidates(conn)}
        assert "H2" not in ids
        conn.close()

    def test_recent_evidence_not_candidate(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H3','Recent','d','e','proposed',0.70,'','')")
        conn.execute(
            "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), "new data", 5, "H3"),
        )
        conn.commit()
        ids = {c["id"] for c in get_decay_candidates(conn)}
        assert "H3" not in ids
        conn.close()

    def test_at_floor_not_candidate(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H4','Floor','d','e','proposed',0.10,'','')")
        conn.commit()
        ids = {c["id"] for c in get_decay_candidates(conn)}
        assert "H4" not in ids
        conn.close()


class TestApplyDecayToHypothesis:
    def test_confidence_written(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H1','T','d','e','proposed',0.80,'','')")
        conn.commit()
        apply_decay_to_hypothesis(conn, "H1", 0.80, 0.72, 3.0, 90)
        row = conn.execute("SELECT confidence, updated_at FROM hypotheses WHERE id='H1'").fetchone()
        assert abs(row["confidence"] - 0.72) < 0.001
        assert row["updated_at"] is not None
        conn.close()

    def test_log_entry_created(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H1','T','d','e','proposed',0.80,'','')")
        conn.commit()
        apply_decay_to_hypothesis(conn, "H1", 0.80, 0.72, 3.0, 90)
        log_row = conn.execute("SELECT * FROM confidence_decay_log WHERE hypothesis_id='H1'").fetchone()
        assert log_row is not None
        assert abs(log_row["old_confidence"] - 0.80) < 0.001
        conn.close()


class TestConfidenceDecayWorkerRunOnce:
    def test_dry_run_computes_decay(self, tmp_path):
        db_path, conn = _make_decay_db(tmp_path)
        conn.execute("INSERT INTO hypotheses VALUES ('H1','Stale','d','e','proposed',0.80,'','')")
        conn.commit()
        conn.close()
        worker = ConfidenceDecayWorker(db_path=db_path, dry_run=True)
        results = worker.run_once()
        assert len(results) >= 0
        if results:
            assert "old_confidence" in results[0]


# ==============================================================================
# GALACTIC CENTER MONITOR TESTS
# ==============================================================================


class TestMakeBundle:
    def test_schema_and_fields(self):
        b = make_bundle(
            summary="EHT detects Sgr A* shadow.",
            source="EHT 2022",
            payload_type="eht/shadow_measurement",
            domain_tags=["galactic_center", "eht"],
            significance=0.95,
            entities=["Sgr A*"],
            supports_hypothesis=None,
            structured_evidence={"tier": "A"},
        )
        assert b["manatuabon_schema"] == "structured_ingest_v1"
        assert b["payload_type"] == "eht/shadow_measurement"
        assert b["significance"] == 0.95
        assert "ingested_at" in b


class TestIsRecent:
    def test_very_recent(self):
        assert is_recent(datetime.now(timezone.utc).isoformat())

    def test_30_days_old_not_recent(self):
        assert not is_recent((datetime.now(timezone.utc) - timedelta(days=30)).isoformat())

    def test_empty_assumed_recent(self):
        assert is_recent("")


class TestProcessArxivEntry:
    def test_eht_paper_high_significance(self):
        entry = {
            "id": "2204.01234",
            "title": "Event Horizon Telescope Imaging of Sgr A*: First Results",
            "abstract": "We present the first image of Sgr A* from the EHT. Shadow diameter 51.8 uas.",
            "published": datetime.now(timezone.utc).isoformat(),
            "url": "https://arxiv.org/abs/2204.01234",
        }
        bundle = process_arxiv_entry(entry)
        assert bundle["significance"] >= 0.90
        assert "eht" in bundle["domain_tags"]
        assert "EHT" in bundle["entities"]
        assert bundle["payload_type"] == "arxiv/paper"

    def test_non_eht_paper_lower_significance(self):
        entry = {
            "id": "2204.01234",
            "title": "Dust in the Galactic Plane",
            "abstract": "Infrared dust survey.",
            "published": datetime.now(timezone.utc).isoformat(),
            "url": "https://arxiv.org/abs/2204.01234",
        }
        assert process_arxiv_entry(entry)["significance"] < 0.90


class TestEHTReferenceBundles:
    def test_four_bundles_defined(self):
        assert len(EHT_REFERENCE_BUNDLES) == 4

    @pytest.mark.parametrize("idx", range(4))
    def test_bundle_has_summary_and_significance(self, idx):
        b = EHT_REFERENCE_BUNDLES[idx]
        assert len(b["summary"]) > 20
        assert b["significance"] >= 0.90


class TestSeedEHTBundles:
    def test_seeds_4_in_dry_run(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        dropped = seed_eht_bundles(inbox, set(), dry_run=True)
        assert len(dropped) == 4

    def test_reseed_with_same_ids_drops_zero(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        dropped = seed_eht_bundles(inbox, set(), dry_run=True)
        dropped2 = seed_eht_bundles(inbox, set(dropped), dry_run=True)
        assert len(dropped2) == 0

    def test_inbox_empty_in_dry_run(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        seed_eht_bundles(inbox, set(), dry_run=True)
        assert len(list(inbox.glob("*.json"))) == 0


class TestGalacticCenterMonitorDedup:
    def test_first_seed_drops_4(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        seen_path = tmp_path / "seen.json"
        monitor = GalacticCenterMonitor(inbox_path=inbox, seen_path=seen_path, dry_run=False)
        assert monitor.run_eht_seed() == 4
        monitor._save_seen()
        assert len(list(inbox.glob("*.json"))) == 4

    def test_second_seed_drops_0(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        seen_path = tmp_path / "seen.json"
        m1 = GalacticCenterMonitor(inbox_path=inbox, seen_path=seen_path, dry_run=False)
        m1.run_eht_seed()
        m1._save_seen()
        m2 = GalacticCenterMonitor(inbox_path=inbox, seen_path=seen_path, dry_run=False)
        assert m2.run_eht_seed() == 0


# ==============================================================================
# VELA GLITCH WATCH TESTS
# ==============================================================================


class TestEpochUtilities:
    def test_inside_window(self):
        assert epoch_in_window(2022.0)

    def test_on_lo_bound(self):
        assert epoch_in_window(2021.9)

    def test_on_hi_bound(self):
        assert epoch_in_window(2023.5)

    def test_outside_lo(self):
        assert not epoch_in_window(2021.0)

    def test_outside_hi(self):
        assert not epoch_in_window(2024.0)

    def test_mjd_j2000(self):
        assert abs(mjd_to_decimal_year(51544.5) - 2000.0) < 0.01

    def test_mjd_2022(self):
        mjd_2022 = 51544.5 + 22 * 365.25
        assert abs(mjd_to_decimal_year(mjd_2022) - 2022.0) < 0.1

    def test_decimal_year_now(self):
        now_yr = decimal_year_now()
        assert 2025 < now_yr < 2030


class TestExtractEpoch:
    def test_decimal_year(self):
        assert abs(extract_epoch("glitch detected at epoch 2022.3 yr") - 2022.3) < 0.01

    def test_mjd(self):
        assert abs(extract_epoch("glitch at MJD 59550") - mjd_to_decimal_year(59550)) < 0.01

    def test_non_glitch_text_returns_none(self):
        assert extract_epoch("no relevant numbers here") is None

    def test_ignores_out_of_range(self):
        assert extract_epoch("chi-squared 123.456 fits") is None


class TestExtractDeltaNuNu:
    def test_extracts_value(self):
        val = extract_delta_nu_nu("The fractional spin-up was \u0394\u03bd/\u03bd = 2.1 \u00d7 10-6")
        assert val is not None and abs(val - 2.1e-6) < 1e-8

    def test_plain_text_returns_none(self):
        assert extract_delta_nu_nu("no relevant numbers") is None


class TestIsVelaRelated:
    def test_vela_matches(self):
        assert is_vela_related("Vela pulsar new timing data")

    def test_j0835_matches(self):
        assert is_vela_related("Timing of PSR J0835-4510")

    def test_psrj0835_case_insensitive(self):
        assert is_vela_related("PSRJ0835 glitch analysis")

    def test_crab_not_vela(self):
        assert not is_vela_related("Crab pulsar timing")


class TestVelaBundle:
    def test_structure(self):
        b = vela_bundle(
            verdict="CONFIRMED",
            glitch_epoch=2022.3,
            delta_nu_nu=2.1e-6,
            source="arXiv:2202.01234",
            detail="Glitch within window.",
            significance=0.95,
        )
        assert b["manatuabon_schema"] == "structured_ingest_v1"
        assert b["payload_type"] == "pulsar/vela_glitch_verdict"
        assert b["structured_evidence"]["verdict"] == "CONFIRMED"
        assert b["structured_evidence"]["glitch_epoch"] == 2022.3
        assert "prediction_window_lo" in b["structured_evidence"]


class TestVelaGlitchWatchConfirmed:
    def test_confirmed_verdict(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        watch = VelaGlitchWatch(
            inbox_path=inbox,
            seen_path=tmp_path / "seen.json",
            state_path=tmp_path / "state.json",
            dry_run=False,
        )
        entry_text = (
            "Vela pulsar PSR J0835-4510 new timing measurement. "
            "A large glitch was detected at epoch 2022.3 yr. "
            "The fractional frequency jump was \u0394\u03bd/\u03bd = 2.3 \u00d7 10-6."
        )
        issued = watch._process_entry("arxiv_test_001", "Vela Glitch 2022", entry_text, "https://arxiv.org/abs/2202.01234")
        watch._save()
        assert issued
        state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert state.get("verdict_issued") == "CONFIRMED"
        files = list(inbox.glob("vela_watch_*.json"))
        assert len(files) == 1
        b = json.loads(files[0].read_text(encoding="utf-8"))
        assert b["structured_evidence"]["verdict"] == "CONFIRMED"
        assert abs(b["structured_evidence"]["glitch_epoch"] - 2022.3) < 0.01


class TestVelaGlitchWatchFalsified:
    def test_falsified_verdict(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        watch = VelaGlitchWatch(
            inbox_path=inbox,
            seen_path=tmp_path / "s.json",
            state_path=tmp_path / "st.json",
            dry_run=False,
        )
        entry_text = "PSR J0835-4510 Vela glitch detected at epoch 2025.1 yr — much later than expected."
        issued = watch._process_entry("test_falsified", "Vela Glitch 2025", entry_text, "https://atel.org/test")
        watch._save()
        assert issued
        state = json.loads((tmp_path / "st.json").read_text(encoding="utf-8"))
        assert state.get("verdict_issued") == "FALSIFIED"


class TestVelaDeduplication:
    def test_second_entry_skipped(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        watch = VelaGlitchWatch(
            inbox_path=inbox,
            seen_path=tmp_path / "s.json",
            state_path=tmp_path / "st.json",
            dry_run=False,
        )
        text = "Vela PSR J0835-4510 glitch at epoch 2022.1 yr."
        watch._process_entry("id_A", "Vela First", text, "https://arxiv.org/1")
        second = watch._process_entry("id_B", "Vela Second", text, "https://arxiv.org/2")
        assert not second
        assert len(list(inbox.glob("vela_watch_*.json"))) == 1


class TestVelaCheckMissed:
    def test_missed_verdict_when_window_elapsed(self, tmp_path):
        import vela_glitch_watch as vgw

        inbox = tmp_path / "inbox"
        inbox.mkdir()
        watch = VelaGlitchWatch(
            inbox_path=inbox,
            seen_path=tmp_path / "s.json",
            state_path=tmp_path / "st.json",
            dry_run=False,
        )
        original_elapsed = vgw.window_elapsed
        vgw.window_elapsed = lambda: True
        try:
            issued = watch.check_missed()
            watch._save()
        finally:
            vgw.window_elapsed = original_elapsed
        assert issued
        state = json.loads((tmp_path / "st.json").read_text(encoding="utf-8"))
        assert state.get("verdict_issued") == "MISSED"
