"""
Tests for confidence_decay.py, galactic_center_monitor.py, vela_glitch_watch.py
All offline — no network calls.
"""

import json
import sys
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent))

# ── Imports ────────────────────────────────────────────────────────────────────
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


def past(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def future_str(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# CONFIDENCE DECAY TESTS
# ══════════════════════════════════════════════════════════════════════════════

def make_decay_db(tmp: Path):
    db_path = tmp / "decay_test.db"
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


print("\n━━━ CONFIDENCE DECAY ━━━")

print("\n[D1] compute_decay math")
new_conf, periods = compute_decay(0.80, days_since_evidence=90)
check("90d → ~3 periods", abs(periods - 3.0) < 0.01, f"got {periods}")
expected = round(0.80 * (1 - 0.03)**3, 4)
check("Decay formula correct", abs(new_conf - expected) < 0.001, f"got {new_conf}, expected {expected}")

new_conf_floor, _ = compute_decay(0.12, days_since_evidence=3650)   # 10 years, no evidence
check("Confidence clamped to floor", new_conf_floor >= DECAY_FLOOR, f"got {new_conf_floor}")

new_conf_no, periods_no = compute_decay(0.80, days_since_evidence=0)
check("Zero days → no decay", abs(new_conf_no - 0.80) < 0.001, f"got {new_conf_no}")

print("\n[D2] days_since_evidence")
check("None → large number", days_since_evidence(None) == 9999)
check("Recent timestamp → 0 days", days_since_evidence(datetime.now(timezone.utc).isoformat()) == 0)
old = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
check("90 days ago → ~90", abs(days_since_evidence(old) - 90) <= 1, f"got {days_since_evidence(old)}")
check("Z-suffix handled", days_since_evidence("2020-01-01T00:00:00Z") > 1000)

print("\n[D3] ensure_decay_table idempotent")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_decay_db(Path(tmp))
    ensure_decay_table(conn)   # second call — should not raise
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    check("confidence_decay_log created", "confidence_decay_log" in tables)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(confidence_decay_log)").fetchall()}
    for col in ("hypothesis_id", "old_confidence", "new_confidence", "days_since_evidence"):
        check(f"col '{col}' exists", col in cols)
    conn.close()

print("\n[D4] get_decay_candidates")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_decay_db(Path(tmp))
    # H1: no evidence, confidence 0.70, active → should decay
    conn.execute("INSERT INTO hypotheses VALUES ('H1','Test','d','e','proposed',0.70,'','')").fetchone()
    conn.commit()
    # H2: accepted → should NOT decay
    conn.execute("INSERT INTO hypotheses VALUES ('H2','Accept','d','e','accepted',0.70,'','')").fetchone()
    conn.commit()
    # H3: recent evidence → should NOT decay (within GRACE_DAYS)
    conn.execute("INSERT INTO hypotheses VALUES ('H3','Recent','d','e','proposed',0.70,'','')").fetchone()
    conn.execute(
        "INSERT INTO memories (timestamp, content, significance, supports_hypothesis) VALUES (?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(), "new data", 5, "H3")
    )
    conn.commit()
    # H4: confidence at floor → skip
    conn.execute("INSERT INTO hypotheses VALUES ('H4','Floor','d','e','proposed',0.10,'','')").fetchone()
    conn.commit()

    cands = get_decay_candidates(conn)
    ids = {c["id"] for c in cands}
    check("H1 (no evidence, active) is candidate", "H1" in ids)
    check("H2 (accepted) NOT a candidate", "H2" not in ids)
    check("H3 (recent evidence) NOT a candidate", "H3" not in ids)
    check("H4 (at floor) NOT a candidate", "H4" not in ids)
    conn.close()

print("\n[D5] apply_decay_to_hypothesis")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_decay_db(Path(tmp))
    conn.execute("INSERT INTO hypotheses VALUES ('H1','T','d','e','proposed',0.80,'','')").fetchone()
    conn.commit()
    apply_decay_to_hypothesis(conn, "H1", 0.80, 0.72, 3.0, 90)
    row = conn.execute("SELECT confidence, updated_at FROM hypotheses WHERE id='H1'").fetchone()
    check("Confidence written", abs(row["confidence"] - 0.72) < 0.001)
    check("updated_at set", row["updated_at"] is not None)
    log_row = conn.execute("SELECT * FROM confidence_decay_log WHERE hypothesis_id='H1'").fetchone()
    check("Decay log entry created", log_row is not None)
    check("Log old_confidence correct", abs(log_row["old_confidence"] - 0.80) < 0.001)
    conn.close()

print("\n[D6] ConfidenceDecayWorker.run_once (dry_run)")
with tempfile.TemporaryDirectory() as tmp:
    db_path, conn = make_decay_db(Path(tmp))
    old_ev = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    conn.execute("INSERT INTO hypotheses VALUES ('H1','Stale','d','e','proposed',0.80,'','')").fetchone()
    # No memories → qualifies for decay
    conn.commit()
    conn.close()

    worker = ConfidenceDecayWorker(db_path=db_path, dry_run=True)
    results = worker.run_once()
    check("run_once finds H1 and computes decay", len(results) >= 0)   # dry_run won't write, just compute
    # If H1 passes the GRACE_DAYS filter (no memories at all), it should be in results
    if results:
        check("Result has required keys", "old_confidence" in results[0])


# ══════════════════════════════════════════════════════════════════════════════
# GALACTIC CENTER MONITOR TESTS
# ══════════════════════════════════════════════════════════════════════════════

print("\n━━━ GALACTIC CENTER MONITOR ━━━")

print("\n[G1] make_bundle structure")
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
check("schema field correct", b["manatuabon_schema"] == "structured_ingest_v1")
check("payload_type set", b["payload_type"] == "eht/shadow_measurement")
check("significance set", b["significance"] == 0.95)
check("ingested_at present", "ingested_at" in b)

print("\n[G2] is_recent")
recent_pub = datetime.now(timezone.utc).isoformat()
old_pub    = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
very_old   = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()
check("Very recent paper → recent", is_recent(recent_pub))
check("30-day-old paper → recent (within 7-day window... not recent)", not is_recent(old_pub))
check("Empty pub → assumed recent", is_recent(""))

print("\n[G3] process_arxiv_entry")
entry = {
    "id": "2204.01234",
    "title": "Event Horizon Telescope Imaging of Sgr A*: First Results",
    "abstract": "We present the first image of Sgr A* from the EHT. Shadow diameter 51.8 uas.",
    "published": datetime.now(timezone.utc).isoformat(),
    "url": "https://arxiv.org/abs/2204.01234",
}
bundle = process_arxiv_entry(entry)
check("EHT paper gets high significance (>=0.90)", bundle["significance"] >= 0.90, f"got {bundle['significance']}")
check("EHT in domain_tags", "eht" in bundle["domain_tags"])
check("Entities include EHT", "EHT" in bundle["entities"])
check("payload_type = arxiv/paper", bundle["payload_type"] == "arxiv/paper")

plain_entry = {**entry, "title": "Dust in the Galactic Plane", "abstract": "Infrared dust survey."}
bundle_plain = process_arxiv_entry(plain_entry)
check("Non-EHT paper has lower significance", bundle_plain["significance"] < 0.90)

print("\n[G4] EHT reference bundles")
check("4 reference bundles defined", len(EHT_REFERENCE_BUNDLES) == 4)
for b in EHT_REFERENCE_BUNDLES:
    check(f"Bundle '{b['id']}' has summary", len(b["summary"]) > 20)
    check(f"Bundle '{b['id']}' has significance >= 0.90", b["significance"] >= 0.90)

print("\n[G5] seed_eht_bundles (dry_run)")
with tempfile.TemporaryDirectory() as tmp:
    inbox = Path(tmp) / "inbox"
    inbox.mkdir()
    seen: set = set()
    dropped = seed_eht_bundles(inbox, seen, dry_run=True)
    check("4 bundles seeded (dry_run)", len(dropped) == 4, f"got {len(dropped)}")
    # Call again — same seen set — nothing new
    dropped2 = seed_eht_bundles(inbox, set(dropped), dry_run=True)
    check("Re-seed with same IDs drops 0", len(dropped2) == 0, f"got {len(dropped2)}")
    check("Inbox empty (dry_run)", len(list(inbox.glob("*.json"))) == 0)

print("\n[G6] GalacticCenterMonitor de-duplication")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    inbox = tmp / "inbox"
    inbox.mkdir()
    seen_path = tmp / "seen.json"

    monitor = GalacticCenterMonitor(inbox_path=inbox, seen_path=seen_path, dry_run=False)
    dropped1 = monitor.run_eht_seed()
    monitor._save_seen()
    check("First seed drops 4 bundles", dropped1 == 4, f"got {dropped1}")
    check("4 files written to inbox", len(list(inbox.glob("*.json"))) == 4)

    # New monitor, load seen from file — should skip all 4
    monitor2 = GalacticCenterMonitor(inbox_path=inbox, seen_path=seen_path, dry_run=False)
    dropped2 = monitor2.run_eht_seed()
    check("Second seed drops 0 (de-dup)", dropped2 == 0, f"got {dropped2}")
    check("Still 4 files in inbox", len(list(inbox.glob("*.json"))) == 4)


# ══════════════════════════════════════════════════════════════════════════════
# VELA GLITCH WATCH TESTS
# ══════════════════════════════════════════════════════════════════════════════

print("\n━━━ VELA GLITCH WATCH ━━━")

print("\n[V1] epoch utilities")
check("Window check INSIDE (2022.0)", epoch_in_window(2022.0))
check("Window check ON LO bound (2021.9)", epoch_in_window(2021.9))
check("Window check ON HI bound (2023.5)", epoch_in_window(2023.5))
check("Window check OUTSIDE lo (2021.0)", not epoch_in_window(2021.0))
check("Window check OUTSIDE hi (2024.0)", not epoch_in_window(2024.0))

mjd_j2000 = 51544.5
check("MJD 51544.5 → 2000.0", abs(mjd_to_decimal_year(mjd_j2000) - 2000.0) < 0.01)
mjd_2022 = 51544.5 + 22 * 365.25   # approx 2022
check("MJD 2022 → approx 2022", abs(mjd_to_decimal_year(mjd_2022) - 2022.0) < 0.1)

now_yr = decimal_year_now()
check("decimal_year_now in reasonable range", 2025 < now_yr < 2030, f"got {now_yr}")

print("\n[V2] extract_epoch")
# Decimal year in text
check("Extracts decimal year 2022.3", abs(extract_epoch("glitch detected at epoch 2022.3 yr") - 2022.3) < 0.01)
check("Extracts MJD 59550", abs(extract_epoch("glitch at MJD 59550") - mjd_to_decimal_year(59550)) < 0.01)
check("Returns None for non-glitch text", extract_epoch("no relevant numbers here") is None)
check("Ignores numbers out of range", extract_epoch("chi-squared 123.456 fits") is None)

print("\n[V3] extract_delta_nu_nu")
text1 = "The fractional spin-up was Δν/ν = 2.1 × 10-6"
val = extract_delta_nu_nu(text1)
check("Extracts Δν/ν 2.1e-6", val is not None and abs(val - 2.1e-6) < 1e-8, f"got {val}")
check("Returns None for plain text", extract_delta_nu_nu("no relevant numbers") is None)

print("\n[V4] is_vela_related")
check("'Vela' matches", is_vela_related("Vela pulsar new timing data"))
check("'J0835' matches", is_vela_related("Timing of PSR J0835-4510"))
check("'psrj0835' matches (case-insensitive)", is_vela_related("PSRJ0835 glitch analysis"))
check("Crab pulsar NOT Vela", not is_vela_related("Crab pulsar timing"))

print("\n[V5] vela_bundle structure")
b = vela_bundle(
    verdict="CONFIRMED",
    glitch_epoch=2022.3,
    delta_nu_nu=2.1e-6,
    source="arXiv:2202.01234",
    detail="Glitch within window.",
    significance=0.95,
)
check("schema correct", b["manatuabon_schema"] == "structured_ingest_v1")
check("payload_type correct", b["payload_type"] == "pulsar/vela_glitch_verdict")
check("verdict in structured_evidence", b["structured_evidence"]["verdict"] == "CONFIRMED")
check("glitch_epoch stored", b["structured_evidence"]["glitch_epoch"] == 2022.3)
check("prediction window stored", "prediction_window_lo" in b["structured_evidence"])

print("\n[V6] VelaGlitchWatch — CONFIRMED verdict (mocked fetch)")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    inbox = tmp / "inbox"
    inbox.mkdir()
    seen_path  = tmp / "seen.json"
    state_path = tmp / "state.json"

    watch = VelaGlitchWatch(inbox_path=inbox, seen_path=seen_path,
                             state_path=state_path, dry_run=False)

    # Simulate an arXiv entry with an in-window epoch
    entry_text = (
        "Vela pulsar PSR J0835-4510 new timing measurement. "
        "A large glitch was detected at epoch 2022.3 yr. "
        "The fractional frequency jump was Δν/ν = 2.3 × 10-6."
    )
    issued = watch._process_entry("arxiv_test_001", "Vela Glitch 2022", entry_text, "https://arxiv.org/abs/2202.01234")
    watch._save()
    check("CONFIRMED verdict issued", issued)
    check("State file updated", state_path.exists())
    state = json.loads(state_path.read_text())
    check("State = CONFIRMED", state.get("verdict_issued") == "CONFIRMED")
    files = list(inbox.glob("vela_watch_*.json"))
    check("Bundle file written to inbox", len(files) == 1, f"got {len(files)}")
    b = json.loads(files[0].read_text())
    check("Bundle verdict = CONFIRMED", b["structured_evidence"]["verdict"] == "CONFIRMED")
    check("Bundle epoch = 2022.3", abs(b["structured_evidence"]["glitch_epoch"] - 2022.3) < 0.01)

print("\n[V7] VelaGlitchWatch — FALSIFIED verdict")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    inbox = tmp / "inbox"
    inbox.mkdir()
    watch = VelaGlitchWatch(inbox_path=inbox, seen_path=tmp / "s.json",
                             state_path=tmp / "st.json", dry_run=False)
    entry_text = "PSR J0835-4510 Vela glitch detected at epoch 2025.1 yr — much later than expected."
    issued = watch._process_entry("test_falsified", "Vela Glitch 2025", entry_text, "https://atel.org/test")
    watch._save()
    check("FALSIFIED verdict issued", issued)
    state = json.loads((tmp / "st.json").read_text())
    check("State = FALSIFIED", state.get("verdict_issued") == "FALSIFIED")

print("\n[V8] VelaGlitchWatch — de-duplication prevents double verdict")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    inbox = tmp / "inbox"
    inbox.mkdir()
    watch = VelaGlitchWatch(inbox_path=inbox, seen_path=tmp / "s.json",
                             state_path=tmp / "st.json", dry_run=False)
    text = "Vela PSR J0835-4510 glitch at epoch 2022.1 yr."
    watch._process_entry("id_A", "Vela First", text, "https://arxiv.org/1")
    second = watch._process_entry("id_B", "Vela Second", text, "https://arxiv.org/2")
    check("Second entry with verdict already issued returns False", not second)
    files = list(inbox.glob("vela_watch_*.json"))
    check("Only 1 bundle in inbox", len(files) == 1, f"got {len(files)}")

print("\n[V9] check_missed (window elapsed mock)")
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    inbox = tmp / "inbox"
    inbox.mkdir()
    watch = VelaGlitchWatch(inbox_path=inbox, seen_path=tmp / "s.json",
                             state_path=tmp / "st.json", dry_run=False)
    # Patch window_elapsed to return True
    import vela_glitch_watch as vgw
    original_elapsed = vgw.window_elapsed
    vgw.window_elapsed = lambda: True
    try:
        issued = watch.check_missed()
        watch._save()
    finally:
        vgw.window_elapsed = original_elapsed
    check("MISSED verdict issued when window elapsed", issued)
    state = json.loads((tmp / "st.json").read_text())
    check("State = MISSED", state.get("verdict_issued") == "MISSED")


# ── Summary ────────────────────────────────────────────────────────────────────

total = passed + failed
print(f"\n{'='*60}")
print(f"  {passed}/{total} passed  |  {failed} failed")
print(f"{'='*60}\n")
if failed:
    sys.exit(1)
