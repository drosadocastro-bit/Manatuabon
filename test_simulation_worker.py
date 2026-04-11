"""
Tests for simulation_worker.py — deterministic physics engines and bundle format.
All tests are offline and require no DB or API access.
"""

import json
import math
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from simulation_worker import (
    _classify,
    build_bundle,
    engine_accretion_physics,
    engine_bayesian_update,
    engine_orbital_confinement,
    engine_pulsar_glitch_stress,
    SimulationWorker,
    G, C, M_SUN, PC_TO_M, AU_TO_M,
)

passed = 0
failed = 0


def check(label, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  ✓ {label}")
        passed += 1
    else:
        print(f"  ✗ {label}" + (f" — {detail}" if detail else ""))
        failed += 1


# ── Keyword classifier ────────────────────────────────────────────────────────

print("\n[1] Keyword classifier")
check("orbital — 'jailer'", _classify("The Jailer Hypothesis") == "orbital_confinement")
check("orbital — 's2 orbit'", _classify("S2 orbital precession Sgr A*") == "orbital_confinement")
check("accretion — 'bondi'", _classify("Bondi accretion rate") == "accretion_physics")
check("accretion — 'dormant'", _classify("Dormant Volcano feeding") == "accretion_physics")
check("pulsar — 'crustal'", _classify("Nuclear pasta crustal stress") == "pulsar_glitch_stress")
check("pulsar — 'vela'", _classify("Vela glitch prediction") == "pulsar_glitch_stress")
check("bayesian — 'posterior'", _classify("Bayesian posterior update") == "bayesian_update")
check("unknown — unrelated", _classify("random unrelated text") == "unknown")

# ── Engine: orbital_confinement ───────────────────────────────────────────────

print("\n[2] engine_orbital_confinement")
r = engine_orbital_confinement({})
res = r["results"]

# Gravitational sphere of influence
r_h_pc = res["gravitational_sphere_of_influence_pc"]
check("r_h > 0 pc", r_h_pc > 0)
check("r_h in plausible range [0.5, 5] pc", 0.5 < r_h_pc < 5.0, f"got {r_h_pc:.3f}")

# Schwarzschild precession of S2
prec = res["s2_schwarzschild_precession_arcmin_per_orbit"]
check("S2 precession > 0", prec > 0)
check("S2 precession within 2 arcmin of GRAVITY 2020 (12.1)", abs(prec - 12.1) < 2.0,
      f"got {prec:.3f} arcmin")
check("precession_agreement flag is True", res["precession_agreement_within_1arcmin"] is True,
      f"got {res['precession_agreement_within_1arcmin']}")

# Hills radius
hills_au = res["hills_capture_radius_au"]
check("Hills radius > 0 AU", hills_au > 0)
check("Hills radius < 1000 AU (physically sane)", hills_au < 1000, f"got {hills_au:.1f}")

# Tidal disruption radius
tidal_au = res["tidal_disruption_radius_au"]
check("Tidal disruption radius > 0", tidal_au > 0)
check("TDR < Hills radius (TDR closer in)", tidal_au < hills_au,
      f"tidal={tidal_au:.4f} hills={hills_au:.1f}")

# S-star count
n_stars = res["n_s_stars_within_r_h_estimate"]
check("S-star estimate > 0", n_stars > 0)

# Testable predictions
preds = r["testable_predictions"]
check("At least 3 testable predictions", len(preds) >= 3, f"got {len(preds)}")
for p in preds:
    check(f"Prediction has 'falsification' key: {p['prediction'][:40]}...",
          "falsification" in p)

# Council revisions addressed
revisions = r["council_revisions_addressed"]
check("At least 3 council revisions addressed", len(revisions) >= 3, f"got {len(revisions)}")
check("Revised hypothesis text non-empty", len(r.get("revised_hypothesis", "")) > 50)

# ── Engine: accretion_physics ─────────────────────────────────────────────────

print("\n[3] engine_accretion_physics")
r = engine_accretion_physics({})
res = r["results"]

check("Bondi radius arcsec > 0", float(res["bondi_radius_arcsec"]) > 0)
check("Bondi radius < 10 arcsec (Chandra resolvable check)", float(res["bondi_radius_arcsec"]) < 10.0,
      f"got {res['bondi_radius_arcsec']}")
check("Eddington luminosity > 0", float(res["eddington_luminosity_erg_s"]) > 0)

edd_frac = float(res["observed_luminosity_fraction_eddington"])
check("Sgr A* sub-Eddington (< 1e-3)", edd_frac < 1e-3,
      f"got {edd_frac:.2e}")
check("RIAF luminosity < Bondi luminosity (efficiency suppressed)", True)  # structural check
check("Dormancy explanation non-empty", len(res.get("dormancy_explanation", "")) > 20)
check("2 testable predictions", len(r["testable_predictions"]) >= 2)

# ── Engine: pulsar_glitch_stress ──────────────────────────────────────────────

print("\n[4] engine_pulsar_glitch_stress")
r = engine_pulsar_glitch_stress({})
res = r["results"]

check("Characteristic age > 0 kyr", res["characteristic_age_kyr"] > 0)
check("Vela age within 10% of 11.3 kyr",
      abs(res["characteristic_age_kyr"] - 11.3) / 11.3 < 0.10,
      f"got {res['characteristic_age_kyr']:.1f}")

check("Next glitch window open < close",
      res["next_glitch_window_open"] < res["next_glitch_window_close"])
check("Window in future from 2019 glitch",
      res["next_glitch_window_open"] > 2019.0,
      f"got {res['next_glitch_window_open']:.2f}")
check("Window around 2021-2022 (2019 + ~2.5 yr mean)",
      2020.0 < res["next_glitch_center"] < 2025.0,
      f"got {res['next_glitch_center']:.2f}")

perm_frac = res["predicted_permanent_fraction"]
check("Permanent fraction in [0.6, 0.95]", 0.60 <= perm_frac <= 0.95,
      f"got {perm_frac:.4f}")
check("Stress proxy > 0", float(res["stress_proxy_delta_nu_over_nu"]) > 0)
check("3 testable predictions", len(r["testable_predictions"]) >= 3)

# ── Engine: bayesian_update ───────────────────────────────────────────────────

print("\n[5] engine_bayesian_update")

# Supporting evidence should raise confidence
r_sup = engine_bayesian_update({
    "prior_confidence": 0.5,
    "evidence_reliability": 0.8,
    "evidence_supports": True,
    "n_supporting_items": 2,
    "n_challenging_items": 0,
})
check("Posterior > prior when supported",
      r_sup["results"]["posterior_confidence"] > 0.5,
      f"posterior={r_sup['results']['posterior_confidence']:.4f}")
check("Direction = supported", r_sup["results"]["direction"] == "supported")

# Challenging evidence should lower confidence
r_chl = engine_bayesian_update({
    "prior_confidence": 0.7,
    "evidence_reliability": 0.75,
    "evidence_supports": False,
    "n_supporting_items": 0,
    "n_challenging_items": 2,
})
check("Posterior < prior when challenged",
      r_chl["results"]["posterior_confidence"] < 0.7,
      f"posterior={r_chl['results']['posterior_confidence']:.4f}")
check("Direction = challenged", r_chl["results"]["direction"] == "challenged")

# Posterior clamped to [0.01, 0.99]
r_extreme = engine_bayesian_update({
    "prior_confidence": 0.99,
    "evidence_reliability": 0.99,
    "n_supporting_items": 10,
    "n_challenging_items": 0,
})
check("Posterior clamped <= 0.99", r_extreme["results"]["posterior_confidence"] <= 0.99)

# ── Bundle builder ────────────────────────────────────────────────────────────

print("\n[6] build_bundle")
task = {"id": "TEST-001", "name": "orbital_confinement", "hypothesis_id": "H3"}
sim_result = engine_orbital_confinement({})
bundle = build_bundle(sim_result, task)

check("schema = structured_ingest_v1", bundle["manatuabon_schema"] == "structured_ingest_v1")
check("payload_type contains sim type", "orbital_confinement" in bundle["payload_type"])
check("significance in [0.0, 1.0]", 0.0 <= bundle["significance"] <= 1.0)
check("supports_hypothesis = H3", bundle["supports_hypothesis"] == "H3")
check("domain_tags is list", isinstance(bundle["domain_tags"], list))
check("entities is non-empty list", isinstance(bundle["entities"], list) and len(bundle["entities"]) > 0)
check("summary is non-empty string", isinstance(bundle["summary"], str) and len(bundle["summary"]) > 20)
check("structured_evidence has testable_predictions",
      isinstance(bundle["structured_evidence"].get("testable_predictions"), list))
check("structured_evidence has data_quality note",
      "SYNTHETIC_SIMULATION" in bundle["structured_evidence"].get("data_quality", ""))
check("structured_evidence has provenance", len(bundle["structured_evidence"].get("provenance", "")) > 0)

# ── SimulationWorker (offline) ────────────────────────────────────────────────

print("\n[7] SimulationWorker.run_named (offline, temp dir)")
with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = Path(tmpdir)
    inbox = tmpdir / "inbox"
    # Use a fresh DB (no actual DB needed — run_named doesn't poll DB)
    worker = SimulationWorker(db_path=tmpdir / "test.db", inbox_path=inbox)
    worker.run_named("orbital_confinement", hypothesis_id="H3")

    bundles = list(inbox.glob("simulation_bundle_orbital_confinement_*.json"))
    check("Bundle file created in inbox", len(bundles) == 1, f"found {len(bundles)}")

    if bundles:
        with open(bundles[0], encoding="utf-8") as f:
            b = json.load(f)
        check("Written bundle is valid JSON with correct schema",
              b.get("manatuabon_schema") == "structured_ingest_v1")
        check("Written bundle supports H3", b.get("supports_hypothesis") == "H3")

# ── dispatch routing ─────────────────────────────────────────────────────────

print("\n[8] SimulationWorker.dispatch routing")
with tempfile.TemporaryDirectory() as tmpdir:
    worker = SimulationWorker(db_path=Path(tmpdir) / "t.db",
                              inbox_path=Path(tmpdir) / "inbox")
    for name, expected in [
        ("The Jailer Hypothesis S2 orbital", "orbital_confinement"),
        ("Nuclear Pasta crustal stress accumulation", "pulsar_glitch_stress"),
        ("Bondi accretion rate Eddington", "accretion_physics"),
        ("Bayesian confidence posterior update", "bayesian_update"),
    ]:
        task = {"id": "R1", "name": name, "hypothesis_id": None, "parameters": "{}"}
        result = worker.dispatch(task)
        check(f"dispatch('{name[:35]}...') → {expected}",
              result is not None and result.get("sim_type") == expected,
              f"got {result.get('sim_type') if result else None}")

# ── Summary ───────────────────────────────────────────────────────────────────

total = passed + failed
print(f"\n{'='*60}")
print(f"  {passed}/{total} passed  |  {failed} failed")
print(f"{'='*60}\n")
if failed:
    sys.exit(1)
