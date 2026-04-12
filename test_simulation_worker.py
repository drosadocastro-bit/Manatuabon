"""
Tests for simulation_worker.py — deterministic physics engines and bundle format.
All tests are offline and require no DB or API access.
"""

import json
import sys
from pathlib import Path

import pytest

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


# -- Keyword classifier -------------------------------------------------------


class TestKeywordClassifier:
    def test_orbital_jailer(self):
        assert _classify("The Jailer Hypothesis") == "orbital_confinement"

    def test_orbital_s2(self):
        assert _classify("S2 orbital precession Sgr A*") == "orbital_confinement"

    def test_accretion_bondi(self):
        assert _classify("Bondi accretion rate") == "accretion_physics"

    def test_accretion_dormant(self):
        assert _classify("Dormant Volcano feeding") == "accretion_physics"

    def test_pulsar_crustal(self):
        assert _classify("Nuclear pasta crustal stress") == "pulsar_glitch_stress"

    def test_pulsar_vela(self):
        assert _classify("Vela glitch prediction") == "pulsar_glitch_stress"

    def test_bayesian_posterior(self):
        assert _classify("Bayesian posterior update") == "bayesian_update"

    def test_unknown_unrelated(self):
        assert _classify("random unrelated text") == "unknown"


# -- Engine: orbital_confinement -----------------------------------------------


class TestOrbitalConfinement:
    @pytest.fixture(autouse=True)
    def _run_engine(self):
        self.r = engine_orbital_confinement({})
        self.res = self.r["results"]

    def test_gravitational_sphere_positive(self):
        assert self.res["gravitational_sphere_of_influence_pc"] > 0

    def test_gravitational_sphere_plausible_range(self):
        r_h = self.res["gravitational_sphere_of_influence_pc"]
        assert 0.5 < r_h < 5.0, f"got {r_h:.3f}"

    def test_s2_precession_positive(self):
        assert self.res["s2_schwarzschild_precession_arcmin_per_orbit"] > 0

    def test_s2_precession_within_2_arcmin_of_gravity_2020(self):
        prec = self.res["s2_schwarzschild_precession_arcmin_per_orbit"]
        assert abs(prec - 12.1) < 2.0, f"got {prec:.3f} arcmin"

    def test_precession_agreement_flag(self):
        assert self.res["precession_agreement_within_1arcmin"] is True

    def test_hills_radius_positive(self):
        assert self.res["hills_capture_radius_au"] > 0

    def test_hills_radius_physically_sane(self):
        assert self.res["hills_capture_radius_au"] < 1000

    def test_tidal_disruption_positive(self):
        assert self.res["tidal_disruption_radius_au"] > 0

    def test_tidal_disruption_less_than_hills(self):
        assert self.res["tidal_disruption_radius_au"] < self.res["hills_capture_radius_au"]

    def test_s_star_estimate_positive(self):
        assert self.res["n_s_stars_within_r_h_estimate"] > 0

    def test_at_least_3_testable_predictions(self):
        assert len(self.r["testable_predictions"]) >= 3

    def test_predictions_have_falsification_key(self):
        for p in self.r["testable_predictions"]:
            assert "falsification" in p, f"Missing falsification in: {p['prediction'][:40]}"

    def test_at_least_3_council_revisions(self):
        assert len(self.r["council_revisions_addressed"]) >= 3

    def test_revised_hypothesis_non_empty(self):
        assert len(self.r.get("revised_hypothesis", "")) > 50


# -- Engine: accretion_physics -------------------------------------------------


class TestAccretionPhysics:
    @pytest.fixture(autouse=True)
    def _run_engine(self):
        self.r = engine_accretion_physics({})
        self.res = self.r["results"]

    def test_bondi_radius_positive(self):
        assert float(self.res["bondi_radius_arcsec"]) > 0

    def test_bondi_radius_chandra_resolvable(self):
        assert float(self.res["bondi_radius_arcsec"]) < 10.0

    def test_eddington_luminosity_positive(self):
        assert float(self.res["eddington_luminosity_erg_s"]) > 0

    def test_sgra_sub_eddington(self):
        edd_frac = float(self.res["observed_luminosity_fraction_eddington"])
        assert edd_frac < 1e-3, f"got {edd_frac:.2e}"

    def test_dormancy_explanation_non_empty(self):
        assert len(self.res.get("dormancy_explanation", "")) > 20

    def test_at_least_2_testable_predictions(self):
        assert len(self.r["testable_predictions"]) >= 2


# -- Engine: pulsar_glitch_stress ----------------------------------------------


class TestPulsarGlitchStress:
    @pytest.fixture(autouse=True)
    def _run_engine(self):
        self.r = engine_pulsar_glitch_stress({})
        self.res = self.r["results"]

    def test_characteristic_age_positive(self):
        assert self.res["characteristic_age_kyr"] > 0

    def test_vela_age_within_10_percent(self):
        age = self.res["characteristic_age_kyr"]
        assert abs(age - 11.3) / 11.3 < 0.10, f"got {age:.1f}"

    def test_glitch_window_ordering(self):
        assert self.res["next_glitch_window_open"] < self.res["next_glitch_window_close"]

    def test_window_after_2019_glitch(self):
        assert self.res["next_glitch_window_open"] > 2019.0

    def test_window_center_around_2021_2022(self):
        center = self.res["next_glitch_center"]
        assert 2020.0 < center < 2025.0, f"got {center:.2f}"

    def test_permanent_fraction_range(self):
        frac = self.res["predicted_permanent_fraction"]
        assert 0.60 <= frac <= 0.95, f"got {frac:.4f}"

    def test_stress_proxy_positive(self):
        assert float(self.res["stress_proxy_delta_nu_over_nu"]) > 0

    def test_at_least_3_predictions(self):
        assert len(self.r["testable_predictions"]) >= 3


# -- Engine: bayesian_update ---------------------------------------------------


class TestBayesianUpdate:
    def test_supporting_evidence_raises_confidence(self):
        r = engine_bayesian_update({
            "prior_confidence": 0.5,
            "evidence_reliability": 0.8,
            "evidence_supports": True,
            "n_supporting_items": 2,
            "n_challenging_items": 0,
        })
        assert r["results"]["posterior_confidence"] > 0.5
        assert r["results"]["direction"] == "supported"

    def test_challenging_evidence_lowers_confidence(self):
        r = engine_bayesian_update({
            "prior_confidence": 0.7,
            "evidence_reliability": 0.75,
            "evidence_supports": False,
            "n_supporting_items": 0,
            "n_challenging_items": 2,
        })
        assert r["results"]["posterior_confidence"] < 0.7
        assert r["results"]["direction"] == "challenged"

    def test_posterior_clamped_upper_bound(self):
        r = engine_bayesian_update({
            "prior_confidence": 0.99,
            "evidence_reliability": 0.99,
            "n_supporting_items": 10,
            "n_challenging_items": 0,
        })
        assert r["results"]["posterior_confidence"] <= 0.99


# -- Bundle builder ------------------------------------------------------------


class TestBuildBundle:
    @pytest.fixture(autouse=True)
    def _build(self):
        task = {"id": "TEST-001", "name": "orbital_confinement", "hypothesis_id": "H3"}
        sim_result = engine_orbital_confinement({})
        self.bundle = build_bundle(sim_result, task)

    def test_schema_version(self):
        assert self.bundle["manatuabon_schema"] == "structured_ingest_v1"

    def test_payload_type_contains_sim_type(self):
        assert "orbital_confinement" in self.bundle["payload_type"]

    def test_significance_range(self):
        assert 0.0 <= self.bundle["significance"] <= 1.0

    def test_supports_hypothesis(self):
        assert self.bundle["supports_hypothesis"] == "H3"

    def test_domain_tags_is_list(self):
        assert isinstance(self.bundle["domain_tags"], list)

    def test_entities_non_empty(self):
        assert isinstance(self.bundle["entities"], list) and len(self.bundle["entities"]) > 0

    def test_summary_non_empty(self):
        assert isinstance(self.bundle["summary"], str) and len(self.bundle["summary"]) > 20

    def test_testable_predictions_in_evidence(self):
        assert isinstance(self.bundle["structured_evidence"].get("testable_predictions"), list)

    def test_data_quality_marker(self):
        assert "SYNTHETIC_SIMULATION" in self.bundle["structured_evidence"].get("data_quality", "")

    def test_provenance_non_empty(self):
        assert len(self.bundle["structured_evidence"].get("provenance", "")) > 0


# -- SimulationWorker (offline) ------------------------------------------------


class TestSimulationWorkerOffline:
    def test_run_named_creates_bundle_in_inbox(self, tmp_path):
        inbox = tmp_path / "inbox"
        worker = SimulationWorker(db_path=tmp_path / "test.db", inbox_path=inbox)
        worker.run_named("orbital_confinement", hypothesis_id="H3")

        bundles = list(inbox.glob("simulation_bundle_orbital_confinement_*.json"))
        assert len(bundles) == 1, f"found {len(bundles)}"

        with open(bundles[0], encoding="utf-8") as f:
            b = json.load(f)
        assert b.get("manatuabon_schema") == "structured_ingest_v1"
        assert b.get("supports_hypothesis") == "H3"


# -- dispatch routing ----------------------------------------------------------


class TestDispatchRouting:
    def test_orbital_dispatch(self, tmp_path):
        worker = SimulationWorker(db_path=tmp_path / "t.db", inbox_path=tmp_path / "inbox")
        task = {"id": "R1", "name": "The Jailer Hypothesis S2 orbital", "hypothesis_id": None, "parameters": "{}"}
        result = worker.dispatch(task)
        assert result is not None and result.get("sim_type") == "orbital_confinement"

    def test_pulsar_dispatch(self, tmp_path):
        worker = SimulationWorker(db_path=tmp_path / "t.db", inbox_path=tmp_path / "inbox")
        task = {"id": "R1", "name": "Nuclear Pasta crustal stress accumulation", "hypothesis_id": None, "parameters": "{}"}
        result = worker.dispatch(task)
        assert result is not None and result.get("sim_type") == "pulsar_glitch_stress"

    def test_accretion_dispatch(self, tmp_path):
        worker = SimulationWorker(db_path=tmp_path / "t.db", inbox_path=tmp_path / "inbox")
        task = {"id": "R1", "name": "Bondi accretion rate Eddington", "hypothesis_id": None, "parameters": "{}"}
        result = worker.dispatch(task)
        assert result is not None and result.get("sim_type") == "accretion_physics"

    def test_bayesian_dispatch(self, tmp_path):
        worker = SimulationWorker(db_path=tmp_path / "t.db", inbox_path=tmp_path / "inbox")
        task = {"id": "R1", "name": "Bayesian confidence posterior update", "hypothesis_id": None, "parameters": "{}"}
        result = worker.dispatch(task)
        assert result is not None and result.get("sim_type") == "bayesian_update"
