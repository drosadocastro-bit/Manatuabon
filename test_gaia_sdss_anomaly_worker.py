import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from gaia_sdss_anomaly_worker import (
    build_gaia_sdss_anomaly_bundle,
    build_gaia_sdss_anomaly_profile,
    ingest_gaia_sdss_bundle,
    write_gaia_sdss_anomaly_files,
)
from gaia_snapshot_importer import build_gaia_snapshot_bundle, write_gaia_snapshot_files
from manatuabon_agent import MemoryManager
from sdss_snapshot_importer import build_sdss_snapshot_bundle, write_sdss_snapshot_files


GAIA_SNAPSHOT = {
    "source": "Gaia DR3",
    "kind": "stellar_snapshot",
    "object_id": "266.41680_-29.00780",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {
        "ra_center": 266.4168,
        "dec_center": -29.0078,
        "radius_deg": 0.25,
        "max_results": 25,
        "query_mode": "tap_sync",
    },
    "summary": {
        "returned_count": 2,
        "proper_motion_count": 2,
        "parallax_count": 2,
        "radial_velocity_count": 1,
        "ruwe_count": 2,
    },
    "stars": [
        {
            "source_id": "101",
            "ra": 266.41680,
            "dec": -29.00780,
            "parallax": 0.5,
            "parallax_error": 0.05,
            "pmra": 12.0,
            "pmra_error": 0.3,
            "pmdec": -4.0,
            "pmdec_error": 0.2,
            "radial_velocity": 25.0,
            "radial_velocity_error": 1.2,
            "phot_g_mean_mag": 15.1,
            "bp_rp": 1.1,
            "ruwe": 1.03,
        },
        {
            "source_id": "102",
            "ra": 266.45000,
            "dec": -29.10000,
            "parallax": 0.2,
            "parallax_error": 0.04,
            "pmra": 1.0,
            "pmra_error": 0.2,
            "pmdec": 0.5,
            "pmdec_error": 0.2,
            "radial_velocity": None,
            "radial_velocity_error": None,
            "phot_g_mean_mag": 17.4,
            "bp_rp": 0.8,
            "ruwe": 1.55,
        },
    ],
}


SDSS_SNAPSHOT = {
    "source": "SDSS",
    "kind": "catalog_snapshot",
    "object_id": "266.41680_-29.00780",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {
        "ra_center": 266.4168,
        "dec_center": -29.0078,
        "radius_arcmin": 60.0,
        "max_results": 50,
        "object_type": "galaxy",
        "query_mode": "sql",
    },
    "rows": [
        {
            "objID": 5001,
            "ra": 266.41682,
            "dec": -29.00779,
            "redshift": 0.12,
            "redshift_error": 0.01,
            "petroMag_r": 17.1,
            "petroMag_g": 18.0,
            "type": 3,
            "subClass": "AGN",
            "velDisp": 220.0,
            "velDispErr": 12.0,
        },
        {
            "objID": 5002,
            "ra": 266.30000,
            "dec": -29.20000,
            "redshift": 0.02,
            "redshift_error": 0.01,
            "petroMag_r": 18.0,
            "petroMag_g": 18.7,
            "type": 3,
            "subClass": "GALAXY",
            "velDisp": 180.0,
            "velDispErr": 10.0,
        },
    ],
    "errors": {},
}


def test_build_gaia_sdss_profile_detects_high_priority_candidate():
    gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
    sdss_bundle = build_sdss_snapshot_bundle(SDSS_SNAPSHOT, allow_new_hypothesis=False)

    profile = build_gaia_sdss_anomaly_profile(
        gaia_bundle,
        sdss_bundle,
        max_sep_arcsec=5.0,
        pm_threshold_masyr=10.0,
        redshift_threshold=0.05,
    )

    assert profile["match_summary"]["matched_star_count"] == 1, profile
    assert profile["match_summary"]["candidate_count"] == 1, profile
    assert profile["anomaly_candidates"][0]["gaia_source_id"] == "101", profile
    assert profile["anomaly_candidates"][0]["high_pm_flag"] is True, profile
    assert profile["anomaly_candidates"][0]["high_redshift_flag"] is True, profile
    assert profile["anomaly_candidates"][0]["review_priority"] == "high", profile
    # Uncertainty fields propagated from Gaia
    assert profile["anomaly_candidates"][0]["pm_total_error_masyr"] is not None, profile
    assert profile["anomaly_candidates"][0]["parallax_error"] is not None, profile
    assert profile["anomaly_candidates"][0]["pmra_error"] is not None, profile
    assert profile["anomaly_candidates"][0]["pmdec_error"] is not None, profile


def test_direct_gaia_sdss_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
    sdss_bundle = build_sdss_snapshot_bundle(SDSS_SNAPSHOT, allow_new_hypothesis=False)
    profile = build_gaia_sdss_anomaly_profile(
        gaia_bundle,
        sdss_bundle,
        max_sep_arcsec=5.0,
        pm_threshold_masyr=10.0,
        redshift_threshold=0.05,
    )
    bundle = build_gaia_sdss_anomaly_bundle(profile)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()

        _, gaia_bundle_json, _ = write_gaia_snapshot_files(GAIA_SNAPSHOT, gaia_bundle, tmp_path, "gaia")
        _, sdss_bundle_json, _ = write_sdss_snapshot_files(SDSS_SNAPSHOT, sdss_bundle, tmp_path, "sdss")
        assert gaia_bundle_json.exists(), gaia_bundle_json
        assert sdss_bundle_json.exists(), sdss_bundle_json

        _, bundle_json, _ = write_gaia_sdss_anomaly_files(profile, bundle, tmp_path, "gaia_vs_sdss")
        result = ingest_gaia_sdss_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result is not None, result
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Gaia x SDSS anomaly profile") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_build_gaia_sdss_profile_detects_high_priority_candidate()
    test_direct_gaia_sdss_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("gaia sdss anomaly worker tests passed")


if __name__ == "__main__":
    main()