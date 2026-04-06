import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from gaia_panstarrs_anomaly_worker import (
    build_gaia_panstarrs_anomaly_bundle,
    build_gaia_panstarrs_anomaly_profile,
    ingest_gaia_panstarrs_bundle,
    write_gaia_panstarrs_anomaly_files,
)
from gaia_snapshot_importer import build_gaia_snapshot_bundle, write_gaia_snapshot_files
from manatuabon_agent import MemoryManager
from panstarrs_snapshot_importer import build_panstarrs_snapshot_bundle, write_panstarrs_snapshot_files


GAIA_SNAPSHOT = {
    "source": "Gaia DR3",
    "kind": "stellar_snapshot",
    "object_id": "187.70593_12.39112",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {"ra_center": 187.70593, "dec_center": 12.39112, "radius_deg": 0.1, "max_results": 25, "query_mode": "tap_sync"},
    "summary": {"returned_count": 2, "proper_motion_count": 2, "parallax_count": 2, "radial_velocity_count": 0, "ruwe_count": 2},
    "stars": [
        {"source_id": "101", "ra": 187.70593, "dec": 12.39112, "parallax": 0.5, "parallax_error": 0.05, "pmra": 12.0, "pmra_error": 0.3, "pmdec": -4.0, "pmdec_error": 0.2, "phot_g_mean_mag": 15.1, "ruwe": 1.02},
        {"source_id": "102", "ra": 187.9, "dec": 12.5, "parallax": 0.1, "parallax_error": 0.05, "pmra": 1.0, "pmra_error": 0.2, "pmdec": 0.4, "pmdec_error": 0.2, "phot_g_mean_mag": 17.2, "ruwe": 1.51},
    ],
}

PANSTARRS_SNAPSHOT = {
    "source": "Pan-STARRS DR2",
    "kind": "catalog_snapshot",
    "object_id": "187.70593_12.39112",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {"ra_center": 187.70593, "dec_center": 12.39112, "radius_deg": 0.05, "max_results": 25, "min_detections": 1, "catalog": "mean", "release": "dr2"},
    "summary": {"returned_count": 2, "multiband_count": 1, "band_counts": {"g": 1, "r": 2, "i": 2, "z": 1, "y": 0}},
    "objects": [
        {"objID": 111, "raMean": 187.70594, "decMean": 12.39113, "nDetections": 7, "gMeanPSFMag": 20.1, "rMeanPSFMag": 19.7, "iMeanPSFMag": 19.5, "zMeanPSFMag": 19.4, "yMeanPSFMag": None, "qualityFlag": 0, "objInfoFlag": 0, "extinction_ebv": 0.03, "extinction_method": "analytical_csc_b"},
        {"objID": 222, "raMean": 187.8, "decMean": 12.6, "nDetections": 1, "gMeanPSFMag": None, "rMeanPSFMag": 21.0, "iMeanPSFMag": 20.7, "zMeanPSFMag": None, "yMeanPSFMag": None, "qualityFlag": 4, "objInfoFlag": 16, "extinction_ebv": 0.03, "extinction_method": "analytical_csc_b"},
    ],
}


def test_build_gaia_panstarrs_profile_detects_candidate():
    gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
    panstarrs_bundle = build_panstarrs_snapshot_bundle(PANSTARRS_SNAPSHOT, allow_new_hypothesis=False)
    profile = build_gaia_panstarrs_anomaly_profile(gaia_bundle, panstarrs_bundle, max_sep_arcsec=2.0, pm_threshold_masyr=10.0, min_detections=3)

    assert profile["match_summary"]["matched_star_count"] == 1, profile
    assert profile["match_summary"]["candidate_count"] == 1, profile
    assert profile["anomaly_candidates"][0]["gaia_source_id"] == "101", profile
    assert profile["anomaly_candidates"][0]["persistent_detection_flag"] is True, profile
    assert profile["anomaly_candidates"][0]["review_priority"] == "high", profile
    # Uncertainty fields propagated from Gaia
    assert profile["anomaly_candidates"][0]["pm_total_error_masyr"] is not None, profile
    assert profile["anomaly_candidates"][0]["parallax_error"] is not None, profile
    assert profile["anomaly_candidates"][0]["pmra_error"] is not None, profile
    assert profile["anomaly_candidates"][0]["pmdec_error"] is not None, profile
    # Dereddened color fields present when extinction_ebv is provided
    assert profile["anomaly_candidates"][0]["g_r_color"] is not None, profile
    assert profile["anomaly_candidates"][0]["g_r_color_dereddened"] is not None, profile
    assert profile["anomaly_candidates"][0]["r_i_color_dereddened"] is not None, profile
    assert profile["anomaly_candidates"][0]["extinction_ebv"] == 0.03, profile


def test_build_gaia_panstarrs_profile_ranks_close_clean_match_above_flagged_match():
    gaia_snapshot = {
        **GAIA_SNAPSHOT,
        "stars": [
            GAIA_SNAPSHOT["stars"][0],
            {
                "source_id": "103",
                "ra": 187.70670,
                "dec": 12.39190,
                "parallax": 0.1,
                "parallax_error": 0.05,
                "pmra": 0.8,
                "pmdec": 0.2,
                "phot_g_mean_mag": 17.0,
                "ruwe": 1.2,
            },
        ],
    }
    pan_snapshot = {
        **PANSTARRS_SNAPSHOT,
        "objects": [
            PANSTARRS_SNAPSHOT["objects"][0],
            {
                "objID": 333,
                "raMean": 187.70670,
                "decMean": 12.39190,
                "nDetections": 0,
                "gMeanPSFMag": 20.2,
                "rMeanPSFMag": 20.0,
                "iMeanPSFMag": 19.8,
                "zMeanPSFMag": None,
                "yMeanPSFMag": None,
                "qualityFlag": 16,
                "objInfoFlag": 64,
            },
        ],
    }

    gaia_bundle = build_gaia_snapshot_bundle(gaia_snapshot, allow_new_hypothesis=False)
    panstarrs_bundle = build_panstarrs_snapshot_bundle(pan_snapshot, allow_new_hypothesis=False)
    profile = build_gaia_panstarrs_anomaly_profile(gaia_bundle, panstarrs_bundle, max_sep_arcsec=5.0, pm_threshold_masyr=10.0, min_detections=3)

    assert profile["match_summary"]["matched_star_count"] == 2, profile
    assert profile["anomaly_candidates"][0]["gaia_source_id"] == "101", profile
    assert profile["anomaly_candidates"][0]["review_priority"] == "high", profile
    assert profile["anomaly_candidates"][1]["gaia_source_id"] == "103", profile
    assert profile["anomaly_candidates"][1]["quality_flagged"] is True, profile
    assert profile["anomaly_candidates"][0]["candidate_score"] > profile["anomaly_candidates"][1]["candidate_score"], profile


def test_direct_gaia_panstarrs_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
    panstarrs_bundle = build_panstarrs_snapshot_bundle(PANSTARRS_SNAPSHOT, allow_new_hypothesis=False)
    profile = build_gaia_panstarrs_anomaly_profile(gaia_bundle, panstarrs_bundle, max_sep_arcsec=2.0, pm_threshold_masyr=10.0, min_detections=3)
    bundle = build_gaia_panstarrs_anomaly_bundle(profile)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, gaia_bundle_json, _ = write_gaia_snapshot_files(GAIA_SNAPSHOT, gaia_bundle, tmp_path, "gaia")
        _, pan_bundle_json, _ = write_panstarrs_snapshot_files(PANSTARRS_SNAPSHOT, panstarrs_bundle, tmp_path, "pan")
        assert gaia_bundle_json.exists(), gaia_bundle_json
        assert pan_bundle_json.exists(), pan_bundle_json
        _, bundle_json, _ = write_gaia_panstarrs_anomaly_files(profile, bundle, tmp_path, "gaia_vs_panstarrs")

        result = ingest_gaia_panstarrs_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)
        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Gaia x Pan-STARRS anomaly profile") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_build_gaia_panstarrs_profile_detects_candidate()
    test_build_gaia_panstarrs_profile_ranks_close_clean_match_above_flagged_match()
    test_direct_gaia_panstarrs_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("gaia panstarrs anomaly worker tests passed")


if __name__ == "__main__":
    main()