import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from gaia_snapshot_importer import build_gaia_snapshot_bundle, write_gaia_snapshot_files
from gaia_ztf_anomaly_worker import (
    build_gaia_ztf_anomaly_bundle,
    build_gaia_ztf_anomaly_profile,
    ingest_gaia_ztf_bundle,
    write_gaia_ztf_anomaly_files,
)
from manatuabon_agent import MemoryManager
from ztf_snapshot_importer import build_ztf_snapshot_bundle, write_ztf_snapshot_files


GAIA_SNAPSHOT = {
    "source": "Gaia DR3",
    "kind": "stellar_snapshot",
    "object_id": "255.57691_12.28378",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {"ra_center": 255.57691, "dec_center": 12.28378, "radius_deg": 0.1, "max_results": 25, "query_mode": "tap_sync"},
    "summary": {"returned_count": 2, "proper_motion_count": 2, "parallax_count": 2, "radial_velocity_count": 0, "ruwe_count": 2},
    "stars": [
        {"source_id": "301", "ra": 255.57691, "dec": 12.28378, "parallax": 0.4, "parallax_error": 0.05, "pmra": 15.0, "pmra_error": 0.3, "pmdec": -3.0, "pmdec_error": 0.2, "phot_g_mean_mag": 16.0, "ruwe": 1.01},
        {"source_id": "302", "ra": 255.8, "dec": 12.5, "parallax": 0.1, "parallax_error": 0.04, "pmra": 1.0, "pmra_error": 0.2, "pmdec": 0.8, "pmdec_error": 0.2, "phot_g_mean_mag": 18.2, "ruwe": 1.52},
    ],
}

ZTF_SNAPSHOT = {
    "source": "ZTF via IRSA",
    "kind": "image_metadata_snapshot",
    "object_id": "255.57691_12.28378",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {"ra_center": 255.57691, "dec_center": 12.28378, "size_deg": 0.1, "max_results": 25, "intersect": "OVERLAPS", "product_type": "science"},
    "summary": {"returned_count": 2, "filter_counts": {"zr": 1, "zg": 1}, "seeing_count": 2, "maglimit_count": 2},
    "frames": [
        {"field": "123", "ccdid": "4", "qid": "2", "filtercode": "zr", "imgtypecode": "o", "obsjd": 2460123.1, "seeing": 1.9, "maglimit": 20.7, "ra": 255.57692, "dec": 12.28377, "infobits": "0", "pid": "987"},
        {"field": "123", "ccdid": "4", "qid": "3", "filtercode": "zg", "imgtypecode": "o", "obsjd": 2460124.2, "seeing": 3.1, "maglimit": 19.8, "ra": 255.9, "dec": 12.6, "infobits": "0", "pid": "988"},
    ],
}


def test_build_gaia_ztf_profile_detects_candidate():
    gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
    ztf_bundle = build_ztf_snapshot_bundle(ZTF_SNAPSHOT, allow_new_hypothesis=False)
    profile = build_gaia_ztf_anomaly_profile(gaia_bundle, ztf_bundle, max_sep_arcsec=5.0, pm_threshold_masyr=10.0, seeing_threshold=2.5)

    assert profile["match_summary"]["matched_star_count"] == 1, profile
    assert profile["match_summary"]["candidate_count"] == 1, profile
    assert profile["anomaly_candidates"][0]["gaia_source_id"] == "301", profile
    assert profile["anomaly_candidates"][0]["good_seeing_flag"] is True, profile
    assert profile["anomaly_candidates"][0]["review_priority"] == "high", profile
    # Uncertainty fields propagated from Gaia
    assert profile["anomaly_candidates"][0]["pm_total_error_masyr"] is not None, profile
    assert profile["anomaly_candidates"][0]["parallax_error"] is not None, profile
    assert profile["anomaly_candidates"][0]["pmra_error"] is not None, profile
    assert profile["anomaly_candidates"][0]["pmdec_error"] is not None, profile


def test_direct_gaia_ztf_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
    ztf_bundle = build_ztf_snapshot_bundle(ZTF_SNAPSHOT, allow_new_hypothesis=False)
    profile = build_gaia_ztf_anomaly_profile(gaia_bundle, ztf_bundle, max_sep_arcsec=5.0, pm_threshold_masyr=10.0, seeing_threshold=2.5)
    bundle = build_gaia_ztf_anomaly_bundle(profile)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, gaia_bundle_json, _ = write_gaia_snapshot_files(GAIA_SNAPSHOT, gaia_bundle, tmp_path, "gaia")
        _, ztf_bundle_json, _ = write_ztf_snapshot_files(ZTF_SNAPSHOT, ztf_bundle, tmp_path, "ztf")
        assert gaia_bundle_json.exists(), gaia_bundle_json
        assert ztf_bundle_json.exists(), ztf_bundle_json
        _, bundle_json, _ = write_gaia_ztf_anomaly_files(profile, bundle, tmp_path, "gaia_vs_ztf")

        result = ingest_gaia_ztf_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)
        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Gaia x ZTF anomaly profile") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_build_gaia_ztf_profile_detects_candidate()
    test_direct_gaia_ztf_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("gaia ztf anomaly worker tests passed")


if __name__ == "__main__":
    main()