"""Tests for replay_manifest.py."""

import json
import tempfile
from pathlib import Path

from gaia_panstarrs_anomaly_worker import (
    build_gaia_panstarrs_anomaly_bundle,
    build_gaia_panstarrs_anomaly_profile,
    write_gaia_panstarrs_anomaly_files,
)
from gaia_snapshot_importer import build_gaia_snapshot_bundle, write_gaia_snapshot_files
from panstarrs_snapshot_importer import build_panstarrs_snapshot_bundle, write_panstarrs_snapshot_files
from replay_manifest import load_manifest, run_manifest


GAIA_SNAPSHOT = {
    "source": "Gaia DR3",
    "kind": "stellar_snapshot",
    "object_id": "187.70593_12.39112",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {"ra_center": 187.70593, "dec_center": 12.39112, "radius_deg": 0.1, "max_results": 25, "query_mode": "tap_sync"},
    "summary": {"returned_count": 1, "proper_motion_count": 1, "parallax_count": 1, "radial_velocity_count": 0, "ruwe_count": 1},
    "stars": [
        {"source_id": "101", "ra": 187.70593, "dec": 12.39112, "parallax": 0.5, "parallax_error": 0.05, "pmra": 12.0, "pmra_error": 0.3, "pmdec": -4.0, "pmdec_error": 0.2, "phot_g_mean_mag": 15.1, "ruwe": 1.02},
    ],
}

PANSTARRS_SNAPSHOT = {
    "source": "Pan-STARRS DR2",
    "kind": "catalog_snapshot",
    "object_id": "187.70593_12.39112",
    "fetched_at": "2026-04-05T00:00:00+00:00",
    "query": {"ra_center": 187.70593, "dec_center": 12.39112, "radius_deg": 0.05, "max_results": 25, "min_detections": 1, "catalog": "mean", "release": "dr2"},
    "summary": {"returned_count": 1, "multiband_count": 1, "band_counts": {"g": 1, "r": 1, "i": 1, "z": 0, "y": 0}},
    "objects": [
        {"objID": 111, "raMean": 187.70594, "decMean": 12.39113, "nDetections": 7, "gMeanPSFMag": 20.1, "rMeanPSFMag": 19.7, "iMeanPSFMag": 19.5, "qualityFlag": 0, "objInfoFlag": 0},
    ],
}


def test_load_manifest_rejects_missing_steps():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as handle:
        json.dump({"description": "incomplete"}, handle)
        handle.flush()
        path = Path(handle.name)
    try:
        failed = False
        try:
            load_manifest(path)
        except ValueError:
            failed = True
        assert failed, "Expected ValueError for missing steps"
    finally:
        path.unlink(missing_ok=True)


def test_load_manifest_rejects_unsupported_worker():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as handle:
        json.dump({"steps": [{"worker": "bad_worker", "gaia_bundle": "a.json"}]}, handle)
        handle.flush()
        path = Path(handle.name)
    try:
        failed = False
        try:
            load_manifest(path)
        except ValueError as exc:
            failed = True
            assert "unsupported worker" in str(exc).lower(), exc
        assert failed, "Expected ValueError for unknown worker"
    finally:
        path.unlink(missing_ok=True)


def test_run_manifest_replays_gaia_panstarrs():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        from db_init import ensure_runtime_db
        ensure_runtime_db(db_path, migrate=False).close()

        gaia_bundle = build_gaia_snapshot_bundle(GAIA_SNAPSHOT, allow_new_hypothesis=False)
        _, gaia_json, _ = write_gaia_snapshot_files(GAIA_SNAPSHOT, gaia_bundle, tmp_path, "gaia")
        pan_bundle = build_panstarrs_snapshot_bundle(PANSTARRS_SNAPSHOT, allow_new_hypothesis=False)
        _, pan_json, _ = write_panstarrs_snapshot_files(PANSTARRS_SNAPSHOT, pan_bundle, tmp_path, "pan")

        manifest = {
            "description": "test replay",
            "steps": [
                {
                    "worker": "gaia_panstarrs",
                    "gaia_bundle": str(gaia_json),
                    "panstarrs_bundle": str(pan_json),
                    "max_sep_arcsec": 2.0,
                    "pm_threshold_masyr": 10.0,
                    "min_detections": 3,
                },
            ],
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        loaded = load_manifest(manifest_path)
        output_dir = tmp_path / "replay_out"
        report = run_manifest(
            loaded,
            output_dir=output_dir,
            db_path=db_path,
            agent_log_path=agent_log_path,
            ingest=False,
        )

        assert report["completed_steps"] == 1, report
        assert report["results"][0]["worker"] == "gaia_panstarrs", report
        assert Path(report["results"][0]["bundle_path"]).exists(), report


def main():
    test_load_manifest_rejects_missing_steps()
    test_load_manifest_rejects_unsupported_worker()
    test_run_manifest_replays_gaia_panstarrs()
    print("replay manifest tests passed")


if __name__ == "__main__":
    main()
