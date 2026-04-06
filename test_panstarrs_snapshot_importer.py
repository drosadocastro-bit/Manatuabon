import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from panstarrs_snapshot_importer import (
    build_panstarrs_snapshot_bundle,
    collect_panstarrs_snapshot,
    ingest_panstarrs_bundle,
    write_panstarrs_snapshot_files,
)


PANSTARRS_RESPONSE = [
    {
        "objID": 111,
        "raMean": 187.70593,
        "decMean": 12.39112,
        "nDetections": 8,
        "gMeanPSFMag": 20.1,
        "rMeanPSFMag": 19.7,
        "iMeanPSFMag": 19.4,
        "zMeanPSFMag": 19.2,
        "yMeanPSFMag": 19.1,
        "qualityFlag": 0,
        "objInfoFlag": 0,
    },
    {
        "objID": 222,
        "raMean": 187.71000,
        "decMean": 12.39500,
        "nDetections": 2,
        "gMeanPSFMag": None,
        "rMeanPSFMag": 21.1,
        "iMeanPSFMag": 20.6,
        "zMeanPSFMag": None,
        "yMeanPSFMag": None,
        "qualityFlag": 4,
        "objInfoFlag": 16,
    },
]

PANSTARRS_STACK_RESPONSE = [
    {
        "objID": 333,
        "raStack": 210.80245,
        "decStack": 54.34876,
        "primaryDetection": 1,
        "gPSFMag": 19.9,
        "rPSFMag": 19.3,
        "iPSFMag": 19.0,
        "zPSFMag": 18.8,
        "yPSFMag": 18.7,
        "qualityFlag": 0,
        "objInfoFlag": 0,
    }
]

PANSTARRS_COLUMNAR_RESPONSE = {
    "info": [
        {"name": "objID"},
        {"name": "raMean"},
        {"name": "decMean"},
        {"name": "nDetections"},
        {"name": "gMeanPSFMag"},
        {"name": "rMeanPSFMag"},
        {"name": "iMeanPSFMag"},
        {"name": "zMeanPSFMag"},
        {"name": "yMeanPSFMag"},
        {"name": "qualityFlag"},
        {"name": "objInfoFlag"},
    ],
    "data": [
        [
            "173152107989689310",
            210.79889762721194,
            54.298860450276074,
            25,
            -999.0,
            17.790300369262695,
            19.863500595092773,
            19.823400497436523,
            19.96500015258789,
            53,
            444915712,
        ]
    ],
}


def fake_fetcher(url: str, params: dict):
    assert "panstarrs" in url.lower(), url
    assert params["pagesize"] == 25, params
    return PANSTARRS_RESPONSE


def test_collect_panstarrs_snapshot_parses_rows():
    snapshot = collect_panstarrs_snapshot(
        ra_center=187.70593,
        dec_center=12.39112,
        radius_deg=0.05,
        max_results=25,
        min_detections=1,
        fetcher=fake_fetcher,
    )

    assert snapshot["summary"]["returned_count"] == 2, snapshot
    assert snapshot["summary"]["multiband_count"] == 1, snapshot
    assert snapshot["summary"]["band_counts"]["r"] == 2, snapshot
    assert snapshot["objects"][0]["objID"] == 111, snapshot


def test_collect_panstarrs_snapshot_parses_columnar_data_rows():
    snapshot = collect_panstarrs_snapshot(
        ra_center=210.802429,
        dec_center=54.348750,
        radius_deg=0.05,
        max_results=25,
        min_detections=0,
        fetcher=lambda url, params: PANSTARRS_COLUMNAR_RESPONSE,
    )

    assert snapshot["summary"]["returned_count"] == 1, snapshot
    assert snapshot["objects"][0]["objID"] == "173152107989689310", snapshot
    assert snapshot["objects"][0]["rMeanPSFMag"] == 17.790300369262695, snapshot


def test_collect_panstarrs_snapshot_falls_back_to_stack_with_broader_radius():
    def fake_fallback_fetcher(url: str, params: dict):
        if "mean.json" in url:
            return []
        if "stack.json" in url:
            return PANSTARRS_STACK_RESPONSE
        raise AssertionError(url)

    snapshot = collect_panstarrs_snapshot(
        ra_center=210.802429,
        dec_center=54.348750,
        radius_deg=0.00833333,
        max_results=25,
        min_detections=1,
        fetcher=fake_fallback_fetcher,
    )

    assert snapshot["query"]["query_mode"] == "stack_relaxed", snapshot
    assert snapshot["query"]["catalog"] == "stack", snapshot
    assert snapshot["query"]["radius_deg_used"] > snapshot["query"]["radius_deg"], snapshot
    assert snapshot["summary"]["returned_count"] == 1, snapshot
    assert snapshot["objects"][0]["objID"] == 333, snapshot


def test_direct_panstarrs_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_panstarrs_snapshot(
        ra_center=187.70593,
        dec_center=12.39112,
        radius_deg=0.05,
        max_results=25,
        min_detections=1,
        fetcher=fake_fetcher,
    )
    bundle = build_panstarrs_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_panstarrs_snapshot_files(snapshot, bundle, tmp_path, "panstarrs_test")

        result = ingest_panstarrs_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result is not None, result
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Structured Pan-STARRS snapshot bundle") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_panstarrs_snapshot_parses_rows()
    test_collect_panstarrs_snapshot_parses_columnar_data_rows()
    test_collect_panstarrs_snapshot_falls_back_to_stack_with_broader_radius()
    test_direct_panstarrs_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("panstarrs snapshot importer tests passed")


if __name__ == "__main__":
    main()