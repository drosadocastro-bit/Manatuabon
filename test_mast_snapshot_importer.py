import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from mast_snapshot_importer import (
    build_mast_snapshot_bundle,
    collect_mast_snapshot,
    ingest_mast_bundle,
    write_mast_snapshot_files,
)


FAKE_ROWS = [
    {
        "obs_id": "jwst-001",
        "target_name": "M87",
        "obs_collection": "JWST",
        "instrument_name": "NIRCam",
        "filters": "F200W",
        "t_exptime": 1200.0,
        "s_ra": 187.70593,
        "s_dec": 12.39112,
        "t_min": 60300.2,
        "proposal_id": "1234",
        "data_rights": "PUBLIC",
    },
    {
        "obs_id": "hst-002",
        "target_name": "M87",
        "obs_collection": "HST",
        "instrument_name": "WFC3/UVIS",
        "filters": "F606W",
        "t_exptime": 900.0,
        "s_ra": 187.70590,
        "s_dec": 12.39110,
        "t_min": 60200.1,
        "proposal_id": "5678",
        "data_rights": "PUBLIC",
    },
    {
        "obs_id": "other-003",
        "target_name": "M87",
        "obs_collection": "GALEX",
        "instrument_name": "GALEX",
        "filters": "NUV",
        "t_exptime": 300.0,
        "s_ra": 187.7,
        "s_dec": 12.39,
        "t_min": 60100.0,
        "proposal_id": "9999",
        "data_rights": "PUBLIC",
    },
]


def fake_fetcher(target: str, radius_deg: float):
    assert target == "M87", target
    assert radius_deg == 0.05, radius_deg
    return FAKE_ROWS


def test_collect_mast_snapshot_filters_to_requested_collections():
    snapshot = collect_mast_snapshot("M87", radius_deg=0.05, collections=["JWST", "HST"], max_results=10, fetcher=fake_fetcher)

    assert snapshot["summary"]["raw_count"] == 3, snapshot
    assert snapshot["summary"]["filtered_count"] == 2, snapshot
    assert snapshot["summary"]["returned_count"] == 2, snapshot
    assert snapshot["observations"][0]["obs_id"] == "jwst-001", snapshot


def test_direct_mast_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_mast_snapshot("M87", radius_deg=0.05, collections=["JWST", "HST"], max_results=10, fetcher=fake_fetcher)
    bundle = build_mast_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_mast_snapshot_files(snapshot, bundle, tmp_path, "M87")

        result = ingest_mast_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result is not None, result
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Structured MAST snapshot bundle") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_mast_snapshot_filters_to_requested_collections()
    test_direct_mast_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("mast snapshot importer tests passed")


if __name__ == "__main__":
    main()