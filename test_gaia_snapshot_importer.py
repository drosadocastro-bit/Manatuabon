import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from gaia_snapshot_importer import (
    build_gaia_snapshot_bundle,
    collect_gaia_snapshot,
    ingest_gaia_bundle,
    write_gaia_snapshot_files,
)
from manatuabon_agent import MemoryManager


GAIA_RESPONSE = {
    "metadata": [
        {"name": "source_id"},
        {"name": "ra"},
        {"name": "dec"},
        {"name": "parallax"},
        {"name": "parallax_error"},
        {"name": "pmra"},
        {"name": "pmra_error"},
        {"name": "pmdec"},
        {"name": "pmdec_error"},
        {"name": "radial_velocity"},
        {"name": "radial_velocity_error"},
        {"name": "phot_g_mean_mag"},
        {"name": "bp_rp"},
        {"name": "ruwe"},
    ],
    "data": [
        [101, 266.4168, -29.0078, 0.50, 0.05, 12.3, 0.4, -3.1, 0.5, 25.0, 1.2, 15.2, 1.1, 1.02],
        [102, 266.4210, -29.0105, 0.20, 0.03, 4.5, 0.3, 1.4, 0.2, None, None, 17.8, 0.9, 1.18],
    ],
}


def fake_fetcher(url: str, params: dict):
    assert "tap-server" in url, url
    assert params["FORMAT"] == "json", params
    assert "SELECT TOP 25" in params["QUERY"], params["QUERY"]
    return GAIA_RESPONSE


def test_collect_gaia_snapshot_parses_tap_rows():
    snapshot = collect_gaia_snapshot(
        ra_center=266.4168,
        dec_center=-29.0078,
        radius_deg=0.25,
        max_results=25,
        fetcher=fake_fetcher,
    )

    assert snapshot["query"]["query_mode"] == "tap_sync", snapshot
    assert snapshot["summary"]["returned_count"] == 2, snapshot
    assert snapshot["summary"]["proper_motion_count"] == 2, snapshot
    assert snapshot["summary"]["radial_velocity_count"] == 1, snapshot
    assert snapshot["stars"][0]["source_id"] == "101", snapshot


def test_direct_gaia_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_gaia_snapshot(
        ra_center=266.4168,
        dec_center=-29.0078,
        radius_deg=0.25,
        max_results=25,
        fetcher=fake_fetcher,
    )
    bundle = build_gaia_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_gaia_snapshot_files(snapshot, bundle, tmp_path, "gaia_test")

        result = ingest_gaia_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result is not None, result
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Structured Gaia snapshot bundle") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_gaia_snapshot_parses_tap_rows()
    test_direct_gaia_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("gaia snapshot importer tests passed")


if __name__ == "__main__":
    main()