import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from sdss_snapshot_importer import (
    build_sdss_snapshot_bundle,
    collect_sdss_snapshot,
    ingest_sdss_bundle,
    write_sdss_snapshot_files,
)


SQL_RESPONSE = [
    {
        "Rows": [
            {
                "objID": 1001,
                "ra": 266.4168,
                "dec": -29.0078,
                "z": 0.12,
                "zErr": 0.01,
                "petroMag_r": 17.1,
                "petroMag_g": 18.0,
                "type": 3,
                "subClass": "STARFORMING",
                "velDisp": 210.0,
                "velDispErr": 12.0,
            },
            {
                "objID": 1002,
                "ra": 266.4000,
                "dec": -29.0100,
                "z": 0.18,
                "zErr": 0.02,
                "petroMag_r": 18.4,
                "petroMag_g": 19.1,
                "type": 3,
                "subClass": "AGN",
                "velDisp": 190.0,
                "velDispErr": 15.0,
            },
        ]
    }
]

RADIAL_RESPONSE = [
    {
        "Rows": [
            {
                "objID": 2001,
                "ra": 266.4100,
                "dec": -29.0000,
                "z": None,
                "zErr": None,
                "petroMag_r": 20.0,
                "petroMag_g": 20.5,
                "type": 3,
                "subClass": "GALAXY",
                "velDisp": None,
                "velDispErr": None,
            }
        ]
    }
]

SPECTRO_RESPONSE = [
    {
        "Rows": [
            {
                "objID": 3001,
                "ra": 210.8024,
                "dec": 54.3487,
                "z": 0.03,
                "zErr": 0.002,
                "petroMag_r": 16.5,
                "petroMag_g": 17.1,
                "type": 3,
                "subClass": "GALAXY",
                "velDisp": 175.0,
                "velDispErr": 9.0,
            }
        ]
    }
]


def fake_fetcher_factory(sql_should_fail: bool = False):
    def fake_fetcher(url: str, params: dict):
        if "SqlSearch" in url:
            if sql_should_fail:
                raise RuntimeError("sql endpoint unavailable")
            return SQL_RESPONSE
        if "RadialSearch" in url:
            return RADIAL_RESPONSE
        raise AssertionError(url)
    return fake_fetcher


def test_collect_sdss_snapshot_uses_sql_when_available():
    snapshot = collect_sdss_snapshot(
        ra_center=266.4168,
        dec_center=-29.0078,
        radius_arcmin=60.0,
        max_results=50,
        object_type="galaxy",
        fetcher=fake_fetcher_factory(sql_should_fail=False),
    )

    assert snapshot["query"]["query_mode"] == "sql", snapshot
    assert len(snapshot["rows"]) == 2, snapshot
    assert snapshot["rows"][0]["objID"] == 1001, snapshot


def test_collect_sdss_snapshot_falls_back_to_radial_search():
    snapshot = collect_sdss_snapshot(
        ra_center=266.4168,
        dec_center=-29.0078,
        radius_arcmin=60.0,
        max_results=50,
        object_type="galaxy",
        fetcher=fake_fetcher_factory(sql_should_fail=True),
    )

    assert snapshot["query"]["query_mode"] == "radial_search", snapshot
    assert "sql" in snapshot["errors"], snapshot
    assert len(snapshot["rows"]) == 1, snapshot


def test_collect_sdss_snapshot_relaxes_empty_typed_query_before_radial_fallback():
    calls = []

    def fake_fetcher(url: str, params: dict):
        calls.append((url, params))
        if "SqlSearch" in url:
            if len([item for item in calls if "SqlSearch" in item[0]]) == 1:
                return [{"Rows": []}]
            return SQL_RESPONSE
        if "RadialSearch" in url:
            raise AssertionError("radial fallback should not be needed when relaxed sql returns rows")
        raise AssertionError(url)

    snapshot = collect_sdss_snapshot(
        ra_center=266.4168,
        dec_center=-29.0078,
        radius_arcmin=60.0,
        max_results=50,
        object_type="galaxy",
        fetcher=fake_fetcher,
    )

    assert snapshot["query"]["query_mode"] == "sql_relaxed", snapshot
    assert "sql_empty" in snapshot["errors"], snapshot
    assert len(snapshot["rows"]) == 2, snapshot


def test_collect_sdss_snapshot_uses_spectro_join_when_photo_queries_are_empty():
    def fake_fetcher(url: str, params: dict):
        if "SqlSearch" in url:
            if "JOIN SpecObj" in params["cmd"]:
                return SPECTRO_RESPONSE
            return [{"Rows": []}]
        if "RadialSearch" in url:
            raise AssertionError("radial fallback should not be needed when spectro join returns rows")
        raise AssertionError(url)

    snapshot = collect_sdss_snapshot(
        ra_center=210.802429,
        dec_center=54.348750,
        radius_arcmin=30.0,
        max_results=25,
        object_type="galaxy",
        fetcher=fake_fetcher,
    )

    assert snapshot["query"]["query_mode"] == "sql_spectro", snapshot
    assert len(snapshot["rows"]) == 1, snapshot
    assert snapshot["rows"][0]["objID"] == 3001, snapshot


def test_direct_sdss_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_sdss_snapshot(
        ra_center=266.4168,
        dec_center=-29.0078,
        radius_arcmin=60.0,
        max_results=50,
        object_type="galaxy",
        fetcher=fake_fetcher_factory(sql_should_fail=False),
    )
    bundle = build_sdss_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_sdss_snapshot_files(snapshot, bundle, tmp_path, "sdss_test")

        result = ingest_sdss_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result is not None, result
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Structured SDSS snapshot bundle") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_sdss_snapshot_uses_sql_when_available()
    test_collect_sdss_snapshot_falls_back_to_radial_search()
    test_collect_sdss_snapshot_relaxes_empty_typed_query_before_radial_fallback()
    test_collect_sdss_snapshot_uses_spectro_join_when_photo_queries_are_empty()
    test_direct_sdss_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("sdss snapshot importer tests passed")


if __name__ == "__main__":
    main()