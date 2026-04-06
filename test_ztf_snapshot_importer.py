import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from ztf_snapshot_importer import (
    build_ztf_snapshot_bundle,
    collect_ztf_snapshot,
    ingest_ztf_bundle,
    write_ztf_snapshot_files,
)


ZTF_CSV = """field,ccdid,qid,filtercode,imgtypecode,obsjd,seeing,maglimit,ra,dec,infobits,pid
123,4,2,zr,o,2460123.1,2.1,20.5,187.70593,12.39112,0,987654321
123,4,3,zg,o,2460124.2,1.9,20.8,187.70800,12.39200,0,987654322
"""


def fake_fetcher(url: str, params: dict):
    assert "ztf/products/sci" in url.lower(), url
    assert params["ct"] == "csv", params
    return ZTF_CSV


def test_collect_ztf_snapshot_parses_csv_rows():
    snapshot = collect_ztf_snapshot(
        ra_center=187.70593,
        dec_center=12.39112,
        size_deg=0.1,
        max_results=25,
        intersect="OVERLAPS",
        fetcher=fake_fetcher,
    )

    assert snapshot["summary"]["returned_count"] == 2, snapshot
    assert snapshot["summary"]["filter_counts"]["zr"] == 1, snapshot
    assert snapshot["summary"]["seeing_count"] == 2, snapshot
    assert snapshot["frames"][0]["field"] == "123", snapshot


def test_direct_ztf_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_ztf_snapshot(
        ra_center=187.70593,
        dec_center=12.39112,
        size_deg=0.1,
        max_results=25,
        intersect="OVERLAPS",
        fetcher=fake_fetcher,
    )
    bundle = build_ztf_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_ztf_snapshot_files(snapshot, bundle, tmp_path, "ztf_test")

        result = ingest_ztf_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path
        assert result is not None, result
        assert result["id"] == 1, result
        assert result.get("hypothesis_generated") is None, result
        assert any(item["summary"].startswith("Structured ZTF snapshot bundle") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_ztf_snapshot_parses_csv_rows()
    test_direct_ztf_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("ztf snapshot importer tests passed")


if __name__ == "__main__":
    main()