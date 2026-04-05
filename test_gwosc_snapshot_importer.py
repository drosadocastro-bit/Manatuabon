import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from gwosc_snapshot_importer import (
    build_gwosc_snapshot_bundle,
    collect_gwosc_event_version_snapshot,
    extract_event_version_id,
    ingest_gwosc_bundle,
    write_gwosc_snapshot_files,
)


GWOSC_EVENT = {
    "name": "GW241110_124123",
    "gps": 1415277701.7,
    "version": 1,
    "catalog": "O4_Discovery_Papers",
    "run": "O4b2Disc",
    "grace_id": "S241110br",
    "aliases": ["GW241110_124123"],
    "doi": "https://doi.org/10.7935/46xh-t016",
    "detectors": ["H1", "L1", "V1"],
    "parameters_url": "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1/parameters?format=api",
    "timelines_url": "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1/timelines?format=api",
    "strain_files_url": "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1/strain-files?format=api",
}

GWOSC_PARAMETERS = {
    "results": [
        {"name": "mass_1_source", "value": 34.2},
        {"name": "mass_2_source", "value": 28.1},
    ]
}

GWOSC_TIMELINES = {
    "results": [
        {"detector": "H1", "segment_start": 1415277690, "segment_end": 1415277710},
    ]
}

GWOSC_STRAIN_FILES = {
    "results": [
        {"detector": "H1", "url": "https://gwosc.org/strain/H1.hdf5"},
        {"detector": "L1", "url": "https://gwosc.org/strain/L1.hdf5"},
    ]
}


def fake_fetcher(url: str):
    payloads = {
        "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1?format=json": GWOSC_EVENT,
        "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1/parameters?format=api": GWOSC_PARAMETERS,
        "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1/timelines?format=api": GWOSC_TIMELINES,
        "https://gwosc.org/api/v2/event-versions/GW241110_124123-v1/strain-files?format=api": GWOSC_STRAIN_FILES,
    }
    if url not in payloads:
        raise AssertionError(f"Unexpected URL: {url}")
    return payloads[url]


def test_extract_event_version_id_from_url():
    value = extract_event_version_id("https://gwosc.org/api/v2/event-versions/GW241110_124123-v1?format=json")
    assert value == "GW241110_124123-v1", value


def test_collect_gwosc_event_version_snapshot_captures_related_metadata():
    snapshot = collect_gwosc_event_version_snapshot("GW241110_124123-v1", fetcher=fake_fetcher)

    assert snapshot["kind"] == "event_version", snapshot
    assert snapshot["record"]["grace_id"] == "S241110br", snapshot
    assert "parameters" in snapshot["related"], snapshot
    assert "strain_files" in snapshot["related"], snapshot
    assert snapshot["errors"] == {}, snapshot


def test_direct_gwosc_bundle_ingest_populates_runtime_db():
    snapshot = collect_gwosc_event_version_snapshot("GW241110_124123-v1", fetcher=fake_fetcher)
    bundle = build_gwosc_snapshot_bundle(snapshot, hypothesis_focus="GW release review")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_gwosc_snapshot_files(snapshot, bundle, tmp_path, "GW241110_124123-v1")

        result = ingest_gwosc_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path

    assert result is not None, result
    assert result["id"] == 1, result
    assert any(item["summary"].startswith("Structured GWOSC snapshot bundle") for item in memories), memories
    assert any(hypothesis["id"] == "AUTO-1" for hypothesis in hypotheses), hypotheses


def test_evidence_only_gwosc_bundle_skips_auto_hypothesis_generation():
    snapshot = collect_gwosc_event_version_snapshot("GW241110_124123-v1", fetcher=fake_fetcher)
    bundle = build_gwosc_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    assert bundle["new_hypothesis"] is None, bundle

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_gwosc_snapshot_files(snapshot, bundle, tmp_path, "GW241110_124123-v1")

        result = ingest_gwosc_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

    assert result is not None, result
    assert result.get("hypothesis_generated") is None, result
    assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_extract_event_version_id_from_url()
    test_collect_gwosc_event_version_snapshot_captures_related_metadata()
    test_direct_gwosc_bundle_ingest_populates_runtime_db()
    test_evidence_only_gwosc_bundle_skips_auto_hypothesis_generation()
    print("gwosc snapshot importer tests passed")


if __name__ == "__main__":
    main()