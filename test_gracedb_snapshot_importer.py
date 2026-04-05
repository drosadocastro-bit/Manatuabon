import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import IngestAgent, MemoryManager
from gracedb_snapshot_importer import (
    build_gracedb_snapshot_bundle,
    collect_gracedb_query_snapshot,
    collect_gracedb_superevent_snapshot,
    ingest_gracedb_bundle,
    write_gracedb_snapshot_files,
)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeGraceDb:
    def superevent(self, superevent_id):
        return FakeResponse({
            "superevent_id": superevent_id,
            "preferred_event": "G123456",
            "gw_id": "GWTEST1",
            "category": "test",
            "t_0": 1234567890.0,
        })

    def labels(self, object_id):
        return FakeResponse({"labels": [{"name": "EM_READY"}, {"name": "DQV"}]})

    def logs(self, object_id):
        return FakeResponse({"log": [{"N": 1, "comment": "Initial analyst note", "created": "2026-04-05T00:00:00Z"}]})

    def files(self, object_id):
        return FakeResponse({"skymap.fits.gz": "https://example/skymap.fits.gz", "coinc.xml": "https://example/coinc.xml"})

    def voevents(self, object_id):
        return FakeResponse({"voevents": [{"N": 3, "voevent_type": "initial", "filename": "voevent.xml"}]})

    def emobservations(self, object_id):
        return FakeResponse({"observations": [{"N": 2, "group": "ZTF", "comment": "follow-up"}]})

    def signoffs(self, object_id):
        return FakeResponse({"signoffs": [{"signoff_type": "ADV", "status": "OK", "comment": "ready"}]})

    def events(self, query=None, max_results=None):
        return iter([
            {"graceid": "G123456", "pipeline": "gstlal", "far": 1.2e-8},
        ][:max_results])

    def superevents(self, query='', max_results=None):
        return iter([
            {"superevent_id": "S190101a", "preferred_event": "G123456", "category": "production"},
            {"superevent_id": "S190102b", "preferred_event": "G123457", "category": "test"},
        ][:max_results])


class GuardNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


class FakeAgentLog:
    def add(self, action, details="", extra=None):
        pass


def test_collect_superevent_snapshot_captures_related_metadata():
    snapshot = collect_gracedb_superevent_snapshot(FakeGraceDb(), "S190101a")

    assert snapshot["kind"] == "superevent", snapshot
    assert snapshot["record"]["preferred_event"] == "G123456", snapshot
    assert "labels" in snapshot["related"], snapshot
    assert "files" in snapshot["related"], snapshot
    assert snapshot["errors"] == {}, snapshot


def test_query_snapshot_bundle_ingests_without_llm():
    snapshot = collect_gracedb_query_snapshot(FakeGraceDb(), 'is_gw: True', mode='superevents', max_results=2)
    bundle = build_gracedb_snapshot_bundle(snapshot, hypothesis_focus="GW review")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        ensure_runtime_db(db_path, migrate=False).close()
        raw_path, bundle_json, _ = write_gracedb_snapshot_files(snapshot, bundle, tmp_path, "gw_query")
        assert raw_path.exists(), raw_path
        assert bundle_json.exists(), bundle_json

        memory = MemoryManager(db_path)
        agent = IngestAgent(GuardNemotron(), memory, FakeAgentLog())
        result = agent.ingest_file(bundle_json)
        memories = memory.get_memories()
        del agent
        del memory

    assert result is not None, result
    assert result["summary"].startswith("Structured GraceDB snapshot bundle"), result
    assert any(memory["summary"].startswith("Structured GraceDB snapshot bundle") for memory in memories), memories


def test_direct_gracedb_bundle_ingest_populates_runtime_db():
    snapshot = collect_gracedb_query_snapshot(FakeGraceDb(), 'is_gw: True', mode='superevents', max_results=2)
    bundle = build_gracedb_snapshot_bundle(snapshot, hypothesis_focus="GW review")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_gracedb_snapshot_files(snapshot, bundle, tmp_path, "gw_query")

        result = ingest_gracedb_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path

    assert result is not None, result
    assert result["id"] == 1, result
    assert any(item["summary"].startswith("Structured GraceDB snapshot bundle") for item in memories), memories
    assert any(hypothesis["id"] == "AUTO-1" for hypothesis in hypotheses), hypotheses


def test_evidence_only_gracedb_bundle_skips_auto_hypothesis_generation():
    snapshot = collect_gracedb_query_snapshot(FakeGraceDb(), 'is_gw: True', mode='superevents', max_results=2)
    bundle = build_gracedb_snapshot_bundle(
        snapshot,
        hypothesis_focus="GW review",
        allow_new_hypothesis=False,
    )

    assert bundle["new_hypothesis"] is None, bundle

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_gracedb_snapshot_files(snapshot, bundle, tmp_path, "gw_query")

        result = ingest_gracedb_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

    assert result is not None, result
    assert result.get("hypothesis_generated") is None, result
    assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_superevent_snapshot_captures_related_metadata()
    test_query_snapshot_bundle_ingests_without_llm()
    test_direct_gracedb_bundle_ingest_populates_runtime_db()
    test_evidence_only_gracedb_bundle_skips_auto_hypothesis_generation()
    print("gracedb snapshot importer tests passed")


if __name__ == "__main__":
    main()