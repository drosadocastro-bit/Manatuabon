import json
import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import IngestAgent, MemoryManager
from pulsar_glitch_importer import ensure_canonical_hypothesis, resolve_canonical_rule, write_bundle
from pulsar_recovery_paper_importer import VELA_RECOVERY_PAPER_2506_02100V1, build_recovery_paper_bundle


class GuardNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


class FakeAgentLog:
    def __init__(self):
        self.events = []

    def add(self, action, details="", extra=None):
        self.events.append((action, details, extra or {}))


def test_build_recovery_paper_bundle_contains_time_domain_measurements():
    bundle = build_recovery_paper_bundle(VELA_RECOVERY_PAPER_2506_02100V1, supports_hypothesis="H19")

    assert bundle["manatuabon_schema"] == "structured_ingest_v1", bundle
    assert bundle["payload_type"] == "pulsar_recovery_paper_bundle", bundle
    assert bundle["supports_hypothesis"] == "H19", bundle
    assert bundle["new_hypothesis"] is None, bundle
    assert 616.34 in bundle["structured_evidence"]["time_domain_measurements"]["recovery_tau_days"], bundle
    assert 314.1 in bundle["structured_evidence"]["time_domain_measurements"]["residual_period_days"], bundle


def test_recovery_paper_bundle_ingests_and_syncs_h19():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        ensure_runtime_db(db_path, migrate=False).close()

        rule = resolve_canonical_rule("PSR B0833-45", "Crustal Memory")
        canonical_id = ensure_canonical_hypothesis(db_path, rule)
        bundle = build_recovery_paper_bundle(VELA_RECOVERY_PAPER_2506_02100V1, supports_hypothesis=canonical_id)
        json_path, _ = write_bundle(bundle, tmp_path, "PSR B0833-45", filename_prefix="pulsar_recovery_paper_bundle")

        memory = MemoryManager(db_path)
        agent = IngestAgent(GuardNemotron(), memory, FakeAgentLog())
        result = agent.ingest_file(json_path)
        hypotheses = memory.get_all_hypotheses(normalized=True)
        h19 = next(h for h in hypotheses if h["id"] == canonical_id)
        del agent
        del memory

    assert result is not None, result
    assert result["supports_hypothesis"] == canonical_id, result
    assert "pulsars" in result["domain_tags"], result
    assert "Structured evidence bundle for Vela Pulsar from arXiv:2506.02100v1" in h19["evidence"], h19
    assert "pulsars" in h19["context_domains"], h19


def main():
    test_build_recovery_paper_bundle_contains_time_domain_measurements()
    test_recovery_paper_bundle_ingests_and_syncs_h19()
    print("pulsar recovery paper importer tests passed")


if __name__ == "__main__":
    main()