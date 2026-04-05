import tempfile
from pathlib import Path

from manatuabon_agent import IngestAgent, MemoryManager
from db_init import ensure_runtime_db
from arxiv_snapshot_importer import (
    build_arxiv_snapshot_bundle,
    ingest_arxiv_bundle,
    parse_arxiv_atom,
    write_arxiv_snapshot_files,
)


SAMPLE_ARXIV_ATOM = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/"
      xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query: search_query=all:vela&amp;id_list=&amp;start=0&amp;max_results=2</title>
  <id>http://arxiv.org/api/testquery</id>
  <updated>2026-04-05T00:00:00-04:00</updated>
  <opensearch:totalResults>2</opensearch:totalResults>
  <opensearch:startIndex>0</opensearch:startIndex>
  <opensearch:itemsPerPage>2</opensearch:itemsPerPage>
  <entry>
    <id>http://arxiv.org/abs/2506.02100v1</id>
    <updated>2025-06-02T00:00:00Z</updated>
    <published>2025-06-02T00:00:00Z</published>
    <title>Post-glitch Recovery and the Neutron Star Structure: The Vela Pulsar</title>
    <summary>We analyze Vela glitch recoveries with day-scale timescales.</summary>
    <author><name>H. Grover</name><arxiv:affiliation>IIT Roorkee</arxiv:affiliation></author>
    <author><name>E. Gugercinoglu</name></author>
    <link href="http://arxiv.org/abs/2506.02100v1" rel="alternate" type="text/html" />
    <link title="pdf" href="http://arxiv.org/pdf/2506.02100v1" rel="related" type="application/pdf" />
    <arxiv:primary_category term="astro-ph.HE" scheme="http://arxiv.org/schemas/atom" />
    <category term="astro-ph.HE" scheme="http://arxiv.org/schemas/atom" />
    <arxiv:comment>15 pages, 10 figures</arxiv:comment>
    <arxiv:journal_ref>Example Journal 42 (2026)</arxiv:journal_ref>
    <arxiv:doi>10.1234/example</arxiv:doi>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2502.06704v2</id>
    <updated>2025-02-10T00:00:00Z</updated>
    <published>2025-02-01T00:00:00Z</published>
    <title>Another Vela Timing Study</title>
    <summary>Timing noise and glitch residuals in Vela.</summary>
    <author><name>A. Researcher</name></author>
    <link href="http://arxiv.org/abs/2502.06704v2" rel="alternate" type="text/html" />
    <link title="pdf" href="http://arxiv.org/pdf/2502.06704v2" rel="related" type="application/pdf" />
    <arxiv:primary_category term="astro-ph.HE" scheme="http://arxiv.org/schemas/atom" />
    <category term="astro-ph.HE" scheme="http://arxiv.org/schemas/atom" />
  </entry>
</feed>
"""


class GuardNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


class FakeAgentLog:
    def add(self, action, details="", extra=None):
        pass


def test_parse_arxiv_atom_extracts_entries():
    parsed = parse_arxiv_atom(SAMPLE_ARXIV_ATOM)

    assert parsed["total_results"] == 2, parsed
    assert len(parsed["entries"]) == 2, parsed
    assert parsed["entries"][0]["id"] == "2506.02100", parsed
    assert parsed["entries"][0]["versioned_id"] == "2506.02100v1", parsed
    assert parsed["entries"][0]["primary_category"] == "astro-ph.HE", parsed
    assert parsed["entries"][0]["authors"][0]["affiliation"] == "IIT Roorkee", parsed


def test_arxiv_snapshot_bundle_ingests_without_llm():
    parsed = parse_arxiv_atom(SAMPLE_ARXIV_ATOM)
    snapshot = {
        "source": "arXiv API",
        "fetched_at": "2026-04-05T00:00:00Z",
        "acknowledgement": "Thank you to arXiv for use of its open access interoperability.",
        "request": {
            "search_query": "all:vela pulsar glitch",
            "id_list": [],
            "start": 0,
            "max_results": 2,
            "page_size": 2,
            "sort_by": "submittedDate",
            "sort_order": "descending",
            "request_delay_seconds": 3.0,
        },
        "feed": {
            "title": parsed["title"],
            "id": parsed["id"],
            "updated": parsed["updated"],
            "total_results": parsed["total_results"],
            "pages_fetched": 1,
        },
        "entries": parsed["entries"],
        "raw_pages": [{"canonical_url": "https://export.arxiv.org/api/query?search_query=all:vela", "xml": SAMPLE_ARXIV_ATOM}],
    }
    bundle = build_arxiv_snapshot_bundle(snapshot, supports_hypothesis="H19", hypothesis_focus="Crustal Memory")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        ensure_runtime_db(db_path, migrate=False).close()
        raw_path, bundle_json, _ = write_arxiv_snapshot_files(snapshot, bundle, tmp_path, "vela")
        assert raw_path.exists(), raw_path
        assert bundle_json.exists(), bundle_json

        memory = MemoryManager(db_path)
        agent = IngestAgent(GuardNemotron(), memory, FakeAgentLog())
        result = agent.ingest_file(bundle_json)
        memories = memory.get_memories()
        del agent
        del memory

    assert result is not None, result
    assert result["supports_hypothesis"] == "H19", result
    assert any(memory["summary"].startswith("Structured arXiv snapshot bundle") for memory in memories), memories


def test_evidence_only_arxiv_bundle_skips_auto_hypothesis_generation():
    parsed = parse_arxiv_atom(SAMPLE_ARXIV_ATOM)
    snapshot = {
        "source": "arXiv API",
        "fetched_at": "2026-04-05T00:00:00Z",
        "acknowledgement": "Thank you to arXiv for use of its open access interoperability.",
        "request": {
            "search_query": "all:vela pulsar glitch",
            "id_list": [],
            "start": 0,
            "max_results": 2,
            "page_size": 2,
            "sort_by": "submittedDate",
            "sort_order": "descending",
            "request_delay_seconds": 3.0,
        },
        "feed": {
            "title": parsed["title"],
            "id": parsed["id"],
            "updated": parsed["updated"],
            "total_results": parsed["total_results"],
            "pages_fetched": 1,
        },
        "entries": parsed["entries"],
        "raw_pages": [{"canonical_url": "https://export.arxiv.org/api/query?search_query=all:vela", "xml": SAMPLE_ARXIV_ATOM}],
    }
    bundle = build_arxiv_snapshot_bundle(snapshot, allow_new_hypothesis=False)

    assert bundle["new_hypothesis"] is None, bundle

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_arxiv_snapshot_files(snapshot, bundle, tmp_path, "vela")

        memory = MemoryManager(db_path)
        agent = IngestAgent(GuardNemotron(), memory, FakeAgentLog())
        result = agent.ingest_file(bundle_json)
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del agent
        del memory

    assert result is not None, result
    assert result.get("hypothesis_generated") is None, result
    assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def test_direct_arxiv_bundle_ingest_populates_runtime_db():
    parsed = parse_arxiv_atom(SAMPLE_ARXIV_ATOM)
    snapshot = {
        "source": "arXiv API",
        "fetched_at": "2026-04-05T00:00:00Z",
        "acknowledgement": "Thank you to arXiv for use of its open access interoperability.",
        "request": {
            "search_query": "all:vela pulsar glitch",
            "id_list": [],
            "start": 0,
            "max_results": 2,
            "page_size": 2,
            "sort_by": "submittedDate",
            "sort_order": "descending",
            "request_delay_seconds": 3.0,
        },
        "feed": {
            "title": parsed["title"],
            "id": parsed["id"],
            "updated": parsed["updated"],
            "total_results": parsed["total_results"],
            "pages_fetched": 1,
        },
        "entries": parsed["entries"],
        "raw_pages": [{"canonical_url": "https://export.arxiv.org/api/query?search_query=all:vela", "xml": SAMPLE_ARXIV_ATOM}],
    }
    bundle = build_arxiv_snapshot_bundle(snapshot, hypothesis_focus="Crustal Memory")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_arxiv_snapshot_files(snapshot, bundle, tmp_path, "vela")

        result = ingest_arxiv_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path

    assert result is not None, result
    assert result["id"] == 1, result
    assert any(item["summary"].startswith("Structured arXiv snapshot bundle") for item in memories), memories
    assert any(hypothesis["id"] == "AUTO-1" for hypothesis in hypotheses), hypotheses


def main():
    test_parse_arxiv_atom_extracts_entries()
    test_arxiv_snapshot_bundle_ingests_without_llm()
    test_evidence_only_arxiv_bundle_skips_auto_hypothesis_generation()
    test_direct_arxiv_bundle_ingest_populates_runtime_db()
    print("arxiv snapshot importer tests passed")


if __name__ == "__main__":
    main()