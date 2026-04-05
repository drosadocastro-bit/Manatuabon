import json
import sqlite3
import tarfile
import tempfile
from pathlib import Path

from manatuabon_agent import IngestAgent
from db_init import ensure_runtime_db
from pulsar_glitch_importer import (
    build_evidence_bundle,
    canonicalize_existing_duplicates,
    ensure_canonical_hypothesis,
    extract_psrcat_package,
    resolve_canonical_rule,
    sync_canonical_hypothesis_support,
    write_bundle,
)


class GuardNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


class FakeMemory:
    def __init__(self):
        self.memories = []
        self.auto_hypotheses = []

    def get_founding_hypotheses(self):
        return []

    def add_memory(self, memory):
        memory_id = len(self.memories) + 1
        self.memories.append(dict(memory, id=memory_id))
        return memory_id

    def add_auto_hypothesis(self, hypothesis):
        self.auto_hypotheses.append(hypothesis)


class FakeAgentLog:
    def __init__(self):
        self.events = []

    def add(self, action, details="", extra=None):
        self.events.append((action, details, extra or {}))


def sample_atnf_rows():
    return [
        {
            "PSRJ": "J0835-4510",
            "PSRB": "B0833-45",
            "NAME": "Vela Pulsar",
            "P0": "0.08933",
            "P1": "1.25e-13",
            "AGE_KYR": "11.3",
            "DISTANCE_KPC": "0.29",
            "B_FIELD_GAUSS": "3.38e12",
        }
    ]


def sample_glitch_rows():
    return [
        {"pulsar": "B0833-45", "glitch_mjd": "40494", "delta_nu_over_nu": "2.3e-6", "permanent_fraction": "0.55", "reference": "Historic 1969 event"},
        {"pulsar": "B0833-45", "glitch_mjd": "51559", "delta_nu_over_nu": "3.1e-6", "permanent_fraction": "0.60", "reference": "Espinoza 2011"},
        {"pulsar": "Vela Pulsar", "glitch_mjd": "58859", "delta_nu_over_nu": "2.7e-6", "permanent_fraction": "0.66", "reference": "Lower 2021"},
    ]


def sample_psrcat_db_text():
    return """#CATALOGUE 2.7.0
PSRJ     J0835-4510                    ref1
PSRB     B0833-45                      ref1
P0       0.08933                       ref1
P1       1.25E-13                      ref1
DIST     0.29                          ref1
AGE      11300                         ref1
BSURF    3.38E12                       ref1
@-----------------------------------------------------------------
PSRJ     J0534+2200                    ref2
PSRB     B0531+21                      ref2
P0       0.033                         ref2
@-----------------------------------------------------------------
"""


def sample_glitch_db_text():
    return """Name                  J2000          Glitch Epoch    Frac Freq Incr Fract Freq Deriv Incr  Q              T_d            Ref.
                      Name              (MJD)           (E-9)              (E-3)                          (d)
____________________________________________________________________________________________________________________________________

B0833-45            J0835-4510        40494            2300(3)            -               0.55(2)       5.0(5)         ref1
B0833-45            J0835-4510        51559            3100(4)            -               0.60(2)       4.0(4)         ref2
B0833-45            J0835-4510        58859            2700(4)            -               0.66(3)       3.0(3)         ref3
"""


def test_build_evidence_bundle_generates_review_ready_payload():
    bundle = build_evidence_bundle(
        sample_atnf_rows(),
        sample_glitch_rows(),
        target="PSR B0833-45",
        hypothesis_focus="Crustal Memory",
    )

    assert bundle["manatuabon_schema"] == "structured_ingest_v1", bundle
    assert bundle["payload_type"] == "pulsar_glitch_evidence_bundle", bundle
    assert bundle["target"]["psrb"] == "B0833-45", bundle
    assert bundle["structured_evidence"]["glitch_summary"]["glitch_count"] == 3, bundle
    assert bundle["new_hypothesis"]["title"].startswith("Crustal Memory in"), bundle


def test_structured_bundle_ingests_without_llm():
    bundle = build_evidence_bundle(
        sample_atnf_rows(),
        sample_glitch_rows(),
        target="PSR B0833-45",
        hypothesis_focus="Crustal Memory",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        inbox_dir = Path(tmpdir)
        json_path, _ = write_bundle(bundle, inbox_dir, "PSR B0833-45")

        memory = FakeMemory()
        agent = IngestAgent(GuardNemotron(), memory, FakeAgentLog())
        result = agent.ingest_file(json_path)

    assert result is not None, result
    assert result["summary"].startswith("Structured evidence bundle for"), result
    assert result["domain_tags"] == ["pulsars"], result
    assert memory.auto_hypotheses, memory.auto_hypotheses
    assert memory.auto_hypotheses[0]["source_file"] == json_path.name, memory.auto_hypotheses[0]
    assert memory.auto_hypotheses[0]["source_memory_ids"] == [1], memory.auto_hypotheses[0]


def test_extract_psrcat_package_reads_embedded_databases():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        package_path = tmp_path / "psrcat_pkg.tar.gz"
        source_dir = tmp_path / "psrcat_tar"
        source_dir.mkdir()
        (source_dir / "psrcat.db").write_text(sample_psrcat_db_text(), encoding="utf-8")
        (source_dir / "glitch.db").write_text(sample_glitch_db_text(), encoding="utf-8")

        with tarfile.open(package_path, "w:gz") as archive:
            archive.add(source_dir / "psrcat.db", arcname="psrcat_tar/psrcat.db")
            archive.add(source_dir / "glitch.db", arcname="psrcat_tar/glitch.db")

        atnf_rows, glitch_rows = extract_psrcat_package(package_path)
        bundle = build_evidence_bundle(atnf_rows, glitch_rows, target="PSR B0833-45")

    assert bundle["target"]["psrb"] == "B0833-45", bundle
    assert bundle["structured_evidence"]["glitch_summary"]["glitch_count"] == 3, bundle
    assert bundle["structured_evidence"]["glitch_summary"]["mean_permanent_fraction"] == 0.603, bundle


def test_canonical_rule_seeds_h19_and_merges_duplicates():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "manatuabon.db"
        conn = ensure_runtime_db(db_path, migrate=False)
        conn.close()
        conn = sqlite3.connect(db_path)
        now = "2026-04-05T16:00:00"
        conn.execute(
            """
            INSERT INTO hypotheses (
                id, title, description, evidence, status, source, date, origin, root_id,
                created_at, updated_at, confidence, confidence_components, confidence_source,
                context_hypotheses, context_domains
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AUTO-410",
                "Crustal Memory in J0835-4510",
                "",
                json.dumps([], ensure_ascii=False),
                "needs_revision",
                "Agent Auto",
                now,
                "agent_auto",
                "AUTO-410",
                now,
                now,
                0.6,
                json.dumps({}, ensure_ascii=False),
                "proposal",
                json.dumps([], ensure_ascii=False),
                json.dumps(["pulsars"], ensure_ascii=False),
            ),
        )
        conn.execute(
            "INSERT INTO memories (timestamp, content, concept_tags, significance, domain_tags, supports_hypothesis, challenges_hypothesis) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                "Structured evidence bundle for J0835-4510 combining 1 pulsar-catalog row(s) and 21 glitch event(s).",
                json.dumps([], ensure_ascii=False),
                0.8,
                json.dumps(["pulsars"], ensure_ascii=False),
                None,
                None,
            ),
        )
        conn.commit()
        conn.close()

        rule = resolve_canonical_rule("PSR B0833-45", "Crustal Memory")
        canonical_id = ensure_canonical_hypothesis(db_path, rule)
        cleanup = canonicalize_existing_duplicates(db_path, rule)
        support_sync = sync_canonical_hypothesis_support(db_path, canonical_id, fallback_domains=rule["context_domains"])

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        canonical_row = conn.execute("SELECT id, title, description, status, evidence, context_hypotheses, context_domains FROM hypotheses WHERE id=?", (canonical_id,)).fetchone()
        duplicate_row = conn.execute("SELECT id, status, merged_into FROM hypotheses WHERE id='AUTO-410'").fetchone()
        memory_row = conn.execute("SELECT supports_hypothesis FROM memories LIMIT 1").fetchone()
        conn.close()

    assert canonical_row["id"] == "H19", canonical_row
    assert canonical_row["title"] == "Crustal Memory in Vela Pulsar", canonical_row
    assert "microhertz" in canonical_row["description"], canonical_row
    assert duplicate_row["status"] == "merged", duplicate_row
    assert duplicate_row["merged_into"] == "H19", duplicate_row
    assert memory_row["supports_hypothesis"] == "H19", memory_row
    assert cleanup["duplicates_merged"] == ["AUTO-410"], cleanup
    assert support_sync["support_memory_count"] == 1, support_sync
    assert "Structured evidence bundle for J0835-4510" in canonical_row["evidence"], canonical_row
    assert "H14" in canonical_row["context_hypotheses"], canonical_row
    assert "pulsars" in canonical_row["context_domains"], canonical_row


def main():
    test_build_evidence_bundle_generates_review_ready_payload()
    test_structured_bundle_ingests_without_llm()
    test_extract_psrcat_package_reads_embedded_databases()
    test_canonical_rule_seeds_h19_and_merges_duplicates()
    print("pulsar glitch importer tests passed")


if __name__ == "__main__":
    main()