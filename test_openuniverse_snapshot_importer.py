import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from openuniverse_snapshot_importer import (
    build_openuniverse_snapshot_bundle,
    collect_openuniverse_snapshot,
    ingest_openuniverse_bundle,
    write_openuniverse_snapshot_files,
)


MANIFEST = {
    "Name": "OpenUniverse 2024 Simulated Roman & Rubin Images",
    "Description": "Synthetic Roman and Rubin imaging products for matched-sky benchmarking.",
    "Documentation": "https://irsa.ipac.caltech.edu/data/theory/openuniverse2024",
    "Contact": "https://irsa.ipac.caltech.edu/docs/help_desk.html",
    "ManagedBy": "IRSA at Caltech",
    "UpdateFrequency": "Finalized and will not be updated.",
    "Tags": ["astronomy", "imaging", "simulations", "object detection", "survey"],
    "License": "https://irsa.ipac.caltech.edu/data_use_terms.html",
    "Citation": "Use the OpenUniverse citation guidance and DOI.",
    "Resources": [
        {
            "Description": "Simulated Roman truth and calibrated images.",
            "ARN": "arn:aws:s3:::nasa-irsa-simulations/openuniverse2024/roman/",
            "Region": "us-east-1",
            "Type": "S3 Bucket",
            "RequesterPays": False,
            "AccountRequired": False,
        },
        {
            "Description": "Simulated Rubin raw, calibrated, and coadded data products.",
            "ARN": "arn:aws:s3:::nasa-irsa-simulations/openuniverse2024/rubin/",
            "Region": "us-east-1",
            "Type": "S3 Bucket",
            "RequesterPays": False,
            "AccountRequired": False,
        },
    ],
    "DataAtWork": {
        "Tutorials": [
            {
                "Title": "Notebook Tutorials",
                "URL": "https://irsa.ipac.caltech.edu/docs/notebooks/",
                "AuthorName": "Caltech/IPAC-IRSA",
                "AuthorURL": "https://irsa.ipac.caltech.edu",
            }
        ]
    },
}


def fake_fetcher(url: str):
    if not url.endswith("openuniverse2024.yaml"):
        raise AssertionError(f"Unexpected URL: {url}")
    return MANIFEST


def test_collect_openuniverse_snapshot_from_preset():
    snapshot = collect_openuniverse_snapshot("openuniverse2024", fetcher=fake_fetcher)

    assert snapshot["kind"] == "synthetic_dataset_manifest", snapshot
    assert snapshot["object_id"] == "openuniverse2024", snapshot
    assert len(snapshot["resources"]) == 2, snapshot
    assert len(snapshot["tutorials"]) == 1, snapshot


def test_openuniverse_bundle_defaults_to_evidence_only_behavior():
    snapshot = collect_openuniverse_snapshot("openuniverse2024", fetcher=fake_fetcher)
    bundle = build_openuniverse_snapshot_bundle(snapshot)

    assert bundle["new_hypothesis"] is None, bundle
    assert "synthetic_data" in bundle["domain_tags"], bundle
    assert "anomaly_detection" in bundle["domain_tags"], bundle


def test_direct_openuniverse_bundle_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_openuniverse_snapshot("openuniverse2024", fetcher=fake_fetcher)
    bundle = build_openuniverse_snapshot_bundle(snapshot)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, bundle_json, _ = write_openuniverse_snapshot_files(snapshot, bundle, tmp_path, "openuniverse2024")

        result = ingest_openuniverse_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path

    assert result is not None, result
    assert result["id"] == 1, result
    assert result.get("hypothesis_generated") is None, result
    assert any(item["summary"].startswith("Structured synthetic dataset snapshot") for item in memories), memories
    assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_collect_openuniverse_snapshot_from_preset()
    test_openuniverse_bundle_defaults_to_evidence_only_behavior()
    test_direct_openuniverse_bundle_ingest_populates_runtime_db_without_auto_hypothesis()
    print("openuniverse snapshot importer tests passed")


if __name__ == "__main__":
    main()