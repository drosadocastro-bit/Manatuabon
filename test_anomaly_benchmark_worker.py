import json
import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from anomaly_benchmark_worker import (
    build_anomaly_benchmark_bundle,
    build_anomaly_benchmark_profile,
    ingest_anomaly_benchmark_bundle,
    load_synthetic_bundle,
    write_anomaly_benchmark_files,
)
from openuniverse_snapshot_importer import (
    build_openuniverse_snapshot_bundle,
    collect_openuniverse_snapshot,
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


def test_build_anomaly_benchmark_profile_from_openuniverse_bundle():
    snapshot = collect_openuniverse_snapshot("openuniverse2024", fetcher=fake_fetcher)
    source_bundle = build_openuniverse_snapshot_bundle(snapshot)
    profile = build_anomaly_benchmark_profile(source_bundle)

    assert profile["score"] == 1.0, profile
    assert profile["signals"]["cross_survey_pair"] is True, profile
    assert profile["signals"]["truth_products_present"] is True, profile
    assert profile["signals"]["public_access"] is True, profile


def test_load_synthetic_bundle_rejects_non_synthetic_input():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        bundle_path = Path(tmpdir) / "not_synthetic.json"
        bundle_path.write_text(
            json.dumps(
                {
                    "manatuabon_schema": "structured_ingest_v1",
                    "payload_type": "arxiv_snapshot_bundle",
                    "summary": "non-synthetic",
                    "domain_tags": ["gravitational_waves"],
                }
            ),
            encoding="utf-8",
        )

        try:
            load_synthetic_bundle(bundle_path)
        except ValueError as exc:
            assert "not marked as synthetic" in str(exc), exc
        else:
            raise AssertionError("Expected synthetic bundle validation to fail")


def test_direct_anomaly_benchmark_ingest_populates_runtime_db_without_auto_hypothesis():
    snapshot = collect_openuniverse_snapshot("openuniverse2024", fetcher=fake_fetcher)
    source_bundle = build_openuniverse_snapshot_bundle(snapshot)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        ensure_runtime_db(db_path, migrate=False).close()
        _, source_bundle_json, _ = write_openuniverse_snapshot_files(snapshot, source_bundle, tmp_path, "openuniverse2024")

        loaded_source = load_synthetic_bundle(source_bundle_json)
        profile = build_anomaly_benchmark_profile(loaded_source)
        benchmark_bundle = build_anomaly_benchmark_bundle(loaded_source, profile)
        _, benchmark_json, _ = write_anomaly_benchmark_files(profile, benchmark_bundle, tmp_path, "openuniverse2024")

        result = ingest_anomaly_benchmark_bundle(benchmark_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

        assert agent_log_path.exists(), agent_log_path

    assert result is not None, result
    assert result["id"] == 1, result
    assert result.get("hypothesis_generated") is None, result
    assert any(item["summary"].startswith("Deterministic anomaly benchmark profile") for item in memories), memories
    assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_build_anomaly_benchmark_profile_from_openuniverse_bundle()
    test_load_synthetic_bundle_rejects_non_synthetic_input()
    test_direct_anomaly_benchmark_ingest_populates_runtime_db_without_auto_hypothesis()
    print("anomaly benchmark worker tests passed")


if __name__ == "__main__":
    main()