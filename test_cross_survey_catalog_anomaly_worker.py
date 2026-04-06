import tempfile
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import MemoryManager
from cross_survey_catalog_anomaly_worker import (
    build_cross_survey_catalog_bundle,
    build_cross_survey_catalog_profile,
    ingest_cross_survey_catalog_bundle,
    load_catalog_rows,
    normalize_catalog_rows,
    write_cross_survey_catalog_files,
)


LEFT_CSV = """id,ra,dec,flux,ellipticity
L1,10.00000,20.00000,100,0.10
L2,10.01000,20.01000,50,0.20
L3,10.03000,20.03000,80,0.15
"""

RIGHT_CSV = """id,ra,dec,flux,ellipticity
R1,10.00005,20.00005,98,0.11
R2,10.01010,20.01010,70,0.35
R4,10.05000,20.05000,40,0.12
"""

TRUTH_CSV = """truth_id,ra,dec
T1,10.00002,20.00002
T2,10.01002,20.01002
T3,10.03001,20.03001
"""


def test_build_cross_survey_catalog_profile_detects_matches_and_outliers():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        left_path = tmp_path / "roman.csv"
        right_path = tmp_path / "rubin.csv"
        truth_path = tmp_path / "truth.csv"
        left_path.write_text(LEFT_CSV, encoding="utf-8")
        right_path.write_text(RIGHT_CSV, encoding="utf-8")
        truth_path.write_text(TRUTH_CSV, encoding="utf-8")

        left_rows, left_meta = normalize_catalog_rows(load_catalog_rows(left_path), catalog_name="roman")
        right_rows, right_meta = normalize_catalog_rows(load_catalog_rows(right_path), catalog_name="rubin")
        truth_rows, truth_meta = normalize_catalog_rows(load_catalog_rows(truth_path), catalog_name="truth")
        profile = build_cross_survey_catalog_profile(
            left_rows,
            right_rows,
            left_name="roman",
            right_name="rubin",
            max_sep_arcsec=1.0,
            truth_rows=truth_rows,
            truth_name="truth",
            left_metadata=left_meta,
            right_metadata=right_meta,
            truth_metadata=truth_meta,
        )

    assert profile["match_summary"]["matched_count"] == 2, profile
    assert profile["match_summary"]["left_only_count"] == 1, profile
    assert profile["match_summary"]["right_only_count"] == 1, profile
    assert profile["flux_residuals"]["p95_fraction"] is not None, profile
    assert profile["truth_overlap"]["roman"]["truth_recall"] == 1.0, profile
    assert profile["truth_overlap"]["rubin"]["truth_recall"] == 0.666667, profile
    assert profile["anomaly_candidates"], profile


def test_direct_cross_survey_catalog_ingest_populates_runtime_db_without_auto_hypothesis():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        left_path = tmp_path / "roman.csv"
        right_path = tmp_path / "rubin.csv"
        truth_path = tmp_path / "truth.csv"
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        left_path.write_text(LEFT_CSV, encoding="utf-8")
        right_path.write_text(RIGHT_CSV, encoding="utf-8")
        truth_path.write_text(TRUTH_CSV, encoding="utf-8")
        ensure_runtime_db(db_path, migrate=False).close()

        left_rows, left_meta = normalize_catalog_rows(load_catalog_rows(left_path), catalog_name="roman")
        right_rows, right_meta = normalize_catalog_rows(load_catalog_rows(right_path), catalog_name="rubin")
        truth_rows, truth_meta = normalize_catalog_rows(load_catalog_rows(truth_path), catalog_name="truth")
        profile = build_cross_survey_catalog_profile(
            left_rows,
            right_rows,
            left_name="roman",
            right_name="rubin",
            max_sep_arcsec=1.0,
            truth_rows=truth_rows,
            truth_name="truth",
            left_metadata=left_meta,
            right_metadata=right_meta,
            truth_metadata=truth_meta,
        )
        bundle = build_cross_survey_catalog_bundle(profile)
        _, bundle_json, _ = write_cross_survey_catalog_files(profile, bundle, tmp_path, "roman_vs_rubin")

        result = ingest_cross_survey_catalog_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory

    assert result is not None, result
    assert result["id"] == 1, result
    assert result.get("hypothesis_generated") is None, result
    assert any(item["summary"].startswith("Cross-survey anomaly catalog profile") for item in memories), memories
    assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_build_cross_survey_catalog_profile_detects_matches_and_outliers()
    test_direct_cross_survey_catalog_ingest_populates_runtime_db_without_auto_hypothesis()
    print("cross survey catalog anomaly worker tests passed")


if __name__ == "__main__":
    main()