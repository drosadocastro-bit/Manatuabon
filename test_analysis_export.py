"""Tests for analysis_export.py."""

import json
import tempfile
from pathlib import Path

from analysis_export import export_csv, export_markdown_table, export_profiles


SAMPLE_PROFILE = {
    "kind": "gaia_panstarrs_anomaly_profile",
    "generated_at": "2026-04-05T12:00:00",
    "anomaly_candidates": [
        {
            "gaia_source_id": "101",
            "separation_arcsec": 0.036,
            "proper_motion_total_masyr": 12.649,
            "pm_total_error_masyr": 0.295,
            "parallax": 0.5,
            "parallax_error": 0.05,
            "parallax_snr": 10.0,
            "pmra": 12.0,
            "pmra_error": 0.3,
            "pmdec": -4.0,
            "pmdec_error": 0.2,
            "gaia_g_mag": 15.1,
            "gaia_ruwe": 1.02,
            "review_priority": "high",
            "candidate_score": 0.72,
        },
        {
            "gaia_source_id": "102",
            "separation_arcsec": 1.2,
            "proper_motion_total_masyr": 1.077,
            "pm_total_error_masyr": 0.212,
            "parallax": 0.1,
            "parallax_error": 0.05,
            "parallax_snr": 2.0,
            "pmra": 1.0,
            "pmra_error": 0.2,
            "pmdec": 0.4,
            "pmdec_error": 0.2,
            "gaia_g_mag": 17.2,
            "gaia_ruwe": 1.51,
            "review_priority": "low",
            "candidate_score": 0.28,
        },
    ],
}


def test_export_csv_writes_valid_file():
    candidates = SAMPLE_PROFILE["anomaly_candidates"]
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        out = Path(tmpdir) / "test.csv"
        export_csv(candidates, out)
        assert out.exists()
        lines = out.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 3, lines  # header + 2 rows
        assert "gaia_source_id" in lines[0]
        assert "pm_total_error_masyr" in lines[0]


def test_export_csv_handles_empty_candidates():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        out = Path(tmpdir) / "empty.csv"
        export_csv([], out)
        assert out.exists()
        content = out.read_text(encoding="utf-8")
        assert "No candidates" in content


def test_export_markdown_table_generates_table():
    candidates = SAMPLE_PROFILE["anomaly_candidates"]
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        out = Path(tmpdir) / "test.md"
        export_markdown_table(candidates, out, profile_kind="gaia_panstarrs_anomaly_profile")
        assert out.exists()
        content = out.read_text(encoding="utf-8")
        assert "gaia_panstarrs_anomaly_profile" in content
        assert "101" in content
        assert "| ---" in content


def test_export_profiles_creates_csv_md_and_report():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        profile_path = tmp_path / "sample_profile.json"
        profile_path.write_text(json.dumps(SAMPLE_PROFILE), encoding="utf-8")
        out_dir = tmp_path / "export"
        report = export_profiles([profile_path], out_dir)

        assert report["total_candidates"] == 2, report
        assert Path(report["csv_path"]).exists(), report
        assert Path(report["markdown_path"]).exists(), report
        csv_content = Path(report["csv_path"]).read_text(encoding="utf-8")
        assert "101" in csv_content
        assert "102" in csv_content


def main():
    test_export_csv_writes_valid_file()
    test_export_csv_handles_empty_candidates()
    test_export_markdown_table_generates_table()
    test_export_profiles_creates_csv_md_and_report()
    print("analysis export tests passed")


if __name__ == "__main__":
    main()
