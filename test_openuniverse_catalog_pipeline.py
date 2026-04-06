import json
import tempfile
from pathlib import Path

from manatuabon_agent import MemoryManager
from openuniverse_catalog_pipeline import run_pipeline


ROMAN_EXPORT = {
    "records": [
        {"object_id": "roman-1", "ra_deg": "10.00000", "dec_deg": "20.00000", "flux_mjy": "100", "mag_ab": "22.1", "axis_ratio": "0.90"},
        {"object_id": "roman-2", "ra_deg": "10.01000", "dec_deg": "20.01000", "flux_mjy": "50", "mag_ab": "23.2", "axis_ratio": "0.75"},
        {"object_id": "roman-3", "ra_deg": "10.03000", "dec_deg": "20.03000", "flux_mjy": "80", "mag_ab": "21.7", "axis_ratio": "0.82"},
    ]
}

RUBIN_EXPORT = {
    "records": [
        {"object_id": "rubin-1", "ra_deg": "10.00005", "dec_deg": "20.00005", "flux_mjy": "97", "mag_ab": "22.2", "axis_ratio": "0.88"},
        {"object_id": "rubin-2", "ra_deg": "10.01010", "dec_deg": "20.01010", "flux_mjy": "70", "mag_ab": "22.7", "axis_ratio": "0.62"},
        {"object_id": "rubin-4", "ra_deg": "10.05000", "dec_deg": "20.05000", "flux_mjy": "40", "mag_ab": "24.8", "axis_ratio": "0.79"},
    ]
}

TRUTH_EXPORT = {
    "records": [
        {"truth_id": "truth-1", "ra_deg": "10.00002", "dec_deg": "20.00002"},
        {"truth_id": "truth-2", "ra_deg": "10.01002", "dec_deg": "20.01002"},
        {"truth_id": "truth-3", "ra_deg": "10.03001", "dec_deg": "20.03001"},
    ]
}


def test_pipeline_normalizes_exports_and_ingests_anomaly_bundle():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        roman_path = tmp_path / "roman_export.json"
        rubin_path = tmp_path / "rubin_export.json"
        truth_path = tmp_path / "truth_export.json"
        db_path = tmp_path / "manatuabon.db"
        agent_log_path = tmp_path / "agent_log.json"
        roman_path.write_text(json.dumps(ROMAN_EXPORT), encoding="utf-8")
        rubin_path.write_text(json.dumps(RUBIN_EXPORT), encoding="utf-8")
        truth_path.write_text(json.dumps(TRUTH_EXPORT), encoding="utf-8")

        result = run_pipeline(
            roman_path=roman_path,
            rubin_path=rubin_path,
            truth_path=truth_path,
            output_dir=tmp_path,
            normalized_format="both",
            max_sep_arcsec=1.0,
            ingest=True,
            db_path=db_path,
            agent_log_path=agent_log_path,
        )

        memory = MemoryManager(db_path)
        memories = memory.get_memories()
        hypotheses = memory.get_all_hypotheses(normalized=True)
        del memory
        assert Path(result["normalized"]["Roman"]["csv"]).exists(), result
        assert Path(result["normalized"]["Rubin"]["csv"]).exists(), result
        assert Path(result["normalized"]["Truth"]["csv"]).exists(), result
        assert Path(result["bundle_json"]).exists(), result
        assert result["match_summary"]["matched_count"] == 2, result
        assert result["truth_overlap"]["Roman"]["truth_recall"] == 1.0, result
        assert result["ingest_result"] is not None, result
        assert result["ingest_result"]["id"] == 1, result
        assert any(item["summary"].startswith("Cross-survey anomaly catalog profile") for item in memories), memories
        assert not any(hypothesis["id"].startswith("AUTO-") for hypothesis in hypotheses), hypotheses


def main():
    test_pipeline_normalizes_exports_and_ingests_anomaly_bundle()
    print("openuniverse catalog pipeline tests passed")


if __name__ == "__main__":
    main()