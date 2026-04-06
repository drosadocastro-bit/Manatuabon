import json
import tempfile
from pathlib import Path

from openuniverse_catalog_normalizer import write_normalized_catalog_files
from cross_survey_catalog_anomaly_worker import load_catalog_rows, normalize_catalog_rows


SAMPLE_JSON = {
    "records": [
        {
            "object_id": "roman-1",
            "ra_deg": "10.1000",
            "dec_deg": "-20.2000",
            "flux_mjy": "12.5",
            "mag_ab": "23.1",
            "axis_ratio": "0.82"
        },
        {
            "object_id": "roman-2",
            "ra_deg": "10.1010",
            "dec_deg": "-20.1990",
            "flux_mjy": "8.1",
            "mag_ab": "24.4",
            "axis_ratio": "0.67"
        }
    ]
}


def test_normalizer_detects_alias_columns_and_standardizes_rows():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        input_path = tmp_path / "roman_export.json"
        input_path.write_text(json.dumps(SAMPLE_JSON), encoding="utf-8")

        rows = load_catalog_rows(input_path)
        normalized, metadata = normalize_catalog_rows(rows, catalog_name="Roman")

    assert metadata["resolved_columns"]["id"] == "object_id", metadata
    assert metadata["resolved_columns"]["ra"] == "ra_deg", metadata
    assert metadata["resolved_columns"]["dec"] == "dec_deg", metadata
    assert metadata["resolved_columns"]["flux"] == "flux_mjy", metadata
    assert metadata["resolved_columns"]["mag"] == "mag_ab", metadata
    assert metadata["resolved_columns"]["shape"] == "axis_ratio", metadata
    assert normalized[0] == {
        "id": "roman-1",
        "ra": 10.1,
        "dec": -20.2,
        "flux": 12.5,
        "mag": 23.1,
        "shape": 0.82,
        "raw_index": 1,
    }, normalized


def test_write_normalized_catalog_files_emits_exact_worker_shape_and_sidecar():
    rows = [
        {"id": "roman-1", "ra": 10.1, "dec": -20.2, "flux": 12.5, "mag": 23.1, "shape": 0.82, "raw_index": 1},
        {"id": "roman-2", "ra": 10.101, "dec": -20.199, "flux": 8.1, "mag": 24.4, "shape": 0.67, "raw_index": 2},
    ]
    metadata = {
        "catalog_name": "Roman",
        "resolved_columns": {"id": "object_id", "ra": "ra_deg", "dec": "dec_deg", "flux": "flux_mjy", "mag": "mag_ab", "shape": "axis_ratio"},
        "input_row_count": 2,
        "usable_row_count": 2,
    }

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        paths = write_normalized_catalog_files(rows, metadata, output_dir=tmp_path, catalog_name="Roman", output_format="both")

        csv_text = paths["csv"].read_text(encoding="utf-8")
        json_rows = json.loads(paths["json"].read_text(encoding="utf-8"))
        sidecar = json.loads(paths["meta"].read_text(encoding="utf-8"))

    assert csv_text.splitlines()[0] == "id,ra,dec,flux,mag,shape", csv_text
    assert json_rows == [
        {"id": "roman-1", "ra": 10.1, "dec": -20.2, "flux": 12.5, "mag": 23.1, "shape": 0.82},
        {"id": "roman-2", "ra": 10.101, "dec": -20.199, "flux": 8.1, "mag": 24.4, "shape": 0.67},
    ], json_rows
    assert sidecar["normalized_schema"] == ["id", "ra", "dec", "flux", "mag", "shape"], sidecar
    assert sidecar["metadata"]["usable_row_count"] == 2, sidecar


def main():
    test_normalizer_detects_alias_columns_and_standardizes_rows()
    test_write_normalized_catalog_files_emits_exact_worker_shape_and_sidecar()
    print("openuniverse catalog normalizer tests passed")


if __name__ == "__main__":
    main()