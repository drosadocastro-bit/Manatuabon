"""Normalize OpenUniverse-style synthetic catalog exports for Manatuabon anomaly workers."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

from cross_survey_catalog_anomaly_worker import load_catalog_rows, normalize_catalog_rows


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "openuniverse_catalog"


def iso_timestamp() -> str:
    return datetime.now().isoformat()


def _standardize_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "id": row.get("id"),
            "ra": row.get("ra"),
            "dec": row.get("dec"),
            "flux": row.get("flux"),
            "mag": row.get("mag"),
            "shape": row.get("shape"),
        }
        for row in rows
    ]


def write_normalized_catalog_files(
    rows: list[dict],
    metadata: dict,
    *,
    output_dir: Path,
    catalog_name: str,
    output_format: str,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(catalog_name)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    standard_rows = _standardize_rows(rows)

    paths: dict[str, Path] = {}
    if output_format in {"csv", "both"}:
        csv_path = output_dir / f"openuniverse_catalog_normalized_{safe_label}_{stamp}.csv"
        csv_tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
        with open(csv_tmp, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["id", "ra", "dec", "flux", "mag", "shape"])
            writer.writeheader()
            writer.writerows(standard_rows)
        csv_tmp.replace(csv_path)
        paths["csv"] = csv_path

    if output_format in {"json", "both"}:
        json_path = output_dir / f"openuniverse_catalog_normalized_{safe_label}_{stamp}.json"
        json_tmp = json_path.with_suffix(json_path.suffix + ".tmp")
        with open(json_tmp, "w", encoding="utf-8") as handle:
            json.dump(standard_rows, handle, indent=2, ensure_ascii=False)
        json_tmp.replace(json_path)
        paths["json"] = json_path

    sidecar = {
        "kind": "openuniverse_catalog_normalization",
        "generated_at": iso_timestamp(),
        "catalog_name": catalog_name,
        "normalized_schema": ["id", "ra", "dec", "flux", "mag", "shape"],
        "metadata": metadata,
        "output_files": {key: str(value) for key, value in paths.items()},
    }
    meta_path = output_dir / f"openuniverse_catalog_normalized_{safe_label}_{stamp}.meta.json"
    meta_tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
    with open(meta_tmp, "w", encoding="utf-8") as handle:
        json.dump(sidecar, handle, indent=2, ensure_ascii=False)
    meta_tmp.replace(meta_path)
    paths["meta"] = meta_path
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize OpenUniverse-style synthetic catalog exports into the exact row shape used by Manatuabon anomaly workers.")
    parser.add_argument("--input", required=True, help="Path to the source catalog export (.csv, .json, .jsonl)")
    parser.add_argument("--catalog-name", required=True, help="Logical label for this catalog, for example Roman, Rubin, or Truth")
    parser.add_argument("--out-dir", default="D:/Manatuabon/data", help="Output directory for normalized catalog files")
    parser.add_argument("--format", choices=["csv", "json", "both"], default="both", help="Normalized output format")
    parser.add_argument("--id-col", default=None, help="Optional explicit ID column override")
    parser.add_argument("--ra-col", default=None, help="Optional explicit right-ascension column override")
    parser.add_argument("--dec-col", default=None, help="Optional explicit declination column override")
    parser.add_argument("--flux-col", default=None, help="Optional explicit flux column override")
    parser.add_argument("--mag-col", default=None, help="Optional explicit magnitude column override")
    parser.add_argument("--shape-col", default=None, help="Optional explicit shape or ellipticity column override")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    raw_rows = load_catalog_rows(input_path)
    normalized_rows, metadata = normalize_catalog_rows(
        raw_rows,
        catalog_name=args.catalog_name,
        ra_col=args.ra_col,
        dec_col=args.dec_col,
        id_col=args.id_col,
        flux_col=args.flux_col,
        mag_col=args.mag_col,
        shape_col=args.shape_col,
    )
    metadata = {
        **metadata,
        "source_file": str(input_path),
        "output_format": args.format,
    }
    paths = write_normalized_catalog_files(
        normalized_rows,
        metadata,
        output_dir=Path(args.out_dir),
        catalog_name=args.catalog_name,
        output_format=args.format,
    )
    print(f"Normalized rows: {metadata['usable_row_count']} / {metadata['input_row_count']}")
    print(f"Resolved columns: {json.dumps(metadata['resolved_columns'], ensure_ascii=False)}")
    for kind, path in paths.items():
        print(f"Wrote {kind}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())