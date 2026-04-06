"""Run OpenUniverse catalog normalization and cross-survey anomaly extraction in one command."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cross_survey_catalog_anomaly_worker import (
    build_cross_survey_catalog_bundle,
    build_cross_survey_catalog_profile,
    ingest_cross_survey_catalog_bundle,
    load_catalog_rows,
    normalize_catalog_rows,
    write_cross_survey_catalog_files,
)
from openuniverse_catalog_normalizer import write_normalized_catalog_files


def _normalize_export(
    input_path: Path,
    *,
    catalog_name: str,
    output_dir: Path,
    output_format: str,
    id_col: str | None = None,
    ra_col: str | None = None,
    dec_col: str | None = None,
    flux_col: str | None = None,
    mag_col: str | None = None,
    shape_col: str | None = None,
) -> tuple[list[dict], dict, dict[str, Path]]:
    raw_rows = load_catalog_rows(input_path)
    normalized_rows, metadata = normalize_catalog_rows(
        raw_rows,
        catalog_name=catalog_name,
        ra_col=ra_col,
        dec_col=dec_col,
        id_col=id_col,
        flux_col=flux_col,
        mag_col=mag_col,
        shape_col=shape_col,
    )
    metadata = {
        **metadata,
        "source_file": str(input_path),
        "output_format": output_format,
    }
    output_paths = write_normalized_catalog_files(
        normalized_rows,
        metadata,
        output_dir=output_dir,
        catalog_name=catalog_name,
        output_format=output_format,
    )
    return normalized_rows, metadata, output_paths


def run_pipeline(
    *,
    roman_path: Path,
    rubin_path: Path,
    truth_path: Path | None,
    output_dir: Path,
    normalized_format: str,
    max_sep_arcsec: float,
    ingest: bool,
    db_path: Path,
    agent_log_path: Path,
) -> dict:
    roman_rows, roman_meta, roman_outputs = _normalize_export(
        roman_path,
        catalog_name="Roman",
        output_dir=output_dir,
        output_format=normalized_format,
    )
    rubin_rows, rubin_meta, rubin_outputs = _normalize_export(
        rubin_path,
        catalog_name="Rubin",
        output_dir=output_dir,
        output_format=normalized_format,
    )

    truth_rows = None
    truth_meta = None
    truth_outputs = None
    if truth_path is not None:
        truth_rows, truth_meta, truth_outputs = _normalize_export(
            truth_path,
            catalog_name="Truth",
            output_dir=output_dir,
            output_format=normalized_format,
        )

    profile = build_cross_survey_catalog_profile(
        roman_rows,
        rubin_rows,
        left_name="Roman",
        right_name="Rubin",
        max_sep_arcsec=max_sep_arcsec,
        truth_rows=truth_rows,
        truth_name="Truth" if truth_rows is not None else None,
        left_metadata=roman_meta,
        right_metadata=rubin_meta,
        truth_metadata=truth_meta,
    )
    bundle = build_cross_survey_catalog_bundle(profile)
    raw_profile_path, bundle_json, bundle_md = write_cross_survey_catalog_files(
        profile,
        bundle,
        output_dir,
        "Roman_vs_Rubin",
    )

    ingest_result = None
    if ingest:
        ingest_result = ingest_cross_survey_catalog_bundle(
            bundle_json,
            db_path=db_path,
            agent_log_path=agent_log_path,
        )

    return {
        "normalized": {
            "Roman": {key: str(value) for key, value in roman_outputs.items()},
            "Rubin": {key: str(value) for key, value in rubin_outputs.items()},
            **({"Truth": {key: str(value) for key, value in truth_outputs.items()}} if truth_outputs else {}),
        },
        "profile": str(raw_profile_path),
        "bundle_json": str(bundle_json),
        "bundle_md": str(bundle_md),
        "ingest_result": ingest_result,
        "match_summary": profile.get("match_summary"),
        "truth_overlap": profile.get("truth_overlap"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize OpenUniverse-style Roman/Rubin/Truth exports and run the cross-survey anomaly extractor in one command.")
    parser.add_argument("--roman", required=True, help="Path to the Roman export (.csv, .json, .jsonl)")
    parser.add_argument("--rubin", required=True, help="Path to the Rubin export (.csv, .json, .jsonl)")
    parser.add_argument("--truth", default=None, help="Optional truth export (.csv, .json, .jsonl)")
    parser.add_argument("--out-dir", default="D:/Manatuabon/data", help="Output directory for normalized catalogs and anomaly artifacts")
    parser.add_argument("--normalized-format", choices=["csv", "json", "both"], default="both", help="Format used for intermediate normalized catalogs")
    parser.add_argument("--max-sep-arcsec", type=float, default=1.0, help="Maximum separation for Roman/Rubin cross-match pairing")
    parser.add_argument("--ingest", action="store_true", help="After writing the anomaly bundle, ingest it directly into the runtime DB")
    parser.add_argument("--db", default="D:/Manatuabon/manatuabon.db", help="SQLite runtime DB path used when --ingest is enabled")
    parser.add_argument("--agent-log", default="D:/Manatuabon/agent_log.json", help="Agent log path used when --ingest is enabled")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_pipeline(
        roman_path=Path(args.roman),
        rubin_path=Path(args.rubin),
        truth_path=Path(args.truth) if args.truth else None,
        output_dir=Path(args.out_dir),
        normalized_format=args.normalized_format,
        max_sep_arcsec=args.max_sep_arcsec,
        ingest=args.ingest,
        db_path=Path(args.db),
        agent_log_path=Path(args.agent_log),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())