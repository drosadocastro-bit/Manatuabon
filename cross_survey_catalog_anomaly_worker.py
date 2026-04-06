"""Extract deterministic cross-survey anomaly features from local synthetic catalogs."""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from openuniverse_snapshot_importer import OPENUNIVERSE_ACKNOWLEDGEMENT
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


ID_CANDIDATES = ("id", "source_id", "object_id", "objid", "truth_id", "match_id", "name")
RA_CANDIDATES = ("ra", "ra_deg", "raj2000", "alpha")
DEC_CANDIDATES = ("dec", "dec_deg", "dej2000", "delta")
FLUX_CANDIDATES = ("flux", "flux_jy", "flux_mjy", "brightness", "intensity", "total_flux")
MAG_CANDIDATES = ("mag", "magnitude", "mag_auto", "mag_ab", "r_mag", "i_mag", "g_mag")
SHAPE_CANDIDATES = ("ellipticity", "shape", "shape_e", "e", "axis_ratio")


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "catalog_benchmark"


def iso_timestamp() -> str:
    return datetime.now().isoformat()


def _coerce_float(value) -> float | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("rows", "records", "sources", "objects", "catalog", "data", "entries"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    raise ValueError("Catalog payload must be a list of objects or a mapping containing rows/records/data.")


def load_catalog_rows(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with open(path, "r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as handle:
            return _extract_rows(json.load(handle))
    if suffix == ".jsonl":
        rows = []
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                item = json.loads(text)
                if isinstance(item, dict):
                    rows.append(item)
        return rows
    raise ValueError(f"Unsupported catalog format: {path}")


def _column_lookup(rows: list[dict]) -> dict[str, str]:
    columns = {}
    for row in rows:
        for key in row.keys():
            normalized = str(key).strip().lower()
            columns.setdefault(normalized, key)
    return columns


def _resolve_column(rows: list[dict], explicit: str | None, candidates: tuple[str, ...], *, required: bool = False) -> str | None:
    if explicit:
        return explicit
    columns = _column_lookup(rows)
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    if required:
        raise ValueError(f"Could not resolve required column from candidates: {', '.join(candidates)}")
    return None


def normalize_catalog_rows(
    rows: list[dict],
    *,
    catalog_name: str,
    ra_col: str | None = None,
    dec_col: str | None = None,
    id_col: str | None = None,
    flux_col: str | None = None,
    mag_col: str | None = None,
    shape_col: str | None = None,
) -> tuple[list[dict], dict]:
    resolved_ra = _resolve_column(rows, ra_col, RA_CANDIDATES, required=True)
    resolved_dec = _resolve_column(rows, dec_col, DEC_CANDIDATES, required=True)
    resolved_id = _resolve_column(rows, id_col, ID_CANDIDATES)
    resolved_flux = _resolve_column(rows, flux_col, FLUX_CANDIDATES)
    resolved_mag = _resolve_column(rows, mag_col, MAG_CANDIDATES)
    resolved_shape = _resolve_column(rows, shape_col, SHAPE_CANDIDATES)

    normalized = []
    for index, row in enumerate(rows, start=1):
        ra = _coerce_float(row.get(resolved_ra))
        dec = _coerce_float(row.get(resolved_dec))
        if ra is None or dec is None:
            continue
        source_id = str(row.get(resolved_id) if resolved_id else f"{catalog_name}_{index}").strip() or f"{catalog_name}_{index}"
        normalized.append({
            "id": source_id,
            "ra": ra,
            "dec": dec,
            "flux": _coerce_float(row.get(resolved_flux)) if resolved_flux else None,
            "mag": _coerce_float(row.get(resolved_mag)) if resolved_mag else None,
            "shape": _coerce_float(row.get(resolved_shape)) if resolved_shape else None,
            "raw_index": index,
        })

    metadata = {
        "catalog_name": catalog_name,
        "resolved_columns": {
            "id": resolved_id,
            "ra": resolved_ra,
            "dec": resolved_dec,
            "flux": resolved_flux,
            "mag": resolved_mag,
            "shape": resolved_shape,
        },
        "input_row_count": len(rows),
        "usable_row_count": len(normalized),
    }
    return normalized, metadata


def angular_separation_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    ra1_rad, dec1_rad, ra2_rad, dec2_rad = map(math.radians, [ra1, dec1, ra2, dec2])
    cos_sep = (
        math.sin(dec1_rad) * math.sin(dec2_rad)
        + math.cos(dec1_rad) * math.cos(dec2_rad) * math.cos(ra1_rad - ra2_rad)
    )
    cos_sep = max(-1.0, min(1.0, cos_sep))
    return math.degrees(math.acos(cos_sep)) * 3600.0


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[mid], 6)
    return round((ordered[mid - 1] + ordered[mid]) / 2.0, 6)


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(max(int(math.ceil(len(ordered) * fraction)) - 1, 0), len(ordered) - 1)
    return round(ordered[index], 6)


def match_catalogs(left_rows: list[dict], right_rows: list[dict], *, max_sep_arcsec: float) -> tuple[list[dict], list[dict], list[dict]]:
    remaining = {row["id"]: row for row in right_rows}
    matches = []
    left_only = []

    for left in left_rows:
        best_id = None
        best_sep = None
        for right_id, right in remaining.items():
            sep = angular_separation_arcsec(left["ra"], left["dec"], right["ra"], right["dec"])
            if sep > max_sep_arcsec:
                continue
            if best_sep is None or sep < best_sep or (sep == best_sep and right_id < best_id):
                best_sep = sep
                best_id = right_id
        if best_id is None:
            left_only.append(left)
            continue
        right = remaining.pop(best_id)
        flux_residual = None
        if left.get("flux") is not None and right.get("flux") is not None:
            denom = max(abs(left["flux"]), abs(right["flux"]), 1e-12)
            flux_residual = abs(left["flux"] - right["flux"]) / denom
        mag_residual = None
        if left.get("mag") is not None and right.get("mag") is not None:
            mag_residual = abs(left["mag"] - right["mag"])
        shape_residual = None
        if left.get("shape") is not None and right.get("shape") is not None:
            shape_residual = abs(left["shape"] - right["shape"])
        anomaly_score = (best_sep / max_sep_arcsec)
        if flux_residual is not None:
            anomaly_score += flux_residual
        if mag_residual is not None:
            anomaly_score += mag_residual
        if shape_residual is not None:
            anomaly_score += shape_residual / 0.3
        matches.append({
            "left_id": left["id"],
            "right_id": right["id"],
            "separation_arcsec": round(best_sep, 6),
            "flux_residual_fraction": round(flux_residual, 6) if flux_residual is not None else None,
            "magnitude_residual": round(mag_residual, 6) if mag_residual is not None else None,
            "shape_residual": round(shape_residual, 6) if shape_residual is not None else None,
            "anomaly_score": round(anomaly_score, 6),
        })
    right_only = list(remaining.values())
    return matches, left_only, right_only


def compute_truth_overlap(catalog_rows: list[dict], truth_rows: list[dict], *, max_sep_arcsec: float) -> dict:
    matches, catalog_only, truth_only = match_catalogs(catalog_rows, truth_rows, max_sep_arcsec=max_sep_arcsec)
    denominator = max(len(truth_rows), 1)
    return {
        "truth_match_count": len(matches),
        "truth_only_count": len(truth_only),
        "catalog_only_count": len(catalog_only),
        "truth_recall": round(len(matches) / denominator, 6),
    }


def build_cross_survey_catalog_profile(
    left_rows: list[dict],
    right_rows: list[dict],
    *,
    left_name: str,
    right_name: str,
    max_sep_arcsec: float,
    truth_rows: list[dict] | None = None,
    truth_name: str | None = None,
    left_metadata: dict | None = None,
    right_metadata: dict | None = None,
    truth_metadata: dict | None = None,
) -> dict:
    matches, left_only, right_only = match_catalogs(left_rows, right_rows, max_sep_arcsec=max_sep_arcsec)
    separations = [item["separation_arcsec"] for item in matches]
    flux_residuals = [item["flux_residual_fraction"] for item in matches if item["flux_residual_fraction"] is not None]
    mag_residuals = [item["magnitude_residual"] for item in matches if item["magnitude_residual"] is not None]
    shape_residuals = [item["shape_residual"] for item in matches if item["shape_residual"] is not None]

    anomaly_candidates = sorted(matches, key=lambda item: item["anomaly_score"], reverse=True)[:5]
    for item in left_only[:3]:
        anomaly_candidates.append({
            "left_id": item["id"],
            "right_id": None,
            "separation_arcsec": None,
            "flux_residual_fraction": None,
            "magnitude_residual": None,
            "shape_residual": None,
            "anomaly_score": 1.25,
            "reason": f"Unmatched in {right_name}",
        })
    for item in right_only[:3]:
        anomaly_candidates.append({
            "left_id": None,
            "right_id": item["id"],
            "separation_arcsec": None,
            "flux_residual_fraction": None,
            "magnitude_residual": None,
            "shape_residual": None,
            "anomaly_score": 1.25,
            "reason": f"Unmatched in {left_name}",
        })

    match_fraction = round(len(matches) / max(min(len(left_rows), len(right_rows)), 1), 6)
    review_flags = []
    if match_fraction < 0.7:
        review_flags.append(f"Cross-survey match fraction is only {match_fraction:.2f}; catalog alignment may be unstable.")
    if flux_residuals and (_percentile(flux_residuals, 0.95) or 0.0) > 0.25:
        review_flags.append("Flux residual tail exceeds 25%; calibration drift or bandpass mismatch needs inspection.")
    if separations and (_percentile(separations, 0.95) or 0.0) > max_sep_arcsec * 0.8:
        review_flags.append("Positional residuals approach the matching radius; centroid alignment should be checked before alerting on outliers.")
    if not matches:
        review_flags.append("No cross-survey matches were found within the requested angular threshold.")

    truth_overlap = None
    if truth_rows:
        truth_overlap = {
            left_name: compute_truth_overlap(left_rows, truth_rows, max_sep_arcsec=max_sep_arcsec),
            right_name: compute_truth_overlap(right_rows, truth_rows, max_sep_arcsec=max_sep_arcsec),
            "truth_name": truth_name,
        }

    return {
        "kind": "cross_survey_catalog_profile",
        "generated_at": iso_timestamp(),
        "catalogs": {
            left_name: left_metadata or {"usable_row_count": len(left_rows)},
            right_name: right_metadata or {"usable_row_count": len(right_rows)},
            **({truth_name: truth_metadata or {"usable_row_count": len(truth_rows)}} if truth_rows and truth_name else {}),
        },
        "pair": {
            "left": left_name,
            "right": right_name,
            "max_separation_arcsec": max_sep_arcsec,
        },
        "match_summary": {
            "matched_count": len(matches),
            "left_only_count": len(left_only),
            "right_only_count": len(right_only),
            "match_fraction": match_fraction,
            "median_separation_arcsec": _median(separations),
            "p95_separation_arcsec": _percentile(separations, 0.95),
        },
        "flux_residuals": {
            "count": len(flux_residuals),
            "median_fraction": _median(flux_residuals),
            "p95_fraction": _percentile(flux_residuals, 0.95),
        },
        "magnitude_residuals": {
            "count": len(mag_residuals),
            "median": _median(mag_residuals),
            "p95": _percentile(mag_residuals, 0.95),
        },
        "shape_residuals": {
            "count": len(shape_residuals),
            "median": _median(shape_residuals),
            "p95": _percentile(shape_residuals, 0.95),
        },
        "truth_overlap": truth_overlap,
        "anomaly_candidates": sorted(anomaly_candidates, key=lambda item: item["anomaly_score"], reverse=True)[:8],
        "review_flags": review_flags,
        "recommended_actions": [
            "Inspect the highest anomaly-score pairs before using this catalog pair to tune alert thresholds.",
            "Keep synthetic cross-survey thresholds separate from council evidence confidence and observational alerting.",
            "Use truth overlap, if available, to bound false positives before applying the extractor to real survey streams.",
        ],
    }


def build_cross_survey_catalog_bundle(profile: dict) -> dict:
    pair = profile.get("pair", {})
    left_name = pair.get("left", "left")
    right_name = pair.get("right", "right")
    match_summary = profile.get("match_summary", {})
    flux_residuals = profile.get("flux_residuals", {})
    truth_overlap = profile.get("truth_overlap") or {}
    truth_note = ""
    if truth_overlap:
        truth_name = truth_overlap.get("truth_name")
        left_truth = truth_overlap.get(left_name, {}).get("truth_recall")
        right_truth = truth_overlap.get(right_name, {}).get("truth_recall")
        truth_note = f" Truth overlap against {truth_name} is {left_name}={left_truth:.2f}, {right_name}={right_truth:.2f}."

    summary = (
        f"Cross-survey anomaly catalog profile for {left_name} vs {right_name}: "
        f"{match_summary.get('matched_count', 0)} matched sources, "
        f"match fraction {match_summary.get('match_fraction', 0.0):.2f}, "
        f"median separation {match_summary.get('median_separation_arcsec')} arcsec."
        f"{truth_note}"
    )
    anomalies = list(profile.get("review_flags") or [])
    if flux_residuals.get("p95_fraction") is not None:
        anomalies.append(f"P95 flux residual fraction is {flux_residuals['p95_fraction']:.3f}.")

    significance = min(round(0.45 + min(match_summary.get("match_fraction", 0.0), 1.0) * 0.25, 3), 0.72)
    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "cross_survey_catalog_anomaly_bundle",
        "summary": summary,
        "entities": [left_name, right_name, "synthetic anomaly catalogs"],
        "topics": [
            "cross-survey anomaly detection",
            "catalog alignment",
            "synthetic benchmark extraction",
            "false-positive control",
        ],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": None,
        "challenges_hypothesis": None,
        "domain_tags": ["synthetic_data", "anomaly_detection", "survey_catalogs", "benchmarking"],
        "source_catalogs": [left_name, right_name, *( [truth_overlap.get("truth_name")] if truth_overlap else [] )],
        "target": {
            "name": f"{left_name} vs {right_name}",
            "input_target": f"{left_name}:{right_name}",
            "kind": "cross_survey_catalog_benchmark",
        },
        "structured_evidence": {
            "benchmark_profile": profile,
        },
        "new_hypothesis": None,
        "manatuabon_context": {
            "acknowledgement": OPENUNIVERSE_ACKNOWLEDGEMENT,
            "simulation_only": True,
            "recommended_mode": "evidence_only",
            "threshold_separation": "Synthetic catalog anomaly thresholds must stay separate from observational council confidence.",
        },
    }


def write_cross_survey_catalog_files(profile: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = output_dir / f"cross_survey_catalog_profile_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(profile, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)
    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="cross_survey_catalog_anomaly_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_cross_survey_catalog_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract deterministic cross-survey anomaly features from local synthetic catalogs.")
    parser.add_argument("--left", required=True, help="Path to the first catalog (.csv, .json, .jsonl)")
    parser.add_argument("--right", required=True, help="Path to the second catalog (.csv, .json, .jsonl)")
    parser.add_argument("--truth", default=None, help="Optional truth catalog (.csv, .json, .jsonl) used to measure recall")
    parser.add_argument("--left-name", default=None, help="Display label for the first catalog")
    parser.add_argument("--right-name", default=None, help="Display label for the second catalog")
    parser.add_argument("--truth-name", default=None, help="Display label for the truth catalog")
    parser.add_argument("--max-sep-arcsec", type=float, default=1.0, help="Maximum angular separation for cross-match pairing")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for derived anomaly artifacts")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the benchmark bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    left_path = Path(args.left)
    right_path = Path(args.right)
    truth_path = Path(args.truth) if args.truth else None

    left_name = args.left_name or left_path.stem
    right_name = args.right_name or right_path.stem
    truth_name = args.truth_name or (truth_path.stem if truth_path else None)

    left_rows_raw = load_catalog_rows(left_path)
    right_rows_raw = load_catalog_rows(right_path)
    truth_rows_raw = load_catalog_rows(truth_path) if truth_path else None

    left_rows, left_metadata = normalize_catalog_rows(left_rows_raw, catalog_name=left_name)
    right_rows, right_metadata = normalize_catalog_rows(right_rows_raw, catalog_name=right_name)
    truth_rows = None
    truth_metadata = None
    if truth_rows_raw is not None and truth_name is not None:
        truth_rows, truth_metadata = normalize_catalog_rows(truth_rows_raw, catalog_name=truth_name)

    profile = build_cross_survey_catalog_profile(
        left_rows,
        right_rows,
        left_name=left_name,
        right_name=right_name,
        max_sep_arcsec=args.max_sep_arcsec,
        truth_rows=truth_rows,
        truth_name=truth_name,
        left_metadata=left_metadata,
        right_metadata=right_metadata,
        truth_metadata=truth_metadata,
    )
    bundle = build_cross_survey_catalog_bundle(profile)
    label = f"{left_name}_vs_{right_name}"
    raw_path, bundle_json, bundle_md = write_cross_survey_catalog_files(profile, bundle, Path(args.inbox), label)
    print(f"Raw cross-survey profile written: {raw_path}")
    print(f"Structured anomaly bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_cross_survey_catalog_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())