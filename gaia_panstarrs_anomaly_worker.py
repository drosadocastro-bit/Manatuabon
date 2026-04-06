"""Cross-match Gaia and Pan-STARRS structured bundles for deterministic anomaly triage."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path

from db_init import ensure_runtime_db
from extinction_lookup import dereddened_color
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


GAIA_PANSTARRS_ACKNOWLEDGEMENT = (
    "Gaia x Pan-STARRS matches are review cues that combine stellar astrometry with wide-field optical photometry. "
    "Treat them as follow-up context, not physical association evidence, until color, quality flags, and foreground distance are reviewed."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now().isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "gaia_panstarrs"


def angular_separation_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    ra1_rad, dec1_rad, ra2_rad, dec2_rad = map(math.radians, [ra1, dec1, ra2, dec2])
    cos_sep = (
        math.sin(dec1_rad) * math.sin(dec2_rad)
        + math.cos(dec1_rad) * math.cos(dec2_rad) * math.cos(ra1_rad - ra2_rad)
    )
    cos_sep = max(-1.0, min(1.0, cos_sep))
    return math.degrees(math.acos(cos_sep)) * 3600.0


def _coerce_float(value):
    if value in (None, "", "null", "None"):
        return None
    return float(value)


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


def load_structured_bundle(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        bundle = json.load(handle)
    if bundle.get("manatuabon_schema") != "structured_ingest_v1":
        raise ValueError(f"Unsupported bundle schema in {path}")
    return bundle


def _extract_gaia_rows(bundle: dict) -> tuple[list[dict], dict]:
    if bundle.get("payload_type") != "gaia_snapshot_bundle":
        raise ValueError("Expected a gaia_snapshot_bundle input.")
    evidence = bundle.get("structured_evidence", {}) if isinstance(bundle.get("structured_evidence"), dict) else {}
    query = evidence.get("query", {}) if isinstance(evidence.get("query"), dict) else {}
    stars = evidence.get("stars", []) if isinstance(evidence.get("stars"), list) else []
    rows = []
    for row in stars:
        ra = _coerce_float(row.get("ra"))
        dec = _coerce_float(row.get("dec"))
        if ra is None or dec is None:
            continue
        rows.append({
            "source_id": str(row.get("source_id") or "unknown"),
            "ra": ra,
            "dec": dec,
            "parallax": _coerce_float(row.get("parallax")),
            "parallax_error": _coerce_float(row.get("parallax_error")),
            "pmra": _coerce_float(row.get("pmra")),
            "pmdec": _coerce_float(row.get("pmdec")),
            "pmra_error": _coerce_float(row.get("pmra_error")),
            "pmdec_error": _coerce_float(row.get("pmdec_error")),
            "phot_g_mean_mag": _coerce_float(row.get("phot_g_mean_mag")),
            "ruwe": _coerce_float(row.get("ruwe")),
        })
    return rows, query


def _extract_panstarrs_rows(bundle: dict) -> tuple[list[dict], dict]:
    if bundle.get("payload_type") != "panstarrs_snapshot_bundle":
        raise ValueError("Expected a panstarrs_snapshot_bundle input.")
    evidence = bundle.get("structured_evidence", {}) if isinstance(bundle.get("structured_evidence"), dict) else {}
    query = evidence.get("query", {}) if isinstance(evidence.get("query"), dict) else {}
    objects = evidence.get("objects", []) if isinstance(evidence.get("objects"), list) else []
    rows = []
    for row in objects:
        ra = _coerce_float(row.get("raMean"))
        dec = _coerce_float(row.get("decMean"))
        if ra is None or dec is None:
            continue
        rows.append({
            "objID": row.get("objID"),
            "ra": ra,
            "dec": dec,
            "nDetections": row.get("nDetections"),
            "gMeanPSFMag": _coerce_float(row.get("gMeanPSFMag")),
            "rMeanPSFMag": _coerce_float(row.get("rMeanPSFMag")),
            "iMeanPSFMag": _coerce_float(row.get("iMeanPSFMag")),
            "qualityFlag": row.get("qualityFlag"),
            "objInfoFlag": row.get("objInfoFlag"),
            "extinction_ebv": _coerce_float(row.get("extinction_ebv")),
            "extinction_method": row.get("extinction_method"),
        })
    return rows, query


def _proper_motion_total(row: dict) -> float | None:
    components = [item for item in (_coerce_float(row.get("pmra")), _coerce_float(row.get("pmdec"))) if item is not None]
    if not components:
        return None
    if len(components) == 1:
        return abs(components[0])
    return math.sqrt(components[0] ** 2 + components[1] ** 2)


def _proper_motion_total_error(row: dict) -> float | None:
    """Propagate pmra_error and pmdec_error through pm_total = sqrt(pmra^2 + pmdec^2)."""
    pmra = _coerce_float(row.get("pmra"))
    pmdec = _coerce_float(row.get("pmdec"))
    pmra_err = _coerce_float(row.get("pmra_error"))
    pmdec_err = _coerce_float(row.get("pmdec_error"))
    if pmra is None or pmdec is None or pmra_err is None or pmdec_err is None:
        return None
    pm_total = math.sqrt(pmra ** 2 + pmdec ** 2)
    if pm_total < 1e-12:
        return None
    return math.sqrt((pmra * pmra_err) ** 2 + (pmdec * pmdec_err) ** 2) / pm_total


def _parallax_snr(row: dict) -> float | None:
    parallax = _coerce_float(row.get("parallax"))
    error = _coerce_float(row.get("parallax_error"))
    if parallax is None or error in (None, 0.0):
        return None
    return abs(parallax) / error


def _color_index(row: dict, left: str, right: str) -> float | None:
    left_mag = _coerce_float(row.get(left))
    right_mag = _coerce_float(row.get(right))
    if left_mag is None or right_mag is None:
        return None
    return round(left_mag - right_mag, 6)


def _has_nonzero_flag(value) -> bool:
    if value in (None, "", "null", "None"):
        return False
    try:
        return int(value) != 0
    except (TypeError, ValueError):
        return True


def _candidate_score(
    *,
    separation_arcsec: float,
    max_sep_arcsec: float,
    proper_motion_total: float | None,
    pm_threshold_masyr: float,
    detections: int,
    min_detections: int,
    parallax_snr: float | None,
    gaia_ruwe: float | None,
    g_r_color: float | None,
    r_i_color: float | None,
    quality_flag,
    obj_info_flag,
) -> float:
    sep_term = max(0.0, 1.0 - (separation_arcsec / max(max_sep_arcsec, 1e-6))) * 0.4
    pm_term = min((proper_motion_total or 0.0) / max(pm_threshold_masyr, 1e-6), 3.0) / 3.0 * 0.25
    detection_term = min(detections / max(min_detections, 1), 1.0) * 0.15
    color_term = 0.0
    if g_r_color is not None:
        color_term += 0.05
    if r_i_color is not None:
        color_term += 0.05
    parallax_term = min((parallax_snr or 0.0) / 5.0, 1.0) * 0.05
    ruwe_term = 0.05 if gaia_ruwe is not None and gaia_ruwe <= 1.4 else 0.0
    quality_penalty = 0.0
    if _has_nonzero_flag(quality_flag):
        quality_penalty += 0.12
    if _has_nonzero_flag(obj_info_flag):
        quality_penalty += 0.08
    score = sep_term + pm_term + detection_term + color_term + parallax_term + ruwe_term - quality_penalty
    return round(max(score, 0.0), 6)


def build_gaia_panstarrs_anomaly_profile(
    gaia_bundle: dict,
    panstarrs_bundle: dict,
    *,
    max_sep_arcsec: float = 5.0,
    pm_threshold_masyr: float = 10.0,
    min_detections: int = 3,
) -> dict:
    gaia_rows, gaia_query = _extract_gaia_rows(gaia_bundle)
    pan_rows, pan_query = _extract_panstarrs_rows(panstarrs_bundle)

    matches = []
    for star in gaia_rows:
        best_match = None
        for obj in pan_rows:
            separation = angular_separation_arcsec(star["ra"], star["dec"], obj["ra"], obj["dec"])
            if separation > max_sep_arcsec:
                continue
            if best_match is None or separation < best_match["separation_arcsec"]:
                proper_motion_total = _proper_motion_total(star)
                pm_total_error = _proper_motion_total_error(star)
                detections = int(obj.get("nDetections") or 0)
                parallax_snr = _parallax_snr(star)
                g_r_color = _color_index(obj, "gMeanPSFMag", "rMeanPSFMag")
                r_i_color = _color_index(obj, "rMeanPSFMag", "iMeanPSFMag")
                # Dereddened colors using Pan-STARRS extinction_ebv when available
                ebv = obj.get("extinction_ebv")
                g_r_color_dered = None
                r_i_color_dered = None
                if ebv is not None and g_r_color is not None:
                    g_mag = _coerce_float(obj.get("gMeanPSFMag"))
                    r_mag = _coerce_float(obj.get("rMeanPSFMag"))
                    if g_mag is not None and r_mag is not None:
                        g_r_color_dered = dereddened_color(g_mag, r_mag, ebv, "ps_g", "ps_r")
                if ebv is not None and r_i_color is not None:
                    r_mag = _coerce_float(obj.get("rMeanPSFMag"))
                    i_mag = _coerce_float(obj.get("iMeanPSFMag"))
                    if r_mag is not None and i_mag is not None:
                        r_i_color_dered = dereddened_color(r_mag, i_mag, ebv, "ps_r", "ps_i")
                # Use dereddened colors for scoring when available, raw otherwise
                score_g_r = g_r_color_dered if g_r_color_dered is not None else g_r_color
                score_r_i = r_i_color_dered if r_i_color_dered is not None else r_i_color
                gaia_ruwe = star.get("ruwe")
                quality_flag = obj.get("qualityFlag")
                obj_info_flag = obj.get("objInfoFlag")
                score = _candidate_score(
                    separation_arcsec=separation,
                    max_sep_arcsec=max_sep_arcsec,
                    proper_motion_total=proper_motion_total,
                    pm_threshold_masyr=pm_threshold_masyr,
                    detections=detections,
                    min_detections=min_detections,
                    parallax_snr=parallax_snr,
                    gaia_ruwe=gaia_ruwe,
                    g_r_color=score_g_r,
                    r_i_color=score_r_i,
                    quality_flag=quality_flag,
                    obj_info_flag=obj_info_flag,
                )
                persistent_detection_flag = bool(detections >= min_detections)
                high_pm_flag = bool((proper_motion_total or 0.0) >= pm_threshold_masyr)
                quality_flagged = _has_nonzero_flag(quality_flag) or _has_nonzero_flag(obj_info_flag)
                review_priority = "low"
                if (
                    score >= 0.55
                    and high_pm_flag
                    and persistent_detection_flag
                    and separation <= max_sep_arcsec * 0.5
                    and (gaia_ruwe is None or gaia_ruwe <= 1.4)
                ):
                    review_priority = "high"
                elif score >= 0.35 and persistent_detection_flag and separation <= max_sep_arcsec * 0.5:
                    review_priority = "medium"
                best_match = {
                    "gaia_source_id": star["source_id"],
                    "panstarrs_objID": obj.get("objID"),
                    "separation_arcsec": round(separation, 6),
                    "proper_motion_total_masyr": round(proper_motion_total, 6) if proper_motion_total is not None else None,
                    "pm_total_error_masyr": round(pm_total_error, 6) if pm_total_error is not None else None,
                    "parallax": star.get("parallax"),
                    "parallax_error": star.get("parallax_error"),
                    "parallax_snr": round(parallax_snr, 6) if parallax_snr is not None else None,
                    "pmra": star.get("pmra"),
                    "pmra_error": star.get("pmra_error"),
                    "pmdec": star.get("pmdec"),
                    "pmdec_error": star.get("pmdec_error"),
                    "gaia_g_mag": star.get("phot_g_mean_mag"),
                    "gaia_ruwe": gaia_ruwe,
                    "nDetections": detections,
                    "g_r_color": g_r_color,
                    "r_i_color": r_i_color,
                    "g_r_color_dereddened": g_r_color_dered,
                    "r_i_color_dereddened": r_i_color_dered,
                    "extinction_ebv": ebv,
                    "qualityFlag": quality_flag,
                    "objInfoFlag": obj_info_flag,
                    "foreground_likely": bool((parallax_snr or 0.0) >= 5.0),
                    "high_pm_flag": high_pm_flag,
                    "persistent_detection_flag": persistent_detection_flag,
                    "quality_flagged": quality_flagged,
                    "review_priority": review_priority,
                    "candidate_score": round(score, 6),
                }
        if best_match is not None:
            matches.append(best_match)

    separations = [item["separation_arcsec"] for item in matches]
    pm_values = [item["proper_motion_total_masyr"] for item in matches if item.get("proper_motion_total_masyr") is not None]
    candidate_matches = [item for item in matches if item.get("review_priority") == "high"]
    medium_priority_matches = [item for item in matches if item.get("review_priority") == "medium"]
    review_flags = []
    if not matches:
        review_flags.append("No Gaia stars matched a Pan-STARRS object within the requested separation threshold.")
    if matches and (_percentile(separations, 0.95) or 0.0) > max_sep_arcsec * 0.8:
        review_flags.append("Match separations approach the configured threshold; astrometric alignment should be checked before using the pair for anomaly triage.")
    if not any(item.get("persistent_detection_flag") for item in matches):
        review_flags.append("Matched Pan-STARRS rows do not show repeated detections, so persistence-based optical review is weak.")
    if candidate_matches and all(item.get("foreground_likely") for item in candidate_matches):
        review_flags.append("Top Gaia x Pan-STARRS candidates are likely foreground stars with optical counterparts rather than exotic outliers.")
    if any((item.get("gaia_ruwe") or 0.0) > 1.4 for item in matches):
        review_flags.append("Some Gaia matches have elevated RUWE values; astrometric quality should be reviewed before escalation.")
    if any(item.get("quality_flagged") for item in matches):
        review_flags.append("Some matched Pan-STARRS rows carry non-zero quality or object-info flags, so photometric interpretation should stay conservative.")

    return {
        "kind": "gaia_panstarrs_anomaly_profile",
        "generated_at": iso_timestamp(),
        "pair": {
            "left": "Gaia DR3",
            "right": "Pan-STARRS DR2",
            "max_separation_arcsec": max_sep_arcsec,
            "proper_motion_threshold_masyr": pm_threshold_masyr,
            "min_detections": min_detections,
        },
        "gaia_query": gaia_query,
        "panstarrs_query": pan_query,
        "gaia_summary": {
            "star_count": len(gaia_rows),
            "proper_motion_count": sum(1 for row in gaia_rows if _proper_motion_total(row) is not None),
            "parallax_count": sum(1 for row in gaia_rows if row.get("parallax") is not None),
        },
        "panstarrs_summary": {
            "row_count": len(pan_rows),
            "persistent_detection_count": sum(1 for row in pan_rows if int(row.get("nDetections") or 0) >= min_detections),
            "multiband_count": sum(1 for row in pan_rows if _color_index(row, "gMeanPSFMag", "rMeanPSFMag") is not None),
        },
        "match_summary": {
            "matched_star_count": len(matches),
            "unmatched_gaia_count": max(len(gaia_rows) - len(matches), 0),
            "match_fraction": round(len(matches) / max(len(gaia_rows), 1), 6),
            "median_separation_arcsec": _median(separations),
            "p95_separation_arcsec": _percentile(separations, 0.95),
            "median_proper_motion_masyr": _median(pm_values),
            "high_pm_match_count": sum(1 for item in matches if item.get("high_pm_flag")),
            "persistent_detection_match_count": sum(1 for item in matches if item.get("persistent_detection_flag")),
            "quality_flagged_match_count": sum(1 for item in matches if item.get("quality_flagged")),
            "medium_priority_match_count": len(medium_priority_matches),
            "candidate_count": len(candidate_matches),
        },
        "anomaly_candidates": sorted(matches, key=lambda item: item["candidate_score"], reverse=True)[:8],
        "review_flags": review_flags,
        "recommended_actions": [
            "Inspect Gaia x Pan-STARRS candidates in optical cutouts before treating them as meaningful anomalies rather than ordinary stellar counterparts.",
            "Use Gaia parallax and RUWE to down-rank foreground or low-quality astrometric matches before escalation.",
            "Keep Gaia x Pan-STARRS cross-match scores separate from council confidence until a human reviews optical context and survey quality flags.",
        ],
    }


def build_gaia_panstarrs_anomaly_bundle(profile: dict) -> dict:
    match_summary = profile.get("match_summary", {}) if isinstance(profile.get("match_summary"), dict) else {}
    summary = (
        f"Gaia x Pan-STARRS anomaly profile with {match_summary.get('matched_star_count', 0)} matched Gaia star(s), "
        f"{match_summary.get('candidate_count', 0)} high-priority review candidate(s), "
        f"{match_summary.get('medium_priority_match_count', 0)} medium-priority match(es), "
        f"and median separation {match_summary.get('median_separation_arcsec')} arcsec."
    )
    anomalies = list(profile.get("review_flags") or [])
    if match_summary.get("median_proper_motion_masyr") is not None:
        anomalies.append(f"Median matched proper motion is {match_summary['median_proper_motion_masyr']:.3f} mas/yr.")
    significance = 0.45
    if match_summary.get("matched_star_count", 0):
        significance += 0.08
    if match_summary.get("candidate_count", 0):
        significance += 0.08
    if match_summary.get("persistent_detection_match_count", 0):
        significance += 0.05
    significance = min(round(significance, 3), 0.72)
    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "gaia_panstarrs_anomaly_bundle",
        "summary": summary,
        "entities": ["Gaia DR3", "Pan-STARRS DR2", "cross-match anomaly triage"],
        "topics": ["observational anomaly triage", "Gaia astrometry", "Pan-STARRS photometry", "cross-survey review"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": None,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "anomaly_detection", "gaia", "panstarrs", "cross_match"],
        "source_catalogs": ["Gaia DR3", "Pan-STARRS DR2"],
        "target": {"name": "Gaia DR3 vs Pan-STARRS DR2", "input_target": "Gaia DR3:Pan-STARRS DR2", "kind": "observational_cross_match"},
        "structured_evidence": {"anomaly_profile": profile},
        "new_hypothesis": None,
        "manatuabon_context": {
            "acknowledgement": GAIA_PANSTARRS_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
            "threshold_separation": "Cross-match scores are review cues only and must not be promoted to council confidence without human validation.",
        },
    }


def write_gaia_panstarrs_anomaly_files(profile: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = output_dir / f"gaia_panstarrs_anomaly_profile_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(profile, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)
    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="gaia_panstarrs_anomaly_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_gaia_panstarrs_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-match Gaia and Pan-STARRS structured bundles for deterministic anomaly triage.")
    parser.add_argument("--gaia-bundle", required=True, help="Path to a Gaia structured bundle JSON")
    parser.add_argument("--panstarrs-bundle", required=True, help="Path to a Pan-STARRS structured bundle JSON")
    parser.add_argument("--max-sep-arcsec", type=float, default=5.0, help="Maximum angular separation used to link Gaia stars to Pan-STARRS objects")
    parser.add_argument("--pm-threshold-masyr", type=float, default=10.0, help="Proper-motion threshold used to mark review candidates")
    parser.add_argument("--min-detections", type=int, default=3, help="Minimum Pan-STARRS detection count used to mark persistent counterparts")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for derived anomaly artifacts")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    gaia_bundle = load_structured_bundle(Path(args.gaia_bundle))
    panstarrs_bundle = load_structured_bundle(Path(args.panstarrs_bundle))
    profile = build_gaia_panstarrs_anomaly_profile(
        gaia_bundle,
        panstarrs_bundle,
        max_sep_arcsec=args.max_sep_arcsec,
        pm_threshold_masyr=args.pm_threshold_masyr,
        min_detections=args.min_detections,
    )
    bundle = build_gaia_panstarrs_anomaly_bundle(profile)
    raw_path, bundle_json, bundle_md = write_gaia_panstarrs_anomaly_files(profile, bundle, Path(args.inbox), "gaia_vs_panstarrs")
    print(f"Raw Gaia x Pan-STARRS profile written: {raw_path}")
    print(f"Structured anomaly bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_gaia_panstarrs_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())