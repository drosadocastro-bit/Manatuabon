"""Cross-match Gaia and ZTF structured bundles for deterministic follow-up triage."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


GAIA_ZTF_ACKNOWLEDGEMENT = (
    "Gaia x ZTF matches are follow-up cues that combine stellar astrometry with time-domain image metadata. "
    "Treat them as review context, not autonomous transient claims, until cadence, seeing, and image-level inspection are checked."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now().isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "gaia_ztf"


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


def _extract_ztf_rows(bundle: dict) -> tuple[list[dict], dict]:
    if bundle.get("payload_type") != "ztf_snapshot_bundle":
        raise ValueError("Expected a ztf_snapshot_bundle input.")
    evidence = bundle.get("structured_evidence", {}) if isinstance(bundle.get("structured_evidence"), dict) else {}
    query = evidence.get("query", {}) if isinstance(evidence.get("query"), dict) else {}
    frames = evidence.get("frames", []) if isinstance(evidence.get("frames"), list) else []
    rows = []
    for row in frames:
        ra = _coerce_float(row.get("ra"))
        dec = _coerce_float(row.get("dec"))
        if ra is None or dec is None:
            continue
        rows.append({
            "field": row.get("field"),
            "ccdid": row.get("ccdid"),
            "qid": row.get("qid"),
            "filtercode": row.get("filtercode"),
            "obsjd": _coerce_float(row.get("obsjd")),
            "seeing": _coerce_float(row.get("seeing")),
            "maglimit": _coerce_float(row.get("maglimit")),
            "ra": ra,
            "dec": dec,
            "pid": row.get("pid"),
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


def build_gaia_ztf_anomaly_profile(
    gaia_bundle: dict,
    ztf_bundle: dict,
    *,
    max_sep_arcsec: float = 30.0,
    pm_threshold_masyr: float = 10.0,
    seeing_threshold: float = 2.5,
) -> dict:
    gaia_rows, gaia_query = _extract_gaia_rows(gaia_bundle)
    ztf_rows, ztf_query = _extract_ztf_rows(ztf_bundle)

    matches = []
    for star in gaia_rows:
        best_match = None
        for frame in ztf_rows:
            separation = angular_separation_arcsec(star["ra"], star["dec"], frame["ra"], frame["dec"])
            if separation > max_sep_arcsec:
                continue
            if best_match is None or separation < best_match["separation_arcsec"]:
                proper_motion_total = _proper_motion_total(star)
                pm_total_error = _proper_motion_total_error(star)
                seeing = frame.get("seeing")
                maglimit = frame.get("maglimit")
                parallax_snr = _parallax_snr(star)
                # Proximity-based scoring: closer separation = higher score
                score = max(0.0, 1.0 - (separation / max(max_sep_arcsec, 1e-6))) * 0.4
                if proper_motion_total is not None:
                    score += min(proper_motion_total / max(pm_threshold_masyr, 1e-6), 3.0) / 3.0 * 0.25
                if seeing is not None and seeing <= seeing_threshold:
                    score += 0.1
                if maglimit is not None and maglimit >= 20.0:
                    score += 0.1
                score += min((parallax_snr or 0.0) / 5.0, 1.0) * 0.05
                if star.get("ruwe") is not None and star["ruwe"] <= 1.4:
                    score += 0.05
                score = round(max(score, 0.0), 6)
                good_seeing_flag = bool(seeing is not None and seeing <= seeing_threshold)
                deep_frame_flag = bool(maglimit is not None and maglimit >= 20.0)
                high_pm_flag = bool((proper_motion_total or 0.0) >= pm_threshold_masyr)
                review_priority = "low"
                if (
                    score >= 0.55
                    and high_pm_flag
                    and good_seeing_flag
                    and separation <= max_sep_arcsec * 0.5
                    and (star.get("ruwe") is None or star["ruwe"] <= 1.4)
                ):
                    review_priority = "high"
                elif score >= 0.35 and good_seeing_flag and separation <= max_sep_arcsec * 0.5:
                    review_priority = "medium"
                best_match = {
                    "gaia_source_id": star["source_id"],
                    "ztf_field": frame.get("field"),
                    "ztf_filtercode": frame.get("filtercode"),
                    "ztf_obsjd": frame.get("obsjd"),
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
                    "gaia_ruwe": star.get("ruwe"),
                    "seeing": seeing,
                    "maglimit": maglimit,
                    "good_seeing_flag": good_seeing_flag,
                    "deep_frame_flag": deep_frame_flag,
                    "high_pm_flag": high_pm_flag,
                    "foreground_likely": bool((parallax_snr or 0.0) >= 5.0),
                    "review_priority": review_priority,
                    "candidate_score": score,
                }
        if best_match is not None:
            matches.append(best_match)

    separations = [item["separation_arcsec"] for item in matches]
    pm_values = [item["proper_motion_total_masyr"] for item in matches if item.get("proper_motion_total_masyr") is not None]
    candidate_matches = [item for item in matches if item.get("review_priority") == "high"]
    medium_priority_matches = [item for item in matches if item.get("review_priority") == "medium"]
    review_flags = []
    if not matches:
        review_flags.append("No Gaia stars matched a ZTF frame footprint center within the requested separation threshold.")
    if matches and (_percentile(separations, 0.95) or 0.0) > max_sep_arcsec * 0.8:
        review_flags.append("Match separations approach the configured threshold; image-footprint geometry should be checked before treating a match as meaningful.")
    if not any(item.get("good_seeing_flag") for item in matches):
        review_flags.append("Matched ZTF frames do not show good seeing, so image-quality follow-up is weak.")
    if candidate_matches and all(item.get("foreground_likely") for item in candidate_matches):
        review_flags.append("Top Gaia x ZTF candidates are likely foreground stars appearing in ordinary survey footprints rather than transient anomalies.")
    if any((item.get("gaia_ruwe") or 0.0) > 1.4 for item in matches):
        review_flags.append("Some Gaia matches have elevated RUWE values; astrometric quality should be reviewed before escalation.")

    return {
        "kind": "gaia_ztf_anomaly_profile",
        "generated_at": iso_timestamp(),
        "pair": {
            "left": "Gaia DR3",
            "right": "ZTF",
            "max_separation_arcsec": max_sep_arcsec,
            "proper_motion_threshold_masyr": pm_threshold_masyr,
            "seeing_threshold": seeing_threshold,
        },
        "gaia_query": gaia_query,
        "ztf_query": ztf_query,
        "gaia_summary": {
            "star_count": len(gaia_rows),
            "proper_motion_count": sum(1 for row in gaia_rows if _proper_motion_total(row) is not None),
            "parallax_count": sum(1 for row in gaia_rows if row.get("parallax") is not None),
        },
        "ztf_summary": {
            "frame_count": len(ztf_rows),
            "good_seeing_count": sum(1 for row in ztf_rows if row.get("seeing") is not None and row.get("seeing") <= seeing_threshold),
            "deep_frame_count": sum(1 for row in ztf_rows if row.get("maglimit") is not None and row.get("maglimit") >= 20.0),
        },
        "match_summary": {
            "matched_star_count": len(matches),
            "unmatched_gaia_count": max(len(gaia_rows) - len(matches), 0),
            "match_fraction": round(len(matches) / max(len(gaia_rows), 1), 6),
            "median_separation_arcsec": _median(separations),
            "p95_separation_arcsec": _percentile(separations, 0.95),
            "median_proper_motion_masyr": _median(pm_values),
            "high_pm_match_count": sum(1 for item in matches if item.get("high_pm_flag")),
            "good_seeing_match_count": sum(1 for item in matches if item.get("good_seeing_flag")),
            "medium_priority_match_count": len(medium_priority_matches),
            "candidate_count": len(candidate_matches),
        },
        "anomaly_candidates": sorted(matches, key=lambda item: item["candidate_score"], reverse=True)[:8],
        "review_flags": review_flags,
        "recommended_actions": [
            "Inspect Gaia x ZTF candidates in cutouts before treating them as transient anomalies rather than ordinary stellar coverage.",
            "Use Gaia parallax and RUWE to down-rank foreground or low-quality astrometric matches before escalation.",
            "Keep Gaia x ZTF cross-match scores separate from council confidence until a human reviews cadence, seeing, and image context.",
        ],
    }


def build_gaia_ztf_anomaly_bundle(profile: dict) -> dict:
    match_summary = profile.get("match_summary", {}) if isinstance(profile.get("match_summary"), dict) else {}
    summary = (
        f"Gaia x ZTF anomaly profile with {match_summary.get('matched_star_count', 0)} matched Gaia star(s), "
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
    if match_summary.get("good_seeing_match_count", 0):
        significance += 0.05
    significance = min(round(significance, 3), 0.72)
    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "gaia_ztf_anomaly_bundle",
        "summary": summary,
        "entities": ["Gaia DR3", "ZTF", "cross-match anomaly triage"],
        "topics": ["observational anomaly triage", "Gaia astrometry", "ZTF time-domain metadata", "cross-survey review"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": None,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "anomaly_detection", "gaia", "ztf", "cross_match"],
        "source_catalogs": ["Gaia DR3", "ZTF via IRSA"],
        "target": {"name": "Gaia DR3 vs ZTF", "input_target": "Gaia DR3:ZTF", "kind": "observational_cross_match"},
        "structured_evidence": {"anomaly_profile": profile},
        "new_hypothesis": None,
        "manatuabon_context": {
            "acknowledgement": GAIA_ZTF_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
            "threshold_separation": "Cross-match scores are review cues only and must not be promoted to council confidence without human validation.",
        },
    }


def write_gaia_ztf_anomaly_files(profile: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = output_dir / f"gaia_ztf_anomaly_profile_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(profile, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)
    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="gaia_ztf_anomaly_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_gaia_ztf_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-match Gaia and ZTF structured bundles for deterministic follow-up triage.")
    parser.add_argument("--gaia-bundle", required=True, help="Path to a Gaia structured bundle JSON")
    parser.add_argument("--ztf-bundle", required=True, help="Path to a ZTF structured bundle JSON")
    parser.add_argument("--max-sep-arcsec", type=float, default=30.0, help="Maximum angular separation used to link Gaia stars to ZTF frame centers")
    parser.add_argument("--pm-threshold-masyr", type=float, default=10.0, help="Proper-motion threshold used to mark review candidates")
    parser.add_argument("--seeing-threshold", type=float, default=2.5, help="Seeing threshold used to mark better-quality ZTF coverage")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for derived anomaly artifacts")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    gaia_bundle = load_structured_bundle(Path(args.gaia_bundle))
    ztf_bundle = load_structured_bundle(Path(args.ztf_bundle))
    profile = build_gaia_ztf_anomaly_profile(
        gaia_bundle,
        ztf_bundle,
        max_sep_arcsec=args.max_sep_arcsec,
        pm_threshold_masyr=args.pm_threshold_masyr,
        seeing_threshold=args.seeing_threshold,
    )
    bundle = build_gaia_ztf_anomaly_bundle(profile)
    raw_path, bundle_json, bundle_md = write_gaia_ztf_anomaly_files(profile, bundle, Path(args.inbox), "gaia_vs_ztf")
    print(f"Raw Gaia x ZTF profile written: {raw_path}")
    print(f"Structured anomaly bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_gaia_ztf_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())