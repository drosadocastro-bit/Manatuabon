"""Export cross-match anomaly bundles as CSV + markdown summary tables.

Reads one or more anomaly profile JSON files produced by the cross-match workers
and writes:
  1. A flat CSV with one row per matched candidate — suitable for paper supplementary material.
  2. A markdown summary table — suitable for inclusion in analysis notebooks or reports.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


EXPORT_SCHEMA = "analysis_export_v1"


def _load_profile(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _extract_candidates(profile: dict) -> list[dict]:
    """Return all candidate dicts from a cross-match anomaly profile."""
    # Profiles have 'anomaly_candidates' at top level
    candidates = profile.get("anomaly_candidates")
    if isinstance(candidates, list):
        return candidates
    # Bundles nest profile inside structured_evidence
    evidence = profile.get("structured_evidence", {})
    if isinstance(evidence, dict):
        inner = evidence.get("anomaly_profile", {})
        if isinstance(inner, dict):
            return inner.get("anomaly_candidates") or []
    return []


def _profile_kind(profile: dict) -> str:
    kind = profile.get("kind")
    if kind:
        return kind
    evidence = profile.get("structured_evidence", {})
    if isinstance(evidence, dict):
        inner = evidence.get("anomaly_profile", {})
        if isinstance(inner, dict):
            return inner.get("kind", "unknown")
    return "unknown"


# Column ordering for CSV: common columns first, then worker-specific columns follow naturally.
_COMMON_COLUMNS = [
    "gaia_source_id",
    "separation_arcsec",
    "proper_motion_total_masyr",
    "pm_total_error_masyr",
    "parallax",
    "parallax_error",
    "parallax_snr",
    "pmra",
    "pmra_error",
    "pmdec",
    "pmdec_error",
    "gaia_g_mag",
    "gaia_ruwe",
    "foreground_likely",
    "high_pm_flag",
    "review_priority",
    "candidate_score",
]


def _ordered_columns(all_keys: set[str]) -> list[str]:
    ordered = [col for col in _COMMON_COLUMNS if col in all_keys]
    remaining = sorted(all_keys - set(ordered))
    return ordered + remaining


def export_csv(candidates: list[dict], output_path: Path) -> Path:
    if not candidates:
        output_path.write_text("# No candidates to export.\n", encoding="utf-8")
        return output_path
    all_keys: set[str] = set()
    for row in candidates:
        all_keys.update(row.keys())
    columns = _ordered_columns(all_keys)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in candidates:
            writer.writerow(row)
    return output_path


def export_markdown_table(candidates: list[dict], output_path: Path, *, profile_kind: str = "unknown") -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"# Analysis Export — {profile_kind}",
        f"",
        f"Generated: {stamp}  ",
        f"Candidate count: {len(candidates)}",
        f"",
    ]
    if not candidates:
        lines.append("_No candidates to display._")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return output_path

    display_cols = [
        "gaia_source_id",
        "separation_arcsec",
        "proper_motion_total_masyr",
        "pm_total_error_masyr",
        "parallax_snr",
        "review_priority",
        "candidate_score",
    ]
    present_cols = [col for col in display_cols if any(row.get(col) is not None for row in candidates)]
    if not present_cols:
        present_cols = list(candidates[0].keys())[:7]

    header = "| " + " | ".join(present_cols) + " |"
    separator = "| " + " | ".join("---" for _ in present_cols) + " |"
    lines.append(header)
    lines.append(separator)
    for row in candidates:
        cells = []
        for col in present_cols:
            val = row.get(col)
            if val is None:
                cells.append("")
            elif isinstance(val, float):
                cells.append(f"{val:.6f}")
            else:
                cells.append(str(val))
        lines.append("| " + " | ".join(cells) + " |")

    lines.append("")
    lines.append("---")
    lines.append(f"_Exported by Manatuabon analysis_export ({EXPORT_SCHEMA})._")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def export_profiles(
    profile_paths: list[Path],
    output_dir: Path,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_candidates: list[dict] = []
    kinds: list[str] = []
    for path in profile_paths:
        profile = _load_profile(path)
        kind = _profile_kind(profile)
        kinds.append(kind)
        candidates = _extract_candidates(profile)
        # Tag each candidate with its source profile for traceability
        for row in candidates:
            row.setdefault("source_profile", path.name)
            row.setdefault("profile_kind", kind)
        all_candidates.extend(candidates)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    combined_kind = ", ".join(sorted(set(kinds))) if kinds else "unknown"
    csv_path = export_csv(all_candidates, output_dir / f"analysis_export_{stamp}.csv")
    md_path = export_markdown_table(
        all_candidates,
        output_dir / f"analysis_export_{stamp}.md",
        profile_kind=combined_kind,
    )
    report = {
        "manatuabon_schema": EXPORT_SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_profiles": [str(p) for p in profile_paths],
        "profile_kinds": kinds,
        "total_candidates": len(all_candidates),
        "csv_path": str(csv_path),
        "markdown_path": str(md_path),
    }
    report_path = output_dir / f"analysis_export_report_{stamp}.json"
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)
    print(f"CSV written: {csv_path}")
    print(f"Markdown written: {md_path}")
    print(f"Export report: {report_path}")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export cross-match anomaly profiles as CSV + markdown summary tables."
    )
    parser.add_argument(
        "--profiles",
        nargs="+",
        required=True,
        help="Paths to one or more anomaly profile or bundle JSON files.",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Output directory for exported files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile_paths = [Path(p) for p in args.profiles]
    export_profiles(profile_paths, Path(args.out_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
