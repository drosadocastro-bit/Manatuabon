"""Build deterministic anomaly benchmark bundles from synthetic structured ingest files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from openuniverse_snapshot_importer import OPENUNIVERSE_ACKNOWLEDGEMENT
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


BENCHMARK_AXES = [
    {
        "axis": "cross_survey_alignment",
        "description": "Measure whether Roman and Rubin detections stay positionally and morphologically consistent after survey-specific calibration.",
        "features": ["centroid residuals", "shape moments", "cross-match completeness"],
    },
    {
        "axis": "truth_consistency",
        "description": "Compare extracted detections against synthetic truth products so false positives can be bounded before real-sky use.",
        "features": ["truth-match recall", "false-positive rate", "label agreement"],
    },
    {
        "axis": "photometric_residuals",
        "description": "Track flux disagreement introduced by calibration, coaddition, and survey transfer effects.",
        "features": ["flux residuals", "magnitude offsets", "bandpass consistency"],
    },
    {
        "axis": "morphology_shift",
        "description": "Detect when source extent or surface-brightness structure drifts beyond the synthetic baseline.",
        "features": ["ellipticity drift", "profile concentration", "PSF-subtracted residuals"],
    },
]

SCORE_RULES = [
    ("cross_survey_pair", 0.30, "Roman and Rubin coverage are both present for cross-survey anomaly checks."),
    ("truth_products_present", 0.20, "Truth products are available for deterministic anomaly validation."),
    ("calibrated_products_present", 0.15, "Calibrated products are available for pipeline-stage comparison."),
    ("stage_diversity_present", 0.10, "Raw or coadded products are available to compare multiple reduction stages."),
    ("tutorial_support", 0.10, "Tutorials or notebooks exist for repeatable onboarding."),
    ("public_access", 0.10, "Resources appear publicly accessible without paid or authenticated gating."),
    ("documentation_support", 0.05, "Documentation, license, and citation context are present for auditability."),
]

DEFAULT_THRESHOLDS = {
    "review_alert": 0.65,
    "high_priority_alert": 0.85,
    "cross_survey_alignment_floor": 0.55,
}


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "synthetic_benchmark"


def _normalize_list(value) -> list:
    return value if isinstance(value, list) else []


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in tokens)


def load_synthetic_bundle(bundle_path: Path) -> dict:
    with open(bundle_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"Synthetic bundle must decode to a JSON object: {bundle_path}")

    schema = str(payload.get("manatuabon_schema") or "").strip().lower()
    if schema != "structured_ingest_v1":
        raise ValueError(f"Unsupported ingest schema for anomaly benchmarking: {bundle_path.name}")

    domain_tags = {str(tag).strip().lower() for tag in _normalize_list(payload.get("domain_tags")) if str(tag).strip()}
    payload_type = str(payload.get("payload_type") or "").strip()
    if "synthetic_data" not in domain_tags and payload_type != "openuniverse_snapshot_bundle":
        raise ValueError(f"Bundle is not marked as synthetic benchmark input: {bundle_path.name}")

    return payload


def build_anomaly_benchmark_profile(source_bundle: dict) -> dict:
    target = source_bundle.get("target") if isinstance(source_bundle.get("target"), dict) else {}
    structured_evidence = source_bundle.get("structured_evidence") if isinstance(source_bundle.get("structured_evidence"), dict) else {}
    manifest = structured_evidence.get("manifest") if isinstance(structured_evidence.get("manifest"), dict) else {}
    resources = [item for item in _normalize_list(structured_evidence.get("resources")) if isinstance(item, dict)]
    tutorials = [item for item in _normalize_list(structured_evidence.get("tutorials")) if isinstance(item, dict)]

    name = str(target.get("name") or manifest.get("Name") or target.get("input_target") or "synthetic dataset").strip()
    descriptions = []
    for field in ("Name", "Description", "Documentation", "Citation"):
        value = manifest.get(field)
        if value:
            descriptions.append(str(value))
    for resource in resources:
        descriptions.extend([
            str(resource.get("description") or ""),
            str(resource.get("arn") or ""),
            str(resource.get("type") or ""),
        ])
    description_blob = " ".join(part for part in descriptions if part).lower()

    signals = {
        "roman_present": _contains_any(description_blob, ("roman",)),
        "rubin_present": _contains_any(description_blob, ("rubin",)),
        "truth_products_present": _contains_any(description_blob, ("truth", "ground truth")),
        "calibrated_products_present": _contains_any(description_blob, ("calibrated", "calibration")),
        "raw_products_present": _contains_any(description_blob, (" raw", "raw ", "/raw", "raw,")),
        "coadded_products_present": _contains_any(description_blob, ("coadd", "coadded")),
        "tutorial_support": bool(tutorials),
        "documentation_support": any(manifest.get(field) for field in ("Documentation", "License", "Citation")),
    }
    signals["cross_survey_pair"] = signals["roman_present"] and signals["rubin_present"]
    signals["stage_diversity_present"] = signals["raw_products_present"] or signals["coadded_products_present"]
    signals["public_access"] = bool(resources) and all(
        not bool(resource.get("account_required")) and not bool(resource.get("requester_pays"))
        for resource in resources
    )

    score_breakdown = []
    score = 0.0
    for signal_name, weight, reason in SCORE_RULES:
        met = bool(signals.get(signal_name))
        contribution = round(weight if met else 0.0, 3)
        score += contribution
        score_breakdown.append({
            "signal": signal_name,
            "weight": weight,
            "met": met,
            "contribution": contribution,
            "reason": reason,
        })
    score = round(min(score, 1.0), 3)

    review_flags = [
        "Derived from synthetic metadata only; image-level anomaly scores require downstream feature extraction from the actual survey products.",
    ]
    if not signals["cross_survey_pair"]:
        review_flags.append("Cross-survey pairing is incomplete; Roman and Rubin are not both visible in the source bundle metadata.")
    if not signals["truth_products_present"]:
        review_flags.append("Truth-product references were not found; false-positive benchmarking may be underconstrained.")
    if not signals["calibrated_products_present"]:
        review_flags.append("Calibrated products were not detected in metadata; stage-to-stage drift checks may be limited.")
    if not signals["public_access"]:
        review_flags.append("One or more resources appear gated; benchmark reproducibility may depend on external credentials or requester-pays access.")
    if not signals["tutorial_support"]:
        review_flags.append("No tutorial or notebook references were found; analyst onboarding will be slower.")

    recommended_actions = [
        "Extract matched source catalogs from Roman and Rubin synthetic products and measure centroid, flux, and morphology residuals.",
        "Use truth products to estimate false-positive rate before enabling anomaly alerts on observational streams.",
        "Freeze a benchmark threshold set locally and keep synthetic-derived thresholds separate from council evidence scoring.",
    ]

    return {
        "dataset": name,
        "source_payload_type": str(source_bundle.get("payload_type") or "structured_bundle"),
        "source_summary": str(source_bundle.get("summary") or "").strip(),
        "resource_count": len(resources),
        "tutorial_count": len(tutorials),
        "signals": signals,
        "score": score,
        "score_breakdown": score_breakdown,
        "review_flags": review_flags,
        "benchmark_axes": BENCHMARK_AXES,
        "recommended_actions": recommended_actions,
        "thresholds": dict(DEFAULT_THRESHOLDS),
    }


def build_anomaly_benchmark_bundle(
    source_bundle: dict,
    profile: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
) -> dict:
    target = source_bundle.get("target") if isinstance(source_bundle.get("target"), dict) else {}
    name = str(profile.get("dataset") or target.get("name") or "synthetic benchmark").strip()
    score = float(profile.get("score") or 0.0)
    summary = (
        f"Deterministic anomaly benchmark profile for {name} scored {score:.2f} readiness from synthetic bundle metadata "
        f"across cross-survey, truth, calibration, and reproducibility checks."
    )
    anomalies = list(profile.get("review_flags") or [])
    significance = round(min(0.5 + score * 0.2, 0.7), 3)
    entities = [item for item in [name, "Nancy Grace Roman Telescope", "Vera C. Rubin Observatory"] if item]
    topics = [
        "anomaly detection benchmarking",
        "synthetic survey calibration",
        "cross-survey alignment",
        "false-positive control",
    ]
    domain_tags = sorted({
        *[str(tag) for tag in _normalize_list(source_bundle.get("domain_tags")) if str(tag).strip()],
        "benchmarking",
    })

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "anomaly_benchmark_bundle",
        "summary": summary,
        "entities": entities,
        "topics": topics,
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": domain_tags,
        "source_catalogs": [
            "Synthetic benchmark bundle",
            str(profile.get("source_payload_type") or "structured_bundle"),
        ],
        "target": {
            "name": name,
            "input_target": target.get("input_target") or name,
            "kind": "synthetic_anomaly_benchmark",
        },
        "structured_evidence": {
            "source_bundle_summary": profile.get("source_summary"),
            "source_payload_type": profile.get("source_payload_type"),
            "benchmark_profile": profile,
            "source_target": target,
        },
        "new_hypothesis": None,
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": OPENUNIVERSE_ACKNOWLEDGEMENT,
            "simulation_only": True,
            "recommended_mode": "evidence_only",
            "threshold_separation": "Keep synthetic-derived thresholds separate from council evidence confidence.",
        },
    }


def write_anomaly_benchmark_files(profile: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"anomaly_benchmark_profile_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(profile, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="anomaly_benchmark_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_anomaly_benchmark_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build deterministic anomaly benchmark bundles from synthetic structured ingest files.")
    parser.add_argument("--bundle", required=True, help="Path to a synthetic structured_ingest_v1 bundle, such as openuniverse_snapshot_bundle_*.json")
    parser.add_argument("--supports-hypothesis", default=None, help="Optional existing hypothesis ID to link the benchmark memory to")
    parser.add_argument("--hypothesis-focus", default=None, help="Optional context label stored in bundle metadata")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for derived anomaly benchmark artifacts")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the benchmark bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle_path = Path(args.bundle)
    source_bundle = load_synthetic_bundle(bundle_path)
    profile = build_anomaly_benchmark_profile(source_bundle)
    label = str(profile.get("dataset") or bundle_path.stem)
    benchmark_bundle = build_anomaly_benchmark_bundle(
        source_bundle,
        profile,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
    )
    raw_path, bundle_json, bundle_md = write_anomaly_benchmark_files(profile, benchmark_bundle, Path(args.inbox), label)
    print(f"Raw anomaly profile written: {raw_path}")
    print(f"Structured benchmark bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_anomaly_benchmark_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())