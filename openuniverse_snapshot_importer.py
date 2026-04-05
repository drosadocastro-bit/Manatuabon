"""Fetch OpenUniverse and related open-data-registry manifests as synthetic anomaly benchmark bundles."""

from __future__ import annotations

import argparse
import json
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


OPEN_DATA_REGISTRY_PRESETS = {
    "openuniverse2024": {
        "manifest_url": "https://raw.githubusercontent.com/awslabs/open-data-registry/main/datasets/openuniverse2024.yaml",
        "registry_url": "https://registry.opendata.aws/openuniverse2024",
        "purpose": "Synthetic Roman and Rubin imaging benchmark for anomaly-detection and cross-survey pipeline validation.",
    },
}
DEFAULT_HEADERS = {
    "User-Agent": "Manatuabon/1.0 (offline-first governed synthetic dataset importer)",
}
OPENUNIVERSE_ACKNOWLEDGEMENT = (
    "OpenUniverse and related registry datasets should be treated as synthetic benchmark inputs for anomaly detection, "
    "pipeline validation, and cross-survey calibration rather than direct observational evidence."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "openuniverse"


def clean_link(value):
    if not isinstance(value, str):
        return value
    match = re.fullmatch(r"\[[^\]]*\]\((https?://[^)]+)\)", value.strip())
    if match:
        return match.group(1)
    return value.strip()


def clean_yaml_value(value):
    if isinstance(value, dict):
        return {str(key): clean_yaml_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean_yaml_value(item) for item in value]
    return clean_link(value)


def fetch_yaml(url: str, *, timeout: int = 30) -> dict:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    payload = yaml.safe_load(response.text)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Open data registry manifest did not decode into a mapping for {url}")
    return clean_yaml_value(payload)


def resolve_manifest(dataset: str | None, manifest_url: str | None) -> tuple[str, str | None, str | None]:
    if bool(dataset) == bool(manifest_url):
        raise ValueError("Provide exactly one of --dataset or --manifest-url.")
    if dataset:
        preset = OPEN_DATA_REGISTRY_PRESETS.get(dataset)
        if not preset:
            raise ValueError(f"Unknown dataset preset: {dataset}")
        return dataset, preset["manifest_url"], preset.get("registry_url")
    assert manifest_url is not None
    parsed = urllib.parse.urlparse(manifest_url)
    slug = Path(parsed.path).stem or "synthetic_dataset"
    return slug, manifest_url, None


def _normalize_resources(resources) -> list[dict]:
    normalized = []
    for item in resources or []:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "description": item.get("Description"),
            "arn": item.get("ARN"),
            "region": item.get("Region"),
            "type": item.get("Type"),
            "requester_pays": item.get("RequesterPays"),
            "account_required": item.get("AccountRequired"),
            "explore": item.get("Explore"),
        })
    return normalized


def _normalize_tutorials(data_at_work) -> list[dict]:
    if not isinstance(data_at_work, dict):
        return []
    tutorials = data_at_work.get("Tutorials") or []
    normalized = []
    for item in tutorials:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "title": item.get("Title"),
            "url": item.get("URL"),
            "notebook_url": item.get("NotebookURL"),
            "author": item.get("AuthorName"),
            "author_url": item.get("AuthorURL"),
            "services": item.get("Services"),
        })
    return normalized


def collect_openuniverse_snapshot(dataset: str | None = None, *, manifest_url: str | None = None, fetcher=fetch_yaml) -> dict:
    slug, resolved_url, registry_url = resolve_manifest(dataset, manifest_url)
    manifest = fetcher(resolved_url)
    resources = _normalize_resources(manifest.get("Resources"))
    tutorials = _normalize_tutorials(manifest.get("DataAtWork"))
    snapshot = {
        "source": "AWS Open Data Registry",
        "kind": "synthetic_dataset_manifest",
        "object_id": slug,
        "fetched_at": iso_timestamp(),
        "manifest_url": resolved_url,
        "registry_url": registry_url,
        "manifest": manifest,
        "resources": resources,
        "tutorials": tutorials,
    }
    return snapshot


def build_openuniverse_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = False,
) -> dict:
    manifest = snapshot.get("manifest", {}) if isinstance(snapshot.get("manifest"), dict) else {}
    resources = snapshot.get("resources") or []
    tutorials = snapshot.get("tutorials") or []
    tags = [str(tag) for tag in (manifest.get("Tags") or []) if str(tag).strip()]
    name = manifest.get("Name") or snapshot.get("object_id") or "OpenUniverse synthetic dataset"
    summary = (
        f"Structured synthetic dataset snapshot for {name} with {len(resources)} AWS resources and {len(tutorials)} tutorial references. "
        f"This dataset is intended for anomaly-detection benchmarking, calibration, and cross-survey validation rather than direct observational inference."
    )
    anomalies = [
        "Synthetic simulation dataset: do not treat this bundle as direct observational evidence in council review.",
    ]
    if not tutorials:
        anomalies.append("No tutorial references were present in the manifest; downstream onboarding may require manual documentation review.")
    if not resources:
        anomalies.append("No AWS resources were listed in the manifest; verify dataset accessibility before use.")

    structured_evidence = {
        "kind": snapshot.get("kind"),
        "object_id": snapshot.get("object_id"),
        "manifest_url": snapshot.get("manifest_url"),
        "registry_url": snapshot.get("registry_url"),
        "manifest": manifest,
        "resources": resources,
        "tutorials": tutorials,
        "anomaly_detection_profile": {
            "synthetic": True,
            "recommended_uses": [
                "cross-survey anomaly detection benchmarking",
                "simulation-to-observation pipeline validation",
                "Roman and Rubin alignment sanity checks",
            ],
            "should_influence_council_directly": False,
        },
    }

    entities = [
        item
        for item in [
            name,
            "Nancy Grace Roman Telescope" if "Roman" in name or "roman" in json.dumps(manifest).lower() else None,
            "Vera C. Rubin Observatory" if "Rubin" in name or "rubin" in json.dumps(manifest).lower() else None,
        ]
        if item
    ]
    topics = [
        "synthetic astronomy datasets",
        "anomaly detection benchmarking",
        "cross-survey simulation",
        *tags[:4],
    ]
    domain_tags = ["synthetic_data", "anomaly_detection", "survey_imaging"]
    significance = min(round(0.48 + min(len(resources), 3) * 0.03 + min(len(tutorials), 2) * 0.02, 3), 0.62)

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "openuniverse_snapshot_bundle",
        "summary": summary,
        "entities": entities,
        "topics": topics,
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": sorted(set(domain_tags)),
        "source_catalogs": ["AWS Open Data Registry", snapshot.get("manifest_url")],
        "target": {
            "name": name,
            "input_target": snapshot.get("object_id"),
            "kind": snapshot.get("kind"),
        },
        "structured_evidence": structured_evidence,
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"Synthetic anomaly benchmark: {name}",
            "body": " ".join([
                summary,
                "Use this only to generate or test anomaly-detection workflows, not as direct evidence for astrophysical claims.",
            ]),
            "confidence": 0.35,
            "predictions": [
                "A robust anomaly-detection pipeline should distinguish synthetic truth products from calibrated noisy images without collapsing them into the same class.",
                "Cross-survey features extracted from matched Roman and Rubin simulations should help benchmark false-positive rates before applying models to observational data.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": OPENUNIVERSE_ACKNOWLEDGEMENT,
            "simulation_only": True,
            "recommended_mode": "evidence_only",
        },
    }


def write_openuniverse_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"openuniverse_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="openuniverse_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_openuniverse_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch OpenUniverse-style synthetic dataset manifests and write Manatuabon anomaly benchmark bundles.")
    parser.add_argument("--dataset", choices=sorted(OPEN_DATA_REGISTRY_PRESETS), default=None, help="Built-in synthetic dataset preset")
    parser.add_argument("--manifest-url", default=None, help="Raw YAML manifest URL from the AWS Open Data Registry GitHub repository")
    parser.add_argument("--supports-hypothesis", default=None, help="Optional existing hypothesis ID to link the bundle to")
    parser.add_argument("--hypothesis-focus", default=None, help="Optional context label stored in bundle metadata")
    parser.add_argument("--allow-new-hypothesis", action="store_true", help="Opt in to generating a hypothesis from synthetic benchmark metadata")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for raw snapshot and structured bundle files")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the structured bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = collect_openuniverse_snapshot(args.dataset, manifest_url=args.manifest_url)
    label = snapshot.get("object_id") or args.dataset or "openuniverse"
    bundle = build_openuniverse_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=args.allow_new_hypothesis,
    )
    raw_path, bundle_json, bundle_md = write_openuniverse_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_openuniverse_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())