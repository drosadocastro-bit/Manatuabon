"""Fetch GWOSC released event-version snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import json
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


DEFAULT_GWOSC_API_ROOT = "https://gwosc.org/api/v2"
DEFAULT_GWOSC_HEADERS = {
    "User-Agent": "Manatuabon/1.0 (offline-first governed GWOSC snapshot importer)",
}
GWOSC_ACKNOWLEDGEMENT = "GWOSC released data is authoritative public-release metadata and should be cited per GWOSC acknowledgement guidance."
URL_FIELDS = ("parameters_url", "timelines_url", "strain_files_url")


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "gwosc"


def clean_link(value):
    if not isinstance(value, str):
        return value
    match = re.fullmatch(r"\[[^\]]*\]\((https?://[^)]+)\)", value.strip())
    if match:
        return match.group(1)
    return value.strip()


def clean_json_value(value):
    if isinstance(value, dict):
        return {key: clean_json_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean_json_value(item) for item in value]
    return clean_link(value)


def fetch_json(url: str, *, timeout: int = 30):
    response = requests.get(url, headers=DEFAULT_GWOSC_HEADERS, timeout=timeout)
    response.raise_for_status()
    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError as exc:
        snippet = response.text[:240].replace("\n", " ")
        raise RuntimeError(f"GWOSC endpoint did not return JSON for {url}. Response snippet: {snippet}") from exc
    return clean_json_value(payload)


def safe_fetch(name: str, url: str | None, errors: dict, *, fetcher=fetch_json):
    if not url:
        return None
    try:
        return fetcher(url)
    except Exception as exc:
        errors[name] = str(exc)
        return None


def extract_event_version_id(event_url: str) -> str:
    parsed = urllib.parse.urlparse(event_url)
    path = parsed.path.rstrip("/")
    marker = "/event-versions/"
    if marker not in path:
        raise ValueError(f"URL does not point to a GWOSC event-version resource: {event_url}")
    return path.split(marker, 1)[1].split("/", 1)[0]


def build_event_version_url(event_version: str, *, include_default_parameters: bool = False) -> str:
    query = {"format": "json"}
    if include_default_parameters:
        query["include-default-parameters"] = "true"
    return f"{DEFAULT_GWOSC_API_ROOT}/event-versions/{urllib.parse.quote(event_version)}?{urllib.parse.urlencode(query)}"


def _item_count(payload) -> int:
    if payload is None:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("results", "parameters", "strain_files", "timelines", "events", "segments"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
        return len(payload)
    return 0


def collect_gwosc_event_version_snapshot(
    event_version: str,
    *,
    include_default_parameters: bool = False,
    include_related: bool = True,
    fetcher=fetch_json,
) -> dict:
    errors = {}
    event_url = build_event_version_url(event_version, include_default_parameters=include_default_parameters)
    record = fetcher(event_url)
    related = {}

    if include_related:
        for field in URL_FIELDS:
            related[field.removesuffix("_url")] = safe_fetch(field.removesuffix("_url"), record.get(field), errors, fetcher=fetcher)

    return {
        "source": "GWOSC",
        "kind": "event_version",
        "object_id": event_version,
        "fetched_at": iso_timestamp(),
        "record": record,
        "related": related,
        "errors": errors,
    }


def build_gwosc_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = True,
) -> dict:
    record = snapshot.get("record", {}) if isinstance(snapshot.get("record"), dict) else {}
    related = snapshot.get("related", {}) if isinstance(snapshot.get("related"), dict) else {}

    event_version = snapshot.get("object_id") or record.get("name") or "GWOSC event"
    parameter_count = _item_count(related.get("parameters"))
    timeline_count = _item_count(related.get("timelines"))
    strain_file_count = _item_count(related.get("strain_files"))
    detectors = [str(item) for item in (record.get("detectors") or []) if str(item).strip()]
    entities = [
        item
        for item in [
            event_version,
            record.get("name"),
            record.get("grace_id"),
            *(record.get("aliases") or []),
        ]
        if item
    ]
    summary = (
        f"Structured GWOSC snapshot bundle for released event version {event_version} with "
        f"{len(detectors)} detectors, {parameter_count} parameter entries, {timeline_count} timeline entries, and {strain_file_count} strain-file records."
    )
    anomalies = []
    if not strain_file_count:
        anomalies.append("No strain-file inventory was retrieved from GWOSC for this released event version.")
    if not parameter_count:
        anomalies.append("No parameter estimation list was retrieved from GWOSC for this released event version.")
    if snapshot.get("errors"):
        anomalies.append("Some linked GWOSC resources could not be fetched; inspect the raw snapshot errors field.")

    significance = 0.74
    if detectors:
        significance += 0.03
    if parameter_count:
        significance += 0.04
    if strain_file_count:
        significance += 0.04
    if record.get("doi"):
        significance += 0.02
    significance = min(round(significance, 3), 0.9)

    structured_evidence = {
        "kind": snapshot.get("kind"),
        "object_id": event_version,
        "record": record,
        "parameters": related.get("parameters"),
        "timelines": related.get("timelines"),
        "strain_files": related.get("strain_files"),
        "errors": snapshot.get("errors", {}),
    }

    domain_tags = ["gravitational_waves"]
    if record.get("grace_id"):
        domain_tags.append("multimessenger")

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "gwosc_snapshot_bundle",
        "summary": summary,
        "entities": entities,
        "topics": ["GWOSC release metadata", "gravitational-wave events", record.get("catalog") or "event catalog", record.get("run") or "observing run"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": sorted(set(domain_tags)),
        "source_catalogs": ["GWOSC API", DEFAULT_GWOSC_API_ROOT],
        "target": {
            "name": event_version,
            "input_target": event_version,
            "kind": snapshot.get("kind"),
        },
        "structured_evidence": structured_evidence,
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"GWOSC release snapshot: {event_version}",
            "body": " ".join([
                summary,
                "Use this release snapshot as public event evidence and reproducibility context rather than as an autonomous conclusion.",
            ]),
            "confidence": 0.62,
            "predictions": [
                "Future released versions of the same GWOSC event should preserve the core event identity while changing parameter, timeline, or file inventories only when a new version is published.",
                "If GraceDB and GWOSC remain linked through the same grace_id, public-release metadata and analyst-workflow metadata should remain mutually traceable.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": GWOSC_ACKNOWLEDGEMENT,
            "api_root": DEFAULT_GWOSC_API_ROOT,
        },
    }


def write_gwosc_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"gwosc_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="gwosc_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_gwosc_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch GWOSC released event-version snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--event-version", default=None, help="GWOSC released event-version identifier, e.g. GW241110_124123-v1")
    parser.add_argument("--event-url", default=None, help="Full GWOSC event-version API URL")
    parser.add_argument("--include-default-parameters", action="store_true", help="Request GWOSC default parameter values in the event-version record")
    parser.add_argument("--skip-related", action="store_true", help="Only fetch the top-level event-version record, not linked parameters/timelines/strain-file inventories")
    parser.add_argument("--supports-hypothesis", default=None, help="Existing hypothesis ID to link the snapshot bundle to")
    parser.add_argument("--hypothesis-focus", default=None, help="Optional hypothesis focus label stored in bundle context")
    parser.add_argument("--evidence-only", action="store_true", help="Write and optionally ingest the snapshot as evidence only without generating a new hypothesis")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for raw snapshot and structured bundle files")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the structured bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    provided = [bool(args.event_version), bool(args.event_url)]
    if sum(provided) != 1:
        raise SystemExit("Provide exactly one of --event-version or --event-url.")

    event_version = args.event_version or extract_event_version_id(args.event_url)
    snapshot = collect_gwosc_event_version_snapshot(
        event_version,
        include_default_parameters=args.include_default_parameters,
        include_related=not args.skip_related,
    )
    bundle = build_gwosc_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_gwosc_snapshot_files(snapshot, bundle, Path(args.inbox), event_version)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_gwosc_bundle(
            bundle_json,
            db_path=Path(args.db),
            agent_log_path=Path(args.agent_log),
        )
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())