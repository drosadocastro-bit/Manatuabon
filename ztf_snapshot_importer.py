"""Fetch ZTF image-metadata snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


ZTF_SCI_URL = "https://irsa.ipac.caltech.edu/ibe/search/ztf/products/sci"
DEFAULT_HEADERS = {"User-Agent": "Manatuabon/1.0 (ZTF snapshot importer)"}
ZTF_ACKNOWLEDGEMENT = (
    "ZTF metadata should be treated as observational time-domain context with provenance, "
    "not as autonomous scientific truth without image-level review and cadence-aware vetting."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "ztf"


def _coerce_float(value):
    if value in (None, "", "null", "None"):
        return None
    return float(value)


def _fetch_text(url: str, params: dict, *, timeout: int = 60) -> str:
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _parse_csv(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader if isinstance(row, dict)]


def collect_ztf_snapshot(
    *,
    ra_center: float,
    dec_center: float,
    size_deg: float = 0.1,
    max_results: int = 50,
    intersect: str = "OVERLAPS",
    fetcher=None,
) -> dict:
    fetch = fetcher or (lambda url, params: _fetch_text(url, params))
    params = {
        "POS": f"{ra_center},{dec_center}",
        "SIZE": size_deg,
        "INTERSECT": intersect.upper(),
        "ct": "csv",
    }
    raw_text = fetch(ZTF_SCI_URL, params)
    rows = _parse_csv(raw_text)
    limited_rows = rows[:max_results]

    frames = []
    for row in limited_rows:
        frames.append({
            "field": row.get("field"),
            "ccdid": row.get("ccdid"),
            "qid": row.get("qid"),
            "filtercode": row.get("filtercode"),
            "imgtypecode": row.get("imgtypecode"),
            "obsjd": _coerce_float(row.get("obsjd")),
            "seeing": _coerce_float(row.get("seeing")),
            "maglimit": _coerce_float(row.get("maglimit")),
            "ra": _coerce_float(row.get("ra")),
            "dec": _coerce_float(row.get("dec")),
            "infobits": row.get("infobits"),
            "pid": row.get("pid"),
        })

    filter_counts = {}
    for row in frames:
        key = str(row.get("filtercode") or "unknown")
        filter_counts[key] = filter_counts.get(key, 0) + 1

    return {
        "source": "ZTF via IRSA",
        "kind": "image_metadata_snapshot",
        "object_id": f"{ra_center:.5f}_{dec_center:.5f}",
        "fetched_at": iso_timestamp(),
        "query": {
            "ra_center": ra_center,
            "dec_center": dec_center,
            "size_deg": size_deg,
            "max_results": max_results,
            "intersect": intersect.upper(),
            "product_type": "science",
        },
        "summary": {
            "returned_count": len(frames),
            "filter_counts": filter_counts,
            "seeing_count": sum(1 for row in frames if row.get("seeing") is not None),
            "maglimit_count": sum(1 for row in frames if row.get("maglimit") is not None),
        },
        "frames": frames,
    }


def build_ztf_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = False,
) -> dict:
    query = snapshot.get("query", {}) if isinstance(snapshot.get("query"), dict) else {}
    summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary"), dict) else {}
    frames = snapshot.get("frames", []) if isinstance(snapshot.get("frames"), list) else []
    row_count = len(frames)
    filter_counts = summary.get("filter_counts", {}) if isinstance(summary.get("filter_counts"), dict) else {}
    seeing_values = [row["seeing"] for row in frames if row.get("seeing") is not None]
    maglimit_values = [row["maglimit"] for row in frames if row.get("maglimit") is not None]

    anomalies = []
    if not frames:
        anomalies.append("No ZTF science-frame metadata rows were returned for the requested search region.")
    if not seeing_values:
        anomalies.append("Returned ZTF rows do not include seeing estimates, so image-quality triage will be limited.")
    if not maglimit_values:
        anomalies.append("Returned ZTF rows do not include limiting magnitudes, so sensitivity-aware follow-up will be limited.")

    significance = 0.48
    if row_count:
        significance += 0.08
    if len(filter_counts) >= 2:
        significance += 0.06
    if len(maglimit_values) >= 3:
        significance += 0.05
    significance = min(round(significance, 3), 0.8)

    bundle_summary = (
        f"Structured ZTF snapshot bundle covering {row_count} science frame(s) near RA={query.get('ra_center')} "
        f"Dec={query.get('dec_center')} within {query.get('size_deg')} deg."
    )

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "ztf_snapshot_bundle",
        "summary": bundle_summary,
        "entities": ["ZTF", "IRSA", f"RA {query.get('ra_center')}", f"Dec {query.get('dec_center')}"],
        "topics": ["ZTF", "time-domain survey", "image metadata snapshot", "transient follow-up"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "time_domain", "ztf", "irsa"],
        "source_catalogs": ["ZTF via IRSA", "https://irsa.ipac.caltech.edu/Missions/ztf.html"],
        "target": {
            "name": f"ZTF region {query.get('ra_center')},{query.get('dec_center')}",
            "input_target": f"{query.get('ra_center')},{query.get('dec_center')}",
            "kind": "ztf_region_search",
        },
        "structured_evidence": {
            "query": query,
            "summary": summary,
            "frames": frames,
        },
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"ZTF observational snapshot near RA {query.get('ra_center')}",
            "body": " ".join([
                bundle_summary,
                "Treat this as provenance-rich time-domain metadata that should guide follow-up rather than autonomous acceptance.",
            ]),
            "confidence": 0.45,
            "predictions": [
                "Repeated ZTF metadata snapshots for the same field should preserve a stable frame distribution unless the archive expands.",
                "Fields with multi-filter coverage and good seeing are stronger candidates for transient anomaly review than sparse single-filter footprints.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": ZTF_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
        },
    }


def write_ztf_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"ztf_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="ztf_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_ztf_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch ZTF metadata snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--ra-center", type=float, required=True, help="Search-region center right ascension in degrees")
    parser.add_argument("--dec-center", type=float, required=True, help="Search-region center declination in degrees")
    parser.add_argument("--size-deg", type=float, default=0.1, help="Search-region full width in degrees")
    parser.add_argument("--max-results", type=int, default=50, help="Maximum number of rows to retain")
    parser.add_argument("--intersect", default="OVERLAPS", choices=["OVERLAPS", "CENTER", "COVERS", "ENCLOSED"], help="IRSA spatial predicate")
    parser.add_argument("--supports-hypothesis", default=None, help="Existing hypothesis ID to link the snapshot bundle to")
    parser.add_argument("--hypothesis-focus", default=None, help="Optional hypothesis focus label stored in bundle context")
    parser.add_argument("--evidence-only", action="store_true", help="Write the snapshot bundle as evidence only without generating a new hypothesis")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for raw snapshot and structured bundle files")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(AGENT_LOG_FILE), help="Agent log path used when --ingest is enabled")
    parser.add_argument("--ingest", action="store_true", help="After writing the structured bundle, ingest it directly into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = collect_ztf_snapshot(
        ra_center=args.ra_center,
        dec_center=args.dec_center,
        size_deg=args.size_deg,
        max_results=args.max_results,
        intersect=args.intersect,
    )
    label = f"{args.ra_center}_{args.dec_center}"
    bundle = build_ztf_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_ztf_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_ztf_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())