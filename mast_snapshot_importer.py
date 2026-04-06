"""Fetch MAST observation snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


MAST_ACKNOWLEDGEMENT = (
    "MAST metadata should be treated as observational archive context with provenance, "
    "not as autonomous scientific truth without instrument-level review."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "mast"


def _table_to_records(rows) -> list[dict]:
    if rows is None:
        return []
    if isinstance(rows, list):
        return [dict(item) for item in rows if isinstance(item, dict)]
    try:
        return [dict(row) for row in rows]
    except Exception:
        return []


def build_mast_client():
    try:
        from astroquery.mast import Observations
    except ImportError as exc:
        raise RuntimeError(
            "MAST importer requires astroquery in the active environment. "
            "Install it with: d:/Manatuabon/.venv/Scripts/python.exe -m pip install astroquery"
        ) from exc
    return Observations


def collect_mast_snapshot(
    target: str,
    *,
    radius_deg: float = 0.05,
    collections: list[str] | None = None,
    max_results: int = 10,
    fetcher=None,
) -> dict:
    collections = [item.strip() for item in (collections or ["JWST", "HST"]) if str(item).strip()]
    if fetcher is None:
        observations = build_mast_client()
        query_result = observations.query_object(target, radius=f"{radius_deg} deg")
    else:
        query_result = fetcher(target, radius_deg)

    rows = _table_to_records(query_result)
    filtered = [
        row for row in rows
        if not collections or str(row.get("obs_collection") or "").strip().upper() in {item.upper() for item in collections}
    ]
    filtered.sort(key=lambda row: row.get("t_min") or 0, reverse=True)
    limited = filtered[:max_results]

    observations = []
    for row in limited:
        observations.append({
            "obs_id": str(row.get("obs_id") or row.get("obsid") or "unknown"),
            "target_name": row.get("target_name") or target,
            "collection": row.get("obs_collection"),
            "instrument": row.get("instrument_name"),
            "filters": row.get("filters"),
            "exposure_s": float(row.get("t_exptime")) if row.get("t_exptime") not in (None, "") else None,
            "ra": float(row.get("s_ra")) if row.get("s_ra") not in (None, "") else None,
            "dec": float(row.get("s_dec")) if row.get("s_dec") not in (None, "") else None,
            "t_min_mjd": float(row.get("t_min")) if row.get("t_min") not in (None, "") else None,
            "proposal_id": row.get("proposal_id"),
            "data_rights": row.get("data_rights"),
        })

    latest = observations[0] if observations else {}
    return {
        "source": "MAST",
        "kind": "observation_snapshot",
        "object_id": target,
        "fetched_at": iso_timestamp(),
        "query": {
            "target": target,
            "radius_deg": radius_deg,
            "collections": collections,
            "max_results": max_results,
        },
        "summary": {
            "raw_count": len(rows),
            "filtered_count": len(filtered),
            "returned_count": len(observations),
            "latest_obs_id": latest.get("obs_id"),
        },
        "observations": observations,
    }


def build_mast_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = False,
) -> dict:
    query = snapshot.get("query", {}) if isinstance(snapshot.get("query"), dict) else {}
    observations = snapshot.get("observations", []) if isinstance(snapshot.get("observations"), list) else []
    target = query.get("target") or snapshot.get("object_id") or "MAST target"
    collections = [str(item) for item in query.get("collections", []) if str(item).strip()]
    instruments = sorted({str(item.get("instrument")) for item in observations if item.get("instrument")})
    anomalies = []
    if not observations:
        anomalies.append("No matching observations were returned for the requested target and collection filter.")
    if any(item.get("data_rights") not in (None, "PUBLIC") for item in observations):
        anomalies.append("Some observations are not public, so downstream review may need archive credentials or proposal access.")
    if any(item.get("collection") == "JWST" for item in observations):
        anomalies.append("JWST anomaly claims should be checked against stage products, calibration context, and a matched control before escalation.")

    significance = 0.52
    if observations:
        significance += 0.08
    if len(observations) >= 3:
        significance += 0.07
    if instruments:
        significance += 0.05
    significance = min(round(significance, 3), 0.8)

    summary = (
        f"Structured MAST snapshot bundle for {target} covering {len(observations)} observation(s) "
        f"from {', '.join(collections) if collections else 'all collections'}."
    )
    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "mast_snapshot_bundle",
        "summary": summary,
        "entities": [item for item in [target, *collections[:3], *instruments[:3]] if item],
        "topics": ["MAST archive", "space observations", *(collections[:3])],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "space_telescopes", "mast"],
        "source_catalogs": ["MAST", "https://mast.stsci.edu"],
        "target": {
            "name": target,
            "input_target": target,
            "collections": collections,
        },
        "structured_evidence": {
            "query": query,
            "summary": snapshot.get("summary"),
            "observations": observations,
            "instruments": instruments,
        },
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"MAST observational snapshot: {target}",
            "body": " ".join([
                summary,
                "Treat this as provenance-rich observation context that may guide follow-up rather than autonomous acceptance.",
            ]),
            "confidence": 0.46,
            "predictions": [
                "Repeated MAST snapshots for the same target should return a stable core observation set unless new public products are released.",
                "Targets with unusual filter or exposure combinations may justify instrument-level anomaly review, not automatic scientific claims.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": MAST_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
        },
    }


def write_mast_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"mast_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="mast_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_mast_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MAST observation snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--target", required=True, help="Target name resolvable by MAST, for example M87 or Crab Nebula")
    parser.add_argument("--radius-deg", type=float, default=0.05, help="Search radius in degrees")
    parser.add_argument("--collections", default="JWST,HST", help="Comma-delimited obs_collection filter, for example JWST,HST or PanSTARRS")
    parser.add_argument("--max-results", type=int, default=10, help="Maximum number of observation rows to retain")
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
    collections = [item.strip() for item in args.collections.split(",") if item.strip()]
    snapshot = collect_mast_snapshot(
        args.target,
        radius_deg=args.radius_deg,
        collections=collections,
        max_results=args.max_results,
    )
    bundle = build_mast_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_mast_snapshot_files(snapshot, bundle, Path(args.inbox), args.target)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_mast_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())