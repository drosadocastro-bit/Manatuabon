"""Fetch Pan-STARRS DR2 catalog snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from db_init import ensure_runtime_db
from extinction_lookup import galactic_ebv
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


PANSTARRS_MEAN_API_URL = "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/mean.json"
PANSTARRS_STACK_API_URL = "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/stack.json"
DEFAULT_HEADERS = {"User-Agent": "Manatuabon/1.0 (Pan-STARRS DR2 snapshot importer)"}
PANSTARRS_ACKNOWLEDGEMENT = (
    "Pan-STARRS metadata should be treated as observational catalog context with provenance, "
    "not as autonomous scientific truth without photometric-quality and survey-footprint review."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "panstarrs"


def _coerce_float(value):
    if value in (None, "", "null", "None"):
        return None
    return float(value)


def _coerce_int(value):
    if value in (None, "", "null", "None"):
        return None
    return int(value)


def _fetch_json(url: str, params: dict, *, timeout: int = 60):
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _tabular_rows_to_dicts(info: list, rows: list) -> list[dict]:
    column_names = []
    for column in info:
        if isinstance(column, dict):
            column_names.append(column.get("name") or column.get("column_name"))
        else:
            column_names.append(None)
    if not column_names or any(name in (None, "") for name in column_names):
        return []

    parsed_rows = []
    for row in rows:
        if isinstance(row, dict):
            parsed_rows.append(dict(row))
            continue
        if isinstance(row, (list, tuple)):
            parsed_rows.append({
                column_names[index]: value
                for index, value in enumerate(row)
                if index < len(column_names)
            })
    return parsed_rows


def _extract_rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            if payload["data"] and isinstance(payload["data"][0], (list, tuple)):
                return _tabular_rows_to_dicts(payload.get("info") or [], payload["data"])
            return [dict(item) for item in payload["data"] if isinstance(item, dict)]
        if isinstance(payload.get("rows"), list):
            if payload["rows"] and isinstance(payload["rows"][0], (list, tuple)):
                return _tabular_rows_to_dicts(payload.get("info") or [], payload["rows"])
            return [dict(item) for item in payload["rows"] if isinstance(item, dict)]
    return []


def _lookup(row: dict, *names: str):
    lowered = {str(key).lower(): value for key, value in row.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def _build_params(*, radius_deg: float, ra_center: float, dec_center: float, max_results: int, min_detections: int, catalog: str) -> dict:
    params = {
        "ra": ra_center,
        "dec": dec_center,
        "radius": radius_deg,
        "pagesize": max_results,
    }
    if catalog == "mean":
        params["nDetections.gte"] = min_detections
        params["columns"] = "[objID,raMean,decMean,nDetections,gMeanPSFMag,rMeanPSFMag,iMeanPSFMag,zMeanPSFMag,yMeanPSFMag,qualityFlag,objInfoFlag]"
    return params


def collect_panstarrs_snapshot(
    *,
    ra_center: float,
    dec_center: float,
    radius_deg: float = 0.05,
    max_results: int = 50,
    min_detections: int = 1,
    fetcher=None,
) -> dict:
    fetch = fetcher or (lambda url, params: _fetch_json(url, params))
    query_mode = "mean"
    attempted_modes = []
    used_radius = radius_deg
    used_catalog = "mean"
    used_min_detections = min_detections
    rows = []
    expanded_radius = min(max(radius_deg * 6.0, 0.05), 0.2)
    attempts = [
        ("mean", "mean", radius_deg, min_detections, PANSTARRS_MEAN_API_URL),
    ]
    if min_detections > 0:
        attempts.append(("mean_relaxed", "mean", radius_deg, 0, PANSTARRS_MEAN_API_URL))
    attempts.append(("stack_relaxed", "stack", expanded_radius, 0, PANSTARRS_STACK_API_URL))

    for mode_name, catalog, current_radius, current_min_detections, url in attempts:
        attempted_modes.append(mode_name)
        params = _build_params(
            radius_deg=current_radius,
            ra_center=ra_center,
            dec_center=dec_center,
            max_results=max_results,
            min_detections=current_min_detections,
            catalog=catalog,
        )
        raw = fetch(url, params)
        candidate_rows = _extract_rows(raw)
        if candidate_rows:
            rows = candidate_rows
            query_mode = mode_name
            used_radius = current_radius
            used_catalog = catalog
            used_min_detections = current_min_detections
            break

    objects = []
    for row in rows:
        ra_val = _coerce_float(_lookup(row, "raMean", "ramean", "raStack", "rastack", "ra"))
        dec_val = _coerce_float(_lookup(row, "decMean", "decmean", "decStack", "decstack", "dec"))
        ebv, ebv_method = galactic_ebv(ra_val, dec_val) if ra_val is not None and dec_val is not None else (None, None)
        objects.append({
            "objID": _lookup(row, "objID", "objid"),
            "raMean": ra_val,
            "decMean": dec_val,
            "nDetections": _coerce_int(_lookup(row, "nDetections", "ndetections", "primaryDetection")),
            "gMeanPSFMag": _coerce_float(_lookup(row, "gMeanPSFMag", "gmeanpsfmag", "gPSFMag", "gpsfmag")),
            "rMeanPSFMag": _coerce_float(_lookup(row, "rMeanPSFMag", "rmeanpsfmag", "rPSFMag", "rpsfmag")),
            "iMeanPSFMag": _coerce_float(_lookup(row, "iMeanPSFMag", "imeanpsfmag", "iPSFMag", "ipsfmag")),
            "zMeanPSFMag": _coerce_float(_lookup(row, "zMeanPSFMag", "zmeanpsfmag", "zPSFMag", "zpsfmag")),
            "yMeanPSFMag": _coerce_float(_lookup(row, "yMeanPSFMag", "ymeanpsfmag", "yPSFMag", "ypsfmag")),
            "qualityFlag": _lookup(row, "qualityFlag", "qualityflag"),
            "objInfoFlag": _lookup(row, "objInfoFlag", "objinfoflag"),
            "extinction_ebv": ebv,
            "extinction_method": ebv_method,
        })

    band_counts = {
        band: sum(1 for row in objects if row.get(f"{band}MeanPSFMag") is not None)
        for band in ("g", "r", "i", "z", "y")
    }
    multiband_count = sum(
        1 for row in objects
        if sum(1 for band in ("g", "r", "i", "z", "y") if row.get(f"{band}MeanPSFMag") is not None) >= 3
    )

    return {
        "source": "Pan-STARRS DR2",
        "kind": "catalog_snapshot",
        "object_id": f"{ra_center:.5f}_{dec_center:.5f}",
        "fetched_at": iso_timestamp(),
        "query": {
            "ra_center": ra_center,
            "dec_center": dec_center,
            "radius_deg": radius_deg,
            "radius_deg_used": used_radius,
            "max_results": max_results,
            "min_detections": min_detections,
            "min_detections_used": used_min_detections,
            "catalog": used_catalog,
            "release": "dr2",
            "query_mode": query_mode,
            "attempted_modes": attempted_modes,
        },
        "summary": {
            "returned_count": len(objects),
            "multiband_count": multiband_count,
            "band_counts": band_counts,
        },
        "objects": objects,
    }


def build_panstarrs_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = False,
) -> dict:
    query = snapshot.get("query", {}) if isinstance(snapshot.get("query"), dict) else {}
    summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary"), dict) else {}
    objects = snapshot.get("objects", []) if isinstance(snapshot.get("objects"), list) else []
    row_count = len(objects)
    multiband_count = int(summary.get("multiband_count") or 0)
    band_counts = summary.get("band_counts", {}) if isinstance(summary.get("band_counts"), dict) else {}
    detection_rich_count = sum(1 for row in objects if (row.get("nDetections") or 0) >= 5)

    anomalies = []
    if not objects:
        anomalies.append("No Pan-STARRS rows were returned for the requested cone search.")
    if query.get("query_mode") in {"mean_relaxed", "stack_relaxed"}:
        anomalies.append("Sparse Pan-STARRS query returned no rows at the strict setting, so the importer relaxed detections and/or switched catalogs to improve coverage.")
    if query.get("catalog") == "stack":
        anomalies.append("Pan-STARRS mean catalog returned no usable rows, so the importer used stack-catalog fallback for broader footprint coverage.")
    if multiband_count == 0:
        anomalies.append("Returned Pan-STARRS rows do not show three-band photometric coverage, so color-based anomaly review will be limited.")
    if detection_rich_count == 0:
        anomalies.append("Returned Pan-STARRS rows do not show repeated detections, so persistence checks will be limited.")

    significance = 0.49
    if row_count:
        significance += 0.08
    if multiband_count >= 3:
        significance += 0.08
    if detection_rich_count >= 3:
        significance += 0.05
    significance = min(round(significance, 3), 0.8)

    bundle_summary = (
        f"Structured Pan-STARRS snapshot bundle covering {row_count} object(s) near RA={query.get('ra_center')} "
        f"Dec={query.get('dec_center')} within {query.get('radius_deg')} deg."
    )

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "panstarrs_snapshot_bundle",
        "summary": bundle_summary,
        "entities": ["Pan-STARRS DR2", f"RA {query.get('ra_center')}", f"Dec {query.get('dec_center')}"],
        "topics": ["Pan-STARRS DR2", "wide-field optical survey", "catalog snapshot", "photometry"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "optical_surveys", "panstarrs"],
        "source_catalogs": ["Pan-STARRS DR2", "https://catalogs.mast.stsci.edu/panstarrs/"],
        "target": {
            "name": f"Pan-STARRS cone {query.get('ra_center')},{query.get('dec_center')}",
            "input_target": f"{query.get('ra_center')},{query.get('dec_center')}",
            "kind": "panstarrs_cone_search",
        },
        "structured_evidence": {
            "query": query,
            "summary": summary,
            "detection_rich_count": detection_rich_count,
            "objects": objects,
        },
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"Pan-STARRS observational snapshot near RA {query.get('ra_center')}",
            "body": " ".join([
                bundle_summary,
                "Treat this as provenance-rich wide-field catalog context that should guide follow-up rather than autonomous acceptance.",
            ]),
            "confidence": 0.45,
            "predictions": [
                "Repeated Pan-STARRS snapshots for the same cone should preserve a stable multiband core object set unless query limits change.",
                "Objects with multi-band photometry and repeated detections can help rank optical anomaly follow-up against Gaia or SDSS context.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": PANSTARRS_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
        },
    }


def write_panstarrs_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"panstarrs_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="panstarrs_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_panstarrs_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Pan-STARRS DR2 snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--ra-center", type=float, required=True, help="Cone-search center right ascension in degrees")
    parser.add_argument("--dec-center", type=float, required=True, help="Cone-search center declination in degrees")
    parser.add_argument("--radius-deg", type=float, default=0.05, help="Cone-search radius in degrees")
    parser.add_argument("--max-results", type=int, default=50, help="Maximum number of rows to retain")
    parser.add_argument("--min-detections", type=int, default=1, help="Minimum Pan-STARRS detection count filter")
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
    snapshot = collect_panstarrs_snapshot(
        ra_center=args.ra_center,
        dec_center=args.dec_center,
        radius_deg=args.radius_deg,
        max_results=args.max_results,
        min_detections=args.min_detections,
    )
    label = f"{args.ra_center}_{args.dec_center}"
    bundle = build_panstarrs_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_panstarrs_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_panstarrs_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())