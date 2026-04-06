"""Fetch SDSS snapshots and convert them into Manatuabon structured bundles."""

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


SDSS_SQL_URL = "https://skyserver.sdss.org/dr18/SkyServerWS/SearchTools/SqlSearch"
SDSS_RADIAL_URL = "https://skyserver.sdss.org/dr18/SkyServerWS/SearchTools/RadialSearch"
DEFAULT_HEADERS = {"User-Agent": "Manatuabon/1.0 (real observational archive snapshot importer)"}
SDSS_ACKNOWLEDGEMENT = (
    "SDSS metadata should be treated as observational survey context with provenance, "
    "not as autonomous scientific truth without survey- and calibration-aware review."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "sdss"


def _normalize_rows(data) -> list[dict]:
    if isinstance(data, list) and data:
        table = data[0] if isinstance(data[0], dict) else {}
        rows = table.get("Rows", []) if isinstance(table, dict) else []
        return [dict(item) for item in rows if isinstance(item, dict)]
    return []


def _matches_object_type(row: dict, object_type: str) -> bool:
    if object_type == "all":
        return True
    row_type = row.get("type")
    if row_type in (None, ""):
        return True
    try:
        row_type = int(row_type)
    except (TypeError, ValueError):
        return True
    if object_type == "galaxy":
        return row_type == 3
    if object_type == "star":
        return row_type == 6
    return True


def _filter_object_type(rows: list[dict], object_type: str) -> list[dict]:
    return [row for row in rows if _matches_object_type(row, object_type)]


def _fetch_json(url: str, params: dict, *, timeout: int = 60):
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _build_sql_query(ra_center: float, dec_center: float, radius_arcmin: float, max_results: int, object_type: str) -> str:
    # Validate numeric bounds to prevent remote SQL injection via crafted floats
    ra_center = float(ra_center)
    dec_center = float(dec_center)
    radius_arcmin = float(radius_arcmin)
    max_results = max(1, min(int(max_results), 5000))
    if not (-360 <= ra_center <= 360):
        raise ValueError(f"RA out of bounds: {ra_center}")
    if not (-90 <= dec_center <= 90):
        raise ValueError(f"Dec out of bounds: {dec_center}")
    if not (0 < radius_arcmin <= 600):
        raise ValueError(f"Radius out of bounds: {radius_arcmin}")

    type_clause = ""
    if object_type == "galaxy":
        type_clause = "AND type = 3"
    elif object_type == "star":
        type_clause = "AND type = 6"

    radius_deg = radius_arcmin / 60.0
    return f"""
    SELECT TOP {max_results}
        objID, ra, dec, z, zErr,
        petroMag_r, petroMag_g,
        type, subClass,
        velDisp, velDispErr
    FROM PhotoObj
    WHERE
        ra BETWEEN {ra_center - radius_deg} AND {ra_center + radius_deg}
        AND dec BETWEEN {dec_center - radius_deg} AND {dec_center + radius_deg}
        {type_clause}
    ORDER BY petroMag_r ASC
    """.strip()


def _build_spectro_sql_query(ra_center: float, dec_center: float, radius_arcmin: float, max_results: int, object_type: str) -> str:
    ra_center = float(ra_center)
    dec_center = float(dec_center)
    radius_arcmin = float(radius_arcmin)
    max_results = max(1, min(int(max_results), 5000))

    type_clause = ""
    if object_type == "galaxy":
        type_clause = "AND p.type = 3"
    elif object_type == "star":
        type_clause = "AND p.type = 6"

    radius_deg = radius_arcmin / 60.0
    return f"""
    SELECT TOP {max_results}
        p.objID, p.ra, p.dec,
        s.z, s.zErr,
        p.petroMag_r, p.petroMag_g,
        p.type,
        s.class AS subClass,
        s.velDisp, s.velDispErr
    FROM PhotoObj AS p
    JOIN SpecObj AS s
        ON s.bestObjID = p.objID
    WHERE
        p.ra BETWEEN {ra_center - radius_deg} AND {ra_center + radius_deg}
        AND p.dec BETWEEN {dec_center - radius_deg} AND {dec_center + radius_deg}
        {type_clause}
    ORDER BY p.petroMag_r ASC
    """.strip()


def collect_sdss_snapshot(
    *,
    ra_center: float,
    dec_center: float,
    radius_arcmin: float = 60.0,
    max_results: int = 50,
    object_type: str = "galaxy",
    fetcher=None,
) -> dict:
    errors = {}
    fetch = fetcher or (lambda url, params: _fetch_json(url, params))
    query_mode = "sql"
    attempted_modes = []
    rows = []

    sql_attempts = [("sql", object_type, _build_sql_query)]
    if object_type != "all":
        sql_attempts.append(("sql_relaxed", "all", _build_sql_query))
    sql_attempts.append(("sql_spectro", object_type, _build_spectro_sql_query))
    if object_type != "all":
        sql_attempts.append(("sql_spectro_relaxed", "all", _build_spectro_sql_query))

    for mode_name, mode_object_type, builder in sql_attempts:
        attempted_modes.append(mode_name)
        sql = builder(ra_center, dec_center, radius_arcmin, max_results, mode_object_type)
        params = {"cmd": sql, "format": "json"}
        try:
            raw = fetch(SDSS_SQL_URL, params)
            candidate_rows = _normalize_rows(raw)
            if mode_object_type != "all":
                candidate_rows = _filter_object_type(candidate_rows, mode_object_type)
            if candidate_rows:
                rows = candidate_rows
                query_mode = mode_name
                break
            if mode_name == "sql":
                errors["sql_empty"] = "SQL endpoint returned zero rows for the requested typed query; trying relaxed fallback."
            if mode_name == "sql_spectro":
                errors["sql_spectro_empty"] = "Spectroscopic SQL join returned zero typed rows; trying relaxed fallback or radial search."
        except Exception as exc:
            errors[mode_name] = str(exc)

    if not rows:
        radial_params = {
            "ra": ra_center,
            "dec": dec_center,
            "radius": radius_arcmin / 60.0,
            "limit": max_results,
            "format": "json",
        }
        radial_attempts = [("radial_search", object_type)]
        if object_type != "all":
            radial_attempts.append(("radial_search_relaxed", "all"))

        for mode_name, mode_object_type in radial_attempts:
            attempted_modes.append(mode_name)
            try:
                raw = fetch(SDSS_RADIAL_URL, radial_params)
                candidate_rows = _normalize_rows(raw)
                if mode_object_type != "all":
                    candidate_rows = _filter_object_type(candidate_rows, mode_object_type)
                if candidate_rows:
                    rows = candidate_rows
                    query_mode = mode_name
                    break
                if mode_name == "radial_search":
                    errors["radial_empty"] = "Radial endpoint returned zero typed rows; trying relaxed fallback without object-type filtering."
            except Exception as exc:
                errors[mode_name] = str(exc)

    normalized_rows = []
    for row in rows:
        ra_val = float(row.get("ra")) if row.get("ra") not in (None, "") else None
        dec_val = float(row.get("dec")) if row.get("dec") not in (None, "") else None
        ebv, ebv_method = galactic_ebv(ra_val, dec_val) if ra_val is not None and dec_val is not None else (None, None)
        normalized_rows.append({
            "objID": row.get("objID") or row.get("objid"),
            "ra": ra_val,
            "dec": dec_val,
            "redshift": float(row.get("z")) if row.get("z") not in (None, "") else None,
            "redshift_error": float(row.get("zErr")) if row.get("zErr") not in (None, "") else None,
            "petroMag_r": float(row.get("petroMag_r")) if row.get("petroMag_r") not in (None, "") else None,
            "petroMag_g": float(row.get("petroMag_g")) if row.get("petroMag_g") not in (None, "") else None,
            "type": row.get("type"),
            "subClass": row.get("subClass") or row.get("subclass"),
            "velDisp": float(row.get("velDisp")) if row.get("velDisp") not in (None, "") else None,
            "velDispErr": float(row.get("velDispErr")) if row.get("velDispErr") not in (None, "") else None,
            "extinction_ebv": ebv,
            "extinction_method": ebv_method,
        })

    return {
        "source": "SDSS",
        "kind": "catalog_snapshot",
        "object_id": f"{ra_center:.5f}_{dec_center:.5f}",
        "fetched_at": iso_timestamp(),
        "query": {
            "ra_center": ra_center,
            "dec_center": dec_center,
            "radius_arcmin": radius_arcmin,
            "max_results": max_results,
            "object_type": object_type,
            "query_mode": query_mode,
            "attempted_modes": attempted_modes,
        },
        "rows": normalized_rows,
        "errors": errors,
    }


def build_sdss_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = False,
) -> dict:
    query = snapshot.get("query", {}) if isinstance(snapshot.get("query"), dict) else {}
    rows = snapshot.get("rows", []) if isinstance(snapshot.get("rows"), list) else []
    row_count = len(rows)
    redshift_count = sum(1 for row in rows if row.get("redshift") is not None)
    veldisp_count = sum(1 for row in rows if row.get("velDisp") is not None)
    subclasses = sorted({str(row.get("subClass")) for row in rows if row.get("subClass")})

    anomalies = []
    if not rows:
        anomalies.append("No SDSS rows were returned for the requested cone search.")
    if query.get("query_mode") in {"radial_search", "radial_search_relaxed"}:
        anomalies.append("SQL endpoint failed and the importer fell back to the radial-search endpoint; field coverage may be reduced.")
    if query.get("query_mode") in {"sql_relaxed", "radial_search_relaxed"}:
        anomalies.append("Typed SDSS search returned zero rows, so the importer relaxed the object-type filter to improve field coverage.")
    if query.get("query_mode") in {"sql_spectro", "sql_spectro_relaxed"}:
        anomalies.append("Broad-band SDSS photo query returned no usable rows, so the importer used a spectroscopic join fallback to recover redshift-bearing objects.")
    if redshift_count == 0:
        anomalies.append("Returned rows do not include redshift values, so bulk-flow and distance-sensitive analysis will be limited.")
    if veldisp_count == 0:
        anomalies.append("Returned rows do not include velocity-dispersion values, so dynamical anomaly review will be limited.")

    significance = 0.5
    if row_count:
        significance += 0.08
    if redshift_count >= 3:
        significance += 0.08
    if veldisp_count >= 3:
        significance += 0.06
    significance = min(round(significance, 3), 0.82)

    summary = (
        f"Structured SDSS snapshot bundle covering {row_count} row(s) near RA={query.get('ra_center')} "
        f"Dec={query.get('dec_center')} within {query.get('radius_arcmin')} arcmin."
    )

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "sdss_snapshot_bundle",
        "summary": summary,
        "entities": [
            "SDSS DR18",
            f"RA {query.get('ra_center')}",
            f"Dec {query.get('dec_center')}",
            *subclasses[:3],
        ],
        "topics": ["SDSS", "optical survey", "catalog snapshot", query.get("object_type") or "objects"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "optical_surveys", "sdss"],
        "source_catalogs": ["SDSS DR18", "https://skyserver.sdss.org/dr18"],
        "target": {
            "name": f"SDSS cone {query.get('ra_center')},{query.get('dec_center')}",
            "input_target": f"{query.get('ra_center')},{query.get('dec_center')}",
            "object_type": query.get("object_type"),
        },
        "structured_evidence": {
            "query": query,
            "row_count": row_count,
            "redshift_count": redshift_count,
            "veldisp_count": veldisp_count,
            "subclasses": subclasses,
            "rows": rows,
        },
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"SDSS observational snapshot near RA {query.get('ra_center')}",
            "body": " ".join([
                summary,
                "Treat this as provenance-rich survey context that should guide follow-up rather than autonomous acceptance.",
            ]),
            "confidence": 0.47,
            "predictions": [
                "Repeated SDSS snapshots for the same cone should remain stable unless query parameters or release content change.",
                "Cones with redshift and velocity-dispersion coverage can be compared against Gaia kinematics for future bulk-flow anomaly review.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": SDSS_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
        },
    }


def write_sdss_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"sdss_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="sdss_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_sdss_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch SDSS snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--ra-center", type=float, required=True, help="Cone-search center right ascension in degrees")
    parser.add_argument("--dec-center", type=float, required=True, help="Cone-search center declination in degrees")
    parser.add_argument("--radius-arcmin", type=float, default=60.0, help="Cone-search radius in arcminutes")
    parser.add_argument("--max-results", type=int, default=50, help="Maximum number of rows to retain")
    parser.add_argument("--object-type", choices=["all", "galaxy", "star"], default="galaxy", help="Optional SDSS object type filter")
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
    snapshot = collect_sdss_snapshot(
        ra_center=args.ra_center,
        dec_center=args.dec_center,
        radius_arcmin=args.radius_arcmin,
        max_results=args.max_results,
        object_type=args.object_type,
    )
    label = f"{args.ra_center}_{args.dec_center}"
    bundle = build_sdss_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_sdss_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_sdss_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())