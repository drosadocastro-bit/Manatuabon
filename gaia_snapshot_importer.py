"""Fetch Gaia DR3 snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


GAIA_TAP_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
DEFAULT_HEADERS = {"User-Agent": "Manatuabon/1.0 (Gaia DR3 snapshot importer)"}
GAIA_ACKNOWLEDGEMENT = (
    "Gaia metadata should be treated as observational astrometry context with provenance, "
    "not as autonomous scientific truth without catalog-quality and foreground-distance review."
)


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "gaia"


def _coerce_float(value):
    if value in (None, "", "null", "None"):
        return None
    return float(value)


def _build_adql_query(ra_center: float, dec_center: float, radius_deg: float, max_results: int) -> str:
    return f"""
    SELECT TOP {max_results}
        source_id,
        ra,
        dec,
        parallax,
        parallax_error,
        pmra,
        pmra_error,
        pmdec,
        pmdec_error,
        radial_velocity,
        radial_velocity_error,
        phot_g_mean_mag,
        bp_rp,
        ruwe
    FROM gaiadr3.gaia_source
    WHERE CONTAINS(
        POINT('ICRS', ra, dec),
        CIRCLE('ICRS', {ra_center}, {dec_center}, {radius_deg})
    ) = 1
    ORDER BY phot_g_mean_mag ASC
    """.strip()


def _fetch_json(url: str, params: dict, *, timeout: int = 60):
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _rows_from_tap_response(data: dict) -> list[dict]:
    metadata = data.get("metadata", []) if isinstance(data, dict) else []
    raw_rows = data.get("data", []) if isinstance(data, dict) else []
    columns = [item.get("name") for item in metadata if isinstance(item, dict) and item.get("name")]
    if not columns:
        return []

    rows = []
    for raw in raw_rows:
        if isinstance(raw, list) and len(raw) == len(columns):
            rows.append(dict(zip(columns, raw)))
    return rows


def collect_gaia_snapshot(
    *,
    ra_center: float,
    dec_center: float,
    radius_deg: float = 0.5,
    max_results: int = 100,
    fetcher=None,
) -> dict:
    fetch = fetcher or (lambda url, params: _fetch_json(url, params))
    adql = _build_adql_query(ra_center, dec_center, radius_deg, max_results)
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "json",
        "QUERY": adql,
    }
    raw = fetch(GAIA_TAP_URL, params)
    rows = _rows_from_tap_response(raw)

    stars = []
    for row in rows:
        stars.append({
            "source_id": str(row.get("source_id")) if row.get("source_id") not in (None, "") else None,
            "ra": _coerce_float(row.get("ra")),
            "dec": _coerce_float(row.get("dec")),
            "parallax": _coerce_float(row.get("parallax")),
            "parallax_error": _coerce_float(row.get("parallax_error")),
            "pmra": _coerce_float(row.get("pmra")),
            "pmra_error": _coerce_float(row.get("pmra_error")),
            "pmdec": _coerce_float(row.get("pmdec")),
            "pmdec_error": _coerce_float(row.get("pmdec_error")),
            "radial_velocity": _coerce_float(row.get("radial_velocity")),
            "radial_velocity_error": _coerce_float(row.get("radial_velocity_error")),
            "phot_g_mean_mag": _coerce_float(row.get("phot_g_mean_mag")),
            "bp_rp": _coerce_float(row.get("bp_rp")),
            "ruwe": _coerce_float(row.get("ruwe")),
        })

    pm_count = sum(1 for row in stars if row.get("pmra") is not None or row.get("pmdec") is not None)
    parallax_count = sum(1 for row in stars if row.get("parallax") is not None)
    radial_velocity_count = sum(1 for row in stars if row.get("radial_velocity") is not None)
    ruwe_count = sum(1 for row in stars if row.get("ruwe") is not None)

    return {
        "source": "Gaia DR3",
        "kind": "stellar_snapshot",
        "object_id": f"{ra_center:.5f}_{dec_center:.5f}",
        "fetched_at": iso_timestamp(),
        "query": {
            "ra_center": ra_center,
            "dec_center": dec_center,
            "radius_deg": radius_deg,
            "max_results": max_results,
            "query_mode": "tap_sync",
        },
        "summary": {
            "returned_count": len(stars),
            "proper_motion_count": pm_count,
            "parallax_count": parallax_count,
            "radial_velocity_count": radial_velocity_count,
            "ruwe_count": ruwe_count,
        },
        "stars": stars,
    }


def build_gaia_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = False,
) -> dict:
    query = snapshot.get("query", {}) if isinstance(snapshot.get("query"), dict) else {}
    summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary"), dict) else {}
    stars = snapshot.get("stars", []) if isinstance(snapshot.get("stars"), list) else []
    row_count = len(stars)
    pm_count = int(summary.get("proper_motion_count") or 0)
    parallax_count = int(summary.get("parallax_count") or 0)
    radial_velocity_count = int(summary.get("radial_velocity_count") or 0)
    ruwe_count = int(summary.get("ruwe_count") or 0)
    bright_count = sum(1 for row in stars if row.get("phot_g_mean_mag") is not None and row["phot_g_mean_mag"] <= 18.0)

    anomalies = []
    if not stars:
        anomalies.append("No Gaia DR3 rows were returned for the requested cone search.")
    if pm_count == 0:
        anomalies.append("Returned Gaia rows do not include proper-motion measurements, so kinematic anomaly review will be limited.")
    if parallax_count == 0:
        anomalies.append("Returned Gaia rows do not include parallax measurements, so foreground-distance vetting will be limited.")
    if radial_velocity_count == 0:
        anomalies.append("Returned Gaia rows do not include radial velocities, so 3D kinematic follow-up will be limited.")

    significance = 0.5
    if row_count:
        significance += 0.08
    if pm_count >= 3:
        significance += 0.08
    if parallax_count >= 3:
        significance += 0.06
    if radial_velocity_count >= 3:
        significance += 0.04
    significance = min(round(significance, 3), 0.82)

    bundle_summary = (
        f"Structured Gaia snapshot bundle covering {row_count} star(s) near RA={query.get('ra_center')} "
        f"Dec={query.get('dec_center')} within {query.get('radius_deg')} deg."
    )

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "gaia_snapshot_bundle",
        "summary": bundle_summary,
        "entities": [
            "Gaia DR3",
            f"RA {query.get('ra_center')}",
            f"Dec {query.get('dec_center')}",
            f"bright stars {bright_count}",
        ],
        "topics": ["Gaia DR3", "stellar astrometry", "catalog snapshot", "kinematics"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["observations", "stellar_astrometry", "gaia"],
        "source_catalogs": ["Gaia DR3", "https://gea.esac.esa.int/archive/"],
        "target": {
            "name": f"Gaia cone {query.get('ra_center')},{query.get('dec_center')}",
            "input_target": f"{query.get('ra_center')},{query.get('dec_center')}",
            "kind": "gaia_cone_search",
        },
        "structured_evidence": {
            "query": query,
            "summary": summary,
            "bright_star_count": bright_count,
            "stars": stars,
        },
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"Gaia observational snapshot near RA {query.get('ra_center')}",
            "body": " ".join([
                bundle_summary,
                "Treat this as provenance-rich stellar context that should guide follow-up rather than autonomous acceptance.",
            ]),
            "confidence": 0.46,
            "predictions": [
                "Repeated Gaia snapshots for the same cone should remain stable aside from archive updates or changed query limits.",
                "Cones with strong proper-motion and parallax coverage can be paired with SDSS or other surveys for controlled anomaly triage.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": GAIA_ACKNOWLEDGEMENT,
            "recommended_mode": "evidence_only",
        },
    }


def write_gaia_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"gaia_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="gaia_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_gaia_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Gaia DR3 snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--ra-center", type=float, required=True, help="Cone-search center right ascension in degrees")
    parser.add_argument("--dec-center", type=float, required=True, help="Cone-search center declination in degrees")
    parser.add_argument("--radius-deg", type=float, default=0.5, help="Cone-search radius in degrees")
    parser.add_argument("--max-results", type=int, default=100, help="Maximum number of rows to retain")
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
    snapshot = collect_gaia_snapshot(
        ra_center=args.ra_center,
        dec_center=args.dec_center,
        radius_deg=args.radius_deg,
        max_results=args.max_results,
    )
    label = f"{args.ra_center}_{args.dec_center}"
    bundle = build_gaia_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_gaia_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_gaia_bundle(bundle_json, db_path=Path(args.db), agent_log_path=Path(args.agent_log))
        print(f"Ingested memory #{ingested['id']} into DB: {Path(args.db)}")
        print(f"Ingest summary: {ingested['summary']}")
        generated = ingested.get("hypothesis_generated") or {}
        if generated.get("id") and generated.get("title"):
            print(f"Generated hypothesis: {generated['id']} - {generated['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())