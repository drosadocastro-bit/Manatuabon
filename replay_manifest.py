"""Replay a saved manifest of bundle paths through deterministic cross-match workers.

This tool reads a JSON manifest listing snapshot bundle paths and worker configurations,
reruns the specified deterministic workers against saved bundles, and writes a timestamped
reproducible output package.  Because the workers operate on frozen bundle artifacts rather
than live archive queries, replaying the same manifest always produces the same profile and
bundle outputs given the same worker code version.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR


SUPPORTED_WORKERS = {"gaia_sdss", "gaia_panstarrs", "gaia_ztf"}


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_gaia_sdss(step: dict, output_dir: Path, *, db_path: Path, agent_log_path: Path, ingest: bool) -> dict:
    from gaia_sdss_anomaly_worker import (
        build_gaia_sdss_anomaly_bundle,
        build_gaia_sdss_anomaly_profile,
        ingest_gaia_sdss_bundle,
        load_structured_bundle,
        write_gaia_sdss_anomaly_files,
    )

    gaia_bundle = load_structured_bundle(Path(step["gaia_bundle"]))
    sdss_bundle = load_structured_bundle(Path(step["sdss_bundle"]))
    profile = build_gaia_sdss_anomaly_profile(
        gaia_bundle,
        sdss_bundle,
        max_sep_arcsec=step.get("max_sep_arcsec", 30.0),
        pm_threshold_masyr=step.get("pm_threshold_masyr", 10.0),
        redshift_threshold=step.get("redshift_threshold", 0.05),
    )
    bundle = build_gaia_sdss_anomaly_bundle(profile)
    raw_path, bundle_json, bundle_md = write_gaia_sdss_anomaly_files(profile, bundle, output_dir, "gaia_vs_sdss")
    result = {"worker": "gaia_sdss", "raw_path": str(raw_path), "bundle_path": str(bundle_json), "report_path": str(bundle_md)}
    if ingest:
        ingested = ingest_gaia_sdss_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)
        result["memory_id"] = ingested["id"]
        result["summary"] = ingested["summary"]
    return result


def _run_gaia_panstarrs(step: dict, output_dir: Path, *, db_path: Path, agent_log_path: Path, ingest: bool) -> dict:
    from gaia_panstarrs_anomaly_worker import (
        build_gaia_panstarrs_anomaly_bundle,
        build_gaia_panstarrs_anomaly_profile,
        ingest_gaia_panstarrs_bundle,
        load_structured_bundle,
        write_gaia_panstarrs_anomaly_files,
    )

    gaia_bundle = load_structured_bundle(Path(step["gaia_bundle"]))
    panstarrs_bundle = load_structured_bundle(Path(step["panstarrs_bundle"]))
    profile = build_gaia_panstarrs_anomaly_profile(
        gaia_bundle,
        panstarrs_bundle,
        max_sep_arcsec=step.get("max_sep_arcsec", 5.0),
        pm_threshold_masyr=step.get("pm_threshold_masyr", 10.0),
        min_detections=step.get("min_detections", 3),
    )
    bundle = build_gaia_panstarrs_anomaly_bundle(profile)
    raw_path, bundle_json, bundle_md = write_gaia_panstarrs_anomaly_files(profile, bundle, output_dir, "gaia_vs_panstarrs")
    result = {"worker": "gaia_panstarrs", "raw_path": str(raw_path), "bundle_path": str(bundle_json), "report_path": str(bundle_md)}
    if ingest:
        ingested = ingest_gaia_panstarrs_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)
        result["memory_id"] = ingested["id"]
        result["summary"] = ingested["summary"]
    return result


def _run_gaia_ztf(step: dict, output_dir: Path, *, db_path: Path, agent_log_path: Path, ingest: bool) -> dict:
    from gaia_ztf_anomaly_worker import (
        build_gaia_ztf_anomaly_bundle,
        build_gaia_ztf_anomaly_profile,
        ingest_gaia_ztf_bundle,
        load_structured_bundle,
        write_gaia_ztf_anomaly_files,
    )

    gaia_bundle = load_structured_bundle(Path(step["gaia_bundle"]))
    ztf_bundle = load_structured_bundle(Path(step["ztf_bundle"]))
    profile = build_gaia_ztf_anomaly_profile(
        gaia_bundle,
        ztf_bundle,
        max_sep_arcsec=step.get("max_sep_arcsec", 30.0),
        pm_threshold_masyr=step.get("pm_threshold_masyr", 10.0),
        seeing_threshold=step.get("seeing_threshold", 2.5),
    )
    bundle = build_gaia_ztf_anomaly_bundle(profile)
    raw_path, bundle_json, bundle_md = write_gaia_ztf_anomaly_files(profile, bundle, output_dir, "gaia_vs_ztf")
    result = {"worker": "gaia_ztf", "raw_path": str(raw_path), "bundle_path": str(bundle_json), "report_path": str(bundle_md)}
    if ingest:
        ingested = ingest_gaia_ztf_bundle(bundle_json, db_path=db_path, agent_log_path=agent_log_path)
        result["memory_id"] = ingested["id"]
        result["summary"] = ingested["summary"]
    return result


WORKER_DISPATCH = {
    "gaia_sdss": _run_gaia_sdss,
    "gaia_panstarrs": _run_gaia_panstarrs,
    "gaia_ztf": _run_gaia_ztf,
}


def load_manifest(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest.get("steps"), list) or not manifest["steps"]:
        raise ValueError("Manifest must contain a non-empty 'steps' array.")
    for index, step in enumerate(manifest["steps"]):
        worker = step.get("worker")
        if worker not in SUPPORTED_WORKERS:
            raise ValueError(f"Step {index}: unsupported worker '{worker}'. Supported: {sorted(SUPPORTED_WORKERS)}")
        if not step.get("gaia_bundle"):
            raise ValueError(f"Step {index}: missing 'gaia_bundle' path.")
    return manifest


def run_manifest(
    manifest: dict,
    *,
    output_dir: Path,
    db_path: Path,
    agent_log_path: Path,
    ingest: bool = False,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    started_at = iso_timestamp()
    results = []
    for index, step in enumerate(manifest["steps"]):
        worker = step["worker"]
        handler = WORKER_DISPATCH[worker]
        result = handler(step, output_dir, db_path=db_path, agent_log_path=agent_log_path, ingest=ingest)
        result["step_index"] = index
        results.append(result)
        print(f"Step {index} ({worker}): {result.get('bundle_path')}")

    replay_report = {
        "manatuabon_schema": "replay_manifest_report_v1",
        "started_at": started_at,
        "finished_at": iso_timestamp(),
        "manifest_steps": len(manifest["steps"]),
        "completed_steps": len(results),
        "ingest": ingest,
        "results": results,
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"replay_report_{stamp}.json"
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(replay_report, handle, indent=2, ensure_ascii=False)
    print(f"Replay report written: {report_path}")
    return replay_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay a manifest of saved bundle paths through deterministic cross-match workers for reproducible analysis."
    )
    parser.add_argument("--manifest", required=True, help="Path to a JSON replay manifest file")
    parser.add_argument("--out-dir", default=str(DEFAULT_INBOX_DIR), help="Output directory for replay artifacts")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite runtime DB path for optional direct ingest")
    parser.add_argument("--agent-log", default=str(Path(DEFAULT_DB_PATH).parent / "agent_log.json"), help="Agent log path")
    parser.add_argument("--ingest", action="store_true", help="Ingest each worker output into the runtime DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = load_manifest(Path(args.manifest))
    run_manifest(
        manifest,
        output_dir=Path(args.out_dir),
        db_path=Path(args.db),
        agent_log_path=Path(args.agent_log),
        ingest=args.ingest,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
