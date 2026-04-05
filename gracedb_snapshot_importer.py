"""Fetch GraceDB snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


DEFAULT_GRACEDB_SERVICE_URL = "https://gracedb.ligo.org/api/"
GRACEDB_ACKNOWLEDGEMENT = "GraceDB data should be treated as source metadata and analyst workflow context, not autonomous truth."


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "gracedb"


def response_json(response):
    if response is None:
        return None
    if hasattr(response, "json"):
        return response.json()
    return response


def build_gracedb_client(
    *,
    service_url: str = DEFAULT_GRACEDB_SERVICE_URL,
    cred: str | None = None,
    force_noauth: bool = False,
    fail_if_noauth: bool = False,
    use_auth: str = "all",
    retries: int = 5,
    backoff_factor: float = 0.1,
):
    try:
        from ligo.gracedb.rest import GraceDb
    except ImportError as exc:
        raise RuntimeError(
            "GraceDB importer requires the 'ligo-gracedb' package in the active Python environment. "
            "Install it with: d:/Manatuabon/.venv/Scripts/python.exe -m pip install ligo-gracedb"
        ) from exc

    kwargs = {
        "service_url": service_url,
        "force_noauth": force_noauth,
        "fail_if_noauth": fail_if_noauth,
        "use_auth": use_auth,
        "retries": retries,
        "backoff_factor": backoff_factor,
    }
    if cred:
        if "," in cred:
            cert_file, key_file = [part.strip() for part in cred.split(",", 1)]
            kwargs["cred"] = (cert_file, key_file)
        else:
            kwargs["cred"] = cred.strip()
    return GraceDb(**kwargs)


def safe_call(name: str, func, errors: dict):
    try:
        return response_json(func())
    except Exception as exc:
        errors[name] = str(exc)
        return None


def normalize_labels(data) -> list[str]:
    if not data:
        return []
    if isinstance(data, dict) and isinstance(data.get("labels"), list):
        return [item.get("name") if isinstance(item, dict) else str(item) for item in data["labels"]]
    if isinstance(data, list):
        return [item.get("name") if isinstance(item, dict) else str(item) for item in data]
    return []


def normalize_logs(data, limit: int = 10) -> list[dict]:
    if not data:
        return []
    items = []
    if isinstance(data, dict):
        items = data.get("log", []) or data.get("logs", []) or []
    elif isinstance(data, list):
        items = data
    normalized = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "id": item.get("N") or item.get("id") or item.get("n"),
            "comment": item.get("comment") or item.get("message"),
            "created": item.get("created") or item.get("date"),
            "filename": item.get("filename"),
        })
    return normalized


def normalize_files(data) -> list[dict]:
    if not data:
        return []
    if isinstance(data, dict):
        return [{"name": key, "url": value} for key, value in data.items()]
    return []


def normalize_voevents(data, limit: int = 10) -> list[dict]:
    if not data:
        return []
    items = []
    if isinstance(data, dict):
        items = data.get("voevents", []) or []
    elif isinstance(data, list):
        items = data
    normalized = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "id": item.get("N") or item.get("id"),
            "type": item.get("voevent_type") or item.get("type"),
            "created": item.get("created") or item.get("date"),
            "filename": item.get("filename"),
        })
    return normalized


def normalize_emobservations(data, limit: int = 10) -> list[dict]:
    if not data:
        return []
    items = []
    if isinstance(data, dict):
        items = data.get("observations", []) or []
    elif isinstance(data, list):
        items = data
    normalized = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "id": item.get("N") or item.get("id"),
            "group": item.get("group"),
            "comment": item.get("comment"),
            "created": item.get("created") or item.get("date"),
        })
    return normalized


def normalize_signoffs(data, limit: int = 10) -> list[dict]:
    if not data:
        return []
    items = []
    if isinstance(data, dict):
        items = data.get("signoffs", []) or []
    elif isinstance(data, list):
        items = data
    normalized = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "type": item.get("signoff_type") or item.get("type"),
            "status": item.get("status"),
            "instrument": item.get("instrument"),
            "comment": item.get("comment"),
        })
    return normalized


def collect_gracedb_event_snapshot(client, event_id: str) -> dict:
    errors = {}
    event_record = response_json(client.event(event_id))
    snapshot = {
        "source": "GraceDB",
        "kind": "event",
        "object_id": event_id,
        "fetched_at": iso_timestamp(),
        "record": event_record,
        "related": {
            "labels": safe_call("labels", lambda: client.labels(event_id), errors),
            "logs": safe_call("logs", lambda: client.logs(event_id), errors),
            "files": safe_call("files", lambda: client.files(event_id), errors),
            "voevents": safe_call("voevents", lambda: client.voevents(event_id), errors),
            "emobservations": safe_call("emobservations", lambda: client.emobservations(event_id), errors),
        },
        "errors": errors,
    }
    return snapshot


def collect_gracedb_superevent_snapshot(client, superevent_id: str) -> dict:
    errors = {}
    superevent_record = response_json(client.superevent(superevent_id))
    snapshot = {
        "source": "GraceDB",
        "kind": "superevent",
        "object_id": superevent_id,
        "fetched_at": iso_timestamp(),
        "record": superevent_record,
        "related": {
            "labels": safe_call("labels", lambda: client.labels(superevent_id), errors),
            "logs": safe_call("logs", lambda: client.logs(superevent_id), errors),
            "files": safe_call("files", lambda: client.files(superevent_id), errors),
            "voevents": safe_call("voevents", lambda: client.voevents(superevent_id), errors),
            "emobservations": safe_call("emobservations", lambda: client.emobservations(superevent_id), errors),
            "signoffs": safe_call("signoffs", lambda: client.signoffs(superevent_id), errors),
        },
        "errors": errors,
    }
    return snapshot


def collect_gracedb_query_snapshot(client, query: str, mode: str = "superevents", max_results: int = 10) -> dict:
    if mode == "events":
        results = list(client.events(query=query, max_results=max_results))
    else:
        results = list(client.superevents(query=query, max_results=max_results))
    return {
        "source": "GraceDB",
        "kind": "query",
        "query_mode": mode,
        "query": query,
        "fetched_at": iso_timestamp(),
        "results": results,
        "errors": {},
    }


def build_gracedb_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = True,
) -> dict:
    kind = snapshot.get("kind")
    related = snapshot.get("related", {})
    record = snapshot.get("record", {}) if isinstance(snapshot.get("record"), dict) else {}

    if kind == "query":
        results = snapshot.get("results", [])
        summary_target = snapshot.get("query") or "GraceDB query"
        summary = f"Structured GraceDB snapshot bundle covering {len(results)} {snapshot.get('query_mode', 'superevent')} result(s) for query '{summary_target}'."
        entities = [summary_target]
        topics = ["GraceDB", snapshot.get("query_mode", "superevents"), "gravitational-wave events"]
        anomalies = []
        if not results:
            anomalies.append("The GraceDB query returned no results.")
        significance = min(0.5 + (0.08 if results else 0.0), 0.8)
        structured_evidence = {
            "query": snapshot.get("query"),
            "query_mode": snapshot.get("query_mode"),
            "result_count": len(results),
            "results": results,
        }
    else:
        object_id = snapshot.get("object_id")
        labels = normalize_labels(related.get("labels"))
        logs = normalize_logs(related.get("logs"))
        files = normalize_files(related.get("files"))
        voevents = normalize_voevents(related.get("voevents"))
        emobservations = normalize_emobservations(related.get("emobservations"))
        signoffs = normalize_signoffs(related.get("signoffs"))
        preferred_event = record.get("preferred_event") or record.get("graceid")
        summary = (
            f"Structured GraceDB snapshot bundle for {kind} {object_id} with "
            f"{len(labels)} labels, {len(logs)} logs, {len(files)} files, {len(voevents)} VOEvents, and {len(emobservations)} EM observations."
        )
        entities = [item for item in [object_id, preferred_event, record.get("gw_id"), record.get("pipeline")] if item]
        topics = ["GraceDB", f"{kind} metadata", "gravitational-wave events", "multimessenger review"]
        anomalies = []
        if not files:
            anomalies.append("No file inventory was available for this GraceDB object during snapshot fetch.")
        if kind == "superevent" and not signoffs:
            anomalies.append("No signoffs were retrieved for this superevent; this may reflect permissions or absent signoffs.")
        if snapshot.get("errors"):
            anomalies.append("Some related GraceDB resources could not be fetched; inspect the raw snapshot errors field.")
        significance = 0.72
        if labels:
            significance += 0.04
        if files:
            significance += 0.04
        if voevents or emobservations:
            significance += 0.03
        significance = min(round(significance, 3), 0.88)
        structured_evidence = {
            "kind": kind,
            "object_id": object_id,
            "record": record,
            "labels": labels,
            "logs": logs,
            "files": files,
            "voevents": voevents,
            "emobservations": emobservations,
            "signoffs": signoffs,
            "errors": snapshot.get("errors", {}),
        }

    domain_tags = ["gravitational_waves"]
    joined = json.dumps(structured_evidence, ensure_ascii=False).lower()
    if "raven" in joined or structured_evidence.get("emobservations"):
        domain_tags.append("multimessenger")

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "gracedb_snapshot_bundle",
        "summary": summary,
        "entities": entities,
        "topics": topics,
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": sorted(set(domain_tags)),
        "source_catalogs": ["GraceDB", DEFAULT_GRACEDB_SERVICE_URL],
        "target": {
            "name": snapshot.get("object_id") or snapshot.get("query") or "GraceDB",
            "input_target": snapshot.get("object_id") or snapshot.get("query") or "GraceDB",
            "kind": snapshot.get("kind"),
        },
        "structured_evidence": structured_evidence,
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"GraceDB snapshot: {snapshot.get('object_id') or snapshot.get('query')}",
            "body": " ".join([
                summary,
                "Use this snapshot as provenance-rich event evidence and analyst workflow context rather than a direct autonomous conclusion.",
            ]),
            "confidence": 0.6,
            "predictions": [
                "Subsequent GraceDB snapshots for the same object should preserve stable identifiers while labels, logs, and related files may evolve.",
                "If the event or superevent is materially updated, a later snapshot should expose changed workflow metadata through logs, files, or labels.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": GRACEDB_ACKNOWLEDGEMENT,
            "service_url": DEFAULT_GRACEDB_SERVICE_URL,
        },
    }


def write_gracedb_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"gracedb_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="gracedb_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_gracedb_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch GraceDB snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--event-id", default=None, help="GraceDB event graceid to fetch")
    parser.add_argument("--superevent-id", default=None, help="GraceDB superevent ID to fetch")
    parser.add_argument("--query", default=None, help="GraceDB query string for search mode")
    parser.add_argument("--query-mode", default="superevents", choices=["superevents", "events"], help="Search mode for --query")
    parser.add_argument("--max-results", type=int, default=10, help="Maximum number of query results to include")
    parser.add_argument("--service-url", default=DEFAULT_GRACEDB_SERVICE_URL, help="GraceDB API root URL")
    parser.add_argument("--cred", default=None, help="Optional X.509 credential spec: combined proxy path OR 'cert.pem,key.pem'")
    parser.add_argument("--force-noauth", action="store_true", help="Skip credential lookup and use unauthenticated requests")
    parser.add_argument("--fail-if-noauth", action="store_true", help="Fail client creation if auth credentials are unavailable")
    parser.add_argument("--use-auth", default="all", choices=["all", "scitoken", "x509"], help="GraceDB client auth mode")
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
    provided = [bool(args.event_id), bool(args.superevent_id), bool(args.query)]
    if sum(provided) != 1:
        raise SystemExit("Provide exactly one of --event-id, --superevent-id, or --query.")

    client = build_gracedb_client(
        service_url=args.service_url,
        cred=args.cred,
        force_noauth=args.force_noauth,
        fail_if_noauth=args.fail_if_noauth,
        use_auth=args.use_auth,
    )

    if args.event_id:
        snapshot = collect_gracedb_event_snapshot(client, args.event_id)
        label = args.event_id
    elif args.superevent_id:
        snapshot = collect_gracedb_superevent_snapshot(client, args.superevent_id)
        label = args.superevent_id
    else:
        snapshot = collect_gracedb_query_snapshot(client, args.query, mode=args.query_mode, max_results=args.max_results)
        label = args.query

    bundle = build_gracedb_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_gracedb_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_gracedb_bundle(
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