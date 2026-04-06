"""Build review-ready Manatuabon evidence bundles from local pulsar catalog tables.

This importer is intentionally offline-first: it reads local CSV or JSON tables,
filters them for a target pulsar, computes glitch summary statistics, and writes a
structured inbox bundle that the watcher can ingest deterministically.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import tarfile
from datetime import datetime, timezone
from pathlib import Path

from db_init import ensure_runtime_db


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INBOX_DIR = BASE_DIR / "inbox"
DEFAULT_DB_PATH = BASE_DIR / "manatuabon.db"


CANONICAL_HYPOTHESIS_RULES = [
    {
        "target_aliases": {"b083345", "j08354510", "vela", "velapulsar"},
        "focus_tokens": {"crustal", "memory"},
        "hypothesis_id": "H19",
        "title": "Crustal Memory in Vela Pulsar",
        "description": (
            "The Vela pulsar glitch sequence may preserve a persistent post-glitch component that records irreversible "
            "crust-superfluid restructuring rather than purely elastic recovery. For well-sampled Vela glitches, "
            "post-glitch timing fits should retain a non-zero asymptotic spin-frequency offset measured in microhertz, "
            "with a recurrent fractional permanent component Delta nu_p / Delta nu >= 0.01 across a subset of events. "
            "Recovery timescales measured in days should not remove that offset under standard two-component fits; if "
            "vortex-creep models eliminate the residual without a persistent term, the hypothesis fails. The thread is "
            "evaluated against ATNF timing parameters, glitch-catalog cadence, and future high-cadence Vela timing updates."
        ),
        "status": "needs_revision",
        "confidence": 0.64,
        "context_hypotheses": [
            {"id": "H14", "title": "The Pulsar Timing Web", "domain": "pulsars"}
        ],
        "context_domains": ["pulsars"],
    }
]


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "pulsar"


def is_psrcat_package(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tar.gz") and "psrcat_pkg" in name


def normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def normalize_name(value: str) -> str:
    return normalize_key(value).replace("psr", "")


def build_target_aliases(target: str) -> set[str]:
    normalized = normalize_name(target)
    aliases = {normalized}
    if "b083345" in normalized or "vela" in normalized:
        aliases.update({"b083345", "j083520451035", "velapulsar", "psrb083345", "psrj083520451035"})
    return aliases


def focus_tokens(text: str) -> set[str]:
    return {normalize_key(part) for part in str(text or "").split() if normalize_key(part)}


def resolve_canonical_rule(target: str, hypothesis_focus: str) -> dict | None:
    aliases = build_target_aliases(target)
    focus = focus_tokens(hypothesis_focus)
    for rule in CANONICAL_HYPOTHESIS_RULES:
        if not (aliases & set(rule["target_aliases"])):
            continue
        if not set(rule["focus_tokens"]).issubset(focus):
            continue
        return rule
    return None


def ensure_canonical_hypothesis(db_path: Path, rule: dict) -> str:
    conn = ensure_runtime_db(db_path, migrate=False)
    try:
        conn.row_factory = sqlite3.Row
        now = datetime.now().isoformat()
        existing = conn.execute("SELECT id FROM hypotheses WHERE id=?", (rule["hypothesis_id"],)).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE hypotheses
                SET title=?, description=?, status=?, updated_at=?, origin=COALESCE(origin, 'manual'), confidence=COALESCE(confidence, ?),
                    context_hypotheses=CASE
                        WHEN context_hypotheses IS NULL OR context_hypotheses='' OR context_hypotheses='[]' THEN ?
                        ELSE context_hypotheses
                    END,
                    context_domains=CASE
                        WHEN context_domains IS NULL OR context_domains='' OR context_domains='[]' THEN ?
                        ELSE context_domains
                    END
                WHERE id=?
                """,
                (
                    rule["title"],
                    rule["description"],
                    rule["status"],
                    now,
                    rule["confidence"],
                    json.dumps(rule.get("context_hypotheses", []), ensure_ascii=False),
                    json.dumps(rule.get("context_domains", []), ensure_ascii=False),
                    rule["hypothesis_id"],
                ),
            )
            conn.commit()
            return rule["hypothesis_id"]

        conn.execute(
            """
            INSERT INTO hypotheses (
                id, title, description, evidence, status, source, date, origin, parent_id, root_id,
                merged_into, created_at, updated_at, confidence, confidence_components, confidence_source,
                context_hypotheses, context_domains
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule["hypothesis_id"],
                rule["title"],
                rule["description"],
                json.dumps([], ensure_ascii=False),
                rule["status"],
                "Manual Canonical Seed",
                now,
                "manual",
                None,
                rule["hypothesis_id"],
                None,
                now,
                now,
                rule["confidence"],
                json.dumps({}, ensure_ascii=False),
                "manual_seed",
                json.dumps(rule.get("context_hypotheses", []), ensure_ascii=False),
                json.dumps(rule.get("context_domains", []), ensure_ascii=False),
            ),
        )
        conn.execute(
            "INSERT INTO confidence_history (hypothesis_id, confidence, source, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
            (
                rule["hypothesis_id"],
                rule["confidence"],
                "manual_seed",
                "Canonical Vela crustal-memory thread initialized for direct evidence linking.",
                now,
            ),
        )
        conn.commit()
        return rule["hypothesis_id"]
    finally:
        conn.close()


def canonicalize_existing_duplicates(db_path: Path, rule: dict) -> dict:
    conn = ensure_runtime_db(db_path, migrate=False)
    try:
        conn.row_factory = sqlite3.Row
        now = datetime.now().isoformat()
        duplicate_rows = conn.execute(
            """
            SELECT id, title, status
            FROM hypotheses
            WHERE id != ?
              AND title IN ('Crustal Memory in J0835-4510', 'Crustal Memory in Vela Pulsar')
            ORDER BY id
            """,
            (rule["hypothesis_id"],),
        ).fetchall()
        duplicate_ids = [row["id"] for row in duplicate_rows]
        for duplicate_id in duplicate_ids:
            conn.execute(
                "UPDATE hypotheses SET status='merged', merged_into=?, updated_at=? WHERE id=?",
                (rule["hypothesis_id"], now, duplicate_id),
            )

        memory_updates = conn.execute(
            """
            UPDATE memories
            SET supports_hypothesis=?, challenges_hypothesis=NULL
            WHERE content LIKE '%Structured evidence bundle for%J0835-4510%'
            """,
            (rule["hypothesis_id"],),
        ).rowcount or 0

        conn.execute(
            "UPDATE hypotheses SET updated_at=? WHERE id=?",
            (now, rule["hypothesis_id"]),
        )
        conn.commit()
        return {"duplicates_merged": duplicate_ids, "memory_updates": memory_updates}
    finally:
        conn.close()


def sync_canonical_hypothesis_support(db_path: Path, hypothesis_id: str, fallback_domains: list[str] | None = None) -> dict:
    conn = ensure_runtime_db(db_path, migrate=False)
    try:
        conn.row_factory = sqlite3.Row
        hypothesis_row = conn.execute(
            "SELECT evidence, context_domains FROM hypotheses WHERE id=?",
            (hypothesis_id,),
        ).fetchone()
        if not hypothesis_row:
            return {"evidence_count": 0, "support_memory_count": 0}

        existing_evidence = []
        try:
            parsed = json.loads(hypothesis_row["evidence"] or "[]")
            if isinstance(parsed, list):
                existing_evidence = [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            existing_evidence = []

        domain_set = set(fallback_domains or [])
        try:
            parsed_domains = json.loads(hypothesis_row["context_domains"] or "[]")
            if isinstance(parsed_domains, list):
                domain_set.update(str(item).strip() for item in parsed_domains if str(item).strip())
        except json.JSONDecodeError:
            pass

        support_rows = conn.execute(
            "SELECT content, domain_tags FROM memories WHERE supports_hypothesis=? ORDER BY id",
            (hypothesis_id,),
        ).fetchall()

        combined_evidence = []
        seen = set()
        for item in existing_evidence:
            if item not in seen:
                combined_evidence.append(item)
                seen.add(item)

        for row in support_rows:
            content = str(row["content"] or "").strip()
            if content and content not in seen:
                combined_evidence.append(content)
                seen.add(content)
            try:
                parsed_domains = json.loads(row["domain_tags"] or "[]")
                if isinstance(parsed_domains, list):
                    domain_set.update(str(item).strip() for item in parsed_domains if str(item).strip())
            except json.JSONDecodeError:
                continue

        conn.execute(
            "UPDATE hypotheses SET evidence=?, context_domains=?, updated_at=? WHERE id=?",
            (
                json.dumps(combined_evidence, ensure_ascii=False),
                json.dumps(sorted(domain_set), ensure_ascii=False),
                datetime.now().isoformat(),
                hypothesis_id,
            ),
        )
        conn.commit()
        return {
            "evidence_count": len(combined_evidence),
            "support_memory_count": len(support_rows),
        }
    finally:
        conn.close()


def read_table_rows(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with open(path, "r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if isinstance(raw, list):
            return [row for row in raw if isinstance(row, dict)]
        if isinstance(raw, dict):
            if isinstance(raw.get("rows"), list):
                return [row for row in raw["rows"] if isinstance(row, dict)]
            if isinstance(raw.get("events"), list):
                return [row for row in raw["events"] if isinstance(row, dict)]
            return [raw]
    if suffix == ".db":
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
        return parse_psrcat_db(text)
    raise ValueError(f"Unsupported table format: {path}")


def extract_psrcat_package(path: Path) -> tuple[list[dict], list[dict]]:
    with tarfile.open(path, "r:gz") as archive:
        psrcat_member = next((member for member in archive.getmembers() if member.name.endswith("/psrcat.db")), None)
        glitch_member = next((member for member in archive.getmembers() if member.name.endswith("/glitch.db")), None)
        if psrcat_member is None or glitch_member is None:
            raise ValueError("psrcat package must contain both psrcat.db and glitch.db")

        psrcat_handle = archive.extractfile(psrcat_member)
        glitch_handle = archive.extractfile(glitch_member)
        if psrcat_handle is None or glitch_handle is None:
            raise ValueError("failed to extract psrcat package members")

        psrcat_text = psrcat_handle.read().decode("utf-8", errors="replace")
        glitch_text = glitch_handle.read().decode("utf-8", errors="replace")
        return parse_psrcat_db(psrcat_text), parse_glitch_db(glitch_text)


def parse_psrcat_db(text: str) -> list[dict]:
    records = []
    current = {}
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if line.startswith("@"):
            if current:
                records.append(current)
                current = {}
            continue

        parts = line.split()
        if len(parts) < 2:
            continue
        key = parts[0]
        if not re.fullmatch(r"[A-Z0-9_+-]+", key):
            continue
        value = parts[1]
        current[key] = value

    if current:
        records.append(current)
    return records


def parse_glitch_db(text: str) -> list[dict]:
    rows = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Name") or stripped.startswith("_"):
            continue
        if len(stripped) < 10:
            continue
        columns = [part.strip() for part in re.split(r"\s{2,}", stripped) if part.strip()]
        if len(columns) < 4:
            continue

        row = {
            "psrb": columns[0] if len(columns) > 0 else None,
            "psrj": columns[1] if len(columns) > 1 else None,
            "glitch_mjd": columns[2] if len(columns) > 2 else None,
            "delta_nu_over_nu": columns[3] if len(columns) > 3 else None,
            "delta_nudot_over_nudot_milli": columns[4] if len(columns) > 4 else None,
            "q_permanent": columns[5] if len(columns) > 5 else None,
            "tau_d": columns[6] if len(columns) > 6 else None,
            "reference": columns[7] if len(columns) > 7 else None,
        }
        rows.append(row)
    return rows


def first_value(row: dict, *keys: str):
    normalized = {normalize_key(key): value for key, value in row.items()}
    for key in keys:
        value = normalized.get(normalize_key(key))
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "nan", "null"}:
            return value
    return None


def as_float(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    text = re.sub(r"\(.*?\)", "", text)
    if text in {"-", "*"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def match_target_rows(rows: list[dict], aliases: set[str], candidate_fields: tuple[str, ...]) -> list[dict]:
    matches = []
    for row in rows:
        for field in candidate_fields:
            value = first_value(row, field)
            if value is None:
                continue
            if normalize_name(str(value)) in aliases:
                matches.append(row)
                break
    return matches


def extract_target_metadata(row: dict, target: str) -> dict:
    display_name = first_value(row, "name", "target", "psr_name", "psrj", "jname", "bname", "psrb") or target
    return {
        "display_name": str(display_name),
        "psrj": first_value(row, "psrj", "jname"),
        "psrb": first_value(row, "psrb", "bname"),
        "period_s": as_float(first_value(row, "p0", "period", "period_s")),
        "period_derivative": as_float(first_value(row, "p1", "pdot", "period_derivative")),
        "characteristic_age_kyr": as_float(first_value(row, "age_kyr", "age", "characteristic_age_kyr", "age_yr")),
        "distance_kpc": as_float(first_value(row, "distance_kpc", "dist", "dist_kpc", "distance")),
        "magnetic_field_gauss": as_float(first_value(row, "b_field_gauss", "bsurf", "bfield", "magnetic_field")),
    }


def extract_glitch_events(rows: list[dict]) -> list[dict]:
    events = []
    for row in rows:
        event = {
            "event_id": first_value(row, "event_id", "glitch_id", "id"),
            "mjd": as_float(first_value(row, "mjd", "glitch_mjd", "epoch_mjd")),
            "year": as_float(first_value(row, "year", "glitch_year", "epoch_year")),
            "delta_nu_over_nu": as_float(first_value(row, "delta_nu_over_nu", "dnu_over_nu", "glitch_domega_fractional", "frac_size")),
            "permanent_fraction": as_float(first_value(row, "permanent_fraction", "perm_fraction", "delta_nu_p_over_delta_nu", "q_permanent")),
            "recovery_tau_days": as_float(first_value(row, "recovery_tau_days", "tau_days", "tau_d")),
            "reference": first_value(row, "reference", "ref", "paper"),
        }
        if any(value is not None for value in event.values()):
            events.append(event)

    events.sort(key=lambda item: (item["mjd"] is None, item["mjd"] if item["mjd"] is not None else item["year"] or 0.0))
    return events


def compute_glitch_summary(events: list[dict]) -> dict:
    mjds = [event["mjd"] for event in events if event.get("mjd") is not None]
    years = [event["year"] for event in events if event.get("year") is not None]
    sizes = [event["delta_nu_over_nu"] for event in events if event.get("delta_nu_over_nu") is not None]
    permanent = [event["permanent_fraction"] for event in events if event.get("permanent_fraction") is not None]

    interval_years = None
    if len(mjds) >= 2:
        deltas = [(right - left) / 365.25 for left, right in zip(mjds[:-1], mjds[1:]) if right > left]
        if deltas:
            interval_years = sum(deltas) / len(deltas)
    elif len(years) >= 2:
        deltas = [right - left for left, right in zip(years[:-1], years[1:]) if right > left]
        if deltas:
            interval_years = sum(deltas) / len(deltas)

    return {
        "glitch_count": len(events),
        "first_mjd": min(mjds) if mjds else None,
        "last_mjd": max(mjds) if mjds else None,
        "first_year": min(years) if years else None,
        "last_year": max(years) if years else None,
        "mean_interval_years": round(interval_years, 3) if interval_years is not None else None,
        "max_delta_nu_over_nu": max(sizes) if sizes else None,
        "mean_delta_nu_over_nu": round(sum(sizes) / len(sizes), 9) if sizes else None,
        "mean_permanent_fraction": round(sum(permanent) / len(permanent), 3) if permanent else None,
        "recent_glitches": events[-5:],
    }


def build_crustal_memory_hypothesis(target_metadata: dict, glitch_summary: dict) -> dict:
    display_name = target_metadata.get("display_name") or "target pulsar"
    glitch_count = glitch_summary.get("glitch_count", 0)
    interval_years = glitch_summary.get("mean_interval_years")
    permanent_fraction = glitch_summary.get("mean_permanent_fraction")
    body_parts = [
        f"Local pulsar catalog ingestion for {display_name} captured {glitch_count} glitch events",
        "suitable for testing whether a persistent post-glitch component indicates crustal memory rather than fully elastic recovery.",
    ]
    if interval_years is not None:
        body_parts.append(f"The mean inter-glitch spacing in the imported table is {interval_years:.2f} years.")
    if permanent_fraction is not None:
        body_parts.append(f"Imported events with permanent-fraction estimates average {permanent_fraction:.2f}.")
    body_parts.append("The hypothesis remains falsifiable by future glitch timing and by evidence that the permanent component is absent or inconsistent across events.")
    return {
        "title": f"Crustal Memory in {display_name}",
        "body": " ".join(body_parts),
        "confidence": 0.67,
        "predictions": [
            "Future Vela glitches should preserve a measurable permanent component rather than fully relaxing to the pre-glitch baseline.",
            "A table-level reanalysis should recover a stable inter-glitch cadence within the historical range if crustal memory persists.",
        ],
    }


def build_evidence_bundle(
    atnf_rows: list[dict],
    glitch_rows: list[dict],
    *,
    target: str = "PSR B0833-45",
    hypothesis_focus: str = "Crustal Memory",
    supports_hypothesis: str | None = None,
    source_catalogs: list[str] | None = None,
) -> dict:
    aliases = build_target_aliases(target)
    target_matches = match_target_rows(atnf_rows, aliases, ("psrj", "jname", "psrb", "bname", "name", "target"))
    glitch_matches = match_target_rows(glitch_rows, aliases, ("psrj", "jname", "psrb", "bname", "name", "target", "pulsar"))

    target_metadata = extract_target_metadata(target_matches[0], target) if target_matches else {"display_name": target}
    events = extract_glitch_events(glitch_matches)
    glitch_summary = compute_glitch_summary(events)

    anomalies = []
    if glitch_summary.get("mean_permanent_fraction") is not None:
        anomalies.append(f"Imported glitches include a persistent post-glitch component with mean fraction {glitch_summary['mean_permanent_fraction']:.2f}.")
    if glitch_summary.get("mean_interval_years") is not None:
        anomalies.append(f"Historical inter-glitch cadence from imported table is {glitch_summary['mean_interval_years']:.2f} years.")
    if glitch_summary.get("glitch_count", 0) == 0:
        anomalies.append("No target-specific glitch rows were found in the supplied table.")

    significance = 0.52
    if target_matches:
        significance += 0.08
    if glitch_summary.get("glitch_count", 0) >= 3:
        significance += 0.10
    if glitch_summary.get("glitch_count", 0) >= 10:
        significance += 0.05
    if len(source_catalogs or []) >= 2:
        significance += 0.05
    if glitch_summary.get("mean_permanent_fraction") is not None:
        significance += 0.05
    significance = max(0.0, min(round(significance, 3), 0.85))

    source_catalogs = source_catalogs or ["ATNF Pulsar Catalogue", "Glitch Catalogue"]
    bundle = {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "pulsar_glitch_evidence_bundle",
        "summary": f"Structured evidence bundle for {target_metadata.get('display_name', target)} combining {len(target_matches)} pulsar-catalog row(s) and {glitch_summary['glitch_count']} glitch event(s).",
        "entities": [target_metadata.get("display_name", target), "pulsar", "glitch", "crustal memory", "neutron star crust"],
        "topics": ["pulsar glitches", "crustal memory", "timing analysis", "superfluid coupling"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["pulsars"],
        "source_catalogs": source_catalogs,
        "target": {
            "name": target_metadata.get("display_name", target),
            "input_target": target,
            "psrj": target_metadata.get("psrj"),
            "psrb": target_metadata.get("psrb"),
        },
        "structured_evidence": {
            "hypothesis_focus": hypothesis_focus,
            "target_metadata": target_metadata,
            "glitch_summary": glitch_summary,
            "catalog_match_count": len(target_matches),
            "glitch_match_count": len(glitch_matches),
        },
        "new_hypothesis": None if supports_hypothesis else build_crustal_memory_hypothesis(target_metadata, glitch_summary),
    }
    return bundle


def build_text_report(bundle: dict) -> str:
    target = bundle.get("target", {})
    evidence = bundle.get("structured_evidence", {})
    glitch_summary = evidence.get("glitch_summary", {})
    lines = [
        f"Pulsar glitch evidence bundle for {target.get('name', 'unknown target')}",
        "=" * 60,
        f"Generated: {iso_timestamp()}",
        f"Catalog rows matched: {evidence.get('catalog_match_count', 0)}",
        f"Glitch rows matched: {evidence.get('glitch_match_count', 0)}",
        f"Mean interval (years): {glitch_summary.get('mean_interval_years')}",
        f"Mean permanent fraction: {glitch_summary.get('mean_permanent_fraction')}",
        f"Summary: {bundle.get('summary', '')}",
    ]
    for anomaly in bundle.get("anomalies", []):
        lines.append(f"- {anomaly}")
    return "\n".join(lines) + "\n"


def write_bundle(bundle: dict, inbox_dir: Path, target_label: str, filename_prefix: str = "pulsar_glitch_bundle") -> tuple[Path, Path]:
    inbox_dir.mkdir(parents=True, exist_ok=True)
    safe_target = sanitize_filename(target_label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = inbox_dir / f"{filename_prefix}_{safe_target}_{stamp}.json"
    txt_path = inbox_dir / f"{filename_prefix}_{safe_target}_{stamp}.md"

    json_tmp = json_path.with_suffix(json_path.suffix + ".tmp")
    txt_tmp = txt_path.with_suffix(txt_path.suffix + ".tmp")

    with open(json_tmp, "w", encoding="utf-8") as handle:
        json.dump(bundle, handle, indent=2, ensure_ascii=False)
    with open(txt_tmp, "w", encoding="utf-8") as handle:
        handle.write(build_text_report(bundle))

    json_tmp.replace(json_path)
    txt_tmp.replace(txt_path)
    return json_path, txt_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Manatuabon pulsar glitch evidence bundle from local tables.")
    parser.add_argument("--atnf", required=True, help="Path to ATNF-style CSV/JSON table, ATNF .db file, or psrcat_pkg.tar.gz package")
    parser.add_argument("--glitches", required=False, help="Path to glitch catalog CSV/JSON/.db table. Optional when --atnf points to psrcat_pkg.tar.gz")
    parser.add_argument("--target", default="PSR B0833-45", help="Target pulsar to filter for")
    parser.add_argument("--hypothesis-focus", default="Crustal Memory", help="Hypothesis focus label to store in the bundle")
    parser.add_argument("--supports-hypothesis", default=None, help="Existing hypothesis ID to link as direct support")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Inbox directory for watcher-ready output")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database used to resolve or seed canonical hypothesis threads")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    atnf_path = Path(args.atnf)
    glitch_path = Path(args.glitches) if args.glitches else None
    db_path = Path(args.db)

    if is_psrcat_package(atnf_path):
        atnf_rows, glitch_rows = extract_psrcat_package(atnf_path)
        source_catalogs = [f"ATNF:{atnf_path.name}:psrcat.db", f"ATNF:{atnf_path.name}:glitch.db"]
    else:
        if glitch_path is None:
            raise SystemExit("--glitches is required unless --atnf points to psrcat_pkg.tar.gz")
        atnf_rows = read_table_rows(atnf_path)
        glitch_rows = read_table_rows(glitch_path)
        source_catalogs = [f"ATNF:{atnf_path.name}", f"GlitchCatalog:{glitch_path.name}"]

    supports_hypothesis = args.supports_hypothesis
    canonical_rule = resolve_canonical_rule(args.target, args.hypothesis_focus)
    if not supports_hypothesis and canonical_rule:
        supports_hypothesis = ensure_canonical_hypothesis(db_path, canonical_rule)
        canonicalize_existing_duplicates(db_path, canonical_rule)
        sync_canonical_hypothesis_support(
            db_path,
            supports_hypothesis,
            fallback_domains=canonical_rule.get("context_domains", []),
        )

    bundle = build_evidence_bundle(
        atnf_rows,
        glitch_rows,
        target=args.target,
        hypothesis_focus=args.hypothesis_focus,
        supports_hypothesis=supports_hypothesis,
        source_catalogs=source_catalogs,
    )
    json_path, txt_path = write_bundle(bundle, Path(args.inbox), args.target)
    print(f"Structured bundle written: {json_path}")
    print(f"Companion report written: {txt_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())