"""Fetch arXiv metadata snapshots and convert them into Manatuabon structured bundles."""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
try:
    from defusedxml.ElementTree import fromstring as _safe_fromstring
    import xml.etree.ElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
    _safe_fromstring = None
from datetime import datetime, timezone
from pathlib import Path

import requests

from db_init import ensure_runtime_db
from manatuabon_agent import AGENT_LOG_FILE, AgentLog, IngestAgent, MemoryManager
from pulsar_glitch_importer import DEFAULT_DB_PATH, DEFAULT_INBOX_DIR, write_bundle


ARXIV_API_URL = "http://export.arxiv.org/api/query"
ARXIV_ACKNOWLEDGEMENT = "Thank you to arXiv for use of its open access interoperability."
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom", "opensearch": "http://a9.com/-/spec/opensearch/1.1/"}
DEFAULT_HEADERS = {
    "User-Agent": "Manatuabon/1.0 (offline-first governed arXiv snapshot importer)",
}


class StructuredBundleOnlyNemotron:
    def chat_json(self, *args, **kwargs):
        raise AssertionError("structured ingest should not call Nemotron")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "arxiv"


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[index:index + size] for index in range(0, len(seq), size)]


def extract_arxiv_id(abs_url: str) -> str:
    if not abs_url:
        return ""
    marker = "/abs/"
    if marker in abs_url:
        return abs_url.split(marker, 1)[1].strip()
    return abs_url.rsplit("/", 1)[-1].strip()


def latest_id(versioned_id: str) -> str:
    if "v" in versioned_id:
        stem, suffix = versioned_id.rsplit("v", 1)
        if suffix.isdigit():
            return stem
    return versioned_id


def text_or_none(element: ET.Element | None, path: str, namespaces: dict | None = None) -> str | None:
    if element is None:
        return None
    found = element.find(path, namespaces or {})
    if found is None or found.text is None:
        return None
    text = found.text.strip()
    return text or None


def parse_arxiv_atom(xml_text: str) -> dict:
    try:
        if _safe_fromstring is not None:
            root = _safe_fromstring(xml_text)
        else:
            root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return {"entries": [], "parse_error": str(e)}
    feed_info = {
        "title": text_or_none(root, "atom:title", ATOM_NS),
        "id": text_or_none(root, "atom:id", ATOM_NS),
        "updated": text_or_none(root, "atom:updated", ATOM_NS),
        "total_results": int(text_or_none(root, "opensearch:totalResults", ATOM_NS) or 0),
        "start_index": int(text_or_none(root, "opensearch:startIndex", ATOM_NS) or 0),
        "items_per_page": int(text_or_none(root, "opensearch:itemsPerPage", ATOM_NS) or 0),
        "entries": [],
    }

    for entry in root.findall("atom:entry", ATOM_NS):
        abs_url = None
        pdf_url = None
        doi_url = None
        for link in entry.findall("atom:link", ATOM_NS):
            rel = link.attrib.get("rel")
            title = link.attrib.get("title")
            href = link.attrib.get("href")
            if rel == "alternate":
                abs_url = href
            elif rel == "related" and title == "pdf":
                pdf_url = href
            elif rel == "related" and title == "doi":
                doi_url = href

        authors = []
        for author in entry.findall("atom:author", ATOM_NS):
            authors.append({
                "name": text_or_none(author, "atom:name", ATOM_NS),
                "affiliation": text_or_none(author, "arxiv:affiliation", ATOM_NS),
            })

        categories = [cat.attrib.get("term") for cat in entry.findall("atom:category", ATOM_NS) if cat.attrib.get("term")]
        primary = entry.find("arxiv:primary_category", ATOM_NS)
        versioned_id = extract_arxiv_id(text_or_none(entry, "atom:id", ATOM_NS) or "")
        entry_payload = {
            "id": latest_id(versioned_id),
            "versioned_id": versioned_id,
            "title": text_or_none(entry, "atom:title", ATOM_NS),
            "summary": text_or_none(entry, "atom:summary", ATOM_NS),
            "published": text_or_none(entry, "atom:published", ATOM_NS),
            "updated": text_or_none(entry, "atom:updated", ATOM_NS),
            "authors": authors,
            "categories": categories,
            "primary_category": primary.attrib.get("term") if primary is not None else None,
            "comment": text_or_none(entry, "arxiv:comment", ATOM_NS),
            "journal_ref": text_or_none(entry, "arxiv:journal_ref", ATOM_NS),
            "doi": text_or_none(entry, "arxiv:doi", ATOM_NS),
            "abs_url": abs_url,
            "pdf_url": pdf_url,
            "doi_url": doi_url,
        }
        feed_info["entries"].append(entry_payload)

    return feed_info


def fetch_arxiv_page(
    *,
    search_query: str | None = None,
    id_list: list[str] | None = None,
    start: int = 0,
    max_results: int = 10,
    sort_by: str = "relevance",
    sort_order: str = "descending",
    timeout: int = 30,
) -> tuple[dict, str, str]:
    params = {
        "start": int(start),
        "max_results": int(max_results),
        "sortBy": sort_by,
        "sortOrder": sort_order,
    }
    if search_query:
        params["search_query"] = search_query
    if id_list:
        params["id_list"] = ",".join(id_list)

    response = requests.get(ARXIV_API_URL, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        guidance = "arXiv API returned 429 Too Many Requests. Wait before retrying and keep at least a 3 second delay between requests."
        if retry_after:
            guidance += f" Retry-After: {retry_after} second(s)."
        raise RuntimeError(guidance)
    response.raise_for_status()
    xml_text = response.text
    parsed = parse_arxiv_atom(xml_text)
    canonical_url = response.url
    return parsed, xml_text, canonical_url


def fetch_arxiv_snapshot(
    *,
    search_query: str | None = None,
    id_list: list[str] | None = None,
    start: int = 0,
    max_results: int = 10,
    page_size: int = 10,
    sort_by: str = "relevance",
    sort_order: str = "descending",
    request_delay: float = 3.0,
) -> dict:
    if not search_query and not id_list:
        raise ValueError("Either search_query or id_list must be provided")

    snapshots = []
    total_entries = []
    raw_pages = []

    if id_list:
        batches = chunked(id_list, max(1, min(int(page_size), 50)))
        for index, batch in enumerate(batches):
            parsed, xml_text, canonical_url = fetch_arxiv_page(
                id_list=batch,
                start=0,
                max_results=len(batch),
                sort_by=sort_by,
                sort_order=sort_order,
            )
            snapshots.append(parsed)
            raw_pages.append({"canonical_url": canonical_url, "xml": xml_text})
            total_entries.extend(parsed["entries"])
            if index < len(batches) - 1:
                time.sleep(request_delay)
    else:
        fetched = 0
        page_size = max(1, min(int(page_size), 2000))
        while fetched < int(max_results):
            current_size = min(page_size, int(max_results) - fetched)
            parsed, xml_text, canonical_url = fetch_arxiv_page(
                search_query=search_query,
                start=int(start) + fetched,
                max_results=current_size,
                sort_by=sort_by,
                sort_order=sort_order,
            )
            snapshots.append(parsed)
            raw_pages.append({"canonical_url": canonical_url, "xml": xml_text})
            total_entries.extend(parsed["entries"])
            fetched += len(parsed["entries"])
            total_available = parsed["total_results"]
            if not parsed["entries"] or fetched >= min(int(max_results), total_available):
                break
            time.sleep(request_delay)

    deduped = []
    seen = set()
    for entry in total_entries:
        key = entry["versioned_id"] or entry["id"]
        if key in seen:
            continue
        deduped.append(entry)
        seen.add(key)

    first_snapshot = snapshots[0] if snapshots else {"total_results": 0, "title": None, "updated": None}
    return {
        "source": "arXiv API",
        "fetched_at": iso_timestamp(),
        "acknowledgement": ARXIV_ACKNOWLEDGEMENT,
        "request": {
            "search_query": search_query,
            "id_list": id_list or [],
            "start": int(start),
            "max_results": int(max_results),
            "page_size": int(page_size),
            "sort_by": sort_by,
            "sort_order": sort_order,
            "request_delay_seconds": request_delay,
        },
        "feed": {
            "title": first_snapshot.get("title"),
            "id": first_snapshot.get("id"),
            "updated": first_snapshot.get("updated"),
            "total_results": first_snapshot.get("total_results", 0),
            "pages_fetched": len(raw_pages),
        },
        "entries": deduped,
        "raw_pages": raw_pages,
    }


def build_arxiv_snapshot_bundle(
    snapshot: dict,
    *,
    supports_hypothesis: str | None = None,
    hypothesis_focus: str | None = None,
    allow_new_hypothesis: bool = True,
) -> dict:
    entries = snapshot.get("entries", [])
    primary_categories = sorted({entry.get("primary_category") for entry in entries if entry.get("primary_category")})
    authors = sorted({author.get("name") for entry in entries for author in entry.get("authors", []) if author.get("name")})
    titles = [entry.get("title") for entry in entries if entry.get("title")]
    query = snapshot.get("request", {}).get("search_query")
    id_list = snapshot.get("request", {}).get("id_list") or []

    summary_target = query or (", ".join(id_list[:3]) if id_list else "arXiv records")
    summary = f"Structured arXiv snapshot bundle covering {len(entries)} paper(s) for {summary_target}."
    anomalies = []
    if any(entry.get("doi") is None for entry in entries):
        anomalies.append("Some arXiv records lack DOI metadata, so downstream evidence may depend on arXiv-only provenance.")
    if any(entry.get("journal_ref") is None for entry in entries):
        anomalies.append("Some arXiv records lack journal references, so publication status should be treated as incomplete unless separately verified.")

    significance = 0.55
    if query:
        significance += 0.05
    if id_list:
        significance += 0.08
    if len(entries) >= 3:
        significance += 0.07
    if primary_categories:
        significance += 0.05
    significance = min(round(significance, 3), 0.85)

    domain_tags = []
    joined = " ".join(filter(None, [query or "", " ".join(primary_categories), " ".join(titles[:5])])).lower()
    if any(token in joined for token in ("pulsar", "astro-ph", "gravitational", "neutron star", "vela")):
        domain_tags.append("pulsars")
    if any(token in joined for token in ("black hole", "event horizon", "hawking")):
        domain_tags.append("black_holes")
    if any(token in joined for token in ("galaxy", "cosmology", "cmb", "dark flow")):
        domain_tags.append("cosmology")

    structured_evidence = {
        "query": snapshot.get("request", {}).get("search_query"),
        "id_list": id_list,
        "feed": snapshot.get("feed", {}),
        "entries": entries,
        "primary_categories": primary_categories,
        "authors": authors[:25],
        "raw_page_count": len(snapshot.get("raw_pages", [])),
    }

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "arxiv_snapshot_bundle",
        "summary": summary,
        "entities": [entity for entity in [summary_target, *(titles[:3])] if entity],
        "topics": ["arXiv literature", "metadata snapshot", *(primary_categories[:5])],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": sorted(set(domain_tags)),
        "source_catalogs": ["arXiv API", "https://info.arxiv.org/help/api/index.html"],
        "target": {
            "name": summary_target,
            "input_target": summary_target,
            "category": primary_categories[0] if primary_categories else None,
        },
        "structured_evidence": structured_evidence,
        "new_hypothesis": None if (supports_hypothesis or not allow_new_hypothesis) else {
            "title": f"arXiv snapshot: {summary_target}",
            "body": " ".join([
                summary,
                f"The snapshot includes {len(entries)} paper(s) across categories {', '.join(primary_categories[:5]) or 'unspecified'}.",
                "Use it as evidence discovery material rather than autonomous acceptance.",
            ]),
            "confidence": 0.58,
            "predictions": [
                "Subsequent literature snapshots for the same query should either stabilize the topic cluster or reveal contradicting categories and abstracts.",
                "Exact-id snapshots should remain reproducible for the same versioned arXiv identifiers.",
            ],
        },
        "manatuabon_context": {
            "hypothesis_focus": hypothesis_focus,
            "acknowledgement": ARXIV_ACKNOWLEDGEMENT,
            "raw_pages": [page["canonical_url"] for page in snapshot.get("raw_pages", [])],
        },
    }


def write_arxiv_snapshot_files(snapshot: dict, bundle: dict, output_dir: Path, label: str) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = sanitize_filename(label)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_path = output_dir / f"arxiv_snapshot_raw_{safe_label}_{stamp}.json"
    raw_tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with open(raw_tmp, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
    raw_tmp.replace(raw_path)

    bundle_json, bundle_md = write_bundle(bundle, output_dir, label, filename_prefix="arxiv_snapshot_bundle")
    return raw_path, bundle_json, bundle_md


def ingest_arxiv_bundle(bundle_path: Path, *, db_path: Path, agent_log_path: Path) -> dict:
    ensure_runtime_db(db_path, migrate=False).close()
    memory = MemoryManager(db_path)
    agent = IngestAgent(StructuredBundleOnlyNemotron(), memory, AgentLog(agent_log_path))
    result = agent.ingest_file(bundle_path)
    if result is None:
        raise RuntimeError(f"Structured ingest returned no memory for {bundle_path.name}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch arXiv metadata snapshots and write Manatuabon structured bundles.")
    parser.add_argument("--query", default=None, help="arXiv search_query expression, e.g. cat:astro-ph.HE+AND+all:vela")
    parser.add_argument("--id-list", default=None, help="Comma-delimited arXiv IDs or versioned IDs")
    parser.add_argument("--start", type=int, default=0, help="Result start index for search mode")
    parser.add_argument("--max-results", type=int, default=10, help="Maximum number of results to fetch")
    parser.add_argument("--page-size", type=int, default=10, help="Page size per request; capped by arXiv API limits")
    parser.add_argument("--sort-by", default="relevance", choices=["relevance", "lastUpdatedDate", "submittedDate"], help="arXiv sortBy parameter")
    parser.add_argument("--sort-order", default="descending", choices=["ascending", "descending"], help="arXiv sortOrder parameter")
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
    id_list = [item.strip() for item in (args.id_list or "").split(",") if item.strip()]
    snapshot = fetch_arxiv_snapshot(
        search_query=args.query,
        id_list=id_list or None,
        start=args.start,
        max_results=args.max_results,
        page_size=args.page_size,
        sort_by=args.sort_by,
        sort_order=args.sort_order,
    )
    label = args.query or (id_list[0] if id_list else "arxiv")
    bundle = build_arxiv_snapshot_bundle(
        snapshot,
        supports_hypothesis=args.supports_hypothesis,
        hypothesis_focus=args.hypothesis_focus,
        allow_new_hypothesis=not args.evidence_only,
    )
    raw_path, bundle_json, bundle_md = write_arxiv_snapshot_files(snapshot, bundle, Path(args.inbox), label)
    print(f"Raw snapshot written: {raw_path}")
    print(f"Structured bundle written: {bundle_json}")
    print(f"Companion report written: {bundle_md}")
    if args.ingest:
        ingested = ingest_arxiv_bundle(
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