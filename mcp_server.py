"""
Manatuabon MCP Server — exposes agent memory, hypotheses, and council
as Model Context Protocol tools for any MCP-compatible client.

Runs over stdio (default) or SSE transport.
Usage:
    python mcp_server.py              # stdio transport (for IDE / Claude Desktop)
    python mcp_server.py --sse 8808   # SSE transport on port 8808
"""

import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Paths — resolved relative to this file so it works from any cwd
# ---------------------------------------------------------------------------
_BASE_DIR = Path(__file__).resolve().parent
_DB_PATH = _BASE_DIR / "manatuabon.db"

log = logging.getLogger("manatuabon.mcp")

# ---------------------------------------------------------------------------
# Lightweight DB helpers (read-only where possible, no MemoryManager import
# needed — keeps the MCP server self-contained and avoids circular deps)
# ---------------------------------------------------------------------------

def _conn():
    """Open a read-only SQLite connection with Row factory."""
    conn = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_dicts(rows):
    return [dict(r) for r in rows]


def _parse_json_field(value, default=None):
    if default is None:
        default = []
    if not value:
        return default
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, (list, dict)) else default
    except (json.JSONDecodeError, TypeError):
        return default


def _clamp(val, lo=0.0, hi=1.0, default=0.5):
    try:
        return round(min(max(float(val), lo), hi), 3)
    except (TypeError, ValueError):
        return default


def _confidence_label(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def _safe_int(val, default=0):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "Manatuabon",
    instructions=(
        "Manatuabon is an offline-first astrophysics RAG observatory. "
        "Use these tools to search agent memories, inspect hypotheses, "
        "review council decisions, and query evidence links. "
        "All confidence scores are 0.0–1.0. Domain tags include: "
        "sgra, pulsars, gravitational_waves, exoplanets, cosmology, etc. "
        "Treat retrieval results as probabilistic evidence, not ground truth."
    ),
)

# ── Memory Tools ──────────────────────────────────────────────────────────

@mcp.tool()
def search_memories(
    query: str,
    domain: str | None = None,
    limit: int = 20,
) -> str:
    """Search agent memories by keyword.  Returns matching memories sorted by
    recency.  Optionally filter by domain tag (e.g. 'sgra', 'pulsars').

    Use this as the primary retrieval tool — always check memory evidence
    before generating claims about astrophysics data.
    """
    limit = min(max(limit, 1), 100)
    with _conn() as conn:
        # FTS is not available; use LIKE-based keyword search across content
        words = [w.strip() for w in query.split() if w.strip()]
        if not words:
            return json.dumps({"error": "Empty query"})

        conditions = ["(" + " OR ".join(
            ["content LIKE ?"] * len(words)
        ) + ")"]
        params: list = [f"%{w}%" for w in words]

        if domain:
            conditions.append("domain_tags LIKE ?")
            params.append(f'%"{domain}"%')

        sql = (
            "SELECT id, timestamp, content, concept_tags, significance, "
            "domain_tags, supports_hypothesis, challenges_hypothesis "
            f"FROM memories WHERE {' AND '.join(conditions)} "
            "ORDER BY id DESC LIMIT ?"
        )
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()

    results = []
    for r in rows:
        results.append({
            "id": r["id"],
            "timestamp": r["timestamp"],
            "summary": r["content"],
            "entities": _parse_json_field(r["concept_tags"]),
            "domain_tags": _parse_json_field(r["domain_tags"]),
            "confidence": _clamp(r["significance"]),
            "confidence_label": _confidence_label(_clamp(r["significance"])),
            "supports_hypothesis": r["supports_hypothesis"],
            "challenges_hypothesis": r["challenges_hypothesis"],
        })

    return json.dumps({
        "query": query,
        "domain_filter": domain,
        "count": len(results),
        "memories": results,
    }, default=str)


@mcp.tool()
def get_memory(memory_id: int) -> str:
    """Retrieve a single memory by ID with full detail."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT id, timestamp, content, concept_tags, significance, "
            "domain_tags, supports_hypothesis, challenges_hypothesis "
            "FROM memories WHERE id=?",
            (memory_id,),
        ).fetchone()

    if not row:
        return json.dumps({"error": f"Memory #{memory_id} not found"})

    return json.dumps({
        "id": row["id"],
        "timestamp": row["timestamp"],
        "summary": row["content"],
        "entities": _parse_json_field(row["concept_tags"]),
        "domain_tags": _parse_json_field(row["domain_tags"]),
        "confidence": _clamp(row["significance"]),
        "confidence_label": _confidence_label(_clamp(row["significance"])),
        "supports_hypothesis": row["supports_hypothesis"],
        "challenges_hypothesis": row["challenges_hypothesis"],
    }, default=str)


@mcp.tool()
def list_memories(
    domain: str | None = None,
    linked_to_hypothesis: str | None = None,
    min_confidence: float = 0.0,
    limit: int = 50,
) -> str:
    """List recent agent memories with optional filters.

    - domain: filter by domain tag (e.g. 'sgra', 'pulsars')
    - linked_to_hypothesis: only memories linked to this hypothesis ID
    - min_confidence: minimum confidence threshold (0.0–1.0)
    - limit: max results (1–100)
    """
    limit = min(max(limit, 1), 100)
    conditions = []
    params: list = []

    if domain:
        conditions.append("domain_tags LIKE ?")
        params.append(f'%"{domain}"%')

    if linked_to_hypothesis:
        conditions.append("(supports_hypothesis=? OR challenges_hypothesis=?)")
        params.extend([linked_to_hypothesis, linked_to_hypothesis])

    if min_confidence > 0.0:
        conditions.append("significance >= ?")
        params.append(min_confidence)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT id, timestamp, content, concept_tags, significance, "
            f"domain_tags, supports_hypothesis, challenges_hypothesis "
            f"FROM memories {where} ORDER BY id DESC LIMIT ?",
            params + [limit],
        ).fetchall()

    results = []
    for r in rows:
        results.append({
            "id": r["id"],
            "timestamp": r["timestamp"],
            "summary": (r["content"] or "")[:200],
            "domain_tags": _parse_json_field(r["domain_tags"]),
            "confidence": _clamp(r["significance"]),
            "confidence_label": _confidence_label(_clamp(r["significance"])),
            "supports_hypothesis": r["supports_hypothesis"],
            "challenges_hypothesis": r["challenges_hypothesis"],
        })

    return json.dumps({"count": len(results), "memories": results}, default=str)


# ── Hypothesis Tools ──────────────────────────────────────────────────────

@mcp.tool()
def list_hypotheses(
    status: str | None = None,
    origin: str | None = None,
    domain: str | None = None,
    limit: int = 30,
) -> str:
    """List hypotheses in the observatory.

    - status: filter by 'active', 'proposed', 'held', 'accepted', 'rejected', 'merged'
    - origin: 'founding' (human-created) or 'agent_auto' (AI-generated)
    - domain: filter by context domain tag
    - limit: max results (1–100)
    """
    limit = min(max(limit, 1), 100)
    conditions = []
    params: list = []

    if status:
        conditions.append("status=?")
        params.append(status)
    if origin:
        conditions.append("origin=?")
        params.append(origin)
    if domain:
        conditions.append("context_domains LIKE ?")
        params.append(f'%"{domain}"%')

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT id, title, description, status, confidence, "
            f"confidence_components, confidence_source, origin, "
            f"parent_id, root_id, merged_into, context_domains, "
            f"created_at, updated_at "
            f"FROM hypotheses {where} ORDER BY updated_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()

    results = []
    for r in rows:
        conf = _clamp(r["confidence"])
        components = _parse_json_field(r["confidence_components"], {})
        results.append({
            "id": r["id"],
            "title": r["title"],
            "text": (r["description"] or "")[:300],
            "status": r["status"],
            "confidence": conf,
            "confidence_label": _confidence_label(conf),
            "origin": r["origin"],
            "root_id": r["root_id"],
            "merged_into": r["merged_into"],
            "context_domains": _parse_json_field(r["context_domains"]),
            "support_count": _safe_int(components.get("support_count")),
            "contradiction_count": _safe_int(components.get("contradiction_count")),
            "updated_at": r["updated_at"],
        })

    return json.dumps({"count": len(results), "hypotheses": results}, default=str)


@mcp.tool()
def get_hypothesis(hypothesis_id: str) -> str:
    """Get full detail for a single hypothesis including evidence and confidence
    components."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM hypotheses WHERE id=?",
            (hypothesis_id,),
        ).fetchone()

    if not row:
        return json.dumps({"error": f"Hypothesis {hypothesis_id} not found"})

    d = dict(row)
    conf = _clamp(d.get("confidence"))
    return json.dumps({
        "id": d["id"],
        "title": d["title"],
        "text": d.get("description"),
        "status": d["status"],
        "confidence": conf,
        "confidence_label": _confidence_label(conf),
        "confidence_source": d.get("confidence_source"),
        "confidence_components": _parse_json_field(d.get("confidence_components"), {}),
        "origin": d.get("origin"),
        "parent_id": d.get("parent_id"),
        "root_id": d.get("root_id"),
        "merged_into": d.get("merged_into"),
        "evidence": _parse_json_field(d.get("evidence")),
        "tags": _parse_json_field(d.get("tags")),
        "context_domains": _parse_json_field(d.get("context_domains")),
        "context_hypotheses": _parse_json_field(d.get("context_hypotheses")),
        "created_at": d.get("created_at"),
        "updated_at": d.get("updated_at"),
    }, default=str)


# ── Council Tools ─────────────────────────────────────────────────────────

@mcp.tool()
def get_council_decision(hypothesis_id: str) -> str:
    """Get the council's decision for a hypothesis including score breakdown
    and reasoning.  Returns null fields if no decision exists yet."""
    with _conn() as conn:
        decision = conn.execute(
            "SELECT * FROM hypothesis_decisions WHERE hypothesis_id=? "
            "ORDER BY id DESC LIMIT 1",
            (hypothesis_id,),
        ).fetchone()

    if not decision:
        return json.dumps({
            "hypothesis_id": hypothesis_id,
            "has_decision": False,
            "message": "No council decision found. This hypothesis may predate council activation.",
        })

    d = dict(decision)
    return json.dumps({
        "hypothesis_id": hypothesis_id,
        "has_decision": True,
        "decision": d["decision"],
        "final_score": _clamp(d.get("final_score")),
        "score_breakdown": _parse_json_field(d.get("score_breakdown"), {}),
        "merged_with": d.get("merged_with"),
        "reasoning": d.get("reasoning"),
        "timestamp": d.get("timestamp"),
    }, default=str)


@mcp.tool()
def get_council_reviews(hypothesis_id: str) -> str:
    """Get all individual council member reviews for a hypothesis.
    Each review includes the agent name, verdict, reasoning, objections,
    and score contributions."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM hypothesis_reviews WHERE hypothesis_id=? ORDER BY id",
            (hypothesis_id,),
        ).fetchall()

    reviews = []
    for r in rows:
        d = dict(r)
        reviews.append({
            "agent": d["agent_name"],
            "verdict": d.get("verdict"),
            "reasoning": d.get("reasoning"),
            "objections": _parse_json_field(d.get("objections")),
            "score_contributions": _parse_json_field(d.get("score_contributions"), {}),
            "details": _parse_json_field(d.get("review_details"), {}),
            "timestamp": d.get("timestamp"),
        })

    return json.dumps({
        "hypothesis_id": hypothesis_id,
        "count": len(reviews),
        "reviews": reviews,
    }, default=str)


# ── Evidence Tools ────────────────────────────────────────────────────────

@mcp.tool()
def list_evidence_requests(
    status: str = "pending",
    hypothesis_id: str | None = None,
    limit: int = 50,
) -> str:
    """List evidence requests — follow-up tasks the council needs answered.

    - status: 'pending', 'completed', 'dismissed', or 'all'
    - hypothesis_id: filter to a specific hypothesis
    - limit: max results (1–100)
    """
    limit = min(max(limit, 1), 100)
    conditions = []
    params: list = []

    if status and status != "all":
        conditions.append("er.status=?")
        params.append(status)
    if hypothesis_id:
        conditions.append("er.hypothesis_id=?")
        params.append(hypothesis_id)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT er.*, h.title AS hypothesis_title, h.status AS hypothesis_status "
            f"FROM evidence_requests er "
            f"LEFT JOIN hypotheses h ON er.hypothesis_id = h.id "
            f"{where} ORDER BY er.id DESC LIMIT ?",
            params + [limit],
        ).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        results.append({
            "id": d["id"],
            "hypothesis_id": d["hypothesis_id"],
            "hypothesis_title": d.get("hypothesis_title"),
            "request_text": d["request_text"],
            "priority": d.get("priority", "medium"),
            "source_agent": d.get("source_agent"),
            "status": d["status"],
            "triggering_decision": d.get("triggering_decision"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
        })

    return json.dumps({"count": len(results), "evidence_requests": results}, default=str)


@mcp.tool()
def list_memory_link_proposals(
    status: str = "pending",
    relation: str | None = None,
    domain: str | None = None,
    limit: int = 30,
) -> str:
    """List memory-hypothesis link proposals awaiting human review.

    - status: 'pending', 'approved', 'rejected', or 'all'
    - relation: 'support' or 'challenge'
    - domain: filter by memory domain tag
    - limit: max results (1–100)
    """
    limit = min(max(limit, 1), 100)
    conditions = []
    params: list = []

    if status and status != "all":
        conditions.append("mlp.status=?")
        params.append(status)
    if relation:
        conditions.append("mlp.relation=?")
        params.append(relation)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT mlp.*, m.content AS memory_summary, m.domain_tags AS memory_domains, "
            f"m.significance AS memory_confidence, h.title AS hypothesis_title "
            f"FROM memory_link_proposals mlp "
            f"LEFT JOIN memories m ON mlp.memory_id = m.id "
            f"LEFT JOIN hypotheses h ON mlp.hypothesis_id = h.id "
            f"{where} ORDER BY mlp.score DESC LIMIT ?",
            params + [limit],
        ).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        mem_domains = _parse_json_field(d.get("memory_domains"))
        # Apply domain post-filter (domain_tags is JSON inside the memories table)
        if domain and domain not in mem_domains:
            continue
        results.append({
            "id": d["id"],
            "memory_id": d["memory_id"],
            "hypothesis_id": d["hypothesis_id"],
            "hypothesis_title": d.get("hypothesis_title"),
            "relation": d["relation"],
            "score": round(float(d.get("score", 0)), 2),
            "rationale": d.get("rationale"),
            "status": d["status"],
            "memory_summary": (d.get("memory_summary") or "")[:200],
            "memory_domains": mem_domains,
            "memory_confidence": _clamp(d.get("memory_confidence")),
        })

    return json.dumps({"count": len(results), "proposals": results}, default=str)


# ── Summary / Stats ───────────────────────────────────────────────────────

@mcp.tool()
def observatory_stats() -> str:
    """Get a high-level summary of the Manatuabon observatory state:
    total memories, hypotheses, council decisions, pending proposals, etc."""
    with _conn() as conn:
        mem_count = conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]
        hyp_count = conn.execute("SELECT COUNT(*) FROM hypotheses").fetchone()[0]
        active_hyp = conn.execute(
            "SELECT COUNT(*) FROM hypotheses WHERE status NOT IN ('archived','merged','rejected','rejected_auto')"
        ).fetchone()[0]
        decision_count = conn.execute("SELECT COUNT(*) FROM hypothesis_decisions").fetchone()[0]
        pending_proposals = conn.execute(
            "SELECT COUNT(*) FROM memory_link_proposals WHERE status='pending'"
        ).fetchone()[0]
        pending_evidence = conn.execute(
            "SELECT COUNT(*) FROM evidence_requests WHERE status='pending'"
        ).fetchone()[0]

    return json.dumps({
        "total_memories": mem_count,
        "total_hypotheses": hyp_count,
        "active_hypotheses": active_hyp,
        "council_decisions": decision_count,
        "pending_link_proposals": pending_proposals,
        "pending_evidence_requests": pending_evidence,
    })


# ── Resources ─────────────────────────────────────────────────────────────

@mcp.resource("manatuabon://observatory/stats")
def resource_stats() -> str:
    """Live observatory statistics as a readable resource."""
    return observatory_stats()


# ── Entrypoint ────────────────────────────────────────────────────────────

def main():
    transport = "stdio"
    port = None

    if "--sse" in sys.argv:
        transport = "sse"
        idx = sys.argv.index("--sse")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])
        else:
            port = 8808

    if transport == "sse":
        import uvicorn
        log.info("MCP server starting on SSE transport, port %d", port)
        uvicorn.run(mcp.sse_app(), host="127.0.0.1", port=port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
