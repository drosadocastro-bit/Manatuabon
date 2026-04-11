"""
MANATUABON BRIDGE — HTTP API for the Agent
=============================================
Exposes the agent's brain to the HTML frontend.
Runs on port 7777 (default), started by manatuabon_agent.py.

Endpoints:
  GET  /status       → agent health + memory count
  GET  /memories     → all agent memories
  GET  /query?q=...  → Nemotron query with memory context
  POST /ingest       → manual text ingest { text, source }
  GET  /hypotheses   → auto-generated hypotheses
  GET  /agent_log    → last 50 activities
  POST /consolidate  → trigger immediate consolidation (Phase 8C)
"""

import json, logging
from pathlib import Path
from aiohttp import web
import aiohttp as aiohttp_client

log = logging.getLogger("manatuabon.bridge")

# These are injected by manatuabon_agent.py at startup
_ingest_agent = None
_memory = None
_agent_log = None
_nemotron = None
_start_time = None
_consolidate_agent = None
_BASE_DIR = Path(__file__).resolve().parent


def clamp_confidence(value, default=0.5):
    try:
        return round(min(max(float(value), 0.0), 1.0), 3)
    except (TypeError, ValueError):
        return default


def confidence_label(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def build_query_confidence(referenced_memories: list, response_text: str) -> dict:
    import re

    sentences = max(1, len([part for part in re.split(r"[.!?]+", response_text or "") if part.strip()]))
    citation_mentions = len(re.findall(r"(?:\[?Memory\s*#(\d+)\]?|#(\d+))", response_text or "", flags=re.IGNORECASE))
    citation_density = min(citation_mentions / sentences, 1.0)

    if not referenced_memories:
        unsupported_synthesis_penalty = 0.18 if len((response_text or "").split()) > 40 else 0.08
        score = clamp_confidence((0.26 if response_text else 0.0) - unsupported_synthesis_penalty)
        return {
            "score": score,
            "label": confidence_label(score),
            "reason": "No specific memories were cited in the answer.",
            "factors": {
                "referenced_memories": 0,
                "average_memory_confidence": 0.0,
                "citation_density": 0.0,
                "fallback_penalty": 1.0 if not response_text else 0.0,
                "unsupported_synthesis_penalty": unsupported_synthesis_penalty,
            },
        }

    avg_memory_confidence = sum(clamp_confidence(m.get("confidence", 0.5)) for m in referenced_memories) / len(referenced_memories)
    coverage_score = min(len(referenced_memories) / 3, 1.0)
    cited_with_ids = 1.0 if citation_mentions else 0.0
    low_quality_penalty = 0.15 if any("AI analysis unavailable" in m.get("summary", "") for m in referenced_memories) else 0.0
    unsupported_synthesis_penalty = 0.12 if citation_density < 0.34 and len((response_text or "").split()) > 80 else 0.0

    score = clamp_confidence(
        0.1
        + 0.42 * avg_memory_confidence
        + 0.2 * coverage_score
        + 0.18 * citation_density
        + 0.1 * cited_with_ids
        - low_quality_penalty
        - unsupported_synthesis_penalty
    )
    reason = f"Based on {len(referenced_memories)} cited memorie(s) with average evidence confidence {avg_memory_confidence:.2f} and citation density {citation_density:.2f}."
    if low_quality_penalty:
        reason += " Penalized because at least one cited memory lacks AI extraction."
    if unsupported_synthesis_penalty:
        reason += " Penalized because the answer makes broad claims with too few citations per sentence."

    return {
        "score": score,
        "label": confidence_label(score),
        "reason": reason,
        "factors": {
            "referenced_memories": len(referenced_memories),
            "average_memory_confidence": round(avg_memory_confidence, 3),
            "coverage_score": round(coverage_score, 3),
            "citation_density": round(citation_density, 3),
            "citation_mentions": citation_mentions,
            "low_quality_penalty": low_quality_penalty,
            "unsupported_synthesis_penalty": unsupported_synthesis_penalty,
        },
    }


def build_dialogue_query(prompt: str, messages: list | None = None) -> str:
    if not messages:
        return prompt

    transcript = []
    for entry in messages[-8:]:
        role = (entry.get("role") or "user").strip().lower()
        content = (entry.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        transcript.append(f"{role.title()}: {content}")

    if not transcript:
        return prompt

    return (
        "Use the following recent journal context only to preserve conversational continuity. "
        "Ground every factual claim in the memory bank, not in unsupported prior chat.\n\n"
        "Recent journal context:\n"
        + "\n".join(transcript)
        + f"\n\nCurrent request:\n{prompt}"
    )


def build_agent_context():
    memories = _memory.get_memories()
    hypotheses = _memory.get_founding_hypotheses()
    auto_hyps = _memory.get_auto_hypotheses()

    mem_context = "\n".join(
        f"Memory #{m['id']} ({m['timestamp'][:10]}): {m['summary']}"
        for m in memories[-20:]
    ) if memories else "No agent memories yet."

    hyp_context = "\n".join(
        f"- {h.get('id', '?')}: {h['title']} — {h.get('desc', h.get('body', ''))[:100]}"
        for h in hypotheses + auto_hyps
    ) if (hypotheses or auto_hyps) else "No hypotheses."

    system_prompt = f"""You are MANATUABON, an astrophysics intelligence built by Danny from Bayamón, Puerto Rico.
You have access to a memory bank of observations, analyses, and hypotheses.
Answer Danny's question using your memory context. Cite every factual claim you rely on with exact memory references in the form [Memory #ID]. If the memory bank is insufficient, say that explicitly instead of guessing.

MEMORY BANK (recent):
{mem_context}

HYPOTHESES:
{hyp_context}"""

    return system_prompt


def extract_referenced_memories(response_text: str) -> list[int]:
    import re

    return sorted({
        int(match)
        for groups in re.findall(r"(?:\[?Memory\s*#(\d+)\]?|#(\d+))", response_text or "", flags=re.IGNORECASE)
        for match in groups
        if match
    })


def extract_cloud_text(data: dict) -> str:
    content = data.get("content") or []
    chunks = []
    for item in content:
        if item.get("type") == "text" and item.get("text"):
            chunks.append(item["text"])
    return "\n".join(chunks).strip()


# Trusted origins for CORS (localhost variants used by the HTML frontends)
_ALLOWED_ORIGINS = {
    "http://127.0.0.1:8765", "http://localhost:8765",
    "http://127.0.0.1:8766", "http://localhost:8766",
    "http://127.0.0.1:7777", "http://localhost:7777",
    "null",  # file:// origin sent by browsers opening local .html files
}


def cors_headers(origin: str = ""):
    allowed = origin if origin in _ALLOWED_ORIGINS else ""
    return {
        "Access-Control-Allow-Origin": allowed,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }


@web.middleware
async def cors_middleware(request, handler):
    """Inject CORS headers into every response based on the request Origin."""
    origin = request.headers.get("Origin", "")
    resp = await handler(request)
    for key, value in cors_headers(origin).items():
        resp.headers[key] = value
    return resp


def safe_limit(value, default: int = 50, max_limit: int = 1000) -> int:
    """Clamp a user-supplied limit to [1, max_limit]."""
    try:
        return max(1, min(int(value), max_limit))
    except (ValueError, TypeError):
        return default


def json_response(data, status=200):
    return web.json_response(data, status=status)


def get_council():
    council = getattr(_consolidate_agent, "council", None)
    if council:
        return council
    return getattr(_ingest_agent, "council", None)


def build_governance_diagnostics():
    council = get_council()
    override_summary = _memory.get_override_summary() if _memory else {"total": 0, "latest": None}
    evidence_summary = _memory.get_evidence_request_summary() if _memory else {"pending": 0, "total": 0}
    return {
        "charter_present": (_BASE_DIR / "GOVERNANCE.md").exists(),
        "decision_policy_present": (_BASE_DIR / "DECISION_POLICY.md").exists(),
        "change_policy_present": (_BASE_DIR / "CHANGE_POLICY.md").exists(),
        "risk_review_present": (_BASE_DIR / "GOVERNANCE_RISK_REVIEW.md").exists(),
        "council_active": bool(council),
        "council_graph_mode": getattr(council, "council_graph_mode", "off") if council else "off",
        "evidence_review_active": bool(council and getattr(council, "evidence_reviewer", None)),
        "held_rereview_gate_active": bool(council and hasattr(council, "_material_evidence_for_rereview")),
        "held_rereview_graph_active": bool(council and hasattr(council, "_get_held_rereview_graph_runner")),
        "quant_review_active": bool(council and getattr(council, "quant_reviewer", None)),
        "reflection_advisory_active": bool(council and getattr(council, "reflection", None)),
        "evidence_request_graph_active": bool(council and hasattr(council, "evaluate_evidence_request_closure")),
        "manual_link_review_active": True,
        "pending_evidence_requests": evidence_summary.get("pending", 0),
        "override_rationale_required": True,
        "override_audit_entries": override_summary.get("total", 0),
        "last_override": override_summary.get("latest"),
    }


# ─── HANDLERS ────────────────────────────────────────────────────────

async def handle_options(request):
    """Handle CORS preflight requests."""
    origin = request.headers.get("Origin", "")
    return web.Response(status=204, headers=cors_headers(origin))


async def handle_status(request):
    """GET /status → agent health."""
    stats = _memory.get_stats()
    logs = _agent_log.recent(5)
    last_ingest = None
    for entry in reversed(logs):
        if entry["action"] in ("ingest_complete", "manual_ingest"):
            last_ingest = entry["timestamp"]
            break

    return json_response({
        "running": True,
        "memories": stats["total_memories"],
        "auto_hypotheses": stats["total_auto_hypotheses"],
        "founding_hypotheses": stats["founding_hypotheses"],
        "last_ingest": last_ingest,
        "uptime_since": _start_time,
        "governance": build_governance_diagnostics(),
    })


async def handle_memories(request):
    """GET /memories → all agent memories."""
    memories = _memory.get_memories()
    return json_response(memories)


async def handle_get_memory_link_proposals(request):
    """GET /api/memory-link-proposals → proposal queue for manual review."""
    try:
        proposals = _memory.get_memory_link_proposals(
            status=request.query.get("status", "pending"),
            domain=request.query.get("domain") or None,
            relation=request.query.get("relation") or None,
            limit=safe_limit(request.query.get("limit", "50")),
        )
        return json_response(proposals)
    except Exception as e:
        log.error("memory-link-proposals error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_get_evidence_requests(request):
    """GET /api/evidence-requests → structured follow-up tasks for held hypotheses."""
    try:
        council = get_council()
        if council and hasattr(council, "evaluate_evidence_request_closure"):
            closure_result = council.evaluate_evidence_request_closure(
                hypothesis_id=request.query.get("hypothesis_id") or None,
                limit=safe_limit(request.query.get("limit", "120"), default=120),
            )
            requests_payload = closure_result.get("evaluated", [])
        else:
            requests_payload = _memory.get_evidence_requests(
                status=request.query.get("status", "pending"),
                hypothesis_id=request.query.get("hypothesis_id") or None,
                limit=safe_limit(request.query.get("limit", "120"), default=120),
            )
        return json_response(requests_payload)
    except Exception as e:
        log.error("evidence-requests error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_review_evidence_request(request):
    """POST /api/evidence-requests/review → complete or dismiss an evidence task."""
    try:
        body = await request.json()
        reviewed = _memory.review_evidence_request(
            int(body.get("request_id", 0)),
            body.get("decision", ""),
            resolution_note=(body.get("resolution_note") or "").strip(),
            satisfied_memory_ids=body.get("satisfied_memory_ids") or [],
        )
        if not reviewed:
            return json_response({"error": "Evidence request not found"}, status=404)
        return json_response(reviewed)
    except ValueError as e:
        return json_response({"error": "Invalid request parameters"}, status=400)
    except Exception as e:
        log.error("review-evidence-request error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_generate_memory_link_proposals(request):
    """POST /api/memory-link-proposals/generate → create bounded review proposals."""
    try:
        body = await request.json() if request.can_read_body else {}
        result = _memory.generate_memory_link_proposals(
            limit=safe_limit(body.get("limit", 20), default=20, max_limit=200),
            min_score=float(body.get("min_score", 2.5)),
            memory_domain=body.get("domain") or None,
            relation=body.get("relation") or None,
        )
        return json_response(result)
    except Exception as e:
        log.error("generate-memory-link-proposals error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_review_memory_link_proposal(request):
    """POST /api/memory-link-proposals/review → approve or reject a proposal."""
    try:
        body = await request.json()
        proposal_id = int(body.get("proposal_id", 0))
        decision = body.get("decision", "")
        if not proposal_id or not decision:
            return json_response({"error": "Missing proposal_id or decision"}, status=400)
        result = _memory.review_memory_link_proposal(
            proposal_id=proposal_id,
            decision=decision,
            reviewer_note=body.get("reviewer_note", ""),
        )
        if not result:
            return json_response({"error": "Proposal not found"}, status=404)
        return json_response(result)
    except ValueError as e:
        return json_response({"error": "Invalid request parameters"}, status=400)
    except Exception as e:
        log.error("review-memory-link-proposal error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_query(request):
    """GET/POST /query → Nemotron answers with memory context."""
    messages = []
    query = ""

    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            return json_response({"error": "Invalid JSON body"}, status=400)

        query = (body.get("prompt") or body.get("q") or "").strip()
        messages = body.get("messages") or []
        if not query and messages:
            for entry in reversed(messages):
                if (entry.get("role") or "").strip().lower() == "user":
                    query = (entry.get("content") or "").strip()
                    if query:
                        break
    else:
        query = request.query.get("q", "").strip()

    if not query:
        return json_response({"error": "Missing query text"}, status=400)

    system_prompt = build_agent_context()

    dialogue_query = build_dialogue_query(query, messages)
    response_text = _nemotron.chat(system_prompt, dialogue_query, temperature=0.5, max_tokens=2048)
    if not response_text:
        return json_response({
            "answer": "Could not reach Nemotron. Is LM Studio running?",
            "sources": [],
            "confidence": 0.0,
            "confidence_details": {
                "score": 0.0,
                "label": "low",
                "reason": "Local model did not return a response.",
                "factors": {
                    "referenced_memories": 0,
                    "average_memory_confidence": 0.0,
                    "coverage_score": 0.0,
                    "citation_density": 0.0,
                    "low_quality_penalty": 0.0,
                },
            },
        })

    referenced = extract_referenced_memories(response_text)
    referenced_memories = _memory.get_memories_by_ids(referenced)
    confidence = build_query_confidence(referenced_memories, response_text)

    return json_response({
        "answer": response_text,
        "sources": referenced,
        "confidence": confidence["score"],
        "confidence_details": confidence,
    })


async def handle_ingest(request):
    """POST /ingest → manual text ingest."""
    try:
        body = await request.json()
    except Exception:
        return json_response({"error": "Invalid JSON body"}, status=400)

    text = body.get("text", "").strip()
    source = body.get("source", "manual")
    if not text:
        return json_response({"error": "Missing 'text' field"}, status=400)

    memory_obj = _ingest_agent.ingest_text(text, source)
    if memory_obj:
        return json_response({
            "memory_id": memory_obj["id"],
            "summary": memory_obj["summary"],
            "entities": memory_obj["entities"],
            "confidence": memory_obj.get("confidence", memory_obj.get("importance", 0.5)),
            "confidence_label": confidence_label(memory_obj.get("confidence", memory_obj.get("importance", 0.5))),
            "confidence_reason": memory_obj.get("confidence_reason", "No confidence reason recorded."),
            "hypothesis": memory_obj.get("hypothesis_generated"),
        })
    return json_response({"error": "Ingest failed"}, status=500)


async def handle_hypotheses(request):
    """GET /hypotheses → auto-generated hypotheses."""
    hyps = _memory.get_auto_hypotheses()
    return json_response(hyps)


async def handle_all_hypotheses(request):
    """GET /api/hypotheses/all → canonical hypothesis loader with optional filters."""
    try:
        status = request.query.get("status") or None
        origin = request.query.get("origin") or None
        root_id = request.query.get("root_id") or None
        active_only = request.query.get("active_only", "false").lower() == "true"
        missing_decision_only = request.query.get("missing_decision", "false").lower() == "true"
        hypotheses = _memory.get_all_hypotheses(
            normalized=True,
            status=status,
            origin=origin,
            root_id=root_id,
            active_only=active_only,
        )
        for hypothesis in hypotheses:
            decision = _memory.get_decision_for_hypothesis(hypothesis["id"])
            hypothesis["has_council_decision"] = bool(decision)
            hypothesis["latest_decision"] = decision
        if missing_decision_only:
            hypotheses = [hypothesis for hypothesis in hypotheses if not hypothesis["has_council_decision"]]
        return json_response(hypotheses)
    except Exception as e:
        log.error("all-hypotheses error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_agent_log(request):
    """GET /agent_log → last 50 activities."""
    entries = _agent_log.recent(50)
    return json_response(entries)


async def handle_consolidate(request):
    """POST /consolidate → trigger immediate consolidation."""
    try:
        insight = _consolidate_agent.run()
        if insight:
            return json_response(insight)
        return json_response({"status": "no_insight_generated"}, status=204)
    except Exception as e:
        log.error("Manual consolidate error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_reject_hypothesis(request):
    """POST /hypotheses/reject → mark an auto-generated hypothesis as rejected."""
    try:
        body = await request.json()
        hyp_id = body.get("hyp_id")
        if not hyp_id and body.get("index") is not None:
            hyps = _memory.get_auto_hypotheses()
            hyp_idx = body.get("index")
            if 0 <= hyp_idx < len(hyps):
                hyp_id = hyps[hyp_idx]["id"]
        if not hyp_id:
            return json_response({"error": "Missing hyp_id"}, status=400)

        updated = _memory.set_auto_hypothesis_status(hyp_id, "rejected_auto")
        if not updated:
            return json_response({"error": "Hypothesis not found"}, status=404)

        _agent_log.add("hypothesis_rejected", f"Rejected: {updated['title']}")
        return json_response({"status": "ok", "hypothesis": updated})
    except Exception as e:
        log.error("reject-hypothesis error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_update_hypothesis_status(request):
    """POST /hypotheses/status → update an auto-generated hypothesis status by id."""
    try:
        body = await request.json()
        hyp_id = (body.get("hyp_id") or "").strip()
        new_status = (body.get("status") or "").strip()
        if not hyp_id or not new_status:
            return json_response({"error": "Missing hyp_id or status"}, status=400)

        updated = _memory.set_auto_hypothesis_status(hyp_id, new_status)
        if not updated:
            return json_response({"error": "Hypothesis not found"}, status=404)

        _agent_log.add("hypothesis_status_updated", f"{updated['title']} → {new_status}")
        return json_response({"status": "ok", "hypothesis": updated})
    except Exception as e:
        log.error("update-hypothesis-status error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_simulations(request):
    """GET /simulations → get all queued simulations."""
    queue = _memory.get_simulation_queue()
    return json_response(queue)

async def handle_dequeue_simulation(request):
    """POST /simulations/dequeue → Colab pulls next pending task."""
    try:
        task = _memory.dequeue_simulation()
        if task:
            return json_response(task)
        return json_response({"status": "empty"}, status=204)
    except Exception as e:
        log.error("dequeue-simulation error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_cloud_proxy(request):
    """POST /cloud → proxy Anthropic API calls from the browser.
    Body: { api_key: str, body: dict }
    This avoids CORS / mixed-content issues by making the call server-side.
    """
    try:
        payload = await request.json()
    except Exception:
        return json_response({"error": "Invalid JSON body"}, status=400)

    api_key = payload.get("api_key", "").strip()
    anthropic_body = payload.get("body")

    if not api_key:
        return json_response({"error": "Missing api_key"}, status=400)
    if not anthropic_body:
        return json_response({"error": "Missing body"}, status=400)

    anthropic_url = "https://api.anthropic.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }

    try:
        async with aiohttp_client.ClientSession() as session:
            async with session.post(anthropic_url, headers=headers,
                                    json=anthropic_body, timeout=aiohttp_client.ClientTimeout(total=60)) as resp:
                resp_body = await resp.text()
                return web.Response(
                    text=resp_body,
                    status=resp.status,
                    content_type="application/json",
                )
    except aiohttp_client.ClientError as e:
        log.error("Cloud proxy network error: %s", e)
        return json_response({"error": "Cloud proxy network error"}, status=502)
    except Exception as e:
        log.error("Cloud proxy error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_cloud_query(request):
    """POST /cloud/query → grounded Anthropic query with source/confidence scoring."""
    try:
        payload = await request.json()
    except Exception:
        return json_response({"error": "Invalid JSON body"}, status=400)

    api_key = (payload.get("api_key") or "").strip()
    prompt = (payload.get("prompt") or "").strip()
    messages = payload.get("messages") or []

    if not api_key:
        return json_response({"error": "Missing api_key"}, status=400)
    if not prompt and messages:
        for entry in reversed(messages):
            if (entry.get("role") or "").strip().lower() == "user":
                prompt = (entry.get("content") or "").strip()
                if prompt:
                    break
    if not prompt:
        return json_response({"error": "Missing prompt"}, status=400)

    system_prompt = build_agent_context()
    dialogue_query = build_dialogue_query(prompt, messages)

    anthropic_url = "https://api.anthropic.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    anthropic_body = {
        "model": payload.get("model") or "claude-sonnet-4-20250514",
        "max_tokens": int(payload.get("max_tokens") or 1000),
        "system": system_prompt,
        "messages": [{"role": "user", "content": dialogue_query}],
    }

    try:
        async with aiohttp_client.ClientSession() as session:
            async with session.post(
                anthropic_url,
                headers=headers,
                json=anthropic_body,
                timeout=aiohttp_client.ClientTimeout(total=90),
            ) as resp:
                resp_text = await resp.text()
                if resp.status >= 400:
                    return web.Response(
                        text=resp_text,
                        status=resp.status,
                        content_type="application/json",
                    )
                data = json.loads(resp_text)
    except aiohttp_client.ClientError as e:
        log.error("Cloud grounded query network error: %s", e)
        return json_response({"error": "Cloud query network error"}, status=502)
    except Exception as e:
        log.error("Cloud grounded query error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

    answer = extract_cloud_text(data)
    referenced = extract_referenced_memories(answer)
    referenced_memories = _memory.get_memories_by_ids(referenced)
    confidence = build_query_confidence(referenced_memories, answer)
    return json_response({
        "answer": answer,
        "sources": referenced,
        "confidence": confidence["score"],
        "confidence_details": confidence,
        "provider": "anthropic",
        "raw": data,
    })

# ─── PERSISTENT CHAT HISTORY HANDLERS ─────────────────────────────────

async def handle_get_chat(request):
    """GET /api/chat → fetch recent chat history."""
    try:
        limit = safe_limit(request.query.get("limit", "50"), default=50, max_limit=200)
        history = _memory.get_chat_history(limit=limit)
        return json_response(history)
    except Exception as e:
        log.error("get-chat error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_post_chat(request):
    """POST /api/chat → push a new message to SQL history."""
    try:
        body = await request.json()
        role = (body.get("role") or "").strip()
        content = (body.get("content") or "").strip()
        if not role or not content:
            return json_response({"error": "Missing role or content"}, status=400)
        _memory.add_chat_message(role, content, body.get("metadata"))
        return json_response({"status": "ok"})
    except ValueError as e:
        return json_response({"error": str(e)}, status=400)
    except Exception as e:
        log.error("post-chat error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

# ─── HYPOTHESIS REVIEW COUNCIL HANDLERS (Phase 18) ─────────────────

async def handle_council_decisions(request):
    """GET /api/council/decisions → all decisions, optional ?status= filter."""
    try:
        status_filter = request.query.get("status", None)
        decisions = _memory.get_all_decisions(status_filter=status_filter)
        return json_response(decisions)
    except Exception as e:
        log.error("council-decisions error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_council_reviews(request):
    """GET /api/council/reviews?hyp_id=... → full review trace for a hypothesis."""
    try:
        hyp_id = request.query.get("hyp_id", "").strip()
        if not hyp_id:
            return json_response({"error": "Missing ?hyp_id= parameter"}, status=400)
        reviews = _memory.get_reviews_for_hypothesis(hyp_id)
        decision = _memory.get_decision_for_hypothesis(hyp_id)
        evidence_requests = _memory.get_evidence_requests(status="all", hypothesis_id=hyp_id, limit=50)
        hypothesis = next((hyp for hyp in _memory.get_all_hypotheses(normalized=True) if hyp["id"] == hyp_id), None)
        return json_response({"reviews": reviews, "decision": decision, "hypothesis": hypothesis, "evidence_requests": evidence_requests})
    except Exception as e:
        log.error("council-reviews error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_viz_transients(request):
    """GET /api/viz/transients?target=...&limit=20 → High-energy monitor data."""
    try:
        target = request.query.get("target", "Sgr A*")
        limit = safe_limit(request.query.get("limit", "20"), default=20, max_limit=500)
        transients = _memory.get_transients(target=target, limit=limit)
        return json_response(transients)
    except Exception as e:
        log.error("viz/transients error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_viz_missions(request):
    """GET /api/viz/missions?name=...&limit=20 → Artemis II/Mission telemetry."""
    try:
        name = request.query.get("name")
        limit = safe_limit(request.query.get("limit", "20"), default=20, max_limit=500)
        missions = _memory.get_missions(mission_name=name, limit=limit)
        return json_response(missions)
    except Exception as e:
        log.error("viz/missions error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

async def handle_council_override(request):
    """POST /api/council/override → Danny manually overrides a decision."""
    try:
        body = await request.json()
        hyp_id = body.get("hyp_id", "").strip()
        new_status = body.get("new_status", "").strip()
        rationale = (body.get("rationale") or body.get("reason") or "").strip()
        actor = (body.get("actor") or "human_override").strip() or "human_override"
        if not hyp_id or not new_status:
            return json_response({"error": "Missing hyp_id or new_status"}, status=400)
        if len(rationale) < 12:
            return json_response({"error": "Manual override rationale must be at least 12 characters"}, status=400)
        updated = _memory.update_hypothesis_status(hyp_id, new_status, rationale=rationale, actor=actor)
        if not updated:
            return json_response({"error": "Hypothesis not found"}, status=404)
        _agent_log.add("council_override", f"Manual override: {updated['title']} → {new_status}", {
            "hypothesis_id": hyp_id,
            "previous_status": updated.get("previous_status"),
            "new_status": new_status,
            "actor": actor,
            "rationale": rationale,
        })
        return json_response({"status": "ok", "hypothesis": updated})
    except Exception as e:
        log.error("council-override error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)


async def handle_council_reprocess(request):
    """POST /api/council/reprocess → backfill council audits for legacy hypotheses."""
    try:
        council = get_council()
        if not council:
            return json_response({"error": "Council is not active in this runtime"}, status=503)

        body = await request.json()
        hyp_id = (body.get("hyp_id") or "").strip()
        force = bool(body.get("force", False))

        if hyp_id:
            result = council.review_existing(hyp_id, force=force)
            return json_response(result)

        result = council.reprocess_legacy(
            limit=safe_limit(body.get("limit", 5), default=5, max_limit=50),
            active_only=bool(body.get("active_only", True)),
            force=force,
            origin=body.get("origin") or None,
            status=body.get("status") or None,
        )
        return json_response(result)
    except Exception as e:
        log.error("council-reprocess error: %s", e)
        return json_response({"error": "Internal server error"}, status=500)

# ─── OBSERVATORY VIZ HANDLERS ─────────────────────────────────────────

async def handle_viz_timeline(request):
    """GET /api/viz/timeline → MAST queue history for observation timeline."""
    queue = _memory.get_mast_queue_stats()
    return json_response(queue)


async def handle_viz_skymap(request):
    """GET /api/viz/skymap → RA/Dec positions from completed MAST targets."""
    import glob
    skymap_points = []
    inbox = _BASE_DIR / "inbox"
    for f in inbox.glob("STScI_Data_*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            coords = data.get("coordinates", {})
            if coords.get("ra") and coords.get("dec"):
                skymap_points.append({
                    "target": data.get("target", "unknown"),
                    "ra": coords["ra"],
                    "dec": coords["dec"],
                    "obs_count": data.get("obs_count", 0),
                    "instruments": data.get("instruments", []),
                    "collections": data.get("collections", []),
                })
        except Exception:
            pass
    return json_response(skymap_points)


async def handle_viz_hypotheses(request):
    """GET /api/viz/hypotheses → all hypotheses + confidence history."""
    all_hyps = _memory.get_all_hypotheses(normalized=True)
    for h in all_hyps:
        h["type"] = h.get("origin", "founding")
        h["confidence_history"] = _memory.get_confidence_history(h.get("id"))

    return json_response(all_hyps)


async def handle_viz_network(request):
    """GET /api/viz/network → memory-hypothesis connection graph."""
    memories = _memory.get_memories()
    hypotheses = _memory.get_founding_hypotheses() + _memory.get_auto_hypotheses()
    
    nodes = []
    edges = []
    
    # Memory nodes
    for m in memories[-50:]:
        nodes.append({
            "id": f"mem_{m['id']}",
            "label": m.get("summary", "")[:40],
            "type": "memory",
            "group": "memory",
        })
    
    # Hypothesis nodes
    for h in hypotheses:
        hid = h.get("id", "?")
        nodes.append({
            "id": f"hyp_{hid}",
            "label": h.get("title", hid)[:40],
            "type": "hypothesis",
            "group": "hypothesis",
            "status": h.get("status", "active"),
        })
    
    # Edges: connect memories that reference hypotheses
    for m in memories[-50:]:
        summary = m.get("summary", "").lower()
        for h in hypotheses:
            hid = h.get("id", "")
            title = h.get("title", "").lower()
            if hid.lower() in summary or (title and title[:15] in summary):
                edges.append({
                    "source": f"mem_{m['id']}",
                    "target": f"hyp_{hid}",
                    "relation": "references",
                })
    
    return json_response({"nodes": nodes, "edges": edges})


# ─── APP FACTORY ─────────────────────────────────────────────────────

def create_bridge_app():
    app = web.Application(
        middlewares=[cors_middleware],
        client_max_size=1024 * 512,  # 512 KB max request body
    )

    # CORS preflight for all routes
    app.router.add_route("OPTIONS", "/{path:.*}", handle_options)

    # API endpoints
    app.router.add_get("/status", handle_status)
    app.router.add_get("/memories", handle_memories)
    app.router.add_get("/api/memory-link-proposals", handle_get_memory_link_proposals)
    app.router.add_post("/api/memory-link-proposals/generate", handle_generate_memory_link_proposals)
    app.router.add_post("/api/memory-link-proposals/review", handle_review_memory_link_proposal)
    app.router.add_get("/api/evidence-requests", handle_get_evidence_requests)
    app.router.add_post("/api/evidence-requests/review", handle_review_evidence_request)
    app.router.add_get("/query", handle_query)
    app.router.add_post("/query", handle_query)
    app.router.add_post("/ingest", handle_ingest)
    app.router.add_get("/hypotheses", handle_hypotheses)
    app.router.add_get("/api/hypotheses/all", handle_all_hypotheses)
    app.router.add_get("/agent_log", handle_agent_log)
    app.router.add_post("/consolidate", handle_consolidate)
    app.router.add_post("/hypotheses/reject", handle_reject_hypothesis)
    app.router.add_post("/hypotheses/status", handle_update_hypothesis_status)
    app.router.add_get("/simulations", handle_simulations)
    app.router.add_post("/simulations/dequeue", handle_dequeue_simulation)
    app.router.add_post("/cloud", handle_cloud_proxy)
    app.router.add_post("/cloud/query", handle_cloud_query)
    
    # Persistent Chat Sync
    app.router.add_get("/api/chat", handle_get_chat)
    app.router.add_post("/api/chat", handle_post_chat)
    
    # Observatory visualization endpoints
    app.router.add_get("/api/viz/timeline", handle_viz_timeline)
    app.router.add_get("/api/viz/skymap", handle_viz_skymap)
    app.router.add_get("/api/viz/hypotheses", handle_viz_hypotheses)
    app.router.add_get("/api/viz/network", handle_viz_network)

    # Hypothesis Review Council (Phase 18)
    app.router.add_get("/api/council/decisions", handle_council_decisions)
    app.router.add_get("/api/council/reviews", handle_council_reviews)
    app.router.add_post("/api/council/override", handle_council_override)
    app.router.add_post("/api/council/reprocess", handle_council_reprocess)

    # High-Energy Transients (Phase 19)
    app.router.add_get("/api/viz/transients", handle_viz_transients)

    # Mission Tracker (Phase 20)
    app.router.add_get("/api/viz/missions", handle_viz_missions)

    return app


def run_bridge(port, ingest_agent, memory, agent_log, nemotron, consolidate_agent=None):
    """Start the bridge server (called from manatuabon_agent.py in a thread)."""
    global _ingest_agent, _memory, _agent_log, _nemotron, _consolidate_agent, _start_time
    _ingest_agent = ingest_agent
    _memory = memory
    _agent_log = agent_log
    _nemotron = nemotron
    _consolidate_agent = consolidate_agent

    from datetime import datetime
    _start_time = datetime.now().isoformat()

    app = create_bridge_app()
    log.info("Bridge API starting on http://127.0.0.1:%d", port)
    web.run_app(app, host="127.0.0.1", port=port, print=None)
