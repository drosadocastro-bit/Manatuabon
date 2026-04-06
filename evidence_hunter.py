"""
Evidence Hunter — active evidence-seeking loop for held hypotheses.

Runs during each consolidation cycle to close the scientific method loop:
  1. Load pending evidence requests for held hypotheses
  2. Search internal memories for matching evidence
  3. Query external APIs (arXiv, MAST, SDSS, transient monitors) for evidence
  4. Classify findings via Tier A/B/C
  5. Auto-link Tier A/B evidence → mark request satisfied → flag for re-review
  6. Auto-reject hypotheses held too long without sufficient evidence

Design principles:
  - Auditable: every action logged with rationale
  - Bounded: caps on queries per cycle to respect rate limits
  - Human-overridable: all auto-actions can be reversed via UI
  - Graceful degradation: external API failures never block the cycle
"""

import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger("manatuabon.evidence_hunter")

_BASE_DIR = Path(__file__).resolve().parent

# ── Config ────────────────────────────────────────────────────────────
MAX_REQUESTS_PER_CYCLE = 10        # Evidence requests processed per consolidation
MAX_MEMORY_HITS = 20               # Internal memory search cap
MAX_ARXIV_RESULTS = 5              # arXiv papers per hypothesis query
MAX_EXTERNAL_QUERIES_PER_CYCLE = 6 # Total external API calls per cycle
STALE_DAYS = 14                    # Days before held hypothesis is auto-rejected
MIN_LINK_SCORE = 3.0               # Minimum relevance score to auto-link
ARXIV_COOLDOWN_SECS = 3            # arXiv rate limit respect

# Domain → external API routing table
DOMAIN_API_MAP = {
    "sgra":          ["arxiv", "mast", "transient"],
    "pulsars":       ["arxiv", "mast", "transient"],
    "black_holes":   ["arxiv", "mast"],
    "cosmology":     ["arxiv", "sdss"],
    "exoplanets":    ["arxiv", "mast"],
    "quasars":       ["arxiv", "sdss", "mast"],
    "cosmic_web":    ["arxiv", "sdss"],
    "seti":          ["arxiv", "radio"],
    "consciousness": ["arxiv"],
}


class EvidenceHunter:
    """Active evidence-seeking agent for held hypotheses with pending requests."""

    def __init__(self, memory, agent_log):
        """
        Args:
            memory: MemoryManager instance (from manatuabon_agent)
            agent_log: AgentLog instance for audit trail
        """
        self.memory = memory
        self.agent_log = agent_log
        self._external_calls_this_cycle = 0

    # ── Main entry point ──────────────────────────────────────────────

    def hunt(self) -> dict:
        """Run one evidence-hunting cycle. Called from consolidation loop.

        Returns summary dict with counts and actions taken.
        """
        log.info("🔍 Evidence Hunter: starting cycle")
        self.agent_log.add("evidence_hunt_start", "Evidence Hunter cycle started")
        self._external_calls_this_cycle = 0

        summary = {
            "requests_processed": 0,
            "memory_matches": 0,
            "external_fetches": 0,
            "requests_satisfied": 0,
            "hypotheses_flagged_for_rereview": 0,
            "hypotheses_auto_rejected": 0,
            "errors": [],
        }

        # 1. Load pending evidence requests for held hypotheses
        pending = self._load_actionable_requests()
        if not pending:
            log.info("🔍 Evidence Hunter: no actionable requests")
            self.agent_log.add("evidence_hunt_complete", "No actionable requests", summary)
            return summary

        log.info("🔍 Evidence Hunter: %d actionable requests", len(pending))

        # 2. Process each request
        for req in pending[:MAX_REQUESTS_PER_CYCLE]:
            try:
                result = self._process_request(req)
                summary["requests_processed"] += 1
                summary["memory_matches"] += result.get("memory_matches", 0)
                summary["external_fetches"] += result.get("external_fetches", 0)
                if result.get("satisfied"):
                    summary["requests_satisfied"] += 1
            except Exception as e:
                log.warning("Evidence hunt error for request #%s: %s", req.get("id"), e)
                summary["errors"].append({"request_id": req.get("id"), "error": str(e)})

        # 3. Check for stale held hypotheses
        stale_count = self._check_stale_hypotheses()
        summary["hypotheses_auto_rejected"] = stale_count

        # 4. Flag hypotheses ready for re-review
        rereview_count = self._flag_rereview_candidates()
        summary["hypotheses_flagged_for_rereview"] = rereview_count

        log.info(
            "🔍 Evidence Hunter: cycle complete — %d processed, %d satisfied, "
            "%d external, %d stale rejected, %d flagged for re-review",
            summary["requests_processed"],
            summary["requests_satisfied"],
            summary["external_fetches"],
            summary["hypotheses_auto_rejected"],
            summary["hypotheses_flagged_for_rereview"],
        )
        self.agent_log.add("evidence_hunt_complete", "Cycle complete", summary)
        return summary

    # ── Request loading ───────────────────────────────────────────────

    def _load_actionable_requests(self) -> list[dict]:
        """Load pending evidence requests where the hypothesis is still held."""
        requests = self.memory.get_evidence_requests(status="pending", limit=200)
        # Only act on requests for held hypotheses
        return [
            r for r in requests
            if r.get("hypothesis_status") == "held"
        ]

    # ── Per-request processing ────────────────────────────────────────

    def _process_request(self, req: dict) -> dict:
        """Search for evidence matching a single evidence request."""
        request_id = req["id"]
        hypothesis_id = req["hypothesis_id"]
        request_text = req["request_text"]
        hypothesis_title = req.get("hypothesis_title", hypothesis_id)

        log.info(
            "🔍 Hunting evidence for request #%d: %s (hyp: %s)",
            request_id, request_text[:80], hypothesis_id,
        )

        result = {"memory_matches": 0, "external_fetches": 0, "satisfied": False}
        found_evidence = []

        # ── Phase 1: Search internal memories ─────────────────────
        memory_hits = self._search_internal_memories(request_text, hypothesis_id)
        result["memory_matches"] = len(memory_hits)
        for hit in memory_hits:
            tier, rationale = self._classify(hit["content"])
            if tier in ("tier_a", "tier_b"):
                found_evidence.append({
                    "memory_id": hit["id"],
                    "source": "internal_memory",
                    "tier": tier,
                    "rationale": rationale,
                    "summary": (hit.get("content") or "")[:200],
                })

        # ── Phase 2: Query external APIs ──────────────────────────
        if not found_evidence and self._can_query_external():
            hypothesis_domains = self._get_hypothesis_domains(hypothesis_id)
            external_hits = self._search_external_apis(
                request_text, hypothesis_title, hypothesis_domains,
            )
            result["external_fetches"] = len(external_hits)
            for ext in external_hits:
                tier, rationale = self._classify(ext["content"])
                if tier in ("tier_a", "tier_b"):
                    # Ingest as new memory and link
                    memory_id = self._ingest_external_evidence(
                        ext["content"], ext["source"], hypothesis_id,
                    )
                    found_evidence.append({
                        "memory_id": memory_id,
                        "source": ext["source"],
                        "tier": tier,
                        "rationale": rationale,
                        "summary": ext["content"][:200],
                    })

        # ── Phase 3: Auto-satisfy if sufficient evidence found ────
        if found_evidence:
            self._satisfy_request(request_id, hypothesis_id, found_evidence)
            result["satisfied"] = True

        return result

    # ── Internal memory search ────────────────────────────────────

    def _search_internal_memories(self, request_text: str, hypothesis_id: str) -> list[dict]:
        """Search existing memories by keywords from the evidence request."""
        # Extract meaningful search terms (4+ chars, skip stopwords)
        stopwords = {
            "this", "that", "with", "from", "have", "been", "will", "what",
            "when", "where", "which", "there", "their", "about", "would",
            "could", "should", "before", "after", "between", "through",
            "evidence", "provide", "attach", "least", "least",
        }
        words = [
            w for w in re.findall(r"[a-z0-9*+-]{4,}", request_text.lower())
            if w not in stopwords
        ]
        if not words:
            return []

        # Build SQL search — OR across all keywords
        conditions = " OR ".join(["content LIKE ?"] * len(words))
        params = [f"%{w}%" for w in words[:8]]  # Cap keywords for performance

        try:
            with self.memory._get_conn() as c:
                rows = c.execute(
                    f"SELECT id, content, significance, domain_tags, "
                    f"supports_hypothesis, challenges_hypothesis "
                    f"FROM memories WHERE ({conditions}) "
                    f"AND supports_hypothesis IS NULL "  # Unlinked memories only
                    f"AND challenges_hypothesis IS NULL "
                    f"ORDER BY id DESC LIMIT ?",
                    params + [MAX_MEMORY_HITS],
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            log.warning("Internal memory search failed: %s", e)
            return []

    # ── External API queries ──────────────────────────────────────

    def _can_query_external(self) -> bool:
        return self._external_calls_this_cycle < MAX_EXTERNAL_QUERIES_PER_CYCLE

    def _search_external_apis(
        self,
        request_text: str,
        hypothesis_title: str,
        domains: list[str],
    ) -> list[dict]:
        """Query external APIs based on hypothesis domain. Returns evidence dicts."""
        # Determine which APIs to call based on domains
        apis_to_call = set()
        for domain in domains:
            for api in DOMAIN_API_MAP.get(domain, ["arxiv"]):
                apis_to_call.add(api)
        if not apis_to_call:
            apis_to_call.add("arxiv")  # Always try arXiv as fallback

        results = []

        # Build a focused search query from hypothesis title + request text
        search_query = self._build_search_query(hypothesis_title, request_text)

        for api in sorted(apis_to_call):
            if not self._can_query_external():
                break
            try:
                hits = self._call_api(api, search_query, hypothesis_title)
                results.extend(hits)
            except Exception as e:
                log.warning("External API '%s' failed: %s", api, e)

        return results

    def _build_search_query(self, hypothesis_title: str, request_text: str) -> str:
        """Build a concise search query from hypothesis + request."""
        # Combine key terms from both, keep it short for API queries
        combined = f"{hypothesis_title} {request_text}"
        # Extract the most distinctive terms (skip common words)
        stopwords = {
            "the", "and", "for", "are", "but", "not", "you", "all", "can",
            "had", "her", "was", "one", "our", "out", "day", "get", "has",
            "him", "his", "how", "its", "may", "new", "now", "old", "see",
            "way", "who", "did", "let", "say", "she", "too", "use",
            "evidence", "provide", "attach", "least", "before", "considering",
            "acceptance", "replace", "support", "direct", "item", "tier",
            "hypothesis", "review",
        }
        terms = [
            w for w in re.findall(r"[a-zA-Z0-9*+-]{3,}", combined)
            if w.lower() not in stopwords
        ]
        # Keep first 6 distinctive terms
        return " ".join(terms[:6])

    def _call_api(self, api: str, query: str, hypothesis_title: str) -> list[dict]:
        """Dispatch to the appropriate external API."""
        if api == "arxiv":
            return self._query_arxiv(query)
        if api == "mast":
            return self._query_mast(hypothesis_title)
        if api == "sdss":
            return self._query_sdss(hypothesis_title)
        if api == "transient":
            return self._query_transients(hypothesis_title)
        if api == "radio":
            return self._query_radio(hypothesis_title)
        return []

    def _query_arxiv(self, query: str) -> list[dict]:
        """Search arXiv for papers matching the query."""
        self._external_calls_this_cycle += 1
        try:
            from data_fetch_agent import fetch_arxiv
            payload = fetch_arxiv(queries=[query], max_results=MAX_ARXIV_RESULTS)
            if not payload or not payload.get("papers"):
                return []
            results = []
            for paper in payload["papers"][:MAX_ARXIV_RESULTS]:
                title = paper.get("title", "")
                abstract = paper.get("abstract", "")
                arxiv_id = paper.get("arxiv_id", "")
                content = (
                    f"[arXiv:{arxiv_id}] {title}. {abstract[:500]}"
                )
                results.append({
                    "source": f"arxiv:{arxiv_id}",
                    "content": content,
                })
            return results
        except Exception as e:
            log.warning("arXiv query failed: %s", e)
            return []

    def _query_mast(self, hypothesis_title: str) -> list[dict]:
        """Search MAST archive for observations related to the hypothesis."""
        self._external_calls_this_cycle += 1
        # Extract a target name from the hypothesis title
        target = self._extract_astro_target(hypothesis_title)
        if not target:
            return []
        try:
            from mast_worker import process_target
            report, structured = process_target(target)
            if not report:
                return []
            return [{
                "source": f"mast:{target}",
                "content": f"[MAST observation] {report[:800]}",
            }]
        except Exception as e:
            log.warning("MAST query for '%s' failed: %s", target, e)
            return []

    def _query_sdss(self, hypothesis_title: str) -> list[dict]:
        """Search SDSS if the hypothesis involves galaxy-scale objects."""
        self._external_calls_this_cycle += 1
        # SDSS requires coordinates — extract common targets
        coords = self._extract_coordinates(hypothesis_title)
        if not coords:
            return []
        try:
            from data_fetch_agent import fetch_sdss
            payload = fetch_sdss(
                ra_center=coords["ra"],
                dec_center=coords["dec"],
                radius_arcmin=60,
                max_results=5,
            )
            if not payload or not payload.get("galaxies"):
                return []
            results = []
            for g in payload["galaxies"][:3]:
                content = (
                    f"[SDSS galaxy] RA={g.get('ra'):.4f} Dec={g.get('dec'):.4f} "
                    f"z={g.get('redshift', 'N/A')} mag_r={g.get('mag_r', 'N/A')}"
                )
                results.append({"source": "sdss", "content": content})
            return results
        except Exception as e:
            log.warning("SDSS query failed: %s", e)
            return []

    def _query_transients(self, hypothesis_title: str) -> list[dict]:
        """Poll transient monitors for recent flux data."""
        self._external_calls_this_cycle += 1
        target = self._extract_astro_target(hypothesis_title)
        if not target:
            return []
        try:
            from transient_worker import TransientWorker
            worker = TransientWorker()
            swift = worker.fetch_swift_bat(target)
            if swift and swift.get("flux") is not None:
                content = (
                    f"[Swift/BAT] {target}: flux={swift['flux']:.4e} "
                    f"({swift.get('band', '15-50 keV')}), "
                    f"MJD={swift.get('mjd', 'N/A')}"
                )
                return [{"source": f"swift_bat:{target}", "content": content}]
            return []
        except Exception as e:
            log.warning("Transient query for '%s' failed: %s", target, e)
            return []

    def _query_radio(self, hypothesis_title: str) -> list[dict]:
        """Query ALMA archive for radio observations."""
        self._external_calls_this_cycle += 1
        target = self._extract_astro_target(hypothesis_title)
        if not target:
            return []
        try:
            from radio_worker import query_alma
            inbox = _BASE_DIR / "inbox"
            query_alma(target, inbox)
            # Check for newly dropped file
            recent = sorted(inbox.glob("alma_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            if recent:
                with open(recent[0], "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("observations"):
                    obs = data["observations"][0]
                    content = (
                        f"[ALMA] {target}: band={obs.get('band', 'N/A')}, "
                        f"frequency={obs.get('frequency', 'N/A')}, "
                        f"resolution={obs.get('spatial_resolution', 'N/A')}"
                    )
                    return [{"source": f"alma:{target}", "content": content}]
            return []
        except Exception as e:
            log.warning("ALMA query for '%s' failed: %s", target, e)
            return []

    # ── Evidence classification ───────────────────────────────────

    def _classify(self, text: str) -> tuple[str, str]:
        """Classify evidence text into Tier A/B/C."""
        from hypothesis_council import EvidenceReviewerAgent
        reviewer = EvidenceReviewerAgent()
        return reviewer.classify_item(text or "")

    # ── Auto-linking and satisfaction ─────────────────────────────

    def _satisfy_request(
        self, request_id: int, hypothesis_id: str, evidence: list[dict],
    ):
        """Mark an evidence request as satisfied and link evidence to hypothesis."""
        memory_ids = [e["memory_id"] for e in evidence if e.get("memory_id")]
        tiers = [e["tier"] for e in evidence]
        sources = [e["source"] for e in evidence]

        resolution_note = (
            f"Auto-satisfied by Evidence Hunter. "
            f"Found {len(evidence)} item(s): "
            f"{', '.join(f'{t.upper()} from {s}' for t, s in zip(tiers, sources))}."
        )

        # Mark the evidence request as completed
        self.memory.review_evidence_request(
            request_id=request_id,
            decision="complete",
            resolution_note=resolution_note,
            satisfied_memory_ids=memory_ids,
        )

        # Link memories to hypothesis (support)
        with self.memory._get_conn() as c:
            for mid in memory_ids:
                c.execute(
                    "UPDATE memories SET supports_hypothesis=? "
                    "WHERE id=? AND supports_hypothesis IS NULL",
                    (hypothesis_id, mid),
                )
            c.commit()

        self.agent_log.add(
            "evidence_hunt_satisfied",
            f"Request #{request_id} satisfied: {len(evidence)} evidence items linked to {hypothesis_id}",
            {
                "request_id": request_id,
                "hypothesis_id": hypothesis_id,
                "evidence": evidence,
                "resolution_note": resolution_note,
            },
        )
        log.info(
            "🔍 Request #%d satisfied: %d items → %s",
            request_id, len(evidence), hypothesis_id,
        )

    def _ingest_external_evidence(
        self, content: str, source: str, hypothesis_id: str,
    ) -> int:
        """Ingest externally fetched evidence as a new memory linked to the hypothesis."""
        memory = {
            "timestamp": datetime.now().isoformat(),
            "summary": content,
            "entities": [source],
            "confidence": 0.7,  # Conservative — external data not yet peer-reviewed
            "supports_hypothesis": hypothesis_id,
        }
        memory_id = self.memory.add_memory(memory)
        log.info(
            "🔍 Ingested external evidence as memory #%d from %s → %s",
            memory_id, source, hypothesis_id,
        )
        return memory_id

    # ── Staleness check ───────────────────────────────────────────

    def _check_stale_hypotheses(self) -> int:
        """Auto-reject hypotheses held for > STALE_DAYS with all requests unsatisfied."""
        rejected_count = 0
        cutoff = (datetime.now() - timedelta(days=STALE_DAYS)).isoformat()

        try:
            with self.memory._get_conn() as c:
                # Find held hypotheses with decisions older than cutoff
                held = c.execute(
                    "SELECT hd.hypothesis_id, hd.timestamp, h.title "
                    "FROM hypothesis_decisions hd "
                    "JOIN hypotheses h ON h.id = hd.hypothesis_id "
                    "WHERE hd.decision = 'held' AND hd.timestamp < ? "
                    "ORDER BY hd.timestamp ASC LIMIT 5",
                    (cutoff,),
                ).fetchall()

            for row in held:
                hid = row["hypothesis_id"]
                # Check if any evidence requests were satisfied
                requests = self.memory.get_evidence_requests(
                    status="all", hypothesis_id=hid, limit=50,
                )
                pending = [r for r in requests if r["status"] == "pending"]
                completed = [r for r in requests if r["status"] == "completed"]

                if not completed and pending:
                    # All requests still pending after STALE_DAYS — reject
                    self._auto_reject_stale(hid, row["title"], row["timestamp"], len(pending))
                    rejected_count += 1

        except Exception as e:
            log.warning("Staleness check failed: %s", e)

        return rejected_count

    def _auto_reject_stale(
        self, hypothesis_id: str, title: str, held_since: str, pending_count: int,
    ):
        """Auto-reject a stale held hypothesis for lack of evidence."""
        reason = (
            f"Auto-rejected by Evidence Hunter: held since {held_since} "
            f"({STALE_DAYS}+ days) with {pending_count} evidence request(s) "
            f"still unsatisfied after active search."
        )
        try:
            with self.memory._get_conn() as c:
                c.execute(
                    "UPDATE hypotheses SET status='rejected_insufficient_evidence' WHERE id=?",
                    (hypothesis_id,),
                )
                # Record the decision
                c.execute(
                    "INSERT INTO hypothesis_decisions "
                    "(hypothesis_id, decision, final_score, reasoning, timestamp) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        hypothesis_id,
                        "rejected_insufficient_evidence",
                        0.0,
                        reason,
                        datetime.now().isoformat(),
                    ),
                )
                # Dismiss remaining pending requests
                c.execute(
                    "UPDATE evidence_requests SET status='dismissed', "
                    "resolution_note=?, resolved_at=?, updated_at=? "
                    "WHERE hypothesis_id=? AND status='pending'",
                    (
                        "Dismissed: hypothesis auto-rejected for insufficient evidence",
                        datetime.now().isoformat(),
                        datetime.now().isoformat(),
                        hypothesis_id,
                    ),
                )
                c.commit()

            self.agent_log.add(
                "evidence_hunt_auto_reject",
                f"Hypothesis '{title}' ({hypothesis_id}) auto-rejected: insufficient evidence after {STALE_DAYS} days",
                {
                    "hypothesis_id": hypothesis_id,
                    "title": title,
                    "held_since": held_since,
                    "pending_requests": pending_count,
                    "reason": reason,
                },
            )
            log.info(
                "🔍 Auto-rejected stale hypothesis: %s (%s) — held since %s",
                title, hypothesis_id, held_since,
            )
        except Exception as e:
            log.error("Failed to auto-reject %s: %s", hypothesis_id, e)

    # ── Re-review flagging ────────────────────────────────────────

    def _flag_rereview_candidates(self) -> int:
        """Count held hypotheses that now have satisfied evidence requests."""
        flagged = 0
        try:
            requests = self.memory.get_evidence_requests(status="completed", limit=200)
            seen_hypotheses = set()
            for req in requests:
                hid = req["hypothesis_id"]
                if hid in seen_hypotheses:
                    continue
                if req.get("hypothesis_status") == "held" and req.get("ready_for_rereview"):
                    seen_hypotheses.add(hid)
                    flagged += 1
                    self.agent_log.add(
                        "evidence_hunt_rereview_ready",
                        f"Hypothesis {hid} has new satisfied evidence — ready for council re-review",
                        {"hypothesis_id": hid},
                    )
        except Exception as e:
            log.warning("Re-review flagging failed: %s", e)
        return flagged

    # ── Utility helpers ───────────────────────────────────────────

    def _get_hypothesis_domains(self, hypothesis_id: str) -> list[str]:
        """Get domain tags for a hypothesis."""
        try:
            with self.memory._get_conn() as c:
                row = c.execute(
                    "SELECT title, description, domain_tags FROM hypotheses WHERE id=?",
                    (hypothesis_id,),
                ).fetchone()
            if not row:
                return []
            if row["domain_tags"]:
                try:
                    return json.loads(row["domain_tags"])
                except (json.JSONDecodeError, TypeError):
                    pass
            # Fallback: detect from text
            from manatuabon_agent import MemoryManager
            text = f"{row['title'] or ''} {row['description'] or ''}"
            return MemoryManager._detect_memory_domains(text)
        except Exception:
            return []

    def _extract_astro_target(self, text: str) -> str | None:
        """Extract a resolvable astronomical target name from text."""
        # Common targets that MAST/transient APIs can resolve
        known_targets = [
            "Sgr A*", "Crab Nebula", "M87", "Cyg X-1", "Sgr B2",
            "Vela Pulsar", "GRS 1915+105", "Eta Carinae", "Cas A",
            "NGC 1275", "3C 273", "Mrk 421", "Centaurus A",
        ]
        text_lower = text.lower()
        for target in known_targets:
            if target.lower() in text_lower:
                return target

        # Try to extract from patterns like "NGC XXXX", "M XX", "IC XXXX"
        m = re.search(r"\b(NGC\s*\d{2,5}|M\s*\d{1,3}|IC\s*\d{2,5})\b", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()

        return None

    def _extract_coordinates(self, text: str) -> dict | None:
        """Extract RA/Dec from text or return known coordinates for named targets."""
        # Known target coordinates (J2000 approximate)
        known_coords = {
            "sgr a*":    {"ra": 266.417, "dec": -29.008},
            "galactic center": {"ra": 266.417, "dec": -29.008},
            "m87":       {"ra": 187.706, "dec": 12.391},
            "crab":      {"ra": 83.633,  "dec": 22.015},
            "cyg x-1":   {"ra": 299.590, "dec": 35.202},
            "great attractor": {"ra": 244.300, "dec": -63.150},
        }
        text_lower = text.lower()
        for name, coords in known_coords.items():
            if name in text_lower:
                return coords

        # Try to extract numeric RA/Dec
        m = re.search(r"RA[=:\s]*([\d.]+).*?Dec[=:\s]*([+-]?[\d.]+)", text, re.IGNORECASE)
        if m:
            try:
                return {"ra": float(m.group(1)), "dec": float(m.group(2))}
            except ValueError:
                pass

        return None
