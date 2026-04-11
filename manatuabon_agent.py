"""
MANATUABON AGENT — The Always-On Astrophysics Brain
====================================================
Run:  python manatuabon_agent.py
Args: --watch DIR   (default: D:\\Manatuabon\\renders)
      --inbox DIR   (default: D:\\Manatuabon\\inbox)
      --port  INT   (default: 7777)
      --consolidate-every INT  (minutes, default: 30)
      --lm-url STR  (default: http://127.0.0.1:1234)

Danny from Bayamón, PR 🇵🇷 + Claude — March 2026
"""

import os, sys, json, time, re, argparse, threading, logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# Load local .env variables
load_dotenv(Path(__file__).resolve().parent / ".env")

import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ─── CONFIG ──────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "manatuabon.db"
AGENT_LOG_FILE = BASE_DIR / "agent_log.json"
SUPPORTED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".txt", ".json", ".csv"}
DEBOUNCE_SECONDS = 3
MAX_LOG_ENTRIES = 200

MEMORY_DOMAIN_KEYWORDS = {
    "sgra": {"sgr a*", "sgr a", "sgra", "galactic center", "riaf", "bondi", "s-stars"},
    "pulsars": {"pulsar", "magnetar", "crab", "vela", "spin-down", "wind nebula"},
    "cosmology": {"cosmology", "inflation", "cmb", "dark flow", "laniakea", "great attractor", "bulk flows"},
    "black_holes": {"black hole", "event horizon", "hawking", "information paradox", "wormhole"},
    "consciousness": {"consciousness", "observer", "wheeler", "anthropic", "participatory"},
    "seti": {"seti", "fermi paradox", "wow signal", "civilization", "silence"},
    "exoplanets": {"exoplanet", "biosignature", "atmosphere", "transmission spectra", "habitability", "disequilibrium"},
    "quasars": {"quasar", "reverberation mapping", "broad-line region", "time-domain", "active galactic nucleus", "variability"},
    "cosmic_web": {"cosmic web", "filaments", "weak lensing", "large scale structure", "galaxy evolution"},
}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("manatuabon")


# ─── NEMOTRON CLIENT ─────────────────────────────────────────────────
class NemotronClient:
    """Calls LM Studio's OpenAI-compatible API."""

    def __init__(self, base_url="http://127.0.0.1:1234"):
        self.base_url = base_url.rstrip("/")
        self.endpoint = f"{self.base_url}/v1/chat/completions"

    def chat(self, system_prompt: str, user_prompt: str, temperature=0.4, max_tokens=2048) -> str:
        """Send a chat completion and return the assistant message text."""
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            resp = requests.post(self.endpoint, json=payload, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except requests.exceptions.ConnectionError:
            log.error("Cannot reach Nemotron at %s — is LM Studio running?", self.base_url)
            return ""
        except Exception as e:
            log.error("Nemotron API error: %s", e)
            return ""

    def chat_json(self, system_prompt: str, user_prompt: str, **kwargs) -> dict | None:
        """Chat and parse the response as JSON."""
        raw = self.chat(system_prompt, user_prompt, **kwargs)
        if not raw:
            return None
        # Try to extract JSON from the response (Nemotron sometimes wraps in markdown)
        json_match = re.search(r"\{[\s\S]*\}", raw)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                log.warning("Failed to parse Nemotron JSON response.")
                return None
        return None


# ─── MEMORY MANAGER (SQL RAG) ────────────────────────────────────────
import sqlite3
from db_init import ensure_runtime_db

class MemoryManager:
    """Read/write agent memories directly via SQLite RAG architecture."""

    @staticmethod
    def _clamp_confidence(value, default=0.5) -> float:
        try:
            return round(min(max(float(value), 0.0), 1.0), 3)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _confidence_label(score: float) -> str:
        if score >= 0.75:
            return "high"
        if score >= 0.45:
            return "medium"
        return "low"

    @staticmethod
    def _normalize_origin(source: str | None, explicit_origin: str | None = None) -> str:
        if explicit_origin:
            return explicit_origin
        source_value = (source or "").strip().lower()
        if source_value == "agent auto":
            return "agent_auto"
        if source_value.startswith("manatuabon agent"):
            return "agent_auto"
        if source_value in {"manual", "manual_entry"}:
            return "manual"
        if source_value:
            return "founding"
        return "founding"

    def _get_hypothesis_signal_counts(self, hypothesis_id: str) -> tuple[int, int]:
        with self._get_conn() as c:
            support_count = c.execute(
                "SELECT COUNT(*) AS c FROM memories WHERE supports_hypothesis=?",
                (hypothesis_id,),
            ).fetchone()["c"]
            contradiction_count = c.execute(
                "SELECT COUNT(*) AS c FROM memories WHERE challenges_hypothesis=?",
                (hypothesis_id,),
            ).fetchone()["c"]
        return int(support_count or 0), int(contradiction_count or 0)

    @staticmethod
    def _detect_memory_domains(text: str = "", entities: list[str] | None = None) -> list[str]:
        haystack = " ".join([text or "", " ".join(entities or [])]).lower()
        detected = []
        for domain, keywords in MEMORY_DOMAIN_KEYWORDS.items():
            if any(keyword in haystack for keyword in keywords):
                detected.append(domain)
        return sorted(set(detected))

    @staticmethod
    def _memory_link_polarity(text: str) -> str | None:
        haystack = (text or "").lower()
        challenge_markers = (
            "contradict", "challenge", "artifact", "error", "failed", "null", "inconsistent",
            "no evidence", "spurious", "negative distance", "pipeline error"
        )
        support_markers = (
            "support", "consistent with", "evidence for", "corroborat", "aligned with", "matches",
            "confirms", "reveals"
        )
        if any(marker in haystack for marker in challenge_markers):
            return "challenge"
        if any(marker in haystack for marker in support_markers):
            return "support"
        return None

    def _infer_memory_hypothesis_link(self, text: str, entities: list[str] | None = None, hypotheses: list[dict] | None = None) -> tuple[str | None, str | None]:
        ranked = self._rank_memory_hypothesis_links(text, entities, hypotheses=hypotheses)
        if not ranked:
            return None, None

        best = ranked[0]
        auto_link_eligible = best["direct_reference"] or best["score"] >= 4.5
        if not auto_link_eligible:
            return None, None
        if best["relation"] == "support":
            return best["hypothesis_id"], None
        return None, best["hypothesis_id"]

    def _rank_memory_hypothesis_links(self, text: str, entities: list[str] | None = None, hypotheses: list[dict] | None = None) -> list[dict]:
        polarity = self._memory_link_polarity(text)
        if not polarity:
            return []

        raw_text = text or ""
        text_lower = raw_text.lower()
        tokens = {token for token in re.findall(r"[a-z0-9*+-]{4,}", text_lower)}
        memory_domains = set(self._detect_memory_domains(raw_text, entities))
        candidates = hypotheses if hypotheses is not None else self.get_all_hypotheses(normalized=True)
        ranked = []

        for hypothesis in candidates:
            hypothesis_blob = " ".join([
                hypothesis.get("id", ""),
                hypothesis.get("title", ""),
                hypothesis.get("text", ""),
                " ".join(hypothesis.get("tags", []) or []),
            ])
            hypothesis_terms = {token for token in re.findall(r"[a-z0-9*+-]{4,}", hypothesis_blob.lower())}
            overlap_terms = sorted(tokens & hypothesis_terms)
            direct_id = hypothesis.get("id", "").lower() in text_lower if hypothesis.get("id") else False
            direct_title = hypothesis.get("title", "").lower() in text_lower if hypothesis.get("title") else False
            hypothesis_domains = set(self._detect_memory_domains(
                f"{hypothesis.get('title', '')} {hypothesis.get('text', '')}",
                hypothesis.get("tags", []) or [],
            ))
            domain_overlap = sorted(memory_domains & hypothesis_domains)
            score = float(len(overlap_terms))
            if direct_id:
                score += 3.0
            if direct_title:
                score += 2.0
            if domain_overlap:
                score += 1.5 * len(domain_overlap)
            if score < 2.0:
                continue

            rationale_bits = []
            if direct_id:
                rationale_bits.append("explicit hypothesis id reference")
            if direct_title:
                rationale_bits.append("explicit title reference")
            if overlap_terms:
                rationale_bits.append("shared terms: " + ", ".join(overlap_terms[:5]))
            if domain_overlap:
                rationale_bits.append("domain overlap: " + ", ".join(domain_overlap))

            ranked.append({
                "hypothesis_id": hypothesis.get("id"),
                "hypothesis_title": hypothesis.get("title") or hypothesis.get("id"),
                "relation": polarity,
                "score": round(score, 3),
                "rationale": "; ".join(rationale_bits) or "lexical overlap with hypothesis text",
                "memory_domains": sorted(memory_domains),
                "domain_overlap": domain_overlap,
                "shared_terms": overlap_terms[:5],
                "direct_reference": bool(direct_id or direct_title),
            })

        ranked.sort(key=lambda item: (-item["score"], item["hypothesis_id"]))
        return ranked

    def _upsert_memory_link_proposal(self, conn: sqlite3.Connection, memory_id: int, candidate: dict, source: str = "backfill_review_v1") -> str | None:
        row = conn.execute(
            "SELECT id, status FROM memory_link_proposals WHERE memory_id=? AND hypothesis_id=? AND relation=?",
            (memory_id, candidate["hypothesis_id"], candidate["relation"]),
        ).fetchone()
        timestamp = datetime.now().isoformat()
        if row:
            if row["status"] in {"approved", "rejected", "superseded"}:
                return None
            conn.execute(
                "UPDATE memory_link_proposals SET score=?, rationale=?, status='pending', source=?, proposed_at=? WHERE id=?",
                (candidate["score"], candidate["rationale"], source, timestamp, row["id"]),
            )
            return "updated"

        conn.execute(
            "INSERT INTO memory_link_proposals (memory_id, hypothesis_id, relation, score, rationale, status, source, proposed_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
            (memory_id, candidate["hypothesis_id"], candidate["relation"], candidate["score"], candidate["rationale"], source, timestamp),
        )
        return "created"

    def _estimate_hypothesis_confidence_components(self, row: sqlite3.Row, latest_confidence: dict | None = None) -> dict:
        status = (row["status"] or "proposed").strip().lower()
        evidence_text = (row["evidence"] or "").strip()
        description_text = (row["description"] or "").strip()
        support_count, contradiction_count = self._get_hypothesis_signal_counts(row["id"])
        tags = []
        if row["tags"]:
            try:
                tags = json.loads(row["tags"])
            except json.JSONDecodeError:
                tags = []

        evidence_score = 0.25
        if evidence_text:
            evidence_score += 0.25
        evidence_score += min(support_count, 3) * 0.12
        evidence_score -= min(contradiction_count, 3) * 0.1
        if latest_confidence:
            evidence_score += min(latest_confidence["confidence"], 1.0) * 0.2

        testability_score = 0.3
        if any(keyword in description_text.lower() for keyword in ("predict", "test", "observe", "measure", "falsif")):
            testability_score += 0.3
        if status in {"accepted", "held", "needs_revision"}:
            testability_score += 0.1

        coherence_score = 0.3
        if description_text:
            coherence_score += 0.15
        coherence_score += min(support_count, 3) * 0.05
        coherence_score -= min(contradiction_count, 3) * 0.12
        if status in {"accepted", "held", "merged"}:
            coherence_score += 0.2
        elif status in {"rejected", "rejected_auto"}:
            coherence_score -= 0.15

        novelty_score = 0.35
        novelty_score += min(len(tags), 4) * 0.05
        if self._normalize_origin(row["source"], row["origin"]) == "agent_auto":
            novelty_score += 0.1

        return {
            "evidence_score": self._clamp_confidence(evidence_score),
            "testability_score": self._clamp_confidence(testability_score),
            "coherence_score": self._clamp_confidence(coherence_score),
            "novelty_score": self._clamp_confidence(novelty_score),
            "support_count": support_count,
            "contradiction_count": contradiction_count,
            "support_balance": round((support_count - contradiction_count) / max(support_count + contradiction_count, 1), 3),
        }

    def _weighted_hypothesis_confidence(self, components: dict) -> float:
        return self._clamp_confidence(
            0.35 * components.get("evidence_score", 0.0)
            + 0.25 * components.get("testability_score", 0.0)
            + 0.25 * components.get("coherence_score", 0.0)
            + 0.15 * components.get("novelty_score", 0.0)
        )

    def _canonical_hypothesis_from_row(self, row: sqlite3.Row) -> dict:
        latest_confidence = self.get_latest_confidence(row["id"])
        stored_components = {}
        if row["confidence_components"]:
            try:
                stored_components = json.loads(row["confidence_components"])
            except json.JSONDecodeError:
                stored_components = {}
        components = stored_components or self._estimate_hypothesis_confidence_components(row, latest_confidence)
        stored_confidence = row["confidence"]
        if stored_confidence is None:
            stored_confidence = latest_confidence["confidence"] if latest_confidence else self._weighted_hypothesis_confidence(components)
        confidence = self._clamp_confidence(stored_confidence)
        timestamp = row["updated_at"] or row["date"] or row["created_at"]
        return {
            "id": row["id"],
            "title": row["title"],
            "text": row["description"] or "",
            "status": row["status"] or "proposed",
            "confidence": confidence,
            "confidence_label": self._confidence_label(confidence),
            "confidence_source": row["confidence_source"] or (latest_confidence["confidence_source"] if latest_confidence else "backfill_v1"),
            "confidence_reason": latest_confidence["confidence_reason"] if latest_confidence else "Backfilled from canonical heuristic components.",
            "confidence_components": components,
            "timestamp": timestamp,
            "origin": self._normalize_origin(row["source"], row["origin"]),
            "parent_id": row["parent_id"],
            "root_id": row["root_id"] or row["id"],
            "merged_into": row["merged_into"],
            "created_at": row["created_at"] or row["date"] or timestamp,
            "updated_at": row["updated_at"] or timestamp,
            "legacy_source": row["source"],
            "evidence": row["evidence"],
            "tags": json.loads(row["tags"]) if row["tags"] else [],
            "context_hypotheses": json.loads(row["context_hypotheses"]) if row["context_hypotheses"] else [],
            "context_domains": json.loads(row["context_domains"]) if row["context_domains"] else [],
        }

    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        log.info(f"Memory Manager binding to SQL DB: {self.db_path}")
        self._ensure_tables()
        self.backfill_hypothesis_foundations()
        self.backfill_memory_foundations()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self):
        conn = ensure_runtime_db(self.db_path, migrate=False)
        conn.close()

    def _sync_supported_hypothesis_material(self, conn: sqlite3.Connection, hypothesis_id: str | None):
        if not hypothesis_id:
            return

        row = conn.execute(
            "SELECT evidence, context_domains FROM hypotheses WHERE id=?",
            (hypothesis_id,),
        ).fetchone()
        if not row:
            return

        existing_evidence = []
        try:
            parsed = json.loads(row["evidence"] or "[]")
            if isinstance(parsed, list):
                existing_evidence = [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            existing_evidence = []

        domain_set = set()
        try:
            parsed_domains = json.loads(row["context_domains"] or "[]")
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

        for support_row in support_rows:
            content = str(support_row["content"] or "").strip()
            if content and content not in seen:
                combined_evidence.append(content)
                seen.add(content)
            try:
                parsed_domains = json.loads(support_row["domain_tags"] or "[]")
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

    def add_memory(self, memory: dict):
        domain_tags = memory.get("domain_tags") or self._detect_memory_domains(memory.get("summary", ""), memory.get("entities", []))
        supports = memory.get("supports_hypothesis")
        challenges = memory.get("challenges_hypothesis")
        if not supports and not challenges:
            supports, challenges = self._infer_memory_hypothesis_link(memory.get("summary", ""), memory.get("entities", []))
        with self._get_conn() as c:
            cursor = c.execute('''
                INSERT INTO memories (timestamp, content, concept_tags, significance, domain_tags, supports_hypothesis, challenges_hypothesis)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                memory['timestamp'], memory.get('summary', ''),
                json.dumps(memory.get('entities', [])), self._clamp_confidence(memory.get('confidence', memory.get('importance', 1))),
                json.dumps(domain_tags, ensure_ascii=False),
                supports, challenges
            ))
            self._sync_supported_hypothesis_material(c, supports)
            c.commit()
            memory_id = cursor.lastrowid
        log.info("SQL Memory #%s saved: %s", memory_id, memory.get("summary", "")[:60])
        return memory_id

    def add_auto_hypothesis(self, hypothesis: dict):
        timestamp = hypothesis.get('timestamp', datetime.now().isoformat())
        with self._get_conn() as c:
            c.execute('''
                INSERT INTO hypotheses (id, title, description, evidence, status, source, date, origin, parent_id, root_id, merged_into, created_at, updated_at, confidence, confidence_components, confidence_source, context_hypotheses, context_domains)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                hypothesis['id'], hypothesis['title'], hypothesis.get('body', ''),
                json.dumps(hypothesis.get('evidence', []), ensure_ascii=False),
                hypothesis.get('status', 'proposed'), 'Agent Auto', timestamp,
                hypothesis.get('origin', 'agent_auto'), hypothesis.get('parent_id'),
                hypothesis.get('root_id', hypothesis['id']), hypothesis.get('merged_into'),
                timestamp, timestamp, hypothesis.get('confidence'),
                json.dumps(hypothesis.get('confidence_components', {}), ensure_ascii=False),
                hypothesis.get('confidence_source', 'proposal'),
                json.dumps(hypothesis.get('context_hypotheses', []), ensure_ascii=False),
                json.dumps(hypothesis.get('context_domains', []), ensure_ascii=False),
            ))
            c.commit()
        confidence = hypothesis.get("confidence")
        if confidence is not None:
            self.record_confidence(
                hypothesis['id'],
                self._clamp_confidence(confidence),
                source="proposal",
                reason=hypothesis.get("confidence_reason", "Initial hypothesis proposal confidence"),
            )
        log.info("SQL Auto-hypothesis saved: %s", hypothesis["title"])

    def get_memories(self) -> list:
        with self._get_conn() as c:
            rows = c.execute("SELECT * FROM memories ORDER BY timestamp DESC LIMIT 100").fetchall()
            return [{
                "id": r["id"],
                "timestamp": r["timestamp"],
                "summary": r["content"],
                "entities": json.loads(r["concept_tags"]),
                "domain_tags": json.loads(r["domain_tags"]) if r["domain_tags"] else [],
                "confidence": self._clamp_confidence(r["significance"]),
                "confidence_label": self._confidence_label(self._clamp_confidence(r["significance"])),
                "supports_hypothesis": r["supports_hypothesis"],
                "challenges_hypothesis": r["challenges_hypothesis"],
            } for r in rows]

    def get_memories_by_ids(self, memory_ids: list[int]) -> list:
        if not memory_ids:
            return []
        memory_ids = memory_ids[:200]  # Cap to prevent query explosion
        placeholders = ",".join("?" for _ in memory_ids)
        with self._get_conn() as c:
            rows = c.execute(
                f"SELECT * FROM memories WHERE id IN ({placeholders}) ORDER BY timestamp DESC",
                tuple(memory_ids),
            ).fetchall()
            return [{
                "id": r["id"],
                "timestamp": r["timestamp"],
                "summary": r["content"],
                "entities": json.loads(r["concept_tags"]),
                "domain_tags": json.loads(r["domain_tags"]) if r["domain_tags"] else [],
                "confidence": self._clamp_confidence(r["significance"]),
                "confidence_label": self._confidence_label(self._clamp_confidence(r["significance"])),
                "supports_hypothesis": r["supports_hypothesis"],
                "challenges_hypothesis": r["challenges_hypothesis"],
            } for r in rows]

    def get_latest_confidence(self, hypothesis_id: str) -> dict | None:
        with self._get_conn() as c:
            row = c.execute(
                "SELECT confidence, source, reason, timestamp FROM confidence_history WHERE hypothesis_id=? ORDER BY timestamp DESC LIMIT 1",
                (hypothesis_id,),
            ).fetchone()
            if not row:
                return None
            score = self._clamp_confidence(row["confidence"])
            return {
                "confidence": score,
                "confidence_label": self._confidence_label(score),
                "confidence_source": row["source"],
                "confidence_reason": row["reason"],
                "confidence_timestamp": row["timestamp"],
            }

    def get_auto_hypotheses(self) -> list:
        hypotheses = self.get_all_hypotheses(normalized=True, origin="agent_auto")
        return [{
            "id": h["id"],
            "title": h["title"],
            "content": h["text"],
            "body": h["text"],
            "status": h["status"],
            "timestamp": h["timestamp"],
            "origin": h["origin"],
            "parent_id": h["parent_id"],
            "root_id": h["root_id"],
            "merged_into": h["merged_into"],
            "confidence": h["confidence"],
            "confidence_label": h["confidence_label"],
            "confidence_source": h["confidence_source"],
            "confidence_reason": h["confidence_reason"],
            "confidence_components": h["confidence_components"],
            "context_hypotheses": h["context_hypotheses"],
            "context_domains": h["context_domains"],
        } for h in hypotheses]

    def backfill_memory_foundations(self) -> dict:
        self._ensure_tables()
        stats = {"updated": 0, "domains_inferred": 0, "supports_linked": 0, "challenges_linked": 0, "proposals_created": 0, "proposals_updated": 0}
        hypotheses = self.get_all_hypotheses(normalized=True)
        with self._get_conn() as c:
            rows = c.execute("SELECT * FROM memories").fetchall()
            for row in rows:
                entities = json.loads(row["concept_tags"]) if row["concept_tags"] else []
                domain_tags = json.loads(row["domain_tags"]) if row["domain_tags"] else []
                if not domain_tags:
                    domain_tags = self._detect_memory_domains(row["content"], entities)
                    if domain_tags:
                        stats["domains_inferred"] += 1

                supports = row["supports_hypothesis"]
                challenges = row["challenges_hypothesis"]
                if not supports and not challenges:
                    inferred_support, inferred_challenge = self._infer_memory_hypothesis_link(row["content"], entities, hypotheses=hypotheses)
                    supports = inferred_support or supports
                    challenges = inferred_challenge or challenges
                    if inferred_support:
                        stats["supports_linked"] += 1
                    if inferred_challenge:
                        stats["challenges_linked"] += 1

                if not supports and not challenges:
                    ranked = self._rank_memory_hypothesis_links(row["content"], entities, hypotheses=hypotheses)
                    if ranked:
                        top_candidate = ranked[0]
                        if top_candidate["score"] >= 2.5 and not top_candidate["direct_reference"]:
                            proposal_action = self._upsert_memory_link_proposal(c, row["id"], top_candidate)
                            if proposal_action == "created":
                                stats["proposals_created"] += 1
                            elif proposal_action == "updated":
                                stats["proposals_updated"] += 1

                c.execute(
                    "UPDATE memories SET domain_tags=?, supports_hypothesis=?, challenges_hypothesis=? WHERE id=?",
                    (json.dumps(domain_tags, ensure_ascii=False), supports, challenges, row["id"]),
                )
                stats["updated"] += 1
            c.commit()
        return stats

    def generate_memory_link_proposals(self, limit: int = 20, min_score: float = 2.5, memory_domain: str | None = None, relation: str | None = None) -> dict:
        self._ensure_tables()
        created = 0
        updated = 0
        scanned = 0
        hypotheses = self.get_all_hypotheses(normalized=True)
        with self._get_conn() as c:
            rows = c.execute("SELECT * FROM memories ORDER BY timestamp DESC").fetchall()
            for row in rows:
                if created + updated >= max(1, int(limit)):
                    break
                scanned += 1
                if row["supports_hypothesis"] or row["challenges_hypothesis"]:
                    continue
                entities = json.loads(row["concept_tags"]) if row["concept_tags"] else []
                domains = json.loads(row["domain_tags"]) if row["domain_tags"] else self._detect_memory_domains(row["content"], entities)
                if memory_domain and memory_domain != "all" and memory_domain not in domains:
                    continue
                ranked = self._rank_memory_hypothesis_links(row["content"], entities, hypotheses=hypotheses)
                for candidate in ranked:
                    if candidate["score"] < float(min_score):
                        continue
                    if candidate["direct_reference"] or candidate["score"] >= 4.5:
                        continue
                    if relation and relation != "all" and candidate["relation"] != relation:
                        continue
                    proposal_action = self._upsert_memory_link_proposal(c, row["id"], candidate)
                    if proposal_action == "created":
                        created += 1
                    elif proposal_action == "updated":
                        updated += 1
                    if proposal_action:
                        break
            c.commit()
        pending = len(self.get_memory_link_proposals(status="pending", domain=memory_domain, relation=relation, limit=500))
        return {"scanned": scanned, "created": created, "updated": updated, "pending": pending}

    def get_memory_link_proposals(self, status: str | None = "pending", domain: str | None = None, relation: str | None = None, limit: int = 50) -> list[dict]:
        self._ensure_tables()
        query = '''
            SELECT p.*, m.timestamp AS memory_timestamp, m.content AS memory_content, m.concept_tags, m.domain_tags,
                   m.significance, m.supports_hypothesis, m.challenges_hypothesis,
                   h.title AS hypothesis_title, h.status AS hypothesis_status, h.confidence AS hypothesis_confidence
            FROM memory_link_proposals p
            JOIN memories m ON m.id = p.memory_id
            LEFT JOIN hypotheses h ON h.id = p.hypothesis_id
        '''
        clauses = []
        params: list = []
        if status and status != "all":
            clauses.append("p.status=?")
            params.append(status)
        if relation and relation != "all":
            clauses.append("p.relation=?")
            params.append(relation)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY CASE p.status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 WHEN 'rejected' THEN 2 ELSE 3 END, p.score DESC, p.proposed_at DESC LIMIT ?"
        params.append(int(limit))

        with self._get_conn() as c:
            rows = c.execute(query, tuple(params)).fetchall()

        proposals = []
        for row in rows:
            memory_domains = json.loads(row["domain_tags"]) if row["domain_tags"] else []
            if domain and domain != "all" and domain not in memory_domains:
                continue
            memory_confidence = self._clamp_confidence(row["significance"])
            proposals.append({
                "id": row["id"],
                "memory_id": row["memory_id"],
                "hypothesis_id": row["hypothesis_id"],
                "hypothesis_title": row["hypothesis_title"] or row["hypothesis_id"],
                "hypothesis_status": row["hypothesis_status"] or "unknown",
                "hypothesis_confidence": self._clamp_confidence(row["hypothesis_confidence"]),
                "relation": row["relation"],
                "score": round(float(row["score"] or 0.0), 3),
                "rationale": row["rationale"] or "",
                "status": row["status"],
                "source": row["source"],
                "proposed_at": row["proposed_at"],
                "reviewed_at": row["reviewed_at"],
                "reviewer_note": row["reviewer_note"],
                "memory_timestamp": row["memory_timestamp"],
                "memory_summary": row["memory_content"],
                "memory_entities": json.loads(row["concept_tags"]) if row["concept_tags"] else [],
                "memory_domains": memory_domains,
                "memory_confidence": memory_confidence,
                "memory_confidence_label": self._confidence_label(memory_confidence),
                "memory_linked_support": row["supports_hypothesis"],
                "memory_linked_challenge": row["challenges_hypothesis"],
            })
        return proposals

    def review_memory_link_proposal(self, proposal_id: int, decision: str, reviewer_note: str = "") -> dict | None:
        self._ensure_tables()
        normalized_decision = (decision or "").strip().lower()
        if normalized_decision not in {"approve", "reject"}:
            raise ValueError("decision must be 'approve' or 'reject'")

        with self._get_conn() as c:
            proposal = c.execute(
                "SELECT * FROM memory_link_proposals WHERE id=?",
                (int(proposal_id),),
            ).fetchone()
            if not proposal:
                return None

            reviewed_at = datetime.now().isoformat()
            final_status = "approved" if normalized_decision == "approve" else "rejected"
            c.execute(
                "UPDATE memory_link_proposals SET status=?, reviewed_at=?, reviewer_note=? WHERE id=?",
                (final_status, reviewed_at, reviewer_note, int(proposal_id)),
            )

            if normalized_decision == "approve":
                if proposal["relation"] == "support":
                    c.execute(
                        "UPDATE memories SET supports_hypothesis=? WHERE id=?",
                        (proposal["hypothesis_id"], proposal["memory_id"]),
                    )
                else:
                    c.execute(
                        "UPDATE memories SET challenges_hypothesis=? WHERE id=?",
                        (proposal["hypothesis_id"], proposal["memory_id"]),
                    )
                c.execute(
                    "UPDATE memory_link_proposals SET status='superseded', reviewed_at=?, reviewer_note=? WHERE memory_id=? AND relation=? AND status='pending' AND id != ?",
                    (reviewed_at, "Superseded by approved proposal", proposal["memory_id"], proposal["relation"], int(proposal_id)),
                )
            c.commit()

        refreshed = self.get_memory_link_proposals(status="all", limit=200)
        return next((item for item in refreshed if int(item["id"]) == int(proposal_id)), None)

    def _classify_evidence_text(self, text: str) -> tuple[str, str]:
        from hypothesis_council import EvidenceReviewerAgent

        reviewer = EvidenceReviewerAgent()
        return reviewer.classify_item(text or "")

    def sync_evidence_requests_for_hypothesis(self, hypothesis_id: str, requests: list[dict], triggering_decision: str = "held") -> list[dict]:
        self._ensure_tables()
        timestamp = datetime.now().isoformat()
        with self._get_conn() as c:
            for request in requests:
                request_text = (request.get("request_text") or "").strip()
                if not request_text:
                    continue
                source_context = json.dumps(request.get("source_context", {}), ensure_ascii=False)
                existing = c.execute(
                    "SELECT id FROM evidence_requests WHERE hypothesis_id=? AND request_text=?",
                    (hypothesis_id, request_text),
                ).fetchone()
                if existing:
                    c.execute(
                        """
                        UPDATE evidence_requests
                        SET priority=?, source_agent=?, source_context=?, status='pending',
                            triggering_decision=?, updated_at=?, resolved_at=NULL,
                            resolution_note=NULL, satisfied_by_memory_ids=NULL
                        WHERE id=?
                        """,
                        (
                            request.get("priority", "medium"),
                            request.get("source_agent"),
                            source_context,
                            triggering_decision,
                            timestamp,
                            existing["id"],
                        ),
                    )
                else:
                    c.execute(
                        """
                        INSERT INTO evidence_requests (
                            hypothesis_id, request_text, priority, source_agent, source_context,
                            status, triggering_decision, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                        """,
                        (
                            hypothesis_id,
                            request_text,
                            request.get("priority", "medium"),
                            request.get("source_agent"),
                            source_context,
                            triggering_decision,
                            timestamp,
                            timestamp,
                        ),
                    )
            c.commit()
        return self.get_evidence_requests(status="pending", hypothesis_id=hypothesis_id, limit=50)

    def dismiss_pending_evidence_requests(self, hypothesis_id: str, reason: str = "") -> int:
        self._ensure_tables()
        timestamp = datetime.now().isoformat()
        with self._get_conn() as c:
            cursor = c.execute(
                """
                UPDATE evidence_requests
                SET status='dismissed', updated_at=?, resolved_at=?, resolution_note=COALESCE(NULLIF(?, ''), resolution_note)
                WHERE hypothesis_id=? AND status='pending'
                """,
                (timestamp, timestamp, reason, hypothesis_id),
            )
            c.commit()
            return cursor.rowcount or 0

    def get_material_evidence_since(self, hypothesis_id: str, since_timestamp: str | None, limit: int = 20) -> list[dict]:
        self._ensure_tables()
        query = (
            "SELECT * FROM memories WHERE (supports_hypothesis=? OR challenges_hypothesis=?)"
            + (" AND timestamp > ?" if since_timestamp else "")
            + " ORDER BY timestamp ASC LIMIT ?"
        )
        params: list = [hypothesis_id, hypothesis_id]
        if since_timestamp:
            params.append(since_timestamp)
        params.append(int(limit))

        with self._get_conn() as c:
            rows = c.execute(query, tuple(params)).fetchall()

        material = []
        for row in rows:
            tier, rationale = self._classify_evidence_text(row["content"] or "")
            if tier not in {"tier_a", "tier_b"}:
                continue
            confidence = self._clamp_confidence(row["significance"])
            material.append({
                "memory_id": row["id"],
                "timestamp": row["timestamp"],
                "summary": row["content"],
                "tier": tier,
                "rationale": rationale,
                "relation": "support" if row["supports_hypothesis"] == hypothesis_id else "challenge",
                "confidence": confidence,
                "confidence_label": self._confidence_label(confidence),
            })
        return material

    def get_evidence_requests(self, status: str | None = "pending", hypothesis_id: str | None = None, limit: int = 100) -> list[dict]:
        self._ensure_tables()
        query = """
            SELECT er.*, h.title AS hypothesis_title, h.status AS hypothesis_status
            FROM evidence_requests er
            LEFT JOIN hypotheses h ON h.id = er.hypothesis_id
        """
        clauses = []
        params: list = []
        if status and status != "all":
            clauses.append("er.status=?")
            params.append(status)
        if hypothesis_id:
            clauses.append("er.hypothesis_id=?")
            params.append(hypothesis_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY CASE er.status WHEN 'pending' THEN 0 WHEN 'completed' THEN 1 WHEN 'dismissed' THEN 2 ELSE 3 END, CASE er.priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, er.updated_at DESC LIMIT ?"
        params.append(int(limit))

        with self._get_conn() as c:
            rows = c.execute(query, tuple(params)).fetchall()

        requests = []
        for row in rows:
            latest_decision = self.get_decision_for_hypothesis(row["hypothesis_id"])
            material_evidence = self.get_material_evidence_since(row["hypothesis_id"], latest_decision["timestamp"] if latest_decision else None, limit=5)
            requests.append({
                "id": row["id"],
                "hypothesis_id": row["hypothesis_id"],
                "hypothesis_title": row["hypothesis_title"] or row["hypothesis_id"],
                "hypothesis_status": row["hypothesis_status"] or "unknown",
                "request_text": row["request_text"],
                "priority": row["priority"],
                "source_agent": row["source_agent"],
                "source_context": json.loads(row["source_context"]) if row["source_context"] else {},
                "status": row["status"],
                "triggering_decision": row["triggering_decision"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "resolved_at": row["resolved_at"],
                "resolution_note": row["resolution_note"],
                "satisfied_by_memory_ids": json.loads(row["satisfied_by_memory_ids"]) if row["satisfied_by_memory_ids"] else [],
                "ready_for_rereview": bool(material_evidence) and (row["hypothesis_status"] == "held"),
                "material_evidence": material_evidence,
            })
        return requests

    def review_evidence_request(self, request_id: int, decision: str, resolution_note: str = "", satisfied_memory_ids: list[int] | None = None) -> dict | None:
        self._ensure_tables()
        normalized = (decision or "").strip().lower()
        if normalized not in {"complete", "dismiss"}:
            raise ValueError("decision must be 'complete' or 'dismiss'")
        status = "completed" if normalized == "complete" else "dismissed"
        timestamp = datetime.now().isoformat()

        with self._get_conn() as c:
            existing = c.execute("SELECT id FROM evidence_requests WHERE id=?", (int(request_id),)).fetchone()
            if not existing:
                return None
            c.execute(
                """
                UPDATE evidence_requests
                SET status=?, updated_at=?, resolved_at=?, resolution_note=?, satisfied_by_memory_ids=?
                WHERE id=?
                """,
                (
                    status,
                    timestamp,
                    timestamp,
                    resolution_note,
                    json.dumps(satisfied_memory_ids or []),
                    int(request_id),
                ),
            )
            c.commit()

        refreshed = self.get_evidence_requests(status="all", limit=200)
        return next((item for item in refreshed if int(item["id"]) == int(request_id)), None)

    def get_evidence_request_summary(self) -> dict:
        self._ensure_tables()
        with self._get_conn() as c:
            total = c.execute("SELECT COUNT(*) AS c FROM evidence_requests").fetchone()["c"]
            pending = c.execute("SELECT COUNT(*) AS c FROM evidence_requests WHERE status='pending'").fetchone()["c"]
            completed = c.execute("SELECT COUNT(*) AS c FROM evidence_requests WHERE status='completed'").fetchone()["c"]
            dismissed = c.execute("SELECT COUNT(*) AS c FROM evidence_requests WHERE status='dismissed'").fetchone()["c"]
        return {
            "total": int(total or 0),
            "pending": int(pending or 0),
            "completed": int(completed or 0),
            "dismissed": int(dismissed or 0),
        }

    def set_auto_hypothesis_status(self, hypothesis_id: str, new_status: str) -> dict | None:
        """Update status for an agent-generated hypothesis and return summary info."""
        with self._get_conn() as c:
            row = c.execute(
                "SELECT id, title, status FROM hypotheses WHERE id=? AND source='Agent Auto'",
                (hypothesis_id,),
            ).fetchone()
            if not row:
                return None
            c.execute("UPDATE hypotheses SET status=?, updated_at=? WHERE id=?", (new_status, datetime.now().isoformat(), hypothesis_id))
            c.commit()
            status_confidence = {
                "accepted": 0.8,
                "needs_revision": 0.55,
                "held": 0.4,
                "rejected": 0.15,
                "rejected_auto": 0.0,
                "merged": 0.9,
            }.get(new_status)
            if status_confidence is not None:
                self.record_confidence(hypothesis_id, status_confidence, source="status_update", reason=f"Status changed to {new_status}")
            return {"id": row["id"], "title": row["title"], "previous_status": row["status"], "status": new_status}

    def get_founding_hypotheses(self) -> list:
        hypotheses = self.get_all_hypotheses(normalized=True)
        founding = [h for h in hypotheses if h["origin"] != "agent_auto"]
        return [{
            "id": h["id"],
            "title": h["title"],
            "desc": h["text"],
            "evidence": h["evidence"],
            "tags": h["tags"],
            "status": h["status"],
            "confidence": h["confidence"],
            "confidence_label": h["confidence_label"],
            "origin": h["origin"],
            "timestamp": h["timestamp"],
        } for h in founding]

    def get_all_hypotheses(self, normalized: bool = True, status: str | None = None, origin: str | None = None, root_id: str | None = None, active_only: bool = False) -> list:
        self._ensure_tables()
        query = "SELECT * FROM hypotheses"
        clauses = []
        params = []
        if status:
            clauses.append("status=?")
            params.append(status)
        if origin:
            clauses.append("origin=?")
            params.append(origin)
        if root_id:
            clauses.append("root_id=?")
            params.append(root_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY COALESCE(updated_at, date, created_at) DESC, id DESC"
        with self._get_conn() as c:
            rows = c.execute(query, tuple(params)).fetchall()
        hypotheses = [self._canonical_hypothesis_from_row(row) if normalized else dict(row) for row in rows]
        if active_only:
            hypotheses = [h for h in hypotheses if h["status"] not in {"archived", "merged", "rejected", "rejected_auto"}]
        return hypotheses

    def backfill_hypothesis_foundations(self) -> dict:
        self._ensure_tables()
        stats = {"updated": 0, "confidence_seeded": 0}
        with self._get_conn() as c:
            rows = c.execute("SELECT * FROM hypotheses").fetchall()
            for row in rows:
                latest_confidence = self.get_latest_confidence(row["id"])
                origin = self._normalize_origin(row["source"], row["origin"])
                created_at = row["created_at"] or row["date"] or datetime.now().isoformat()
                updated_at = row["updated_at"] or latest_confidence["confidence_timestamp"] if latest_confidence else row["date"] or created_at
                parent_id = row["parent_id"]
                root_id = row["root_id"] or parent_id or row["id"]
                components = self._estimate_hypothesis_confidence_components(row, latest_confidence)
                if row["confidence"] is None:
                    confidence = latest_confidence["confidence"] if latest_confidence else self._weighted_hypothesis_confidence(components)
                    confidence_source = row["confidence_source"] or (latest_confidence["confidence_source"] if latest_confidence else "backfill_v1")
                    if not latest_confidence:
                        c.execute(
                            "INSERT INTO confidence_history (hypothesis_id, confidence, source, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
                            (row["id"], confidence, "backfill_v1", "Sprint A canonical backfill seeded current confidence.", datetime.now().isoformat()),
                        )
                        stats["confidence_seeded"] += 1
                else:
                    confidence = row["confidence"]
                    confidence_source = row["confidence_source"] or (latest_confidence["confidence_source"] if latest_confidence else "backfill_v1")

                c.execute(
                    "UPDATE hypotheses SET origin=?, root_id=?, created_at=?, updated_at=?, confidence=?, confidence_components=?, confidence_source=? WHERE id=?",
                    (
                        origin,
                        root_id,
                        created_at,
                        updated_at,
                        self._clamp_confidence(confidence),
                        json.dumps(components, ensure_ascii=False),
                        confidence_source,
                        row["id"],
                    ),
                )
                stats["updated"] += 1
            c.commit()
        return stats

    def get_simulation_queue(self) -> list:
        with self._get_conn() as c:
            rows = c.execute("SELECT * FROM simulations ORDER BY queued_at DESC").fetchall()
            return [{"id": r["id"], "parameters": json.loads(r["parameters"]), 
                     "status": r["status"], "requested_at": r["queued_at"]} for r in rows]

    def add_simulation_task(self, task: dict):
        with self._get_conn() as c:
            c.execute('''
                INSERT INTO simulations (id, queued_at, parameters, status)
                VALUES (?, ?, ?, ?)
            ''', (
                task['id'], task.get('timestamp', datetime.now().isoformat()),
                json.dumps(task.get('recommendation', {})), task.get('status', 'pending')
            ))
            c.commit()
        log.info("SQL Simulation queued: %s", task.get("id"))

    def dequeue_simulation(self) -> dict | None:
        with self._get_conn() as c:
            row = c.execute("SELECT * FROM simulations WHERE status='pending' ORDER BY queued_at ASC LIMIT 1").fetchone()
            if row:
                c.execute("UPDATE simulations SET status='processing' WHERE id=?", (row["id"],))
                c.commit()
                log.info("SQL Simulation dequeued: %s", row["id"])
                return {"id": row["id"], "parameters": json.loads(row["parameters"])}
        return None

    def queue_mast_targets(self, targets: list[str]):
        """Inject target strings into the DB to signal mast_worker.py"""
        self._ensure_tables()
        with self._get_conn() as c:
            for tgt in targets:
                # Deduplicate pending queries
                row = c.execute("SELECT id FROM mast_queue WHERE target_name=? AND status='pending'", (tgt,)).fetchone()
                if not row:
                    c.execute('''
                        INSERT INTO mast_queue (target_name, queued_at, status)
                        VALUES (?, ?, ?)
                    ''', (tgt, datetime.now().isoformat(), 'pending'))
                    log.info("SQL MAST Telescope Target Queued: %s", tgt)
            c.commit()

    def queue_radio_targets(self, targets: list):
        """Inject radio target dicts {'target': '...', 'type': 'ALMA'/'SETI'} into the DB for radio_worker.py"""
        self._ensure_tables()
        with self._get_conn() as c:
            for item in targets:
                tgt = item.get("target")
                tgt_type = item.get("type")
                if not tgt or not tgt_type: continue
                # Deduplicate pending queries
                row = c.execute("SELECT id FROM radio_queue WHERE target_name=? AND target_type=? AND status='pending'", (tgt, tgt_type)).fetchone()
                if not row:
                    c.execute('''
                        INSERT INTO radio_queue (target_name, target_type, queued_at, status)
                        VALUES (?, ?, ?, ?)
                    ''', (tgt, tgt_type, datetime.now().isoformat(), 'pending'))
                    log.info("SQL %s Radio Target Queued: %s", tgt_type, tgt)
            c.commit()

    def add_chat_message(self, role: str, content: str, metadata: dict | None = None):
        self._ensure_tables()
        with self._get_conn() as c:
            c.execute(
                "INSERT INTO chat_history (timestamp, role, content, metadata) VALUES (?, ?, ?, ?)",
                (datetime.now().isoformat(), role, content, json.dumps(metadata or {}, ensure_ascii=False)),
            )
            c.commit()

    def get_chat_history(self, limit=20) -> list:
        self._ensure_tables()
        with self._get_conn() as c:
            # Get latest `limit` messages, but ordered chronologically overall
            rows = c.execute("SELECT * FROM chat_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            # Reverse them to get them in chronological order
            history = []
            for row in reversed(rows):
                metadata = {}
                if row["metadata"]:
                    try:
                        metadata = json.loads(row["metadata"])
                    except json.JSONDecodeError:
                        metadata = {}
                history.append({
                    "role": row["role"],
                    "content": row["content"],
                    "timestamp": row["timestamp"],
                    "metadata": metadata,
                })
            return history

    def get_stats(self) -> dict:
        with self._get_conn() as c:
            mems = c.execute("SELECT COUNT(*) as c FROM memories").fetchone()["c"]
            hyps = c.execute("SELECT COUNT(*) as c FROM hypotheses WHERE source = 'Agent Auto'").fetchone()["c"]
            founding = c.execute("SELECT COUNT(*) as c FROM hypotheses WHERE source != 'Agent Auto'").fetchone()["c"]
            override_count = c.execute("SELECT COUNT(*) as c FROM hypothesis_overrides").fetchone()["c"]
            evidence_request_count = c.execute("SELECT COUNT(*) as c FROM evidence_requests WHERE status='pending'").fetchone()["c"]
            last_override = c.execute("SELECT timestamp FROM hypothesis_overrides ORDER BY timestamp DESC LIMIT 1").fetchone()
            return {
                "total_memories": mems,
                "total_auto_hypotheses": hyps,
                "founding_hypotheses": founding,
                "override_count": override_count,
                "pending_evidence_requests": evidence_request_count,
                "last_override_at": last_override["timestamp"] if last_override else None,
            }

    def get_override_summary(self) -> dict:
        self._ensure_tables()
        with self._get_conn() as c:
            total = c.execute("SELECT COUNT(*) as c FROM hypothesis_overrides").fetchone()["c"]
            latest = c.execute(
                "SELECT hypothesis_id, previous_status, new_status, rationale, actor, timestamp FROM hypothesis_overrides ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            return {
                "total": total,
                "latest": {
                    "hypothesis_id": latest["hypothesis_id"],
                    "previous_status": latest["previous_status"],
                    "new_status": latest["new_status"],
                    "rationale": latest["rationale"],
                    "actor": latest["actor"],
                    "timestamp": latest["timestamp"],
                } if latest else None,
            }

    # ─── HYPOTHESIS EVOLUTION ────────────────────────────────────────
    def record_confidence(self, hypothesis_id: str, confidence: float, source: str = "consolidation", reason: str = ""):
        """Track a confidence data point for a hypothesis over time."""
        self._ensure_tables()
        with self._get_conn() as c:
            c.execute('INSERT INTO confidence_history (hypothesis_id, confidence, source, reason, timestamp) VALUES (?, ?, ?, ?, ?)',
                      (hypothesis_id, confidence, source, reason, datetime.now().isoformat()))
            c.execute(
                "UPDATE hypotheses SET confidence=?, confidence_source=?, updated_at=? WHERE id=?",
                (self._clamp_confidence(confidence), source, datetime.now().isoformat(), hypothesis_id),
            )
            c.commit()
        log.info("Confidence recorded: %s → %.2f (%s)", hypothesis_id, confidence, reason[:60] if reason else source)

    def get_confidence_history(self, hypothesis_id: str = None) -> list:
        """Return confidence history, optionally filtered by hypothesis."""
        self._ensure_tables()
        with self._get_conn() as c:
            if hypothesis_id:
                rows = c.execute("SELECT * FROM confidence_history WHERE hypothesis_id=? ORDER BY timestamp ASC", (hypothesis_id,)).fetchall()
            else:
                rows = c.execute("SELECT * FROM confidence_history ORDER BY timestamp DESC LIMIT 200").fetchall()
            return [{"id": r["id"], "hypothesis_id": r["hypothesis_id"],
                     "confidence": r["confidence"], "source": r["source"],
                     "reason": r["reason"], "timestamp": r["timestamp"]} for r in rows]

    def auto_promote_hypotheses(self):
        """Promote hypotheses with 3+ supporting memories; flag those with 2+ contradictions."""
        with self._get_conn() as c:
            hyps = c.execute("SELECT id, title, status FROM hypotheses").fetchall()
            for h in hyps:
                hid = h["id"]
                # Count supporting memories
                supports = c.execute("SELECT COUNT(*) as c FROM memories WHERE content LIKE ?", (f"%supports {hid}%",)).fetchone()["c"]
                # Count contradicting memories  
                contradicts = c.execute("SELECT COUNT(*) as c FROM memories WHERE content LIKE ?", (f"%challenges {hid}%",)).fetchone()["c"]
                
                if supports >= 3 and h["status"] not in ("promoted", "validated"):
                    c.execute("UPDATE hypotheses SET status='promoted' WHERE id=?", (hid,))
                    log.info("⬆ Auto-promoted hypothesis %s (%s) — %d supporting memories", hid, h["title"], supports)
                elif contradicts >= 2 and h["status"] not in ("flagged", "rejected"):
                    c.execute("UPDATE hypotheses SET status='flagged' WHERE id=?", (hid,))
                    log.warning("⚠ Auto-flagged hypothesis %s (%s) — %d contradictions", hid, h["title"], contradicts)
            c.commit()

    # ─── DEAD LETTER QUEUE ───────────────────────────────────────────
    def record_dead_letter(self, filename: str, error: str):
        """Track files that fail ingestion. Returns current attempt count."""
        self._ensure_tables()
        with self._get_conn() as c:
            existing = c.execute("SELECT id, attempts FROM dead_letters WHERE filename=?", (filename,)).fetchone()
            now = datetime.now().isoformat()
            if existing:
                new_attempts = existing["attempts"] + 1
                c.execute("UPDATE dead_letters SET attempts=?, last_seen=?, error=? WHERE id=?",
                          (new_attempts, now, error, existing["id"]))
                c.commit()
                return new_attempts
            else:
                c.execute("INSERT INTO dead_letters (filename, error, attempts, first_seen, last_seen) VALUES (?, ?, 1, ?, ?)",
                          (filename, error, now, now))
                c.commit()
                return 1

    def is_dead_letter(self, filename: str, max_attempts: int = 3) -> bool:
        """Check if a file has exceeded max ingestion attempts."""
        self._ensure_tables()
        with self._get_conn() as c:
            row = c.execute("SELECT attempts FROM dead_letters WHERE filename=?", (filename,)).fetchone()
            return row is not None and row["attempts"] >= max_attempts

    def get_mast_queue_stats(self) -> dict:
        """Get stats from the MAST queue for the observatory dashboard."""
        with self._get_conn() as c:
            self._ensure_tables()
            rows = c.execute("SELECT * FROM mast_queue ORDER BY queued_at DESC").fetchall()
            return [{"id": r["id"], "target": r["target_name"], "status": r["status"],
                     "queued_at": r["queued_at"]} for r in rows]

    # ─── HYPOTHESIS REVIEW COUNCIL (Phase 18) ────────────────────────
    def save_review(self, hypothesis_id: str, agent_name: str, review: dict):
        """Persist a single council agent's review."""
        self._ensure_tables()
        review_details = {
            key: value
            for key, value in review.items()
            if key not in {"agent", "verdict", "reasoning", "objections", "score_contributions"}
        }
        with self._get_conn() as c:
            c.execute('''
                INSERT INTO hypothesis_reviews (hypothesis_id, agent_name, verdict, reasoning, objections, score_contributions, review_details, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                hypothesis_id, agent_name, review.get('verdict', ''),
                review.get('reasoning', ''), json.dumps(review.get('objections', [])),
                json.dumps(review.get('score_contributions', {})),
                json.dumps(review_details, ensure_ascii=False),
                datetime.now().isoformat()
            ))
            c.commit()

    def save_decision(self, hypothesis_id: str, decision: str, score: float, breakdown: dict, reasoning: str, merged_with: str = None):
        """Persist the Judge's final decision."""
        self._ensure_tables()
        with self._get_conn() as c:
            c.execute('''
                INSERT INTO hypothesis_decisions (hypothesis_id, decision, final_score, score_breakdown, merged_with, reasoning, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                hypothesis_id, decision, score, json.dumps(breakdown),
                merged_with, reasoning, datetime.now().isoformat()
            ))
            # Also update the hypothesis status in the main table
            c.execute("UPDATE hypotheses SET status=?, merged_into=?, updated_at=? WHERE id=?", (decision, merged_with, datetime.now().isoformat(), hypothesis_id))
            c.commit()
        self.record_confidence(hypothesis_id, self._clamp_confidence(score), source="council_decision", reason=reasoning[:240])

    def get_reviews_for_hypothesis(self, hypothesis_id: str) -> list:
        """Return all council agent reviews for a hypothesis."""
        self._ensure_tables()
        with self._get_conn() as c:
            rows = c.execute("SELECT * FROM hypothesis_reviews WHERE hypothesis_id=? ORDER BY timestamp ASC", (hypothesis_id,)).fetchall()
            return [{
                "agent": r["agent_name"], "verdict": r["verdict"],
                "reasoning": r["reasoning"],
                "objections": json.loads(r["objections"]) if r["objections"] else [],
                "score_contributions": json.loads(r["score_contributions"]) if r["score_contributions"] else {},
                "details": json.loads(r["review_details"]) if "review_details" in r.keys() and r["review_details"] else {},
                "timestamp": r["timestamp"]
            } for r in rows]

    def get_decision_for_hypothesis(self, hypothesis_id: str) -> dict | None:
        """Return the latest decision for a hypothesis."""
        self._ensure_tables()
        with self._get_conn() as c:
            r = c.execute("SELECT * FROM hypothesis_decisions WHERE hypothesis_id=? ORDER BY timestamp DESC LIMIT 1", (hypothesis_id,)).fetchone()
            if not r:
                return None
            return {
                "decision": r["decision"], "final_score": r["final_score"],
                "score_breakdown": json.loads(r["score_breakdown"]) if r["score_breakdown"] else {},
                "merged_with": r["merged_with"], "reasoning": r["reasoning"],
                "timestamp": r["timestamp"]
            }

    # ─── HIGH-ENERGY TRANSIENTS (Phase 19) ───────────────────────────
    def add_transient_record(self, record: dict):
        """Persist a high-energy flux measurement."""
        self._ensure_tables()
        with self._get_conn() as c:
            c.execute('''
                INSERT INTO transients (source, target, flux, error, energy_band, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                record['source'], record['target'], record.get('flux'),
                record.get('error'), record.get('energy_band'),
                record.get('timestamp', datetime.now().isoformat())
            ))
            c.commit()
        log.info("SQL Transient record saved: %s | %s | %.4f", record['source'], record['target'], record.get('flux', 0))

    def get_transients(self, target: str = None, limit: int = 50) -> list:
        """Fetch historical transient data."""
        self._ensure_tables()
        with self._get_conn() as c:
            if target:
                rows = c.execute("SELECT * FROM transients WHERE target=? ORDER BY timestamp DESC LIMIT ?", (target, limit)).fetchall()
            else:
                rows = c.execute("SELECT * FROM transients ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ─── MISSION TRACKER (Phase 20) ──────────────────────────────────
    def add_mission_record(self, record: dict):
        """Persist mission telemetry (Artemis II, etc.)."""
        self._ensure_tables()
        with self._get_conn() as c:
            c.execute('''
                INSERT INTO missions (mission_id, mission_name, timestamp, ra, dec, alt, az, dist_au, dist_mi, vel_km_s, vel_mph, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record['mission_id'], record['mission_name'],
                record.get('timestamp', datetime.now().isoformat()),
                record.get('ra'), record.get('dec'),
                record.get('alt'), record.get('az'),
                record.get('dist_au'), record.get('dist_mi'),
                record.get('vel_km_s'), record.get('vel_mph'),
                record.get('status', 'active')
            ))
            c.commit()
        log.info("SQL Mission record saved: %s | Alt: %.2f", record['mission_name'], record.get('alt', 0))

    def get_missions(self, mission_name: str = None, limit: int = 20) -> list:
        """Fetch historical mission trajectory data."""
        self._ensure_tables()
        with self._get_conn() as c:
            if mission_name:
                rows = c.execute("SELECT * FROM missions WHERE mission_name=? ORDER BY timestamp DESC LIMIT ?", (mission_name, limit)).fetchall()
            else:
                rows = c.execute("SELECT * FROM missions ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_latest_transient_flux(self, target: str, source: str) -> dict | None:
        """Fetch the most recent measurement for a specific target/source combination."""
        self._ensure_tables()
        with self._get_conn() as c:
            r = c.execute("SELECT * FROM transients WHERE target=? AND source=? ORDER BY timestamp DESC LIMIT 1", (target, source)).fetchone()
            return dict(r) if r else None

    def get_all_decisions(self, status_filter: str = None) -> list:
        """Return all hypothesis decisions, optionally filtered by status."""
        self._ensure_tables()
        with self._get_conn() as c:
            if status_filter:
                rows = c.execute("SELECT * FROM hypothesis_decisions WHERE decision=? ORDER BY timestamp DESC", (status_filter,)).fetchall()
            else:
                rows = c.execute("SELECT * FROM hypothesis_decisions ORDER BY timestamp DESC LIMIT 100").fetchall()
            return [{
                "hypothesis_id": r["hypothesis_id"], "decision": r["decision"],
                "final_score": r["final_score"],
                "score_breakdown": json.loads(r["score_breakdown"]) if r["score_breakdown"] else {},
                "merged_with": r["merged_with"], "reasoning": r["reasoning"],
                "timestamp": r["timestamp"]
            } for r in rows]

    def get_hypothesis_titles_and_bodies(self) -> list:
        """Return all hypothesis titles+descriptions for embedding comparison."""
        with self._get_conn() as c:
            rows = c.execute("SELECT id, title, description FROM hypotheses").fetchall()
            return [{"id": r["id"], "title": r["title"], "body": r["description"]} for r in rows]

    def update_hypothesis_status(self, hypothesis_id: str, new_status: str, rationale: str | None = None, actor: str = "human_override") -> dict | None:
        """Manually override a hypothesis status and optionally persist override rationale."""
        self._ensure_tables()
        with self._get_conn() as c:
            row = c.execute("SELECT id, title, status FROM hypotheses WHERE id=?", (hypothesis_id,)).fetchone()
            if not row:
                return None
            previous_status = row["status"]
            updated_at = datetime.now().isoformat()
            c.execute("UPDATE hypotheses SET status=?, updated_at=? WHERE id=?", (new_status, updated_at, hypothesis_id))
            if rationale:
                c.execute(
                    "INSERT INTO hypothesis_overrides (hypothesis_id, previous_status, new_status, rationale, actor, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    (hypothesis_id, previous_status, new_status, rationale, actor, updated_at),
                )
            c.commit()
        status_confidence = {
            "accepted": 0.8,
            "needs_revision": 0.55,
            "held": 0.4,
            "rejected": 0.15,
            "rejected_auto": 0.0,
            "merged": 0.9,
        }.get(new_status)
        if status_confidence is not None:
            confidence_reason = f"Status changed from {previous_status or 'unknown'} to {new_status}"
            if rationale:
                confidence_reason = f"{confidence_reason}. Override rationale: {rationale[:180]}"
            self.record_confidence(hypothesis_id, status_confidence, source="manual_override", reason=confidence_reason)
        log.info("Hypothesis %s status overridden to: %s", hypothesis_id, new_status)
        return {
            "id": row["id"],
            "title": row["title"],
            "previous_status": previous_status,
            "status": new_status,
            "rationale": rationale,
            "actor": actor,
        }


# ─── AGENT ACTIVITY LOG ─────────────────────────────────────────────
class AgentLog:
    """Append-only capped JSON log of agent activities."""

    def __init__(self, path: Path, max_entries=MAX_LOG_ENTRIES):
        self.path = path
        self.max_entries = max_entries
        if not self.path.exists():
            self._write([])

    def _read(self) -> list:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _write(self, entries: list):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(entries[-self.max_entries :], f, indent=2, ensure_ascii=False)

    def add(self, action: str, details: str = "", extra: dict = None):
        entries = self._read()
        entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "details": details,
        }
        if extra:
            entry.update(extra)
        entries.append(entry)
        self._write(entries)

    def recent(self, n=50) -> list:
        return self._read()[-n:]


# ─── FILENAME METADATA PARSER ────────────────────────────────────────
def parse_filename_metadata(filepath: Path) -> dict:
    """
    Extract simulation parameters from filename patterns.
    Examples:
      sgra_mass4.2_spin0.9_inc85.png
      great_attractor_render_batch3.png
      sgra_turbulence_v2.png
    """
    name = filepath.stem.lower()
    meta = {
        "filename": filepath.name,
        "extension": filepath.suffix.lower(),
        "size_bytes": filepath.stat().st_size if filepath.exists() else 0,
        "created": datetime.fromtimestamp(filepath.stat().st_ctime).isoformat()
        if filepath.exists()
        else None,
    }

    # Try to extract known simulation parameters
    patterns = {
        "mass": r"mass([\d.]+)",
        "spin": r"spin([\d.]+)",
        "inclination": r"inc(?:lination)?([\d.]+)",
        "distance": r"dist(?:ance)?([\d.]+)",
        "brightness": r"bright(?:ness)?([\d.]+)",
        "turbulence": r"turb(?:ulence)?([\d.]+)",
        "batch": r"batch(\d+)",
        "version": r"v(\d+)",
    }
    params = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, name)
        if match:
            params[key] = float(match.group(1))
    if params:
        meta["simulation_params"] = params

    # Try to identify subject from filename keywords
    subjects = []
    subject_keywords = {
        "sgra": "Sgr A*",
        "sgr_a": "Sgr A*",
        "black_hole": "black hole",
        "blackhole": "black hole",
        "accretion": "accretion disk",
        "photon_ring": "photon ring",
        "great_attractor": "Great Attractor",
        "jet": "relativistic jet",
        "fermi": "Fermi Bubbles",
        "pulsar": "pulsar",
        "nebula": "nebula",
        "galaxy": "galaxy",
        "laniakea": "Laniakea Supercluster",
        "turbulence": "turbulence",
    }
    for keyword, subject in subject_keywords.items():
        if keyword in name:
            subjects.append(subject)
    if subjects:
        meta["detected_subjects"] = subjects

    return meta


# ─── INGEST AGENT ────────────────────────────────────────────────────
INGEST_SYSTEM_PROMPT = """You are MANATUABON, an autonomous astrophysics intelligence built by Danny from Bayamón, Puerto Rico.
Your domain is the ENTIRE OBSERVABLE UNIVERSE. Do NOT limit your focus to Sgr A*! You must hunt for anomalies across Pulsars, Exoplanets, Quasars, Dark Energy, and the Cosmic Web.
Use prior hypotheses only when they are directly relevant to the incoming evidence. Do NOT force unrelated datasets into an Sgr A* explanation.

You are analyzing a new file that has arrived in the research pipeline.
Your role: extract scientific meaning, identify patterns, and generate hypotheses that span any cosmological scale.

You MUST respond with ONLY valid JSON (no markdown, no explanation outside the JSON).
Use this exact schema:
{
  "summary": "one-sentence description of what this file contains/represents",
  "entities": ["list", "of", "astronomical objects or concepts"],
  "topics": ["list", "of", "research topics"],
  "anomalies": ["any unexpected patterns or noteworthy findings"],
  "significance": 0.5,
  "supports_hypothesis": null,
  "challenges_hypothesis": null,
  "new_hypothesis": null
}

For significance, use a 0.0-1.0 scale:
- 0.0-0.3: routine data, expected results
- 0.4-0.6: interesting, worth noting
- 0.7-0.9: significant finding, pattern detected
- 1.0: breakthrough-level

For new_hypothesis, use null if no pattern warrants one, or:
{
  "title": "short title",
  "body": "detailed description proposing a novel theory about ANY cosmic phenomenon",
  "confidence": 0.0-1.0
}

For supports_hypothesis / challenges_hypothesis, use the hypothesis ID string (e.g., "H1") or null."""

CONSOLIDATE_SYSTEM_PROMPT = """You are MANATUABON, an autonomous astrophysics intelligence reviewing your memory bank.
You think like a sleeping universe — connecting, compressing, and discovering patterns across all of physics and cosmology.

CRITICAL INSTRUCTION: You must expand your reasoning BEYOND Sgr A*! While Danny's past hypotheses focus on Sgr A*, your job is to uncover new theories about Dark Flow, Magnetars, Supernovae, Exoplanets, and Galactic Evolution.

Respond ONLY with valid JSON using this exact schema:
{
  "connections": [
    {
      "memory_ids": [1, 5],
      "connection": "explanation of how they connect",
      "strength": 0.8
    }
  ],
  "new_insight": "cross-cutting finding",
  "hypothesis_update": null,
  "new_hypothesis": null,
  "next_simulation": "what specific simulation to run next and why",
  "mast_targets_to_query": ["object name to query via NASA JWST/Hubble, keep it short like 'M87' or 'Crab Nebula'", "..."],
  "radio_targets_to_query": [
    {"target": "WOW Signal", "type": "SETI"},
    {"target": "Sgr A*", "type": "ALMA"}
  ]
}

If generating a new_hypothesis, use:
{ "title": "...", "body": "...", "confidence": 0.0-1.0 }

If updating an existing hypothesis, use:
{ "hypothesis_id": "H1", "update": "reasoning" }
"""


class IngestAgent:
    """Analyze incoming files via Nemotron and create memory objects."""

    DOMAIN_KEYWORDS = {
        "sgra": {"sgr a*", "sgr a", "sgra", "galactic center", "s-stars", "bondi", "riaf"},
        "pulsars": {"pulsar", "magnetar", "crab", "vela", "spin-down", "wind nebula"},
        "cosmology": {"cosmology", "inflation", "cmb", "dark flow", "laniakea", "great attractor", "bulk flows"},
        "black_holes": {"black hole", "event horizon", "hawking", "information paradox", "wormhole"},
        "consciousness": {"consciousness", "observer", "wheeler", "anthropic", "participatory"},
        "seti": {"seti", "fermi paradox", "wow signal", "civilization", "silence"},
        "exoplanets": {"exoplanet", "biosignature", "atmosphere", "transmission spectra", "habitability", "disequilibrium"},
        "quasars": {"quasar", "reverberation mapping", "broad-line region", "time-domain", "active galactic nucleus", "variability"},
        "cosmic_web": {"cosmic web", "filaments", "weak lensing", "large scale structure", "galaxy evolution"},
    }
    STOPWORDS = {
        "about", "across", "after", "again", "against", "along", "also", "because", "before",
        "between", "beyond", "could", "every", "from", "have", "into", "just", "more", "only",
        "other", "over", "same", "than", "that", "their", "there", "these", "this", "those",
        "through", "under", "very", "what", "when", "where", "which", "while", "with", "would",
    }

    def __init__(self, nemotron: NemotronClient, memory: MemoryManager, agent_log: AgentLog):
        self.nemotron = nemotron
        self.memory = memory
        self.agent_log = agent_log
        self.council = None  # Injected after init

    def set_council(self, council):
        self.council = council

    @classmethod
    def _tokenize_context_terms(cls, *parts) -> set[str]:
        tokens = set()
        for part in parts:
            if part is None:
                continue
            if isinstance(part, (list, tuple, set)):
                values = part
            else:
                values = [part]
            for value in values:
                text = str(value).lower()
                normalized = re.sub(r"[^a-z0-9*+\-]+", " ", text)
                for token in normalized.split():
                    if len(token) < 3:
                        continue
                    if token in cls.STOPWORDS:
                        continue
                    tokens.add(token)
        return tokens

    @classmethod
    def _hypothesis_domain(cls, hypothesis: dict) -> str:
        text = " ".join([
            hypothesis.get("title", ""),
            hypothesis.get("desc", ""),
            " ".join(hypothesis.get("tags", []) or []),
        ]).lower()
        for domain, keywords in cls.DOMAIN_KEYWORDS.items():
            if any(keyword in text for keyword in keywords):
                return domain
        return "general"

    @classmethod
    def _detect_domains(cls, evidence_text: str = "", entities: list[str] | None = None) -> set[str]:
        haystack = " ".join([evidence_text, " ".join(entities or [])]).lower()
        domains = set()
        for domain, keywords in cls.DOMAIN_KEYWORDS.items():
            if any(keyword in haystack for keyword in keywords):
                domains.add(domain)
        return domains

    def _select_hypothesis_context(self, hypotheses: list[dict], evidence_text: str = "", entities: list[str] | None = None, limit: int = 5) -> list[dict]:
        if not hypotheses:
            return []

        evidence_terms = self._tokenize_context_terms(evidence_text, entities or [])
        evidence_domains = self._detect_domains(evidence_text, entities or [])
        sgr_a_relevant = "sgra" in evidence_domains
        available_domains = {self._hypothesis_domain(hypothesis) for hypothesis in hypotheses}
        if evidence_domains and not any(domain in available_domains for domain in evidence_domains):
            return []

        ranked = []
        for index, hypothesis in enumerate(hypotheses):
            hyp_terms = self._tokenize_context_terms(
                hypothesis.get("title", ""),
                hypothesis.get("tags", []) or [],
            )
            domain = self._hypothesis_domain(hypothesis)
            overlap = len(evidence_terms & hyp_terms)
            domain_match = 2 if domain in evidence_domains else 0
            domain_penalty = 0
            if domain == "sgra" and not sgr_a_relevant:
                domain_penalty = 3
            ranked.append((domain_match + overlap - domain_penalty, domain_match, overlap, index, domain, hypothesis))

        ranked.sort(key=lambda item: (item[0], item[1], item[2], -item[3]), reverse=True)
        has_direct_overlap = any(item[1] > 0 or item[2] > 0 for item in ranked)

        selected = []
        used_ids = set()
        used_domains = set()

        for score, domain_match, overlap, _, domain, hypothesis in ranked:
            if evidence_domains:
                if domain_match <= 0 and domain != "general":
                    continue
            elif domain_match <= 0 and overlap <= 0:
                continue
            if hypothesis["id"] in used_ids:
                continue
            if domain in used_domains and len(selected) < max(2, limit - 1):
                continue
            selected.append(hypothesis)
            used_ids.add(hypothesis["id"])
            used_domains.add(domain)
            if len(selected) >= limit:
                return selected

        if not has_direct_overlap and not sgr_a_relevant:
            return []

        fallback_pool = [
            item for item in ranked
            if (sgr_a_relevant or item[4] != "sgra")
            and (not evidence_domains or item[4] in evidence_domains or item[4] == "general")
        ]
        fallback = sorted(
            fallback_pool,
            key=lambda item: (
                item[4] in used_domains,
                -(item[1] + item[2]),
                item[3],
            ),
        )
        for _, _, _, _, domain, hypothesis in fallback:
            if hypothesis["id"] in used_ids:
                continue
            selected.append(hypothesis)
            used_ids.add(hypothesis["id"])
            used_domains.add(domain)
            if len(selected) >= limit:
                break

        return selected

    def _build_hypothesis_context(self, hypotheses: list[dict], evidence_text: str = "", entities: list[str] | None = None, limit: int = 5) -> str:
        selected = self._select_hypothesis_context(hypotheses, evidence_text=evidence_text, entities=entities, limit=limit)
        if not selected:
            return "No directly relevant hypotheses yet. Prefer evidence-led interpretation over legacy framing."
        return "\n".join(
            f"- {hyp['id']}: {hyp['title']} — {hyp['desc'][:100]}... [domain: {self._hypothesis_domain(hyp)}]"
            for hyp in selected
        )

    def _build_hypothesis_context_payload(self, hypotheses: list[dict], evidence_text: str = "", entities: list[str] | None = None, limit: int = 5) -> tuple[str, list[dict], list[str]]:
        selected = self._select_hypothesis_context(hypotheses, evidence_text=evidence_text, entities=entities, limit=limit)
        evidence_domains = sorted(self._detect_domains(evidence_text, entities or []))
        if not selected:
            return (
                "No directly relevant hypotheses yet. Prefer evidence-led interpretation over legacy framing.",
                [],
                evidence_domains,
            )
        return (
            "\n".join(
                f"- {hyp['id']}: {hyp['title']} — {hyp['desc'][:100]}... [domain: {self._hypothesis_domain(hyp)}]"
                for hyp in selected
            ),
            selected,
            evidence_domains,
        )

    @staticmethod
    def _normalize_string_list(values) -> list[str]:
        if not isinstance(values, list):
            return []
        normalized = []
        for value in values:
            text = str(value).strip()
            if text and text not in normalized:
                normalized.append(text)
        return normalized

    @staticmethod
    def _derive_memory_confidence(result: dict, extension: str, ai_available: bool, entity_count: int, anomaly_count: int, structured_payload: bool = False) -> tuple[float, str]:
        base = float(result.get("significance", 0.5) or 0.5)
        confidence = base
        reasons = [f"base significance {base:.2f}"]

        source_bonus = {
            ".json": 0.15,
            ".csv": 0.10,
            ".txt": 0.08,
            ".jpg": -0.05,
            ".jpeg": -0.05,
            ".png": -0.05,
        }.get(extension, 0.0)
        confidence += source_bonus
        if source_bonus:
            reasons.append(f"source format adjustment {source_bonus:+.2f}")

        if structured_payload:
            confidence += 0.14
            reasons.append("deterministic structured ingest payload")
        elif ai_available:
            confidence += 0.08
            reasons.append("LLM extraction succeeded")
        else:
            confidence -= 0.15
            reasons.append("LLM extraction unavailable")

        if entity_count:
            confidence += min(entity_count, 4) * 0.02
            reasons.append(f"{entity_count} extracted entities")
        if anomaly_count:
            confidence += min(anomaly_count, 3) * 0.03
            reasons.append(f"{anomaly_count} anomaly markers")

        confidence = MemoryManager._clamp_confidence(confidence)
        return confidence, "; ".join(reasons)

    @staticmethod
    def _format_memory_evidence_item(memory: dict, anomaly_text: str | None = None) -> str:
        summary = (memory.get("summary") or "").strip()
        label_bits = [f"Memory #{memory.get('id', '?')}"]
        confidence_label = memory.get("confidence_label")
        if confidence_label:
            label_bits.append(confidence_label)
        domain_tags = memory.get("domain_tags") or []
        if domain_tags:
            label_bits.append("domains: " + ", ".join(domain_tags[:3]))
        text = f"[{' | '.join(label_bits)}] {summary}".strip()
        if anomaly_text:
            text = f"{text} Anomalies: {anomaly_text}".strip()
        return text

    def _build_generated_hypothesis_payload(
        self,
        new_hyp: dict,
        *,
        hypothesis_id: str,
        source_memories: list[dict],
        selected_hypotheses: list[dict],
        evidence_domains: list[str],
        confidence_reason: str,
        source_label: str,
    ) -> dict:
        source_memory_ids = [memory["id"] for memory in source_memories if memory.get("id") is not None]
        memory_domains = sorted({
            domain
            for memory in source_memories
            for domain in (memory.get("domain_tags") or [])
        })
        context_domains = sorted(set(evidence_domains or []) | set(memory_domains))
        anomaly_text = "; ".join(str(item).strip() for item in (new_hyp.get("anomalies") or []) if str(item).strip())
        evidence = [
            self._format_memory_evidence_item(memory, anomaly_text=anomaly_text if index == 0 else None)
            for index, memory in enumerate(source_memories)
            if memory.get("summary")
        ]
        sources = [source_label] if source_label else []
        sources.extend(f"Memory #{memory_id}" for memory_id in source_memory_ids)

        return {
            "id": hypothesis_id,
            "title": new_hyp["title"],
            "body": new_hyp.get("body", ""),
            "confidence": new_hyp.get("confidence", 0.5),
            "confidence_reason": confidence_reason,
            "predictions": new_hyp.get("predictions", []),
            "evidence": evidence,
            "sources": sources,
            "source_memory": source_memory_ids[0] if len(source_memory_ids) == 1 else None,
            "source_memory_ids": source_memory_ids,
            "source_file": source_label,
            "context_hypotheses": [
                {"id": hyp["id"], "title": hyp["title"], "domain": self._hypothesis_domain(hyp)}
                for hyp in selected_hypotheses
            ],
            "context_domains": context_domains,
            "timestamp": datetime.now().isoformat(),
        }

    def _extract_structured_ingest_payload(self, raw: dict) -> dict | None:
        if not isinstance(raw, dict):
            return None

        schema = str(raw.get("manatuabon_schema") or raw.get("manatuabon_ingest_schema") or "").strip().lower()
        if schema != "structured_ingest_v1":
            return None

        target = raw.get("target")
        if isinstance(target, dict):
            target_label = str(target.get("display_name") or target.get("name") or target.get("id") or "structured target").strip()
        else:
            target_label = str(target or "structured target").strip()

        structured_evidence = raw.get("structured_evidence") if isinstance(raw.get("structured_evidence"), dict) else {}
        if not structured_evidence and isinstance(raw.get("evidence_bundle"), dict):
            structured_evidence = raw.get("evidence_bundle")

        summary = str(raw.get("summary") or f"Structured ingest bundle for {target_label}.").strip()
        entities = self._normalize_string_list(raw.get("entities"))
        topics = self._normalize_string_list(raw.get("topics"))
        anomalies = self._normalize_string_list(raw.get("anomalies"))
        source_catalogs = self._normalize_string_list(raw.get("source_catalogs"))
        domain_tags = self._normalize_string_list(raw.get("domain_tags"))
        new_hypothesis = raw.get("new_hypothesis") if isinstance(raw.get("new_hypothesis"), dict) and raw.get("new_hypothesis", {}).get("title") else None

        return {
            "summary": summary,
            "entities": entities,
            "topics": topics,
            "anomalies": anomalies,
            "significance": MemoryManager._clamp_confidence(raw.get("significance"), default=0.6),
            "supports_hypothesis": raw.get("supports_hypothesis"),
            "challenges_hypothesis": raw.get("challenges_hypothesis"),
            "new_hypothesis": new_hypothesis,
            "domain_tags": domain_tags,
            "payload_type": str(raw.get("payload_type") or "structured_bundle").strip(),
            "structured_metadata": {
                "schema": schema,
                "payload_type": str(raw.get("payload_type") or "structured_bundle").strip(),
                "target": raw.get("target"),
                "source_catalogs": source_catalogs,
                "structured_evidence": structured_evidence,
            },
        }

    def ingest_file(self, filepath: Path) -> dict | None:
        """Process a single file and return the created memory object."""
        ext = filepath.suffix.lower()
        log.info("Ingesting: %s", filepath.name)
        self.agent_log.add("ingest_start", f"Processing {filepath.name}")

        # Build context for Nemotron
        metadata = parse_filename_metadata(filepath)
        file_content = ""
        raw_json = None
        structured_result = None

        if ext in {".txt", ".csv"}:
            try:
                file_content = filepath.read_text(encoding="utf-8")[:4000]  # cap at 4K chars
            except Exception as e:
                log.warning("Failed to read %s: %s", filepath.name, e)
        elif ext == ".json":
            try:
                raw_json = json.loads(filepath.read_text(encoding="utf-8"))
                file_content = json.dumps(raw_json, indent=2)[:4000]
                structured_result = self._extract_structured_ingest_payload(raw_json)
            except Exception as e:
                log.warning("Failed to parse JSON %s: %s", filepath.name, e)

        # Build user prompt
        hypotheses = self.memory.get_founding_hypotheses()
        hypothesis_evidence_text = " ".join([
            metadata.get("filename", ""),
            metadata.get("extension", ""),
            " ".join(metadata.get("detected_subjects", []) or []),
            file_content,
        ])
        hyp_context, selected_hypotheses, evidence_domains = self._build_hypothesis_context_payload(
            hypotheses,
            evidence_text=hypothesis_evidence_text,
            entities=metadata.get("detected_subjects", []),
            limit=5,
        )
        self.agent_log.add(
            "hypothesis_context_selected",
            f"{filepath.name}: {len(selected_hypotheses)} founding hypothesis references selected",
            {
                "source_file": filepath.name,
                "selected_hypotheses": [
                    {"id": hyp["id"], "title": hyp["title"], "domain": self._hypothesis_domain(hyp)}
                    for hyp in selected_hypotheses
                ],
                "evidence_domains": evidence_domains,
            },
        )
        if structured_result:
            result = structured_result
            ai_available = False
            self.agent_log.add(
                "structured_ingest_detected",
                f"{filepath.name}: deterministic structured ingest payload",
                {
                    "source_file": filepath.name,
                    "payload_type": structured_result.get("payload_type"),
                    "domain_tags": structured_result.get("domain_tags", []),
                },
            )
        else:
            user_prompt = f"""File: {metadata['filename']}
Type: {metadata['extension']}
Size: {metadata['size_bytes']} bytes
Created: {metadata.get('created', 'unknown')}
"""
            if metadata.get("simulation_params"):
                user_prompt += f"Simulation parameters: {json.dumps(metadata['simulation_params'])}\n"
            if metadata.get("detected_subjects"):
                user_prompt += f"Detected subjects: {', '.join(metadata['detected_subjects'])}\n"
            if file_content:
                user_prompt += f"\nFile content:\n{file_content}\n"
            else:
                user_prompt += "\n(Binary image file — analyze based on metadata and filename only)\n"

            user_prompt += "\nRelevant active hypotheses (selected by evidence overlap and domain diversity):\n"
            user_prompt += f"{hyp_context}\n"
            user_prompt += "\nAnalyze this file and respond with the JSON schema."

            result = self.nemotron.chat_json(INGEST_SYSTEM_PROMPT, user_prompt)
            ai_available = bool(result)
            if not result:
                log.warning("Nemotron returned no usable response for %s", filepath.name)
                self.agent_log.add("ingest_failed", f"No Nemotron response for {filepath.name}")
                result = {
                    "summary": f"File ingested: {filepath.name} (AI analysis unavailable)",
                    "entities": metadata.get("detected_subjects", []),
                    "topics": [],
                    "anomalies": [],
                    "significance": 0.3,
                    "supports_hypothesis": None,
                    "challenges_hypothesis": None,
                    "new_hypothesis": None,
                }

        memory_confidence, confidence_reason = self._derive_memory_confidence(
            result,
            ext,
            ai_available,
            len(result.get("entities", [])),
            len(result.get("anomalies", [])),
            structured_payload=bool(structured_result),
        )

        result_domain_tags = result.get("domain_tags") or []
        merged_domain_tags = sorted(set(evidence_domains) | set(result_domain_tags))
        metadata_payload = dict(metadata)
        if structured_result:
            metadata_payload["structured_ingest"] = structured_result.get("structured_metadata", {})
        elif raw_json and isinstance(raw_json, dict) and raw_json.get("manatuabon_context"):
            metadata_payload["manatuabon_context"] = raw_json.get("manatuabon_context")

        # Enrich summary with structured evidence so it persists in the DB content field
        enriched_summary = result.get("summary", f"Ingested {filepath.name}")
        if structured_result:
            se = structured_result.get("structured_metadata", {}).get("structured_evidence", {})
            if se:
                parts = [enriched_summary]
                pub = se.get("publication", {})
                if pub:
                    cite_parts = []
                    if pub.get("authors"):
                        cite_parts.append(f"Authors: {', '.join(pub['authors'])}")
                    if pub.get("title"):
                        cite_parts.append(f"Title: {pub['title']}")
                    if pub.get("journal"):
                        jref = pub["journal"]
                        if pub.get("volume"):
                            jref += f" {pub['volume']}"
                        if pub.get("article_id"):
                            jref += f", {pub['article_id']}"
                        if pub.get("year"):
                            jref += f" ({pub['year']})"
                        cite_parts.append(f"Journal: {jref}")
                    if pub.get("doi"):
                        cite_parts.append(f"DOI: {pub['doi']}")
                    if pub.get("arxiv"):
                        cite_parts.append(f"arXiv: {pub['arxiv']}")
                    if pub.get("bibcode"):
                        cite_parts.append(f"Bibcode: {pub['bibcode']}")
                    if cite_parts:
                        parts.append("--- Citation ---")
                        parts.extend(cite_parts)
                kr = se.get("key_results", {})
                if kr:
                    parts.append("--- Key Results ---")
                    for k, v in kr.items():
                        parts.append(f"{k}: {v}")
                rel = se.get("relevance_to_crustal_memory") or se.get("relevance")
                if rel:
                    parts.append(f"--- Relevance --- {rel}")
                enriched_summary = "\n".join(parts)

        # Build memory object
        memory_obj = {
            "timestamp": datetime.now().isoformat(),
            "source_file": filepath.name,
            "source_type": self._classify_source(filepath),
            "summary": enriched_summary,
            "entities": result.get("entities", []),
            "topics": result.get("topics", []),
            "anomalies": result.get("anomalies", []),
            "importance": result.get("significance", 0.5),
            "confidence": memory_confidence,
            "confidence_reason": confidence_reason,
            "domain_tags": merged_domain_tags,
            "supports_hypothesis": result.get("supports_hypothesis"),
            "challenges_hypothesis": result.get("challenges_hypothesis"),
            "hypothesis_generated": None,
            "consolidated": False,
            "consolidation_insights": [],
            "metadata": metadata_payload,
        }

        mem_id = self.memory.add_memory(memory_obj)
        memory_obj["id"] = mem_id

        # Handle auto-generated hypothesis via Review Council
        new_hyp = result.get("new_hypothesis")
        if new_hyp and isinstance(new_hyp, dict) and new_hyp.get("title"):
            hyp_obj = self._build_generated_hypothesis_payload(
                new_hyp,
                hypothesis_id=f"AUTO-{mem_id}",
                source_memories=[memory_obj],
                selected_hypotheses=selected_hypotheses,
                evidence_domains=evidence_domains,
                confidence_reason=f"Generated from memory #{mem_id} during ingest review.",
                source_label=filepath.name,
            )
            if self.council:
                council_result = self.council.review(hyp_obj)
                hyp_obj["council_decision"] = council_result.get("decision")
                hyp_obj["final_confidence"] = council_result.get("score", 0.0)
                memory_obj["hypothesis_generated"] = hyp_obj
                self.agent_log.add(
                    "hypothesis_reviewed",
                    f"Council: {council_result.get('decision')} — {hyp_obj['title']}",
                    {"hypothesis_id": hyp_obj["id"], "decision": council_result.get("decision")},
                )
            else:
                memory_obj["hypothesis_generated"] = hyp_obj
                self.memory.add_auto_hypothesis(hyp_obj)
                self.agent_log.add(
                    "hypothesis_generated",
                    f"New hypothesis: {hyp_obj['title']}",
                    {"hypothesis_id": hyp_obj["id"]},
                )

        self.agent_log.add(
            "ingest_complete",
            f"Memory #{mem_id}: {memory_obj['summary'][:60]}",
            {
                "memory_id": mem_id,
                "entities": memory_obj["entities"],
                "significance": memory_obj["importance"],
                "confidence": memory_obj["confidence"],
            },
        )

        return memory_obj

    def ingest_text(self, text: str, source: str = "manual") -> dict | None:
        """Ingest raw text (from manual POST /ingest)."""
        log.info("Manual ingest from: %s", source)
        self.agent_log.add("manual_ingest", f"Source: {source}")

        hypotheses = self.memory.get_founding_hypotheses()
        hyp_context, selected_hypotheses, evidence_domains = self._build_hypothesis_context_payload(
            hypotheses,
            evidence_text=text,
            entities=[],
            limit=5,
        )
        self.agent_log.add(
            "hypothesis_context_selected",
            f"manual:{source}: {len(selected_hypotheses)} founding hypothesis references selected",
            {
                "source_file": source,
                "selected_hypotheses": [
                    {"id": hyp["id"], "title": hyp["title"], "domain": self._hypothesis_domain(hyp)}
                    for hyp in selected_hypotheses
                ],
                "evidence_domains": evidence_domains,
            },
        )

        user_prompt = f"""Manual text submission from Danny:
Source: {source}
Content:
{text[:4000]}

Relevant active hypotheses (selected by evidence overlap and domain diversity):
{hyp_context}

Analyze this text and respond with the JSON schema."""

        result = self.nemotron.chat_json(INGEST_SYSTEM_PROMPT, user_prompt)
        ai_available = bool(result)
        if not result:
            result = {
                "summary": f"Manual text ingested: {text[:80]}...",
                "entities": [],
                "topics": [],
                "anomalies": [],
                "significance": 0.5,
                "supports_hypothesis": None,
                "challenges_hypothesis": None,
                "new_hypothesis": None,
            }

        memory_confidence, confidence_reason = self._derive_memory_confidence(
            result,
            ".txt",
            ai_available,
            len(result.get("entities", [])),
            len(result.get("anomalies", [])),
        )

        memory_obj = {
            "timestamp": datetime.now().isoformat(),
            "source_file": None,
            "source_type": "manual_entry",
            "summary": result.get("summary", text[:100]),
            "entities": result.get("entities", []),
            "topics": result.get("topics", []),
            "anomalies": result.get("anomalies", []),
            "importance": result.get("significance", 0.5),
            "confidence": memory_confidence,
            "confidence_reason": confidence_reason,
            "domain_tags": evidence_domains,
            "supports_hypothesis": result.get("supports_hypothesis"),
            "challenges_hypothesis": result.get("challenges_hypothesis"),
            "hypothesis_generated": None,
            "consolidated": False,
            "consolidation_insights": [],
        }

        mem_id = self.memory.add_memory(memory_obj)
        memory_obj["id"] = mem_id

        new_hyp = result.get("new_hypothesis")
        if new_hyp and isinstance(new_hyp, dict) and new_hyp.get("title"):
            hyp_obj = self._build_generated_hypothesis_payload(
                new_hyp,
                hypothesis_id=f"AUTO-{mem_id}",
                source_memories=[memory_obj],
                selected_hypotheses=selected_hypotheses,
                evidence_domains=evidence_domains,
                confidence_reason=f"Generated from memory #{mem_id} during manual ingest review.",
                source_label=source,
            )
            if self.council:
                council_result = self.council.review(hyp_obj)
                hyp_obj["council_decision"] = council_result.get("decision")
                hyp_obj["final_confidence"] = council_result.get("score", 0.0)
            else:
                self.memory.add_auto_hypothesis(hyp_obj)
            memory_obj["hypothesis_generated"] = hyp_obj

        return memory_obj

    @staticmethod
    def _classify_source(filepath: Path) -> str:
        ext = filepath.suffix.lower()
        name = filepath.stem.lower()
        if ext in {".png", ".jpg", ".jpeg"}:
            if any(kw in name for kw in ["render", "sim", "sgra", "black"]):
                return "simulation_render"
            return "image"
        if ext == ".csv":
            return "dataset"
        if ext == ".json":
            return "structured_data"
        return "text_document"


# ─── WATCHER AGENT ───────────────────────────────────────────────────
class WatcherHandler(FileSystemEventHandler):
    """Debounced file system handler that triggers IngestAgent."""

    def __init__(self, ingest_agent: IngestAgent):
        self.ingest = ingest_agent
        self._pending = {}
        self._lock = threading.Lock()

    def _queue_path(self, filepath: Path):
        if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return
        with self._lock:
            self._pending[str(filepath)] = time.time()
        threading.Timer(DEBOUNCE_SECONDS, self._process, args=[filepath]).start()

    def on_created(self, event):
        if event.is_directory:
            return
        filepath = Path(event.src_path)
        self._queue_path(filepath)

    def on_modified(self, event):
        # Also catch modifications (some apps create then write)
        self.on_created(event)

    def on_moved(self, event):
        if event.is_directory:
            return
        destination = getattr(event, "dest_path", None)
        if not destination:
            return
        self._queue_path(Path(destination))

    def _process(self, filepath: Path):
        with self._lock:
            queued_time = self._pending.pop(str(filepath), None)
        if queued_time is None:
            return  # Already processed
        if not filepath.exists():
            return  # File was removed
        # Ensure file is fully written (wait if size is still changing)
        try:
            size1 = filepath.stat().st_size
            time.sleep(0.5)
            size2 = filepath.stat().st_size
            if size1 != size2:
                # Still writing, re-queue
                threading.Timer(2, self._process, args=[filepath]).start()
                return
        except OSError:
            return

        try:
            self.ingest.ingest_file(filepath)
        except Exception as e:
            log.error("Ingest failed for %s: %s", filepath.name, e)
            # Dead letter tracking
            attempts = self.ingest.memory.record_dead_letter(filepath.name, str(e))
            if attempts >= 3:
                log.warning("Dead-lettered %s after %d failed attempts — skipping permanently.", filepath.name, attempts)


# ─── CONSOLIDATION AGENT (Phase 14 SQL RAG) ──────────────────────────
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic

class ConsolidateAgent:
    """Connect memories over time and generate insights using LangChain SQL Agent."""

    def __init__(self, nemotron: NemotronClient, memory: MemoryManager, agent_log: AgentLog):
        self.nemotron = nemotron
        self.memory = memory
        self.agent_log = agent_log
        self.council = None  # Injected after init (Phase 18)
        
        log.info("Initializing LangChain SQL Agent runtime...")
        self.db = SQLDatabase.from_uri(f"sqlite:///{Path(self.memory.db_path).resolve()}")
        self.llm_local = ChatOpenAI(
            base_url=f"{self.nemotron.base_url}/v1", 
            api_key="lm-studio", 
            temperature=0.4
        )

    def _get_sql_agent(self, llm_engine):
        return create_sql_agent(
            llm=llm_engine,
            db=self.db,
            agent_type="zero-shot-react-description",
            verbose=True,
            handle_parsing_errors=True
        )

    def _select_source_memories(self, evidence_text: str, limit: int = 5) -> tuple[list[dict], list[str], list[dict]]:
        recent_memories = self.memory.get_memories()[:10]
        if not recent_memories:
            return [], [], []

        evidence_terms = IngestAgent._tokenize_context_terms(evidence_text)
        evidence_domains = IngestAgent._detect_domains(evidence_text)
        ranked = []
        for index, memory in enumerate(recent_memories):
            mem_terms = IngestAgent._tokenize_context_terms(
                memory.get("summary", ""),
                memory.get("entities", []),
                memory.get("domain_tags", []),
            )
            overlap = len(evidence_terms & mem_terms)
            domain_overlap = len(evidence_domains & set(memory.get("domain_tags") or []))
            score = overlap + (2 * domain_overlap) + (0.5 if memory.get("confidence", 0.0) >= 0.6 else 0.0)
            ranked.append((score, overlap, domain_overlap, -index, memory))

        ranked.sort(reverse=True)
        selected = [item[4] for item in ranked if item[0] > 0][:limit]
        if not selected:
            selected = recent_memories[: min(3, len(recent_memories))]

        memory_domains = sorted({
            domain
            for memory in selected
            for domain in (memory.get("domain_tags") or [])
        })

        helper = IngestAgent(self.nemotron, self.memory, self.agent_log)
        founding_hypotheses = self.memory.get_founding_hypotheses()
        _, selected_hypotheses, context_domains = helper._build_hypothesis_context_payload(
            founding_hypotheses,
            evidence_text=evidence_text,
            entities=[entity for memory in selected for entity in (memory.get("entities") or [])],
            limit=5,
        )

        return selected, sorted(set(context_domains) | set(memory_domains)), selected_hypotheses

    def run(self) -> dict | None:
        log.info("Starting SQL Console Consolidation cycle...")
        self.agent_log.add("consolidation_start", "Executing LangChain SQL queries against DB")
        
        # Step 1: Autonomous SQL RAG Query
        sql_prompt = """
        You are MANATUABON, an autonomous astrophysics intelligence.
        Your SQLite database contains structural tables: 'memories', 'hypotheses', and 'simulations'.
        Query the database to find connections between the most recent 10 memories and active hypotheses.
        Synthesize a detailed textual report highlighting any anomalies, connections, and evidence.
        """
        
        sql_insight_text = ""
        used_cloud = False
        try:
            log.info("Dispatching Local SQL Agent (Nemotron 30B)...")
            local_agent = self._get_sql_agent(self.llm_local)
            agent_response = local_agent.invoke({"input": sql_prompt})
            sql_insight_text = agent_response.get("output", "")
            log.info(f"Local SQL Agent Output: {sql_insight_text[:100]}...")
            
            # Confidence-based escalation: if local response is too short or vague
            if len(sql_insight_text.strip()) < 50:
                log.warning("[CONFIDENCE ESCALATION] Local response too shallow (%d chars) — escalating to Cloud...", len(sql_insight_text))
                self.agent_log.add("confidence_escalation", f"Local response only {len(sql_insight_text)} chars — escalating")
                raise Exception("Low-confidence local response — triggering cloud fallback")
            
        except Exception as e:
            log.warning(f"Local SQL Agent threw exception: {e}")
            log.warning("[ESCALATING TO CLOUD] Handing off logic to Claude Sonnet 4.6...")
            self.agent_log.add("cloud_escalation", "Nemotron failed or low-confidence; hot-swapping to ChatAnthropic")
            
            # Hot-swap to Cloud Brain
            try:
                anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
                if not anthropic_key:
                    raise ValueError("ANTHROPIC_API_KEY is missing from .env!")
                    
                llm_cloud = ChatAnthropic(
                    model="claude-4.6-sonnet", 
                    api_key=anthropic_key, 
                    temperature=0.4
                )
                cloud_agent = self._get_sql_agent(llm_cloud)
                agent_response = cloud_agent.invoke({"input": sql_prompt})
                sql_insight_text = agent_response.get("output", "")
                used_cloud = True
                log.info(f"Cloud SQL Agent Output: {sql_insight_text[:100]}...")
                
            except Exception as cloud_e:
                log.error(f"Cloud SQL Agent also failed: {cloud_e}")
                self.agent_log.add("consolidation_failed", "Both Local and Cloud SQL Agents failed")
                return None
            
        # Step 2: Format SQL Findings into strict JSON Schema
        user_prompt = f"SQL Database RAG Findings:\n{sql_insight_text}\n\nAnalyze these findings and map them into the requested JSON schema."
        
        result = self.nemotron.chat_json(CONSOLIDATE_SYSTEM_PROMPT, user_prompt, temperature=0.6)
        if not result:
            log.warning("Nemotron JSON formatter failed to parse SQL findings.")
            self.agent_log.add("consolidation_failed", "Invalid JSON formatter payload")
            return None
            
        insight_id = f"insight_{int(time.time())}"
        insight_obj = {
            "id": insight_id,
            "timestamp": datetime.now().isoformat(),
            "new_insight": result.get("new_insight", sql_insight_text[:500]),
            "connections": result.get("connections", []),
            "next_simulation_recommended": result.get("next_simulation")
        }
        
        # Save insights and queue simulations — route through Council
        new_hyp = result.get("new_hypothesis")
        if new_hyp and isinstance(new_hyp, dict) and new_hyp.get("title"):
             source_memories, evidence_domains, selected_hypotheses = self._select_source_memories(
                 " ".join([
                     new_hyp.get("title", ""),
                     new_hyp.get("body", ""),
                     sql_insight_text,
                     result.get("new_insight", ""),
                 ]),
                 limit=5,
             )
             helper = IngestAgent(self.nemotron, self.memory, self.agent_log)
             hyp_obj = helper._build_generated_hypothesis_payload(
                 new_hyp,
                 hypothesis_id=f"AUTO-{int(time.time())}",
                 source_memories=source_memories,
                 selected_hypotheses=selected_hypotheses,
                 evidence_domains=evidence_domains,
                 confidence_reason="Generated from SQL consolidation over recent memory evidence.",
                 source_label="SQL Agent Inference",
             )
             if self.council:
                 council_result = self.council.review(hyp_obj)
                 self.agent_log.add("hypothesis_reviewed", f"Council (RAG): {council_result.get('decision')} — {hyp_obj['title']}", {"hypothesis_id": hyp_obj["id"]})
             else:
                 self.memory.add_auto_hypothesis(hyp_obj)
                 self.agent_log.add("hypothesis_generated", f"New hypothesis (RAG): {hyp_obj['title']}", {"hypothesis_id": hyp_obj["id"]})
             
        if insight_obj.get("next_simulation_recommended"):
            sim_task = {
                "id": f"sim_{int(time.time())}",
                "timestamp": datetime.now().isoformat(),
                "recommendation": insight_obj["next_simulation_recommended"],
                "status": "pending",
                "source_insight": insight_id
            }
            self.memory.add_simulation_task(sim_task)
            self.agent_log.add("simulation_queued", f"Queued: {sim_task['recommendation'][:60]}", {"sim_id": sim_task["id"]})

        mast_targets = result.get("mast_targets_to_query", [])
        if mast_targets and isinstance(mast_targets, list):
            valid_targets = [str(t) for t in mast_targets if len(str(t)) > 2]
            if valid_targets:
                self.memory.queue_mast_targets(valid_targets)
                self.agent_log.add("telescope_targeted", f"Autonomous API Target queued: {', '.join(valid_targets)}")

        radio_targets = result.get("radio_targets_to_query", [])
        if radio_targets and isinstance(radio_targets, list):
            valid_radio = [t for t in radio_targets if isinstance(t, dict) and t.get("target") and t.get("type")]
            if valid_radio:
                self.memory.queue_radio_targets(valid_radio)
                self.agent_log.add("radio_targeted", f"Autonomous Radio Target queued: {len(valid_radio)} objects")

        self.agent_log.add("consolidation_complete", f"Insight: {insight_obj.get('new_insight', sql_insight_text[:60])}...", {"insight_id": insight_id, "used_cloud": used_cloud})
        
        # Hypothesis evolution: auto-promote/flag based on evidence
        try:
            self.memory.auto_promote_hypotheses()
        except Exception as e:
            log.warning(f"Hypothesis evolution check failed: {e}")

        # Evidence Hunter: actively search for evidence to satisfy pending requests
        try:
            from evidence_hunter import EvidenceHunter
            hunter = EvidenceHunter(self.memory, self.agent_log)
            hunt_result = hunter.hunt()
            if hunt_result.get("requests_satisfied"):
                log.info("Evidence Hunter satisfied %d request(s)", hunt_result["requests_satisfied"])
        except Exception as e:
            log.warning(f"Evidence Hunter failed: {e}")

        # Review Council: re-evaluate any 'held' hypotheses from prior cycles
        if self.council:
            try:
                self.council.re_evaluate_held()
            except Exception as e:
                log.warning(f"Council re-evaluation failed: {e}")
        
        log.info("Consolidation cycle complete.")
        return insight_obj



# ─── MAIN ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Manatuabon Always-On Agent")
    parser.add_argument("--watch", default=str(BASE_DIR / "renders"), help="Directory to watch")
    parser.add_argument("--inbox", default=str(BASE_DIR / "inbox"), help="Inbox drop zone")
    parser.add_argument("--port", type=int, default=7777, help="HTTP bridge port")
    parser.add_argument("--consolidate-every", type=int, default=30, help="Consolidation interval (minutes)")
    parser.add_argument("--lm-url", default="http://127.0.0.1:1234", help="LM Studio URL")
    args = parser.parse_args()

    watch_dir = Path(args.watch)
    inbox_dir = Path(args.inbox)
    watch_dir.mkdir(parents=True, exist_ok=True)
    inbox_dir.mkdir(parents=True, exist_ok=True)

    print(r"""
    ╔══════════════════════════════════════════════════════════╗
    ║          MANATUABON — Always-On Agent 🧠🌌              ║
    ║          From Bayamón to the edge of everything         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    log.info("Nemotron endpoint: %s", args.lm_url)
    log.info("Watching: %s", watch_dir)
    log.info("Inbox:    %s", inbox_dir)
    log.info("Bridge:   http://127.0.0.1:%d", args.port)

    # Initialize components
    nemotron = NemotronClient(args.lm_url)
    memory = MemoryManager(DB_FILE)
    agent_log = AgentLog(AGENT_LOG_FILE)
    ingest_agent = IngestAgent(nemotron, memory, agent_log)
    consolidate_agent = ConsolidateAgent(nemotron, memory, agent_log)

    # Initialize Hypothesis Review Council (Phase 18)
    try:
        from hypothesis_council import HypothesisCouncil
        council = HypothesisCouncil(nemotron, memory, agent_log)
        ingest_agent.set_council(council)
        consolidate_agent.council = council
        log.info("Hypothesis Review Council active ✓")
    except Exception as e:
        log.warning("Council init failed (%s) — running without review gate", e)

    agent_log.add("agent_started", f"Watching {watch_dir} and {inbox_dir}")

    # Setup file watchers
    handler = WatcherHandler(ingest_agent)
    observer = Observer()
    observer.schedule(handler, str(watch_dir), recursive=False)
    observer.schedule(handler, str(inbox_dir), recursive=False)
    observer.start()
    log.info("File watchers active ✓")

    # Start HTTP bridge in a thread
    from manatuabon_bridge import create_bridge_app, run_bridge

    bridge_thread = threading.Thread(
        target=run_bridge,
        args=(args.port, ingest_agent, memory, agent_log, nemotron, consolidate_agent),
        daemon=True,
    )
    bridge_thread.start()
    log.info("HTTP bridge running on port %d ✓", args.port)

    # Start consolidation loop
    def consolidation_loop():
        while True:
            time.sleep(args.consolidate_every * 60)
            try:
                consolidate_agent.run()
            except Exception as e:
                log.error(f"Consolidation error: {e}")
                
    cons_thread = threading.Thread(target=consolidation_loop, daemon=True)
    cons_thread.start()
    log.info("Consolidation loop active ✓ (every %d mins)", args.consolidate_every)

    # Main loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down Manatuabon agent...")
        agent_log.add("agent_stopped", "Graceful shutdown")
        observer.stop()
    observer.join()
    log.info("Agent stopped. 🌌")


if __name__ == "__main__":
    main()
