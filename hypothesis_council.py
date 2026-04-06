"""
MANATUABON — Hypothesis Review Council 🧪⚖️🔬
================================================
A multi-agent peer review system that evaluates every auto-generated
hypothesis before it enters the knowledge base.

Agents:
  • Skeptic   — tries to falsify (Nemotron local, free)
  • Archivist — checks for duplicates via embeddings (local MiniLM)
  • Judge     — renders final verdict (Claude Cloud)

Danny from Bayamón, PR 🇵🇷 — April 2026
"""

import os, json, re, logging, copy
from datetime import datetime
from pathlib import Path
import numpy as np
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

log = logging.getLogger("manatuabon.council")

# ─── EMBEDDING SIMILARITY ENGINE ─────────────────────────────────────

class EmbeddingSimilarity:
    """Cosine similarity via local all-MiniLM-L6-v2."""

    def __init__(self, model_path: str | None = None):
        if model_path is None:
            model_path = str(Path(__file__).resolve().parent / "models" / "all-MiniLM-L6-v2")
        from sentence_transformers import SentenceTransformer
        log.info("Loading embedding model from %s...", model_path)
        self.model = SentenceTransformer(model_path)
        log.info("Embedding model loaded ✓")

    def encode(self, text: str) -> np.ndarray:
        return self.model.encode(text, normalize_embeddings=True)

    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        return float(np.dot(a, b))

    def text_similarity(self, text_a: str, text_b: str) -> float:
        return self.cosine_similarity(self.encode(text_a), self.encode(text_b))


# ─── HYPOTHESIS NORMALIZER ───────────────────────────────────────────

class HypothesisNormalizer:
    """Enforce strict schema and auto-reject garbage hypotheses."""

    @staticmethod
    def _normalize_evidence(raw_evidence) -> list[str]:
        if raw_evidence is None:
            return []
        if isinstance(raw_evidence, list):
            return [str(item).strip() for item in raw_evidence if str(item).strip()]
        if isinstance(raw_evidence, str):
            text = raw_evidence.strip()
            if not text:
                return []
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                pass
            return [text]
        return [str(raw_evidence).strip()]

    @staticmethod
    def normalize(raw_hyp: dict) -> dict:
        """Normalize a raw hypothesis dict into the council schema."""
        source_memory = raw_hyp.get("source_memory")
        source_memory_ids = list(raw_hyp.get("source_memory_ids") or [])
        if source_memory is not None and source_memory not in source_memory_ids:
            source_memory_ids.insert(0, source_memory)
        return {
            "id": raw_hyp.get("id", f"AUTO-{int(datetime.now().timestamp())}"),
            "title": raw_hyp.get("title", "Untitled Hypothesis"),
            "claim": raw_hyp.get("body", raw_hyp.get("claim", "")),
            "predictions": raw_hyp.get("predictions", []),
            "evidence": HypothesisNormalizer._normalize_evidence(raw_hyp.get("evidence", [])),
            "sources": raw_hyp.get("sources", []),
            "confidence": raw_hyp.get("confidence", 0.5),
            "context_hypotheses": raw_hyp.get("context_hypotheses", []),
            "context_domains": raw_hyp.get("context_domains", []),
            "source_memory": source_memory,
            "source_memory_ids": source_memory_ids,
            "source_file": raw_hyp.get("source_file"),
            "source_type": raw_hyp.get("source_type"),
            "timestamp": raw_hyp.get("timestamp", datetime.now().isoformat()),
        }

    @staticmethod
    def auto_reject(hyp: dict) -> tuple[bool, str]:
        """Return (should_reject, reason) based on hard rules."""
        # Rule 1: No predictions = reject
        if not hyp.get("predictions") or len(hyp["predictions"]) == 0:
            return True, "No falsifiable predictions provided"
        # Rule 2: Confidence below floor
        if hyp.get("confidence", 0) < 0.15:
            return True, f"Confidence too low ({hyp.get('confidence', 0):.2f} < 0.15)"
        # Rule 3: Empty claim
        if not hyp.get("claim") or len(hyp["claim"].strip()) < 20:
            return True, "Claim is missing or too short (< 20 chars)"
        return False, ""


# ─── COUNCIL AGENTS ──────────────────────────────────────────────────

SKEPTIC_PROMPT = """You are the SKEPTIC on the Manatuabon Hypothesis Review Council.
Your SOLE JOB is to try to FALSIFY the hypothesis below. Be rigorous and merciless.

Consider:
1. Does this violate known laws of physics?
2. Are the predictions specific enough to be tested?
3. Is there a simpler explanation (Occam's Razor)?
4. Does this confuse correlation with causation?

Respond ONLY with valid JSON:
{
  "verdict": "weak" | "plausible" | "strong",
  "reasoning": "your detailed reasoning",
  "objections": ["specific objection 1", "specific objection 2"],
  "score_contributions": {
    "coherence": 0.0-1.0,
    "testability": 0.0-1.0
  }
}"""

ARCHIVIST_PROMPT = """You are the ARCHIVIST on the Manatuabon Hypothesis Review Council.
Your SOLE JOB is to check whether this hypothesis is REDUNDANT or already covered by existing hypotheses.

EXISTING HYPOTHESES IN DATABASE:
{existing_hypotheses}

DUPLICATE SIMILARITY SCORE: {similarity_score:.2f} (most similar: "{most_similar_title}")

Consider:
1. Is the core claim substantially different from existing hypotheses?
2. Does it add genuinely new insight, or just rephrase something we already have?
3. If it's partially overlapping, should it be MERGED with the existing one?

Respond ONLY with valid JSON:
{{
    "verdict": "unique" | "partial_overlap" | "duplicate",
    "reasoning": "your detailed reasoning",
    "objections": ["what it overlaps with"],
    "score_contributions": {{
        "novelty": 0.0-1.0,
        "evidence_support": 0.0-1.0
    }}
}}"""

JUDGE_PROMPT = """You are the JUDGE on the Manatuabon Hypothesis Review Council.
You have received reviews from the Skeptic and the Archivist. Your job is to render a FINAL VERDICT.

HYPOTHESIS:
Title: {title}
Claim: {claim}
Predictions: {predictions}
Confidence: {confidence}

SKEPTIC'S REVIEW:
{skeptic_review}

ARCHIVIST'S REVIEW:
{archivist_review}

EVIDENCE REVIEW:
{evidence_review}

QUANT REVIEW (ADVISORY):
{quant_review}

Score the hypothesis on these 5 dimensions (each 0.0-1.0):
- coherence: Does the claim logically follow from its evidence?
- evidence_support: How much existing data backs the claim?
- testability: Are the predictions specific and falsifiable?
- novelty: Does this add genuinely new insight?
- redundancy_penalty: How much does it overlap with existing hypotheses? (0 = unique, 1 = total duplicate)

Then determine the final decision:
- "accepted" if final_score >= 0.70
- "needs_revision" if 0.50 <= final_score < 0.70
- "held" if 0.30 <= final_score < 0.50
- "rejected" if final_score < 0.30
- "merged" if the Archivist flagged it as a duplicate

Respond ONLY with valid JSON:
{{
    "verdict": "accepted" | "rejected" | "needs_revision" | "held" | "merged",
    "reasoning": "your comprehensive final reasoning",
    "scores": {{
        "coherence": 0.0-1.0,
        "evidence_support": 0.0-1.0,
        "testability": 0.0-1.0,
        "novelty": 0.0-1.0,
        "redundancy_penalty": 0.0-1.0
    }},
    "final_score": 0.0-1.0,
    "merge_target": null
}}"""

QUANT_REVIEW_PATTERNS = {
    "mass": (r"\bmass\b", r"\bm☉\b", r"\bsolar masses?\b", r"\bkg\b"),
    "distance": (r"\bdistance\b", r"\bpc\b", r"\bkpc\b", r"\bmpc\b", r"\bly\b", r"\bparsec\b"),
    "flux": (r"\bflux\b", r"\bfluence\b", r"\bluminosity\b", r"\bbrightness\b", r"\bmagnitude\b"),
    "time": (r"\bperiod\b", r"\btimescale\b", r"\bhour\b", r"\bday\b", r"\bsecond\b", r"\byear\b"),
    "velocity": (r"\bvelocity\b", r"\bspeed\b", r"\bkm/s\b", r"\bproper motion\b"),
    "frequency": (r"\bfrequency\b", r"\bhz\b", r"\bmhz\b", r"\bghz\b", r"\bperiodicity\b"),
    "temperature": (r"\btemperature\b", r"\bkelvin\b", r"\bthermal\b"),
    "redshift": (r"\bredshift\b", r"\bz\s*[=<>]", r"\bz\b"),
    "orbit": (r"\borbit\b", r"\bsemi-major\b", r"\beccentricity\b", r"\btransit\b"),
    "statistics": (r"\bsigma\b", r"\bsignificance\b", r"\bp-value\b", r"\bcorrelat", r"\bratio\b", r"%"),
}

QUANT_EQUATION_RULES = [
    (("mass", "orbit", "time"), "Kepler/orbital dynamics"),
    (("flux", "distance"), "inverse-square scaling"),
    (("redshift", "velocity"), "redshift-velocity relation"),
    (("frequency", "time"), "frequency-period consistency"),
    (("temperature", "flux"), "thermal emission scaling"),
    (("statistics",), "statistical significance check"),
]

QUANT_EXPECTED_INPUTS = {
    "Kepler/orbital dynamics": {"orbit", "time", "mass"},
    "inverse-square scaling": {"flux", "distance"},
    "redshift-velocity relation": {"redshift", "velocity"},
    "frequency-period consistency": {"frequency", "time"},
    "thermal emission scaling": {"temperature", "flux"},
    "statistical significance check": {"statistics"},
}

EVIDENCE_TIER_SCORES = {"tier_a": 1.0, "tier_b": 0.58, "tier_c": 0.22, "none": 0.0}
EVIDENCE_DECISION_RANK = {"accepted": 3, "needs_revision": 2, "held": 1, "rejected": 0}

REFLECTION_PROMPT = """You are the REFLECTION agent on the Manatuabon Hypothesis Review Council.
Your job is to audit unresolved hypotheses after the Judge has spoken.

Rules:
1. Do not invent evidence.
2. Do not replace the Judge's decision.
3. Produce the minimum set of concrete fixes or evidence requests needed for the next review cycle.
4. If the hypothesis is close to viable, say exactly what must improve.
5. If it is not close, say whether it should remain held or be rejected later.

HYPOTHESIS:
Title: {title}
Claim: {claim}
Predictions: {predictions}
Evidence: {evidence}
Context Domains: {context_domains}

SKEPTIC REVIEW:
{skeptic_review}

ARCHIVIST REVIEW:
{archivist_review}

JUDGE REVIEW:
{judge_review}

CURRENT DECISION: {decision}
CURRENT FINAL SCORE: {final_score}
CURRENT SCORE BREAKDOWN:
{breakdown}

Respond ONLY with valid JSON:
{{
    "verdict": "revise_and_retry" | "hold_for_evidence" | "ready_for_rejudge" | "reject_later",
    "reasoning": "brief but concrete explanation of the next best action",
    "objections": ["highest priority blocker 1", "highest priority blocker 2"],
    "score_contributions": {{
        "readiness": 0.0-1.0,
        "evidence_support": 0.0-1.0,
        "testability": 0.0-1.0
    }},
    "recommended_decision": "needs_revision" | "held" | "rejected" | "accepted",
    "rerun_worthy": true,
    "blockers": ["blocking issue 1", "blocking issue 2"],
    "concrete_revisions": ["revision step 1", "revision step 2"],
    "evidence_requests": ["evidence needed 1", "evidence needed 2"]
}}"""


class SkepticAgent:
    """Tries to falsify the hypothesis using local Nemotron."""

    def __init__(self, nemotron):
        self.nemotron = nemotron

    def review(self, hyp: dict) -> dict:
        user_prompt = f"""HYPOTHESIS UNDER REVIEW:
Title: {hyp['title']}
Claim: {hyp['claim']}
Predictions: {json.dumps(hyp.get('predictions', []))}
Confidence: {hyp.get('confidence', 0.5)}
Evidence: {json.dumps(hyp.get('evidence', []))}"""

        result = self.nemotron.chat_json(SKEPTIC_PROMPT, user_prompt, temperature=0.3)
        if not result:
            return {
                "agent": "skeptic", "verdict": "plausible",
                "reasoning": "Skeptic agent failed to respond — defaulting to plausible.",
                "objections": [], "score_contributions": {"coherence": 0.5, "testability": 0.5}
            }
        result["agent"] = "skeptic"
        return result


class ArchivistAgent:
    """Checks for duplicates using embeddings + LLM reasoning."""

    def __init__(self, nemotron, embedder: EmbeddingSimilarity):
        self.nemotron = nemotron
        self.embedder = embedder

    def review(self, hyp: dict, existing_hypotheses: list) -> dict:
        # Step 1: Compute cosine similarity against all existing
        hyp_text = f"{hyp['title']} {hyp['claim']}"
        best_sim = 0.0
        best_match = "N/A"

        for existing in existing_hypotheses:
            existing_text = f"{existing['title']} {existing.get('body', '')}"
            sim = self.embedder.text_similarity(hyp_text, existing_text)
            if sim > best_sim:
                best_sim = sim
                best_match = existing['title']

        # Step 2: LLM-based contextual review
        existing_list = "\n".join(
            f"  - {h['id']}: {h['title']}" for h in existing_hypotheses[:15]
        ) if existing_hypotheses else "  (No existing hypotheses)"

        prompt = ARCHIVIST_PROMPT.format(
            existing_hypotheses=existing_list,
            similarity_score=best_sim,
            most_similar_title=best_match
        )

        user_prompt = f"""HYPOTHESIS UNDER REVIEW:
Title: {hyp['title']}
Claim: {hyp['claim']}"""

        result = self.nemotron.chat_json(prompt, user_prompt, temperature=0.3)
        if not result:
            result = {
                "verdict": "unique", "reasoning": "Archivist failed to respond — defaulting to unique.",
                "objections": [], "score_contributions": {"novelty": 0.5, "evidence_support": 0.5}
            }

        result["agent"] = "archivist"
        result["similarity_score"] = best_sim
        result["most_similar"] = best_match
        return result


class JudgeAgent:
    """Renders final verdict. Uses Cloud Claude for maximum reasoning."""

    def __init__(self, nemotron, anthropic_key: str = None):
        self.nemotron = nemotron
        self.anthropic_key = anthropic_key or os.environ.get("ANTHROPIC_API_KEY")

    def _get_cloud_llm(self):
        if not self.anthropic_key:
            return None
        try:
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model="claude-sonnet-4-20250514",
                api_key=self.anthropic_key,
                temperature=0.3
            )
        except (ImportError, ValueError, OSError) as e:
            log.warning("Could not initialize Cloud Judge: %s", e)
            return None

    def review(self, hyp: dict, skeptic_review: dict, archivist_review: dict, evidence_review: dict | None = None, quant_review: dict | None = None) -> dict:
        prompt = JUDGE_PROMPT.format(
            title=hyp['title'],
            claim=hyp['claim'],
            predictions=json.dumps(hyp.get('predictions', [])),
            confidence=hyp.get('confidence', 0.5),
            skeptic_review=json.dumps(skeptic_review, indent=2),
            archivist_review=json.dumps(archivist_review, indent=2),
            evidence_review=json.dumps(evidence_review or {"status": "not_triggered"}, indent=2),
            quant_review=json.dumps(quant_review or {"status": "not_triggered"}, indent=2),
        )

        # Try Cloud first for highest quality
        cloud_llm = self._get_cloud_llm()
        if cloud_llm:
            try:
                log.info("[JUDGE] Sending to Cloud Claude for verdict...")
                response = cloud_llm.invoke(prompt)
                raw = response.content if hasattr(response, 'content') else str(response)
                json_match = re.search(r"\{[\s\S]*\}", raw)
                if json_match:
                    result = json.loads(json_match.group())
                    result["agent"] = "judge"
                    result["engine"] = "cloud"
                    return result
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as e:
                log.warning("[JUDGE] Cloud failed (%s), falling back to local...", e)

        # Fallback to local Nemotron
        log.info("[JUDGE] Using local Nemotron for verdict...")
        result = self.nemotron.chat_json(prompt, "Render your verdict now.", temperature=0.4)
        if not result:
            result = self._default_verdict(hyp, skeptic_review, archivist_review)
        result["agent"] = "judge"
        result["engine"] = "local"
        return result

    def _default_verdict(self, hyp, skeptic, archivist):
        """Fallback if both LLMs fail."""
        score = hyp.get("confidence", 0.5)
        decision = "held"
        if score >= 0.7: decision = "needs_revision"
        elif score < 0.3: decision = "rejected"
        return {
            "verdict": decision,
            "reasoning": "Both Cloud and Local LLMs failed to render verdict. Defaulting based on raw confidence.",
            "scores": {"coherence": 0.5, "evidence_support": 0.5, "testability": 0.5, "novelty": 0.5, "redundancy_penalty": 0.0},
            "final_score": score,
            "merge_target": None
        }


class EvidenceReviewerAgent:
    """Deterministic evidence-tier reviewer implementing governance evidence policy."""

    DIRECT_MARKERS = (
        "observed", "measured", "detected", "catalog", "dataset", "survey", "instrument", "jwst", "gaia", "ligo",
        "fermi", "alma", "mast", "arxiv", "doi", "memory #", "light curve", "spectrum", "photometry", "spectroscopy",
    )
    WEAK_MARKERS = (
        "consistent with", "candidate", "hint", "suggest", "possible", "partial", "preliminary", "archival", "correl",
        "align", "matches", "supports", "weakly", "tentative",
    )
    SPECULATIVE_MARKERS = (
        "may", "might", "could", "perhaps", "speculative", "analogy", "narrative", "extrapolat", "inspired by",
        "hypothetical", "conjecture", "if true", "would imply",
    )
    NUMERIC_MEASUREMENT_PATTERN = re.compile(r"\b\d+(?:\.\d+)?\s*(?:%|σ|hz|khz|mhz|ghz|s|sec|seconds?|min|minutes?|hr|hours?|days?|years?|kpc|mpc|pc|ly|km/s|m/s|kev|ev|mag)\b", re.IGNORECASE)

    def review(self, hyp: dict) -> dict:
        evidence_items = [str(item).strip() for item in (hyp.get("evidence") or []) if str(item).strip()]
        if not evidence_items:
            return self._empty_review()

        classified_items = []
        tier_counts = {"tier_a": 0, "tier_b": 0, "tier_c": 0}
        total_score = 0.0

        for item in evidence_items:
            tier, rationale = self._classify_item(item)
            classified_items.append({"text": item, "tier": tier, "rationale": rationale})
            tier_counts[tier] += 1
            total_score += EVIDENCE_TIER_SCORES[tier]

        strongest_tier = "tier_a" if tier_counts["tier_a"] else "tier_b" if tier_counts["tier_b"] else "tier_c"
        quality_score = round(total_score / max(len(evidence_items), 1), 3)
        decision_cap, policy_flags = self._decision_cap(tier_counts)

        objections = []
        if policy_flags.get("no_evidence"):
            objections.append("No concrete supporting evidence was provided.")
        if policy_flags.get("speculative_only"):
            objections.append("All listed evidence is speculative synthesis rather than direct support.")
        if policy_flags.get("no_direct_support"):
            objections.append("No Tier A direct support is present yet.")

        reasoning_parts = [
            f"Evidence tiers: A={tier_counts['tier_a']}, B={tier_counts['tier_b']}, C={tier_counts['tier_c']}",
            f"Strongest tier available: {strongest_tier.replace('_', ' ').upper()}.",
        ]
        if decision_cap != "accepted":
            reasoning_parts.append(f"Governance cap applied: evidence currently supports at most '{decision_cap}'.")
        else:
            reasoning_parts.append("Evidence quality is sufficient to avoid a governance-based decision cap.")

        return {
            "agent": "evidence_reviewer",
            "verdict": strongest_tier,
            "reasoning": " ".join(reasoning_parts),
            "objections": objections,
            "score_contributions": {
                "evidence_quality": quality_score,
                "tier_a_presence": 1.0 if tier_counts["tier_a"] else 0.0,
                "speculation_penalty": round(min(tier_counts["tier_c"] / max(len(evidence_items), 1), 1.0), 3),
            },
            "tier_counts": tier_counts,
            "strongest_tier": strongest_tier,
            "classified_items": classified_items,
            "decision_cap": decision_cap,
            "policy_flags": policy_flags,
            "advisory_only": True,
        }

    def classify_item(self, item: str) -> tuple[str, str]:
        return self._classify_item(item)

    def _empty_review(self) -> dict:
        return {
            "agent": "evidence_reviewer",
            "verdict": "none",
            "reasoning": "No evidence items were provided, so the hypothesis cannot advance beyond a held state under governance policy.",
            "objections": ["No evidence items were provided."],
            "score_contributions": {
                "evidence_quality": 0.0,
                "tier_a_presence": 0.0,
                "speculation_penalty": 0.0,
            },
            "tier_counts": {"tier_a": 0, "tier_b": 0, "tier_c": 0},
            "strongest_tier": "none",
            "classified_items": [],
            "decision_cap": "held",
            "policy_flags": {"no_evidence": True, "speculative_only": False, "no_direct_support": True},
            "advisory_only": True,
        }

    def _classify_item(self, item: str) -> tuple[str, str]:
        text = item.lower()
        has_direct = any(marker in text for marker in self.DIRECT_MARKERS) or bool(self.NUMERIC_MEASUREMENT_PATTERN.search(text))
        has_weak = any(marker in text for marker in self.WEAK_MARKERS)
        has_speculative = any(marker in text for marker in self.SPECULATIVE_MARKERS)

        if has_direct and not has_speculative:
            return "tier_a", "direct observational or source-backed marker"
        if has_direct and has_speculative:
            return "tier_b", "mixed direct and speculative language"
        if has_weak:
            return "tier_b", "weak or partial support marker"
        if has_speculative:
            return "tier_c", "speculative synthesis marker"
        return "tier_b", "unclassified evidence treated as weak support"

    def _decision_cap(self, tier_counts: dict) -> tuple[str, dict]:
        total = sum(tier_counts.values())
        policy_flags = {
            "no_evidence": total == 0,
            "speculative_only": total > 0 and tier_counts["tier_c"] == total,
            "no_direct_support": tier_counts["tier_a"] == 0,
        }
        if policy_flags["no_evidence"] or policy_flags["speculative_only"]:
            return "held", policy_flags
        if policy_flags["no_direct_support"]:
            return "needs_revision", policy_flags
        return "accepted", policy_flags


class QuantReviewerAgent:
    """Deterministic quantitative consistency reviewer with no external dependencies."""

    NUMERIC_PATTERN = re.compile(r"(?<![A-Za-z])(\d+(?:\.\d+)?)\s*(?:%|σ|hz|khz|mhz|ghz|s|sec|seconds?|min|minutes?|hr|hours?|days?|years?|kpc|mpc|pc|ly|km/s|m/s|kev|ev)?", re.IGNORECASE)

    def should_review(self, hyp: dict) -> bool:
        text = self._combined_text(hyp)
        if self.NUMERIC_PATTERN.search(text):
            return True
        if len(self._extract_quantities(text)) >= 2:
            return True
        return any(keyword in text.lower() for keyword in ("equation", "model", "fit", "scale", "rate", "period", "redshift", "flux", "luminosity"))

    def review(self, hyp: dict) -> dict:
        text = self._combined_text(hyp)
        quantities = self._extract_quantities(text)
        equations = self._select_equations(quantities)
        missing_inputs = self._infer_missing_inputs(quantities, equations)
        numeric_hits = self.NUMERIC_PATTERN.findall(text)
        prediction_count = len(hyp.get("predictions") or [])
        evidence_count = len(hyp.get("evidence") or [])

        specificity = min(1.0, 0.2 * len(quantities) + (0.25 if numeric_hits else 0.0) + (0.15 if prediction_count else 0.0))
        grounding = max(0.0, min(1.0, 0.35 + 0.2 * len(equations) - 0.12 * len(missing_inputs) + (0.08 if evidence_count else 0.0)))
        falsifiability = max(0.0, min(1.0, 0.25 + 0.18 * prediction_count + (0.12 if numeric_hits else 0.0) - (0.08 if not equations else 0.0)))
        average = round((specificity + grounding + falsifiability) / 3, 3)

        if average >= 0.7:
            verdict = "strong"
        elif average >= 0.45:
            verdict = "plausible"
        else:
            verdict = "weak"

        objections = []
        if not numeric_hits:
            objections.append("No explicit quantitative anchor was found in the claim, predictions, or evidence.")
        if missing_inputs:
            objections.append("Relevant physical relations are implied, but some required inputs are still missing.")
        if prediction_count == 0:
            objections.append("No quantitative prediction is available for falsification.")

        reasoning_bits = []
        if quantities:
            reasoning_bits.append(f"Detected quantitative dimensions: {', '.join(quantities)}.")
        else:
            reasoning_bits.append("The hypothesis uses limited quantitative language.")
        if equations:
            reasoning_bits.append(f"Relevant check families: {', '.join(equations)}.")
        if missing_inputs:
            reasoning_bits.append(f"Missing inputs for a stronger quantitative review: {', '.join(missing_inputs)}.")
        else:
            reasoning_bits.append("Enough placeholders exist to attempt a later equation-backed review once real data is attached.")

        return {
            "agent": "quant_reviewer",
            "verdict": verdict,
            "reasoning": " ".join(reasoning_bits),
            "objections": objections,
            "score_contributions": {
                "quantitative_specificity": round(specificity, 3),
                "dimensional_grounding": round(grounding, 3),
                "falsifiability": round(falsifiability, 3),
            },
            "quantitative_claims_present": bool(quantities),
            "extracted_quantities": quantities,
            "equations_considered": equations,
            "missing_inputs": missing_inputs,
            "numeric_anchors": numeric_hits[:6],
            "recommended_measurements": self._recommended_measurements(quantities, missing_inputs),
            "advisory_only": True,
            "stub": True,
        }

    def _combined_text(self, hyp: dict) -> str:
        return " ".join([
            hyp.get("title", ""),
            hyp.get("claim", ""),
            " ".join(hyp.get("predictions", []) or []),
            " ".join(hyp.get("evidence", []) or []),
        ])

    def _extract_quantities(self, text: str) -> list[str]:
        found = []
        for name, patterns in QUANT_REVIEW_PATTERNS.items():
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
                found.append(name)
        return found

    def _select_equations(self, quantities: list[str]) -> list[str]:
        quantity_set = set(quantities)
        equations = []
        for required, label in QUANT_EQUATION_RULES:
            overlap = len(quantity_set.intersection(required))
            threshold = 1 if len(required) == 1 else max(1, len(required) - 1)
            if overlap >= threshold:
                equations.append(label)
        return equations

    def _infer_missing_inputs(self, quantities: list[str], equations: list[str]) -> list[str]:
        quantity_set = set(quantities)
        missing = set()
        for equation in equations:
            missing.update(QUANT_EXPECTED_INPUTS.get(equation, set()) - quantity_set)
        return sorted(missing)

    def _recommended_measurements(self, quantities: list[str], missing_inputs: list[str]) -> list[str]:
        recommendations = []
        if "time" in quantities and "frequency" not in quantities:
            recommendations.append("Record cadence or period estimates alongside any variability claim.")
        if "flux" in quantities and "distance" in missing_inputs:
            recommendations.append("Add a distance prior before using brightness as a physical luminosity argument.")
        if "orbit" in quantities and "mass" in missing_inputs:
            recommendations.append("Provide a mass estimate or dynamical prior for orbital claims.")
        if "statistics" in quantities:
            recommendations.append("State the sample size and significance threshold explicitly.")
        if not recommendations:
            recommendations.append("Attach one measured quantity with units to the next review cycle.")
        return recommendations


class ReflectionAgent:
    """Advisory pass for unresolved hypotheses. Audit-first and non-mutating."""

    def __init__(self, nemotron):
        self.nemotron = nemotron

    def review(self, hyp: dict, skeptic_review: dict, archivist_review: dict, judge_review: dict, decision: str, final_score: float, breakdown: dict) -> dict:
        prompt = REFLECTION_PROMPT.format(
            title=hyp["title"],
            claim=hyp["claim"],
            predictions=json.dumps(hyp.get("predictions", []), ensure_ascii=False),
            evidence=json.dumps(hyp.get("evidence", []), ensure_ascii=False),
            context_domains=json.dumps(hyp.get("context_domains", []), ensure_ascii=False),
            skeptic_review=json.dumps(skeptic_review, indent=2, ensure_ascii=False),
            archivist_review=json.dumps(archivist_review, indent=2, ensure_ascii=False),
            judge_review=json.dumps(judge_review, indent=2, ensure_ascii=False),
            decision=decision,
            final_score=round(final_score, 3),
            breakdown=json.dumps(breakdown, indent=2, ensure_ascii=False),
        )
        result = self.nemotron.chat_json(prompt, "Produce an audit-only reflection review.", temperature=0.2)
        if not result:
            result = self._default_review(decision, final_score, breakdown)

        result.setdefault("verdict", "hold_for_evidence" if decision == "held" else "revise_and_retry")
        result.setdefault("reasoning", "Reflection pass completed without additional structured guidance.")
        result.setdefault("objections", [])
        result.setdefault(
            "score_contributions",
            {
                "readiness": round(final_score, 3),
                "evidence_support": round(breakdown.get("evidence_support", 0.5), 3),
                "testability": round(breakdown.get("testability", 0.5), 3),
            },
        )
        result.setdefault("recommended_decision", decision)
        result.setdefault("rerun_worthy", decision == "needs_revision")
        result.setdefault("blockers", list(result.get("objections", [])))
        result.setdefault("concrete_revisions", [])
        result.setdefault("evidence_requests", [])
        result["agent"] = "reflection"
        return result

    def _default_review(self, decision: str, final_score: float, breakdown: dict) -> dict:
        return {
            "verdict": "hold_for_evidence" if decision == "held" else "revise_and_retry",
            "reasoning": "Reflection agent failed to respond. Preserving the current council decision and recording the need for human follow-up.",
            "objections": [],
            "score_contributions": {
                "readiness": round(final_score, 3),
                "evidence_support": round(breakdown.get("evidence_support", 0.5), 3),
                "testability": round(breakdown.get("testability", 0.5), 3),
            },
            "recommended_decision": decision,
            "rerun_worthy": decision == "needs_revision",
            "blockers": [],
            "concrete_revisions": [],
            "evidence_requests": [],
        }


# ─── SCORING ENGINE ──────────────────────────────────────────────────

class ScoringEngine:
    """5-dimension weighted scoring."""

    WEIGHTS = {
        "coherence": 0.25,
        "evidence_support": 0.20,
        "testability": 0.25,
        "novelty": 0.15,
        "redundancy_penalty": 0.15,
    }

    @classmethod
    def compute(cls, judge_scores: dict) -> tuple[float, dict]:
        """Returns (final_score, breakdown)."""
        breakdown = {}
        weighted_sum = 0.0

        for dim, weight in cls.WEIGHTS.items():
            raw = judge_scores.get(dim, 0.5)
            if dim == "redundancy_penalty":
                # Redundancy is inverted: high penalty = bad
                contribution = (1.0 - raw) * weight
            else:
                contribution = raw * weight
            breakdown[dim] = round(raw, 3)
            weighted_sum += contribution

        final = round(min(max(weighted_sum, 0.0), 1.0), 3)
        return final, breakdown


# ─── DECISION ENGINE ─────────────────────────────────────────────────

class DecisionEngine:
    """Threshold-based routing of final scores to decisions."""

    @staticmethod
    def decide(final_score: float, archivist_verdict: str, merge_target: str = None) -> str:
        # If archivist flagged as duplicate, force merge
        if archivist_verdict == "duplicate" and merge_target:
            return "merged"
        if final_score >= 0.70:
            return "accepted"
        if final_score >= 0.50:
            return "needs_revision"
        if final_score >= 0.30:
            return "held"
        return "rejected"


class _GraphPreviewMemoryProxy:
    """Read-through, no-write proxy for safe shadow graph comparisons."""

    def __init__(self, memory):
        self._memory = memory

    def __getattr__(self, name):
        return getattr(self._memory, name)

    def add_auto_hypothesis(self, hyp):
        return None

    def save_review(self, hypothesis_id, agent_name, review):
        return None

    def save_decision(self, hypothesis_id, decision, score, breakdown, reasoning, merged_with=None):
        return None

    def dismiss_pending_evidence_requests(self, hypothesis_id, reason=""):
        return 0

    def sync_evidence_requests_for_hypothesis(self, hypothesis_id, requests, triggering_decision="held"):
        preview_items = []
        for index, request in enumerate(requests, start=1):
            item = dict(request)
            item.update({
                "id": index,
                "hypothesis_id": hypothesis_id,
                "status": "preview",
                "triggering_decision": triggering_decision,
            })
            preview_items.append(item)
        return preview_items


class _GraphPreviewAgentLogProxy:
    """No-op agent log for safe shadow graph comparisons."""

    def __init__(self, agent_log):
        self._agent_log = agent_log

    def __getattr__(self, name):
        return getattr(self._agent_log, name)

    def add(self, event_type, message):
        return None


# ─── HYPOTHESIS COUNCIL ORCHESTRATOR ─────────────────────────────────

class HypothesisCouncil:
    """The full review pipeline orchestrator."""

    def __init__(self, nemotron, memory, agent_log, anthropic_key: str | None = None, council_graph_mode: str | None = None):
        self.memory = memory
        self.agent_log = agent_log
        self.normalizer = HypothesisNormalizer()
        self.embedder = EmbeddingSimilarity()
        self.skeptic = SkepticAgent(nemotron)
        self.archivist = ArchivistAgent(nemotron, self.embedder)
        self.evidence_reviewer = EvidenceReviewerAgent()
        self.quant_reviewer = QuantReviewerAgent()
        self.judge = JudgeAgent(nemotron, anthropic_key)
        self.reflection = ReflectionAgent(nemotron)
        self.council_graph_mode = self._resolve_graph_mode(council_graph_mode)
        self._graph_runner_cache = {}
        log.info("Hypothesis Review Council initialized ✓")

    def _resolve_graph_mode(self, requested_mode: str | None) -> str:
        normalized = (requested_mode or os.getenv("MANATUABON_COUNCIL_GRAPH_MODE", "off")).strip().lower()
        if normalized not in {"off", "primary", "shadow"}:
            log.warning("Unknown MANATUABON_COUNCIL_GRAPH_MODE='%s' — defaulting to off", normalized)
            return "off"
        return normalized

    def _get_graph_mode(self) -> str:
        mode = getattr(self, "council_graph_mode", None)
        if mode in {"off", "primary", "shadow"}:
            return mode
        mode = self._resolve_graph_mode(None)
        self.council_graph_mode = mode
        return mode

    def _get_graph_runner_cache(self) -> dict:
        cache = getattr(self, "_graph_runner_cache", None)
        if cache is None:
            cache = {}
            self._graph_runner_cache = cache
        return cache

    def _build_shadow_council(self):
        shadow = self.__class__.__new__(self.__class__)
        shadow.__dict__ = self.__dict__.copy()
        shadow.memory = _GraphPreviewMemoryProxy(self.memory)
        shadow.agent_log = _GraphPreviewAgentLogProxy(self.agent_log)
        shadow._graph_runner_cache = {}
        return shadow

    def _get_council_graph_runner(self, preview: bool = False):
        from council_graph import CouncilGraphRunner

        cache = self._get_graph_runner_cache()
        cache_key = "preview" if preview else "primary"
        runner = cache.get(cache_key)
        if runner:
            return runner

        owner = self._build_shadow_council() if preview else self
        runner = CouncilGraphRunner(owner)
        cache[cache_key] = runner
        return runner

    def _get_held_rereview_graph_runner(self):
        from council_graph import HeldReReviewGraphRunner

        cache = self._get_graph_runner_cache()
        runner = cache.get("held_rereview")
        if runner:
            return runner
        runner = HeldReReviewGraphRunner(self)
        cache["held_rereview"] = runner
        return runner

    def _get_evidence_request_closure_graph_runner(self):
        from council_graph import EvidenceRequestClosureGraphRunner

        cache = self._get_graph_runner_cache()
        runner = cache.get("evidence_request_closure")
        if runner:
            return runner
        runner = EvidenceRequestClosureGraphRunner(self)
        cache["evidence_request_closure"] = runner
        return runner

    def _normalize_graph_result(self, state: dict) -> dict:
        decision = state.get("decision")
        hypothesis_id = (state.get("hypothesis") or {}).get("id")
        normalized = {
            "decision": decision,
            "hypothesis_id": hypothesis_id,
            "graph_audit": state.get("audit_trail", []),
        }
        if state.get("status"):
            normalized["status"] = state.get("status")
        if state.get("legacy"):
            normalized["legacy"] = state.get("legacy")

        if decision == "rejected_auto":
            normalized["reason"] = state.get("reasoning", "")
            return normalized

        if decision == "merged":
            normalized["similarity"] = state.get("score")
            normalized["merge_target"] = state.get("merge_target")
            return normalized

        normalized.update({
            "score": state.get("score"),
            "breakdown": state.get("breakdown", {}),
            "evidence_review": state.get("evidence_review"),
            "evidence_requests": state.get("evidence_requests", []),
            "decision_adjustments": state.get("decision_adjustments", []),
            "quant_review": state.get("quant_review"),
            "reflection": state.get("reflection_review"),
        })
        return normalized

    def _compare_graph_result(self, hyp: dict, legacy_result: dict, graph_result: dict) -> None:
        legacy_decision = legacy_result.get("decision")
        graph_decision = graph_result.get("decision")
        if legacy_decision != graph_decision:
            log.warning("Council graph parity mismatch for %s: legacy=%s graph=%s", hyp["id"], legacy_decision, graph_decision)
            self.agent_log.add(
                "council_graph_parity_mismatch",
                f"graph parity mismatch: '{hyp['title']}' legacy={legacy_decision} graph={graph_decision}",
            )
            return
        self.agent_log.add(
            "council_graph_parity_match",
            f"graph parity match: '{hyp['title']}' -> {legacy_decision}",
        )

    def _maybe_run_evidence_review(self, hyp: dict) -> dict | None:
        if not getattr(self, "evidence_reviewer", None):
            return None
        try:
            log.info("🧾 Evidence reviewer checking: %s", hyp["title"])
            evidence_review = self.evidence_reviewer.review(hyp)
            self.memory.save_review(hyp["id"], "evidence_reviewer", evidence_review)
            log.info("   Evidence tier: %s", evidence_review.get("strongest_tier", evidence_review.get("verdict")))
            self.agent_log.add(
                "council_evidence_review",
                f"🧾 evidence reviewer: '{hyp['title']}' -> cap {evidence_review.get('decision_cap', 'accepted')}"
            )
            return evidence_review
        except (KeyError, ValueError, TypeError, RuntimeError) as e:
            log.warning("Evidence review failed for %s: %s", hyp["title"], e)
            return None

    @staticmethod
    def _format_memory_evidence_item(memory: dict) -> str:
        summary = (memory.get("summary") or "").strip()
        label_bits = [f"Memory #{memory.get('id', '?')}"]
        confidence_label = memory.get("confidence_label")
        if confidence_label:
            label_bits.append(confidence_label)
        domain_tags = memory.get("domain_tags") or []
        if domain_tags:
            label_bits.append("domains: " + ", ".join(domain_tags[:3]))
        return f"[{' | '.join(label_bits)}] {summary}".strip()

    def _hydrate_hypothesis_provenance(self, hyp: dict) -> dict:
        source_memory_ids = list(hyp.get("source_memory_ids") or [])
        source_memory = hyp.get("source_memory")
        if source_memory is not None and source_memory not in source_memory_ids:
            source_memory_ids.insert(0, source_memory)

        normalized_ids = []
        for item in source_memory_ids:
            try:
                normalized_ids.append(int(item))
            except (TypeError, ValueError):
                continue

        if not normalized_ids or not hasattr(self.memory, "get_memories_by_ids"):
            return hyp

        source_memories = self.memory.get_memories_by_ids(normalized_ids)
        if not source_memories:
            return hyp

        if not hyp.get("evidence"):
            hyp["evidence"] = [
                self._format_memory_evidence_item(memory)
                for memory in source_memories
                if memory.get("summary")
            ]

        if not hyp.get("context_domains"):
            hyp["context_domains"] = sorted({
                domain
                for memory in source_memories
                for domain in (memory.get("domain_tags") or [])
            })

        if not hyp.get("sources"):
            sources = [f"Memory #{memory['id']}" for memory in source_memories if memory.get("id") is not None]
            if hyp.get("source_file"):
                sources.insert(0, hyp["source_file"])
            hyp["sources"] = sources

        return hyp

    def _build_evidence_request_payloads(self, hyp: dict, evidence_review: dict | None, quant_review: dict | None, reflection_review: dict | None) -> list[dict]:
        requests = []
        policy_flags = (evidence_review or {}).get("policy_flags", {}) if evidence_review else {}

        if policy_flags.get("no_evidence"):
            requests.append({
                "request_text": "Attach at least one concrete evidence item linked to this hypothesis before re-review.",
                "priority": "high",
                "source_agent": "evidence_reviewer",
                "source_context": {"flag": "no_evidence"},
            })
        if policy_flags.get("speculative_only"):
            requests.append({
                "request_text": "Replace Tier C-only speculative support with at least one Tier A or Tier B evidence item.",
                "priority": "high",
                "source_agent": "evidence_reviewer",
                "source_context": {"flag": "speculative_only"},
            })
        if policy_flags.get("no_direct_support"):
            requests.append({
                "request_text": "Add direct observational or literature-backed support before considering acceptance.",
                "priority": "medium",
                "source_agent": "evidence_reviewer",
                "source_context": {"flag": "no_direct_support"},
            })

        for item in (reflection_review or {}).get("evidence_requests", []) or []:
            request_text = str(item).strip()
            if request_text:
                requests.append({
                    "request_text": request_text,
                    "priority": "medium",
                    "source_agent": "reflection",
                    "source_context": {},
                })

        for item in (quant_review or {}).get("recommended_measurements", []) or []:
            request_text = str(item).strip()
            if request_text:
                requests.append({
                    "request_text": request_text,
                    "priority": "medium",
                    "source_agent": "quant_reviewer",
                    "source_context": {},
                })

        deduped = []
        seen = set()
        for request in requests:
            key = request["request_text"].strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(request)
        return deduped

    def _sync_evidence_request_workflow(self, hyp: dict, decision: str, evidence_review: dict | None, quant_review: dict | None, reflection_review: dict | None) -> list[dict]:
        if decision != "held":
            if hasattr(self.memory, "dismiss_pending_evidence_requests"):
                self.memory.dismiss_pending_evidence_requests(
                    hyp["id"],
                    reason=f"Hypothesis moved to {decision}; pending evidence requests closed.",
                )
            return []

        if not hasattr(self.memory, "sync_evidence_requests_for_hypothesis"):
            return []

        payloads = self._build_evidence_request_payloads(hyp, evidence_review, quant_review, reflection_review)
        if not payloads:
            return []
        synced = self.memory.sync_evidence_requests_for_hypothesis(hyp["id"], payloads, triggering_decision=decision)
        if synced:
            self.agent_log.add(
                "council_evidence_requests",
                f"🧷 evidence requests: '{hyp['title']}' -> {len(synced)} pending task(s)"
            )
        return synced

    def _material_evidence_for_rereview(self, hypothesis_id: str, last_decision_timestamp: str | None) -> list[dict]:
        if not hasattr(self.memory, "get_material_evidence_since"):
            return []
        return self.memory.get_material_evidence_since(hypothesis_id, last_decision_timestamp)

    def _maybe_run_quant_review(self, hyp: dict) -> dict | None:
        if not getattr(self, "quant_reviewer", None):
            return None
        if not self.quant_reviewer.should_review(hyp):
            return None
        try:
            log.info("📐 Quant reviewer checking: %s", hyp["title"])
            quant_review = self.quant_reviewer.review(hyp)
            self.memory.save_review(hyp["id"], "quant_reviewer", quant_review)
            log.info("   Quant verdict: %s", quant_review.get("verdict"))
            self.agent_log.add(
                "council_quant_review",
                f"📐 quant reviewer: '{hyp['title']}' -> {quant_review.get('verdict', 'n/a')}"
            )
            return quant_review
        except (KeyError, ValueError, TypeError, RuntimeError) as e:
            log.warning("Quant review failed for %s: %s", hyp["title"], e)
            return None

    def _should_trigger_reflection(self, decision: str, final_score: float, breakdown: dict) -> bool:
        if decision == "held":
            return True
        if decision != "needs_revision":
            return False
        return (
            final_score < 0.62
            or breakdown.get("evidence_support", 0.5) < 0.55
            or breakdown.get("testability", 0.5) < 0.55
            or breakdown.get("coherence", 0.5) < 0.55
        )

    def _maybe_run_reflection(self, hyp: dict, skeptic_review: dict, archivist_review: dict, judge_review: dict, decision: str, final_score: float, breakdown: dict) -> dict | None:
        if not self._should_trigger_reflection(decision, final_score, breakdown):
            return None
        try:
            log.info("🪞 Reflection reviewing: %s", hyp["title"])
            reflection_review = self.reflection.review(
                hyp,
                skeptic_review,
                archivist_review,
                judge_review,
                decision,
                final_score,
                breakdown,
            )
            self.memory.save_review(hyp["id"], "reflection", reflection_review)
            log.info("   Reflection verdict: %s", reflection_review.get("verdict"))
            self.agent_log.add(
                "council_reflection",
                f"🪞 reflection: '{hyp['title']}' -> {reflection_review.get('recommended_decision', decision)}"
            )
            return reflection_review
        except (KeyError, ValueError, TypeError, RuntimeError) as e:
            log.warning("Reflection review failed for %s: %s", hyp["title"], e)
            return None

    def _apply_evidence_policy(self, decision: str, evidence_review: dict | None) -> tuple[str, list[str]]:
        if decision == "merged" or not evidence_review:
            return decision, []
        decision_cap = evidence_review.get("decision_cap", "accepted")
        if decision_cap not in EVIDENCE_DECISION_RANK or decision not in EVIDENCE_DECISION_RANK:
            return decision, []
        if EVIDENCE_DECISION_RANK[decision] <= EVIDENCE_DECISION_RANK[decision_cap]:
            return decision, []
        policy_flags = evidence_review.get("policy_flags", {})
        reasons = []
        if policy_flags.get("no_evidence"):
            reasons.append("No evidence items were provided.")
        if policy_flags.get("speculative_only"):
            reasons.append("Only Tier C speculative evidence was present.")
        if policy_flags.get("no_direct_support") and decision_cap == "needs_revision":
            reasons.append("No Tier A direct support is present yet.")
        return decision_cap, reasons

    def _finalize_review_cycle(self, hyp: dict, skeptic_review: dict, archivist_review: dict, judge_review: dict, evidence_review: dict | None = None, quant_review: dict | None = None) -> dict:
        judge_scores = judge_review.get("scores", {})
        final_score, breakdown = ScoringEngine.compute(judge_scores)
        log.info("   Judge score: %.3f", final_score)

        archivist_verdict = archivist_review.get("verdict", "unique")
        merge_target = judge_review.get("merge_target")
        decision = DecisionEngine.decide(final_score, archivist_verdict, merge_target)
        policy_adjustments = []
        governed_decision, policy_adjustments = self._apply_evidence_policy(decision, evidence_review)
        if governed_decision != decision:
            log.info("🧭 Evidence policy downgraded decision: %s -> %s", decision, governed_decision)
            self.agent_log.add(
                "council_evidence_gate",
                f"🧭 evidence gate: '{hyp['title']}' {decision} -> {governed_decision}"
            )
            decision = governed_decision
        reflection_review = self._maybe_run_reflection(
            hyp,
            skeptic_review,
            archivist_review,
            judge_review,
            decision,
            final_score,
            breakdown,
        )

        reasoning = judge_review.get("reasoning", "No reasoning provided")
        if policy_adjustments:
            reasoning = reasoning + "\n\nGovernance evidence gate: " + " ".join(policy_adjustments)
        self.memory.save_decision(hyp["id"], decision, final_score, breakdown, reasoning)

        evidence_requests = self._sync_evidence_request_workflow(hyp, decision, evidence_review, quant_review, reflection_review)

        emoji = {"accepted": "🟢", "rejected": "🔴", "needs_revision": "🟡", "held": "⚪", "merged": "🔵"}.get(decision, "❓")
        log.info("%s COUNCIL DECISION: %s → %s (score: %.3f)", emoji, hyp["title"], decision, final_score)
        self.agent_log.add("council_decision", f"{emoji} {decision}: '{hyp['title']}' (score: {final_score:.3f})")

        return {
            "decision": decision,
            "score": final_score,
            "breakdown": breakdown,
            "hypothesis_id": hyp["id"],
            "evidence_review": evidence_review,
            "evidence_requests": evidence_requests,
            "decision_adjustments": policy_adjustments,
            "quant_review": quant_review,
            "reflection": reflection_review,
        }

    def _run_review_pipeline_legacy(self, hyp: dict, *, persist_proposed: bool, auto_reject: bool) -> dict:
        hyp_id = hyp["id"]
        log.info("━━━ COUNCIL REVIEW: %s ━━━", hyp["title"])

        if auto_reject:
            should_reject, reject_reason = self.normalizer.auto_reject(hyp)
            if should_reject:
                log.info("⛔ Auto-rejected: %s — %s", hyp["title"], reject_reason)
                self.agent_log.add("council_auto_reject", f"Rejected '{hyp['title']}': {reject_reason}")

                hyp["status"] = "rejected_auto"
                self.memory.add_auto_hypothesis(hyp)
                self.memory.save_decision(hyp_id, "rejected_auto", 0.0, {}, reject_reason)
                return {"decision": "rejected_auto", "reason": reject_reason, "hypothesis_id": hyp_id}

        if persist_proposed:
            hyp["status"] = "proposed"
            self.memory.add_auto_hypothesis(hyp)

        log.info("🔴 Skeptic reviewing: %s", hyp["title"])
        skeptic_review = self.skeptic.review(hyp)
        self.memory.save_review(hyp_id, "skeptic", skeptic_review)
        log.info("   Skeptic verdict: %s", skeptic_review.get("verdict"))

        log.info("📚 Archivist reviewing: %s", hyp["title"])
        existing = self.memory.get_hypothesis_titles_and_bodies()
        existing = [h for h in existing if h["id"] != hyp_id]
        archivist_review = self.archivist.review(hyp, existing)
        self.memory.save_review(hyp_id, "archivist", archivist_review)
        log.info("   Archivist verdict: %s (sim: %.2f)", archivist_review.get("verdict"), archivist_review.get("similarity_score", 0))

        evidence_review = self._maybe_run_evidence_review(hyp)
        quant_review = self._maybe_run_quant_review(hyp)

        if (
            archivist_review.get("verdict") == "duplicate"
            and archivist_review.get("most_similar")
            and archivist_review.get("similarity_score", 0) > 0.75
        ):
            log.info("🔵 MERGE detected! Similarity %.2f > 0.75", archivist_review["similarity_score"])
            self.memory.save_decision(
                hyp_id, "merged", archivist_review["similarity_score"],
                {}, f"Merged: cosine similarity {archivist_review['similarity_score']:.2f} with '{archivist_review.get('most_similar', 'unknown')}'",
                merged_with=archivist_review.get("most_similar")
            )
            self.agent_log.add("council_merged", f"Merged '{hyp['title']}' with '{archivist_review.get('most_similar')}'")
            return {"decision": "merged", "similarity": archivist_review["similarity_score"], "hypothesis_id": hyp_id}

        log.info("⚖️ Judge reviewing: %s", hyp["title"])
        judge_review = self.judge.review(hyp, skeptic_review, archivist_review, evidence_review=evidence_review, quant_review=quant_review)
        self.memory.save_review(hyp_id, "judge", judge_review)
        return self._finalize_review_cycle(hyp, skeptic_review, archivist_review, judge_review, evidence_review=evidence_review, quant_review=quant_review)

    def _run_review_pipeline(self, hyp: dict, *, persist_proposed: bool, auto_reject: bool) -> dict:
        hyp = self._hydrate_hypothesis_provenance(hyp)
        graph_mode = self._get_graph_mode()

        if graph_mode == "primary":
            graph_result = self._get_council_graph_runner().run(
                copy.deepcopy(hyp),
                persist_proposed=persist_proposed,
                auto_reject=auto_reject,
            )
            return self._normalize_graph_result(graph_result)

        legacy_result = self._run_review_pipeline_legacy(hyp, persist_proposed=persist_proposed, auto_reject=auto_reject)

        if graph_mode == "shadow":
            graph_result = self._get_council_graph_runner(preview=True).run(
                copy.deepcopy(hyp),
                persist_proposed=persist_proposed,
                auto_reject=auto_reject,
            )
            self._compare_graph_result(hyp, legacy_result, self._normalize_graph_result(graph_result))

        return legacy_result

    def _load_existing_hypothesis(self, hypothesis_id: str):
        with self.memory._get_conn() as c:
            return c.execute("SELECT * FROM hypotheses WHERE id=?", (hypothesis_id,)).fetchone()

    def _build_existing_hypothesis_payload(self, row) -> dict:
        raw_evidence = row["evidence"] if "evidence" in row.keys() else None
        parsed_evidence = HypothesisNormalizer._normalize_evidence(raw_evidence)
        return {
            "id": row["id"],
            "title": row["title"] or "Untitled Hypothesis",
            "claim": row["description"] or "",
            "body": row["description"] or "",
            "predictions": [],
            "evidence": parsed_evidence,
            "sources": [row["source"]] if row["source"] else [],
            "confidence": row["confidence"] if row["confidence"] is not None else 0.5,
            "timestamp": row["updated_at"] or row["created_at"] or row["date"] or datetime.now().isoformat(),
        }

    def review(self, raw_hypothesis: dict) -> dict:
        """Full review pipeline. Returns the decision dict."""
        hyp = self.normalizer.normalize(raw_hypothesis)
        return self._run_review_pipeline(hyp, persist_proposed=True, auto_reject=True)

    def review_existing(self, hypothesis_id: str, force: bool = False) -> dict:
        row = self._load_existing_hypothesis(hypothesis_id)
        if not row:
            return {"status": "error", "hypothesis_id": hypothesis_id, "reason": "Hypothesis not found"}

        existing_decision = self.memory.get_decision_for_hypothesis(hypothesis_id)
        if existing_decision and not force:
            return {
                "status": "skipped",
                "hypothesis_id": hypothesis_id,
                "reason": "Council decision already exists",
                "decision": existing_decision,
            }

        hyp = self.normalizer.normalize(self._build_existing_hypothesis_payload(row))
        self.agent_log.add("council_reprocess", f"Reprocessing legacy hypothesis '{hyp['title']}'")
        result = self._run_review_pipeline(hyp, persist_proposed=False, auto_reject=False)
        result["status"] = "processed"
        result["legacy"] = True
        return result

    def reprocess_legacy(self, limit: int = 5, active_only: bool = True, force: bool = False, origin: str | None = None, status: str | None = None) -> dict:
        candidates = self.memory.get_all_hypotheses(normalized=True, origin=origin, status=status, active_only=active_only)
        queued = []
        skipped = 0

        for hyp in candidates:
            if self.memory.get_decision_for_hypothesis(hyp["id"]) and not force:
                skipped += 1
                continue
            queued.append(hyp)
            if len(queued) >= max(1, min(limit, 25)):
                break

        processed = []
        for hyp in queued:
            processed.append(self.review_existing(hyp["id"], force=force))

        return {
            "status": "ok",
            "requested_limit": limit,
            "processed_count": len(processed),
            "skipped_existing_count": skipped,
            "processed": processed,
        }

    def re_evaluate_held(self):
        """Re-evaluate hypotheses with 'held' status from prior cycles."""
        if self._get_graph_mode() == "primary":
            return self._get_held_rereview_graph_runner().run(limit=3)

        held = self.memory.get_all_decisions(status_filter="held")
        if not held:
            return
        log.info("♻️ Re-evaluating %d held hypotheses...", len(held))
        for h in held[:3]:  # Cap at 3 per cycle to save tokens
            hid = h["hypothesis_id"]
            material_evidence = self._material_evidence_for_rereview(hid, h.get("timestamp"))
            if not material_evidence:
                log.info("♻️ Skipping held hypothesis without new Tier A/B evidence: %s", hid)
                self.agent_log.add("council_rereview_skipped", f"Held hypothesis '{hid}' skipped: no new Tier A/B evidence")
                continue
            # Get the hypothesis data
            with self.memory._get_conn() as c:
                row = c.execute("SELECT * FROM hypotheses WHERE id=?", (hid,)).fetchone()
                if row:
                    evidence_items = []
                    if row["evidence"]:
                        evidence_items.append(row["evidence"])
                    evidence_items.extend([
                        f"[{item['tier'].upper()} {item['relation']}] Memory #{item['memory_id']}: {item['summary']}"
                        for item in material_evidence
                    ])
                    raw = {
                        "id": row["id"], "title": row["title"],
                        "body": row["description"], "confidence": 0.5,
                        "predictions": [], "evidence": evidence_items, "timestamp": datetime.now().isoformat()
                    }
                    # Don't re-insert, just re-review
                    log.info("♻️ Re-reviewing held hypothesis: %s", row["title"])
                    # Run just skeptic + judge (archivist already checked)
                    normalized = self.normalizer.normalize(raw)
                    skeptic_review = self.skeptic.review(normalized)
                    self.memory.save_review(hid, "skeptic", skeptic_review)
                    existing = self.memory.get_hypothesis_titles_and_bodies()
                    existing = [e for e in existing if e["id"] != hid]
                    archivist_review = self.archivist.review(normalized, existing)
                    self.memory.save_review(hid, "archivist", archivist_review)
                    evidence_review = self._maybe_run_evidence_review(normalized)
                    quant_review = self._maybe_run_quant_review(normalized)
                    judge_review = self.judge.review(
                        normalized, skeptic_review, archivist_review, evidence_review=evidence_review, quant_review=quant_review
                    )
                    self.memory.save_review(hid, "judge", judge_review)
                    finalized = self._finalize_review_cycle(normalized, skeptic_review, archivist_review, judge_review, evidence_review=evidence_review, quant_review=quant_review)
                    log.info("♻️ Held re-evaluation: %s → %s (%.3f)", row["title"], finalized["decision"], finalized["score"])

    def evaluate_evidence_request_closure(self, hypothesis_id: str | None = None, limit: int = 100) -> dict:
        return self._get_evidence_request_closure_graph_runner().run(hypothesis_id=hypothesis_id, limit=limit)
