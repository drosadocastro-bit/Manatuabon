from __future__ import annotations

from datetime import datetime
from typing import Any

from graph_state import CouncilGraphState, EvidenceRequestClosureState, HeldReReviewState
from hypothesis_council import DecisionEngine, ScoringEngine


class CouncilGraphRunner:
    """LangGraph-ready bounded council workflow runner.

    This intentionally mirrors the current HypothesisCouncil review pipeline
    without replacing it yet. The runner is dependency-light so the repo can
    establish state parity before adopting the LangGraph package itself.
    """

    def __init__(self, council: Any):
        self.council = council

    def run(self, hypothesis: dict[str, Any], *, persist_proposed: bool = True, auto_reject: bool = True) -> dict[str, Any]:
        state = CouncilGraphState(
            hypothesis=dict(hypothesis),
            persist_proposed=persist_proposed,
            auto_reject=auto_reject,
        )
        return self.run_state(state).to_dict()

    def run_state(self, state: CouncilGraphState) -> CouncilGraphState:
        self._load_context(state)
        if state.terminated:
            return state

        self._run_skeptic(state)
        self._run_archivist(state)
        self._run_evidence_review(state)
        self._run_quant_review(state)

        if self._merge_gate(state):
            return state

        self._run_judge(state)
        self._apply_evidence_policy(state)
        self._run_reflection_if_needed(state)
        self._persist_decision(state)
        self._sync_evidence_requests(state)
        self._emit_audit_summary(state)
        return state

    def _load_context(self, state: CouncilGraphState) -> None:
        hyp = state.hypothesis
        state.log_step("load_context", hypothesis_id=hyp.get("id"), title=hyp.get("title"))

        if state.auto_reject:
            should_reject, reject_reason = self.council.normalizer.auto_reject(hyp)
            if should_reject:
                hyp["status"] = "rejected_auto"
                if state.persist_proposed:
                    self.council.memory.add_auto_hypothesis(hyp)
                self.council.agent_log.add("council_auto_reject", f"Rejected '{hyp['title']}': {reject_reason}")
                self.council.memory.save_decision(hyp["id"], "rejected_auto", 0.0, {}, reject_reason)
                state.decision = "rejected_auto"
                state.reasoning = reject_reason
                state.terminated = True
                state.status = "processed"
                state.log_step("auto_reject", reason=reject_reason)
                return

        if state.persist_proposed:
            hyp["status"] = "proposed"
            self.council.memory.add_auto_hypothesis(hyp)
            state.log_step("persist_proposed", status="proposed")

        existing = self.council.memory.get_hypothesis_titles_and_bodies()
        state.existing_hypotheses = [item for item in existing if item["id"] != hyp["id"]]
        state.log_step("load_existing_hypotheses", count=len(state.existing_hypotheses))

    def _run_skeptic(self, state: CouncilGraphState) -> None:
        review = self.council.skeptic.review(state.hypothesis)
        state.skeptic_review = review
        self.council.memory.save_review(state.hypothesis["id"], "skeptic", review)
        state.log_step("run_skeptic", verdict=review.get("verdict"))

    def _run_archivist(self, state: CouncilGraphState) -> None:
        review = self.council.archivist.review(state.hypothesis, state.existing_hypotheses)
        state.archivist_review = review
        self.council.memory.save_review(state.hypothesis["id"], "archivist", review)
        state.log_step(
            "run_archivist",
            verdict=review.get("verdict"),
            similarity_score=review.get("similarity_score"),
            most_similar=review.get("most_similar"),
        )

    def _run_evidence_review(self, state: CouncilGraphState) -> None:
        state.evidence_review = self.council._maybe_run_evidence_review(state.hypothesis)
        state.log_step(
            "run_evidence_review",
            verdict=(state.evidence_review or {}).get("verdict"),
            decision_cap=(state.evidence_review or {}).get("decision_cap"),
        )

    def _run_quant_review(self, state: CouncilGraphState) -> None:
        state.quant_review = self.council._maybe_run_quant_review(state.hypothesis)
        state.log_step("run_quant_review", verdict=(state.quant_review or {}).get("verdict"))

    def _merge_gate(self, state: CouncilGraphState) -> bool:
        review = state.archivist_review or {}
        if not (
            review.get("verdict") == "duplicate"
            and review.get("most_similar")
            and review.get("similarity_score", 0) > 0.75
        ):
            state.log_step("merge_gate", merged=False)
            return False

        reasoning = (
            f"Merged: cosine similarity {review['similarity_score']:.2f} "
            f"with '{review.get('most_similar', 'unknown')}'"
        )
        self.council.memory.save_decision(
            state.hypothesis["id"],
            "merged",
            review["similarity_score"],
            {},
            reasoning,
            merged_with=review.get("most_similar"),
        )
        self.council.agent_log.add(
            "council_merged",
            f"Merged '{state.hypothesis['title']}' with '{review.get('most_similar')}'",
        )
        state.decision = "merged"
        state.score = review.get("similarity_score")
        state.merge_target = review.get("most_similar")
        state.reasoning = reasoning
        state.terminated = True
        state.status = "processed"
        state.log_step("merge_gate", merged=True, merge_target=state.merge_target, similarity_score=state.score)
        return True

    def _run_judge(self, state: CouncilGraphState) -> None:
        review = self.council.judge.review(
            state.hypothesis,
            state.skeptic_review,
            state.archivist_review,
            evidence_review=state.evidence_review,
            quant_review=state.quant_review,
        )
        state.judge_review = review
        self.council.memory.save_review(state.hypothesis["id"], "judge", review)
        state.log_step("run_judge", verdict=review.get("verdict"), engine=review.get("engine"))

    def _apply_evidence_policy(self, state: CouncilGraphState) -> None:
        judge_scores = (state.judge_review or {}).get("scores", {})
        final_score, breakdown = ScoringEngine.compute(judge_scores)
        archivist_verdict = (state.archivist_review or {}).get("verdict", "unique")
        merge_target = (state.judge_review or {}).get("merge_target")
        decision = DecisionEngine.decide(final_score, archivist_verdict, merge_target)
        governed_decision, adjustments = self.council._apply_evidence_policy(decision, state.evidence_review)

        state.score = final_score
        state.breakdown = breakdown
        state.merge_target = merge_target
        state.decision = governed_decision
        state.decision_adjustments = adjustments
        state.reasoning = (state.judge_review or {}).get("reasoning", "No reasoning provided")
        if adjustments:
            state.reasoning += "\n\nGovernance evidence gate: " + " ".join(adjustments)
        state.log_step(
            "apply_evidence_policy",
            raw_decision=decision,
            governed_decision=governed_decision,
            score=final_score,
            adjustments=adjustments,
        )

    def _run_reflection_if_needed(self, state: CouncilGraphState) -> None:
        reflection = self.council._maybe_run_reflection(
            state.hypothesis,
            state.skeptic_review,
            state.archivist_review,
            state.judge_review,
            state.decision,
            state.score or 0.0,
            state.breakdown,
        )
        state.reflection_review = reflection
        state.log_step("run_reflection_if_needed", triggered=bool(reflection))

    def _persist_decision(self, state: CouncilGraphState) -> None:
        self.council.memory.save_decision(
            state.hypothesis["id"],
            state.decision,
            state.score or 0.0,
            state.breakdown,
            state.reasoning or "No reasoning provided",
        )
        state.status = "processed"
        state.log_step("persist_decision", decision=state.decision, score=state.score)

    def _sync_evidence_requests(self, state: CouncilGraphState) -> None:
        state.evidence_requests = self.council._sync_evidence_request_workflow(
            state.hypothesis,
            state.decision or "held",
            state.evidence_review,
            state.quant_review,
            state.reflection_review,
        )
        state.log_step("sync_evidence_requests", count=len(state.evidence_requests))

    def _emit_audit_summary(self, state: CouncilGraphState) -> None:
        self.council.agent_log.add(
            "council_graph_preview",
            f"graph preview: '{state.hypothesis['title']}' -> {state.decision} ({len(state.audit_trail)} steps)",
        )
        state.log_step("emit_audit_summary", decision=state.decision, steps=len(state.audit_trail))


class HeldReReviewGraphRunner:
    """Bounded re-review runner for hypotheses currently in held state."""

    def __init__(self, council: Any):
        self.council = council
        self.review_runner = CouncilGraphRunner(council)

    def run(self, *, limit: int = 3) -> dict[str, Any]:
        state = HeldReReviewState(limit=max(1, int(limit)))
        held = self.council.memory.get_all_decisions(status_filter="held") or []
        state.candidates = held[: state.limit]
        state.log_step("load_held_candidates", count=len(state.candidates))

        for item in state.candidates:
            hypothesis_id = item.get("hypothesis_id")
            material_evidence = self.council._material_evidence_for_rereview(hypothesis_id, item.get("timestamp"))
            if not material_evidence:
                self.council.agent_log.add("council_rereview_skipped", f"Held hypothesis '{hypothesis_id}' skipped: no new Tier A/B evidence")
                state.skipped.append({
                    "hypothesis_id": hypothesis_id,
                    "reason": "no new Tier A/B evidence",
                    "material_evidence": [],
                })
                state.log_step("skip_without_material_evidence", hypothesis_id=hypothesis_id)
                continue

            row = self.council._load_existing_hypothesis(hypothesis_id)
            if not row:
                state.skipped.append({
                    "hypothesis_id": hypothesis_id,
                    "reason": "hypothesis not found",
                    "material_evidence": material_evidence,
                })
                state.log_step("skip_missing_hypothesis", hypothesis_id=hypothesis_id)
                continue

            payload = self.council._build_existing_hypothesis_payload(row)
            evidence_items = list(payload.get("evidence", []))
            evidence_items.extend([
                f"[{entry['tier'].upper()} {entry['relation']}] Memory #{entry['memory_id']}: {entry['summary']}"
                for entry in material_evidence
            ])
            payload.update({
                "claim": row["description"] or payload.get("claim", ""),
                "body": row["description"] or payload.get("body", ""),
                "predictions": payload.get("predictions", []),
                "evidence": evidence_items,
                "timestamp": datetime.now().isoformat(),
            })
            normalized = self.council.normalizer.normalize(payload)
            self.council.agent_log.add("council_rereview", f"Re-reviewing held hypothesis '{normalized['title']}' via graph runner")
            result = self.review_runner.run(normalized, persist_proposed=False, auto_reject=False)
            result["material_evidence"] = material_evidence
            result["legacy"] = True
            state.processed.append(result)
            state.log_step("invoke_council_graph", hypothesis_id=hypothesis_id, decision=result.get("decision"))

        state.log_step("record_rereview_outcome", processed_count=len(state.processed), skipped_count=len(state.skipped))
        return state.to_dict()


class EvidenceRequestClosureGraphRunner:
    """Read-only bounded runner for evidence-request readiness and closure inspection."""

    def __init__(self, council: Any):
        self.council = council

    def run(self, *, hypothesis_id: str | None = None, limit: int = 100) -> dict[str, Any]:
        state = EvidenceRequestClosureState(hypothesis_id=hypothesis_id, limit=max(1, int(limit)))
        requests = self.council.memory.get_evidence_requests(status="pending", hypothesis_id=hypothesis_id, limit=state.limit)
        state.requests = requests
        state.log_step("load_pending_requests", count=len(requests), hypothesis_id=hypothesis_id)

        for request in requests:
            latest_decision = self.council.memory.get_decision_for_hypothesis(request["hypothesis_id"])
            since_timestamp = latest_decision["timestamp"] if latest_decision else None
            material_evidence = self.council._material_evidence_for_rereview(request["hypothesis_id"], since_timestamp)
            ready = bool(material_evidence) and request.get("hypothesis_status") == "held"
            evaluated = dict(request)
            evaluated["material_evidence"] = material_evidence
            evaluated["ready_for_rereview"] = ready
            evaluated["recommended_action"] = "queue_rereview" if ready else "await_evidence"
            state.evaluated.append(evaluated)
            state.log_step(
                "evaluate_request",
                request_id=request.get("id"),
                hypothesis_id=request.get("hypothesis_id"),
                ready_for_rereview=ready,
            )

        state.log_step("summarize_requests", evaluated_count=len(state.evaluated))
        return state.to_dict()
