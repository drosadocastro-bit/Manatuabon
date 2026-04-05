from council_graph import CouncilGraphRunner, EvidenceRequestClosureGraphRunner, HeldReReviewGraphRunner
from hypothesis_council import EvidenceReviewerAgent, HypothesisCouncil, HypothesisNormalizer, QuantReviewerAgent


class StubAgent:
    def __init__(self, payload):
        self.payload = payload

    def review(self, *args, **kwargs):
        return dict(self.payload)


class RecordingReflectionAgent:
    def review(self, hyp, skeptic_review, archivist_review, judge_review, decision, final_score, breakdown):
        return {
            "agent": "reflection",
            "verdict": "hold_for_evidence" if decision == "held" else "revise_and_retry",
            "reasoning": "Reflection preserved the workflow outcome.",
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
            "evidence_requests": ["Attach one targeted follow-up observation." if decision == "held" else ""],
        }


class FakeMemory:
    def __init__(self):
        self.reviews = []
        self.decisions = []
        self.added = []
        self.dismissed = []
        self.synced = []
        self.held_rows = []
        self.material_map = {}
        self.request_rows = []
        self.hypothesis_rows = {}

    def add_auto_hypothesis(self, hyp):
        self.added.append(hyp["id"])

    def save_review(self, hypothesis_id, agent_name, review):
        self.reviews.append((hypothesis_id, agent_name, review))

    def save_decision(self, hypothesis_id, decision, score, breakdown, reasoning, merged_with=None):
        self.decisions.append((hypothesis_id, decision, score, breakdown, reasoning, merged_with))

    def get_hypothesis_titles_and_bodies(self):
        return [
            {"id": "EX-1", "title": "Existing Quiet Universe Hypothesis", "body": "Quiet systems may reflect environmental dormancy."},
            {"id": "EX-2", "title": "Duplicate Quiet Universe Hypothesis", "body": "Same claim with minor wording changes."},
        ]

    def dismiss_pending_evidence_requests(self, hypothesis_id, reason):
        self.dismissed.append((hypothesis_id, reason))

    def sync_evidence_requests_for_hypothesis(self, hypothesis_id, payloads, triggering_decision):
        synced = []
        for index, payload in enumerate(payloads, start=1):
            item = dict(payload)
            item.update({
                "id": index,
                "hypothesis_id": hypothesis_id,
                "status": "pending",
                "triggering_decision": triggering_decision,
            })
            synced.append(item)
        self.synced.append((hypothesis_id, synced))
        return synced

    def get_all_decisions(self, status_filter=None):
        if status_filter == "held":
            return list(self.held_rows)
        return list(self.held_rows)

    def get_material_evidence_since(self, hypothesis_id, since_timestamp, limit=20):
        return list(self.material_map.get(hypothesis_id, []))[:limit]

    def get_evidence_requests(self, status="pending", hypothesis_id=None, limit=100):
        rows = list(self.request_rows)
        if hypothesis_id:
            rows = [row for row in rows if row["hypothesis_id"] == hypothesis_id]
        if status and status != "all":
            rows = [row for row in rows if row["status"] == status]
        return rows[:limit]

    def get_decision_for_hypothesis(self, hypothesis_id):
        for item in reversed(self.decisions):
            if item[0] == hypothesis_id:
                return {
                    "decision": item[1],
                    "final_score": item[2],
                    "merged_with": item[5],
                    "timestamp": "2026-04-04T13:00:00",
                }
        for item in self.held_rows:
            if item["hypothesis_id"] == hypothesis_id:
                return {"decision": "held", "final_score": 0.4, "merged_with": None, "timestamp": item.get("timestamp")}
        return None


class FakeAgentLog:
    def __init__(self):
        self.events = []

    def add(self, event_type, message):
        self.events.append((event_type, message))


def build_council(judge_payload, archivist_payload=None):
    council = HypothesisCouncil.__new__(HypothesisCouncil)
    council.memory = FakeMemory()
    council.agent_log = FakeAgentLog()
    council.normalizer = HypothesisNormalizer()
    council.council_graph_mode = "off"
    council._graph_runner_cache = {}
    council.skeptic = StubAgent({
        "agent": "skeptic",
        "verdict": "plausible",
        "reasoning": "No immediate contradiction.",
        "objections": [],
        "score_contributions": {"coherence": 0.7, "testability": 0.65},
    })
    council.archivist = StubAgent(archivist_payload or {
        "agent": "archivist",
        "verdict": "unique",
        "reasoning": "No duplicate found.",
        "objections": [],
        "score_contributions": {"novelty": 0.72, "evidence_support": 0.6},
        "similarity_score": 0.18,
        "most_similar": "N/A",
    })
    council.evidence_reviewer = EvidenceReviewerAgent()
    council.quant_reviewer = QuantReviewerAgent()
    council.judge = StubAgent(judge_payload)
    council.reflection = RecordingReflectionAgent()
    council._load_existing_hypothesis = lambda hypothesis_id: council.memory.hypothesis_rows.get(hypothesis_id)
    return council


def high_score_judge(verdict="accepted"):
    return {
        "agent": "judge",
        "verdict": verdict,
        "reasoning": "Judge considered the hypothesis operationally strong.",
        "scores": {
            "coherence": 0.84,
            "evidence_support": 0.82,
            "testability": 0.78,
            "novelty": 0.68,
            "redundancy_penalty": 0.1,
        },
        "final_score": 0.782,
        "merge_target": None,
    }


def partial_overlap_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Environmental quiescence variant",
        "body": "Quiet accretion may arise from environmental dormancy patterns rather than intrinsic engine failure.",
        "claim": "Quiet accretion may arise from environmental dormancy patterns rather than intrinsic engine failure.",
        "predictions": ["Nearby systems in comparable environments should show reduced activity windows."],
        "evidence": ["Preliminary archival notes suggest a comparable dormancy pattern in a related target."],
        "confidence": 0.63,
    }


def speculative_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Speculative field coupling",
        "body": "A hidden field could explain the transient if the source may couple to an unknown environment.",
        "claim": "A hidden field could explain the transient if the source may couple to an unknown environment.",
        "predictions": ["Future monitoring might reveal a similar event."],
        "evidence": ["This could be explained by analogy with unrelated anomalies."],
        "confidence": 0.71,
    }


def test_council_graph_keeps_partial_overlap_separate():
    council = build_council(
        high_score_judge(verdict="needs_revision"),
        archivist_payload={
            "agent": "archivist",
            "verdict": "partial_overlap",
            "reasoning": "Related idea, but not the same hypothesis.",
            "objections": ["Shares environmental framing with an existing hypothesis."],
            "score_contributions": {"novelty": 0.58, "evidence_support": 0.42},
            "similarity_score": 0.83,
            "most_similar": "Existing Quiet Universe Hypothesis",
        },
    )
    runner = CouncilGraphRunner(council)

    result = runner.run(partial_overlap_hypothesis("GRAPH-NO-MERGE"))

    assert result["decision"] != "merged", result
    assert council.memory.decisions[-1][1] != "merged", council.memory.decisions
    assert any(step["step"] == "merge_gate" and not step.get("merged") for step in result["audit_trail"]), result["audit_trail"]


def test_council_graph_applies_evidence_gate_and_syncs_requests():
    council = build_council(high_score_judge())
    runner = CouncilGraphRunner(council)

    result = runner.run(speculative_hypothesis("GRAPH-HELD"))

    assert result["decision"] == "held", result
    assert result["decision_adjustments"], result
    assert result["evidence_requests"], result
    assert council.memory.synced, council.memory.synced


def test_primary_graph_mode_routes_live_review_pipeline():
    council = build_council(high_score_judge(verdict="needs_revision"))
    council.council_graph_mode = "primary"

    result = council._run_review_pipeline(partial_overlap_hypothesis("GRAPH-PRIMARY"), persist_proposed=False, auto_reject=False)

    assert result["decision"] != "merged", result
    assert result.get("graph_audit"), result


def test_shadow_graph_mode_compares_without_extra_persistence():
    council = build_council(high_score_judge())
    council.council_graph_mode = "shadow"

    result = council._run_review_pipeline(speculative_hypothesis("GRAPH-SHADOW"), persist_proposed=False, auto_reject=False)

    assert result["decision"] == "held", result
    assert len(council.memory.decisions) == 1, council.memory.decisions
    assert any(event[0] == "council_graph_parity_match" for event in council.agent_log.events), council.agent_log.events


def test_held_rereview_graph_processes_only_material_evidence():
    council = build_council(high_score_judge(verdict="needs_revision"))
    council.memory.held_rows = [
        {"hypothesis_id": "HELD-1", "timestamp": "2026-04-04T10:00:00"},
        {"hypothesis_id": "HELD-2", "timestamp": "2026-04-04T10:05:00"},
    ]
    council.memory.material_map = {
        "HELD-1": [{
            "memory_id": 11,
            "timestamp": "2026-04-04T11:00:00",
            "summary": "Observed light curve shows a measured 4.2 day periodic dip.",
            "tier": "tier_a",
            "relation": "support",
        }],
        "HELD-2": [],
    }
    council.memory.hypothesis_rows = {
        "HELD-1": {
            "id": "HELD-1",
            "title": "Held candidate",
            "description": "A re-reviewable held hypothesis.",
            "evidence": None,
            "source": "Agent Auto",
            "confidence": 0.5,
            "updated_at": "2026-04-04T10:00:00",
            "created_at": "2026-04-04T09:00:00",
            "date": "2026-04-04T09:00:00",
        }
    }

    runner = HeldReReviewGraphRunner(council)
    result = runner.run(limit=3)

    assert result["processed"], result
    assert result["processed"][0]["hypothesis"]["id"] == "HELD-1", result
    assert result["skipped"][0]["hypothesis_id"] == "HELD-2", result


def test_evidence_request_closure_graph_marks_ready_for_rereview():
    council = build_council(high_score_judge())
    council.memory.request_rows = [{
        "id": 1,
        "hypothesis_id": "HELD-1",
        "hypothesis_title": "Held candidate",
        "hypothesis_status": "held",
        "request_text": "Attach at least one concrete evidence item linked to this hypothesis before re-review.",
        "priority": "high",
        "source_agent": "evidence_reviewer",
        "source_context": {"flag": "no_evidence"},
        "status": "pending",
        "triggering_decision": "held",
        "created_at": "2026-04-04T10:00:00",
        "updated_at": "2026-04-04T10:00:00",
        "resolved_at": None,
        "resolution_note": None,
        "satisfied_by_memory_ids": [],
        "ready_for_rereview": False,
        "material_evidence": [],
    }]
    council.memory.material_map = {
        "HELD-1": [{
            "memory_id": 21,
            "timestamp": "2026-04-04T11:10:00",
            "summary": "JWST photometry detected the same cadence in the follow-up dataset.",
            "tier": "tier_a",
            "relation": "support",
        }]
    }
    council.memory.held_rows = [{"hypothesis_id": "HELD-1", "timestamp": "2026-04-04T10:00:00"}]

    runner = EvidenceRequestClosureGraphRunner(council)
    result = runner.run(hypothesis_id="HELD-1", limit=10)

    assert result["evaluated"][0]["ready_for_rereview"] is True, result
    assert result["evaluated"][0]["recommended_action"] == "queue_rereview", result


def main():
    test_council_graph_keeps_partial_overlap_separate()
    test_council_graph_applies_evidence_gate_and_syncs_requests()
    test_primary_graph_mode_routes_live_review_pipeline()
    test_shadow_graph_mode_compares_without_extra_persistence()
    test_held_rereview_graph_processes_only_material_evidence()
    test_evidence_request_closure_graph_marks_ready_for_rereview()
    print("council graph tests passed")


if __name__ == "__main__":
    main()