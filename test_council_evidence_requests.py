from hypothesis_council import HypothesisCouncil, HypothesisNormalizer


class StubAgent:
    def __init__(self, payload):
        self.payload = payload

    def review(self, *args, **kwargs):
        return dict(self.payload)


class RecordingReflectionAgent:
    def __init__(self, evidence_requests=None):
        self.calls = 0
        self._evidence_requests = evidence_requests or []

    def review(self, hyp, skeptic_review, archivist_review, judge_review, decision, final_score, breakdown):
        self.calls += 1
        return {
            "agent": "reflection",
            "verdict": "hold_for_evidence",
            "reasoning": "Reflection requests explicit follow-up evidence.",
            "objections": [],
            "score_contributions": {
                "readiness": round(final_score, 3),
                "evidence_support": round(breakdown.get("evidence_support", 0.5), 3),
                "testability": round(breakdown.get("testability", 0.5), 3),
            },
            "recommended_decision": decision,
            "rerun_worthy": False,
            "blockers": [],
            "concrete_revisions": [],
            "evidence_requests": list(self._evidence_requests),
        }


class FakeMemory:
    def __init__(self, material_evidence=None):
        self.reviews = []
        self.decisions = []
        self.added = []
        self.synced_requests = []
        self.dismissed = []
        self.material_evidence = material_evidence or []

    def add_auto_hypothesis(self, hyp):
        self.added.append(hyp["id"])

    def save_review(self, hypothesis_id, agent_name, review):
        self.reviews.append((hypothesis_id, agent_name, review))

    def save_decision(self, hypothesis_id, decision, score, breakdown, reasoning, merged_with=None):
        self.decisions.append((hypothesis_id, decision, score, breakdown, reasoning, merged_with))

    def get_hypothesis_titles_and_bodies(self):
        return []

    def sync_evidence_requests_for_hypothesis(self, hypothesis_id, requests, triggering_decision="held"):
        self.synced_requests.append((hypothesis_id, requests, triggering_decision))
        return [{"hypothesis_id": hypothesis_id, **request, "status": "pending"} for request in requests]

    def dismiss_pending_evidence_requests(self, hypothesis_id, reason=""):
        self.dismissed.append((hypothesis_id, reason))
        return 1

    def get_material_evidence_since(self, hypothesis_id, since_timestamp):
        return list(self.material_evidence)


class FakeAgentLog:
    def __init__(self):
        self.events = []

    def add(self, event_type, message):
        self.events.append((event_type, message))


def build_council(material_evidence=None, reflection_requests=None, judge_payload=None):
    council = HypothesisCouncil.__new__(HypothesisCouncil)
    council.memory = FakeMemory(material_evidence=material_evidence)
    council.agent_log = FakeAgentLog()
    council.normalizer = HypothesisNormalizer()
    council.skeptic = StubAgent({
        "agent": "skeptic",
        "verdict": "plausible",
        "reasoning": "No fatal contradiction.",
        "objections": [],
        "score_contributions": {"coherence": 0.55, "testability": 0.48},
    })
    council.archivist = StubAgent({
        "agent": "archivist",
        "verdict": "unique",
        "reasoning": "No duplicate found.",
        "objections": [],
        "score_contributions": {"novelty": 0.68, "evidence_support": 0.35},
        "similarity_score": 0.12,
        "most_similar": "N/A",
    })
    from hypothesis_council import EvidenceReviewerAgent, QuantReviewerAgent

    council.evidence_reviewer = EvidenceReviewerAgent()
    council.quant_reviewer = QuantReviewerAgent()
    council.judge = StubAgent(judge_payload or {
        "agent": "judge",
        "verdict": "held",
        "reasoning": "Evidence remains too weak.",
        "scores": {
            "coherence": 0.5,
            "evidence_support": 0.28,
            "testability": 0.45,
            "novelty": 0.65,
            "redundancy_penalty": 0.1,
        },
        "final_score": 0.43,
        "merge_target": None,
    })
    council.reflection = RecordingReflectionAgent(evidence_requests=reflection_requests or ["Cite a directly relevant observing record."])
    return council


def held_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Follow-up evidence test",
        "body": "This hypothesis may explain the source if stronger evidence arrives.",
        "predictions": ["A follow-up observation should reproduce the reported pattern."],
        "evidence": ["This could match a speculative anomaly if true."],
        "confidence": 0.46,
    }


def test_held_decision_generates_structured_evidence_requests():
    council = build_council()
    result = council.review(held_hypothesis("TEST-EVIDENCE-REQUESTS"))

    assert result["decision"] == "held", result
    assert result["evidence_requests"], result
    assert council.memory.synced_requests, council.memory.synced_requests
    request_texts = [request["request_text"] for request in council.memory.synced_requests[0][1]]
    assert any("Tier A or Tier B evidence item" in text for text in request_texts), request_texts
    assert any("observing record" in text for text in request_texts), request_texts


def test_non_held_decision_dismisses_pending_evidence_requests():
    council = build_council(judge_payload={
        "agent": "judge",
        "verdict": "accepted",
        "reasoning": "Direct evidence is strong.",
        "scores": {
            "coherence": 0.82,
            "evidence_support": 0.8,
            "testability": 0.78,
            "novelty": 0.6,
            "redundancy_penalty": 0.08,
        },
        "final_score": 0.77,
        "merge_target": None,
    })
    result = council.review({
        "id": "TEST-EVIDENCE-DISMISS",
        "title": "Accepted hypothesis",
        "body": "Observed light curve and dataset support this claim.",
        "predictions": ["The event should repeat at the measured cadence."],
        "evidence": ["Observed dataset detected a recurring 4.2 day signal."],
        "confidence": 0.79,
    })

    assert result["decision"] == "accepted", result
    assert council.memory.dismissed, council.memory.dismissed


def test_material_evidence_gate_reports_rereview_eligibility():
    council = build_council(material_evidence=[{
        "memory_id": 88,
        "tier": "tier_a",
        "relation": "support",
        "summary": "Observed spectrum confirms the predicted line.",
        "timestamp": "2026-04-04T12:00:00",
    }])
    evidence = council._material_evidence_for_rereview("TEST-HYP", "2026-04-04T10:00:00")

    assert len(evidence) == 1, evidence
    assert evidence[0]["tier"] == "tier_a", evidence


def main():
    test_held_decision_generates_structured_evidence_requests()
    test_non_held_decision_dismisses_pending_evidence_requests()
    test_material_evidence_gate_reports_rereview_eligibility()
    print("council evidence request tests passed")


if __name__ == "__main__":
    main()