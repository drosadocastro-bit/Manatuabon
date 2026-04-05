from hypothesis_council import HypothesisCouncil, HypothesisNormalizer, QuantReviewerAgent


class StubAgent:
    def __init__(self, payload):
        self.payload = payload

    def review(self, *args, **kwargs):
        return dict(self.payload)


class RecordingReflectionAgent:
    def __init__(self):
        self.calls = 0

    def review(self, hyp, skeptic_review, archivist_review, judge_review, decision, final_score, breakdown):
        self.calls += 1
        return {
            "agent": "reflection",
            "verdict": "hold_for_evidence",
            "reasoning": "Reflection preserved the current outcome.",
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
            "evidence_requests": [],
        }


class FakeMemory:
    def __init__(self):
        self.reviews = []
        self.decisions = []
        self.added = []

    def add_auto_hypothesis(self, hyp):
        self.added.append(hyp["id"])

    def save_review(self, hypothesis_id, agent_name, review):
        self.reviews.append((hypothesis_id, agent_name, review))

    def save_decision(self, hypothesis_id, decision, score, breakdown, reasoning, merged_with=None):
        self.decisions.append((hypothesis_id, decision, score, breakdown, reasoning, merged_with))

    def get_hypothesis_titles_and_bodies(self):
        return []


class FakeAgentLog:
    def __init__(self):
        self.events = []

    def add(self, event_type, message):
        self.events.append((event_type, message))


def build_council():
    council = HypothesisCouncil.__new__(HypothesisCouncil)
    council.memory = FakeMemory()
    council.agent_log = FakeAgentLog()
    council.normalizer = HypothesisNormalizer()
    council.skeptic = StubAgent({
        "agent": "skeptic",
        "verdict": "plausible",
        "reasoning": "No fatal physical contradiction yet.",
        "objections": [],
        "score_contributions": {"coherence": 0.62, "testability": 0.61},
    })
    council.archivist = StubAgent({
        "agent": "archivist",
        "verdict": "unique",
        "reasoning": "Distinct from prior entries.",
        "objections": [],
        "score_contributions": {"novelty": 0.68, "evidence_support": 0.5},
        "similarity_score": 0.1,
        "most_similar": "N/A",
    })
    council.quant_reviewer = QuantReviewerAgent()
    council.judge = StubAgent({
        "agent": "judge",
        "verdict": "needs_revision",
        "reasoning": "Interesting, but still underconstrained.",
        "scores": {
            "coherence": 0.62,
            "evidence_support": 0.5,
            "testability": 0.6,
            "novelty": 0.68,
            "redundancy_penalty": 0.12,
        },
        "final_score": 0.596,
        "merge_target": None,
    })
    council.reflection = RecordingReflectionAgent()
    return council


def parameterized_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Orbital period scaling test",
        "body": "A compact companion with a 4.2 day orbit should imply a host mass near 1.3 solar masses if the transit depth remains below 2%.",
        "predictions": ["Transit timing should repeat every 4.2 days within 1% tolerance."],
        "evidence": ["Light curve shows a candidate 4.2 day periodic dip at 1.8% depth."],
        "confidence": 0.58,
        "context_domains": ["exoplanets"],
    }


def qualitative_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Qualitative morphology hypothesis",
        "body": "The source may be unusual because its environment looks disturbed and asymmetrical.",
        "predictions": ["Further imaging should clarify whether the structure is real."],
        "evidence": ["One descriptive observing note mentions asymmetry."],
        "confidence": 0.51,
        "context_domains": ["testing"],
    }


def test_quant_reviewer_triggers_for_parameterized_hypothesis():
    council = build_council()
    result = council.review(parameterized_hypothesis("TEST-QUANT-YES"))

    quant_reviews = [review for _, agent_name, review in council.memory.reviews if agent_name == "quant_reviewer"]
    assert len(quant_reviews) == 1, council.memory.reviews
    assert result["quant_review"] is not None, result
    assert result["quant_review"]["advisory_only"] is True, result["quant_review"]
    assert "orbit" in result["quant_review"]["extracted_quantities"], result["quant_review"]
    assert result["decision"] == "needs_revision", result


def test_quant_reviewer_skips_purely_qualitative_hypothesis():
    council = build_council()
    result = council.review(qualitative_hypothesis("TEST-QUANT-NO"))

    assert result["quant_review"] is None, result
    assert not any(agent_name == "quant_reviewer" for _, agent_name, _ in council.memory.reviews), council.memory.reviews


def main():
    test_quant_reviewer_triggers_for_parameterized_hypothesis()
    test_quant_reviewer_skips_purely_qualitative_hypothesis()
    print("council quant reviewer tests passed")


if __name__ == "__main__":
    main()