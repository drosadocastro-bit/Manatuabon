from hypothesis_council import HypothesisCouncil, HypothesisNormalizer


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
            "verdict": "revise_and_retry" if decision == "needs_revision" else "hold_for_evidence",
            "reasoning": "Reflection captured concrete follow-up work.",
            "objections": ["Need sharper supporting evidence."],
            "score_contributions": {
                "readiness": round(final_score, 3),
                "evidence_support": round(breakdown.get("evidence_support", 0.5), 3),
                "testability": round(breakdown.get("testability", 0.5), 3),
            },
            "recommended_decision": decision,
            "rerun_worthy": decision == "needs_revision",
            "blockers": ["Evidence remains too weak."],
            "concrete_revisions": ["Add one stronger observable prediction."],
            "evidence_requests": ["Cite at least one supporting memory or observation."],
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


def build_council(judge_payload):
    council = HypothesisCouncil.__new__(HypothesisCouncil)
    council.memory = FakeMemory()
    council.agent_log = FakeAgentLog()
    council.normalizer = HypothesisNormalizer()
    council.skeptic = StubAgent({
        "agent": "skeptic",
        "verdict": "plausible",
        "reasoning": "Reasonable but incomplete.",
        "objections": ["Needs stronger constraints."],
        "score_contributions": {"coherence": 0.58, "testability": 0.48},
    })
    council.archivist = StubAgent({
        "agent": "archivist",
        "verdict": "unique",
        "reasoning": "Not a duplicate.",
        "objections": [],
        "score_contributions": {"novelty": 0.7, "evidence_support": 0.42},
        "similarity_score": 0.22,
        "most_similar": "N/A",
    })
    council.judge = StubAgent(judge_payload)
    council.reflection = RecordingReflectionAgent()
    return council


def sample_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Reflection Trigger Test",
        "body": "A test hypothesis that should remain unresolved until evidence improves.",
        "predictions": ["Observed variability should correlate with the proposed mechanism."],
        "evidence": ["Sparse archival notes only."],
        "confidence": 0.55,
        "context_domains": ["testing"],
    }


def test_reflection_triggers_for_borderline_revision():
    council = build_council({
        "agent": "judge",
        "verdict": "needs_revision",
        "reasoning": "Promising idea, but evidence support remains weak.",
        "scores": {
            "coherence": 0.58,
            "evidence_support": 0.42,
            "testability": 0.52,
            "novelty": 0.7,
            "redundancy_penalty": 0.2,
        },
        "final_score": 0.564,
        "merge_target": None,
    })
    result = council.review(sample_hypothesis("TEST-REFLECT-YES"))

    assert result["decision"] == "needs_revision", result
    assert council.reflection.calls == 1, council.reflection.calls
    assert any(agent_name == "reflection" for _, agent_name, _ in council.memory.reviews), council.memory.reviews
    assert result["reflection"]["recommended_decision"] == "needs_revision", result["reflection"]


def test_reflection_skips_accepted_hypotheses():
    council = build_council({
        "agent": "judge",
        "verdict": "accepted",
        "reasoning": "Well supported and testable.",
        "scores": {
            "coherence": 0.86,
            "evidence_support": 0.8,
            "testability": 0.82,
            "novelty": 0.72,
            "redundancy_penalty": 0.1,
        },
        "final_score": 0.8,
        "merge_target": None,
    })
    result = council.review(sample_hypothesis("TEST-REFLECT-NO"))

    assert result["decision"] == "accepted", result
    assert council.reflection.calls == 0, council.reflection.calls
    assert not any(agent_name == "reflection" for _, agent_name, _ in council.memory.reviews), council.memory.reviews


def main():
    test_reflection_triggers_for_borderline_revision()
    test_reflection_skips_accepted_hypotheses()
    print("council reflection tests passed")


if __name__ == "__main__":
    main()