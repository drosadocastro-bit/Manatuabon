from hypothesis_council import EvidenceReviewerAgent, HypothesisCouncil, HypothesisNormalizer, QuantReviewerAgent


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
            "verdict": "hold_for_evidence" if decision == "held" else "revise_and_retry",
            "reasoning": "Reflection preserved the outcome for regression testing.",
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


class FakeMemory:
    def __init__(self):
        self.reviews = []
        self.decisions = []
        self.added = []
        self.memories_by_id = {}

    def add_auto_hypothesis(self, hyp):
        self.added.append(hyp["id"])

    def save_review(self, hypothesis_id, agent_name, review):
        self.reviews.append((hypothesis_id, agent_name, review))

    def save_decision(self, hypothesis_id, decision, score, breakdown, reasoning, merged_with=None):
        self.decisions.append((hypothesis_id, decision, score, breakdown, reasoning, merged_with))

    def get_hypothesis_titles_and_bodies(self):
        return []

    def get_memories_by_ids(self, memory_ids):
        return [self.memories_by_id[memory_id] for memory_id in memory_ids if memory_id in self.memories_by_id]


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
        "reasoning": "No immediate contradiction.",
        "objections": [],
        "score_contributions": {"coherence": 0.7, "testability": 0.65},
    })
    council.archivist = StubAgent({
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


def speculative_only_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Speculative field coupling",
        "body": "A hidden field could explain the transient if the source may couple to an unknown environment.",
        "predictions": ["Future monitoring might reveal a similar event."],
        "evidence": ["This could be explained by analogy with unrelated anomalies.", "The event may fit a speculative narrative if true."],
        "confidence": 0.71,
    }


def weak_support_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Tentative archival match",
        "body": "The source may trace a repeatable process based on partial archival alignment.",
        "predictions": ["A follow-up cadence should recover the signal again."],
        "evidence": ["Archival notes are consistent with a candidate recurrence.", "A tentative correlation appears in preliminary summaries."],
        "confidence": 0.74,
    }


def direct_support_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Measured periodic transit",
        "body": "A 4.2 day transit signal implies an orbiting companion with measurable recurrence.",
        "predictions": ["Transit timing should repeat every 4.2 days within 1% tolerance."],
        "evidence": ["Observed light curve shows a measured 4.2 day periodic dip at 1.8% depth.", "JWST photometry detected the same cadence in the follow-up dataset."],
        "confidence": 0.78,
    }


def sourced_memory_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Instrument-backed periodic transit",
        "body": "A detected periodic dip implies an orbiting companion with measurable recurrence.",
        "predictions": ["Transit timing should repeat every 4.2 days within 1% tolerance."],
        "evidence": [],
        "source_memory": 101,
        "confidence": 0.78,
    }


def overlapping_but_distinct_hypothesis(hyp_id):
    return {
        "id": hyp_id,
        "title": "Environmental quiescence variant",
        "body": "Quiet accretion may arise from environmental dormancy patterns rather than intrinsic engine failure.",
        "predictions": ["Nearby systems in comparable environments should show reduced activity windows."],
        "evidence": ["Preliminary archival notes suggest a comparable dormancy pattern in a related target."],
        "confidence": 0.63,
    }


def test_speculative_only_evidence_caps_decision_to_held():
    council = build_council(high_score_judge())
    result = council.review(speculative_only_hypothesis("TEST-EVIDENCE-HELD"))

    assert result["decision"] == "held", result
    assert result["evidence_review"]["decision_cap"] == "held", result["evidence_review"]
    assert "Only Tier C speculative evidence was present." in result["decision_adjustments"], result


def test_weak_evidence_caps_acceptance_to_needs_revision():
    council = build_council(high_score_judge())
    result = council.review(weak_support_hypothesis("TEST-EVIDENCE-REVISION"))

    assert result["decision"] == "needs_revision", result
    assert result["evidence_review"]["decision_cap"] == "needs_revision", result["evidence_review"]
    assert "No Tier A direct support is present yet." in result["decision_adjustments"], result


def test_direct_evidence_allows_acceptance():
    council = build_council(high_score_judge())
    result = council.review(direct_support_hypothesis("TEST-EVIDENCE-ACCEPT"))

    assert result["decision"] == "accepted", result
    assert result["evidence_review"]["decision_cap"] == "accepted", result["evidence_review"]
    assert result["decision_adjustments"] == [], result


def test_source_memory_hydrates_evidence_for_review():
    council = build_council(high_score_judge())
    council.memory.memories_by_id[101] = {
        "id": 101,
        "timestamp": "2026-04-04T16:00:00",
        "summary": "Observed light curve shows a measured 4.2 day periodic dip at 1.8% depth.",
        "entities": ["JWST"],
        "domain_tags": ["exoplanets"],
        "confidence": 0.9,
        "confidence_label": "high",
        "supports_hypothesis": None,
        "challenges_hypothesis": None,
    }

    result = council.review(sourced_memory_hypothesis("TEST-EVIDENCE-SOURCE"))

    assert result["decision"] == "accepted", result
    assert result["evidence_review"]["decision_cap"] == "accepted", result["evidence_review"]
    assert result["decision_adjustments"] == [], result


def test_partial_overlap_high_similarity_does_not_auto_merge():
    council = build_council(high_score_judge(verdict="needs_revision"))
    council.archivist = StubAgent({
        "agent": "archivist",
        "verdict": "partial_overlap",
        "reasoning": "Related idea, but not the same hypothesis.",
        "objections": ["Shares an environmental framing with an existing hypothesis."],
        "score_contributions": {"novelty": 0.58, "evidence_support": 0.42},
        "similarity_score": 0.83,
        "most_similar": "Existing Quiet Universe Hypothesis",
    })

    result = council.review(overlapping_but_distinct_hypothesis("TEST-NO-MERGE-PARTIAL"))

    assert result["decision"] != "merged", result
    assert council.memory.decisions[-1][1] != "merged", council.memory.decisions


def main():
    test_speculative_only_evidence_caps_decision_to_held()
    test_weak_evidence_caps_acceptance_to_needs_revision()
    test_direct_evidence_allows_acceptance()
    test_source_memory_hydrates_evidence_for_review()
    test_partial_overlap_high_similarity_does_not_auto_merge()
    print("council evidence policy tests passed")


if __name__ == "__main__":
    main()