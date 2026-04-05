from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class CouncilGraphState:
    """Serializable state for one bounded council review run."""

    hypothesis: dict[str, Any]
    persist_proposed: bool = True
    auto_reject: bool = True
    existing_hypotheses: list[dict[str, Any]] = field(default_factory=list)
    skeptic_review: dict[str, Any] | None = None
    archivist_review: dict[str, Any] | None = None
    evidence_review: dict[str, Any] | None = None
    quant_review: dict[str, Any] | None = None
    judge_review: dict[str, Any] | None = None
    reflection_review: dict[str, Any] | None = None
    decision: str | None = None
    score: float | None = None
    breakdown: dict[str, Any] = field(default_factory=dict)
    decision_adjustments: list[str] = field(default_factory=list)
    evidence_requests: list[dict[str, Any]] = field(default_factory=list)
    merge_target: str | None = None
    status: str | None = None
    legacy: bool = False
    terminated: bool = False
    reasoning: str | None = None
    errors: list[str] = field(default_factory=list)
    audit_trail: list[dict[str, Any]] = field(default_factory=list)

    def log_step(self, step: str, **details: Any) -> None:
        entry = {"step": step}
        if details:
            entry.update(details)
        self.audit_trail.append(entry)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class HeldReReviewState:
    """Serializable state for bounded held re-review runs."""

    limit: int = 3
    candidates: list[dict[str, Any]] = field(default_factory=list)
    processed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    audit_trail: list[dict[str, Any]] = field(default_factory=list)

    def log_step(self, step: str, **details: Any) -> None:
        entry = {"step": step}
        if details:
            entry.update(details)
        self.audit_trail.append(entry)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceRequestClosureState:
    """Serializable state for evidence-request readiness and closure inspection."""

    hypothesis_id: str | None = None
    limit: int = 100
    requests: list[dict[str, Any]] = field(default_factory=list)
    evaluated: list[dict[str, Any]] = field(default_factory=list)
    audit_trail: list[dict[str, Any]] = field(default_factory=list)

    def log_step(self, step: str, **details: Any) -> None:
        entry = {"step": step}
        if details:
            entry.update(details)
        self.audit_trail.append(entry)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
