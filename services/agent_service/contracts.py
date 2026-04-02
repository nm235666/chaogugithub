from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DecisionConfidence:
    score: int | None
    label: str
    summary: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReviewBlock:
    summary: str
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RoleOutput:
    role: str
    content: str
    matched: bool = True
    logic_view: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AgentAnalysisResult:
    analysis_markdown: str
    used_model: str
    requested_model: str
    attempts: list[dict[str, Any]]
    used_context_dims: list[str]
    decision_confidence: DecisionConfidence
    risk_review: ReviewBlock
    portfolio_view: ReviewBlock
    logic_view: dict[str, Any] = field(default_factory=dict)
    role_outputs: list[RoleOutput] = field(default_factory=list)
    legacy_payload: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload = dict(self.legacy_payload)
        payload.update(
            {
                "analysis_markdown": self.analysis_markdown,
                "used_model": self.used_model,
                "requested_model": self.requested_model,
                "attempts": self.attempts,
                "used_context_dims": self.used_context_dims,
                "decision_confidence": self.decision_confidence.to_dict(),
                "risk_review": self.risk_review.to_dict(),
                "portfolio_view": self.portfolio_view.to_dict(),
                "logic_view": self.logic_view,
                "role_outputs": [item.to_dict() for item in self.role_outputs],
            }
        )
        return payload
