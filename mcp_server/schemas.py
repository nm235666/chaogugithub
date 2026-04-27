from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EmptyArgs(BaseModel):
    pass


class TableCountsArgs(BaseModel):
    tables: list[str] = Field(default_factory=list)


class ReadonlyQueryArgs(BaseModel):
    sql: str
    params: list[Any] = Field(default_factory=list)
    limit: int = 100


class JobListArgs(BaseModel):
    job_key: str = ""
    status: str = ""
    unresolved_only: bool = True
    limit: int = 50


class WriteRequest(BaseModel):
    actor: str = ""
    reason: str = ""
    idempotency_key: str = ""
    dry_run: bool = True
    confirm: bool = False


class JobTriggerArgs(WriteRequest):
    job_key: str


class FunnelScoreAlignArgs(WriteRequest):
    score_date: str = ""
    max_candidates: int = 10000


class FunnelReviewRefreshArgs(WriteRequest):
    horizon_days: int = 5
    limit: int = 200


class DecisionSnapshotArgs(WriteRequest):
    job_key: str = "decision_daily_snapshot"


class ReconcilePositionsArgs(WriteRequest):
    limit: int = 500


class PortfolioOrderReviewsArgs(WriteRequest):
    horizon_days: int = 5
    limit: int = 200
    order_status: str = "executed"


class AgentStartRunArgs(BaseModel):
    agent_key: str = "funnel_progress_agent"
    mode: str = "auto"
    trigger_source: str = "mcp"
    actor: str = "mcp"
    goal: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    schedule_key: str = ""
    dedupe: bool = True
    correlation_id: str = ""
    parent_run_id: str = ""


class AgentGetRunArgs(BaseModel):
    run_id: str


class AgentListRunsArgs(BaseModel):
    agent_key: str = ""
    status: str = ""
    limit: int = 50


class AgentCancelRunArgs(BaseModel):
    run_id: str
    actor: str = "mcp"
    reason: str = "mcp cancel"


class AgentApprovalArgs(BaseModel):
    run_id: str
    actor: str = "mcp"
    reason: str = ""
    idempotency_key: str = ""


class MemoryListArgs(BaseModel):
    memory_type: str = ""
    ts_code: str = ""
    scope: str = ""
    source_agent_key: str = ""
    status: str = "active"
    limit: int = 50


class MemorySearchArgs(BaseModel):
    ts_code: str = ""
    scope: str = ""
    memory_type: str = ""
    limit: int = 20


class MemoryRecordArgs(WriteRequest):
    memory_type: str
    source_run_id: str = ""
    source_agent_key: str = ""
    ts_code: str = ""
    scope: str = ""
    summary: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    score: float = 0
    status: str = "active"


class GovernanceQualityArgs(BaseModel):
    agent_key: str = ""
    window_days: int = 7
    refresh: bool = False


class GovernanceRuleListArgs(BaseModel):
    agent_key: str = ""
    tool_name: str = ""
    enabled: str = ""
    limit: int = 100


class GovernanceRuleUpsertArgs(WriteRequest):
    rule_key: str
    agent_key: str = ""
    tool_name: str = ""
    risk_level: str = "low"
    decision: str = "allow"
    enabled: bool = True
    thresholds: dict[str, Any] = Field(default_factory=dict)


class GovernanceEvaluateArgs(BaseModel):
    agent_key: str
    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    run_id: str = ""
    correlation_id: str = ""
    requested_dry_run: bool = True
