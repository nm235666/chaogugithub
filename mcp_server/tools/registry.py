from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel, ValidationError

from mcp_server import schemas
from . import agents_tools, business, db_tools, governance_tools, jobs_tools, memory_tools, scheduler_tools, system_tools


class MCPToolError(RuntimeError):
    pass


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    schema_model: type[BaseModel]
    handler: Callable[[BaseModel], dict[str, Any]]

    def to_mcp_tool(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.schema_model.model_json_schema(),
        }


TOOLS: dict[str, ToolDefinition] = {
    "system.health_snapshot": ToolDefinition(
        "system.health_snapshot",
        "Return database, key table, job, and business closure health summary.",
        schemas.EmptyArgs,
        system_tools.health_snapshot,
    ),
    "db.table_counts": ToolDefinition(
        "db.table_counts",
        "Return row counts for allowed business tables.",
        schemas.TableCountsArgs,
        db_tools.table_counts,
    ),
    "db.readonly_query": ToolDefinition(
        "db.readonly_query",
        "Run a restricted SELECT query against allowed business tables.",
        schemas.ReadonlyQueryArgs,
        db_tools.readonly_query,
    ),
    "jobs.list_definitions": ToolDefinition(
        "jobs.list_definitions",
        "List registered job definitions.",
        schemas.EmptyArgs,
        jobs_tools.list_definitions,
    ),
    "jobs.list_runs": ToolDefinition(
        "jobs.list_runs",
        "List recent job runs.",
        schemas.JobListArgs,
        jobs_tools.list_runs,
    ),
    "jobs.list_alerts": ToolDefinition(
        "jobs.list_alerts",
        "List recent job alerts.",
        schemas.JobListArgs,
        jobs_tools.list_alerts,
    ),
    "jobs.trigger": ToolDefinition(
        "jobs.trigger",
        "Dry-run or trigger an allowlisted operational job.",
        schemas.JobTriggerArgs,
        jobs_tools.trigger,
    ),
    "scheduler.check_cron_sync": ToolDefinition(
        "scheduler.check_cron_sync",
        "Check installed crontab against registered jobs.",
        schemas.EmptyArgs,
        scheduler_tools.check_cron_sync,
    ),
    "business.closure_gap_scan": ToolDefinition(
        "business.closure_gap_scan",
        "Scan decision, funnel, portfolio, and review closure gaps.",
        schemas.EmptyArgs,
        business.closure_gap_scan,
    ),
    "business.repair_funnel_score_align": ToolDefinition(
        "business.repair_funnel_score_align",
        "Promote ingested funnel candidates when latest score rows exist.",
        schemas.FunnelScoreAlignArgs,
        business.repair_funnel_score_align,
    ),
    "business.repair_funnel_review_refresh": ToolDefinition(
        "business.repair_funnel_review_refresh",
        "Refresh T+N funnel review snapshots.",
        schemas.FunnelReviewRefreshArgs,
        business.repair_funnel_review_refresh,
    ),
    "business.run_decision_snapshot": ToolDefinition(
        "business.run_decision_snapshot",
        "Run the decision daily snapshot job logic.",
        schemas.DecisionSnapshotArgs,
        business.run_decision_snapshot,
    ),
    "business.reconcile_portfolio_positions": ToolDefinition(
        "business.reconcile_portfolio_positions",
        "Rebuild portfolio positions from executed orders with quantity and price.",
        schemas.ReconcilePositionsArgs,
        business.reconcile_portfolio_positions,
    ),
    "business.portfolio_closure_scan": ToolDefinition(
        "business.portfolio_closure_scan",
        "Scan portfolio orders, positions, and review closure gaps.",
        schemas.EmptyArgs,
        business.portfolio_closure_scan,
    ),
    "business.generate_portfolio_order_reviews": ToolDefinition(
        "business.generate_portfolio_order_reviews",
        "Generate T+N reviews for executed portfolio orders.",
        schemas.PortfolioOrderReviewsArgs,
        business.generate_portfolio_order_reviews,
    ),
    "agents.start_run": ToolDefinition(
        "agents.start_run",
        "Create a persisted Agent runtime run.",
        schemas.AgentStartRunArgs,
        agents_tools.start_run,
    ),
    "agents.get_run": ToolDefinition(
        "agents.get_run",
        "Get an Agent runtime run with steps.",
        schemas.AgentGetRunArgs,
        agents_tools.get_agent_run,
    ),
    "agents.list_runs": ToolDefinition(
        "agents.list_runs",
        "List Agent runtime runs.",
        schemas.AgentListRunsArgs,
        agents_tools.list_agent_runs,
    ),
    "agents.cancel_run": ToolDefinition(
        "agents.cancel_run",
        "Cancel a queued or running Agent runtime run.",
        schemas.AgentCancelRunArgs,
        agents_tools.cancel_agent_run_tool,
    ),
    "agents.approve_run": ToolDefinition(
        "agents.approve_run",
        "Approve a waiting Agent run and execute pending write steps.",
        schemas.AgentApprovalArgs,
        agents_tools.approve_agent_run,
    ),
    "agents.reject_run": ToolDefinition(
        "agents.reject_run",
        "Reject a waiting Agent run without executing pending write steps.",
        schemas.AgentApprovalArgs,
        agents_tools.reject_agent_run,
    ),
    "memory.list_items": ToolDefinition(
        "memory.list_items",
        "List persisted Agent quality memories.",
        schemas.MemoryListArgs,
        memory_tools.list_items,
    ),
    "memory.record_item": ToolDefinition(
        "memory.record_item",
        "Record a quality memory item with write guards.",
        schemas.MemoryRecordArgs,
        memory_tools.record_item,
    ),
    "memory.search_relevant": ToolDefinition(
        "memory.search_relevant",
        "Search active quality memories relevant to a symbol or scope.",
        schemas.MemorySearchArgs,
        memory_tools.search_relevant,
    ),
    "governance.quality_snapshot": ToolDefinition(
        "governance.quality_snapshot",
        "Return Agent quality scores and risk status.",
        schemas.GovernanceQualityArgs,
        governance_tools.quality_snapshot,
    ),
    "governance.list_rules": ToolDefinition(
        "governance.list_rules",
        "List Agent governance policy rules.",
        schemas.GovernanceRuleListArgs,
        governance_tools.list_rules,
    ),
    "governance.upsert_rule": ToolDefinition(
        "governance.upsert_rule",
        "Create or update an Agent governance policy rule.",
        schemas.GovernanceRuleUpsertArgs,
        governance_tools.upsert_rule,
    ),
    "governance.evaluate_action": ToolDefinition(
        "governance.evaluate_action",
        "Evaluate the policy decision for an Agent action.",
        schemas.GovernanceEvaluateArgs,
        governance_tools.evaluate_action,
    ),
}


def list_tools() -> list[dict[str, Any]]:
    return [tool.to_mcp_tool() for tool in TOOLS.values()]


def call_tool(name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    tool = TOOLS.get(str(name or "").strip())
    if not tool:
        raise MCPToolError(f"unknown_tool:{name}")
    try:
        payload = tool.schema_model.model_validate(arguments or {})
    except ValidationError as exc:
        raise MCPToolError(f"invalid_arguments:{exc}") from exc
    try:
        return tool.handler(payload)
    except MCPToolError:
        raise
    except Exception as exc:
        raise MCPToolError(str(exc)) from exc
