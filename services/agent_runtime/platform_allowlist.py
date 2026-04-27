"""Single source of truth for Agent auto-write tool names (funnel repair tools).

Environment variable AGENT_AUTO_WRITE_TOOL_ALLOWLIST may further restrict to a
subset of these names (comma-separated). Unknown names in env are ignored.
"""

from __future__ import annotations

# Keep in sync with mcp_server business repair tools and executor WRITE_TOOLS.
FUNNEL_AUTO_WRITE_TOOLS: tuple[str, ...] = (
    "business.repair_funnel_score_align",
    "business.repair_funnel_review_refresh",
)
