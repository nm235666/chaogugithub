from __future__ import annotations

# Stage A (structure-only): expose user-decision domain capabilities
# through a layer facade, without changing underlying service behavior.
from services.funnel_service import (  # noqa: F401
    VALID_STATES,
    create_candidate,
    get_candidate,
    get_funnel_metrics,
    list_candidates,
    transition_candidate,
)

__all__ = [
    "VALID_STATES",
    "create_candidate",
    "get_candidate",
    "get_funnel_metrics",
    "list_candidates",
    "transition_candidate",
]
