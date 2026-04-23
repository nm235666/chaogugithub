from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiLayerContract:
    pattern: str
    layer: str
    domain: str
    methods: tuple[str, ...]
    mode: str = "prefix"  # "prefix" | "exact" | "regex"


_CONTRACTS: tuple[ApiLayerContract, ...] = (
    # Layer 1: user decision
    ApiLayerContract(pattern="/api/decision/actions", layer="layer1_user_decision", domain="decision_action", methods=("GET", "POST"), mode="exact"),
    ApiLayerContract(pattern="/api/decision/board", layer="layer1_user_decision", domain="decision_board", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/stock", layer="layer1_user_decision", domain="decision_board", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/plan", layer="layer1_user_decision", domain="decision_plan", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/history", layer="layer1_user_decision", domain="decision_history", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/kill-switch", layer="layer1_user_decision", domain="decision_control", methods=("GET", "POST"), mode="exact"),
    ApiLayerContract(pattern="/api/decision/strategy-runs", layer="layer1_user_decision", domain="decision_strategy", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/strategy-runs/run", layer="layer1_user_decision", domain="decision_strategy", methods=("POST",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/strategies", layer="layer1_user_decision", domain="decision_strategy", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/decision/snapshot/run", layer="layer1_user_decision", domain="decision_snapshot", methods=("POST",), mode="exact"),
    ApiLayerContract(pattern="/api/funnel", layer="layer1_user_decision", domain="funnel", methods=("GET", "POST"), mode="prefix"),
    ApiLayerContract(pattern=r"^/api/funnel/candidates/[^/]+/transition$", layer="layer1_user_decision", domain="funnel_transition", methods=("POST",), mode="regex"),
    # Layer 2: data assets
    ApiLayerContract(pattern="/api/decision/scores", layer="layer2_data_assets", domain="decision_scoreboard", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/stock-scores", layer="layer2_data_assets", domain="stock_scores", methods=("GET",), mode="prefix"),
    # Layer 3: verification & research
    ApiLayerContract(pattern="/api/decision/calibration", layer="layer3_verification_research", domain="decision_calibration", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/quant-factors", layer="layer3_verification_research", domain="quant_factors", methods=("GET", "POST"), mode="prefix"),
    # Layer 4: governance
    ApiLayerContract(pattern="/api/system", layer="layer4_backoffice_governance", domain="system_governance", methods=("GET", "POST"), mode="prefix"),
    ApiLayerContract(pattern="/api/auth", layer="layer4_backoffice_governance", domain="auth_governance", methods=("GET", "POST"), mode="prefix"),
    ApiLayerContract(pattern="/api/jobs", layer="layer4_backoffice_governance", domain="job_governance", methods=("GET",), mode="prefix"),
    ApiLayerContract(pattern="/api/job-runs", layer="layer4_backoffice_governance", domain="job_governance", methods=("GET",), mode="prefix"),
    ApiLayerContract(pattern="/api/job-alerts", layer="layer4_backoffice_governance", domain="job_governance", methods=("GET",), mode="prefix"),
    ApiLayerContract(pattern="/api/source-monitor", layer="layer4_backoffice_governance", domain="ops_monitoring", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/database-audit", layer="layer4_backoffice_governance", domain="ops_monitoring", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/db-health", layer="layer4_backoffice_governance", domain="ops_monitoring", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/signal-audit", layer="layer4_backoffice_governance", domain="signal_governance", methods=("GET",), mode="exact"),
    ApiLayerContract(pattern="/api/signal-quality", layer="layer4_backoffice_governance", domain="signal_governance", methods=("GET", "POST"), mode="prefix"),
    ApiLayerContract(pattern="/api/metrics/summary", layer="layer4_backoffice_governance", domain="metrics_summary", methods=("GET",), mode="exact"),
)


def _match(contract: ApiLayerContract, path: str) -> bool:
    if contract.mode == "exact":
        return path == contract.pattern
    if contract.mode == "prefix":
        return path.startswith(contract.pattern)
    return bool(re.match(contract.pattern, path))


def resolve_api_layer_contract(path: str) -> ApiLayerContract | None:
    if not str(path or "").startswith("/api/"):
        return None
    # Prefer exact / regex before broad prefixes.
    ordered = sorted(_CONTRACTS, key=lambda c: (0 if c.mode == "exact" else (1 if c.mode == "regex" else 2), -len(c.pattern)))
    for contract in ordered:
        if _match(contract, path):
            return contract
    return None


def is_api_method_allowed(path: str, method: str) -> bool:
    contract = resolve_api_layer_contract(path)
    if contract is None:
        return True
    return str(method or "").upper() in set(contract.methods)


def list_api_layer_contracts() -> list[dict[str, object]]:
    return [
        {
            "pattern": c.pattern,
            "mode": c.mode,
            "layer": c.layer,
            "domain": c.domain,
            "methods": list(c.methods),
        }
        for c in _CONTRACTS
    ]
