from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from check_gpt_provider_nodes import probe_endpoint_autovalidate
from llm_gateway import get_provider_observability_7d, get_runtime_rate_limit_status
from llm_provider_config import DEFAULT_PROVIDER_FILE, get_default_rate_limit_per_minute, reload_provider_runtime

_LOCK = threading.RLock()


def _resolve_config_path() -> Path:
    configured = (os.getenv("LLM_PROVIDER_CONFIG_FILE", "") or "").strip()
    path = configured or DEFAULT_PROVIDER_FILE
    return Path(path).resolve()


def _default_payload() -> dict[str, Any]:
    return {
        "default_request_model": "GPT-5.4",
        "fallback_models": ["GPT-5.4", "kimi-k2.5", "deepseek-chat"],
        "default_rate_limit_per_minute": 10,
        "providers": {},
    }


def _load_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _default_payload()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _default_payload()
    if not isinstance(data, dict):
        return _default_payload()
    merged = _default_payload()
    merged.update(data)
    if not isinstance(merged.get("providers"), dict):
        merged["providers"] = {}
    merged["default_rate_limit_per_minute"] = max(
        1,
        int(merged.get("default_rate_limit_per_minute") or get_default_rate_limit_per_minute() or 10),
    )
    return merged


def _save_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _as_nodes(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _normalize_provider_key(value: str) -> str:
    return (value or "").strip().lower()


def _mask_api_key(value: str) -> str:
    key = (value or "").strip()
    if len(key) <= 8:
        return "*" * len(key)
    return f"{key[:4]}...{key[-4:]}"


def _resolve_node_api_key(node: dict[str, Any]) -> tuple[str, str]:
    api_key = str(node.get("api_key") or "").strip()
    api_key_env = str(node.get("api_key_env") or "").strip()
    if api_key.startswith("env:"):
        env_name = api_key[4:].strip()
        if env_name:
            return str(os.getenv(env_name, "") or "").strip(), f"env:{env_name}"
        return "", "inline"
    if api_key_env:
        return str(os.getenv(api_key_env, "") or "").strip(), f"env:{api_key_env}"
    return api_key, "inline"


def _sanitize_node(
    *,
    provider_key: str,
    index: int,
    node: dict[str, Any],
    default_limit: int,
) -> dict[str, Any]:
    status = str(node.get("status") or node.get("health_status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    rate_limit_enabled = bool(node.get("rate_limit_enabled", True))
    node_limit = node.get("rate_limit_per_minute", None)
    rate_limit_per_minute = max(1, int(node_limit or default_limit))
    model_name = str(node.get("model") or provider_key).strip() or provider_key
    base_url = str(node.get("base_url") or "").strip()
    resolved_api_key, api_key_source = _resolve_node_api_key(node)
    runtime = get_runtime_rate_limit_status(
        model=model_name,
        base_url=base_url,
        rate_limit_enabled=rate_limit_enabled,
        rate_limit_per_minute=rate_limit_per_minute,
    )
    observability = get_provider_observability_7d(str(runtime.get("provider_signature") or ""))
    return {
        "provider_key": provider_key,
        "index": index,
        "model": model_name,
        "base_url": base_url,
        "api_key_masked": _mask_api_key(resolved_api_key),
        "has_api_key": bool(resolved_api_key),
        "api_key_source": api_key_source,
        "api_key_env": str(node.get("api_key_env") or ""),
        "default_temperature": float(node.get("default_temperature", 0.2) or 0.2),
        "enabled": bool(node.get("enabled", True)),
        "status": status,
        "rate_limit_enabled": rate_limit_enabled,
        "rate_limit_per_minute": rate_limit_per_minute,
        "last_checked_at": node.get("last_checked_at"),
        "last_http_status": node.get("last_http_status"),
        "last_latency_ms": node.get("last_latency_ms"),
        "last_error": node.get("last_error"),
        "health_status": str(node.get("health_status") or ""),
        "health_recommendation": str(node.get("health_recommendation") or ""),
        "runtime_status": runtime.get("runtime_status"),
        "window_reset_at": runtime.get("window_reset_at"),
        "provider_signature": runtime.get("provider_signature"),
        "count_current_minute": runtime.get("count_current_minute"),
        "counter_source": runtime.get("counter_source"),
        "observability_7d": observability,
    }


def _recommend_status(*, ok: bool, status_code: int | None, error_text: str) -> str:
    if ok:
        return "active"
    if status_code in {401, 403, 404}:
        return "disabled"
    if status_code == 429:
        return "degraded"
    if status_code is not None and status_code >= 500:
        return "degraded"
    if "timeout" in (error_text or "").lower():
        return "degraded"
    return "disabled"


def _persist_probe_result(
    *,
    provider_key: str,
    index: int,
    result,
    attempts: list[dict[str, Any]],
) -> None:
    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        nodes = _as_nodes((cfg.get("providers") or {}).get(provider_key))
        pos = index - 1
        if pos < 0 or pos >= len(nodes):
            return
        node = dict(nodes[pos])
        recommendation = _recommend_status(
            ok=bool(result.ok),
            status_code=result.status_code,
            error_text=str(result.error or ""),
        )
        node["last_checked_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        node["last_http_status"] = result.status_code
        node["last_latency_ms"] = int(result.latency_ms or 0)
        node["last_error"] = str(result.error or "")
        node["last_probe_model"] = str(result.used_model or "")
        node["last_probe_base_url"] = str(result.used_base_url or "")
        node["last_probe_attempts"] = list(attempts[-8:])
        node["health_status"] = "healthy" if result.ok else "unhealthy"
        node["health_recommendation"] = recommendation
        node["consecutive_failures"] = 0 if result.ok else int(node.get("consecutive_failures") or 0) + 1
        if result.ok:
            node["enabled"] = True
            node["status"] = "active"
        nodes[pos] = node
        (cfg.get("providers") or {})[provider_key] = nodes
        _save_payload(path, cfg)
    reload_provider_runtime()


def list_llm_providers() -> dict[str, Any]:
    path = _resolve_config_path()
    with _LOCK:
        payload = _load_payload(path)
    default_limit = max(1, int(payload.get("default_rate_limit_per_minute") or 10))
    provider_items: list[dict[str, Any]] = []
    for provider_key_raw, raw_nodes in (payload.get("providers") or {}).items():
        provider_key = _normalize_provider_key(str(provider_key_raw))
        for idx, node in enumerate(_as_nodes(raw_nodes), start=1):
            provider_items.append(
                _sanitize_node(
                    provider_key=provider_key,
                    index=idx,
                    node=node,
                    default_limit=default_limit,
                )
            )
    return {
        "ok": True,
        "config_path": str(path),
        "default_request_model": str(payload.get("default_request_model") or ""),
        "fallback_models": list(payload.get("fallback_models") or []),
        "default_rate_limit_per_minute": default_limit,
        "items": provider_items,
    }


def _parse_index(value: Any) -> int:
    idx = int(value or 0)
    if idx <= 0:
        raise ValueError("index 必须是正整数")
    return idx


def create_llm_provider(payload: dict[str, Any]) -> dict[str, Any]:
    provider_key = _normalize_provider_key(str(payload.get("provider_key") or payload.get("model_key") or ""))
    model = str(payload.get("model") or provider_key).strip() or provider_key
    base_url = str(payload.get("base_url") or "").strip()
    api_key = str(payload.get("api_key") or "").strip()
    api_key_env = str(payload.get("api_key_env") or "").strip()
    if not provider_key:
        raise ValueError("provider_key 不能为空")
    if not base_url:
        raise ValueError("base_url 不能为空")
    if not api_key and not api_key_env:
        raise ValueError("api_key 或 api_key_env 至少提供一个")
    status = str(payload.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    default_temperature = float(payload.get("default_temperature", 0.2) or 0.2)
    rate_limit_enabled = bool(payload.get("rate_limit_enabled", True))
    rate_limit_per_minute = payload.get("rate_limit_per_minute", None)

    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        providers = cfg.setdefault("providers", {})
        current = _as_nodes(providers.get(provider_key))
        default_limit = max(1, int(cfg.get("default_rate_limit_per_minute") or 10))
        current.append(
            {
                "model": model,
                "base_url": base_url,
                "api_key": api_key,
                "api_key_env": api_key_env,
                "default_temperature": default_temperature,
                "enabled": status == "active",
                "status": status,
                "rate_limit_enabled": rate_limit_enabled,
                "rate_limit_per_minute": (
                    max(1, int(rate_limit_per_minute))
                    if rate_limit_per_minute is not None
                    else default_limit
                ),
            }
        )
        providers[provider_key] = current
        _save_payload(path, cfg)
    reload_provider_runtime()
    return list_llm_providers()


def update_llm_provider(payload: dict[str, Any]) -> dict[str, Any]:
    provider_key = _normalize_provider_key(str(payload.get("provider_key") or payload.get("model_key") or ""))
    if not provider_key:
        raise ValueError("provider_key 不能为空")
    idx = _parse_index(payload.get("index"))
    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        providers = cfg.setdefault("providers", {})
        nodes = _as_nodes(providers.get(provider_key))
        if not nodes:
            raise ValueError(f"未找到 provider_key={provider_key}")
        pos = idx - 1
        if pos >= len(nodes):
            raise ValueError(f"index 超出范围，当前数量={len(nodes)}")
        node = dict(nodes[pos])
        default_limit = max(1, int(cfg.get("default_rate_limit_per_minute") or 10))
        for field in ("model", "base_url", "status"):
            if field in payload:
                node[field] = str(payload.get(field) or "").strip()
        if "api_key" in payload and str(payload.get("api_key") or "").strip():
            node["api_key"] = str(payload.get("api_key") or "").strip()
        if "api_key_env" in payload:
            node["api_key_env"] = str(payload.get("api_key_env") or "").strip()
        if "default_temperature" in payload:
            node["default_temperature"] = float(payload.get("default_temperature") or 0.2)
        if "rate_limit_enabled" in payload:
            node["rate_limit_enabled"] = bool(payload.get("rate_limit_enabled"))
        if "rate_limit_per_minute" in payload:
            value = payload.get("rate_limit_per_minute")
            node["rate_limit_per_minute"] = max(1, int(value)) if value is not None else default_limit
        normalized_status = str(node.get("status") or "active").strip().lower()
        if normalized_status not in {"active", "disabled"}:
            normalized_status = "active"
        node["status"] = normalized_status
        node["enabled"] = normalized_status == "active"
        nodes[pos] = node
        providers[provider_key] = nodes
        _save_payload(path, cfg)
    reload_provider_runtime()
    return list_llm_providers()


def delete_llm_provider(payload: dict[str, Any]) -> dict[str, Any]:
    provider_key = _normalize_provider_key(str(payload.get("provider_key") or payload.get("model_key") or ""))
    if not provider_key:
        raise ValueError("provider_key 不能为空")
    idx = _parse_index(payload.get("index"))
    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        providers = cfg.setdefault("providers", {})
        nodes = _as_nodes(providers.get(provider_key))
        if not nodes:
            raise ValueError(f"未找到 provider_key={provider_key}")
        pos = idx - 1
        if pos >= len(nodes):
            raise ValueError(f"index 超出范围，当前数量={len(nodes)}")
        nodes.pop(pos)
        if nodes:
            providers[provider_key] = nodes
        else:
            providers.pop(provider_key, None)
        _save_payload(path, cfg)
    reload_provider_runtime()
    return list_llm_providers()


def test_one_llm_provider(payload: dict[str, Any]) -> dict[str, Any]:
    provider_key = _normalize_provider_key(str(payload.get("provider_key") or payload.get("model_key") or ""))
    idx = _parse_index(payload.get("index"))
    timeout_s = float(payload.get("timeout_s", 20.0) or 20.0)
    probe_retries = int(payload.get("probe_retries", 0) or 0)
    case_mode = str(payload.get("case_mode", "auto") or "auto")

    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        nodes = _as_nodes((cfg.get("providers") or {}).get(provider_key))
    if not nodes:
        raise ValueError(f"未找到 provider_key={provider_key}")
    pos = idx - 1
    if pos >= len(nodes):
        raise ValueError(f"index 超出范围，当前数量={len(nodes)}")
    node = nodes[pos]
    model = str(node.get("model") or provider_key).strip() or provider_key
    base_url = str(node.get("base_url") or "").strip()
    api_key, api_key_source = _resolve_node_api_key(node)
    if not base_url or not api_key:
        raise ValueError("节点缺少 base_url/api_key")
    result, attempts = probe_endpoint_autovalidate(
        base_url=base_url,
        api_key=api_key,
        probe_model=model,
        case_mode=case_mode,
        timeout=timeout_s,
        probe_retries=max(0, probe_retries),
    )
    _persist_probe_result(provider_key=provider_key, index=idx, result=result, attempts=attempts)
    recommendation = _recommend_status(ok=bool(result.ok), status_code=result.status_code, error_text=str(result.error or ""))
    return {
        "ok": True,
        "result": {
            "provider_key": provider_key,
            "index": idx,
            "node_model": model,
            "node_base_url": base_url,
            "probe_ok": bool(result.ok),
            "status_code": result.status_code,
            "latency_ms": result.latency_ms,
            "error": result.error,
            "used_model": result.used_model,
            "used_base_url": result.used_base_url,
            "attempts": attempts[-10:],
            "api_key_source": api_key_source,
            "health_recommendation": recommendation,
        },
    }


def test_model_llm_providers(payload: dict[str, Any]) -> dict[str, Any]:
    provider_key = _normalize_provider_key(str(payload.get("provider_key") or payload.get("model_key") or payload.get("model") or ""))
    if not provider_key:
        raise ValueError("provider_key 不能为空")
    timeout_s = float(payload.get("timeout_s", 20.0) or 20.0)
    probe_retries = int(payload.get("probe_retries", 0) or 0)
    case_mode = str(payload.get("case_mode", "auto") or "auto")
    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        nodes = _as_nodes((cfg.get("providers") or {}).get(provider_key))
    results: list[dict[str, Any]] = []
    for idx, node in enumerate(nodes, start=1):
        one = test_one_llm_provider(
            {
                "provider_key": provider_key,
                "index": idx,
                "timeout_s": timeout_s,
                "probe_retries": probe_retries,
                "case_mode": case_mode,
            }
        )
        results.append(one["result"])
    return {"ok": True, "provider_key": provider_key, "results": results, "total": len(results)}


def update_default_rate_limit(payload: dict[str, Any]) -> dict[str, Any]:
    value = max(1, int(payload.get("default_rate_limit_per_minute") or 10))
    path = _resolve_config_path()
    with _LOCK:
        cfg = _load_payload(path)
        cfg["default_rate_limit_per_minute"] = value
        _save_payload(path, cfg)
    reload_provider_runtime()
    return list_llm_providers()
