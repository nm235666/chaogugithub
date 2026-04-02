#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from db_compat import get_redis_client
from llm_provider_config import (
    get_default_rate_limit_per_minute,
    get_default_fallback_models,
    get_default_request_model,
    get_provider_config,
    get_provider_candidates,
)


DEFAULT_LLM_MODEL = get_default_request_model()
_deepseek_cfg = get_provider_config("deepseek-chat")
_gpt_cfg = get_provider_config("gpt-5.4")
_kimi_cfg = get_provider_config("kimi-k2.5")
DEFAULT_LLM_BASE_URL = _deepseek_cfg.base_url if _deepseek_cfg else ""
DEFAULT_LLM_API_KEY = _deepseek_cfg.api_key if _deepseek_cfg else ""
GPT54_BASE_URL = _gpt_cfg.base_url if _gpt_cfg else ""
GPT54_API_KEY = _gpt_cfg.api_key if _gpt_cfg else ""
KIMI_BASE_URL = _kimi_cfg.base_url if _kimi_cfg else ""
KIMI_API_KEY = _kimi_cfg.api_key if _kimi_cfg else ""

TRANSIENT_HTTP_CODES = {408, 409, 425, 429, 500, 502, 503, 504, 520, 522, 524}
RATE_LIMIT_KEY_PREFIX = "llm:rate:provider"
RATE_STATE_KEY_PREFIX = "llm:rate:state"
METRICS_KEY_PREFIX = "llm:metrics:provider"
METRICS_TTL_SECONDS = 9 * 24 * 3600
_LOCAL_RATE_BUCKETS: dict[str, tuple[int, float]] = {}
_LOCAL_RATE_LOCK = threading.Lock()
_LOCAL_FALLBACK_WARNED_AT = 0.0
_LOCAL_STATE: dict[str, dict[str, float]] = {}
_LOCAL_METRICS: dict[str, dict[str, int]] = {}
_LOCAL_LATENCIES: dict[str, list[int]] = {}
RATE_LIMIT_WARMUP_SECONDS = max(0, int(os.getenv("LLM_RATE_LIMIT_WARMUP_SECONDS", "15") or "15"))
RATE_LIMIT_COOLDOWN_SECONDS = max(0, int(os.getenv("LLM_RATE_LIMIT_COOLDOWN_SECONDS", "20") or "20"))
RATE_LIMIT_FAILURE_PENALTY = max(0, int(os.getenv("LLM_RATE_LIMIT_FAILURE_PENALTY", "1") or "1"))
RATE_LIMIT_429_PENALTY = max(0, int(os.getenv("LLM_RATE_LIMIT_429_PENALTY", "2") or "2"))


@dataclass(frozen=True)
class LLMRoute:
    model: str
    base_url: str
    api_key: str
    temperature: float
    provider_signature: str = ""
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 10


@dataclass(frozen=True)
class LLMAttempt:
    model: str
    base_url: str
    error: str = ""


@dataclass(frozen=True)
class LLMCallResult:
    text: str
    requested_model: str
    used_model: str
    used_base_url: str
    attempts: tuple[LLMAttempt, ...]


def _normalize_base_url(base_url: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    return raw.rstrip("/").lower()


def _build_provider_signature(model: str, base_url: str) -> str:
    return f"{normalize_model_name(model).lower()}|{_normalize_base_url(base_url)}"


def _rate_window() -> tuple[int, int]:
    now = int(time.time())
    window_id = now // 60
    window_reset_at = (window_id + 1) * 60
    return window_id, window_reset_at


def _redis_key(signature: str, window_id: int) -> str:
    return f"{RATE_LIMIT_KEY_PREFIX}:{signature}:{window_id}"


def _state_redis_key(signature: str) -> str:
    return f"{RATE_STATE_KEY_PREFIX}:{signature}"


def _metrics_redis_key(signature: str, day: str) -> str:
    return f"{METRICS_KEY_PREFIX}:{signature}:{day}"


def _latency_redis_key(signature: str, day: str) -> str:
    return f"{METRICS_KEY_PREFIX}:{signature}:{day}:latency"


def _day_key(ts: float | None = None) -> str:
    if ts is None:
        ts = time.time()
    return time.strftime("%Y%m%d", time.gmtime(ts))


def _extract_http_status_from_error(error_text: str) -> int | None:
    text = str(error_text or "")
    m = re.search(r"HTTP\s+(\d{3})", text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _warn_local_fallback_once(message: str) -> None:
    global _LOCAL_FALLBACK_WARNED_AT
    now = time.time()
    if now - _LOCAL_FALLBACK_WARNED_AT < 60:
        return
    _LOCAL_FALLBACK_WARNED_AT = now
    print(message)


def _local_allow(signature: str, limit: int, now_ts: float | None = None) -> tuple[bool, int, int]:
    if now_ts is None:
        now_ts = time.time()
    window_id = int(now_ts // 60)
    window_reset_at = (window_id + 1) * 60
    key = f"{signature}:{window_id}"
    with _LOCAL_RATE_LOCK:
        count, _ = _LOCAL_RATE_BUCKETS.get(key, (0, float(window_reset_at)))
        count += 1
        _LOCAL_RATE_BUCKETS[key] = (count, float(window_reset_at))
        stale_keys = [k for k, (_, exp) in _LOCAL_RATE_BUCKETS.items() if exp <= now_ts]
        for k in stale_keys[:256]:
            _LOCAL_RATE_BUCKETS.pop(k, None)
    return count <= max(1, int(limit)), count, window_reset_at


def _read_local_state(signature: str) -> dict[str, float]:
    with _LOCAL_RATE_LOCK:
        return dict(_LOCAL_STATE.get(signature) or {})


def _write_local_state(signature: str, state: dict[str, float]) -> None:
    with _LOCAL_RATE_LOCK:
        _LOCAL_STATE[signature] = dict(state)


def _record_metrics_local(signature: str, fields: dict[str, int], latency_ms: int | None) -> None:
    day = _day_key()
    key = f"{signature}:{day}"
    with _LOCAL_RATE_LOCK:
        bucket = _LOCAL_METRICS.get(key, {})
        for field, delta in fields.items():
            bucket[field] = int(bucket.get(field, 0)) + int(delta)
        _LOCAL_METRICS[key] = bucket
        if latency_ms is not None and latency_ms >= 0:
            lkey = f"{signature}:{day}"
            arr = _LOCAL_LATENCIES.get(lkey, [])
            arr.append(int(latency_ms))
            if len(arr) > 5000:
                arr = arr[-5000:]
            _LOCAL_LATENCIES[lkey] = arr
        # prune old keys
        today = int(_day_key())
        for k in list(_LOCAL_METRICS.keys())[:2000]:
            try:
                d = int(k.rsplit(":", 1)[-1])
                if today - d > 10:
                    _LOCAL_METRICS.pop(k, None)
            except Exception:
                continue


def _read_state(signature: str) -> tuple[dict[str, float], str]:
    client = get_redis_client()
    if client is not None:
        try:
            raw = client.hgetall(_state_redis_key(signature)) or {}
            parsed: dict[str, float] = {}
            for key in ("failure_streak", "last_success_ts", "last_failure_ts", "blocked_until", "rl429_until"):
                try:
                    parsed[key] = float(raw.get(key) or 0.0)
                except Exception:
                    parsed[key] = 0.0
            return parsed, "redis"
        except Exception:
            pass
    return _read_local_state(signature), "local"


def _write_state(signature: str, state: dict[str, float]) -> None:
    client = get_redis_client()
    if client is not None:
        try:
            key = _state_redis_key(signature)
            payload = {k: str(v) for k, v in state.items()}
            if payload:
                client.hset(key, mapping=payload)
                client.expire(key, max(300, RATE_LIMIT_COOLDOWN_SECONDS * 3))
                return
        except Exception:
            pass
    _write_local_state(signature, state)


def record_provider_metrics(
    signature: str,
    *,
    success: bool | None = None,
    status_code: int | None = None,
    rate_limited: bool = False,
    switch_event: bool = False,
    latency_ms: int | None = None,
) -> None:
    day = _day_key()
    fields: dict[str, int] = {}
    if success is True:
        fields["success"] = 1
    elif success is False:
        fields["failed"] = 1
    if rate_limited:
        fields["rate_limited"] = 1
    if switch_event:
        fields["switch_count"] = 1
    if status_code == 429:
        fields["http_429"] = 1
    if not fields and latency_ms is None:
        return
    client = get_redis_client()
    if client is not None:
        try:
            mkey = _metrics_redis_key(signature, day)
            if fields:
                for k, v in fields.items():
                    client.hincrby(mkey, k, int(v))
            if latency_ms is not None and latency_ms >= 0:
                lkey = _latency_redis_key(signature, day)
                client.rpush(lkey, int(latency_ms))
                client.ltrim(lkey, -5000, -1)
                client.expire(lkey, METRICS_TTL_SECONDS)
            client.expire(mkey, METRICS_TTL_SECONDS)
            return
        except Exception:
            pass
    _record_metrics_local(signature, fields, latency_ms)


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    arr = sorted(int(v) for v in values if int(v) >= 0)
    if not arr:
        return 0
    idx = max(0, min(len(arr) - 1, int(0.95 * (len(arr) - 1))))
    return int(arr[idx])


def get_provider_observability_7d(signature: str) -> dict:
    days: list[str] = []
    now_ts = time.time()
    for i in range(7):
        day_ts = now_ts - i * 86400
        days.append(_day_key(day_ts))
    totals = {"success": 0, "failed": 0, "rate_limited": 0, "http_429": 0, "switch_count": 0}
    latencies: list[int] = []
    source = "redis"
    client = get_redis_client()
    if client is not None:
        try:
            for day in days:
                mkey = _metrics_redis_key(signature, day)
                raw = client.hgetall(mkey) or {}
                for k in totals:
                    totals[k] += int(raw.get(k) or 0)
                lkey = _latency_redis_key(signature, day)
                values = client.lrange(lkey, 0, -1) or []
                for v in values:
                    try:
                        latencies.append(int(v))
                    except Exception:
                        continue
        except Exception:
            source = "local"
            client = None
    if client is None:
        source = "local"
        with _LOCAL_RATE_LOCK:
            for day in days:
                key = f"{signature}:{day}"
                bucket = _LOCAL_METRICS.get(key, {})
                for k in totals:
                    totals[k] += int(bucket.get(k, 0))
                latencies.extend(_LOCAL_LATENCIES.get(key, []))
    total_calls = totals["success"] + totals["failed"]
    success_rate = round((totals["success"] / total_calls) * 100, 2) if total_calls > 0 else 0.0
    return {
        "source": source,
        "days": 7,
        "total_calls": total_calls,
        "success": totals["success"],
        "failed": totals["failed"],
        "success_rate_pct": success_rate,
        "p95_latency_ms": _p95(latencies),
        "rate_limited": totals["rate_limited"],
        "http_429": totals["http_429"],
        "switch_count": totals["switch_count"],
    }


def mark_provider_result(signature: str, *, success: bool, status_code: int | None = None) -> None:
    now_ts = time.time()
    state, _ = _read_state(signature)
    failure_streak = int(state.get("failure_streak", 0.0) or 0)
    blocked_until = float(state.get("blocked_until", 0.0) or 0.0)
    rl429_until = float(state.get("rl429_until", 0.0) or 0.0)
    if success:
        state["failure_streak"] = 0.0
        state["last_success_ts"] = now_ts
        # Keep cooldown only when it is still active; success does not necessarily mean clear.
        state["blocked_until"] = blocked_until if blocked_until > now_ts else 0.0
        state["rl429_until"] = rl429_until if rl429_until > now_ts else 0.0
    else:
        failure_streak += 1
        state["failure_streak"] = float(failure_streak)
        state["last_failure_ts"] = now_ts
        if status_code == 429:
            state["rl429_until"] = max(rl429_until, now_ts + float(RATE_LIMIT_COOLDOWN_SECONDS))
            state["blocked_until"] = max(blocked_until, now_ts + float(RATE_LIMIT_COOLDOWN_SECONDS))
        elif failure_streak >= 3:
            state["blocked_until"] = max(blocked_until, now_ts + float(RATE_LIMIT_COOLDOWN_SECONDS))
    _write_state(signature, state)


def rate_limit_allow(signature: str, limit: int) -> tuple[bool, int, int]:
    capped_limit = max(1, int(limit or get_default_rate_limit_per_minute()))
    window_id, window_reset_at = _rate_window()
    key = _redis_key(signature, window_id)
    now_ts = time.time()
    state, _ = _read_state(signature)
    blocked_until = float(state.get("blocked_until", 0.0) or 0.0)
    if blocked_until > now_ts:
        record_provider_metrics(signature, rate_limited=True)
        return False, int(state.get("failure_streak", 0.0) or 0), window_reset_at
    effective_limit = capped_limit
    last_success_ts = float(state.get("last_success_ts", 0.0) or 0.0)
    last_failure_ts = float(state.get("last_failure_ts", 0.0) or 0.0)
    if (
        RATE_LIMIT_WARMUP_SECONDS > 0
        and last_success_ts > 0
        and last_success_ts > last_failure_ts
        and (now_ts - last_success_ts) < RATE_LIMIT_WARMUP_SECONDS
    ):
        effective_limit = max(1, capped_limit // 2)
    failure_streak = int(state.get("failure_streak", 0.0) or 0)
    rl429_until = float(state.get("rl429_until", 0.0) or 0.0)
    penalty = failure_streak * RATE_LIMIT_FAILURE_PENALTY
    if rl429_until > now_ts:
        penalty += RATE_LIMIT_429_PENALTY
    client = get_redis_client()
    if client is None:
        _warn_local_fallback_once("[llm-gateway] Redis 不可用，LLM 限速降级为进程内计数。")
        allowed, value, reset_at = _local_allow(signature, effective_limit)
        adjusted = value + penalty
        if not (adjusted <= effective_limit and allowed):
            record_provider_metrics(signature, rate_limited=True)
        return adjusted <= effective_limit and allowed, adjusted, reset_at
    try:
        value = int(client.incr(key))
        if value == 1:
            client.expire(key, 120)
        adjusted = value + penalty
        if not (adjusted <= effective_limit):
            record_provider_metrics(signature, rate_limited=True)
        return adjusted <= effective_limit, adjusted, window_reset_at
    except Exception as exc:
        _warn_local_fallback_once(f"[llm-gateway] Redis 限速失败({exc})，已降级为进程内计数。")
        allowed, value, reset_at = _local_allow(signature, effective_limit)
        adjusted = value + penalty
        if not (adjusted <= effective_limit and allowed):
            record_provider_metrics(signature, rate_limited=True)
        return adjusted <= effective_limit and allowed, adjusted, reset_at


def get_runtime_rate_limit_status(
    *,
    model: str,
    base_url: str,
    rate_limit_enabled: bool,
    rate_limit_per_minute: int | None,
) -> dict:
    signature = _build_provider_signature(model, base_url)
    limit = max(1, int(rate_limit_per_minute or get_default_rate_limit_per_minute()))
    _, window_reset_at = _rate_window()
    if not rate_limit_enabled:
        return {
            "provider_signature": signature,
            "runtime_status": "ok",
            "window_reset_at": window_reset_at,
            "rate_limit_enabled": False,
            "rate_limit_per_minute": limit,
            "count_current_minute": 0,
        }
    key = _redis_key(signature, int(time.time()) // 60)
    client = get_redis_client()
    state, state_source = _read_state(signature)
    count = 0
    source = "redis"
    if client is None:
        source = "local"
        with _LOCAL_RATE_LOCK:
            count = int((_LOCAL_RATE_BUCKETS.get(f"{signature}:{int(time.time()) // 60}") or (0, 0.0))[0])
    else:
        try:
            count = int(client.get(key) or 0)
        except Exception:
            source = "local"
            with _LOCAL_RATE_LOCK:
                count = int((_LOCAL_RATE_BUCKETS.get(f"{signature}:{int(time.time()) // 60}") or (0, 0.0))[0])
    return {
        "provider_signature": signature,
        "runtime_status": (
            "cooldown"
            if float(state.get("blocked_until", 0.0) or 0.0) > time.time()
            else ("rate_limited" if count >= limit else "ok")
        ),
        "window_reset_at": window_reset_at,
        "rate_limit_enabled": True,
        "rate_limit_per_minute": limit,
        "count_current_minute": count,
        "counter_source": source,
        "failure_streak": int(state.get("failure_streak", 0.0) or 0),
        "blocked_until": int(state.get("blocked_until", 0.0) or 0.0),
        "state_source": state_source,
        "warmup_seconds": RATE_LIMIT_WARMUP_SECONDS,
        "cooldown_seconds": RATE_LIMIT_COOLDOWN_SECONDS,
    }


def normalize_model_name(model: str) -> str:
    raw = (model or "").strip()
    m = raw.lower().replace("_", "-")
    if m in {"", "auto", "default"}:
        return "auto"
    if m in {"kimi2.5", "kimi-2.5", "kimi k2.5", "kimi-k2", "kimi2", "kimi"}:
        return "kimi-k2.5"
    if m in {"gpt54", "gpt-54", "gpt 5.4"}:
        return "GPT-5.4"
    return raw


def normalize_temperature_for_model(model: str, temperature: float) -> float:
    m = normalize_model_name(model).lower()
    if m.startswith("kimi-k2.5") or m.startswith("kimi-k2"):
        return 1.0
    return temperature


def ensure_provider_ready(model: str, base_url: str, api_key: str) -> tuple[str, str]:
    normalized_model = normalize_model_name(model) or "unknown"
    resolved_base_url = (base_url or "").strip()
    resolved_api_key = (api_key or "").strip()
    missing: list[str] = []
    if not resolved_base_url:
        missing.append("base_url")
    if not resolved_api_key:
        missing.append("api_key")
    if missing:
        missing_text = "、".join(missing)
        raise RuntimeError(f"LLM 提供商未配置完整：{normalized_model} 缺少 {missing_text}，请检查环境变量。")
    return resolved_base_url, resolved_api_key


def resolve_provider(model: str, base_url: str = "", api_key: str = "") -> tuple[str, str]:
    m = normalize_model_name(model).lower()
    if base_url.strip() or api_key.strip():
        return ensure_provider_ready(model, base_url or DEFAULT_LLM_BASE_URL, api_key or DEFAULT_LLM_API_KEY)
    if m == "auto":
        fallbacks = get_default_fallback_models()
        m = (fallbacks[0] if fallbacks else "GPT-5.4").lower()
    config = get_provider_config(m)
    if config:
        return ensure_provider_ready(model, config.base_url, config.api_key)
    return ensure_provider_ready(model, base_url or DEFAULT_LLM_BASE_URL, api_key or DEFAULT_LLM_API_KEY)


def build_route(model: str, base_url: str = "", api_key: str = "", temperature: float = 0.2) -> LLMRoute:
    normalized_model = normalize_model_name(model)
    if normalized_model.lower() == "auto":
        fallbacks = get_default_fallback_models()
        normalized_model = fallbacks[0] if fallbacks else "GPT-5.4"
    resolved_base_url, resolved_api_key = resolve_provider(normalized_model, base_url, api_key)
    return LLMRoute(
        model=normalized_model,
        base_url=resolved_base_url,
        api_key=resolved_api_key,
        temperature=normalize_temperature_for_model(normalized_model, temperature),
        provider_signature=_build_provider_signature(normalized_model, resolved_base_url),
        rate_limit_enabled=True,
        rate_limit_per_minute=get_default_rate_limit_per_minute(),
    )


def build_routes(model: str, base_url: str = "", api_key: str = "", temperature: float = 0.2) -> list[LLMRoute]:
    normalized_model = normalize_model_name(model)
    if normalized_model.lower() == "auto":
        fallbacks = get_default_fallback_models()
        normalized_model = fallbacks[0] if fallbacks else "GPT-5.4"
    if base_url.strip() or api_key.strip():
        return [build_route(normalized_model, base_url=base_url, api_key=api_key, temperature=temperature)]

    routes: list[LLMRoute] = []
    candidates = get_provider_candidates(normalized_model)
    if not candidates:
        return [build_route(normalized_model, base_url=base_url, api_key=api_key, temperature=temperature)]
    for item in candidates:
        resolved_base_url, resolved_api_key = ensure_provider_ready(item.model, item.base_url, item.api_key)
        route_limit = int(item.rate_limit_per_minute or get_default_rate_limit_per_minute())
        routes.append(
            LLMRoute(
                model=item.model,
                base_url=resolved_base_url,
                api_key=resolved_api_key,
                temperature=normalize_temperature_for_model(item.model, temperature),
                provider_signature=_build_provider_signature(item.model, resolved_base_url),
                rate_limit_enabled=bool(item.rate_limit_enabled),
                rate_limit_per_minute=max(1, route_limit),
            )
        )
    return routes


def sanitize_json_value(value):
    if isinstance(value, dict):
        return {str(k): sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(v) for v in value]
    if isinstance(value, float):
        if value != value:
            return None
        if value == float("inf") or value == float("-inf"):
            return None
    return value


def post_chat_completions(
    route: LLMRoute,
    payload: dict,
    timeout_s: int = 120,
    max_retries: int = 3,
) -> str:
    body = json.dumps(sanitize_json_value(payload), ensure_ascii=False, allow_nan=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {route.api_key}",
        "Connection": "close",
    }
    url = route.base_url.rstrip("/") + "/chat/completions"
    last_error = None

    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=max(timeout_s, 30)) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            last_error = f"HTTP {exc.code} {exc.reason} | {detail}"
            if exc.code in TRANSIENT_HTTP_CODES and attempt < max_retries:
                time.sleep(1.5 * (2**attempt))
                continue
            raise RuntimeError(f"LLM接口错误: {last_error}") from exc
        except Exception as exc:
            last_error = str(exc)
            if attempt < max_retries:
                time.sleep(1.5 * (2**attempt))
                continue

    try:
        import requests  # type: ignore

        response = requests.post(
            url,
            json=sanitize_json_value(payload),
            headers={"Authorization": f"Bearer {route.api_key}", "Connection": "close"},
            timeout=max(timeout_s, 30),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code} {response.reason} | {response.text}")
        return response.text
    except Exception as exc:  # pragma: no cover
        if last_error:
            raise RuntimeError(f"{last_error}; fallback requests error: {exc}") from exc
        raise RuntimeError(str(exc)) from exc


def _compact_error_preview(raw_text: str, limit: int = 280) -> str:
    txt = re.sub(r"\s+", " ", (raw_text or "")).strip()
    if len(txt) <= limit:
        return txt
    return txt[: limit - 3] + "..."


def _extract_message_content(content) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                if item.strip():
                    parts.append(item.strip())
                continue
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
                continue
            if isinstance(text, dict):
                value = text.get("value")
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
                    continue
            if item.get("type") == "output_text":
                value = item.get("text")
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
        return "\n".join(parts).strip()
    return ""


def _extract_text_from_response(raw_text: str) -> str:
    try:
        obj = json.loads(raw_text)
    except Exception as exc:
        raise RuntimeError(f"LLM返回非JSON: {_compact_error_preview(raw_text)}") from exc

    if isinstance(obj, dict):
        error_obj = obj.get("error")
        if error_obj:
            if isinstance(error_obj, dict):
                message = error_obj.get("message") or error_obj.get("code") or error_obj
            else:
                message = error_obj
            raise RuntimeError(f"LLM返回错误对象: {message}")

        choices = obj.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                message = first.get("message")
                if isinstance(message, dict):
                    content = _extract_message_content(message.get("content"))
                    if content:
                        return content
                delta = first.get("delta")
                if isinstance(delta, dict):
                    content = _extract_message_content(delta.get("content"))
                    if content:
                        return content
                text = _extract_message_content(first.get("text"))
                if text:
                    return text

        output_text = obj.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        output = obj.get("output")
        if isinstance(output, list):
            parts: list[str] = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                content = _extract_message_content(item.get("content"))
                if content:
                    parts.append(content)
            if parts:
                return "\n".join(parts).strip()

        data = obj.get("data")
        if isinstance(data, dict):
            content = _extract_message_content(data.get("content"))
            if content:
                return content

    raise RuntimeError(
        "LLM返回缺少可解析内容，响应摘要: "
        f"{_compact_error_preview(raw_text)}"
    )


def build_model_candidates(model: str) -> list[str]:
    raw = (model or "").strip()
    normalized = normalize_model_name(raw)
    default_fallbacks = list(get_default_fallback_models())
    if not default_fallbacks:
        default_fallbacks = ["GPT-5.4", "kimi-k2.5", "deepseek-chat"]
    if normalized.lower() == "auto":
        return default_fallbacks
    parts = [normalize_model_name(x) for x in re.split(r"[|,]", raw) if x.strip()]
    if not parts:
        return default_fallbacks
    deduped: list[str] = []
    seen: set[str] = set()
    for item in parts:
        key = item.lower()
        if key == "auto":
            for fallback_model in default_fallbacks:
                fallback_key = fallback_model.lower()
                if fallback_key not in seen:
                    deduped.append(fallback_model)
                    seen.add(fallback_key)
            continue
        if key not in seen:
            deduped.append(item)
            seen.add(key)
    return deduped or default_fallbacks


def chat_completion_with_fallback(
    *,
    model: str,
    messages: list[dict],
    temperature: float = 0.2,
    base_url: str = "",
    api_key: str = "",
    timeout_s: int = 120,
    max_retries: int = 3,
    **extra_payload,
) -> LLMCallResult:
    requested_model = normalize_model_name(model)
    candidate_models = build_model_candidates(requested_model)
    attempts: list[LLMAttempt] = []
    errors: list[str] = []

    for candidate in candidate_models:
        routes = build_routes(model=candidate, base_url=base_url, api_key=api_key, temperature=temperature)
        for route in routes:
            if route.rate_limit_enabled:
                allowed, current_count, window_reset_at = rate_limit_allow(
                    route.provider_signature,
                    route.rate_limit_per_minute,
                )
                if not allowed:
                    err = (
                        f"rate_limited provider={route.provider_signature} "
                        f"count={current_count} limit={route.rate_limit_per_minute} reset_at={window_reset_at}"
                    )
                    attempts.append(LLMAttempt(model=route.model, base_url=route.base_url, error=err))
                    errors.append(f"{route.model}@{route.base_url}: {err}")
                    continue
            payload = {
                "model": route.model,
                "temperature": route.temperature,
                "messages": messages,
            }
            payload.update(extra_payload)
            started_at = time.time()
            try:
                raw_text = post_chat_completions(route=route, payload=payload, timeout_s=timeout_s, max_retries=max_retries)
                content = _extract_text_from_response(raw_text)
                if route.rate_limit_enabled:
                    mark_provider_result(route.provider_signature, success=True, status_code=200)
                    record_provider_metrics(
                        route.provider_signature,
                        success=True,
                        status_code=200,
                        latency_ms=int((time.time() - started_at) * 1000),
                    )
                attempts.append(LLMAttempt(model=route.model, base_url=route.base_url, error=""))
                return LLMCallResult(
                    text=content,
                    requested_model=requested_model,
                    used_model=route.model,
                    used_base_url=route.base_url,
                    attempts=tuple(attempts),
                )
            except Exception as exc:
                err = str(exc)
                if route.rate_limit_enabled:
                    status_code = _extract_http_status_from_error(err)
                    mark_provider_result(
                        route.provider_signature,
                        success=False,
                        status_code=status_code,
                    )
                    record_provider_metrics(
                        route.provider_signature,
                        success=False,
                        status_code=status_code,
                        latency_ms=int((time.time() - started_at) * 1000),
                        switch_event=True,
                    )
                attempts.append(LLMAttempt(model=route.model, base_url=route.base_url, error=err))
                errors.append(f"{route.model}@{route.base_url}: {err}")
                continue

    raise RuntimeError(" | ".join(errors) or "所有模型调用均失败")


def chat_completion_text(
    *,
    model: str,
    messages: list[dict],
    temperature: float = 0.2,
    base_url: str = "",
    api_key: str = "",
    timeout_s: int = 120,
    max_retries: int = 3,
    **extra_payload,
) -> str:
    return chat_completion_with_fallback(
        model=model,
        messages=messages,
        temperature=temperature,
        base_url=base_url,
        api_key=api_key,
        timeout_s=timeout_s,
        max_retries=max_retries,
        **extra_payload,
    ).text
