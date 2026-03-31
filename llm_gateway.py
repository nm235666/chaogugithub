#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from llm_provider_config import (
    DEFAULT_FALLBACK_MODELS,
    DEFAULT_REQUEST_MODEL,
    PROVIDER_CONFIGS,
    get_provider_candidates,
)


DEFAULT_LLM_MODEL = DEFAULT_REQUEST_MODEL
DEFAULT_LLM_BASE_URL = PROVIDER_CONFIGS["deepseek-chat"].base_url
DEFAULT_LLM_API_KEY = PROVIDER_CONFIGS["deepseek-chat"].api_key
GPT54_BASE_URL = PROVIDER_CONFIGS["gpt-5.4"].base_url
GPT54_API_KEY = PROVIDER_CONFIGS["gpt-5.4"].api_key
KIMI_BASE_URL = PROVIDER_CONFIGS["kimi-k2.5"].base_url
KIMI_API_KEY = PROVIDER_CONFIGS["kimi-k2.5"].api_key

TRANSIENT_HTTP_CODES = {408, 409, 425, 429, 500, 502, 503, 504, 520, 522, 524}


@dataclass(frozen=True)
class LLMRoute:
    model: str
    base_url: str
    api_key: str
    temperature: float


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
        m = DEFAULT_FALLBACK_MODELS[0].lower()
    config = PROVIDER_CONFIGS.get(m)
    if config:
        return ensure_provider_ready(model, config.base_url, config.api_key)
    return ensure_provider_ready(model, base_url or DEFAULT_LLM_BASE_URL, api_key or DEFAULT_LLM_API_KEY)


def build_route(model: str, base_url: str = "", api_key: str = "", temperature: float = 0.2) -> LLMRoute:
    normalized_model = normalize_model_name(model)
    if normalized_model.lower() == "auto":
        normalized_model = DEFAULT_FALLBACK_MODELS[0]
    resolved_base_url, resolved_api_key = resolve_provider(normalized_model, base_url, api_key)
    return LLMRoute(
        model=normalized_model,
        base_url=resolved_base_url,
        api_key=resolved_api_key,
        temperature=normalize_temperature_for_model(normalized_model, temperature),
    )


def build_routes(model: str, base_url: str = "", api_key: str = "", temperature: float = 0.2) -> list[LLMRoute]:
    normalized_model = normalize_model_name(model)
    if normalized_model.lower() == "auto":
        normalized_model = DEFAULT_FALLBACK_MODELS[0]
    if base_url.strip() or api_key.strip():
        return [build_route(normalized_model, base_url=base_url, api_key=api_key, temperature=temperature)]

    routes: list[LLMRoute] = []
    candidates = get_provider_candidates(normalized_model)
    if not candidates:
        return [build_route(normalized_model, base_url=base_url, api_key=api_key, temperature=temperature)]
    for item in candidates:
        resolved_base_url, resolved_api_key = ensure_provider_ready(item.model, item.base_url, item.api_key)
        routes.append(
            LLMRoute(
                model=item.model,
                base_url=resolved_base_url,
                api_key=resolved_api_key,
                temperature=normalize_temperature_for_model(item.model, temperature),
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
    if normalized.lower() == "auto":
        return list(DEFAULT_FALLBACK_MODELS)
    parts = [normalize_model_name(x) for x in re.split(r"[|,]", raw) if x.strip()]
    if not parts:
        return list(DEFAULT_FALLBACK_MODELS)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in parts:
        key = item.lower()
        if key == "auto":
            for fallback_model in DEFAULT_FALLBACK_MODELS:
                fallback_key = fallback_model.lower()
                if fallback_key not in seen:
                    deduped.append(fallback_model)
                    seen.add(fallback_key)
            continue
        if key not in seen:
            deduped.append(item)
            seen.add(key)
    return deduped or list(DEFAULT_FALLBACK_MODELS)


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
            payload = {
                "model": route.model,
                "temperature": route.temperature,
                "messages": messages,
            }
            payload.update(extra_payload)
            try:
                raw_text = post_chat_completions(route=route, payload=payload, timeout_s=timeout_s, max_retries=max_retries)
                content = _extract_text_from_response(raw_text)
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
