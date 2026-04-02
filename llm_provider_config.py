#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderConfig:
    model: str
    base_url: str
    api_key: str
    default_temperature: float = 0.2
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int | None = None


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _normalize_model_key(model: str) -> str:
    return (model or "").strip().lower()


def _safe_float(value, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _resolve_api_key_from_item(item: dict) -> str:
    api_key = str(item.get("api_key") or "").strip()
    api_key_env = str(item.get("api_key_env") or "").strip()
    if api_key.startswith("env:"):
        env_name = api_key[4:].strip()
        if env_name:
            return _env(env_name, "")
    if api_key_env:
        return _env(api_key_env, "")
    return api_key


DEFAULT_PROVIDER_FILE = "/home/zanbo/zanbotest/config/llm_providers.json"
RELOAD_INTERVAL_SECONDS = 2.0

_LOCK = threading.RLock()
_LAST_RELOAD_AT = 0.0
_LAST_PROVIDER_FILE = ""
_LAST_PROVIDER_FILE_MTIME = -1.0

DEFAULT_REQUEST_MODEL = "auto"
DEFAULT_FALLBACK_MODELS: tuple[str, ...] = ("GPT-5.4", "kimi-k2.5", "deepseek-chat")
DEFAULT_RATE_LIMIT_PER_MINUTE = 10
PROVIDER_CONFIGS: dict[str, ProviderConfig] = {}
PROVIDER_BACKUP_CONFIGS: dict[str, tuple[ProviderConfig, ...]] = {}


def _resolve_provider_file() -> str:
    return _env("LLM_PROVIDER_CONFIG_FILE", DEFAULT_PROVIDER_FILE)


def _load_external_provider_config(provider_file: str) -> dict:
    if not provider_file:
        return {}
    if not os.path.exists(provider_file):
        return {}
    try:
        with open(provider_file, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _build_env_defaults() -> tuple[str, tuple[str, ...]]:
    default_request_model = _env("LLM_DEFAULT_REQUEST_MODEL", "auto") or "auto"
    fallback_models = tuple(
        x.strip()
        for x in _env("LLM_FALLBACK_MODELS", "GPT-5.4,kimi-k2.5,deepseek-chat").split(",")
        if x.strip()
    )
    return default_request_model, (fallback_models or ("GPT-5.4", "kimi-k2.5", "deepseek-chat"))


def _build_env_provider_configs() -> tuple[dict[str, ProviderConfig], dict[str, tuple[ProviderConfig, ...]]]:
    providers: dict[str, ProviderConfig] = {
        "gpt-5.4": ProviderConfig(
            model="GPT-5.4",
            base_url=_env("GPT54_BASE_URL", "http://192.168.5.43:8087/v1"),
            api_key=_env("GPT54_API_KEY"),
            default_temperature=0.2,
        ),
        "kimi-k2.5": ProviderConfig(
            model="kimi-k2.5",
            base_url=_env("KIMI_BASE_URL", "https://api.moonshot.cn/v1"),
            api_key=_env("KIMI_API_KEY"),
            default_temperature=1.0,
        ),
        "deepseek-chat": ProviderConfig(
            model="deepseek-chat",
            base_url=_env("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
            api_key=_env("DEEPSEEK_API_KEY"),
            default_temperature=0.2,
        ),
        "deepseek-reasoner": ProviderConfig(
            model="deepseek-reasoner",
            base_url=_env("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
            api_key=_env("DEEPSEEK_API_KEY"),
            default_temperature=0.2,
        ),
    }
    backups: dict[str, tuple[ProviderConfig, ...]] = {
        "gpt-5.4": tuple(
            item
            for item in (
                ProviderConfig(
                    model="GPT-5.4",
                    base_url=_env("GPT54_ALT2_BASE_URL", "http://38.175.200.219:8317/v1"),
                    api_key=_env("GPT54_ALT2_API_KEY"),
                    default_temperature=0.2,
                ),
                ProviderConfig(
                    model="GPT-5.4",
                    base_url=_env("GPT54_ALT3_BASE_URL", "https://free.9e.nz/v1"),
                    api_key=_env("GPT54_ALT3_API_KEY"),
                    default_temperature=0.2,
                ),
                ProviderConfig(
                    model="GPT-5.4",
                    base_url=_env("GPT54_ALT4_BASE_URL", "http://ice.v.ua/v1"),
                    api_key=_env("GPT54_ALT4_API_KEY"),
                    default_temperature=0.2,
                ),
                ProviderConfig(
                    model="GPT-5.4",
                    base_url=_env("GPT54_ALT5_BASE_URL", "https://ai.dooo.ng/v1"),
                    api_key=_env("GPT54_ALT5_API_KEY"),
                    default_temperature=0.2,
                ),
            )
            if item.base_url and item.api_key
        ),
    }
    return providers, backups


def _apply_external_overrides(
    external_cfg: dict,
    default_request_model: str,
    fallback_models: tuple[str, ...],
    default_rate_limit_per_minute: int,
    provider_configs: dict[str, ProviderConfig],
    provider_backups: dict[str, tuple[ProviderConfig, ...]],
) -> tuple[str, tuple[str, ...], int, dict[str, ProviderConfig], dict[str, tuple[ProviderConfig, ...]]]:
    external_default = str(external_cfg.get("default_request_model") or "").strip()
    if external_default:
        default_request_model = external_default

    external_fallback = external_cfg.get("fallback_models")
    if isinstance(external_fallback, list):
        parsed = tuple(str(x).strip() for x in external_fallback if str(x).strip())
        if parsed:
            fallback_models = parsed
    default_rate_limit_per_minute = max(1, _safe_int(external_cfg.get("default_rate_limit_per_minute", 10), 10))

    providers = external_cfg.get("providers")
    if isinstance(providers, dict):
        for model_key_raw, entries in providers.items():
            model_key = _normalize_model_key(str(model_key_raw))
            if not model_key:
                continue
            if isinstance(entries, dict):
                entry_list = [entries]
            elif isinstance(entries, list):
                entry_list = entries
            else:
                continue

            parsed: list[ProviderConfig] = []
            for item in entry_list:
                if not isinstance(item, dict):
                    continue
                enabled = item.get("enabled", True)
                status = str(item.get("status") or item.get("health_status") or "").strip().lower()
                # Skip providers explicitly disabled by health-check script or manual ops.
                if enabled is False or status in {"disabled", "inactive", "down", "unavailable", "unhealthy"}:
                    continue
                base_url = str(item.get("base_url") or "").strip()
                api_key = _resolve_api_key_from_item(item)
                if not base_url or not api_key:
                    continue
                rate_limit_enabled = bool(item.get("rate_limit_enabled", True))
                node_limit = item.get("rate_limit_per_minute", None)
                rate_limit_per_minute = (
                    max(1, _safe_int(node_limit, default_rate_limit_per_minute))
                    if node_limit is not None
                    else default_rate_limit_per_minute
                )
                parsed.append(
                    ProviderConfig(
                        model=str(item.get("model") or model_key_raw).strip() or str(model_key_raw).strip(),
                        base_url=base_url,
                        api_key=api_key,
                        default_temperature=_safe_float(item.get("default_temperature", 0.2), 0.2),
                        rate_limit_enabled=rate_limit_enabled,
                        rate_limit_per_minute=rate_limit_per_minute,
                    )
                )
            if parsed:
                provider_configs[model_key] = parsed[0]
                provider_backups[model_key] = tuple(parsed[1:])
    return default_request_model, fallback_models, default_rate_limit_per_minute, provider_configs, provider_backups


def _refresh_runtime(force: bool = False) -> None:
    global _LAST_RELOAD_AT, _LAST_PROVIDER_FILE, _LAST_PROVIDER_FILE_MTIME
    global DEFAULT_REQUEST_MODEL, DEFAULT_FALLBACK_MODELS, DEFAULT_RATE_LIMIT_PER_MINUTE
    global PROVIDER_CONFIGS, PROVIDER_BACKUP_CONFIGS

    now = time.time()
    provider_file = _resolve_provider_file()
    try:
        mtime = os.path.getmtime(provider_file) if provider_file and os.path.exists(provider_file) else -1.0
    except Exception:
        mtime = -1.0

    with _LOCK:
        same_source = provider_file == _LAST_PROVIDER_FILE and mtime == _LAST_PROVIDER_FILE_MTIME
        if not force and same_source and (now - _LAST_RELOAD_AT) < RELOAD_INTERVAL_SECONDS:
            return

        default_request_model, fallback_models = _build_env_defaults()
        default_rate_limit_per_minute = 10
        provider_configs, provider_backups = _build_env_provider_configs()
        external_cfg = _load_external_provider_config(provider_file)
        default_request_model, fallback_models, default_rate_limit_per_minute, provider_configs, provider_backups = _apply_external_overrides(
            external_cfg,
            default_request_model,
            fallback_models,
            default_rate_limit_per_minute,
            provider_configs,
            provider_backups,
        )

        DEFAULT_REQUEST_MODEL = default_request_model
        DEFAULT_FALLBACK_MODELS = fallback_models
        DEFAULT_RATE_LIMIT_PER_MINUTE = default_rate_limit_per_minute
        PROVIDER_CONFIGS = provider_configs
        PROVIDER_BACKUP_CONFIGS = provider_backups

        _LAST_PROVIDER_FILE = provider_file
        _LAST_PROVIDER_FILE_MTIME = mtime
        _LAST_RELOAD_AT = now


def get_default_request_model() -> str:
    _refresh_runtime()
    return DEFAULT_REQUEST_MODEL


def get_default_fallback_models() -> tuple[str, ...]:
    _refresh_runtime()
    return DEFAULT_FALLBACK_MODELS


def get_default_rate_limit_per_minute() -> int:
    _refresh_runtime()
    return max(1, int(DEFAULT_RATE_LIMIT_PER_MINUTE or 10))


def get_provider_config(model: str) -> ProviderConfig | None:
    _refresh_runtime()
    return PROVIDER_CONFIGS.get(_normalize_model_key(model))


def get_provider_candidates(model: str) -> tuple[ProviderConfig, ...]:
    _refresh_runtime()
    key = _normalize_model_key(model)
    primary = PROVIDER_CONFIGS.get(key)
    if not primary:
        return tuple()
    backups = PROVIDER_BACKUP_CONFIGS.get(key, tuple())
    deduped: list[ProviderConfig] = []
    seen: set[tuple[str, str, str]] = set()
    for item in (primary, *backups):
        signature = (item.model, item.base_url.rstrip("/"), item.api_key)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(item)
    return tuple(deduped)


def reload_provider_runtime() -> None:
    _refresh_runtime(force=True)


# load once at import so old callers reading module globals still get usable defaults
_refresh_runtime(force=True)
