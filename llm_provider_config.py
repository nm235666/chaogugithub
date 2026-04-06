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


@dataclass(frozen=True)
class EmbeddingProfile:
    scene: str
    model: str
    fallback_models: tuple[str, ...]
    dimensions: int = 1536
    batch_size: int = 8
    timeout_seconds: int = 25
    max_retries: int = 1


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


def _safe_bool(value, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled", "active"}:
        return True
    if text in {"0", "false", "no", "off", "disabled", "inactive"}:
        return False
    return default


def _resolve_api_key_from_item(item: dict) -> str:
    api_key = str(item.get("api_key") or "").strip()
    api_key_env = str(item.get("api_key_env") or "").strip()
    if api_key.startswith("env:"):
        env_name = api_key[4:].strip()
        if env_name:
            return _env(env_name, "")
    if api_key_env:
        env_key = _env(api_key_env, "")
        if env_key:
            return env_key
        # Keep inline fallback for safer migration to env-based dedicated channels.
        return api_key
    return api_key


def _resolve_base_url_from_item(item: dict) -> str:
    base_url = str(item.get("base_url") or "").strip()
    base_url_env = str(item.get("base_url_env") or "").strip()
    if base_url_env:
        env_url = _env(base_url_env, "")
        if env_url:
            return env_url
        # Keep inline fallback for safer migration to env-based dedicated channels.
        return base_url
    return base_url


DEFAULT_PROVIDER_FILE = "/home/zanbo/zanbotest/config/llm_providers.json"
RELOAD_INTERVAL_SECONDS = 2.0

_LOCK = threading.RLock()
_LAST_RELOAD_AT = 0.0
_LAST_PROVIDER_FILE = ""
_LAST_PROVIDER_FILE_MTIME = -1.0
_LAST_EMPTY_OVERRIDE_KEYS: set[str] = set()

DEFAULT_REQUEST_MODEL = "auto"
# Runtime source of truth is config/llm_providers.json.
# These defaults are only used as a cold-start fallback when config file/env is missing.
DEFAULT_FALLBACK_MODELS: tuple[str, ...] = ("GPT-5.4", "kimi-k2.5", "deepseek-chat")
DEFAULT_RATE_LIMIT_PER_MINUTE = 10
PROVIDER_CONFIGS: dict[str, ProviderConfig] = {}
PROVIDER_BACKUP_CONFIGS: dict[str, tuple[ProviderConfig, ...]] = {}
EMBEDDING_PROFILES: dict[str, EmbeddingProfile] = {}


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
        "gpt-5.4-multi-role": ProviderConfig(
            model="GPT-5.4",
            base_url=_env("GPT54_MULTI_ROLE_BASE_URL"),
            api_key=_env("GPT54_MULTI_ROLE_API_KEY"),
            default_temperature=0.2,
        ),
        "gpt-5.4-trend": ProviderConfig(
            model="GPT-5.4",
            base_url=_env("GPT54_TREND_BASE_URL"),
            api_key=_env("GPT54_TREND_API_KEY"),
            default_temperature=0.2,
        ),
        "gpt-5.4-daily-summary": ProviderConfig(
            model="GPT-5.4",
            base_url=_env("GPT54_DAILY_SUMMARY_BASE_URL"),
            api_key=_env("GPT54_DAILY_SUMMARY_API_KEY"),
            default_temperature=0.2,
        ),
        "zhipu-news": ProviderConfig(
            model="glm-4-plus",
            base_url=_env("ZHIPU_NEWS_BASE_URL", "https://open.bigmodel.cn/api/paas/v4"),
            api_key=_env("ZHIPU_NEWS_API_KEY"),
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
    # Legacy hard-coded GPT alt nodes are intentionally removed.
    # Backup nodes should be declared in config/llm_providers.json provider arrays.
    backups: dict[str, tuple[ProviderConfig, ...]] = {}
    return providers, backups


def _build_default_embedding_profiles() -> dict[str, EmbeddingProfile]:
    return {
        "news": EmbeddingProfile(
            scene="news",
            model="gpt-5.4",
            fallback_models=("kimi-k2.5", "deepseek-chat"),
            dimensions=1536,
            batch_size=8,
            timeout_seconds=25,
            max_retries=1,
        ),
        "report": EmbeddingProfile(
            scene="report",
            model="gpt-5.4",
            fallback_models=("deepseek-chat",),
            dimensions=1536,
            batch_size=6,
            timeout_seconds=25,
            max_retries=1,
        ),
        "chatroom": EmbeddingProfile(
            scene="chatroom",
            model="gpt-5.4",
            fallback_models=("kimi-k2.5",),
            dimensions=1536,
            batch_size=10,
            timeout_seconds=20,
            max_retries=1,
        ),
    }


def _parse_embedding_profiles(raw: object, base_profiles: dict[str, EmbeddingProfile]) -> dict[str, EmbeddingProfile]:
    out = dict(base_profiles)
    if not isinstance(raw, dict):
        return out
    for scene_raw, payload in raw.items():
        scene = str(scene_raw or "").strip().lower()
        if scene not in {"news", "report", "chatroom"}:
            continue
        if not isinstance(payload, dict):
            continue
        model = str(payload.get("model") or out.get(scene, EmbeddingProfile(scene=scene, model="", fallback_models=())).model or "").strip()
        fallback_models_raw = payload.get("fallback_models")
        if isinstance(fallback_models_raw, list):
            fallback_models = tuple(str(x or "").strip() for x in fallback_models_raw if str(x or "").strip())
        else:
            fallback_models = tuple(out.get(scene, EmbeddingProfile(scene=scene, model="", fallback_models=())).fallback_models)
        dimensions = max(8, _safe_int(payload.get("dimensions", out.get(scene, EmbeddingProfile(scene=scene, model="", fallback_models=())).dimensions), 1536))
        batch_size = max(1, _safe_int(payload.get("batch_size", out.get(scene, EmbeddingProfile(scene=scene, model="", fallback_models=())).batch_size), 8))
        timeout_seconds = max(5, _safe_int(payload.get("timeout_seconds", out.get(scene, EmbeddingProfile(scene=scene, model="", fallback_models=())).timeout_seconds), 25))
        max_retries = max(0, _safe_int(payload.get("max_retries", out.get(scene, EmbeddingProfile(scene=scene, model="", fallback_models=())).max_retries), 1))
        if not model:
            continue
        out[scene] = EmbeddingProfile(
            scene=scene,
            model=model,
            fallback_models=fallback_models,
            dimensions=dimensions,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
    return out


def _apply_external_overrides(
    external_cfg: dict,
    default_request_model: str,
    fallback_models: tuple[str, ...],
    default_rate_limit_per_minute: int,
    provider_configs: dict[str, ProviderConfig],
    provider_backups: dict[str, tuple[ProviderConfig, ...]],
    embedding_profiles: dict[str, EmbeddingProfile],
) -> tuple[str, tuple[str, ...], int, dict[str, ProviderConfig], dict[str, tuple[ProviderConfig, ...]], dict[str, EmbeddingProfile]]:
    global _LAST_EMPTY_OVERRIDE_KEYS
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
    empty_override_keys: set[str] = set()
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
                enabled = _safe_bool(item.get("enabled", True), True)
                status = str(item.get("status") or item.get("health_status") or "").strip().lower()
                # Skip providers explicitly disabled by health-check script or manual ops.
                if (not enabled) or status in {"disabled", "inactive", "down", "unavailable", "unhealthy"}:
                    continue
                base_url = _resolve_base_url_from_item(item)
                api_key = _resolve_api_key_from_item(item)
                if not base_url or not api_key:
                    continue
                rate_limit_enabled = _safe_bool(item.get("rate_limit_enabled", True), True)
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
            else:
                # External config explicitly declared this key but provided no active node.
                # In that case, do not silently fall back to env defaults.
                provider_configs.pop(model_key, None)
                provider_backups.pop(model_key, None)
                empty_override_keys.add(model_key)
    if empty_override_keys != _LAST_EMPTY_OVERRIDE_KEYS:
        for model_key in sorted(empty_override_keys):
            print(
                f"[llm-provider-config] provider_key={model_key} has no active nodes in external config; runtime candidates disabled",
                flush=True,
            )
        _LAST_EMPTY_OVERRIDE_KEYS = set(empty_override_keys)
    embedding_profiles = _parse_embedding_profiles(external_cfg.get("embedding_profiles"), embedding_profiles)
    return default_request_model, fallback_models, default_rate_limit_per_minute, provider_configs, provider_backups, embedding_profiles


def _refresh_runtime(force: bool = False) -> None:
    global _LAST_RELOAD_AT, _LAST_PROVIDER_FILE, _LAST_PROVIDER_FILE_MTIME
    global DEFAULT_REQUEST_MODEL, DEFAULT_FALLBACK_MODELS, DEFAULT_RATE_LIMIT_PER_MINUTE
    global PROVIDER_CONFIGS, PROVIDER_BACKUP_CONFIGS, EMBEDDING_PROFILES

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
        embedding_profiles = _build_default_embedding_profiles()
        external_cfg = _load_external_provider_config(provider_file)
        default_request_model, fallback_models, default_rate_limit_per_minute, provider_configs, provider_backups, embedding_profiles = _apply_external_overrides(
            external_cfg,
            default_request_model,
            fallback_models,
            default_rate_limit_per_minute,
            provider_configs,
            provider_backups,
            embedding_profiles,
        )

        DEFAULT_REQUEST_MODEL = default_request_model
        DEFAULT_FALLBACK_MODELS = fallback_models
        DEFAULT_RATE_LIMIT_PER_MINUTE = default_rate_limit_per_minute
        PROVIDER_CONFIGS = provider_configs
        PROVIDER_BACKUP_CONFIGS = provider_backups
        EMBEDDING_PROFILES = embedding_profiles

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


def is_provider_declared_but_unavailable(model: str) -> bool:
    _refresh_runtime()
    key = _normalize_model_key(model)
    return key in _LAST_EMPTY_OVERRIDE_KEYS


def reload_provider_runtime() -> None:
    _refresh_runtime(force=True)


def get_embedding_profile(scene: str) -> dict:
    _refresh_runtime()
    key = str(scene or "").strip().lower()
    profile = EMBEDDING_PROFILES.get(key) or EMBEDDING_PROFILES.get("news")
    if not profile:
        return {
            "scene": key or "news",
            "model": "gpt-5.4",
            "fallback_models": ["kimi-k2.5", "deepseek-chat"],
            "dimensions": 1536,
            "batch_size": 8,
            "timeout_seconds": 25,
            "max_retries": 1,
        }
    return {
        "scene": profile.scene,
        "model": profile.model,
        "fallback_models": list(profile.fallback_models),
        "dimensions": int(profile.dimensions),
        "batch_size": int(profile.batch_size),
        "timeout_seconds": int(profile.timeout_seconds),
        "max_retries": int(profile.max_retries),
    }


def get_embedding_profiles() -> dict[str, dict]:
    _refresh_runtime()
    return {scene: get_embedding_profile(scene) for scene in ("news", "report", "chatroom")}


# load once at import so old callers reading module globals still get usable defaults
_refresh_runtime(force=True)
