#!/usr/bin/env python3
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderConfig:
    model: str
    base_url: str
    api_key: str
    default_temperature: float = 0.2


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


DEFAULT_REQUEST_MODEL = os.getenv("LLM_DEFAULT_REQUEST_MODEL", "auto")
DEFAULT_FALLBACK_MODELS = tuple(
    x.strip()
    for x in os.getenv(
        "LLM_FALLBACK_MODELS",
        "GPT-5.4,kimi-k2.5,deepseek-chat",
    ).split(",")
    if x.strip()
)

PROVIDER_CONFIGS: dict[str, ProviderConfig] = {
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

PROVIDER_BACKUP_CONFIGS: dict[str, tuple[ProviderConfig, ...]] = {
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


def get_provider_candidates(model: str) -> tuple[ProviderConfig, ...]:
    key = (model or "").strip().lower()
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
