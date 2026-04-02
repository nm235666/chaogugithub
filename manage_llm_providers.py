#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from typing import Any

DEFAULT_PATH = "/home/zanbo/zanbotest/config/llm_providers.json"


def resolve_path(path_arg: str) -> str:
    path = (path_arg or "").strip() or os.getenv("LLM_PROVIDER_CONFIG_FILE", DEFAULT_PATH).strip()
    if not path:
        path = DEFAULT_PATH
    return path


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def default_payload() -> dict[str, Any]:
    return {
        "default_request_model": "GPT-5.4",
        "fallback_models": ["GPT-5.4", "kimi-k2.5", "deepseek-chat"],
        "default_rate_limit_per_minute": 10,
        "providers": {},
    }


def load_payload(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return default_payload()
    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if isinstance(data, dict):
            merged = default_payload()
            merged.update(data)
            if not isinstance(merged.get("providers"), dict):
                merged["providers"] = {}
            return merged
    except Exception:
        pass
    return default_payload()


def save_payload(path: str, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def normalize_key(model: str) -> str:
    return (model or "").strip().lower()


def ensure_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    return []


def cmd_list(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = load_payload(path)
    print(f"配置文件: {path}")
    print(f"default_request_model: {payload.get('default_request_model', '')}")
    print("fallback_models:", ",".join(payload.get("fallback_models", [])))
    providers = payload.get("providers", {})
    if not providers:
        print("providers: (empty)")
        return 0
    for model_key, raw in providers.items():
        rows = ensure_list(raw)
        print(f"\n[{model_key}]")
        for idx, row in enumerate(rows, start=1):
            model_name = row.get("model", model_key)
            base_url = row.get("base_url", "")
            api_key = str(row.get("api_key", "")).strip()
            api_key_env = str(row.get("api_key_env", "")).strip()
            has_key = bool(api_key) or bool(api_key_env)
            temp = row.get("default_temperature", "")
            key_source = f"env:{api_key_env}" if api_key_env else ("inline" if api_key else "empty")
            print(f"  {idx}. model={model_name} base_url={base_url} api_key={'set' if has_key else 'empty'} source={key_source} temp={temp}")
    return 0


def cmd_add(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = load_payload(path)
    key = normalize_key(args.model)
    if not key:
        raise SystemExit("model 不能为空")
    if not args.base_url.strip() or (not args.api_key.strip() and not args.api_key_env.strip()):
        raise SystemExit("base_url 不能为空，且 api_key/api_key_env 至少一个不为空")
    providers = payload.setdefault("providers", {})
    rows = ensure_list(providers.get(key))
    rows.append(
        {
            "model": args.model.strip(),
            "base_url": args.base_url.strip(),
            "api_key": args.api_key.strip(),
            "api_key_env": args.api_key_env.strip(),
            "default_temperature": args.temperature,
            "status": "active",
            "rate_limit_enabled": True,
            "rate_limit_per_minute": int(payload.get("default_rate_limit_per_minute") or 10),
        }
    )
    providers[key] = rows
    save_payload(path, payload)
    print(f"已新增 {key} 提供商，当前数量: {len(rows)}")
    return 0


def cmd_update(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = load_payload(path)
    key = normalize_key(args.model)
    providers = payload.setdefault("providers", {})
    rows = ensure_list(providers.get(key))
    if not rows:
        raise SystemExit(f"未找到模型: {key}")
    idx = max(args.index, 1) - 1
    if idx >= len(rows):
        raise SystemExit(f"index 超出范围: {args.index}, 当前 {len(rows)}")
    row = dict(rows[idx])
    if args.base_url is not None:
        row["base_url"] = args.base_url.strip()
    if args.api_key is not None:
        row["api_key"] = args.api_key.strip()
    if args.api_key_env is not None:
        row["api_key_env"] = args.api_key_env.strip()
    if args.temperature is not None:
        row["default_temperature"] = args.temperature
    if args.name is not None:
        row["model"] = args.name.strip()
    rows[idx] = row
    providers[key] = rows
    save_payload(path, payload)
    print(f"已更新 {key} 第 {args.index} 条")
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = load_payload(path)
    key = normalize_key(args.model)
    providers = payload.setdefault("providers", {})
    rows = ensure_list(providers.get(key))
    if not rows:
        raise SystemExit(f"未找到模型: {key}")
    idx = max(args.index, 1) - 1
    if idx >= len(rows):
        raise SystemExit(f"index 超出范围: {args.index}, 当前 {len(rows)}")
    removed = rows.pop(idx)
    if rows:
        providers[key] = rows
    else:
        providers.pop(key, None)
    save_payload(path, payload)
    print(f"已删除 {key} 第 {args.index} 条: {removed.get('base_url', '')}")
    return 0


def cmd_set_default(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = load_payload(path)
    payload["default_request_model"] = args.model.strip()
    save_payload(path, payload)
    print(f"default_request_model 已更新为: {payload['default_request_model']}")
    return 0


def cmd_set_fallback(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = load_payload(path)
    models = [x.strip() for x in args.models.split(",") if x.strip()]
    if not models:
        raise SystemExit("models 不能为空")
    payload["fallback_models"] = models
    save_payload(path, payload)
    print("fallback_models 已更新为:", ",".join(models))
    return 0


def cmd_init_from_env(args: argparse.Namespace) -> int:
    path = resolve_path(args.file)
    payload = default_payload()
    payload["default_request_model"] = os.getenv("LLM_DEFAULT_REQUEST_MODEL", "auto").strip() or "auto"
    env_fb = os.getenv("LLM_FALLBACK_MODELS", "GPT-5.4,kimi-k2.5,deepseek-chat")
    payload["fallback_models"] = [x.strip() for x in env_fb.split(",") if x.strip()]

    providers: dict[str, list[dict[str, Any]]] = {}

    def add(model_key: str, model_name: str, base_url_env: str, api_key_env: str, temp: float) -> None:
        base_url = os.getenv(base_url_env, "").strip()
        api_key = os.getenv(api_key_env, "").strip()
        if not base_url or not api_key:
            return
        providers.setdefault(model_key, []).append(
            {
                "model": model_name,
                "base_url": base_url,
                "api_key": api_key,
                "default_temperature": temp,
            }
        )

    add("gpt-5.4", "GPT-5.4", "GPT54_BASE_URL", "GPT54_API_KEY", 0.2)
    add("gpt-5.4", "GPT-5.4", "GPT54_ALT2_BASE_URL", "GPT54_ALT2_API_KEY", 0.2)
    add("gpt-5.4", "GPT-5.4", "GPT54_ALT3_BASE_URL", "GPT54_ALT3_API_KEY", 0.2)
    add("gpt-5.4", "GPT-5.4", "GPT54_ALT4_BASE_URL", "GPT54_ALT4_API_KEY", 0.2)
    add("gpt-5.4", "GPT-5.4", "GPT54_ALT5_BASE_URL", "GPT54_ALT5_API_KEY", 0.2)
    add("kimi-k2.5", "kimi-k2.5", "KIMI_BASE_URL", "KIMI_API_KEY", 1.0)
    add("deepseek-chat", "deepseek-chat", "DEEPSEEK_BASE_URL", "DEEPSEEK_API_KEY", 0.2)

    payload["providers"] = providers
    save_payload(path, payload)
    print(f"已根据当前环境变量生成: {path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="LLM 提供商配置管理（增删改查）")
    p.add_argument("--file", default="", help=f"配置文件路径，默认 {DEFAULT_PATH}")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="查看当前配置")

    p_add = sub.add_parser("add", help="新增一个提供商节点")
    p_add.add_argument("--model", required=True, help="模型键，例如 gpt-5.4")
    p_add.add_argument("--base-url", required=True)
    p_add.add_argument("--api-key", default="")
    p_add.add_argument("--api-key-env", default="")
    p_add.add_argument("--temperature", type=float, default=0.2)

    p_upd = sub.add_parser("update", help="更新指定节点")
    p_upd.add_argument("--model", required=True)
    p_upd.add_argument("--index", type=int, default=1, help="从 1 开始")
    p_upd.add_argument("--base-url")
    p_upd.add_argument("--api-key")
    p_upd.add_argument("--api-key-env")
    p_upd.add_argument("--temperature", type=float)
    p_upd.add_argument("--name", help="模型名字段，例如 GPT-5.4")

    p_del = sub.add_parser("remove", help="删除指定节点")
    p_del.add_argument("--model", required=True)
    p_del.add_argument("--index", type=int, default=1, help="从 1 开始")

    p_def = sub.add_parser("set-default", help="设置默认请求模型")
    p_def.add_argument("--model", required=True)

    p_fb = sub.add_parser("set-fallback", help="设置 fallback 顺序")
    p_fb.add_argument("--models", required=True, help='逗号分隔，如 "GPT-5.4,kimi-k2.5,deepseek-chat"')
    sub.add_parser("init-from-env", help="按当前环境变量初始化配置文件")

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "list":
        return cmd_list(args)
    if args.command == "add":
        return cmd_add(args)
    if args.command == "update":
        return cmd_update(args)
    if args.command == "remove":
        return cmd_remove(args)
    if args.command == "set-default":
        return cmd_set_default(args)
    if args.command == "set-fallback":
        return cmd_set_fallback(args)
    if args.command == "init-from-env":
        return cmd_init_from_env(args)
    raise SystemExit(f"不支持的命令: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
