#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ProbeResult:
    ok: bool
    status_code: int | None
    latency_ms: int
    error: str
    used_base_url: str
    used_model: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="探测 llm_providers.json 中 GPT 节点可用性：可用优先，不可用自动停用(enabled=false)"
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parent / "config" / "llm_providers.json"),
        help="配置文件路径",
    )
    parser.add_argument("--provider-key", default="gpt-5.4", help="provider key，默认 gpt-5.4")
    parser.add_argument("--probe-model", default="gpt-5.4", help="探测请求使用的模型名（默认小写）")
    parser.add_argument(
        "--case-mode",
        default="auto",
        choices=["auto", "lower", "upper", "both", "raw"],
        help="模型名大小写探测模式：auto=自动双探测",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="单节点超时秒数")
    parser.add_argument("--probe-retries", type=int, default=1, help="单节点额外重试次数（默认1，即最多2次）")
    parser.add_argument(
        "--disable-fail-threshold",
        type=int,
        default=2,
        help="连续失败达到该阈值才自动停用节点（默认2）",
    )
    parser.add_argument("--interval", type=float, default=0.0, help="循环探测间隔秒，0=只运行一次")
    parser.add_argument("--keep-unhealthy-enabled", action="store_true", help="仅标记不自动停用")
    return parser.parse_args()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _short(s: str, n: int = 240) -> str:
    s = (s or "").strip().replace("\n", " ")
    return s[:n]


def _strip_v1(base_url: str) -> str:
    b = (base_url or "").rstrip("/")
    if b.lower().endswith("/v1"):
        return b[:-3].rstrip("/")
    return b


def build_base_url_candidates(base_url: str) -> list[str]:
    raw = (base_url or "").strip().rstrip("/")
    if not raw:
        return []
    candidates: list[str] = []
    if raw.lower().endswith("/v1"):
        candidates.extend([raw, _strip_v1(raw)])
    else:
        candidates.extend([f"{raw}/v1", raw])
    dedup: list[str] = []
    seen = set()
    for c in candidates:
        cc = c.rstrip("/")
        if not cc or cc in seen:
            continue
        seen.add(cc)
        dedup.append(cc)
    return dedup


def build_model_candidates(model: str, case_mode: str) -> list[str]:
    raw = (model or "").strip() or "gpt-5.4"
    lower = raw.lower()
    upper = raw.upper()
    title = raw[:1].upper() + raw[1:]
    mode = (case_mode or "auto").lower()
    if mode == "raw":
        candidates = [raw]
    elif mode == "lower":
        candidates = [lower]
    elif mode == "upper":
        candidates = [upper]
    elif mode == "both":
        candidates = [lower, upper, raw]
    else:
        # auto: prioritize lower-case (OpenAI兼容网关常见要求) + upper/title/raw.
        candidates = [lower, upper, title, raw]
    dedup: list[str] = []
    seen = set()
    for c in candidates:
        cc = c.strip()
        if not cc or cc in seen:
            continue
        seen.add(cc)
        dedup.append(cc)
    return dedup


def probe_endpoint_once(base_url: str, api_key: str, model: str, timeout: float) -> ProbeResult:
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "你是什么大模型？只回答我的提问，不要多余的内容。"}],
    }
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "OpenAI/Python",
            "Accept": "application/json",
            "Connection": "close",
        },
        method="POST",
    )
    ctx = ssl.create_default_context()
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            latency_ms = int((time.time() - t0) * 1000)
            if resp.status == 200 and ("choices" in body or body.strip().startswith("{")):
                return ProbeResult(
                    ok=True,
                    status_code=200,
                    latency_ms=latency_ms,
                    error="",
                    used_base_url=base_url.rstrip("/"),
                    used_model=model,
                )
            return ProbeResult(
                ok=False,
                status_code=resp.status,
                latency_ms=latency_ms,
                error=f"unexpected response: {_short(body)}",
                used_base_url=base_url.rstrip("/"),
                used_model=model,
            )
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return ProbeResult(
            ok=False,
            status_code=int(getattr(e, "code", 0) or 0),
            latency_ms=int((time.time() - t0) * 1000),
            error=_short(body or str(e)),
            used_base_url=base_url.rstrip("/"),
            used_model=model,
        )
    except Exception as e:
        return ProbeResult(
            ok=False,
            status_code=None,
            latency_ms=int((time.time() - t0) * 1000),
            error=_short(str(e)),
            used_base_url=base_url.rstrip("/"),
            used_model=model,
        )


def probe_endpoint_autovalidate(
    base_url: str,
    api_key: str,
    probe_model: str,
    case_mode: str,
    timeout: float,
    probe_retries: int,
) -> tuple[ProbeResult, list[dict[str, Any]]]:
    base_candidates = build_base_url_candidates(base_url)
    model_candidates = build_model_candidates(probe_model, case_mode)
    attempts: list[dict[str, Any]] = []
    best_fail: ProbeResult | None = None

    rounds = max(int(probe_retries), 0) + 1
    for round_idx in range(rounds):
        for api_base in base_candidates:
            for model in model_candidates:
                r = probe_endpoint_once(base_url=api_base, api_key=api_key, model=model, timeout=timeout)
                attempts.append(
                    {
                        "round": round_idx + 1,
                        "base_url": api_base,
                        "model": model,
                        "ok": bool(r.ok),
                        "status_code": r.status_code,
                        "latency_ms": r.latency_ms,
                        "error": r.error,
                    }
                )
                if r.ok:
                    return r, attempts
                if best_fail is None:
                    best_fail = r

    if best_fail is None:
        best_fail = ProbeResult(
            ok=False,
            status_code=None,
            latency_ms=0,
            error="no base_url/model candidates",
            used_base_url=base_url.rstrip("/"),
            used_model=probe_model,
        )
    return best_fail, attempts


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("配置文件格式错误：根节点必须是 JSON object")
    return data


def save_config(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def check_once(
    config_path: Path,
    provider_key: str,
    probe_model: str,
    case_mode: str,
    timeout: float,
    probe_retries: int,
    disable_fail_threshold: int,
    keep_unhealthy_enabled: bool,
) -> int:
    data = load_config(config_path)
    providers_obj = data.get("providers")
    if not isinstance(providers_obj, dict):
        raise ValueError("配置文件缺少 providers 对象")
    items = providers_obj.get(provider_key)
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        raise ValueError(f"providers.{provider_key} 必须是数组")

    checked: list[dict[str, Any]] = []
    ts = now_utc_iso()
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        node = dict(item)
        base_url = str(node.get("base_url") or "").strip()
        api_key = str(node.get("api_key") or "").strip()
        model = str(node.get("model") or probe_model).strip() or probe_model

        if not base_url or not api_key:
            node["enabled"] = False if not keep_unhealthy_enabled else bool(node.get("enabled", True))
            node["health_status"] = "unhealthy"
            node["status"] = "disabled" if not keep_unhealthy_enabled else "unhealthy"
            node["last_checked_at"] = ts
            node["last_http_status"] = None
            node["last_latency_ms"] = 0
            node["last_error"] = "missing base_url/api_key"
            node["last_probe_model"] = model
            node["last_probe_base_url"] = base_url
            checked.append(node)
            print(f"[{idx}] DOWN {base_url or '<empty>'} err=missing base_url/api_key")
            continue

        r, attempts = probe_endpoint_autovalidate(
            base_url=base_url,
            api_key=api_key,
            probe_model=model,
            case_mode=case_mode,
            timeout=timeout,
            probe_retries=probe_retries,
        )
        fail_streak = int(node.get("consecutive_failures") or 0)
        node["last_checked_at"] = ts
        node["last_http_status"] = r.status_code
        node["last_latency_ms"] = r.latency_ms
        node["last_error"] = r.error
        node["last_probe_model"] = r.used_model
        node["last_probe_base_url"] = r.used_base_url
        node["last_probe_attempts"] = attempts[-8:]
        if r.ok:
            node["consecutive_failures"] = 0
            node["enabled"] = True
            node["health_status"] = "healthy"
            node["status"] = "active"
            # Write back verified working combo for production callers.
            node["base_url"] = r.used_base_url
            node["model"] = r.used_model
            print(f"[{idx}] OK   {r.used_base_url} model={r.used_model} latency={r.latency_ms}ms")
        else:
            fail_streak += 1
            node["consecutive_failures"] = fail_streak
            if keep_unhealthy_enabled:
                node["enabled"] = bool(node.get("enabled", True))
                node["status"] = "unhealthy"
            elif fail_streak < max(int(disable_fail_threshold), 1):
                node["enabled"] = True
                node["status"] = "degraded"
            else:
                node["enabled"] = False
                node["status"] = "disabled"
            node["health_status"] = "unhealthy"
            print(
                f"[{idx}] DOWN {base_url} status={r.status_code if r.status_code is not None else '-'} "
                f"model={r.used_model} api_base={r.used_base_url} latency={r.latency_ms}ms "
                f"fail_streak={fail_streak} err={r.error}"
            )
        checked.append(node)

    healthy = [x for x in checked if x.get("health_status") == "healthy"]
    unhealthy = [x for x in checked if x.get("health_status") != "healthy"]
    healthy.sort(key=lambda x: int(x.get("last_latency_ms") or 10**9))

    ordered = healthy + unhealthy
    for i, x in enumerate(ordered, start=1):
        x["priority"] = i

    providers_obj[provider_key] = ordered
    save_config(config_path, data)

    print(
        f"[done] provider={provider_key} total={len(ordered)} "
        f"healthy={len(healthy)} unhealthy={len(unhealthy)} "
        f"config={config_path}"
    )
    return 0


def main() -> int:
    args = parse_args()
    path = Path(args.config).resolve()

    if args.interval and args.interval > 0:
        while True:
            try:
                check_once(
                    config_path=path,
                    provider_key=args.provider_key,
                    probe_model=args.probe_model,
                    case_mode=args.case_mode,
                    timeout=args.timeout,
                    probe_retries=args.probe_retries,
                    disable_fail_threshold=args.disable_fail_threshold,
                    keep_unhealthy_enabled=bool(args.keep_unhealthy_enabled),
                )
            except Exception as exc:
                print(f"[error] {exc}")
            time.sleep(args.interval)
        # unreachable
    return check_once(
        config_path=path,
        provider_key=args.provider_key,
        probe_model=args.probe_model,
        case_mode=args.case_mode,
        timeout=args.timeout,
        probe_retries=args.probe_retries,
        disable_fail_threshold=args.disable_fail_threshold,
        keep_unhealthy_enabled=bool(args.keep_unhealthy_enabled),
    )


if __name__ == "__main__":
    raise SystemExit(main())
