#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import backend.server as backend_server
import db_compat as sqlite3

from llm_gateway import normalize_model_name, normalize_temperature_for_model
from services.agent_service import run_multi_role_analysis

DEFAULT_ROLES = [
    "宏观经济分析师",
    "股票分析师",
    "国际资本分析师",
    "汇率分析师",
]

ROLE_PROFILES = {
    "宏观经济分析师": {
        "focus": "经济周期、增长与通胀、政策方向、利率与信用环境",
    },
    "股票分析师": {
        "focus": "价格趋势、量价结构、估值与交易拥挤度",
    },
    "国际资本分析师": {
        "focus": "跨境资金流、风险偏好、全球资产配置偏移",
    },
    "汇率分析师": {
        "focus": "汇率方向、利差变化、汇率对盈利与估值的影响",
    },
    "行业分析师": {
        "focus": "行业景气、供需结构、竞争格局与政策监管",
    },
    "风险控制官": {
        "focus": "组合回撤、尾部风险、情景压力测试",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="多角色视角分析一家公司")
    parser.add_argument("--company", default="", help="公司名（如 平安银行）")
    parser.add_argument("--ts-code", default="", help="股票代码（如 000001.SZ）")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--lookback", type=int, default=120, help="日线回看交易日数量")
    parser.add_argument("--roles", default=",".join(DEFAULT_ROLES), help="角色列表，逗号分隔")
    parser.add_argument("--roles-config", default="", help="外部角色设定JSON文件路径（可覆盖/新增角色设定）")
    parser.add_argument("--model", default="auto", help="模型名；默认 auto 自动路由并降级")
    parser.add_argument("--temperature", type=float, default=0.2, help="采样温度")
    return parser.parse_args()


def resolve_company(conn: sqlite3.Connection, company: str, ts_code: str) -> tuple[str, str]:
    if ts_code:
        code = ts_code.strip().upper()
        row = conn.execute("SELECT ts_code, name FROM stock_codes WHERE ts_code = ?", (code,)).fetchone()
        if not row:
            raise ValueError(f"未找到股票代码: {code}")
        return row[0], row[1]

    name = company.strip()
    if not name:
        raise ValueError("请至少传入 --company 或 --ts-code")

    row = conn.execute(
        """
        SELECT ts_code, name
        FROM stock_codes
        WHERE name = ?
        ORDER BY CASE list_status WHEN 'L' THEN 0 ELSE 1 END
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    if row:
        return row[0], row[1]

    row = conn.execute(
        """
        SELECT ts_code, name
        FROM stock_codes
        WHERE name LIKE ?
        ORDER BY CASE list_status WHEN 'L' THEN 0 ELSE 1 END, ts_code
        LIMIT 1
        """,
        (f"%{name}%",),
    ).fetchone()
    if not row:
        raise ValueError(f"未找到公司: {name}")
    return row[0], row[1]


def load_role_profiles(roles_config_path: str) -> dict:
    profiles = dict(ROLE_PROFILES)
    p = roles_config_path.strip()
    if not p:
        return profiles
    cfg_path = Path(p).resolve()
    if not cfg_path.exists():
        raise ValueError(f"roles config 不存在: {cfg_path}")
    obj = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("roles config 必须是对象(JSON Object)")
    for role, spec in obj.items():
        if isinstance(spec, dict):
            profiles[str(role)] = spec
    return profiles


def main() -> int:
    args = parse_args()
    args.model = normalize_model_name(args.model)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    roles = [r.strip() for r in args.roles.split(",") if r.strip()] or list(DEFAULT_ROLES)
    role_profiles = load_role_profiles(args.roles_config)
    missing = [r for r in roles if r not in role_profiles]
    if missing:
        print(f"警告：以下角色没有显式配置，将按默认职责分析：{', '.join(missing)}", file=sys.stderr)

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        ts_code, name = resolve_company(conn, args.company, args.ts_code)
    finally:
        conn.close()

    backend_server.DB_PATH = Path(args.db_path).resolve()
    payload = run_multi_role_analysis(
        backend_server.build_agent_service_deps(),
        ts_code=ts_code,
        lookback=args.lookback,
        roles=roles,
        model=args.model,
        temperature=args.temperature,
    )

    print(f"公司: {name} ({ts_code})")
    print(f"请求模型: {args.model}")
    print(f"角色: {', '.join(roles)}")
    print("多角色分析中...\n")
    print(f"实际模型: {payload.get('used_model') or args.model}\n")
    print(payload.get("analysis_markdown") or payload.get("analysis") or "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
