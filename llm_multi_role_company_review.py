#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import db_compat as sqlite3
import sys
from pathlib import Path

from llm_gateway import chat_completion_with_fallback, normalize_model_name, normalize_temperature_for_model

DEFAULT_ROLES = [
    "宏观经济分析师",
    "股票分析师",
    "国际资本分析师",
    "汇率分析师",
]

ROLE_PROFILES = {
    "宏观经济分析师": {
        "focus": "经济周期、增长与通胀、政策方向、利率与信用环境",
        "framework": "总量-政策-传导链条（宏观变量 -> 行业/资产定价）",
        "indicators": ["GDP/PMI趋势", "通胀与实际利率", "信用扩张/社融", "政策预期变化"],
        "risk_bias": "偏重宏观拐点与政策误判风险",
    },
    "股票分析师": {
        "focus": "价格趋势、量价结构、估值与交易拥挤度",
        "framework": "趋势-动量-波动-成交量联合判断",
        "indicators": ["MA结构", "涨跌幅/回撤", "成交量变化", "波动率", "关键支撑阻力"],
        "risk_bias": "偏重交易层面的失真与假突破风险",
    },
    "国际资本分析师": {
        "focus": "跨境资金流、风险偏好、全球资产配置偏移",
        "framework": "全球流动性-风险偏好-资金流向三段式",
        "indicators": ["北向/南向资金行为", "美债收益率变化", "全球风险资产相关性", "地缘风险溢价"],
        "risk_bias": "偏重外部冲击与资金风格切换风险",
    },
    "汇率分析师": {
        "focus": "汇率方向、利差变化、汇率对盈利与估值的影响",
        "framework": "利差-汇率-资产定价传导",
        "indicators": ["美元指数趋势", "中美利差", "人民币汇率波动", "汇率敏感行业影响"],
        "risk_bias": "偏重汇率波动放大财务与估值波动的风险",
    },
    "行业分析师": {
        "focus": "行业景气、供需结构、竞争格局与政策监管",
        "framework": "景气周期-竞争格局-盈利能力",
        "indicators": ["行业增速", "价格/库存", "龙头份额变化", "监管与政策红利"],
        "risk_bias": "偏重行业β变化与景气反转风险",
    },
    "风险控制官": {
        "focus": "组合回撤、尾部风险、情景压力测试",
        "framework": "仓位-波动-回撤约束",
        "indicators": ["最大回撤", "波动率阈值", "流动性风险", "事件冲击情景"],
        "risk_bias": "偏重先活下来再优化收益",
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
    parser.add_argument(
        "--roles",
        default=",".join(DEFAULT_ROLES),
        help="角色列表，逗号分隔",
    )
    parser.add_argument(
        "--roles-config",
        default="",
        help="外部角色设定JSON文件路径（可覆盖/新增角色设定）",
    )
    parser.add_argument(
        "--model",
        default="auto",
        help="模型名；默认 auto 自动路由并降级",
    )
    parser.add_argument("--temperature", type=float, default=0.2, help="采样温度")
    return parser.parse_args()

def resolve_company(conn: sqlite3.Connection, company: str, ts_code: str) -> tuple[str, str]:
    if ts_code:
        code = ts_code.strip().upper()
        row = conn.execute(
            "SELECT ts_code, name FROM stock_codes WHERE ts_code = ?",
            (code,),
        ).fetchone()
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

    row2 = conn.execute(
        """
        SELECT ts_code, name
        FROM stock_codes
        WHERE name LIKE ?
        ORDER BY CASE list_status WHEN 'L' THEN 0 ELSE 1 END, ts_code
        LIMIT 1
        """,
        (f"%{name}%",),
    ).fetchone()
    if not row2:
        raise ValueError(f"未找到公司: {name}")
    return row2[0], row2[1]

def load_company_context(db_path: str, company: str, ts_code: str, lookback: int) -> dict:
    backend_dir = Path(__file__).resolve().parent / "backend"
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    import server as backend_server  # type: ignore

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        real_ts_code, real_name = resolve_company(conn, company, ts_code)
    finally:
        conn.close()

    backend_server.DB_PATH = Path(db_path).resolve()
    return backend_server.build_multi_role_context(real_ts_code, lookback)


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


def call_llm(model: str, temperature: float, prompt: str):
    return chat_completion_with_fallback(
        model=model,
        temperature=temperature,
        timeout_s=180,
        max_retries=3,
        messages=[
            {
                "role": "system",
                "content": "你是专业投研团队的总协调人，擅长多角色观点整合，表达清晰、客观、可执行。",
            },
            {"role": "user", "content": prompt},
        ],
    )


def build_prompt(context: dict, roles: list[str], role_profiles: dict) -> str:
    role_lines = "\n".join([f"- {r}" for r in roles])
    role_specs = []
    for r in roles:
        spec = role_profiles.get(r, {})
        role_specs.append(
            {
                "role": r,
                "focus": spec.get("focus", "围绕该角色的标准职责进行分析"),
                "framework": spec.get("framework", "使用该角色常用分析框架"),
                "indicators": spec.get("indicators", []),
                "risk_bias": spec.get("risk_bias", "识别与该角色相关的核心风险"),
            }
        )
    prompt = (
        "请你以“投研委员会会议纪要”的形式，基于下面数据进行多角色分析。\n\n"
        f"参与角色：\n{role_lines}\n\n"
        "每个角色必须严格按其角色设定发言，不得混淆口径。\n"
        "请综合使用公司画像、价格行为、财务、估值、个股/市场资金流、公司事件、宏观流动性、汇率、利率利差、公司治理、风险情景等信息。\n"
        "如果某部分数据为空或不足，请明确指出“该维度数据暂缺/不足”，不要假装已看到数据。\n"
        "分析时优先引用数据里的最新日期、最新值、变化方向和分位水平，避免空泛表述。\n"
        "输出要求：\n"
        "1) 每个角色单独一节，给出观点、核心依据、主要风险、关注指标。\n"
        "2) 角色观点可以有分歧，但要明确分歧点。\n"
        "3) 最后给出“综合结论”：短期(5-20交易日)、中期(1-3个月)两个层面的概率判断。\n"
        "4) 给出“行动清单”：继续观察/择时/风控阈值（价格、波动、量能、估值、资金流、汇率或利率信号）。\n"
        "5) 若发现基本面、估值、资金流、宏观或治理之间存在冲突，请单列“关键分歧”。\n"
        "6) 必须附上非投资建议免责声明。\n\n"
        f"角色设定(JSON)：\n{json.dumps(role_specs, ensure_ascii=False)}\n\n"
        f"输入数据(JSON)：\n{json.dumps(context, ensure_ascii=False)}"
    )
    return prompt


def main() -> int:
    args = parse_args()
    args.model = normalize_model_name(args.model)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    if not roles:
        roles = DEFAULT_ROLES
    role_profiles = load_role_profiles(args.roles_config)

    context = load_company_context(
        db_path=args.db_path,
        company=args.company,
        ts_code=args.ts_code,
        lookback=args.lookback,
    )
    ts_code = context["company_profile"]["ts_code"]
    name = context["company_profile"]["name"]

    prompt = build_prompt(context, roles, role_profiles)

    print(f"公司: {name} ({ts_code})")
    print(f"请求模型: {args.model}")
    print(f"角色: {', '.join(roles)}")
    print("多角色分析中...\n")

    result = call_llm(model=args.model, temperature=args.temperature, prompt=prompt)
    print(f"实际模型: {result.used_model}\n")
    print(result.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
