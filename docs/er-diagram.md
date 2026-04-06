# Zanbo Quant 数据库 ER 图

> **生成时间**: 2026-04-05  
> **数据库**: PostgreSQL 主库  
> **表数量**: 40+ 张表

---

## 一、ER 图总览（按模块划分）

```mermaid
erDiagram
    %% ==================== 核心股票数据模块 ====================
    stock_codes ||--o{ stock_daily_prices : "1:N"
    stock_codes ||--o{ stock_minline : "1:N"
    stock_codes ||--o{ stock_valuation_daily : "1:N"
    stock_codes ||--o{ stock_financials : "1:N"
    stock_codes ||--o{ stock_events : "1:N"
    stock_codes ||--o{ stock_scores_daily : "1:N"
    stock_codes ||--o{ stock_news_items : "1:N"
    stock_codes ||--o{ company_governance : "1:N"
    stock_codes ||--o{ capital_flow_stock : "1:N"
    stock_codes ||--o{ risk_scenarios : "1:N"
    stock_codes ||--o{ theme_stock_mapping : "1:N"
    stock_codes ||--o{ stock_alias_dictionary : "1:N"
    stock_codes ||--o| investment_signal_tracker : "1:0..1"
    stock_codes ||--o{ investment_signal_daily_snapshots : "1:N"
    
    %% ==================== 用户认证模块 ====================
    app_auth_users ||--o{ app_auth_sessions : "1:N"
    app_auth_users ||--o{ app_auth_usage_daily : "1:N"
    app_auth_users ||--o{ app_auth_email_verifications : "1:N"
    app_auth_users ||--o{ app_auth_password_resets : "1:N"
    app_auth_users ||--o{ app_auth_audit_logs : "1:N"
    app_auth_role_policies ||--o{ app_auth_users : "1:N (role)"
    
    %% ==================== 新闻模块 ====================
    news_feed_items ||--o{ stock_news_items : "源新闻:个股关联"
    news_feed_items ||--o{ news_feed_items_archive : "归档"
    
    %% ==================== 群聊舆情模块 ====================
    chatroom_list_items ||--o{ wechat_chatlog_clean_items : "1:N"
    chatroom_list_items ||--o{ chatroom_investment_analysis : "1:N"
    wechat_chatlog_clean_items ||--o{ chatroom_investment_analysis : "来源消息"
    chatroom_investment_analysis ||--o{ chatroom_stock_candidate_pool : "聚合生成"
    
    %% ==================== 投资信号模块 ====================
    investment_signal_tracker ||--o{ investment_signal_daily_snapshots : "1:N"
    investment_signal_tracker ||--o{ investment_signal_events : "1:N"
    
    %% ==================== 主题热点模块 ====================
    theme_definitions ||--o{ theme_aliases : "1:N"
    theme_definitions ||--o{ theme_hotspot_tracker : "1:N"
    theme_definitions ||--o{ theme_hotspot_evidence : "1:N"
    theme_definitions ||--o{ theme_hotspot_snapshots : "1:N"
    theme_stock_mapping }o--|| theme_definitions : "N:1"
    
    %% ==================== 宏观数据模块 ====================
    macro_series ||--o{ rate_curve_points : "无直接关联"
    fx_daily ||--o{ spread_daily : "衍生计算"
    
    %% ==================== 研究模块 ====================
    stock_codes ||--o{ multi_role_analysis_history : "1:N"
    stock_codes ||--o{ research_reports : "1:N"
    stock_codes ||--o{ quantaalpha_runs : "1:N"
    stock_codes ||--o{ quantaalpha_factor_results : "1:N"
```

---

## 二、核心股票数据模块

### 2.1 股票主表与行情数据

```mermaid
erDiagram
    stock_codes {
        TEXT ts_code PK "股票代码(主键)"
        TEXT symbol "数字代码"
        TEXT name "股票简称"
        TEXT area "所属地区"
        TEXT industry "所属行业"
        TEXT market "所属市场"
        TEXT list_date "上市日期"
        TEXT delist_date "退市日期"
        TEXT list_status "上市状态(L/D/P)"
    }
    
    stock_daily_prices {
        TEXT ts_code PK,FK "股票代码"
        TEXT trade_date PK "交易日期"
        REAL open "开盘价"
        REAL high "最高价"
        REAL low "最低价"
        REAL close "收盘价"
        REAL pre_close "前收盘价"
        REAL change "涨跌额"
        REAL pct_chg "涨跌幅"
        REAL vol "成交量"
        REAL amount "成交额"
    }
    
    stock_minline {
        TEXT ts_code PK,FK "股票代码"
        TEXT trade_date PK "交易日"
        TEXT minute_time PK "分钟时间点"
        REAL price "成交价"
        REAL avg_price "均价"
        REAL volume "该分钟成交量"
        REAL total_volume "累计成交量"
        TEXT source "数据来源"
    }
    
    stock_valuation_daily {
        TEXT ts_code PK,FK "股票代码"
        TEXT trade_date PK "交易日期"
        REAL pe "市盈率"
        REAL pe_ttm "滚动PE"
        REAL pb "市净率"
        REAL ps "市销率"
        REAL ps_ttm "滚动PS"
        REAL dv_ratio "股息率"
        REAL dv_ttm "滚动股息率"
        REAL total_mv "总市值"
        REAL circ_mv "流通市值"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
    }
    
    stock_financials {
        TEXT ts_code PK,FK "股票代码"
        TEXT report_period PK "报告期末日期"
        TEXT report_type "报告类型(年报/季报)"
        TEXT ann_date "公告日期"
        REAL revenue "营业收入"
        REAL op_profit "营业利润"
        REAL net_profit "归母净利润"
        REAL net_profit_excl_nr "扣非净利润"
        REAL roe "净资产收益率"
        REAL gross_margin "毛利率"
        REAL debt_to_assets "资产负债率"
        REAL operating_cf "经营现金流"
        REAL free_cf "自由现金流"
        REAL eps "每股收益"
        REAL bps "每股净资产"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
    }
    
    stock_events {
        INTEGER id PK "主键ID"
        TEXT ts_code FK "股票代码"
        TEXT event_type "事件类型(分红/回购/解禁)"
        TEXT event_date "事件日期"
        TEXT ann_date "公告日期"
        TEXT title "事件标题"
        TEXT detail_json "详细JSON"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
        TEXT event_key "事件唯一键"
    }
    
    stock_scores_daily {
        TEXT score_date PK "评分日期"
        TEXT ts_code PK,FK "股票代码"
        TEXT name "股票简称"
        TEXT symbol "数字代码"
        TEXT market "市场"
        TEXT area "地区"
        TEXT industry "行业"
        TEXT score_grade "评分等级"
        REAL total_score "综合总分"
        REAL trend_score "趋势分"
        REAL financial_score "财务分"
        REAL valuation_score "估值分"
        REAL capital_flow_score "资金流分"
        REAL event_score "事件分"
        REAL news_score "新闻分"
        REAL risk_score "风险分"
        TEXT score_payload_json "评分明细JSON"
        INTEGER industry_rank "行业内排名"
        INTEGER industry_count "行业样本数"
        TEXT industry_score_grade "行业内等级"
        TEXT update_time "更新时间"
    }
    
    company_governance {
        TEXT ts_code PK,FK "股票代码"
        TEXT asof_date PK "数据日期"
        TEXT holder_structure_json "股东结构JSON"
        TEXT board_structure_json "董监高结构JSON"
        TEXT mgmt_change_json "人事变动JSON"
        TEXT incentive_plan_json "激励计划JSON"
        REAL governance_score "治理评分"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
    }
    
    stock_codes ||--o{ stock_daily_prices : "1:N"
    stock_codes ||--o{ stock_minline : "1:N"
    stock_codes ||--o{ stock_valuation_daily : "1:N"
    stock_codes ||--o{ stock_financials : "1:N"
    stock_codes ||--o{ stock_events : "1:N"
    stock_codes ||--o{ stock_scores_daily : "1:N"
    stock_codes ||--o{ company_governance : "1:N"
```

---

## 三、用户认证与权限模块

```mermaid
erDiagram
    app_auth_users {
        INTEGER id PK "用户ID"
        TEXT username UK "用户名"
        TEXT password_hash "密码哈希"
        TEXT display_name "显示名称"
        TEXT email "邮箱"
        INTEGER email_verified "邮箱是否验证"
        TEXT role "角色(admin/pro/limited)"
        TEXT tier "等级"
        TEXT invite_code_used "使用的邀请码"
        INTEGER is_active "是否激活"
        INTEGER failed_login_count "登录失败次数"
        TIMESTAMP locked_until "锁定截止时间"
        TIMESTAMP last_login_at "最后登录时间"
        TIMESTAMP created_at "创建时间"
        TIMESTAMP updated_at "更新时间"
    }
    
    app_auth_sessions {
        INTEGER id PK "会话ID"
        INTEGER user_id FK "用户ID"
        TEXT session_token_hash UK "会话令牌哈希"
        TIMESTAMP expires_at "过期时间"
        TIMESTAMP created_at "创建时间"
        TIMESTAMP last_seen_at "最后访问时间"
    }
    
    app_auth_role_policies {
        INTEGER id PK "策略ID"
        TEXT role UK "角色名"
        TEXT permissions_json "权限列表JSON"
        INTEGER trend_daily_limit "走势分析日限额"
        INTEGER multi_role_daily_limit "多角色分析日限额"
        TIMESTAMP created_at "创建时间"
        TIMESTAMP updated_at "更新时间"
    }
    
    app_auth_usage_daily {
        INTEGER id PK "记录ID"
        INTEGER user_id FK "用户ID"
        TEXT usage_date "使用日期"
        INTEGER trend_count "走势分析次数"
        INTEGER multi_role_count "多角色分析次数"
        TIMESTAMP created_at "创建时间"
        TIMESTAMP updated_at "更新时间"
    }
    
    app_auth_invites {
        INTEGER id PK "邀请码ID"
        TEXT invite_code UK "邀请码"
        INTEGER max_uses "最大使用次数"
        INTEGER used_count "已使用次数"
        TIMESTAMP expires_at "过期时间"
        INTEGER is_active "是否有效"
        TEXT created_by "创建者"
        TIMESTAMP created_at "创建时间"
        TIMESTAMP updated_at "更新时间"
    }
    
    app_auth_email_verifications {
        INTEGER id PK "验证ID"
        INTEGER user_id FK "用户ID"
        TEXT email "邮箱地址"
        TEXT verify_code "验证码"
        TIMESTAMP expires_at "过期时间"
        TIMESTAMP used_at "使用时间"
        TIMESTAMP created_at "创建时间"
    }
    
    app_auth_password_resets {
        INTEGER id PK "重置ID"
        INTEGER user_id FK "用户ID"
        TEXT username "用户名"
        TEXT reset_code "重置码"
        TIMESTAMP expires_at "过期时间"
        TIMESTAMP used_at "使用时间"
        TIMESTAMP created_at "创建时间"
    }
    
    app_auth_audit_logs {
        INTEGER id PK "日志ID"
        TEXT event_type "事件类型"
        TEXT username "用户名"
        INTEGER user_id "用户ID"
        TEXT result "结果(ok/fail)"
        TEXT detail "详细内容"
        TEXT ip "IP地址"
        TEXT user_agent "用户代理"
        TIMESTAMP created_at "创建时间"
    }
    
    app_auth_users ||--o{ app_auth_sessions : "1:N"
    app_auth_users ||--o{ app_auth_usage_daily : "1:N"
    app_auth_users ||--o{ app_auth_email_verifications : "1:N"
    app_auth_users ||--o{ app_auth_password_resets : "1:N"
    app_auth_users ||--o{ app_auth_audit_logs : "1:N"
    app_auth_role_policies ||--o{ app_auth_users : "role关联"
```

---

## 四、新闻与资讯模块

```mermaid
erDiagram
    news_feed_items {
        INTEGER id PK "新闻ID"
        TEXT source "新闻来源"
        TEXT title "新闻标题"
        TEXT link "原文链接"
        TEXT guid "源站唯一标识"
        TEXT summary "新闻摘要"
        TEXT category "新闻分类"
        TEXT author "作者"
        TEXT pub_date "发布时间"
        TEXT fetched_at "抓取时间"
        TEXT content_hash "内容哈希"
        INTEGER llm_system_score "系统重要性评分"
        INTEGER llm_finance_impact_score "财经影响评分"
        TEXT llm_finance_importance "财经重要程度"
        TEXT llm_impacts_json "结构化影响JSON"
        TEXT llm_model "评分模型"
        TEXT llm_scored_at "评分时间"
        TEXT llm_prompt_version "提示词版本"
        TEXT llm_raw_output "模型原始输出"
    }
    
    news_feed_items_archive {
        INTEGER id PK "归档ID"
        TEXT source "新闻来源"
        TEXT title "新闻标题"
        TEXT link "原文链接"
        TEXT pub_date "发布时间"
        TEXT archived_at "归档时间"
    }
    
    news_daily_summaries {
        INTEGER id PK "总结ID"
        TEXT summary_date "总结日期"
        TEXT filter_importance "筛选条件"
        TEXT source_filter "数据源筛选"
        INTEGER news_count "新闻条数"
        TEXT model "总结模型"
        TEXT prompt_version "提示词版本"
        TEXT summary_markdown "总结内容"
        TEXT created_at "创建时间"
    }
    
    stock_news_items {
        INTEGER id PK "记录ID"
        TEXT ts_code FK "股票代码"
        TEXT company_name "公司名称"
        TEXT source "新闻来源"
        TEXT news_code "新闻源编号"
        TEXT title "新闻标题"
        TEXT summary "新闻摘要"
        TEXT link "原文链接"
        TEXT pub_time "发布时间"
        INTEGER comment_num "评论数"
        TEXT relation_stock_tags_json "关联股票标签"
        TEXT content_hash "内容哈希"
        INTEGER llm_system_score "系统评分"
        INTEGER llm_finance_impact_score "财经影响评分"
        TEXT llm_finance_importance "重要程度"
        TEXT llm_impacts_json "影响JSON"
        TEXT llm_summary "LLM摘要"
        TEXT llm_model "评分模型"
        TEXT llm_scored_at "评分时间"
        TEXT llm_prompt_version "提示词版本"
        TEXT llm_raw_output "原始输出"
    }
    
    news_feed_items ||--o{ stock_news_items : "关联个股"
    news_feed_items ||--o{ news_feed_items_archive : "归档"
```

---

## 五、群聊舆情模块

```mermaid
erDiagram
    chatroom_list_items {
        TEXT room_id PK "群聊唯一ID"
        TEXT remark "群备注名"
        TEXT nick_name "群昵称"
        TEXT owner "群主标识"
        INTEGER user_count "成员数量"
        TEXT source_url "来源接口"
        TEXT first_seen_at "首次发现时间"
        TEXT last_seen_at "最后出现时间"
        INTEGER skip_realtime_monitor "是否跳过监控"
        TEXT skip_realtime_reason "跳过原因"
        TEXT last_message_date "最近消息日期"
        TEXT last_chatlog_backfill_at "最近补抓时间"
        INTEGER last_30d_raw_message_count "30天原始消息数"
        INTEGER last_30d_clean_message_count "30天清洗消息数"
        TEXT llm_chatroom_summary "群聊简介"
        TEXT llm_chatroom_tags_json "分类标签"
        TEXT llm_chatroom_primary_category "主分类"
        TEXT llm_chatroom_activity_level "活跃度等级"
        TEXT llm_chatroom_risk_level "风险等级"
        INTEGER llm_chatroom_confidence "分类置信度"
        TEXT llm_chatroom_tagged_at "标签时间"
    }
    
    wechat_chatlog_clean_items {
        INTEGER id PK "消息ID"
        TEXT talker FK "群聊名称"
        TEXT query_date_start "查询起始日期"
        TEXT query_date_end "查询结束日期"
        TEXT message_date "消息日期"
        TEXT message_time "消息时间"
        TEXT sender_name "发送人昵称"
        TEXT sender_id "发送人ID"
        TEXT message_type "消息类型"
        TEXT content "原始内容"
        TEXT content_clean "清洗后内容"
        INTEGER is_quote "是否引用"
        TEXT quote_sender_name "被引用发送人"
        TEXT quote_content "被引用内容"
        TEXT message_key "消息唯一键"
        TEXT fetched_at "抓取时间"
    }
    
    chatroom_investment_analysis {
        INTEGER id PK "分析ID"
        TEXT room_id FK "群聊ID"
        TEXT talker "群聊名称"
        TEXT analysis_date "分析日期"
        INTEGER analysis_window_days "分析窗口天数"
        INTEGER message_count "消息条数"
        INTEGER sender_count "发言人数"
        TEXT latest_message_date "最新消息日期"
        TEXT room_summary "群聊摘要"
        TEXT targets_json "投资标的JSON"
        TEXT final_bias "投资倾向(看多/看空)"
        TEXT model "使用模型"
        TEXT prompt_version "提示词版本"
        TEXT raw_output "原始输出"
        TEXT created_at "创建时间"
    }
    
    chatroom_stock_candidate_pool {
        INTEGER id PK "候选ID"
        TEXT candidate_name "候选标的名称"
        TEXT candidate_type "候选类型"
        INTEGER bullish_room_count "看多群数"
        INTEGER bearish_room_count "看空群数"
        INTEGER net_score "净分值"
        TEXT dominant_bias "主导方向"
        INTEGER mention_count "提及次数"
        INTEGER room_count "涉及群数"
        TEXT latest_analysis_date "最近分析日期"
        TEXT sample_reasons_json "样例理由JSON"
        TEXT source_room_ids_json "来源群ID列表"
        TEXT created_at "创建时间"
    }
    
    chatroom_list_items ||--o{ wechat_chatlog_clean_items : "1:N"
    chatroom_list_items ||--o{ chatroom_investment_analysis : "1:N"
    wechat_chatlog_clean_items ||--o{ chatroom_investment_analysis : "来源"
    chatroom_investment_analysis ||--o{ chatroom_stock_candidate_pool : "聚合"
```

---

## 六、投资信号模块

```mermaid
erDiagram
    investment_signal_tracker {
        INTEGER id PK "信号ID"
        TEXT signal_key UK "信号唯一键"
        TEXT signal_type "信号类型"
        TEXT subject_name "标的名称"
        TEXT ts_code FK "股票代码"
        TEXT direction "方向(看多/看空/中性)"
        REAL signal_strength "信号强度"
        REAL confidence "置信度"
        INTEGER evidence_count "证据数量"
        INTEGER news_count "新闻数量"
        INTEGER stock_news_count "个股新闻数"
        INTEGER chatroom_count "群聊数量"
        TEXT signal_status "状态(活跃/观察)"
        TEXT latest_signal_date "最新信号日期"
        TEXT evidence_json "证据JSON"
        TEXT source_summary_json "来源汇总"
        TEXT created_at "创建时间"
        TEXT update_time "更新时间"
    }
    
    investment_signal_daily_snapshots {
        INTEGER id PK "快照ID"
        TEXT snapshot_at PK "快照时间"
        TEXT snapshot_date "快照日期"
        TEXT signal_key FK "信号键"
        TEXT signal_type "信号类型"
        TEXT subject_name "标的名称"
        TEXT ts_code FK "股票代码"
        TEXT direction "方向"
        REAL signal_strength "信号强度"
        REAL confidence "置信度"
        INTEGER evidence_count "证据数量"
        TEXT evidence_json "证据JSON"
        TEXT created_at "创建时间"
    }
    
    investment_signal_events {
        INTEGER id PK "事件ID"
        TEXT signal_key FK "信号键"
        TEXT event_time "事件发生时间"
        TEXT event_date "事件日期"
        TEXT event_type "事件类型"
        TEXT old_direction "原方向"
        TEXT new_direction "新方向"
        REAL old_strength "原强度"
        REAL new_strength "新强度"
        REAL delta_strength "强度变化"
        TEXT event_level "事件级别"
        TEXT driver_type "驱动类型"
        TEXT driver_source "驱动来源"
        TEXT event_summary "事件摘要"
        TEXT evidence_json "证据JSON"
        TEXT created_at "创建时间"
    }
    
    investment_signal_tracker ||--o{ investment_signal_daily_snapshots : "1:N"
    investment_signal_tracker ||--o{ investment_signal_events : "1:N"
```

---

## 七、主题热点模块

```mermaid
erDiagram
    theme_definitions {
        INTEGER id PK "主题ID"
        TEXT theme_name UK "主题名称"
        TEXT theme_group "主题分组"
        TEXT description "描述"
        TEXT keywords_json "关键词JSON"
        INTEGER priority "优先级"
        INTEGER enabled "是否启用"
        TEXT created_at "创建时间"
        TEXT update_time "更新时间"
    }
    
    theme_aliases {
        INTEGER id PK "别名ID"
        TEXT theme_name FK "主题名称"
        TEXT alias "别名"
        TEXT alias_type "别名类型"
        REAL confidence "置信度"
        TEXT source "来源"
        TEXT created_at "创建时间"
    }
    
    theme_hotspot_tracker {
        INTEGER id PK "追踪ID"
        TEXT theme_name UK "主题名称"
        TEXT theme_group "主题分组"
        TEXT direction "方向"
        REAL theme_strength "主题强度"
        REAL confidence "置信度"
        INTEGER evidence_count "证据数"
        INTEGER intl_news_count "国际新闻数"
        INTEGER domestic_news_count "国内新闻数"
        INTEGER stock_news_count "个股新闻数"
        INTEGER chatroom_count "群聊数"
        TEXT latest_evidence_time "最新证据时间"
        TEXT heat_level "热度等级"
        TEXT top_terms_json "热词JSON"
        TEXT top_stocks_json "热门股票JSON"
        TEXT update_time "更新时间"
    }
    
    theme_hotspot_evidence {
        INTEGER id PK "证据ID"
        TEXT theme_name "主题名称"
        TEXT source_type "来源类型"
        TEXT source_table "来源表"
        TEXT source_id "来源ID"
        TEXT title "标题"
        TEXT direction "方向"
        REAL weight "权重"
        TEXT ts_code "股票代码"
        TEXT sentiment_label "情感标签"
        TEXT created_at "创建时间"
    }
    
    theme_hotspot_snapshots {
        INTEGER id PK "快照ID"
        TEXT snapshot_date PK "快照日期"
        INTEGER lookback_days PK "回溯天数"
        TEXT theme_name PK "主题名称"
        REAL theme_strength "主题强度"
        TEXT heat_level "热度等级"
        TEXT created_at "创建时间"
    }
    
    theme_definitions ||--o{ theme_aliases : "1:N"
    theme_definitions ||--o{ theme_hotspot_tracker : "1:N"
    theme_definitions ||--o{ theme_hotspot_evidence : "1:N"
    theme_definitions ||--o{ theme_hotspot_snapshots : "1:N"
```

---

## 八、宏观与资金模块

```mermaid
erDiagram
    macro_series {
        TEXT indicator_code PK "指标代码"
        TEXT indicator_name "指标名称"
        TEXT freq PK "频率(D/W/M/Q/Y)"
        TEXT period PK "统计周期"
        REAL value "指标值"
        TEXT unit "单位"
        TEXT source "数据来源"
        TEXT publish_date "发布日期"
        TEXT update_time "更新时间"
    }
    
    rate_curve_points {
        TEXT market PK "市场(CN/US)"
        TEXT curve_code PK "曲线代码"
        TEXT trade_date PK "交易日期"
        TEXT tenor PK "期限(1M/3M/10Y)"
        REAL value "点位值"
        TEXT unit "单位"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
    }
    
    fx_daily {
        TEXT pair_code PK "汇率对代码"
        TEXT trade_date PK "交易日期"
        REAL open "开盘价"
        REAL high "最高价"
        REAL low "最低价"
        REAL close "收盘价"
        REAL pct_chg "涨跌幅"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
    }
    
    spread_daily {
        TEXT spread_code PK "利差代码"
        TEXT trade_date PK "交易日期"
        REAL value "利差值"
        TEXT unit "单位(bp)"
        TEXT source "数据来源"
        TEXT update_time "更新时间"
    }
    
    capital_flow_market {
        TEXT trade_date PK "交易日期"
        TEXT flow_type PK "资金流向类型"
        REAL net_inflow "净流入金额"
        REAL buy_amount "买入金额"
        REAL sell_amount "卖出金额"
        TEXT unit "单位"
        TEXT source "数据来源"
    }
    
    capital_flow_stock {
        TEXT ts_code PK,FK "股票代码"
        TEXT trade_date PK "交易日期"
        REAL net_inflow "净流入"
        REAL main_inflow "主力资金净流入"
        REAL super_large_inflow "超大单净流入"
        REAL large_inflow "大单净流入"
        REAL medium_inflow "中单净流入"
        REAL small_inflow "小单净流入"
        TEXT update_time "更新时间"
    }
    
    risk_scenarios {
        INTEGER id PK "场景ID"
        TEXT ts_code FK "股票代码"
        TEXT scenario_date "情景日期"
        TEXT scenario_name "情景名称"
        TEXT horizon "观察期限"
        REAL pnl_impact "盈亏影响"
        REAL max_drawdown "最大回撤"
        REAL var_95 "95% VaR"
        REAL cvar_95 "95% CVaR"
        TEXT assumptions_json "假设JSON"
        TEXT update_time "更新时间"
    }
```

---

## 九、研究工具模块

```mermaid
erDiagram
    multi_role_analysis_history {
        INTEGER id PK "分析ID"
        TEXT job_id UK "任务ID"
        TEXT version "版本"
        TEXT status "状态"
        TEXT ts_code FK "股票代码"
        TEXT name "股票名称"
        INTEGER lookback "回溯天数"
        TEXT roles_json "角色列表JSON"
        INTEGER accept_auto_degrade "允许降级"
        TEXT requested_model "请求模型"
        TEXT used_model "实际使用模型"
        TEXT attempts_json "尝试记录JSON"
        TEXT role_runs_json "角色运行结果JSON"
        TEXT aggregator_run_json "聚合结果JSON"
        TEXT analysis_markdown "分析结果Markdown"
        TEXT created_at "创建时间"
        TEXT finished_at "完成时间"
    }
    
    research_reports {
        INTEGER id PK "报告ID"
        TEXT report_key UK "报告唯一键"
        TEXT report_type "报告类型"
        TEXT ts_code FK "股票代码"
        TEXT report_date "报告日期"
        TEXT title "报告标题"
        TEXT content_markdown "报告内容"
        TEXT model "使用模型"
        TEXT prompt_version "提示词版本"
        TEXT created_at "创建时间"
    }
    
    quantaalpha_runs {
        INTEGER id PK "运行ID"
        TEXT run_id UK "运行唯一ID"
        TEXT ts_code FK "股票代码"
        TEXT run_date "运行日期"
        TEXT status "状态"
        TEXT parameters_json "参数JSON"
        TEXT created_at "创建时间"
    }
    
    quantaalpha_factor_results {
        INTEGER id PK "结果ID"
        INTEGER run_id FK "运行ID"
        TEXT factor_name "因子名称"
        REAL factor_value "因子值"
        REAL weight "权重"
        TEXT created_at "创建时间"
    }
    
    logic_view_cache {
        TEXT entity_type PK "实体类型"
        TEXT entity_key PK "实体键"
        TEXT content_hash PK "内容哈希"
        TEXT logic_view_json "逻辑视图JSON"
        TEXT created_at "创建时间"
        TEXT update_time "更新时间"
    }
    
    multi_role_analysis_history }o--|| stock_codes : "分析对象"
    research_reports }o--|| stock_codes : "报告对象"
    quantaalpha_runs }o--|| stock_codes : "运行对象"
    quantaalpha_runs ||--o{ quantaalpha_factor_results : "1:N"
```

---

## 十、辅助数据表

```mermaid
erDiagram
    stock_alias_dictionary {
        INTEGER id PK "别名ID"
        TEXT alias UK "别名"
        TEXT ts_code FK "股票代码"
        TEXT stock_name "股票名称"
        TEXT alias_type "别名类型"
        REAL confidence "置信度"
        INTEGER used_count "使用次数"
        TEXT last_used_at "最后使用时间"
        TEXT created_at "创建时间"
    }
    
    theme_stock_mapping {
        INTEGER id PK "映射ID"
        TEXT theme_name "主题名称"
        TEXT ts_code FK "股票代码"
        TEXT stock_name "股票名称"
        TEXT relation_type "关系类型"
        REAL weight "权重"
        TEXT created_at "创建时间"
    }
    
    signal_mapping_blocklist {
        INTEGER id PK "黑名单ID"
        TEXT term UK "词条"
        TEXT target_type "目标类型"
        TEXT match_type "匹配类型"
        TEXT reason "原因"
        INTEGER enabled "是否启用"
        TEXT created_at "创建时间"
    }
    
    signal_quality_rules {
        TEXT rule_key PK "规则键"
        TEXT rule_value "规则值"
        TEXT value_type "值类型"
        TEXT category "分类"
        TEXT description "描述"
        INTEGER enabled "是否启用"
        TEXT created_at "创建时间"
    }
```

---

## 十一、表统计信息

| 模块 | 表名 | 当前行数 | 说明 |
|------|------|----------|------|
| **股票核心** | stock_codes | 5,814 | 股票基础信息主表 |
| | stock_daily_prices | 1,327,993 | 日线行情 |
| | stock_minline | 1,757,145 | 分钟线数据 |
| | stock_valuation_daily | 125,985 | 估值日频 |
| | stock_financials | 17,631 | 财务指标 |
| | stock_events | 128,529 | 股票事件 |
| | stock_scores_daily | 16,479 | 综合评分 |
| | company_governance | 2,744 | 公司治理 |
| **新闻** | news_feed_items | 6,740 | 财经快讯 |
| | news_feed_items_archive | 50 | 新闻归档 |
| | news_daily_summaries | 3 | 日报总结 |
| | stock_news_items | 554 | 个股新闻 |
| **群聊** | chatroom_list_items | 166 | 群聊列表 |
| | wechat_chatlog_clean_items | 320,945 | 清洗消息 |
| | chatroom_investment_analysis | 27 | 投资倾向分析 |
| | chatroom_stock_candidate_pool | 176 | 候选池 |
| **宏观资金** | macro_series | 35,329 | 宏观指标 |
| | fx_daily | 1,565 | 汇率日线 |
| | rate_curve_points | 188 | 利率曲线 |
| | spread_daily | 47 | 利差数据 |
| | capital_flow_market | 476 | 市场资金流 |
| | capital_flow_stock | 1,256,146 | 个股资金流 |
| | risk_scenarios | 27,425 | 风险场景 |

---

## 十二、关键关系说明

### 12.1 核心关联关系

```
┌─────────────────────────────────────────────────────────────────┐
│                        核心关联关系                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  stock_codes (主表)                                             │
│       │                                                         │
│       ├──► stock_daily_prices (1:N) 日线行情                    │
│       ├──► stock_minline (1:N) 分钟线                           │
│       ├──► stock_valuation_daily (1:N) 估值数据                 │
│       ├──► stock_financials (1:N) 财务数据                      │
│       ├──► stock_events (1:N) 股票事件                          │
│       ├──► stock_scores_daily (1:N) 综合评分                    │
│       ├──► capital_flow_stock (1:N) 个股资金流                  │
│       ├──► stock_news_items (1:N) 个股新闻                      │
│       ├──► company_governance (1:N) 公司治理                    │
│       ├──► risk_scenarios (1:N) 风险场景                        │
│       └──► investment_signal_tracker (1:0..1) 投资信号          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 12.2 投资信号数据流

```
┌─────────────────────────────────────────────────────────────────┐
│                      投资信号数据流                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  新闻/群聊数据                                                   │
│       │                                                         │
│       ▼                                                         │
│  investment_signal_tracker (当前信号状态)                       │
│       │                                                         │
│       ├──► investment_signal_daily_snapshots (历史快照)         │
│       └──► investment_signal_events (状态变更事件)              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 12.3 用户认证数据流

```
┌─────────────────────────────────────────────────────────────────┐
│                       用户认证数据流                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  app_auth_users (用户主表)                                      │
│       │                                                         │
│       ├──► app_auth_sessions (登录会话)                         │
│       ├──► app_auth_usage_daily (日使用统计)                    │
│       ├──► app_auth_email_verifications (邮箱验证)              │
│       ├──► app_auth_password_resets (密码重置)                  │
│       └──► app_auth_audit_logs (审计日志)                       │
│                                                                 │
│  app_auth_role_policies (角色策略) ◄───── 角色关联              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

**文档结束**

---

> 💡 **说明**: 
> - 本 ER 图基于 2026-04-05 的数据库结构生成
> - 表行数来源于 `docs/database_dictionary.md`
> - 表关系基于外键约束和业务逻辑推导
