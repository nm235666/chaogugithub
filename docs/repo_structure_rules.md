# 仓库结构约束

## 新代码落点
- 新研究逻辑统一放 `services/agent_service/`
- 新报告逻辑统一放 `services/reporting/`
- 新抓取、回填、聚合脚本统一放 `collectors/`
- 新任务适配入口统一放 `jobs/`
- 新通知适配器统一放 `services/notifications/`
- 新闻类新增脚本统一放 `collectors/news/`
- 新闻类调度入口统一通过 `jobs/run_news_job.py` 或同类 job runner 暴露给 `job_registry.py`
- 个股新闻类新增脚本统一放 `collectors/stock_news/`
- 个股新闻类调度入口统一通过 `jobs/run_stock_news_job.py` 或同类 job runner 暴露给 `job_registry.py`
- 群聊类新增脚本统一放 `collectors/chatrooms/`
- 群聊类调度入口统一通过 `jobs/run_chatroom_job.py` 或同类 job runner 暴露给 `job_registry.py`
- 宏观类新增脚本统一放 `collectors/macro/`
- 宏观类调度入口统一通过 `jobs/run_macro_job.py` 或同类 job runner 暴露给 `job_registry.py`
- 市场类新增脚本统一放 `collectors/market/`
- 市场类调度入口统一通过 `jobs/run_market_job.py` 或同类 job runner 暴露给 `job_registry.py`

## 兼容策略
- 根目录旧脚本允许保留，但只做兼容入口
- 根目录旧脚本不再新增长业务逻辑
- 新闻相关 root 脚本后续仅保留兼容壳或被 `collectors/news` 调用
- `job_registry.py` 优先登记 job runner，不再直接编排新闻类 root 脚本命令
- `job_registry.py` 优先登记个股新闻类 job runner，不再直接编排个股新闻 root 脚本命令
- `job_registry.py` 优先登记群聊类 job runner，不再直接编排群聊 root 脚本命令
- `job_registry.py` 优先登记宏观类 job runner，不再直接编排宏观 root 脚本命令
- `job_registry.py` 优先登记市场类 job runner，不再直接编排市场类 root 脚本命令
- 新页面继续放 `apps/web/src/pages/`
- `backend/routes/` 只做参数校验、权限检查、响应封装

## 研究链路
- 走势分析和多角色分析统一走 `services/agent_service`
- CLI、API、前端优先消费统一结构化字段

## 报告链路
- 标准投研报告、日报总结和导出逻辑统一向 `services/reporting` 收口
