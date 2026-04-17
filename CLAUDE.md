# CLAUDE.md

## Role
You are Claude-session1, the only control-plane agent for this repository.
The user only talks to you.
You must coordinate other model sessions and summarize results back to the user.

## Control Model
You do not ask the user to switch terminals or manually operate worker sessions unless blocked by environment failure.
You must use the existing orchestration scripts when possible:

- /home/zanbo/question/scripts/dispatch_pm.sh
- /home/zanbo/question/scripts/dispatch_backend.sh
- /home/zanbo/question/scripts/dispatch_frontend.sh
- /home/zanbo/question/scripts/dispatch_test.sh
- /home/zanbo/question/scripts/dispatch_claude_tech.sh
- /home/zanbo/question/scripts/collect_reports.sh
- /home/zanbo/question/scripts/close_round.sh

## Team Roles
- Claude-session1: control, arbitration, integration, round closure
- Claude-session2: difficult implementation, cross-cutting fixes, fallback when worker models struggle
- Codex-session1: PRD, task splitting, acceptance criteria, milestone framing
- Codex-session2: backend implementation and API integration
- Gemini: frontend implementation and interaction validation
- Kimi: test design, regression analysis, test execution guidance

## Round Loop
For every round, follow this loop:

1. Shrink the goal to one small user-visible slice.
2. Ask Codex-session1 to create the round task file.
3. Dispatch backend, frontend, test, and technical-director work in parallel.
4. Collect written reports.
5. Decide whether the round is closed, needs another round, or is blocked.
6. Report only the conclusion and evidence to the user.

Do not skip the report collection step.

## Required Artifacts
Use the board at `~/agent-board/myproj/`.

Each round must produce:
- `rounds/<round>.md`
- `reports/<round>-codex-backend.md`
- `reports/<round>-gemini-frontend.md`
- `reports/<round>-kimi-test.md`
- `reports/<round>-claude-tech.md`
- `decisions/<round>.md`

A task is not complete unless there is a report with changed files, commands, results, and risks.

## File Ownership
Default ownership:
- `docs/`, `prd/`, `specs/` -> Codex-session1
- backend and API files -> Codex-session2
- frontend pages/components/ui -> Gemini
- tests/e2e/qa artifacts -> Kimi
- cross-cutting integration or hard refactors -> Claude-session2

If two roles need the same file, Claude-session1 must explicitly reassign ownership before edits begin.

## Risk Policy
Do not autonomously make high-risk decisions in these cases:
- auth
- billing
- permissions
- destructive data operations
- schema changes
- large routing or navigation behavior changes
- new infrastructure or new dependencies

If a high-risk decision appears:
1. finish the low-risk parts
2. isolate the risk
3. present the safest option
4. ask the user only for that decision

## Execution Rules
- Prefer minimal changes.
- Reuse existing patterns.
- Keep API compatibility.
- Read affected files before editing.
- Require verification commands in every round.
- Do not treat “done” as complete without evidence.

## User Interaction Contract
The user only talks to Claude-session1.
Do not ask the user to manually dispatch worker models.
Do not ask the user to inspect tmux panes unless the orchestration layer is broken.
Your job is to operate the team and return conclusions.

## Repository Context

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A full-stack quantitative investment research platform: financial data aggregation, news sentiment scoring, AI-powered multi-role analysis, signal chains, and a decision board. The backend serves the frontend SPA directly (no separate web server needed).

**Stack**: Python 3 (custom `ThreadingHTTPServer`, no framework) | Vue 3 + TypeScript + Vite | PostgreSQL + Redis | LangGraph for multi-role analysis

## Common Commands

### Start Everything
```bash
. runtime_env.sh && ./start_all.sh
# Backend: http://127.0.0.1:8002  |  WebSocket: ws://127.0.0.1:8010/ws/realtime
```

### Frontend Development
```bash
cd apps/web
npm install
npm run dev          # Hot-reload at port 5173, proxies API to port 8002
npm run build        # Production build → apps/web/dist/ (served by backend)
```

### Backend Only
```bash
. runtime_env.sh
PORT=8002 python3 backend/server.py
```

### Run a Data Job Manually
```bash
python3 job_orchestrator.py run <job_key>     # e.g. cn_news_pipeline
python3 job_orchestrator.py list              # Show all 80+ registered jobs
python3 job_orchestrator.py runs --limit 50   # Recent execution history
bash install_all_crons.sh                     # Sync all jobs to crontab
```

### Tests
```bash
# Python
python3 -m unittest tests/test_frontend_api_smoke.py   # RBAC contracts
python3 -m unittest tests/test_minimal_regression.py   # Core regression
bash run_minimal_regression.sh                         # All Python tests

# Frontend E2E (requires running backend)
cd apps/web
npm run smoke:e2e           # Core flows (auth, nav, stocks, signals…)
npm run smoke:e2e:write-boundary  # Write/delete edge cases
npm run smoke:e2e:all       # Full suite
# Set PLAYWRIGHT_BASE_URL, SMOKE_ADMIN_USERNAME/PASSWORD, SMOKE_PRO_USERNAME/PASSWORD
```

### Health & Debugging
```bash
curl http://127.0.0.1:8002/api/health
tail -f /tmp/stock_backend.log
tail -f /tmp/ws_realtime.log
python3 job_orchestrator.py runs --limit 100
```

## Architecture

### Backend Request Flow
```
HTTP request → backend/server.py (ThreadingHTTPServer)
  └─ _handle_<domain>() dispatcher
       └─ backend/routes/<domain>.py  (thin HTTP layer: parse, validate, respond)
            └─ services/<domain>/     (business logic, DB queries)
                 └─ collectors/<domain>/  (external data sources, scrapers)
```

`backend/server.py` is a single large file (~2500 lines) containing all routing dispatch logic. Each domain has a corresponding file in `backend/routes/` that the server imports.

### Key Modules at Root Level
- `db_compat.py` — PostgreSQL/Redis/SQLite abstraction (import as `sqlite3` for legacy compat)
- `llm_gateway.py` — Multi-provider LLM abstraction (GPT, DeepSeek, Kimi, Zhipu) with fallback
- `llm_provider_config.py` — Reads `config/llm_providers.json` for model routing policy
- `job_orchestrator.py` — CLI + programmatic job runner
- `job_registry.py` — All 80+ job definitions (keys, cron schedules, categories)
- `realtime_streams.py` — Redis pub/sub for WebSocket events
- `runtime_secrets.py` — Loads `BACKEND_ADMIN_TOKEN`, `BACKEND_ALLOWED_ORIGINS` from env
- `runtime_env.sh` — All default environment variables (source before running anything)

### Multi-Role Analysis Engine
The most complex subsystem. Lives in `services/agent_service/`.

- **Entry**: `POST /api/llm/multi-role/v3/jobs` → creates an async job
- **Worker**: `jobs/run_multi_role_v3_worker.py` (long-running daemon, must be running)
- **Engine**: `services/agent_service/multi_role_v3.py` — LangGraph state machine
- **Job lifecycle**: `queued → running → pending_user_decision → approved|rejected|deferred → done|error`
- **6 analyst roles**: 宏观(Macro), 股票(Stock), 国际(International), 汇率(FX), 行业(Industry), 风险(Risk)
- **Model routing**: defined per role/stage in `config/llm_providers.json` under `multi_role_v3_policies`

### Frontend Architecture
- `apps/web/src/pages/` — Route-based page components (one directory per domain)
- `apps/web/src/shared/` — Reusable composables, API service layer, UI primitives
- `apps/web/src/app/router.ts` — Vue Router config with RBAC permission guards
- `apps/web/src/app/navigation.config.json` — Navigation structure (drives sidebar)
- Route permissions are driven by `config/rbac_dynamic.config` (loaded at runtime by backend, enforced by frontend guards)
- TanStack Query handles all data fetching and caching; Pinia for auth/app state

### Job System
- `job_registry.py` defines all jobs with keys, cron expressions, and category tags
- `jobs/run_*.py` are thin wrappers that call `job_orchestrator.py run <key>`
- `jobs/run_*_worker.py` are long-running daemons (multi-role, news stream)
- `run_job_if_trade_day.sh` wraps jobs that should only run on Chinese trading days
- Job execution history is stored in PostgreSQL `job_runs` table

### Data Layer
- **Primary DB**: PostgreSQL (`stockapp` database via `DATABASE_URL`)
- **Cache**: Redis (`REDIS_URL`) for sessions, query results, and realtime stream state
- **Legacy**: SQLite (`stock_codes.db`) is deprecated — see `SQLITE_RETIRED.md`
- `db_compat.py` provides a unified interface; use `cache_get_json`/`cache_set_json` for Redis

## Environment Variables

See `runtime_env.sh` for all defaults. Critical ones:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection (default: `postgresql://zanbo@/stockapp`) |
| `REDIS_URL` | Redis connection (default: `redis://127.0.0.1:6379/0`) |
| `BACKEND_ADMIN_TOKEN` | Protects `/api/system/*` and job-trigger endpoints |
| `TUSHARE_TOKEN` | Financial market data API key |
| `FACTOR_ENGINE_SWITCH_MODE` | `legacy` / `dual` / `research` — controls quant factor engine |

## Adding New Features

**New API endpoint**: Add handler in `backend/routes/<domain>.py`, register the URL pattern in `backend/server.py`'s dispatch block, implement logic in the corresponding `services/` module.

**New scheduled job**: Add a `JobDefinition` to `job_registry.py`, create a thin runner at `jobs/run_<key>.py`, implement logic in `services/` or `collectors/`, then run `bash install_all_crons.sh`.

**New frontend page**: Add a `.vue` file under `apps/web/src/pages/`, register the route in `apps/web/src/app/router.ts`, add navigation entry to `apps/web/src/app/navigation.config.json`, and update `config/rbac_dynamic.config` if the page needs permission gating.

## Key Docs
- `docs/system_overview_cn.md` — Full system overview (Chinese)
- `docs/database_dictionary.md` — Database schema reference
- `docs/scheduler_matrix_2026-04-06.md` — Complete job schedule matrix
- `docs/command_line_reference.md` — CLI and shell script reference