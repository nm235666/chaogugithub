# Agent guide (token-conscious)

Use this file first; avoid whole-repo exploration when a path below is enough.

## Run / entry

- Backend: `python3 backend/server.py` (env: `runtime_env.sh`, `PORT`, `DATABASE_URL` / `USE_POSTGRES` via `db_compat.py`).
- Implementation: [`backend/http_server/`](backend/http_server/) — [`bootstrap.py`](backend/http_server/bootstrap.py) starts server; [`handler.py`](backend/http_server/handler.py) is `ApiHandler`.
- Public facade re-exports: [`backend/server.py`](backend/server.py) (scripts may `import backend.server`).

## Where to change what

| Goal | Start here | Notes |
|------|------------|--------|
| New HTTP route or JSON contract | [`backend/routes/*.py`](backend/routes/) | Wire in [`handler.py`](backend/http_server/handler.py) `do_GET` / `do_POST` if not already dispatched. |
| Business logic | [`services/<domain>/`](services/) | Prefer extending existing service over growing [`legacy_queries.py`](backend/http_server/legacy_queries.py). |
| RBAC / navigation payload | [`backend/http_server/rbac.py`](backend/http_server/rbac.py), [`config/rbac_dynamic.config.json`](config/rbac_dynamic.config.json) | Static fallbacks in [`config.py`](backend/http_server/config.py). |
| Auth tables / sessions | [`backend/http_server/auth_users.py`](backend/http_server/auth_users.py) | DB path: [`config.py`](backend/http_server/config.py) `DB_PATH` (CLI overrides: patch `backend.http_server.config.DB_PATH`). |
| API layer write rules | [`backend/layers/`](backend/layers/) | `api_contracts`, `write_policies`. |
| MCP tools | [`mcp_server/`](mcp_server/) | Separate HTTP JSON-RPC; not `backend/server.py`. |
| Frontend | [`apps/web/src/`](apps/web/src/) | Vite + Vue 3; API clients under `services/api/`. |

## Do not read by default

- Whole [`legacy_queries.py`](backend/http_server/legacy_queries.py): grep for `def query_` / symbol name, then open the matching region only.
- `docs/metrics/daily/*.json` (noisy); use a named report or `_latest*` if needed.
- `**/Obsolete*/**` (archive).

## Tests (cheap “spec” for behavior)

- API / RBAC smoke: `tests/test_frontend_api_smoke.py`, `tests/test_api_write_boundary_smoke.py`, `tests/test_projection_maturity.py`.
- E2E: `apps/web/tests/e2e/` (Playwright).

## Conventions

- Keep diffs scoped; match existing import and naming style in the touched package.
- PostgreSQL is the primary store when `USE_POSTGRES=1`; `db_compat` abstracts SQLite compatibility.
