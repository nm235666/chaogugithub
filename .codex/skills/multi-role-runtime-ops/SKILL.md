---
name: multi-role-runtime-ops
---

# Multi-Role Runtime Ops

Use this skill when multi-role v2 jobs are unstable, slow, or stuck in incorrect states.

## Goal

Keep `/api/llm/multi-role/v2/*` reliable for the main frontend workflow.

## Scope

- `backend/server.py`
- `backend/routes/stocks.py`
- `apps/web/src/pages/research/MultiRoleResearchPage.vue`
- `multi_role_analysis_history` runtime state

## Common failure patterns

- job stays `queued` and never transitions
- `85%` aggregating for too long
- aggregator fallback text shown with no retry path
- `retry-aggregate` returns `404` due to stale backend process
- result reuse misses expected done result

## Workflow

1. Verify endpoint health and auth first.
2. Verify route existence on the running process (not only in code).
3. Inspect one target `job_id` in log and DB side by side.
4. Classify as runtime loss, provider instability, or state-machine defect.
5. Apply minimal fix that preserves v1 compatibility and frontend behavior.
6. Re-run minimal compile/build/regression checks.

## Guardrails

- Do not break existing v1 APIs.
- Keep changes local to multi-role v2 path unless explicitly required.
- Prefer explicit user-facing status over silent waiting.
- Avoid schema migrations unless task explicitly asks.

## Output

- root cause classification
- exact fix and files touched
- verification commands and results
- remaining risk and rollback point
