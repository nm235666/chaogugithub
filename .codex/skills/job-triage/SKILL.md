---
name: job-triage
---

# Job Triage

Use this skill for cron/job_orchestrator failures, stuck pipelines, or repeated task regressions.

## Goal

Restore task pipeline health quickly and prevent repeated failures.

## Scope

- `job_registry.py`
- `job_orchestrator.py`
- cron installers / run scripts
- `/tmp/*log` runtime logs
- process and lock files

## Workflow

1. Identify failed jobs by key, run_id, timestamp.
2. Check current process state and lock state.
3. Read latest logs and classify error type:
   - config error
   - source instability
   - code bug
   - timeout/retry policy mismatch
4. Apply smallest safe fix.
5. Run one manual job execution to verify.
6. Decide whether to backfill missed window.

## Triage output format

1. Incident summary
2. Root cause
3. Immediate mitigation
4. Permanent fix
5. Backfill command (if needed)
6. Residual risk

## Guardrails

- stop retry storms before deep changes
- avoid restarting all services unless necessary
- keep schedule semantics unchanged unless explicitly requested
