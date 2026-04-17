---
name: repo-scan
---

# Repo Scan

Use this skill when you need to build quick project context before doing work, including the first turn of a new conversation.

## Goal

Understand just enough of the codebase to act safely, without over-reading.

This skill also replaces the old "project bootstrap" flow.

## Read order

1. `AGENTS.md`
2. `docs/system_overview_cn.md`
3. `README_WEB.md`
4. target module routes / API / stores
5. closest service or job entrypoint

## What to capture

- user-facing module involved
- backend entrypoints involved
- validation options available
- docs that may need refresh
- current top priorities from `AGENTS.md`
- how the task aligns with frontend presentation first and testability second

## Suggested scan commands

- `rg --files apps/web/src`
- `rg --files backend/routes services jobs`
- `rg -n "<feature_keyword>" apps/web/src backend services jobs docs`

## Output

1. Relevant files
2. Module boundary
3. Main calling path
4. Obvious risks
5. Best smallest next step
6. Current top priorities
