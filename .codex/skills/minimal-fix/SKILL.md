---
name: minimal-fix
---

# Minimal Fix

Use this skill when the task should be solved with the smallest possible code change.

## Goal

- keep scope tight
- avoid unrelated cleanup
- preserve API and route compatibility
- improve frontend presentation first, testability second

## Read first

1. `AGENTS.md`
2. the exact file(s) being changed
3. nearest related API / store / route file if the change is user-facing

## Workflow

1. Identify the exact broken behavior.
2. Find the narrowest fix point.
3. Reuse existing helpers, queries, stores, or service functions.
4. Do not expand into refactor unless clearly required.
5. Run the smallest meaningful validation.

## Good fit

- wrong label or empty state
- broken table/detail rendering
- route mismatch
- small API/client mismatch
- doc drift caused by a small change

## Output

- smallest changed surface
- why no broader refactor was done
- validation run
- remaining risks
