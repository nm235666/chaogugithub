---
name: data-quality-audit
---

# Data Quality Audit

Use this skill when table quality drifts, duplicates appear, or scoring coverage drops.

## Goal

Keep data usable for dashboards, LLM analysis, and signal generation.

## Scope

- key business tables: news, stock news, prices, minline, chatroom analysis, signals
- dedupe scripts, backfill scripts, scoring scripts
- quality audit/report scripts

## Audit dimensions

- completeness (missing rows/fields)
- freshness (latest update lag)
- uniqueness (exact and semantic duplicates)
- validity (NaN/invalid JSON/invalid enum)
- consistency (field semantics between pages/APIs)

## Workflow

1. baseline counts and freshness snapshot
2. detect obvious quality issues
3. map each issue to script-level remediation
4. run targeted repair commands
5. re-check and report delta

## Output

- quality issue matrix (table -> issue -> severity)
- exact repair commands
- post-repair deltas
- what still needs source-level fix
