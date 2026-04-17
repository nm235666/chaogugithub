---
name: doc-freshness
---

# Doc Freshness

Use this skill when code changes might make documentation stale.

## Priority

Documentation freshness is mandatory before git push.

## Check these files first

1. `AGENTS.md`
2. `README_WEB.md`
3. `docs/system_overview_cn.md`
4. `SQLITE_RETIRED.md`

Then check module-specific docs if relevant.

## Common stale areas

- routes and page entrypoints
- API methods or endpoint behavior
- task names and schedules
- runtime/ deployment description
- frontend ownership (`apps/web` vs `frontend`)
- database runtime assumptions

## Workflow

1. Compare the changed code with the nearest matching docs.
2. Update the smallest set of docs needed.
3. Avoid copying dynamic code detail into `AGENTS.md`; prefer references there.
4. List exact mismatches, not just "docs may be stale".
5. Before declaring the task complete, say whether docs are now current enough for git push.

## Output

- stale docs found / not found
- exact mismatches
- updated files
- minimal files to update
- remaining doc risks
