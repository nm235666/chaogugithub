---
name: frontend-delivery
---

# Frontend Delivery

Use this skill when the task is mainly about user-facing pages, layout, loading states, error states, table/detail presentation, charts, route behavior, or dashboard clarity.

## Goal

Optimize for:
1. Frontend presentation quality
2. Testability
3. Stability

This skill also covers the old "frontend page optimization" prompt flow.

## Read first

1. `AGENTS.md`
2. `apps/web/src/app/router.ts`
3. relevant page in `apps/web/src/pages/`
4. relevant API client in `apps/web/src/services/api/`
5. relevant store in `apps/web/src/stores/`
6. shared UI in `apps/web/src/shared/`

## Checklist

- What visible user-facing issue are we fixing?
- Is there already a shared component or query pattern to reuse?
- Are loading, empty, and error states acceptable?
- Does the route still make sense?
- Does permission handling still make sense?
- Did we preserve API compatibility?
- Is the page clearer after the change, not just technically correct?

## Validation

- If frontend code changed, prefer `cd /home/zanbo/zanbotest/apps/web && npm run build`
- If only small TS/Vue logic changed and full build is too expensive, still explain what was not validated

## Report format

- What changed
- What did not change
- Frontend presentation impact
- Testability impact
- Remaining risks
