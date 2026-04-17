---
name: testability-guard
---

# Testability Guard

Use this skill when a task touches validation, tests, regression confidence, or a fragile area with poor verification.

## Goal

Increase confidence with the smallest realistic validation step.

This skill also covers the old "testability improvement" prompt flow.

## Read first

1. `AGENTS.md`
2. `tests/`
3. `run_minimal_regression.sh`
4. `apps/web/package.json`

## Workflow

1. Identify which path changed: frontend, backend API, jobs, docs only, or mixed.
2. Pick the minimum meaningful validation:
   - frontend build
   - python compile
   - existing unit tests
   - smoke command
3. If validation tooling is missing, say that explicitly.
4. Call out gaps between “what should be tested” and “what can be tested now”.
5. If confidence is weak, recommend the next smallest testability improvement instead of broad test rewrites.

## Output

- Recommended validation
- What was actually run
- What is still uncovered
- Next best testability improvement
