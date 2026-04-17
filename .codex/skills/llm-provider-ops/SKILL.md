---
name: llm-provider-ops
---

# LLM Provider Ops

Use this skill when LLM provider routing becomes unstable or score/analyze jobs fail due to model channel issues.

## Goal

Keep LLM calls available with controlled fallback and avoid false disable/false healthy judgments.

## Scope

- `llm_provider_config.py`
- `llm_gateway.py`
- `config/llm_providers.json`
- `check_gpt_provider_nodes.py`
- LLM scoring/analyze jobs that consume provider config

## Common failure patterns

- `HTTP 401 INVALID_API_KEY`
- `HTTP 403` / Cloudflare `1010`
- `HTTP 429` rate limit / cooldown
- `HTTP 503/524` transient upstream failures
- model naming mismatch (`gpt-5.4` vs `GPT-5.4`)
- endpoint path mismatch (`/v1` vs root)
- parameter mismatch (provider rejects `temperature`, etc.)

## Workflow

1. Verify provider node with protocol-level request first (same payload style as production).
2. Distinguish permanent failure from transient failure.
3. Update node health metadata (`enabled`, `status`, `health_status`, `consecutive_failures`).
4. Ensure healthy nodes are prioritized in config order.
5. Re-run one small real job (`--limit 1`) to validate end-to-end.
6. Only then restart backlog/parallel jobs.

## Guardrails

- Do not disable a node on a single timeout by default.
- Prefer fail-threshold and retry policy for unstable networks.
- Keep fallback model chain explicit and auditable.
- Never commit raw keys in docs or prompts.

## Output

- which nodes are healthy/unhealthy and why
- what changed in provider config and routing
- whether backlog jobs can be safely resumed
