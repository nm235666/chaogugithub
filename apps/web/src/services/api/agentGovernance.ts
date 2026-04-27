import { http } from '../http'

export type AgentQualityScore = {
  id?: string
  agent_key: string
  metric_date?: string
  window_days?: number
  total_runs?: number
  succeeded_runs?: number
  failed_runs?: number
  waiting_approval_runs?: number
  approval_approved?: number
  approval_rejected?: number
  changed_count?: number
  conflict_count?: number
  avg_duration_seconds?: number
  success_rate?: number
  failure_rate?: number
  approval_pass_rate?: number
  risk_score?: number
  risk_status?: string
  evidence?: Record<string, any>
  updated_at?: string
}

export type GovernanceRule = {
  id?: string
  rule_key: string
  agent_key?: string
  tool_name?: string
  risk_level?: string
  decision?: string
  enabled?: boolean
  thresholds?: Record<string, any>
  reason?: string
  updated_at?: string
}

export type PolicyDecision = {
  id: string
  run_id?: string
  correlation_id?: string
  agent_key?: string
  tool_name?: string
  requested_dry_run?: boolean
  decision?: string
  risk_level?: string
  reason?: string
  evidence?: Record<string, any>
  created_at?: string
}

export async function fetchGovernanceQuality(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/agent-governance/quality', { params })
  return data as { ok: boolean; items: AgentQualityScore[]; window_days?: number }
}

export async function recomputeGovernanceQuality(payload: { agent_key?: string; window_days?: number }) {
  const { data } = await http.post('/api/agent-governance/recompute', payload)
  return data as { ok: boolean; items: AgentQualityScore[] }
}

export async function fetchGovernanceRules(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/agent-governance/rules', { params })
  return data as { ok: boolean; items: GovernanceRule[] }
}

export async function upsertGovernanceRule(payload: GovernanceRule & { actor?: string; reason?: string }) {
  const { data } = await http.post('/api/agent-governance/rules', payload)
  return data as { ok: boolean; item: GovernanceRule }
}

export async function fetchPolicyDecisions(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/agent-governance/policy-decisions', { params })
  return data as { ok: boolean; items: PolicyDecision[] }
}
