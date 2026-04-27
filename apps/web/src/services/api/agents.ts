import { http } from '../http'

export type AgentRun = {
  id: string
  agent_key: string
  status: string
  mode?: string
  trigger_source?: string
  schedule_key?: string
  actor?: string
  correlation_id?: string
  parent_run_id?: string
  metadata?: Record<string, any>
  goal?: Record<string, any>
  plan?: Record<string, any>
  result?: Record<string, any>
  error_text?: string
  approval_required?: boolean
  created_at?: string
  updated_at?: string
  finished_at?: string
  steps?: AgentStep[]
}

export type AgentMemoryItem = {
  id: string
  memory_type: string
  source_run_id?: string
  source_agent_key?: string
  ts_code?: string
  scope?: string
  summary?: string
  evidence?: Record<string, any>
  score?: number
  status?: string
  created_at?: string
  updated_at?: string
}

export type AgentTimelineEvent = {
  type: string
  at?: string
  run_id?: string
  agent_key?: string
  tool_name?: string
  status?: string
  audit_id?: number
  message_type?: string
  payload?: Record<string, any>
}

export type AgentStep = {
  id: string
  run_id: string
  step_index: number
  tool_name: string
  args?: Record<string, any>
  dry_run?: boolean
  status: string
  audit_id?: number
  result?: Record<string, any>
  error_text?: string
  created_at?: string
  updated_at?: string
}

export async function fetchAgentStackHealth() {
  const { data } = await http.get('/api/agents/health')
  return data as Record<string, any>
}

export async function fetchMcpToolAudit(params?: { limit?: number; dry_run_only?: boolean; write_only?: boolean }) {
  const { data } = await http.get('/api/agents/mcp-audit', { params })
  return data as { ok: boolean; items: Record<string, unknown>[]; total_returned?: number }
}

export async function fetchAgentRuns(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/agents/runs', { params })
  return data as { ok: boolean; items: AgentRun[] }
}

export async function fetchAgentTimeline(correlationId: string) {
  const { data } = await http.get('/api/agents/timeline', { params: { correlation_id: correlationId, limit: 200 } })
  return data as { ok: boolean; correlation_id: string; events: AgentTimelineEvent[]; runs: AgentRun[]; messages: Record<string, any>[] }
}

export async function fetchAgentMemoryItems(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/agents/memory', { params })
  return data as { ok: boolean; items: AgentMemoryItem[] }
}

export async function fetchAgentRun(runId: string) {
  const { data } = await http.get(`/api/agents/runs/${encodeURIComponent(runId)}`)
  return data as { ok: boolean; run: AgentRun }
}

export async function startAgentRun(payload: {
  agent_key: string
  trigger_source?: string
  actor?: string
  goal?: Record<string, any>
  metadata?: Record<string, any>
  schedule_key?: string
  dedupe?: boolean
  correlation_id?: string
  parent_run_id?: string
}) {
  const { data } = await http.post('/api/agents/runs', payload)
  return data as { ok: boolean; run: AgentRun }
}

export async function approveAgentRun(runId: string, payload: { reason: string; actor?: string; idempotency_key?: string }) {
  const { data } = await http.post(`/api/agents/runs/${encodeURIComponent(runId)}/approve`, {
    ...payload,
    decision: 'approved',
  })
  return data as { ok: boolean; run: AgentRun }
}

export async function rejectAgentRun(runId: string, payload: { reason: string; actor?: string; idempotency_key?: string }) {
  const { data } = await http.post(`/api/agents/runs/${encodeURIComponent(runId)}/approve`, {
    ...payload,
    decision: 'rejected',
  })
  return data as { ok: boolean; run: AgentRun }
}
