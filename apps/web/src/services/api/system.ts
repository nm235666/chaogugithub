import { http } from '../http'

export type LlmProviderItem = {
  provider_key: string
  index: number
  model: string
  base_url: string
  api_key_masked: string
  has_api_key: boolean
  api_key_source?: string
  api_key_env?: string
  default_temperature: number
  enabled: boolean
  status: 'active' | 'disabled' | string
  rate_limit_enabled: boolean
  rate_limit_per_minute: number
  runtime_status?: 'ok' | 'rate_limited' | string
  window_reset_at?: number
  count_current_minute?: number
  last_checked_at?: string
  last_http_status?: number | null
  last_latency_ms?: number | null
  last_error?: string
  health_status?: string
  health_recommendation?: string
  observability_7d?: {
    source?: string
    days?: number
    total_calls?: number
    success?: number
    failed?: number
    success_rate_pct?: number
    p95_latency_ms?: number
    rate_limited?: number
    http_429?: number
    switch_count?: number
  }
}

type LlmMethod = 'get' | 'post'
let llmProvidersEndpointBase = '/api/system/llm-providers'

function _candidateBases(): string[] {
  const bases = [llmProvidersEndpointBase, '/api/system/llm-providers', '/api/llm-providers']
  if (typeof window !== 'undefined') {
    const protocol = window.location.protocol || 'http:'
    const host = window.location.hostname || '127.0.0.1'
    bases.push(`${protocol}//${host}:8077/api/system/llm-providers`)
    bases.push(`${protocol}//${host}:8077/api/llm-providers`)
    bases.push(`${protocol}//${host}:8002/api/system/llm-providers`)
    bases.push(`${protocol}//${host}:8002/api/llm-providers`)
  }
  return Array.from(new Set(bases))
}

async function _callLlmProvidersApi(method: LlmMethod, suffix = '', payload?: any) {
  const candidates = _candidateBases().map((base) => `${base}${suffix}`)
  let lastError: any = null
  for (const url of candidates) {
    try {
      if (method === 'get') {
        const { data } = await http.get(url)
        llmProvidersEndpointBase = url.replace(new RegExp(`${suffix}$`), '')
        return data
      }
      const { data } = await http.post(url, payload || {})
      llmProvidersEndpointBase = url.replace(new RegExp(`${suffix}$`), '')
      return data
    } catch (error: any) {
      lastError = error
      const status = Number(error?.status || error?.response?.status || 0)
      // 401/403 表示接口存在但权限问题，不继续换地址
      if (status === 401 || status === 403) throw error
      // 仅对 404 或网络错误继续尝试其他候选
      if (status && status !== 404) throw error
      continue
    }
  }
  throw lastError || new Error('LLM providers API unavailable')
}

export async function fetchSignalQualityConfig() {
  const { data } = await http.get('/api/signal-quality/config')
  return data
}

export async function saveSignalQualityRules(items: Array<Record<string, any>>) {
  const { data } = await http.post('/api/signal-quality/rules/save', { items })
  return data
}

export async function saveSignalQualityBlocklist(items: Array<Record<string, any>>) {
  const { data } = await http.post('/api/signal-quality/blocklist/save', { items })
  return data
}

export async function fetchJobs() {
  const { data } = await http.get('/api/jobs')
  return data
}

export async function fetchJobRuns(params: Record<string, any>) {
  const { data } = await http.get('/api/job-runs', { params })
  return data
}

export async function fetchJobAlerts(params: Record<string, any>) {
  const { data } = await http.get('/api/job-alerts', { params })
  return data
}

export async function triggerJob(job_key: string) {
  const { data } = await http.get('/api/jobs/trigger', { params: { job_key } })
  return data
}

export async function dryRunJob(job_key: string) {
  const { data } = await http.get('/api/jobs/dry-run', { params: { job_key } })
  return data
}

export async function fetchAuthUsersSummary() {
  const { data } = await http.get('/api/auth/users/summary')
  return data
}

export async function fetchAuthInvites(params: Record<string, any>) {
  const { data } = await http.get('/api/auth/invites', { params })
  return data
}

export async function createAuthInvite(payload: { invite_code?: string; max_uses?: number; expires_at?: string }) {
  const { data } = await http.post('/api/auth/invite/create', payload)
  return data
}

export async function updateAuthInvite(payload: { invite_code: string; max_uses?: number; expires_at?: string; is_active?: boolean }) {
  const { data } = await http.post('/api/auth/invite/update', payload)
  return data
}

export async function deleteAuthInvite(invite_code: string) {
  const { data } = await http.post('/api/auth/invite/delete', { invite_code })
  return data
}

export async function fetchAuthUsers(params: Record<string, any>) {
  const { data } = await http.get('/api/auth/users', { params })
  return data
}

export async function updateAuthUser(payload: { user_id?: number; username?: string; role?: string; is_active?: boolean; display_name?: string }) {
  const { data } = await http.post('/api/auth/user/update', payload)
  return data
}

export async function resetAuthUserPassword(payload: { user_id?: number; username?: string; new_password: string }) {
  const { data } = await http.post('/api/auth/user/reset-password', payload)
  return data
}

export async function resetAuthUserTrendQuota(payload: { user_id?: number; username?: string; usage_date?: string }) {
  const { data } = await http.post('/api/auth/user/reset-trend-quota', payload)
  return data
}

export async function resetAuthUserMultiRoleQuota(payload: { user_id?: number; username?: string; usage_date?: string }) {
  const { data } = await http.post('/api/auth/user/reset-multi-role-quota', payload)
  return data
}

export async function resetAuthQuotaBatch(payload: { usage_date?: string; role?: string; usernames?: string[] | string }) {
  const { data } = await http.post('/api/auth/quota/reset-batch', payload)
  return data
}

export async function fetchAuthSessions(params: Record<string, any>) {
  const { data } = await http.get('/api/auth/sessions', { params })
  return data
}

export async function revokeAuthSession(session_id: number) {
  const { data } = await http.post('/api/auth/session/revoke', { session_id })
  return data
}

export async function revokeAuthUserSessions(user_id: number) {
  const { data } = await http.post('/api/auth/user/revoke-sessions', { user_id })
  return data
}

export async function fetchAuthAuditLogs(params: Record<string, any>) {
  const { data } = await http.get('/api/auth/audit-logs', { params })
  return data
}

export async function fetchLlmProviders() {
  const data = await _callLlmProvidersApi('get')
  return data as {
    ok: boolean
    default_request_model: string
    fallback_models: string[]
    default_rate_limit_per_minute: number
    items: LlmProviderItem[]
  }
}

export async function createLlmProvider(payload: Record<string, any>) {
  return _callLlmProvidersApi('post', '/create', payload)
}

export async function updateLlmProvider(payload: Record<string, any>) {
  return _callLlmProvidersApi('post', '/update', payload)
}

export async function deleteLlmProvider(payload: { provider_key: string; index: number }) {
  return _callLlmProvidersApi('post', '/delete', payload)
}

export async function testOneLlmProvider(payload: { provider_key: string; index: number; timeout_s?: number; probe_retries?: number; case_mode?: string }) {
  return _callLlmProvidersApi('post', '/test-one', payload)
}

export async function testModelLlmProviders(payload: { provider_key?: string; model?: string; timeout_s?: number; probe_retries?: number; case_mode?: string }) {
  return _callLlmProvidersApi('post', '/test-model', payload)
}

export async function updateDefaultLlmRateLimit(default_rate_limit_per_minute: number) {
  return _callLlmProvidersApi('post', '/default-rate-limit', { default_rate_limit_per_minute })
}
