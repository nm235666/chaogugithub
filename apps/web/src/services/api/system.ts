import { http } from '../http'

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
