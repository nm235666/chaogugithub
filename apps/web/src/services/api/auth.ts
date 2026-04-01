import { http } from '../http'

export type AuthStatus = {
  ok: boolean
  auth_required: boolean
  token_present: boolean
  token_valid: boolean
  has_user_accounts?: boolean
  trend_quota?: {
    limit: number | null
    used: number
    remaining: number | null
  }
  multi_role_quota?: {
    limit: number | null
    used: number
    remaining: number | null
  }
  permission_matrix?: Record<string, string[]>
  effective_permissions?: string[]
  user?: {
    id: number
    username: string
    display_name?: string
    email?: string
    role?: string
    tier?: string
    email_verified?: boolean
  } | null
}

let cachedStatus: AuthStatus | null = null
let cachedAt = 0
let inflight: Promise<AuthStatus> | null = null
const CACHE_MS = 30_000

export async function fetchAuthStatus(force = false): Promise<AuthStatus> {
  const now = Date.now()
  if (!force && cachedStatus && now - cachedAt < CACHE_MS) {
    return cachedStatus
  }
  if (!force && inflight) return inflight
  inflight = http
    .get<AuthStatus>('/api/auth/status')
    .then((resp) => {
      cachedStatus = resp.data
      cachedAt = Date.now()
      return resp.data
    })
    .finally(() => {
      inflight = null
    })
  return inflight
}

export function clearAuthStatusCache() {
  cachedStatus = null
  cachedAt = 0
}

export async function loginWithToken(token: string): Promise<AuthStatus> {
  const { data } = await http.post<AuthStatus>('/api/auth/login', { token })
  clearAuthStatusCache()
  return data
}

export async function loginWithPassword(username: string, password: string): Promise<AuthStatus & { token?: string }> {
  const { data } = await http.post<AuthStatus & { token?: string }>('/api/auth/login', { username, password })
  clearAuthStatusCache()
  return data
}

export async function registerAccount(
  username: string,
  password: string,
  displayName: string,
  inviteCode: string,
): Promise<
  AuthStatus & {
    token?: string
  }
> {
  const { data } = await http.post<AuthStatus & { token?: string }>('/api/auth/register', {
    username,
    password,
    display_name: displayName,
    invite_code: inviteCode,
  })
  clearAuthStatusCache()
  return data
}

export async function logoutAuth(): Promise<void> {
  await http.post('/api/auth/logout', {})
  clearAuthStatusCache()
}

export async function forgotPassword(username_or_email: string) {
  const { data } = await http.post('/api/auth/forgot-password', { username_or_email })
  return data
}

export async function resetPasswordWithCode(username: string, reset_code: string, new_password: string) {
  const { data } = await http.post('/api/auth/reset-password', { username, reset_code, new_password })
  return data
}
