const ADMIN_TOKEN_KEY = 'zanbo_admin_token'

export function readAdminToken() {
  if (typeof window === 'undefined') return ''
  return (
    window.localStorage.getItem(ADMIN_TOKEN_KEY)?.trim() ||
    window.sessionStorage.getItem(ADMIN_TOKEN_KEY)?.trim() ||
    ''
  )
}

export function setAdminToken(token: string, persist = true) {
  if (typeof window === 'undefined') return
  const normalized = String(token || '').trim()
  if (!normalized) return
  if (persist) {
    window.localStorage.setItem(ADMIN_TOKEN_KEY, normalized)
    window.sessionStorage.removeItem(ADMIN_TOKEN_KEY)
  } else {
    window.sessionStorage.setItem(ADMIN_TOKEN_KEY, normalized)
    window.localStorage.removeItem(ADMIN_TOKEN_KEY)
  }
}

export function clearAdminToken() {
  if (typeof window === 'undefined') return
  window.localStorage.removeItem(ADMIN_TOKEN_KEY)
  window.sessionStorage.removeItem(ADMIN_TOKEN_KEY)
}

