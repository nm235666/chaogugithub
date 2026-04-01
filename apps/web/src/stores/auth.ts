import { defineStore } from 'pinia'
import { clearAuthStatusCache, fetchAuthStatus, type AuthStatus } from '../services/api/auth'

type AuthState = {
  status: AuthStatus | null
  loaded: boolean
}

export const useAuthStore = defineStore('auth', {
  state: (): AuthState => ({
    status: null,
    loaded: false,
  }),
  getters: {
    user: (state) => state.status?.user || null,
    role: (state) => String(state.status?.user?.role || state.status?.user?.tier || ''),
    effectivePermissions: (state) => (Array.isArray(state.status?.effective_permissions) ? state.status?.effective_permissions || [] : []),
    isAuthenticated: (state) => Boolean(state.status?.token_valid),
    authRequired: (state) => Boolean(state.status?.auth_required),
    isAdmin(): boolean {
      return this.role === 'admin'
    },
  },
  actions: {
    async refresh(force = false) {
      if (force) clearAuthStatusCache()
      this.status = await fetchAuthStatus(force)
      this.loaded = true
      return this.status
    },
  },
})
