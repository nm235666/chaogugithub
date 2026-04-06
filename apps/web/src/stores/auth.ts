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
    role: (state) => String(state.status?.user?.role || state.status?.user?.tier || '').trim().toLowerCase(),
    effectivePermissions: (state) => (Array.isArray(state.status?.effective_permissions) ? state.status?.effective_permissions || [] : []),
    dynamicRoutePermissions: (state) =>
      state.status?.dynamic_rbac?.route_permissions && typeof state.status.dynamic_rbac.route_permissions === 'object'
        ? state.status.dynamic_rbac.route_permissions
        : {},
    permissionCatalog: (state) => (Array.isArray(state.status?.dynamic_rbac?.permission_catalog) ? state.status?.dynamic_rbac?.permission_catalog || [] : []),
    dynamicNavigationGroups: (state) => (Array.isArray(state.status?.dynamic_rbac?.navigation_groups) ? state.status?.dynamic_rbac?.navigation_groups || [] : []),
    rbacDynamicVersion: (state) => String(state.status?.rbac_dynamic_version || state.status?.dynamic_rbac?.version || 'unknown'),
    rbacDynamicSource: (state) => String(state.status?.rbac_dynamic_source || state.status?.dynamic_rbac?.source || 'unknown'),
    rbacDynamicEnforced: (state) => Boolean(state.status?.rbac_dynamic_enforced),
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
