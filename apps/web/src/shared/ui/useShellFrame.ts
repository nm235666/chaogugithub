import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { clearAuthStatusCache, logoutAuth } from '../../services/api/auth'
import { clearAdminToken, readAdminToken } from '../../services/authToken'
import { fetchNavigationGroups } from '../../services/api/system'
import { hasPermissionByEffective } from '../../app/permissions'
import { NAV_GROUPS, resolveNavigationGroups, type NavGroupConfig, type NavSurface } from '../../app/navigation'
import { describeCrossModeContext, extractCrossModeContext } from '../../app/crossModeContext'
import {
  defaultPathForLayerId,
  layerIdForNavGroup,
  resolveLayerByPath,
  sidebarExpandAllGroups,
} from '../../app/layers'
import { useAuthStore } from '../../stores/auth'
import { useRealtimeStore } from '../../stores/realtime'
import { useUiStore } from '../../stores/ui'

export function useShellFrame(surface: NavSurface) {
  const ui = useUiStore()
  const realtime = useRealtimeStore()
  const auth = useAuthStore()
  const route = useRoute()
  const router = useRouter()
  const hasAdminToken = ref(!!readAdminToken())
  const remoteNavGroups = ref<NavGroupConfig[] | null>(null)

  const navGroups = computed(() => {
    const source = remoteNavGroups.value?.length ? remoteNavGroups.value : NAV_GROUPS
    return [...source]
      .filter((group) => group.surface === surface)
      .sort((a, b) => a.order - b.order)
      .map((group) => ({
        ...group,
        items: group.items.filter((item) => hasPermissionByEffective(auth.effectivePermissions, auth.role, item.permission)),
      }))
      .filter((group) => group.items.length > 0)
  })

  const shellModeLabel = computed(() => (surface === 'app' ? '研究工作台' : '后台管理台'))
  const shellModeDescription = computed(() =>
    surface === 'app' ? '今日任务、结论、动作与研究输入' : '监控、配置、任务、权限与审计',
  )
  const shellBadge = computed(() => (surface === 'app' ? 'User Mode' : 'Admin Mode'))
  const canSwitchToAdmin = computed(
    () => surface === 'app' && auth.role === 'admin' && hasPermissionByEffective(auth.effectivePermissions, auth.role, 'admin_system'),
  )
  const canSwitchToApp = computed(
    () => surface === 'admin' && hasPermissionByEffective(auth.effectivePermissions, auth.role, 'research_advanced'),
  )
  const canSwitchMode = computed(() => canSwitchToAdmin.value || canSwitchToApp.value)
  const targetSurface = computed<NavSurface | ''>(() => {
    if (canSwitchToAdmin.value) return 'admin'
    if (canSwitchToApp.value) return 'app'
    return ''
  })
  const switchLabel = computed(() => {
    if (canSwitchToAdmin.value) return '进入后台管理'
    if (canSwitchToApp.value) return '回到研究工作台'
    return ''
  })
  const sidebarOpen = computed(() => ui.isSidebarOpen(surface))
  const sidebarWidthClass = computed(() => (sidebarOpen.value ? 'w-[296px]' : 'w-[104px]'))

  function isNavActive(to: string): boolean {
    const targetPath = String(to || '').split('?')[0] || ''
    if (!targetPath) return false
    if (route.path === targetPath) return true
    if (
      targetPath === '/app/data/stocks/list' ||
      targetPath === '/app/data/stocks/detail' ||
      targetPath.startsWith('/app/data/stocks/detail/')
    ) {
      return (
        route.path.startsWith('/app/data/stocks/detail') ||
        route.path.startsWith('/app/stocks/detail') ||
        route.path.startsWith('/stocks/detail')
      )
    }
    if (targetPath.startsWith('/app/data/chatrooms/investment')) {
      return route.path.startsWith('/app/data/chatrooms/investment')
    }
    return false
  }

  function isNavGroupExpanded(group: NavGroupConfig): boolean {
    if (sidebarExpandAllGroups(route.path)) return true
    const gid = layerIdForNavGroup(group)
    const cur = resolveLayerByPath(route.path)
    if (!gid || !cur) return true
    return gid === cur.id
  }

  function navGroupLayerDefaultPath(group: NavGroupConfig): string {
    const gid = layerIdForNavGroup(group)
    if (gid) return defaultPathForLayerId(gid)
    return group.items[0]?.to || (surface === 'admin' ? '/admin/dashboard' : '/app/desk/workbench')
  }

  async function switchMode() {
    const target = targetSurface.value
    if (!target) return
    ui.closeMobileNav()
    const ctx = extractCrossModeContext(route.query as Record<string, unknown>, target as NavSurface)
    await router.push({ path: ctx.targetPath, query: ctx.preservedQuery })
    ui.showToast(describeCrossModeContext(ctx, target as NavSurface), 'info')
  }

  async function logout() {
    try {
      await logoutAuth()
    } catch {
      // ignore logout API errors; local clear is enough for UX
    }
    clearAdminToken()
    clearAuthStatusCache()
    auth.status = null
    auth.loaded = false
    hasAdminToken.value = false
    router.replace('/login')
  }

  watch(
    () => route.fullPath,
    () => {
      ui.closeMobileNav()
    },
  )

  onMounted(async () => {
    try {
      const payload = await fetchNavigationGroups()
      const rawGroups = Array.isArray(payload?.groups) ? payload.groups : []
      const hasExplicitSurface = rawGroups.some((group) => {
        if (!group || typeof group !== 'object') return false
        const value = String((group as Record<string, unknown>).surface || '').trim()
        return value === 'app' || value === 'admin'
      })
      if ((payload?.validation?.invalid_groups || 0) > 0 || (payload?.validation?.invalid_items || 0) > 0) {
        console.warn(
          `[nav-config] server validation invalid_groups=${payload?.validation?.invalid_groups || 0}, invalid_items=${payload?.validation?.invalid_items || 0}`,
        )
      }
      const normalized = resolveNavigationGroups(payload?.groups)
      if (hasExplicitSurface && normalized.length) {
        remoteNavGroups.value = normalized
      } else if (!hasExplicitSurface) {
        console.warn('[nav-config] server payload missing dual-mode surface metadata, fallback to local defaults')
      } else {
        console.warn('[nav-config] server payload has no usable groups, fallback to local defaults')
      }
    } catch {
      // Keep local NAV_GROUPS when server config is unavailable.
    }
  })

  return {
    auth,
    realtime,
    ui,
    hasAdminToken,
    navGroups,
    shellModeLabel,
    shellModeDescription,
    shellBadge,
    canSwitchMode,
    switchLabel,
    switchMode,
    sidebarOpen,
    sidebarWidthClass,
    isNavActive,
    isNavGroupExpanded,
    navGroupLayerDefaultPath,
    logout,
  }
}
