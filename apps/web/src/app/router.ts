import { createRouter, createWebHistory } from 'vue-router'
import { hasPermissionByEffective, type AppPermission } from './permissions'
import { useAuthStore } from '../stores/auth'

type RouteMetaAuth = {
  auth?: boolean
  guest?: boolean
  permission?: AppPermission
}

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', component: () => import('../pages/auth/LoginPage.vue'), meta: { guest: true } as RouteMetaAuth },
    { path: '/upgrade', component: () => import('../pages/auth/UpgradePage.vue'), meta: { auth: true, permission: 'public' } as RouteMetaAuth },
    { path: '/', redirect: '/dashboard' },

    { path: '/dashboard', component: () => import('../pages/dashboard/DashboardPage.vue'), meta: { auth: true, permission: 'admin_system' } as RouteMetaAuth },
    { path: '/stocks/list', component: () => import('../pages/stocks/StocksListPage.vue'), meta: { auth: true, permission: 'stocks_advanced' } as RouteMetaAuth },
    { path: '/stocks/scores', component: () => import('../pages/stocks/StockScoresPage.vue'), meta: { auth: true, permission: 'stocks_advanced' } as RouteMetaAuth },
    { path: '/stocks/detail/:tsCode?', component: () => import('../pages/stocks/StockDetailPage.vue'), props: true, meta: { auth: true, permission: 'stocks_advanced' } as RouteMetaAuth },
    { path: '/stocks/prices', component: () => import('../pages/stocks/PricesPage.vue'), meta: { auth: true, permission: 'stocks_advanced' } as RouteMetaAuth },
    { path: '/stocks/minline', redirect: '/stocks/prices' },
    { path: '/macro', component: () => import('../pages/macro/MacroPage.vue'), meta: { auth: true, permission: 'macro_advanced' } as RouteMetaAuth },

    { path: '/intelligence/global-news', component: () => import('../pages/intelligence/GlobalNewsPage.vue'), meta: { auth: true, permission: 'news_read' } as RouteMetaAuth },
    { path: '/intelligence/cn-news', component: () => import('../pages/intelligence/CnNewsPage.vue'), meta: { auth: true, permission: 'news_read' } as RouteMetaAuth },
    { path: '/intelligence/stock-news', component: () => import('../pages/intelligence/StockNewsPage.vue'), meta: { auth: true, permission: 'stock_news_read' } as RouteMetaAuth },
    { path: '/intelligence/daily-summaries', component: () => import('../pages/intelligence/DailySummariesPage.vue'), meta: { auth: true, permission: 'daily_summary_read' } as RouteMetaAuth },

    { path: '/signals/overview', component: () => import('../pages/signals/SignalsOverviewPage.vue'), meta: { auth: true, permission: 'signals_advanced' } as RouteMetaAuth },
    { path: '/signals/themes', component: () => import('../pages/signals/ThemesPage.vue'), meta: { auth: true, permission: 'signals_advanced' } as RouteMetaAuth },
    { path: '/signals/timeline', component: () => import('../pages/signals/SignalTimelinePage.vue'), meta: { auth: true, permission: 'signals_advanced' } as RouteMetaAuth },
    { path: '/signals/audit', component: () => import('../pages/signals/SignalAuditPage.vue'), meta: { auth: true, permission: 'signals_advanced' } as RouteMetaAuth },
    { path: '/signals/quality-config', component: () => import('../pages/signals/SignalQualityConfigPage.vue'), meta: { auth: true, permission: 'signals_advanced' } as RouteMetaAuth },
    { path: '/signals/state-timeline', component: () => import('../pages/signals/SignalStateTimelinePage.vue'), meta: { auth: true, permission: 'signals_advanced' } as RouteMetaAuth },

    { path: '/research/reports', component: () => import('../pages/research/ReportsPage.vue'), meta: { auth: true, permission: 'research_advanced' } as RouteMetaAuth },
    { path: '/research/quant-factors', component: () => import('../pages/research/QuantFactorsPage.vue'), meta: { auth: true, permission: 'research_advanced' } as RouteMetaAuth },
    { path: '/research/multi-role', component: () => import('../pages/research/MultiRoleResearchPage.vue'), meta: { auth: true, permission: 'multi_role_analyze' } as RouteMetaAuth },
    { path: '/research/trend', component: () => import('../pages/research/TrendAnalysisPage.vue'), meta: { auth: true, permission: 'trend_analyze' } as RouteMetaAuth },

    { path: '/chatrooms/overview', component: () => import('../pages/chatrooms/ChatroomsOverviewPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced' } as RouteMetaAuth },
    { path: '/chatrooms/candidates', component: () => import('../pages/chatrooms/ChatroomCandidatesPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced' } as RouteMetaAuth },
    { path: '/chatrooms/chatlog', component: () => import('../pages/chatrooms/ChatlogPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced' } as RouteMetaAuth },
    { path: '/chatrooms/investment', component: () => import('../pages/chatrooms/ChatroomInvestmentPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced' } as RouteMetaAuth },

    { path: '/system/source-monitor', component: () => import('../pages/system/SourceMonitorPage.vue'), meta: { auth: true, permission: 'admin_system' } as RouteMetaAuth },
    { path: '/system/jobs-ops', component: () => import('../pages/system/JobsOpsPage.vue'), meta: { auth: true, permission: 'admin_system' } as RouteMetaAuth },
    { path: '/system/llm-providers', component: () => import('../pages/system/LlmProvidersPage.vue'), meta: { auth: true, permission: 'admin_system' } as RouteMetaAuth },
    { path: '/system/permissions', component: () => import('../pages/system/RolePoliciesPage.vue'), meta: { auth: true, permission: 'admin_system' } as RouteMetaAuth },
    { path: '/system/database-audit', component: () => import('../pages/system/DatabaseAuditPage.vue'), meta: { auth: true, permission: 'admin_system' } as RouteMetaAuth },
    { path: '/system/invites', component: () => import('../pages/system/InviteAdminPage.vue'), meta: { auth: true, permission: 'admin_users' } as RouteMetaAuth },
    { path: '/system/users', component: () => import('../pages/system/UserAdminPage.vue'), meta: { auth: true, permission: 'admin_users' } as RouteMetaAuth },

    { path: '/:pathMatch(.*)*', redirect: '/dashboard' },
  ],
})

router.beforeEach(async (to) => {
  const auth = useAuthStore()
  const meta = (to.meta || {}) as RouteMetaAuth
  if (!auth.loaded) {
    try {
      await auth.refresh()
    } catch {
      auth.loaded = true
    }
  }
  if (meta.guest) {
    if (auth.authRequired && auth.isAuthenticated) {
      return { path: '/dashboard' }
    }
    return true
  }
  if (!meta.auth) return true
  if (!auth.authRequired) return true
  if (!auth.isAuthenticated) {
    return { path: '/login', query: { redirect: to.fullPath } }
  }
  const matchedPattern = String(to.matched[to.matched.length - 1]?.path || to.path || '').trim()
  const routePermissionMap = auth.dynamicRoutePermissions || {}
  const requiredPermission = String(routePermissionMap[matchedPattern] || routePermissionMap[to.path] || '').trim()
  if (!requiredPermission && auth.rbacDynamicEnforced) {
    console.warn(`[rbac-dynamic] missing route permission mapping for path=${to.path}, pattern=${matchedPattern}`)
    return { path: '/upgrade', query: { from: to.fullPath } }
  }
  const resolvedPermission = String(requiredPermission || meta.permission || '').trim()
  if (!resolvedPermission) {
    console.warn(`[rbac] missing permission mapping for protected route path=${to.path}, pattern=${matchedPattern}`)
    return { path: '/upgrade', query: { from: to.fullPath } }
  }
  const role = auth.role
  if (!hasPermissionByEffective(auth.effectivePermissions, role, resolvedPermission)) {
    return { path: '/upgrade', query: { from: to.fullPath } }
  }
  return true
})
