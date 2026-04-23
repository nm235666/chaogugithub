import { createRouter, createWebHistory, type LocationQueryRaw } from 'vue-router'
import { hasPermissionByEffective, type AppPermission } from './permissions'
import { resolveDefaultLandingPath } from './navigation'
import { useAuthStore } from '../stores/auth'

type RouteMetaAuth = {
  auth?: boolean
  guest?: boolean
  permission?: AppPermission
  resolveLanding?: boolean
  surface?: 'app' | 'admin' | 'shared'
}

const RoutePlaceholder = {
  render: () => null,
}

function redirectToSurface(to: { path: string; query: LocationQueryRaw; hash: string }) {
  const path = String(to.path || '').trim()
  let targetPath = path
  if (path === '/dashboard') targetPath = '/admin/dashboard'
  else if (path === '/market/conclusion') targetPath = '/app/market'
  else if (path === '/macro') targetPath = '/app/macro'
  else if (path === '/macro/regime') targetPath = '/app/macro-regime'
  else if (path === '/research/workbench') targetPath = '/app/workbench'
  else if (path === '/research/funnel') targetPath = '/app/funnel'
  else if (path === '/research/decision') targetPath = '/app/decision'
  else if (path === '/portfolio/positions') targetPath = '/app/positions'
  else if (path === '/portfolio/orders') targetPath = '/app/orders'
  else if (path === '/portfolio/review') targetPath = '/app/review'
  else if (path === '/portfolio/allocation') targetPath = '/app/allocation'
  else if (path === '/signals/audit') targetPath = '/admin/system/signals-audit'
  else if (path === '/signals/quality-config') targetPath = '/admin/system/signals-quality'
  else if (path === '/signals/state-timeline') targetPath = '/admin/system/signals-state-timeline'
  else if (path.startsWith('/system/')) targetPath = `/admin${path}`
  else if (path.startsWith('/stocks/')) targetPath = `/app${path}`
  else if (path.startsWith('/intelligence/')) targetPath = `/app${path}`
  else if (path.startsWith('/signals/')) targetPath = `/app${path}`
  else if (path.startsWith('/research/')) targetPath = `/app${path}`
  else if (path.startsWith('/portfolio/')) targetPath = `/app${path}`
  else if (path.startsWith('/chatrooms/')) targetPath = `/app${path}`
  return { path: targetPath, query: to.query, hash: to.hash }
}

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', component: () => import('../pages/auth/LoginPage.vue'), meta: { guest: true, surface: 'shared' } as RouteMetaAuth },
    { path: '/upgrade', component: () => import('../pages/auth/UpgradePage.vue'), meta: { auth: true, permission: 'public', surface: 'shared' } as RouteMetaAuth },
    { path: '/', component: RoutePlaceholder, meta: { resolveLanding: true } as RouteMetaAuth },

    { path: '/app/workbench', component: () => import('../pages/research/ResearchWorkbenchPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/funnel', component: () => import('../pages/research/CandidateFunnelPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/decision', component: () => import('../pages/research/DecisionBoardPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/orders', component: () => import('../pages/portfolio/OrdersPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/positions', component: () => import('../pages/portfolio/PositionsPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/review', component: () => import('../pages/portfolio/ReviewPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/allocation', component: () => import('../pages/portfolio/AllocationPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/market', component: () => import('../pages/market/MarketConclusionPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },

    { path: '/app/stocks/list', component: () => import('../pages/stocks/StocksListPage.vue'), meta: { auth: true, permission: 'stocks_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/stocks/scores', component: () => import('../pages/stocks/StockScoresPage.vue'), meta: { auth: true, permission: 'stocks_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/stocks/detail/:tsCode?', component: () => import('../pages/stocks/StockDetailPage.vue'), props: true, meta: { auth: true, permission: 'stocks_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/stocks/prices', component: () => import('../pages/stocks/PricesPage.vue'), meta: { auth: true, permission: 'stocks_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/macro', component: () => import('../pages/macro/MacroPage.vue'), meta: { auth: true, permission: 'macro_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/macro-regime', component: () => import('../pages/macro/MacroRegimePage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },

    { path: '/app/intelligence/global-news', component: () => import('../pages/intelligence/GlobalNewsPage.vue'), meta: { auth: true, permission: 'news_read', surface: 'app' } as RouteMetaAuth },
    { path: '/app/intelligence/cn-news', component: () => import('../pages/intelligence/CnNewsPage.vue'), meta: { auth: true, permission: 'news_read', surface: 'app' } as RouteMetaAuth },
    { path: '/app/intelligence/stock-news', component: () => import('../pages/intelligence/StockNewsPage.vue'), meta: { auth: true, permission: 'stock_news_read', surface: 'app' } as RouteMetaAuth },
    { path: '/app/intelligence/daily-summaries', component: () => import('../pages/intelligence/DailySummariesPage.vue'), meta: { auth: true, permission: 'daily_summary_read', surface: 'app' } as RouteMetaAuth },

    { path: '/app/signals/overview', component: () => import('../pages/signals/SignalsOverviewPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/signals/themes', component: () => import('../pages/signals/ThemesPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/signals/graph', component: () => import('../pages/signals/SignalChainGraphPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/signals/timeline', component: () => import('../pages/signals/SignalTimelinePage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/admin/system/signals-audit', component: () => import('../pages/signals/SignalAuditPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/signals-quality', component: () => import('../pages/signals/SignalQualityConfigPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/signals-state-timeline', component: () => import('../pages/signals/SignalStateTimelinePage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'admin' } as RouteMetaAuth },

    { path: '/app/research/reports', component: () => import('../pages/research/ReportsPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/research/scoreboard', component: () => import('../pages/research/ScoreboardPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/research/task-inbox', component: () => import('../pages/research/TaskInboxPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/research/quant-factors', component: () => import('../pages/research/QuantFactorsPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/research/multi-role', component: () => import('../pages/research/MultiRoleResearchPage.vue'), meta: { auth: true, permission: 'multi_role_analyze', surface: 'app' } as RouteMetaAuth },
    { path: '/app/research/roundtable', component: () => import('../pages/research/ChiefRoundtablePage.vue'), meta: { auth: true, permission: 'multi_role_analyze', surface: 'app' } as RouteMetaAuth },
    { path: '/app/research/trend', component: () => import('../pages/research/TrendAnalysisPage.vue'), meta: { auth: true, permission: 'trend_analyze', surface: 'app' } as RouteMetaAuth },

    { path: '/app/chatrooms/overview', component: () => import('../pages/chatrooms/ChatroomsOverviewPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/chatrooms/candidates', component: () => import('../pages/chatrooms/ChatroomCandidatesPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/chatrooms/chatlog', component: () => import('../pages/chatrooms/ChatlogPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/chatrooms/investment', component: () => import('../pages/chatrooms/ChatroomInvestmentPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/chatrooms/investment/room', component: () => import('../pages/chatrooms/ChatroomRoomDetailPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app' } as RouteMetaAuth },
    { path: '/app/chatrooms/investment/sender', component: () => import('../pages/chatrooms/ChatroomSenderDetailPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app' } as RouteMetaAuth },

    { path: '/admin/dashboard', component: () => import('../pages/dashboard/DashboardPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/source-monitor', component: () => import('../pages/system/SourceMonitorPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/jobs-ops', component: () => import('../pages/system/JobsOpsPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/llm-providers', component: () => import('../pages/system/LlmProvidersPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/permissions', component: () => import('../pages/system/RolePoliciesPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/database-audit', component: () => import('../pages/system/DatabaseAuditPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/invites', component: () => import('../pages/system/InviteAdminPage.vue'), meta: { auth: true, permission: 'admin_users', surface: 'admin' } as RouteMetaAuth },
    { path: '/admin/system/users', component: () => import('../pages/system/UserAdminPage.vue'), meta: { auth: true, permission: 'admin_users', surface: 'admin' } as RouteMetaAuth },

    { path: '/dashboard', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/stocks/list', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/stocks/scores', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/stocks/detail/:tsCode?', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/stocks/prices', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/stocks/minline', redirect: (to) => redirectToSurface({ path: '/stocks/prices', query: to.query, hash: to.hash }) },
    { path: '/macro', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/macro/regime', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/intelligence/global-news', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/intelligence/cn-news', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/intelligence/stock-news', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/intelligence/daily-summaries', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/overview', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/themes', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/graph', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/timeline', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/audit', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/quality-config', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/signals/state-timeline', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/workbench', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/reports', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/scoreboard', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/funnel', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/decision', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/task-inbox', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/quant-factors', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/multi-role', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/roundtable', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/research/trend', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    {
      path: '/app/themes',
      redirect: (to) => ({ path: '/app/signals/themes', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/stocks',
      redirect: (to) => ({ path: '/app/stocks/list', query: to.query, hash: to.hash }),
    },
    { path: '/market/conclusion', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/portfolio/positions', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/portfolio/orders', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/portfolio/review', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/portfolio/allocation', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/chatrooms/overview', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/chatrooms/candidates', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/chatrooms/chatlog', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/chatrooms/investment', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/chatrooms/investment/room', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/chatrooms/investment/sender', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/source-monitor', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/jobs-ops', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/llm-providers', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/permissions', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/database-audit', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/invites', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },
    { path: '/system/users', redirect: (to) => redirectToSurface({ path: to.path, query: to.query, hash: to.hash }) },

    { path: '/:pathMatch(.*)*', component: RoutePlaceholder, meta: { resolveLanding: true } as RouteMetaAuth },
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
  if ((to.meta as RouteMetaAuth | undefined)?.resolveLanding) {
    return {
      path: resolveDefaultLandingPath({
        role: auth.role,
        effectivePermissions: auth.effectivePermissions,
        dynamicNavigationGroups: auth.dynamicNavigationGroups,
      }),
    }
  }
  if (meta.guest) {
    if (auth.authRequired && auth.isAuthenticated) {
      return {
        path: resolveDefaultLandingPath({
          role: auth.role,
          effectivePermissions: auth.effectivePermissions,
          dynamicNavigationGroups: auth.dynamicNavigationGroups,
        }),
      }
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
  const staticPermission = String(meta.permission || '').trim()
  if (!requiredPermission && auth.rbacDynamicEnforced && !staticPermission) {
    console.warn(`[rbac-dynamic] missing route permission mapping for path=${to.path}, pattern=${matchedPattern}`)
    return { path: '/upgrade', query: { from: to.fullPath } }
  }
  const resolvedPermission = String(requiredPermission || staticPermission || '').trim()
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
