import { createRouter, createWebHistory, type LocationQueryRaw } from 'vue-router'
import { hasPermissionByEffective, type AppPermission } from './permissions'
import { resolveDefaultLandingPath } from './navigation'
import { migrateLegacyAppPath, type LayerId } from './layers'
import { useAuthStore } from '../stores/auth'

type RouteMetaAuth = {
  auth?: boolean
  guest?: boolean
  permission?: AppPermission
  resolveLanding?: boolean
  surface?: 'app' | 'admin' | 'shared'
  layer?: LayerId
  /** Nested under IntelligenceHubPage: render body without outer AppShell */
  intelligenceHubChild?: boolean
}

function redirectIntelligenceHubRoot(to: { path: string; fullPath: string }) {
  const normalized = String(to.path || '').replace(/\/$/, '') || ''
  if (normalized !== '/app/data/intelligence') return true
  const auth = useAuthStore()
  const picks: Array<{ seg: string; perm: AppPermission }> = [
    { seg: 'global-news', perm: 'news_read' },
    { seg: 'cn-news', perm: 'news_read' },
    { seg: 'stock-news', perm: 'stock_news_read' },
    { seg: 'daily-summaries', perm: 'daily_summary_read' },
  ]
  for (const { seg, perm } of picks) {
    if (hasPermissionByEffective(auth.effectivePermissions, auth.role, perm)) {
      return `/app/data/intelligence/${seg}`
    }
  }
  return { path: '/upgrade', query: { from: to.fullPath } }
}

const RoutePlaceholder = {
  render: () => null,
}

function redirectToSurface(to: { path: string; query: LocationQueryRaw; hash: string }) {
  const path = String(to.path || '').trim()
  let targetPath = path
  if (path === '/dashboard') targetPath = '/admin/dashboard'
  else if (path === '/market/conclusion') targetPath = '/app/desk/market'
  else if (path === '/macro') targetPath = '/app/data/macro'
  else if (path === '/macro/regime') targetPath = '/app/desk/macro-regime'
  else if (path === '/research/workbench') targetPath = '/app/desk/workbench'
  else if (path === '/research/funnel') targetPath = '/app/desk/funnel'
  else if (path === '/research/decision') targetPath = '/app/desk/board'
  else if (path === '/portfolio/positions') targetPath = '/app/desk/positions'
  else if (path === '/portfolio/orders') targetPath = '/app/desk/orders'
  else if (path === '/portfolio/review') targetPath = '/app/desk/review'
  else if (path === '/portfolio/allocation') targetPath = '/app/desk/allocation'
  else if (path === '/signals/audit') targetPath = '/admin/system/signals-audit'
  else if (path === '/signals/quality-config') targetPath = '/admin/system/signals-quality'
  else if (path === '/signals/state-timeline') targetPath = '/admin/system/signals-state-timeline'
  else if (path.startsWith('/system/')) targetPath = `/admin${path}`
  else if (path.startsWith('/stocks/')) targetPath = `/app/data${path}`
  else if (path.startsWith('/intelligence/')) targetPath = `/app/data${path}`
  else if (path.startsWith('/signals/')) targetPath = `/app/data${path}`
  else if (path.startsWith('/research/')) targetPath = migrateLegacyAppPath(`/app${path}`)
  else if (path.startsWith('/portfolio/')) targetPath = migrateLegacyAppPath(`/app${path}`)
  else if (path.startsWith('/chatrooms/')) targetPath = `/app/data${path}`
  else if (path.startsWith('/app/')) targetPath = migrateLegacyAppPath(path)
  return { path: targetPath, query: to.query, hash: to.hash }
}

const layeredRedirect = (path: string) => ({
  path,
  redirect: (to: { path: string; query: LocationQueryRaw; hash: string }) =>
    redirectToSurface({ path: to.path, query: to.query, hash: to.hash }),
})

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', component: () => import('../pages/auth/LoginPage.vue'), meta: { guest: true, surface: 'shared' } as RouteMetaAuth },
    { path: '/upgrade', component: () => import('../pages/auth/UpgradePage.vue'), meta: { auth: true, permission: 'public', surface: 'shared' } as RouteMetaAuth },
    { path: '/', component: RoutePlaceholder, meta: { resolveLanding: true } as RouteMetaAuth },

    { path: '/app/desk/today', component: () => import('../pages/research/TodayTradingPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/workbench', component: () => import('../pages/research/ResearchWorkbenchPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/funnel', component: () => import('../pages/research/CandidateFunnelPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/board', component: () => import('../pages/research/DecisionBoardPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/orders', component: () => import('../pages/portfolio/OrdersPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/positions', component: () => import('../pages/portfolio/PositionsPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/review', component: () => import('../pages/portfolio/ReviewPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/trade-chain/:orderNo', component: () => import('../pages/portfolio/TradeChainDetailPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/allocation', component: () => import('../pages/portfolio/AllocationPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/market', component: () => import('../pages/market/MarketConclusionPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },
    { path: '/app/desk/macro-regime', component: () => import('../pages/macro/MacroRegimePage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l1' } as RouteMetaAuth },

    { path: '/app/data/scoreboard', component: () => import('../pages/research/ScoreboardPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/stocks/list', component: () => import('../pages/stocks/StocksListPage.vue'), meta: { auth: true, permission: 'stocks_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/stocks/scores', component: () => import('../pages/stocks/StockScoresPage.vue'), meta: { auth: true, permission: 'stocks_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/stocks/detail/:tsCode?', component: () => import('../pages/stocks/StockDetailPage.vue'), props: true, meta: { auth: true, permission: 'stocks_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/stocks/prices', component: () => import('../pages/stocks/PricesPage.vue'), meta: { auth: true, permission: 'stocks_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/macro', component: () => import('../pages/macro/MacroPage.vue'), meta: { auth: true, permission: 'macro_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },

    {
      path: '/app/data/intelligence',
      component: () => import('../pages/intelligence/IntelligenceHubPage.vue'),
      meta: { auth: true, permission: 'public', surface: 'app', layer: 'l2' } as RouteMetaAuth,
      beforeEnter: (to, _from, next) => {
        const r = redirectIntelligenceHubRoot(to)
        if (r === true) {
          next()
          return
        }
        if (typeof r === 'string') {
          next(r)
          return
        }
        next(r)
      },
      children: [
        {
          path: 'global-news',
          component: () => import('../pages/intelligence/GlobalNewsPage.vue'),
          meta: { auth: true, permission: 'news_read', surface: 'app', layer: 'l2', intelligenceHubChild: true } as RouteMetaAuth,
        },
        {
          path: 'cn-news',
          component: () => import('../pages/intelligence/CnNewsPage.vue'),
          meta: { auth: true, permission: 'news_read', surface: 'app', layer: 'l2', intelligenceHubChild: true } as RouteMetaAuth,
        },
        {
          path: 'stock-news',
          component: () => import('../pages/intelligence/StockNewsPage.vue'),
          meta: { auth: true, permission: 'stock_news_read', surface: 'app', layer: 'l2', intelligenceHubChild: true } as RouteMetaAuth,
        },
        {
          path: 'daily-summaries',
          component: () => import('../pages/intelligence/DailySummariesPage.vue'),
          meta: { auth: true, permission: 'daily_summary_read', surface: 'app', layer: 'l2', intelligenceHubChild: true } as RouteMetaAuth,
        },
      ],
    },

    { path: '/app/data/signals/overview', component: () => import('../pages/signals/SignalsOverviewPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/signals/themes', component: () => import('../pages/signals/ThemesPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/signals/graph', component: () => import('../pages/signals/SignalChainGraphPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/signals/timeline', component: () => import('../pages/signals/SignalTimelinePage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },

    { path: '/app/data/chatrooms/overview', component: () => import('../pages/chatrooms/ChatroomsOverviewPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/chatrooms/candidates', component: () => import('../pages/chatrooms/ChatroomCandidatesPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/chatrooms/chatlog', component: () => import('../pages/chatrooms/ChatlogPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/chatrooms/investment', component: () => import('../pages/chatrooms/ChatroomInvestmentPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/chatrooms/investment/room', component: () => import('../pages/chatrooms/ChatroomRoomDetailPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },
    { path: '/app/data/chatrooms/investment/sender', component: () => import('../pages/chatrooms/ChatroomSenderDetailPage.vue'), meta: { auth: true, permission: 'chatrooms_advanced', surface: 'app', layer: 'l2' } as RouteMetaAuth },

    { path: '/app/lab/quant-factors', component: () => import('../pages/research/QuantFactorsPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l3' } as RouteMetaAuth },
    { path: '/app/lab/multi-role', component: () => import('../pages/research/MultiRoleResearchPage.vue'), meta: { auth: true, permission: 'multi_role_analyze', surface: 'app', layer: 'l3' } as RouteMetaAuth },
    { path: '/app/lab/roundtable', component: () => import('../pages/research/ChiefRoundtablePage.vue'), meta: { auth: true, permission: 'multi_role_analyze', surface: 'app', layer: 'l3' } as RouteMetaAuth },
    { path: '/app/lab/trend', component: () => import('../pages/research/TrendAnalysisPage.vue'), meta: { auth: true, permission: 'trend_analyze', surface: 'app', layer: 'l3' } as RouteMetaAuth },
    { path: '/app/lab/reports', component: () => import('../pages/research/ReportsPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l3' } as RouteMetaAuth },
    { path: '/app/lab/task-inbox', component: () => import('../pages/research/TaskInboxPage.vue'), meta: { auth: true, permission: 'research_advanced', surface: 'app', layer: 'l3' } as RouteMetaAuth },

    { path: '/admin/dashboard', component: () => import('../pages/dashboard/DashboardPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/source-monitor', component: () => import('../pages/system/SourceMonitorPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/jobs-ops', component: () => import('../pages/system/JobsOpsPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/agents-ops', component: () => import('../pages/system/AgentOpsPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/agent-governance', component: () => import('../pages/system/AgentGovernancePage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/llm-providers', component: () => import('../pages/system/LlmProvidersPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/permissions', component: () => import('../pages/system/RolePoliciesPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/database-audit', component: () => import('../pages/system/DatabaseAuditPage.vue'), meta: { auth: true, permission: 'admin_system', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/signals-audit', component: () => import('../pages/signals/SignalAuditPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/signals-quality', component: () => import('../pages/signals/SignalQualityConfigPage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/signals-state-timeline', component: () => import('../pages/signals/SignalStateTimelinePage.vue'), meta: { auth: true, permission: 'signals_advanced', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/invites', component: () => import('../pages/system/InviteAdminPage.vue'), meta: { auth: true, permission: 'admin_users', surface: 'admin', layer: 'l4' } as RouteMetaAuth },
    { path: '/admin/system/users', component: () => import('../pages/system/UserAdminPage.vue'), meta: { auth: true, permission: 'admin_users', surface: 'admin', layer: 'l4' } as RouteMetaAuth },

    layeredRedirect('/dashboard'),
    layeredRedirect('/stocks/list'),
    layeredRedirect('/stocks/scores'),
    layeredRedirect('/stocks/detail/:tsCode?'),
    layeredRedirect('/stocks/prices'),
    { path: '/stocks/minline', redirect: (to) => redirectToSurface({ path: '/stocks/prices', query: to.query, hash: to.hash }) },
    layeredRedirect('/macro'),
    layeredRedirect('/macro/regime'),
    layeredRedirect('/intelligence/global-news'),
    layeredRedirect('/intelligence/cn-news'),
    layeredRedirect('/intelligence/stock-news'),
    layeredRedirect('/intelligence/daily-summaries'),
    layeredRedirect('/signals/overview'),
    layeredRedirect('/signals/themes'),
    layeredRedirect('/signals/graph'),
    layeredRedirect('/signals/timeline'),
    layeredRedirect('/signals/audit'),
    layeredRedirect('/signals/quality-config'),
    layeredRedirect('/signals/state-timeline'),
    layeredRedirect('/research/workbench'),
    layeredRedirect('/research/reports'),
    layeredRedirect('/research/scoreboard'),
    layeredRedirect('/research/funnel'),
    layeredRedirect('/research/decision'),
    layeredRedirect('/research/task-inbox'),
    layeredRedirect('/research/quant-factors'),
    layeredRedirect('/research/multi-role'),
    layeredRedirect('/research/roundtable'),
    layeredRedirect('/research/trend'),

    layeredRedirect('/app/workbench'),
    layeredRedirect('/app/funnel'),
    layeredRedirect('/app/decision'),
    layeredRedirect('/app/orders'),
    layeredRedirect('/app/positions'),
    layeredRedirect('/app/review'),
    layeredRedirect('/app/allocation'),
    layeredRedirect('/app/market'),
    layeredRedirect('/app/macro'),
    layeredRedirect('/app/macro-regime'),
    layeredRedirect('/app/stocks/list'),
    layeredRedirect('/app/stocks/scores'),
    layeredRedirect('/app/stocks/detail/:tsCode?'),
    layeredRedirect('/app/stocks/prices'),
    layeredRedirect('/app/intelligence/global-news'),
    layeredRedirect('/app/intelligence/cn-news'),
    layeredRedirect('/app/intelligence/stock-news'),
    layeredRedirect('/app/intelligence/daily-summaries'),
    layeredRedirect('/app/signals/overview'),
    layeredRedirect('/app/signals/themes'),
    layeredRedirect('/app/signals/graph'),
    layeredRedirect('/app/signals/timeline'),
    layeredRedirect('/app/chatrooms/overview'),
    layeredRedirect('/app/chatrooms/candidates'),
    layeredRedirect('/app/chatrooms/chatlog'),
    layeredRedirect('/app/chatrooms/investment'),
    layeredRedirect('/app/chatrooms/investment/room'),
    layeredRedirect('/app/chatrooms/investment/sender'),
    layeredRedirect('/app/research/scoreboard'),
    layeredRedirect('/app/research/quant-factors'),
    layeredRedirect('/app/research/multi-role'),
    layeredRedirect('/app/research/roundtable'),
    layeredRedirect('/app/research/trend'),
    layeredRedirect('/app/research/reports'),
    layeredRedirect('/app/research/task-inbox'),
    {
      path: '/app/themes',
      redirect: (to) => ({ path: '/app/data/signals/themes', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/stocks',
      redirect: (to) => ({ path: '/app/data/stocks/list', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/research/workbench',
      redirect: (to) => ({ path: '/app/desk/workbench', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/research/decision',
      redirect: (to) => ({ path: '/app/desk/board', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/research/funnel',
      redirect: (to) => ({ path: '/app/desk/funnel', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/portfolio/orders',
      redirect: (to) => ({ path: '/app/desk/orders', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/portfolio/positions',
      redirect: (to) => ({ path: '/app/desk/positions', query: to.query, hash: to.hash }),
    },
    {
      path: '/app/portfolio/review',
      redirect: (to) => ({ path: '/app/desk/review', query: to.query, hash: to.hash }),
    },
    layeredRedirect('/market/conclusion'),
    layeredRedirect('/portfolio/positions'),
    layeredRedirect('/portfolio/orders'),
    layeredRedirect('/portfolio/review'),
    layeredRedirect('/portfolio/allocation'),
    layeredRedirect('/chatrooms/overview'),
    layeredRedirect('/chatrooms/candidates'),
    layeredRedirect('/chatrooms/chatlog'),
    layeredRedirect('/chatrooms/investment'),
    layeredRedirect('/chatrooms/investment/room'),
    layeredRedirect('/chatrooms/investment/sender'),
    layeredRedirect('/system/source-monitor'),
    layeredRedirect('/system/jobs-ops'),
    layeredRedirect('/system/llm-providers'),
    layeredRedirect('/system/permissions'),
    layeredRedirect('/system/database-audit'),
    layeredRedirect('/system/invites'),
    layeredRedirect('/system/users'),

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
