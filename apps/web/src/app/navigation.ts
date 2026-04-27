import type { AppPermission } from './permissions'
import { APP_PERMISSION_VALUES, hasPermissionByEffective } from './permissions'
import { migrateLegacyAppPath } from './layers'
import config from './navigation.config.json'

export type NavSurface = 'app' | 'admin'

export type NavItemConfig = {
  to: string
  label: string
  desc: string
  permission: AppPermission
  surface: NavSurface
}

export type NavGroupConfig = {
  id: string
  title: string
  order: number
  surface: NavSurface
  items: NavItemConfig[]
}

type NavigationConfigPayload = {
  version?: string
  source?: string
  groups?: unknown
}

const PERMISSION_SET = new Set<string>(APP_PERMISSION_VALUES)

export function toSurfacePath(rawPath: string): string {
  const path = String(rawPath || '').trim()
  if (!path.startsWith('/')) return path
  const markerIndex = path.search(/[?#]/)
  const pathname = markerIndex >= 0 ? path.slice(0, markerIndex) : path
  const suffix = markerIndex >= 0 ? path.slice(markerIndex) : ''
  const withSuffix = (nextPath: string) => `${nextPath}${suffix}`

  if (pathname === '/dashboard') return withSuffix('/admin/dashboard')
  if (pathname === '/market/conclusion') return withSuffix('/app/desk/market')
  if (pathname === '/macro') return withSuffix('/app/data/macro')
  if (pathname === '/macro/regime') return withSuffix('/app/desk/macro-regime')
  if (pathname === '/research/workbench') return withSuffix('/app/desk/workbench')
  if (pathname === '/research/funnel') return withSuffix('/app/desk/funnel')
  if (pathname === '/research/decision') return withSuffix('/app/desk/board')
  if (pathname === '/portfolio/positions') return withSuffix('/app/desk/positions')
  if (pathname === '/portfolio/orders') return withSuffix('/app/desk/orders')
  if (pathname === '/portfolio/review') return withSuffix('/app/desk/review')
  if (pathname === '/portfolio/allocation') return withSuffix('/app/desk/allocation')
  if (pathname === '/signals/audit') return withSuffix('/admin/system/signals-audit')
  if (pathname === '/signals/quality-config') return withSuffix('/admin/system/signals-quality')
  if (pathname === '/signals/state-timeline') return withSuffix('/admin/system/signals-state-timeline')
  if (pathname.startsWith('/system/')) return withSuffix(`/admin${pathname}`)
  if (pathname.startsWith('/stocks/')) return withSuffix(`/app/data${pathname}`)
  if (pathname.startsWith('/intelligence/')) return withSuffix(`/app/data${pathname}`)
  if (pathname.startsWith('/signals/')) return withSuffix(`/app/data${pathname}`)
  if (pathname.startsWith('/research/')) return withSuffix(migrateLegacyAppPath(`/app${pathname}`))
  if (pathname.startsWith('/portfolio/')) return withSuffix(migrateLegacyAppPath(`/app${pathname}`))
  if (pathname.startsWith('/chatrooms/')) return withSuffix(`/app/data${pathname}`)
  if (pathname.startsWith('/app/')) return withSuffix(migrateLegacyAppPath(pathname))
  return path
}

export function inferNavSurface(rawPath: string): NavSurface {
  const path = toSurfacePath(rawPath)
  if (path.startsWith('/admin/')) return 'admin'
  return 'app'
}

const FALLBACK_NAV_GROUPS: NavGroupConfig[] = [
  {
    id: 'layer1-desk',
    title: '第一层 用户决策',
    order: 1,
    surface: 'app',
    items: [
      { to: '/app/desk/today', label: '今日交易台', desc: '后端生成买卖动作、数量和原因', permission: 'research_advanced', surface: 'app' },
      { to: '/app/desk/orders', label: '计划单', desc: '计划单、执行单、取消单', permission: 'research_advanced', surface: 'app' },
      { to: '/app/desk/positions', label: '持仓看板', desc: '当前纸面持仓与状态', permission: 'research_advanced', surface: 'app' },
      { to: '/app/desk/review', label: '执行复盘', desc: '执行偏差与复盘结论', permission: 'research_advanced', surface: 'app' },
      { to: '/app/desk/macro-regime', label: '三周期状态', desc: '短/中/长期宏观状态与配置语言', permission: 'research_advanced', surface: 'app' },
    ],
  },
  {
    id: 'layer2-data',
    title: '第二层 数据资产',
    order: 2,
    surface: 'app',
    items: [
      { to: '/app/data/scoreboard', label: '评分总览', desc: '宏观模式、行业排序、短名单与入选理由一站式查看', permission: 'research_advanced', surface: 'app' },
      { to: '/app/data/stocks/list', label: '股票列表', desc: '股票基础数据检索', permission: 'stocks_advanced', surface: 'app' },
      { to: '/app/data/stocks/scores', label: '综合评分', desc: '行业内评分与核心指标排序', permission: 'stocks_advanced', surface: 'app' },
      { to: '/app/data/macro', label: '宏观数据', desc: '宏观原始指标与口径', permission: 'macro_advanced', surface: 'app' },
      { to: '/app/data/intelligence', label: '资讯中心', desc: '国际 / 国内 / 个股新闻与日报总结', permission: 'news_read', surface: 'app' },
      { to: '/app/data/signals/overview', label: '投资信号', desc: '股票与主题信号总览', permission: 'signals_advanced', surface: 'app' },
      { to: '/app/data/signals/themes', label: '主题热点', desc: '主题强度、方向、预期与证据链', permission: 'signals_advanced', surface: 'app' },
      { to: '/app/data/signals/graph', label: '产业链图谱', desc: '主题、行业、股票关系浏览', permission: 'signals_advanced', surface: 'app' },
      { to: '/app/data/chatrooms/investment', label: '群聊投资倾向', desc: '群聊结论、情绪和标的清单', permission: 'chatrooms_advanced', surface: 'app' },
    ],
  },
  {
    id: 'layer3-lab',
    title: '第三层 验证与研究',
    order: 3,
    surface: 'app',
    items: [
      { to: '/app/lab/quant-factors', label: '因子与回测', desc: '因子挖掘、组合验证与回测沉淀', permission: 'research_advanced', surface: 'app' },
      { to: '/app/lab/multi-role', label: '多角色研究', desc: '多角色协作研究与论证', permission: 'multi_role_analyze', surface: 'app' },
      { to: '/app/lab/roundtable', label: '首席圆桌', desc: '首席视角下的圆桌讨论与裁决', permission: 'multi_role_analyze', surface: 'app' },
      { to: '/app/lab/trend', label: '趋势分析', desc: '趋势判断与节奏研究', permission: 'trend_analyze', surface: 'app' },
      { to: '/app/lab/reports', label: '研究报告', desc: '研究报告沉淀与查阅', permission: 'research_advanced', surface: 'app' },
      { to: '/app/lab/task-inbox', label: '研究任务收件箱', desc: '研究侧任务进度与状态', permission: 'research_advanced', surface: 'app' },
    ],
  },
  {
    id: 'layer4-admin',
    title: '第四层 后台治理',
    order: 1,
    surface: 'admin',
    items: [
      { to: '/admin/dashboard', label: '系统总控台', desc: '全局健康度、热点、任务与新鲜度', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/source-monitor', label: '数据源监控', desc: '数据源、进程、实时链路统一看板', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/jobs-ops', label: '任务调度中心', desc: '任务列表、dry-run、触发与告警观测', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/agents-ops', label: 'Agent 运营台', desc: 'Agent 运行、审批、步骤与审计追踪', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/agent-governance', label: 'Agent 治理中心', desc: '质量评分、策略闸门、降级与阻断', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/llm-providers', label: 'LLM 节点管理', desc: '模型节点 CRUD、限速配置与联通测试', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/permissions', label: '角色权限策略', desc: '配置 pro/limited/admin 的权限与日配额', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/database-audit', label: '数据库审计', desc: '缺口、重复、未评分、陈旧数据', permission: 'admin_system', surface: 'admin' },
      { to: '/admin/system/signals-audit', label: '信号质量审计', desc: '误映射、弱信号与质量问题', permission: 'signals_advanced', surface: 'admin' },
      { to: '/admin/system/signals-quality', label: '信号质量配置', desc: '规则参数与映射黑名单', permission: 'signals_advanced', surface: 'admin' },
      { to: '/admin/system/signals-state-timeline', label: '状态时间线', desc: '状态机迁移与市场预期层', permission: 'signals_advanced', surface: 'admin' },
      { to: '/admin/system/invites', label: '邀请码管理', desc: '管理员邀请码与账号规模管理', permission: 'admin_users', surface: 'admin' },
      { to: '/admin/system/users', label: '用户与会话', desc: '用户、会话、审计日志管理', permission: 'admin_users', surface: 'admin' },
    ],
  },
]

function normalizeNavGroups(raw: unknown): NavGroupConfig[] {
  if (!Array.isArray(raw)) return []
  const out: NavGroupConfig[] = []
  for (const group of raw) {
    if (!group || typeof group !== 'object') continue
    const g = group as Record<string, unknown>
    const id = String(g.id || '').trim()
    const title = String(g.title || '').trim()
    const order = Number(g.order || 0)
    const rawItems = Array.isArray(g.items) ? g.items : []
    if (!id || !title || !Number.isFinite(order) || rawItems.length === 0) continue
    const groupSurfaceRaw = String(g.surface || '').trim()
    const groupSurface = groupSurfaceRaw === 'admin' || groupSurfaceRaw === 'app' ? (groupSurfaceRaw as NavSurface) : null
    const items = rawItems
      .map((item) => {
        if (!item || typeof item !== 'object') return null
        const it = item as Record<string, unknown>
        const to = toSurfacePath(String(it.to || '').trim())
        const label = String(it.label || '').trim()
        const desc = String(it.desc || '').trim()
        const permission = String(it.permission || '').trim()
        const itemSurfaceRaw = String(it.surface || '').trim()
        const surface =
          itemSurfaceRaw === 'admin' || itemSurfaceRaw === 'app'
            ? (itemSurfaceRaw as NavSurface)
            : groupSurface || inferNavSurface(to)
        if (!to || !label || !permission || !PERMISSION_SET.has(permission)) return null
        return { to, label, desc, permission: permission as AppPermission, surface }
      })
      .filter(Boolean) as NavItemConfig[]
    if (!items.length) continue

    const itemsBySurface: Record<NavSurface, NavItemConfig[]> = { app: [], admin: [] }
    for (const item of items) itemsBySurface[item.surface].push(item)

    for (const surface of ['app', 'admin'] as NavSurface[]) {
      const scopedItems = itemsBySurface[surface]
      if (!scopedItems.length) continue
      const scopedId = groupSurface && groupSurface === surface ? id : `${id}-${surface}`
      out.push({ id: scopedId, title, order: Math.floor(order), surface, items: scopedItems })
    }
  }
  return out.sort((a, b) => a.order - b.order)
}

const configPayload = (config || {}) as NavigationConfigPayload
export const NAV_CONFIG_VERSION = String(configPayload.version || 'local-fallback')
export const NAV_CONFIG_SOURCE = String(configPayload.source || 'repo_default')

export const NAV_GROUPS: NavGroupConfig[] = (() => {
  const normalized = normalizeNavGroups(configPayload.groups)
  if (normalized.length) return normalized
  console.warn('[nav-config] invalid local navigation.config.json, fallback to minimal defaults')
  return FALLBACK_NAV_GROUPS.filter((group) => group.surface !== 'app' || group.id === 'layer1-desk')
})()

export function resolveNavigationGroups(raw: unknown): NavGroupConfig[] {
  const normalized = normalizeNavGroups(raw)
  return normalized.length ? normalized : NAV_GROUPS
}

export function resolveNavigationGroupsForSurface(raw: unknown, surface: NavSurface): NavGroupConfig[] {
  return resolveNavigationGroups(raw).filter((group) => group.surface === surface)
}

export function resolveDefaultLandingPath(options: {
  role?: string | null
  effectivePermissions?: string[] | null
  dynamicNavigationGroups?: unknown
}): string {
  const role = String(options.role || '').trim().toLowerCase()
  const groups = resolveNavigationGroups(options.dynamicNavigationGroups)
  const effectivePermissions = Array.isArray(options.effectivePermissions) ? options.effectivePermissions : []

  if (role !== 'admin') {
    for (const group of groups) {
      for (const item of group.items) {
        if (item.to === '/app/desk/today' && hasPermissionByEffective(effectivePermissions, role, item.permission)) {
          return '/app/desk/today'
        }
        if (item.to === '/app/desk/workbench' && hasPermissionByEffective(effectivePermissions, role, item.permission)) {
          return '/app/desk/workbench'
        }
      }
    }
    if (hasPermissionByEffective(effectivePermissions, role, 'research_advanced')) {
      return '/app/desk/workbench'
    }
  }

  if (role === 'admin' && hasPermissionByEffective(effectivePermissions, role, 'admin_system')) {
    return '/admin/dashboard'
  }

  if (role === 'admin' && hasPermissionByEffective(effectivePermissions, role, 'research_advanced')) {
    return '/app/desk/workbench'
  }

  for (const group of groups) {
    for (const item of group.items) {
      if (hasPermissionByEffective(effectivePermissions, role, item.permission)) {
        return toSurfacePath(item.to)
      }
    }
  }
  return '/app/data/intelligence'
}
