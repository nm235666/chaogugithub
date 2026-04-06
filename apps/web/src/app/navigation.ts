import type { AppPermission } from './permissions'
import { APP_PERMISSION_VALUES } from './permissions'
import config from './navigation.config.json'

export type NavItemConfig = {
  to: string
  label: string
  desc: string
  permission: AppPermission
}

export type NavGroupConfig = {
  id: string
  title: string
  order: number
  items: NavItemConfig[]
}

type NavigationConfigPayload = {
  version?: string
  source?: string
  groups?: unknown
}

const PERMISSION_SET = new Set<string>(APP_PERMISSION_VALUES)

const FALLBACK_NAV_GROUPS: NavGroupConfig[] = [
  {
    id: 'workspace',
    title: '工作台',
    order: 1,
    items: [{ to: '/dashboard', label: '总控台', desc: '全局健康度、热点、任务与新鲜度', permission: 'admin_system' }],
  },
  {
    id: 'market',
    title: '市场数据',
    order: 2,
    items: [
      { to: '/stocks/list', label: '股票列表', desc: '代码、简称、市场、地区快速检索', permission: 'stocks_advanced' },
      { to: '/stocks/scores', label: '综合评分', desc: '行业内评分与核心指标排序', permission: 'stocks_advanced' },
      { to: '/stocks/detail/000001.SZ', label: '股票详情', desc: '统一聚合价格、新闻、群聊与分析', permission: 'stocks_advanced' },
      { to: '/stocks/prices', label: '价格中心', desc: '日线 + 分钟线统一查询与图表', permission: 'stocks_advanced' },
    ],
  },
  {
    id: 'news',
    title: '资讯中心',
    order: 3,
    items: [
      { to: '/intelligence/global-news', label: '国际资讯', desc: '全球财经新闻、评分与映射', permission: 'news_read' },
      { to: '/intelligence/cn-news', label: '国内资讯', desc: '新浪 / 东财资讯统一看', permission: 'news_read' },
      { to: '/intelligence/stock-news', label: '个股新闻', desc: '聚焦单股新闻与立即采集', permission: 'stock_news_read' },
      { to: '/intelligence/daily-summaries', label: '新闻日报总结', desc: '日报生成、历史查询与双格式导出', permission: 'daily_summary_read' },
    ],
  },
  {
    id: 'signals',
    title: '信号研究',
    order: 4,
    items: [
      { to: '/signals/overview', label: '投资信号', desc: '股票与主题信号总览', permission: 'signals_advanced' },
      { to: '/signals/themes', label: '主题热点', desc: '主题强度、方向、预期与证据链', permission: 'signals_advanced' },
      { to: '/signals/audit', label: '信号质量审计', desc: '误映射、弱信号与质量问题', permission: 'signals_advanced' },
      { to: '/signals/quality-config', label: '信号质量配置', desc: '规则参数与映射黑名单', permission: 'signals_advanced' },
      { to: '/signals/state-timeline', label: '状态时间线', desc: '状态机迁移与市场预期层', permission: 'signals_advanced' },
    ],
  },
  {
    id: 'research',
    title: '深度研究',
    order: 5,
    items: [
      { to: '/macro', label: '宏观看板', desc: '宏观指标查询与序列趋势', permission: 'macro_advanced' },
      { to: '/research/trend', label: '走势分析', desc: 'LLM 股票走势分析工作台', permission: 'trend_analyze' },
      { to: '/research/reports', label: '标准报告', desc: '统一投研报告列表', permission: 'research_advanced' },
      { to: '/research/quant-factors', label: '因子挖掘', desc: 'QuantaAlpha 旁路因子挖掘与回测', permission: 'research_advanced' },
      { to: '/research/multi-role', label: '多角色分析', desc: 'LLM 多角色公司分析工作台', permission: 'multi_role_analyze' },
    ],
  },
  {
    id: 'sentiment',
    title: '舆情监控',
    order: 6,
    items: [
      { to: '/chatrooms/overview', label: '群聊总览', desc: '群聊标签、状态、拉取健康度', permission: 'chatrooms_advanced' },
      { to: '/chatrooms/chatlog', label: '聊天记录', desc: '消息正文、引用和筛选查询', permission: 'chatrooms_advanced' },
      { to: '/chatrooms/investment', label: '投资倾向', desc: '群聊结论、情绪和标的清单', permission: 'chatrooms_advanced' },
      { to: '/chatrooms/candidates', label: '股票候选池', desc: '群聊汇总候选池与偏向', permission: 'chatrooms_advanced' },
    ],
  },
  {
    id: 'system',
    title: '系统管理',
    order: 7,
    items: [
      { to: '/system/source-monitor', label: '数据源监控', desc: '数据源、进程、实时链路统一看板', permission: 'admin_system' },
      { to: '/system/jobs-ops', label: '任务调度中心', desc: '任务列表、dry-run、触发与告警观测', permission: 'admin_system' },
      { to: '/system/llm-providers', label: 'LLM 节点管理', desc: '模型节点 CRUD、限速配置与联通测试', permission: 'admin_system' },
      { to: '/system/permissions', label: '角色权限策略', desc: '配置 pro/limited/admin 的权限与日配额', permission: 'admin_system' },
      { to: '/system/database-audit', label: '数据库审计', desc: '缺口、重复、未评分、陈旧数据', permission: 'admin_system' },
      { to: '/system/invites', label: '邀请码管理', desc: '管理员邀请码与账号规模管理', permission: 'admin_users' },
      { to: '/system/users', label: '用户与会话', desc: '用户、会话、审计日志管理', permission: 'admin_users' },
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
    const items = rawItems
      .map((item) => {
        if (!item || typeof item !== 'object') return null
        const it = item as Record<string, unknown>
        const to = String(it.to || '').trim()
        const label = String(it.label || '').trim()
        const desc = String(it.desc || '').trim()
        const permission = String(it.permission || '').trim()
        if (!to || !label || !permission || !PERMISSION_SET.has(permission)) return null
        return { to, label, desc, permission: permission as AppPermission }
      })
      .filter(Boolean) as NavItemConfig[]
    if (!items.length) continue
    out.push({ id, title, order: Math.floor(order), items })
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
  return FALLBACK_NAV_GROUPS
})()
