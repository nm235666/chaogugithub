import type { AppPermission } from './permissions'
import type { NavSurface } from './navigation'

export type LayerId = 'l1' | 'l2' | 'l3' | 'l4'

export type LayerDef = {
  id: LayerId
  shortLabel: string
  label: string
  description: string
  prefix: string
  defaultPath: string
  permission: AppPermission
  surface: NavSurface
}

export const LAYER_DEFS: LayerDef[] = [
  {
    id: 'l1',
    shortLabel: '决策',
    label: '第一层 用户决策层',
    description: '动作执行、闭环回执与日常决策驾驶舱',
    prefix: '/app/desk',
    defaultPath: '/app/desk/workbench',
    permission: 'research_advanced',
    surface: 'app',
  },
  {
    id: 'l2',
    shortLabel: '数据',
    label: '第二层 数据资产层',
    description: '股票、资讯、信号、群聊与评分等事实输入',
    prefix: '/app/data',
    defaultPath: '/app/data/scoreboard',
    permission: 'research_advanced',
    surface: 'app',
  },
  {
    id: 'l3',
    shortLabel: '研究',
    label: '第三层 验证与研究层',
    description: '因子、回测、多角色与研究沉淀',
    prefix: '/app/lab',
    defaultPath: '/app/lab/quant-factors',
    permission: 'research_advanced',
    surface: 'app',
  },
  {
    id: 'l4',
    shortLabel: '治理',
    label: '第四层 后台治理层',
    description: '任务、数据源、权限、审计与节点治理',
    prefix: '/admin',
    defaultPath: '/admin/dashboard',
    permission: 'admin_system',
    surface: 'admin',
  },
]

export const LAYER_PATH_MIGRATION: Record<string, string> = {
  '/app/workbench': '/app/desk/workbench',
  '/app/funnel': '/app/desk/funnel',
  '/app/decision': '/app/desk/board',
  '/app/orders': '/app/desk/orders',
  '/app/positions': '/app/desk/positions',
  '/app/review': '/app/desk/review',
  '/app/allocation': '/app/desk/allocation',
  '/app/market': '/app/desk/market',
  '/app/macro-regime': '/app/desk/macro-regime',
  '/app/macro': '/app/data/macro',
  '/app/research/scoreboard': '/app/data/scoreboard',
  '/app/research/quant-factors': '/app/lab/quant-factors',
  '/app/research/multi-role': '/app/lab/multi-role',
  '/app/research/roundtable': '/app/lab/roundtable',
  '/app/research/trend': '/app/lab/trend',
  '/app/research/reports': '/app/lab/reports',
  '/app/research/task-inbox': '/app/lab/task-inbox',
}

export const LAYER_PREFIX_MIGRATION: Array<[string, string]> = [
  ['/app/stocks/', '/app/data/stocks/'],
  ['/app/intelligence/', '/app/data/intelligence/'],
  ['/app/signals/', '/app/data/signals/'],
  ['/app/chatrooms/', '/app/data/chatrooms/'],
]

const ADMIN_INTERNAL_SIGNALS = new Set([
  '/admin/system/signals-audit',
  '/admin/system/signals-quality',
  '/admin/system/signals-state-timeline',
])

export function migrateLegacyAppPath(rawPath: string): string {
  const path = String(rawPath || '').trim()
  if (!path) return path
  if (LAYER_PATH_MIGRATION[path]) return LAYER_PATH_MIGRATION[path]
  for (const [oldPrefix, newPrefix] of LAYER_PREFIX_MIGRATION) {
    if (path.startsWith(oldPrefix)) {
      const remainder = path.slice(oldPrefix.length)
      if (oldPrefix === '/app/signals/') {
        const candidate = `/admin/system/signals-${remainder.replace(/\/.*$/, '')}`
        if (ADMIN_INTERNAL_SIGNALS.has(candidate)) return path
      }
      return newPrefix + remainder
    }
  }
  return path
}

export function resolveLayerByPath(rawPath: string): LayerDef | null {
  const cleaned = String(rawPath || '').split(/[?#]/)[0] || ''
  if (!cleaned) return null
  if (cleaned === '/admin' || cleaned.startsWith('/admin/')) return LAYER_DEFS[3]
  for (const def of LAYER_DEFS) {
    if (def.prefix === '/admin') continue
    if (cleaned === def.prefix || cleaned.startsWith(def.prefix + '/')) return def
  }
  return null
}

export function isLayerActive(layerId: LayerId, path: string): boolean {
  const resolved = resolveLayerByPath(path)
  return !!resolved && resolved.id === layerId
}

export function layerForSurface(surface: NavSurface): LayerDef[] {
  return LAYER_DEFS.filter((def) => def.surface === surface)
}

export function getLayerById(id: LayerId): LayerDef | null {
  return LAYER_DEFS.find((def) => def.id === id) || null
}

/** Minimal nav group shape for layer mapping (avoids importing NavGroupConfig from navigation.ts). */
export type NavGroupLayerRef = {
  id: string
  items: Array<{ to: string }>
}

const NAV_GROUP_ID_TO_LAYER: Record<string, LayerId> = {
  'layer1-desk': 'l1',
  'layer2-data': 'l2',
  'layer3-lab': 'l3',
  'layer4-admin': 'l4',
}

export function layerIdForNavGroupId(groupId: string): LayerId | null {
  return NAV_GROUP_ID_TO_LAYER[String(groupId || '').trim()] ?? null
}

export function layerIdForNavGroup(group: NavGroupLayerRef): LayerId | null {
  const byId = layerIdForNavGroupId(group.id)
  if (byId) return byId
  const first = group.items?.[0]?.to
  if (!first) return null
  const pathname = String(first).split(/[?#]/)[0] || ''
  const def = resolveLayerByPath(migrateLegacyAppPath(pathname))
  return def?.id ?? null
}

/** When true, sidebar shows all nav groups expanded (e.g. unknown /app/* path). */
export function sidebarExpandAllGroups(pathname: string): boolean {
  return resolveLayerByPath(pathname) === null
}

export function defaultPathForLayerId(id: LayerId): string {
  return getLayerById(id)?.defaultPath ?? '/app/desk/workbench'
}
