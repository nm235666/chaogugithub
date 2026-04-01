export type AppPermission =
  | 'public'
  | 'news_read'
  | 'stock_news_read'
  | 'daily_summary_read'
  | 'trend_analyze'
  | 'multi_role_analyze'
  | 'admin_users'
  | 'admin_system'
  | 'research_advanced'
  | 'signals_advanced'
  | 'chatrooms_advanced'
  | 'stocks_advanced'
  | 'macro_advanced'

const ROLE_PERMS: Record<string, Set<AppPermission | '*'> > = {
  admin: new Set<AppPermission | '*'> (['*']),
  pro: new Set<AppPermission | '*'> ([
    'news_read',
    'stock_news_read',
    'daily_summary_read',
    'trend_analyze',
    'multi_role_analyze',
    'research_advanced',
    'signals_advanced',
    'chatrooms_advanced',
    'stocks_advanced',
    'macro_advanced',
  ]),
  limited: new Set<AppPermission | '*'> ([
    'news_read',
    'stock_news_read',
    'daily_summary_read',
    'trend_analyze',
    'multi_role_analyze',
  ]),
}

export function hasPermission(role: string, permission?: AppPermission | null): boolean {
  if (!permission || permission === 'public') return true
  const perms = ROLE_PERMS[String(role || '').toLowerCase()] || new Set<AppPermission | '*'>()
  if (perms.has('*')) return true
  return perms.has(permission)
}

export function hasPermissionByEffective(effective: string[] | null | undefined, role: string, permission?: AppPermission | null): boolean {
  if (!permission || permission === 'public') return true
  const list = Array.isArray(effective) ? effective : []
  if (list.includes('*') || list.includes(String(permission))) return true
  return hasPermission(role, permission)
}
