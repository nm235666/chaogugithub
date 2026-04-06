export type AppPermission = string

export const APP_PERMISSION_VALUES: string[] = [
  'public',
  'news_read',
  'stock_news_read',
  'daily_summary_read',
  'trend_analyze',
  'multi_role_analyze',
  'admin_users',
  'admin_system',
  'research_advanced',
  'signals_advanced',
  'chatrooms_advanced',
  'stocks_advanced',
  'macro_advanced',
]

export function hasPermissionByEffective(effective: string[] | null | undefined, role: string, permission?: AppPermission | null): boolean {
  const _role = String(role || '').trim().toLowerCase()
  void _role
  if (!permission || permission === 'public') return true
  const list = Array.isArray(effective) ? effective : []
  if (list.includes('*') || list.includes(String(permission))) return true
  return false
}
