import { NAV_CONFIG_VERSION, NAV_GROUPS } from './navigation'

export type PermissionGroupView = {
  id: string
  label: string
  description: string
  permissions: string[]
}

const GROUP_DESCRIPTIONS: Record<string, string> = {
  workspace: '总控入口与管理员总览',
  market: '股票列表、综合评分、股票详情、价格中心',
  news: '国际资讯、国内资讯、个股新闻、新闻日报总结',
  signals: '信号总览、主题热点、质量审计与配置',
  research: '宏观、走势分析、标准报告、因子挖掘、多角色分析',
  sentiment: '群聊总览、聊天记录、投资倾向、候选池',
  system: '用户与会话、邀请码、系统管理能力',
}

function uniquePermissions(permissions: string[]) {
  return Array.from(new Set(permissions.filter((item) => String(item || '').trim())))
}

export const PERMISSION_GROUP_VIEWS: PermissionGroupView[] = NAV_GROUPS.map((group) => ({
  id: group.id,
  label: group.title,
  description: GROUP_DESCRIPTIONS[group.id] || `${group.title}相关权限`,
  permissions: uniquePermissions(group.items.map((item) => item.permission)),
}))

export const PERMISSION_GROUP_VERSION = NAV_CONFIG_VERSION
