<template>
  <div class="min-h-screen bg-[var(--bg)] px-4 py-10 text-[var(--ink)]">
    <div class="mx-auto max-w-[920px] rounded-[28px] border border-[var(--line)] bg-white p-6 shadow-[var(--shadow)]">
      <div class="text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Zanbo Quant</div>
      <h1 class="mt-2 text-3xl font-extrabold" style="font-family: var(--font-display)">功能升级提示</h1>
      <p class="mt-2 text-sm text-[var(--muted)]">
        当前页面会根据账号的真实权限和额度展示可用能力。若需更多研究、信号或系统能力，请联系管理员升级。
      </p>

      <div
        v-if="blockedContext"
        class="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-4 text-sm text-amber-900"
      >
        <div class="text-xs uppercase tracking-[0.14em] text-amber-700">当前受限任务</div>
        <div class="mt-1 text-base font-bold">{{ blockedContext.title }}</div>
        <div class="mt-2 leading-6">{{ blockedContext.message }}</div>
        <div v-if="blockedContext.required.length" class="mt-3 flex flex-wrap gap-2 text-xs">
          <span class="rounded-full border border-amber-300 bg-white px-3 py-1 font-semibold text-amber-800">
            需要权限：{{ blockedContext.required.join('、') }}
          </span>
          <span class="rounded-full border border-amber-300 bg-white px-3 py-1 font-semibold text-amber-800">
            当前缺失：{{ blockedContext.missing.join('、') || '未知' }}
          </span>
        </div>
        <div class="mt-3 flex flex-wrap gap-2">
          <button class="rounded-2xl border border-amber-300 bg-white px-3 py-2 font-semibold text-amber-900" @click="goBack">
            返回上一页
          </button>
          <RouterLink
            v-for="item in blockedContext.alternatives"
            :key="item.to"
            :to="item.to"
            class="rounded-2xl border border-amber-300 bg-white px-3 py-2 font-semibold text-amber-900"
          >
            {{ item.label }}
          </RouterLink>
        </div>
      </div>

      <div class="mt-5 grid gap-3 md:grid-cols-3">
        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-[var(--muted)]">当前角色</div>
          <div class="mt-2 text-lg font-semibold text-[var(--ink)]">{{ roleLabel }}</div>
          <div class="mt-1 text-sm text-[var(--muted)]">{{ usernameText }}</div>
        </div>
        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-[var(--muted)]">走势分析额度</div>
          <div class="mt-2 text-lg font-semibold text-[var(--ink)]">{{ formatQuota(auth.trend_quota) }}</div>
          <div class="mt-1 text-sm text-[var(--muted)]">{{ quotaHint(auth.trend_quota) }}</div>
        </div>
        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-[var(--muted)]">多角色额度</div>
          <div class="mt-2 text-lg font-semibold text-[var(--ink)]">{{ formatQuota(auth.multi_role_quota) }}</div>
          <div class="mt-1 text-sm text-[var(--muted)]">{{ quotaHint(auth.multi_role_quota) }}</div>
        </div>
      </div>

      <div class="mt-5 grid gap-3 md:grid-cols-2">
        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-sm font-semibold text-[var(--ink)]">已开放模块</div>
          <div class="mt-3 space-y-3">
            <div v-for="group in enabledGroups" :key="`enabled-${group.id}`">
              <div class="text-xs uppercase tracking-[0.14em] text-[var(--muted)]">{{ group.title }}</div>
              <ul class="mt-2 space-y-1 text-sm text-[var(--muted)]">
                <li v-for="item in group.items" :key="item.code">{{ item.label }}</li>
              </ul>
            </div>
            <div v-if="!enabledGroups.length" class="text-sm text-[var(--muted)]">当前没有可展示的开放能力。</div>
          </div>
        </div>

        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-sm font-semibold text-[var(--ink)]">待升级模块</div>
          <div class="mt-3 space-y-3">
            <div v-for="group in lockedGroups" :key="`locked-${group.id}`">
              <div class="text-xs uppercase tracking-[0.14em] text-[var(--muted)]">{{ group.title }}</div>
              <ul class="mt-2 space-y-1 text-sm text-[var(--muted)]">
                <li v-for="item in group.items" :key="item.code">{{ item.label }}</li>
              </ul>
            </div>
            <div v-if="!lockedGroups.length" class="text-sm text-[var(--muted)]">当前账号已拥有全部公开能力。</div>
          </div>
        </div>
      </div>

      <div class="mt-5 rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4 text-sm text-[var(--muted)]">
        <div class="font-semibold text-[var(--ink)]">升级建议</div>
        <div class="mt-2 leading-7">
          若你当前已能完成资讯阅读与基础研究，但仍需股票全景、信号研究、群聊分析或系统运维能力，建议升级到更高权限角色，保持页面入口、研究链路和配额一致。
        </div>
      </div>

      <div class="mt-5 flex flex-wrap gap-2">
        <RouterLink :to="primaryEntry" class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white">{{ primaryEntryLabel }}</RouterLink>
        <RouterLink
          v-if="canUseTrend"
          to="/research/trend"
          class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold text-[var(--ink)]"
        >
          进入走势分析
        </RouterLink>
        <RouterLink
          v-if="canUseMultiRole"
          to="/research/multi-role"
          class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold text-[var(--ink)]"
        >
          进入多角色分析
        </RouterLink>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { resolveDefaultLandingPath } from '../../app/navigation'
import { APP_PERMISSION_VALUES, hasPermissionByEffective } from '../../app/permissions'
import type { AuthStatus } from '../../services/api/auth'
import { useAuthStore } from '../../stores/auth'
import { readQueryString } from '../../shared/utils/urlState'

type QuotaStatus = {
  limit: number | null
  used: number
  remaining: number | null
}

type PermissionViewItem = {
  code: string
  label: string
  group: string
}
type UpgradeBlockedContext = {
  title: string
  required: string[]
  missing: string[]
  message: string
  alternatives: Array<{ to: string; label: string }>
}

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const GROUP_LABELS: Record<string, string> = {
  workspace: '工作台',
  news: '资讯中心',
  research: '深度研究',
  signals: '信号研究',
  sentiment: '舆情监控',
  market: '市场数据',
  system: '系统管理',
}

const PERMISSION_LABELS: Record<string, { label: string; group: string }> = {
  public: { label: '公开访问', group: 'workspace' },
  news_read: { label: '国际/国内资讯', group: 'news' },
  stock_news_read: { label: '个股新闻', group: 'news' },
  daily_summary_read: { label: '新闻日报总结', group: 'news' },
  trend_analyze: { label: 'LLM 股票走势分析', group: 'research' },
  multi_role_analyze: { label: 'LLM 多角色分析', group: 'research' },
  research_advanced: { label: '标准报告 / 决策板 / 因子挖掘', group: 'research' },
  signals_advanced: { label: '投资信号 / 主题热点 / 图谱', group: 'signals' },
  chatrooms_advanced: { label: '群聊分析 / 候选池', group: 'sentiment' },
  stocks_advanced: { label: '股票列表 / 评分 / 详情 / 价格', group: 'market' },
  macro_advanced: { label: '宏观看板', group: 'research' },
  admin_users: { label: '用户 / 邀请码管理', group: 'system' },
  admin_system: { label: '系统监控 / 任务 / 权限策略', group: 'system' },
}

const EMPTY_AUTH_STATUS: AuthStatus = {
  ok: false,
  auth_required: true,
  token_present: false,
  token_valid: false,
  user: null,
  effective_permissions: [],
}

const auth = computed<AuthStatus>(() => authStore.status || EMPTY_AUTH_STATUS)
const role = computed(() => String(authStore.role || '').trim().toLowerCase())
const roleLabel = computed(() => {
  if (role.value === 'admin') return '管理员'
  if (role.value === 'pro') return 'Pro'
  if (role.value === 'limited') return 'Limited'
  return role.value || '未识别角色'
})
const usernameText = computed(() => String(auth.value?.user?.display_name || auth.value?.user?.username || '未登录账号'))
const primaryEntry = computed(() =>
  resolveDefaultLandingPath({
    role: authStore.role,
    effectivePermissions: authStore.effectivePermissions,
    dynamicNavigationGroups: authStore.dynamicNavigationGroups,
  }),
)
const primaryEntryLabel = computed(() => (role.value === 'admin' ? '进入总控台' : '返回可用首页'))
const canUseTrend = computed(() => hasPermissionByEffective(authStore.effectivePermissions, role.value, 'trend_analyze'))
const canUseMultiRole = computed(() => hasPermissionByEffective(authStore.effectivePermissions, role.value, 'multi_role_analyze'))
const blockedFrom = computed(() => readQueryString(route.query as Record<string, unknown>, 'from', ''))

const permissionCatalog = computed(() => {
  const raw = Array.isArray(authStore.permissionCatalog) ? authStore.permissionCatalog : []
  const catalog = raw
    .map((item: any) => {
      const code = String(item?.code || '').trim()
      if (!code || code === 'public') return null
      const fallback = PERMISSION_LABELS[code] || { label: code, group: String(item?.group || 'workspace') }
      return {
        code,
        label: String(item?.label || fallback.label || code),
        group: String(item?.group || fallback.group || 'workspace'),
      }
    })
    .filter(Boolean) as PermissionViewItem[]
  if (catalog.length) return catalog
  return APP_PERMISSION_VALUES
    .filter((code) => code !== 'public')
    .map((code) => ({
      code,
      label: PERMISSION_LABELS[code]?.label || code,
      group: PERMISSION_LABELS[code]?.group || 'workspace',
    }))
})

const enabledGroups = computed(() => groupPermissions(permissionCatalog.value.filter((item) => authStore.effectivePermissions.includes(item.code))))
const lockedGroups = computed(() => groupPermissions(permissionCatalog.value.filter((item) => !authStore.effectivePermissions.includes(item.code))))

const PERMISSION_HINT_LABELS: Record<string, string> = {
  admin_users: '用户与邀请码管理',
  admin_system: '系统管理与运维',
  research_advanced: '高级投研模块',
  signals_advanced: '信号研究与图谱',
  stocks_advanced: '股票全景与评分',
  chatrooms_advanced: '群聊投资分析',
  trend_analyze: '走势分析',
  multi_role_analyze: '多角色研究',
  macro_advanced: '宏观研究',
}

const blockedContext = computed<UpgradeBlockedContext | null>(() => {
  const raw = blockedFrom.value
  if (!raw) return null
  const path = String(raw.split('?')[0] || '').trim()
  if (!path) return null
  const defaultAlternatives = [{ to: primaryEntry.value, label: primaryEntryLabel.value }]
  const newsAlternatives = [{ to: '/intelligence/global-news', label: '先回资讯中心' }, ...defaultAlternatives]
  const stockNewsAlternatives = [{ to: '/intelligence/stock-news', label: '先看个股新闻' }, ...defaultAlternatives]
  const rules: Array<{ test: (pathName: string) => boolean; title: string; required: string[]; alternatives: Array<{ to: string; label: string }> }> = [
    {
      test: (pathName) => pathName === '/dashboard',
      title: '访问总控台',
      required: ['admin_system'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName === '/intelligence/global-news' || pathName === '/intelligence/cn-news',
      title: '访问资讯中心',
      required: ['news_read'],
      alternatives: defaultAlternatives,
    },
    {
      test: (pathName) => pathName === '/intelligence/stock-news',
      title: '访问个股新闻',
      required: ['stock_news_read'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName === '/intelligence/daily-summaries',
      title: '访问新闻日报总结',
      required: ['daily_summary_read'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/system/users') || pathName.startsWith('/system/invites'),
      title: '访问用户与邀请码管理',
      required: ['admin_users'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/system/'),
      title: '访问系统管理模块',
      required: ['admin_system'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/macro'),
      title: '访问宏观研究看板',
      required: ['macro_advanced'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/chatrooms/'),
      title: '访问群聊投资分析',
      required: ['chatrooms_advanced'],
      alternatives: stockNewsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/research/multi-role') || pathName.startsWith('/research/roundtable'),
      title: '访问多角色研究',
      required: ['multi_role_analyze'],
      alternatives: [{ to: '/research/trend', label: '先看走势分析' }, ...defaultAlternatives],
    },
    {
      test: (pathName) => pathName.startsWith('/research/trend'),
      title: '访问走势分析',
      required: ['trend_analyze'],
      alternatives: [{ to: '/research/scoreboard', label: '先看评分总览' }, ...defaultAlternatives],
    },
    {
      test: (pathName) => pathName.startsWith('/research/'),
      title: '访问高级投研能力',
      required: ['research_advanced'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/signals/'),
      title: '访问信号研究与图谱',
      required: ['signals_advanced'],
      alternatives: newsAlternatives,
    },
    {
      test: (pathName) => pathName.startsWith('/stocks/'),
      title: '访问股票高级模块',
      required: ['stocks_advanced'],
      alternatives: stockNewsAlternatives,
    },
  ]
  const matched = rules.find((item) => item.test(path))
  if (!matched) return null
  const required = matched.required
  const missing = required.filter((code) => !authStore.effectivePermissions.includes(code))
  return {
    title: matched.title,
    required: required.map((code) => PERMISSION_HINT_LABELS[code] || code),
    missing: missing.map((code) => PERMISSION_HINT_LABELS[code] || code),
    message: `系统已拦截“${path}”访问，当前账号权限不足。升级后可继续这条业务链路。`,
    alternatives: matched.alternatives,
  }
})

function groupPermissions(items: PermissionViewItem[]) {
  const groups = new Map<string, { id: string; title: string; items: PermissionViewItem[] }>()
  for (const item of items) {
    if (!groups.has(item.group)) {
      groups.set(item.group, {
        id: item.group,
        title: GROUP_LABELS[item.group] || item.group,
        items: [],
      })
    }
    groups.get(item.group)?.items.push(item)
  }
  return Array.from(groups.values())
}

function formatQuota(quota: unknown) {
  const q = quota as QuotaStatus | null | undefined
  if (!q || q.limit == null) return '无限制'
  return `${q.remaining ?? 0} / ${q.limit}`
}

function quotaHint(quota: unknown) {
  const q = quota as QuotaStatus | null | undefined
  if (!q || q.limit == null) return '当前角色没有固定次数限制'
  return `已使用 ${q.used} 次，剩余 ${q.remaining ?? 0} 次`
}

function goBack() {
  router.back()
}

onMounted(async () => {
  if (!authStore.loaded) {
    await authStore.refresh(true)
  }
})
</script>
