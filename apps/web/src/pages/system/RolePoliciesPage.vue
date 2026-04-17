<template>
  <AppShell title="角色权限策略" subtitle="配置 admin / pro / limited 的可访问权限与每日配额，保存后立即生效。">
    <div class="space-y-4">
      <PageSection title="全局操作" subtitle="可刷新当前策略，或一键恢复默认基线。">
        <div class="flex flex-wrap gap-2">
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="refreshPolicies">
            刷新策略
          </button>
          <button class="rounded-2xl bg-stone-800 px-4 py-3 text-sm font-semibold text-white disabled:opacity-40" :disabled="resetPending" @click="resetDefaults">
            {{ resetPending ? '恢复中...' : '恢复默认策略' }}
          </button>
        </div>
        <div v-if="message" class="mt-3 text-sm text-[var(--muted)]">{{ message }}</div>
        <div
          v-if="actionFeedback.text"
          class="mt-3 rounded-[18px] border px-4 py-3 text-sm"
          :class="actionFeedbackClass"
        >
          <div class="font-semibold">最近写操作反馈</div>
          <div class="mt-1">{{ actionFeedback.text }}</div>
          <div v-if="actionFeedback.confirmedAt" class="mt-1 text-xs opacity-80">
            最近确认时间：{{ actionFeedback.confirmedAt }}
          </div>
          <div v-if="actionFeedback.tone === 'error'" class="mt-2 text-xs opacity-80">
            建议：核查策略配置是否合法；如持续失败，请前往"数据源监控"页排查后台状态。
          </div>
        </div>
      </PageSection>

      <PageSection title="角色配置" subtitle="每个角色独立配置权限集合与日配额（留空表示不限）。">
        <div class="mb-4 rounded-[18px] border border-[rgba(15,97,122,0.16)] bg-[rgba(15,97,122,0.06)] px-4 py-3 text-sm text-[var(--muted)]">
          admin 独占管理员权限与全量访问能力；pro、limited 只允许配置研究与阅读类权限，保存时会自动剔除越权项。
        </div>
        <div class="mb-4 rounded-[18px] border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm text-[var(--muted)]">
          <div class="font-semibold text-[var(--ink)]">分组与权限码映射</div>
          <div class="mt-1 text-xs">映射版本：{{ mappingVersion }}</div>
          <div class="mt-2 grid gap-2 md:grid-cols-2">
            <div v-for="group in permissionGroups" :key="`mapping-${group.id}`" class="rounded-xl border border-[var(--line)] bg-white px-3 py-2">
              <div class="font-semibold text-[var(--ink)]">{{ group.label }}</div>
              <div class="mt-1 text-xs">{{ group.permissions.join(' / ') }}</div>
            </div>
          </div>
        </div>
        <div v-if="policyWarnings.length" class="mb-4 rounded-[18px] border border-[rgba(214,134,72,0.28)] bg-[rgba(214,134,72,0.08)] px-4 py-3 text-sm text-[var(--muted)]">
          <div class="font-semibold text-[var(--ink)]">发现越权权限</div>
          <div class="mt-1">
            当前策略含 {{ policyWarnings.length }} 项越权权限，已在编辑视图中隔离，不会随保存再次写回。
          </div>
        </div>
        <div class="space-y-4">
          <div
            v-for="role in roleOrder"
            :key="role"
            class="rounded-[20px] border border-[var(--line)] bg-white p-4 shadow-[var(--shadow-soft)]"
          >
            <div class="flex items-center justify-between gap-2">
              <div class="text-lg font-bold">{{ role }}</div>
              <button class="rounded-2xl bg-[var(--brand)] px-3 py-2 text-sm font-semibold text-white disabled:opacity-40" :disabled="!!roleActionPending[role]" @click="saveRole(role)">
                {{ roleActionPending[role] ? '保存中...' : `保存 ${role}` }}
              </button>
            </div>
            <div class="mt-3 grid gap-3 md:grid-cols-2">
              <label class="text-sm text-[var(--muted)]">
                走势日配额（trend）
                <input
                  v-model.trim="drafts[role].trend_daily_limit_text"
                  class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-3 py-2"
                  placeholder="留空表示不限"
                />
              </label>
              <label class="text-sm text-[var(--muted)]">
                多角色日配额（multi-role）
                <input
                  v-model.trim="drafts[role].multi_role_daily_limit_text"
                  class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-3 py-2"
                  placeholder="留空表示不限"
                />
              </label>
            </div>
            <div class="mt-3 text-sm font-semibold">权限项</div>
            <div v-if="role === 'admin'" class="mt-2 rounded-xl border border-[var(--line)] px-3 py-3 text-sm">
              <div class="flex items-center gap-2">
                <input type="checkbox" :checked="true" disabled />
                <span class="font-semibold">*</span>
                <span class="text-[var(--muted)]">admin 全量权限（只读）</span>
              </div>
            </div>
            <div v-else class="mt-2 space-y-3">
              <div
                v-for="group in permissionGroupsForRole(role)"
                :key="`${role}-${group.id}`"
                class="rounded-xl border border-[var(--line)] px-3 py-3"
              >
                <div class="mb-2">
                  <div class="text-sm font-semibold text-[var(--ink)]">{{ group.label }}</div>
                  <div class="text-xs text-[var(--muted)]">{{ group.description }}</div>
                </div>
                <div class="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                  <label
                    v-for="perm in group.permissions"
                    :key="`${role}-${group.id}-${perm}`"
                    class="flex items-center gap-2 rounded-xl border border-[var(--line)] px-3 py-2 text-sm"
                  >
                    <input
                      type="checkbox"
                      :checked="drafts[role].permissions.has(perm)"
                      @change="togglePermission(role, perm)"
                    />
                    <span>{{ perm }}</span>
                  </label>
                </div>
              </div>
            </div>
            <div v-if="invalidPermissionsByRole[role]?.length" class="mt-3 rounded-[16px] border border-[rgba(214,134,72,0.28)] bg-[rgba(214,134,72,0.06)] px-3 py-3 text-sm text-[var(--muted)]">
              已隔离的越权权限：{{ invalidPermissionsByRole[role].length }} 项
            </div>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import { fetchAuthRolePolicies, fetchNavigationGroups, resetAuthRolePoliciesToDefault, updateAuthRolePolicy, type AuthRolePolicy } from '../../services/api/system'
import { useAuthStore } from '../../stores/auth'
import { useUiStore } from '../../stores/ui'

const message = ref('')
const roleOrder = ['admin', 'pro', 'limited']
const auth = useAuthStore()
const ui = useUiStore()
const permissionGroups = ref<Array<{ id: string; label: string; description: string; permissions: string[] }>>([])
const mappingVersion = ref('server:unknown')
const resetPending = ref(false)
const roleActionPending = reactive<Record<string, boolean>>({})
const actionFeedback = reactive<{ text: string; tone: 'success' | 'error' | 'info'; confirmedAt: string }>({
  text: '',
  tone: 'info',
  confirmedAt: '',
})
const allPermissions = ref<string[]>([
  '*',
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
  'admin_users',
  'admin_system',
])
const rolePermissionAllowlist = reactive<Record<string, string[]>>({
  admin: ['*'],
  pro: [],
  limited: [],
})

type RoleDraft = {
  permissions: Set<string>
  trend_daily_limit_text: string
  multi_role_daily_limit_text: string
}

const drafts = reactive<Record<string, RoleDraft>>({
  admin: { permissions: new Set(['*']), trend_daily_limit_text: '', multi_role_daily_limit_text: '' },
  pro: { permissions: new Set(), trend_daily_limit_text: '', multi_role_daily_limit_text: '' },
  limited: { permissions: new Set(), trend_daily_limit_text: '', multi_role_daily_limit_text: '' },
})
const invalidPermissionsByRole = reactive<Record<string, string[]>>({
  admin: [],
  pro: [],
  limited: [],
})

const policyWarnings = computed(() =>
  roleOrder.flatMap((role) => (invalidPermissionsByRole[role] || []).map((perm) => `${role}：${perm}`)),
)
const actionFeedbackClass = computed(() => {
  if (actionFeedback.tone === 'success') return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  if (actionFeedback.tone === 'error') return 'border-red-200 bg-red-50 text-red-700'
  return 'border-[var(--line)] bg-[rgba(255,255,255,0.72)] text-[var(--muted)]'
})

function resolveActionError(error: any) {
  return String(error?.response?.data?.error || error?.message || 'unknown')
}

function setActionFeedback(messageText: string, tone: 'success' | 'error' | 'info' = 'info') {
  actionFeedback.text = messageText
  actionFeedback.tone = tone
  actionFeedback.confirmedAt = new Date().toLocaleString('zh-CN', { hour12: false })
}

function permissionsForRole(role: string) {
  return rolePermissionAllowlist[role] || allPermissions.value
}

function permissionGroupsForRole(role: string) {
  const allowlist = new Set(permissionsForRole(role))
  return permissionGroups.value
    .map((group) => ({
      ...group,
      permissions: group.permissions.filter((perm) => allowlist.has(perm)),
    }))
    .filter((group) => group.permissions.length > 0)
}

function applyRolePolicy(item: AuthRolePolicy) {
  const role = String(item.role || '').toLowerCase()
  if (!role || !drafts[role]) return
  const allowlist = new Set(permissionsForRole(role))
  const incoming = Array.isArray(item.permissions) ? item.permissions.map((x) => String(x || '').trim()).filter(Boolean) : []
  const invalid = role === 'admin' ? [] : incoming.filter((perm) => !allowlist.has(perm))
  invalidPermissionsByRole[role] = invalid
  drafts[role].permissions = new Set(role === 'admin' ? ['*'] : incoming.filter((perm) => allowlist.has(perm)))
  drafts[role].trend_daily_limit_text = item.trend_daily_limit == null ? '' : String(item.trend_daily_limit)
  drafts[role].multi_role_daily_limit_text = item.multi_role_daily_limit == null ? '' : String(item.multi_role_daily_limit)
}

function parseLimit(text: string): number | null {
  const raw = String(text || '').trim()
  if (!raw) return null
  const num = Number(raw)
  if (!Number.isFinite(num) || num < 0) throw new Error(`配额必须是非负整数，当前输入=${raw}`)
  return Math.floor(num)
}

function serializePermissions(role: string): string[] {
  if (role === 'admin') return ['*']
  const allowlist = new Set(permissionsForRole(role))
  const values = Array.from(drafts[role].permissions)
  return values.filter((x) => x !== '*' && allowlist.has(x)).sort()
}

function togglePermission(role: string, perm: string) {
  if (!drafts[role]) return
  if (role === 'admin') {
    drafts[role].permissions = new Set(['*'])
    return
  }
  const allowlist = new Set(permissionsForRole(role))
  if (perm === '*' || !allowlist.has(perm)) return
  const next = new Set(drafts[role].permissions)
  if (next.has(perm)) next.delete(perm)
  else next.add(perm)
  drafts[role].permissions = next
}

const { refetch } = useQuery({
  queryKey: ['auth-role-policies'],
  queryFn: async () => {
    const authData = await fetchAuthRolePolicies()
    await auth.refresh(true)
    const dynamic = auth.status?.dynamic_rbac || {}
    const catalog = Array.isArray(dynamic.permission_catalog) ? dynamic.permission_catalog : []
    const catalogCodes = catalog
      .map((item) => String(item?.code || '').trim())
      .filter(Boolean)
    const systemReserved = new Set(
      catalog
        .filter((item: any) => Boolean(item?.system_reserved))
        .map((item: any) => String(item?.code || '').trim())
        .filter(Boolean),
    )
    if (catalogCodes.length) {
      allPermissions.value = ['*', ...catalogCodes]
    }
    const matrix = auth.status?.permission_matrix || {}
    rolePermissionAllowlist.admin = ['*']
    rolePermissionAllowlist.pro = Array.isArray(matrix.pro)
      ? matrix.pro.filter((perm) => perm !== '*' && !systemReserved.has(perm))
      : catalogCodes.filter((perm) => !systemReserved.has(perm))
    rolePermissionAllowlist.limited = Array.isArray(matrix.limited)
      ? matrix.limited.filter((perm) => perm !== '*' && !systemReserved.has(perm))
      : catalogCodes.filter((perm) => !systemReserved.has(perm))
    const navigationGroups = Array.isArray(dynamic.navigation_groups) ? dynamic.navigation_groups : []
    const nextGroups: Array<{ id: string; label: string; description: string; permissions: string[] }> = navigationGroups
      .map((group: any) => {
        const items = Array.isArray(group?.items) ? group.items : []
        const permissions = Array.from(
          new Set(items.map((item: any) => String(item?.permission || '').trim()).filter(Boolean)),
        ) as string[]
        return {
          id: String(group?.id || '').trim(),
          label: String(group?.title || '').trim(),
          description: `${String(group?.title || '').trim()}相关权限`,
          permissions,
        }
      })
      .filter((group: any) => group.id && group.label && group.permissions.length > 0)
    if (nextGroups.length) {
      permissionGroups.value = nextGroups
    }
    try {
      const navPayload = await fetchNavigationGroups()
      mappingVersion.value = `${String(navPayload?.source || 'server')}:${String(navPayload?.version || '-')}`
    } catch {
      mappingVersion.value = `${String(dynamic.source || 'server')}:${String(dynamic.version || '-')}`
    }
    for (const role of roleOrder) {
      if (!drafts[role]) {
        drafts[role] = { permissions: new Set(), trend_daily_limit_text: '', multi_role_daily_limit_text: '' }
      }
    }
    for (const item of authData.roles || []) applyRolePolicy(item)
    message.value = `策略来源：${authData.effective_source || 'db'}`
    return authData
  },
})

async function saveRole(role: string) {
  roleActionPending[role] = true
  try {
    await updateAuthRolePolicy({
      role,
      permissions: serializePermissions(role),
      trend_daily_limit: parseLimit(drafts[role].trend_daily_limit_text),
      multi_role_daily_limit: parseLimit(drafts[role].multi_role_daily_limit_text),
    })
    await refetch()
    const successMessage = `已保存 ${role} 策略`
    setActionFeedback(successMessage, 'success')
    ui.showToast(successMessage, 'success')
  } catch (error: any) {
    const errorMessage = resolveActionError(error)
    const failureMessage = `保存 ${role} 策略失败：${errorMessage}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  } finally {
    delete roleActionPending[role]
  }
}

async function refreshPolicies() {
  await refetch()
}

async function resetDefaults() {
  resetPending.value = true
  try {
    const data = await resetAuthRolePoliciesToDefault()
    for (const item of data.roles || []) applyRolePolicy(item)
    const successMessage = '已恢复默认策略'
    setActionFeedback(successMessage, 'success')
    ui.showToast(successMessage, 'success')
  } catch (error: any) {
    const errorMessage = resolveActionError(error)
    const failureMessage = `恢复默认策略失败：${errorMessage}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  } finally {
    resetPending.value = false
  }
}
</script>
