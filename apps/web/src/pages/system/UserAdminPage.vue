<template>
  <AppShell title="用户与会话管理" subtitle="用户列表、角色变更、禁用解封、重置密码、会话强制下线与审计日志。">
    <div class="space-y-4">
      <div class="page-hero-grid">
        <div class="page-hero-card">
          <div class="page-insight-label">User Admin</div>
          <div class="page-hero-title">先判断账号状态，再做角色、会话和额度操作。</div>
          <div class="page-hero-copy">
            这页最容易变成“按钮堆”。更合理的使用顺序是：先定位用户，再确认角色/状态，再做密码重置、额度修复和会话强退，最后回看审计日志。
          </div>
          <div class="page-action-cluster">
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="onRefreshUsers">刷新用户列表</button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)]" @click="onRefreshSessions">刷新会话</button>
          </div>
        </div>
        <div class="page-insight-list">
          <div class="page-insight-item">
            <div class="page-insight-label">当前后台关注点</div>
            <div class="page-insight-value">{{ activeUsersCount < (users?.total ?? 0) ? '存在禁用账号' : '账号状态正常' }}</div>
            <div class="page-insight-note">启用 {{ activeUsersCount }} / 总数 {{ users?.total ?? (users?.items?.length || 0) }}。</div>
          </div>
          <div class="page-insight-item">
            <div class="page-insight-label">安全面板</div>
            <div class="page-insight-value">{{ sessions?.total ?? (sessions?.items?.length || 0) }} 个在线会话</div>
            <div class="page-insight-note">如遇异常登录或改密，优先处理会话，再回查审计日志。</div>
          </div>
        </div>
      </div>

      <div class="kpi-grid">
        <StatCard title="用户总数" :value="users?.total ?? (users?.items?.length || 0)" hint="当前用户列表统计" />
        <StatCard title="启用用户" :value="activeUsersCount" hint="禁用用户可在列表中恢复" />
        <StatCard title="在线会话" :value="sessions?.total ?? (sessions?.items?.length || 0)" hint="支持单会话强制下线" />
        <StatCard title="审计日志" :value="audit?.total ?? (audit?.items?.length || 0)" hint="认证安全事件总量" />
      </div>

      <div
        v-if="actionFeedback.text"
        class="rounded-[20px] border px-4 py-3 text-sm"
        :class="actionFeedbackClass"
      >
        <div class="font-semibold">最近写操作反馈</div>
        <div class="mt-1">{{ actionFeedback.text }}</div>
        <div v-if="actionFeedback.confirmedAt" class="mt-1 text-xs opacity-80">
          最近确认时间：{{ actionFeedback.confirmedAt }}
        </div>
        <div v-if="actionFeedback.tone === 'error'" class="mt-2 text-xs opacity-80">
          建议：核查操作参数；如持续失败，请前往"数据源监控"页排查后台状态或查看后台日志。
        </div>
      </div>

      <PageSection title="用户列表" subtitle="支持账号检索、角色调整、启停、密码重置。">
        <div class="table-lead">
          <div class="table-lead-copy">用户卡片优先用来做账号状态判断。真正有风险的操作包括：改角色、禁用、重置密码和额度归零，建议逐个确认。</div>
          <div class="flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">角色 {{ userFilters.role || '全部' }}</span>
            <span class="metric-chip">状态 {{ userFilters.active === '' ? '全部' : (userFilters.active === '1' ? '启用' : '禁用') }}</span>
          </div>
        </div>
        <div class="mb-3 grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <input v-model.trim="userFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="账号/昵称/邮箱关键字" />
          <select v-model="userFilters.role" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部角色</option>
            <option value="admin">admin</option>
            <option value="pro">pro</option>
            <option value="limited">limited</option>
          </select>
          <select v-model="userFilters.active" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部状态</option>
            <option value="1">启用</option>
            <option value="0">禁用</option>
          </select>
          <select v-model.number="userFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="10">10 / 页</option>
            <option :value="6">6 / 页</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="onRefreshUsers">刷新</button>
        </div>
        <div class="grid gap-3 2xl:grid-cols-2">
          <div v-for="u in users?.items || []" :key="u.id" class="rounded-[20px] border border-[var(--line)] bg-white p-4 shadow-[var(--shadow-soft)]">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div class="font-bold">{{ u.username }} <span class="text-sm text-[var(--muted)]">({{ u.display_name || '-' }})</span></div>
              <StatusBadge :value="u.is_active ? 'ok' : 'warn'" :label="u.is_active ? '启用' : '禁用'" />
            </div>
            <div class="mt-1 text-sm text-[var(--muted)]">
              邮箱 {{ u.email || '-' }} · 角色 {{ u.role }} · 失败次数 {{ u.failed_login_count || 0 }} · 锁定至 {{ u.locked_until || '-' }}
            </div>
            <div class="mt-1 text-sm text-[var(--muted)]">
              走势次数（UTC {{ u.trend_usage_date_utc || '-' }}）· 已用 {{ u.trend_used_today ?? 0 }} / 限额 {{ u.trend_limit ?? '不限' }} · 剩余 {{ u.trend_remaining_today ?? '不限' }}
            </div>
            <div class="mt-1 text-sm text-[var(--muted)]">
              多角色次数（UTC {{ u.trend_usage_date_utc || '-' }}）· 已用 {{ u.multi_role_used_today ?? 0 }} / 限额 {{ u.multi_role_limit ?? '不限' }} · 剩余 {{ u.multi_role_remaining_today ?? '不限' }}
            </div>
            <div class="mt-3 grid gap-2 xl:grid-cols-3 md:grid-cols-2">
              <select v-model="editUsers[u.id].role" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
                <option value="admin">admin</option>
                <option value="pro">pro</option>
                <option value="limited">limited</option>
              </select>
              <select v-model="editUsers[u.id].is_active" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
                <option :value="true">启用</option>
                <option :value="false">禁用</option>
              </select>
              <button class="rounded-2xl bg-stone-800 px-3 py-2 text-sm font-semibold text-white disabled:opacity-40" :disabled="!!userActionPending[u.id]" @click="saveUser(u.id)">{{ userActionPending[u.id] === 'save' ? '保存中...' : '保存用户' }}</button>
              <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold disabled:opacity-40" :disabled="!!userActionPending[u.id]" @click="resetUserPassword(u.id, u.username)">
                {{ userActionPending[u.id] === 'password' ? '重置中...' : '重置密码' }}
              </button>
              <button
                class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold disabled:opacity-40"
                :disabled="u.role !== 'limited' || !!userActionPending[u.id]"
                @click="resetUserTrendQuota(u.id, u.username)"
              >
                {{ userActionPending[u.id] === 'trend' ? '重置中...' : '重置今日走势次数' }}
              </button>
              <button
                class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold disabled:opacity-40"
                :disabled="u.role !== 'limited' || !!userActionPending[u.id]"
                @click="resetUserMultiRoleQuota(u.id, u.username)"
              >
                {{ userActionPending[u.id] === 'multi_role' ? '重置中...' : '重置今日多角色次数' }}
              </button>
            </div>
          </div>
        </div>
      </PageSection>

      <PageSection title="额度批量重置" subtitle="按日期重置用户的走势+多角色额度计数，支持按角色或用户名白名单过滤。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input v-model.trim="quotaBatch.usage_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="UTC 日期，如 2026-04-01" />
          <select v-model="quotaBatch.role" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部角色</option>
            <option value="limited">limited</option>
            <option value="pro">pro</option>
            <option value="admin">admin</option>
          </select>
          <input v-model.trim="quotaBatch.usernames_text" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="可选：用户名，逗号分隔，如 user1,user2" />
          <button class="rounded-2xl bg-stone-800 px-4 py-3 text-sm font-semibold text-white disabled:opacity-40" :disabled="quotaBatchPending" @click="runQuotaBatchReset">{{ quotaBatchPending ? '执行中...' : '执行' }}</button>
        </div>
        <div v-if="quotaBatchMessage" class="mt-3 text-sm text-[var(--muted)]">{{ quotaBatchMessage }}</div>
      </PageSection>

      <PageSection title="活跃会话" subtitle="按用户查看在线会话并强制下线。">
        <div class="mb-3 grid gap-3 xl:grid-cols-3 md:grid-cols-2">
          <input v-model.trim="sessionFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="按账号关键字筛选会话" />
          <select v-model.number="sessionFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="10">10 / 页</option>
            <option :value="20">20 / 页</option>
            <option :value="40">40 / 页</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="onRefreshSessions">刷新</button>
        </div>
        <div class="grid gap-3 2xl:grid-cols-2">
          <InfoCard
            v-for="s in sessions?.items || []"
            :key="s.session_id"
            :title="`${s.username} · 会话#${s.session_id}`"
            :meta="`最后活跃 ${formatDateTime(s.last_seen_at)} · 过期 ${formatDateTime(s.expires_at)}`"
            :description="`token: ${s.token_hash_preview}`"
          >
            <template #badge>
              <button class="rounded-2xl border border-red-300 bg-red-50 px-3 py-1 text-xs font-semibold text-red-700 disabled:opacity-40" :disabled="!!sessionActionPending[s.session_id]" @click="revokeSession(s.session_id)">
                {{ sessionActionPending[s.session_id] ? '处理中...' : '强制下线' }}
              </button>
            </template>
          </InfoCard>
        </div>
      </PageSection>

      <PageSection title="认证审计日志" subtitle="登录失败、注册、登出等安全事件。">
        <div class="mb-3 grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <input v-model.trim="auditFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="用户/IP/详情关键字" />
          <input v-model.trim="auditFilters.event_type" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="事件类型，如 login" />
          <select v-model="auditFilters.result" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部结果</option>
            <option value="ok">ok</option>
            <option value="fail">fail</option>
          </select>
          <select v-model.number="auditFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="12">12 / 页</option>
            <option :value="20">20 / 页</option>
            <option :value="40">40 / 页</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="onRefreshAudit">刷新</button>
        </div>
        <div class="grid gap-3 2xl:grid-cols-2">
          <InfoCard
            v-for="a in audit?.items || []"
            :key="a.id"
            :title="`${a.event_type} · ${a.username || 'anonymous'}`"
            :meta="`${a.result} · ${a.ip || '-'} · ${formatDateTime(a.created_at)}`"
            :description="a.detail || ''"
          />
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import { fetchAuthAuditLogs, fetchAuthSessions, fetchAuthUsers, resetAuthQuotaBatch, resetAuthUserMultiRoleQuota, resetAuthUserPassword, resetAuthUserTrendQuota, revokeAuthSession, updateAuthUser } from '../../services/api/system'
import { formatDateTime } from '../../shared/utils/format'
import { confirmDangerAction, promptInputAction } from '../../shared/utils/confirm'
import { useUiStore } from '../../stores/ui'

const userFilters = reactive({ keyword: '', role: '', active: '', page: 1, page_size: 20 })
const sessionFilters = reactive({ keyword: '', page: 1, page_size: 10 })
const auditFilters = reactive({ keyword: '', event_type: '', result: '', page: 1, page_size: 12 })
const quotaBatch = reactive({ usage_date: '', role: 'limited', usernames_text: '' })
const quotaBatchMessage = ref('')
const quotaBatchPending = ref(false)
const userActionPending = reactive<Record<number, string>>({})
const sessionActionPending = reactive<Record<number, boolean>>({})
const actionFeedback = reactive<{ text: string; tone: 'success' | 'error' | 'info'; confirmedAt: string }>({
  text: '',
  tone: 'info',
  confirmedAt: '',
})

const editUsers = reactive<Record<number, { role: string; is_active: boolean }>>({})
const ui = useUiStore()

const { data: users, refetch: refetchUsers } = useQuery({
  queryKey: ['auth-users', userFilters],
  queryFn: () => fetchAuthUsers(userFilters),
})

watch(
  () => users.value?.items,
  (items) => {
    for (const u of items || []) {
      editUsers[u.id] = { role: u.role || 'limited', is_active: !!u.is_active }
    }
  },
  { immediate: true },
)

const { data: sessions, refetch: refetchSessions } = useQuery({
  queryKey: ['auth-sessions', sessionFilters],
  queryFn: () => fetchAuthSessions(sessionFilters),
})

const { data: audit, refetch: refetchAudit } = useQuery({
  queryKey: ['auth-audit', auditFilters],
  queryFn: () => fetchAuthAuditLogs(auditFilters),
})

const activeUsersCount = computed(() =>
  (users.value?.items || []).reduce((sum: number, item: Record<string, any>) => sum + (item.is_active ? 1 : 0), 0),
)
const actionFeedbackClass = computed(() => {
  if (actionFeedback.tone === 'success') return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  if (actionFeedback.tone === 'error') return 'border-red-200 bg-red-50 text-red-700'
  return 'border-[var(--line)] bg-[rgba(255,255,255,0.72)] text-[var(--muted)]'
})

function resolveActionError(error: any) {
  return String(error?.response?.data?.error || error?.message || 'unknown')
}

function setActionFeedback(message: string, tone: 'success' | 'error' | 'info' = 'info') {
  actionFeedback.text = message
  actionFeedback.tone = tone
  actionFeedback.confirmedAt = new Date().toLocaleString('zh-CN', { hour12: false })
}

async function saveUser(userId: number) {
  const edit = editUsers[userId]
  if (!edit) return
  if (!await confirmDangerAction('保存用户配置', String(userId), '将更新该用户角色或启用状态。')) return
  userActionPending[userId] = 'save'
  try {
    await updateAuthUser({ user_id: userId, role: edit.role, is_active: edit.is_active })
    await refetchUsers()
    const message = `用户 ${userId} 已保存：角色 ${edit.role} · 状态 ${edit.is_active ? '启用' : '禁用'}`
    setActionFeedback(message, 'success')
    ui.showToast(message, 'success')
  } catch (error: any) {
    const detail = resolveActionError(error)
    const message = `保存用户 ${userId} 失败：${detail}`
    setActionFeedback(message, 'error')
    ui.showToast(message, 'error')
  } finally {
    delete userActionPending[userId]
  }
}

async function resetUserPassword(userId: number, username: string) {
  const next = await promptInputAction(
    '重置用户密码',
    `请输入用户 ${username} 的新密码`,
    '至少 6 位',
    '该操作将立即生效，用户需要使用新密码重新登录。',
  )
  if (!next) return
  userActionPending[userId] = 'password'
  try {
    await resetAuthUserPassword({ user_id: userId, new_password: next })
    await Promise.all([refetchUsers(), refetchSessions()])
    const message = `已重置 ${username} 的密码，新密码已生效。`
    setActionFeedback(message, 'success')
    ui.showToast(message, 'success')
  } catch (error: any) {
    const detail = resolveActionError(error)
    const message = `重置 ${username} 密码失败：${detail}`
    setActionFeedback(message, 'error')
    ui.showToast(message, 'error')
  } finally {
    delete userActionPending[userId]
  }
}

async function resetUserTrendQuota(userId: number, username: string) {
  if (!await confirmDangerAction('重置走势次数', username, '将重置该用户今日（UTC）的走势额度消耗。')) return
  userActionPending[userId] = 'trend'
  try {
    await resetAuthUserTrendQuota({ user_id: userId })
    await refetchUsers()
    const message = `已重置 ${username} 今日（UTC）走势次数。`
    setActionFeedback(message, 'success')
    ui.showToast(message, 'success')
  } catch (error: any) {
    const detail = resolveActionError(error)
    const message = `重置 ${username} 今日走势次数失败：${detail}`
    setActionFeedback(message, 'error')
    ui.showToast(message, 'error')
  } finally {
    delete userActionPending[userId]
  }
}

async function resetUserMultiRoleQuota(userId: number, username: string) {
  if (!await confirmDangerAction('重置多角色次数', username, '将重置该用户今日（UTC）的多角色额度消耗。')) return
  userActionPending[userId] = 'multi_role'
  try {
    await resetAuthUserMultiRoleQuota({ user_id: userId })
    await refetchUsers()
    const message = `已重置 ${username} 今日（UTC）多角色次数。`
    setActionFeedback(message, 'success')
    ui.showToast(message, 'success')
  } catch (error: any) {
    const detail = resolveActionError(error)
    const message = `重置 ${username} 今日多角色次数失败：${detail}`
    setActionFeedback(message, 'error')
    ui.showToast(message, 'error')
  } finally {
    delete userActionPending[userId]
  }
}

async function runQuotaBatchReset() {
  const usernames = quotaBatch.usernames_text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
  quotaBatchPending.value = true
  try {
    const payload = await resetAuthQuotaBatch({
      usage_date: quotaBatch.usage_date,
      role: quotaBatch.role,
      usernames,
    })
    quotaBatchMessage.value = `批量完成：匹配用户 ${payload.matched_users || 0}，实际重置记录 ${payload.affected_rows || 0}，日期 ${payload.usage_date || '-'}`
    setActionFeedback(quotaBatchMessage.value, 'success')
    ui.showToast('批量额度重置已完成。', 'success')
    await refetchUsers()
  } catch (error: any) {
    const detail = resolveActionError(error)
    quotaBatchMessage.value = `批量重置失败：${detail}`
    setActionFeedback(quotaBatchMessage.value, 'error')
    ui.showToast(quotaBatchMessage.value, 'error')
  } finally {
    quotaBatchPending.value = false
  }
}

async function revokeSession(sessionId: number) {
  if (!await confirmDangerAction('强制下线会话', String(sessionId), '该会话将立即失效，需要重新登录。')) return
  sessionActionPending[sessionId] = true
  try {
    await revokeAuthSession(sessionId)
    await refetchSessions()
    const message = `会话 ${sessionId} 已强制下线。`
    setActionFeedback(message, 'success')
    ui.showToast(message, 'success')
  } catch (error: any) {
    const detail = resolveActionError(error)
    const message = `强制下线会话 ${sessionId} 失败：${detail}`
    setActionFeedback(message, 'error')
    ui.showToast(message, 'error')
  } finally {
    delete sessionActionPending[sessionId]
  }
}

function onRefreshUsers() {
  refetchUsers()
}

function onRefreshSessions() {
  refetchSessions()
}

function onRefreshAudit() {
  refetchAudit()
}
</script>
