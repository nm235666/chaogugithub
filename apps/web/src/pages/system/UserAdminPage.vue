<template>
  <AppShell title="用户与会话管理" subtitle="用户列表、角色变更、禁用解封、重置密码、会话强制下线与审计日志。">
    <div class="space-y-4">
      <PageSection title="用户列表" subtitle="支持账号检索、角色调整、启停、密码重置。">
        <div class="mb-3 grid gap-3 xl:grid-cols-4 md:grid-cols-2">
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
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="onRefreshUsers">刷新</button>
        </div>
        <div class="space-y-3">
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
              <button class="rounded-2xl bg-stone-800 px-3 py-2 text-sm font-semibold text-white" @click="saveUser(u.id)">保存用户</button>
              <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold" @click="resetUserPassword(u.id, u.username)">
                重置密码
              </button>
              <button
                class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold"
                :disabled="u.role !== 'limited'"
                @click="resetUserTrendQuota(u.id, u.username)"
              >
                重置今日走势次数
              </button>
              <button
                class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold"
                :disabled="u.role !== 'limited'"
                @click="resetUserMultiRoleQuota(u.id, u.username)"
              >
                重置今日多角色次数
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
          <button class="rounded-2xl bg-stone-800 px-4 py-3 text-sm font-semibold text-white" @click="runQuotaBatchReset">执行</button>
        </div>
        <div v-if="quotaBatchMessage" class="mt-3 text-sm text-[var(--muted)]">{{ quotaBatchMessage }}</div>
      </PageSection>

      <PageSection title="活跃会话" subtitle="按用户查看在线会话并强制下线。">
        <div class="mb-3 grid gap-3 xl:grid-cols-2 md:grid-cols-2">
          <input v-model.trim="sessionFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="按账号关键字筛选会话" />
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="onRefreshSessions">刷新</button>
        </div>
        <div class="space-y-2">
          <InfoCard
            v-for="s in sessions?.items || []"
            :key="s.session_id"
            :title="`${s.username} · 会话#${s.session_id}`"
            :meta="`最后活跃 ${formatDateTime(s.last_seen_at)} · 过期 ${formatDateTime(s.expires_at)}`"
            :description="`token: ${s.token_hash_preview}`"
          >
            <template #badge>
              <button class="rounded-2xl border border-red-300 bg-red-50 px-3 py-1 text-xs font-semibold text-red-700" @click="revokeSession(s.session_id)">
                强制下线
              </button>
            </template>
          </InfoCard>
        </div>
      </PageSection>

      <PageSection title="认证审计日志" subtitle="登录失败、注册、登出等安全事件。">
        <div class="mb-3 grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input v-model.trim="auditFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="用户/IP/详情关键字" />
          <input v-model.trim="auditFilters.event_type" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="事件类型，如 login" />
          <select v-model="auditFilters.result" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部结果</option>
            <option value="ok">ok</option>
            <option value="fail">fail</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold" @click="onRefreshAudit">刷新</button>
        </div>
        <div class="space-y-2">
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
import { reactive, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchAuthAuditLogs, fetchAuthSessions, fetchAuthUsers, resetAuthQuotaBatch, resetAuthUserMultiRoleQuota, resetAuthUserPassword, resetAuthUserTrendQuota, revokeAuthSession, updateAuthUser } from '../../services/api/system'
import { formatDateTime } from '../../shared/utils/format'
import { confirmDangerAction, infoNoticeAction, promptInputAction } from '../../shared/utils/confirm'

const userFilters = reactive({ keyword: '', role: '', active: '', page: 1, page_size: 20 })
const sessionFilters = reactive({ keyword: '', page: 1, page_size: 20 })
const auditFilters = reactive({ keyword: '', event_type: '', result: '', page: 1, page_size: 20 })
const quotaBatch = reactive({ usage_date: '', role: 'limited', usernames_text: '' })
const quotaBatchMessage = ref('')

const editUsers = reactive<Record<number, { role: string; is_active: boolean }>>({})

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

async function saveUser(userId: number) {
  const edit = editUsers[userId]
  if (!edit) return
  if (!await confirmDangerAction('保存用户配置', String(userId), '将更新该用户角色或启用状态。')) return
  await updateAuthUser({ user_id: userId, role: edit.role, is_active: edit.is_active })
  await refetchUsers()
}

async function resetUserPassword(userId: number, username: string) {
  const next = await promptInputAction(
    '重置用户密码',
    `请输入用户 ${username} 的新密码`,
    '至少 6 位',
    '该操作将立即生效，用户需要使用新密码重新登录。',
  )
  if (!next) return
  await resetAuthUserPassword({ user_id: userId, new_password: next })
  await refetchUsers()
  await refetchSessions()
}

async function resetUserTrendQuota(userId: number, username: string) {
  if (!await confirmDangerAction('重置走势次数', username, '将重置该用户今日（UTC）的走势额度消耗。')) return
  await resetAuthUserTrendQuota({ user_id: userId })
  await refetchUsers()
  await infoNoticeAction('操作完成', `已重置 ${username} 今日（UTC）LLM走势次数。`)
}

async function resetUserMultiRoleQuota(userId: number, username: string) {
  if (!await confirmDangerAction('重置多角色次数', username, '将重置该用户今日（UTC）的多角色额度消耗。')) return
  await resetAuthUserMultiRoleQuota({ user_id: userId })
  await refetchUsers()
  await infoNoticeAction('操作完成', `已重置 ${username} 今日（UTC）LLM多角色次数。`)
}

async function runQuotaBatchReset() {
  const usernames = quotaBatch.usernames_text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
  const payload = await resetAuthQuotaBatch({
    usage_date: quotaBatch.usage_date,
    role: quotaBatch.role,
    usernames,
  })
  quotaBatchMessage.value = `批量完成：匹配用户 ${payload.matched_users || 0}，实际重置记录 ${payload.affected_rows || 0}，日期 ${payload.usage_date || '-'}`
  await refetchUsers()
}

async function revokeSession(sessionId: number) {
  if (!await confirmDangerAction('强制下线会话', String(sessionId), '该会话将立即失效，需要重新登录。')) return
  await revokeAuthSession(sessionId)
  await refetchSessions()
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
