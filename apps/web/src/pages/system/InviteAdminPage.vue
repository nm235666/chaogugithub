<template>
  <AppShell title="管理员邀请码管理" subtitle="邀请码查看、增删改查、停用与账号统计一站式管理。">
    <div class="space-y-4">
      <PageSection title="账号总览" subtitle="当前账号规模与分级分布。">
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
          <StatCard title="总账号" :value="summary?.summary?.total_users ?? 0" hint="注册账号总数" />
          <StatCard title="活跃账号" :value="summary?.summary?.active_users ?? 0" hint="is_active = 1" />
          <StatCard title="已验证邮箱" :value="summary?.summary?.verified_users ?? 0" hint="email_verified = 1" />
          <StatCard title="受限账号" :value="summary?.summary?.limited_users ?? 0" hint="limited" />
          <StatCard title="专业账号" :value="summary?.summary?.pro_users ?? 0" hint="pro" />
          <StatCard title="管理员账号" :value="summary?.summary?.admin_users ?? 0" hint="admin" />
        </div>
      </PageSection>

      <PageSection title="创建邀请码" subtitle="支持一次性邀请码，也支持多次可用邀请码。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input v-model.trim="createForm.invite_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="邀请码（留空自动生成）" />
          <input v-model.number="createForm.max_uses" type="number" min="1" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="最大使用次数" />
          <input v-model.trim="createForm.expires_at" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="过期时间（可选，如 2026-12-31 23:59:59）" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-50" :disabled="createPending" @click="onCreate">
            {{ createPending ? '创建中...' : '创建邀请码' }}
          </button>
        </div>
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
            建议：核查邀请码参数是否合法；如持续失败，请前往"数据源监控"页排查后台状态。
          </div>
        </div>
      </PageSection>

      <PageSection title="邀请码列表" subtitle="支持筛选、停用/启用、修改、删除。">
        <div class="mb-3 grid gap-3 xl:grid-cols-3 md:grid-cols-2">
          <input v-model.trim="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="按邀请码搜索" />
          <select v-model="filters.active" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部状态</option>
            <option value="1">仅启用</option>
            <option value="0">仅停用</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold text-[var(--ink)]" @click="refreshList">刷新</button>
        </div>

        <div class="space-y-3">
          <div
            v-for="item in list?.items || []"
            :key="item.invite_code"
            class="rounded-[20px] border border-[var(--line)] bg-white p-4 shadow-[var(--shadow-soft)]"
          >
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div class="text-base font-bold">{{ item.invite_code }}</div>
                <div class="mt-1 text-sm text-[var(--muted)]">
                  创建人 {{ item.created_by || '-' }} · 使用 {{ item.used_count }}/{{ item.max_uses }} · 余量 {{ item.remaining_uses ?? '不限' }} · 过期 {{ item.expires_at || '未设置' }}
                </div>
              </div>
              <StatusBadge :value="item.is_active ? 'ok' : 'warn'" :label="item.is_active ? '启用中' : '已停用'" />
            </div>
            <div class="mt-3 grid gap-2 xl:grid-cols-5 md:grid-cols-2">
              <input v-model.number="editCache[item.invite_code].max_uses" type="number" min="1" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm disabled:opacity-40" :disabled="inviteActionPending[item.invite_code] === 'update' || inviteActionPending[item.invite_code] === 'delete'" />
              <input v-model.trim="editCache[item.invite_code].expires_at" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm disabled:opacity-40" :disabled="inviteActionPending[item.invite_code] === 'update' || inviteActionPending[item.invite_code] === 'delete'" placeholder="过期时间（可空）" />
              <select v-model="editCache[item.invite_code].is_active" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm disabled:opacity-40" :disabled="inviteActionPending[item.invite_code] === 'update' || inviteActionPending[item.invite_code] === 'delete'">
                <option :value="true">启用</option>
                <option :value="false">停用</option>
              </select>
              <button
                class="rounded-2xl bg-stone-800 px-3 py-2 text-sm font-semibold text-white disabled:opacity-40"
                :disabled="inviteActionPending[item.invite_code] === 'update' || inviteActionPending[item.invite_code] === 'delete'"
                @click="onUpdate(item.invite_code)"
              >
                {{ inviteActionPending[item.invite_code] === 'update' ? '保存中...' : '保存' }}
              </button>
              <button
                class="rounded-2xl border border-red-300 bg-red-50 px-3 py-2 text-sm font-semibold text-red-700 disabled:opacity-40"
                :disabled="inviteActionPending[item.invite_code] === 'update' || inviteActionPending[item.invite_code] === 'delete'"
                @click="onDelete(item.invite_code)"
              >
                {{ inviteActionPending[item.invite_code] === 'delete' ? '删除中...' : '删除' }}
              </button>
            </div>
          </div>
        </div>

        <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ filters.page }} / {{ list?.total_pages || 1 }} 页 · 共 {{ list?.total || 0 }} 条</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page <= 1" @click="goPrevPage">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page >= (list?.total_pages || 1)" @click="goNextPage">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { createAuthInvite, deleteAuthInvite, fetchAuthInvites, fetchAuthUsersSummary, updateAuthInvite } from '../../services/api/system'
import { confirmDangerAction } from '../../shared/utils/confirm'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'
import { useUiStore } from '../../stores/ui'

const queryClient = useQueryClient()
const route = useRoute()
const router = useRouter()
const ui = useUiStore()

const filters = reactive({
  keyword: '',
  active: '',
  page: 1,
  page_size: 20,
})

const createForm = reactive({
  invite_code: '',
  max_uses: 1,
  expires_at: '',
})

const editCache = reactive<Record<string, { max_uses: number; expires_at: string; is_active: boolean }>>({})
const inviteActionPending = reactive<Record<string, 'update' | 'delete' | undefined>>({})
const actionFeedback = reactive<{ text: string; tone: 'success' | 'error' | 'info'; confirmedAt: string }>({
  text: '',
  tone: 'info',
  confirmedAt: '',
})

const { data: summary, refetch: refetchSummary } = useQuery({
  queryKey: ['auth-users-summary'],
  queryFn: fetchAuthUsersSummary,
})

const { data: list, refetch: refetchList } = useQuery({
  queryKey: ['auth-invites', filters],
  queryFn: () => fetchAuthInvites(filters),
  placeholderData: keepPreviousData,
})

watch(
  () => list.value?.items,
  (items) => {
    for (const item of items || []) {
      editCache[item.invite_code] = {
        max_uses: Number(item.max_uses || 1),
        expires_at: String(item.expires_at || ''),
        is_active: !!item.is_active,
      }
    }
  },
  { immediate: true },
)

watch(
  () => [filters.keyword, filters.active],
  () => {
    filters.page = 1
  },
)

function syncRouteFromFilters() {
  router.replace({
    query: buildCleanQuery({
      keyword: filters.keyword,
      active: filters.active,
      page: filters.page,
      page_size: filters.page_size,
    }),
  })
}

function applyRouteFilters() {
  const q = route.query as Record<string, unknown>
  Object.assign(filters, {
    keyword: readQueryString(q, 'keyword', ''),
    active: readQueryString(q, 'active', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(20, readQueryNumber(q, 'page_size', 20)),
  })
}

const createMutation = useMutation({
  mutationFn: () =>
    createAuthInvite({
      invite_code: createForm.invite_code || undefined,
      max_uses: createForm.max_uses || 1,
      expires_at: createForm.expires_at || undefined,
    }),
  onSuccess: (resp: any) => {
    createForm.invite_code = ''
    createForm.max_uses = 1
    createForm.expires_at = ''
    const successMessage = `创建成功：${resp.invite_code}`
    setActionFeedback(successMessage, 'success')
    ui.showToast(successMessage, 'success')
    queryClient.invalidateQueries({ queryKey: ['auth-invites'] })
  },
  onError: (error: Error) => {
    const failureMessage = `创建失败：${resolveActionError(error)}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  },
})

const createPending = computed(() => createMutation.isPending.value)
const actionFeedbackClass = computed(() => {
  if (actionFeedback.tone === 'success') return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  if (actionFeedback.tone === 'error') return 'border-red-200 bg-red-50 text-red-700'
  return 'border-[var(--line)] bg-[rgba(255,255,255,0.72)] text-[var(--muted)]'
})

function resolveActionError(error: unknown) {
  return String((error as any)?.response?.data?.error || (error as Error)?.message || 'unknown')
}

function setActionFeedback(text: string, tone: 'success' | 'error' | 'info' = 'info') {
  actionFeedback.text = text
  actionFeedback.tone = tone
  actionFeedback.confirmedAt = new Date().toLocaleString('zh-CN', { hour12: false })
}

function onCreate() {
  createMutation.mutate()
}

async function onUpdate(inviteCode: string) {
  const edit = editCache[inviteCode]
  if (!edit) return
  inviteActionPending[inviteCode] = 'update'
  try {
    await updateAuthInvite({
      invite_code: inviteCode,
      max_uses: edit.max_uses,
      expires_at: edit.expires_at || '',
      is_active: !!edit.is_active,
    })
    const successMessage = `更新成功：${inviteCode}`
    setActionFeedback(successMessage, 'success')
    ui.showToast(successMessage, 'success')
    await refetchList()
  } catch (error) {
    const failureMessage = `更新失败：${resolveActionError(error)}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  } finally {
    delete inviteActionPending[inviteCode]
  }
}

async function onDelete(inviteCode: string) {
  if (!await confirmDangerAction('删除邀请码', inviteCode, '删除后将无法恢复，请确认该邀请码不再使用。')) return
  inviteActionPending[inviteCode] = 'delete'
  try {
    await deleteAuthInvite(inviteCode)
    const successMessage = `删除成功：${inviteCode}`
    setActionFeedback(successMessage, 'success')
    ui.showToast(successMessage, 'success')
    await refetchList()
    await refetchSummary()
  } catch (error) {
    const failureMessage = `删除失败：${resolveActionError(error)}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  } finally {
    delete inviteActionPending[inviteCode]
  }
}

function refreshList() {
  filters.page = 1
  syncRouteFromFilters()
  refetchSummary()
}

function goPrevPage() {
  if (filters.page <= 1) return
  filters.page -= 1
  syncRouteFromFilters()
}

function goNextPage() {
  const totalPages = Number(list.value?.total_pages || 1)
  if (filters.page >= totalPages) return
  filters.page += 1
  syncRouteFromFilters()
}

watch(
  () => route.query,
  () => {
    applyRouteFilters()
  },
  { immediate: true },
)
</script>
