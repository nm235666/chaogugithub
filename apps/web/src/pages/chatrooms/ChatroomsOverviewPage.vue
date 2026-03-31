<template>
  <AppShell title="群聊总览" subtitle="统一查看群聊标签、监控状态、活跃度和最近拉取情况。">
    <div class="space-y-4">
      <PageSection title="筛选条件" subtitle="按分类、风控、监控状态和拉取状态快速定位群聊。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input
            v-model="draftFilters.keyword"
            class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
            placeholder="群聊名称 / 标签 / 摘要关键词"
          />
          <select v-model="draftFilters.skip_realtime_monitor" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部监控状态</option>
            <option value="0">监控中</option>
            <option value="1">已暂停监控</option>
          </select>
          <select v-model="draftFilters.fetch_status" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部拉取状态</option>
            <option value="ok">拉取正常</option>
            <option value="failed">最近失败</option>
            <option value="unknown">暂无状态</option>
          </select>
          <select v-model="draftFilters.primary_category" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部主分类</option>
            <option v-for="item in filterOptions.primary_categories" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="draftFilters.activity_level" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部活跃度</option>
            <option v-for="item in filterOptions.activity_levels" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="draftFilters.risk_level" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部风险等级</option>
            <option v-for="item in filterOptions.risk_levels" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model.number="draftFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
            <option :value="100">100 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isFetching" @click="handleRefresh">
            {{ isFetching ? '查询中...' : '查询' }}
          </button>
        </div>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="群聊总数" :value="formatNumber(summary.room_total)" hint="chatroom_list_items 总群数" />
        <StatCard title="有聊天记录" :value="formatNumber(summary.room_with_logs)" hint="已成功清洗并入库的群" />
        <StatCard title="暂停监控" :value="formatNumber(summary.skip_total)" hint="被标记为不再实时监控的群" />
        <StatCard title="已完成标签" :value="formatNumber(summary.tagged_total)" hint="已做 LLM 分类的群聊数" />
      </div>

      <PageSection :title="`群聊结果 (${result?.total || 0})`" subtitle="群聊标签、活跃度、风险等级与拉取状态统一展示。">
        <DataTable :columns="columns" :rows="resultRows" row-key="room_id" empty-text="暂无群聊数据">
          <template #cell_display_name="{ row }">
            <div class="min-w-[240px]">
              <div class="font-semibold text-[var(--ink)]">{{ row.display_name }}</div>
              <div class="mt-1 text-xs text-[var(--muted)]">
                {{ row.user_count_label }} · 群主 {{ row.owner_label }}
              </div>
              <div v-if="row.tag_list.length" class="mt-2 flex flex-wrap gap-2">
                <span
                  v-for="tag in row.tag_list.slice(0, 4)"
                  :key="tag"
                  class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-2.5 py-1 text-xs text-[var(--muted)]"
                >
                  {{ tag }}
                </span>
              </div>
            </div>
          </template>
          <template #cell_llm_chatroom_primary_category="{ row }">
            <StatusBadge :value="row.llm_chatroom_primary_category ? 'brand' : 'muted'" :label="row.llm_chatroom_primary_category || '-'" />
          </template>
          <template #cell_llm_chatroom_activity_level="{ row }">
            <StatusBadge :value="activityTone(row.llm_chatroom_activity_level)" :label="row.llm_chatroom_activity_level || '-'" />
          </template>
          <template #cell_llm_chatroom_risk_level="{ row }">
            <StatusBadge :value="riskTone(row.llm_chatroom_risk_level)" :label="row.llm_chatroom_risk_level || '-'" />
          </template>
          <template #cell_monitor_status="{ row }">
            <StatusBadge :value="row.monitor_value" :label="row.monitor_label" />
          </template>
          <template #cell_last_chatlog_backfill_status="{ row }">
            <StatusBadge :value="fetchTone(row.last_chatlog_backfill_status)" :label="fetchLabel(row.last_chatlog_backfill_status)" />
          </template>
          <template #cell_last_chatlog_backfill_at="{ row }">
            <div class="text-sm text-[var(--ink)]">{{ formatDateTime(row.last_chatlog_backfill_at) }}</div>
          </template>
          <template #cell_last_message_date="{ row }">
            <div class="text-sm text-[var(--ink)]">{{ formatDate(row.last_message_date) }}</div>
          </template>
          <template #cell_message_row_count="{ row }">
            <div class="font-semibold text-[var(--ink)]">{{ formatNumber(row.message_row_count) }}</div>
            <div class="mt-1 text-xs text-[var(--muted)]">
              30天清洗 {{ formatNumber(row.last_30d_clean_message_count) }} / 原始 {{ formatNumber(row.last_30d_raw_message_count) }}
            </div>
          </template>
          <template #cell_actions="{ row }">
            <div class="flex min-w-[136px] flex-col gap-2">
              <button
                class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)] disabled:cursor-not-allowed disabled:opacity-50"
                :disabled="fetchingRoomId === row.room_id || row.skip_realtime_monitor === 1"
                @click="runFetch(row.room_id, 'today')"
              >
                {{ fetchingRoomId === row.room_id ? '拉取中...' : '立即拉取' }}
              </button>
              <button
                class="rounded-xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)] disabled:cursor-not-allowed disabled:opacity-50"
                :disabled="fetchingRoomId === row.room_id || row.skip_realtime_monitor === 1"
                @click="runFetch(row.room_id, 'yesterday_and_today')"
              >
                补抓昨今
              </button>
            </div>
          </template>
        </DataTable>

        <div class="mt-4 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ queryFilters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="queryFilters.page -= 1">上一页</button>
            <button
              class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40"
              :disabled="queryFilters.page >= (result?.total_pages || 1)"
              @click="queryFilters.page += 1"
            >
              下一页
            </button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import { fetchChatrooms, triggerChatroomFetch } from '../../services/api/chatrooms'
import { formatDate, formatDateTime, formatNumber } from '../../shared/utils/format'

const queryClient = useQueryClient()
const fetchingRoomId = ref('')

const queryFilters = reactive({
  keyword: '',
  primary_category: '',
  activity_level: '',
  risk_level: '',
  skip_realtime_monitor: '',
  fetch_status: '',
  page: 1,
  page_size: 20,
})
const draftFilters = reactive({ ...queryFilters })

const columns = [
  { key: 'display_name', label: '群聊名称' },
  { key: 'room_id', label: '群ID' },
  { key: 'llm_chatroom_primary_category', label: '主分类' },
  { key: 'llm_chatroom_activity_level', label: '活跃度' },
  { key: 'llm_chatroom_risk_level', label: '风险等级' },
  { key: 'monitor_status', label: '监控状态' },
  { key: 'last_chatlog_backfill_status', label: '最近拉取状态' },
  { key: 'last_chatlog_backfill_at', label: '最近拉取时间' },
  { key: 'last_message_date', label: '最近消息日期' },
  { key: 'message_row_count', label: '聊天记录' },
  { key: 'actions', label: '操作' },
]

const {
  data: result,
  refetch,
  isFetching,
} = useQuery({
  queryKey: ['chatrooms', queryFilters],
  queryFn: () => fetchChatrooms(queryFilters),
})

const runFetchMutation = useMutation({
  mutationFn: ({ roomId, mode }: { roomId: string; mode: string }) => triggerChatroomFetch(roomId, mode),
  onSuccess: async () => {
    await queryClient.invalidateQueries({ queryKey: ['chatrooms'] })
  },
  onSettled: () => {
    fetchingRoomId.value = ''
  },
})

const summary = computed(() => result.value?.summary || {})
const filterOptions = computed(
  () =>
    result.value?.filters || {
      primary_categories: [],
      activity_levels: [],
      risk_levels: [],
    },
)

const resultRows = computed(() =>
  (result.value?.items || []).map((row: Record<string, any>) => ({
    ...row,
    display_name: displayName(row),
    owner_label: row.owner || '-',
    user_count_label: row.user_count ? `${formatNumber(row.user_count)} 人` : '人数未知',
    tag_list: parseTags(row.llm_chatroom_tags_json),
    monitor_label: Number(row.skip_realtime_monitor || 0) === 0 ? '监控中' : '已暂停监控',
    monitor_value: Number(row.skip_realtime_monitor || 0) === 0 ? 'ok' : 'muted',
  })),
)

function displayName(row: Record<string, any>) {
  return [row.remark, row.nick_name, row.room_id].map((item) => String(item || '').trim()).find(Boolean) || '-'
}

function parseTags(raw: unknown): string[] {
  const text = String(raw || '').trim()
  if (!text) return []
  try {
    const parsed = JSON.parse(text)
    if (Array.isArray(parsed)) return parsed.map((item) => String(item || '').trim()).filter(Boolean)
    return []
  } catch {
    return text
      .split(/[,\n，]/)
      .map((item) => item.trim())
      .filter(Boolean)
  }
}

function activityTone(value: unknown) {
  const text = String(value || '').trim()
  if (text === '高') return 'success'
  if (text === '中') return 'warning'
  if (text === '低') return 'muted'
  return 'muted'
}

function riskTone(value: unknown) {
  const text = String(value || '').trim()
  if (text === '高') return 'danger'
  if (text === '中') return 'warning'
  if (text === '低') return 'success'
  return 'muted'
}

function fetchTone(value: unknown) {
  const text = String(value || '').trim()
  if (text === 'ok') return 'success'
  if (!text) return 'muted'
  return 'danger'
}

function fetchLabel(value: unknown) {
  const text = String(value || '').trim()
  if (text === 'ok') return '正常'
  if (!text) return '暂无状态'
  return text
}

function handleRefresh() {
  Object.assign(queryFilters, { ...draftFilters, page: 1 })
  refetch()
}

async function runFetch(roomId: string, mode: string) {
  fetchingRoomId.value = roomId
  await runFetchMutation.mutateAsync({ roomId, mode })
}
</script>
