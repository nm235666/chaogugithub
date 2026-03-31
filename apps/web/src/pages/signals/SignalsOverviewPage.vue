<template>
  <AppShell title="投资信号总览" subtitle="股票与主题信号统一收敛，支持来源深筛、状态机筛选和时间线跳转。">
    <div class="space-y-4">
      <PageSection title="信号筛选" subtitle="支持 1d / 7d 口径、来源、状态机和群聊股票等深筛。">
        <div class="grid gap-3 xl:grid-cols-7 md:grid-cols-2">
          <select v-model="filters.scope" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="7d">7 天口径</option>
            <option value="1d">1 天口径</option>
            <option value="30d">30 天口径</option>
          </select>
          <select v-model="filters.signal_group" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部分组</option>
            <option value="stock">仅股票</option>
            <option value="non_stock">仅主题/其他</option>
            <option value="chatroom_stock">仅群聊股票</option>
          </select>
          <select v-model="filters.direction" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部方向</option>
            <option v-for="item in directionOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="filters.source_filter" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部来源</option>
            <option value="chatroom">群聊股票</option>
          </select>
          <select v-model="filters.signal_status" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部状态</option>
            <option v-for="item in statusOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="股票 / 主题关键词" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyFilters">
            {{ isFetching ? '查询中...' : '应用筛选' }}
          </button>
        </div>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="全部信号" :value="summary.signal_total ?? 0" hint="当前口径下的全量信号" />
        <StatCard title="看多信号" :value="summary.bullish_total ?? 0" hint="方向为看多" />
        <StatCard title="看空信号" :value="summary.bearish_total ?? 0" hint="方向为看空" />
        <StatCard title="活跃信号" :value="summary.active_total ?? 0" hint="当前状态仍然活跃" />
      </div>

      <PageSection :title="`信号结果 (${result?.total || 0})`" subtitle="信号强度、来源结构和状态机统一展示，点击即可跳时间线。">
        <DataTable :columns="columns" :rows="result?.items || []" row-key="signal_key" empty-text="暂无信号结果">
          <template #cell_signal_type="{ row }">{{ signalTypeLabel(row.signal_type) }}</template>
          <template #cell_subject_name="{ row }">
            <button class="text-left font-semibold text-[var(--brand)] hover:underline" @click="goTimeline(row)">{{ row.subject_name || '-' }}</button>
            <div class="mt-1 text-xs text-[var(--muted)]">{{ row.ts_code || row.signal_key || '-' }}</div>
          </template>
          <template #cell_direction="{ row }"><StatusBadge :value="row.direction" :label="row.direction || '-'" /></template>
          <template #cell_signal_strength="{ row }">{{ formatNumber(row.signal_strength, 2) }}</template>
          <template #cell_confidence="{ row }">{{ formatNumber(row.confidence, 1) }}</template>
          <template #cell_signal_status="{ row }"><StatusBadge :value="row.signal_status" :label="row.signal_status || '-'" /></template>
          <template #cell_source_mix="{ row }">
            <div class="flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">国际 {{ sourceSummary(row).intl_news || 0 }}</span>
              <span class="metric-chip">国内 {{ sourceSummary(row).domestic_news || 0 }}</span>
              <span class="metric-chip">个股 {{ sourceSummary(row).stock_news || 0 }}</span>
              <span class="metric-chip">群聊 {{ sourceSummary(row).chatroom || 0 }}</span>
            </div>
          </template>
          <template #cell_actions="{ row }">
            <div class="flex flex-wrap gap-2">
              <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]" @click="goTimeline(row)">信号时间线</button>
              <button class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]" @click="goStateTimeline(row)">状态时间线</button>
            </div>
          </template>
        </DataTable>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive } from 'vue'
import { useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import { fetchInvestmentSignals } from '../../services/api/signals'
import { formatNumber } from '../../shared/utils/format'
import { parseJsonObject } from '../../shared/utils/finance'

const router = useRouter()

const filters = reactive({
  scope: '7d',
  signal_group: '',
  direction: '',
  source_filter: '',
  signal_status: '',
  keyword: '',
  page_size: 30,
})
const queryFilters = reactive({
  scope: '7d',
  signal_group: '',
  direction: '',
  source_filter: '',
  signal_status: '',
  keyword: '',
  page: 1,
  page_size: 30,
})

const columns = [
  { key: 'signal_type', label: '实体类型' },
  { key: 'subject_name', label: '实体名称' },
  { key: 'direction', label: '方向' },
  { key: 'signal_strength', label: '强度' },
  { key: 'confidence', label: '置信度' },
  { key: 'signal_status', label: '状态机' },
  { key: 'source_mix', label: '来源结构' },
  { key: 'actions', label: '跳转' },
]
const { data: result, isFetching } = useQuery({
  queryKey: computed(() => ['investment-signals', { ...queryFilters }]),
  queryFn: () => fetchInvestmentSignals({ ...queryFilters }),
})
const summary = computed(() => result.value?.summary || {})
const directionOptions = computed(() => result.value?.filters?.directions || [])
const statusOptions = computed(() => result.value?.filters?.signal_statuses || [])

function signalTypeLabel(value: unknown) {
  const raw = String(value || '').trim()
  return ({ stock: '股票', theme: '主题' } as Record<string, string>)[raw] || raw || '-'
}

function sourceSummary(row: Record<string, any>) {
  return parseJsonObject(row.source_summary_json)
}

function goTimeline(row: Record<string, any>) {
  router.push({ path: '/signals/timeline', query: { signal_key: row.signal_key } })
}

function goStateTimeline(row: Record<string, any>) {
  const scope = row.signal_type === 'theme' ? 'theme' : 'stock'
  const signalKey = scope === 'theme' && !String(row.signal_key || '').startsWith('theme:') ? `theme:${row.subject_name}` : row.signal_key
  router.push({ path: '/signals/state-timeline', query: { signal_scope: scope, signal_key: signalKey } })
}

function applyFilters() {
  queryFilters.scope = filters.scope
  queryFilters.signal_group = filters.signal_group
  queryFilters.direction = filters.direction
  queryFilters.source_filter = filters.source_filter
  queryFilters.signal_status = filters.signal_status
  queryFilters.keyword = (filters.keyword || '').trim()
  queryFilters.page_size = Number(filters.page_size) || 30
  queryFilters.page = 1
}
</script>
