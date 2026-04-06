<template>
  <AppShell title="投资信号总览" subtitle="股票与主题信号统一收敛，支持来源深筛、状态机筛选和时间线跳转。">
    <div class="space-y-4">
      <PageSection title="信号筛选" subtitle="常用筛选先露出，高级筛选按需展开，避免首屏先看到完整筛选墙。">
        <div class="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
          <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <label class="text-sm font-semibold text-[var(--ink)]">
              观察口径
              <select v-model="filters.scope" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                <option value="7d">7 天口径</option>
                <option value="1d">1 天口径</option>
                <option value="30d">30 天口径</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              信号分组
              <select v-model="filters.signal_group" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                <option value="">全部分组</option>
                <option value="stock">仅股票</option>
                <option value="non_stock">仅主题/其他</option>
                <option value="chatroom_stock">仅群聊股票</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              关键词
              <input v-model="filters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="股票 / 主题关键词" />
            </label>
          </div>

          <div class="rounded-[22px] border border-[var(--line)] bg-white/78 p-4">
            <div class="flex items-center justify-between gap-2">
              <div>
                <div class="text-sm font-bold text-[var(--ink)]">高级筛选</div>
                <div class="mt-1 text-xs text-[var(--muted)]">方向、来源和状态机在需要时再展开，保留结果理解上下文。</div>
              </div>
              <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--muted)]" :aria-expanded="advancedFiltersOpen ? 'true' : 'false'" @click="advancedFiltersOpen = !advancedFiltersOpen">
                {{ advancedFiltersOpen ? '收起' : '展开' }}
              </button>
            </div>
            <div v-if="advancedFiltersOpen" class="mt-3 grid gap-3 md:grid-cols-3">
              <label class="text-sm font-semibold text-[var(--ink)]">
                方向
                <select v-model="filters.direction" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                  <option value="">全部方向</option>
                  <option v-for="item in directionOptions" :key="item" :value="item">{{ item }}</option>
                </select>
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                来源
                <select v-model="filters.source_filter" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                  <option value="">全部来源</option>
                  <option value="chatroom">群聊股票</option>
                </select>
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                状态机
                <select v-model="filters.signal_status" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                  <option value="">全部状态</option>
                  <option v-for="item in statusOptions" :key="item" :value="item">{{ item }}</option>
                </select>
              </label>
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyFilters">
                {{ isFetching ? '查询中...' : '应用筛选' }}
              </button>
              <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)]" @click="resetFilters">恢复默认筛选</button>
            </div>
          </div>
        </div>

        <div v-if="activeFilterChips.length" class="mt-3 flex flex-wrap gap-2">
          <span v-for="item in activeFilterChips" :key="item" class="metric-chip">{{ item }}</span>
        </div>

        <StatePanel
          v-if="signalsError"
          class="mt-3"
          tone="danger"
          title="信号列表加载失败"
          :description="signalsError"
        >
          <template #action>
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="retryQuery">重新加载</button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 font-semibold text-[var(--ink)]" @click="resetFilters">清空筛选</button>
          </template>
        </StatePanel>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="全部信号" :value="summary.signal_total ?? 0" hint="当前口径下的全量信号" />
        <StatCard title="看多信号" :value="summary.bullish_total ?? 0" hint="方向为看多" />
        <StatCard title="看空信号" :value="summary.bearish_total ?? 0" hint="方向为看空" />
        <StatCard title="活跃信号" :value="summary.active_total ?? 0" hint="当前状态仍然活跃" />
      </div>

      <PageSection :title="`信号结果 (${result?.total || 0})`" subtitle="小屏先看摘要卡片，桌面端保留表格与时间线跳转。">
        <StatePanel
          v-if="!signalsError && !isFetching && !(result?.items || []).length"
          class="mb-4"
          tone="warning"
          title="当前筛选下没有信号结果"
          description="可以先清空来源、状态机或关键词筛选，再回到 7 天口径查看更完整的结果。"
        >
          <template #action>
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 font-semibold text-white" @click="resetFilters">恢复默认筛选</button>
          </template>
        </StatePanel>

        <div class="grid gap-3 lg:hidden">
          <InfoCard
            v-for="row in result?.items || []"
            :key="row.signal_key"
            :title="row.subject_name || '-'"
            :meta="`${signalTypeLabel(row.signal_type)} · ${row.ts_code || row.signal_key || '-'}`"
            :description="`强度 ${formatNumber(row.signal_strength, 2)} · 置信度 ${formatNumber(row.confidence, 1)}`"
          >
            <div class="flex flex-wrap gap-2">
              <StatusBadge :value="row.direction" :label="row.direction || '-'" />
              <StatusBadge :value="row.signal_status" :label="row.signal_status || '-'" />
              <span class="metric-chip">国际 {{ sourceSummary(row).intl_news || 0 }}</span>
              <span class="metric-chip">国内 {{ sourceSummary(row).domestic_news || 0 }}</span>
              <span class="metric-chip">个股 {{ sourceSummary(row).stock_news || 0 }}</span>
              <span class="metric-chip">群聊 {{ sourceSummary(row).chatroom || 0 }}</span>
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)]" @click="goTimeline(row)">信号时间线</button>
              <button class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs font-semibold text-[var(--muted)]" @click="goStateTimeline(row)">状态时间线</button>
            </div>
          </InfoCard>
        </div>

        <DataTable class="hidden lg:block" :columns="columns" :rows="result?.items || []" row-key="signal_key" empty-text="暂无信号结果" caption="投资信号结果表" aria-label="投资信号结果表">
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
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchInvestmentSignals } from '../../services/api/signals'
import { formatNumber } from '../../shared/utils/format'
import { parseJsonObject } from '../../shared/utils/finance'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

const route = useRoute()
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
const advancedFiltersOpen = ref(false)

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

const { data: result, isFetching, error, refetch } = useQuery({
  queryKey: computed(() => ['investment-signals', { ...queryFilters }]),
  queryFn: () => fetchInvestmentSignals({ ...queryFilters }),
  placeholderData: keepPreviousData,
})

const summary = computed(() => result.value?.summary || {})
const directionOptions = computed(() => result.value?.filters?.directions || [])
const statusOptions = computed(() => result.value?.filters?.signal_statuses || [])
const signalsError = computed(() => error.value?.message || '')
const activeFilterChips = computed(() => {
  const items = [
    queryFilters.scope ? `口径 ${queryFilters.scope}` : '',
    queryFilters.signal_group ? `分组 ${queryFilters.signal_group}` : '',
    queryFilters.direction ? `方向 ${queryFilters.direction}` : '',
    queryFilters.source_filter ? `来源 ${queryFilters.source_filter}` : '',
    queryFilters.signal_status ? `状态 ${queryFilters.signal_status}` : '',
    queryFilters.keyword ? `关键词 ${queryFilters.keyword}` : '',
  ]
  return items.filter(Boolean)
})

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
  syncRouteFromQuery()
}

function resetFilters() {
  Object.assign(filters, {
    scope: '7d',
    signal_group: '',
    direction: '',
    source_filter: '',
    signal_status: '',
    keyword: '',
    page_size: 30,
  })
  applyFilters()
}

function retryQuery() {
  refetch()
}

function syncRouteFromQuery() {
  router.replace({
    query: buildCleanQuery({
      scope: queryFilters.scope,
      signal_group: queryFilters.signal_group,
      direction: queryFilters.direction,
      source_filter: queryFilters.source_filter,
      signal_status: queryFilters.signal_status,
      keyword: queryFilters.keyword,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteQuery() {
  const q = route.query as Record<string, unknown>
  const next = {
    scope: readQueryString(q, 'scope', '7d'),
    signal_group: readQueryString(q, 'signal_group', ''),
    direction: readQueryString(q, 'direction', ''),
    source_filter: readQueryString(q, 'source_filter', ''),
    signal_status: readQueryString(q, 'signal_status', ''),
    keyword: readQueryString(q, 'keyword', ''),
    page_size: Math.max(10, readQueryNumber(q, 'page_size', 30)),
  }
  Object.assign(filters, next)
  Object.assign(queryFilters, {
    ...next,
    page: 1,
  })
}

onMounted(() => {
  applyRouteQuery()
})

watch(
  () => route.query,
  () => {
    applyRouteQuery()
  },
)
</script>
