<template>
  <AppShell title="股票价格中心" subtitle="同一页面查看日线和分钟线，统一查询、统一图表、统一明细。">
    <div class="space-y-4">
      <PageSection title="查询条件" subtitle="把日线和分钟线拆成两个上下文模块，优先让主图表和结果更快露出。">
        <div class="grid gap-4 xl:grid-cols-2">
          <div class="rounded-[22px] border border-[var(--line)] bg-white/78 p-4">
            <div class="mb-3 text-sm font-bold text-[var(--ink)]">日线查询</div>
            <div class="grid gap-3 md:grid-cols-2">
              <label class="text-sm font-semibold text-[var(--ink)]">
                股票代码
                <input v-model="filters.ts_code" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 000001.SZ" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                每页条数
                <select v-model.number="filters.page_size" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                  <option :value="20">20 / 页</option>
                  <option :value="50">50 / 页</option>
                  <option :value="100">100 / 页</option>
                </select>
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                开始日期
                <input v-model="filters.start_date" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="YYYYMMDD" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                结束日期
                <input v-model="filters.end_date" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="YYYYMMDD" />
              </label>
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="submitSearch">查询日线</button>
              <span class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs text-[var(--muted)]">支持按股票或时间区间快速回看</span>
            </div>
          </div>

          <div class="rounded-[22px] border border-[var(--line)] bg-white/78 p-4">
            <div class="mb-3 text-sm font-bold text-[var(--ink)]">分钟线查询</div>
            <div class="grid gap-3 md:grid-cols-2">
              <label class="text-sm font-semibold text-[var(--ink)]">
                股票代码
                <input v-model="minuteFilters.ts_code" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 600114.SH" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                交易日
                <input v-model="minuteFilters.trade_date" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="YYYYMMDD，可空" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                数据点数
                <select v-model.number="minuteFilters.page_size" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                  <option :value="500">500 点</option>
                  <option :value="300">300 点</option>
                  <option :value="240">240 点</option>
                </select>
              </label>
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <button class="rounded-2xl bg-stone-900 px-4 py-3 font-semibold text-white" @click="submitMinuteSearch">查询分钟线</button>
              <span class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs text-[var(--muted)]">适合盘中走势和均价结构回看</span>
            </div>
          </div>
        </div>

        <div class="mt-3 flex flex-wrap gap-2 lg:hidden">
          <button class="rounded-full px-4 py-2 text-sm font-semibold" :class="mobilePanel === 'daily' ? 'bg-[var(--brand)] text-white' : 'border border-[var(--line)] bg-white text-[var(--muted)]'" @click="showDailyPanel">日线视图</button>
          <button class="rounded-full px-4 py-2 text-sm font-semibold" :class="mobilePanel === 'minute' ? 'bg-[var(--brand)] text-white' : 'border border-[var(--line)] bg-white text-[var(--muted)]'" @click="showMinutePanel">分钟线视图</button>
        </div>

        <div class="mt-3 text-sm text-[var(--muted)]" role="status" aria-live="polite">
          <template v-if="!hasSearched && !hasMinuteSearched">请先输入查询条件，再执行日线或分钟线查询。</template>
          <template v-else-if="result?.message">{{ result.message }}</template>
          <template v-else>日线与分钟线会分别保留最近一次查询结果。</template>
        </div>
      </PageSection>

      <div :class="mobilePanel === 'daily' ? '' : 'hidden lg:block'">
        <PageSection title="收盘趋势" subtitle="用最近这次查询结果里的收盘价快速判断趋势。">
          <TrendAreaChart
            v-if="showChart"
            :labels="chart.labels"
            :series="chart.series"
            :height="320"
            empty-text="暂无日线数据"
          />
          <div v-else class="loading-skeleton rounded-2xl border border-dashed border-[var(--line)] px-4 py-10 text-center text-sm text-[var(--muted)]">
            暂无可展示的趋势数据，先执行一次查询。
          </div>
        </PageSection>

        <PageSection :title="`日线结果 (${result?.total || 0})`" subtitle="点击股票代码可回到统一详情页。">
          <div class="grid gap-3 lg:hidden">
            <InfoCard
              v-for="row in result?.items || []"
              :key="`${row.ts_code}-${row.trade_date}`"
              :title="row.name || row.ts_code || '-'"
              :meta="`${row.ts_code || '-'} · ${formatDate(row.trade_date)}`"
              :description="`收盘 ${formatNumber(row.close, 2)} · 涨跌幅 ${formatPercent(row.pct_chg, 2)}`"
            >
              <div class="flex flex-wrap gap-2 text-xs">
                <span class="metric-chip">开盘 <strong>{{ formatNumber(row.open, 2) }}</strong></span>
                <span class="metric-chip">最高 <strong>{{ formatNumber(row.high, 2) }}</strong></span>
                <span class="metric-chip">最低 <strong>{{ formatNumber(row.low, 2) }}</strong></span>
                <span class="metric-chip">成交量 <strong>{{ formatNumber(row.vol, 0) }}</strong></span>
              </div>
            </InfoCard>
          </div>

          <DataTable class="hidden lg:block" :columns="columns" :rows="result?.items || []" row-key="trade_date" empty-text="暂无日线数据" caption="日线结果表" aria-label="日线结果表">
            <template #cell-ts_code="{ row }">
              <RouterLink class="font-semibold text-[var(--brand)]" :to="`/stocks/detail/${row.ts_code}`">{{ row.ts_code || '-' }}</RouterLink>
            </template>
            <template #cell-name="{ row }">{{ row.name || '-' }}</template>
            <template #cell-trade_date="{ row }">{{ formatDate(row.trade_date) }}</template>
            <template #cell-open="{ row }">{{ formatNumber(row.open, 2) }}</template>
            <template #cell-high="{ row }">{{ formatNumber(row.high, 2) }}</template>
            <template #cell-low="{ row }">{{ formatNumber(row.low, 2) }}</template>
            <template #cell-close="{ row }">{{ formatNumber(row.close, 2) }}</template>
            <template #cell-pre_close="{ row }">{{ formatNumber(row.pre_close, 2) }}</template>
            <template #cell-change="{ row }">{{ formatNumber(row.change, 2) }}</template>
            <template #cell-pct_chg="{ row }">{{ formatPercent(row.pct_chg, 2) }}</template>
            <template #cell-vol="{ row }">{{ formatNumber(row.vol, 0) }}</template>
            <template #cell-amount="{ row }">{{ formatNumber(row.amount, 0) }}</template>
          </DataTable>

          <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
            <div>第 {{ queryParams.page }} / {{ result?.total_pages || 1 }} 页</div>
            <div class="flex gap-2">
              <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryParams.page <= 1" @click="goPrevPage">上一页</button>
              <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryParams.page >= (result?.total_pages || 1)" @click="goNextPage">下一页</button>
            </div>
          </div>
        </PageSection>
      </div>

      <div :class="mobilePanel === 'minute' ? '' : 'hidden lg:block'">
        <PageSection title="分钟 K 线图" subtitle="分钟价格 + 均价 + 成交量。">
          <MinuteKlineChart v-if="hasMinuteSearched" :items="minuteResult?.items || []" :height="620" empty-text="暂无分钟线数据" />
          <div v-else class="loading-skeleton rounded-2xl border border-dashed border-[var(--line)] px-4 py-10 text-center text-sm text-[var(--muted)]">
            暂无可展示的分钟线数据，先执行一次查询。
          </div>
        </PageSection>

        <PageSection :title="`分钟线结果 (${minuteResult?.total || 0})`" subtitle="最近一批分钟线明细。">
          <div class="grid gap-3 lg:hidden">
            <InfoCard
              v-for="row in minuteResult?.items || []"
              :key="`${row.trade_date}-${row.minute_time}`"
              :title="`${formatDate(row.trade_date)} ${row.minute_time || '-'}`"
              :description="`价格 ${formatNumber(row.price, 3)} · 均价 ${formatNumber(row.avg_price, 3)}`"
            >
              <div class="flex flex-wrap gap-2 text-xs">
                <span class="metric-chip">成交量 <strong>{{ formatNumber(row.volume, 0) }}</strong></span>
                <span class="metric-chip">累计成交量 <strong>{{ formatNumber(row.total_volume, 0) }}</strong></span>
              </div>
            </InfoCard>
          </div>

          <DataTable class="hidden lg:block" :columns="minuteColumns" :rows="minuteResult?.items || []" row-key="minute_time" empty-text="暂无分钟线明细" caption="分钟线结果表" aria-label="分钟线结果表">
            <template #cell-trade_date="{ row }">{{ formatDate(row.trade_date) }}</template>
            <template #cell-price="{ row }">{{ formatNumber(row.price, 3) }}</template>
            <template #cell-avg_price="{ row }">{{ formatNumber(row.avg_price, 3) }}</template>
            <template #cell-volume="{ row }">{{ formatNumber(row.volume, 0) }}</template>
            <template #cell-total_volume="{ row }">{{ formatNumber(row.total_volume, 0) }}</template>
          </DataTable>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, reactive, ref, watch } from 'vue'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchStockMinline, fetchStockPrices } from '../../services/api/stocks'
import { formatDate, formatNumber, formatPercent } from '../../shared/utils/format'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

const TrendAreaChart = defineAsyncComponent(() => import('../../shared/charts/TrendAreaChart.vue'))
const MinuteKlineChart = defineAsyncComponent(() => import('../../shared/charts/MinuteKlineChart.vue'))
const route = useRoute()
const router = useRouter()

const filters = reactive({
  ts_code: '',
  start_date: '',
  end_date: '',
  page_size: 20,
})
const queryParams = reactive({
  ts_code: '',
  start_date: '',
  end_date: '',
  page: 1,
  page_size: 20,
})
const hasSearched = ref(false)
const mobilePanel = ref<'daily' | 'minute'>('daily')
const minuteFilters = reactive({
  ts_code: '600114.SH',
  trade_date: '',
  page_size: 500,
})
const minuteQueryParams = reactive({
  ts_code: '600114.SH',
  trade_date: '',
  page: 1,
  page_size: 500,
})
const hasMinuteSearched = ref(false)

const columns = [
  { key: 'trade_date', label: '交易日期' },
  { key: 'ts_code', label: '股票代码' },
  { key: 'name', label: '股票简称' },
  { key: 'open', label: '开盘价' },
  { key: 'high', label: '最高价' },
  { key: 'low', label: '最低价' },
  { key: 'close', label: '收盘价' },
  { key: 'pre_close', label: '昨收价' },
  { key: 'change', label: '涨跌额' },
  { key: 'pct_chg', label: '涨跌幅' },
  { key: 'vol', label: '成交量' },
  { key: 'amount', label: '成交额' },
]
const minuteColumns = [
  { key: 'trade_date', label: '交易日' },
  { key: 'minute_time', label: '分钟时间' },
  { key: 'price', label: '价格' },
  { key: 'avg_price', label: '均价' },
  { key: 'volume', label: '成交量' },
  { key: 'total_volume', label: '累计成交量' },
]

const { data: result } = useQuery({
  queryKey: computed(() => ['stock-prices', { ...queryParams }]),
  queryFn: () => fetchStockPrices({ ...queryParams }),
  enabled: computed(() => hasSearched.value),
  placeholderData: keepPreviousData,
})
const { data: minuteResult } = useQuery({
  queryKey: computed(() => ['stock-minline-on-prices-page', { ...minuteQueryParams }]),
  queryFn: () => fetchStockMinline({ ...minuteQueryParams }),
  enabled: computed(() => hasMinuteSearched.value),
  placeholderData: keepPreviousData,
})

const chart = computed(() => {
  const rows = [...(result.value?.items || [])].reverse()
  return {
    labels: rows.map((item: Record<string, any>) => formatDate(item.trade_date)),
    series: [
      {
        name: '收盘价',
        data: rows.map((item: Record<string, any>) => Number(item.close)),
        color: '#0f617a',
        area: true,
      },
    ],
  }
})

const showChart = computed(() => hasSearched.value && (result.value?.items?.length || 0) > 0)

function submitSearch() {
  queryParams.ts_code = (filters.ts_code || '').trim().toUpperCase()
  queryParams.start_date = (filters.start_date || '').trim()
  queryParams.end_date = (filters.end_date || '').trim()
  queryParams.page_size = Number(filters.page_size) || 20
  queryParams.page = 1
  hasSearched.value = true
  mobilePanel.value = 'daily'
  syncRouteFromState()
}

function goPrevPage() {
  if (queryParams.page <= 1) return
  queryParams.page -= 1
  syncRouteFromState()
}

function goNextPage() {
  const totalPages = Number(result.value?.total_pages || 1)
  if (queryParams.page >= totalPages) return
  queryParams.page += 1
  syncRouteFromState()
}

function submitMinuteSearch() {
  minuteQueryParams.ts_code = (minuteFilters.ts_code || '').trim().toUpperCase()
  minuteQueryParams.trade_date = (minuteFilters.trade_date || '').trim()
  minuteQueryParams.page_size = Number(minuteFilters.page_size) || 500
  minuteQueryParams.page = 1
  hasMinuteSearched.value = true
  mobilePanel.value = 'minute'
  syncRouteFromState()
}

function showDailyPanel() {
  mobilePanel.value = 'daily'
  syncRouteFromState()
}

function showMinutePanel() {
  mobilePanel.value = 'minute'
  syncRouteFromState()
}

function syncRouteFromState() {
  router.replace({
    query: buildCleanQuery({
      panel: mobilePanel.value,
      daily: hasSearched.value ? '1' : '',
      ts_code: hasSearched.value ? queryParams.ts_code : '',
      start_date: hasSearched.value ? queryParams.start_date : '',
      end_date: hasSearched.value ? queryParams.end_date : '',
      page: hasSearched.value ? queryParams.page : '',
      page_size: hasSearched.value ? queryParams.page_size : '',
      minute: hasMinuteSearched.value ? '1' : '',
      m_ts_code: hasMinuteSearched.value ? minuteQueryParams.ts_code : '',
      m_trade_date: hasMinuteSearched.value ? minuteQueryParams.trade_date : '',
      m_page_size: hasMinuteSearched.value ? minuteQueryParams.page_size : '',
    }),
  })
}

function applyRouteState() {
  const q = route.query as Record<string, unknown>
  const panel = readQueryString(q, 'panel', 'daily')
  mobilePanel.value = panel === 'minute' ? 'minute' : 'daily'

  const dailyEnabled = readQueryString(q, 'daily', '') === '1'
    || !!readQueryString(q, 'ts_code', '')
    || !!readQueryString(q, 'start_date', '')
    || !!readQueryString(q, 'end_date', '')
  if (dailyEnabled) {
    const tsCode = readQueryString(q, 'ts_code', '').toUpperCase()
    const startDate = readQueryString(q, 'start_date', '')
    const endDate = readQueryString(q, 'end_date', '')
    const page = Math.max(1, readQueryNumber(q, 'page', 1))
    const pageSize = Math.max(20, readQueryNumber(q, 'page_size', 20))
    Object.assign(filters, { ts_code: tsCode, start_date: startDate, end_date: endDate, page_size: pageSize })
    Object.assign(queryParams, { ts_code: tsCode, start_date: startDate, end_date: endDate, page, page_size: pageSize })
    hasSearched.value = true
  } else {
    hasSearched.value = false
  }

  const minuteEnabled = readQueryString(q, 'minute', '') === '1'
    || !!readQueryString(q, 'm_ts_code', '')
    || !!readQueryString(q, 'm_trade_date', '')
  if (minuteEnabled) {
    const tsCode = readQueryString(q, 'm_ts_code', minuteFilters.ts_code).toUpperCase()
    const tradeDate = readQueryString(q, 'm_trade_date', '')
    const pageSize = Math.max(240, readQueryNumber(q, 'm_page_size', 500))
    Object.assign(minuteFilters, { ts_code: tsCode, trade_date: tradeDate, page_size: pageSize })
    Object.assign(minuteQueryParams, { ts_code: tsCode, trade_date: tradeDate, page: 1, page_size: pageSize })
    hasMinuteSearched.value = true
  } else {
    hasMinuteSearched.value = false
  }
}

watch(
  () => route.query,
  () => {
    applyRouteState()
  },
  { immediate: true },
)
</script>
