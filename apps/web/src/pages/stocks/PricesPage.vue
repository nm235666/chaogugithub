<template>
  <AppShell title="股票价格中心" subtitle="同一页面查看日线和分钟线，统一查询、统一图表、统一明细。">
    <div class="space-y-4">
      <PageSection title="查询条件" subtitle="按股票、日期区间和分页查看日线数据。">
        <div class="grid gap-3 xl:grid-cols-[180px_160px_160px_140px_120px] md:grid-cols-2">
          <input v-model="filters.ts_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="股票代码，如 000001.SZ" />
          <input v-model="filters.start_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="开始日期 YYYYMMDD" />
          <input v-model="filters.end_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="结束日期 YYYYMMDD" />
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
            <option :value="100">100 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="submitSearch">查询</button>
        </div>
        <p v-if="!hasSearched" class="mt-2 text-sm text-[var(--muted)]">请先输入股票代码或日期区间，再点击“查询”。</p>
        <p v-else-if="result?.message" class="mt-2 text-sm text-amber-700">{{ result.message }}</p>
      </PageSection>

      <PageSection title="收盘趋势" subtitle="用最近这次查询结果里的收盘价快速判断趋势。">
        <TrendAreaChart
          v-if="showChart"
          :labels="chart.labels"
          :series="chart.series"
          :height="320"
          empty-text="暂无日线数据"
        />
        <div v-else class="rounded-2xl border border-dashed border-[var(--line)] bg-white/70 px-4 py-10 text-center text-sm text-[var(--muted)]">
          暂无可展示的趋势数据，先执行一次查询。
        </div>
      </PageSection>

      <PageSection :title="`日线结果 (${result?.total || 0})`" subtitle="点击股票代码可回到统一详情页。">
        <DataTable :columns="columns" :rows="result?.items || []" row-key="trade_date" empty-text="暂无日线数据">
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

      <PageSection title="分钟线查询" subtitle="同页查看分钟 K 线与明细，无需切换到单独页面。">
        <div class="grid gap-3 xl:grid-cols-[180px_180px_140px_120px] md:grid-cols-2">
          <input v-model="minuteFilters.ts_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="股票代码，如 600114.SH" />
          <input v-model="minuteFilters.trade_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="交易日 YYYYMMDD，可空" />
          <select v-model.number="minuteFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="500">500 点</option>
            <option :value="300">300 点</option>
            <option :value="240">240 点</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="submitMinuteSearch">查询</button>
        </div>
        <p v-if="!hasMinuteSearched" class="mt-2 text-sm text-[var(--muted)]">请先输入分钟线查询条件，再点击“查询”。</p>
      </PageSection>

      <PageSection title="分钟 K 线图" subtitle="分钟价格 + 均价 + 成交量。">
        <MinuteKlineChart v-if="hasMinuteSearched" :items="minuteResult?.items || []" :height="620" empty-text="暂无分钟线数据" />
        <div v-else class="rounded-2xl border border-dashed border-[var(--line)] bg-white/70 px-4 py-10 text-center text-sm text-[var(--muted)]">
          暂无可展示的分钟线数据，先执行一次查询。
        </div>
      </PageSection>

      <PageSection :title="`分钟线结果 (${minuteResult?.total || 0})`" subtitle="最近一批分钟线明细。">
        <DataTable :columns="minuteColumns" :rows="minuteResult?.items || []" row-key="minute_time" empty-text="暂无分钟线明细">
          <template #cell-trade_date="{ row }">{{ formatDate(row.trade_date) }}</template>
          <template #cell-price="{ row }">{{ formatNumber(row.price, 3) }}</template>
          <template #cell-avg_price="{ row }">{{ formatNumber(row.avg_price, 3) }}</template>
          <template #cell-volume="{ row }">{{ formatNumber(row.volume, 0) }}</template>
          <template #cell-total_volume="{ row }">{{ formatNumber(row.total_volume, 0) }}</template>
        </DataTable>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, reactive, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import { fetchStockMinline, fetchStockPrices } from '../../services/api/stocks'
import { formatDate, formatNumber, formatPercent } from '../../shared/utils/format'
const TrendAreaChart = defineAsyncComponent(() => import('../../shared/charts/TrendAreaChart.vue'))
const MinuteKlineChart = defineAsyncComponent(() => import('../../shared/charts/MinuteKlineChart.vue'))

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
})
const { data: minuteResult } = useQuery({
  queryKey: computed(() => ['stock-minline-on-prices-page', { ...minuteQueryParams }]),
  queryFn: () => fetchStockMinline({ ...minuteQueryParams }),
  enabled: computed(() => hasMinuteSearched.value),
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
}

function goPrevPage() {
  if (queryParams.page <= 1) return
  queryParams.page -= 1
}

function goNextPage() {
  const totalPages = Number(result.value?.total_pages || 1)
  if (queryParams.page >= totalPages) return
  queryParams.page += 1
}

function submitMinuteSearch() {
  minuteQueryParams.ts_code = (minuteFilters.ts_code || '').trim().toUpperCase()
  minuteQueryParams.trade_date = (minuteFilters.trade_date || '').trim()
  minuteQueryParams.page_size = Number(minuteFilters.page_size) || 500
  minuteQueryParams.page = 1
  hasMinuteSearched.value = true
}
</script>
