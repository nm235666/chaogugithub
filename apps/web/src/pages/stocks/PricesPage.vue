<template>
  <AppShell title="股票日线价格" subtitle="统一查询历史日线、看收盘趋势，再跳转到详情和研究动作。">
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
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">查询</button>
        </div>
      </PageSection>

      <PageSection title="收盘趋势" subtitle="用最近这次查询结果里的收盘价快速判断趋势。">
        <TrendAreaChart :labels="chart.labels" :series="chart.series" :height="320" empty-text="暂无日线数据" />
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
          <div>第 {{ filters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page <= 1" @click="filters.page -= 1">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page >= (result?.total_pages || 1)" @click="filters.page += 1">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import TrendAreaChart from '../../shared/charts/TrendAreaChart.vue'
import { fetchStockPrices } from '../../services/api/stocks'
import { formatDate, formatNumber, formatPercent } from '../../shared/utils/format'

const filters = reactive({
  ts_code: '',
  start_date: '',
  end_date: '',
  page: 1,
  page_size: 20,
})

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

const { data: result } = useQuery({
  queryKey: ['stock-prices', filters],
  queryFn: () => fetchStockPrices(filters),
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
</script>
