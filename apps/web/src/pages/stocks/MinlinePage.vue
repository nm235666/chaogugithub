<template>
  <AppShell title="股票分钟线" subtitle="统一看分钟 K 线、均价和成交量，不再依赖独立旧页。">
    <div class="space-y-4">
      <PageSection title="分钟线查询" subtitle="查询某只股票某个交易日的分钟数据。">
        <div class="grid gap-3 xl:grid-cols-[180px_180px_140px_120px] md:grid-cols-2">
          <input v-model="filters.ts_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="股票代码，如 600114.SH" />
          <input v-model="filters.trade_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="交易日 YYYYMMDD，可空" />
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="500">500 点</option>
            <option :value="300">300 点</option>
            <option :value="240">240 点</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">绘制</button>
        </div>
      </PageSection>

      <PageSection title="分钟 K 线图" subtitle="使用分钟价格构造 K 线，并叠加均价与成交量。">
        <MinuteKlineChart :items="result?.items || []" :height="620" empty-text="暂无分钟线数据" />
      </PageSection>

      <PageSection :title="`分钟数据 (${result?.total || 0})`" subtitle="最近一批分钟数据明细。">
        <DataTable :columns="columns" :rows="result?.items || []" row-key="minute_time" empty-text="暂无分钟线明细">
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
import { reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import MinuteKlineChart from '../../shared/charts/MinuteKlineChart.vue'
import { fetchStockMinline } from '../../services/api/stocks'
import { formatDate, formatNumber } from '../../shared/utils/format'

const filters = reactive({
  ts_code: '600114.SH',
  trade_date: '',
  page: 1,
  page_size: 500,
})

const columns = [
  { key: 'trade_date', label: '交易日' },
  { key: 'minute_time', label: '分钟时间' },
  { key: 'price', label: '价格' },
  { key: 'avg_price', label: '均价' },
  { key: 'volume', label: '成交量' },
  { key: 'total_volume', label: '累计成交量' },
]

const { data: result } = useQuery({
  queryKey: ['stock-minline', filters],
  queryFn: () => fetchStockMinline(filters),
})
</script>
