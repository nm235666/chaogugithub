<template>
  <AppShell title="宏观数据看板" subtitle="统一查询宏观指标、频率、区间和历史序列。">
    <div class="space-y-4">
      <PageSection title="宏观查询" subtitle="选择指标、频率与周期区间，快速查看时间序列。">
        <div class="grid gap-3 xl:grid-cols-[1.2fr_120px_160px_160px_120px] md:grid-cols-2">
          <select v-model="filters.indicator_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">选择指标（可选）</option>
            <option v-for="item in indicatorOptions" :key="item.indicator_code" :value="item.indicator_code">
              {{ item.indicator_name || item.indicator_code }} [{{ item.indicator_code }}]
            </option>
          </select>
          <select v-model="filters.freq" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部频率</option>
            <option value="D">日频</option>
            <option value="W">周频</option>
            <option value="M">月频</option>
            <option value="Q">季频</option>
            <option value="Y">年频</option>
          </select>
          <input v-model="filters.period_start" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="起始周期，如 202001" />
          <input v-model="filters.period_end" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="结束周期，如 202512" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">查询</button>
        </div>
      </PageSection>

      <PageSection title="指标趋势" subtitle="按统计周期查看指标走势。">
        <TrendAreaChart :labels="chart.labels" :series="chart.series" :height="360" empty-text="暂无宏观序列数据" />
      </PageSection>

      <PageSection :title="`宏观结果 (${result?.total || 0})`" subtitle="查询结果与源字段保持一致，便于校验和导出。">
        <DataTable :columns="columns" :rows="result?.items || []" row-key="period" empty-text="暂无宏观结果" />
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import TrendAreaChart from '../../shared/charts/TrendAreaChart.vue'
import { fetchMacroIndicators, fetchMacroSeries } from '../../services/api/macro'

const filters = reactive({
  indicator_code: '',
  freq: '',
  period_start: '',
  period_end: '',
  keyword: '',
  page: 1,
  page_size: 500,
})

const columns = [
  { key: 'indicator_code', label: '指标代码' },
  { key: 'indicator_name', label: '指标名称' },
  { key: 'freq', label: '频率' },
  { key: 'period', label: '统计周期' },
  { key: 'value', label: '指标值' },
  { key: 'source', label: '数据来源' },
]

const { data: indicators } = useQuery({
  queryKey: ['macro-indicators'],
  queryFn: fetchMacroIndicators,
})

const { data: result } = useQuery({
  queryKey: ['macro-series', filters],
  queryFn: () => fetchMacroSeries(filters),
})

const indicatorOptions = computed(() => indicators.value?.items || [])
const chart = computed(() => {
  const rows = [...(result.value?.items || [])].sort((a: Record<string, any>, b: Record<string, any>) => String(a.period || '').localeCompare(String(b.period || '')))
  const label = rows[0]?.indicator_name || rows[0]?.indicator_code || '指标值'
  return {
    labels: rows.map((item: Record<string, any>) => String(item.period || '')),
    series: [
      {
        name: label,
        data: rows.map((item: Record<string, any>) => Number(item.value)),
        color: '#0f617a',
        area: true,
      },
    ],
  }
})
</script>
