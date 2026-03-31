<template>
  <AppShell title="股票综合评分" subtitle="统一看行业内评分、核心分项与估值/趋势维度，不再散落在旧页面里。">
    <div class="space-y-4">
      <PageSection title="筛选器" subtitle="按行业、市场、关键词与日期筛选评分结果。">
        <div class="grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="代码或简称" />
          <select v-model="filters.industry" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部行业</option>
            <option v-for="item in scoreFilters?.industries || []" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="filters.market" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部市场</option>
            <option v-for="item in scoreFilters?.markets || []" :key="item" :value="item">{{ item }}</option>
          </select>
          <input v-model="filters.score_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="评分日期 YYYYMMDD，可空" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyFilters">
            {{ isFetching ? '查询中...' : '应用筛选' }}
          </button>
        </div>
      </PageSection>

      <PageSection :title="`评分结果 (${scores?.total || 0})`" subtitle="点击股票名进入详情页做进一步研究。">
        <DataTable :columns="columns" :rows="scores?.items || []" row-key="ts_code" empty-text="暂无评分数据">
          <template #cell-name="{ row }">
            <RouterLink class="font-semibold text-[var(--brand)]" :to="`/stocks/detail/${row.ts_code}`">{{ row.name || row.ts_code }}</RouterLink>
          </template>
          <template #cell-total_score="{ row }">{{ formatNumber(row.total_score, 2) }}</template>
          <template #cell-industry_total_score="{ row }">{{ formatNumber(row.industry_total_score, 2) }}</template>
          <template #cell-news_score="{ row }">{{ formatNumber(row.news_score, 2) }}</template>
          <template #cell-trend_score="{ row }">{{ formatNumber(row.trend_score, 2) }}</template>
          <template #cell-score_grade="{ row }"><StatusBadge value="brand" :label="row.score_grade || '-'" /></template>
        </DataTable>
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
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchStockScoreFilters, fetchStockScores } from '../../services/api/stocks'
import { formatNumber } from '../../shared/utils/format'

const filters = reactive({ keyword: '', industry: '', market: '', score_date: '', page_size: 20 })
const queryFilters = reactive({ keyword: '', industry: '', market: '', score_date: '', page: 1, page_size: 20 })
const columns = [
  { key: 'name', label: '股票' },
  { key: 'industry', label: '行业' },
  { key: 'market', label: '市场' },
  { key: 'total_score', label: '总分' },
  { key: 'industry_total_score', label: '行业内总分' },
  { key: 'trend_score', label: '趋势分' },
  { key: 'news_score', label: '新闻分' },
  { key: 'score_grade', label: '评分等级' },
]

const { data: scoreFilters } = useQuery({ queryKey: ['stock-score-filters'], queryFn: fetchStockScoreFilters })
const { data: scores, isFetching } = useQuery({
  queryKey: computed(() => ['stock-scores', { ...queryFilters }]),
  queryFn: () => fetchStockScores({ ...queryFilters }),
})

function applyFilters() {
  queryFilters.keyword = (filters.keyword || '').trim()
  queryFilters.industry = filters.industry
  queryFilters.market = filters.market
  queryFilters.score_date = (filters.score_date || '').trim()
  queryFilters.page_size = Number(filters.page_size) || 20
  queryFilters.page = 1
}
</script>
