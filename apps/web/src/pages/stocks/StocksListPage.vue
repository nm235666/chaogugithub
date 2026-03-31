<template>
  <AppShell title="股票列表" subtitle="统一股票入口，先搜代码、简称、市场、地区，再进入详情与研究工作流。">
    <div class="space-y-4">
      <PageSection title="筛选检索" subtitle="这是整个股票研究链路的入口页。">
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="输入股票代码或简称" />
          <select v-model="filters.status" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部状态</option>
            <option value="L">上市</option>
            <option value="D">退市</option>
            <option value="P">暂停</option>
          </select>
          <select v-model="filters.market" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部市场</option>
            <option v-for="item in stockFilters?.markets || []" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="filters.area" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部地区</option>
            <option v-for="item in stockFilters?.areas || []" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
            <option :value="100">100 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">应用筛选</button>
        </div>
      </PageSection>

      <PageSection :title="`股票结果 (${stocks?.total || 0})`" subtitle="点击代码或名称进入统一股票详情页。">
        <DataTable :columns="columns" :rows="stocks?.items || []" row-key="ts_code" empty-text="暂无股票结果">
          <template #cell-ts_code="{ row }">
            <RouterLink class="font-bold text-[var(--brand)]" :to="`/stocks/detail/${row.ts_code}`">{{ row.ts_code }}</RouterLink>
          </template>
          <template #cell-name="{ row }">
            <RouterLink class="font-semibold" :to="`/stocks/detail/${row.ts_code}`">{{ row.name }}</RouterLink>
          </template>
          <template #cell-list_status="{ row }">
            <StatusBadge :value="row.list_status" :label="listStatusLabel(row.list_status)" />
          </template>
        </DataTable>
        <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ filters.page }} / {{ stocks?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page <= 1" @click="filters.page -= 1">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page >= (stocks?.total_pages || 1)" @click="filters.page += 1">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchStockFilters, fetchStocks } from '../../services/api/stocks'
import { listStatusLabel } from '../../shared/utils/format'

const filters = reactive({
  keyword: '',
  status: '',
  market: '',
  area: '',
  page: 1,
  page_size: 20,
})

const columns = [
  { key: 'ts_code', label: '股票代码' },
  { key: 'symbol', label: '交易代码' },
  { key: 'name', label: '简称' },
  { key: 'area', label: '地区' },
  { key: 'industry', label: '行业' },
  { key: 'market', label: '市场' },
  { key: 'list_date', label: '上市日期' },
  { key: 'list_status', label: '上市状态' },
]

const { data: stockFilters } = useQuery({ queryKey: ['stock-filters'], queryFn: fetchStockFilters })
const { data: stocks } = useQuery({
  queryKey: ['stocks', filters],
  queryFn: () => fetchStocks(filters),
})
</script>
