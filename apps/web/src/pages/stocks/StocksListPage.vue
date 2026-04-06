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
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyFilters">
            {{ isFetching ? '查询中...' : '应用筛选' }}
          </button>
        </div>
      </PageSection>

      <PageSection :title="`股票结果 (${stocks?.total || 0})`" subtitle="点击代码或名称进入统一股票详情页。">
        <div class="grid gap-3 lg:hidden">
          <InfoCard
            v-for="row in stocks?.items || []"
            :key="row.ts_code"
            :title="row.name || row.ts_code || '-'"
            :meta="`${row.ts_code || '-'} · ${row.industry || '-'} · ${row.market || '-'}`"
            :description="`地区 ${row.area || '-'} · 上市 ${row.list_date || '-'} · 状态 ${listStatusLabel(row.list_status)}`"
          >
            <template #badge>
              <StatusBadge :value="row.list_status" :label="listStatusLabel(row.list_status)" />
            </template>
            <div class="mt-3">
              <RouterLink class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--brand)]" :to="`/stocks/detail/${row.ts_code}`">查看详情</RouterLink>
            </div>
          </InfoCard>
        </div>

        <DataTable class="hidden lg:block" :columns="columns" :rows="stocks?.items || []" row-key="ts_code" empty-text="暂无股票结果">
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
          <div>第 {{ queryFilters.page }} / {{ stocks?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="goPrevPage">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page >= (stocks?.total_pages || 1)" @click="goNextPage">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, watch } from 'vue'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchStockFilters, fetchStocks } from '../../services/api/stocks'
import { listStatusLabel } from '../../shared/utils/format'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

const route = useRoute()
const router = useRouter()

const filters = reactive({
  keyword: '',
  status: '',
  market: '',
  area: '',
  page_size: 20,
})
const queryFilters = reactive({
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
const { data: stocks, isFetching } = useQuery({
  queryKey: computed(() => ['stocks', { ...queryFilters }]),
  queryFn: () => fetchStocks({ ...queryFilters }),
  placeholderData: keepPreviousData,
})

function applyFilters() {
  queryFilters.keyword = (filters.keyword || '').trim()
  queryFilters.status = filters.status
  queryFilters.market = filters.market
  queryFilters.area = filters.area
  queryFilters.page_size = Number(filters.page_size) || 20
  queryFilters.page = 1
  syncRouteFromQuery()
}

function goPrevPage() {
  if (queryFilters.page <= 1) return
  queryFilters.page -= 1
  syncRouteFromQuery()
}

function goNextPage() {
  const totalPages = Number(stocks.value?.total_pages || 1)
  if (queryFilters.page >= totalPages) return
  queryFilters.page += 1
  syncRouteFromQuery()
}

function syncRouteFromQuery() {
  router.replace({
    query: buildCleanQuery({
      keyword: queryFilters.keyword,
      status: queryFilters.status,
      market: queryFilters.market,
      area: queryFilters.area,
      page: queryFilters.page,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteQuery() {
  const q = route.query as Record<string, unknown>
  const next = {
    keyword: readQueryString(q, 'keyword', ''),
    status: readQueryString(q, 'status', ''),
    market: readQueryString(q, 'market', ''),
    area: readQueryString(q, 'area', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(20, readQueryNumber(q, 'page_size', 20)),
  }
  Object.assign(filters, {
    keyword: next.keyword,
    status: next.status,
    market: next.market,
    area: next.area,
    page_size: next.page_size,
  })
  Object.assign(queryFilters, next)
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
