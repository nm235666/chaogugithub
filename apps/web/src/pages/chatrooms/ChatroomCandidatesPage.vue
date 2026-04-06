<template>
  <AppShell title="股票候选池" subtitle="按群聊观点聚合出来的股票 / 主题候选池统一查看。">
    <div class="space-y-4">
      <PageSection title="候选池结果" subtitle="这是群聊信息转投资线索的统一入口。">
        <fieldset class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <legend class="sr-only">候选池筛选条件</legend>
          <label class="text-sm font-semibold text-[var(--ink)]">
            关键词
            <input v-model="filters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="关键词" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            类型
            <select v-model="filters.candidate_type" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部类型</option>
              <option value="股票">股票</option>
              <option value="主题">主题</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            方向
            <select v-model="filters.dominant_bias" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部方向</option>
              <option value="看多">看多</option>
              <option value="看空">看空</option>
            </select>
          </label>
          <div class="flex items-end">
            <button class="w-full rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="reload">刷新</button>
          </div>
        </fieldset>
        <div class="mt-4 grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <InfoCard title="全部候选" :meta="`当前命中 ${result?.total ?? 0}`" :description="`累计候选 ${summary.candidate_total ?? 0}`" />
          <InfoCard title="看多 / 看空" :meta="`看多 ${summary.bullish_total ?? 0}`" :description="`看空 ${summary.bearish_total ?? 0}`" />
          <InfoCard title="股票候选" :meta="`股票 ${summary.stock_total ?? 0}`" :description="`单页展示 ${filters.page_size} 条`" />
          <InfoCard title="当前筛选" :meta="filterMeta" :description="filterHint" />
        </div>
        <StatePanel
          v-if="queryError"
          class="mt-4"
          tone="danger"
          title="候选池加载失败"
          :description="queryError"
        >
          <template #action>
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="reload">重新加载</button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 font-semibold text-[var(--ink)]" @click="resetFilters">清空筛选</button>
          </template>
        </StatePanel>
        <StatePanel
          v-else-if="!isFetching && !(result?.items || []).length"
          class="mt-4"
          tone="warning"
          title="当前没有命中的候选"
          description="可以先清空关键词或方向筛选，回到全量候选池看最近 7 天的聚合结果。"
        >
          <template #action>
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 font-semibold text-white" @click="resetFilters">恢复默认筛选</button>
          </template>
        </StatePanel>
        <div class="mt-4 space-y-2">
          <InfoCard
            v-for="item in result?.items || []"
            :key="item.candidate_name + String(item.latest_analysis_date)"
            :title="item.candidate_name || '-'" :meta="`${item.candidate_type || '-'} · 净分 ${item.net_score ?? 0} · 提及 ${item.mention_count ?? 0}`"
            class="cursor-pointer"
            @click="openCandidate(item)"
          >
            <template #badge>
              <StatusBadge :value="item.dominant_bias" :label="item.dominant_bias || '-'" />
            </template>
            <div class="flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">群数 {{ item.room_count ?? 0 }}</span>
              <span class="metric-chip">最新分析 {{ item.latest_analysis_date || '-' }}</span>
              <span v-if="item.ts_code" class="metric-chip">代码 {{ item.ts_code }}</span>
              <span class="metric-chip text-[var(--brand)]">点击查看下一步</span>
            </div>
          </InfoCard>
        </div>
        <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ queryFilters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="prevPage">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page >= (result?.total_pages || 1)" @click="nextPage">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import { fetchCandidatePool } from '../../services/api/chatrooms'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

const router = useRouter()
const route = useRoute()
const filters = reactive({ keyword: '', candidate_type: '', dominant_bias: '', page: 1, page_size: 30 })
const queryFilters = reactive({ keyword: '', candidate_type: '', dominant_bias: '', page: 1, page_size: 30 })
const { data: result, isFetching, error } = useQuery({
  queryKey: computed(() => ['candidate-pool', { ...queryFilters }]),
  queryFn: () => fetchCandidatePool({ ...queryFilters }),
  placeholderData: keepPreviousData,
})
const summary = computed(() => result.value?.summary || {})
const queryError = computed(() => error.value?.message || '')
const filterMeta = computed(() => {
  const parts = [queryFilters.keyword ? `关键词 ${queryFilters.keyword}` : '', queryFilters.candidate_type || '', queryFilters.dominant_bias || '']
    .filter(Boolean)
  return parts.join(' · ') || '未启用筛选'
})
const filterHint = computed(() => (queryFilters.keyword || queryFilters.candidate_type || queryFilters.dominant_bias ? '当前视图已经收窄。' : '当前是全量候选池视图。'))

function reload() {
  Object.assign(queryFilters, {
    keyword: filters.keyword,
    candidate_type: filters.candidate_type,
    dominant_bias: filters.dominant_bias,
    page: 1,
    page_size: Number(filters.page_size) || 30,
  })
  syncRouteFromFilters()
}

function resetFilters() {
  Object.assign(filters, { keyword: '', candidate_type: '', dominant_bias: '', page: 1, page_size: 30 })
  Object.assign(queryFilters, { ...filters })
  syncRouteFromFilters()
}

function openCandidate(item: Record<string, any>) {
  const tsCode = String(item.ts_code || '').trim()
  const candidateName = String(item.candidate_name || '').trim()
  if (tsCode) {
    router.push({ path: `/stocks/detail/${encodeURIComponent(tsCode)}` })
    return
  }
  if (String(item.candidate_type || '').trim() === '主题') {
    router.push({ path: '/signals/themes', query: { keyword: candidateName } })
    return
  }
  router.push({ path: '/chatrooms/investment', query: { target_keyword: candidateName } })
}

function prevPage() {
  if (queryFilters.page <= 1) return
  queryFilters.page -= 1
  syncRouteFromFilters()
}

function nextPage() {
  const totalPages = Number(result.value?.total_pages || 1)
  if (queryFilters.page >= totalPages) return
  queryFilters.page += 1
  syncRouteFromFilters()
}

function syncRouteFromFilters() {
  router.replace({
    query: buildCleanQuery({
      keyword: queryFilters.keyword,
      candidate_type: queryFilters.candidate_type,
      dominant_bias: queryFilters.dominant_bias,
      page: queryFilters.page,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteFilters() {
  const q = route.query as Record<string, unknown>
  const next = {
    keyword: readQueryString(q, 'keyword', ''),
    candidate_type: readQueryString(q, 'candidate_type', ''),
    dominant_bias: readQueryString(q, 'dominant_bias', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(30, readQueryNumber(q, 'page_size', 30)),
  }
  Object.assign(filters, next)
  Object.assign(queryFilters, next)
}

watch(
  () => route.query,
  () => {
    applyRouteFilters()
  },
  { immediate: true },
)
</script>
