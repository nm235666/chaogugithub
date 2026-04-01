<template>
  <div class="space-y-4">
    <PageSection v-if="!hideFilterPanel" :title="pageTitle" :subtitle="pageSubtitle">
      <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
        <select v-if="showSource" v-model="draftFilters.source" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
          <option value="">全部来源</option>
          <option v-for="item in sourceOptions" :key="item" :value="item">{{ sourceLabel(item) }}</option>
        </select>
        <input v-model="draftFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="关键词" />
        <input v-model="draftFilters.date_from" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="开始日期，如 2026-03-20" />
        <input v-model="draftFilters.date_to" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="结束日期，如 2026-03-28" />
        <select v-model.number="draftFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
          <option :value="20">20 / 页</option>
          <option :value="50">50 / 页</option>
        </select>
        <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isFetching" @click="applyFilters">
          {{ isFetching ? '查询中...' : '查询' }}
        </button>
      </div>
      <div class="mt-4 flex flex-wrap gap-2">
        <button
          v-for="level in importanceLevels"
          :key="level"
          class="rounded-full border px-3 py-2 text-sm font-semibold transition"
          :class="selectedFinanceLevels.includes(level) ? 'border-[var(--brand)] bg-[rgba(15,97,122,0.08)] text-[var(--brand)]' : 'border-[var(--line)] bg-white text-[var(--muted)]'"
          @click="toggleLevel(level)"
        >
          {{ level }}
        </button>
      </div>
    </PageSection>

    <PageSection :title="`新闻结果 (${result?.total || 0})`" subtitle="新闻评分、情绪、影响标签和信号联动统一展示。">
      <div class="space-y-3">
        <InfoCard
          v-for="item in result?.items || []"
          :key="item.id"
          :title="item.title || '-'"
          :meta="`${sourceLabel(item.source)} · ${formatDateTime(item.pub_date)} · 情绪 ${item.llm_sentiment_label || '-'} · 实际模型 ${item.llm_model || '-'}`"
          :description="item.summary || ''"
        >
          <template #badge>
            <StatusBadge :value="item.llm_finance_importance || 'muted'" :label="item.llm_finance_importance || '未评级'" />
          </template>

          <div class="mt-3 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">系统评分 <strong>{{ item.llm_system_score ?? '-' }}</strong></span>
            <span class="metric-chip">财经影响 <strong>{{ item.llm_finance_impact_score ?? '-' }}</strong></span>
            <span class="metric-chip">情绪分 <strong>{{ item.llm_sentiment_score ?? '-' }}</strong></span>
            <span class="metric-chip">情绪模型 <strong>{{ item.llm_sentiment_model || '-' }}</strong></span>
          </div>

          <div v-if="impactTags(item).length" class="mt-3">
            <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">影响标签</div>
            <div class="flex flex-wrap gap-2 text-xs">
              <span v-for="tag in impactTags(item)" :key="`${tag.group}-${tag.label}-${tag.direction}`" class="metric-chip">
                {{ tag.group }} · {{ tag.label }} <strong>{{ tag.direction }}</strong>
              </span>
            </div>
          </div>

          <div v-if="relatedStocks(item).length" class="mt-3">
            <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">关联股票 / 信号联动</div>
            <div class="flex flex-wrap gap-2">
              <button
                v-for="stock in relatedStocks(item)"
                :key="`${item.id}-${stock.ts_code || stock.name}`"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                @click="goStock(stock)"
              >
                {{ stock.name || stock.ts_code }}
              </button>
              <button
                v-for="stock in relatedStocks(item)"
                :key="`${item.id}-${stock.ts_code || stock.name}-signal`"
                class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                @click="goSignal(stock)"
              >
                信号时间线 · {{ stock.ts_code || stock.name }}
              </button>
            </div>
          </div>

          <div class="mt-3 grid gap-2 xl:grid-cols-2">
            <div class="rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-3 text-sm text-[var(--muted)]">
              <div class="text-xs font-semibold uppercase tracking-[0.14em]">情绪链路</div>
              <div class="mt-2 leading-7">{{ item.llm_sentiment_reason || '暂无情绪链路说明。' }}</div>
            </div>
            <div class="rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-3 text-sm text-[var(--muted)]">
              <div class="text-xs font-semibold uppercase tracking-[0.14em]">风险 / 板块观察</div>
              <div class="mt-2 leading-7">{{ riskText(item) }}</div>
            </div>
          </div>
        </InfoCard>
      </div>
      <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ localFilters.page }} / {{ result?.total_pages || 1 }} 页</div>
        <div class="flex gap-2">
          <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="localFilters.page <= 1" @click="changePage(-1)">上一页</button>
          <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="localFilters.page >= (result?.total_pages || 1)" @click="changePage(1)">下一页</button>
        </div>
      </div>
    </PageSection>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchNewsSources } from '../../services/api/news'
import { formatDateTime } from '../../shared/utils/format'
import { importanceOptions, parseImpactTags, parseRelatedStocks, sourceLabel } from '../../shared/utils/finance'

const props = defineProps<{
  pageTitle: string
  pageSubtitle: string
  queryKey: readonly unknown[]
  queryFn: () => Promise<any>
  filters: Record<string, any>
  showSource?: boolean
  loadSources?: boolean
  hideFilterPanel?: boolean
}>()

const router = useRouter()
const localFilters = props.filters as Record<string, any>
const draftFilters = reactive({ ...localFilters })
const selectedFinanceLevels = ref(
  String(localFilters.finance_levels || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean),
)

watch(
  selectedFinanceLevels,
  (levels) => {
    draftFilters.finance_levels = levels.join(',')
  },
  { deep: true, immediate: true },
)

watch(
  () => ({ ...localFilters }),
  (next) => {
    Object.assign(draftFilters, next || {})
    selectedFinanceLevels.value = String((next || {}).finance_levels || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  },
  { deep: true },
)

const { data: sourceData } = useQuery({
  queryKey: ['news-sources'],
  queryFn: fetchNewsSources,
  enabled: !!props.loadSources,
})

const { data: result, isFetching, refetch } = useQuery({ queryKey: props.queryKey, queryFn: props.queryFn })
const sourceOptions = computed(() => sourceData.value?.items || [])
const importanceLevels = importanceOptions()
const hideFilterPanel = computed(() => Boolean(props.hideFilterPanel))

function toggleLevel(level: string) {
  if (selectedFinanceLevels.value.includes(level)) {
    selectedFinanceLevels.value = selectedFinanceLevels.value.filter((item) => item !== level)
  } else {
    selectedFinanceLevels.value = [...selectedFinanceLevels.value, level]
  }
  draftFilters.finance_levels = selectedFinanceLevels.value.join(',')
  applyFilters()
}

function applyFilters() {
  Object.assign(localFilters, { ...draftFilters, page: 1 })
  refetch()
}

function changePage(delta: number) {
  localFilters.page = Math.max(1, Number(localFilters.page || 1) + delta)
  refetch()
}

function impactTags(item: Record<string, any>) {
  return parseImpactTags(item.llm_impacts_json).slice(0, 8)
}

function relatedStocks(item: Record<string, any>) {
  return parseRelatedStocks(item.related_stock_names_json || item.llm_direct_related_stock_names_json, item.related_ts_codes_json || item.llm_direct_related_ts_codes_json).slice(0, 4)
}

function riskText(item: Record<string, any>) {
  const tags = impactTags(item)
  if (!tags.length) return '暂无结构化风险/板块标签。'
  return tags
    .slice(0, 4)
    .map((tag) => `${tag.group}:${tag.label}(${tag.direction})`)
    .join('；')
}

function goStock(stock: { ts_code: string; name: string }) {
  if (stock.ts_code) {
    router.push({ path: `/stocks/detail/${encodeURIComponent(stock.ts_code)}` })
    return
  }
  router.push({ path: '/stocks/list', query: { keyword: stock.name } })
}

function goSignal(stock: { ts_code: string; name: string }) {
  if (stock.ts_code) {
    router.push({ path: '/signals/timeline', query: { signal_key: `stock:${stock.ts_code}` } })
    return
  }
  router.push({ path: '/signals/overview', query: { keyword: stock.name, entity_type: '股票' } })
}
</script>
