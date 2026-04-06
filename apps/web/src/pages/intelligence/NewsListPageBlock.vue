<template>
  <div class="space-y-4">
    <PageSection v-if="!hideFilterPanel" :title="pageTitle" :subtitle="pageSubtitle">
      <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
        <label v-if="showSource" class="text-sm font-semibold text-[var(--ink)]">
          来源
          <select v-model="draftFilters.source" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部来源</option>
            <option v-for="item in sourceOptions" :key="item" :value="item">{{ sourceLabel(item) }}</option>
          </select>
        </label>
        <label class="text-sm font-semibold text-[var(--ink)]">
          关键词
          <input v-model="draftFilters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="关键词" />
        </label>
        <label class="text-sm font-semibold text-[var(--ink)]">
          开始日期
          <input v-model="draftFilters.date_from" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 2026-03-20" />
        </label>
        <label class="text-sm font-semibold text-[var(--ink)]">
          结束日期
          <input v-model="draftFilters.date_to" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 2026-03-28" />
        </label>
        <label class="text-sm font-semibold text-[var(--ink)]">
          每页条数
          <select v-model.number="draftFilters.page_size" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
          </select>
        </label>
        <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isFetching" @click="applyFilters">
          {{ isFetching ? '查询中...' : '查询' }}
        </button>
        <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="semanticSearching" @click="runSemanticSearch">
          {{ semanticSearching ? '语义检索中...' : '语义检索' }}
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
      <div v-if="semanticError" class="mb-3 rounded-[16px] border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
        {{ semanticError }}
      </div>
      <div v-if="semanticHits.length" class="mb-3 rounded-[16px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-3">
        <div class="mb-2 text-sm font-semibold text-[var(--ink)]">语义检索结果（{{ semanticHits.length }}）</div>
        <div class="space-y-2">
          <button
            v-for="(hit, idx) in semanticHits"
            :key="`${hit.source_type}-${hit.source_id}-${idx}`"
            class="w-full rounded-2xl border border-[var(--line)] bg-white px-3 py-3 text-left transition hover:border-[var(--brand)]"
            @click="openSemanticHit(hit)"
          >
            <div class="text-sm font-semibold text-[var(--ink)]">{{ hit.title || '-' }}</div>
            <div class="mt-1 text-xs text-[var(--muted)]">相关性 {{ Number(hit.rerank_score || 0).toFixed(3) }} · {{ hit.published_at || '-' }}</div>
            <div class="mt-2 text-sm text-[var(--muted)]">{{ hit.snippet || '' }}</div>
          </button>
        </div>
      </div>
      <div class="space-y-3">
        <InfoCard
          v-for="item in result?.items || []"
          :key="item.id"
          :title="compactHeadline(item)"
          :meta="''"
          :description="''"
          :class="cardToneClass(item)"
        >
          <template #title>
            <div class="flex flex-wrap items-center gap-2">
              <span class="rounded-full border px-2.5 py-0.5 text-[13px] font-semibold leading-5" :class="evaluationBadgeClass(item)">
                {{ evaluationLabel(item) }}
              </span>
              <span>{{ String(item.title || '-') }}</span>
            </div>
          </template>
          <div class="mt-2 flex flex-wrap items-center gap-2 text-xs">
            <span class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-2 py-1 font-semibold text-[var(--muted)]">发布时间</span>
            <span class="rounded-full border border-[var(--line)] bg-white px-2 py-1 font-semibold text-[var(--ink)]">{{ formatChinaDateTime(item.pub_date) }}</span>
          </div>
          <div class="mt-2">
            <button
              class="text-sm font-semibold text-[var(--brand)]"
              @click="toggleExpand(item)"
            >
              {{ isExpanded(item) ? '[收起详情]' : '[展开详情]' }}
            </button>
          </div>

          <div v-if="isExpanded(item)" class="mt-3 space-y-3">
            <div v-if="item.summary" class="text-sm text-[var(--muted)]">{{ item.summary }}</div>

            <div class="flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">来源 <strong>{{ sourceLabel(item.source) }}</strong></span>
              <span class="metric-chip">系统评分 <strong>{{ scoreLabel(item.llm_system_score, item) }}</strong></span>
              <span class="metric-chip">财经影响 <strong>{{ scoreLabel(item.llm_finance_impact_score, item) }}</strong></span>
              <span class="metric-chip">情绪分 <strong>{{ item.llm_sentiment_score ?? '-' }}</strong></span>
              <span class="metric-chip">情绪模型 <strong>{{ item.llm_sentiment_model || '-' }}</strong></span>
              <span class="metric-chip">实际模型 <strong>{{ item.llm_model || '-' }}</strong></span>
            </div>
            <div v-if="zeroScoreHint(item)" class="text-xs text-[var(--muted)]">
              {{ zeroScoreHint(item) }}
            </div>

            <div v-if="impactTags(item).length">
              <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">影响标签</div>
              <div class="flex flex-wrap gap-2 text-xs">
                <span v-for="tag in impactTags(item)" :key="`${tag.group}-${tag.label}-${tag.direction}`" class="metric-chip">
                  {{ tag.group }} · {{ tag.label }} <strong>{{ tag.direction }}</strong>
                </span>
              </div>
            </div>

            <div v-if="relatedStocks(item).length">
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

            <details class="rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-3">
              <summary class="cursor-pointer text-sm font-semibold text-[var(--ink)]">展开更多说明</summary>
              <div class="mt-3 grid gap-2 xl:grid-cols-2">
                <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-3 py-3 text-sm text-[var(--muted)]">
                  <div class="text-xs font-semibold uppercase tracking-[0.14em]">情绪链路</div>
                  <div class="mt-2 leading-7">{{ item.llm_sentiment_reason || '暂无情绪链路说明。' }}</div>
                </div>
                <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-3 py-3 text-sm text-[var(--muted)]">
                  <div class="text-xs font-semibold uppercase tracking-[0.14em]">风险 / 板块观察</div>
                  <div class="mt-2 leading-7">{{ riskText(item) }}</div>
                </div>
              </div>
            </details>
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
import { fetchNewsSources, searchNewsSemantic } from '../../services/api/news'
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
const expandedMap = ref<Record<string, boolean>>({})
const semanticSearching = ref(false)
const semanticHits = ref<Array<Record<string, any>>>([])
const semanticError = ref('')
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

async function runSemanticSearch() {
  const query = String(draftFilters.keyword || '').trim()
  if (!query) {
    semanticError.value = '请先输入关键词，再执行语义检索。'
    semanticHits.value = []
    return
  }
  semanticSearching.value = true
  semanticError.value = ''
  try {
    const payload = await searchNewsSemantic({ query, scene: 'news', top_k: 8 })
    semanticHits.value = Array.isArray(payload?.hits) ? payload.hits : []
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || '未知错误')
    semanticError.value = `语义检索失败：${message}`
    semanticHits.value = []
  } finally {
    semanticSearching.value = false
  }
}

function changePage(delta: number) {
  localFilters.page = Math.max(1, Number(localFilters.page || 1) + delta)
  refetch()
}

function impactTags(item: Record<string, any>) {
  return parseImpactTags(item.llm_impacts_json).slice(0, 8)
}

function itemKey(item: Record<string, any>) {
  return String(item.id || item.news_id || item.title || '')
}

function isExpanded(item: Record<string, any>) {
  return Boolean(expandedMap.value[itemKey(item)])
}

function toggleExpand(item: Record<string, any>) {
  const key = itemKey(item)
  if (!key) return
  expandedMap.value = {
    ...expandedMap.value,
    [key]: !expandedMap.value[key],
  }
}

function evaluationLabel(item: Record<string, any>) {
  return String(item.llm_finance_importance || '未评级')
}

function normalizeImportance(value: string) {
  return String(value || '').trim().toLowerCase()
}

function cardToneClass(item: Record<string, any>) {
  const level = normalizeImportance(evaluationLabel(item))
  if (level === '极高') {
    return 'border-red-300 bg-[linear-gradient(180deg,rgba(254,242,242,0.92)_0%,rgba(254,226,226,0.78)_100%)]'
  }
  return ''
}

function compactHeadline(item: Record<string, any>) {
  return String(item.title || '-')
}

function evaluationBadgeClass(item: Record<string, any>) {
  const level = normalizeImportance(evaluationLabel(item))
  if (level === '极高') return 'border-red-300 bg-red-50 text-red-700'
  if (level === '高') return 'border-orange-400 bg-orange-100 text-orange-800 ring-1 ring-orange-300/70'
  if (level === '中' || level === '一般') return 'border-blue-300 bg-blue-50 text-blue-700'
  if (level === '低' || level === '较低') return 'border-emerald-300 bg-emerald-50 text-emerald-700'
  return 'border-slate-300 bg-slate-50 text-slate-700'
}

function formatChinaDateTime(value: unknown) {
  const text = String(value ?? '').trim()
  if (!text) return '-'
  const parsed = new Date(text)
  if (Number.isNaN(parsed.getTime())) {
    return text.replace('T', ' ').replace('Z', '')
  }
  return new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(parsed).replace(/\//g, '-')
}

function hasAnalysis(item: Record<string, any>) {
  return Boolean(String(item.llm_model || '').trim() || String(item.llm_summary || '').trim() || String(item.llm_scored_at || '').trim())
}

function scoreLabel(value: unknown, item: Record<string, any>) {
  if (value == null || value === '') return '-'
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return String(value)
  if (numberValue === 0 && hasAnalysis(item)) return '0（已分析）'
  return String(numberValue)
}

function zeroScoreHint(item: Record<string, any>) {
  if (!hasAnalysis(item)) return ''
  const fields = [
    Number(item.llm_system_score ?? NaN) === 0 ? '系统评分' : '',
    Number(item.llm_finance_impact_score ?? NaN) === 0 ? '财经影响' : '',
  ].filter(Boolean)
  if (!fields.length) return ''
  return `${fields.join(' / ')}已完成分析，当前评分为 0，不代表未评分或接口异常。`
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

function openSemanticHit(hit: Record<string, any>) {
  const sourceId = String(hit.source_id || '').trim()
  if (!sourceId) return
  const matched = (result.value?.items || []).find((item: Record<string, any>) => String(item.id || '') === sourceId)
  if (matched) {
    toggleExpand(matched)
    return
  }
  draftFilters.keyword = String(hit.title || draftFilters.keyword || '')
  applyFilters()
}
</script>
