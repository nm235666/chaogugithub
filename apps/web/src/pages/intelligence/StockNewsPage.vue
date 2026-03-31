<template>
  <AppShell title="个股新闻" subtitle="个股新闻查询、立即采集、补评分、影响解析与运维动作统一工作台。">
    <div class="space-y-4">
      <PageSection title="查询与采集" subtitle="按股票、来源、日期、重要度和评分状态筛选，也可以直接触发采集或补评分。">
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-2">
          <input v-model="filters.ts_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="ts_code，如 000001.SZ" />
          <input v-model="filters.company_name" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="公司名" />
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="新闻关键词" />
          <select v-model="filters.source" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部来源</option>
            <option v-for="item in sourceOptions" :key="item" :value="item">{{ sourceLabel(item) }}</option>
          </select>
          <input v-model="filters.date_from" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="开始日期 YYYY-MM-DD" />
          <input v-model="filters.date_to" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="结束日期 YYYY-MM-DD" />
          <select v-model="filters.scored" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部评分状态</option>
            <option value="unscored">只看未评分</option>
            <option value="scored">只看已评分</option>
          </select>
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
          </select>
          <div class="xl:col-span-2 flex gap-2">
            <button class="flex-1 rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">查询</button>
            <button class="flex-1 rounded-2xl bg-blue-700 px-4 py-3 font-semibold text-white" :disabled="isFetchPending" @click="runFetch">
              {{ isFetchPending ? '采集中...' : '立即采集' }}
            </button>
          </div>
          <button class="rounded-2xl bg-stone-800 px-4 py-3 font-semibold text-white disabled:opacity-50" :disabled="isScorePending || !currentScoreTarget" @click="runScoreCurrent">
            {{ isScorePending ? '补评分中...' : '立即补评分' }}
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
        <div v-if="actionMessage" class="mt-3 text-sm text-[var(--muted)]">{{ actionMessage }}</div>
      </PageSection>

      <PageSection :title="`个股新闻 (${result?.total || 0})`" subtitle="评分、情绪、影响项和逐条重评分动作都放在这里。">
        <div class="space-y-3">
          <InfoCard
            v-for="item in result?.items || []"
            :key="item.id"
            :title="item.title || '-'"
            :meta="`${item.ts_code || '-'} · ${sourceLabel(item.source)} · ${formatDateTime(item.pub_time)} · 实际模型 ${item.llm_model || '-'}`"
            :description="item.llm_summary || item.summary || ''"
          >
            <template #badge>
              <StatusBadge :value="item.llm_finance_importance || 'muted'" :label="item.llm_finance_importance || '未评级'" />
            </template>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">系统评分 <strong>{{ item.llm_system_score ?? '-' }}</strong></span>
              <span class="metric-chip">财经影响 <strong>{{ item.llm_finance_impact_score ?? '-' }}</strong></span>
              <span class="metric-chip">情绪分 <strong>{{ item.llm_sentiment_score ?? '-' }}</strong></span>
              <span class="metric-chip">情绪标签 <strong>{{ item.llm_sentiment_label || '-' }}</strong></span>
            </div>
            <div v-if="impactTags(item).length" class="mt-3 flex flex-wrap gap-2 text-xs">
              <span v-for="tag in impactTags(item)" :key="`${item.id}-${tag.group}-${tag.label}-${tag.direction}`" class="metric-chip">
                {{ tag.group }} · {{ tag.label }} <strong>{{ tag.direction }}</strong>
              </span>
            </div>
            <div class="mt-3 grid gap-2 xl:grid-cols-2">
              <div class="rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-3 text-sm text-[var(--muted)]">
                <div class="text-xs font-semibold uppercase tracking-[0.14em]">情绪链路</div>
                <div class="mt-2 leading-7">{{ item.llm_sentiment_reason || '暂无情绪链路。' }}</div>
              </div>
              <div class="rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-3 text-sm text-[var(--muted)]">
                <div class="text-xs font-semibold uppercase tracking-[0.14em]">操作</div>
                <div class="mt-2 flex flex-wrap gap-2">
                  <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]" :disabled="rowScoringId === item.id" @click="rescoreRow(item)">
                    {{ rowScoringId === item.id ? '重评分中...' : '单条重评分' }}
                  </button>
                  <button class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]" @click="goDetail(item.ts_code)">
                    打开股票详情
                  </button>
                  <button class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]" @click="goSignal(item.ts_code, item.company_name)">
                    查看信号
                  </button>
                </div>
              </div>
            </div>
          </InfoCard>
        </div>
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
import { computed, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchStockNews, fetchStockNewsSources, triggerStockNewsScore } from '../../services/api/news'
import { triggerStockNewsFetch } from '../../services/api/stocks'
import { formatDateTime } from '../../shared/utils/format'
import { importanceOptions, parseImpactTags, sourceLabel } from '../../shared/utils/finance'

const router = useRouter()
const queryClient = useQueryClient()

const filters = reactive({
  ts_code: '',
  company_name: '',
  keyword: '',
  source: '',
  finance_levels: '极高,高,中',
  date_from: '',
  date_to: '',
  scored: '',
  page: 1,
  page_size: 20,
})

const selectedFinanceLevels = ref(
  String(filters.finance_levels || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean),
)
const actionMessage = ref('')
const rowScoringId = ref(0)
const importanceLevels = importanceOptions()
const isFetchPending = computed(() => fetchMutation.isPending.value)
const isScorePending = computed(() => scoreMutation.isPending.value)
const currentScoreTarget = computed(() => filters.ts_code.trim().toUpperCase() || String(result.value?.items?.[0]?.ts_code || '').trim().toUpperCase())

watch(
  selectedFinanceLevels,
  (levels) => {
    filters.finance_levels = levels.join(',')
    filters.page = 1
  },
  { deep: true, immediate: true },
)

const { data: sourceData } = useQuery({ queryKey: ['stock-news-sources'], queryFn: fetchStockNewsSources })
const sourceOptions = computed(() => sourceData.value?.items || [])

const { data: result, refetch } = useQuery({ queryKey: ['stock-news', filters], queryFn: () => fetchStockNews(filters) })

const fetchMutation = useMutation({
  mutationFn: () => triggerStockNewsFetch({ ...filters, score: 1 }),
  onSuccess: async (payload) => {
    actionMessage.value = `采集完成${payload.used_model ? ` · 实际模型 ${payload.used_model}` : ''}`
    await queryClient.invalidateQueries({ queryKey: ['stock-news'] })
  },
  onError: (error: Error) => {
    actionMessage.value = `采集失败：${error.message}`
  },
})

const scoreMutation = useMutation({
  mutationFn: () => triggerStockNewsScore({ ts_code: currentScoreTarget.value, limit: Math.max(filters.page_size, 20), force: filters.scored === 'scored' ? 1 : 0 }),
  onSuccess: async (payload) => {
    actionMessage.value = `补评分完成${payload.used_model ? ` · 实际模型 ${payload.used_model}` : ''}`
    await refetch()
  },
  onError: (error: Error) => {
    actionMessage.value = `补评分失败：${error.message}`
  },
})

function toggleLevel(level: string) {
  if (selectedFinanceLevels.value.includes(level)) {
    selectedFinanceLevels.value = selectedFinanceLevels.value.filter((item) => item !== level)
  } else {
    selectedFinanceLevels.value = [...selectedFinanceLevels.value, level]
  }
}

function impactTags(item: Record<string, any>) {
  return parseImpactTags(item.llm_impacts_json).slice(0, 8)
}

function runFetch() {
  fetchMutation.mutate()
}

function runScoreCurrent() {
  scoreMutation.mutate()
}

async function rescoreRow(item: Record<string, any>) {
  rowScoringId.value = Number(item.id || 0)
  actionMessage.value = `正在重评新闻 ${item.id} ...`
  try {
    const payload = await triggerStockNewsScore({ row_id: item.id, limit: 1, force: 1 })
    actionMessage.value = `单条重评分完成${payload.used_model ? ` · 实际模型 ${payload.used_model}` : ''}`
    await refetch()
  } catch (error) {
    actionMessage.value = `单条重评分失败：${(error as Error).message}`
  } finally {
    rowScoringId.value = 0
  }
}

function goDetail(tsCode: string) {
  if (!tsCode) return
  router.push({ path: `/stocks/detail/${encodeURIComponent(tsCode)}` })
}

function goSignal(tsCode: string, companyName: string) {
  if (tsCode) {
    router.push({ path: '/signals/timeline', query: { signal_key: `stock:${tsCode}` } })
    return
  }
  router.push({ path: '/signals/overview', query: { keyword: companyName, entity_type: '股票' } })
}
</script>
