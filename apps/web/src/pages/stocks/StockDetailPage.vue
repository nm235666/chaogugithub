<template>
  <AppShell :title="pageTitle" :subtitle="pageSubtitle">
    <div class="space-y-4">
      <PageSection title="研究定位" subtitle="支持输入 ts_code 或股票简称，把详情、图表、新闻、群聊与 LLM 动作压进一个入口。">
        <div class="grid gap-4 2xl:grid-cols-[1.15fr_0.85fr]">
          <div class="space-y-4">
            <div class="grid gap-3 xl:grid-cols-[1fr_140px_140px]">
              <label class="text-sm font-semibold text-[var(--ink)] xl:col-span-1">
                股票代码 / 简称
                <input
                  v-model="tsCodeInput"
                  class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
                  placeholder="如 000001.SZ / 平安银行"
                  @keydown.enter="applyCode"
                />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                回看区间
                <select v-model.number="lookback" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                  <option :value="60">近 60 日</option>
                  <option :value="120">近 120 日</option>
                  <option :value="240">近 240 日</option>
                </select>
              </label>
              <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyCode">
                {{ isFetching ? '刷新中...' : '刷新详情' }}
              </button>
            </div>

            <div class="flex flex-wrap gap-2">
              <StatusBadge :value="profile.list_status" :label="listStatusLabel(String(profile.list_status || ''))" />
              <StatusBadge value="brand" :label="profile.market || '未知市场'" />
              <StatusBadge value="info" :label="profile.industry || '未知行业'" />
              <StatusBadge value="muted" :label="profile.area || '未知地区'" />
            </div>

            <MetricGrid :items="overviewItems" columns-class="xl:grid-cols-3 md:grid-cols-2" empty-text="暂无概览数据" />
          </div>

          <div class="grid gap-3">
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" :disabled="isFetchNewsPending" @click="fetchStockNewsNow">
              {{ isFetchNewsPending ? '采集中...' : '立即采集股票新闻' }}
            </button>
            <button class="rounded-2xl bg-stone-900 px-4 py-3 font-semibold text-white" :disabled="isTrendPending" @click="runTrend">
              {{ isTrendPending ? '分析中...' : '发起 LLM 走势分析' }}
            </button>
            <button class="rounded-2xl bg-blue-700 px-4 py-3 font-semibold text-white" :disabled="isMultiRolePending" @click="runMultiRole">
              {{ isMultiRolePending ? '任务创建中...' : '发起多角色分析' }}
            </button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)] disabled:opacity-40" :disabled="!activeMultiRoleJobId && !multiRoleResult" @click="clearMultiRoleTaskTracking">
              清除当前任务跟踪
            </button>
            <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)] shadow-[var(--shadow-soft)]" role="status" aria-live="polite">
              {{ actionMessage }}
            </div>
            <div v-if="restoredMultiRoleHint" class="text-xs text-[var(--muted)]">已从会话恢复任务跟踪（{{ restoredMultiRoleHint }}）</div>
            <div v-if="multiRolePersistenceNotice" class="text-xs text-[var(--muted)]">{{ multiRolePersistenceNotice }}</div>
            <div class="grid gap-3 md:grid-cols-3">
              <InfoCard
                v-for="item in actionStatusCards"
                :key="item.title"
                :title="item.title"
                :meta="item.meta"
                :description="item.description"
              >
                <template #badge>
                  <StatusBadge :value="item.tone" :label="item.label" />
                </template>
              </InfoCard>
            </div>

            <div class="grid gap-3 xl:grid-cols-2">
              <RouterLink to="/stocks/scores" class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] px-4 py-4 text-sm shadow-[var(--shadow-soft)]">
                <div class="font-semibold text-[var(--ink)]">综合评分页</div>
                <div class="mt-1 text-[var(--muted)]">查看行业内位置和分项评分。</div>
              </RouterLink>
              <RouterLink to="/intelligence/stock-news" class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] px-4 py-4 text-sm shadow-[var(--shadow-soft)]">
                <div class="font-semibold text-[var(--ink)]">个股新闻页</div>
                <div class="mt-1 text-[var(--muted)]">延展查看更完整的相关新闻和评分。</div>
              </RouterLink>
              <RouterLink to="/signals/overview" class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] px-4 py-4 text-sm shadow-[var(--shadow-soft)]">
                <div class="font-semibold text-[var(--ink)]">投资信号页</div>
                <div class="mt-1 text-[var(--muted)]">看这只股票是否已进入信号榜。</div>
              </RouterLink>
              <RouterLink to="/chatrooms/candidates" class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] px-4 py-4 text-sm shadow-[var(--shadow-soft)]">
                <div class="font-semibold text-[var(--ink)]">群聊候选池</div>
                <div class="mt-1 text-[var(--muted)]">继续看群聊交易偏向和讨论热度。</div>
              </RouterLink>
            </div>
          </div>
        </div>

        <div v-if="detailError" class="mt-4 rounded-[20px] border border-[rgba(178,77,84,0.18)] bg-[rgba(178,77,84,0.06)] px-4 py-3 text-sm text-[var(--danger)]">
          <div class="font-semibold">股票详情加载失败</div>
          <div class="mt-1">{{ detailError }}</div>
          <div class="mt-3 flex flex-wrap gap-2">
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="refetch()">重新加载</button>
            <RouterLink to="/stocks/prices" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 font-semibold text-[var(--ink)]">去价格中心</RouterLink>
            <RouterLink to="/intelligence/stock-news" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 font-semibold text-[var(--ink)]">去个股新闻页</RouterLink>
          </div>
        </div>
      </PageSection>

      <div class="flex flex-wrap gap-2">
        <a v-for="item in sectionAnchors" :key="item.href" :href="item.href" class="section-anchor-link">{{ item.label }}</a>
      </div>

      <div class="grid gap-4 2xl:grid-cols-[1.08fr_0.92fr]">
        <PageSection id="price-trend" title="近端价格走势" subtitle="最近一段收盘价的结构，用来快速判断趋势背景和区间位置。">
          <TrendAreaChart v-if="chartsReady" :labels="dailyChart.labels" :series="dailyChart.series" :height="320" empty-text="暂无日线数据" />
          <div v-else class="loading-skeleton h-[320px] rounded-[20px] border border-[var(--line)] px-4 py-4 text-sm text-[var(--muted)]">
            图表加载中...
          </div>
          <div class="mt-4 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">最新收盘 <strong>{{ formatNumber(priceSummary.latest_close, 3) }}</strong></span>
            <span class="metric-chip">最新涨跌幅 <strong>{{ formatSignedPercent(priceSummary.latest_pct_chg, 2) }}</strong></span>
            <span class="metric-chip">区间收益 <strong>{{ formatSignedPercent(priceSummary.range_return_pct, 2) }}</strong></span>
            <span class="metric-chip">区间高点 <strong>{{ formatNumber(priceSummary.high_lookback, 3) }}</strong></span>
            <span class="metric-chip">区间低点 <strong>{{ formatNumber(priceSummary.low_lookback, 3) }}</strong></span>
          </div>
          <div class="mt-3 flex flex-wrap gap-2 text-xs">
            <span v-for="item in rollupChips" :key="item.label" class="metric-chip">{{ item.label }} <strong>{{ item.value }}</strong></span>
          </div>
        </PageSection>

        <PageSection title="最新分钟走势" subtitle="最近一批分钟线，帮助判断盘中价格和均价是否同向。">
          <TrendAreaChart v-if="chartsReady" :labels="minuteChart.labels" :series="minuteChart.series" :height="320" empty-text="暂无分钟线数据" />
          <div v-else class="loading-skeleton h-[320px] rounded-[20px] border border-[var(--line)] px-4 py-4 text-sm text-[var(--muted)]">
            图表加载中...
          </div>
          <div class="mt-4 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">最新分钟价 <strong>{{ formatNumber(latestMinute.price, 3) }}</strong></span>
            <span class="metric-chip">最新均价 <strong>{{ formatNumber(latestMinute.avg_price, 3) }}</strong></span>
            <span class="metric-chip">分钟时间 <strong>{{ latestMinuteLabel }}</strong></span>
            <span class="metric-chip">最新成交量 <strong>{{ formatNumber(latestMinute.volume, 0) }}</strong></span>
            <span class="metric-chip">累计成交量 <strong>{{ formatNumber(latestMinute.total_volume, 0) }}</strong></span>
          </div>
        </PageSection>
      </div>

      <PageSection id="company-panels" title="公司深度面板" subtitle="按模块切换查看，避免评分、财务、治理、风险信息堆在一起。">
        <div class="flex flex-wrap gap-2">
          <button
            v-for="item in detailTabs"
            :key="item.key"
            class="rounded-full border px-3 py-2 text-sm font-semibold transition"
            :class="activeDetailTab === item.key ? 'border-[var(--brand)] bg-[rgba(15,97,122,0.08)] text-[var(--brand)]' : 'border-[var(--line)] bg-white text-[var(--muted)]'"
            @click="activeDetailTab = item.key"
          >
            {{ item.label }}
          </button>
        </div>

        <div class="mt-4">
          <MetricGrid v-if="activeDetailTab === 'score'" :items="scoreItems" empty-text="暂无评分数据" />
          <MetricGrid v-else-if="activeDetailTab === 'fundamental'" :items="fundamentalItems" empty-text="暂无财务或估值数据" />
          <MetricGrid v-else-if="activeDetailTab === 'flowRisk'" :items="flowRiskItems" empty-text="暂无资金流或风险数据" />

          <div v-else-if="activeDetailTab === 'events'" class="space-y-2">
            <InfoCard
              v-for="item in eventItems"
              :key="`${item.event_type}-${item.event_date}-${item.title}`"
              :title="item.title || item.event_type || '-'"
              :meta="joinParts([item.event_type, formatDate(item.event_date || item.ann_date)])"
              :description="summarizeDetail(item.detail)"
            />
          </div>

          <div v-else-if="activeDetailTab === 'governance'">
            <MetricGrid :items="governanceItems" columns-class="xl:grid-cols-2 md:grid-cols-2" empty-text="暂无治理数据" />
            <div v-if="topHolderChips.length || boardMemberChips.length" class="mt-4 space-y-3">
              <div v-if="topHolderChips.length">
                <div class="mb-2 text-sm font-semibold text-[var(--ink)]">前五大股东</div>
                <div class="flex flex-wrap gap-2 text-xs">
                  <span v-for="item in topHolderChips" :key="item" class="metric-chip">{{ item }}</span>
                </div>
              </div>
              <div v-if="boardMemberChips.length">
                <div class="mb-2 text-sm font-semibold text-[var(--ink)]">董事会成员</div>
                <div class="flex flex-wrap gap-2 text-xs">
                  <span v-for="item in boardMemberChips" :key="item" class="metric-chip">{{ item }}</span>
                </div>
              </div>
            </div>
          </div>

          <div v-else class="space-y-2">
            <InfoCard
              v-for="item in riskItems"
              :key="`${item.scenario_name}-${item.horizon}`"
              :title="item.scenario_name || '-'"
              :meta="joinParts([riskSummary.scenario_date || '', item.horizon || ''])"
              :description="summarizeAssumptions(item.assumptions)"
            >
              <div class="mt-3 flex flex-wrap gap-2 text-xs">
                <span class="metric-chip">PnL <strong>{{ formatNumber(item.pnl_impact, 2) }}</strong></span>
                <span class="metric-chip">回撤 <strong>{{ formatNumber(item.max_drawdown, 2) }}</strong></span>
                <span class="metric-chip">VaR95 <strong>{{ formatNumber(item.var_95, 2) }}</strong></span>
                <span class="metric-chip">CVaR95 <strong>{{ formatNumber(item.cvar_95, 2) }}</strong></span>
              </div>
            </InfoCard>
          </div>
        </div>
      </PageSection>

      <details id="news-impact" class="group">
        <summary class="cursor-pointer list-none rounded-[24px] border border-[var(--line)] bg-white/82 px-4 py-4 text-base font-bold text-[var(--ink)] shadow-[var(--shadow-soft)]">
          相关个股新闻与群聊摘要
          <span class="ml-2 text-sm font-normal text-[var(--muted)]">先看摘要，需要时再展开详细卡片。</span>
        </summary>
        <div class="mt-4 grid gap-4 xl:grid-cols-2">
          <PageSection title="相关个股新闻" subtitle="最近几条个股新闻、重要度和事件影响项。">
          <div class="space-y-2">
            <InfoCard
              v-for="item in stockNewsItems"
              :key="item.link || item.title"
              :title="item.title || '-'"
              :meta="joinParts([formatDateTime(item.pub_time), `影响分 ${formatNumber(item.finance_impact_score, 0)}`])"
              :description="item.llm_summary || item.summary || ''"
            >
              <template #badge>
                <StatusBadge :value="item.finance_importance || 'muted'" :label="item.finance_importance || '未评级'" />
              </template>
              <div v-if="item.impacts?.length" class="mt-3 flex flex-wrap gap-2 text-xs">
                <span v-for="impact in item.impacts.slice(0, 4)" :key="JSON.stringify(impact)" class="metric-chip">
                  {{ impact.target || impact.market || impact.sector || impact.name || '影响项' }}
                  <strong>{{ impact.direction || impact.sentiment || '中性' }}</strong>
                </span>
              </div>
            </InfoCard>
          </div>
          </PageSection>

          <PageSection title="群聊与候选池" subtitle="看是否已进入候选池，哪些群在讨论，最终偏向如何。">
          <div class="space-y-2">
            <InfoCard
              v-if="candidatePoolItem"
              title="候选池聚合结果"
              :meta="joinParts([`净分 ${candidatePoolItem.net_score || 0}`, `群数 ${candidatePoolItem.room_count || 0}`, candidatePoolItem.latest_analysis_date || ''])"
              :description="joinParts([`看多群 ${candidatePoolItem.bullish_room_count || 0}`, `看空群 ${candidatePoolItem.bearish_room_count || 0}`, `提及 ${candidatePoolItem.mention_count || 0} 次`])"
            >
              <template #badge>
                <StatusBadge :value="candidatePoolItem.dominant_bias" :label="candidatePoolItem.dominant_bias || '-'" />
              </template>
            </InfoCard>

            <InfoCard
              v-for="item in chatroomMentions"
              :key="`${item.room_id}-${item.update_time}`"
              :title="item.talker || item.room_id || '-'"
              :meta="joinParts([formatDate(item.analysis_date), `最新消息 ${formatDate(item.latest_message_date)}`])"
              :description="item.room_summary || ''"
            >
              <template #badge>
                <StatusBadge :value="item.final_bias" :label="item.final_bias || '-'" />
              </template>
            </InfoCard>
          </div>
          </PageSection>
        </div>
      </details>

      <details id="llm-analysis" class="group">
        <summary class="cursor-pointer list-none rounded-[24px] border border-[var(--line)] bg-white/82 px-4 py-4 text-base font-bold text-[var(--ink)] shadow-[var(--shadow-soft)]">
          LLM 分析结果
          <span class="ml-2 text-sm font-normal text-[var(--muted)]">默认折叠，降低详情页纵向滚动成本。</span>
        </summary>
        <div class="mt-4 grid gap-4 xl:grid-cols-2">
          <PageSection title="LLM 走势分析" subtitle="直接在本页看趋势判断，不用再切到单独分析页。">
          <MarkdownBlock :content="trendResult || '尚未发起走势分析。'" />
          </PageSection>
          <PageSection title="LLM 多角色分析" subtitle="继续保留异步轮询结果，让分析完成后自动落回这个页面。">
          <MarkdownBlock :content="multiRoleResult || '尚未发起多角色分析。'" />
          </PageSection>
        </div>
      </details>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import MetricGrid from '../../shared/ui/MetricGrid.vue'
import {
  fetchMultiRoleTask,
  fetchStockDetail,
  triggerMultiRoleTask,
  triggerStockNewsFetch,
  triggerTrendAnalysis,
} from '../../services/api/stocks'
import { formatDate, formatDateTime, formatNumber, formatPercent, listStatusLabel } from '../../shared/utils/format'
import { buildTaskScopeKey } from '../../shared/taskPersistence/taskPersistence'
import { usePersistedTaskRunner } from '../../shared/taskPersistence/usePersistedTaskRunner'

const TrendAreaChart = defineAsyncComponent(() => import('../../shared/charts/TrendAreaChart.vue'))

type DetailRow = Record<string, any>

function toNumberOrNull(value: unknown): number | null {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function looksLikeTsCode(value: string): boolean {
  return /^[0-9A-Z]{6}\.(SZ|SH|BJ)$/i.test(value.trim())
}

function joinParts(parts: Array<unknown>): string {
  return parts
    .map((item) => String(item ?? '').trim())
    .filter(Boolean)
    .join(' · ')
}

function formatSignedPercent(value: unknown, digits = 2): string {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num > 0 ? '+' : ''}${formatPercent(num, digits)}`
}

function summarizeDetail(detail: unknown): string {
  if (!detail || typeof detail !== 'object' || Array.isArray(detail)) return ''
  const items = Object.entries(detail as Record<string, unknown>)
    .filter(([, value]) => value != null && String(value).trim() !== '')
    .slice(0, 3)
    .map(([key, value]) => `${key}: ${Array.isArray(value) ? `${value.length}项` : String(value)}`)
  return items.join(' · ')
}

function summarizeAssumptions(assumptions: unknown): string {
  if (!assumptions || typeof assumptions !== 'object' || Array.isArray(assumptions)) return '暂无补充假设。'
  const items = Object.entries(assumptions as Record<string, unknown>)
    .slice(0, 4)
    .map(([key, value]) => `${key}: ${value == null ? '-' : String(value)}`)
  return items.join(' · ') || '暂无补充假设。'
}

const route = useRoute()
const router = useRouter()

const activeKeyword = computed(() => String(route.query.keyword || '').trim())
const activeTsCode = computed(() => {
  const routeTsCode = String(route.params.tsCode || '').trim().toUpperCase()
  const queryTsCode = String(route.query.ts_code || '').trim().toUpperCase()
  if (routeTsCode) return routeTsCode
  if (queryTsCode && looksLikeTsCode(queryTsCode)) return queryTsCode
  return activeKeyword.value ? '' : '000001.SZ'
})

const lookback = ref(120)
const tsCodeInput = ref('000001.SZ')
const trendResult = ref('')
const multiRoleResult = ref('')
const actionMessage = ref('准备就绪')
const fetchNewsState = ref<'idle' | 'pending' | 'success' | 'error'>('idle')
const trendState = ref<'idle' | 'pending' | 'success' | 'error'>('idle')
const multiRoleState = ref<'idle' | 'pending' | 'success' | 'error'>('idle')
const multiRoleAttempts = ref<Array<Record<string, any>>>([])
const activeMultiRoleJobId = ref('')
const chartsReady = ref(false)
const detailTabs = [
  { key: 'score', label: '评分结构' },
  { key: 'fundamental', label: '基本面与估值' },
  { key: 'flowRisk', label: '资金流与风险' },
  { key: 'events', label: '公司事件' },
  { key: 'governance', label: '治理画像' },
  { key: 'risk', label: '风险情景' },
] as const
const activeDetailTab = ref<(typeof detailTabs)[number]['key']>('score')
let multiRoleTimer = 0
const multiRoleTaskScopeKey = computed(() => buildTaskScopeKey(String(route.name || 'stock-detail'), resolvedTsCode.value || activeTsCode.value || 'default'))
const {
  restoredHint: restoredMultiRoleHint,
  noticeMessage: multiRolePersistenceNotice,
  restoreTaskSnapshot: restoreMultiRoleTaskSnapshot,
  persistTaskSnapshot: persistMultiRoleTaskSnapshot,
  clearPersistedTaskSnapshot: clearMultiRoleTaskSnapshot,
} = usePersistedTaskRunner(() => multiRoleTaskScopeKey.value)
const sectionAnchors = [
  { href: '#price-trend', label: '价格走势' },
  { href: '#company-panels', label: '公司面板' },
  { href: '#news-impact', label: '新闻与群聊' },
  { href: '#llm-analysis', label: 'LLM 分析' },
]

watch(
  [activeTsCode, activeKeyword],
  ([tsCode, keyword]) => {
    tsCodeInput.value = keyword || tsCode || '000001.SZ'
    chartsReady.value = false
    window.setTimeout(() => {
      chartsReady.value = true
    }, 80)
  },
  { immediate: true },
)

const { data: detail, refetch, isFetching, error } = useQuery({
  queryKey: ['stock-detail', activeTsCode, activeKeyword, lookback],
  queryFn: () => fetchStockDetail({
    ts_code: activeTsCode.value,
    keyword: activeKeyword.value,
    lookback: lookback.value,
  }),
})

const detailData = computed<DetailRow>(() => (detail.value ?? {}) as DetailRow)
const detailError = computed(() => error.value?.message || '')
const profile = computed<DetailRow>(() => (detailData.value.profile ?? {}) as DetailRow)
const priceSummary = computed<DetailRow>(() => (detailData.value.price_summary ?? {}) as DetailRow)
const score = computed<DetailRow>(() => (detailData.value.score ?? {}) as DetailRow)
const financialSummary = computed<DetailRow>(() => (detailData.value.financial_summary ?? {}) as DetailRow)
const valuationSummary = computed<DetailRow>(() => (detailData.value.valuation_summary ?? {}) as DetailRow)
const capitalFlowSummary = computed<DetailRow>(() => (detailData.value.capital_flow_summary ?? {}) as DetailRow)
const governanceSummary = computed<DetailRow>(() => (detailData.value.governance_summary ?? {}) as DetailRow)
const riskSummary = computed<DetailRow>(() => (detailData.value.risk_summary ?? {}) as DetailRow)
const priceRollups = computed<DetailRow>(() => (detailData.value.price_rollups ?? {}) as DetailRow)
const latestMinute = computed<DetailRow>(() => (detailData.value.latest_minline ?? {}) as DetailRow)
const candidatePoolItem = computed<DetailRow | null>(() => (detailData.value.candidate_pool_item ?? null) as DetailRow | null)
const chatroomMentions = computed<DetailRow[]>(() => (detailData.value.chatroom_mentions ?? []) as DetailRow[])
const stockNewsItems = computed<DetailRow[]>(() => (((detailData.value.stock_news_summary ?? {}) as DetailRow).recent_items ?? []) as DetailRow[])
const eventItems = computed<DetailRow[]>(() => (((detailData.value.event_summary ?? {}) as DetailRow).recent_events ?? []).slice(0, 6) as DetailRow[])
const riskItems = computed<DetailRow[]>(() => (((riskSummary.value.items ?? []) as DetailRow[]).slice(0, 6)))

const resolvedTsCode = computed(() => String(profile.value.ts_code || activeTsCode.value || '').trim())
const resolvedName = computed(() => String(profile.value.name || activeKeyword.value || '').trim())

const pageTitle = computed(() => {
  if (resolvedName.value && resolvedTsCode.value) return `${resolvedName.value} · 股票详情`
  if (resolvedTsCode.value) return `${resolvedTsCode.value} · 股票详情`
  return '股票详情'
})

const pageSubtitle = computed(() => {
  const freshness = [
    priceSummary.value.latest_trade_date ? `日线 ${formatDate(priceSummary.value.latest_trade_date)}` : '',
    latestMinute.value.trade_date ? `分钟线 ${formatDate(latestMinute.value.trade_date)} ${latestMinute.value.minute_time || ''}` : '',
    stockNewsItems.value[0]?.pub_time ? `个股新闻 ${formatDateTime(stockNewsItems.value[0].pub_time)}` : '',
    financialSummary.value.latest_report_period ? `财报 ${financialSummary.value.latest_report_period}` : '',
  ].filter(Boolean)
  return joinParts([
    profile.value.industry,
    profile.value.market,
    profile.value.area,
    profile.value.list_status ? `上市状态 ${listStatusLabel(String(profile.value.list_status))}` : '',
    freshness.length ? `数据新鲜度 ${freshness.join(' / ')}` : '',
  ]) || '统一聚合价格、评分、新闻、群聊观点和 LLM 分析结果。'
})

const latestMinuteLabel = computed(() => {
  if (!latestMinute.value.trade_date && !latestMinute.value.minute_time) return '-'
  return joinParts([formatDate(latestMinute.value.trade_date), latestMinute.value.minute_time])
})

const overviewItems = computed(() => [
  { label: '股票', value: resolvedName.value || resolvedTsCode.value || '-', hint: resolvedTsCode.value || '-' },
  { label: '最新收盘', value: formatNumber(priceSummary.value.latest_close, 3), hint: `最新日期 ${formatDate(priceSummary.value.latest_trade_date)}` },
  { label: '最新涨跌幅', value: formatSignedPercent(priceSummary.value.latest_pct_chg, 2), hint: `区间收益 ${formatSignedPercent(priceSummary.value.range_return_pct, 2)}` },
  { label: '行业内评分', value: formatNumber(score.value.industry_total_score, 2), hint: `总分 ${formatNumber(score.value.total_score, 2)}` },
  { label: '最新分钟价', value: formatNumber(latestMinute.value.price, 3), hint: latestMinuteLabel.value },
  { label: '个股新闻', value: String(stockNewsItems.value.length || 0), hint: stockNewsItems.value[0]?.pub_time ? `最近一条 ${formatDateTime(stockNewsItems.value[0].pub_time)}` : '暂无新闻' },
])

const rollupChips = computed(() => {
  const items = (priceRollups.value.items ?? []) as DetailRow[]
  if (!items.length) return []
  return items
    .filter((x) => [30, 90, 365].includes(Number(x.window_days)))
    .map((x) => ({
      label: `${Number(x.window_days)}天收益`,
      value: formatSignedPercent(x.close_change_pct, 2),
    }))
})
const actionStatusCards = computed(() => {
  const items = [
    {
      title: '股票新闻采集',
      state: fetchNewsState.value,
      description: fetchNewsState.value === 'idle' ? '尚未触发，适合先补齐最近个股新闻。'
        : fetchNewsState.value === 'pending' ? '正在采集并回填最新个股新闻。'
          : fetchNewsState.value === 'success' ? '最近一次采集已完成，可查看新闻区结果。'
            : '最近一次采集失败，请查看动作反馈并重试。',
    },
    {
      title: '走势分析',
      state: trendState.value,
      description: trendState.value === 'idle' ? '尚未触发，适合先看短期趋势判断。'
        : trendState.value === 'pending' ? 'LLM 正在生成走势分析摘要。'
          : trendState.value === 'success' ? '走势分析已完成，结果在下方 LLM 区。'
            : '走势分析失败，请查看反馈后重试。',
    },
    {
      title: '多角色分析',
      state: multiRoleState.value,
      description: multiRoleState.value === 'idle' ? '尚未触发，适合需要更深研究时再发起。'
        : multiRoleState.value === 'pending' ? '后台任务进行中，页面会自动回填结果。'
          : multiRoleState.value === 'success' ? '多角色分析已完成，可展开查看完整结论。'
            : '多角色分析失败，请查看反馈后重试。',
    },
  ]
  return items.map((item) => ({
    ...item,
    tone: item.state === 'success' ? 'success' : item.state === 'error' ? 'danger' : item.state === 'pending' ? 'warning' : 'muted',
    label: item.state === 'success' ? '已完成' : item.state === 'error' ? '失败' : item.state === 'pending' ? '进行中' : '待触发',
    meta: resolvedTsCode.value || resolvedName.value || '-',
  }))
})

const scoreItems = computed(() => [
  { label: '总分', value: formatNumber(score.value.total_score, 2), hint: score.value.score_grade || '-' },
  { label: '行业内总分', value: formatNumber(score.value.industry_total_score, 2), hint: score.value.industry_score_grade || '-' },
  { label: '趋势分', value: formatNumber(score.value.trend_score, 2), hint: '价格行为与结构' },
  { label: '财务分', value: formatNumber(score.value.financial_score, 2), hint: '盈利与现金流质量' },
  { label: '估值分', value: formatNumber(score.value.valuation_score, 2), hint: '当前估值与历史分位' },
  { label: '新闻分', value: formatNumber(score.value.news_score, 2), hint: '新闻催化与扰动' },
  { label: '市场分', value: formatNumber(score.value.market_score, 2), hint: '市场环境或风格' },
  { label: '风险分', value: formatNumber(score.value.risk_score, 2), hint: '回撤与尾部风险' },
])

const valuationCurrent = computed<DetailRow>(() => (valuationSummary.value.current ?? {}) as DetailRow)
const valuationPercentile = computed<DetailRow>(() => (valuationSummary.value.history_percentile ?? {}) as DetailRow)
const financialLatest = computed<DetailRow>(() => (financialSummary.value.latest ?? {}) as DetailRow)
const financialTrend = computed<DetailRow>(() => (financialSummary.value.trend ?? {}) as DetailRow)
const stockFlowLatest = computed<DetailRow>(() => ((capitalFlowSummary.value.stock_flow ?? {}) as DetailRow).latest ?? {})
const stockFlowRecent = computed<DetailRow>(() => ((capitalFlowSummary.value.stock_flow ?? {}) as DetailRow).recent_5d_sum ?? {})

const fundamentalItems = computed(() => [
  { label: '报告期', value: String(financialLatest.value.report_period || '-'), hint: financialLatest.value.ann_date ? `公告日 ${formatDate(financialLatest.value.ann_date)}` : '-' },
  { label: '营收同比', value: formatSignedPercent(financialTrend.value.revenue_yoy_pct, 2), hint: `营收 ${formatNumber(financialLatest.value.revenue, 2)}` },
  { label: '净利同比', value: formatSignedPercent(financialTrend.value.net_profit_yoy_pct, 2), hint: `净利 ${formatNumber(financialLatest.value.net_profit, 2)}` },
  { label: 'ROE', value: formatPercent(financialLatest.value.roe, 2), hint: `同比变动 ${formatNumber(financialTrend.value.roe_change, 2)}` },
  { label: '毛利率', value: formatPercent(financialLatest.value.gross_margin, 2), hint: `资产负债率 ${formatPercent(financialLatest.value.debt_to_assets, 2)}` },
  { label: '经营现金流', value: formatNumber(financialLatest.value.operating_cf, 2), hint: `自由现金流 ${formatNumber(financialLatest.value.free_cf, 2)}` },
  { label: 'PE(TTM)', value: formatNumber(valuationCurrent.value.pe_ttm, 2), hint: `分位 ${formatPercent(valuationPercentile.value.pe_ttm_pct, 2)}` },
  { label: 'PB', value: formatNumber(valuationCurrent.value.pb, 2), hint: `分位 ${formatPercent(valuationPercentile.value.pb_pct, 2)}` },
  { label: '股息率', value: formatPercent(valuationCurrent.value.dv_ttm, 2), hint: `分位 ${formatPercent(valuationPercentile.value.dv_ttm_pct, 2)}` },
  { label: '总市值', value: formatNumber(valuationCurrent.value.total_mv, 2), hint: `流通市值 ${formatNumber(valuationCurrent.value.circ_mv, 2)}` },
])

const governanceHolderSummary = computed<DetailRow>(() => (governanceSummary.value.holder_summary ?? {}) as DetailRow)
const governanceBoardSummary = computed<DetailRow>(() => (governanceSummary.value.board_summary ?? {}) as DetailRow)
const governanceItems = computed(() => [
  { label: '治理评分', value: formatNumber(governanceSummary.value.governance_score, 2), hint: governanceSummary.value.asof_date ? `截至 ${formatDate(governanceSummary.value.asof_date)}` : '-' },
  { label: '第一大股东占比', value: formatPercent(governanceHolderSummary.value.top1_ratio, 2), hint: `前十合计 ${formatPercent(governanceHolderSummary.value.top10_ratio_sum, 2)}` },
  { label: '股东户数', value: formatNumber(governanceHolderSummary.value.holder_num_latest, 0), hint: governanceHolderSummary.value.pledge_stat_latest ? `质押 ${governanceHolderSummary.value.pledge_stat_latest}` : '暂无质押摘要' },
  { label: '董事会薪酬周期', value: String(governanceBoardSummary.value.reward_period || '-'), hint: `总薪酬 ${formatNumber(governanceBoardSummary.value.total_reward, 2)}` },
])

const flowRiskItems = computed(() => [
  { label: '个股净流入(最新)', value: formatNumber(stockFlowLatest.value.net_inflow, 2), hint: stockFlowLatest.value.trade_date ? formatDate(stockFlowLatest.value.trade_date) : '-' },
  { label: '主力净流入(最新)', value: formatNumber(stockFlowLatest.value.main_inflow, 2), hint: `超大单 ${formatNumber(stockFlowLatest.value.super_large_inflow, 2)}` },
  { label: '5日净流入合计', value: formatNumber(stockFlowRecent.value.net_inflow, 2), hint: `5日主力合计 ${formatNumber(stockFlowRecent.value.main_inflow, 2)}` },
  { label: '治理评分', value: formatNumber(governanceSummary.value.governance_score, 2), hint: governanceSummary.value.asof_date ? `画像日期 ${formatDate(governanceSummary.value.asof_date)}` : '-' },
  { label: '风险情景日期', value: riskSummary.value.scenario_date ? formatDate(riskSummary.value.scenario_date) : '-', hint: `共 ${riskItems.value.length} 个情景` },
  { label: '候选池状态', value: candidatePoolItem.value?.dominant_bias || '未进入', hint: candidatePoolItem.value ? `净分 ${candidatePoolItem.value.net_score || 0}` : '暂无群聊候选池记录' },
])

const topHolderChips = computed(() => {
  const rows = (governanceHolderSummary.value.top10_holders ?? []) as DetailRow[]
  return rows.slice(0, 5).map((item) => {
    const name = item.holder_name || item.name || item.holder || '股东'
    const ratio = item.hold_ratio || item.ratio || item.share_ratio
    return `${name} ${formatPercent(ratio, 2)}`
  })
})

const boardMemberChips = computed(() => {
  const rows = (governanceBoardSummary.value.members ?? []) as DetailRow[]
  return rows.slice(0, 8).map((item) => {
    const name = item.name || item.member_name || '成员'
    const role = item.title || item.position || item.role || ''
    return joinParts([name, role])
  })
})

const dailyChart = computed(() => {
  const rows = (detailData.value.recent_prices ?? []) as DetailRow[]
  const labels: string[] = []
  const values: Array<number | null> = []
  rows.forEach((item) => {
    labels.push(formatDate(item.trade_date))
    values.push(toNumberOrNull(item.close))
  })
  return {
    labels,
    series: [
      { name: '收盘价', data: values, color: '#0f617a', area: true },
    ],
  }
})

const minuteChart = computed(() => {
  const rows = (detailData.value.recent_minline ?? []) as DetailRow[]
  const labels: string[] = []
  const prices: Array<number | null> = []
  const averages: Array<number | null> = []
  rows.forEach((item) => {
    labels.push(joinParts([formatDate(item.trade_date), item.minute_time]))
    prices.push(toNumberOrNull(item.price))
    averages.push(toNumberOrNull(item.avg_price))
  })
  return {
    labels,
    series: [
      { name: '分钟价', data: prices, color: '#0f617a', area: true },
      { name: '均价', data: averages, color: '#d68648', area: false },
    ],
  }
})

function applyCode() {
  const raw = tsCodeInput.value.trim()
  if (!raw) {
    router.push({ path: '/stocks/detail/000001.SZ' })
    return
  }
  const upper = raw.toUpperCase()
  if (looksLikeTsCode(upper)) {
    router.push({ path: `/stocks/detail/${encodeURIComponent(upper)}`, query: {} })
    return
  }
  router.push({ path: '/stocks/detail', query: { keyword: raw } })
}

const fetchNewsMutation = useMutation({
  mutationFn: () => triggerStockNewsFetch({
    ts_code: resolvedTsCode.value,
    company_name: resolvedName.value,
    page_size: 20,
    score: 1,
  }),
  onSuccess: (payload: DetailRow) => {
    fetchNewsState.value = 'success'
    const actualModel = String(payload.items?.[0]?.llm_model || '')
    actionMessage.value = `股票新闻采集完成：${resolvedName.value || resolvedTsCode.value || '-'}${actualModel ? ` · 实际模型 ${actualModel}` : ''}`
    refetch()
  },
  onError: (mutationError: Error) => {
    fetchNewsState.value = 'error'
    actionMessage.value = `股票新闻采集失败：${mutationError.message}`
  },
})

const trendMutation = useMutation({
  mutationFn: () => triggerTrendAnalysis({
    ts_code: resolvedTsCode.value,
    lookback: lookback.value,
  }),
  onSuccess: (payload: DetailRow) => {
    trendState.value = 'success'
    const actualModel = String(payload.used_model || payload.model || '')
    actionMessage.value = `走势分析完成：${resolvedName.value || resolvedTsCode.value || '-'}${actualModel ? ` · 实际模型 ${actualModel}` : ''}`
    trendResult.value = payload.analysis_markdown || payload.analysis || payload.result || payload.message || JSON.stringify(payload, null, 2)
  },
  onError: (mutationError: Error) => {
    trendState.value = 'error'
    actionMessage.value = `走势分析失败：${mutationError.message}`
    trendResult.value = `走势分析失败：${mutationError.message}`
  },
})

const multiRoleMutation = useMutation({
  mutationFn: () => triggerMultiRoleTask({
    ts_code: resolvedTsCode.value,
    lookback: Math.max(lookback.value, 120),
  }),
  onSuccess: (payload: DetailRow) => {
    const jobId = String(payload.job_id || '').trim()
    if (!jobId) {
      multiRoleState.value = 'error'
      actionMessage.value = '多角色分析任务创建成功，但未返回 job_id。'
      multiRoleResult.value = '任务创建成功，但未返回 job_id。'
      return
    }
    multiRoleState.value = 'pending'
    activeMultiRoleJobId.value = jobId
    actionMessage.value = `多角色分析任务已创建：${resolvedName.value || resolvedTsCode.value || '-'}`
    persistCurrentMultiRoleTask({
      status: 'queued',
      stage: 'queued',
      progress: 5,
      requested_model: String(payload.requested_model || payload.model || ''),
      updated_at: new Date().toISOString(),
    })
    startMultiRolePolling(jobId)
  },
  onError: (mutationError: Error) => {
    multiRoleState.value = 'error'
    actionMessage.value = `多角色分析失败：${mutationError.message}`
    multiRoleResult.value = `分析失败：${mutationError.message}`
    activeMultiRoleJobId.value = ''
    clearMultiRoleTaskSnapshot()
  },
})

const isFetchNewsPending = computed(() => fetchNewsMutation.isPending.value)
const isTrendPending = computed(() => trendMutation.isPending.value)
const isMultiRolePending = computed(() => multiRoleMutation.isPending.value)

function fetchStockNewsNow() {
  if (!resolvedTsCode.value) return
  fetchNewsState.value = 'pending'
  actionMessage.value = `正在采集 ${resolvedName.value || resolvedTsCode.value || '-'} 的股票新闻...`
  fetchNewsMutation.mutate()
}

function runTrend() {
  if (!resolvedTsCode.value) {
    trendResult.value = '当前没有可分析的股票代码。'
    return
  }
  trendState.value = 'pending'
  actionMessage.value = `正在分析 ${resolvedName.value || resolvedTsCode.value || '-'} 的走势...`
  trendResult.value = '正在生成走势分析...'
  trendMutation.mutate()
}

function runMultiRole() {
  if (!resolvedTsCode.value) {
    multiRoleResult.value = '当前没有可分析的股票代码。'
    return
  }
  multiRoleState.value = 'pending'
  actionMessage.value = `正在创建 ${resolvedName.value || resolvedTsCode.value || '-'} 的多角色分析任务...`
  multiRoleResult.value = '任务已创建，正在后台生成分析...'
  multiRoleAttempts.value = []
  multiRoleMutation.mutate()
}

function persistCurrentMultiRoleTask(result: Record<string, any>) {
  if (!activeMultiRoleJobId.value) return
  persistMultiRoleTaskSnapshot({
    jobId: activeMultiRoleJobId.value,
    status: String(result.status || multiRoleState.value || ''),
    stage: String(result.stage || ''),
    progress: Number(result.progress || 0),
    requestedModel: String(result.requested_model || ''),
    usedModel: String(result.used_model || result.model || ''),
    attempts: multiRoleAttempts.value,
    actionMessage: actionMessage.value,
    error: String(result.error || ''),
    updatedAt: String(result.updated_at || new Date().toISOString()),
    data: {
      multiRoleResult: multiRoleResult.value,
      multiRoleState: multiRoleState.value,
    },
  })
}

function startMultiRolePolling(jobId: string) {
  activeMultiRoleJobId.value = jobId
  window.clearTimeout(multiRoleTimer)
  const poll = async () => {
    try {
      const result = await fetchMultiRoleTask({ job_id: jobId })
      multiRoleAttempts.value = Array.isArray(result.attempts) ? result.attempts : multiRoleAttempts.value
      if (result.status === 'done') {
        multiRoleState.value = 'success'
        const actualModel = String(result.used_model || result.model || '')
        actionMessage.value = `多角色分析完成：${resolvedName.value || resolvedTsCode.value || '-'}${actualModel ? ` · 实际模型 ${actualModel}` : ''}`
        multiRoleResult.value = result.analysis_markdown || result.analysis || result.result || '分析完成，但未返回正文。'
        persistCurrentMultiRoleTask(result as Record<string, any>)
        return
      }
      if (result.status === 'error' || result.status === 'aborted') {
        multiRoleState.value = 'error'
        actionMessage.value = `多角色分析失败：${result.error || result.message || '未知错误'}`
        multiRoleResult.value = `分析失败：${result.error || result.message || '未知错误'}`
        persistCurrentMultiRoleTask(result as Record<string, any>)
        return
      }
      multiRoleResult.value = `任务状态：${result.message || result.status || '运行中'}\n进度：${result.progress || 0}%`
      persistCurrentMultiRoleTask(result as Record<string, any>)
      multiRoleTimer = window.setTimeout(poll, 3000)
    } catch (e: any) {
      multiRoleState.value = 'error'
      actionMessage.value = `多角色分析轮询失败：${e?.message || String(e)}`
      multiRoleResult.value = actionMessage.value
      persistCurrentMultiRoleTask({ status: 'error', progress: 100, error: String(e?.message || e) })
    }
  }
  poll()
}

function clearMultiRoleTaskTracking() {
  window.clearTimeout(multiRoleTimer)
  activeMultiRoleJobId.value = ''
  multiRoleAttempts.value = []
  multiRoleState.value = 'idle'
  multiRoleResult.value = ''
  actionMessage.value = '已清除当前任务跟踪。'
  clearMultiRoleTaskSnapshot()
}

function restoreMultiRoleTask() {
  const restored = restoreMultiRoleTaskSnapshot()
  if (!restored) {
    window.clearTimeout(multiRoleTimer)
    activeMultiRoleJobId.value = ''
    multiRoleAttempts.value = []
    multiRoleState.value = 'idle'
    return
  }
  activeMultiRoleJobId.value = String(restored.jobId || '')
  multiRoleAttempts.value = Array.isArray(restored.attempts) ? restored.attempts : []
  actionMessage.value = restored.actionMessage || '已恢复历史任务跟踪。'
  multiRoleResult.value = String(restored.data?.multiRoleResult || multiRoleResult.value || '任务跟踪已恢复，正在同步最新状态...')
  const restoredState = String(restored.data?.multiRoleState || restored.status || '').toLowerCase()
  if (restoredState === 'done' || restoredState === 'success') {
    multiRoleState.value = 'success'
  } else if (restoredState === 'error' || restoredState === 'aborted') {
    multiRoleState.value = 'error'
  } else {
    multiRoleState.value = 'pending'
  }
  if (activeMultiRoleJobId.value && multiRoleState.value === 'pending') {
    startMultiRolePolling(activeMultiRoleJobId.value)
  }
}

onMounted(() => {
  restoreMultiRoleTask()
})

watch(
  () => multiRoleTaskScopeKey.value,
  () => {
    window.clearTimeout(multiRoleTimer)
    restoreMultiRoleTask()
  },
)

onBeforeUnmount(() => {
  window.clearTimeout(multiRoleTimer)
})
</script>
