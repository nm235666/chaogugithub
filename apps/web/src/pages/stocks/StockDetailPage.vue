<template>
  <AppShell :title="pageTitle" :subtitle="pageSubtitle">
    <div class="space-y-4">
      <PageSection title="研究定位" subtitle="支持输入 ts_code 或股票简称，把详情、图表、新闻、群聊与 LLM 动作压进一个入口。">
        <div class="grid gap-4 2xl:grid-cols-[1.15fr_0.85fr]">
          <div class="space-y-4">
            <div class="grid gap-3 xl:grid-cols-[1fr_140px_140px]">
              <input
                v-model="tsCodeInput"
                class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
                placeholder="输入 ts_code 或简称，如 000001.SZ / 平安银行"
                @keydown.enter="applyCode"
              />
              <select v-model.number="lookback" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                <option :value="60">近 60 日</option>
                <option :value="120">近 120 日</option>
                <option :value="240">近 240 日</option>
              </select>
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
            <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)] shadow-[var(--shadow-soft)]">
              {{ actionMessage }}
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
          {{ detailError }}
        </div>
      </PageSection>

      <div class="grid gap-4 2xl:grid-cols-[1.08fr_0.92fr]">
        <PageSection title="近端价格走势" subtitle="最近一段收盘价的结构，用来快速判断趋势背景和区间位置。">
          <TrendAreaChart :labels="dailyChart.labels" :series="dailyChart.series" :height="320" empty-text="暂无日线数据" />
          <div class="mt-4 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">最新收盘 <strong>{{ formatNumber(priceSummary.latest_close, 3) }}</strong></span>
            <span class="metric-chip">最新涨跌幅 <strong>{{ formatSignedPercent(priceSummary.latest_pct_chg, 2) }}</strong></span>
            <span class="metric-chip">区间收益 <strong>{{ formatSignedPercent(priceSummary.range_return_pct, 2) }}</strong></span>
            <span class="metric-chip">区间高点 <strong>{{ formatNumber(priceSummary.high_lookback, 3) }}</strong></span>
            <span class="metric-chip">区间低点 <strong>{{ formatNumber(priceSummary.low_lookback, 3) }}</strong></span>
          </div>
        </PageSection>

        <PageSection title="最新分钟走势" subtitle="最近一批分钟线，帮助判断盘中价格和均价是否同向。">
          <TrendAreaChart :labels="minuteChart.labels" :series="minuteChart.series" :height="320" empty-text="暂无分钟线数据" />
          <div class="mt-4 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">最新分钟价 <strong>{{ formatNumber(latestMinute.price, 3) }}</strong></span>
            <span class="metric-chip">最新均价 <strong>{{ formatNumber(latestMinute.avg_price, 3) }}</strong></span>
            <span class="metric-chip">分钟时间 <strong>{{ latestMinuteLabel }}</strong></span>
            <span class="metric-chip">最新成交量 <strong>{{ formatNumber(latestMinute.volume, 0) }}</strong></span>
            <span class="metric-chip">累计成交量 <strong>{{ formatNumber(latestMinute.total_volume, 0) }}</strong></span>
          </div>
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-3">
        <PageSection title="评分结构" subtitle="把总分、行业内评分和趋势/新闻等核心分项放在一块看。">
          <MetricGrid :items="scoreItems" empty-text="暂无评分数据" />
        </PageSection>
        <PageSection title="基本面与估值" subtitle="先看财报质量，再看当前估值和历史分位。">
          <MetricGrid :items="fundamentalItems" empty-text="暂无财务或估值数据" />
        </PageSection>
        <PageSection title="资金流与风险" subtitle="把个股资金、治理、风险情景合在一个面板里。">
          <MetricGrid :items="flowRiskItems" empty-text="暂无资金流或风险数据" />
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-2">
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

      <div class="grid gap-4 xl:grid-cols-3">
        <PageSection title="公司事件" subtitle="最近公告、事件或动作，帮助解释价格与情绪变化。">
          <div class="space-y-2">
            <InfoCard
              v-for="item in eventItems"
              :key="`${item.event_type}-${item.event_date}-${item.title}`"
              :title="item.title || item.event_type || '-'"
              :meta="joinParts([item.event_type, formatDate(item.event_date || item.ann_date)])"
              :description="summarizeDetail(item.detail)"
            />
          </div>
        </PageSection>

        <PageSection title="治理画像" subtitle="股东结构、董事会与管理层变化，帮助看控制权和治理稳健度。">
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
        </PageSection>

        <PageSection title="风险情景" subtitle="关注压力情景下的收益、回撤和尾部风险，而不是只看单点上涨空间。">
          <div class="space-y-2">
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
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-2">
        <PageSection title="LLM 走势分析" subtitle="直接在本页看趋势判断，不用再切到单独分析页。">
          <MarkdownBlock :content="trendResult || '尚未发起走势分析。'" />
        </PageSection>
        <PageSection title="LLM 多角色分析" subtitle="继续保留异步轮询结果，让分析完成后自动落回这个页面。">
          <MarkdownBlock :content="multiRoleResult || '尚未发起多角色分析。'" />
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import TrendAreaChart from '../../shared/charts/TrendAreaChart.vue'
import MetricGrid from '../../shared/ui/MetricGrid.vue'
import {
  fetchMultiRoleTask,
  fetchStockDetail,
  triggerMultiRoleTask,
  triggerStockNewsFetch,
  triggerTrendAnalysis,
} from '../../services/api/stocks'
import { formatDate, formatDateTime, formatNumber, formatPercent, listStatusLabel } from '../../shared/utils/format'

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
  if (routeTsCode) return routeTsCode
  return activeKeyword.value ? '' : '000001.SZ'
})

const lookback = ref(120)
const tsCodeInput = ref('000001.SZ')
const trendResult = ref('')
const multiRoleResult = ref('')
const actionMessage = ref('准备就绪')
let multiRoleTimer = 0

watch(
  [activeTsCode, activeKeyword],
  ([tsCode, keyword]) => {
    tsCodeInput.value = keyword || tsCode || '000001.SZ'
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
    const actualModel = String(payload.items?.[0]?.llm_model || '')
    actionMessage.value = `股票新闻采集完成：${resolvedName.value || resolvedTsCode.value || '-'}${actualModel ? ` · 实际模型 ${actualModel}` : ''}`
    refetch()
  },
  onError: (mutationError: Error) => {
    actionMessage.value = `股票新闻采集失败：${mutationError.message}`
  },
})

const trendMutation = useMutation({
  mutationFn: () => triggerTrendAnalysis({
    ts_code: resolvedTsCode.value,
    lookback: lookback.value,
  }),
  onSuccess: (payload: DetailRow) => {
    const actualModel = String(payload.used_model || payload.model || '')
    actionMessage.value = `走势分析完成：${resolvedName.value || resolvedTsCode.value || '-'}${actualModel ? ` · 实际模型 ${actualModel}` : ''}`
    trendResult.value = payload.analysis || payload.result || payload.message || JSON.stringify(payload, null, 2)
  },
  onError: (mutationError: Error) => {
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
      actionMessage.value = '多角色分析任务创建成功，但未返回 job_id。'
      multiRoleResult.value = '任务创建成功，但未返回 job_id。'
      return
    }
    actionMessage.value = `多角色分析任务已创建：${resolvedName.value || resolvedTsCode.value || '-'}`
    window.clearTimeout(multiRoleTimer)
    const poll = async () => {
      const result = await fetchMultiRoleTask({ job_id: jobId })
      if (result.status === 'done') {
        const actualModel = String(result.used_model || result.model || '')
        actionMessage.value = `多角色分析完成：${resolvedName.value || resolvedTsCode.value || '-'}${actualModel ? ` · 实际模型 ${actualModel}` : ''}`
        multiRoleResult.value = result.analysis_markdown || result.analysis || result.result || '分析完成，但未返回正文。'
        return
      }
      if (result.status === 'error') {
        actionMessage.value = `多角色分析失败：${result.error || result.message || '未知错误'}`
        multiRoleResult.value = `分析失败：${result.error || result.message || '未知错误'}`
        return
      }
      multiRoleResult.value = `任务状态：${result.message || result.status || '运行中'}\n进度：${result.progress || 0}%`
      multiRoleTimer = window.setTimeout(poll, 3000)
    }
    poll()
  },
  onError: (mutationError: Error) => {
    actionMessage.value = `多角色分析失败：${mutationError.message}`
    multiRoleResult.value = `分析失败：${mutationError.message}`
  },
})

const isFetchNewsPending = computed(() => fetchNewsMutation.isPending.value)
const isTrendPending = computed(() => trendMutation.isPending.value)
const isMultiRolePending = computed(() => multiRoleMutation.isPending.value)

function fetchStockNewsNow() {
  if (!resolvedTsCode.value) return
  actionMessage.value = `正在采集 ${resolvedName.value || resolvedTsCode.value || '-'} 的股票新闻...`
  fetchNewsMutation.mutate()
}

function runTrend() {
  if (!resolvedTsCode.value) {
    trendResult.value = '当前没有可分析的股票代码。'
    return
  }
  actionMessage.value = `正在分析 ${resolvedName.value || resolvedTsCode.value || '-'} 的走势...`
  trendResult.value = '正在生成走势分析...'
  trendMutation.mutate()
}

function runMultiRole() {
  if (!resolvedTsCode.value) {
    multiRoleResult.value = '当前没有可分析的股票代码。'
    return
  }
  actionMessage.value = `正在创建 ${resolvedName.value || resolvedTsCode.value || '-'} 的多角色分析任务...`
  multiRoleResult.value = '任务已创建，正在后台生成分析...'
  multiRoleMutation.mutate()
}

onBeforeUnmount(() => {
  window.clearTimeout(multiRoleTimer)
})
</script>
