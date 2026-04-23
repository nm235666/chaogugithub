<template>
  <AppShell title="市场结论" subtitle="今日交易主线、主要风险、行业影响与候选方向综合汇总。">
    <div class="space-y-4">
      <!-- Header CTA -->
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div v-if="data" class="text-xs text-[var(--muted)]">
          数据更新：{{ freshnessLabel }}
        </div>
        <RouterLink
          to="/app/decision"
          class="rounded-2xl bg-[var(--brand)] px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:opacity-90"
        >
          进入决策工作台 →
        </RouterLink>
      </div>

      <!-- Loading state -->
      <div v-if="isPending" class="py-16 text-center text-sm text-[var(--muted)]">
        <div class="text-base font-semibold text-[var(--ink)]">加载市场结论中...</div>
        <div class="mt-1">正在聚合新闻、宏观与信号数据</div>
      </div>

      <!-- Error state -->
      <div v-else-if="isError" class="rounded-[var(--radius-lg)] border border-rose-200 bg-rose-50 px-6 py-8 text-center">
        <div class="text-base font-semibold text-rose-700">加载失败</div>
        <div class="mt-1 text-sm text-rose-600">{{ String(error) }}</div>
        <button
          class="mt-4 rounded-2xl border border-rose-300 bg-white px-4 py-2 text-sm font-semibold text-rose-700 transition hover:bg-rose-50"
          @click="() => refetch()"
        >
          重试
        </button>
      </div>

      <template v-else-if="data">
        <div class="mb-4 rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm">
          <div class="flex flex-wrap items-center gap-2">
            <span class="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">可用性分级</span>
            <span class="rounded-full px-2.5 py-1 text-xs font-semibold" :class="availabilityBadgeClass">{{ availabilityLabel }}</span>
            <span class="text-xs text-[var(--muted)]">{{ pageStatusDescription }}</span>
          </div>
          <div class="mt-2 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">数据源 {{ sourcesCount }}</span>
            <span class="metric-chip">候选 {{ candidateDirections.length }}</span>
            <span class="metric-chip">风险 {{ mainRisks.length }}</span>
            <span v-for="item in missingInputs" :key="item" class="metric-chip text-amber-700">{{ item }}</span>
          </div>
        </div>

        <StatePanel
          v-if="pageStatus !== 'ready'"
          class="mb-4"
          :tone="pageStatus === 'not_initialized' ? 'warning' : 'default'"
          :title="pageStatusTitle"
          :description="pageStatusDescription"
        >
          <template #action>
            <RouterLink
              to="/app/signals/themes"
              class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
            >
              去看主题信号
            </RouterLink>
            <RouterLink
              to="/app/workbench"
              class="rounded-2xl bg-[var(--brand)] px-4 py-2 text-sm font-semibold text-white transition hover:opacity-90"
            >
              回研究工作台
            </RouterLink>
          </template>
        </StatePanel>

        <!-- 今日交易主线 -->
        <PageSection title="今日交易主线" subtitle="综合宏观、新闻与信号的核心交易逻辑。">
          <div class="rounded-2xl border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-5 py-4 shadow-[var(--shadow-soft)]">
            <div class="mb-3 flex flex-wrap items-center gap-2">
              <span
                v-if="conflictResolution"
                class="inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-semibold"
                :class="confidenceBadgeClass"
                data-testid="conclusion-confidence-badge"
                :data-confidence="confidencePct"
                :data-winner-source="conflictResolution.winning_source || ''"
                :data-direction="conflictResolution.direction || ''"
                :data-needs-review="String(!!conflictResolution.needs_review)"
              >
                结论可信度 {{ confidencePct }}%
                <span v-if="conflictResolution.direction" class="opacity-80">· {{ conflictResolution.direction }}</span>
              </span>
              <span
                v-if="conflictResolution?.winning_source"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1 text-xs text-[var(--muted)]"
                :data-testid="'conclusion-winner-source'"
                :data-winner-source="conflictResolution.winning_source"
              >
                主导来源：{{ sourceLabel(conflictResolution.winning_source) }}
              </span>
              <button
                v-if="conflictResolution?.score_breakdown?.sources?.length"
                type="button"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                data-testid="conclusion-rule-trace-toggle"
                @click="ruleTraceOpen = !ruleTraceOpen"
              >
                {{ ruleTraceOpen ? '隐藏规则链' : '查看规则链' }}
              </button>
            </div>

            <div
              v-if="conflictResolution?.needs_review"
              class="mb-3 flex items-start gap-2 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800"
              data-testid="conclusion-needs-review-alert"
            >
              <span class="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-amber-500 text-[10px] font-bold text-white">!</span>
              <div>
                <div class="font-semibold">当前可信度较低，建议人工复核。</div>
                <div v-if="conflictResolution.dissenting_sources?.length" class="mt-0.5 text-amber-700">
                  异议来源：{{ (conflictResolution.dissenting_sources as string[]).map(sourceLabel).join('、') }}
                </div>
              </div>
            </div>

            <div v-if="apiData.trading_theme" class="text-[15px] leading-7 text-[var(--ink)]">
              {{ apiData.trading_theme }}
            </div>
            <div v-else class="text-sm text-[var(--muted)]">
              {{ pageStatus === 'not_initialized' ? '关键数据源尚未准备完成，暂时无法形成主线判断。' : '当前证据覆盖不足，尚未形成稳定交易主线。' }}
            </div>
            <div v-if="missingInputs.length" class="mt-3 flex flex-wrap gap-2 text-xs">
              <span class="font-semibold text-[var(--muted)]">缺失输入：</span>
              <span v-for="item in missingInputs" :key="item" class="metric-chip">{{ item }}</span>
            </div>
          </div>
        </PageSection>

        <!-- 冲突裁决规则链 (R30) -->
        <PageSection
          v-if="ruleTraceOpen && conflictResolution?.score_breakdown?.sources?.length"
          title="冲突裁决规则链"
          subtitle="按 信号强度(45%) · 时效(20%) · AI一致性(20%) · 风险优先(15%) 综合评分。"
        >
          <div
            class="overflow-x-auto rounded-2xl border border-[var(--line)] bg-white"
            data-testid="conclusion-score-breakdown"
            :data-winner-source="conflictResolution.winning_source || ''"
          >
            <table class="w-full text-xs">
              <thead class="bg-[var(--muted-bg,#f5f7fa)] text-[var(--muted)]">
                <tr class="border-b border-[var(--line)]">
                  <th class="px-3 py-2 text-left font-semibold">来源</th>
                  <th class="px-3 py-2 text-left font-semibold">方向</th>
                  <th class="px-3 py-2 text-right font-semibold">综合</th>
                  <th class="px-3 py-2 text-right font-semibold">信号强度</th>
                  <th class="px-3 py-2 text-right font-semibold">时效</th>
                  <th class="px-3 py-2 text-right font-semibold">AI一致性</th>
                  <th class="px-3 py-2 text-right font-semibold">风险优先</th>
                  <th class="px-3 py-2 text-right font-semibold">数据龄(小时)</th>
                  <th class="px-3 py-2 text-right font-semibold">样本数</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="row in scoreBreakdownSources"
                  :key="row.source"
                  class="border-b border-[var(--line)] last:border-b-0"
                  :class="row.source === conflictResolution.winning_source ? 'bg-emerald-50/60' : ''"
                  :data-source="row.source"
                  :data-composite="row.composite"
                  :data-is-winner="row.source === conflictResolution.winning_source ? 'true' : 'false'"
                >
                  <td class="px-3 py-2">
                    <span class="font-semibold text-[var(--ink)]">{{ sourceLabel(row.source) }}</span>
                    <span
                      v-if="row.source === conflictResolution.winning_source"
                      class="ml-2 rounded-full bg-emerald-600 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-white"
                    >Winner</span>
                    <span
                      v-else-if="(conflictResolution.dissenting_sources as string[] | undefined)?.includes(row.source)"
                      class="ml-2 rounded-full bg-amber-500 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-white"
                      data-testid="conclusion-dissenting-chip"
                      :data-source="row.source"
                    >Dissent</span>
                  </td>
                  <td class="px-3 py-2 text-[var(--ink)]">{{ row.direction }}</td>
                  <td class="px-3 py-2 text-right font-semibold text-[var(--ink)]">{{ fmtScore(row.composite) }}</td>
                  <td class="px-3 py-2 text-right">{{ fmtScore(row.signal_strength) }}</td>
                  <td class="px-3 py-2 text-right">{{ fmtScore(row.recency_score) }}</td>
                  <td class="px-3 py-2 text-right">{{ fmtScore(row.ai_consistency) }}</td>
                  <td class="px-3 py-2 text-right">{{ fmtScore(row.risk_priority) }}</td>
                  <td class="px-3 py-2 text-right text-[var(--muted)]">{{ fmtHours(row.data_age_hours) }}</td>
                  <td class="px-3 py-2 text-right text-[var(--muted)]">{{ row.row_count ?? '-' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div
            v-if="apiData.resolution_basis"
            class="mt-2 text-xs text-[var(--muted)]"
            data-testid="conclusion-resolution-basis"
          >
            裁决依据：{{ apiData.resolution_basis }}
          </div>
        </PageSection>

        <!-- 主要风险 -->
        <PageSection title="主要风险" subtitle="当前需要关注的核心风险因子。">
          <div v-if="mainRisks.length === 0" class="text-sm text-[var(--muted)]">
            {{ pageStatus === 'ready' ? '当前未识别到需要前置披露的核心风险。' : '当前缺少稳定风险输入，建议先到信号总览继续核对风险侧证据。' }}
          </div>
          <ul v-else class="space-y-2">
            <li
              v-for="(risk, idx) in mainRisks"
              :key="idx"
              class="flex items-start gap-3 rounded-2xl border border-rose-100 bg-rose-50 px-4 py-3"
            >
              <span class="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-rose-200 text-[11px] font-bold text-rose-700">{{ idx + 1 }}</span>
              <span class="text-sm text-rose-800">{{ risk }}</span>
            </li>
          </ul>
        </PageSection>

        <!-- 行业影响 -->
        <PageSection title="行业影响" subtitle="受益行业与受压行业分析。">
          <div class="grid gap-4 sm:grid-cols-2">
            <div>
              <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-emerald-600">受益行业</div>
              <div v-if="benefitingSectors.length === 0" class="text-sm text-[var(--muted)]">当前未形成稳定受益行业判断。</div>
              <ul v-else class="space-y-1.5">
                <li
                  v-for="(sector, idx) in benefitingSectors"
                  :key="idx"
                  class="flex items-center gap-2 rounded-2xl border border-emerald-100 bg-emerald-50 px-3 py-2 text-sm text-emerald-800"
                >
                  <span class="h-1.5 w-1.5 shrink-0 rounded-full bg-emerald-500" />
                  {{ sector }}
                </li>
              </ul>
            </div>
            <div>
              <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-rose-600">受压行业</div>
              <div v-if="pressuredSectors.length === 0" class="text-sm text-[var(--muted)]">当前未形成稳定受压行业判断。</div>
              <ul v-else class="space-y-1.5">
                <li
                  v-for="(sector, idx) in pressuredSectors"
                  :key="idx"
                  class="flex items-center gap-2 rounded-2xl border border-rose-100 bg-rose-50 px-3 py-2 text-sm text-rose-800"
                >
                  <span class="h-1.5 w-1.5 shrink-0 rounded-full bg-rose-500" />
                  {{ sector }}
                </li>
              </ul>
            </div>
          </div>
        </PageSection>

        <!-- 候选方向 -->
        <PageSection title="候选方向" subtitle="当前具备交易逻辑的重点候选标的。">
          <div v-if="candidateDirections.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-8 text-center text-sm text-[var(--muted)]">
            当前还没有形成稳定候选方向。建议先去主题热点或信号总览筛出今日主线，再回到这里收口。
          </div>
          <div v-else class="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            <div
              v-for="candidate in candidateDirections"
              :key="candidate.ts_code"
              class="rounded-[var(--radius-card)] border border-[var(--line)] bg-white p-4 shadow-[var(--shadow-soft)] transition hover:shadow-[var(--shadow-card)]"
            >
              <div class="flex items-start justify-between gap-2">
                <div>
                  <div class="text-[15px] font-bold text-[var(--ink)]">{{ candidate.name || candidate.ts_code }}</div>
                  <div class="mt-0.5 text-xs text-[var(--muted)]">{{ candidate.ts_code }}</div>
                </div>
              </div>
              <div v-if="candidate.reason" class="mt-2 text-[13px] leading-6 text-[var(--muted)] line-clamp-3">
                {{ candidate.reason }}
              </div>
              <div class="mt-3 flex flex-wrap gap-2">
                <RouterLink
                  :to="`/app/stocks/detail/${candidate.ts_code}`"
                  class="rounded-full border border-[var(--line)] bg-white px-3 py-1 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                >
                  查看详情
                </RouterLink>
                <RouterLink
                  :to="`/app/workbench?ts_code=${candidate.ts_code}`"
                  class="rounded-full bg-[var(--brand)] px-3 py-1 text-xs font-semibold text-white transition hover:opacity-90"
                >
                  进入研究
                </RouterLink>
              </div>
            </div>
          </div>
        </PageSection>

        <!-- 冲突说明 -->
        <PageSection
          v-if="apiData.conflict_note"
          title="冲突说明"
          subtitle="信号或逻辑存在分歧时的处理说明。"
        >
          <div class="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-4">
            <div class="text-sm font-semibold text-amber-800">冲突说明</div>
            <div class="mt-1 text-[13px] leading-6 text-amber-700">{{ apiData.conflict_note }}</div>
            <div v-if="apiData.resolution_basis" class="mt-3">
              <div class="text-xs font-semibold uppercase tracking-[0.14em] text-amber-700">解决依据</div>
              <div class="mt-1 text-[13px] leading-6 text-amber-700">{{ apiData.resolution_basis }}</div>
            </div>
          </div>
        </PageSection>

        <!-- 数据来源 -->
        <PageSection title="数据来源" subtitle="本次结论依赖的数据源统计。">
          <div v-if="sourcesCount === 0" class="text-sm text-[var(--muted)]">暂无来源信息</div>
          <div v-else class="space-y-3">
            <div class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm">
              <span class="font-semibold text-[var(--ink)]">{{ sourcesCount }}</span>
              <span class="ml-1 text-[var(--muted)]">个数据来源</span>
            </div>
            <div class="flex flex-wrap gap-3">
              <div
                v-for="src in sourcesList"
                :key="src.key"
                class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-xs text-[var(--muted)]"
              >
                <span class="font-semibold text-[var(--ink)]">{{ src.label }}</span>
                <span class="ml-1">· {{ src.count ?? 0 }}</span>
                <span class="ml-1">· {{ sourceStatusLabel(src.status) }}</span>
              </div>
            </div>
          </div>
        </PageSection>
      </template>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import { fetchMarketConclusion } from '../../services/api/market'

const {
  data,
  isPending,
  isError,
  error,
  refetch,
} = useQuery({
  queryKey: ['market-conclusion'],
  queryFn: fetchMarketConclusion,
  refetchInterval: 300_000,
})

const apiData = computed(() => (data.value as any) || {})
const pageStatus = computed(() => String(apiData.value?.status || '').trim() || 'empty')
const missingInputs = computed<string[]>(() => Array.isArray(apiData.value?.missing_inputs) ? apiData.value.missing_inputs : [])
const availabilityLabel = computed(() => {
  if (pageStatus.value === 'ready') return 'ready'
  if (pageStatus.value === 'insufficient_evidence') return 'degraded'
  if (pageStatus.value === 'not_initialized') return 'not_initialized'
  return pageStatus.value || 'unknown'
})
const availabilityBadgeClass = computed(() => {
  if (pageStatus.value === 'ready') return 'border border-emerald-200 bg-emerald-100 text-emerald-800'
  if (pageStatus.value === 'insufficient_evidence') return 'border border-amber-200 bg-amber-100 text-amber-800'
  if (pageStatus.value === 'not_initialized') return 'border border-rose-200 bg-rose-100 text-rose-700'
  return 'border border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]'
})
const pageStatusTitle = computed(() => {
  if (pageStatus.value === 'insufficient_evidence') return '当前结论为降级输出'
  if (pageStatus.value === 'not_initialized') return '关键来源尚未初始化'
  if (pageStatus.value === 'error') return '市场结论生成失败'
  return '市场结论暂未就绪'
})
const pageStatusDescription = computed(() => {
  const reason = String(apiData.value?.status_reason || '').trim()
  if (reason) return reason
  if (pageStatus.value === 'not_initialized') return '系统还没有拿到足够的数据源，无法稳定生成市场结论。'
  if (pageStatus.value === 'insufficient_evidence') return '部分输入已到位，但还不够支撑完整主线、风险和候选方向。'
  return '当前还没有命中足够的市场结论数据。'
})

const freshnessLabel = computed(() => {
  if (!apiData.value?.generated_at) return '未知'
  try {
    return new Date(apiData.value.generated_at).toLocaleString('zh-CN')
  } catch {
    return apiData.value.generated_at
  }
})

const mainRisks = computed<string[]>(() => {
  const r = apiData.value?.main_risks
  if (!r) return []
  if (Array.isArray(r)) return r
  return []
})

const benefitingSectors = computed<string[]>(() => {
  const s = apiData.value?.benefiting_sectors ?? apiData.value?.sector_impact?.benefiting_sectors
  if (!s) return []
  if (Array.isArray(s)) return s
  return []
})

const pressuredSectors = computed<string[]>(() => {
  const s = apiData.value?.pressured_sectors ?? apiData.value?.sector_impact?.pressured_sectors
  if (!s) return []
  if (Array.isArray(s)) return s
  return []
})

const candidateDirections = computed<any[]>(() => {
  const c = apiData.value?.candidate_directions
  if (!c) return []
  if (Array.isArray(c)) return c
  return []
})

const sourcesCount = computed<number>(() => {
  const s = apiData.value?.sources
  if (!s) return 0
  if (Array.isArray(s)) return s.length
  if (typeof s === 'number') return s
  return 0
})

const sourcesList = computed<Array<{ key: string; label: string; count?: number; status?: string }>>(() => {
  const s = apiData.value?.sources
  if (!Array.isArray(s)) return []
  return s.slice(0, 10).map((item: any, idx: number) => ({
    key: String(item?.key || idx),
    label: String(item?.label || item?.key || '来源'),
    count: typeof item?.count === 'number' ? item.count : undefined,
    status: String(item?.status || '').trim() || undefined,
  }))
})

function sourceStatusLabel(status?: string) {
  if (status === 'ready') return '已纳入'
  if (status === 'empty') return '当前为空'
  if (status === 'missing') return '缺失'
  return status || '未知'
}

// R30 conflict arbitration wiring
interface ConflictSourceRow {
  source: string
  direction: string
  composite: number
  signal_strength: number
  recency_score: number
  ai_consistency: number
  risk_priority: number
  data_age_hours: number
  row_count?: number
}

const ruleTraceOpen = ref(false)

const conflictResolution = computed(() => {
  const cr = apiData.value?.conflict_resolution
  return cr && typeof cr === 'object' ? cr : null
})

const confidencePct = computed(() => {
  const raw = Number(conflictResolution.value?.confidence ?? 0)
  if (!Number.isFinite(raw) || raw <= 0) return 0
  return Math.round(Math.max(0, Math.min(1, raw)) * 100)
})

const confidenceBadgeClass = computed(() => {
  const pct = confidencePct.value
  if (pct >= 75) return 'bg-emerald-100 text-emerald-800 border border-emerald-200'
  if (pct >= 50) return 'bg-sky-100 text-sky-800 border border-sky-200'
  return 'bg-amber-100 text-amber-800 border border-amber-200'
})

const scoreBreakdownSources = computed<ConflictSourceRow[]>(() => {
  const sources = conflictResolution.value?.score_breakdown?.sources
  return Array.isArray(sources) ? (sources as ConflictSourceRow[]) : []
})

const SOURCE_LABELS: Record<string, string> = {
  theme_hotspots: '主题热点',
  investment_signals: '投资信号',
  news_daily_summaries: '新闻日报',
  stock_news_items: '个股新闻',
  macro_series: '宏观序列',
  risk_scenarios: '风险情景',
}

function sourceLabel(key: string | undefined | null): string {
  const k = String(key || '').trim()
  if (!k) return '未知来源'
  return SOURCE_LABELS[k] || k
}

function fmtScore(v: unknown): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '-'
  return n.toFixed(2)
}

function fmtHours(v: unknown): string {
  const n = Number(v)
  if (!Number.isFinite(n) || n >= 900) return '-'
  return n.toFixed(1)
}

</script>
