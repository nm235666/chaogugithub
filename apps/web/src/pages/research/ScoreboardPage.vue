<template>
  <AppShell title="评分总览" subtitle="数据统计层主入口：聚合评分证据，不承载动作执行。">
    <div class="space-y-4">
      <PageSection title="总览状态" subtitle="先确认当前评分快照是否完整，再决定下钻到方向验证层。">
        <div class="flex flex-wrap gap-2">
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isFetching" @click="refetch()">
            {{ isFetching ? '刷新中...' : '刷新总览' }}
          </button>
          <RouterLink class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)]" to="/app/decision">打开决策看板</RouterLink>
          <RouterLink class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)]" to="/app/stocks/scores">查看股票评分列表</RouterLink>
        </div>
        <div class="mt-3 rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
          <div>
            本页使用 <code>/api/decision/scores</code> 聚合统计与理由包；执行动作与回执请到
            <RouterLink to="/app/decision" class="font-semibold text-[var(--brand)] hover:underline">决策看板</RouterLink>。
          </div>
          <div class="mt-2 flex flex-wrap gap-2 text-xs">
            <span class="metric-chip">数据统计层</span>
            <span class="metric-chip">结果展示层输入</span>
            <span class="metric-chip">方向验证层下钻</span>
          </div>
        </div>

        <StatePanel
          v-if="queryError"
          class="mt-4"
          tone="danger"
          title="评分总览加载失败"
          :description="queryError"
        >
          <template #action>
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="refetch()">重新加载</button>
          </template>
        </StatePanel>

        <StatePanel
          v-else-if="!isFetching && !shortlist.length"
          class="mt-4"
          tone="warning"
          title="当前还没有可展示的短名单"
          :description="shortlistEmptyDescription"
        >
          <template #action>
            <RouterLink class="rounded-2xl bg-[var(--brand)] px-4 py-2 font-semibold text-white" to="/app/stocks/scores">去看股票评分</RouterLink>
          </template>
        </StatePanel>

        <StatePanel
          v-else-if="degradedSources.length"
          class="mt-4"
          tone="warning"
          title="评分总览已降级展示"
          :description="`以下数据源当前缺失或未准备好：${degradedSources.join('、')}。页面仍会继续展示已有评分结果。`"
        >
          <template #action>
            <RouterLink class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 font-semibold text-[var(--ink)]" to="/app/decision">去看执行视角</RouterLink>
          </template>
        </StatePanel>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="市场模式" :value="macroRegime.label || '证据不足'" :hint="macroRegimeHint" />
        <StatCard title="市场总分" :value="formatNumber(macroRegime.score, 1)" :hint="snapshotDateLabel" />
        <StatCard title="行业榜" :value="String(industryScores.length || 0)" :hint="`Top 行业 ${topIndustry || '-'}`" />
        <StatCard title="自动短名单" :value="String(shortlist.length || 0)" :hint="`理由包 ${reasonPacketCount}`" />
      </div>

      <div class="grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
        <PageSection title="宏观评分卡" subtitle="把全局评分快照翻译成可以立即理解的市场模式。">
          <InfoCard :title="macroRegime.label || '证据不足'" :meta="`总分 ${formatNumber(macroRegime.score, 1)} · 模式 ${macroRegime.mode || '-'}`" :description="macroRegimeDescription">
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <span v-for="item in macroRegime.factors || []" :key="item" class="metric-chip">{{ item }}</span>
            </div>
          </InfoCard>
        </PageSection>

        <PageSection title="行业评分榜" subtitle="保留行业上下文，避免只盯单票分数。">
          <DataTable :columns="industryColumns" :rows="industryScores" row-key="industry" :empty-text="industryEmptyText" caption="行业评分榜">
            <template #cell-score="{ row }">{{ formatNumber(row.score, 2) }}</template>
            <template #cell-top_stocks="{ row }">
              <div class="space-y-1">
                <div v-for="stock in row.top_stocks || []" :key="stock.ts_code" class="text-xs text-[var(--muted)]">
                  {{ stock.name || stock.ts_code || '-' }} · {{ formatNumber(stock.score, 1) }}
                </div>
              </div>
            </template>
          </DataTable>
        </PageSection>
      </div>

      <PageSection title="自动短名单" subtitle="优先展示高分样本，并给出建议动作和下钻入口。">
        <DataTable :columns="shortlistColumns" :rows="shortlist" row-key="ts_code" empty-text="暂无自动短名单" caption="自动短名单">
          <template #cell-ts_code="{ row }">
            <RouterLink :to="`/app/stocks/detail/${row.ts_code}`" class="font-semibold text-[var(--brand)] hover:underline">
              {{ row.name || row.ts_code || '-' }}
            </RouterLink>
            <div class="mt-1 text-xs text-[var(--muted)]">{{ row.ts_code || '-' }}</div>
          </template>
          <template #cell-total_score="{ row }">{{ formatNumber(row.total_score, 2) }}</template>
          <template #cell-industry_total_score="{ row }">{{ formatNumber(row.industry_total_score, 2) }}</template>
          <template #cell-position_label="{ row }"><StatusBadge :value="row.position_label || 'muted'" :label="row.position_label || '-'"/></template>
          <template #cell_actions="{ row }">
            <div class="flex flex-wrap gap-2">
              <RouterLink :to="`/app/stocks/detail/${row.ts_code}`" class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]">
                股票详情
              </RouterLink>
              <RouterLink :to="`/app/decision?ts_code=${encodeURIComponent(row.ts_code || '')}`" class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]">
                执行视角
              </RouterLink>
            </div>
          </template>
        </DataTable>
      </PageSection>

      <PageSection title="入选理由" subtitle="按评分、新闻、信号、候选池拆分来源，减少“只看结果不知原因”。">
        <div v-if="shortlist.length" class="grid gap-3 xl:grid-cols-2">
          <InfoCard
            v-for="item in shortlist"
            :key="item.ts_code"
            :title="`${item.name || item.ts_code || '-'} · ${item.industry || '未知行业'}`"
            :meta="`${item.ts_code || '-'} · ${resolvePacket(item.ts_code).status === 'degraded' ? '降级展示' : '完整展示'}`"
            :description="resolveScoreReason(item.ts_code).decision_reason || item.decision_reason || '暂无结构化理由。'"
          >
            <template #badge>
              <StatusBadge :value="item.position_label || 'muted'" :label="item.position_label || '-'"/>
            </template>
            <div class="mt-3 grid gap-3 md:grid-cols-2">
              <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
                <div class="font-semibold text-[var(--ink)]">评分</div>
                <div class="mt-2">总分 {{ formatNumber(resolveScoreReason(item.ts_code).total_score, 1) }} · 行业内 {{ formatNumber(resolveScoreReason(item.ts_code).industry_total_score, 1) }}</div>
                <div class="mt-1">等级 {{ resolveScoreReason(item.ts_code).score_grade || '-' }} · {{ resolveScoreReason(item.ts_code).position_label || '-' }}</div>
                <div v-if="resolveScoreReason(item.ts_code).summary_points?.length" class="mt-2 flex flex-wrap gap-2 text-xs">
                  <span v-for="point in resolveScoreReason(item.ts_code).summary_points || []" :key="point" class="metric-chip">{{ point }}</span>
                </div>
              </div>
              <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
                <div class="font-semibold text-[var(--ink)]">新闻</div>
                <div class="mt-2">总数 {{ resolvePacket(item.ts_code).news?.count || 0 }} · 高重要 {{ resolvePacket(item.ts_code).news?.high_importance_count || 0 }}</div>
                <div class="mt-1">最近 {{ resolvePacket(item.ts_code).news?.latest_pub_time || '-' }}</div>
                <div class="mt-2 space-y-1 text-xs">
                  <div v-for="news in resolvePacket(item.ts_code).news?.items || []" :key="news.title">{{ news.title }}</div>
                </div>
              </div>
              <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
                <div class="font-semibold text-[var(--ink)]">信号</div>
                <div class="mt-2">命中 {{ resolvePacket(item.ts_code).signals?.count || 0 }} · 最近 {{ resolvePacket(item.ts_code).signals?.latest_signal_date || '-' }}</div>
                <div class="mt-1">{{ (resolvePacket(item.ts_code).signals?.directions || []).join(' / ') || '暂无方向标签' }}</div>
                <div class="mt-2 space-y-1 text-xs">
                  <div v-for="signal in resolvePacket(item.ts_code).signals?.items || []" :key="signal.signal_key">{{ signal.subject_name || signal.signal_key || '-' }} · {{ signal.signal_status || '-' }}</div>
                </div>
              </div>
              <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
                <div class="font-semibold text-[var(--ink)]">候选池</div>
                <div class="mt-2">匹配 {{ resolvePacket(item.ts_code).candidate_pool?.matched_count || 0 }} · 偏向 {{ resolvePacket(item.ts_code).candidate_pool?.dominant_bias || '-' }}</div>
                <div class="mt-1">提及 {{ resolvePacket(item.ts_code).candidate_pool?.mention_count || 0 }} · 群数 {{ resolvePacket(item.ts_code).candidate_pool?.room_count || 0 }}</div>
                <div class="mt-1">最近 {{ resolvePacket(item.ts_code).candidate_pool?.latest_analysis_date || '-' }}</div>
              </div>
            </div>
          </InfoCard>
        </div>
        <div v-else class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
          {{ reasonPacketEmptyText }}
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchDecisionScoreboard } from '../../services/api/decision'
import { formatNumber } from '../../shared/utils/format'
import type { DecisionScoreboardPayload, ReasonPacket } from '../../shared/types/api'

const industryColumns = [
  { key: 'industry', label: '行业' },
  { key: 'score', label: '行业分' },
  { key: 'count', label: '样本数' },
  { key: 'top_stocks', label: 'Top 股票' },
]

const shortlistColumns = [
  { key: 'ts_code', label: '股票' },
  { key: 'industry', label: '行业' },
  { key: 'total_score', label: '总分' },
  { key: 'industry_total_score', label: '行业分' },
  { key: 'position_label', label: '建议动作' },
  { key: 'actions', label: '跳转' },
]

const { data, isFetching, error, refetch } = useQuery({
  queryKey: ['decision-scoreboard'],
  queryFn: () => fetchDecisionScoreboard({ page_size: 8 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
})

const payload = computed<DecisionScoreboardPayload>(() => data.value || {
  generated_at: '',
  snapshot_date: '',
  macro_regime: {},
  industry_scores: [],
  stock_shortlist: [],
  reason_packets: {},
  source_health: {},
})
const macroRegime = computed(() => payload.value.macro_regime || {})
const industryScores = computed(() => payload.value.industry_scores || [])
const shortlist = computed(() => payload.value.stock_shortlist || [])
const queryError = computed(() => error.value?.message || '')
const degradedSources = computed(() =>
  Object.entries(payload.value.source_health || {})
    .filter(([, status]) => status && status !== 'ok')
    .map(([source]) => source),
)
const topIndustry = computed(() => industryScores.value[0]?.industry || '')
const snapshotDateLabel = computed(() => `快照 ${payload.value.snapshot_date || '-'}`)
const reasonPacketCount = computed(() => Object.keys(payload.value.reason_packets || {}).length)
const shortlistEmptyDescription = computed(() => {
  if (degradedSources.value.length) return `当前短名单为空，主要因为这些输入缺失或未准备好：${degradedSources.value.join('、')}。`
  return '当前没有命中短名单，可能是无信号、无评分、过滤条件过严，或最新快照尚未生成。'
})
const macroRegimeHint = computed(() => {
  if (macroRegime.value?.mode) return `模式 ${macroRegime.value.mode}`
  if (degradedSources.value.length) return `缺口 ${degradedSources.value.slice(0, 2).join(' / ')}`
  return '等待模式快照'
})
const macroRegimeDescription = computed(() => {
  if (macroRegime.value?.summary) return macroRegime.value.summary
  if (degradedSources.value.length) return `当前无法形成稳定市场模式摘要，缺失来源：${degradedSources.value.join('、')}。`
  return '暂无市场模式摘要，需确认评分快照、宏观模式与行业评分是否已生成。'
})
const industryEmptyText = computed(() => {
  if (degradedSources.value.includes('industry_scores')) return '行业评分缺失，当前无法生成行业排序。'
  return '暂无行业评分数据'
})
const reasonPacketEmptyText = computed(() => {
  if (!shortlist.value.length) return '当前没有短名单，因此没有对应理由包。'
  if (degradedSources.value.length) return `理由包暂未生成，可能缺失来源：${degradedSources.value.join('、')}。`
  return '当前还没有理由包可展示，可能尚未生成或未命中当前短名单。'
})

function resolvePacket(tsCode: string): ReasonPacket {
  return (payload.value.reason_packets || {})[tsCode] || { ts_code: tsCode, status: 'empty' }
}

function resolveScoreReason(tsCode: string) {
  return resolvePacket(tsCode).score || {}
}
</script>
