<template>
  <AppShell title="LLM 股票走势分析" subtitle="基于近段日线特征，让模型输出趋势判断、风险点和观察重点。">
    <div class="space-y-4">
      <PageSection title="分析发起" subtitle="输入股票代码和回看天数，模型由后端自动路由，默认 GPT-5.4 优先，失败自动降级。">
        <div class="grid gap-3 xl:grid-cols-[180px_120px_120px] md:grid-cols-2">
          <input v-model="form.ts_code" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="股票代码，如 000001.SZ" />
          <input v-model.number="form.lookback" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="回看天数" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-50" :disabled="isPending || quotaBlocked" @click="runAnalysis">
            {{ isPending ? '分析中...' : '分析' }}
          </button>
        </div>
        <div v-if="quotaText" class="mt-3 text-sm text-[var(--muted)]">{{ quotaText }}</div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <PageSection title="分析结论" subtitle="保留 Markdown 原文，方便继续阅读与复制。">
          <template #action>
            <div class="flex flex-wrap gap-2">
              <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white" :disabled="!result" @click="downloadMarkdown">下载 Markdown</button>
              <button class="rounded-2xl bg-blue-700 px-4 py-2 text-white" :disabled="!result" @click="downloadImage">下载图片</button>
            </div>
          </template>
          <div ref="analysisExportRef">
            <div class="mb-3 text-sm text-[var(--muted)]">实际模型：{{ actualModel }}</div>
            <div v-if="attemptChain" class="mb-3 text-sm text-[var(--muted)]">尝试链路：{{ attemptChain }}</div>
            <div v-if="dataDimensionTags.length" class="mb-3 flex flex-wrap gap-2">
              <span v-for="tag in dataDimensionTags" :key="tag" class="metric-chip">{{ tag }}</span>
            </div>
            <MarkdownBlock :content="analysisText" />
          </div>
        </PageSection>
        <PageSection title="特征摘要" subtitle="展示参与本次分析的核心日线特征。">
          <MetricGrid :items="featureItems" columns-class="xl:grid-cols-1 md:grid-cols-1" empty-text="尚未拿到特征数据" />
        </PageSection>
      </div>

      <PageSection title="逻辑链路" subtitle="如果后端返回了逻辑视图，就优先结构化展示。">
        <div v-if="logicViewItems.length" class="space-y-2">
          <InfoCard v-for="item in logicViewItems" :key="item.title" :title="item.title" :description="item.description" />
        </div>
        <div v-else class="rounded-[20px] border border-dashed border-[var(--line)] bg-[rgba(255,255,255,0.52)] px-4 py-10 text-center text-sm text-[var(--muted)]">
          当前结果未返回稳定的结构化逻辑链路。
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, nextTick, reactive, ref } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import MetricGrid from '../../shared/ui/MetricGrid.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { triggerTrendAnalysis } from '../../services/api/stocks'
import { fetchAuthStatus } from '../../services/api/auth'
import { formatNumber } from '../../shared/utils/format'
import { downloadElementAsImage, downloadTextFile } from '../../shared/utils/export'

const form = reactive({
  ts_code: '000001.SZ',
  lookback: 120,
})

const result = ref<Record<string, any> | null>(null)
const analysisExportRef = ref<HTMLElement | null>(null)
const { data: authStatus, refetch: refetchAuthStatus } = useQuery({
  queryKey: ['auth-status-trend-page'],
  queryFn: () => fetchAuthStatus(),
})
const quotaText = computed(() => {
  const quota = authStatus.value?.trend_quota
  if (!quota || quota.limit == null) return ''
  return `今日剩余次数：${quota.remaining} / ${quota.limit}`
})
const quotaBlocked = computed(() => {
  const quota = authStatus.value?.trend_quota
  return !!(quota && quota.limit != null && Number(quota.remaining || 0) <= 0)
})
const analysisText = computed(() => result.value?.analysis || '（点击“分析”开始）')
const actualModel = computed(() => result.value?.used_model || result.value?.model || '待执行')
const attemptChain = computed(() =>
  Array.isArray(result.value?.attempts)
    ? result.value.attempts
        .map((item: Record<string, any>) => `${item.model || '-'}${item.error ? '×' : '√'}`)
        .join(' -> ')
    : '',
)
const features = computed<Record<string, any>>(() => (result.value?.features || {}) as Record<string, any>)
const trendMetrics = computed<Record<string, any>>(() => (features.value.trend_metrics || {}) as Record<string, any>)
const latest = computed<Record<string, any>>(() => (features.value.latest || {}) as Record<string, any>)
const dataDimensionTags = computed(() => {
  const tags = ['日线价格', '趋势指标', '均线结构', '波动率']
  if (latest.value.trade_date) tags.push(`最新交易日 ${latest.value.trade_date}`)
  if (Number(features.value.samples || 0) > 0) tags.push(`样本 ${features.value.samples} 条`)
  return tags
})

const featureItems = computed(() => [
  { label: '股票', value: features.value.name || form.ts_code, hint: `样本数 ${features.value.samples || 0}` },
  { label: '区间', value: `${features.value.date_range?.start || '-'} ~ ${features.value.date_range?.end || '-'}`, hint: latest.value.trade_date ? `最新交易日 ${latest.value.trade_date}` : '-' },
  { label: '最新收盘', value: formatNumber(latest.value.close, 3), hint: `最新涨跌 ${formatNumber(latest.value.pct_chg, 2)}%` },
  { label: '区间收益', value: `${formatNumber(trendMetrics.value.total_return_pct, 2)}%`, hint: `年化波动 ${formatNumber(trendMetrics.value.annualized_volatility_pct, 2)}%` },
  { label: 'MA5/10/20', value: `${formatNumber(trendMetrics.value.ma5, 2)} / ${formatNumber(trendMetrics.value.ma10, 2)} / ${formatNumber(trendMetrics.value.ma20, 2)}`, hint: `MA60 ${formatNumber(trendMetrics.value.ma60, 2)}` },
  { label: '距 MA20', value: `${formatNumber(trendMetrics.value.distance_to_ma20_pct, 2)}%`, hint: `平均日涨跌 ${formatNumber(trendMetrics.value.avg_daily_pct_chg, 2)}%` },
  { label: '平均成交量', value: formatNumber(trendMetrics.value.avg_volume, 0), hint: '' },
].filter((item) => item.value || item.hint))

const logicViewItems = computed(() => {
  const logic = result.value?.logic_view
  if (!logic || typeof logic !== 'object') return []
  const chains = Array.isArray(logic.chains) ? logic.chains : []
  const summary = logic.summary || {}
  const summaryItems = [
    summary.conclusion ? { title: '核心结论', description: summary.conclusion } : null,
    summary.focus ? { title: '关注方向', description: summary.focus } : null,
    summary.risk ? { title: '风险提示', description: summary.risk } : null,
  ].filter(Boolean) as Array<{ title: string; description: string }>
  const chainItems = chains.map((item: Record<string, any>, index: number) => ({
    title: item.title || `链路 ${index + 1}`,
    description: Array.isArray(item.nodes) ? item.nodes.join(' -> ') : (item.raw || ''),
  }))
  return [...summaryItems, ...chainItems]
})

const mutation = useMutation({
  mutationFn: () => triggerTrendAnalysis({
    ts_code: form.ts_code.trim().toUpperCase(),
    lookback: form.lookback,
  }),
  onSuccess: (payload: Record<string, any>) => {
    result.value = payload
    refetchAuthStatus()
  },
  onError: (error: Error) => {
    result.value = { analysis: `分析失败：${error.message}` }
  },
})

const isPending = computed(() => mutation.isPending.value)

function runAnalysis() {
  mutation.mutate()
}

function downloadMarkdown() {
  if (!result.value) return
  const tsCode = form.ts_code.trim().toUpperCase() || 'stock'
  downloadTextFile(analysisText.value, `${tsCode}_LLM走势分析.md`, 'text/markdown;charset=utf-8')
}

async function downloadImage() {
  if (!analysisExportRef.value || !result.value) return
  await nextTick()
  const tsCode = form.ts_code.trim().toUpperCase() || 'stock'
  await downloadElementAsImage(analysisExportRef.value, `${tsCode}_LLM走势分析.png`)
}
</script>
