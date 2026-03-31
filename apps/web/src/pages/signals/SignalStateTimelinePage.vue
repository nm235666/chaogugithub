<template>
  <AppShell title="状态机时间线" subtitle="查看主题或股票信号在初始、强化、弱化、证伪、反转之间的演化。">
    <div class="space-y-4">
      <PageSection title="查询条件" subtitle="输入 signal_key 或按 scope 辅助识别。">
        <div class="grid gap-3 xl:grid-cols-[140px_1fr_120px] md:grid-cols-2">
          <select v-model="filters.signal_scope" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">自动识别</option>
            <option value="theme">主题</option>
            <option value="stock">股票</option>
          </select>
          <input v-model="filters.signal_key" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="例如 theme:黄金 或 stock:000001.SZ" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">查询</button>
        </div>
      </PageSection>

      <PageSection title="当前状态概览" subtitle="先看主体、方向、状态、强度和置信度。">
        <div class="grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <StatCard title="主体" :value="signal.subject_name || '-'" :hint="signal.signal_key || '-'" />
          <StatCard title="方向" :value="signal.direction || '-'" :hint="signal.signal_scope || '-'" />
          <StatCard title="当前状态" :value="signal.current_state || '-'" :hint="`来源 ${signal.dominant_source || '-'}`" />
          <StatCard title="强度" :value="Number(signal.signal_strength || 0).toFixed(1)" hint="当前综合强度" />
          <StatCard title="置信度" :value="`${Number(signal.confidence || 0).toFixed(1)}%`" hint="当前信号置信度" />
        </div>
      </PageSection>

      <PageSection v-if="marketExpectations.length" title="市场预期层" subtitle="与该主题直接相关的预期市场。">
        <div class="space-y-2">
          <InfoCard
            v-for="item in marketExpectations"
            :key="item.question"
            :title="item.question || '-'" :meta="`成交量 ${Number(item.volume || 0).toFixed(0)} · 流动性 ${Number(item.liquidity || 0).toFixed(0)}`"
          />
        </div>
      </PageSection>

      <PageSection :title="`状态事件 (${result?.total || 0})`" subtitle="按时间顺序回放状态迁移。">
        <div class="space-y-2">
          <InfoCard
            v-for="item in events"
            :key="`${item.event_time}-${item.event_type}-${item.driver_title}`"
            :title="item.new_state || item.event_type || '-'"
            :meta="joinParts([item.event_time, item.driver_type, item.driver_title])"
            :description="item.event_summary || ''"
          >
            <template #badge>
              <StatusBadge :value="item.new_direction || 'muted'" :label="item.new_direction || '-'" />
            </template>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">强度 <strong>{{ Number(item.old_strength || 0).toFixed(1) }} -> {{ Number(item.new_strength || 0).toFixed(1) }}</strong></span>
              <span class="metric-chip">置信度 <strong>{{ Number(item.old_confidence || 0).toFixed(1) }} -> {{ Number(item.new_confidence || 0).toFixed(1) }}</strong></span>
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

      <PageSection title="导出" subtitle="先支持 Markdown 导出，方便把状态时间线留档。">
        <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" :disabled="!signal.signal_key" @click="downloadMarkdown">下载 Markdown</button>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchSignalStateTimeline } from '../../services/api/signals'

function joinParts(parts: Array<unknown>) {
  return parts.map((item) => String(item ?? '').trim()).filter(Boolean).join(' · ')
}

function downloadText(content: string, filename: string) {
  const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

const filters = reactive({
  signal_scope: '',
  signal_key: '',
  page: 1,
  page_size: 20,
})
const route = useRoute()

watch(
  () => route.query,
  (query) => {
    const scope = String(query.signal_scope || '').trim()
    const key = String(query.signal_key || '').trim()
    if (scope) filters.signal_scope = scope
    if (key) {
      filters.signal_key = key
      filters.page = 1
      return
    }
    const legacyName = String(query.entity_name || '').trim()
    if (legacyName) {
      filters.signal_scope = filters.signal_scope || 'theme'
      filters.signal_key = `theme:${legacyName}`
      filters.page = 1
    }
  },
  { immediate: true, deep: true },
)

const { data: result } = useQuery({
  queryKey: ['signal-state-timeline', filters],
  queryFn: () => fetchSignalStateTimeline(filters),
  enabled: computed(() => !!filters.signal_key),
})

const signal = computed(() => result.value?.signal || {})
const events = computed(() => result.value?.events || [])
const marketExpectations = computed(() => result.value?.market_expectations || [])

function buildMarkdown() {
  const lines = [
    '# 状态机时间线',
    '',
    `- 主体: ${signal.value.subject_name || '-'}`,
    `- scope: ${signal.value.signal_scope || filters.signal_scope || '-'}`,
    `- signal_key: ${signal.value.signal_key || filters.signal_key || '-'}`,
    `- 当前方向: ${signal.value.direction || '-'}`,
    `- 当前状态: ${signal.value.current_state || '-'}`,
    `- 强度: ${Number(signal.value.signal_strength || 0).toFixed(1)}`,
    `- 置信度: ${Number(signal.value.confidence || 0).toFixed(1)}%`,
    '',
  ]
  if (marketExpectations.value.length) {
    lines.push('## 市场预期层', '')
    marketExpectations.value.forEach((item: Record<string, any>, index: number) => {
      lines.push(`${index + 1}. ${item.question || '-'} | 成交量 ${Number(item.volume || 0).toFixed(0)} | 流动性 ${Number(item.liquidity || 0).toFixed(0)}`)
    })
    lines.push('')
  }
  lines.push('## 状态事件', '')
  events.value.forEach((item: Record<string, any>, index: number) => {
    lines.push(`### ${index + 1}. ${item.event_time || '-'} ${item.new_state || item.event_type || '-'}`)
    lines.push(`- 新方向: ${item.new_direction || '-'}`)
    lines.push(`- 事件摘要: ${item.event_summary || '-'}`)
    lines.push(`- 强度变化: ${Number(item.old_strength || 0).toFixed(1)} -> ${Number(item.new_strength || 0).toFixed(1)}`)
    lines.push(`- 置信度变化: ${Number(item.old_confidence || 0).toFixed(1)} -> ${Number(item.new_confidence || 0).toFixed(1)}`)
    lines.push(`- 驱动: ${item.driver_type || '-'} / ${item.driver_title || '-'}`)
    lines.push('')
  })
  return lines.join('\n')
}

function downloadMarkdown() {
  const key = String(signal.value.signal_key || filters.signal_key || 'signal')
    .replace(/[^\w\u4e00-\u9fa5:-]+/g, '_')
    .replaceAll(':', '_')
  downloadText(buildMarkdown(), `${key}_状态时间线.md`)
}
</script>
