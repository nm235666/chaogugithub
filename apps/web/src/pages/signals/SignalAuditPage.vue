<template>
  <AppShell title="信号质量审计" subtitle="专门检查投资信号里的弱信号、误映射和仅主题传导问题。">
    <div class="space-y-4">
      <PageSection title="口径切换" subtitle="支持近 7 天累计和最近 1 天两种审计视角。">
        <div class="flex flex-wrap gap-3">
          <button class="rounded-2xl px-4 py-3 font-semibold text-white" :class="scope === '7d' ? 'bg-[var(--brand)]' : 'bg-stone-800'" @click="scope = '7d'">近 7 天累计</button>
          <button class="rounded-2xl px-4 py-3 font-semibold text-white" :class="scope === '1d' ? 'bg-[var(--brand)]' : 'bg-stone-800'" @click="scope = '1d'">最近 1 天</button>
        </div>
      </PageSection>

      <PageSection title="审计汇总" subtitle="先看质量分和几个最关键的风险计数。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <StatCard title="信号总数" :value="summary.signal_total ?? 0" :hint="`股票 ${summary.stock_total ?? 0} · 活跃 ${summary.active_total ?? 0}`" />
          <StatCard title="群聊股票信号" :value="summary.chatroom_stock_total ?? 0" :hint="`仅主题传导 ${summary.theme_only_stock_total ?? 0}`" />
          <StatCard title="代码名股票" :value="summary.code_named_stock_total ?? 0" :hint="`缺失 ts_code ${summary.missing_ts_stock_total ?? 0}`" />
          <StatCard title="质量分" :value="stats.quality_score ?? '-'" :hint="stats.scope_label || '-'" />
        </div>
      </PageSection>

      <PageSection v-for="section in sections" :key="section.key" :title="section.title" :subtitle="section.desc">
        <DataTable :columns="columns" :rows="section.items" row-key="signal_key" empty-text="当前没有这一类问题">
          <template #cell-display_name="{ row }">
            <div>
              <div class="font-semibold text-[var(--ink)]">{{ row.display_name || row.subject_name || '-' }}</div>
              <div class="mt-1 flex flex-wrap gap-2 text-xs">
                <RouterLink class="text-[var(--brand)]" :to="`/signals/overview?keyword=${encodeURIComponent(row.ts_code || row.display_name || row.subject_name || '')}`">查看信号详情</RouterLink>
                <RouterLink class="text-[var(--brand)]" :to="`/signals/timeline?entity_name=${encodeURIComponent(row.display_name || row.subject_name || '')}&scope=${scope}`">查看时间线</RouterLink>
              </div>
            </div>
          </template>
          <template #cell-ts_code="{ row }">{{ row.ts_code || '-' }}</template>
          <template #cell-direction="{ row }"><StatusBadge :value="row.direction" :label="row.direction || '-'" /></template>
          <template #cell-signal_strength="{ row }">{{ Number(row.signal_strength || 0).toFixed(2) }}</template>
          <template #cell-confidence="{ row }">{{ Number(row.confidence || 0).toFixed(2) }}%</template>
          <template #cell-dominant_source="{ row }">{{ row.dominant_source || '-' }}</template>
          <template #cell-top_evidence="{ row }">{{ row.top_evidence || '-' }}</template>
          <template #cell-latest_signal_date="{ row }">{{ row.latest_signal_date || '-' }}</template>
        </DataTable>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchSignalAudit } from '../../services/api/signals'

const scope = ref<'7d' | '1d'>('7d')

const { data: result } = useQuery({
  queryKey: ['signal-audit', scope],
  queryFn: () => fetchSignalAudit({ scope: scope.value }),
})

const summary = computed(() => result.value?.summary || {})
const stats = computed(() => result.value?.stats || {})
const sections = computed(() => {
  const raw = result.value?.sections || {}
  return [
    { key: 'code_named', title: '代码名股票', desc: '股票主体直接显示成代码名，影响可读性。', items: raw.code_named_stocks || [] },
    { key: 'missing_ts', title: '缺失 ts_code 的股票信号', desc: '股票信号没有标准代码，后续无法稳定关联。', items: raw.missing_ts_stock || [] },
    { key: 'theme_only', title: '仅主题传导股票', desc: '只有主题映射，没有新闻或群聊直接证据。', items: raw.theme_only_stocks || [] },
    { key: 'low_conf', title: '低置信活跃信号', desc: '状态活跃但置信度偏低，需要重点复核。', items: raw.low_conf_active || [] },
    { key: 'weak_chatroom', title: '弱群聊股票信号', desc: '群聊零散提及，尚未形成稳定共识。', items: raw.weak_chatroom_stocks || [] },
    { key: 'no_direct', title: '无直接来源股票', desc: '没有新闻、个股新闻、群聊直接证据。', items: raw.no_direct_source_stocks || [] },
  ]
})

const columns = [
  { key: 'display_name', label: '股票' },
  { key: 'ts_code', label: '代码' },
  { key: 'direction', label: '方向' },
  { key: 'signal_strength', label: '强度' },
  { key: 'confidence', label: '置信度' },
  { key: 'dominant_source', label: '主来源' },
  { key: 'top_evidence', label: '证据摘要' },
  { key: 'latest_signal_date', label: '最近时间' },
]
</script>
