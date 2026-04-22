<template>
  <AppShell title="荐股人详情" subtitle="查看单个荐股人的方向记录、命中率和分布群聊。">
    <div class="space-y-4">
      <PageSection title="基础信息" subtitle="荐股人 30 日准确率与统计。">
        <InfoCard :title="senderName || '未知荐股人'" :meta="metaText" :description="accuracyText">
          <div class="mt-3 grid gap-3 md:grid-cols-3">
            <div class="metric-chip">总信号 <strong>{{ summary.total ?? 0 }}</strong></div>
            <div class="metric-chip">已校验 <strong>{{ summary.evaluated ?? 0 }}</strong></div>
            <div class="metric-chip">待校验 <strong>{{ summary.pending ?? 0 }}</strong></div>
            <div class="metric-chip">命中 <strong>{{ summary.hit ?? 0 }}</strong></div>
            <div class="metric-chip">失误 <strong>{{ summary.miss ?? 0 }}</strong></div>
            <div class="metric-chip">平盘 <strong>{{ summary.flat ?? 0 }}</strong></div>
          </div>
        </InfoCard>
      </PageSection>

      <PageSection title="分布群聊" subtitle="该荐股人在哪些群发过方向信号。">
        <div class="grid gap-3 xl:grid-cols-2">
          <InfoCard v-for="item in rooms" :key="`${item.room_id}-${item.talker}`" :title="item.talker || item.room_id" :meta="item.room_id || ''">
            <div class="mt-2 text-sm text-[var(--muted)]">
              信号 {{ item.signal_count }} 条 · 命中 {{ item.hit_count }} 条 · 命中率 {{ fmtPct(item.hit_rate) }}
            </div>
          </InfoCard>
        </div>
      </PageSection>

      <PageSection title="方向记录" subtitle="按时间倒序展示每条荐股方向。">
        <div class="space-y-2">
          <InfoCard
            v-for="item in records"
            :key="item.id"
            :title="`${item.stock_name || item.ts_code} · ${item.direction}`"
            :meta="`${item.signal_date || '-'} · ${item.ts_code || '-'} · ${item.talker || item.room_id || '-'}`"
            :description="item.source_content || ''"
          >
            <div class="mt-2 text-sm text-[var(--muted)]">
              状态 {{ item.validation_status || '-' }} · 结果 {{ item.verdict || '-' }} · 次日收益 {{ fmtReturn(item.return_1d) }}
            </div>
          </InfoCard>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchChatroomSenderDetail } from '../../services/api/chatrooms'

const route = useRoute()
const senderName = computed(() => String(route.query.sender_name || '').trim())

const { data: detail } = useQuery({
  queryKey: computed(() => ['chatroom-sender-detail', senderName.value]),
  queryFn: () => fetchChatroomSenderDetail({ sender_name: senderName.value, page: 1, page_size: 120 }),
})

const summary = computed(() => detail.value?.summary || {})
const rooms = computed(() => detail.value?.rooms || [])
const records = computed(() => detail.value?.items || [])
const accuracy = computed(() => detail.value?.accuracy || {})

const metaText = computed(() => {
  const parts = []
  if (accuracy.value.as_of_date) parts.push(`截至 ${accuracy.value.as_of_date}`)
  if (accuracy.value.accuracy_label) parts.push(accuracy.value.accuracy_label)
  return parts.join(' · ')
})
const accuracyText = computed(() => {
  const sample = Number(accuracy.value.sample_size || 0)
  const hitRate = Number(accuracy.value.hit_rate || 0)
  if (!sample) return '30 日准确率暂无样本'
  return `30 日准确率 ${fmtPct(hitRate)} (n=${sample}) · 命中 ${accuracy.value.hit_count || 0}`
})

function fmtPct(value: unknown) {
  const n = Number(value || 0)
  if (!Number.isFinite(n)) return '-'
  return `${(n * 100).toFixed(1)}%`
}

function fmtReturn(value: unknown) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '-'
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}%`
}
</script>
