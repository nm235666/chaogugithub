<template>
  <AppShell title="群聊详情" subtitle="查看单个群聊的荐股方向、命中与回查证据。">
    <div class="space-y-4">
      <PageSection title="基础信息" subtitle="群聊信息与 30 日准确率。">
        <InfoCard :title="roomTitle" :meta="roomMeta" :description="accuracyText">
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

      <PageSection title="常见标的" subtitle="该群提及并形成方向判断最多的股票。">
        <div class="grid gap-3 xl:grid-cols-2">
          <InfoCard v-for="item in topStocks" :key="item.ts_code" :title="item.stock_name || item.ts_code" :meta="item.ts_code">
            <div class="mt-2 text-sm text-[var(--muted)]">
              信号 {{ item.signal_count }} 条 · 命中 {{ item.hit_count }} 条 · 命中率 {{ fmtPct(item.hit_rate) }}
            </div>
          </InfoCard>
        </div>
      </PageSection>

      <PageSection title="方向记录" subtitle="按时间倒序展示每条荐股方向记录。">
        <div class="space-y-2">
          <InfoCard
            v-for="item in records"
            :key="item.id"
            :title="`${item.stock_name || item.ts_code} · ${item.direction}`"
            :meta="`${item.signal_date || '-'} · ${item.ts_code || '-'} · ${item.sender_name || '未知发送人'}`"
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
import { fetchChatroomRoomDetail } from '../../services/api/chatrooms'

const route = useRoute()
const roomId = computed(() => String(route.query.room_id || '').trim())
const talker = computed(() => String(route.query.talker || '').trim())

const { data: detail } = useQuery({
  queryKey: computed(() => ['chatroom-room-detail', roomId.value, talker.value]),
  queryFn: () => fetchChatroomRoomDetail({ room_id: roomId.value, talker: talker.value, page: 1, page_size: 80 }),
})

const room = computed(() => detail.value?.room || {})
const summary = computed(() => detail.value?.summary || {})
const topStocks = computed(() => detail.value?.top_stocks || [])
const records = computed(() => detail.value?.items || [])
const accuracy = computed(() => detail.value?.accuracy || {})

const roomTitle = computed(() => String(room.value.talker || room.value.nick_name || room.value.room_id || '未命名群聊'))
const roomMeta = computed(() => {
  const parts = [String(room.value.room_id || '').trim()]
  if (room.value.activity_level) parts.push(`活跃 ${room.value.activity_level}`)
  if (room.value.risk_level) parts.push(`风险 ${room.value.risk_level}`)
  return parts.filter(Boolean).join(' · ')
})
const accuracyText = computed(() => {
  const sample = Number(accuracy.value.sample_size || 0)
  const hitRate = Number(accuracy.value.hit_rate || 0)
  if (!sample) return '30 日准确率暂无样本'
  return `30 日准确率 ${fmtPct(hitRate)} (n=${sample}) · ${accuracy.value.accuracy_label || '未标注'}`
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
