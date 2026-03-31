<template>
  <AppShell title="信号时间线" subtitle="看主题或股票信号如何从初始走到强化、弱化、证伪或反转。">
    <div class="space-y-4">
      <PageSection title="时间线查询" subtitle="按 signal_key 查看完整时间线，例如 theme:黄金 / stock:000001.SZ。">
        <div class="grid gap-3 xl:grid-cols-[1fr_140px_120px] md:grid-cols-2">
          <input v-model="filters.signal_key" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="signal_key，如 theme:黄金 或 stock:000001.SZ" />
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">刷新</button>
        </div>
      </PageSection>

      <PageSection title="时间线结果" subtitle="这里优先保证结构完整，后续再接图形化时间轴。">
        <div v-if="filters.signal_key && !events.length" class="mb-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          当前 signal_key 暂无事件记录：<strong>{{ filters.signal_key }}</strong>。可切换到“状态时间线”查看状态机事件。
        </div>
        <div class="space-y-2">
          <InfoCard
            v-for="item in events"
            :key="`${item.id}-${item.event_time || item.event_date}`"
            :title="item.driver_title || item.event_type || '-'"
            :meta="`${item.event_time || item.event_date || '-'} · ${item.new_state || '-'} · ${item.new_direction || '-'}`"
            :description="item.event_summary || item.reason || ''"
          >
            <template #badge>
              <StatusBadge :value="item.new_state || item.new_direction || 'muted'" :label="item.new_state || item.new_direction || '-'" />
            </template>
          </InfoCard>
        </div>
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
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchSignalTimeline } from '../../services/api/signals'

const route = useRoute()
const filters = reactive({ signal_key: '', page: 1, page_size: 20 })

watch(
  () => route.query,
  (query) => {
    const signalKey = String(query.signal_key || '').trim()
    if (signalKey) {
      filters.signal_key = signalKey
      filters.page = 1
      return
    }
    const legacyName = String(query.entity_name || '').trim()
    if (legacyName) {
      filters.signal_key = `theme:${legacyName}`
      filters.page = 1
    }
  },
  { immediate: true, deep: true },
)

const { data: result } = useQuery({
  queryKey: ['signal-timeline', filters],
  queryFn: () => fetchSignalTimeline(filters),
  enabled: computed(() => !!filters.signal_key),
})

const events = computed(() => result.value?.events || [])
</script>
