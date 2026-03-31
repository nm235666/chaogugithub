<template>
  <AppShell title="信号时间线" subtitle="看主题或股票信号如何从初始走到强化、弱化、证伪或反转。">
    <div class="space-y-4">
      <PageSection title="时间线查询" subtitle="按实体名称和口径查看完整时间线。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input v-model="filters.entity_name" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="实体名称，如 黄金 / 恒立液压" />
          <select v-model="filters.scope" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="7d">7 天</option>
            <option value="1d">1 天</option>
          </select>
          <input v-model="filters.entity_type" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="实体类型，可空" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white">刷新</button>
        </div>
      </PageSection>

      <PageSection title="时间线结果" subtitle="这里优先保证结构完整，后续再接图形化时间轴。">
        <div class="space-y-2">
          <InfoCard v-for="item in result?.items || []" :key="item.id" :title="item.entity_name || '-'" :meta="`${item.event_date || item.created_at || '-'} · ${item.state || '-'} · ${item.direction || '-'}`" :description="item.event_summary || item.reason || ''">
            <template #badge>
              <StatusBadge :value="item.state || item.direction || 'muted'" :label="item.state || item.direction || '-'" />
            </template>
          </InfoCard>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchSignalTimeline } from '../../services/api/signals'

const filters = reactive({ entity_name: '', entity_type: '', scope: '7d', page: 1, page_size: 50 })
const { data: result } = useQuery({ queryKey: ['signal-timeline', filters], queryFn: () => fetchSignalTimeline(filters) })
</script>
