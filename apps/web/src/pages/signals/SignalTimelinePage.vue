<template>
  <AppShell title="信号时间线" subtitle="看主题或股票信号如何从初始走到强化、弱化、证伪或反转。">
    <div class="space-y-4">
      <PageSection title="时间线查询" subtitle="按 signal_key 查看完整时间线，例如 theme:黄金 / stock:000001.SZ。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input v-model="filters.signal_key" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="signal_key，如 theme:黄金 或 stock:000001.SZ" />
          <input v-model="filters.state_filter" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="状态筛选（可选）" />
          <input v-model="filters.driver_filter" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="驱动来源筛选（可选）" />
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="refreshFromFirstPage">刷新</button>
        </div>
      </PageSection>

      <PageSection title="信号走势图" subtitle="优先展示强度与置信度，辅助定位状态变化和关键事件。">
        <div class="mb-3 grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <StatCard title="请求 key" :value="result?.requested_signal_key || '-'" hint="原始查询" />
          <StatCard title="解析 key" :value="result?.resolved_signal_key || '-'" hint="自动纠偏后的 key" />
          <StatCard title="事件总数" :value="result?.total || 0" hint="当前 signal_key 历史事件数" />
          <StatCard title="快照数" :value="snapshots.length" hint="用于强度/置信度趋势图" />
        </div>
        <TrendAreaChart :labels="timelineChart.labels" :series="timelineChart.series" :height="320" empty-text="暂无可绘制的时间线数据" />
      </PageSection>

      <PageSection title="时间线结果" subtitle="图表用于总览，下面保留逐条事件明细。">
        <div v-if="filters.signal_key && !filteredEvents.length" class="mb-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          当前 signal_key 暂无事件记录：<strong>{{ filters.signal_key }}</strong>。可切换到“状态时间线”查看状态机事件。
        </div>
        <div class="space-y-2">
          <InfoCard
            v-for="item in filteredEvents"
            :key="`${item.id}-${item.event_time || item.event_date}`"
            :title="item.driver_title || item.event_type || '-'"
            :meta="`${item.event_time || item.event_date || '-'} · ${item.new_state || '-'} · ${item.new_direction || '-'} · 强度 ${formatNumber(item.new_strength, 2)} · 置信 ${formatNumber(item.new_confidence, 2)}`"
            :description="item.event_summary || item.reason || ''"
          >
            <template #badge>
              <StatusBadge :value="item.new_state || item.new_direction || 'muted'" :label="item.new_state || item.new_direction || '-'" />
            </template>
          </InfoCard>
        </div>
        <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ filters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page <= 1" @click="goPrevPage">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page >= (result?.total_pages || 1)" @click="goNextPage">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import TrendAreaChart from '../../shared/charts/TrendAreaChart.vue'
import { fetchSignalTimeline } from '../../services/api/signals'
import { formatNumber } from '../../shared/utils/format'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

const route = useRoute()
const router = useRouter()
const filters = reactive({
  signal_key: '',
  state_filter: '',
  driver_filter: '',
  page: 1,
  page_size: 20,
})

watch(
  () => route.query,
  (query) => {
    const q = query as Record<string, unknown>
    const signalKey = readQueryString(q, 'signal_key', '')
    filters.state_filter = readQueryString(q, 'state_filter', '')
    filters.driver_filter = readQueryString(q, 'driver_filter', '')
    filters.page = Math.max(1, readQueryNumber(q, 'page', 1))
    filters.page_size = Math.max(20, readQueryNumber(q, 'page_size', 20))
    if (signalKey) {
      filters.signal_key = signalKey
      return
    }
    const legacyName = readQueryString(q, 'entity_name', '')
    if (legacyName) {
      filters.signal_key = `theme:${legacyName}`
    }
  },
  { immediate: true, deep: true },
)

const { data: result } = useQuery({
  queryKey: computed(() => ['signal-timeline', { ...filters }]),
  queryFn: () => fetchSignalTimeline({ ...filters }),
  enabled: computed(() => !!filters.signal_key),
  placeholderData: keepPreviousData,
})

const events = computed(() => result.value?.events || [])
const snapshots = computed<Array<Record<string, any>>>(() => (result.value?.snapshots || []) as Array<Record<string, any>>)

const filteredEvents = computed(() => {
  const state = filters.state_filter.trim()
  const driver = filters.driver_filter.trim().toLowerCase()
  return events.value.filter((item: Record<string, any>) => {
    if (state && String(item.new_state || '') !== state) return false
    if (driver) {
      const driverText = `${item.driver_type || ''} ${item.driver_source || ''} ${item.driver_title || ''}`.toLowerCase()
      if (!driverText.includes(driver)) return false
    }
    return true
  })
})

const timelineChart = computed(() => {
  if (snapshots.value.length) {
    const ordered = [...snapshots.value].reverse()
    return {
      labels: ordered.map((item) => String(item.snapshot_date || item.snapshot_at || '').slice(0, 10) || '-'),
      series: [
        {
          name: '信号强度',
          data: ordered.map((item) => {
            const value = Number(item.signal_strength)
            return Number.isFinite(value) ? value : null
          }),
          color: '#0f617a',
          area: true,
        },
        {
          name: '置信度',
          data: ordered.map((item) => {
            const value = Number(item.confidence)
            return Number.isFinite(value) ? value : null
          }),
          color: '#1f8a59',
          area: false,
        },
      ],
    }
  }
  const orderedEvents = [...events.value].reverse()
  return {
    labels: orderedEvents.map((item: Record<string, any>) => String(item.event_date || item.event_time || '').slice(0, 10) || '-'),
    series: [
      {
        name: '事件后强度',
        data: orderedEvents.map((item: Record<string, any>) => {
          const value = Number(item.new_strength)
          return Number.isFinite(value) ? value : null
        }),
        color: '#0f617a',
        area: true,
      },
      {
        name: '事件后置信度',
        data: orderedEvents.map((item: Record<string, any>) => {
          const value = Number(item.new_confidence)
          return Number.isFinite(value) ? value : null
        }),
        color: '#1f8a59',
        area: false,
      },
    ],
  }
})

function refreshFromFirstPage() {
  filters.page = 1
  syncRouteFromFilters()
}

function syncRouteFromFilters() {
  router.replace({
    query: buildCleanQuery({
      signal_key: filters.signal_key,
      state_filter: filters.state_filter,
      driver_filter: filters.driver_filter,
      page: filters.page,
      page_size: filters.page_size,
    }),
  })
}

function goPrevPage() {
  if (filters.page <= 1) return
  filters.page -= 1
  syncRouteFromFilters()
}

function goNextPage() {
  const totalPages = Number(result.value?.total_pages || 1)
  if (filters.page >= totalPages) return
  filters.page += 1
  syncRouteFromFilters()
}
</script>
