<template>
  <AppShell title="候选漏斗" subtitle="候选股票从进入到执行的完整状态机管理，支持按阶段过滤与流转操作。">
    <div class="space-y-4">
      <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm">
        <div class="flex flex-wrap items-center gap-2">
          <span class="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">可用性分级</span>
          <span class="rounded-full px-2.5 py-1 text-xs font-semibold" :class="availabilityBadgeClass">{{ availabilityLabel }}</span>
          <span class="text-xs text-[var(--muted)]">{{ availabilityReason }}</span>
        </div>
        <div class="mt-2 flex flex-wrap gap-2 text-xs">
          <span v-if="upstreamScoreDate" class="metric-chip">上游评分日期 {{ upstreamScoreDate }}</span>
          <span class="metric-chip">上游评分样本 {{ upstreamScoreCount }}</span>
          <span class="metric-chip">漏斗总量 {{ funnelTotal }}</span>
          <span v-for="item in availabilityMissingInputs" :key="item" class="metric-chip text-amber-700">{{ item }}</span>
        </div>
      </div>

      <!-- 漏斗概览 stat cards -->
      <PageSection title="漏斗概览" subtitle="各阶段候选数量统计。">
        <div class="grid gap-3 grid-cols-2 sm:grid-cols-4 xl:grid-cols-5">
          <button
            v-for="s in STATE_LIST"
            :key="s.key"
            class="rounded-[var(--radius-md)] border p-3 text-left transition hover:shadow-[var(--shadow-card)]"
            :class="[
              activeFilter === s.key
                ? 'border-[var(--brand)] bg-[var(--brand)]/5 shadow-[var(--shadow-card)]'
                : 'border-[var(--line)] bg-white shadow-[var(--shadow-soft)]',
            ]"
            @click="toggleFilter(s.key)"
          >
            <div class="text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">{{ s.label }}</div>
            <div class="mt-1.5 text-2xl font-extrabold" :class="s.colorClass">
              {{ stateCountMap[s.key] ?? 0 }}
            </div>
          </button>
        </div>
      </PageSection>

      <!-- 漏斗指标 -->
      <PageSection title="漏斗指标" subtitle="整体转化效率指标。">
        <div v-if="metricsLoading" class="text-sm text-[var(--muted)]">加载中...</div>
        <MetricGrid
          v-else
          :items="metricsItems"
          columns-class="grid-cols-3"
        />
      </PageSection>

      <!-- 候选列表 -->
      <PageSection title="候选列表" :subtitle="listSubtitle">
        <template #action>
          <div class="flex flex-wrap gap-2">
            <button
              class="rounded-full border px-3 py-1 text-xs font-semibold transition"
              :class="activeFilter === '' ? 'border-[var(--brand)] bg-[var(--brand)] text-white' : 'border-[var(--line)] bg-white text-[var(--ink)] hover:border-[var(--brand)] hover:text-[var(--brand)]'"
              @click="activeFilter = ''"
            >
              全部
            </button>
            <button
              v-for="s in STATE_LIST"
              :key="s.key"
              class="rounded-full border px-3 py-1 text-xs font-semibold transition"
              :class="activeFilter === s.key ? 'border-[var(--brand)] bg-[var(--brand)] text-white' : 'border-[var(--line)] bg-white text-[var(--ink)] hover:border-[var(--brand)] hover:text-[var(--brand)]'"
              @click="toggleFilter(s.key)"
            >
              {{ s.label }}
            </button>
          </div>
        </template>

        <div v-if="candidatesLoading" class="py-8 text-center text-sm text-[var(--muted)]">加载中...</div>
        <div v-else-if="candidatesError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-700">
          加载候选列表失败，请刷新重试。
        </div>
        <div v-else-if="filteredCandidates.length === 0 && activeFilter" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-10 text-center text-sm text-[var(--muted)]">
          {{ `当前阶段（${stateLabel(activeFilter)}）暂无候选` }}
        </div>
        <div v-else-if="filteredCandidates.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] bg-gray-50 px-6 py-8 text-center">
          <div class="text-sm font-semibold text-[var(--ink)]">候选池暂无标的</div>
          <div class="mt-2 text-xs text-[var(--muted)]">候选标的尚未进入漏斗，或当前筛选条件无结果。</div>
          <div class="mt-4 flex flex-wrap justify-center gap-2">
            <RouterLink to="/app/market" class="rounded-full border border-[var(--brand)] bg-white px-4 py-2 text-xs font-semibold text-[var(--brand)]">
              查看市场结论获取方向
            </RouterLink>
            <RouterLink to="/app/signals/overview" class="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs font-semibold text-[var(--ink)]">
              从信号中发现候选
            </RouterLink>
            <RouterLink to="/app/chatrooms/investment" class="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs font-semibold text-[var(--ink)]">
              从群聊候选池导入
            </RouterLink>
          </div>
        </div>
        <div v-else class="divide-y divide-[var(--line)]">
          <div
            v-for="candidate in filteredCandidates"
            :key="candidate.id"
            class="cursor-pointer py-3 transition hover:bg-[var(--panel-soft)]"
            :class="{ 'bg-[var(--brand)]/5': selectedId === candidate.id }"
            @click="selectCandidate(candidate)"
          >
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div class="min-w-0">
                <div class="text-sm font-semibold text-[var(--ink)]">
                  {{ candidate.ts_code }}
                  <span v-if="candidate.name" class="ml-1 font-normal text-[var(--muted)]">{{ candidate.name }}</span>
                </div>
                <div v-if="candidate.last_transition_reason" class="mt-0.5 text-xs text-[var(--muted)] truncate max-w-sm">
                  {{ candidate.last_transition_reason }}
                </div>
              </div>
              <div class="flex items-center gap-2">
                <span :class="stateBadgeClass(candidate.current_state)">
                  {{ stateLabel(candidate.current_state) }}
                </span>
                <div v-if="candidate.last_updated" class="text-xs text-[var(--muted)]">
                  {{ formatDate(candidate.last_updated) }}
                </div>
              </div>
            </div>
          </div>
        </div>
      </PageSection>

      <!-- 流转操作面板 -->
      <PageSection v-if="selectedCandidate" title="流转操作" subtitle="对选中候选执行状态流转。">
        <div class="space-y-4">
          <div class="flex flex-wrap items-center gap-3">
            <div class="text-sm font-semibold text-[var(--ink)]">
              {{ selectedCandidate.ts_code }}
              <span v-if="selectedCandidate.name" class="font-normal text-[var(--muted)]">{{ selectedCandidate.name }}</span>
            </div>
            <span :class="stateBadgeClass(selectedCandidate.current_state)">
              当前：{{ stateLabel(selectedCandidate.current_state) }}
            </span>
          </div>

          <div class="grid gap-3 sm:grid-cols-3">
            <label class="text-sm font-semibold text-[var(--ink)]">
              目标状态
              <select v-model="transitionForm.to_state" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm">
                <option value="">请选择...</option>
                <option v-for="s in nextStates" :key="s.key" :value="s.key">{{ s.label }}</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              触发来源
              <select v-model="transitionForm.trigger_source" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm">
                <option value="manual">手动操作</option>
                <option value="ai_screen">AI筛选</option>
                <option value="decision_board">决策工作台</option>
                <option value="system">系统触发</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              流转原因
              <input
                v-model="transitionForm.reason"
                class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm"
                placeholder="简要说明流转原因（可选）"
              />
            </label>
          </div>

          <div v-if="transitionError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            流转失败：{{ transitionError }}
          </div>
          <div v-if="transitionSuccess" class="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
            流转成功！
          </div>

          <div class="flex gap-2">
            <button
              class="rounded-2xl bg-[var(--brand)] px-5 py-2.5 text-sm font-semibold text-white disabled:opacity-60"
              :disabled="!transitionForm.to_state || transitionPending"
              @click="doTransition"
            >
              {{ transitionPending ? '处理中...' : '确认流转' }}
            </button>
            <button
              class="rounded-2xl border border-[var(--line)] bg-white px-5 py-2.5 text-sm font-semibold text-[var(--ink)]"
              @click="clearSelection"
            >
              取消
            </button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { ref, computed, reactive } from 'vue'
import { RouterLink } from 'vue-router'
import { useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import MetricGrid from '../../shared/ui/MetricGrid.vue'
import type { MetricGridItem } from '../../shared/ui/MetricGrid.vue'
import {
  fetchFunnelCandidates,
  fetchFunnelMetrics,
  transitionFunnelCandidate,
  type FunnelCandidate,
  type FunnelMetrics,
} from '../../services/api/funnel'

interface StateConfig {
  key: string
  label: string
  colorClass: string
  badgeClass: string
}

const STATE_LIST: StateConfig[] = [
  { key: 'ingested', label: '已进入', colorClass: 'text-gray-600', badgeClass: 'inline-flex items-center rounded-full border border-gray-200 bg-gray-100 px-2.5 py-1 text-xs font-semibold text-gray-700' },
  { key: 'amplified', label: '已增强', colorClass: 'text-blue-600', badgeClass: 'inline-flex items-center rounded-full border border-blue-200 bg-blue-100 px-2.5 py-1 text-xs font-semibold text-blue-700' },
  { key: 'ai_screen_passed', label: 'AI初筛', colorClass: 'text-purple-600', badgeClass: 'inline-flex items-center rounded-full border border-purple-200 bg-purple-100 px-2.5 py-1 text-xs font-semibold text-purple-700' },
  { key: 'shortlisted', label: '短名单', colorClass: 'text-cyan-600', badgeClass: 'inline-flex items-center rounded-full border border-cyan-200 bg-cyan-100 px-2.5 py-1 text-xs font-semibold text-cyan-700' },
  { key: 'decision_ready', label: '待决策', colorClass: 'text-amber-600', badgeClass: 'inline-flex items-center rounded-full border border-amber-200 bg-amber-100 px-2.5 py-1 text-xs font-semibold text-amber-700' },
  { key: 'confirmed', label: '已确认', colorClass: 'text-emerald-600', badgeClass: 'inline-flex items-center rounded-full border border-emerald-200 bg-emerald-100 px-2.5 py-1 text-xs font-semibold text-emerald-700' },
  { key: 'rejected', label: '已淘汰', colorClass: 'text-rose-600', badgeClass: 'inline-flex items-center rounded-full border border-rose-200 bg-rose-100 px-2.5 py-1 text-xs font-semibold text-rose-700' },
  { key: 'deferred', label: '已暂缓', colorClass: 'text-orange-600', badgeClass: 'inline-flex items-center rounded-full border border-orange-200 bg-orange-100 px-2.5 py-1 text-xs font-semibold text-orange-700' },
  { key: 'executed', label: '已执行', colorClass: 'text-emerald-700', badgeClass: 'inline-flex items-center rounded-full border border-emerald-300 bg-emerald-200 px-2.5 py-1 text-xs font-semibold text-emerald-800' },
  { key: 'reviewed', label: '已复盘', colorClass: 'text-teal-600', badgeClass: 'inline-flex items-center rounded-full border border-teal-200 bg-teal-100 px-2.5 py-1 text-xs font-semibold text-teal-700' },
]

// Valid state transitions
const STATE_TRANSITIONS: Record<string, string[]> = {
  ingested: ['amplified', 'rejected', 'deferred'],
  amplified: ['ai_screen_passed', 'rejected', 'deferred'],
  ai_screen_passed: ['shortlisted', 'rejected', 'deferred'],
  shortlisted: ['decision_ready', 'rejected', 'deferred'],
  decision_ready: ['confirmed', 'rejected', 'deferred'],
  confirmed: ['executed', 'deferred'],
  executed: ['reviewed'],
  deferred: ['ingested', 'rejected'],
  rejected: [],
  reviewed: [],
}

const activeFilter = ref('')
const selectedCandidate = ref<FunnelCandidate | null>(null)
const selectedId = ref<string>('')
const transitionForm = reactive({ to_state: '', trigger_source: 'manual', reason: '' })
const transitionError = ref('')
const transitionSuccess = ref(false)
const transitionPending = ref(false)

const queryClient = useQueryClient()

const {
  data: candidatesData,
  isPending: candidatesLoading,
  isError: candidatesError,
} = useQuery({
  queryKey: ['funnel-candidates'],
  queryFn: () => fetchFunnelCandidates({ limit: 200 }),
})

const {
  data: metricsData,
  isPending: metricsLoading,
} = useQuery({
  queryKey: ['funnel-metrics'],
  queryFn: fetchFunnelMetrics,
})

const allCandidates = computed<FunnelCandidate[]>(() => {
  const raw = candidatesData.value
  if (!raw) return []
  const rows = Array.isArray(raw) ? raw : Array.isArray(raw.items) ? raw.items : Array.isArray(raw.candidates) ? raw.candidates : []
  return rows.map((item: any) => ({
    id: String(item?.id || ''),
    ts_code: String(item?.ts_code || '').toUpperCase(),
    name: String(item?.name || ''),
    current_state: String(item?.current_state || item?.state || ''),
    last_transition_reason: String(item?.last_transition_reason || item?.reason || ''),
    last_updated: String(item?.last_updated || item?.updated_at || ''),
    created_at: String(item?.created_at || ''),
  }))
})

const metricsPayload = computed<Record<string, any>>(() => (metricsData.value || {}) as Record<string, any>)
const candidatesPayload = computed<Record<string, any>>(() => (candidatesData.value || {}) as Record<string, any>)

const stateCountMap = computed(() => {
  const apiCounts = metricsPayload.value?.state_counts
  if (apiCounts && typeof apiCounts === 'object') {
    return Object.fromEntries(Object.entries(apiCounts).map(([k, v]) => [k, Number(v || 0)]))
  }
  const map: Record<string, number> = {}
  for (const c of allCandidates.value) {
    map[c.current_state] = (map[c.current_state] || 0) + 1
  }
  return map
})

const filteredCandidates = computed(() => {
  if (!activeFilter.value) return allCandidates.value
  return allCandidates.value.filter((c) => c.current_state === activeFilter.value)
})

const listSubtitle = computed(() => {
  const total = filteredCandidates.value.length
  const filterLabel = activeFilter.value ? `阶段：${stateLabel(activeFilter.value)}，` : ''
  return `${filterLabel}共 ${total} 条`
})

const metricsItems = computed<MetricGridItem[]>(() => {
  const m: FunnelMetrics = metricsData.value || {}
  const total = Number((metricsPayload.value.total ?? m.candidate_count ?? allCandidates.value.length) || 0)
  const decisionReady = Number(stateCountMap.value.decision_ready || 0)
  const executed = Number(stateCountMap.value.executed || 0)
  const conversion = total > 0 ? executed / total : 0
  return [
    { label: '候选总数', value: total || '-' },
    { label: '平均决策天数', value: m.avg_days_to_decision != null ? `${m.avg_days_to_decision.toFixed(1)} 天` : '-' },
    { label: '待决策', value: decisionReady },
    { label: '转化率', value: m.conversion_rate != null ? `${(m.conversion_rate * 100).toFixed(1)}%` : `${(conversion * 100).toFixed(1)}%` },
  ]
})

const availabilityStatus = computed(() => {
  const fromMetrics = String(metricsPayload.value.status || '').trim()
  if (fromMetrics) return fromMetrics
  return String(candidatesPayload.value.status || '').trim() || 'unknown'
})
const availabilityReason = computed(() => {
  const reason = String(metricsPayload.value.status_reason || candidatesPayload.value.status_reason || '').trim()
  if (reason) return reason
  if (availabilityStatus.value === 'ready') return '漏斗链路可用于筛选与流转。'
  if (availabilityStatus.value === 'degraded') return '部分上游已就绪，但漏斗候选仍需补齐。'
  if (availabilityStatus.value === 'not_initialized') return '漏斗表尚未初始化。'
  return '当前缺少稳定漏斗输入。'
})
const availabilityLabel = computed(() => {
  if (availabilityStatus.value === 'ready') return 'ready'
  if (availabilityStatus.value === 'degraded') return 'degraded'
  if (availabilityStatus.value === 'not_initialized') return 'not_initialized'
  if (availabilityStatus.value === 'empty') return 'empty'
  return 'unknown'
})
const availabilityBadgeClass = computed(() => {
  if (availabilityStatus.value === 'ready') return 'border border-emerald-200 bg-emerald-100 text-emerald-800'
  if (availabilityStatus.value === 'degraded') return 'border border-amber-200 bg-amber-100 text-amber-800'
  if (availabilityStatus.value === 'not_initialized') return 'border border-rose-200 bg-rose-100 text-rose-700'
  return 'border border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]'
})
const availabilityMissingInputs = computed<string[]>(() => {
  const v = metricsPayload.value.missing_inputs || candidatesPayload.value.missing_inputs
  return Array.isArray(v) ? v.slice(0, 5).map((item) => String(item)) : []
})
const upstreamScoreDate = computed(() => String(metricsPayload.value?.upstream_scores?.latest_score_date || candidatesPayload.value?.upstream_scores?.latest_score_date || '').trim())
const upstreamScoreCount = computed(() => Number(metricsPayload.value?.upstream_scores?.latest_count ?? candidatesPayload.value?.upstream_scores?.latest_count ?? 0))
const funnelTotal = computed(() => Number(metricsPayload.value.total ?? candidatesPayload.value.total ?? allCandidates.value.length))

const nextStates = computed(() => {
  if (!selectedCandidate.value) return []
  const valid = STATE_TRANSITIONS[selectedCandidate.value.current_state] || []
  return STATE_LIST.filter((s) => valid.includes(s.key))
})

function stateLabel(key: string): string {
  return STATE_LIST.find((s) => s.key === key)?.label ?? key
}

function stateBadgeClass(key: string): string {
  return STATE_LIST.find((s) => s.key === key)?.badgeClass
    ?? 'inline-flex items-center rounded-full border border-gray-200 bg-gray-100 px-2.5 py-1 text-xs font-semibold text-gray-700'
}

function toggleFilter(key: string) {
  activeFilter.value = activeFilter.value === key ? '' : key
}

function selectCandidate(c: FunnelCandidate) {
  selectedCandidate.value = c
  selectedId.value = c.id
  transitionForm.to_state = ''
  transitionForm.reason = ''
  transitionError.value = ''
  transitionSuccess.value = false
}

function clearSelection() {
  selectedCandidate.value = null
  selectedId.value = ''
}

function formatDate(s: string): string {
  try {
    return new Date(s).toLocaleDateString('zh-CN')
  } catch {
    return s
  }
}

async function doTransition() {
  if (!selectedCandidate.value || !transitionForm.to_state) return
  transitionPending.value = true
  transitionError.value = ''
  transitionSuccess.value = false
  try {
    await transitionFunnelCandidate(selectedCandidate.value.id, {
      to_state: transitionForm.to_state,
      reason: transitionForm.reason || undefined,
      trigger_source: transitionForm.trigger_source,
    })
    transitionSuccess.value = true
    await queryClient.invalidateQueries({ queryKey: ['funnel-candidates'] })
    await queryClient.invalidateQueries({ queryKey: ['funnel-metrics'] })
    clearSelection()
  } catch (e: any) {
    transitionError.value = e?.message || '未知错误'
  } finally {
    transitionPending.value = false
  }
}
</script>
