<template>
  <AppShell title="因子挖掘工作台" subtitle="第三层（验证与研究层）主入口：因子研究、回测验证、复盘修正。">
    <div class="space-y-4">
      <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm">
        <div class="text-[var(--muted)]">
          本页属于验证与研究层，负责把研究方向沉淀为可验证能力。若要执行投资动作，请回到
          <RouterLink to="/app/decision" class="font-semibold text-[var(--brand)] hover:underline">投研决策板</RouterLink>。
        </div>
        <div class="mt-2 flex flex-wrap gap-2 text-xs">
          <RouterLink to="/app/decision" class="metric-chip font-semibold text-[var(--brand)]">去决策层</RouterLink>
          <RouterLink to="/app/review" class="metric-chip font-semibold text-[var(--brand)]">去复盘层</RouterLink>
        </div>
      </div>
      <PageSection title="任务启动" subtitle="输入研究方向后，可启动 mine/backtest 任务（research 使用自研等价适配层）。">
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-2">
          <input v-model.trim="form.direction" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 xl:col-span-2" placeholder="研究方向，如：红利低波 + 价值增强" />
          <input v-model.number="form.lookback" type="number" min="1" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="lookback" />
          <select v-model="form.config_profile" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="default">default</option>
            <option value="conservative">conservative</option>
            <option value="aggressive">aggressive</option>
          </select>
          <select v-model="form.engine_profile" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="auto">auto (默认)</option>
            <option value="business">business</option>
            <option value="research">research</option>
          </select>
          <input v-model.trim="form.llm_profile" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="llm_profile (auto)" />
        </div>
        <div class="mt-3 flex flex-wrap gap-2">
          <button class="rounded-2xl bg-indigo-700 px-4 py-2 text-white disabled:opacity-40" :disabled="!form.direction || mineMutation.isPending.value" @click="runMine">
            {{ mineMutation.isPending.value ? '启动中...' : '启动因子挖掘' }}
          </button>
          <button class="rounded-2xl bg-fuchsia-700 px-4 py-2 text-white disabled:opacity-40" :disabled="autoResearchMutation.isPending.value" @click="runAutoResearch">
            {{ autoResearchMutation.isPending.value ? '启动中...' : '自动研究并沉淀' }}
          </button>
          <button class="rounded-2xl bg-emerald-700 px-4 py-2 text-white disabled:opacity-40" :disabled="!form.direction || backtestMutation.isPending.value" @click="runBacktest">
            {{ backtestMutation.isPending.value ? '启动中...' : '启动回测' }}
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2" :disabled="!activeTaskId" @click="refreshTask">
            刷新任务
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)] disabled:opacity-40" :disabled="!activeTaskId && !taskView" @click="clearCurrentTaskTracking">
            清除当前任务跟踪
          </button>
        </div>
        <div v-if="message" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          {{ message }}
        </div>
        <div class="mt-2 text-xs text-[var(--muted)]">
          Worker：{{ workerSummary }}
        </div>
        <div v-if="healthAlerts.length" class="mt-2 rounded-[14px] border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          <div v-for="item in healthAlerts" :key="item.code">{{ item.message }}</div>
        </div>
        <div v-if="recentErrorCodeSummary" class="mt-2 text-xs text-[var(--muted)]">
          最近错误码分布：{{ recentErrorCodeSummary }}
        </div>
        <div v-if="restoredTaskHint" class="mt-2 text-sm text-[var(--muted)]">已从会话恢复任务跟踪（{{ restoredTaskHint }}）</div>
        <div v-if="taskPersistenceNotice" class="mt-2 text-sm text-[var(--muted)]">{{ taskPersistenceNotice }}</div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[1fr_1fr]">
        <PageSection title="当前任务" subtitle="轮询状态：pending / running / done / error。">
          <div v-if="!taskView" class="text-sm text-[var(--muted)]">暂无任务</div>
          <template v-else>
            <InfoCard :title="taskView.task_id || '-'" :meta="`${taskView.task_type || '-'} · ${taskView.job_key || '-'} · ${taskView.status || '-'} · ${taskView.stage || '-'} · ${taskProgressText}`" :description="taskErrorText || taskRuntimeText">
              <template #badge>
                <StatusBadge :value="taskView.status || 'muted'" :label="taskView.status || '-'" />
              </template>
            </InfoCard>
            <div class="mt-2 text-xs text-[var(--muted)]">
              引擎：{{ taskView.engine_used || '-' }} · 模式：{{ taskView.engine_profile || 'auto' }} · 来源：自研等价适配层 · 库版本：{{ taskView.library_version || '-' }} · 晋升：{{ taskView.promotion_state || '-' }}
            </div>
            <div class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] p-3 text-sm text-[var(--ink)]">
              <div class="font-semibold">{{ taskReadableHeadline }}</div>
              <div class="mt-1 text-xs text-[var(--muted)]">{{ taskReadableAdvice }}</div>
              <div class="mt-2 text-xs text-[var(--muted)]">
                指标：IC {{ fmtMetric(taskView.metrics?.ic) }} · RankIC {{ fmtMetric(taskView.metrics?.rank_ic) }} · ARR {{ fmtPercent(taskView.metrics?.arr) }} · MDD {{ fmtPercent(taskView.metrics?.mdd) }} · Calmar {{ fmtMetric(taskView.metrics?.calmar) }}
              </div>
              <div class="mt-1 text-xs text-[var(--muted)]">
                相对基准：ΔARR {{ fmtDelta(taskView.baseline_compare?.delta_arr) }} · ΔCalmar {{ fmtDelta(taskView.baseline_compare?.delta_calmar) }} · ΔMDD {{ fmtDelta(taskView.baseline_compare?.delta_mdd) }}
              </div>
              <div v-if="taskTroubleshootingText" class="mt-1 text-xs text-[var(--muted)]">
                排障摘要：{{ taskTroubleshootingText }}
              </div>
              <details class="mt-2">
                <summary class="cursor-pointer text-xs text-[var(--muted)]">查看原始技术明细</summary>
                <pre class="mt-2 overflow-x-auto whitespace-pre-wrap text-xs text-[var(--muted)]">{{ taskMetricsText }}</pre>
              </details>
            </div>
          </template>
        </PageSection>

        <PageSection :title="`最近结果 (${results.length})`" subtitle="来自 /api/quant-factors/results。">
          <div class="space-y-2">
            <InfoCard
              v-for="item in results"
              :key="item.task_id"
              :title="item.task_id || '-'"
              :meta="`${item.task_type || '-'} · ${item.status || '-'} · ${item.duration_seconds ?? '-'}s`"
              :description="resultDescription(item)"
            >
              <template #badge>
                <StatusBadge :value="item.status || 'muted'" :label="item.status || '-'" />
              </template>
            </InfoCard>
          </div>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchQuantHealth, fetchQuantResults, fetchQuantTask, startQuantAutoResearch, startQuantBacktest, startQuantMine } from '../../services/api/quantFactors'
import { buildTaskScopeKey } from '../../shared/taskPersistence/taskPersistence'
import { usePersistedTaskRunner } from '../../shared/taskPersistence/usePersistedTaskRunner'

const form = reactive({
  direction: '',
  market_scope: 'A_share',
  lookback: 120,
  config_profile: 'default',
  llm_profile: 'auto',
  engine_profile: 'auto' as 'auto' | 'business' | 'research',
})
const message = ref('')
const activeTaskId = ref('')
const taskView = ref<Record<string, any> | null>(null)
let pollTimer: number | null = null
const taskScopeKey = buildTaskScopeKey('quant-factors', 'active-task')
const {
  restoredHint: restoredTaskHint,
  noticeMessage: taskPersistenceNotice,
  restoreTaskSnapshot,
  persistTaskSnapshot,
  clearPersistedTaskSnapshot,
} = usePersistedTaskRunner(() => taskScopeKey)

const resultsQuery = useQuery({
  queryKey: ['quant-factor-results'],
  queryFn: () => fetchQuantResults({ page: 1, page_size: 20 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 20_000 : 90_000),
})

const healthQuery = useQuery({
  queryKey: ['quant-factor-health'],
  queryFn: () => fetchQuantHealth(),
  refetchInterval: () => (document.visibilityState === 'visible' ? 10_000 : 60_000),
})

const mineMutation = useMutation({
  mutationFn: () => startQuantMine({ ...form }),
  onSuccess: async (payload) => {
    activeTaskId.value = String(payload?.task_id || '')
    message.value = `因子挖掘任务已启动：${activeTaskId.value || '-'}`
    persistTaskSnapshot({
      jobId: activeTaskId.value,
      status: String(payload?.status || 'queued'),
      stage: String(payload?.stage || 'queued'),
      progress: Number(payload?.progress_pct || 0),
      actionMessage: message.value,
      updatedAt: new Date().toISOString(),
    })
    await refreshTask()
    startPolling()
    resultsQuery.refetch()
  },
  onError: (error: Error) => {
    message.value = `启动失败：${error.message}`
  },
})

const autoResearchMutation = useMutation({
  mutationFn: () => startQuantAutoResearch({ ...form }),
  onSuccess: async (payload) => {
    activeTaskId.value = String(payload?.task_id || '')
    message.value = `自动研究任务已启动：${activeTaskId.value || '-'}`
    persistTaskSnapshot({
      jobId: activeTaskId.value,
      status: String(payload?.status || 'queued'),
      stage: String(payload?.stage || 'queued'),
      progress: Number(payload?.progress_pct || 0),
      actionMessage: message.value,
      updatedAt: new Date().toISOString(),
    })
    await refreshTask()
    startPolling()
    resultsQuery.refetch()
  },
  onError: (error: Error) => {
    message.value = `启动失败：${error.message}`
  },
})

const backtestMutation = useMutation({
  mutationFn: () => startQuantBacktest({ ...form }),
  onSuccess: async (payload) => {
    activeTaskId.value = String(payload?.task_id || '')
    message.value = `回测任务已启动：${activeTaskId.value || '-'}`
    persistTaskSnapshot({
      jobId: activeTaskId.value,
      status: String(payload?.status || 'queued'),
      stage: String(payload?.stage || 'queued'),
      progress: Number(payload?.progress_pct || 0),
      actionMessage: message.value,
      updatedAt: new Date().toISOString(),
    })
    await refreshTask()
    startPolling()
    resultsQuery.refetch()
  },
  onError: (error: Error) => {
    message.value = `启动失败：${error.message}`
  },
})

const results = computed<Array<Record<string, any>>>(() => (resultsQuery.data.value?.items || []) as Array<Record<string, any>>)
const workerSummary = computed(() => {
  const worker = healthQuery.data.value?.worker || {}
  const queue = healthQuery.data.value?.queue || {}
  const workerState = worker.alive ? '在线' : '离线'
  const heartbeatAge = Number(worker.heartbeat_age_seconds)
  const heartbeatText = Number.isFinite(heartbeatAge) && heartbeatAge >= 0 ? `${heartbeatAge}s` : '-'
  return `${workerState} · mode=${worker.execution_mode || '-'} · pending=${queue.pending ?? '-'} · running=${queue.running ?? '-'} · heartbeat_age=${heartbeatText}`
})
const healthAlerts = computed<Array<Record<string, any>>>(() => {
  const rows = healthQuery.data.value?.alerts
  return Array.isArray(rows) ? (rows as Array<Record<string, any>>) : []
})
const recentErrorCodeSummary = computed(() => {
  const map = healthQuery.data.value?.queue?.recent_error_codes || {}
  const entries = Object.entries(map as Record<string, any>)
  if (!entries.length) return ''
  return entries
    .slice(0, 5)
    .map(([code, count]) => `${code}×${count}`)
    .join(' / ')
})
const taskErrorText = computed(() => {
  const item = taskView.value
  if (!item) return ''
  const code = String(item.error_code || '')
  const messageText = String(item.error_message || '').trim()
  if (code === 'RUNNER_CONFIG_INVALID') {
    if (messageText) return `运行环境未就绪（依赖或配置缺失）：${messageText}`
    return '运行环境未就绪（依赖或配置缺失），请检查因子引擎运行环境。'
  }
  return String(messageText || code || '任务运行中')
})
const taskMetricsText = computed(() => {
  const item = taskView.value
  if (!item) return '{}'
  return JSON.stringify(
    {
      engine: item.engine || '-',
      engine_profile: item.engine_profile || '-',
      engine_used: item.engine_used || '-',
      stage: item.stage || '-',
      progress_pct: item.progress_pct ?? '-',
      worker_state: item.worker_state || {},
      library_version: item.library_version || '-',
      baseline_compare: item.baseline_compare || item.benchmark_delta || {},
      status_message: item.status_message || '',
      output_tail: item.output_tail || '',
      metrics: item.metrics || {},
      artifacts: item.artifacts || {},
      output: item.output || {},
    },
    null,
    2,
  )
})
const taskProgressText = computed(() => {
  const progress = Number(taskView.value?.progress_pct)
  if (!Number.isFinite(progress)) return '-'
  return `${Math.max(0, Math.min(100, Math.round(progress)))}%`
})
const taskRuntimeText = computed(() => {
  const item = taskView.value
  if (!item) return ''
  const engine = String(item.engine_used || '').trim()
  const stage = String(item.stage || '').trim()
  const msg = String(item.status_message || '').trim()
  const tail = String(item.output_tail || '').trim()
  return [engine ? `引擎：${engine}` : '', stage ? `阶段：${stage}` : '', msg, tail].filter(Boolean).join(' · ')
})
const taskTroubleshootingText = computed(() => {
  const trace = taskView.value?.troubleshooting || {}
  const stage = String(trace.stage || taskView.value?.stage || '').trim()
  const engine = String(trace.engine || taskView.value?.engine_used || '').trim()
  const duration = Number(trace.duration_seconds)
  const durationText = Number.isFinite(duration) ? `${duration.toFixed(1)}s` : '-'
  const err = String(trace.last_error || taskView.value?.error_message || taskView.value?.error_code || '').trim()
  const tail = String(trace.output_tail || taskView.value?.output_tail || '').trim()
  const parts = [stage ? `stage=${stage}` : '', engine ? `engine=${engine}` : '', `duration=${durationText}`, err ? `error=${err}` : '', tail ? `tail=${tail}` : '']
  return parts.filter(Boolean).join(' · ')
})
const taskReadableHeadline = computed(() => {
  const item = taskView.value || {}
  const delta = item.baseline_compare || item.benchmark_delta || {}
  const deltaArr = Number(delta?.delta_arr)
  const deltaCalmar = Number(delta?.delta_calmar)
  const deltaMdd = Number(delta?.delta_mdd)
  const done = String(item.status || '') === 'done'
  if (!done) return '任务进行中：正在计算因子与回测结果。'
  const better = Number.isFinite(deltaArr) && Number.isFinite(deltaCalmar) && deltaArr > 0 && deltaCalmar > 0
  const mddSafe = Number.isFinite(deltaMdd) ? deltaMdd <= 0.02 : true
  if (better && mddSafe) return '结论：该方向当前优于基准，可作为候选策略。'
  if (better && !mddSafe) return '结论：收益改善，但回撤偏高，需降风险后再用。'
  return '结论：当前未明显优于基准，建议继续迭代方向。'
})
const taskReadableAdvice = computed(() => {
  const item = taskView.value || {}
  if (String(item.status || '') !== 'done') return '建议：等待任务结束后再看结论。'
  const delta = item.baseline_compare || item.benchmark_delta || {}
  const deltaArr = Number(delta?.delta_arr)
  const deltaCalmar = Number(delta?.delta_calmar)
  const deltaMdd = Number(delta?.delta_mdd)
  if (Number.isFinite(deltaArr) && Number.isFinite(deltaCalmar) && deltaArr > 0 && deltaCalmar > 0 && Number.isFinite(deltaMdd) && deltaMdd <= 0.02) {
    return '建议动作：保留这条方向，改小步长继续做参数与持仓约束优化。'
  }
  if (Number.isFinite(deltaArr) && deltaArr > 0) {
    return '建议动作：收益有提升，但先优化回撤（缩股票池、降低换手）后再推进。'
  }
  return '建议动作：换研究方向关键词，或缩短 lookback 再试。'
})

function fmtMetric(value: any) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return num.toFixed(4)
}

function fmtPercent(value: any) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${(num * 100).toFixed(2)}%`
}

function fmtDelta(value: any) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  const sign = num > 0 ? '+' : ''
  return `${sign}${num.toFixed(4)}`
}

function resultDescription(item: Record<string, any>) {
  const metrics = item.metrics || {}
  const arr = fmtPercent(metrics.arr)
  const mdd = fmtPercent(metrics.mdd)
  const calmar = fmtMetric(metrics.calmar)
  const delta = item.baseline_compare || item.benchmark_delta || {}
  const deltaArr = fmtDelta(delta?.delta_arr)
  const deltaCalmar = fmtDelta(delta?.delta_calmar)
  const good = Number(delta?.delta_arr) > 0 && Number(delta?.delta_calmar) > 0
  const verdict = good ? '优于基准' : '未优于基准'
  return `${verdict} · ARR ${arr} · MDD ${mdd} · Calmar ${calmar} · ΔARR ${deltaArr} · ΔCalmar ${deltaCalmar}`
}

async function refreshTask() {
  const taskId = activeTaskId.value
  if (!taskId) return
  try {
    const payload = await fetchQuantTask(taskId)
    taskView.value = payload
    const status = String(payload?.status || '')
    persistTaskSnapshot({
      jobId: taskId,
      status,
      stage: String(payload?.stage || status),
      progress: Number(payload?.progress_pct || (status === 'done' || status === 'error' ? 100 : 50)),
      actionMessage: message.value,
      error: String(payload?.error_message || payload?.error_code || ''),
      updatedAt: String(payload?.update_time || new Date().toISOString()),
      data: {
        taskView: payload,
      },
    })
    if (status === 'done' || status === 'error') {
      stopPolling()
      resultsQuery.refetch()
    }
  } catch (error: any) {
    message.value = `任务查询失败：${String(error?.message || error)}`
  }
}

function startPolling() {
  stopPolling()
  pollTimer = window.setInterval(() => {
    refreshTask()
  }, 3000)
}

function stopPolling() {
  if (pollTimer !== null) {
    window.clearInterval(pollTimer)
    pollTimer = null
  }
}

function runMine() {
  mineMutation.mutate()
}

function runBacktest() {
  backtestMutation.mutate()
}

function runAutoResearch() {
  autoResearchMutation.mutate()
}

function clearCurrentTaskTracking() {
  stopPolling()
  activeTaskId.value = ''
  taskView.value = null
  message.value = '已清除当前任务跟踪。'
  clearPersistedTaskSnapshot()
}

onMounted(async () => {
  const restored = restoreTaskSnapshot()
  if (!restored) return
  activeTaskId.value = String(restored.jobId || '')
  message.value = restored.actionMessage || '已恢复历史任务跟踪。'
  taskView.value = (restored.data?.taskView || null) as Record<string, any> | null
  if (!activeTaskId.value) return
  await refreshTask()
  const status = String(taskView.value?.status || restored.status || '').toLowerCase()
  if (status && status !== 'done' && status !== 'error' && status !== 'aborted') {
    startPolling()
  }
})

onBeforeUnmount(() => {
  stopPolling()
})
</script>
