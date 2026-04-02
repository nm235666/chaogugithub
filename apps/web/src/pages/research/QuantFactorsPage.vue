<template>
  <AppShell title="因子挖掘工作台" subtitle="QuantaAlpha 旁路：因子挖掘、回测、任务状态与结果统一查看。">
    <div class="space-y-4">
      <PageSection title="任务启动" subtitle="输入研究方向后，可启动 mine/backtest 任务。">
        <div class="grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <input v-model.trim="form.direction" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 xl:col-span-2" placeholder="研究方向，如：红利低波 + 价值增强" />
          <input v-model.number="form.lookback" type="number" min="1" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="lookback" />
          <select v-model="form.config_profile" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="default">default</option>
            <option value="conservative">conservative</option>
            <option value="aggressive">aggressive</option>
          </select>
          <input v-model.trim="form.llm_profile" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="llm_profile (auto)" />
        </div>
        <div class="mt-3 flex flex-wrap gap-2">
          <button class="rounded-2xl bg-indigo-700 px-4 py-2 text-white disabled:opacity-40" :disabled="!form.direction || mineMutation.isPending.value" @click="runMine">
            {{ mineMutation.isPending.value ? '启动中...' : '启动因子挖掘' }}
          </button>
          <button class="rounded-2xl bg-emerald-700 px-4 py-2 text-white disabled:opacity-40" :disabled="!form.direction || backtestMutation.isPending.value" @click="runBacktest">
            {{ backtestMutation.isPending.value ? '启动中...' : '启动回测' }}
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2" :disabled="!activeTaskId" @click="refreshTask">
            刷新任务
          </button>
        </div>
        <div v-if="message" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          {{ message }}
        </div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[1fr_1fr]">
        <PageSection title="当前任务" subtitle="轮询状态：pending / running / done / error。">
          <div v-if="!taskView" class="text-sm text-[var(--muted)]">暂无任务</div>
          <template v-else>
            <InfoCard :title="taskView.task_id || '-'" :meta="`${taskView.task_type || '-'} · ${taskView.job_key || '-'} · ${taskView.status || '-'}`" :description="taskErrorText">
              <template #badge>
                <StatusBadge :value="taskView.status || 'muted'" :label="taskView.status || '-'" />
              </template>
            </InfoCard>
            <div class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] p-3 text-xs text-[var(--muted)]">
              <pre class="overflow-x-auto whitespace-pre-wrap">{{ taskMetricsText }}</pre>
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
import { computed, onBeforeUnmount, reactive, ref } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchQuantResults, fetchQuantTask, startQuantBacktest, startQuantMine } from '../../services/api/quantFactors'

const form = reactive({
  direction: '',
  market_scope: 'A_share',
  lookback: 120,
  config_profile: 'default',
  llm_profile: 'auto',
})
const message = ref('')
const activeTaskId = ref('')
const taskView = ref<Record<string, any> | null>(null)
let pollTimer: number | null = null

const resultsQuery = useQuery({
  queryKey: ['quant-factor-results'],
  queryFn: () => fetchQuantResults({ page: 1, page_size: 20 }),
  refetchInterval: 20_000,
})

const mineMutation = useMutation({
  mutationFn: () => startQuantMine({ ...form }),
  onSuccess: async (payload) => {
    activeTaskId.value = String(payload?.task_id || '')
    message.value = `因子挖掘任务已启动：${activeTaskId.value || '-'}`
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
    await refreshTask()
    startPolling()
    resultsQuery.refetch()
  },
  onError: (error: Error) => {
    message.value = `启动失败：${error.message}`
  },
})

const results = computed<Array<Record<string, any>>>(() => (resultsQuery.data.value?.items || []) as Array<Record<string, any>>)
const taskErrorText = computed(() => {
  const item = taskView.value
  if (!item) return ''
  return String(item.error_message || item.error_code || '任务运行中')
})
const taskMetricsText = computed(() => {
  const item = taskView.value
  if (!item) return '{}'
  return JSON.stringify(
    {
      metrics: item.metrics || {},
      artifacts: item.artifacts || {},
      output: item.output || {},
    },
    null,
    2,
  )
})

function resultDescription(item: Record<string, any>) {
  const metrics = item.metrics || {}
  const arr = metrics.arr ?? '-'
  const mdd = metrics.mdd ?? '-'
  const calmar = metrics.calmar ?? '-'
  const ic = metrics.ic ?? '-'
  const rankIc = metrics.rank_ic ?? '-'
  return `ARR=${arr} · MDD=${mdd} · Calmar=${calmar} · IC=${ic} · RankIC=${rankIc}`
}

async function refreshTask() {
  const taskId = activeTaskId.value
  if (!taskId) return
  try {
    const payload = await fetchQuantTask(taskId)
    taskView.value = payload
    const status = String(payload?.status || '')
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

onBeforeUnmount(() => {
  stopPolling()
})
</script>
