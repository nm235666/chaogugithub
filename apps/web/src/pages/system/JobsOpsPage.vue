<template>
  <AppShell title="任务调度与观测中心" subtitle="统一查看 job 定义、运行记录、告警和 dry-run/触发结果。">
    <div class="space-y-4">
      <div class="page-hero-grid">
        <div class="page-hero-card">
          <div class="page-insight-label">Ops Console</div>
          <div class="page-hero-title">先判断调度健康，再进入单任务排障。</div>
          <div class="page-hero-copy">
            这页最重要的不是“触发按钮”，而是先看成功率、失败 TopN 和未恢复告警，确定到底是系统性异常还是单任务问题，再做 dry-run 或手动触发。
          </div>
          <div class="page-action-cluster">
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="refreshAll">刷新任务中心</button>
          </div>
        </div>
        <div class="page-insight-list">
          <div class="page-insight-item">
            <div class="page-insight-label">当前需要优先处理</div>
            <div class="page-insight-value">{{ alertsRows.length ? '未恢复告警' : '调度总体稳定' }}</div>
            <div class="page-insight-note">未恢复告警 {{ alertsRows.length }} 条；优先处理 error 再看 warning。</div>
          </div>
          <div class="page-insight-item">
            <div class="page-insight-label">单任务入口</div>
            <div class="page-insight-value">{{ selectedJob?.job_key || '先从左侧选择任务' }}</div>
            <div class="page-insight-note">优先 dry-run 验证命令，再决定是否真正触发。</div>
          </div>
        </div>
      </div>

      <PageSection title="任务筛选与操作" subtitle="先按域/关键字定位任务，再做 dry-run 或手动触发。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <label class="text-sm font-semibold text-[var(--ink)]">
            关键字
            <input v-model="filters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="筛选 job_key / 名称" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            任务域
            <select v-model="filters.category" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部域</option>
              <option v-for="item in categoryOptions" :key="item" :value="item">{{ item }}</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            样本范围
            <select v-model.number="runLimit" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option :value="30">最近 30 条运行</option>
              <option :value="80">最近 80 条运行</option>
              <option :value="150">最近 150 条运行</option>
            </select>
          </label>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="refreshAll">刷新</button>
        </div>
        <div class="mt-3 text-sm text-[var(--muted)]">当前选中：{{ selectedJob?.job_key || '未选择' }}</div>
      </PageSection>

      <PageSection title="调度健康基线（最近样本）" subtitle="快速看成功率、平均耗时、失败 TopN。">
        <div class="kpi-grid">
          <StatCard title="任务定义" :value="jobRows.length" hint="当前注册任务总数" />
          <StatCard title="成功率" :value="successRateText" hint="最近运行样本成功率" />
          <StatCard title="平均耗时" :value="avgDurationText" hint="单位秒（仅已完成）" />
          <StatCard title="未恢复告警" :value="alertsRows.length" hint="默认 unresolved_only=1" />
        </div>
        <div class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          失败任务 TopN：{{ failedTopNText }}
        </div>
        <div class="mt-2 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          最近失败趋势（按日期）：{{ failedTrendText }}
        </div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-2">
        <PageSection :title="`任务定义 (${filteredJobs.length})`" subtitle="点击某条任务后，右侧可直接 dry-run/触发。">
          <div class="table-lead">
            <div class="table-lead-copy">左侧列表用于定位任务，右侧才是操作区。建议先按域和关键字收敛，再点开单任务处理。</div>
            <div class="flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">任务域 {{ filters.category || '全部' }}</span>
              <span class="metric-chip">样本 {{ runLimit }} 条</span>
            </div>
          </div>
          <div class="max-h-[560px] space-y-2 overflow-auto pr-1">
            <InfoCard
              v-for="item in filteredJobs"
              :key="item.job_key"
              :title="item.job_key"
              :meta="`${item.category || '-'} · ${item.schedule_expr || '-'} · ${item.enabled ? 'enabled' : 'disabled'}`"
              :description="item.name || item.description || ''"
              @click="selectJob(item)"
            >
              <template #badge>
                <StatusBadge :value="selectedJob?.job_key === item.job_key ? 'info' : 'muted'" :label="selectedJob?.job_key === item.job_key ? '当前' : '选择'" />
              </template>
            </InfoCard>
          </div>
        </PageSection>

        <PageSection title="选中任务操作区" subtitle="优先 dry-run 验证命令，再按需触发执行。">
          <div class="surface-note mb-3">
            操作建议：先 `dry-run` 看命令是否合理，再做真实触发；如果最近运行区已经有连续失败，先排查不要盲点重试。
          </div>
          <div class="flex flex-wrap gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="!selectedJob || dryRunMutation.isPending.value || triggerMutation.isPending.value" @click="doDryRun">
              {{ dryRunMutation.isPending.value ? 'dry-run 中...' : 'dry-run' }}
            </button>
            <button class="rounded-2xl bg-blue-700 px-4 py-2 text-white disabled:opacity-40" :disabled="dryRunMutation.isPending.value || triggerMutation.isPending.value" @click="doTrigger">
              {{ triggerMutation.isPending.value ? '触发中...' : '手动触发' }}
            </button>
          </div>
          <div
            v-if="actionFeedback.text"
            class="mt-3 rounded-[18px] border px-4 py-3 text-sm"
            :class="actionFeedbackClass"
          >
            <div class="font-semibold">最近写操作反馈</div>
            <div class="mt-1">{{ actionFeedback.text }}</div>
            <div v-if="actionFeedback.confirmedAt" class="mt-1 text-xs opacity-80">
              最近确认时间：{{ actionFeedback.confirmedAt }}
            </div>
          </div>

          <div class="mt-4 space-y-2">
            <div
              v-for="cmd in dryRunCommands"
              :key="cmd"
              class="rounded-[18px] border border-[var(--line)] bg-white/80 px-3 py-3"
            >
              <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">
                dry-run command · {{ selectedJob?.job_key || '-' }}
              </div>
              <div class="text-sm text-[var(--ink)] break-all">{{ cmd }}</div>
              <div class="mt-2">
                <button class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)]" @click="copyCommand(cmd)">
                  复制命令
                </button>
              </div>
            </div>
          </div>
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-2">
        <PageSection :title="`最近运行 (${runsRows.length})`" subtitle="用于排查耗时、失败与运行态。">
          <div class="max-h-[560px] space-y-2 overflow-auto pr-1">
            <InfoCard
              v-for="item in runsRows"
              :key="item.id"
              :title="item.job_key || '-'"
              :meta="`#${item.id} · ${item.status || '-'} · ${formatDateTime(item.started_at)} · ${item.duration_seconds ?? '-'} 秒`"
              :description="item.error_text || `trigger=${item.trigger_mode || '-'} · exit=${item.exit_code ?? '-'}`"
            >
              <template #badge>
                <StatusBadge :value="item.status || 'muted'" :label="item.status || '-'" />
              </template>
            </InfoCard>
          </div>
        </PageSection>

        <PageSection :title="`未恢复告警 (${alertsRows.length})`" subtitle="优先处理 error 级别，再看 warning。">
          <div class="max-h-[560px] space-y-2 overflow-auto pr-1">
            <InfoCard
              v-for="item in alertsRows"
              :key="item.id"
              :title="item.job_key || '-'"
              :meta="`#${item.id} · ${item.severity || '-'} · run_id=${item.run_id || '-'} · ${formatDateTime(item.created_at)}`"
              :description="item.message || item.detail_text || ''"
            >
              <template #badge>
                <StatusBadge :value="item.severity || 'warn'" :label="item.severity || '-'" />
              </template>
              <div class="mt-3">
                <button class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)]" @click="copyDebugCommand(item)">
                  复制排障命令
                </button>
              </div>
            </InfoCard>
          </div>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import { dryRunJob, fetchJobAlerts, fetchJobRuns, fetchJobs, triggerJob } from '../../services/api/system'
import { formatDateTime, formatNumber } from '../../shared/utils/format'
import { useUiStore } from '../../stores/ui'

const filters = reactive({
  keyword: '',
  category: '',
})
const runLimit = ref(80)
const selectedJob = ref<Record<string, any> | null>(null)
const dryRunCommands = ref<string[]>([])
const actionFeedback = reactive<{ text: string; tone: 'success' | 'error' | 'info'; confirmedAt: string }>({
  text: '',
  tone: 'info',
  confirmedAt: '',
})
const ui = useUiStore()

const jobsQuery = useQuery({
  queryKey: ['jobs-definitions'],
  queryFn: fetchJobs,
  refetchInterval: () => (document.visibilityState === 'visible' ? 60_000 : 180_000),
})

const runsQuery = useQuery({
  queryKey: ['jobs-runs', runLimit],
  queryFn: () => fetchJobRuns({ limit: runLimit.value }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
})

const alertsQuery = useQuery({
  queryKey: ['jobs-alerts'],
  queryFn: () => fetchJobAlerts({ unresolved_only: 1, limit: 50 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
})

const dryRunMutation = useMutation({
  mutationFn: (jobKey: string) => dryRunJob(jobKey),
  onSuccess: (payload) => {
    const commands = Array.isArray(payload?.commands) ? payload.commands : []
    dryRunCommands.value = commands.map((cmd: unknown) => Array.isArray(cmd) ? cmd.join(' ') : String(cmd || ''))
    const successMessage = `dry-run 完成：${payload?.job_key || ''}，命令数 ${dryRunCommands.value.length}`
    setActionFeedback(successMessage, 'success')
    ui.showToast('dry-run 已完成', 'success')
  },
  onError: (error: Error) => {
    dryRunCommands.value = []
    const failureMessage = `dry-run 失败：${resolveActionError(error)}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  },
})

const triggerMutation = useMutation({
  mutationFn: (jobKey: string) => triggerJob(jobKey),
  onSuccess: async (payload) => {
    const successMessage = `触发完成：${payload?.job_key || ''} · status=${payload?.status || '-'} · run_id=${payload?.run_id || '-'}`
    setActionFeedback(successMessage, 'success')
    ui.showToast('任务触发已提交', 'info')
    await Promise.all([runsQuery.refetch(), alertsQuery.refetch()])
  },
  onError: (error: Error) => {
    const failureMessage = `触发失败：${resolveActionError(error)}`
    setActionFeedback(failureMessage, 'error')
    ui.showToast(failureMessage, 'error')
  },
})

const jobRows = computed<Array<Record<string, any>>>(() => (jobsQuery.data.value?.items || []) as Array<Record<string, any>>)
const runsRows = computed<Array<Record<string, any>>>(() => (runsQuery.data.value?.items || []) as Array<Record<string, any>>)
const alertsRows = computed<Array<Record<string, any>>>(() => (alertsQuery.data.value?.items || []) as Array<Record<string, any>>)
const actionFeedbackClass = computed(() => {
  if (actionFeedback.tone === 'success') return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  if (actionFeedback.tone === 'error') return 'border-red-200 bg-red-50 text-red-700'
  return 'border-[var(--line)] bg-[rgba(255,255,255,0.72)] text-[var(--muted)]'
})

function resolveActionError(error: unknown) {
  return String((error as any)?.response?.data?.error || (error as Error)?.message || 'unknown')
}

function setActionFeedback(text: string, tone: 'success' | 'error' | 'info' = 'info') {
  actionFeedback.text = text
  actionFeedback.tone = tone
  actionFeedback.confirmedAt = new Date().toLocaleString('zh-CN', { hour12: false })
}

const categoryOptions = computed(() => {
  const set = new Set<string>()
  jobRows.value.forEach((item) => {
    const category = String(item.category || '').trim()
    if (category) set.add(category)
  })
  return Array.from(set).sort()
})

const filteredJobs = computed(() => {
  const keyword = filters.keyword.trim().toLowerCase()
  const category = filters.category.trim()
  return jobRows.value.filter((item) => {
    if (category && String(item.category || '') !== category) return false
    if (!keyword) return true
    const haystack = `${item.job_key || ''} ${item.name || ''} ${item.description || ''}`.toLowerCase()
    return haystack.includes(keyword)
  })
})

const successRateText = computed(() => {
  if (!runsRows.value.length) return '-'
  const success = runsRows.value.filter((item) => String(item.status || '') === 'success').length
  return `${formatNumber((success / runsRows.value.length) * 100, 1)}%`
})

const avgDurationText = computed(() => {
  const durations = runsRows.value
    .map((item) => Number(item.duration_seconds))
    .filter((value) => Number.isFinite(value) && value >= 0)
  if (!durations.length) return '-'
  const avg = durations.reduce((sum, value) => sum + value, 0) / durations.length
  return `${formatNumber(avg, 2)} 秒`
})

const failedTopNText = computed(() => {
  const byJob = new Map<string, number>()
  runsRows.value
    .filter((item) => String(item.status || '') !== 'success')
    .forEach((item) => {
      const jobKey = String(item.job_key || '-')
      byJob.set(jobKey, (byJob.get(jobKey) || 0) + 1)
    })
  const pairs = Array.from(byJob.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5)
  if (!pairs.length) return '暂无失败任务'
  return pairs.map(([jobKey, count]) => `${jobKey} (${count})`).join('，')
})
const failedTrendText = computed(() => {
  const byDate = new Map<string, number>()
  runsRows.value
    .filter((item) => String(item.status || '') !== 'success')
    .forEach((item) => {
      const raw = String(item.started_at || item.created_at || '').trim()
      const day = raw ? raw.slice(0, 10) : 'unknown'
      byDate.set(day, (byDate.get(day) || 0) + 1)
    })
  const items = Array.from(byDate.entries())
    .sort((a, b) => String(b[0]).localeCompare(String(a[0])))
    .slice(0, 5)
    .reverse()
  if (!items.length) return '暂无失败样本'
  return items.map(([day, count]) => `${day}:${count}`).join(' | ')
})

watch(
  () => filteredJobs.value,
  (items) => {
    if (!items.length) {
      selectedJob.value = null
      return
    }
    if (!selectedJob.value) {
      selectedJob.value = items[0]
      return
    }
    const keep = items.find((item) => item.job_key === selectedJob.value?.job_key)
    if (!keep) selectedJob.value = items[0]
  },
  { immediate: true },
)

function selectJob(item: Record<string, any>) {
  selectedJob.value = item
  dryRunCommands.value = []
}

function doDryRun() {
  if (!selectedJob.value?.job_key) return
  dryRunMutation.mutate(String(selectedJob.value.job_key))
}

function doTrigger() {
  if (!selectedJob.value?.job_key) {
    setActionFeedback('触发失败：请先从任务列表中选择一个任务。', 'error')
    return
  }
  triggerMutation.mutate(String(selectedJob.value.job_key))
}

function refreshAll() {
  jobsQuery.refetch()
  runsQuery.refetch()
  alertsQuery.refetch()
  ui.showToast('任务中心数据已刷新', 'success')
}

async function copyCommand(command: string) {
  try {
    await navigator.clipboard.writeText(command)
    ui.showToast('命令已复制', 'success')
  } catch {
    ui.showToast('复制失败，请手动复制', 'error')
  }
}

function buildDebugCommand(item: Record<string, any>) {
  const jobKey = String(item.job_key || '').trim()
  if (!jobKey) return ''
  return `python3 /home/zanbo/zanbotest/job_orchestrator.py runs --job-key ${jobKey} --limit 20`
}

function copyDebugCommand(item: Record<string, any>) {
  const command = buildDebugCommand(item)
  if (!command) {
    ui.showToast('当前告警缺少 job_key，无法生成命令', 'error')
    return
  }
  copyCommand(command)
}
</script>
