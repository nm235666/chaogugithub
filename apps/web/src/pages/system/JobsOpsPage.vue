<template>
  <AppShell title="任务调度与观测中心" subtitle="统一查看 job 定义、运行记录、告警和 dry-run/触发结果。">
    <div class="space-y-4">
      <PageSection title="任务筛选与操作" subtitle="先按域/关键字定位任务，再做 dry-run 或手动触发。">
        <div class="grid gap-3 xl:grid-cols-[1fr_180px_180px_120px] md:grid-cols-2">
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="筛选 job_key / 名称" />
          <select v-model="filters.category" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部域</option>
            <option v-for="item in categoryOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model.number="runLimit" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="30">最近 30 条运行</option>
            <option :value="80">最近 80 条运行</option>
            <option :value="150">最近 150 条运行</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="refreshAll">刷新</button>
        </div>
        <div class="mt-3 text-sm text-[var(--muted)]">当前选中：{{ selectedJob?.job_key || '未选择' }}</div>
      </PageSection>

      <PageSection title="调度健康基线（最近样本）" subtitle="快速看成功率、平均耗时、失败 TopN。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <StatCard title="任务定义" :value="jobRows.length" hint="当前注册任务总数" />
          <StatCard title="成功率" :value="successRateText" hint="最近运行样本成功率" />
          <StatCard title="平均耗时" :value="avgDurationText" hint="单位秒（仅已完成）" />
          <StatCard title="未恢复告警" :value="alertsRows.length" hint="默认 unresolved_only=1" />
        </div>
        <div class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          失败任务 TopN：{{ failedTopNText }}
        </div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[1fr_1fr]">
        <PageSection :title="`任务定义 (${filteredJobs.length})`" subtitle="点击某条任务后，右侧可直接 dry-run/触发。">
          <div class="space-y-2">
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
          <div class="flex flex-wrap gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="!selectedJob || dryRunMutation.isPending.value" @click="doDryRun">
              {{ dryRunMutation.isPending.value ? 'dry-run 中...' : 'dry-run' }}
            </button>
            <button class="rounded-2xl bg-blue-700 px-4 py-2 text-white disabled:opacity-40" :disabled="!selectedJob || triggerMutation.isPending.value" @click="doTrigger">
              {{ triggerMutation.isPending.value ? '触发中...' : '手动触发' }}
            </button>
          </div>
          <div v-if="actionMessage" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
            {{ actionMessage }}
          </div>

          <div class="mt-4 space-y-2">
            <InfoCard
              v-for="cmd in dryRunCommands"
              :key="cmd"
              title="dry-run command"
              :meta="selectedJob?.job_key || '-'"
              :description="cmd"
            />
          </div>
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <PageSection :title="`最近运行 (${runsRows.length})`" subtitle="用于排查耗时、失败与运行态。">
          <div class="space-y-2">
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
          <div class="space-y-2">
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

const filters = reactive({
  keyword: '',
  category: '',
})
const runLimit = ref(80)
const selectedJob = ref<Record<string, any> | null>(null)
const dryRunCommands = ref<string[]>([])
const actionMessage = ref('')

const jobsQuery = useQuery({
  queryKey: ['jobs-definitions'],
  queryFn: fetchJobs,
  refetchInterval: 60_000,
})

const runsQuery = useQuery({
  queryKey: ['jobs-runs', runLimit],
  queryFn: () => fetchJobRuns({ limit: runLimit.value }),
  refetchInterval: 30_000,
})

const alertsQuery = useQuery({
  queryKey: ['jobs-alerts'],
  queryFn: () => fetchJobAlerts({ unresolved_only: 1, limit: 50 }),
  refetchInterval: 30_000,
})

const dryRunMutation = useMutation({
  mutationFn: (jobKey: string) => dryRunJob(jobKey),
  onSuccess: (payload) => {
    const commands = Array.isArray(payload?.commands) ? payload.commands : []
    dryRunCommands.value = commands.map((cmd: unknown) => Array.isArray(cmd) ? cmd.join(' ') : String(cmd || ''))
    actionMessage.value = `dry-run 完成：${payload?.job_key || ''}，命令数 ${dryRunCommands.value.length}`
  },
  onError: (error: Error) => {
    actionMessage.value = `dry-run 失败：${error.message}`
    dryRunCommands.value = []
  },
})

const triggerMutation = useMutation({
  mutationFn: (jobKey: string) => triggerJob(jobKey),
  onSuccess: async (payload) => {
    actionMessage.value = `触发完成：${payload?.job_key || ''} · status=${payload?.status || '-'} · run_id=${payload?.run_id || '-'}`
    await Promise.all([runsQuery.refetch(), alertsQuery.refetch()])
  },
  onError: (error: Error) => {
    actionMessage.value = `触发失败：${error.message}`
  },
})

const jobRows = computed<Array<Record<string, any>>>(() => (jobsQuery.data.value?.items || []) as Array<Record<string, any>>)
const runsRows = computed<Array<Record<string, any>>>(() => (runsQuery.data.value?.items || []) as Array<Record<string, any>>)
const alertsRows = computed<Array<Record<string, any>>>(() => (alertsQuery.data.value?.items || []) as Array<Record<string, any>>)

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
  actionMessage.value = ''
}

function doDryRun() {
  if (!selectedJob.value?.job_key) return
  dryRunMutation.mutate(String(selectedJob.value.job_key))
}

function doTrigger() {
  if (!selectedJob.value?.job_key) return
  triggerMutation.mutate(String(selectedJob.value.job_key))
}

function refreshAll() {
  jobsQuery.refetch()
  runsQuery.refetch()
  alertsQuery.refetch()
}
</script>
