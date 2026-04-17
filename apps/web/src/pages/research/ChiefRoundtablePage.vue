<template>
  <AppShell title="首席圆桌" subtitle="三位首席分析师多空辩论，为中性裁决提供第二意见。">
    <div class="space-y-4">
      <!-- Input -->
      <PageSection title="发起圆桌" subtitle="输入股票代码，三位首席（成长派 / 价值派 / 宏观策略派）将独立给出立场并由主持人综合裁决。">
        <div class="grid gap-3 xl:grid-cols-[1fr_200px_180px] md:grid-cols-2">
          <label class="text-sm font-semibold text-[var(--ink)]">
            股票代码
            <input
              v-model.trim="tsCodeInput"
              class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
              placeholder="如 000001.SZ"
              @keydown.enter="submitJob"
            />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            关联多角色任务（可选）
            <input
              v-model.trim="sourceJobIdInput"
              class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
              placeholder="multi_role_v3 job_id"
            />
          </label>
          <button
            class="mt-auto rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-50"
            :disabled="isSubmitting || !tsCodeInput"
            @click="submitJob"
          >
            {{ isSubmitting ? '创建中...' : '发起圆桌' }}
          </button>
        </div>
        <div class="mt-3 rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)]">
          {{ statusMessage }}
        </div>
        <div v-if="currentJobId" class="mt-2 flex flex-wrap gap-2">
          <button
            class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)]"
            @click="clearJob"
          >
            清除当前任务
          </button>
        </div>
      </PageSection>

      <!-- Job status -->
      <PageSection title="任务状态" subtitle="实时轮询，任务完成后自动展示三位首席立场与综合裁决。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <InfoCard title="任务 ID" :meta="currentJobId || '-'" :description="jobStatus ? `状态: ${jobStatus}` : '等待创建任务'">
            <template #badge>
              <StatusBadge :value="jobStatusTone" :label="jobStatusLabel" />
            </template>
          </InfoCard>
          <InfoCard title="股票" :meta="jobData?.ts_code || '-'" :description="jobData?.context?.stock_name || '等待任务完成'" />
          <InfoCard title="当前阶段" :meta="jobData?.stage || '-'" :description="jobData?.error || '无错误'" />
          <InfoCard title="触发方式" :meta="jobData?.trigger || '-'" :description="jobData?.source_job_id ? `关联任务: ${jobData.source_job_id}` : '无关联任务'" />
        </div>
      </PageSection>

      <!-- Chief positions -->
      <PageSection
        v-if="jobDone && positions && Object.keys(positions).length"
        title="三位首席立场"
        subtitle="每位首席基于各自哲学独立发表多空立场，观点不互相影响。"
      >
        <div class="grid gap-3 xl:grid-cols-3 md:grid-cols-2">
          <div
            v-for="(pos, key) in positions"
            :key="key"
            class="rounded-2xl border-2 p-4"
            :class="chiefCardClass(pos.position)"
          >
            <div class="mb-2 flex items-start justify-between gap-2">
              <div>
                <div class="text-xs font-semibold uppercase tracking-wide text-[var(--muted)]">{{ pos.philosophy_short || key }}</div>
                <div class="mt-0.5 text-base font-bold text-[var(--ink)]">{{ pos.name }}</div>
              </div>
              <span class="shrink-0 rounded-full px-3 py-1 text-sm font-bold text-white" :class="positionBadgeClass(pos.position)">
                {{ positionLabel(pos.position) }}
              </span>
            </div>
            <div class="mt-2 text-sm font-medium text-[var(--ink)]">{{ pos.argument }}</div>
            <div class="mt-2 flex items-start gap-2 text-sm text-[var(--muted)]">
              <span class="mt-0.5 shrink-0 rounded bg-amber-100 px-1.5 py-0.5 text-xs font-semibold text-amber-700">顾虑</span>
              <span>{{ pos.concern }}</span>
            </div>
            <div class="mt-3 flex items-center justify-between text-xs text-[var(--muted)]">
              <span>置信度</span>
              <span class="font-semibold text-[var(--ink)]">{{ pos.confidence ?? '-' }}</span>
            </div>
            <div class="mt-1 h-1.5 w-full rounded-full bg-[var(--panel-soft)]">
              <div class="h-1.5 rounded-full transition-all" :class="confidenceBarClass(pos.position)" :style="{ width: `${pos.confidence ?? 0}%` }" />
            </div>
          </div>
        </div>
      </PageSection>

      <!-- Synthesis verdict -->
      <PageSection
        v-if="jobDone && synthesis && synthesis.verdict"
        title="综合裁决"
        subtitle="圆桌主持人综合三位首席立场后给出的最终方向判断。"
      >
        <div
          class="rounded-2xl border-2 p-5 transition-all"
          :class="verdictCardClass(synthesis.direction)"
        >
          <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div class="text-xs font-semibold uppercase tracking-wide text-[var(--muted)]">最终方向</div>
              <div class="mt-1 text-2xl font-bold" :class="verdictTextClass(synthesis.direction)">
                {{ positionLabel(synthesis.direction) }}
              </div>
            </div>
            <div class="flex flex-wrap gap-2">
              <span class="rounded-full px-3 py-1 text-sm font-bold text-white" :class="positionBadgeClass(synthesis.direction)">
                {{ positionLabel(synthesis.direction) }}
              </span>
              <span class="rounded-full border border-[var(--line)] bg-white px-3 py-1 text-sm font-semibold text-[var(--ink)]">
                {{ consensusLabel(synthesis.consensus) }}
              </span>
            </div>
          </div>
          <div class="mt-4 rounded-xl bg-white/80 px-4 py-3 text-sm font-medium text-[var(--ink)]">
            {{ synthesis.verdict }}
          </div>
          <div v-if="synthesis.majority_argument" class="mt-3 text-sm text-[var(--muted)]">
            <span class="mr-1 font-semibold text-[var(--ink)]">主流逻辑：</span>{{ synthesis.majority_argument }}
          </div>
          <div v-if="synthesis.dissent && synthesis.dissent !== '无明显分歧'" class="mt-3 flex items-start gap-2 text-sm text-[var(--muted)]">
            <span class="mt-0.5 shrink-0 rounded bg-amber-100 px-1.5 py-0.5 text-xs font-semibold text-amber-700">少数意见</span>
            <span>{{ synthesis.dissent }}</span>
          </div>
          <div class="mt-4 flex flex-wrap items-center gap-3 border-t border-[var(--line)] pt-4">
            <button
              class="rounded-full px-4 py-2 text-sm font-semibold text-white disabled:opacity-50"
              :class="positionBadgeClass(synthesis.direction)"
              :disabled="saveToDecisionBoardPending || saveToDecisionBoardDone"
              @click="saveToDecisionBoard"
            >
              {{ saveToDecisionBoardPending ? '记录中...' : saveToDecisionBoardDone ? '已记录到决策板 ✓' : '记录到决策板' }}
            </button>
            <span v-if="saveToDecisionBoardError" class="text-xs text-red-600">{{ saveToDecisionBoardError }}</span>
            <span v-else-if="saveToDecisionBoardDone && saveToDecisionBoardReceipt" class="text-xs text-emerald-700">
              已写入 {{ saveToDecisionBoardReceipt.source || 'decision_board' }} · {{ saveToDecisionBoardReceipt.trace?.action_id || saveToDecisionBoardReceipt.trace?.run_id || '-' }}
            </span>
          </div>
        </div>
      </PageSection>

      <!-- History list -->
      <PageSection title="近期圆桌任务" subtitle="按时间倒序，点击任务 ID 可恢复到该任务状态。">
        <div class="mb-3 flex flex-wrap gap-3">
          <input
            v-model.trim="historyTsCode"
            class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm"
            placeholder="按股票筛选，如 000001.SZ"
            @keydown.enter="historyQuery.refetch()"
          />
          <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]" @click="historyQuery.refetch()">
            刷新
          </button>
        </div>
        <DataTable
          :columns="historyColumns"
          :rows="historyItems"
          row-key="job_id"
          empty-text="暂无圆桌任务记录"
          caption="圆桌任务历史"
        >
          <template #cell-job_id="{ row }">
            <button
              class="rounded-full border border-[var(--line)] bg-white px-2 py-0.5 text-xs font-semibold text-[var(--brand)] hover:border-[var(--brand)]"
              @click="restoreJob(row.job_id)"
            >
              {{ String(row.job_id).slice(0, 8) }}…
            </button>
          </template>
          <template #cell-status="{ row }">
            <StatusBadge :value="jobToneByStatus(row.status)" :label="row.status" />
          </template>
          <template #cell-direction="{ row }">
            <span v-if="row.synthesis?.direction" class="rounded-full px-2.5 py-0.5 text-xs font-bold text-white" :class="positionBadgeClass(row.synthesis.direction)">
              {{ positionLabel(row.synthesis.direction) }}
            </span>
            <span v-else class="text-[var(--muted)]">-</span>
          </template>
        </DataTable>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import { createRoundtableJob, getRoundtableJob, listRoundtableJobs } from '../../services/api/roundtable'
import { recordDecisionAction, type DecisionTraceReceipt } from '../../services/api/decision'

const route = useRoute()

const tsCodeInput = ref('')
const sourceJobIdInput = ref('')
const currentJobId = ref('')
const statusMessage = ref('输入股票代码后点击"发起圆桌"。')
const historyTsCode = ref('')
const saveToDecisionBoardPending = ref(false)
const saveToDecisionBoardDone = ref(false)
const saveToDecisionBoardError = ref('')
const saveToDecisionBoardReceipt = ref<DecisionTraceReceipt | null>(null)

// -- polling --
let pollTimer: ReturnType<typeof setInterval> | null = null
const jobData = ref<Record<string, any> | null>(null)

const jobStatus = computed(() => String(jobData.value?.status || ''))
const jobDone = computed(() => jobStatus.value === 'done')
const positions = computed<Record<string, any>>(() => jobData.value?.positions ?? {})
const synthesis = computed<Record<string, any>>(() => jobData.value?.synthesis ?? {})

const jobStatusTone = computed(() => jobToneByStatus(jobStatus.value))
const jobStatusLabel = computed(() => jobStatus.value || '等待')

function jobToneByStatus(status: string) {
  if (status === 'done') return 'success'
  if (status === 'error') return 'error'
  if (status === 'running') return 'warning'
  if (status === 'queued') return 'muted'
  return 'muted'
}

async function pollJob() {
  if (!currentJobId.value) return
  try {
    const data = await getRoundtableJob(currentJobId.value)
    if (data?.ok === false) return
    jobData.value = data?.job_id ? data : (data as any)
    if (jobStatus.value === 'done') {
      statusMessage.value = `圆桌任务完成（${currentJobId.value.slice(0, 8)}）。`
      stopPolling()
    } else if (jobStatus.value === 'error') {
      statusMessage.value = `任务出错：${jobData.value?.error || '未知错误'}`
      stopPolling()
    }
  } catch {
    // swallow polling errors
  }
}

function startPolling() {
  stopPolling()
  pollTimer = setInterval(pollJob, 3000)
}

function stopPolling() {
  if (pollTimer !== null) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

onUnmounted(stopPolling)

// -- submit --
const isSubmitting = ref(false)

async function submitJob() {
  const ts = tsCodeInput.value.trim().toUpperCase()
  if (!ts) return
  isSubmitting.value = true
  statusMessage.value = '正在创建圆桌任务...'
  try {
    const res = await createRoundtableJob({
      ts_code: ts,
      trigger: 'manual',
      source_job_id: sourceJobIdInput.value.trim() || undefined,
    })
    if (res?.ok === false) throw new Error(res.error || '创建失败')
    currentJobId.value = res.job_id || res?.job?.job_id || ''
    jobData.value = res.job ?? null
    statusMessage.value = `任务已创建（${currentJobId.value.slice(0, 8)}…），正在等待 worker 处理...`
    saveToDecisionBoardDone.value = false
    saveToDecisionBoardError.value = ''
    saveToDecisionBoardReceipt.value = null
    startPolling()
  } catch (err: any) {
    statusMessage.value = `创建失败：${err?.message || String(err)}`
  } finally {
    isSubmitting.value = false
  }
}

async function restoreJob(jobId: string) {
  if (!jobId) return
  currentJobId.value = jobId
  statusMessage.value = `正在加载任务 ${jobId.slice(0, 8)}…`
  saveToDecisionBoardDone.value = false
  saveToDecisionBoardError.value = ''
  saveToDecisionBoardReceipt.value = null
  try {
    const data = await getRoundtableJob(jobId)
    jobData.value = data?.job_id ? data : (data as any)
    if (jobStatus.value !== 'done' && jobStatus.value !== 'error') {
      startPolling()
    } else {
      statusMessage.value = jobStatus.value === 'done' ? '任务已完成。' : `任务出错：${jobData.value?.error || ''}`
    }
  } catch (err: any) {
    statusMessage.value = `加载失败：${err?.message || String(err)}`
  }
}

function clearJob() {
  currentJobId.value = ''
  jobData.value = null
  statusMessage.value = '已清除当前任务。'
  stopPolling()
}

// -- history --
const historyQuery = useQuery({
  queryKey: computed(() => ['roundtable-history', historyTsCode.value]),
  queryFn: () => listRoundtableJobs({ page: 1, page_size: 20, ts_code: historyTsCode.value }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
})

const historyItems = computed<Array<Record<string, any>>>(() => (historyQuery.data.value as any)?.items ?? [])

const historyColumns = [
  { key: 'job_id', label: '任务 ID' },
  { key: 'ts_code', label: '股票' },
  { key: 'status', label: '状态' },
  { key: 'direction', label: '裁决方向' },
  { key: 'trigger', label: '触发方式' },
  { key: 'created_at', label: '创建时间' },
]

// -- decision board save --
async function saveToDecisionBoard() {
  if (!jobData.value || !synthesis.value?.direction) return
  saveToDecisionBoardPending.value = true
  saveToDecisionBoardError.value = ''
  saveToDecisionBoardReceipt.value = null
  try {
    const direction = synthesis.value.direction
    const action_type = direction === 'bullish' ? 'confirm' : direction === 'bearish' ? 'reject' : 'defer'
    const data = await recordDecisionAction({
      action_type,
      ts_code: jobData.value.ts_code || '',
      stock_name: jobData.value.context?.stock_name || '',
      note: synthesis.value.verdict || '',
      context: {
        source: 'chief_roundtable',
        job_id: currentJobId.value,
        direction,
        consensus: synthesis.value.consensus,
        majority_argument: synthesis.value.majority_argument || null,
        dissent: synthesis.value.dissent || null,
        source_job_id: jobData.value.source_job_id || '',
      },
    })
    saveToDecisionBoardReceipt.value = ((data as Record<string, any>)?.receipt || null) as DecisionTraceReceipt | null
    saveToDecisionBoardDone.value = true
  } catch (err: any) {
    saveToDecisionBoardError.value = `保存失败：${err?.message || String(err)}`
  } finally {
    saveToDecisionBoardPending.value = false
  }
}

// -- style helpers --
function positionLabel(pos: string) {
  if (pos === 'bullish') return '看多'
  if (pos === 'bearish') return '看空'
  if (pos === 'neutral') return '中性'
  return pos || '-'
}

function positionBadgeClass(pos: string) {
  if (pos === 'bullish') return 'bg-emerald-500'
  if (pos === 'bearish') return 'bg-red-500'
  return 'bg-stone-400'
}

function chiefCardClass(pos: string) {
  if (pos === 'bullish') return 'border-emerald-300 bg-emerald-50/40'
  if (pos === 'bearish') return 'border-red-300 bg-red-50/40'
  return 'border-[var(--line)] bg-white'
}

function verdictCardClass(direction: string) {
  if (direction === 'bullish') return 'border-emerald-400 bg-emerald-50/30'
  if (direction === 'bearish') return 'border-red-400 bg-red-50/30'
  return 'border-[var(--line)] bg-white'
}

function verdictTextClass(direction: string) {
  if (direction === 'bullish') return 'text-emerald-700'
  if (direction === 'bearish') return 'text-red-700'
  return 'text-[var(--ink)]'
}

function confidenceBarClass(pos: string) {
  if (pos === 'bullish') return 'bg-emerald-500'
  if (pos === 'bearish') return 'bg-red-500'
  return 'bg-stone-400'
}

function consensusLabel(consensus: string) {
  if (consensus === 'agree') return '观点一致'
  if (consensus === 'split') return '存在分歧'
  if (consensus === 'oppose') return '观点对立'
  return consensus || '-'
}

// -- deep-link restore --
onMounted(() => {
  const restoreId = String(route.query.job_id || '').trim()
  if (restoreId) {
    restoreJob(restoreId)
    return
  }
  // Pre-fill form from query params (e.g. from "升级到首席圆桌" button)
  const preTs = String(route.query.ts_code || '').trim().toUpperCase()
  const preSource = String(route.query.source_job_id || '').trim()
  if (preTs) tsCodeInput.value = preTs
  if (preSource) sourceJobIdInput.value = preSource
})
</script>
