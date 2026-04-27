<template>
  <AppShell title="Agent 运营台" subtitle="查看 Agent 运行、工具步骤、审批状态与 MCP 审计追踪。">
    <div class="space-y-4">
      <PageSection title="栈健康" subtitle="GET /api/agents/health；MCP 探测依赖 runtime_env 中的 MCP_* 变量。">
        <div class="flex flex-wrap items-center gap-2">
          <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" type="button" @click="healthQuery.refetch()">
            刷新探活
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" type="button" @click="exportMcpAudit">
            导出 MCP 审计 JSON
          </button>
        </div>
        <pre class="mt-2 max-h-[200px] overflow-auto rounded-xl border border-[var(--line)] bg-[var(--panel-soft)] p-3 text-xs text-[var(--muted)]">{{ pretty(healthQuery.data.value) }}</pre>
      </PageSection>

      <PageSection title="运行入口" subtitle="手动创建闭环 Agent run，worker 会处理 queued 状态。漏斗类可在 goal 中传 score_date / max_candidates。">
        <div class="grid gap-3 md:grid-cols-[1fr_1fr_auto]">
          <label class="text-sm font-semibold text-[var(--ink)]">
            Agent
            <select v-model="selectedAgentKey" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="funnel_progress_agent">funnel_progress_agent</option>
              <option value="portfolio_reconcile_agent">portfolio_reconcile_agent</option>
              <option value="portfolio_review_agent">portfolio_review_agent</option>
              <option value="decision_orchestrator_agent">decision_orchestrator_agent</option>
              <option value="memory_refresh_agent">memory_refresh_agent</option>
              <option value="job_failure_diag_agent">job_failure_diag_agent</option>
              <option value="decision_ops_read_agent">decision_ops_read_agent</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            运行筛选
            <select v-model="filters.agent_key" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部 Agent</option>
              <option value="funnel_progress_agent">funnel_progress_agent</option>
              <option value="portfolio_reconcile_agent">portfolio_reconcile_agent</option>
              <option value="portfolio_review_agent">portfolio_review_agent</option>
              <option value="decision_orchestrator_agent">decision_orchestrator_agent</option>
              <option value="memory_refresh_agent">memory_refresh_agent</option>
              <option value="job_failure_diag_agent">job_failure_diag_agent</option>
              <option value="decision_ops_read_agent">decision_ops_read_agent</option>
            </select>
          </label>
          <button class="self-end rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-50" :disabled="startMutation.isPending.value" @click="startRun">
            {{ startMutation.isPending.value ? '创建中...' : '创建 run' }}
          </button>
        </div>
        <label class="mt-3 block text-sm font-semibold text-[var(--ink)]">
          goal（JSON，可选）
          <textarea
            v-model="goalJson"
            class="mt-1 min-h-[120px] w-full rounded-2xl border border-[var(--line)] bg-white px-3 py-2 font-mono text-xs"
            placeholder='例如 {"score_date":"20260423","max_candidates":2000}'
          />
        </label>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
        <PageSection :title="`Agent Runs (${runs.length})`" subtitle="点击一条运行查看 steps 和审批动作。">
          <div class="mb-3 flex flex-wrap items-center gap-2">
            <select v-model="filters.status" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
              <option value="">全部状态</option>
              <option value="queued">queued</option>
              <option value="running">running</option>
              <option value="waiting_approval">waiting_approval</option>
              <option value="succeeded">succeeded</option>
              <option value="failed">failed</option>
              <option value="cancelled">cancelled</option>
            </select>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" @click="refreshRuns">刷新</button>
          </div>
          <div class="max-h-[680px] space-y-2 overflow-auto pr-1">
            <InfoCard
              v-for="run in runs"
              :key="run.id"
              :title="run.agent_key"
              :meta="`${run.status} · ${run.trigger_source || '-'} · ${formatDateTime(run.created_at)}`"
              :description="runSummary(run)"
              @click="selectRun(run.id)"
            >
              <template #badge>
                <StatusBadge :value="badgeValue(run.status)" :label="run.status || '-'" />
              </template>
              <div class="mt-2 flex flex-wrap gap-2 text-xs">
                <span class="metric-chip">changed {{ Number(run.result?.changed_count || 0) }}</span>
                <span v-if="run.approval_required" class="metric-chip">approval</span>
                <span class="metric-chip">{{ run.id.slice(0, 18) }}</span>
                <span v-if="run.correlation_id" class="metric-chip">corr {{ run.correlation_id.slice(0, 16) }}</span>
                <span v-if="run.metadata?.job_key" class="metric-chip">job {{ run.metadata.job_key }}</span>
              </div>
            </InfoCard>
            <div v-if="!runs.length && !runsQuery.isPending.value" class="surface-note">暂无 Agent 运行记录。</div>
          </div>
        </PageSection>

        <PageSection title="运行详情" subtitle="查看工具步骤、审计 id、待批准动作和错误。">
          <div v-if="!activeRun" class="surface-note">从左侧选择一个 run。</div>
          <div v-else class="space-y-4">
            <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3">
              <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div class="text-sm font-bold text-[var(--ink)]">{{ activeRun.agent_key }}</div>
                  <div class="mt-1 text-xs text-[var(--muted)]">{{ activeRun.id }}</div>
                </div>
                <StatusBadge :value="badgeValue(activeRun.status)" :label="activeRun.status" />
              </div>
              <div class="mt-3 grid gap-2 text-sm md:grid-cols-3">
                <div class="metric-chip">trigger {{ activeRun.trigger_source || '-' }}</div>
                <div class="metric-chip">changed {{ Number(activeRun.result?.changed_count || 0) }}</div>
                <div class="metric-chip">steps {{ activeRun.steps?.length || 0 }}</div>
                <div v-if="activeRun.correlation_id" class="metric-chip md:col-span-2">corr {{ activeRun.correlation_id }}</div>
                <div v-if="activeRun.parent_run_id" class="metric-chip">parent {{ activeRun.parent_run_id.slice(0, 18) }}</div>
              </div>
            </div>

            <div v-if="activeRun.status === 'waiting_approval'" class="rounded-[18px] border border-amber-200 bg-amber-50 px-4 py-3">
              <div class="text-sm font-bold text-amber-900">审批动作</div>
              <textarea v-model="approvalReason" class="mt-2 min-h-[88px] w-full rounded-2xl border border-amber-200 bg-white px-3 py-2 text-sm" placeholder="填写批准或拒绝原因" />
              <div class="mt-2 flex flex-wrap gap-2">
                <button class="rounded-2xl bg-emerald-700 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50" :disabled="approvalBusy || !approvalReason.trim()" @click="approveRun">
                  批准并执行
                </button>
                <button class="rounded-2xl bg-rose-700 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50" :disabled="approvalBusy || !approvalReason.trim()" @click="rejectRun">
                  拒绝
                </button>
              </div>
            </div>

            <div class="space-y-2">
              <InfoCard
                v-for="step in activeRun.steps || []"
                :key="step.id"
                :title="`${step.step_index}. ${step.tool_name}`"
                :meta="`${step.status} · ${step.dry_run ? 'dry-run' : 'write'} · audit=${step.audit_id || '-'}`"
                :description="step.error_text || stepDescription(step)"
              >
                <template #badge>
                  <StatusBadge :value="badgeValue(step.status)" :label="step.status || '-'" />
                </template>
              </InfoCard>
            </div>

            <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3">
              <div class="text-sm font-bold text-[var(--ink)]">结果摘要</div>
              <pre class="mt-2 max-h-[260px] overflow-auto whitespace-pre-wrap text-xs text-[var(--muted)]">{{ pretty(activeRun.result) }}</pre>
            </div>

            <div v-if="activeRun.correlation_id" class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3">
              <div class="flex items-center justify-between gap-3">
                <div class="text-sm font-bold text-[var(--ink)]">链路时间线</div>
                <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold" @click="timelineQuery.refetch()">刷新</button>
              </div>
              <div class="mt-3 max-h-[320px] space-y-2 overflow-auto">
                <div v-for="event in timelineEvents" :key="`${event.type}-${event.at}-${event.run_id}-${event.tool_name || event.message_type || ''}`" class="rounded-xl border border-[var(--line)] px-3 py-2 text-xs">
                  <div class="flex flex-wrap items-center gap-2">
                    <StatusBadge :value="badgeValue(event.status || event.type)" :label="event.type" />
                    <span class="font-semibold text-[var(--ink)]">{{ event.agent_key || event.tool_name || event.message_type || '-' }}</span>
                    <span class="text-[var(--muted)]">{{ formatDateTime(event.at) }}</span>
                  </div>
                  <div class="mt-1 text-[var(--muted)]">
                    {{ event.tool_name || event.message_type || event.status || '-' }}
                    <span v-if="event.audit_id"> · audit={{ event.audit_id }}</span>
                    <span v-if="event.run_id"> · {{ event.run_id.slice(0, 18) }}</span>
                  </div>
                </div>
                <div v-if="!timelineEvents.length && !timelineQuery.isPending.value" class="surface-note">暂无链路事件。</div>
              </div>
            </div>
          </div>
        </PageSection>
      </div>

      <PageSection title="质量记忆" subtitle="来自复盘结论、失败模式和受控修复审计的 Agent 记忆。">
        <div class="mb-3 flex flex-wrap items-center gap-2">
          <input v-model="memoryFilters.ts_code" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm" placeholder="ts_code" />
          <select v-model="memoryFilters.memory_type" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
            <option value="">全部类型</option>
            <option value="effective_signal">effective_signal</option>
            <option value="failed_signal">failed_signal</option>
            <option value="execution_slippage">execution_slippage</option>
            <option value="review_rule_correction">review_rule_correction</option>
            <option value="agent_failure_pattern">agent_failure_pattern</option>
          </select>
          <select v-model="memoryFilters.status" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
            <option value="active">active</option>
            <option value="">全部状态</option>
            <option value="muted">muted</option>
            <option value="superseded">superseded</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" @click="memoryQuery.refetch()">刷新记忆</button>
        </div>
        <div class="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
          <InfoCard
            v-for="item in memoryItems"
            :key="item.id"
            :title="item.memory_type"
            :meta="`${item.status || '-'} · ${item.ts_code || item.scope || '-'} · ${formatDateTime(item.updated_at || item.created_at)}`"
            :description="item.summary || '-'"
          >
            <template #badge>
              <StatusBadge :value="badgeValue(item.status || 'active')" :label="item.status || 'active'" />
            </template>
            <div class="mt-2 flex flex-wrap gap-2 text-xs">
              <span v-if="item.source_agent_key" class="metric-chip">{{ item.source_agent_key }}</span>
              <span class="metric-chip">score {{ Number(item.score || 0).toFixed(2) }}</span>
            </div>
          </InfoCard>
          <div v-if="!memoryItems.length && !memoryQuery.isPending.value" class="surface-note">暂无质量记忆。</div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import {
  approveAgentRun,
  fetchAgentRun,
  fetchAgentMemoryItems,
  fetchAgentRuns,
  fetchAgentStackHealth,
  fetchAgentTimeline,
  fetchMcpToolAudit,
  rejectAgentRun,
  startAgentRun,
  type AgentRun,
  type AgentStep,
} from '../../services/api/agents'
import { formatDateTime } from '../../shared/utils/format'
import { useUiStore } from '../../stores/ui'

const queryClient = useQueryClient()
const ui = useUiStore()
const selectedAgentKey = ref('portfolio_reconcile_agent')
const selectedRunId = ref('')
const approvalReason = ref('')
const filters = reactive({ agent_key: '', status: '' })
const memoryFilters = reactive({ memory_type: '', ts_code: '', status: 'active' })
const goalJson = ref('{}')

const healthQuery = useQuery({
  queryKey: ['agent-stack-health'],
  queryFn: fetchAgentStackHealth,
  staleTime: 15_000,
})

const runsQuery = useQuery({
  queryKey: ['agent-runs', filters],
  queryFn: () => fetchAgentRuns({ agent_key: filters.agent_key, status: filters.status, limit: 80 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
})

const detailQuery = useQuery({
  queryKey: ['agent-run-detail', selectedRunId],
  queryFn: () => fetchAgentRun(selectedRunId.value),
  enabled: computed(() => Boolean(selectedRunId.value)),
  refetchInterval: () => (document.visibilityState === 'visible' && selectedRunId.value ? 20_000 : false),
})

const runs = computed<AgentRun[]>(() => (Array.isArray(runsQuery.data.value?.items) ? runsQuery.data.value?.items || [] : []))
const activeRun = computed<AgentRun | null>(() => detailQuery.data.value?.run || null)

const timelineQuery = useQuery({
  queryKey: ['agent-timeline', computed(() => activeRun.value?.correlation_id || '')],
  queryFn: () => fetchAgentTimeline(activeRun.value?.correlation_id || ''),
  enabled: computed(() => Boolean(activeRun.value?.correlation_id)),
  refetchInterval: () => (document.visibilityState === 'visible' && activeRun.value?.correlation_id ? 30_000 : false),
})

const memoryQuery = useQuery({
  queryKey: ['agent-memory', memoryFilters],
  queryFn: () => fetchAgentMemoryItems({ ...memoryFilters, limit: 60 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 60_000 : 180_000),
})

const timelineEvents = computed(() => timelineQuery.data.value?.events || [])
const memoryItems = computed(() => memoryQuery.data.value?.items || [])
const approvalBusy = computed(() => approveMutation.isPending.value || rejectMutation.isPending.value)

const startMutation = useMutation({
  mutationFn: () =>
    startAgentRun({
      agent_key: selectedAgentKey.value,
      trigger_source: 'manual',
      actor: 'agent-ops-page',
      goal: parseGoal(),
      metadata: { source: 'agent-ops-page' },
      dedupe: false,
    }),
  onSuccess: async (data) => {
    selectedRunId.value = data.run.id
    ui.showToast(`Agent run 已创建：${data.run.agent_key}`, 'success')
    await refreshRuns()
  },
  onError: (error: any) => ui.showToast(error?.message || '创建 Agent run 失败', 'error'),
})

const approveMutation = useMutation({
  mutationFn: () => approveAgentRun(selectedRunId.value, { reason: approvalReason.value.trim(), actor: 'agent-ops-page' }),
  onSuccess: async () => {
    approvalReason.value = ''
    ui.showToast('审批通过，已执行待写步骤。', 'success')
    await refreshRuns()
  },
  onError: (error: any) => ui.showToast(error?.message || '审批失败', 'error'),
})

const rejectMutation = useMutation({
  mutationFn: () => rejectAgentRun(selectedRunId.value, { reason: approvalReason.value.trim(), actor: 'agent-ops-page' }),
  onSuccess: async () => {
    approvalReason.value = ''
    ui.showToast('已拒绝该 Agent run。', 'success')
    await refreshRuns()
  },
  onError: (error: any) => ui.showToast(error?.message || '拒绝失败', 'error'),
})

watch(activeRun, () => {
  approvalReason.value = ''
})

function parseGoal(): Record<string, unknown> {
  try {
    const parsed = JSON.parse(goalJson.value.trim() || '{}') as unknown
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>
    }
    ui.showToast('goal 需为 JSON 对象', 'error')
    return {}
  } catch {
    ui.showToast('goal JSON 无法解析', 'error')
    return {}
  }
}

async function exportMcpAudit() {
  try {
    const data = await fetchMcpToolAudit({ limit: 200 })
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `mcp-audit-${Date.now()}.json`
    a.click()
    URL.revokeObjectURL(url)
    ui.showToast('已下载 MCP 审计片段', 'success')
  } catch (error: any) {
    ui.showToast(error?.message || '导出失败', 'error')
  }
}

function startRun() {
  startMutation.mutate()
}

function selectRun(runId: string) {
  selectedRunId.value = runId
}

async function refreshRuns() {
  await queryClient.invalidateQueries({ queryKey: ['agent-runs'] })
  if (selectedRunId.value) await queryClient.invalidateQueries({ queryKey: ['agent-run-detail'] })
}

function approveRun() {
  approveMutation.mutate()
}

function rejectRun() {
  rejectMutation.mutate()
}

function runSummary(run: AgentRun) {
  const result = run.result || {}
  const pending = Array.isArray(result.pending_write_steps) ? result.pending_write_steps.length : 0
  const warnings = Array.isArray(result.warnings) ? result.warnings.length : 0
  return `${result.closure_status || '-'} · pending=${pending} · warnings=${warnings}`
}

function stepDescription(step: AgentStep) {
  const result = step.result || {}
  if (result.error) return String(result.error)
  if (typeof result.changed_count !== 'undefined') return `changed=${result.changed_count} · skipped=${result.skipped_count || 0}`
  return JSON.stringify(step.args || {})
}

function badgeValue(status: string) {
  if (['succeeded', 'success', 'approved'].includes(status)) return 'success'
  if (['failed', 'error', 'cancelled', 'rejected'].includes(status)) return 'error'
  if (['waiting_approval', 'pending_approval', 'running', 'queued'].includes(status)) return 'warn'
  return 'muted'
}

function pretty(value: unknown) {
  try {
    return JSON.stringify(value || {}, null, 2)
  } catch {
    return String(value || '')
  }
}
</script>
