<template>
  <AppShell title="Agent 治理中心" subtitle="质量评分、策略闸门、自动降级与阻断记录。">
    <div class="space-y-4">
      <PageSection title="质量评分" subtitle="默认最近 7 天，分数来自 run、审批、MCP 审计和失败记忆。">
        <div class="mb-3 flex flex-wrap items-center gap-2">
          <select v-model="filters.agent_key" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
            <option value="">全部 Agent</option>
            <option v-for="agent in agentOptions" :key="agent" :value="agent">{{ agent }}</option>
          </select>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" @click="qualityQuery.refetch()">刷新</button>
          <button class="rounded-2xl bg-[var(--brand)] px-3 py-2 text-sm font-semibold text-white disabled:opacity-50" :disabled="recomputeMutation.isPending.value" @click="recompute">
            {{ recomputeMutation.isPending.value ? '计算中...' : '重新计算' }}
          </button>
        </div>
        <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          <InfoCard
            v-for="item in qualityItems"
            :key="`${item.agent_key}-${item.metric_date}`"
            :title="item.agent_key"
            :meta="`${item.metric_date || '-'} · success ${percent(item.success_rate)} · fail ${percent(item.failure_rate)}`"
            :description="`runs=${item.total_runs || 0} · changed=${item.changed_count || 0} · conflicts=${item.conflict_count || 0}`"
            @click="selectedAgent = item.agent_key"
          >
            <template #badge>
              <StatusBadge :value="badgeValue(item.risk_status || 'healthy')" :label="item.risk_status || 'healthy'" />
            </template>
            <div class="mt-2 flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">risk {{ Number(item.risk_score || 0).toFixed(2) }}</span>
              <span class="metric-chip">approval {{ percent(item.approval_pass_rate) }}</span>
              <span class="metric-chip">avg {{ Number(item.avg_duration_seconds || 0).toFixed(1) }}s</span>
            </div>
          </InfoCard>
          <div v-if="!qualityItems.length && !qualityQuery.isPending.value" class="surface-note">暂无评分数据，可点击重新计算。</div>
        </div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[1fr_1fr]">
        <PageSection title="策略规则" subtitle="规则可将动作设为 allow、dry_run_only、requires_approval 或 blocked。">
          <div class="mb-3 grid gap-2 md:grid-cols-2">
            <input v-model="ruleForm.rule_key" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm" placeholder="rule_key" />
            <input v-model="ruleForm.agent_key" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm" placeholder="agent_key，可空" />
            <input v-model="ruleForm.tool_name" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm" placeholder="tool_name，可空" />
            <select v-model="ruleForm.decision" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
              <option value="allow">allow</option>
              <option value="dry_run_only">dry_run_only</option>
              <option value="requires_approval">requires_approval</option>
              <option value="blocked">blocked</option>
            </select>
            <select v-model="ruleForm.risk_level" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
              <option value="low">low</option>
              <option value="medium">medium</option>
              <option value="high">high</option>
              <option value="critical">critical</option>
            </select>
            <label class="flex items-center gap-2 rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
              <input v-model="ruleForm.enabled" type="checkbox" />
              enabled
            </label>
          </div>
          <div class="mb-3 flex flex-wrap gap-2">
            <button class="rounded-2xl bg-[var(--brand)] px-3 py-2 text-sm font-semibold text-white disabled:opacity-50" :disabled="ruleMutation.isPending.value || !ruleForm.rule_key" @click="saveRule">
              保存规则
            </button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" @click="rulesQuery.refetch()">刷新规则</button>
          </div>
          <div class="max-h-[480px] space-y-2 overflow-auto">
            <InfoCard
              v-for="rule in rules"
              :key="rule.id || rule.rule_key"
              :title="rule.rule_key"
              :meta="`${rule.agent_key || '*'} · ${rule.tool_name || '*'} · ${rule.risk_level || '-'}`"
              :description="rule.reason || '-'"
              @click="loadRule(rule)"
            >
              <template #badge>
                <StatusBadge :value="rule.enabled ? badgeValue(rule.decision || 'allow') : 'muted'" :label="rule.enabled ? rule.decision || '-' : 'disabled'" />
              </template>
            </InfoCard>
            <div v-if="!rules.length && !rulesQuery.isPending.value" class="surface-note">暂无治理规则。</div>
          </div>
        </PageSection>

        <PageSection title="策略判断记录" subtitle="runtime 每次 gate 判断都会落库，可追溯降级与阻断。">
          <div class="mb-3 flex flex-wrap items-center gap-2">
            <select v-model="decisionFilters.decision" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
              <option value="">全部决策</option>
              <option value="allow">allow</option>
              <option value="dry_run_only">dry_run_only</option>
              <option value="requires_approval">requires_approval</option>
              <option value="blocked">blocked</option>
            </select>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold" @click="decisionsQuery.refetch()">刷新</button>
          </div>
          <div class="max-h-[620px] space-y-2 overflow-auto">
            <InfoCard
              v-for="item in decisions"
              :key="item.id"
              :title="item.tool_name || '-'"
              :meta="`${item.agent_key || '-'} · ${formatDateTime(item.created_at)}`"
              :description="item.reason || '-'"
            >
              <template #badge>
                <StatusBadge :value="badgeValue(item.decision || 'allow')" :label="item.decision || '-'" />
              </template>
              <div class="mt-2 flex flex-wrap gap-2 text-xs">
                <span class="metric-chip">{{ item.risk_level || 'low' }}</span>
                <span v-if="item.run_id" class="metric-chip">{{ item.run_id.slice(0, 18) }}</span>
              </div>
            </InfoCard>
            <div v-if="!decisions.length && !decisionsQuery.isPending.value" class="surface-note">暂无策略判断记录。</div>
          </div>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { formatDateTime } from '../../shared/utils/format'
import { useUiStore } from '../../stores/ui'
import {
  fetchGovernanceQuality,
  fetchGovernanceRules,
  fetchPolicyDecisions,
  recomputeGovernanceQuality,
  upsertGovernanceRule,
  type GovernanceRule,
} from '../../services/api/agentGovernance'

const ui = useUiStore()
const queryClient = useQueryClient()
const selectedAgent = ref('')
const filters = reactive({ agent_key: '' })
const decisionFilters = reactive({ decision: '' })
const ruleForm = reactive<GovernanceRule>({
  rule_key: '',
  agent_key: '',
  tool_name: '',
  risk_level: 'low',
  decision: 'dry_run_only',
  enabled: true,
  thresholds: {},
  reason: 'manual governance rule update',
})

const agentOptions = [
  'funnel_progress_agent',
  'portfolio_reconcile_agent',
  'portfolio_review_agent',
  'decision_orchestrator_agent',
  'memory_refresh_agent',
  'governance_quality_refresh_agent',
]

const qualityQuery = useQuery({
  queryKey: ['agent-governance-quality', filters],
  queryFn: () => fetchGovernanceQuality({ agent_key: filters.agent_key, window_days: 7 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 60_000 : 180_000),
})

const rulesQuery = useQuery({
  queryKey: ['agent-governance-rules', selectedAgent],
  queryFn: () => fetchGovernanceRules({ agent_key: selectedAgent.value, limit: 100 }),
})

const decisionsQuery = useQuery({
  queryKey: ['agent-policy-decisions', decisionFilters, selectedAgent],
  queryFn: () => fetchPolicyDecisions({ agent_key: selectedAgent.value, decision: decisionFilters.decision, limit: 100 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 45_000 : 180_000),
})

const qualityItems = computed(() => qualityQuery.data.value?.items || [])
const rules = computed(() => rulesQuery.data.value?.items || [])
const decisions = computed(() => decisionsQuery.data.value?.items || [])

const recomputeMutation = useMutation({
  mutationFn: () => recomputeGovernanceQuality({ agent_key: filters.agent_key, window_days: 7 }),
  onSuccess: async () => {
    ui.showToast('治理质量评分已刷新。', 'success')
    await queryClient.invalidateQueries({ queryKey: ['agent-governance-quality'] })
  },
  onError: (error: any) => ui.showToast(error?.message || '刷新评分失败', 'error'),
})

const ruleMutation = useMutation({
  mutationFn: () => upsertGovernanceRule({ ...ruleForm, actor: 'agent-governance-page' }),
  onSuccess: async () => {
    ui.showToast('治理规则已保存。', 'success')
    await queryClient.invalidateQueries({ queryKey: ['agent-governance-rules'] })
  },
  onError: (error: any) => ui.showToast(error?.message || '保存规则失败', 'error'),
})

function recompute() {
  recomputeMutation.mutate()
}

function saveRule() {
  ruleMutation.mutate()
}

function loadRule(rule: GovernanceRule) {
  Object.assign(ruleForm, {
    rule_key: rule.rule_key,
    agent_key: rule.agent_key || '',
    tool_name: rule.tool_name || '',
    risk_level: rule.risk_level || 'low',
    decision: rule.decision || 'dry_run_only',
    enabled: Boolean(rule.enabled),
    thresholds: rule.thresholds || {},
    reason: rule.reason || 'manual governance rule update',
  })
}

function percent(value: unknown) {
  return `${Math.round(Number(value || 0) * 100)}%`
}

function badgeValue(status: string) {
  if (['healthy', 'allow', 'succeeded', 'success'].includes(status)) return 'success'
  if (['blocked', 'failed', 'error'].includes(status)) return 'error'
  if (['degraded', 'dry_run_only', 'requires_approval', 'waiting_approval'].includes(status)) return 'warn'
  return 'muted'
}
</script>
