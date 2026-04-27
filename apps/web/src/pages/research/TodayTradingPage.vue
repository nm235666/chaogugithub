<template>
  <AppShell title="今日交易台" subtitle="后端生成今日动作清单：买什么、卖什么、买卖多少，以及为什么。">
    <div class="space-y-4">
      <PageSection title="后端业务状态" :subtitle="`生成时间 ${formatDate(payload.generated_at)}`">
        <div class="mb-4 grid gap-2 text-xs md:grid-cols-5">
          <RouterLink to="/app/desk/today" class="rounded-2xl border border-[var(--brand)] bg-[var(--panel-soft)] px-3 py-2 font-semibold text-[var(--brand)]">1 今日交易台</RouterLink>
          <RouterLink to="/app/desk/orders" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] hover:border-[var(--brand)]">2 计划单确认</RouterLink>
          <RouterLink to="/app/desk/positions" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] hover:border-[var(--brand)]">3 持仓看板</RouterLink>
          <RouterLink to="/app/desk/review" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] hover:border-[var(--brand)]">4 执行复盘</RouterLink>
          <RouterLink to="/app/desk/macro-regime" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] hover:border-[var(--brand)]">5 三周期状态</RouterLink>
        </div>
        <div class="grid gap-3 md:grid-cols-4">
          <div class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <div class="text-xs text-[var(--muted)]">可执行动作</div>
            <div class="mt-1 text-2xl font-semibold text-[var(--ink)]">{{ Number(summary.executable || 0) }}</div>
          </div>
          <div class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <div class="text-xs text-[var(--muted)]">持仓诊断</div>
            <div class="mt-1 text-2xl font-semibold text-[var(--ink)]">{{ Number(summary.positions || 0) }}</div>
          </div>
          <div class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <div class="text-xs text-[var(--muted)]">账户假设</div>
            <div class="mt-1 text-2xl font-semibold text-[var(--ink)]">{{ formatMoney(account.equity) }}</div>
          </div>
          <div class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <div class="text-xs text-[var(--muted)]">风控门禁</div>
            <div class="mt-1">
              <span :class="riskGateClass">{{ riskGate.ok ? '可创建计划单' : '已阻断' }}</span>
            </div>
          </div>
        </div>
        <div v-if="riskMessages.length" class="mt-3 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          {{ riskMessages.join('；') }}
        </div>
        <div v-if="message" class="mt-3 rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm text-[var(--muted)]">
          {{ message }}
        </div>
      </PageSection>

      <PageSection title="今日动作清单" :subtitle="`优先处理 ${focusActions.length} 个，候选 ${candidateActions.length} 个，观察 ${watchlistActions.length} 个`">
        <template #action>
          <button class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)] hover:border-[var(--brand)]" @click="() => refetch()">
            刷新
          </button>
        </template>
        <div v-if="isPending" class="py-10 text-center text-sm text-[var(--muted)]">正在从后端生成今日动作...</div>
        <div v-else-if="isError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-700">
          今日动作查询失败：{{ errorText }}
        </div>
        <div v-else-if="!items.length" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-10 text-center text-sm text-[var(--muted)]">
          暂无动作。先检查评分数据、持仓数据和 Kill Switch。
        </div>
        <div v-else class="space-y-4">
          <div class="grid gap-2 md:grid-cols-3">
            <button
              v-for="tab in actionTabs"
              :key="tab.key"
              type="button"
              class="rounded-2xl border px-3 py-2 text-left text-xs font-semibold"
              :class="activeTab === tab.key ? 'border-[var(--brand)] bg-[var(--brand)] text-white' : 'border-[var(--line)] bg-white text-[var(--ink)]'"
              @click="activeTab = tab.key"
            >
              {{ tab.label }} <span class="opacity-80">{{ tabCount(tab.key) }}</span>
            </button>
          </div>
          <div v-if="payload.condenser?.sort_basis" class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs text-[var(--muted)]">
            收敛规则：{{ payload.condenser.sort_basis }}
          </div>
          <div v-if="!visibleItems.length" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-8 text-center text-sm text-[var(--muted)]">
            当前分层暂无动作，可切换到其他分层查看。
          </div>
          <article
            v-for="item in visibleItems"
            :key="item.id"
            class="rounded-3xl border bg-white px-4 py-4 shadow-sm"
            :class="item.can_create_order ? 'border-emerald-200' : 'border-[var(--line)]'"
          >
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <div class="flex flex-wrap items-center gap-2">
                  <span :class="actionClass(item.action_type)">{{ item.action_label || actionLabel(item.action_type) }}</span>
                  <span class="font-mono text-sm font-semibold text-[var(--ink)]">{{ item.ts_code }}</span>
                  <span v-if="item.name" class="text-sm text-[var(--muted)]">{{ item.name }}</span>
                  <span class="metric-chip">{{ item.horizon === 'long' ? '长线持仓' : '短线动作' }}</span>
                  <span v-if="item.rule_tier_label" class="metric-chip">规则 {{ item.rule_tier_label }}</span>
                  <span v-if="item.action_group_label" class="metric-chip">{{ item.action_group_label }}</span>
                  <span class="metric-chip">{{ chainLabel(item) }}</span>
                </div>
                <p class="mt-2 text-sm text-[var(--ink)]">{{ item.reason?.summary || '-' }}</p>
                <p v-if="item.risk?.summary" class="mt-1 text-xs text-[var(--muted)]">风险：{{ item.risk.summary }}</p>
                <div v-if="!item.can_create_order" class="mt-2 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                  <div class="font-semibold">不可执行原因</div>
                  <div>{{ nonExecutableText(item) }}</div>
                </div>
              </div>
              <div class="flex flex-wrap justify-end gap-2">
                <button
                  class="rounded-full bg-[var(--brand)] px-3 py-2 text-xs font-semibold text-white disabled:opacity-50"
                  :disabled="!item.can_create_order || isCreating(item)"
                  @click="createPlan(item)"
                >
                  {{ isCreating(item) ? '创建中...' : item.can_create_order ? '生成计划单' : '不可执行' }}
                </button>
                <button
                  v-if="item.agent_can_create_order"
                  class="rounded-full border border-violet-300 bg-violet-50 px-3 py-2 text-xs font-semibold text-violet-700 disabled:opacity-50"
                  :disabled="isCreating(item)"
                  @click="createPlan(item, true)"
                >
                  {{ isCreating(item) ? '创建中...' : '按 Agent 建议生成' }}
                </button>
                <button
                  class="rounded-full border border-violet-300 bg-white px-3 py-2 text-xs font-semibold text-violet-700 disabled:opacity-50"
                  :disabled="isRefreshingAgent(item)"
                  @click="refreshAgent(item)"
                >
                  {{ isRefreshingAgent(item) ? '评估中...' : '重新评估这只' }}
                </button>
              </div>
            </div>
            <div v-if="item.agent_opinion" class="mt-3 rounded-2xl border border-violet-200 bg-violet-50 px-3 py-3 text-xs text-violet-800">
              <div class="flex flex-wrap items-center gap-2">
                <span class="font-semibold">交易建议 Agent</span>
                <span class="metric-chip">{{ item.agent_opinion.action_label || agentActionLabel(item.agent_opinion.action) }}</span>
                <span class="metric-chip">置信度 {{ formatScore(item.agent_opinion.confidence) }}</span>
                <span v-if="item.agent_opinion.source" class="metric-chip">来源 {{ agentSourceLabel(item.agent_opinion.source) }}</span>
                <span v-if="item.agent_opinion.cached_at" class="metric-chip">缓存 {{ formatDate(item.agent_opinion.cached_at) }}</span>
              </div>
              <p class="mt-2">{{ item.agent_opinion.summary || 'Agent 暂无补充判断。' }}</p>
              <p v-if="item.agent_opinion.invalid_if" class="mt-1">失效条件：{{ item.agent_opinion.invalid_if }}</p>
              <p v-if="item.agent_order_payload" class="mt-1 font-semibold">
                Agent 计划：{{ actionLabel(item.agent_order_payload.action_type) }} {{ item.agent_order_payload.size }} 股，目标仓位 {{ formatPct(item.agent_opinion.target_position_pct) }}，需要人工确认。
              </p>
            </div>
            <div class="mt-3 grid gap-2 text-xs sm:grid-cols-4">
              <span class="metric-chip">数量 <strong>{{ item.quantity ?? 0 }}</strong></span>
              <span class="metric-chip">计划价 <strong>{{ formatPrice(item.reference_price) }}</strong></span>
              <span class="metric-chip">目标仓位 <strong>{{ formatPct(item.target_position_pct) }}</strong></span>
              <span class="metric-chip">当前持仓 <strong>{{ item.current_quantity ?? 0 }}</strong></span>
            </div>
            <div class="mt-2 grid gap-2 text-xs sm:grid-cols-3">
              <span class="metric-chip">下一步 {{ nextStepLabel(item.next_step) }}</span>
              <span class="metric-chip">风控 {{ item.execution_flow?.risk_gate === 'passed' ? '通过' : '阻断' }}</span>
              <span v-if="item.execution_flow?.chain_order_no" class="metric-chip">交易链 {{ item.execution_flow.chain_order_no }}</span>
            </div>
            <div class="mt-2 flex flex-wrap gap-1.5 text-xs">
              <span class="metric-chip">总分 {{ formatScore(item.evidence?.total_score) }}</span>
              <span class="metric-chip">趋势 {{ formatScore(item.evidence?.trend_score) }}</span>
              <span class="metric-chip">风险 {{ formatScore(item.evidence?.risk_score) }}</span>
              <span v-if="item.evidence?.industry" class="metric-chip">行业 {{ item.evidence.industry }}</span>
              <span v-if="item.strategy?.strategy_key" class="metric-chip">策略 {{ item.strategy.strategy_key }}</span>
              <span v-if="item.strategy?.strategy_weight_action" :class="strategyWeightClass(item.strategy.strategy_weight_action)" :title="item.strategy.strategy_weight_reason || ''">
                权重 {{ strategyWeightLabel(item.strategy.strategy_weight_action) }}
                <template v-if="item.strategy.strategy_weight_multiplier">×{{ formatScore(item.strategy.strategy_weight_multiplier) }}</template>
              </span>
              <span v-if="item.action_priority_score" class="metric-chip">优先级 {{ formatScore(item.action_priority_score) }}</span>
              <span class="metric-chip">来源 {{ item.source || item.evidence?.source || '-' }}</span>
            </div>
          </article>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import { fetchDecisionTodayActions, recordDecisionAction, refreshDecisionTradeAdvisor, type TodayActionItem } from '../../services/api/decision'
import { createPortfolioOrder } from '../../services/api/portfolio'

const queryClient = useQueryClient()
const message = ref('')
const creatingId = ref('')
const refreshingAgentId = ref('')
const activeTab = ref<'focus' | 'candidate' | 'watchlist'>('focus')
const actionTabs = [
  { key: 'focus', label: '今日必处理' },
  { key: 'candidate', label: '今日候选' },
  { key: 'watchlist', label: '观察池' },
] as const

const { data, isPending, isError, error, refetch } = useQuery({
  queryKey: ['decision-today-actions'],
  queryFn: () => fetchDecisionTodayActions({ limit: 80 }),
  staleTime: 60 * 1000,
})

const payload = computed(() => data.value || {})
const items = computed<TodayActionItem[]>(() => payload.value.items || [])
const focusActions = computed<TodayActionItem[]>(() => payload.value.focus_actions || deriveActionGroup('focus'))
const candidateActions = computed<TodayActionItem[]>(() => payload.value.candidate_actions || deriveActionGroup('candidate'))
const watchlistActions = computed<TodayActionItem[]>(() => payload.value.watchlist_actions || deriveActionGroup('watchlist'))
const visibleItems = computed(() => {
  if (activeTab.value === 'candidate') return candidateActions.value
  if (activeTab.value === 'watchlist') return watchlistActions.value
  return focusActions.value
})
const summary = computed<Record<string, any>>(() => payload.value.summary || {})
const account = computed<Record<string, any>>(() => payload.value.account || {})
const riskGate = computed<Record<string, any>>(() => payload.value.risk_gate || {})
const riskMessages = computed(() => [
  ...((riskGate.value.blockers || []) as string[]),
  ...((riskGate.value.warnings || []) as string[]),
])
const errorText = computed(() => (error.value instanceof Error ? error.value.message : String(error.value || 'unknown')))
const riskGateClass = computed(() =>
  riskGate.value.ok
    ? 'inline-flex rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs font-semibold text-emerald-700'
    : 'inline-flex rounded-full border border-rose-200 bg-rose-50 px-2.5 py-1 text-xs font-semibold text-rose-700',
)

const createPlanMutation = useMutation({
  mutationFn: async (args: { item: TodayActionItem; useAgent?: boolean }) => {
    const item = args.item
    const orderPayload = (args.useAgent ? item.agent_order_payload : item.order_payload) || {
      ts_code: item.ts_code,
      action_type: item.action_type,
      planned_price: Number(item.reference_price || 0),
      size: Number(item.quantity || 0),
      note: item.reason?.summary || '',
    }
    const decisionAction = await recordDecisionAction({
      action_type: 'confirm',
      ts_code: item.ts_code,
      stock_name: item.name || '',
      note: orderPayload.note || item.reason?.summary || '',
      snapshot_date: payload.value.snapshot_date || '',
      idempotency_key: [
        'today-trading',
        payload.value.snapshot_date || '',
        item.id,
        args.useAgent ? 'agent' : 'rule',
        orderPayload.action_type,
        orderPayload.size,
        orderPayload.planned_price,
      ].join(':'),
      context: {
        source: 'today_trading',
        action_group: item.action_group,
        action_group_label: item.action_group_label,
        action_priority_score: item.action_priority_score,
        use_agent: Boolean(args.useAgent),
        rule_tier: item.rule_tier,
        rule_tier_label: item.rule_tier_label,
        execution_flow: item.execution_flow,
      },
      strategy: item.strategy || {},
      today_action: {
        id: item.id,
        horizon: item.horizon,
        action_type: item.action_type,
        action_label: item.action_label,
        source: item.source,
        reason: item.reason,
        risk: item.risk,
        evidence: item.evidence,
        agent_opinion: item.agent_opinion,
      },
      evidence_packet: {
        today_action_id: item.id,
        source: item.source,
        evidence: item.evidence || {},
        risk: item.risk || {},
        agent_opinion: item.agent_opinion || {},
        order_payload: orderPayload,
      },
      evidence_chain_complete: true,
      execution_status: 'planned',
      position_recommendation: item.target_position_pct ? `${item.target_position_pct}%` : '',
      target_position_pct: Number(item.target_position_pct || 0),
      priority: item.action_group === 'focus' ? 'high' : item.action_group === 'candidate' ? 'medium' : 'low',
      trigger_reason: item.reason?.summary || '',
    })
    const decisionActionId = decisionAction?.id || decisionAction?.trace?.action_id || decisionAction?.action_id
    return createPortfolioOrder({
      ...orderPayload,
      decision_action_id: decisionActionId,
    })
  },
  onMutate: ({ item, useAgent }) => {
    creatingId.value = item.id
    message.value = `正在${useAgent ? '按 Agent 建议' : ''}创建 ${item.ts_code} 的计划单...`
  },
  onSuccess: async (_result, { item, useAgent }) => {
    message.value = `${item.ts_code} ${useAgent ? 'Agent 建议' : ''}计划单已创建。`
    await queryClient.invalidateQueries({ queryKey: ['portfolio-orders'] })
    await queryClient.invalidateQueries({ queryKey: ['decision-today-actions'] })
  },
  onError: (err: Error, { item }) => {
    message.value = `${item.ts_code} 计划单创建失败：${err.message}`
  },
  onSettled: () => {
    creatingId.value = ''
  },
})

const refreshAgentMutation = useMutation({
  mutationFn: (item: TodayActionItem) => refreshDecisionTradeAdvisor({ ts_code: item.ts_code }),
  onMutate: (item) => {
    refreshingAgentId.value = item.id
    message.value = `正在重新评估 ${item.ts_code}，完成后会写入缓存...`
  },
  onSuccess: async (_result, item) => {
    message.value = `${item.ts_code} Agent 评估已提交，后台完成后会自动写入缓存。`
    await queryClient.invalidateQueries({ queryKey: ['decision-today-actions'] })
    window.setTimeout(() => {
      void queryClient.invalidateQueries({ queryKey: ['decision-today-actions'] })
    }, 8000)
  },
  onError: (err: Error, item) => {
    message.value = `${item.ts_code} Agent 重新评估失败：${err.message}`
  },
  onSettled: () => {
    refreshingAgentId.value = ''
  },
})

function createPlan(item: TodayActionItem, useAgent = false) {
  if (createPlanMutation.isPending.value) return
  if (useAgent) {
    if (!item.agent_can_create_order) return
    createPlanMutation.mutate({ item, useAgent: true })
    return
  }
  if (!item.can_create_order) return
  createPlanMutation.mutate({ item, useAgent: false })
}

function refreshAgent(item: TodayActionItem) {
  if (refreshAgentMutation.isPending.value) return
  refreshAgentMutation.mutate(item)
}

function isCreating(item: TodayActionItem): boolean {
  return creatingId.value === item.id
}

function isRefreshingAgent(item: TodayActionItem): boolean {
  return refreshingAgentId.value === item.id
}

function tabCount(key: string): number {
  if (key === 'candidate') return candidateActions.value.length
  if (key === 'watchlist') return watchlistActions.value.length
  return focusActions.value.length
}

function fallbackActionGroup(item: TodayActionItem): 'focus' | 'candidate' | 'watchlist' {
  if (item.action_group === 'focus' || item.action_group === 'candidate' || item.action_group === 'watchlist') return item.action_group
  const action = item.action_type
  const tier = item.rule_tier
  const currentQuantity = Number(item.current_quantity || 0)
  const executable = Boolean(item.can_create_order || item.agent_can_create_order)
  if (currentQuantity > 0 && ['close', 'reduce', 'sell'].includes(action)) return 'focus'
  if (executable && ['buy', 'add', 'close', 'reduce', 'sell'].includes(action) && tier !== 'probe_buy') return 'focus'
  if (executable || tier === 'probe_buy' || ['buy', 'add'].includes(action)) return 'candidate'
  return 'watchlist'
}

function deriveActionGroup(group: 'focus' | 'candidate' | 'watchlist'): TodayActionItem[] {
  return items.value.filter((item) => fallbackActionGroup(item) === group)
}

function nonExecutableText(item: TodayActionItem): string {
  const reasons = item.non_executable_reasons || []
  if (reasons.length) return reasons.join('；')
  if (item.action_type === 'watch') return '观察动作只进入观察池，不生成计划单。'
  if (item.action_type === 'hold') return '继续持有不需要生成新的计划单。'
  return '当前动作未通过风控、数量或交易类型检查。'
}

function chainLabel(item: TodayActionItem): string {
  const mode = item.execution_flow?.chain_mode
  if (mode === 'new') return '新交易链'
  if (mode === 'reuse') return `复用交易链 ${item.execution_flow?.chain_order_no || ''}`.trim()
  return '不进入交易链'
}

function nextStepLabel(value?: string): string {
  const labels: Record<string, string> = {
    create_order_plan: '生成计划单',
    observe_only: '继续观察',
    hold_position: '继续持有',
    blocked: '等待风控或数据修复',
  }
  return value ? labels[value] || value : '-'
}

function agentActionLabel(value?: string): string {
  const labels: Record<string, string> = {
    strong_buy: '强买入',
    probe_buy: '小仓位试买',
    buy: '买入',
    add: '加仓',
    hold: '继续持有',
    watch: '继续观察',
    reduce: '减仓',
    close: '清仓',
    avoid: '放弃',
    not_reviewed: '未评估',
    pending_review: '待评估',
  }
  return value ? labels[value] || value : '-'
}

function agentSourceLabel(value?: string): string {
  const labels: Record<string, string> = {
    llm: 'LLM',
    heuristic: '规则兜底',
    heuristic_fast: '快速建议',
    heuristic_fallback: 'LLM失败兜底',
    heuristic_disabled: 'LLM关闭',
    not_needed: '无需复核',
    not_reviewed: '未评估',
    pending_review: '待盘前评估',
    refresh_running: '评估中',
    refresh_failed: '评估失败',
  }
  return value ? labels[value] || value : '-'
}

function actionLabel(value?: string): string {
  const labels: Record<string, string> = { buy: '新买', add: '加仓', reduce: '减仓', close: '清仓', sell: '卖出', hold: '持有', watch: '观察' }
  return value ? labels[value] || value : '-'
}

function actionClass(value?: string): string {
  const base = 'inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold'
  if (value === 'buy' || value === 'add') return `${base} border-emerald-200 bg-emerald-50 text-emerald-700`
  if (value === 'reduce' || value === 'close' || value === 'sell') return `${base} border-rose-200 bg-rose-50 text-rose-700`
  return `${base} border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]`
}

function formatPrice(value?: number): string {
  const n = Number(value || 0)
  return n > 0 ? n.toFixed(2) : '-'
}

function formatMoney(value: unknown): string {
  const n = Number(value || 0)
  return n > 0 ? `${Math.round(n).toLocaleString('zh-CN')}` : '-'
}

function formatPct(value: unknown): string {
  const n = Number(value || 0)
  return n > 0 ? `${n.toFixed(1)}%` : '0%'
}

function formatScore(value: unknown): string {
  const n = Number(value || 0)
  return n > 0 ? n.toFixed(1) : '-'
}

function strategyWeightLabel(action?: string): string {
  const map: Record<string, string> = {
    boost: '加强',
    keep: '维持',
    reduce: '降低',
    pause: '暂停',
  }
  return action ? (map[action] ?? action) : '-'
}

function strategyWeightClass(action?: string): string {
  const base = 'metric-chip'
  if (action === 'boost') return `${base} border-emerald-200 bg-emerald-50 text-emerald-700`
  if (action === 'reduce') return `${base} border-amber-200 bg-amber-50 text-amber-700`
  if (action === 'pause') return `${base} border-rose-200 bg-rose-50 text-rose-700`
  return base
}

function formatDate(value?: string): string {
  if (!value) return '-'
  return value.replace('T', ' ').replace('Z', '')
}
</script>
