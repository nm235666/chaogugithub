<template>
  <AppShell title="多角色公司分析" subtitle="支持股票简称搜索、异步任务轮询、按角色查看和分角色下载。">
    <div class="space-y-4">
      <PageSection title="分析发起" subtitle="可以直接输 ts_code，也可以输股票简称，由前端自动解析第一条匹配结果。">
        <div class="grid gap-3 xl:grid-cols-[1fr_140px_180px] md:grid-cols-2">
          <label class="text-sm font-semibold text-[var(--ink)] xl:col-span-1">
            股票代码 / 简称
            <input v-model="form.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 000001.SZ / 平安银行" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            回看区间
            <select v-model.number="form.lookback" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option :value="60">60 日</option>
              <option :value="120">120 日</option>
              <option :value="240">240 日</option>
            </select>
          </label>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" :disabled="isPending" @click="runAnalysis">
            {{ isPending ? '任务创建中...' : '发起分析' }}
          </button>
        </div>
        <label class="mt-3 flex items-center gap-2 text-sm text-[var(--muted)]">
          <input v-model="acceptAutoDegrade" type="checkbox" />
          允许自动降级完成（失败角色不阻塞全局汇总）
        </label>
        <div class="mt-3 rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)] shadow-[var(--shadow-soft)]" role="status" aria-live="polite">
          {{ actionMessage }}
        </div>
        <div class="mt-2 flex flex-wrap gap-2">
          <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] disabled:opacity-40" :disabled="!currentJobId" @click="clearCurrentTaskTracking">
            清除当前任务跟踪
          </button>
        </div>
        <div v-if="restoredTaskHint" class="mt-1 text-sm text-[var(--muted)]">已从会话恢复任务跟踪（{{ restoredTaskHint }}）</div>
        <div v-if="taskPersistenceNotice" class="mt-1 text-sm text-[var(--muted)]">{{ taskPersistenceNotice }}</div>
        <div class="mt-2 text-sm text-[var(--muted)]">实际模型：{{ usedModel }}</div>
        <div v-if="attemptChain" class="mt-1 text-sm text-[var(--muted)]">尝试链路：{{ attemptChain }}</div>
        <div v-if="quotaHint" class="mt-1 text-sm text-[var(--muted)]">今日额度：{{ quotaHint }}</div>
        <div v-if="pendingDecision" class="mt-3 rounded-2xl border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <div>有角色任务失败，等待你的决策：重试 / 降级 / 终止。</div>
          <div class="mt-2 flex flex-wrap gap-2">
            <button class="rounded-xl bg-[var(--brand)] px-3 py-2 text-white" :disabled="decisionPending" @click="submitDecision('retry')">重试失败角色</button>
            <button class="rounded-xl bg-stone-700 px-3 py-2 text-white" :disabled="decisionPending" @click="submitDecision('degrade')">降级并继续汇总</button>
            <button class="rounded-xl bg-red-700 px-3 py-2 text-white" :disabled="decisionPending" @click="submitDecision('abort')">终止任务</button>
          </div>
        </div>
      </PageSection>

      <PageSection title="任务状态区" subtitle="先看当前任务走到哪里，再决定是否进入角色原文和完整 Markdown。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <InfoCard title="当前任务" :meta="currentJobId || '-'" :description="statusSummary">
            <template #badge>
              <StatusBadge :value="taskStatusTone" :label="taskStatusLabel" />
            </template>
          </InfoCard>
          <InfoCard title="已解析股票" :meta="resolvedStock.ts_code || '-'" :description="resolvedStock.name || '等待解析'" />
          <InfoCard title="模型链路" :meta="usedModel || '-'" :description="attemptChain || '等待任务返回模型链路'" />
          <InfoCard title="额度提示" :meta="quotaHint || '-'" :description="pendingDecision ? '当前存在失败角色，等待用户决策。' : '当前无额外用户决策阻塞。'" />
        </div>
        <div v-if="taskStatus" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.78)] px-4 py-3 text-sm text-[var(--muted)]">
          当前阶段：{{ taskStage || taskStatus }} · {{ statusSummary }}
          <span v-if="taskUpdatedAt" class="ml-2">· 最近更新 {{ taskUpdatedAt }}</span>
        </div>
      </PageSection>

      <PageSection title="子任务进度" subtitle="每个角色独立执行，状态可单独追踪。">
        <div v-if="roleRuns.length" class="space-y-2">
          <div v-for="item in roleRuns" :key="item.role" class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
            <div class="flex items-center justify-between gap-2">
              <div class="font-semibold">{{ item.role }}</div>
              <div class="text-[var(--muted)]">{{ item.status || '-' }}</div>
            </div>
            <div class="mt-1 text-[var(--muted)]">
              模型：{{ item.used_model || item.requested_model || '-' }} · 重试 {{ item.retry_count || 0 }} 次 · 耗时 {{ item.duration_ms || 0 }}ms
            </div>
            <div v-if="item.error" class="mt-1 text-red-600">错误：{{ item.error }}</div>
          </div>
        </div>
        <div v-else class="text-sm text-[var(--muted)]">尚未启动角色子任务。</div>
      </PageSection>

      <PageSection title="阶段时间线" subtitle="展示 v3 六阶段编排状态与当前执行位置。">
          <div v-if="stageTimeline.length" class="grid gap-2 md:grid-cols-3">
            <div v-for="item in stageTimeline" :key="item.stage" class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
              <div class="font-semibold">{{ item.stage }}</div>
              <div class="mt-1 text-[var(--muted)]">{{ item.status || '-' }}</div>
            </div>
          </div>
        <div v-else class="text-sm text-[var(--muted)]">尚无阶段数据。</div>
      </PageSection>

      <PageSection title="辩论面板" subtitle="展示 Bull/Bear 与风险三方辩论摘要。">
        <div class="grid gap-3 xl:grid-cols-2 md:grid-cols-1">
          <InfoCard title="Bull/Bear 研究辩论" :meta="`rounds ${researchDebateRounds}`">
            <div class="markdown-compact" v-html="researchDebateHtml" />
          </InfoCard>
          <InfoCard title="风险三方辩论" :meta="`rounds ${riskDebateRounds}`">
            <div class="markdown-compact" v-html="riskDebateHtml" />
          </InfoCard>
        </div>
      </PageSection>

      <PageSection title="审批面板" subtitle="Portfolio Manager 最终审批结论。">
        <InfoCard title="审批结论" :meta="portfolioDecisionMeta">
          <div class="markdown-compact" v-html="portfolioDecisionHtml" />
        </InfoCard>
      </PageSection>

      <PageSection title="角色视图" subtitle="优先展示按角色切分后的结论，也保留完整原文。">
        <template #action>
          <div class="flex flex-wrap gap-2">
            <button
              class="rounded-2xl bg-emerald-700 px-4 py-2 text-white"
              :disabled="!canRetryAggregate || aggregateRetryPending"
              @click="retryAggregate"
            >
              {{ aggregateRetryPending ? '重试汇总中...' : '重试汇总' }}
            </button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white" :disabled="!selectedRoleSection" @click="downloadRoleMarkdown">下载当前角色 Markdown</button>
            <button class="rounded-2xl bg-blue-700 px-4 py-2 text-white" :disabled="!fullMarkdown" @click="downloadFullMarkdown">下载完整 Markdown</button>
          </div>
        </template>
        <InfoCard title="汇总面板" :meta="aggregatorPanelMeta" class="mb-4">
          <div class="markdown-compact" v-html="aggregatorPanelHtml" />
        </InfoCard>
        <div v-if="roleSections.length" class="mb-4 flex flex-wrap gap-2">
          <button
            v-for="item in roleSections"
            :key="item.role"
            class="rounded-2xl px-4 py-2 text-sm font-semibold text-white"
            :class="activeRole === item.role ? 'bg-[var(--brand)]' : 'bg-stone-800'"
            @click="activeRole = item.role"
          >
            {{ item.role }}
          </button>
        </div>
        <MarkdownBlock :content="selectedRoleContent" />
      </PageSection>

      <PageSection title="公共结论" subtitle="统一展示置信度、风险复核和行动视图。">
        <div class="grid gap-3 xl:grid-cols-3 md:grid-cols-1">
          <InfoCard title="决策置信度" :description="decisionConfidenceSummary" :meta="decisionConfidenceMeta" />
          <InfoCard title="风险复核" :meta="riskReviewMeta">
            <div class="markdown-compact" v-html="riskReviewHtml" />
          </InfoCard>
          <InfoCard title="行动视图" :meta="portfolioViewMeta">
            <div class="markdown-compact" v-html="portfolioViewHtml" />
          </InfoCard>
        </div>
        <div v-if="usedContextDims.length" class="mt-3 flex flex-wrap gap-2">
          <span v-for="item in usedContextDims" :key="item" class="metric-chip">{{ item }}</span>
        </div>
      </PageSection>

      <PageSection title="风控与通知" subtitle="展示 pre-trade 风控校验结果和通知发送状态。">
        <div class="grid gap-3 xl:grid-cols-2 md:grid-cols-1">
          <InfoCard
            title="Pre-trade 风控"
            :description="riskCheckDescription"
            :meta="riskCheckMeta"
          />
          <InfoCard
            title="通知发送"
            :description="notificationDescription"
            :meta="notificationMeta"
          />
        </div>
      </PageSection>

      <PageSection title="公共结论 / 完整原文" subtitle="如果角色切分不够完整，仍然可以回到完整 Markdown 原文。">
        <button class="mb-3 rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]" :aria-expanded="fullMarkdownExpanded ? 'true' : 'false'" @click="fullMarkdownExpanded = !fullMarkdownExpanded">
          {{ fullMarkdownExpanded ? '收起完整 Markdown' : '展开完整 Markdown' }}
        </button>
        <MarkdownBlock v-if="fullMarkdownExpanded" :content="fullMarkdown" />
        <div v-else class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-4 text-sm text-[var(--muted)]">
          完整原文默认不直接渲染，优先把首屏留给任务状态、子任务进度和关键结论。
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import MarkdownIt from 'markdown-it'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import {
  actMultiRoleTaskV3,
  decideMultiRoleTaskV3,
  fetchMultiRoleTaskV3,
  fetchStocks,
  streamMultiRoleTaskV3,
  triggerMultiRoleTaskV3,
} from '../../services/api/stocks'
import { fetchAuthStatus } from '../../services/api/auth'
import { buildTaskScopeKey } from '../../shared/taskPersistence/taskPersistence'
import { usePersistedTaskRunner } from '../../shared/taskPersistence/usePersistedTaskRunner'

function looksLikeTsCode(value: string) {
  return /^[0-9A-Z]{6}\.(SZ|SH|BJ)$/i.test(value.trim())
}

function downloadText(content: string, filename: string) {
  const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

const form = reactive({ keyword: '000001.SZ', lookback: 120 })
const fullMarkdown = ref('等待发起分析...')
const actionMessage = ref('准备就绪')
const roleSections = ref<Array<Record<string, any>>>([])
const activeRole = ref('')
const usedModel = ref('')
const attempts = ref<Array<Record<string, any>>>([])
const resolvedStock = ref<{ ts_code: string; name: string }>({ ts_code: '', name: '' })
const decisionConfidence = ref<Record<string, any>>({})
const riskReview = ref<Record<string, any>>({})
const portfolioView = ref<Record<string, any>>({})
const usedContextDims = ref<string[]>([])
const preTradeCheck = ref<Record<string, any>>({})
const notification = ref<Record<string, any>>({})
const commonSectionsMarkdown = ref('')
const roleRuns = ref<Array<Record<string, any>>>([])
const aggregatorRun = ref<Record<string, any>>({})
const stageTimeline = ref<Array<Record<string, any>>>([])
const researchDebate = ref<Record<string, any>>({})
const riskDebate = ref<Record<string, any>>({})
const portfolioReview = ref<Record<string, any>>({})
const acceptAutoDegrade = ref(true)
const currentJobId = ref('')
const taskStatus = ref('')
const taskStage = ref('')
const taskUpdatedAt = ref('')
const decisionPending = ref(false)
const aggregateRetryPending = ref(false)
const pollRetryCount = ref(0)
const pollMaxRetries = 5
const fullMarkdownExpanded = ref(false)
let timer = 0
let streamController: AbortController | null = null
const taskScopeKey = computed(() => buildTaskScopeKey('multi-role-research', 'active-task'))
const {
  restoredHint: restoredTaskHint,
  noticeMessage: taskPersistenceNotice,
  restoreTaskSnapshot,
  persistTaskSnapshot,
  clearPersistedTaskSnapshot,
} = usePersistedTaskRunner(() => taskScopeKey.value)

const selectedRoleSection = computed(() => roleSections.value.find((item) => item.role === activeRole.value) || null)
const selectedRoleContent = computed(() => selectedRoleSection.value?.content || fullMarkdown.value)
const attemptChain = computed(() => attempts.value.map((item) => `${item.model || '-'}${item.error ? '×' : '√'}`).join(' -> '))
const riskCheckDescription = computed(() => {
  if (!Object.keys(preTradeCheck.value).length) return '未启用或暂无风控校验结果。'
  const reasons = Array.isArray(preTradeCheck.value.reasons) ? preTradeCheck.value.reasons : []
  if (reasons.length) return reasons.join('；')
  return preTradeCheck.value.allowed ? '通过校验，可进入后续模拟执行。' : '未通过校验，请先检查约束。'
})
const riskCheckMeta = computed(() => {
  if (!Object.keys(preTradeCheck.value).length) return '-'
  const checks = Array.isArray(preTradeCheck.value.checks) ? preTradeCheck.value.checks.length : 0
  return `${preTradeCheck.value.allowed ? 'allowed' : 'blocked'} · checks ${checks}`
})
const notificationDescription = computed(() => {
  if (!Object.keys(notification.value).length) return '未启用通知或本次未触发。'
  if (notification.value.ok) return '通知已发送。'
  if (notification.value.skipped) return `通知未发送：${notification.value.reason || 'skipped'}`
  return `通知失败：${notification.value.error || 'unknown error'}`
})
const notificationMeta = computed(() => {
  if (!Object.keys(notification.value).length) return '-'
  if (notification.value.ok) return 'ok'
  if (notification.value.skipped) return 'skipped'
  return 'error'
})
const pendingDecision = computed(() => taskStatus.value === 'pending_user_decision')
const taskStatusLabel = computed(() => taskStatus.value || 'idle')
const taskStatusTone = computed(() => {
  if (pendingDecision.value) return 'warning'
  if (taskStatus.value === 'running' && taskStage.value) return 'info'
  if (['done', 'done_with_warnings', 'approved'].includes(taskStatus.value)) return 'success'
  if (['error', 'rejected', 'aborted'].includes(taskStatus.value)) return 'danger'
  if (taskStatus.value) return 'info'
  return 'muted'
})
const statusSummary = computed(() => {
  if (pendingDecision.value) return '存在失败角色，等待重试、降级或终止决策。'
  if (!taskStatus.value) return '尚未启动分析任务。'
  if (taskStatus.value === 'running' && taskStage.value === 'research_debate') {
    return `研究辩论阶段正在收敛中，已完成 ${researchDebateRounds.value} 轮，后续将继续进入研究经理裁决。`
  }
  if (taskStatus.value === 'running' && taskStage.value === 'risk_debate') {
    return `风险辩论阶段正在收敛中，已完成 ${riskDebateRounds.value} 轮，后续将继续进入 Portfolio Manager 审批。`
  }
  if (['done', 'done_with_warnings', 'approved'].includes(taskStatus.value)) return '任务已完成，优先查看汇总面板与审批结论。'
  if (['error', 'rejected', 'aborted'].includes(taskStatus.value)) return '任务已终止或失败，请查看子任务进度和错误信息。'
  return `任务仍在运行中，当前状态 ${taskStatus.value}。`
})
const markdown = new MarkdownIt({ html: false, linkify: true, breaks: true })
function normalizeSummary(value: unknown, fallback: string) {
  const text = String(value || '').trim()
  return text || fallback
}
const decisionConfidenceSummary = computed(() => normalizeSummary(decisionConfidence.value.summary, '原始分析未稳定提供数值化置信度。'))
const decisionConfidenceMeta = computed(() => {
  const label = String(decisionConfidence.value.label || '').trim()
  if (!label || label === '无' || label.toLowerCase() === 'none' || label.toLowerCase() === 'markdown') return '未显式给出'
  return label
})
const riskReviewText = computed(() => normalizeSummary(riskReview.value.summary, '暂无结构化风险复核'))
const riskReviewMeta = computed(() => {
  const source = String(riskReview.value.source || '').trim()
  if (!source || source.toLowerCase() === 'markdown') return '结构化摘要'
  return source
})
const riskReviewHtml = computed(() => markdown.render(riskReviewText.value))
const portfolioViewText = computed(() => normalizeSummary(portfolioView.value.summary, '暂无结构化行动视图'))
const portfolioViewMeta = computed(() => {
  const source = String(portfolioView.value.source || '').trim()
  if (!source || source.toLowerCase() === 'markdown') return '结构化摘要'
  return source
})
const portfolioViewHtml = computed(() => markdown.render(portfolioViewText.value))
const aggregatorPanelMeta = computed(() => {
  const run = aggregatorRun.value || {}
  const status = String(run.status || taskStatus.value || '-')
  const model = String(run.used_model || usedModel.value || '-')
  const duration = Number(run.duration_ms || 0)
  return `status ${status} · model ${model} · ${duration}ms`
})
const aggregatorPanelMarkdown = computed(() => {
  const content = String(commonSectionsMarkdown.value || '').trim()
  if (content) return content
  const err = String((aggregatorRun.value || {}).error || '').trim()
  if (err) return `> 汇总器错误：${err}`
  return '暂无汇总正文，可先查看各角色原文。'
})
const aggregatorPanelHtml = computed(() => markdown.render(aggregatorPanelMarkdown.value))
const researchDebateRounds = computed(() => Number((researchDebate.value?.rounds || []).length || 0))
const riskDebateRounds = computed(() => Number((riskDebate.value?.rounds || []).length || 0))
const researchDebateText = computed(() => {
  const summary = String((researchDebate.value?.summary || {}).claim || '').trim()
  if (summary) return summary
  return '暂无研究辩论摘要。'
})
const riskDebateText = computed(() => {
  const summary = String((riskDebate.value?.summary || {}).claim || '').trim()
  if (summary) return summary
  return '暂无风险辩论摘要。'
})
const researchDebateHtml = computed(() => markdown.render(researchDebateText.value))
const riskDebateHtml = computed(() => markdown.render(riskDebateText.value))
const portfolioDecisionText = computed(() => {
  const decision = String(portfolioReview.value?.decision || '').trim().toLowerCase()
  const claim = String(portfolioReview.value?.claim || '').trim()
  const label = decision ? `决策：${decision.toUpperCase()}` : '决策：未显式给出'
  if (!claim) return `${label}\n\n暂无审批说明。`
  return `${label}\n\n${claim}`
})
const portfolioDecisionMeta = computed(() => {
  const decision = String(portfolioReview.value?.decision || '').trim().toLowerCase()
  if (!decision) return 'pending'
  return decision
})
const portfolioDecisionHtml = computed(() => markdown.render(portfolioDecisionText.value))
const hasAggregatorContent = computed(() => String(commonSectionsMarkdown.value || '').trim().length > 0)
const canRetryAggregate = computed(() => {
  const status = taskStatus.value
  const aggStatus = String(aggregatorRun.value?.status || '')
  const terminal = status === 'done' || status === 'done_with_warnings' || status === 'approved' || status === 'rejected' || status === 'deferred'
  const needRetry = aggStatus === 'error' || !hasAggregatorContent.value
  return Boolean(currentJobId.value) && terminal && needRetry
})
const { data: authStatus, refetch: refetchAuthStatus } = useQuery({
  queryKey: ['auth-status-multi-role-page'],
  queryFn: () => fetchAuthStatus(true),
  staleTime: 10_000,
})
const quotaHint = computed(() => {
  const q = authStatus.value?.multi_role_quota as any
  if (!q) return ''
  if (q.limit == null) return '不限'
  return `${q.used ?? 0} / ${q.limit}，剩余 ${q.remaining ?? 0}`
})

function clearTimer() {
  window.clearTimeout(timer)
  timer = 0
}

function pollRetryDelayMs(retryCount: number) {
  return Math.min(8000, 1000 * (2 ** Math.max(0, retryCount - 1)))
}

function stopLiveStream() {
  if (streamController) {
    streamController.abort()
    streamController = null
  }
}

function resetResultViews() {
  roleSections.value = []
  activeRole.value = ''
  usedModel.value = ''
  attempts.value = []
  decisionConfidence.value = {}
  riskReview.value = {}
  portfolioView.value = {}
  usedContextDims.value = []
  preTradeCheck.value = {}
  notification.value = {}
  commonSectionsMarkdown.value = ''
  stageTimeline.value = []
  researchDebate.value = {}
  riskDebate.value = {}
  portfolioReview.value = {}
  taskStage.value = ''
  taskUpdatedAt.value = ''
}

function persistCurrentTaskSnapshot(task: Record<string, any> = {}) {
  if (!currentJobId.value) return
  persistTaskSnapshot({
    jobId: currentJobId.value,
    status: String(task.status || taskStatus.value || ''),
    stage: String(task.stage || taskStage.value || ''),
    progress: Number(task.progress || 0),
    requestedModel: String(task.requested_model || task.model || ''),
    usedModel: String(task.used_model || usedModel.value || ''),
    attempts: Array.isArray(attempts.value) ? attempts.value : [],
    actionMessage: actionMessage.value,
    error: String(task.error || ''),
    updatedAt: String(task.updated_at || taskUpdatedAt.value || new Date().toISOString()),
    data: {
      resolvedStock: resolvedStock.value,
      fullMarkdown: fullMarkdown.value,
      roleSections: roleSections.value,
      activeRole: activeRole.value,
    },
  })
}

function applyTaskSnapshot(task: Record<string, any>, resolved?: { ts_code: string; name: string }) {
  taskStatus.value = String(task.status || '')
  taskStage.value = String(task.stage || task.status || '')
  taskUpdatedAt.value = String(task.updated_at || taskUpdatedAt.value || '')
  stageTimeline.value = Array.isArray(task.v3_stage_timeline) ? task.v3_stage_timeline : stageTimeline.value
  researchDebate.value = (task.v3_research_debate || {}) as Record<string, any>
  riskDebate.value = (task.v3_risk_debate || {}) as Record<string, any>
  portfolioReview.value = (task.v3_portfolio_review || {}) as Record<string, any>
  roleRuns.value = Array.isArray(task.role_runs) ? task.role_runs : []
  aggregatorRun.value = (task.aggregator_run || {}) as Record<string, any>
  attempts.value = Array.isArray(task.attempts) ? task.attempts : attempts.value

  if (task.status === 'done' || task.status === 'done_with_warnings' || task.status === 'approved' || task.status === 'rejected' || task.status === 'deferred') {
    usedModel.value = String(task.used_model || task.model || usedModel.value || '')
    fullMarkdown.value = task.analysis_markdown || task.analysis || task.result || fullMarkdown.value || '分析完成，但未返回正文。'
    roleSections.value = Array.isArray(task.role_outputs) ? task.role_outputs : (Array.isArray(task.role_sections) ? task.role_sections : roleSections.value)
    activeRole.value = roleSections.value[0]?.role || activeRole.value
    decisionConfidence.value = (task.decision_confidence || {}) as Record<string, any>
    riskReview.value = (task.risk_review || {}) as Record<string, any>
    portfolioView.value = (task.portfolio_view || {}) as Record<string, any>
    usedContextDims.value = Array.isArray(task.used_context_dims) ? task.used_context_dims : []
    preTradeCheck.value = (task.pre_trade_check || {}) as Record<string, any>
    notification.value = (task.notification || {}) as Record<string, any>
    commonSectionsMarkdown.value = String(task.common_sections_markdown || '')
    const statusLabel = String(task.status || '').toUpperCase()
    actionMessage.value = `分析完成：${task.name || resolved?.name || resolved?.ts_code || resolvedStock.value.name || resolvedStock.value.ts_code} · ${statusLabel}${task.status === 'done_with_warnings' ? '（含降级告警）' : ''}${usedModel.value ? ` · 实际模型 ${usedModel.value}` : ''}`
    persistCurrentTaskSnapshot(task)
    return true
  }
  if (task.status === 'pending_user_decision') {
    actionMessage.value = '有角色任务失败，等待你的决策（重试/降级/终止）。'
    persistCurrentTaskSnapshot(task)
    return true
  }
  if (task.status === 'error') {
    actionMessage.value = `分析失败：${task.error || task.message || '未知错误'}`
    fullMarkdown.value = `分析失败：${task.error || task.message || '未知错误'}`
    resetResultViews()
    persistCurrentTaskSnapshot(task)
    return true
  }
  const queueInfo = (task.queue_info || {}) as Record<string, any>
  const queueAlert = Boolean(queueInfo.alert)
  const queueEtaMinutes = Number(queueInfo.estimated_wait_minutes || 0)
  if (task.status === 'queued' && queueAlert) {
    const etaLabel = queueEtaMinutes > 0 ? `${queueEtaMinutes}` : '1'
    actionMessage.value = `排队中，预计等待 ${etaLabel} 分钟（前方排队 ${queueInfo.queue_position || '-'} / 总排队 ${queueInfo.queued_total || '-'}）`
    return false
  }
  actionMessage.value = `任务运行中：${task.progress || 0}% · ${task.message || task.status || '运行中'}`
  persistCurrentTaskSnapshot(task)
  return false
}

function startPolling(jobId: string, resolved?: { ts_code: string; name: string }) {
  clearTimer()
  pollRetryCount.value = 0
  const poll = async (): Promise<void> => {
    try {
      const task = await fetchMultiRoleTaskV3(jobId)
      pollRetryCount.value = 0
      const terminal = applyTaskSnapshot(task as Record<string, any>, resolved)
      if (!terminal) timer = window.setTimeout(poll, 3000)
    } catch (error: any) {
      pollRetryCount.value += 1
      const message = error?.message || String(error)
      if (pollRetryCount.value <= pollMaxRetries) {
        const delay = pollRetryDelayMs(pollRetryCount.value)
        const seconds = Math.max(1, Math.round(delay / 1000))
        actionMessage.value = `轮询短暂失败（第 ${pollRetryCount.value}/${pollMaxRetries} 次），${seconds} 秒后自动重试...`
        persistCurrentTaskSnapshot({ status: taskStatus.value || 'running', stage: taskStage.value || 'polling' })
        timer = window.setTimeout(poll, delay)
        return
      }
      actionMessage.value = `轮询失败：${message}（endpoint: /api/llm/multi-role/v3/jobs/${jobId}）`
      persistCurrentTaskSnapshot({ status: 'error', stage: 'polling', error: message, progress: 100 })
    }
  }
  poll()
}

function startLiveStream(jobId: string, resolved?: { ts_code: string; name: string }) {
  clearTimer()
  stopLiveStream()
  const controller = new AbortController()
  streamController = controller
  streamMultiRoleTaskV3(
    { job_id: jobId, interval_ms: 1000, timeout_seconds: 300 },
    {
      signal: controller.signal,
      onMessage: (packet: Record<string, any>) => {
        const event = String(packet?.event || '')
        if (event === 'update' && packet?.job) {
          const terminal = applyTaskSnapshot(packet.job as Record<string, any>, resolved)
          if (terminal) controller.abort()
          return
        }
        if (event === 'timeout') {
          actionMessage.value = '流式连接超时，切换轮询继续跟踪...'
          persistCurrentTaskSnapshot({ status: taskStatus.value || 'running', stage: taskStage.value || 'stream_timeout' })
          controller.abort()
          startPolling(jobId, resolved)
          return
        }
        if (event === 'error') {
          actionMessage.value = `流式更新失败：${packet?.error || 'unknown error'}，切换轮询继续跟踪...`
          persistCurrentTaskSnapshot({ status: taskStatus.value || 'running', stage: taskStage.value || 'stream_error', error: String(packet?.error || '') })
          controller.abort()
          startPolling(jobId, resolved)
        }
      },
    },
  ).catch((error: any) => {
    if (controller.signal.aborted) return
    actionMessage.value = `流式更新中断：${error?.message || String(error)}，切换轮询继续跟踪...`
    persistCurrentTaskSnapshot({ status: taskStatus.value || 'running', stage: taskStage.value || 'stream_disconnected', error: String(error?.message || error) })
    startPolling(jobId, resolved)
  }).finally(() => {
    if (streamController === controller) streamController = null
  })
}

const mutation = useMutation({
  mutationFn: async () => {
    const raw = form.keyword.trim()
    if (!raw) throw new Error('请输入股票代码或简称')
    if (looksLikeTsCode(raw)) {
      return { ts_code: raw.toUpperCase(), name: '' }
    }
    const searchResult = await fetchStocks({ keyword: raw, status: 'L', page: 1, page_size: 5 })
    const first = searchResult.items?.[0]
    if (!first?.ts_code) throw new Error(`未找到匹配股票：${raw}`)
    return { ts_code: String(first.ts_code), name: String(first.name || '') }
  },
  onSuccess: async (resolved) => {
    stopLiveStream()
    clearTimer()
    resolvedStock.value = resolved
    resetResultViews()
    roleRuns.value = []
    aggregatorRun.value = {}
    taskStatus.value = ''
    currentJobId.value = ''
    actionMessage.value = `已解析股票：${resolved.name || resolved.ts_code}，正在创建分析任务...`
    const payload = await triggerMultiRoleTaskV3({
      ts_code: resolved.ts_code,
      lookback: form.lookback,
      accept_auto_degrade: acceptAutoDegrade.value,
      max_research_debate_rounds: 1,
      max_risk_debate_rounds: 1,
      early_stop: true,
    })
    const jobId = String(payload.job_id || '')
    currentJobId.value = jobId
    fullMarkdown.value = '任务已创建，正在后台生成分析...'
    persistCurrentTaskSnapshot({
      status: String(payload.status || 'queued'),
      stage: String(payload.stage || 'queued'),
      progress: Number(payload.progress || 5),
      requested_model: String(payload.requested_model || payload.model || ''),
      updated_at: String(payload.updated_at || new Date().toISOString()),
    })
    const terminal = applyTaskSnapshot(payload as Record<string, any>, resolved)
    if (!terminal && jobId) startLiveStream(jobId, resolved)
    refetchAuthStatus()
  },
  onError: (error: Error) => {
    stopLiveStream()
    clearTimer()
    actionMessage.value = `分析失败：${error.message}`
    fullMarkdown.value = `分析失败：${error.message}`
    resetResultViews()
    roleRuns.value = []
    aggregatorRun.value = {}
    taskStatus.value = ''
    currentJobId.value = ''
    clearPersistedTaskSnapshot()
  },
})

const isPending = computed(() => mutation.isPending.value)

function runAnalysis() {
  mutation.mutate()
}

async function submitDecision(action: 'retry' | 'degrade' | 'abort') {
  if (!currentJobId.value || decisionPending.value) return
  decisionPending.value = true
  try {
    const res = await decideMultiRoleTaskV3(currentJobId.value, action)
    const terminal = applyTaskSnapshot(res as Record<string, any>, resolvedStock.value)
    if (action === 'abort' || res.status === 'error') {
      actionMessage.value = `任务已终止：${res.error || '用户终止'}`
      fullMarkdown.value = `任务已终止：${res.error || '用户终止'}`
      persistCurrentTaskSnapshot(res as Record<string, any>)
      return
    }
    actionMessage.value = action === 'degrade' ? '正在按降级策略收口汇总...' : '正在执行补重试，请稍候...'
    persistCurrentTaskSnapshot(res as Record<string, any>)
    if (!terminal && currentJobId.value) startLiveStream(currentJobId.value, resolvedStock.value)
  } catch (error: any) {
    actionMessage.value = `决策提交失败：${error?.message || String(error)}`
    persistCurrentTaskSnapshot({ status: taskStatus.value || '', stage: taskStage.value || '', error: String(error?.message || error) })
  } finally {
    decisionPending.value = false
  }
}

async function retryAggregate() {
  if (!currentJobId.value || aggregateRetryPending.value || !canRetryAggregate.value) return
  aggregateRetryPending.value = true
  actionMessage.value = '正在重试聚合汇总，请稍候...'
  try {
    const res = await actMultiRoleTaskV3(currentJobId.value, 're_aggregate')
    const terminal = applyTaskSnapshot(res as Record<string, any>, resolvedStock.value)
    const aggStatus = String((res.aggregator_run || {}).status || '')
    actionMessage.value = aggStatus === 'done' ? '汇总重试成功。' : '汇总重试失败，已保留角色原文。'
    persistCurrentTaskSnapshot(res as Record<string, any>)
    if (!terminal && currentJobId.value) startLiveStream(currentJobId.value, resolvedStock.value)
  } catch (error: any) {
    actionMessage.value = `重试汇总失败：${error?.message || String(error)}`
    persistCurrentTaskSnapshot({ status: taskStatus.value || '', stage: taskStage.value || '', error: String(error?.message || error) })
  } finally {
    aggregateRetryPending.value = false
  }
}

function downloadRoleMarkdown() {
  if (!selectedRoleSection.value) return
  const stock = resolvedStock.value.ts_code || 'UNKNOWN'
  downloadText(selectedRoleSection.value.content || '', `${stock}_${selectedRoleSection.value.role}_多角色分析.md`)
}

function downloadFullMarkdown() {
  const stock = resolvedStock.value.ts_code || 'UNKNOWN'
  downloadText(fullMarkdown.value || '', `${stock}_LLM多角色公司分析.md`)
}

function clearCurrentTaskTracking() {
  stopLiveStream()
  clearTimer()
  currentJobId.value = ''
  taskStatus.value = ''
  taskStage.value = ''
  taskUpdatedAt.value = ''
  attempts.value = []
  actionMessage.value = '已清除当前任务跟踪。'
  clearPersistedTaskSnapshot()
}

onMounted(async () => {
  const restored = restoreTaskSnapshot()
  if (!restored) return
  currentJobId.value = String(restored.jobId || '')
  taskStatus.value = String(restored.status || '')
  taskStage.value = String(restored.stage || '')
  usedModel.value = String(restored.usedModel || '')
  attempts.value = Array.isArray(restored.attempts) ? restored.attempts : []
  actionMessage.value = restored.actionMessage || '已恢复历史任务跟踪。'
  if (restored.data?.resolvedStock) {
    const restoredStockRaw = restored.data.resolvedStock as Record<string, any>
    resolvedStock.value = {
      ts_code: String(restoredStockRaw.ts_code || ''),
      name: String(restoredStockRaw.name || ''),
    }
    if (resolvedStock.value.ts_code) form.keyword = resolvedStock.value.ts_code
  }
  if (typeof restored.data?.fullMarkdown === 'string' && restored.data.fullMarkdown) {
    fullMarkdown.value = String(restored.data.fullMarkdown)
  }
  if (Array.isArray(restored.data?.roleSections) && restored.data.roleSections.length) {
    roleSections.value = restored.data.roleSections as Array<Record<string, any>>
    activeRole.value = String(restored.data?.activeRole || roleSections.value[0]?.role || '')
  }
  if (!currentJobId.value) return
  try {
    const latest = await fetchMultiRoleTaskV3(currentJobId.value)
    const terminal = applyTaskSnapshot(latest as Record<string, any>, resolvedStock.value)
    if (!terminal) startLiveStream(currentJobId.value, resolvedStock.value)
  } catch (error: any) {
    actionMessage.value = `恢复任务失败：${error?.message || String(error)}`
    persistCurrentTaskSnapshot({ status: taskStatus.value || 'error', stage: taskStage.value || '', error: String(error?.message || error) })
  }
})

onBeforeUnmount(() => {
  stopLiveStream()
  clearTimer()
})
</script>

<style scoped>
.markdown-compact :deep(p),
.markdown-compact :deep(ul),
.markdown-compact :deep(ol) {
  margin: 0;
}

.markdown-compact :deep(p + p),
.markdown-compact :deep(ul + p),
.markdown-compact :deep(p + ul),
.markdown-compact :deep(ol + p),
.markdown-compact :deep(p + ol) {
  margin-top: 0.5rem;
}

.markdown-compact :deep(ul),
.markdown-compact :deep(ol) {
  padding-left: 1rem;
}

.markdown-compact :deep(li + li) {
  margin-top: 0.25rem;
}
</style>
