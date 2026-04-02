<template>
  <AppShell title="多角色公司分析" subtitle="支持股票简称搜索、异步任务轮询、按角色查看和分角色下载。">
    <div class="space-y-4">
      <PageSection title="分析发起" subtitle="可以直接输 ts_code，也可以输股票简称，由前端自动解析第一条匹配结果。">
        <div class="grid gap-3 xl:grid-cols-[1fr_140px_180px] md:grid-cols-2">
          <input v-model="form.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="输入 ts_code 或简称，如 000001.SZ / 平安银行" />
          <select v-model.number="form.lookback" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="60">60 日</option>
            <option :value="120">120 日</option>
            <option :value="240">240 日</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" :disabled="isPending" @click="runAnalysis">
            {{ isPending ? '任务创建中...' : '发起分析' }}
          </button>
        </div>
        <div class="mt-3 rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)] shadow-[var(--shadow-soft)]">
          {{ actionMessage }}
        </div>
        <div class="mt-2 text-sm text-[var(--muted)]">实际模型：{{ usedModel }}</div>
        <div v-if="attemptChain" class="mt-1 text-sm text-[var(--muted)]">尝试链路：{{ attemptChain }}</div>
        <div v-if="quotaHint" class="mt-1 text-sm text-[var(--muted)]">今日额度：{{ quotaHint }}</div>
      </PageSection>

      <PageSection title="角色视图" subtitle="优先展示按角色切分后的结论，也保留完整原文。">
        <template #action>
          <div class="flex flex-wrap gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white" :disabled="!selectedRoleSection" @click="downloadRoleMarkdown">下载当前角色 Markdown</button>
            <button class="rounded-2xl bg-blue-700 px-4 py-2 text-white" :disabled="!fullMarkdown" @click="downloadFullMarkdown">下载完整 Markdown</button>
          </div>
        </template>
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
          <InfoCard title="决策置信度" :description="decisionConfidence.summary || '暂无结构化置信度'" :meta="decisionConfidence.label || '-'" />
          <InfoCard title="风险复核" :description="riskReview.summary || '暂无结构化风险复核'" :meta="riskReview.source || '-'" />
          <InfoCard title="行动视图" :description="portfolioView.summary || '暂无结构化行动视图'" :meta="portfolioView.source || '-'" />
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
        <MarkdownBlock :content="fullMarkdown" />
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchMultiRoleTask, fetchStocks, triggerMultiRoleTask } from '../../services/api/stocks'
import { fetchAuthStatus } from '../../services/api/auth'

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
let timer = 0

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
    resolvedStock.value = resolved
    usedModel.value = ''
    attempts.value = []
    actionMessage.value = `已解析股票：${resolved.name || resolved.ts_code}，正在创建分析任务...`
    const payload = await triggerMultiRoleTask({ ts_code: resolved.ts_code, lookback: form.lookback })
    const jobId = payload.job_id
    fullMarkdown.value = '任务已创建，正在后台生成分析...'
    if (!jobId) return
    window.clearTimeout(timer)
    const poll = async () => {
      const res = await fetchMultiRoleTask({ job_id: jobId })
      if (res.status === 'done') {
        usedModel.value = String(res.used_model || res.model || '')
        attempts.value = Array.isArray(res.attempts) ? res.attempts : []
        actionMessage.value = `分析完成：${res.name || resolved.name || resolved.ts_code}${usedModel.value ? ` · 实际模型 ${usedModel.value}` : ''}`
        fullMarkdown.value = res.analysis_markdown || res.analysis || res.result || '分析完成，但未返回正文。'
        roleSections.value = Array.isArray(res.role_outputs) ? res.role_outputs : (Array.isArray(res.role_sections) ? res.role_sections : [])
        activeRole.value = roleSections.value[0]?.role || ''
        decisionConfidence.value = (res.decision_confidence || {}) as Record<string, any>
        riskReview.value = (res.risk_review || {}) as Record<string, any>
        portfolioView.value = (res.portfolio_view || {}) as Record<string, any>
        usedContextDims.value = Array.isArray(res.used_context_dims) ? res.used_context_dims : []
        preTradeCheck.value = (res.pre_trade_check || {}) as Record<string, any>
        notification.value = (res.notification || {}) as Record<string, any>
        return
      }
      if (res.status === 'error') {
        attempts.value = Array.isArray(res.attempts) ? res.attempts : []
        actionMessage.value = `分析失败：${res.error || res.message || '未知错误'}`
        fullMarkdown.value = `分析失败：${res.error || res.message || '未知错误'}`
        roleSections.value = []
        activeRole.value = ''
        decisionConfidence.value = {}
        riskReview.value = {}
        portfolioView.value = {}
        usedContextDims.value = []
        preTradeCheck.value = {}
        notification.value = {}
        return
      }
      actionMessage.value = `任务运行中：${res.progress || 0}% · ${res.message || res.status || '运行中'}`
      timer = window.setTimeout(poll, 3000)
    }
    poll()
    refetchAuthStatus()
  },
  onError: (error: Error) => {
    actionMessage.value = `分析失败：${error.message}`
    fullMarkdown.value = `分析失败：${error.message}`
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
  },
})

const isPending = computed(() => mutation.isPending.value)

function runAnalysis() {
  mutation.mutate()
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
</script>
