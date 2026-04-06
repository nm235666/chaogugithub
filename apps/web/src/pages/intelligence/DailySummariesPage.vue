<template>
  <AppShell title="新闻日报总结" subtitle="支持查询历史日报、主动生成今日总结，并把结果直接沉淀在新前端里。">
    <div class="space-y-4">
      <PageSection title="查询与生成" subtitle="按日期、来源关键字和实际模型筛选，也可以主动生成今日总结。">
        <div class="grid gap-3 xl:grid-cols-[180px_1fr_180px_120px] md:grid-cols-2">
          <input v-model="draftFilters.summary_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="日期 YYYY-MM-DD（可选）" />
          <input v-model="draftFilters.source_filter" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="来源过滤关键字（可选）" />
          <select v-model="draftFilters.model" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部模型</option>
            <option value="GPT-5.4">GPT-5.4</option>
            <option value="gpt-5.4-daily-summary">gpt-5.4-daily-summary</option>
            <option value="zhipu-news">zhipu-news</option>
            <option value="kimi-k2.5">kimi-k2.5</option>
            <option value="deepseek-chat">deepseek-chat</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isFetching" @click="applyFilters">
            {{ isFetching ? '查询中...' : '查询' }}
          </button>
        </div>
        <div class="mt-3 flex flex-wrap gap-3">
          <button class="rounded-2xl bg-blue-700 px-4 py-3 font-semibold text-white disabled:opacity-50" :disabled="isGenerating || !canGenerate" @click="generateTodaySummary">
            {{ isGenerating ? '生成中...' : '生成今日总结' }}
          </button>
          <button
            class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm font-semibold text-[var(--ink)] disabled:opacity-40"
            :disabled="!activeJobId && !attempts.length"
            @click="clearCurrentTaskTracking"
          >
            清除当前任务跟踪
          </button>
          <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)] shadow-[var(--shadow-soft)]">
            {{ actionMessage }}
          </div>
        </div>
        <div v-if="restoredTaskHint" class="mt-2 text-sm text-[var(--muted)]">已从会话恢复任务跟踪（{{ restoredTaskHint }}）</div>
        <div v-if="taskPersistenceNotice" class="mt-2 text-sm text-[var(--muted)]">{{ taskPersistenceNotice }}</div>
        <div v-if="attemptChain" class="mt-2 text-sm text-[var(--muted)]">尝试链路：{{ attemptChain }}</div>
        <div v-if="lastAttemptError" class="mt-2 text-sm text-[var(--muted)]">最后错误：{{ lastAttemptError }}</div>
        <div v-if="protocolMetaText" class="mt-2 text-sm text-[var(--muted)]">{{ protocolMetaText }}</div>
        <StatePanel
          v-if="queryError"
          class="mt-3"
          tone="danger"
          title="日报列表加载失败"
          :description="queryError"
        >
          <template #action>
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="reloadList">重新加载</button>
          </template>
        </StatePanel>
      </PageSection>

      <PageSection title="任务闭环状态" subtitle="展示日报任务的通知发送状态，便于联调 webhook。">
        <InfoCard title="通知发送" :description="notificationDescription" :meta="notificationMeta" />
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <PageSection :title="`日报结果 (${result?.total || 0})`" subtitle="左侧列表切换，右侧查看正文。">
          <StatePanel
            v-if="!queryError && !isFetching && !(result?.items || []).length"
            class="mb-4"
            tone="warning"
            title="当前筛选下没有日报结果"
            description="可以先清空日期和来源关键字，或直接生成今日总结建立一条新的日报记录。"
          >
            <template #action>
              <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 font-semibold text-white" @click="resetFilters">恢复默认筛选</button>
            </template>
          </StatePanel>
          <div class="space-y-2">
            <InfoCard
              v-for="item in result?.items || []"
              :key="item.id"
              :title="item.summary_date || '-'" :meta="`实际模型 ${item.model || '-'} · 新闻数 ${item.news_count || 0} · 创建 ${formatDateTime(item.created_at)}`"
              :description="item.title || item.source_filter || ''"
              @click="selectItem(item)"
            >
              <template #badge>
                <StatusBadge :value="selectedItem?.id === item.id ? 'info' : 'muted'" :label="selectedItem?.id === item.id ? '当前查看' : '查看'" />
              </template>
            </InfoCard>
          </div>
        <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ queryFilters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="goPrevPage">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page >= (result?.total_pages || 1)" @click="goNextPage">下一页</button>
          </div>
        </div>
      </PageSection>

        <PageSection title="总结详情" subtitle="保留 Markdown 原文，并支持直接下载。">
          <template #action>
            <div class="flex flex-wrap gap-2">
              <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white" :disabled="!selectedItem" @click="downloadMarkdown">下载 Markdown</button>
              <button class="rounded-2xl bg-blue-700 px-4 py-2 text-white" :disabled="!selectedItem" @click="downloadImage">下载图片</button>
            </div>
          </template>
          <div ref="detailExportRef">
            <MarkdownBlock :content="selectedContent" />
          </div>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { keepPreviousData, useMutation, useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import { fetchDailySummaries, fetchDailySummaryTask, triggerDailySummaryGenerate } from '../../services/api/news'
import { fetchAuthStatus } from '../../services/api/auth'
import { formatDateTime } from '../../shared/utils/format'
import { downloadElementAsImage, downloadTextFile } from '../../shared/utils/export'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'
import { buildTaskScopeKey } from '../../shared/taskPersistence/taskPersistence'
import { usePersistedTaskRunner } from '../../shared/taskPersistence/usePersistedTaskRunner'

const route = useRoute()
const router = useRouter()

const queryFilters = reactive({
  summary_date: '',
  source_filter: '',
  model: '',
  page: 1,
  page_size: 10,
})
const draftFilters = reactive({ ...queryFilters })

const selectedItem = ref<Record<string, any> | null>(null)
const actionMessage = ref('准备就绪')
const attempts = ref<Array<Record<string, any>>>([])
const activeJobId = ref('')
const detailExportRef = ref<HTMLElement | null>(null)
const latestTaskNotification = ref<Record<string, any>>({})
let pollTimer = 0
const taskScopeKey = computed(() => buildTaskScopeKey(String(route.name || 'daily-summaries'), 'generate'))
const {
  restoredHint: restoredTaskHint,
  noticeMessage: taskPersistenceNotice,
  restoreTaskSnapshot,
  persistTaskSnapshot,
  clearPersistedTaskSnapshot,
} = usePersistedTaskRunner(() => taskScopeKey.value)

const { data: result, refetch, isFetching, error } = useQuery({
  queryKey: ['daily-summaries', queryFilters],
  queryFn: () => fetchDailySummaries(queryFilters),
  placeholderData: keepPreviousData,
})

watch(
  () => result.value?.items,
  (items) => {
    if (!items?.length) {
      selectedItem.value = null
      return
    }
    if (!selectedItem.value) {
      selectedItem.value = items[0]
      return
    }
    const matched = items.find((item: Record<string, any>) => item.id === selectedItem.value?.id)
    if (matched) selectedItem.value = matched
  },
  { immediate: true },
)

function normalizeMarkdownForDisplay(raw: string) {
  let text = String(raw || '')
  if (!text.includes('\n') && text.includes('\\n')) {
    text = text.replace(/\\n/g, '\n')
  }
  text = text.replace(/\r\n/g, '\n')
  text = text.replace(/(^|\n)(#{1,6})(?=\S)/g, '$1$2 ')
  text = text.replace(/([。；：.!?])\s*(#{1,6}\s)/g, '$1\n\n$2')
  return text
}

const selectedContent = computed(() => {
  const primary = String(selectedItem.value?.analysis_markdown || '').trim()
  if (primary) return normalizeMarkdownForDisplay(primary)
  const fallback = selectedItem.value?.summary_markdown || selectedItem.value?.summary_text || '请选择左侧一条日报总结查看内容。'
  return normalizeMarkdownForDisplay(String(fallback))
})
const attemptChain = computed(() => attempts.value.map((item) => `${item.model || '-'}${item.error ? '×' : '√'}`).join(' -> '))
const lastAttemptError = computed(() => {
  const failed = [...attempts.value].reverse().find((item) => String(item?.error || '').trim())
  return String(failed?.error || '').trim()
})
const queryError = computed(() => error.value?.message || '')
const protocolMetaText = computed(() => {
  const protocol = result.value?.protocol || {}
  const version = String(protocol.version || '').trim()
  const primary = String(protocol.primary_markdown_field || '').trim()
  const compat = String(protocol.compat_markdown_field || '').trim()
  const retireAfter = String(protocol.compat_retire_after || '').trim()
  if (!version && !primary && !compat && !retireAfter) return ''
  return `协议版本 ${version || '-'} · 主字段 ${primary || '-'} · 兼容字段 ${compat || '-'} · 兼容退场时间 ${retireAfter || '-'}`
})
const notificationDescription = computed(() => {
  if (!Object.keys(latestTaskNotification.value).length) return '尚未触发任务通知或通知开关未开启。'
  if (latestTaskNotification.value.ok) return '通知已发送。'
  if (latestTaskNotification.value.skipped) return `通知未发送：${latestTaskNotification.value.reason || 'skipped'}`
  return `通知失败：${latestTaskNotification.value.error || 'unknown error'}`
})
const notificationMeta = computed(() => {
  if (!Object.keys(latestTaskNotification.value).length) return '-'
  if (latestTaskNotification.value.ok) return 'ok'
  if (latestTaskNotification.value.skipped) return 'skipped'
  return 'error'
})

function persistDailyTask(task: Record<string, any>, messageOverride = '') {
  if (!activeJobId.value) return
  persistTaskSnapshot({
    jobId: activeJobId.value,
    status: String(task.status || ''),
    stage: String(task.stage || ''),
    progress: Number(task.progress || 0),
    requestedModel: String(task.requested_model || ''),
    usedModel: String(task.used_model || ''),
    attempts: Array.isArray(attempts.value) ? attempts.value : [],
    actionMessage: messageOverride || actionMessage.value,
    error: String(task.error || ''),
    updatedAt: String(task.updated_at || new Date().toISOString()),
    data: {
      notification: latestTaskNotification.value,
    },
  })
}

function startTaskPolling(jobId: string) {
  activeJobId.value = jobId
  window.clearTimeout(pollTimer)
  const poll = async () => {
    try {
      const task = await fetchDailySummaryTask({ job_id: jobId })
      attempts.value = Array.isArray(task.attempts) ? task.attempts : attempts.value
      if (task.status === 'done') {
        actionMessage.value = task.fallback_used || (task.used_model && task.requested_model && task.used_model !== task.requested_model)
          ? `日报总结生成完成 · 已自动降级到可用模型 ${task.used_model || '-'}`
          : `日报总结生成完成${task.used_model ? ` · 实际模型 ${task.used_model}` : ''}`
        latestTaskNotification.value = (task.notification || {}) as Record<string, any>
        persistDailyTask(task, actionMessage.value)
        await refetch()
        if (task.item) selectedItem.value = task.item
        return
      }
      if (task.status === 'error' || task.status === 'aborted') {
        const errorDetail = task.error || task.message || '未知错误'
        actionMessage.value = task.final_error_code === 'rate_limited' || errorDetail.includes('rate_limited')
          ? `日报总结生成失败：专用通道限流后仍未找到可用模型 · ${errorDetail}`
          : `日报总结生成失败：${errorDetail}`
        latestTaskNotification.value = (task.notification || {}) as Record<string, any>
        persistDailyTask(task, actionMessage.value)
        return
      }
      actionMessage.value = attempts.value.length
        ? `日报总结生成中：${task.progress || 0}% · 正在尝试可用模型链路`
        : `日报总结生成中：${task.progress || 0}% · ${task.message || task.stage || '运行中'}`
      persistDailyTask(task, actionMessage.value)
      pollTimer = window.setTimeout(poll, 3000)
    } catch (e: any) {
      actionMessage.value = `任务轮询失败：${e?.message || String(e)}`
      persistDailyTask({ status: 'error', stage: 'error', progress: 100, error: String(e?.message || e) }, actionMessage.value)
    }
  }
  poll()
}

const generateMutation = useMutation({
  mutationFn: () => triggerDailySummaryGenerate({ model: 'auto' }),
  onSuccess: (payload: Record<string, any>) => {
    const jobId = String(payload.job_id || '')
    attempts.value = Array.isArray(payload.attempts) ? payload.attempts : []
    actionMessage.value = `日报总结任务已创建：${payload.summary_date || ''} · 正在尝试日报模型链路`
    if (!jobId) return
    activeJobId.value = jobId
    latestTaskNotification.value = {}
    persistDailyTask({
      status: String(payload.status || 'queued'),
      stage: String(payload.stage || 'queued'),
      progress: Number(payload.progress || 0),
      requested_model: String(payload.requested_model || payload.model || ''),
      used_model: String(payload.used_model || ''),
      updated_at: String(payload.updated_at || new Date().toISOString()),
    })
    startTaskPolling(jobId)
  },
  onError: (error: Error) => {
    actionMessage.value = `生成失败：${error.message}`
    attempts.value = []
    latestTaskNotification.value = {}
    activeJobId.value = ''
    clearPersistedTaskSnapshot()
  },
})

const isGenerating = computed(() => generateMutation.isPending.value)
const { data: authStatus } = useQuery({
  queryKey: ['auth-status-for-daily-summary'],
  queryFn: () => fetchAuthStatus(),
})
const canGenerate = computed(() => {
  const role = String(authStatus.value?.user?.role || authStatus.value?.user?.tier || '')
  return role !== 'limited'
})

function selectItem(item: Record<string, any>) {
  selectedItem.value = item
}

function downloadMarkdown() {
  if (!selectedItem.value) return
  const date = selectedItem.value.summary_date || 'daily_summary'
  const model = selectedItem.value.model || 'model'
  const content = [
    `# 新闻日报总结`,
    '',
    `- 所选日期: ${date}`,
    `- 实际模型: ${model}`,
    '',
    selectedContent.value,
  ].join('\n')
  downloadTextFile(content, `${date}_新闻日报总结_${model}.md`, 'text/markdown;charset=utf-8')
}

async function downloadImage() {
  if (!selectedItem.value || !detailExportRef.value) return
  await nextTick()
  await downloadElementAsImage(detailExportRef.value, `${selectedItem.value.summary_date || 'daily_summary'}_新闻日报总结_${selectedItem.value.model || 'model'}.png`)
}

function generateTodaySummary() {
  actionMessage.value = '正在创建日报总结任务，并准备尝试可用模型链路...'
  generateMutation.mutate()
}

function clearCurrentTaskTracking() {
  activeJobId.value = ''
  attempts.value = []
  latestTaskNotification.value = {}
  actionMessage.value = '已清除当前任务跟踪。'
  window.clearTimeout(pollTimer)
  clearPersistedTaskSnapshot()
}

function applyFilters() {
  Object.assign(queryFilters, { ...draftFilters, page: 1 })
  syncRouteFromQuery()
}

function resetFilters() {
  Object.assign(draftFilters, { summary_date: '', source_filter: '', model: '', page: 1, page_size: 10 })
  applyFilters()
}

function reloadList() {
  refetch()
}

function syncRouteFromQuery() {
  router.replace({
    query: buildCleanQuery({
      summary_date: queryFilters.summary_date,
      source_filter: queryFilters.source_filter,
      model: queryFilters.model,
      page: queryFilters.page,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteQuery() {
  const q = route.query as Record<string, unknown>
  const next = {
    summary_date: readQueryString(q, 'summary_date', ''),
    source_filter: readQueryString(q, 'source_filter', ''),
    model: readQueryString(q, 'model', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(10, readQueryNumber(q, 'page_size', 10)),
  }
  Object.assign(draftFilters, next)
  Object.assign(queryFilters, next)
}

function goPrevPage() {
  if (queryFilters.page <= 1) return
  queryFilters.page -= 1
  syncRouteFromQuery()
}

function goNextPage() {
  const totalPages = Number(result.value?.total_pages || 1)
  if (queryFilters.page >= totalPages) return
  queryFilters.page += 1
  syncRouteFromQuery()
}

onMounted(() => {
  applyRouteQuery()
  const restored = restoreTaskSnapshot()
  if (!restored) return
  activeJobId.value = String(restored.jobId || '')
  attempts.value = Array.isArray(restored.attempts) ? restored.attempts : []
  actionMessage.value = restored.actionMessage || '已恢复历史任务跟踪。'
  latestTaskNotification.value = (restored.data?.notification || {}) as Record<string, any>
  const terminalStatus = ['done', 'error', 'aborted']
  if (activeJobId.value && !terminalStatus.includes(String(restored.status || '').toLowerCase())) {
    startTaskPolling(activeJobId.value)
  }
})

watch(
  () => route.query,
  () => {
    applyRouteQuery()
  },
)

onBeforeUnmount(() => {
  window.clearTimeout(pollTimer)
})
</script>
