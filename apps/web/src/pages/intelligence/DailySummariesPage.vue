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
          <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(238,244,247,0.78)_100%)] px-4 py-3 text-sm text-[var(--muted)] shadow-[var(--shadow-soft)]">
            {{ actionMessage }}
          </div>
        </div>
        <div v-if="attemptChain" class="mt-2 text-sm text-[var(--muted)]">尝试链路：{{ attemptChain }}</div>
        <div v-if="protocolMetaText" class="mt-2 text-sm text-[var(--muted)]">{{ protocolMetaText }}</div>
      </PageSection>

      <PageSection title="任务闭环状态" subtitle="展示日报任务的通知发送状态，便于联调 webhook。">
        <InfoCard title="通知发送" :description="notificationDescription" :meta="notificationMeta" />
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <PageSection :title="`日报结果 (${result?.total || 0})`" subtitle="左侧列表切换，右侧查看正文。">
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
              <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="queryFilters.page -= 1">上一页</button>
              <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page >= (result?.total_pages || 1)" @click="queryFilters.page += 1">下一页</button>
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
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import { fetchDailySummaries, fetchDailySummaryTask, triggerDailySummaryGenerate } from '../../services/api/news'
import { fetchAuthStatus } from '../../services/api/auth'
import { formatDateTime } from '../../shared/utils/format'
import { downloadElementAsImage, downloadTextFile } from '../../shared/utils/export'

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
const detailExportRef = ref<HTMLElement | null>(null)
const latestTaskNotification = ref<Record<string, any>>({})
let pollTimer = 0

const { data: result, refetch, isFetching } = useQuery({
  queryKey: ['daily-summaries', queryFilters],
  queryFn: () => fetchDailySummaries(queryFilters),
})

watch(
  () => result.value?.items,
  (items) => {
    if (!items?.length) return
    if (!selectedItem.value) {
      selectedItem.value = items[0]
      return
    }
    const matched = items.find((item: Record<string, any>) => item.id === selectedItem.value?.id)
    if (matched) selectedItem.value = matched
  },
  { immediate: true },
)

const selectedContent = computed(() => {
  const primary = String(selectedItem.value?.analysis_markdown || '').trim()
  if (primary) return primary
  return selectedItem.value?.summary_markdown || selectedItem.value?.summary_text || '请选择左侧一条日报总结查看内容。'
})
const attemptChain = computed(() => attempts.value.map((item) => `${item.model || '-'}${item.error ? '×' : '√'}`).join(' -> '))
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

const generateMutation = useMutation({
  mutationFn: () => triggerDailySummaryGenerate({}),
  onSuccess: (payload: Record<string, any>) => {
    const jobId = String(payload.job_id || '')
    attempts.value = Array.isArray(payload.attempts) ? payload.attempts : []
    actionMessage.value = `日报总结任务已创建：${payload.summary_date || ''} · 自动路由中（默认 GPT-5.4）`
    if (!jobId) return
    window.clearTimeout(pollTimer)
    const poll = async () => {
      const task = await fetchDailySummaryTask({ job_id: jobId })
      attempts.value = Array.isArray(task.attempts) ? task.attempts : attempts.value
      if (task.status === 'done') {
        actionMessage.value = `日报总结生成完成${task.used_model ? ` · 实际模型 ${task.used_model}` : ''}`
        latestTaskNotification.value = (task.notification || {}) as Record<string, any>
        await refetch()
        if (task.item) selectedItem.value = task.item
        return
      }
      if (task.status === 'error') {
        actionMessage.value = `日报总结生成失败：${task.error || task.message || '未知错误'}`
        latestTaskNotification.value = (task.notification || {}) as Record<string, any>
        return
      }
      actionMessage.value = `日报总结生成中：${task.progress || 0}% · ${task.message || task.stage || '运行中'}`
      pollTimer = window.setTimeout(poll, 3000)
    }
    poll()
  },
  onError: (error: Error) => {
    actionMessage.value = `生成失败：${error.message}`
    attempts.value = []
    latestTaskNotification.value = {}
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
  downloadTextFile(selectedContent.value, `${date}_新闻日报总结_${model}.md`, 'text/markdown;charset=utf-8')
}

async function downloadImage() {
  if (!selectedItem.value || !detailExportRef.value) return
  await nextTick()
  await downloadElementAsImage(detailExportRef.value, `${selectedItem.value.summary_date || 'daily_summary'}_新闻日报总结_${selectedItem.value.model || 'model'}.png`)
}

function generateTodaySummary() {
  actionMessage.value = '正在创建日报总结任务...'
  generateMutation.mutate()
}

function applyFilters() {
  Object.assign(queryFilters, { ...draftFilters, page: 1 })
}
</script>
