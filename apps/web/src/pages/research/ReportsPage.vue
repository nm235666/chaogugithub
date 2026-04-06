<template>
  <AppShell title="标准投研报告" subtitle="统一展示股票、主题、市场报告，支持详情抽屉、内链跳转和双格式下载。">
    <div class="space-y-4">
      <PageSection title="报告筛选" subtitle="按类型、日期和关键词收敛报告，再在右侧展开详情。">
        <div class="grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <input v-model="draftFilters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="关键词 / 主体 / Markdown 内容" />
          <select v-model="draftFilters.report_type" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部类型</option>
            <option v-for="item in reportTypeOptions" :key="item" :value="item">{{ reportTypeLabel(item) }}</option>
          </select>
          <select v-model="draftFilters.report_date" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部日期</option>
            <option v-for="item in reportDateOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model.number="draftFilters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isFetching" @click="applyFilters">
            {{ isFetching ? '查询中...' : '查询' }}
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="semanticSearching" @click="runSemanticSearch">
            {{ semanticSearching ? '语义检索中...' : '语义检索' }}
          </button>
        </div>
        <div v-if="protocolMetaText" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          {{ protocolMetaText }}
        </div>
        <div v-if="semanticError" class="mt-3 rounded-[16px] border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ semanticError }}
        </div>
        <div v-if="semanticHits.length" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3">
          <div class="mb-2 text-sm font-semibold text-[var(--ink)]">语义检索命中（{{ semanticHits.length }}）</div>
          <div class="space-y-2">
            <button
              v-for="(hit, index) in semanticHits"
              :key="`${hit.source_type}-${hit.source_id}-${index}`"
              class="w-full rounded-2xl border border-[var(--line)] bg-white px-3 py-3 text-left transition hover:border-[var(--brand)]"
              @click="openSemanticHit(hit)"
            >
              <div class="text-sm font-semibold text-[var(--ink)]">{{ hit.title || '-' }}</div>
              <div class="mt-1 text-xs text-[var(--muted)]">
                {{ hit.source_type || '-' }} · 相似度 {{ Number(hit.rerank_score || 0).toFixed(3) }} · {{ hit.published_at || '-' }}
              </div>
              <div class="mt-2 text-sm text-[var(--muted)]">{{ hit.snippet || '' }}</div>
            </button>
          </div>
          <div v-if="semanticTraceText" class="mt-2 text-xs text-[var(--muted)]">{{ semanticTraceText }}</div>
        </div>
        <StatePanel
          v-if="queryError"
          class="mt-3"
          tone="danger"
          title="报告列表加载失败"
          :description="queryError"
        >
          <template #action>
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="reloadList">重新加载</button>
          </template>
        </StatePanel>
      </PageSection>

      <PageSection :title="`报告结果 (${result?.total || 0})`" subtitle="列表看摘要，抽屉里看正文、预期层和相关内链。">
        <StatePanel
          v-if="!queryError && !selectedItem && (result?.items || []).length"
          class="mb-4"
          title="点击报告卡片查看详情"
          description="列表默认不再自动展开右侧抽屉，避免遮挡筛选区；只有主动点击报告卡片后才会打开详情。"
        />
        <StatePanel
          v-if="!queryError && !isFetching && !(result?.items || []).length"
          class="mb-4"
          tone="warning"
          title="当前没有命中的报告"
          description="可以先清空关键词或日期筛选，回到全量报告列表，再从右侧详情继续下钻。"
        >
          <template #action>
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 font-semibold text-white" @click="resetFilters">恢复默认筛选</button>
          </template>
        </StatePanel>
        <div class="space-y-2">
          <InfoCard
            v-for="item in result?.items || []"
            :key="item.id"
            :title="item.subject_name || item.subject_key || '-'" :meta="`${reportTypeLabel(item.report_type)} · ${item.report_date || '-'} · ${item.model || '-'}`"
            :description="reportPreview(item.analysis_markdown || item.markdown_content)"
            @click="openDetail(item)"
          >
            <template #badge>
              <StatusBadge value="info" label="查看详情" />
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
    </div>

    <DetailDrawer
      :open="!!selectedItem"
      :title="selectedTitle"
      :subtitle="selectedSubtitle"
      eyebrow="标准投研报告"
      @close="selectedItem = null"
    >
      <div v-if="selectedItem" class="space-y-4">
        <div class="flex flex-wrap gap-2">
          <StatusBadge value="brand" :label="reportTypeLabel(selectedItem.report_type)" />
          <StatusBadge value="info" :label="selectedItem.report_date || '-'" />
          <StatusBadge value="muted" :label="selectedItem.model || '-'" />
        </div>

        <PageSection title="导出与跳转" subtitle="把报告沉淀出去，或继续下钻到股票、主题与信号时间线。">
          <template #action>
            <div class="flex flex-wrap gap-2">
              <button type="button" class="rounded-2xl bg-stone-800 px-4 py-2 text-white" @click="downloadMarkdown">下载 Markdown</button>
              <button type="button" class="rounded-2xl bg-blue-700 px-4 py-2 text-white" @click="downloadImage">下载图片</button>
            </div>
          </template>
          <div v-if="downloadStatus" class="mb-3 rounded-[16px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-2 text-sm text-[var(--muted)]">
            {{ downloadStatus }}
          </div>
          <div class="flex flex-wrap gap-2">
            <button
              v-for="link in relatedLinks"
              :key="`${link.type}-${link.value}`"
              type="button"
              class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
              @click="goLink(link)"
            >
              {{ link.label }}
            </button>
          </div>
        </PageSection>

        <PageSection title="市场预期层" subtitle="把报告关联到的主题预期合约也收在一起看。">
          <div v-if="marketExpectations.length" class="space-y-2">
            <InfoCard
              v-for="item in marketExpectations"
              :key="item.question"
              :title="item.question || '-'"
              :meta="`成交量 ${item.volume ?? '-'} · 流动性 ${item.liquidity ?? '-'} · 截止 ${item.end_date || '-'}`"
              :description="item.source_url || ''"
            />
          </div>
          <div v-else class="rounded-[20px] border border-dashed border-[var(--line)] bg-[rgba(255,255,255,0.52)] px-4 py-8 text-center text-sm text-[var(--muted)]">
            当前报告未关联到市场预期数据。
          </div>
        </PageSection>

        <div ref="detailExportRef">
          <PageSection title="报告正文" subtitle="保留 Markdown 原文，便于继续细读。">
            <MarkdownBlock :content="selectedMarkdown" />
          </PageSection>
        </div>
      </div>
    </DetailDrawer>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import DetailDrawer from '../../shared/ui/DetailDrawer.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import { fetchResearchReports, searchAiRetrieval } from '../../services/api/research'
import { downloadElementAsImage, downloadTextFile } from '../../shared/utils/export'
import { parseJsonArray, parseJsonObject } from '../../shared/utils/finance'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

const route = useRoute()
const router = useRouter()

const queryFilters = reactive({ keyword: '', report_type: '', report_date: '', page: 1, page_size: 20 })
const draftFilters = reactive({ ...queryFilters })
const selectedItem = ref<Record<string, any> | null>(null)
const detailExportRef = ref<HTMLElement | null>(null)
const downloadStatus = ref('')
const semanticSearching = ref(false)
const semanticHits = ref<Array<Record<string, any>>>([])
const semanticError = ref('')
const semanticTrace = ref<Record<string, any>>({})

const { data: result, isFetching, error, refetch } = useQuery({
  queryKey: ['research-reports', queryFilters],
  queryFn: () => fetchResearchReports(queryFilters),
  placeholderData: keepPreviousData,
})

watch(
  () => result.value?.items,
  (items) => {
    if (!items?.length) {
      selectedItem.value = null
      return
    }
    if (!selectedItem.value) return
    const matched = items.find((item: Record<string, any>) => item.id === selectedItem.value?.id)
    selectedItem.value = matched || null
  },
  { immediate: true },
)

const reportTypeOptions = computed(() => result.value?.filters?.report_types || [])
const reportDateOptions = computed(() => result.value?.filters?.report_dates || [])
const selectedTitle = computed(() => selectedItem.value?.subject_name || selectedItem.value?.subject_key || '报告详情')
const selectedSubtitle = computed(() => [reportTypeLabel(selectedItem.value?.report_type), selectedItem.value?.report_date, selectedItem.value?.model].filter(Boolean).join(' · '))
const selectedMarkdown = computed(() => {
  const primary = String(selectedItem.value?.analysis_markdown || '').trim()
  if (primary) return primary
  const compat = String(selectedItem.value?.markdown_content || '').trim()
  return compat || '暂无报告正文。'
})
const marketExpectations = computed(() => selectedItem.value?.market_expectations || [])
const relatedLinks = computed(() => buildRelatedLinks(selectedItem.value))
const protocolMetaText = computed(() => {
  const protocol = result.value?.protocol || {}
  const version = String(protocol.version || '').trim()
  const primary = String(protocol.primary_markdown_field || '').trim()
  const compat = String(protocol.compat_markdown_field || '').trim()
  const retireAfter = String(protocol.compat_retire_after || '').trim()
  if (!version && !primary && !compat && !retireAfter) return ''
  return `协议版本 ${version || '-'} · 主字段 ${primary || '-'} · 兼容字段 ${compat || '-'} · 兼容退场时间 ${retireAfter || '-'}`
})
const queryError = computed(() => error.value?.message || '')
const semanticTraceText = computed(() => {
  const trace = semanticTrace.value || {}
  const keywordCandidates = Number(trace.keyword_candidates || 0)
  const vectorCandidates = Number(trace.vector_candidates || 0)
  const mergedCandidates = Number(trace.merged_candidates || 0)
  if (!keywordCandidates && !vectorCandidates && !mergedCandidates) return ''
  return `关键词召回 ${keywordCandidates} · 向量召回 ${vectorCandidates} · 重排后 ${mergedCandidates}`
})

function reportTypeLabel(value: unknown) {
  const raw = String(value || '').trim()
  return ({ stock: '股票报告', theme: '主题报告', market: '市场报告' } as Record<string, string>)[raw] || raw || '-'
}

function reportPreview(markdown: unknown) {
  return String(markdown || '').replace(/[#>*`_-]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 180) || '暂无摘要'
}

function openDetail(item: Record<string, any>) {
  selectedItem.value = item
  downloadStatus.value = ''
}

function buildRelatedLinks(item: Record<string, any> | null) {
  if (!item) return []
  const links: Array<{ type: string; value: string; label: string }> = []
  const seen = new Set<string>()
  const context = parseJsonObject(item.context_json)
  const pushLink = (type: string, value: unknown, label: string) => {
    const text = String(value || '').trim()
    const key = `${type}:${text}`
    if (!text || seen.has(key)) return
    seen.add(key)
    links.push({ type, value: text, label })
  }
  if (String(item.report_type || '') === 'stock') {
    pushLink('stock', item.subject_key, `股票详情 · ${item.subject_name || item.subject_key}`)
    pushLink('signal', `stock:${item.subject_key}`, '查看股票信号时间线')
  }
  if (String(item.report_type || '') === 'theme') {
    pushLink('theme', item.subject_key, `主题热点 · ${item.subject_name || item.subject_key}`)
    pushLink('theme-state', `theme:${item.subject_key}`, '查看主题状态时间线')
  }
  ;(parseJsonArray(context.theme_rows) || []).slice(0, 4).forEach((row) => {
    const themeName = Array.isArray(row) ? row[0] : row?.theme_name
    pushLink('theme', themeName, `主题 · ${themeName}`)
  })
  ;(parseJsonArray(context.themes) || []).slice(0, 4).forEach((row) => {
    const themeName = Array.isArray(row) ? row[0] : row?.theme_name || row
    pushLink('theme', themeName, `主题 · ${themeName}`)
  })
  return links
}

function goLink(link: { type: string; value: string }) {
  if (link.type === 'stock') {
    router.push({ path: `/stocks/detail/${encodeURIComponent(link.value)}` })
    return
  }
  if (link.type === 'signal') {
    router.push({ path: '/signals/timeline', query: { signal_key: link.value } })
    return
  }
  if (link.type === 'theme') {
    router.push({ path: '/signals/themes', query: { keyword: link.value } })
    return
  }
  if (link.type === 'theme-state') {
    router.push({ path: '/signals/state-timeline', query: { signal_scope: 'theme', signal_key: link.value } })
  }
}

function downloadMarkdown() {
  if (!selectedItem.value) return
  downloadTextFile(selectedMarkdown.value, `${selectedTitle.value}_${selectedItem.value.report_date || 'report'}.md`, 'text/markdown;charset=utf-8')
  downloadStatus.value = 'Markdown 下载已触发。'
}

async function downloadImage() {
  if (!selectedItem.value) {
    downloadStatus.value = '下载失败：当前没有可导出的报告。'
    return
  }
  if (!detailExportRef.value) {
    downloadStatus.value = '下载失败：报告内容还未渲染完成，请稍后重试。'
    return
  }
  downloadStatus.value = '正在生成图片，请稍候...'
  try {
    await nextTick()
    await downloadElementAsImage(
      detailExportRef.value,
      `${selectedTitle.value}_${selectedItem.value.report_date || 'report'}.png`,
      1600,
      { stripImages: true },
    )
    downloadStatus.value = '图片下载成功。'
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || '未知错误')
    downloadStatus.value = `下载失败：${message}`
  }
}

function applyFilters() {
  Object.assign(queryFilters, { ...draftFilters, page: 1 })
  syncRouteFromQuery()
}

function resetFilters() {
  Object.assign(draftFilters, { keyword: '', report_type: '', report_date: '', page: 1, page_size: 20 })
  applyFilters()
}

function reloadList() {
  refetch()
}

async function runSemanticSearch() {
  const keyword = String(draftFilters.keyword || '').trim()
  if (!keyword) {
    semanticError.value = '请先输入关键词，再执行语义检索。'
    semanticHits.value = []
    return
  }
  semanticSearching.value = true
  semanticError.value = ''
  try {
    const res = await searchAiRetrieval({
      query: keyword,
      scene: 'report',
      top_k: 8,
    })
    semanticHits.value = Array.isArray(res?.hits) ? res.hits : []
    semanticTrace.value = (res?.trace || {}) as Record<string, any>
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err || '未知错误')
    semanticError.value = `语义检索失败：${message}`
    semanticHits.value = []
    semanticTrace.value = {}
  } finally {
    semanticSearching.value = false
  }
}

function openSemanticHit(hit: Record<string, any>) {
  const sourceType = String(hit.source_type || '').trim().toLowerCase()
  const sourceId = String(hit.source_id || '').trim()
  if (sourceType === 'report' && sourceId) {
    const matched = (result.value?.items || []).find((item: Record<string, any>) => String(item.id || '') === sourceId)
    if (matched) {
      openDetail(matched)
      return
    }
  }
  draftFilters.keyword = String(hit.title || draftFilters.keyword || '')
  applyFilters()
}

function syncRouteFromQuery() {
  router.replace({
    query: buildCleanQuery({
      keyword: queryFilters.keyword,
      report_type: queryFilters.report_type,
      report_date: queryFilters.report_date,
      page: queryFilters.page,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteQuery() {
  const q = route.query as Record<string, unknown>
  const next = {
    keyword: readQueryString(q, 'keyword', ''),
    report_type: readQueryString(q, 'report_type', ''),
    report_date: readQueryString(q, 'report_date', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(20, readQueryNumber(q, 'page_size', 20)),
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
})

watch(
  () => route.query,
  () => {
    applyRouteQuery()
  },
)
</script>
