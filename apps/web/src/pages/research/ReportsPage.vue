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
        </div>
        <div v-if="protocolMetaText" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          {{ protocolMetaText }}
        </div>
      </PageSection>

      <PageSection :title="`报告结果 (${result?.total || 0})`" subtitle="列表看摘要，抽屉里看正文、预期层和相关内链。">
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
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="queryFilters.page -= 1">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page >= (result?.total_pages || 1)" @click="queryFilters.page += 1">下一页</button>
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
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import DetailDrawer from '../../shared/ui/DetailDrawer.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import { fetchResearchReports } from '../../services/api/research'
import { downloadElementAsImage, downloadTextFile } from '../../shared/utils/export'
import { parseJsonArray, parseJsonObject } from '../../shared/utils/finance'

const router = useRouter()

const queryFilters = reactive({ keyword: '', report_type: '', report_date: '', page: 1, page_size: 20 })
const draftFilters = reactive({ ...queryFilters })
const selectedItem = ref<Record<string, any> | null>(null)
const detailExportRef = ref<HTMLElement | null>(null)
const downloadStatus = ref('')

const { data: result, isFetching } = useQuery({ queryKey: ['research-reports', queryFilters], queryFn: () => fetchResearchReports(queryFilters) })

watch(
  () => result.value?.items,
  (items) => {
    if (!items?.length || selectedItem.value) return
    selectedItem.value = items[0]
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
}
</script>
