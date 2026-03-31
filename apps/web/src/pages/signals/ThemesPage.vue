<template>
  <AppShell title="主题热点引擎" subtitle="主题热度、方向、预期、股票映射和状态机时间线统一收束。">
    <div class="space-y-4">
      <PageSection title="主题筛选" subtitle="先按热度和状态筛，再下钻到单个主题。">
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-2">
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="主题关键词" />
          <select v-model="filters.theme_group" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部分组</option>
            <option v-for="item in groupOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="filters.direction" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部方向</option>
            <option v-for="item in directionOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="filters.heat_level" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部热度</option>
            <option v-for="item in heatOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="filters.state" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部状态</option>
            <option v-for="item in stateOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyFilters">
            {{ isFetching ? '查询中...' : '刷新' }}
          </button>
        </div>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="主题总数" :value="summary.theme_total ?? 0" hint="主题热点主表" />
        <StatCard title="看多主题" :value="summary.bullish_total ?? 0" hint="方向为看多" />
        <StatCard title="看空主题" :value="summary.bearish_total ?? 0" hint="方向为看空" />
        <StatCard title="高热主题" :value="summary.high_heat_total ?? 0" hint="极高 / 高热度" />
      </div>

      <PageSection title="主题结果" subtitle="优先用结果列表定位重点，再展开单个主题详情。">
        <div class="space-y-3">
          <InfoCard
            v-for="item in result?.items || []"
            :key="item.theme_name"
            :title="item.theme_name || '-'" :meta="`${item.theme_group || '-'} · 热度 ${item.heat_level || '-'} · 状态 ${item.current_state || '-'} · 最新证据 ${item.latest_evidence_time || '-'}`"
            :description="themeDescription(item)"
            @click="openDetail(item)"
          >
            <template #badge>
              <StatusBadge :value="item.direction || 'muted'" :label="item.direction || '-'" />
            </template>
          </InfoCard>
        </div>
      </PageSection>
    </div>

    <DetailDrawer
      :open="!!selectedItem"
      :title="selectedItem?.theme_name || '主题详情'"
      :subtitle="selectedSubtitle"
      eyebrow="统一热点主题引擎"
      @close="selectedItem = null"
    >
      <div v-if="selectedItem" class="space-y-4">
        <PageSection title="主题总览" subtitle="主题强度、来源和状态机一屏看完。">
          <template #action>
            <div class="flex flex-wrap gap-2">
              <button type="button" class="rounded-2xl bg-stone-800 px-4 py-2 text-white" @click="goSignalTimeline">信号时间线</button>
              <button type="button" class="rounded-2xl bg-blue-700 px-4 py-2 text-white" @click="goStateTimeline">状态时间线</button>
              <button type="button" class="rounded-2xl bg-[var(--brand)] px-4 py-2 text-white" @click="downloadImage">下载链路图</button>
            </div>
          </template>
          <div v-if="downloadStatus" class="mb-3 rounded-[16px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-3 py-2 text-sm text-[var(--muted)]">
            {{ downloadStatus }}
          </div>
          <div ref="detailExportRef" class="space-y-4">
            <div class="flex flex-wrap gap-2">
              <StatusBadge :value="selectedItem.direction" :label="selectedItem.direction || '-'" />
              <StatusBadge value="brand" :label="selectedItem.heat_level || '-'" />
              <StatusBadge value="info" :label="selectedItem.current_state || '无状态'" />
            </div>
            <div class="grid gap-3 xl:grid-cols-3 md:grid-cols-2">
              <div class="metric-chip">主题强度 <strong>{{ selectedItem.theme_strength ?? '-' }}</strong></div>
              <div class="metric-chip">置信度 <strong>{{ selectedItem.confidence ?? '-' }}</strong></div>
              <div class="metric-chip">证据数 <strong>{{ selectedItem.evidence_count ?? '-' }}</strong></div>
              <div class="metric-chip">国际新闻 <strong>{{ selectedItem.intl_news_count ?? 0 }}</strong></div>
              <div class="metric-chip">国内新闻 <strong>{{ selectedItem.domestic_news_count ?? 0 }}</strong></div>
              <div class="metric-chip">个股新闻 <strong>{{ selectedItem.stock_news_count ?? 0 }}</strong></div>
              <div class="metric-chip">群聊 <strong>{{ selectedItem.chatroom_count ?? 0 }}</strong></div>
              <div class="metric-chip">股票映射 <strong>{{ selectedItem.stock_link_count ?? 0 }}</strong></div>
            </div>
          </div>
        </PageSection>

        <PageSection title="主题到股票池映射" subtitle="把主题和关联股票池直接落成可点击入口。">
          <div v-if="topStocks.length" class="flex flex-wrap gap-2">
            <button
              v-for="stock in topStocks"
              :key="stock.ts_code || stock.stock_name"
              class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
              @click="goStock(stock)"
            >
              {{ stock.stock_name || stock.ts_code }} · {{ stock.weight ?? '-' }}
            </button>
          </div>
          <div v-else class="text-sm text-[var(--muted)]">当前主题暂无股票池映射。</div>
        </PageSection>

        <PageSection title="市场预期层" subtitle="把相关主题的预期市场问题并到主题页里。">
          <div v-if="marketExpectations.length" class="space-y-2">
            <InfoCard
              v-for="item in marketExpectations"
              :key="item.question"
              :title="item.question || '-'" :meta="`成交量 ${item.volume ?? '-'} · 流动性 ${item.liquidity ?? '-'} · 截止 ${item.end_date || '-'}`"
              :description="item.source_url || ''"
            />
          </div>
          <div v-else class="text-sm text-[var(--muted)]">当前主题暂无预期层数据。</div>
        </PageSection>

        <PageSection title="证据链" subtitle="优先看主题逻辑，再看最近证据。">
          <div v-if="evidenceItems.length" class="space-y-2">
            <InfoCard
              v-for="item in evidenceItems"
              :key="`${item.source}-${item.title}`"
              :title="item.title || item.theme_name || item.source || '-'"
              :meta="`${item.source || '-'} · ${item.pub_date || item.pub_time || item.event_time || '-'}`"
              :description="item.summary || item.reason || ''"
            />
          </div>
          <div v-else class="text-sm text-[var(--muted)]">当前主题暂无证据链数据。</div>
        </PageSection>
      </div>
    </DetailDrawer>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, nextTick, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import DetailDrawer from '../../shared/ui/DetailDrawer.vue'
import { fetchThemeHotspots } from '../../services/api/signals'
import { parseJsonArray } from '../../shared/utils/finance'
import { downloadElementAsImage } from '../../shared/utils/export'

const router = useRouter()

const filters = reactive({ keyword: '', theme_group: '', direction: '', heat_level: '', state: '', page_size: 20 })
const queryFilters = reactive({ keyword: '', theme_group: '', direction: '', heat_level: '', state: '', page: 1, page_size: 20 })
const selectedItem = ref<Record<string, any> | null>(null)
const detailExportRef = ref<HTMLElement | null>(null)
const downloadStatus = ref('')
const { data: result, isFetching } = useQuery({
  queryKey: computed(() => ['theme-hotspots', { ...queryFilters }]),
  queryFn: () => fetchThemeHotspots({ ...queryFilters }),
})

const summary = computed(() => result.value?.summary || {})
const groupOptions = computed(() => result.value?.filters?.theme_groups || [])
const directionOptions = computed(() => result.value?.filters?.directions || [])
const heatOptions = computed(() => result.value?.filters?.heat_levels || [])
const stateOptions = computed(() => result.value?.filters?.states || [])
const selectedSubtitle = computed(() => [selectedItem.value?.theme_group, selectedItem.value?.heat_level, selectedItem.value?.current_state].filter(Boolean).join(' · '))
const topStocks = computed(() => parseJsonArray(selectedItem.value?.top_stocks_json).slice(0, 12))
const evidenceItems = computed(() => parseJsonArray(selectedItem.value?.evidence_json).slice(0, 10))
const marketExpectations = computed(() => selectedItem.value?.market_expectations || [])

function themeDescription(item: Record<string, any>) {
  const topTerms = parseJsonArray(item.top_terms_json).map((term) => String(term.term || term.name || term).trim()).filter(Boolean).slice(0, 6)
  return [`强度 ${item.theme_strength ?? '-'}`, `置信度 ${item.confidence ?? '-'}`, topTerms.length ? `主题词 ${topTerms.join(' / ')}` : ''].filter(Boolean).join(' · ')
}

function openDetail(item: Record<string, any>) {
  selectedItem.value = item
  downloadStatus.value = ''
}

function goSignalTimeline() {
  if (!selectedItem.value) return
  router.push({ path: '/signals/timeline', query: { signal_key: `theme:${selectedItem.value.theme_name}` } })
}

function goStateTimeline() {
  if (!selectedItem.value) return
  router.push({ path: '/signals/state-timeline', query: { signal_scope: 'theme', signal_key: `theme:${selectedItem.value.theme_name}` } })
}

function goStock(stock: Record<string, any>) {
  const tsCode = String(stock.ts_code || '').trim().toUpperCase()
  if (tsCode) {
    router.push({ path: `/stocks/detail/${encodeURIComponent(tsCode)}` })
    return
  }
  router.push({ path: '/stocks/list', query: { keyword: stock.stock_name || '' } })
}

async function downloadImage() {
  if (!detailExportRef.value || !selectedItem.value) {
    downloadStatus.value = '下载失败：当前没有可导出的主题内容。'
    return
  }
  try {
    downloadStatus.value = '正在生成链路图，请稍候...'
    await nextTick()
    await downloadElementAsImage(detailExportRef.value, `${selectedItem.value.theme_name || 'theme'}_主题链路图.png`)
    downloadStatus.value = '链路图下载成功。'
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || '未知错误')
    downloadStatus.value = `下载失败：${message}`
  }
}

function applyFilters() {
  queryFilters.keyword = (filters.keyword || '').trim()
  queryFilters.theme_group = filters.theme_group
  queryFilters.direction = filters.direction
  queryFilters.heat_level = filters.heat_level
  queryFilters.state = filters.state
  queryFilters.page_size = Number(filters.page_size) || 20
  queryFilters.page = 1
}
</script>
