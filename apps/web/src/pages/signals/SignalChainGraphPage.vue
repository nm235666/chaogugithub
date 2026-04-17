<template>
  <AppShell title="产业链图谱" subtitle="主题 + 行业双中心关系浏览，点击节点即可下钻到详情页或切换中心。">
    <div class="space-y-4">
      <PageSection title="图谱筛选" subtitle="先选择中心类型，再用关键词、层级和节点数量控制图谱粒度。">
        <div class="grid gap-3 xl:grid-cols-[1.2fr_0.8fr]">
          <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <label class="text-sm font-semibold text-[var(--ink)]">
              中心类型
              <select v-model="filters.center_type" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                <option value="theme">主题中心</option>
                <option value="industry">行业中心</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              关键词 / 中心
              <input v-model="filters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 AI、黄金、银行、半导体" />
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              深度
              <select v-model="filters.depth" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                <option :value="1">1 层</option>
                <option :value="2">2 层</option>
                <option :value="3">3 层</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              节点上限
              <select v-model="filters.limit" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
                <option :value="8">8</option>
                <option :value="12">12</option>
                <option :value="16">16</option>
                <option :value="24">24</option>
              </select>
            </label>
          </div>

          <div class="rounded-[24px] border border-[var(--line)] bg-white/80 p-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div class="text-sm font-bold text-[var(--ink)]">关系筛选</div>
                <div class="mt-1 text-xs text-[var(--muted)]">只过滤边，不破坏节点浏览和下钻。</div>
              </div>
              <div class="flex flex-wrap gap-2">
                <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold text-[var(--ink)]" @click="resetFilters">重置</button>
                <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 text-sm font-semibold text-white" @click="applyFilters">
                  {{ isFetching ? '加载中...' : '刷新图谱' }}
                </button>
              </div>
            </div>
            <div class="mt-3 flex flex-wrap items-center gap-2 text-xs">
              <button
                type="button"
                class="rounded-full border px-3 py-2 font-semibold transition disabled:cursor-not-allowed disabled:opacity-45"
                :class="showSecondary ? 'border-[var(--line)] bg-white text-[var(--ink)]' : 'border-[var(--brand)] bg-[var(--brand)] text-white'"
                :disabled="!canShowTrunkOnly"
                @click="toggleSecondary(false)"
              >
                只看主干
              </button>
              <button
                type="button"
                class="rounded-full border px-3 py-2 font-semibold transition"
                :class="showSecondary ? 'border-[var(--brand)] bg-[var(--brand)] text-white' : 'border-[var(--line)] bg-white text-[var(--ink)]'"
                @click="toggleSecondary(true)"
              >
                展开二级节点
              </button>
              <span v-if="!canShowTrunkOnly" class="text-[11px] text-[var(--muted)]">当前中心无可折叠二级节点</span>
            </div>
            <div v-if="relationOptions.length" class="mt-3 flex flex-wrap gap-2">
              <button
                class="rounded-full border px-3 py-2 text-xs font-semibold transition"
                :class="relationFilter === '' ? 'border-[var(--brand)] bg-[var(--brand)] text-white' : 'border-[var(--line)] bg-white text-[var(--ink)]'"
                @click="relationFilter = ''"
              >
                全部关系
              </button>
              <button
                v-for="item in relationOptions"
                :key="item.key"
                class="rounded-full border px-3 py-2 text-xs font-semibold transition"
                :class="relationFilter === item.key ? 'border-[var(--brand)] bg-[var(--brand)] text-white' : 'border-[var(--line)] bg-white text-[var(--ink)]'"
                @click="relationFilter = item.key"
              >
                {{ item.label }} · {{ item.count }}
              </button>
            </div>
            <div v-else class="mt-3 text-sm text-[var(--muted)]">当前图谱暂无可筛选关系。</div>
            <div class="mt-3 rounded-[20px] border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-xs leading-6 text-[var(--muted)]">
              <div class="font-semibold text-[var(--ink)]">统计口径说明</div>
              <div class="mt-1">统计卡片基于当前画布渲染结果，处理顺序为先按关系筛选，再根据视图模式决定是否折叠二级节点。</div>
              <div class="mt-2 flex flex-wrap gap-2 text-[11px]">
                <span class="metric-chip">当前关系：{{ readableRelationFilter }}</span>
                <span class="metric-chip">当前视图：{{ readableViewMode }}</span>
              </div>
            </div>
          </div>
        </div>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="节点数" :value="displaySummary.node_count" hint="当前画布展示节点数" />
        <StatCard title="边数" :value="displaySummary.edge_count" hint="当前画布关系连线" />
        <StatCard title="主题数" :value="displaySummary.theme_count" hint="当前画布主题节点数" />
        <StatCard title="行业数" :value="displaySummary.industry_count" hint="当前画布行业节点数" />
      </div>

      <div class="grid gap-4" :class="graphSplitClass">
        <PageSection title="关系图" subtitle="主题、行业、股票三层关系图，节点可点击，中心可切换。">
          <template #action>
            <div class="flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">中心：{{ centerLabel || '-' }}</span>
              <span class="metric-chip">最新评分：{{ summary.latest_score_date || '-' }}</span>
              <span class="metric-chip">中心类型：{{ centerTypeLabel(filters.center_type) }}</span>
              <span class="metric-chip">关系筛选：{{ readableRelationFilter }}</span>
              <span class="metric-chip">视图模式：{{ readableViewMode }}</span>
              <span class="metric-chip" v-if="hiddenNodeCount > 0">已折叠 {{ hiddenNodeCount }} 个节点</span>
            </div>
          </template>

          <StatePanel
            v-if="graphEmpty"
            class="mb-4"
            tone="warning"
            title="当前图谱暂无可展示数据"
            :description="graphSummaryMessage"
          >
            <template #action>
              <div class="flex flex-wrap gap-2">
                <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 font-semibold text-white" @click="applyFilters">重新加载</button>
                <button
                  v-for="item in centerSuggestions"
                  :key="`${item.type}-${item.label}-empty`"
                  class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]"
                  @click="quickSwitchCenter(item.type, item.label)"
                >
                  {{ item.label }}
                </button>
              </div>
            </template>
          </StatePanel>

          <div v-if="graphWeakData && !graphEmpty" class="mb-4 rounded-[24px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(239,245,248,0.9)_100%)] p-4">
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <div class="text-sm font-bold text-[var(--ink)]">当前中心数据较弱</div>
                <div class="mt-1 max-w-[60ch] text-sm leading-6 text-[var(--muted)]">
                  图谱已保留当前中心，但节点和关系不足以支撑完整浏览。建议切换到一个更强的主题或行业中心继续查看主干关系。
                </div>
              </div>
              <div class="flex flex-wrap gap-2">
                <button
                  v-for="item in centerSuggestions"
                  :key="`${item.type}-${item.label}-weak`"
                  class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]"
                  @click="quickSwitchCenter(item.type, item.label)"
                >
                  {{ item.label }}
                </button>
              </div>
            </div>
          </div>

          <div v-if="!graphEmpty" class="overflow-hidden rounded-[28px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.95)_0%,rgba(244,248,250,0.9)_100%)]">
            <div class="graph-toolbar border-b border-[var(--line)] px-4 py-3 text-xs text-[var(--muted)]">
              <div>
                选中节点：<span class="font-semibold text-[var(--ink)]">{{ selectedNode?.label || centerLabel || '-' }}</span>
                <span class="mx-2">·</span>
                单击查看详情，双击主题/行业切中心。
              </div>
              <div class="flex items-center gap-2">
                <span class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-[11px] font-semibold text-[var(--muted)]">
                  {{ graphViewMode === 'full' ? '显示二级节点' : '仅显示主干' }}
                </span>
                <button class="graph-tool-btn" @click="zoomOut">缩小</button>
                <button class="graph-tool-btn" @click="zoomIn">放大</button>
                <button class="graph-tool-btn" @click="recenterGraph">回到中心</button>
              </div>
            </div>
            <div class="graph-stage" :style="{ height: `${graphHeight}px` }">
              <RelationGraph
                ref="graphRef"
                class="relation-graph-core"
                :options="graphOptions"
                :on-node-click="onGraphNodeClick"
                :on-canvas-click="onGraphCanvasClick"
              >
                <template #node="{ node }">
                  <button
                    type="button"
                    class="graph-node"
                    :class="graphNodeClass(node)"
                    @click.stop="selectNode(String(node.id || ''))"
                    @dblclick.stop="handleNodeDoubleClick(node)"
                  >
                    <div class="graph-node-title">{{ node.text }}</div>
                    <div class="graph-node-meta">{{ graphNodeMeta(node) }}</div>
                  </button>
                </template>
              </RelationGraph>
            </div>
          </div>
        </PageSection>

        <PageSection title="节点详情" subtitle="右侧固定展示当前选中节点的解释、关联节点和跳转入口。">
          <div v-if="selectedNode" class="space-y-4">
            <div class="rounded-[24px] border border-[var(--line)] bg-white/82 p-4">
              <div class="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div class="text-xs uppercase tracking-[0.16em] text-[var(--muted)]">{{ nodeTypeLabel(selectedNode.type) }}</div>
                  <div class="mt-1 text-2xl font-extrabold text-[var(--ink)]">{{ selectedNode.label }}</div>
                  <div class="mt-2 text-sm text-[var(--muted)]">{{ selectedNode.subtitle || selectedNode.summary || '-' }}</div>
                </div>
                <StatusBadge :value="selectedNode.status || 'info'" :label="selectedNode.status || '-'" />
              </div>

              <div class="mt-4 grid gap-2 md:grid-cols-2">
                <div class="metric-chip">权重 <strong>{{ formatValue(selectedNode.weight) }}</strong></div>
                <div class="metric-chip">分数 <strong>{{ formatValue(selectedNode.score) }}</strong></div>
                <div class="metric-chip">强度 <strong>{{ formatValue(selectedNode.strength) }}</strong></div>
                <div class="metric-chip">层级 <strong>L{{ selectedNode.layer ?? '-' }}</strong></div>
              </div>
            </div>

            <div v-if="detailWhy" class="rounded-[24px] border border-[var(--line)] bg-[var(--panel-soft)] p-4">
              <div class="text-sm font-bold text-[var(--ink)]">为什么是这个节点</div>
              <div class="mt-2 text-sm leading-6 text-[var(--muted)]">{{ detailWhy }}</div>
            </div>

            <div v-if="selectedNode.highlights?.length" class="rounded-[24px] border border-[var(--line)] bg-white/82 p-4">
              <div class="text-sm font-bold text-[var(--ink)]">关键要点</div>
              <div class="mt-3 flex flex-wrap gap-2">
                <span v-for="item in selectedNode.highlights" :key="item" class="metric-chip">{{ item }}</span>
              </div>
            </div>

            <div v-if="detailMetrics.length" class="rounded-[24px] border border-[var(--line)] bg-white/82 p-4">
              <div class="text-sm font-bold text-[var(--ink)]">关键指标</div>
              <div class="mt-3 grid gap-2 md:grid-cols-2">
                <div v-for="item in detailMetrics" :key="item.label" class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm">
                  <div class="text-xs text-[var(--muted)]">{{ item.label }}</div>
                  <div class="mt-1 font-semibold text-[var(--ink)]">{{ item.value }}</div>
                </div>
              </div>
            </div>

            <div v-if="relatedNodes.length" class="rounded-[24px] border border-[var(--line)] bg-white/82 p-4">
              <div class="text-sm font-bold text-[var(--ink)]">它和谁有关</div>
              <div class="mt-3 flex flex-wrap gap-2">
                <button
                  v-for="item in relatedNodes"
                  :key="item.id"
                  type="button"
                  class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                  @click="selectNode(item.id)"
                >
                  {{ item.label }} · {{ nodeTypeLabel(item.type) }}
                </button>
              </div>
            </div>

            <div v-if="selectedEdgeRows.length" class="rounded-[24px] border border-[var(--line)] bg-white/82 p-4">
              <div class="text-sm font-bold text-[var(--ink)]">关联关系</div>
              <div class="mt-3 space-y-2">
                <InfoCard
                  v-for="edge in selectedEdgeRows"
                  :key="edge.id"
                  :title="edge.relation_label || edge.relation_key || '-'"
                  :meta="edgeSummary(edge)"
                  :description="edge.summary || ''"
                />
              </div>
            </div>

            <div class="rounded-[24px] border border-[var(--line)] bg-white/82 p-4">
              <div class="text-sm font-bold text-[var(--ink)]">跳转入口</div>
              <div class="mt-3 flex flex-wrap gap-2">
                <button
                  v-for="item in selectedActions"
                  :key="`${item.label}-${item.to}`"
                  type="button"
                  class="rounded-2xl bg-[var(--brand)] px-4 py-2 text-sm font-semibold text-white"
                  @click="goTo(item.to)"
                >
                  {{ item.label }}
                </button>
                <button
                  v-if="canSwitchCenter"
                  type="button"
                  class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]"
                  @click="switchCenterToSelected"
                >
                  以此为中心
                </button>
                <button
                  type="button"
                  class="rounded-2xl border border-emerald-300 bg-emerald-50 px-4 py-2 text-sm font-semibold text-emerald-800 transition hover:border-emerald-500 hover:bg-emerald-100"
                  @click="goToDecision(selectedNode)"
                >
                  → 决策板
                </button>
              </div>
            </div>

            <div v-if="selectedNode.id === centerId && centerDetailSummary" class="rounded-[24px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] p-4">
              <div class="text-sm font-bold text-[var(--ink)]">中心解释</div>
              <div class="mt-2 text-sm leading-6 text-[var(--muted)]">{{ centerDetailSummary }}</div>
              <div v-if="centerDetailNextSteps.length" class="mt-3 flex flex-wrap gap-2">
                <button
                  v-for="item in centerDetailNextSteps"
                  :key="`${item.label}-${item.to}`"
                  type="button"
                  class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]"
                  @click="goTo(item.to)"
                >
                  {{ item.label }}
                </button>
              </div>
            </div>
          </div>
          <div v-else class="rounded-[24px] border border-[var(--line)] bg-white/82 p-5 text-sm text-[var(--muted)]">
            请先在左侧选择一个节点。
          </div>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import RelationGraph from 'relation-graph/vue3'
import { useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import { fetchSignalChainGraph } from '../../services/api/signals'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'
import { buildGraphRenderPayload } from './graph-adapter'

type GraphNode = Record<string, any>
type GraphEdge = Record<string, any>
type GraphViewMode = 'trunk' | 'full'

const route = useRoute()
const router = useRouter()
const graphRef = ref<any>(null)
const viewportWidth = ref<number>(typeof window !== 'undefined' ? window.innerWidth : 1280)
const zoomLevel = ref(100)

const MIN_ZOOM = 65
const MAX_ZOOM = 155
const ZOOM_STEP = 15

let removeResizeListener: (() => void) | null = null
let renderTimer: number | null = null
let pendingFitToView = true
let isApplyingRouteQuery = false

const filters = reactive({
  center_type: 'theme',
  keyword: '',
  depth: 2,
  limit: 12,
})
const queryFilters = reactive({
  center_type: 'theme',
  center_key: '',
  depth: 2,
  limit: 12,
})
const relationFilter = ref('')
const selectedNodeId = ref('')
const showSecondary = ref(true)
const graphViewMode = computed<GraphViewMode>(() => (showSecondary.value ? 'full' : 'trunk'))

const { data: result, isFetching } = useQuery({
  queryKey: computed(() => ['signal-chain-graph', { ...queryFilters }]),
  queryFn: () => fetchSignalChainGraph({ ...queryFilters }),
  placeholderData: keepPreviousData,
})

const graphNodes = computed(() => (Array.isArray(result.value?.nodes) ? result.value.nodes : []) as GraphNode[])
const graphEdges = computed(() => (Array.isArray(result.value?.edges) ? result.value.edges : []) as GraphEdge[])
const summary = computed(() => result.value?.summary || {})
const centerNode = computed(() => result.value?.center || null)
const centerId = computed(() => String(centerNode.value?.id || graphNodes.value[0]?.id || ''))
const centerLabel = computed(() => String(result.value?.center_label || centerNode.value?.label || ''))
const graphDetail = computed(() => result.value?.detail || {})
const graphEmpty = computed(() => Boolean(summary.value?.empty) || graphNodes.value.length === 0)
const graphSummaryMessage = computed(() => String(summary.value?.message || '当前没有可展示的图谱数据。'))
const graphWeakData = computed(() => !graphEmpty.value && ((graphNodes.value.length <= 1) || (graphEdges.value.length === 0)))
const canSwitchCenter = computed(() => ['theme', 'industry'].includes(String(selectedNode.value?.type || '')))
const isIndustryCenterMode = computed(() => String(queryFilters.center_type || filters.center_type || 'theme') === 'industry')
const graphSplitClass = computed(() =>
  isIndustryCenterMode.value ? 'xl:grid-cols-[minmax(0,1.56fr)_minmax(320px,0.56fr)]' : 'xl:grid-cols-[minmax(0,1.35fr)_minmax(360px,0.72fr)]',
)

const graphOptions = computed<any>(() => ({
  layout: {
    layoutName: 'fixed',
    fixedRootNode: true,
  },
  disableNodeClickEffect: true,
  disableLineClickEffect: true,
  moveToCenterWhenRefresh: true,
  zoomToFitWhenRefresh: false,
  defaultJunctionPoint: 'border',
  defaultLineWidth: 1.2,
  defaultLineColor: '#8aa6b9',
  defaultNodeBorderColor: '#9fb6c5',
  defaultNodeColor: '#f7fbfd',
  defaultNodeFontColor: '#102131',
  defaultNodeBorderWidth: 1,
  defaultNodeWidth: 184,
  defaultNodeHeight: 66,
  backgroundColor: 'transparent',
  disableDragNode: true,
  disableDragLine: true,
  disableDragCanvas: false,
  allowShowZoomMenu: false,
  allowShowMiniToolBar: false,
  allowShowRefreshButton: false,
  allowShowDownloadButton: false,
  useAnimationWhenRefresh: false,
}))

const relationOptions = computed(() => {
  const counts = new Map<string, number>()
  for (const edge of graphEdges.value) {
    const key = String(edge.relation_key || '').trim()
    if (!key) continue
    counts.set(key, (counts.get(key) || 0) + 1)
  }
  return Array.from(counts.entries())
    .map(([key, count]) => ({ key, count, label: relationLabel(key) }))
    .sort((a, b) => a.label.localeCompare(b.label, 'zh-Hans-CN'))
})

const displayEdges = computed(() => {
  if (!relationFilter.value) return graphEdges.value
  return graphEdges.value.filter((edge) => String(edge.relation_key || '') === relationFilter.value)
})

const renderPayload = computed(() =>
  buildGraphRenderPayload({
    nodes: graphNodes.value,
    edges: graphEdges.value,
    centerType: isIndustryCenterMode.value ? 'industry' : 'theme',
    centerId: centerId.value,
    relationKey: relationFilter.value,
    selectedNodeId: selectedNodeId.value,
    limit: Number(queryFilters.limit || 12),
    viewportWidth: viewportWidth.value,
    hideSecondary: !showSecondary.value,
  }),
)
const displaySummary = computed(() => {
  const nodes = Array.isArray(renderPayload.value?.jsonData?.nodes) ? renderPayload.value.jsonData.nodes : []
  const lines = Array.isArray(renderPayload.value?.jsonData?.lines) ? renderPayload.value.jsonData.lines : []
  let themeCount = 0
  let industryCount = 0
  for (const item of nodes) {
    const rawType = String(item?.data?.raw?.type || '')
    if (rawType === 'theme') themeCount += 1
    if (rawType === 'industry') industryCount += 1
  }
  return {
    node_count: nodes.length,
    edge_count: lines.length,
    theme_count: themeCount,
    industry_count: industryCount,
  }
})
const readableRelationFilter = computed(() => (relationFilter.value ? relationLabel(relationFilter.value) : '全部关系'))
const readableViewMode = computed(() => (graphViewMode.value === 'full' ? '展开二级节点' : '只看主干'))

const centerSuggestions = computed(() => {
  if (isIndustryCenterMode.value) {
    return [
      { type: 'industry', label: '软件' },
      { type: 'industry', label: '半导体' },
      { type: 'industry', label: '银行' },
      { type: 'industry', label: '黄金' },
    ]
  }
  return [
    { type: 'theme', label: 'AI' },
    { type: 'theme', label: '黄金' },
    { type: 'theme', label: '机器人' },
    { type: 'theme', label: '消费电子' },
  ]
})

const hiddenNodeCount = computed(() => renderPayload.value.hiddenNodeCount)
const canShowTrunkOnly = computed(() => renderPayload.value.secondaryCandidateCount > 0)
const graphHeight = computed(() => {
  const base = isIndustryCenterMode.value ? 680 : 640
  const bonus = Math.max(0, Math.min(220, hiddenNodeCount.value * 4))
  return Math.max(560, Math.min(860, base + bonus))
})

const selectedNode = computed<GraphNode | null>(() => {
  const id = String(selectedNodeId.value || centerId.value || '')
  return graphNodes.value.find((node) => String(node.id || '') === id) || graphNodes.value[0] || null
})

const selectedEdgeRows = computed(() => {
  const selectedId = String(selectedNode.value?.id || '')
  if (!selectedId) return []
  return displayEdges.value.filter((edge) => String(edge.source || '') === selectedId || String(edge.target || '') === selectedId)
})

const relatedNodes = computed(() => {
  const selectedId = String(selectedNode.value?.id || '')
  if (!selectedId) return []
  const ids = new Set<string>()
  for (const edge of selectedEdgeRows.value) {
    if (String(edge.source || '') !== selectedId) ids.add(String(edge.source || ''))
    if (String(edge.target || '') !== selectedId) ids.add(String(edge.target || ''))
  }
  return graphNodes.value.filter((node) => ids.has(String(node.id || '')))
})

const selectedActions = computed(() => {
  const actions = Array.isArray(selectedNode.value?.actions) ? selectedNode.value.actions : []
  return actions.filter((item) => item && String(item.to || '').trim())
})

const detailMetrics = computed(() => {
  const node = selectedNode.value
  if (!node || !node.metrics || typeof node.metrics !== 'object') return []
  return Object.entries(node.metrics)
    .filter(([, value]) => value !== null && value !== undefined && value !== '')
    .slice(0, 8)
    .map(([label, value]) => ({ label: metricLabel(label), value: formatValue(value) }))
})

const detailWhy = computed(() => {
  if (!selectedNode.value) return ''
  if (selectedNode.value.id === centerId.value) {
    return String(graphDetail.value?.why || selectedNode.value.summary || '')
  }
  return String(selectedNode.value.summary || selectedNode.value.subtitle || '')
})

const centerDetailSummary = computed(() => String(graphDetail.value?.why || ''))
const centerDetailNextSteps = computed(() => (Array.isArray(graphDetail.value?.next_steps) ? graphDetail.value.next_steps : []))

watch(
  () => [centerId.value, graphNodes.value.length] as const,
  () => {
    if (!graphNodes.value.length) {
      selectedNodeId.value = ''
      return
    }
    const target = String(selectedNodeId.value || centerId.value)
    const exists = graphNodes.value.some((node) => String(node.id || '') === target)
    selectedNodeId.value = exists ? target : String(centerId.value || graphNodes.value[0]?.id || '')
  },
  { immediate: true },
)

watch(
  () => route.query,
  () => {
    applyRouteQuery()
  },
)

watch(relationFilter, () => {
  if (isApplyingRouteQuery) return
  syncRouteFromQuery()
})

watch(showSecondary, () => {
  if (isApplyingRouteQuery) return
  syncRouteFromQuery()
})

watch(
  () => [
    graphNodes.value,
    graphEdges.value,
    relationFilter.value,
    showSecondary.value,
    queryFilters.center_type,
    queryFilters.limit,
    centerId.value,
    viewportWidth.value,
  ] as const,
  () => {
    pendingFitToView = true
    scheduleRenderGraph()
  },
  { deep: true },
)

watch(selectedNodeId, () => {
  pendingFitToView = false
  scheduleRenderGraph()
})

function relationLabel(key: string) {
  return (
    ({
      theme_industry: '主题→行业',
      industry_theme: '行业→主题',
      industry_stock: '行业→股票',
      theme_stock: '主题→股票',
    } as Record<string, string>)[key] || key || '-'
  )
}

function metricLabel(key: string) {
  return (
    ({
      avg_score: '平均分',
      top_score: '顶部得分',
      stock_count: '股票数',
      confidence: '置信度',
      signal_strength: '信号强度',
      theme_strength: '主题强度',
      evidence_count: '证据数',
      relation_weight: '关系权重',
    } as Record<string, string>)[key] || key
  )
}

function formatValue(value: unknown) {
  const num = Number(value)
  if (Number.isFinite(num)) return num.toFixed(num >= 100 ? 0 : 2)
  return String(value ?? '-')
}

function centerTypeLabel(value: string) {
  return value === 'industry' ? '行业中心' : '主题中心'
}

function nodeTypeLabel(value: string) {
  return (
    ({
      theme: '主题',
      industry: '行业',
      stock: '股票',
    } as Record<string, string>)[String(value || '')] || String(value || '-')
  )
}

function edgeSummary(edge: GraphEdge) {
  return `${String(edge.source || '-')} → ${String(edge.target || '-')} · 证据 ${edge.evidence_count ?? 0}`
}

function selectNode(nodeId: string) {
  if (!nodeId) return
  selectedNodeId.value = nodeId
}

function goTo(path: string) {
  if (!path) return
  router.push(path)
}

function switchCenterToSelected() {
  const node = selectedNode.value
  if (!node || !['theme', 'industry'].includes(String(node.type || ''))) return
  filters.center_type = String(node.type)
  filters.keyword = String(node.label || '')
  applyFilters()
}

function applyFilters() {
  queryFilters.center_type = filters.center_type || 'theme'
  queryFilters.center_key = (filters.keyword || '').trim()
  queryFilters.depth = Number(filters.depth) || 2
  queryFilters.limit = Number(filters.limit) || 12
  syncRouteFromQuery()
}

function resetFilters() {
  filters.center_type = 'theme'
  filters.keyword = ''
  filters.depth = 2
  filters.limit = 12
  relationFilter.value = ''
  showSecondary.value = true
  applyFilters()
}

function toggleSecondary(next: boolean) {
  if (!next && !canShowTrunkOnly.value) return
  showSecondary.value = next
}

function quickSwitchCenter(type: string, label: string) {
  filters.center_type = type === 'industry' ? 'industry' : 'theme'
  filters.keyword = label
  applyFilters()
}

function goToDecision(node: GraphNode | null) {
  if (!node) return
  const query: Record<string, string> = {}
  const nodeType = String(node.type || '')
  const label = String(node.label || '').trim()
  const tsCode = String(node.ts_code || node.id || '').trim()
  if (nodeType === 'stock' && /^[0-9]{6}\.(SZ|SH|BJ)$/i.test(tsCode)) {
    query.ts_code = tsCode.toUpperCase()
  } else if (label) {
    query.keyword = label.slice(0, 30)
  }
  query.from = 'signal_graph'
  // Structured action template: evidence from signal node context
  const summary = String(node.summary || node.subtitle || '').trim()
  const statusLabel = String(node.status || '').trim()
  const evidenceParts: string[] = [`[信号图谱] ${label.slice(0, 30)}`]
  if (nodeType) evidenceParts.push(`类型=${nodeType}`)
  if (statusLabel) evidenceParts.push(`状态=${statusLabel}`)
  query.evidence = evidenceParts.join(' · ')
  query.note = `信号触发观察 · ${label.slice(0, 20)}${summary ? ' · ' + summary.slice(0, 20) : ''}`
  router.push({ path: '/research/decision', query })
}

function syncRouteFromQuery() {
  router.replace({
    query: buildCleanQuery({
      center_type: queryFilters.center_type,
      center_key: queryFilters.center_key,
      depth: queryFilters.depth,
      limit: queryFilters.limit,
      relation_key: relationFilter.value,
      view: graphViewMode.value,
    }),
  })
}

function applyRouteQuery() {
  isApplyingRouteQuery = true
  try {
    const q = route.query as Record<string, unknown>
    const centerKey = readQueryString(q, 'center_key', readQueryString(q, 'keyword', ''))
    const next = {
      center_type: readQueryString(q, 'center_type', 'theme'),
      keyword: centerKey,
      depth: Math.max(1, Math.min(3, readQueryNumber(q, 'depth', 2))),
      limit: Math.max(8, Math.min(24, readQueryNumber(q, 'limit', 12))),
    }
    Object.assign(filters, next)
    Object.assign(queryFilters, {
      center_type: next.center_type,
      center_key: centerKey,
      depth: next.depth,
      limit: next.limit,
    })
    relationFilter.value = readQueryString(q, 'relation_key', '')
    const view = readQueryString(q, 'view', 'full')
    showSecondary.value = view !== 'trunk'
  } finally {
    isApplyingRouteQuery = false
  }
}

function graphNodeClass(node: any) {
  const role = String(node?.data?.role || '')
  const isSelected = String(node?.id || '') === String(selectedNode.value?.id || '')
  return {
    'graph-node-center': role === 'center',
    'graph-node-primary': role === 'primary',
    'graph-node-secondary': role === 'secondary',
    'graph-node-selected': isSelected,
  }
}

function graphNodeMeta(node: any) {
  const meta = String(node?.data?.meta || '').trim()
  if (meta) return meta
  const score = Number(node?.data?.score)
  if (Number.isFinite(score)) return `评分 ${formatValue(score)}`
  return '-'
}

function handleNodeDoubleClick(node: any) {
  const id = String(node?.id || '')
  if (!id) return
  const raw = graphNodes.value.find((item) => String(item.id || '') === id)
  if (!raw) return
  if (!['theme', 'industry'].includes(String(raw.type || ''))) return
  filters.center_type = String(raw.type)
  filters.keyword = String(raw.label || '')
  applyFilters()
}

function onGraphNodeClick(node: any) {
  const id = String(node?.id || '')
  if (id) selectNode(id)
}

function onGraphCanvasClick() {
  if (centerId.value) {
    selectedNodeId.value = centerId.value
  }
}

function getGraphInstance() {
  return graphRef.value?.getInstance?.() || null
}

function scheduleRenderGraph() {
  if (renderTimer) {
    window.clearTimeout(renderTimer)
  }
  renderTimer = window.setTimeout(() => {
    renderGraph(pendingFitToView)
    renderTimer = null
    pendingFitToView = false
  }, 24)
}

function renderGraph(fitToView = true) {
  if (graphEmpty.value) return
  const graph = graphRef.value
  if (!graph?.setJsonData) return
  graph.setJsonData(renderPayload.value.jsonData, true, async (instance: any) => {
    if (selectedNodeId.value) {
      instance?.setCheckedNode?.(selectedNodeId.value)
    }
    if (fitToView) {
      await instance?.zoomToFit?.()
      zoomLevel.value = 100
    }
  })
}

async function zoomIn() {
  const instance = getGraphInstance()
  if (!instance) return
  zoomLevel.value = Math.min(MAX_ZOOM, zoomLevel.value + ZOOM_STEP)
  await instance.setZoom?.(zoomLevel.value)
}

async function zoomOut() {
  const instance = getGraphInstance()
  if (!instance) return
  zoomLevel.value = Math.max(MIN_ZOOM, zoomLevel.value - ZOOM_STEP)
  await instance.setZoom?.(zoomLevel.value)
}

async function recenterGraph() {
  const instance = getGraphInstance()
  if (!instance) return
  instance.focusRootNode?.()
  await instance.zoomToFit?.()
  zoomLevel.value = 100
}

onMounted(async () => {
  const syncViewportWidth = () => {
    viewportWidth.value = window.innerWidth
  }
  if (typeof window !== 'undefined') {
    syncViewportWidth()
    window.addEventListener('resize', syncViewportWidth)
    removeResizeListener = () => {
      window.removeEventListener('resize', syncViewportWidth)
    }
  }
  applyRouteQuery()
  await nextTick()
  scheduleRenderGraph()
})

onBeforeUnmount(() => {
  if (removeResizeListener) {
    removeResizeListener()
    removeResizeListener = null
  }
  if (renderTimer) {
    window.clearTimeout(renderTimer)
    renderTimer = null
  }
})
</script>

<style scoped>
.graph-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.graph-tool-btn {
  border-radius: 999px;
  border: 1px solid var(--line);
  background: white;
  padding: 6px 12px;
  font-size: 12px;
  font-weight: 700;
  color: var(--ink);
  transition: all 0.2s ease;
}

.graph-tool-btn:hover {
  border-color: var(--brand);
  color: var(--brand);
}

.graph-stage {
  position: relative;
  overflow: hidden;
}

.relation-graph-core {
  width: 100%;
  height: 100%;
  background:
    radial-gradient(100% 90% at 0% 50%, rgba(15, 126, 173, 0.1) 0%, rgba(15, 126, 173, 0) 62%),
    linear-gradient(0deg, rgba(15, 23, 42, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(15, 23, 42, 0.03) 1px, transparent 1px);
  background-size: auto, 24px 24px, 24px 24px;
}

.graph-node {
  width: 100%;
  border-radius: 16px;
  border: 1px solid var(--graph-muted);
  background: #fdfefe;
  padding: 10px 12px;
  text-align: left;
  line-height: 1.2;
  box-shadow: 0 8px 20px rgba(11, 40, 58, 0.08);
  transition: all 0.18s ease;
}

.graph-node-title {
  color: #102131;
  font-size: 13px;
  font-weight: 800;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.graph-node-meta {
  margin-top: 4px;
  color: #4b5f71;
  font-size: 11px;
  font-weight: 600;
}

.graph-node-center {
  border-width: 2px;
  border-color: var(--graph-center);
  background: linear-gradient(180deg, #f6fbfe 0%, #edf6fa 100%);
  box-shadow: 0 10px 24px rgba(15, 97, 122, 0.2);
}

.graph-node-center .graph-node-title {
  font-size: 20px;
  font-weight: 900;
  color: #0f4f66;
}

.graph-node-primary {
  border-color: var(--graph-primary);
  background: #f8fbfc;
}

.graph-node-secondary {
  border-color: var(--graph-secondary);
  background: #fbfdfe;
}

.graph-node-selected {
  border-color: var(--graph-center);
  box-shadow: 0 0 0 2px rgba(15, 97, 122, 0.2), 0 12px 28px rgba(15, 97, 122, 0.16);
}

@media (max-width: 900px) {
  .graph-toolbar {
    flex-direction: column;
    align-items: flex-start;
  }

  .graph-node-center .graph-node-title {
    font-size: 17px;
  }
}
</style>
