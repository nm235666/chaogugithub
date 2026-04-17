<template>
  <AppShell title="群聊投资倾向总览" subtitle="统一查看每个群的整体看多/看空结论、情绪分和重点投资标的。">
    <div class="space-y-4">
      <PageSection title="汇总概览" subtitle="先看整体有多少群在分析、多少偏多、多少偏空。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <StatCard title="已分析群聊" :value="summary.analysis_total ?? 0" hint="最近一次分析结果汇总" />
          <StatCard title="整体看多" :value="summary.bullish_total ?? 0" hint="最终结论偏多的群聊" />
          <StatCard title="整体看空" :value="summary.bearish_total ?? 0" hint="最终结论偏空的群聊" />
          <StatCard title="有标的清单" :value="summary.with_targets_total ?? 0" hint="分析中识别出投资标的" />
        </div>
      </PageSection>

      <PageSection title="筛选条件" subtitle="按群名、整体偏向和目标关键词检索。">
        <fieldset class="grid gap-3 xl:grid-cols-5 md:grid-cols-2">
          <legend class="sr-only">投资倾向筛选条件</legend>
          <label class="text-sm font-semibold text-[var(--ink)]">
            群聊关键词
            <input v-model="filters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="群名 / 群总结" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            倾向
            <select v-model="filters.final_bias" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部倾向</option>
              <option v-for="item in finalBiasOptions" :key="item" :value="item">{{ item }}</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            标的关键词
            <input v-model="filters.target_keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="如 原油 / 英伟达" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            每页条数
            <select v-model.number="filters.page_size" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option :value="8">8 / 页</option>
              <option :value="12">12 / 页</option>
              <option :value="20">20 / 页</option>
              <option :value="50">50 / 页</option>
              <option :value="100">100 / 页</option>
            </select>
          </label>
          <div class="flex items-end">
            <button class="w-full rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="applyFilters">查询</button>
          </div>
        </fieldset>
      </PageSection>

      <PageSection :title="`分析结果 (${result?.total || 0})`" subtitle="每个群卡片里同时展示总结、情绪和标的列表。">
        <div class="grid gap-3 2xl:grid-cols-2">
          <InfoCard
            v-for="item in result?.items || []"
            :key="item.room_id"
            :title="item.remark || item.nick_name || item.talker || item.room_id || '(未命名群)'"
            :meta="joinParts([`room_id ${item.room_id || '-'}`, item.latest_message_date ? `最新消息 ${item.latest_message_date}` : '', item.analysis_date ? `分析日期 ${item.analysis_date}` : '', item.model ? `主分析模型 ${item.model}` : '', item.llm_sentiment_model ? `情绪模型 ${item.llm_sentiment_model}` : ''])"
            :description="item.room_summary || ''"
          >
            <template #badge>
              <StatusBadge :value="item.final_bias" :label="item.final_bias || '-'" />
            </template>
            <div class="mt-3 grid gap-3 xl:grid-cols-3 md:grid-cols-2">
              <div class="metric-chip">分析窗口 <strong>{{ item.analysis_window_days ?? '-' }} 天</strong></div>
              <div class="metric-chip">消息数 <strong>{{ item.message_count ?? 0 }}</strong></div>
              <div class="metric-chip">发言人数 <strong>{{ item.sender_count ?? 0 }}</strong></div>
              <div class="metric-chip">成员数 <strong>{{ item.user_count ?? '-' }}</strong></div>
              <div class="metric-chip">情绪分 <strong>{{ item.llm_sentiment_score ?? '-' }}</strong></div>
              <div class="metric-chip">情绪标签 <strong>{{ item.llm_sentiment_label || '未评' }}</strong></div>
            </div>
            <div class="mt-3 text-sm text-[var(--muted)]">{{ item.llm_sentiment_reason || '暂无情绪原因说明。' }}</div>
            <div class="mt-3 space-y-2">
              <div class="text-sm font-semibold text-[var(--ink)]">投资标的</div>
              <div class="max-h-[220px] space-y-2 overflow-auto pr-1">
                <InfoCard
                  v-for="target in parseTargets(item.targets_json)"
                  :key="`${item.room_id}-${target.name}-${target.bias}`"
                  :title="target.name || '-'" :description="target.reason || ''"
                >
                  <template #badge>
                    <StatusBadge :value="target.bias" :label="target.bias || '-'" />
                  </template>
                  <div class="mt-2 flex flex-wrap gap-2">
                    <button
                      class="rounded-full border border-emerald-300 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-800 transition hover:border-emerald-500 hover:bg-emerald-100"
                      @click="goDecisionFromChatroom(target, item)"
                    >
                      → 决策板
                    </button>
                  </div>
                </InfoCard>
              </div>
            </div>
          </InfoCard>
        </div>
        <div class="table-pager mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ queryFilters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page <= 1" @click="goPrevPage">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="queryFilters.page >= (result?.total_pages || 1)" @click="goNextPage">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchChatroomInvestment } from '../../services/api/chatrooms'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

function joinParts(parts: Array<unknown>) {
  return parts.map((item) => String(item ?? '').trim()).filter(Boolean).join(' · ')
}

function parseTargets(raw: unknown): Array<Record<string, any>> {
  if (!raw) return []
  if (Array.isArray(raw)) return raw
  try {
    const parsed = JSON.parse(String(raw))
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

const filters = reactive({
  keyword: '',
  final_bias: '',
  target_keyword: '',
  page: 1,
  page_size: 8,
})
const queryFilters = reactive({
  keyword: '',
  final_bias: '',
  target_keyword: '',
  page: 1,
  page_size: 8,
})
const route = useRoute()
const router = useRouter()

const { data: result } = useQuery({
  queryKey: computed(() => ['chatroom-investment', { ...queryFilters }]),
  queryFn: () => fetchChatroomInvestment({ ...queryFilters }),
  placeholderData: keepPreviousData,
})

const summary = computed(() => result.value?.summary || {})
const finalBiasOptions = computed(() => result.value?.filters?.final_biases || [])

function syncRouteFromFilters() {
  router.replace({
    query: buildCleanQuery({
      keyword: queryFilters.keyword,
      final_bias: queryFilters.final_bias,
      target_keyword: queryFilters.target_keyword,
      page: queryFilters.page,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteFilters() {
  const q = route.query as Record<string, unknown>
  const next = {
    keyword: readQueryString(q, 'keyword', ''),
    final_bias: readQueryString(q, 'final_bias', ''),
    target_keyword: readQueryString(q, 'target_keyword', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(8, readQueryNumber(q, 'page_size', 8)),
  }
  Object.assign(filters, next)
  Object.assign(queryFilters, next)
}

function applyFilters() {
  Object.assign(queryFilters, {
    keyword: filters.keyword,
    final_bias: filters.final_bias,
    target_keyword: filters.target_keyword,
    page: 1,
    page_size: Number(filters.page_size) || 8,
  })
  syncRouteFromFilters()
}

function goPrevPage() {
  if (queryFilters.page <= 1) return
  queryFilters.page -= 1
  syncRouteFromFilters()
}

function goNextPage() {
  const totalPages = Number(result.value?.total_pages || 1)
  if (queryFilters.page >= totalPages) return
  queryFilters.page += 1
  syncRouteFromFilters()
}

function goDecisionFromChatroom(target: Record<string, any>, _roomItem?: Record<string, any>) {
  const name = String(target.name || target.ts_code || '').trim()
  const tsCode = String(target.ts_code || '').trim()
  const bias = String(target.bias || '').trim()
  const query: Record<string, string> = { from: 'chatroom' }
  if (tsCode) {
    query.ts_code = tsCode
    query.keyword = tsCode
  } else if (name) {
    query.keyword = name.slice(0, 20)
  }
  // Structured action template: pre-fill evidence source and note from chatroom context
  if (name) {
    const biasLabel = bias ? `偏向=${bias}` : ''
    query.evidence = `[群聊倾向] ${name.slice(0, 30)}${biasLabel ? ' · ' + biasLabel : ''}`
    query.note = `群聊触发观察 · ${name.slice(0, 20)}${bias ? ' · ' + bias : ''}`
  }
  router.push({ path: '/research/decision', query })
}

watch(
  () => route.query,
  () => {
    applyRouteFilters()
  },
  { immediate: true },
)
</script>
