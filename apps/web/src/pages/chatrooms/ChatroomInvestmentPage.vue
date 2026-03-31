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
        <div class="grid gap-3 xl:grid-cols-[1.2fr_180px_1fr_140px_120px] md:grid-cols-2">
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="搜索群名 / 群总结" />
          <select v-model="filters.final_bias" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部倾向</option>
            <option v-for="item in finalBiasOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <input v-model="filters.target_keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="按投资标的关键词筛选，如 原油 / 英伟达" />
          <select v-model.number="filters.page_size" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="20">20 / 页</option>
            <option :value="50">50 / 页</option>
            <option :value="100">100 / 页</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">查询</button>
        </div>
      </PageSection>

      <PageSection :title="`分析结果 (${result?.total || 0})`" subtitle="每个群卡片里同时展示总结、情绪和标的列表。">
        <div class="space-y-2">
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
              <InfoCard
                v-for="target in parseTargets(item.targets_json)"
                :key="`${item.room_id}-${target.name}-${target.bias}`"
                :title="target.name || '-'" :description="target.reason || ''"
              >
                <template #badge>
                  <StatusBadge :value="target.bias" :label="target.bias || '-'" />
                </template>
              </InfoCard>
            </div>
          </InfoCard>
        </div>
        <div class="mt-3 flex items-center justify-between text-sm text-[var(--muted)]">
          <div>第 {{ filters.page }} / {{ result?.total_pages || 1 }} 页</div>
          <div class="flex gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page <= 1" @click="filters.page -= 1">上一页</button>
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white disabled:opacity-40" :disabled="filters.page >= (result?.total_pages || 1)" @click="filters.page += 1">下一页</button>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchChatroomInvestment } from '../../services/api/chatrooms'

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
  page_size: 20,
})

const { data: result } = useQuery({
  queryKey: ['chatroom-investment', filters],
  queryFn: () => fetchChatroomInvestment(filters),
})

const summary = computed(() => result.value?.summary || {})
const finalBiasOptions = computed(() => result.value?.filters?.final_biases || [])
</script>
