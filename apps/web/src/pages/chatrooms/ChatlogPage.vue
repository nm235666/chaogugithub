<template>
  <AppShell title="聊天记录查询" subtitle="按群聊、发送人、关键词和引用状态查看清洗后的微信聊天记录。">
    <div class="space-y-4">
      <PageSection title="筛选条件" subtitle="支持按群聊、发送人、关键词、是否引用和日期区间筛选。">
        <fieldset class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <legend class="sr-only">聊天记录筛选条件</legend>
          <label class="text-sm font-semibold text-[var(--ink)]">
            群聊
            <select v-model="filters.talker" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部群聊</option>
              <option v-for="item in filterOptions.talkers" :key="item" :value="item">{{ item }}</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            发送人
            <select v-model="filters.sender_name" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部发送人</option>
              <option v-for="item in filterOptions.senders" :key="item" :value="item">{{ item }}</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            关键词
            <input v-model="filters.keyword" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="消息内容 / 引用内容 / 发送人" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            引用状态
            <select v-model="filters.is_quote" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="">全部消息</option>
              <option value="1">仅引用</option>
              <option value="0">仅非引用</option>
            </select>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            开始日期
            <input v-model="filters.query_date_start" type="date" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            结束日期
            <input v-model="filters.query_date_end" type="date" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            每页条数
            <select v-model.number="filters.page_size" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
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

      <PageSection :title="`聊天结果 (${result?.total || 0})`" subtitle="正文、引用内容与元信息统一卡片化展示。">
        <div class="space-y-2">
          <InfoCard
            v-for="item in result?.items || []"
            :key="item.id"
            :title="item.sender_name || '(未知发送人)'"
            :meta="joinParts([`群聊 ${item.talker || '-'}`, item.message_date, item.message_time])"
            :description="item.content_clean || item.content || ''"
          >
            <template #badge>
              <StatusBadge :value="Number(item.is_quote) === 1 ? 'info' : (item.message_type === 'system' ? 'warning' : 'muted')" :label="messageTypeLabel(item)" />
            </template>
            <div v-if="Number(item.is_quote) === 1" class="mt-3 rounded-[16px] border border-[var(--line)] bg-[var(--panel-soft)] p-3 text-sm text-[var(--ink-soft)]">
              <div class="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">引用消息</div>
              <div class="mt-2">{{ joinParts([item.quote_sender_name, item.quote_time_text]) || '未知引用来源' }}</div>
              <div class="mt-2 leading-7">{{ item.quote_content || '-' }}</div>
            </div>
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
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import { keepPreviousData, useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchWechatChatlog } from '../../services/api/chatrooms'
import { buildCleanQuery, readQueryNumber, readQueryString } from '../../shared/utils/urlState'

function joinParts(parts: Array<unknown>) {
  return parts.map((item) => String(item ?? '').trim()).filter(Boolean).join(' · ')
}

function messageTypeLabel(item: Record<string, any>) {
  if (Number(item.is_quote) === 1) return '引用消息'
  if (item.message_type === 'system') return '系统消息'
  return '普通消息'
}

const filters = reactive({
  talker: '',
  sender_name: '',
  keyword: '',
  is_quote: '',
  query_date_start: '',
  query_date_end: '',
  page: 1,
  page_size: 20,
})
const queryFilters = reactive({
  talker: '',
  sender_name: '',
  keyword: '',
  is_quote: '',
  query_date_start: '',
  query_date_end: '',
  page: 1,
  page_size: 20,
})
const route = useRoute()
const router = useRouter()

const { data: result } = useQuery({
  queryKey: computed(() => ['wechat-chatlog', { ...queryFilters }]),
  queryFn: () => fetchWechatChatlog({ ...queryFilters }),
  placeholderData: keepPreviousData,
})

const filterOptions = computed(() => ({
  talkers: result.value?.filters?.talkers || [],
  senders: result.value?.filters?.senders || [],
}))

function syncRouteFromFilters() {
  router.replace({
    query: buildCleanQuery({
      talker: queryFilters.talker,
      sender_name: queryFilters.sender_name,
      keyword: queryFilters.keyword,
      is_quote: queryFilters.is_quote,
      query_date_start: queryFilters.query_date_start,
      query_date_end: queryFilters.query_date_end,
      page: queryFilters.page,
      page_size: queryFilters.page_size,
    }),
  })
}

function applyRouteFilters() {
  const q = route.query as Record<string, unknown>
  const next = {
    talker: readQueryString(q, 'talker', ''),
    sender_name: readQueryString(q, 'sender_name', ''),
    keyword: readQueryString(q, 'keyword', ''),
    is_quote: readQueryString(q, 'is_quote', ''),
    query_date_start: readQueryString(q, 'query_date_start', ''),
    query_date_end: readQueryString(q, 'query_date_end', ''),
    page: Math.max(1, readQueryNumber(q, 'page', 1)),
    page_size: Math.max(20, readQueryNumber(q, 'page_size', 20)),
  }
  Object.assign(filters, next)
  Object.assign(queryFilters, next)
}

function applyFilters() {
  Object.assign(queryFilters, {
    talker: filters.talker,
    sender_name: filters.sender_name,
    keyword: filters.keyword,
    is_quote: filters.is_quote,
    query_date_start: filters.query_date_start,
    query_date_end: filters.query_date_end,
    page: 1,
    page_size: Number(filters.page_size) || 20,
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

watch(
  () => route.query,
  () => {
    applyRouteFilters()
  },
  { immediate: true },
)
</script>
