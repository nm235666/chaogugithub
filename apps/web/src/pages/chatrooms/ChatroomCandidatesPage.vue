<template>
  <AppShell title="股票候选池" subtitle="按群聊观点聚合出来的股票 / 主题候选池统一查看。">
    <div class="space-y-4">
      <PageSection title="候选池结果" subtitle="这是群聊信息转投资线索的统一入口。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <input v-model="filters.keyword" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="关键词" />
          <select v-model="filters.candidate_type" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部类型</option>
            <option value="股票">股票</option>
            <option value="主题">主题</option>
          </select>
          <select v-model="filters.bias" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">全部方向</option>
            <option value="看多">看多</option>
            <option value="看空">看空</option>
          </select>
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" @click="filters.page = 1">刷新</button>
        </div>
        <div class="mt-4 space-y-2">
          <InfoCard v-for="item in result?.items || []" :key="item.candidate_name + String(item.latest_analysis_date)" :title="item.candidate_name || '-'" :meta="`${item.candidate_type || '-'} · 净分 ${item.net_score ?? 0} · 提及 ${item.mention_count ?? 0}`">
            <template #badge>
              <StatusBadge :value="item.dominant_bias" :label="item.dominant_bias || '-'" />
            </template>
          </InfoCard>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { reactive } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchCandidatePool } from '../../services/api/chatrooms'

const filters = reactive({ keyword: '', candidate_type: '', bias: '', page: 1, page_size: 30 })
const { data: result } = useQuery({ queryKey: ['candidate-pool', filters], queryFn: () => fetchCandidatePool(filters) })
</script>
