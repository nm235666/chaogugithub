<template>
  <AppShell title="信号质量配置" subtitle="把规则阈值和映射黑名单收进统一后台，而不是继续停留在旧 HTML 页面。">
    <div class="space-y-4">
      <PageSection title="配置概览" subtitle="先看规则和黑名单数量，再决定要不要编辑。">
        <div class="grid gap-3 xl:grid-cols-4 md:grid-cols-2">
          <StatCard title="规则数" :value="rules.length" :hint="`启用 ${rules.filter((item) => item.enabled).length}`" />
          <StatCard title="黑名单项" :value="blocklist.length" :hint="`启用 ${blocklist.filter((item) => item.enabled).length}`" />
          <StatCard title="更新时间" :value="generatedAt || '-'" hint="来自后端配置查询" />
          <StatCard title="当前状态" :value="saveMessage || '准备就绪'" hint="保存成功后会自动回刷" />
        </div>
      </PageSection>

      <PageSection title="规则参数" subtitle="适合调节阈值、权重和开关。">
        <template #action>
          <div class="flex flex-wrap gap-2">
            <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white" @click="addRule">新增规则</button>
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-2 text-white" :disabled="isSaving" @click="saveAll">保存全部</button>
          </div>
        </template>
        <div class="space-y-3 lg:hidden">
          <InfoCard
            v-for="(row, idx) in rules"
            :key="`${row.rule_key || 'rule'}-${idx}`"
            :title="row.rule_key || '未命名规则'"
            :meta="`分类 ${row.category || '-'} · 类型 ${row.value_type || '-'}`"
          >
            <div class="grid gap-2">
              <label class="text-sm font-semibold text-[var(--ink)]">
                规则键
                <input v-model="row.rule_key" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                规则值
                <input v-model="row.rule_value" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                值类型
                <select v-model="row.value_type" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2">
                  <option value="number">数值</option>
                  <option value="bool">布尔</option>
                  <option value="text">文本</option>
                </select>
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                分类
                <input v-model="row.category" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                说明
                <textarea v-model="row.description" class="mt-1 min-h-[72px] w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="inline-flex items-center gap-2 text-sm text-[var(--muted)]">
                <input v-model="row.enabled" type="checkbox" />
                启用规则
              </label>
              <button class="rounded-xl bg-[rgba(178,77,84,0.1)] px-3 py-2 text-sm font-semibold text-[var(--danger)]" @click="removeRule(row)">移除规则</button>
            </div>
          </InfoCard>
        </div>

        <DataTable class="hidden lg:block" :columns="ruleColumns" :rows="rules" row-key="rule_key" empty-text="暂无规则">
          <template #cell-enabled="{ row }"><input v-model="row.enabled" type="checkbox" /></template>
          <template #cell-rule_key="{ row }"><input v-model="row.rule_key" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-rule_value="{ row }"><input v-model="row.rule_value" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-value_type="{ row }">
            <select v-model="row.value_type" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2">
              <option value="number">数值</option>
              <option value="bool">布尔</option>
              <option value="text">文本</option>
            </select>
          </template>
          <template #cell-category="{ row }"><input v-model="row.category" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-description="{ row }"><textarea v-model="row.description" class="min-h-[42px] w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-actions="{ row }"><button class="rounded-xl bg-[rgba(178,77,84,0.1)] px-3 py-2 text-sm font-semibold text-[var(--danger)]" @click="removeRule(row)">移除</button></template>
        </DataTable>
      </PageSection>

      <PageSection title="映射黑名单" subtitle="用于拦截主题词被误认成股票或其他不该进榜的词。">
        <template #action>
          <button class="rounded-2xl bg-stone-800 px-4 py-2 text-white" @click="addBlock">新增黑名单</button>
        </template>
        <div class="space-y-3 lg:hidden">
          <InfoCard
            v-for="(row, idx) in blocklist"
            :key="`${row.id || 'block'}-${idx}`"
            :title="row.term || '未命名词项'"
            :meta="`${row.target_type || '-'} · ${row.match_type || '-'}`"
          >
            <div class="grid gap-2">
              <label class="text-sm font-semibold text-[var(--ink)]">
                词项
                <input v-model="row.term" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                目标类型
                <select v-model="row.target_type" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2">
                  <option value="stock">股票</option>
                  <option value="theme">主题</option>
                  <option value="all">全部</option>
                </select>
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                匹配方式
                <select v-model="row.match_type" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2">
                  <option value="exact">精确</option>
                  <option value="contains">包含</option>
                  <option value="prefix">前缀</option>
                </select>
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                来源
                <input v-model="row.source" class="mt-1 w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="text-sm font-semibold text-[var(--ink)]">
                原因
                <textarea v-model="row.reason" class="mt-1 min-h-[72px] w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" />
              </label>
              <label class="inline-flex items-center gap-2 text-sm text-[var(--muted)]">
                <input v-model="row.enabled" type="checkbox" />
                启用词项
              </label>
              <button class="rounded-xl bg-[rgba(178,77,84,0.1)] px-3 py-2 text-sm font-semibold text-[var(--danger)]" @click="removeBlock(row)">移除黑名单项</button>
            </div>
          </InfoCard>
        </div>

        <DataTable class="hidden lg:block" :columns="blockColumns" :rows="blocklist" row-key="id" empty-text="暂无黑名单">
          <template #cell-enabled="{ row }"><input v-model="row.enabled" type="checkbox" /></template>
          <template #cell-term="{ row }"><input v-model="row.term" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-target_type="{ row }">
            <select v-model="row.target_type" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2">
              <option value="stock">股票</option>
              <option value="theme">主题</option>
              <option value="all">全部</option>
            </select>
          </template>
          <template #cell-match_type="{ row }">
            <select v-model="row.match_type" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2">
              <option value="exact">精确</option>
              <option value="contains">包含</option>
              <option value="prefix">前缀</option>
            </select>
          </template>
          <template #cell-source="{ row }"><input v-model="row.source" class="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-reason="{ row }"><textarea v-model="row.reason" class="min-h-[42px] w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2" /></template>
          <template #cell-actions="{ row }"><button class="rounded-xl bg-[rgba(178,77,84,0.1)] px-3 py-2 text-sm font-semibold text-[var(--danger)]" @click="removeBlock(row)">移除</button></template>
        </DataTable>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchSignalQualityConfig, saveSignalQualityBlocklist, saveSignalQualityRules } from '../../services/api/system'
import { confirmDangerAction } from '../../shared/utils/confirm'

const rules = ref<Array<Record<string, any>>>([])
const blocklist = ref<Array<Record<string, any>>>([])
const generatedAt = ref('')
const saveMessage = ref('')

const ruleColumns = [
  { key: 'enabled', label: '启用' },
  { key: 'rule_key', label: '规则键' },
  { key: 'rule_value', label: '值' },
  { key: 'value_type', label: '值类型' },
  { key: 'category', label: '分类' },
  { key: 'description', label: '说明' },
  { key: 'actions', label: '操作' },
]

const blockColumns = [
  { key: 'enabled', label: '启用' },
  { key: 'term', label: '词项' },
  { key: 'target_type', label: '目标类型' },
  { key: 'match_type', label: '匹配方式' },
  { key: 'source', label: '来源' },
  { key: 'reason', label: '原因' },
  { key: 'actions', label: '操作' },
]

function normalizeRule(item: Record<string, any> = {}) {
  return {
    rule_key: String(item.rule_key || '').trim(),
    rule_value: item.rule_value == null ? '' : String(item.rule_value),
    value_type: String(item.value_type || 'number').trim() || 'number',
    category: String(item.category || '').trim(),
    description: String(item.description || '').trim(),
    enabled: !!Number(item.enabled ?? 1) || item.enabled === true,
  }
}

function normalizeBlock(item: Record<string, any> = {}) {
  return {
    id: item.id ?? `${Date.now()}-${Math.random()}`,
    term: String(item.term || '').trim(),
    target_type: String(item.target_type || 'stock').trim() || 'stock',
    match_type: String(item.match_type || 'exact').trim() || 'exact',
    source: String(item.source || 'signal_quality_admin').trim() || 'signal_quality_admin',
    reason: String(item.reason || '').trim(),
    enabled: !!Number(item.enabled ?? 1) || item.enabled === true,
  }
}

async function loadConfig() {
  const data = await fetchSignalQualityConfig()
  rules.value = (data.rules || []).map(normalizeRule)
  blocklist.value = (data.blocklist || []).map(normalizeBlock)
  generatedAt.value = data.generated_at || ''
  return data
}

useQuery({
  queryKey: ['signal-quality-config'],
  queryFn: loadConfig,
})

const saveMutation = useMutation({
  mutationFn: async () => {
    const cleanRules = rules.value.map(normalizeRule).filter((item) => item.rule_key)
    const cleanBlocks = blocklist.value.map(normalizeBlock).filter((item) => item.term)
    const [ruleResp, blockResp] = await Promise.all([
      saveSignalQualityRules(cleanRules),
      saveSignalQualityBlocklist(cleanBlocks),
    ])
    return { ruleResp, blockResp }
  },
  onSuccess: async (payload) => {
    saveMessage.value = `保存成功：规则 ${payload.ruleResp.affected || 0} 条，黑名单 ${payload.blockResp.affected || 0} 条`
    await loadConfig()
  },
  onError: (error: Error) => {
    saveMessage.value = `保存失败：${error.message}`
  },
})

const isSaving = computed(() => saveMutation.isPending.value)

function addRule() {
  rules.value.push(normalizeRule({ category: 'manual', enabled: 1 }))
}

async function removeRule(row: Record<string, any>) {
  if (!await confirmDangerAction('移除规则', row.rule_key || '未命名规则')) return
  rules.value = rules.value.filter((item) => item !== row)
}

function addBlock() {
  blocklist.value.push(normalizeBlock({ enabled: 1 }))
}

async function removeBlock(row: Record<string, any>) {
  if (!await confirmDangerAction('移除黑名单项', row.term || '未命名词项')) return
  blocklist.value = blocklist.value.filter((item) => item !== row)
}

function saveAll() {
  saveMutation.mutate()
}
</script>
