<template>
  <AppShell title="LLM 节点管理台" subtitle="统一管理模型节点地址、限速策略与联通测试。">
    <div class="space-y-4">
      <PageSection title="全局限速" subtitle="未配置单节点限速时默认使用该阈值（每分钟）。">
        <div class="flex flex-wrap items-center gap-3">
          <input v-model.number="defaultRateLimit" type="number" min="1" class="w-40 rounded-2xl border border-[var(--line)] bg-white px-4 py-3" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white" :disabled="saveDefaultLimitPending" @click="saveDefaultLimit">
            {{ saveDefaultLimitPending ? '保存中...' : '保存默认限速' }}
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" @click="refreshAll">刷新</button>
        </div>
      </PageSection>

      <PageSection title="新增节点" subtitle="推荐优先使用 api_key_env，避免在配置文件存明文 key。">
        <div class="grid gap-3 xl:grid-cols-3 md:grid-cols-2">
          <input v-model="createForm.provider_key" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="provider_key" />
          <input v-model="createForm.model" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="model" />
          <input v-model="createForm.base_url" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="base_url" />
          <input v-model="createForm.api_key" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="api_key（可留空）" />
          <input v-model="createForm.api_key_env" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="api_key_env（推荐）" />
          <select v-model="createForm.status" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="active">active</option>
            <option value="disabled">disabled</option>
          </select>
          <div class="flex items-center gap-3 rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <label class="text-sm">限速</label>
            <input v-model="createForm.rate_limit_enabled" type="checkbox" />
            <input v-model.number="createForm.rate_limit_per_minute" type="number" min="1" class="w-24 rounded-xl border border-[var(--line)] px-2 py-1" />
          </div>
        </div>
        <div class="mt-3">
          <button class="rounded-2xl bg-stone-800 px-4 py-3 text-white" :disabled="createPending" @click="createNode">
            {{ createPending ? '新增中...' : '新增节点' }}
          </button>
        </div>
      </PageSection>

      <PageSection :title="`节点列表 (${rows.length})`" subtitle="支持单节点测试与当前模型一键测试。">
        <div
          v-if="authHint.visible"
          class="mb-3 rounded-2xl border px-4 py-3 text-sm"
          :class="authHint.kind === 'forbidden' ? 'border-amber-200 bg-amber-50 text-amber-800' : 'border-blue-200 bg-blue-50 text-blue-800'"
        >
          <div class="font-semibold">{{ authHint.title }}</div>
          <div class="mt-1">{{ authHint.message }}</div>
          <div class="mt-3 flex flex-wrap gap-2">
            <button class="rounded-xl border border-[var(--line)] bg-white px-3 py-2" @click="refreshAll">重试加载</button>
            <button v-if="authHint.kind === 'unauthorized'" class="rounded-xl border border-blue-200 bg-blue-100 px-3 py-2 text-blue-800" @click="goLogin">
              去登录
            </button>
          </div>
        </div>
        <div v-if="loadError" class="mb-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ loadError }}
        </div>

        <div class="mb-3 flex flex-wrap items-center gap-3">
          <select v-model="selectedModelForBatch" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option value="">选择模型做一键测试</option>
            <option v-for="m in modelKeys" :key="m" :value="m">{{ m }}</option>
          </select>
          <button class="rounded-2xl bg-blue-700 px-4 py-3 text-white disabled:opacity-50" :disabled="!selectedModelForBatch || batchTestPending" @click="runBatchTest">
            {{ batchTestPending ? '测试中...' : '一键测试当前模型全部节点' }}
          </button>
        </div>

        <div v-if="currentPreview" class="mb-3 rounded-2xl border border-[var(--line)] bg-[rgba(255,255,255,0.85)] p-4">
          <div class="text-sm font-semibold">当前节点预览</div>
          <div class="mt-2 text-sm text-[var(--muted)]">
            {{ currentPreview.provider_key }} #{{ currentPreview.index }} · {{ currentPreview.model }} · {{ currentPreview.base_url }}
          </div>
          <div class="mt-2 text-sm text-[var(--muted)]">
            状态：{{ currentPreview.status }} · 运行态：{{ currentPreview.runtime_status || 'ok' }} · 限速：{{ currentPreview.rate_limit_enabled ? `${currentPreview.count_current_minute || 0}/${currentPreview.rate_limit_per_minute}` : 'disabled' }}
          </div>
          <div class="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm disabled:opacity-60"
              :disabled="!currentPreview || singleTestPending"
              @click.stop="testOne(currentPreview)"
            >
              {{ singleTestPending && singleTestTargetKey === previewKey ? '测试中...' : '单独测试' }}
            </button>
            <button
              type="button"
              class="rounded-xl px-3 py-2 text-sm"
              :class="currentPreview.status === 'active' ? 'border border-amber-200 bg-amber-50 text-amber-700' : 'border border-emerald-200 bg-emerald-50 text-emerald-700'"
              @click.stop="toggleNodeStatus(currentPreview)"
            >
              {{ currentPreview.status === 'active' ? '关闭节点' : '开启节点' }}
            </button>
          </div>
        </div>

        <div class="space-y-2">
          <InfoCard
            v-for="item in rows"
            :key="`${item.provider_key}-${item.index}`"
            :title="`${item.provider_key} #${item.index}`"
            :meta="`${item.model} · ${item.base_url}`"
            :description="buildDescription(item)"
          >
            <template #badge>
              <StatusBadge :value="item.runtime_status === 'rate_limited' ? 'warn' : (item.status === 'active' ? 'success' : 'muted')" :label="item.runtime_status || item.status" />
            </template>
            <template #default>
              <div class="mt-3 flex flex-wrap gap-2">
                <button type="button" class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm" @click.stop="setCurrentPreview(item)">设为当前预览</button>
                <button type="button" class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm" @click.stop="openEdit(item)">编辑</button>
                <button type="button" class="rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700" @click.stop="removeNode(item)">删除</button>
                <button
                  type="button"
                  class="rounded-xl border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-700 disabled:opacity-60"
                  :disabled="singleTestPending"
                  @click.stop="testOne(item)"
                >
                  {{ singleTestPending && singleTestTargetKey === `${item.provider_key}-${item.index}` ? '测试中...' : '单独测试' }}
                </button>
                <button
                  type="button"
                  class="rounded-xl px-3 py-2 text-sm"
                  :class="item.status === 'active' ? 'border border-amber-200 bg-amber-50 text-amber-700' : 'border border-emerald-200 bg-emerald-50 text-emerald-700'"
                  @click.stop="toggleNodeStatus(item)"
                >
                  {{ item.status === 'active' ? '关闭节点' : '开启节点' }}
                </button>
              </div>
            </template>
          </InfoCard>
        </div>
      </PageSection>

      <div
        v-if="editModal.visible"
        class="fixed inset-0 z-[1200] flex items-center justify-center bg-[rgba(15,23,42,0.45)] p-4"
        @click.self="closeEdit"
      >
        <div class="w-full max-w-5xl rounded-[28px] border border-[var(--line)] bg-[rgba(255,255,255,0.98)] p-5 shadow-2xl">
          <div class="mb-4 flex items-center justify-between gap-3">
            <div>
              <div class="text-xl font-extrabold tracking-tight" style="font-family: var(--font-display)">编辑节点</div>
              <div class="mt-1 text-sm text-[var(--muted)]">不填 api_key 将保留原值。</div>
            </div>
            <button type="button" class="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm" @click="closeEdit">关闭</button>
          </div>
          <div class="grid gap-3 xl:grid-cols-3 md:grid-cols-2">
            <input v-model="editModal.form.model" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="model" />
            <input v-model="editModal.form.base_url" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="base_url" />
            <input v-model="editModal.form.api_key" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="新 api_key（留空=保持原值）" />
            <input v-model="editModal.form.api_key_env" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="api_key_env（可留空）" />
            <select v-model="editModal.form.status" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <option value="active">active</option>
              <option value="disabled">disabled</option>
            </select>
            <div class="flex items-center gap-3 rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
              <label class="text-sm">限速</label>
              <input v-model="editModal.form.rate_limit_enabled" type="checkbox" />
              <input v-model.number="editModal.form.rate_limit_per_minute" type="number" min="1" class="w-24 rounded-xl border border-[var(--line)] px-2 py-1" />
            </div>
          </div>
          <div class="mt-3 rounded-2xl border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-800">
            说明：`api_key` 留空时不会覆盖现有 key；只有填写了新值才会更新。
          </div>
          <div class="mt-4 flex gap-2">
            <button type="button" class="rounded-2xl bg-stone-800 px-4 py-3 text-white" :disabled="editPending" @click="saveEdit">{{ editPending ? '保存中...' : '保存修改' }}</button>
            <button type="button" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" @click="closeEdit">取消</button>
          </div>
        </div>
      </div>

      <PageSection v-if="lastTestText" title="测试结果" subtitle="单节点或一键测试返回。">
        <pre class="overflow-x-auto rounded-2xl border border-[var(--line)] bg-[rgba(255,255,255,0.78)] p-4 text-xs">{{ lastTestText }}</pre>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import {
  createLlmProvider,
  deleteLlmProvider,
  fetchLlmProviders,
  testModelLlmProviders,
  testOneLlmProvider,
  updateDefaultLlmRateLimit,
  updateLlmProvider,
  type LlmProviderItem,
} from '../../services/api/system'
import { confirmDangerAction } from '../../shared/utils/confirm'

const rows = ref<LlmProviderItem[]>([])
const defaultRateLimit = ref(10)
const selectedModelForBatch = ref('')
const currentPreview = ref<LlmProviderItem | null>(null)
const lastTestText = ref('')
const loadError = ref('')
const authHint = reactive({
  visible: false,
  kind: '' as 'unauthorized' | 'forbidden' | '',
  title: '',
  message: '',
})
const router = useRouter()

const createForm = reactive({
  provider_key: 'gpt-5.4',
  model: 'gpt-5.4',
  base_url: '',
  api_key: '',
  api_key_env: '',
  status: 'active',
  rate_limit_enabled: true,
  rate_limit_per_minute: 10,
})

const editModal = reactive({
  visible: false,
  provider_key: '',
  index: 0,
  form: {
    model: '',
    base_url: '',
    api_key: '',
    api_key_env: '',
    status: 'active',
    rate_limit_enabled: true,
    rate_limit_per_minute: 10,
  },
})

const createPending = ref(false)
const editPending = ref(false)
const batchTestPending = ref(false)
const saveDefaultLimitPending = ref(false)
const singleTestPending = ref(false)
const singleTestTargetKey = ref('')

const modelKeys = computed(() => Array.from(new Set(rows.value.map((item) => String(item.model || '').trim()).filter(Boolean))).sort())
const previewKey = computed(() =>
  currentPreview.value ? `${currentPreview.value.provider_key}-${currentPreview.value.index}` : '',
)

function buildDescription(item: LlmProviderItem): string {
  const obs = item.observability_7d || {}
  return [
    `status=${item.status}`,
    `runtime=${item.runtime_status || 'ok'}`,
    `rate=${item.rate_limit_enabled ? `${item.count_current_minute || 0}/${item.rate_limit_per_minute}` : 'disabled'}`,
    `api_key=${item.api_key_masked || '-'}`,
    item.api_key_source ? `key_source=${item.api_key_source}` : '',
    item.health_recommendation ? `health=${item.health_recommendation}` : '',
    `7d_sr=${Number(obs.success_rate_pct || 0).toFixed(1)}%`,
    `7d_p95=${obs.p95_latency_ms ?? 0}ms`,
    `7d_429=${obs.http_429 ?? 0}`,
    `7d_rl=${obs.rate_limited ?? 0}`,
    `7d_switch=${obs.switch_count ?? 0}`,
    `probe=${item.last_http_status ?? '-'} / ${item.last_latency_ms ?? '-'}ms`,
    item.last_error ? `err=${item.last_error}` : '',
  ]
    .filter(Boolean)
    .join(' · ')
}

async function refreshAll() {
  try {
    loadError.value = ''
    authHint.visible = false
    const data = await fetchLlmProviders()
    rows.value = Array.isArray(data.items) ? data.items : []
    defaultRateLimit.value = Number(data.default_rate_limit_per_minute || 10)
    if (!currentPreview.value && rows.value.length) {
      currentPreview.value = rows.value[0]
    } else if (currentPreview.value) {
      const found = rows.value.find(
        (item) =>
          item.provider_key === currentPreview.value?.provider_key &&
          item.index === currentPreview.value?.index,
      )
      currentPreview.value = found || (rows.value[0] || null)
    }
  } catch (error: any) {
    const status = Number(error?.status || error?.response?.status || 0)
    const detail = String(error?.response?.data?.error || error?.message || 'unknown')
    rows.value = []
    currentPreview.value = null
    loadError.value = `节点加载失败（status=${status || '-'}）：${detail}`
    if (status === 401) {
      authHint.visible = true
      authHint.kind = 'unauthorized'
      authHint.title = '需要先登录'
      authHint.message = '当前会话未登录或已失效，请先登录后再访问 LLM 节点管理。'
    } else if (status === 403) {
      authHint.visible = true
      authHint.kind = 'forbidden'
      authHint.title = '权限不足'
      authHint.message = '当前账号缺少 admin_system 权限，请联系管理员开通后重试。'
    }
  }
}

async function createNode() {
  createPending.value = true
  try {
    await createLlmProvider({ ...createForm })
    await refreshAll()
  } finally {
    createPending.value = false
  }
}

function openEdit(item: LlmProviderItem) {
  editModal.visible = true
  editModal.provider_key = item.provider_key
  editModal.index = item.index
  editModal.form.model = item.model
  editModal.form.base_url = item.base_url
  editModal.form.api_key = ''
  editModal.form.api_key_env = item.api_key_env || ''
  editModal.form.status = item.status
  editModal.form.rate_limit_enabled = !!item.rate_limit_enabled
  editModal.form.rate_limit_per_minute = Number(item.rate_limit_per_minute || defaultRateLimit.value)
}

function closeEdit() {
  editModal.visible = false
}

async function saveEdit() {
  editPending.value = true
  try {
    await updateLlmProvider({
      provider_key: editModal.provider_key,
      index: editModal.index,
      model: editModal.form.model,
      base_url: editModal.form.base_url,
      api_key: editModal.form.api_key,
      api_key_env: editModal.form.api_key_env,
      status: editModal.form.status,
      rate_limit_enabled: editModal.form.rate_limit_enabled,
      rate_limit_per_minute: editModal.form.rate_limit_per_minute,
    })
    await refreshAll()
    closeEdit()
  } finally {
    editPending.value = false
  }
}

async function removeNode(item: LlmProviderItem) {
  if (!await confirmDangerAction('删除 LLM 节点', `${item.provider_key}#${item.index}`, '删除后需要重新配置节点信息。')) return
  await deleteLlmProvider({ provider_key: item.provider_key, index: item.index })
  await refreshAll()
}

async function testOne(item: LlmProviderItem) {
  if (!item) return
  singleTestPending.value = true
  singleTestTargetKey.value = `${item.provider_key}-${item.index}`
  try {
    const data = await testOneLlmProvider({ provider_key: item.provider_key, index: item.index })
    lastTestText.value = JSON.stringify(data, null, 2)
    await refreshAll()
  } catch (error: any) {
    const status = Number(error?.status || error?.response?.status || 0)
    const detail = String(error?.response?.data?.error || error?.message || 'unknown')
    lastTestText.value = JSON.stringify(
      {
        ok: false,
        action: 'test_one_llm_provider',
        provider_key: item.provider_key,
        index: item.index,
        status: status || null,
        error: detail,
      },
      null,
      2,
    )
  } finally {
    singleTestPending.value = false
    singleTestTargetKey.value = ''
  }
}

async function runBatchTest() {
  if (!selectedModelForBatch.value) return
  batchTestPending.value = true
  try {
    const data = await testModelLlmProviders({ model: selectedModelForBatch.value })
    lastTestText.value = JSON.stringify(data, null, 2)
  } finally {
    batchTestPending.value = false
  }
}

function setCurrentPreview(item: LlmProviderItem) {
  currentPreview.value = item
}

async function toggleNodeStatus(item: LlmProviderItem) {
  const action = item.status === 'active' ? '关闭节点' : '开启节点'
  if (!await confirmDangerAction(action, `${item.provider_key}#${item.index}`)) return
  await updateLlmProvider({
    provider_key: item.provider_key,
    index: item.index,
    status: item.status === 'active' ? 'disabled' : 'active',
  })
  await refreshAll()
}

async function saveDefaultLimit() {
  saveDefaultLimitPending.value = true
  try {
    await updateDefaultLlmRateLimit(defaultRateLimit.value)
    await refreshAll()
  } finally {
    saveDefaultLimitPending.value = false
  }
}

function goLogin() {
  router.push('/login')
}

refreshAll()
</script>
