<template>
  <AppShell title="数据源监控" subtitle="把数据源、进程、实时链路、任务编排和最近错误统一收口。">
    <div class="space-y-4">
      <PageSection title="监控总览" subtitle="这是运维、研发、研究三方都该看的统一监控页。">
        <StatePanel
          v-if="monitorError"
          class="mb-4"
          tone="danger"
          title="监控数据加载失败"
          :description="monitorError"
        >
          <template #action>
            <button class="rounded-2xl bg-stone-900 px-4 py-2 font-semibold text-white" @click="reload">重新加载</button>
          </template>
        </StatePanel>
        <StatePanel
          v-else-if="suspiciousSummary"
          class="mb-4"
          tone="warning"
          title="监控总览存在可疑汇总"
          :description="suspiciousSummary"
        />
        <div class="mb-3 text-sm text-[var(--muted)]">
          最后刷新 {{ formatDateTime(summaryNow) }} · 数据源 {{ monitor ? '接口成功' : '暂无数据' }}
        </div>
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
          <StatCard title="数据源正常" :value="effectiveSummary.source_ok" :hint="`总数 ${effectiveSummary.source_total}`" />
          <StatCard title="数据源延迟" :value="effectiveSummary.source_warn" hint="需关注但未完全失效" />
          <StatCard title="数据源异常" :value="effectiveSummary.source_error" hint="优先排查" />
          <StatCard title="进程正常" :value="effectiveSummary.process_ok" hint="守护进程、Worker、服务进程" />
          <StatCard title="进程告警" :value="effectiveSummary.process_warn" hint="日志旧或心跳异常" />
          <StatCard title="进程异常" :value="effectiveSummary.process_error" hint="服务离线" />
        </div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-2">
        <PageSection title="数据源状态" subtitle="按数据源粒度看状态、最近更新时间和说明。">
          <div class="space-y-2">
            <InfoCard v-for="item in monitor?.sources || []" :key="item.key" :title="item.name || item.key" :meta="`${item.detail || '-'} · 最近更新 ${formatDateTime(item.last_update)}`">
              <template #badge><StatusBadge :value="item.status" :label="item.status_text || item.status || '-'" /></template>
            </InfoCard>
          </div>
        </PageSection>
        <PageSection title="进程 / Worker / WS 链路" subtitle="把关键后台服务的活性集中展示。">
          <div class="space-y-2">
            <InfoCard v-for="item in monitor?.processes || []" :key="item.key" :title="item.name || item.key" :meta="`${item.detail || '-'} · 最近更新 ${formatDateTime(item.last_update)}`">
              <template #badge><StatusBadge :value="item.status" :label="item.status_text || item.status || '-'" /></template>
            </InfoCard>
          </div>
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-[0.8fr_1.2fr]">
        <PageSection title="任务编排总览" subtitle="用 job_orchestrator 视角看定时任务，而不是只看脚本进程。">
          <div class="grid gap-3 xl:grid-cols-2">
            <StatCard title="任务定义" :value="monitor?.orchestrator?.summary?.definitions_total ?? 0" hint="当前已注册任务数" />
            <StatCard title="最近运行" :value="monitor?.orchestrator?.summary?.recent_total ?? 0" hint="最近抓到的任务运行条数" />
            <StatCard title="成功" :value="monitor?.orchestrator?.summary?.success ?? 0" hint="最近成功任务" />
            <StatCard title="失败" :value="monitor?.orchestrator?.summary?.failed ?? 0" hint="最近失败任务" />
          </div>
        </PageSection>
        <PageSection title="最近任务运行" subtitle="直接看最近任务状态，异常任务可一键跳到总控台继续排查。">
          <div class="space-y-2">
            <InfoCard
              v-for="item in monitor?.orchestrator?.recent_runs || []"
              :key="item.id"
              :title="item.job_key || '-'" :meta="`状态 ${item.status || '-'} · 开始 ${formatDateTime(item.started_at)} · 耗时 ${item.duration_seconds ?? '-'} 秒`"
              :description="item.stderr_tail || item.stdout_tail || ''"
            >
              <template #badge><StatusBadge :value="item.status" :label="item.status || '-'" /></template>
            </InfoCard>
          </div>
        </PageSection>
      </div>

      <PageSection title="QuantaAlpha 任务队列健康" subtitle="Worker 在线状态、待处理/运行中队列深度与近期错误集中展示，无需跳转即可完成巡检。">
        <div class="grid gap-3 xl:grid-cols-5 md:grid-cols-3">
          <StatCard title="Worker 状态" :value="quantAlive ? '在线' : '离线'" :hint="quantAlive ? `心跳 ${quantHbAge}s 前` : '心跳中断'" />
          <StatCard title="待处理 (pending)" :value="quantPending" hint="等待执行的任务数" />
          <StatCard title="运行中 (running)" :value="quantRunning" hint="当前正在执行的任务数" />
          <StatCard title="近期错误类型" :value="quantErrorTypes" hint="最近 N 条任务的错误码种类" />
          <StatCard title="执行模式" :value="quantMode" hint="worker 当前执行模式" />
        </div>
        <!-- Anomaly guidance: shown when worker is offline or errors present -->
        <div v-if="quantAnomalyHints.length" class="mt-3 rounded-[18px] border border-amber-200 bg-amber-50/60 px-4 py-3 space-y-1">
          <div class="text-sm font-semibold text-amber-800">建议下一步操作</div>
          <div v-for="(hint, i) in quantAnomalyHints" :key="i" class="text-sm text-amber-700">• {{ hint }}</div>
        </div>
        <div v-if="quantRecentErrors.length" class="mt-3 space-y-1">
          <div class="text-sm font-semibold text-[var(--ink)]">近期错误明细</div>
          <div v-for="(e, i) in quantRecentErrors" :key="i" class="rounded-[14px] border border-[var(--line)] bg-white px-4 py-2 text-sm text-[var(--ink)]">
            {{ e.error_code || e.type || '-' }}: {{ e.message || e.detail || '-' }}
          </div>
        </div>
        <div v-else class="mt-3 text-sm text-[var(--muted)]">近期无错误记录</div>
      </PageSection>

      <PageSection title="最近错误日志" subtitle="优先看 tail，迅速判断是数据延迟、进程退出还是接口报错。">
        <div class="grid gap-4 xl:grid-cols-2">
          <div
            v-for="item in monitor?.logs || []"
            :key="item.path"
            class="rounded-[24px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] p-4 shadow-[var(--shadow-soft)]"
          >
            <div class="flex items-start justify-between gap-3">
              <div>
                <div class="text-base font-bold text-[var(--ink)]">{{ item.name }}</div>
                <div class="mt-1 text-sm text-[var(--muted)]">{{ item.path }} · {{ formatDateTime(item.last_update) }}</div>
              </div>
              <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]" @click="copyLog(item.tail || '')">复制 tail</button>
            </div>
            <pre class="mt-3 max-h-64 overflow-auto rounded-[18px] bg-[#0f1720] p-3 text-xs leading-6 text-[#d8edf7]">{{ item.tail || '暂无日志' }}</pre>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import StatePanel from '../../shared/ui/StatePanel.vue'
import { fetchSourceMonitor } from '../../services/api/dashboard'
import { fetchQuantHealth } from '../../services/api/quantFactors'
import { formatDateTime } from '../../shared/utils/format'

const { data: monitor, error, refetch } = useQuery({
  queryKey: ['source-monitor'],
  queryFn: fetchSourceMonitor,
  refetchInterval: () => (document.visibilityState === 'visible' ? 60_000 : 180_000),
})

const { data: quantHealth } = useQuery({
  queryKey: ['quant-health-monitor'],
  queryFn: fetchQuantHealth,
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
  staleTime: 15_000,
})

const quantAlive = computed(() => Boolean((quantHealth.value?.worker || quantHealth.value?.data?.worker)?.alive))
const quantHbAge = computed(() => (quantHealth.value?.worker || quantHealth.value?.data?.worker)?.heartbeat_age_seconds ?? '-')
const quantPending = computed(() => (quantHealth.value?.queue || quantHealth.value?.data?.queue)?.pending ?? 0)
const quantRunning = computed(() => (quantHealth.value?.queue || quantHealth.value?.data?.queue)?.running ?? 0)
const quantMode = computed(() => (quantHealth.value?.worker || quantHealth.value?.data?.worker)?.execution_mode || '-')
const quantRecentErrors = computed(() => quantHealth.value?.recent_errors || quantHealth.value?.data?.recent_errors || [])
const quantErrorTypes = computed(() => {
  const errs = quantRecentErrors.value
  if (!errs.length) return '无'
  const types = new Set(errs.map((e: Record<string, any>) => e.error_code || e.type || ''))
  return types.size
})

const quantAnomalyHints = computed((): string[] => {
  const hints: string[] = []
  if (!quantAlive.value) {
    hints.push('Worker 离线：检查 /tmp/quantaalpha_worker.log，确认 jobs/run_quantaalpha_worker.py 进程在运行。')
    hints.push('可在任务调度中心（左侧导航 → 任务调度中心）查看 quantaalpha_mine_daily 任务最近状态。')
  }
  if (Number(quantPending.value) > 5) {
    hints.push(`待处理任务积压（${quantPending.value} 条）：请确认 worker 进程正常消费，或检查 Redis 连接是否正常。`)
  }
  if (quantRecentErrors.value.length > 0) {
    hints.push('有近期错误记录：优先查看错误明细，确认是 EXECUTION_EXCEPTION（任务逻辑）还是 THREAD_LOST（进程崩溃）。')
    hints.push('若错误为 EXTERNAL_TIMEOUT，可检查 LLM 节点管理页（左侧导航 → LLM 节点管理）的节点可用性。')
  }
  return hints
})

const effectiveSummary = computed(() => {
  const sourceItems = monitor.value?.sources || []
  const processItems = monitor.value?.processes || []
  const summary = monitor.value?.summary || {}
  const sourceTotal = Number(summary.source_total ?? sourceItems.length ?? 0)
  const sourceOk = Number(summary.source_ok ?? 0)
  const sourceWarn = Number(summary.source_warn ?? 0)
  const sourceError = Number(summary.source_error ?? 0)
  const processOk = Number(summary.process_ok ?? 0)
  const processWarn = Number(summary.process_warn ?? 0)
  const processError = Number(summary.process_error ?? 0)
  const summaryLooksEmpty =
    sourceTotal > 0 &&
    sourceOk === 0 &&
    sourceWarn === 0 &&
    sourceError === 0 &&
    processOk === 0 &&
    processWarn === 0 &&
    processError === 0
  if (!summaryLooksEmpty) {
    return {
      source_total: sourceTotal,
      source_ok: sourceOk,
      source_warn: sourceWarn,
      source_error: sourceError,
      process_ok: processOk,
      process_warn: processWarn,
      process_error: processError,
    }
  }
  return {
    source_total: sourceItems.length,
    source_ok: sourceItems.filter((item: Record<string, any>) => item.status === 'ok').length,
    source_warn: sourceItems.filter((item: Record<string, any>) => item.status === 'warn').length,
    source_error: sourceItems.filter((item: Record<string, any>) => item.status === 'error').length,
    process_ok: processItems.filter((item: Record<string, any>) => item.status === 'ok').length,
    process_warn: processItems.filter((item: Record<string, any>) => item.status === 'warn').length,
    process_error: processItems.filter((item: Record<string, any>) => item.status === 'error').length,
  }
})

const suspiciousSummary = computed(() => {
  if (!monitor.value) return ''
  const summary = monitor.value.summary || {}
  const hasDetails = Boolean((monitor.value.sources || []).length || (monitor.value.processes || []).length || (monitor.value.orchestrator?.recent_runs || []).length)
  const allZero =
    Number(summary.source_ok ?? 0) === 0 &&
    Number(summary.source_warn ?? 0) === 0 &&
    Number(summary.source_error ?? 0) === 0 &&
    Number(summary.process_ok ?? 0) === 0 &&
    Number(summary.process_warn ?? 0) === 0 &&
    Number(summary.process_error ?? 0) === 0
  if (!hasDetails || !allZero) return ''
  return '接口返回了明细，但汇总区全为 0；页面已按明细重新聚合，建议继续核对缓存或接口 summary 字段。'
})

const summaryNow = computed(() => monitor.value?.summary?.now || '')
const monitorError = computed(() => error.value?.message || '')

function reload() {
  refetch()
}

async function copyLog(text: string) {
  try {
    await navigator.clipboard.writeText(text || '')
  } catch {
    // ignore clipboard errors
  }
}
</script>
