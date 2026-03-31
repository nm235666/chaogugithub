<template>
  <AppShell title="数据源监控" subtitle="把数据源、进程、实时链路、任务编排和最近错误统一收口。">
    <div class="space-y-4">
      <PageSection title="监控总览" subtitle="这是运维、研发、研究三方都该看的统一监控页。">
        <div class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
          <StatCard title="数据源正常" :value="monitor?.summary?.source_ok ?? 0" :hint="`总数 ${monitor?.summary?.source_total ?? 0}`" />
          <StatCard title="数据源延迟" :value="monitor?.summary?.source_warn ?? 0" hint="需关注但未完全失效" />
          <StatCard title="数据源异常" :value="monitor?.summary?.source_error ?? 0" hint="优先排查" />
          <StatCard title="进程正常" :value="monitor?.summary?.process_ok ?? 0" hint="守护进程、Worker、服务进程" />
          <StatCard title="进程告警" :value="monitor?.summary?.process_warn ?? 0" hint="日志旧或心跳异常" />
          <StatCard title="进程异常" :value="monitor?.summary?.process_error ?? 0" hint="服务离线" />
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
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import { fetchSourceMonitor } from '../../services/api/dashboard'
import { formatDateTime } from '../../shared/utils/format'

const { data: monitor } = useQuery({ queryKey: ['source-monitor'], queryFn: fetchSourceMonitor, refetchInterval: 60_000 })

async function copyLog(text: string) {
  try {
    await navigator.clipboard.writeText(text || '')
  } catch {
    // ignore clipboard errors
  }
}
</script>
