<template>
  <AppShell title="总控台" subtitle="把系统健康、研究优先级、任务编排与重点情报压进一个统一研究入口。">
    <div class="space-y-4">
      <PageSection title="核心状态" subtitle="优先判断今天系统是不是足够新、足够稳，再决定去哪个模块深入。">
        <div v-if="dashboard" class="grid gap-3 xl:grid-cols-6 md:grid-cols-3">
          <StatCard title="上市股票" :value="dashboard.overview?.listed_total ?? 0" :hint="`总股票 ${dashboard.overview?.stock_total ?? 0}`" />
          <StatCard title="国际/国内新闻" :value="dashboard.overview?.news_total ?? 0" :hint="`个股新闻 ${dashboard.overview?.stock_news_total ?? 0}`" />
          <StatCard title="群聊记录" :value="dashboard.overview?.chatlog_total ?? 0" :hint="`群聊 ${dashboard.overview?.chatroom_total ?? 0}`" />
          <StatCard title="监控群聊" :value="dashboard.overview?.monitored_chatroom_total ?? 0" hint="纳入实时拉取与跨天补抓" />
          <StatCard title="候选池标的" :value="dashboard.overview?.candidate_total ?? 0" hint="来自群聊与信号聚合" />
          <StatCard title="日报总结" :value="dashboard.overview?.daily_summary_total ?? 0" :hint="`刷新于 ${formatDateTime(dashboard.generated_at)}`" />
        </div>
      </PageSection>

      <div class="grid gap-4 2xl:grid-cols-[1.3fr_0.7fr]">
        <PageSection title="研究优先队列" subtitle="今天最值得先看的股票、候选池和高重要新闻。">
          <div class="grid gap-4 xl:grid-cols-3">
            <div>
              <div class="mb-2 text-sm font-bold">评分领先股票</div>
              <div class="space-y-2">
                <InfoCard v-for="item in dashboard?.top_scores || []" :key="item.ts_code" :title="item.name || item.ts_code" :meta="`${item.ts_code || '-'} · ${item.industry || '-'} · ${item.market || '-'}`">
                  <template #badge>
                    <StatusBadge value="brand" :label="`总分 ${Number(item.industry_total_score ?? item.total_score ?? 0).toFixed(1)}`" />
                  </template>
                </InfoCard>
              </div>
            </div>
            <div>
              <div class="mb-2 text-sm font-bold">群聊候选池</div>
              <div class="space-y-2">
                <InfoCard v-for="item in dashboard?.candidate_pool_top || []" :key="item.candidate_name" :title="item.candidate_name || '-'" :meta="`${item.candidate_type || '-'} · 净分 ${item.net_score ?? 0} · 群数 ${item.room_count ?? 0}`">
                  <template #badge>
                    <StatusBadge :value="item.dominant_bias" :label="item.dominant_bias || '-'" />
                  </template>
                </InfoCard>
              </div>
            </div>
            <div>
              <div class="mb-2 text-sm font-bold">高重要新闻</div>
              <div class="space-y-2">
                <InfoCard v-for="item in dashboard?.important_news || []" :key="item.id" :title="item.title || '-'" :meta="`${item.source || '-'} · ${formatDateTime(item.pub_date)}`">
                  <template #badge>
                    <StatusBadge :value="item.llm_finance_importance || 'muted'" :label="item.llm_finance_importance || '未评级'" />
                  </template>
                </InfoCard>
              </div>
            </div>
          </div>
        </PageSection>

        <PageSection title="任务编排回放" subtitle="直接看最近任务状态，而不是只看抽象计数。">
          <div v-if="(dashboard?.orchestrator_alerts || []).length" class="mb-3 space-y-2">
            <InfoCard
              v-for="item in dashboard?.orchestrator_alerts || []"
              :key="`alert-${item.id}`"
              :title="item.message || item.job_key || '-'"
              :meta="`任务 ${item.job_key || '-'} · run_id ${item.run_id ?? '-'} · ${formatDateTime(item.created_at)}`"
            >
              <template #badge>
                <StatusBadge :value="item.severity || 'error'" :label="item.severity || 'error'" />
              </template>
            </InfoCard>
          </div>
          <div class="space-y-2">
            <InfoCard
              v-for="item in dashboard?.orchestrator_jobs || []"
              :key="item.id"
              :title="item.job_key || '-'"
              :meta="`开始 ${formatDateTime(item.started_at)} · 耗时 ${item.duration_seconds ?? '-'} 秒 · 退出码 ${item.exit_code ?? '-'}`"
            >
              <template #badge>
                <StatusBadge :value="item.status" :label="item.status || '-'" />
              </template>
            </InfoCard>
          </div>
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-2">
        <PageSection title="系统状态板" subtitle="数据库新鲜度、缺口、未评分与重复风险。">
          <div v-if="dashboard?.database_health" class="grid gap-3 xl:grid-cols-3 md:grid-cols-2">
            <StatCard title="日线最新" :value="formatDate(dashboard.database_health.daily_latest)" :hint="`分钟线 ${formatDate(dashboard.database_health.minline_latest)} · 评分 ${formatDate(dashboard.database_health.scores_latest)}`" />
            <StatCard title="事件/治理缺口" :value="`${dashboard.database_health.miss_events ?? '-'} / ${dashboard.database_health.miss_governance ?? '-'}`" :hint="`资金流 ${dashboard.database_health.miss_flow ?? '-'} · 分钟线 ${dashboard.database_health.miss_minline ?? '-'}`" />
            <StatCard title="新闻未评分" :value="dashboard.database_health.news_unscored ?? 0" :hint="`个股新闻 ${dashboard.database_health.stock_news_unscored ?? 0}`" />
            <StatCard title="新闻重复组" :value="dashboard.database_health.news_dup_link ?? 0" :hint="`个股新闻 ${dashboard.database_health.stock_news_dup_link ?? 0}`" />
            <StatCard title="宏观发布日期缺失" :value="dashboard.database_health.macro_publish_empty ?? 0" hint="越接近 0 越健康" />
            <StatCard title="群聊去重异常" :value="dashboard.database_health.chatlog_dup_key ?? 0" hint="理论上应尽量接近 0" />
          </div>
        </PageSection>

        <PageSection title="实时链路与数据源" subtitle="看关键数据源、实时链路、评分与日报系统是否在线。">
          <div class="space-y-2">
            <InfoCard
              v-for="item in dashboard?.source_monitor?.sources || []"
              :key="item.key"
              :title="item.name || item.key"
              :meta="`${item.detail || '-'} · 最近更新 ${formatDateTime(item.last_update)}`"
            >
              <template #badge>
                <StatusBadge :value="item.status" :label="item.status_text || item.status || '-'" />
              </template>
            </InfoCard>
          </div>
        </PageSection>
      </div>

      <div class="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <PageSection title="调度编排专区" subtitle="从首页直接看到最近任务和调度入口。">
          <div class="grid gap-3 md:grid-cols-2">
            <RouterLink to="/system/source-monitor" class="rounded-[20px] border border-[var(--line)] bg-white px-4 py-4 shadow-[var(--shadow-soft)]">
              <div class="font-semibold text-[var(--ink)]">数据源 / Worker / WS</div>
              <div class="mt-1 text-sm text-[var(--muted)]">排查实时链路、日志尾部和进程状态。</div>
            </RouterLink>
            <RouterLink to="/system/database-audit" class="rounded-[20px] border border-[var(--line)] bg-white px-4 py-4 shadow-[var(--shadow-soft)]">
              <div class="font-semibold text-[var(--ink)]">数据库审计</div>
              <div class="mt-1 text-sm text-[var(--muted)]">查看缺口、重复、未评分与陈旧数据。</div>
            </RouterLink>
            <RouterLink to="/intelligence/stock-news" class="rounded-[20px] border border-[var(--line)] bg-white px-4 py-4 shadow-[var(--shadow-soft)]">
              <div class="font-semibold text-[var(--ink)]">个股新闻积压</div>
              <div class="mt-1 text-sm text-[var(--muted)]">直接处理未评分个股新闻与采集动作。</div>
            </RouterLink>
            <RouterLink to="/intelligence/daily-summaries" class="rounded-[20px] border border-[var(--line)] bg-white px-4 py-4 shadow-[var(--shadow-soft)]">
              <div class="font-semibold text-[var(--ink)]">日报总结工作台</div>
              <div class="mt-1 text-sm text-[var(--muted)]">主动生成日报、查看模型链路与导出。</div>
            </RouterLink>
          </div>
        </PageSection>

        <PageSection title="页面新鲜度与直达入口" subtitle="优先打开最可能有问题、最需要人工介入的页面。">
          <div class="grid gap-3 md:grid-cols-2">
            <InfoCard title="新闻未评分" :meta="`国际/国内 ${dashboard?.database_health?.news_unscored ?? 0} · 个股 ${dashboard?.database_health?.stock_news_unscored ?? 0}`" description="如果这里持续偏大，优先去新闻页和个股新闻页处理。">
              <template #badge><StatusBadge :value="Number(dashboard?.database_health?.stock_news_unscored || 0) > 0 ? 'warning' : 'success'" :label="Number(dashboard?.database_health?.stock_news_unscored || 0) > 0 ? '待处理' : '健康'" /></template>
            </InfoCard>
            <InfoCard title="事件 / 治理 / 资金 / 分钟线缺口" :meta="`事件 ${dashboard?.database_health?.miss_events ?? '-'} · 治理 ${dashboard?.database_health?.miss_governance ?? '-'} · 资金 ${dashboard?.database_health?.miss_flow ?? '-'} · 分钟线 ${dashboard?.database_health?.miss_minline ?? '-'}`" description="缺口显著时先去数据库审计或数据源监控页。">
              <template #badge><StatusBadge value="info" label="数据补齐" /></template>
            </InfoCard>
          </div>
        </PageSection>
      </div>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { RouterLink } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import { fetchDashboard } from '../../services/api/dashboard'
import { formatDate, formatDateTime } from '../../shared/utils/format'

const { data } = useQuery({
  queryKey: ['dashboard'],
  queryFn: fetchDashboard,
  refetchInterval: 60_000,
})

const dashboard = computed(() => data.value)
</script>
