<template>
  <AppShell title="数据库审计" subtitle="缺口、重复、未评分、Markdown 报告和健康指标放到统一工作面板。">
    <div class="space-y-4">
      <PageSection title="数据库健康快照" subtitle="结构化健康数据适合快速巡检。">
        <div class="grid gap-3 xl:grid-cols-3 md:grid-cols-2">
          <StatCard title="日线最新" :value="formatDate(health?.daily_latest)" :hint="`分钟线 ${formatDate(health?.minline_latest)} · 评分 ${formatDate(health?.scores_latest)}`" />
          <StatCard title="事件/治理缺口" :value="`${health?.miss_events ?? '-'} / ${health?.miss_governance ?? '-'}`" :hint="`资金流 ${health?.miss_flow ?? '-'} · 分钟线 ${health?.miss_minline ?? '-'}`" />
          <StatCard title="新闻未评分" :value="health?.news_unscored ?? 0" :hint="`个股新闻 ${health?.stock_news_unscored ?? 0}`" />
          <StatCard title="重复组" :value="`${health?.news_dup_link ?? 0} / ${health?.stock_news_dup_link ?? 0}`" hint="国际/个股新闻重复组" />
          <StatCard title="宏观发布日期缺失" :value="health?.macro_publish_empty ?? 0" hint="应继续补齐" />
          <StatCard title="群聊去重异常" :value="health?.chatlog_dup_key ?? 0" hint="理论上应尽量接近 0" />
        </div>
      </PageSection>

      <PageSection title="审计报告" subtitle="Markdown 报告直接在新前端里解析，不再裸文本展示。">
        <MarkdownBlock :content="audit?.markdown || '暂无审计报告'" />
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import MarkdownBlock from '../../shared/markdown/MarkdownBlock.vue'
import { fetchDatabaseAudit, fetchDbHealth } from '../../services/api/dashboard'
import { formatDate } from '../../shared/utils/format'

const { data: audit } = useQuery({ queryKey: ['database-audit'], queryFn: fetchDatabaseAudit })
const { data: health } = useQuery({ queryKey: ['db-health'], queryFn: fetchDbHealth })
</script>
