<template>
  <AppShell title="国际财经资讯" subtitle="国际新闻、重要度、情绪与映射结果统一查看。">
    <NewsListPageBlock
      page-title="国际财经资讯"
      page-subtitle="过滤国内源，只看国际财经链路。"
      :query-key="['news', 'global', filters]"
      :query-fn="() => fetchNews({ ...filters, exclude_sources: excludedSources.join(','), exclude_source_prefixes: excludedPrefixes.join(',') })"
      v-model:filters="filters"
      :show-source="true"
      :load-sources="true"
    />
  </AppShell>
</template>

<script setup lang="ts">
import { reactive } from 'vue'
import AppShell from '../../shared/ui/AppShell.vue'
import { fetchNews } from '../../services/api/news'
import NewsListPageBlock from './NewsListPageBlock.vue'

const excludedSources = ['cn_sina_7x24', 'cn_eastmoney_fastnews']
const excludedPrefixes = ['cn_']
const filters = reactive({ source: '', keyword: '', date_from: '', date_to: '', finance_levels: '极高,高,中', page: 1, page_size: 20 })
</script>
