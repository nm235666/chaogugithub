<template>
  <AppShell title="国内财经资讯" subtitle="国内新闻源独立看，适合盯政策、A 股、产业链和盘中快讯。">
    <NewsListPageBlock
      page-title="国内财经资讯"
      page-subtitle="只展示国内新闻源。"
      :query-key="['news', 'cn', filters]"
      :query-fn="() => fetchNews({ ...filters, source_prefixes: 'cn_' })"
      v-model:filters="filters"
      :show-source="false"
      :load-sources="false"
    />
  </AppShell>
</template>

<script setup lang="ts">
import { reactive } from 'vue'
import AppShell from '../../shared/ui/AppShell.vue'
import { fetchNews } from '../../services/api/news'
import NewsListPageBlock from './NewsListPageBlock.vue'

const filters = reactive({ keyword: '', date_from: '', date_to: '', finance_levels: '极高,高,中', page: 1, page_size: 20 })
</script>
