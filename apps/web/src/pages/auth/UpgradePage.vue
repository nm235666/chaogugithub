<template>
  <div class="min-h-screen bg-[var(--bg)] px-4 py-10 text-[var(--ink)]">
    <div class="mx-auto max-w-[760px] rounded-[28px] border border-[var(--line)] bg-white p-6 shadow-[var(--shadow)]">
      <div class="text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Zanbo Quant</div>
      <h1 class="mt-2 text-3xl font-extrabold" style="font-family: var(--font-display)">功能升级提示</h1>
      <p class="mt-2 text-sm text-[var(--muted)]">
        当前账号为受限版，仅开放基础模块。若需访问高阶服务，请联系管理员升级权限。
      </p>

      <div class="mt-5 grid gap-3 md:grid-cols-2">
        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-sm font-semibold">已开放模块</div>
          <ul class="mt-2 text-sm text-[var(--muted)]">
            <li>国际财经资讯</li>
            <li>国内财经资讯</li>
            <li>个股资讯</li>
            <li>新闻日报总结（结果展示）</li>
            <li>LLM 股票走势分析（每日最多 3 次）</li>
          </ul>
        </div>
        <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-4">
          <div class="text-sm font-semibold">待升级模块</div>
          <ul class="mt-2 text-sm text-[var(--muted)]">
            <li>多角色公司分析</li>
            <li>投资信号/主题热点</li>
            <li>群聊分析与候选池</li>
            <li>宏观高级分析</li>
            <li>系统运维与调度能力</li>
          </ul>
        </div>
      </div>

      <div class="mt-5 flex flex-wrap gap-2">
        <RouterLink to="/intelligence/global-news" class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white">进入国际资讯</RouterLink>
        <RouterLink to="/research/trend" class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold text-[var(--ink)]">
          进入走势分析
        </RouterLink>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { RouterLink } from 'vue-router'
</script>

