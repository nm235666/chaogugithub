<template>
  <div class="min-h-screen bg-[radial-gradient(circle_at_top_right,_rgba(59,130,246,0.12),_transparent_32%),linear-gradient(180deg,#f4f7fb_0%,#eef3f9_100%)] text-[var(--ink)]" data-shell-surface="admin">
    <a href="#main-content" class="sr-only focus:not-sr-only focus:fixed focus:left-3 focus:top-3 focus:z-[1600] focus:rounded-full focus:bg-slate-800 focus:px-4 focus:py-2 focus:text-sm focus:font-semibold focus:text-white">
      跳到主内容
    </a>
    <div class="mx-auto flex min-h-screen max-w-[var(--content-max)] gap-4 px-3 py-3 md:px-4">
      <aside :class="['hidden shrink-0 xl:block', sidebarWidthClass]" data-shell-nav="admin">
        <div class="relative overflow-hidden rounded-[var(--radius-lg)] border border-slate-800/10 bg-[linear-gradient(180deg,#111827_0%,#1e293b_100%)] p-4 text-white shadow-[var(--shadow-float)]">
          <div class="absolute inset-x-0 top-0 h-20 bg-[radial-gradient(circle_at_top_right,rgba(96,165,250,0.28),transparent_60%)]" />
          <div class="relative mb-5">
            <div class="inline-flex rounded-full border border-white/12 bg-white/8 px-3 py-1 text-xs uppercase tracking-[0.18em] text-white/75">{{ shellBadge }}</div>
            <div class="mt-3 text-xl font-extrabold">后台管理台</div>
            <div class="mt-2 text-sm text-white/70">监控、配置、权限与审计统一治理</div>
          </div>
          <nav class="relative space-y-5">
            <div
              v-for="group in navGroups"
              :key="group.id"
              class="space-y-2"
              :data-nav-group="group.id"
              :data-nav-group-expanded="isNavGroupExpanded(group) ? 'true' : 'false'"
            >
              <template v-if="isNavGroupExpanded(group)">
                <div class="px-2 text-xs font-semibold uppercase tracking-[0.14em] text-white/50">{{ group.title }}</div>
                <RouterLink
                  v-for="item in group.items"
                  :key="item.to"
                  :to="item.to"
                  class="block rounded-[var(--radius-md)] border border-transparent px-3 py-2.5 text-sm transition-all duration-200"
                  :class="isNavActive(item.to) ? 'border-sky-300/70 bg-white/12 text-white shadow-[0_0_0_1px_rgba(125,211,252,0.16)]' : 'text-white/72 hover:border-white/10 hover:bg-white/7 hover:text-white'"
                >
                  <div class="font-semibold">{{ item.label }}</div>
                  <div class="mt-0.5 text-xs text-white/50">{{ item.desc }}</div>
                </RouterLink>
              </template>
              <RouterLink
                v-else
                :to="navGroupLayerDefaultPath(group)"
                class="flex w-full items-center justify-between gap-2 rounded-[var(--radius-md)] border border-white/10 bg-white/5 px-3 py-2.5 text-left text-sm text-white/85 transition hover:border-white/15 hover:bg-white/10"
                :aria-expanded="'false'"
                :aria-label="`${group.title}，前往该层入口`"
              >
                <span class="text-xs font-semibold uppercase tracking-[0.14em]">{{ group.title }}</span>
                <span class="shrink-0 text-white/45" aria-hidden="true">›</span>
              </RouterLink>
            </div>
          </nav>
        </div>
      </aside>

      <div class="min-w-0 flex-1">
        <LayerNavBar class="mb-3" :groups="navGroups" />
        <header class="mb-4 rounded-[var(--radius-lg)] border border-slate-200 bg-white/96 px-4 py-4 shadow-[var(--shadow-card)] backdrop-blur" data-shell-header="admin">
          <div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div class="min-w-0">
              <div class="mb-3 flex items-center gap-2 xl:hidden">
                <button
                  class="rounded-[var(--radius-md)] bg-slate-800 px-4 py-2 text-sm font-semibold text-white transition hover:bg-slate-900"
                  :aria-expanded="ui.mobileNavOpen ? 'true' : 'false'"
                  aria-controls="mobile-admin-nav"
                  aria-label="打开后台导航"
                  @click="ui.toggleMobileNav()"
                >
                  {{ ui.mobileNavOpen ? '关闭导航' : '后台导航' }}
                </button>
              </div>
              <div class="text-xs uppercase tracking-[0.18em] text-slate-500">{{ shellBadge }}</div>
              <div class="mt-1 text-[28px] font-extrabold leading-tight tracking-tight text-slate-950" style="font-family: var(--font-display)">{{ title }}</div>
              <div class="mt-1 text-sm text-slate-600">{{ subtitle || shellModeDescription }}</div>
            </div>
            <div class="flex flex-wrap items-center gap-2.5">
              <div class="rounded-[var(--radius-md)] border border-slate-200 bg-slate-50 px-3 py-2 text-sm">
                <div class="text-xs uppercase tracking-[0.13em] text-slate-500">实时连接</div>
                <div class="mt-1 flex items-center gap-2 font-semibold text-slate-900">
                  <span :class="['inline-block size-2.5 rounded-full', realtime.connected ? 'bg-emerald-500' : 'bg-amber-500']" />
                  {{ realtime.connected ? '在线' : '重连中' }}
                </div>
              </div>
              <div class="hidden rounded-[var(--radius-md)] border border-slate-200 bg-white px-3 py-2 text-sm md:block">
                <div class="text-xs uppercase tracking-[0.13em] text-slate-500">最近事件</div>
                <div class="mt-1 max-w-[240px] truncate font-semibold text-slate-900">{{ realtime.lastEvent || '暂无' }}</div>
              </div>
              <button class="hidden rounded-[var(--radius-md)] border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-800 transition hover:bg-slate-50 xl:block" @click="ui.toggleSidebar('admin')">
                {{ sidebarOpen ? '收起导航' : '展开导航' }}
              </button>
              <button
                v-if="canSwitchMode"
                class="rounded-[var(--radius-md)] bg-slate-900 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-slate-950"
                data-shell-switch="admin"
                @click="switchMode"
              >
                {{ switchLabel }}
              </button>
              <button
                v-if="hasAdminToken"
                class="rounded-[var(--radius-md)] border border-[var(--line)] bg-white px-4 py-2.5 text-sm font-semibold text-[var(--ink)] transition hover:bg-gray-50"
                @click="logout"
              >
                退出登录
              </button>
            </div>
          </div>
        </header>

        <main id="main-content" tabindex="-1" class="page-main-stack">
          <slot />
        </main>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="ui.mobileNavOpen" class="fixed inset-0 z-50 xl:hidden">
        <button class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="关闭后台导航" @click="ui.closeMobileNav()" />
        <aside
          id="mobile-admin-nav"
          class="absolute inset-y-0 left-0 w-[min(82vw,320px)] overflow-y-auto border-r border-slate-200/10 bg-[linear-gradient(180deg,#111827_0%,#1e293b_100%)] p-4 text-white shadow-xl"
          role="dialog"
          aria-modal="true"
          aria-label="后台主导航"
        >
          <div class="mb-4 flex items-center justify-between">
            <div>
              <div class="inline-flex rounded-full border border-white/12 bg-white/8 px-3 py-1 text-xs uppercase tracking-[0.18em] text-white/75">{{ shellBadge }}</div>
              <div class="mt-2 text-lg font-extrabold">后台导航</div>
            </div>
            <button class="rounded-full border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-white transition hover:bg-white/10" @click="ui.closeMobileNav()">关闭</button>
          </div>
          <nav class="space-y-5">
            <div
              v-for="group in navGroups"
              :key="`mobile-${group.id}`"
              class="space-y-2"
              :data-nav-group="group.id"
              :data-nav-group-expanded="isNavGroupExpanded(group) ? 'true' : 'false'"
            >
              <template v-if="isNavGroupExpanded(group)">
                <div class="px-2 text-xs font-semibold uppercase tracking-[0.14em] text-white/50">{{ group.title }}</div>
                <RouterLink
                  v-for="item in group.items"
                  :key="`mobile-${item.to}`"
                  :to="item.to"
                  class="block rounded-[var(--radius-md)] border border-transparent px-3 py-2.5 text-sm transition-all"
                  :class="isNavActive(item.to) ? 'border-sky-300/70 bg-white/12 text-white' : 'text-white/72 hover:border-white/10 hover:bg-white/7 hover:text-white'"
                  @click="ui.closeMobileNav()"
                >
                  <div class="font-semibold">{{ item.label }}</div>
                  <div class="mt-0.5 text-xs text-white/50">{{ item.desc }}</div>
                </RouterLink>
              </template>
              <RouterLink
                v-else
                :to="navGroupLayerDefaultPath(group)"
                class="flex w-full items-center justify-between gap-2 rounded-[var(--radius-md)] border border-white/10 bg-white/5 px-3 py-2.5 text-left text-sm text-white/85 transition hover:border-white/15 hover:bg-white/10"
                :aria-expanded="'false'"
                :aria-label="`${group.title}，前往该层入口`"
                @click="ui.closeMobileNav()"
              >
                <span class="text-xs font-semibold uppercase tracking-[0.14em]">{{ group.title }}</span>
                <span class="shrink-0 text-white/45" aria-hidden="true">›</span>
              </RouterLink>
            </div>
          </nav>
        </aside>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { RouterLink } from 'vue-router'
import LayerNavBar from './LayerNavBar.vue'
import { useShellFrame } from './useShellFrame'

defineProps<{
  title: string
  subtitle?: string
}>()

const {
  hasAdminToken,
  navGroups,
  shellBadge,
  shellModeDescription,
  realtime,
  ui,
  canSwitchMode,
  switchLabel,
  switchMode,
  sidebarOpen,
  sidebarWidthClass,
  isNavActive,
  isNavGroupExpanded,
  navGroupLayerDefaultPath,
  logout,
} = useShellFrame('admin')
</script>
