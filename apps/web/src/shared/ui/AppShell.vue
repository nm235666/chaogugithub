<template>
  <div class="min-h-screen bg-[var(--bg)] text-[var(--ink)]">
    <a href="#main-content" class="sr-only focus:not-sr-only focus:fixed focus:left-3 focus:top-3 focus:z-[1600] focus:rounded-full focus:bg-[var(--brand)] focus:px-4 focus:py-2 focus:text-sm focus:font-semibold focus:text-white">
      跳到主内容
    </a>
    <div class="mx-auto flex min-h-screen max-w-[1600px] gap-4 px-3 py-3 md:px-4">
      <aside :class="sidebarClasses">
        <div class="relative overflow-hidden rounded-[30px] border border-white/12 bg-[linear-gradient(180deg,#0d1720_0%,#12384a_58%,#0c6977_100%)] p-4 text-white shadow-2xl shadow-[rgba(10,32,44,0.22)]">
          <div class="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent_0%,rgba(255,255,255,0.34)_50%,transparent_100%)]" />
          <div class="absolute -left-10 top-20 h-44 w-44 rounded-full bg-[rgba(255,255,255,0.08)] blur-3xl" />
          <div class="absolute bottom-2 right-2 h-40 w-40 rounded-full bg-[rgba(214,134,72,0.18)] blur-3xl" />
          <div class="absolute inset-y-0 right-0 w-px bg-[linear-gradient(180deg,transparent_0%,rgba(255,255,255,0.16)_50%,transparent_100%)]" />
          <div class="relative mb-5">
            <div class="inline-flex rounded-full border border-white/16 bg-white/10 px-3 py-1 text-xs uppercase tracking-[0.18em] text-white/82">Zanbo Quant</div>
            <div class="mt-3 text-xl font-extrabold">研究终端</div>
            <div class="mt-2 text-sm leading-6 text-white/72">统一股票、新闻、信号</div>
          </div>
          <nav class="relative space-y-5">
            <div v-for="group in navGroups" :key="group.title" class="space-y-2">
              <div class="px-2 text-xs font-semibold uppercase tracking-[0.14em] text-white/54">{{ group.title }}</div>
              <RouterLink
                v-for="item in group.items"
                :key="item.to"
                :to="item.to"
                class="block rounded-[22px] border px-3 py-3 text-sm transition"
                :class="isNavActive(item.to) ? 'border-white/14 bg-white/18 text-white shadow-lg ring-1 ring-white/18 backdrop-blur-sm' : 'border-white/8 bg-white/[0.05] text-white/84 hover:border-white/14 hover:bg-white/10 hover:text-white'"
              >
                <div class="font-semibold">{{ item.label }}</div>
                <div class="mt-1 text-xs text-white/55">{{ item.desc }}</div>
              </RouterLink>
            </div>
          </nav>
        </div>
      </aside>

      <div class="min-w-0 flex-1">
        <header class="mb-4 rounded-[28px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.9)_0%,rgba(255,255,255,0.72)_100%)] px-4 py-4 shadow-[var(--shadow)] backdrop-blur-xl">
          <div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <div class="mb-3 flex items-center gap-2 xl:hidden">
                <button
                  class="rounded-[18px] bg-[linear-gradient(135deg,var(--brand)_0%,var(--brand-ink)_100%)] px-4 py-2 text-sm font-semibold text-white"
                  :aria-expanded="ui.mobileNavOpen ? 'true' : 'false'"
                  aria-controls="mobile-app-nav"
                  aria-label="打开导航菜单"
                  @click="ui.toggleMobileNav()"
                >
                  {{ ui.mobileNavOpen ? '关闭菜单' : '导航菜单' }}
                </button>
                <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-3 py-2 text-xs text-[var(--muted)]">
                  <div class="font-semibold text-[var(--ink)]">最近事件</div>
                  <div class="mt-1 max-w-[180px] truncate">{{ realtime.lastEvent || '暂无' }}</div>
                </div>
              </div>
              <div class="text-xs uppercase tracking-[0.18em] text-[var(--muted)]">Zanbo Quant Vue Frontend</div>
              <div class="mt-1 text-[30px] font-extrabold tracking-tight" style="font-family: var(--font-display)">{{ title }}</div>
              <div class="mt-1 text-sm text-[var(--muted)]">{{ subtitle }}</div>
            </div>
            <div class="flex flex-wrap items-center gap-3">
              <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(238,244,247,0.88)_100%)] px-3 py-2 text-sm shadow-[var(--shadow-soft)]">
                <div class="text-xs uppercase tracking-[0.13em] text-[var(--muted)]">实时连接</div>
                <div class="mt-1 flex items-center gap-2 font-semibold">
                  <span :class="['inline-block size-2.5 rounded-full', realtime.connected ? 'bg-emerald-500' : 'bg-amber-500']" />
                  {{ realtime.connected ? '在线' : '重连中' }}
                </div>
              </div>
              <div class="hidden rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(238,244,247,0.88)_100%)] px-3 py-2 text-sm shadow-[var(--shadow-soft)] md:block">
                <div class="text-xs uppercase tracking-[0.13em] text-[var(--muted)]">最近事件</div>
                <div class="mt-1 max-w-[300px] truncate font-semibold">{{ realtime.lastEvent || '暂无' }}</div>
              </div>
              <button class="hidden rounded-[20px] bg-[linear-gradient(135deg,var(--brand)_0%,var(--brand-ink)_100%)] px-4 py-3 text-sm font-semibold text-white xl:block" @click="ui.toggleSidebar()">
                {{ ui.sidebarOpen ? '收起导航' : '展开导航' }}
              </button>
              <button
                v-if="hasAdminToken"
                class="rounded-[20px] border border-[var(--line)] bg-white px-4 py-3 text-sm font-semibold text-[var(--ink)]"
                @click="logout"
              >
                退出登录
              </button>
            </div>
          </div>
        </header>

        <main id="main-content" tabindex="-1">
          <slot />
        </main>
      </div>
    </div>

    <Teleport to="body">
      <div
        v-if="ui.mobileNavOpen"
        class="fixed inset-0 z-50 xl:hidden"
      >
        <button class="absolute inset-0 bg-[rgba(8,17,26,0.52)]" aria-label="关闭导航菜单" @click="ui.closeMobileNav()" />
        <aside
          id="mobile-app-nav"
          class="absolute inset-y-0 left-0 w-[min(88vw,340px)] overflow-y-auto border-r border-white/12 bg-[linear-gradient(180deg,#0d1720_0%,#12384a_58%,#0c6977_100%)] p-4 text-white shadow-2xl"
          role="dialog"
          aria-modal="true"
          aria-label="主导航"
        >
          <div class="mb-4 flex items-center justify-between">
            <div>
              <div class="inline-flex rounded-full border border-white/16 bg-white/10 px-3 py-1 text-xs uppercase tracking-[0.18em] text-white/82">Zanbo Quant</div>
              <div class="mt-2 text-lg font-extrabold">研究终端导航</div>
            </div>
            <button class="rounded-full border border-white/18 bg-white/10 px-3 py-2 text-sm font-semibold text-white" @click="ui.closeMobileNav()">关闭</button>
          </div>
          <nav class="space-y-5">
            <div v-for="group in navGroups" :key="`mobile-${group.title}`" class="space-y-2">
              <div class="px-2 text-xs font-semibold uppercase tracking-[0.14em] text-white/54">{{ group.title }}</div>
              <RouterLink
                v-for="item in group.items"
                :key="`mobile-${item.to}`"
                :to="item.to"
                class="block rounded-[22px] border px-3 py-3 text-sm transition"
                :class="isNavActive(item.to) ? 'border-white/14 bg-white/18 text-white shadow-lg ring-1 ring-white/18 backdrop-blur-sm' : 'border-white/8 bg-white/[0.05] text-white/84 hover:border-white/14 hover:bg-white/10 hover:text-white'"
                @click="ui.closeMobileNav()"
              >
                <div class="font-semibold">{{ item.label }}</div>
                <div class="mt-1 text-xs text-white/55">{{ item.desc }}</div>
              </RouterLink>
            </div>
          </nav>
        </aside>
      </div>
    </Teleport>
    <AppDialogHost />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import AppDialogHost from './AppDialogHost.vue'
import { useUiStore } from '../../stores/ui'
import { useRealtimeStore } from '../../stores/realtime'
import { useAuthStore } from '../../stores/auth'
import { clearAuthStatusCache, logoutAuth } from '../../services/api/auth'
import { clearAdminToken, readAdminToken } from '../../services/authToken'
import { APP_PERMISSION_VALUES, hasPermissionByEffective, type AppPermission } from '../../app/permissions'
import { NAV_GROUPS, type NavGroupConfig } from '../../app/navigation'
import { fetchNavigationGroups } from '../../services/api/system'

const props = defineProps<{
  title: string
  subtitle?: string
}>()

const ui = useUiStore()
const realtime = useRealtimeStore()
const auth = useAuthStore()
const route = useRoute()
const router = useRouter()
const hasAdminToken = ref(!!readAdminToken())
const remoteNavGroups = ref<NavGroupConfig[] | null>(null)
const permissionValueSet = computed(() => {
  const catalogCodes = Array.isArray(auth.permissionCatalog)
    ? auth.permissionCatalog.map((item: any) => String(item?.code || '').trim()).filter(Boolean)
    : []
  return new Set<string>(catalogCodes.length ? catalogCodes : APP_PERMISSION_VALUES)
})

const sidebarClasses = computed(() => [
  'hidden shrink-0 xl:block',
  ui.sidebarOpen ? 'w-[310px]' : 'w-[110px]',
])

const title = computed(() => props.title)
const subtitle = computed(() => props.subtitle || '统一研究、监控与信号分析工作流')

const navGroups = computed(() => {
  const source = remoteNavGroups.value?.length ? remoteNavGroups.value : NAV_GROUPS
  return [...source]
    .sort((a, b) => a.order - b.order)
    .map((group) => ({
      ...group,
      items: group.items.filter((item) => hasPermissionByEffective(auth.effectivePermissions, auth.role, item.permission)),
    }))
    .filter((group) => group.items.length > 0)
})

function normalizeNavigationGroups(value: unknown): NavGroupConfig[] {
  if (!Array.isArray(value)) return []
  const groups: NavGroupConfig[] = []
  let invalidGroups = 0
  let invalidItems = 0
  for (const group of value) {
    if (!group || typeof group !== 'object') {
      invalidGroups += 1
      continue
    }
    const g = group as Record<string, unknown>
    const id = String(g.id || '').trim()
    const title = String(g.title || '').trim()
    const orderRaw = Number(g.order || 0)
    if (!id || !title || !Number.isFinite(orderRaw)) {
      invalidGroups += 1
      continue
    }
    const rawItems = Array.isArray(g.items) ? g.items : []
    const items = rawItems
      .map((item) => {
        if (!item || typeof item !== 'object') {
          invalidItems += 1
          return null
        }
        const it = item as Record<string, unknown>
        const to = String(it.to || '').trim()
        const label = String(it.label || '').trim()
        const desc = String(it.desc || '').trim()
        const permission = String(it.permission || '').trim()
        if (!to || !label || !permissionValueSet.value.has(permission)) {
          invalidItems += 1
          return null
        }
        return { to, label, desc, permission: permission as AppPermission }
      })
      .filter(Boolean) as NavGroupConfig['items']
    if (!items.length) {
      invalidGroups += 1
      continue
    }
    groups.push({ id, title, order: Math.floor(orderRaw), items })
  }
  if (invalidGroups > 0 || invalidItems > 0) {
    console.warn(`[nav-config] ignored invalid groups=${invalidGroups}, invalid items=${invalidItems}`)
  }
  return groups
}

function isNavActive(to: string): boolean {
  const targetPath = String(to || '').split('?')[0] || ''
  if (!targetPath) return false
  if (route.path === targetPath) return true
  // 股票详情是动态路径，导航固定指向示例代码，激活态按前缀判断
  if (targetPath.startsWith('/stocks/detail/')) {
    return route.path.startsWith('/stocks/detail')
  }
  return false
}

watch(
  () => route.fullPath,
  () => {
    ui.closeMobileNav()
  },
)

onMounted(async () => {
  try {
    const payload = await fetchNavigationGroups()
    if ((payload?.validation?.invalid_groups || 0) > 0 || (payload?.validation?.invalid_items || 0) > 0) {
      console.warn(
        `[nav-config] server validation invalid_groups=${payload?.validation?.invalid_groups || 0}, invalid_items=${payload?.validation?.invalid_items || 0}`,
      )
    }
    const normalized = normalizeNavigationGroups(payload?.groups)
    if (normalized.length) {
      remoteNavGroups.value = normalized
    } else {
      console.warn('[nav-config] server payload has no usable groups, fallback to local defaults')
    }
  } catch {
    // Keep local NAV_GROUPS when server config is unavailable.
  }
})

async function logout() {
  try {
    await logoutAuth()
  } catch {
    // ignore logout API errors; local clear is enough for UX
  }
  clearAdminToken()
  clearAuthStatusCache()
  auth.status = null
  auth.loaded = false
  hasAdminToken.value = false
  router.replace('/login')
}

</script>
