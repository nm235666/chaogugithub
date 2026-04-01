<template>
  <div class="min-h-screen bg-[var(--bg)] text-[var(--ink)]">
    <div class="mx-auto flex min-h-screen max-w-[1600px] gap-4 px-3 py-3 md:px-4">
      <aside :class="sidebarClasses">
        <div class="relative overflow-hidden rounded-[30px] border border-white/12 bg-[linear-gradient(180deg,#0d1720_0%,#12384a_58%,#0c6977_100%)] p-4 text-white shadow-2xl shadow-[rgba(10,32,44,0.22)]">
          <div class="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent_0%,rgba(255,255,255,0.34)_50%,transparent_100%)]" />
          <div class="absolute -left-10 top-20 h-44 w-44 rounded-full bg-[rgba(255,255,255,0.08)] blur-3xl" />
          <div class="absolute bottom-2 right-2 h-40 w-40 rounded-full bg-[rgba(214,134,72,0.18)] blur-3xl" />
          <div class="absolute inset-y-0 right-0 w-px bg-[linear-gradient(180deg,transparent_0%,rgba(255,255,255,0.16)_50%,transparent_100%)]" />
          <div class="relative mb-5">
            <div class="inline-flex rounded-full border border-white/16 bg-white/10 px-3 py-1 text-[11px] uppercase tracking-[0.22em] text-white/82">Zanbo Quant</div>
            <div class="mt-3 text-xl font-extrabold">研究终端</div>
            <div class="mt-2 text-sm leading-6 text-white/72">统一股票、新闻、信号</div>
          </div>
          <nav class="relative space-y-5">
            <div v-for="group in navGroups" :key="group.title" class="space-y-2">
              <div class="px-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-white/54">{{ group.title }}</div>
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
              <div class="text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Zanbo Quant NextGen Frontend</div>
              <div class="mt-1 text-[30px] font-extrabold tracking-tight" style="font-family: var(--font-display)">{{ title }}</div>
              <div class="mt-1 text-sm text-[var(--muted)]">{{ subtitle }}</div>
            </div>
            <div class="flex flex-wrap items-center gap-3">
              <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(238,244,247,0.88)_100%)] px-3 py-2 text-sm shadow-[var(--shadow-soft)]">
                <div class="text-[11px] uppercase tracking-[0.15em] text-[var(--muted)]">实时连接</div>
                <div class="mt-1 flex items-center gap-2 font-semibold">
                  <span :class="['inline-block size-2.5 rounded-full', realtime.connected ? 'bg-emerald-500' : 'bg-amber-500']" />
                  {{ realtime.connected ? '在线' : '重连中' }}
                </div>
              </div>
              <div class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(238,244,247,0.88)_100%)] px-3 py-2 text-sm shadow-[var(--shadow-soft)]">
                <div class="text-[11px] uppercase tracking-[0.15em] text-[var(--muted)]">最近事件</div>
                <div class="mt-1 max-w-[300px] truncate font-semibold">{{ realtime.lastEvent || '暂无' }}</div>
              </div>
              <button class="rounded-[20px] bg-[linear-gradient(135deg,var(--brand)_0%,var(--brand-ink)_100%)] px-4 py-3 text-sm font-semibold text-white" @click="ui.toggleSidebar()">
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

        <main>
          <slot />
        </main>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { useUiStore } from '../../stores/ui'
import { useRealtimeStore } from '../../stores/realtime'
import { useAuthStore } from '../../stores/auth'
import { clearAuthStatusCache, fetchAuthStatus, logoutAuth } from '../../services/api/auth'
import { clearAdminToken, readAdminToken } from '../../services/authToken'
import { hasPermissionByEffective, type AppPermission } from '../../app/permissions'

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
const currentRole = ref('')

const sidebarClasses = computed(() => [
  'hidden shrink-0 xl:block',
  ui.sidebarOpen ? 'w-[310px]' : 'w-[110px]',
])

const title = computed(() => props.title)
const subtitle = computed(() => props.subtitle || '统一研究、监控与信号分析工作流')

const fullNavGroups = [
  {
    title: '总控',
    items: [
      { to: '/dashboard', label: '总控台', desc: '全局健康度、热点、任务与新鲜度', permission: 'admin_system' as AppPermission },
      { to: '/system/source-monitor', label: '数据源监控', desc: '数据源、进程、实时链路统一看板', permission: 'admin_system' as AppPermission },
      { to: '/system/database-audit', label: '数据库审计', desc: '缺口、重复、未评分、陈旧数据', permission: 'admin_system' as AppPermission },
      { to: '/system/invites', label: '邀请码管理', desc: '管理员邀请码与账号规模管理', permission: 'admin_users' as AppPermission },
      { to: '/system/users', label: '用户与会话', desc: '用户、会话、审计日志管理', permission: 'admin_users' as AppPermission },
    ],
  },
  {
    title: '股票',
    items: [
      { to: '/stocks/list', label: '股票列表', desc: '代码、简称、市场、地区快速检索', permission: 'stocks_advanced' as AppPermission },
      { to: '/stocks/scores', label: '综合评分', desc: '行业内评分与核心指标排序', permission: 'stocks_advanced' as AppPermission },
      { to: '/stocks/detail/000001.SZ', label: '股票详情', desc: '统一聚合价格、新闻、群聊与分析', permission: 'stocks_advanced' as AppPermission },
      { to: '/stocks/prices', label: '价格中心', desc: '日线 + 分钟线统一查询与图表', permission: 'stocks_advanced' as AppPermission },
    ],
  },
  {
    title: '情报与信号',
    items: [
      { to: '/intelligence/global-news', label: '国际资讯', desc: '全球财经新闻、评分与映射', permission: 'news_read' as AppPermission },
      { to: '/intelligence/cn-news', label: '国内资讯', desc: '新浪 / 东财资讯统一看', permission: 'news_read' as AppPermission },
      { to: '/intelligence/stock-news', label: '个股新闻', desc: '聚焦单股新闻与立即采集', permission: 'stock_news_read' as AppPermission },
      { to: '/intelligence/daily-summaries', label: '新闻日报总结', desc: '日报生成、历史查询与双格式导出', permission: 'daily_summary_read' as AppPermission },
      { to: '/signals/overview', label: '投资信号', desc: '股票与主题信号总览', permission: 'signals_advanced' as AppPermission },
      { to: '/signals/themes', label: '主题热点', desc: '主题强度、方向、预期与证据链', permission: 'signals_advanced' as AppPermission },
      { to: '/signals/audit', label: '信号审计', desc: '误映射、弱信号与质量问题', permission: 'signals_advanced' as AppPermission },
      { to: '/signals/quality-config', label: '信号配置', desc: '规则参数与映射黑名单', permission: 'signals_advanced' as AppPermission },
      { to: '/signals/state-timeline', label: '状态时间线', desc: '状态机迁移与市场预期层', permission: 'signals_advanced' as AppPermission },
    ],
  },
  {
    title: '研究与舆情',
    items: [
      { to: '/macro', label: '宏观看板', desc: '宏观指标查询与序列趋势', permission: 'macro_advanced' as AppPermission },
      { to: '/research/trend', label: '走势分析', desc: 'LLM 股票走势分析工作台', permission: 'trend_analyze' as AppPermission },
      { to: '/research/reports', label: '标准报告', desc: '统一投研报告列表', permission: 'research_advanced' as AppPermission },
      { to: '/research/multi-role', label: '多角色分析', desc: 'LLM 多角色公司分析工作台', permission: 'multi_role_analyze' as AppPermission },
      { to: '/chatrooms/overview', label: '群聊总览', desc: '群聊标签、状态、拉取健康度', permission: 'chatrooms_advanced' as AppPermission },
      { to: '/chatrooms/chatlog', label: '聊天记录', desc: '消息正文、引用和筛选查询', permission: 'chatrooms_advanced' as AppPermission },
      { to: '/chatrooms/investment', label: '投资倾向', desc: '群聊结论、情绪和标的清单', permission: 'chatrooms_advanced' as AppPermission },
      { to: '/chatrooms/candidates', label: '股票候选池', desc: '群聊汇总候选池与偏向', permission: 'chatrooms_advanced' as AppPermission },
    ],
  },
]

const navGroups = computed(() => {
  return fullNavGroups
    .map((group) => ({
      ...group,
      items: group.items.filter((item) => hasPermissionByEffective(auth.effectivePermissions, currentRole.value, item.permission)),
    }))
    .filter((group) => group.items.length > 0)
})

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

onMounted(async () => {
  try {
    const status = await fetchAuthStatus()
    currentRole.value = String(status.user?.role || status.user?.tier || '')
  } catch {
    currentRole.value = ''
  }
})
</script>
