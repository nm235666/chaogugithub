<template>
  <AppShell title="交易链详情" subtitle="按 8 位订单号查看一笔交易从入场到出场的完整过程。">
    <div class="space-y-4">
      <PageSection title="交易链概览" :subtitle="orderNo ? `订单号 ${orderNo}` : '未选择订单号'">
        <div v-if="isPending" class="py-8 text-center text-sm text-[var(--muted)]">加载交易链...</div>
        <div v-else-if="isError || !chain?.ok" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-700">
          加载交易链失败：{{ errorText }}
        </div>
        <div v-else class="space-y-4">
          <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div class="font-mono text-xl font-semibold text-[var(--ink)]">{{ chain.order_no }}</div>
              <div class="mt-1 text-sm text-[var(--muted)]">
                {{ chain.ts_code || '-' }} · {{ chain.action_summary || '-' }} · {{ formatDate(chain.started_at) }} 至 {{ formatDate(chain.ended_at) }}
              </div>
              <div v-if="chainStrategy.strategy_key" class="mt-2 flex flex-wrap gap-2 text-xs">
                <span class="rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 font-semibold text-blue-700">
                  来源策略 {{ chainStrategy.strategy_key }}
                </span>
                <span v-if="chainStrategy.strategy_fit_score" class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-2.5 py-1 text-[var(--muted)]">
                  匹配 {{ formatScore(chainStrategy.strategy_fit_score) }}
                </span>
                <span v-if="chainStrategy.strategy_action_bias" class="rounded-full border border-[var(--line)] bg-[var(--panel-soft)] px-2.5 py-1 text-[var(--muted)]">
                  倾向 {{ chainStrategy.strategy_action_bias }}
                </span>
              </div>
            </div>
            <div class="flex flex-wrap gap-2">
              <RouterLink
                :to="`/app/desk/review?order_id=${encodeURIComponent(chain.order_no || orderNo)}`"
                class="rounded-full border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-semibold text-blue-700 transition hover:border-blue-400"
              >
                查看复盘
              </RouterLink>
              <RouterLink
                to="/app/desk/positions"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
              >
                返回持仓
              </RouterLink>
            </div>
          </div>

          <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
            <MetricCard label="状态" :value="chainStatusLabel(chain.chain_status)" />
            <MetricCard label="买入金额" :value="formatMoney(chain.total_buy_amount)" />
            <MetricCard label="卖出金额" :value="formatMoney(chain.total_sell_amount)" />
            <MetricCard label="已实现盈亏" :value="formatPnl(chain.realized_pnl)" :tone="pnlTone(chain.realized_pnl)" />
            <MetricCard label="收益率" :value="formatPct(chain.return_pct)" :tone="pnlTone(chain.return_pct)" />
          </div>
        </div>
      </PageSection>

      <PageSection title="交易时间线" subtitle="每次新买、加仓、减仓、清仓后的仓位和均价变化。">
        <div v-if="timeline.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-8 text-center text-sm text-[var(--muted)]">
          暂无交易动作。
        </div>
        <div v-else class="space-y-3">
          <div
            v-for="event in timeline"
            :key="event.id"
            class="rounded-2xl border border-[var(--line)] bg-white px-4 py-4"
          >
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <div class="text-sm font-semibold text-[var(--ink)]">
                  {{ actionTypeLabel(event.action_type) }} · {{ formatDate(event.executed_at || event.created_at) }}
                </div>
                <div class="mt-1 text-xs text-[var(--muted)]">
                  成交价 {{ formatPrice(event.price ?? event.executed_price ?? event.planned_price) }} · 数量 {{ event.size ?? '-' }} · 金额 {{ formatMoney(event.amount) }}
                </div>
              </div>
              <div class="text-right text-xs text-[var(--muted)]">
                <div>操作后持仓 {{ event.quantity_after ?? '-' }}</div>
                <div>操作后均价 {{ formatPrice(event.avg_cost_after) }}</div>
              </div>
            </div>
            <div class="mt-3 rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-xs leading-5 text-[var(--muted)]">
              {{ event.note || '本次操作没有备注。' }}
              <span v-if="event.strategy_context?.strategy_key" class="ml-2 text-blue-700">
                来源策略：{{ event.strategy_context.strategy_key }}
              </span>
            </div>
          </div>
        </div>
      </PageSection>

      <PageSection title="交易复盘" subtitle="同一交易链下的 pending 和人工复盘记录。">
        <div v-if="reviews.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-8 text-center text-sm text-[var(--muted)]">
          暂无复盘记录。
        </div>
        <div v-else class="grid gap-3 md:grid-cols-2">
          <div
            v-for="review in reviews"
            :key="review.id"
            class="rounded-2xl border border-[var(--line)] bg-white px-4 py-4"
          >
            <div class="flex items-center justify-between gap-2">
              <div class="text-sm font-semibold text-[var(--ink)]">{{ reviewTagLabel(review.review_tag) }}</div>
              <div class="text-xs text-[var(--muted)]">{{ review.review_count || 1 }} 条记录</div>
            </div>
            <div class="mt-2 text-xs leading-5 text-[var(--muted)]">
              {{ review.review_note || review.order_note || '还没有人工复盘结论。' }}
            </div>
            <div v-if="review.strategy_context?.strategy_key" class="mt-2 text-xs text-blue-700">
              来源策略：{{ review.strategy_context.strategy_key }}
            </div>
          </div>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, defineComponent } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import { fetchPortfolioTradeChain, type PortfolioReview, type PortfolioTradeChainDetail } from '../../services/api/portfolio'

const route = useRoute()
const orderNo = computed(() => String(route.params.orderNo || '').trim())

const { data, isPending, isError, error } = useQuery({
  queryKey: computed(() => ['portfolio-trade-chain', orderNo.value]),
  queryFn: () => fetchPortfolioTradeChain(orderNo.value),
})

const chain = computed<PortfolioTradeChainDetail | null>(() => {
  const raw = data.value as PortfolioTradeChainDetail | undefined
  return raw || null
})

const timeline = computed(() => chain.value?.timeline || [])
const reviews = computed<PortfolioReview[]>(() => chain.value?.reviews || [])
const chainStrategy = computed<Record<string, any>>(() => chain.value?.strategy_context || {})
const errorText = computed(() => String((chain.value as any)?.error || (error.value as any)?.message || '未知错误'))

function formatDate(s?: string): string {
  if (!s) return '-'
  try {
    return new Date(s).toLocaleDateString('zh-CN')
  } catch {
    return s
  }
}

function formatPrice(v?: number | null): string {
  if (v == null) return '-'
  return Number(v).toFixed(2)
}

function formatMoney(v?: number | null): string {
  if (v == null) return '-'
  return Number(v).toFixed(2)
}

function formatPnl(v?: number | null): string {
  if (v == null) return '-'
  const value = Number(v)
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}`
}

function formatPct(v?: number | null): string {
  if (v == null) return '-'
  const value = Number(v)
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

function formatScore(v?: number | string | null): string {
  if (v == null || v === '') return '-'
  const value = Number(v)
  return Number.isFinite(value) ? value.toFixed(2) : String(v)
}

function pnlTone(v?: number | null): 'good' | 'bad' | 'neutral' {
  if (v == null) return 'neutral'
  if (Number(v) > 0) return 'good'
  if (Number(v) < 0) return 'bad'
  return 'neutral'
}

function actionTypeLabel(action?: string): string {
  const map: Record<string, string> = {
    buy: '新买',
    add: '加仓',
    sell: '卖出',
    reduce: '减仓',
    close: '清仓',
    watch: '观察',
    defer: '暂缓',
  }
  return action ? (map[action] ?? action) : '-'
}

function reviewTagLabel(tag?: string): string {
  const map: Record<string, string> = { win: '盈利', loss: '亏损', neutral: '中性', pending: '待评' }
  return tag ? (map[tag] ?? tag) : '-'
}

function chainStatusLabel(status?: string): string {
  if (status === 'closed') return '已闭环'
  if (status === 'open') return '持仓中'
  return status || '-'
}

const MetricCard = defineComponent({
  props: {
    label: { type: String, required: true },
    value: { type: String, required: true },
    tone: { type: String, default: 'neutral' },
  },
  computed: {
    valueClass(): string {
      if (this.tone === 'good') return 'text-emerald-600'
      if (this.tone === 'bad') return 'text-rose-600'
      return 'text-[var(--ink)]'
    },
  },
  template: `
    <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">{{ label }}</div>
      <div class="mt-1 text-lg font-semibold tabular-nums" :class="valueClass">{{ value }}</div>
    </div>
  `,
})
</script>
