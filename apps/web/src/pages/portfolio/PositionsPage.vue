<template>
  <AppShell title="当前持仓" subtitle="模拟交易账户持仓概览，含成本、市值与浮动盈亏。">
    <div class="space-y-4">
      <PageSection title="持仓列表" subtitle="当前所有活跃持仓。">
        <template #action>
          <RouterLink
            to="/app/desk/board"
            class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
          >
            前往决策工作台
          </RouterLink>
        </template>

        <div v-if="isPending" class="py-8 text-center text-sm text-[var(--muted)]">加载中...</div>
        <div v-else-if="isError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-700">
          加载持仓失败：{{ String(error) }}
          <button class="ml-2 underline" @click="() => refetch()">重试</button>
        </div>
        <div v-else-if="positions.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-12 text-center">
          <div class="text-base font-semibold text-[var(--ink)]">暂无持仓</div>
          <div class="mt-1 text-sm text-[var(--muted)]">在组合订单被模拟成交后，真实持仓将出现在这里。</div>
          <RouterLink
            to="/app/desk/board"
            class="mt-4 inline-flex items-center rounded-full border border-[var(--line)] bg-white px-4 py-1.5 text-sm font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
          >
            前往决策工作台 →
          </RouterLink>
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="border-b border-[var(--line)] text-left">
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">代码</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">交易链</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">名称</th>
                <th class="pb-2 pr-4 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">持仓量</th>
                <th class="pb-2 pr-4 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">均价</th>
                <th class="pb-2 pr-4 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">现价</th>
                <th class="pb-2 pr-4 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">浮盈亏</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">更新时间</th>
                <th class="pb-2 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">操作</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-[var(--line)]">
              <tr
                v-for="pos in positions"
                :key="pos.id"
                class="transition hover:bg-[var(--panel-soft)]"
              >
                <td class="py-3 pr-4 font-semibold text-[var(--ink)]">
                  <RouterLink
                    :to="`/app/data/stocks/detail/${pos.ts_code}`"
                    class="transition hover:text-[var(--brand)]"
                  >
                    {{ pos.ts_code }}
                  </RouterLink>
                </td>
                <td class="py-3 pr-4 font-mono text-xs" :title="pos.chain_order_id || pos.id">
                  <RouterLink
                    :to="`/app/desk/trade-chain/${encodeURIComponent(displayOrderNo(pos))}`"
                    class="text-[var(--brand)] underline-offset-2 transition hover:underline"
                  >
                    {{ displayOrderNo(pos) }}
                  </RouterLink>
                </td>
                <td class="py-3 pr-4 text-[var(--muted)]">{{ pos.name || '-' }}</td>
                <td class="py-3 pr-4 text-right tabular-nums">{{ pos.quantity ?? pos.size ?? '-' }}</td>
                <td class="py-3 pr-4 text-right tabular-nums">{{ formatPrice(pos.avg_cost ?? pos.avg_price) }}</td>
                <td class="py-3 pr-4 text-right tabular-nums text-[var(--muted)]">{{ formatPrice(pos.last_price ?? pos.current_price) }}</td>
                <td class="py-3 pr-4 text-right tabular-nums font-semibold" :class="pnlClass(pos.unrealized_pnl)">
                  {{ formatPnl(pos.unrealized_pnl) }}
                </td>
                <td class="py-3 pr-4 text-xs text-[var(--muted)]">{{ formatDate(pos.updated_at ?? pos.created_at) }}</td>
                <td class="py-3 text-right">
                  <div class="inline-flex flex-wrap justify-end gap-1.5">
                    <button
                      class="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 transition hover:border-emerald-400 hover:bg-emerald-100 disabled:cursor-not-allowed disabled:opacity-50"
                      :disabled="isTrading(pos)"
                      @click="runPositionTrade(pos, 'add')"
                    >
                      {{ isTrading(pos, 'add') ? '加仓中...' : '加仓' }}
                    </button>
                    <button
                      class="rounded-full border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-700 transition hover:border-amber-400 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-50"
                      :disabled="isTrading(pos)"
                      @click="runPositionTrade(pos, 'reduce')"
                    >
                      {{ isTrading(pos, 'reduce') ? '减仓中...' : '减仓' }}
                    </button>
                    <button
                      class="rounded-full border border-rose-200 bg-rose-50 px-3 py-1.5 text-xs font-semibold text-rose-700 transition hover:border-rose-400 hover:bg-rose-100 disabled:cursor-not-allowed disabled:opacity-50"
                      :disabled="isTrading(pos)"
                      @click="runPositionTrade(pos, 'close')"
                    >
                      {{ isTrading(pos, 'close') ? '清仓中...' : '清仓复盘' }}
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="tradeError" class="mt-3 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
          {{ tradeError }}
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { RouterLink, useRouter } from 'vue-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import { createPortfolioOrder, fetchPortfolioPositions, updatePortfolioOrder, type PortfolioPosition } from '../../services/api/portfolio'

const router = useRouter()
const queryClient = useQueryClient()
const tradingKey = ref('')
const tradeError = ref('')

type PositionTradeAction = 'add' | 'reduce' | 'close'

const TRADE_ACTION_LABELS: Record<PositionTradeAction, string> = {
  add: '加仓',
  reduce: '减仓',
  close: '清仓',
}

const {
  data,
  isPending,
  isError,
  error,
  refetch,
} = useQuery({
  queryKey: ['portfolio-positions'],
  queryFn: fetchPortfolioPositions,
  refetchInterval: 60_000,
})

const positions = computed<PortfolioPosition[]>(() => {
  const raw = data.value as any
  if (!raw) return []
  if (Array.isArray(raw)) return raw
  if (Array.isArray(raw.items)) return raw.items
  if (Array.isArray(raw.positions)) return raw.positions
  return []
})

function formatPrice(v?: number | null): string {
  if (v == null) return '-'
  return v.toFixed(2)
}

function formatPnl(v?: number | null): string {
  if (v == null) return '-'
  const prefix = v > 0 ? '+' : ''
  return `${prefix}${v.toFixed(2)}`
}

function pnlClass(v?: number | null): string {
  if (v == null) return 'text-[var(--muted)]'
  if (v > 0) return 'text-emerald-600'
  if (v < 0) return 'text-rose-600'
  return 'text-[var(--muted)]'
}

function formatDate(s?: string): string {
  if (!s) return '-'
  try {
    return new Date(s).toLocaleDateString('zh-CN')
  } catch {
    return s
  }
}

function positionQuantity(pos: PortfolioPosition): number {
  const raw = pos.quantity ?? pos.size ?? 0
  const value = Number(raw)
  return Number.isFinite(value) ? Math.trunc(value) : 0
}

function positionPrice(pos: PortfolioPosition): number | null {
  const raw = pos.last_price ?? pos.current_price ?? pos.avg_cost ?? pos.avg_price
  const value = Number(raw)
  return Number.isFinite(value) && value > 0 ? value : null
}

function tradeKey(pos: PortfolioPosition, action: PositionTradeAction): string {
  return `${String(pos.ts_code || '')}:${action}`
}

function isTrading(pos: PortfolioPosition, action?: PositionTradeAction): boolean {
  if (!tradingKey.value) return false
  if (action) return tradingKey.value === tradeKey(pos, action)
  return tradingKey.value.startsWith(`${String(pos.ts_code || '')}:`)
}

const positionTradeMutation = useMutation({
  mutationFn: async (args: { position: PortfolioPosition; action: PositionTradeAction; size: number; executedPrice: number; reason: string }) => {
    const label = TRADE_ACTION_LABELS[args.action]
    const created = await createPortfolioOrder({
      ts_code: args.position.ts_code,
      action_type: args.action,
      planned_price: args.executedPrice,
      size: args.size,
      chain_order_no: args.position.order_no || undefined,
      note: `持仓看板一键${label}：${args.position.ts_code}；本次操作原因：${args.reason}`,
    })
    const orderId = String((created as Record<string, any>)?.id || '').trim()
    const orderNo = String((created as Record<string, any>)?.order_no || args.position.order_no || orderId).trim()
    if ((created as Record<string, any>)?.ok !== true || !orderId) {
      throw new Error(String((created as Record<string, any>)?.error || `创建${label}订单失败`))
    }
    const updated = await updatePortfolioOrder(orderId, {
      status: 'executed',
      executed_price: args.executedPrice,
      executed_at: new Date().toISOString(),
    })
    if ((updated as Record<string, any>)?.ok !== true) {
      throw new Error(String((updated as Record<string, any>)?.error || `${label}执行失败`))
    }
    return { orderId, orderNo, action: args.action }
  },
  onSettled: () => {
    tradingKey.value = ''
    queryClient.invalidateQueries({ queryKey: ['portfolio-positions'] })
    queryClient.invalidateQueries({ queryKey: ['portfolio-orders'] })
    queryClient.invalidateQueries({ queryKey: ['portfolio-reviews'] })
    queryClient.invalidateQueries({ queryKey: ['portfolio-review-chains'] })
  },
})

function promptTradeSize(pos: PortfolioPosition, action: PositionTradeAction): number | null {
  const currentSize = positionQuantity(pos)
  if (action === 'close') return currentSize
  const label = TRADE_ACTION_LABELS[action]
  const input = window.prompt(`${label} ${String(pos.ts_code || '')}，请输入数量：`, action === 'reduce' ? String(currentSize) : '')
  if (input == null) return null
  const size = Number(String(input).trim())
  if (!Number.isFinite(size) || size <= 0 || !Number.isInteger(size)) {
    tradeError.value = `${label}数量必须是正整数。`
    return null
  }
  if (action === 'reduce' && size > currentSize) {
    tradeError.value = `减仓数量不能超过当前持仓 ${currentSize}。`
    return null
  }
  return size
}

function promptTradePrice(pos: PortfolioPosition, action: PositionTradeAction): number | null {
  const label = TRADE_ACTION_LABELS[action]
  const defaultPrice = positionPrice(pos)
  const input = window.prompt(`确认${label} ${String(pos.ts_code || '')}，请输入成交价：`, defaultPrice != null ? String(defaultPrice) : '')
  if (input == null) return null
  const executedPrice = Number(String(input).trim().replace(',', '.'))
  if (!Number.isFinite(executedPrice) || executedPrice <= 0) {
    tradeError.value = `${label}成交价必须是大于 0 的数字。`
    return null
  }
  return executedPrice
}

function promptTradeReason(pos: PortfolioPosition, action: PositionTradeAction): string | null {
  const label = TRADE_ACTION_LABELS[action]
  const input = window.prompt(`${label} ${String(pos.ts_code || '')}，请填写本次操作原因：`, '')
  if (input == null) return null
  const reason = String(input).trim()
  if (!reason) {
    tradeError.value = `${label}必须填写本次操作原因。`
    return null
  }
  return reason
}

async function runPositionTrade(pos: PortfolioPosition, action: PositionTradeAction) {
  const tsCode = String(pos.ts_code || '').trim()
  const currentSize = positionQuantity(pos)
  const label = TRADE_ACTION_LABELS[action]
  if (!tsCode || currentSize <= 0) {
    tradeError.value = `当前持仓数量无效，无法${label}。`
    return
  }
  if (action === 'close') {
    const ok = window.confirm(`确认清仓 ${tsCode}？将按当前持仓 ${currentSize} 全部卖出。`)
    if (!ok) return
  }
  tradeError.value = ''
  const size = promptTradeSize(pos, action)
  if (size == null) return
  const executedPrice = promptTradePrice(pos, action)
  if (executedPrice == null) return
  const reason = promptTradeReason(pos, action)
  if (reason == null) return
  tradingKey.value = tradeKey(pos, action)
  try {
    const result = await positionTradeMutation.mutateAsync({ position: pos, action, size, executedPrice, reason })
    if (action === 'close') {
      await router.push({ path: '/app/desk/review', query: { order_id: result.orderNo } })
    }
  } catch (e: any) {
    tradeError.value = e?.message || `${label}失败`
  }
}

function displayOrderNo(pos: PortfolioPosition): string {
  const orderNo = String(pos.order_no || '').trim()
  if (orderNo) return orderNo
  return String(pos.id || '').slice(0, 8)
}
</script>
