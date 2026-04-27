<template>
  <AppShell title="交易复盘" subtitle="模拟成交后的复盘队列：待复盘、人工结论与规则沉淀。">
    <div class="space-y-4">
      <!-- 复盘列表 -->
      <PageSection title="复盘闭环" :subtitle="`共 ${reviews.length} 条交易链`">
        <div v-if="filterOrderId" class="mb-3 flex items-center gap-2 rounded-2xl border border-blue-200 bg-blue-50 px-3 py-2 text-xs text-blue-700">
          <span class="font-semibold">当前过滤：订单 {{ filterOrderId }}</span>
          <RouterLink to="/app/desk/review" class="ml-auto text-blue-500 underline">查看全部复盘</RouterLink>
        </div>
        <div v-if="reviewsLoading" class="py-8 text-center text-sm text-[var(--muted)]">加载中...</div>
        <div v-else-if="reviewsError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-700">
          加载复盘记录失败，请刷新重试。
        </div>
        <div v-else-if="reviews.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-10 text-center text-sm text-[var(--muted)]">
          暂无复盘记录。即使复盘被删除，也可以从下方“可复盘交易链”重新发起复盘。
        </div>
        <div v-else class="space-y-4">
          <div
            v-for="review in reviews"
            :key="review.id"
            class="rounded-2xl border border-[var(--line)] bg-white px-4 py-4"
          >
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div>
                <div class="text-sm font-semibold text-[var(--ink)]">
                  交易 #
                  <RouterLink
                    :to="`/app/desk/trade-chain/${encodeURIComponent(displayReviewOrderNo(review))}`"
                    class="text-[var(--brand)] underline-offset-2 transition hover:underline"
                    :title="review.chain_order_id || review.order_id || review.id"
                  >
                    {{ displayReviewOrderNo(review) }}
                  </RouterLink>{{ review.ts_code ? ` · ${review.ts_code}` : '' }}
                </div>
                <div class="mt-0.5 text-xs text-[var(--muted)]">{{ formatDate(review.created_at) }} · {{ review.review_count || 0 }} 条复盘记录</div>
                <div v-if="review.strategy_context?.strategy_key" class="mt-1 text-xs font-semibold text-blue-700">
                  来源策略 {{ review.strategy_context.strategy_key }}
                </div>
              </div>
              <div class="flex items-center gap-2">
                <span :class="reviewTagClass(review.review_tag)">{{ reviewTagLabel(review.review_tag) }}</span>
                <span v-if="review.slippage != null" class="text-xs text-[var(--muted)]">
                  滑点 {{ review.slippage > 0 ? '+' : '' }}{{ review.slippage.toFixed(2) }}%
                </span>
              </div>
            </div>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <button
                v-if="review.order_id"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                @click="prefillReview(review)"
              >
                补一次复盘
              </button>
              <button
                v-if="review.order_id"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 font-semibold text-[var(--muted)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                @click="copyOrderId(review.order_id)"
              >
                复制订单号
              </button>
              <RouterLink
                v-if="review.order_id && !filterOrderId"
                :to="`/app/desk/review?order_id=${encodeURIComponent(review.order_no || review.order_id || '')}`"
                class="rounded-full border border-blue-200 bg-blue-50 px-3 py-1.5 font-semibold text-blue-700 transition hover:border-blue-400"
              >
                只看此交易
              </RouterLink>
              <RouterLink
                :to="`/app/desk/trade-chain/${encodeURIComponent(displayReviewOrderNo(review))}`"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
              >
                查看详情
              </RouterLink>
              <button
                class="rounded-full border border-rose-200 bg-rose-50 px-3 py-1.5 font-semibold text-rose-700 transition hover:border-rose-400 disabled:cursor-not-allowed disabled:opacity-50"
                :disabled="deletingReviewId === String(review.id)"
                @click="deleteReview(review)"
              >
                {{ deletingReviewId === String(review.id) ? '删除中...' : '删除整组复盘' }}
              </button>
            </div>

            <div class="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
              <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-3">
                <div class="mb-1 text-xs font-semibold uppercase text-[var(--muted)]">原始判断</div>
                <div class="text-sm font-semibold text-[var(--ink)]">{{ review.snapshot_id || review.decision_action_id || '-' }}</div>
                <div class="mt-1 text-xs leading-5 text-[var(--muted)]">{{ review.decision_payload?.trigger_reason || review.decision_note || '当前没有关联的判断说明。' }}</div>
              </div>
              <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-3">
                <div class="mb-1 text-xs font-semibold uppercase text-[var(--muted)]">来源策略</div>
                <div class="text-sm font-semibold text-[var(--ink)]">{{ strategyLabel(review.strategy_context) }}</div>
                <div class="mt-1 text-xs leading-5 text-[var(--muted)]">{{ strategyDetail(review.strategy_context) }}</div>
              </div>
              <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-3">
                <div class="mb-1 text-xs font-semibold uppercase text-[var(--muted)]">动作建议</div>
                <div class="text-sm font-semibold text-[var(--ink)]">{{ review.action_summary || reviewActionLabel(review.action_type) }}</div>
                <div class="mt-1 text-xs leading-5 text-[var(--muted)]">{{ review.decision_payload?.position_pct_range || review.order_note || '当前没有动作仓位说明。' }}</div>
              </div>
              <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-3">
                <div class="mb-1 text-xs font-semibold uppercase text-[var(--muted)]">执行结果</div>
                <div class="text-sm font-semibold text-[var(--ink)]">{{ reviewExecutionLabel(review.order_status, review.decision_payload?.execution_status) }}</div>
                <div class="mt-1 text-xs leading-5 text-[var(--muted)]">{{ review.executed_at ? `执行于 ${formatDate(review.executed_at)}` : '当前未记录执行时间。' }}</div>
              </div>
              <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-3">
                <div class="mb-1 text-xs font-semibold uppercase text-[var(--muted)]">结果归因</div>
                <div class="text-sm font-semibold text-[var(--ink)]">{{ reviewTagLabel(review.review_tag) }}</div>
                <div class="mt-1 text-xs leading-5 text-[var(--muted)]">{{ review.review_note || review.decision_payload?.review_conclusion || '当前还没有结果归因说明。' }}</div>
              </div>
              <div class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-3">
                <div class="mb-1 text-xs font-semibold uppercase text-[var(--muted)]">规则修正建议</div>
                <div class="text-sm font-semibold text-[var(--ink)]">{{ review.rule_correction_hint ? '已形成' : '待补充' }}</div>
                <div class="mt-1 text-xs leading-5 text-[var(--muted)]">{{ review.rule_correction_hint || '当前还没有沉淀规则修正建议。' }}</div>
              </div>
            </div>
          </div>
        </div>
      </PageSection>

      <PageSection title="可复盘交易链" subtitle="同一个 8 位订单号只显示一行，入场到清仓作为一笔完整交易复盘。">
        <div v-if="chainsLoading" class="py-6 text-center text-sm text-[var(--muted)]">加载交易链...</div>
        <div v-else-if="reviewChains.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-8 text-center text-sm text-[var(--muted)]">
          暂无可复盘交易链。订单执行或平仓后，会出现在这里。
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="border-b border-[var(--line)] text-left">
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">交易链</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">代码</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">策略</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">链路</th>
                <th class="pb-2 pr-4 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">入场 / 出场</th>
                <th class="pb-2 pr-4 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">数量</th>
                <th class="pb-2 pr-4 text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">状态</th>
                <th class="pb-2 text-right text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">操作</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-[var(--line)]">
              <tr v-for="chain in reviewChains" :key="chain.order_no || chain.id" class="transition hover:bg-[var(--panel-soft)]">
                <td class="py-3 pr-4 font-mono text-xs" :title="chain.chain_order_id || chain.latest_order_id || chain.id">
                  <RouterLink
                    :to="`/app/desk/trade-chain/${encodeURIComponent(chain.order_no || chain.id)}`"
                    class="text-[var(--brand)] underline-offset-2 transition hover:underline"
                  >
                    {{ chain.order_no || chain.id }}
                  </RouterLink>
                </td>
                <td class="py-3 pr-4 font-semibold text-[var(--ink)]">{{ chain.ts_code || '-' }}</td>
                <td class="py-3 pr-4 text-xs text-blue-700">{{ strategyLabel(chain.strategy_context) }}</td>
                <td class="py-3 pr-4 text-[var(--muted)]">
                  <div>{{ chain.action_summary || '-' }}</div>
                  <div class="mt-0.5 text-xs text-[var(--muted)]">{{ chain.event_count || 0 }} 次动作 · {{ formatDate(chain.ended_at) }}</div>
                </td>
                <td class="py-3 pr-4 text-right tabular-nums">{{ formatChainPrice(chain) }}</td>
                <td class="py-3 pr-4 text-right tabular-nums">{{ chain.quantity ?? '-' }}</td>
                <td class="py-3 pr-4">
                  <span :class="chainStatusClass(chain.chain_status)">{{ chainStatusLabel(chain.chain_status) }}</span>
                </td>
                <td class="py-3 text-right">
                  <button
                    class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--brand)] transition hover:border-[var(--brand)]"
                    @click="prefillChainReview(chain)"
                  >
                    复盘此交易
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </PageSection>

      <!-- 添加复盘 -->
      <PageSection title="添加复盘" subtitle="为已执行订单补充人工结论；系统生成的 pending 记录代表待复盘。">
        <div class="space-y-4">
          <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <label class="text-sm font-semibold text-[var(--ink)]">
              订单号 / 长订单 ID
              <input
                v-model="form.order_id"
                class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm"
                placeholder="关联订单编号（可选）"
              />
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              交易评级
              <select v-model="form.review_tag" class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm">
                <option value="">请选择...</option>
                <option value="win">盈利（win）</option>
                <option value="loss">亏损（loss）</option>
                <option value="neutral">中性（neutral）</option>
                <option value="pending">待评（pending）</option>
              </select>
            </label>
            <label class="text-sm font-semibold text-[var(--ink)]">
              滑点（%）
              <input
                v-model.number="form.slippage"
                type="number"
                step="0.01"
                class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm"
                placeholder="如 -0.15"
              />
            </label>
          </div>
          <label class="text-sm font-semibold text-[var(--ink)]">
            复盘笔记
            <textarea
              v-model="form.review_note"
              rows="3"
              class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm"
              placeholder="交易回顾与经验总结..."
            />
          </label>

          <div v-if="submitError || deleteError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            {{ submitError || deleteError }}
          </div>
          <div v-if="copySuccess" class="rounded-2xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-700">
            {{ copySuccess }}
          </div>
          <div v-if="submitSuccess" class="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
            复盘记录已保存。
          </div>

          <button
            class="rounded-2xl bg-[var(--brand)] px-5 py-2.5 text-sm font-semibold text-white disabled:opacity-60"
            :disabled="!form.review_tag || submitPending"
            @click="submitReview"
          >
            {{ submitPending ? '提交中...' : '提交复盘' }}
          </button>
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { ref, reactive, computed, watch } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useQuery, useQueryClient } from '@tanstack/vue-query'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import { fetchPortfolioReviewChains, fetchPortfolioReviewGroups, createPortfolioReview, deletePortfolioReview, type PortfolioReview, type PortfolioReviewChain, type StrategyContext } from '../../services/api/portfolio'

const route = useRoute()
const filterOrderId = computed(() => String(route.query.order_id || '').trim())

const {
  data: reviewsData,
  isPending: reviewsLoading,
  isError: reviewsError,
} = useQuery({
  queryKey: computed(() => ['portfolio-reviews', filterOrderId.value]),
  queryFn: () => fetchPortfolioReviewGroups({ limit: 50, order_id: filterOrderId.value || undefined }),
})

const {
  data: chainsData,
  isPending: chainsLoading,
} = useQuery({
  queryKey: ['portfolio-review-chains'],
  queryFn: () => fetchPortfolioReviewChains({ limit: 100 }),
})

const reviews = computed<PortfolioReview[]>(() => {
  const raw = reviewsData.value
  if (!raw) return []
  if (Array.isArray(raw)) return raw
  if (Array.isArray(raw.items)) return raw.items
  if (Array.isArray(raw.reviews)) return raw.reviews
  return []
})

const reviewChains = computed<PortfolioReviewChain[]>(() => {
  const raw = chainsData.value
  if (!raw) return []
  if (Array.isArray(raw)) return raw
  if (Array.isArray(raw.items)) return raw.items
  if (Array.isArray(raw.chains)) return raw.chains
  return []
})

const form = reactive({
  order_id: '',
  review_tag: '',
  slippage: null as number | null,
  review_note: '',
})

const submitPending = ref(false)
const submitError = ref('')
const submitSuccess = ref(false)
const deleteError = ref('')
const deletingReviewId = ref('')
const copySuccess = ref('')
const queryClient = useQueryClient()

watch(
  filterOrderId,
  (orderId) => {
    if (orderId && !form.order_id) form.order_id = orderId
  },
  { immediate: true },
)

function formatDate(s?: string): string {
  if (!s) return '-'
  try {
    return new Date(s).toLocaleDateString('zh-CN')
  } catch {
    return s
  }
}

function reviewTagLabel(t?: string): string {
  const map: Record<string, string> = { win: '盈利', loss: '亏损', neutral: '中性', pending: '待评' }
  return t ? (map[t] ?? t) : '-'
}

function reviewActionLabel(action?: string): string {
  const map: Record<string, string> = {
    buy: '新买',
    add: '加仓',
    reduce: '减仓',
    sell: '卖出',
    close: '清仓',
    watch: '观察',
    defer: '暂缓',
  }
  return action ? (map[action] ?? action) : '未关联动作'
}

function reviewExecutionLabel(orderStatus?: string, decisionExecutionStatus?: string): string {
  const status = orderStatus || decisionExecutionStatus || ''
  const map: Record<string, string> = {
    planned: '待执行',
    partial: '部分执行',
    executed: '已执行',
    cancelled: '已取消',
    executing: '执行中',
    done: '已完成',
  }
  return status ? (map[status] ?? status) : '未关联执行状态'
}

function formatPrice(v?: number | null): string {
  if (v == null) return '-'
  return Number(v).toFixed(2)
}

function formatChainPrice(chain: PortfolioReviewChain): string {
  const entry = formatPrice(chain.entry_price)
  const exit = formatPrice(chain.exit_price)
  return chain.exit_price == null ? entry : `${entry} / ${exit}`
}

function strategyLabel(strategy?: StrategyContext): string {
  const key = String(strategy?.strategy_key || '').trim()
  return key || '-'
}

function strategyDetail(strategy?: StrategyContext): string {
  if (!strategy?.strategy_key) return '当前交易没有关联策略来源。'
  const parts: string[] = []
  if (strategy.strategy_fit_score != null) parts.push(`匹配 ${formatStrategyScore(strategy.strategy_fit_score)}`)
  if (strategy.strategy_candidate_rank) parts.push(`候选 #${strategy.strategy_candidate_rank}`)
  if (strategy.strategy_action_bias) parts.push(`倾向 ${strategy.strategy_action_bias}`)
  return parts.join(' · ') || String(strategy.strategy_source || 'strategy_selection')
}

function formatStrategyScore(v?: number | string | null): string {
  if (v == null || v === '') return '-'
  const value = Number(v)
  return Number.isFinite(value) ? value.toFixed(2) : String(v)
}

function reviewTagClass(t?: string): string {
  const base = 'inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold'
  if (t === 'win') return `${base} border-emerald-200 bg-emerald-100 text-emerald-700`
  if (t === 'loss') return `${base} border-rose-200 bg-rose-100 text-rose-700`
  if (t === 'neutral') return `${base} border-sky-200 bg-sky-100 text-sky-700`
  return `${base} border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]`
}

function shortId(id?: string): string {
  const value = String(id || '').trim()
  if (!value) return '-'
  if (value.length <= 12) return value
  return `${value.slice(0, 8)}...${value.slice(-4)}`
}

function displayReviewOrderNo(review: PortfolioReview): string {
  const orderNo = String(review.order_no || '').trim()
  if (orderNo) return orderNo
  return shortId(review.order_id || review.id)
}

function chainStatusLabel(status?: string): string {
  if (status === 'closed') return '已闭环'
  if (status === 'open') return '持仓中'
  return status || '-'
}

function chainStatusClass(status?: string): string {
  const base = 'inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold'
  if (status === 'closed') return `${base} border-emerald-200 bg-emerald-50 text-emerald-700`
  if (status === 'open') return `${base} border-blue-200 bg-blue-50 text-blue-700`
  return `${base} border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]`
}

function prefillReview(review: PortfolioReview) {
  form.order_id = String(review.order_no || review.order_id || '').trim()
  form.review_tag = review.review_tag === 'pending' ? '' : String(review.review_tag || '')
  form.slippage = review.slippage ?? null
  form.review_note = ''
  submitSuccess.value = false
  submitError.value = ''
  deleteError.value = ''
}

function prefillChainReview(chain: PortfolioReviewChain) {
  form.order_id = String(chain.order_no || chain.latest_order_id || '').trim()
  form.review_tag = ''
  form.slippage = null
  form.review_note = ''
  submitSuccess.value = false
  submitError.value = ''
  deleteError.value = ''
}

async function copyOrderId(orderId?: string) {
  const value = String(orderId || '').trim()
  if (!value) return
  try {
    await navigator.clipboard.writeText(value)
    copySuccess.value = '完整订单 ID 已复制。'
  } catch {
    copySuccess.value = `完整订单 ID：${value}`
  }
  setTimeout(() => {
    copySuccess.value = ''
  }, 3000)
}

async function deleteReview(review: PortfolioReview) {
  const reviewId = String(review.id || '').trim()
  if (!reviewId) return
  const ids = (review.reviews || []).map((item) => String(item.id || '').trim()).filter(Boolean)
  if (ids.length === 0) ids.push(reviewId)
  const ok = window.confirm(`删除这组复盘记录吗？\n交易 ${displayReviewOrderNo(review)}，共 ${ids.length} 条`)
  if (!ok) return
  deletingReviewId.value = reviewId
  deleteError.value = ''
  submitSuccess.value = false
  try {
    for (const id of ids) {
      const result = await deletePortfolioReview(id)
      if ((result as Record<string, any>)?.ok !== true) {
        throw new Error(String((result as Record<string, any>)?.error || '删除复盘失败'))
      }
    }
    await queryClient.invalidateQueries({ queryKey: ['portfolio-reviews'] })
    await queryClient.invalidateQueries({ queryKey: ['portfolio-review-chains'] })
  } catch (e: any) {
    deleteError.value = e?.message || '删除复盘失败'
  } finally {
    deletingReviewId.value = ''
  }
}

async function submitReview() {
  if (!form.review_tag) return
  submitPending.value = true
  submitError.value = ''
  deleteError.value = ''
  submitSuccess.value = false
  try {
    await createPortfolioReview({
      order_id: form.order_id || undefined,
      review_tag: form.review_tag,
      slippage: form.slippage ?? undefined,
      review_note: form.review_note || undefined,
    })
    submitSuccess.value = true
    form.order_id = filterOrderId.value || ''
    form.review_tag = ''
    form.slippage = null
    form.review_note = ''
    await queryClient.invalidateQueries({ queryKey: ['portfolio-reviews'] })
    await queryClient.invalidateQueries({ queryKey: ['portfolio-review-chains'] })
  } catch (e: any) {
    submitError.value = e?.message || '提交失败'
  } finally {
    submitPending.value = false
  }
}
</script>
