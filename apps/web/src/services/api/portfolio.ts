import { http } from '../http'

export interface PortfolioPosition {
  id: string
  ts_code: string
  name?: string
  quantity?: number
  avg_cost?: number
  last_price?: number
  market_value?: number
  size?: number
  avg_price?: number
  current_price?: number
  unrealized_pnl?: number
  order_no?: string
  chain_order_id?: string
  owner_hash?: string
  created_at?: string
  updated_at?: string
}

export interface PortfolioOrder {
  id: string
  ts_code: string
  name?: string
  action_type?: string
  planned_price?: number
  executed_price?: number
  size?: number
  status?: string
  order_no?: string
  chain_order_id?: string
  owner_hash?: string
  created_at?: string
  executed_at?: string
  decision_action_id?: string
  note?: string
  strategy_context?: StrategyContext
  decision_payload?: Record<string, any>
}

export interface StrategyContext {
  strategy_key?: string
  strategy_run_id?: string | number
  strategy_run_key?: string
  strategy_candidate_rank?: string | number
  strategy_fit_score?: number
  strategy_action_bias?: string
  strategy_source?: string
  summary?: string
}

export interface PortfolioReview {
  id: string
  order_id?: string
  review_tag?: string
  slippage?: number
  review_note?: string
  created_at?: string
  ts_code?: string
  action_type?: string
  order_status?: string
  executed_at?: string
  executed_price?: number
  order_no?: string
  chain_order_id?: string
  owner_hash?: string
  decision_action_id?: string
  order_note?: string
  snapshot_id?: string
  decision_note?: string
  decision_payload?: {
    execution_status?: string
    review_conclusion?: string
    evidence_sources?: string[]
    trigger_reason?: string
    position_pct_range?: string
  }
  strategy_context?: StrategyContext
  rule_correction_hint?: string
  action_summary?: string
  review_count?: number
  pending_count?: number
  completed_count?: number
  reviews?: PortfolioReview[]
}

export interface PortfolioReviewChain {
  id: string
  order_no: string
  chain_order_id?: string
  owner_hash?: string
  ts_code?: string
  action_summary?: string
  chain_status?: 'open' | 'closed' | string
  entry_price?: number
  exit_price?: number
  quantity?: number
  event_count?: number
  started_at?: string
  ended_at?: string
  latest_order_id?: string
  latest_action_type?: string
  events?: PortfolioOrder[]
  strategy_context?: StrategyContext
}

export interface PortfolioTradeChainDetail extends PortfolioReviewChain {
  ok?: boolean
  error?: string
  total_buy_amount?: number
  total_sell_amount?: number
  realized_pnl?: number
  return_pct?: number | null
  remaining_quantity?: number
  avg_cost?: number
  timeline?: Array<PortfolioOrder & {
    price?: number
    amount?: number
    quantity_after?: number
    avg_cost_after?: number
  }>
  reviews?: PortfolioReview[]
}

export interface StrategyPerformanceItem {
  strategy_key: string
  strategy_source?: string
  trade_count?: number
  closed_trade_count?: number
  win_count?: number
  loss_count?: number
  neutral_count?: number
  pending_count?: number
  total_realized_pnl?: number
  avg_return_pct?: number | null
  avg_fit_score?: number | null
  latest_trade_at?: string
  win_rate?: number
  updated_at?: string
  performance?: Record<string, any>
}

export async function fetchPortfolioPositions() {
  const { data } = await http.get('/api/portfolio/positions')
  return data
}

export async function fetchPortfolioOrders(params?: { status?: string; limit?: number; decision_action_id?: string }) {
  const { data } = await http.get('/api/portfolio/orders', { params })
  return data
}

export async function createPortfolioOrder(payload: {
  ts_code: string
  action_type: string
  planned_price?: number
  size?: number
  decision_action_id?: string | number
  chain_order_no?: string
  note?: string
}) {
  const { data } = await http.post('/api/portfolio/orders', payload)
  return data
}

export async function createPortfolioOrderFromDecision(payload: {
  decision_action_id: string | number
  action_type: 'buy' | 'add' | 'sell' | 'reduce' | 'close'
  planned_price: number
  size: number
  note?: string
}) {
  const { data } = await http.post('/api/portfolio/orders/from-decision', payload)
  return data
}

export async function updatePortfolioOrder(id: string, payload: Record<string, any>) {
  const { data } = await http.patch(`/api/portfolio/orders/${id}`, payload)
  return data
}

export async function fetchPortfolioReviews(params?: { order_id?: string; limit?: number }) {
  const { data } = await http.get('/api/portfolio/review', { params })
  return data
}

export async function fetchPortfolioReviewGroups(params?: { order_id?: string; limit?: number }) {
  const { data } = await http.get('/api/portfolio/review/groups', { params })
  return data
}

export async function fetchPortfolioReviewChains(params?: { limit?: number }) {
  const { data } = await http.get('/api/portfolio/review/chains', { params })
  return data
}

export async function auditPortfolioStrategyAttribution(payload: { apply?: boolean; repair?: boolean; limit?: number } = {}) {
  const { data } = await http.post('/api/portfolio/strategy-attribution/audit', payload)
  return data
}

export async function fetchPortfolioStrategyPerformance(params: { limit?: number; refresh?: boolean } = {}) {
  const { data } = await http.get<{ ok?: boolean; summary?: Record<string, any>; items?: StrategyPerformanceItem[] }>(
    '/api/portfolio/strategy-performance',
    { params },
  )
  return data
}

export async function refreshPortfolioStrategyPerformance(payload: { limit?: number } = {}) {
  const { data } = await http.post<{ ok?: boolean; summary?: Record<string, any>; items?: StrategyPerformanceItem[] }>(
    '/api/portfolio/strategy-performance/refresh',
    payload,
  )
  return data
}

export async function fetchPortfolioTradeChain(orderNo: string) {
  const { data } = await http.get(`/api/portfolio/trade-chains/${encodeURIComponent(orderNo)}`)
  return data
}

export async function createPortfolioReview(payload: {
  order_id?: string
  review_tag: string
  slippage?: number
  review_note?: string
}) {
  const { data } = await http.post('/api/portfolio/review', payload)
  return data
}

export async function deletePortfolioReview(id: string) {
  const { data } = await http.delete(`/api/portfolio/review/${encodeURIComponent(id)}`)
  return data
}
