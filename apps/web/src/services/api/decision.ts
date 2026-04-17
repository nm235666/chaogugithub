import { http } from '../http'
import type { DecisionScoreboardPayload } from '../../shared/types/api'

export type DecisionTraceReceipt = {
  status?: string
  source?: string
  context?: Record<string, any>
  trace?: {
    action_id?: string
    run_id?: string
    snapshot_id?: string
  }
}

export async function fetchDecisionBoard(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/board', { params })
  return data
}

export async function fetchDecisionScoreboard(params: Record<string, any> = {}) {
  const { data } = await http.get<DecisionScoreboardPayload>('/api/decision/scores', { params })
  return data
}

export async function fetchDecisionStock(params: Record<string, any>) {
  const { data } = await http.get('/api/decision/stock', { params })
  return data
}

export async function fetchDecisionPlan(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/plan', { params })
  return data
}

export async function fetchDecisionStrategies(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/strategies', { params })
  return data
}

export async function fetchDecisionStrategyRuns(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/strategy-runs', { params })
  return data
}

export async function fetchDecisionStrategyRun(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/strategy-runs', { params })
  return data
}

export async function runDecisionStrategyLab(payload: Record<string, any> = {}) {
  const { data } = await http.post('/api/decision/strategy-runs/run', payload)
  return data
}

export async function fetchDecisionHistory(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/history', { params })
  return data
}

export async function fetchDecisionActions(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/actions', { params })
  return data
}

export async function fetchDecisionKillSwitch() {
  const { data } = await http.get('/api/decision/kill-switch')
  return data
}

export async function setDecisionKillSwitch(payload: { allow_trading: boolean; reason?: string }) {
  const { data } = await http.post('/api/decision/kill-switch', payload)
  return data
}

export async function runDecisionSnapshot() {
  const { data } = await http.post('/api/decision/snapshot/run', {})
  return data
}

export async function fetchDecisionCalibration(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/decision/calibration', { params })
  return data
}

export async function recordDecisionAction(payload: {
  action_type: 'confirm' | 'reject' | 'defer' | 'watch' | 'review'
  ts_code: string
  stock_name?: string
  note?: string
  snapshot_date?: string
  context?: Record<string, any>
  /** Structured evidence items: source label + optional url/value */
  evidence_sources?: Array<{ label: string; url?: string; value?: string }>
  /** Lifecycle status of the resulting action: pending | running | done | failed */
  execution_status?: string
  /** Free-text review conclusion, recorded after action is evaluated */
  review_conclusion?: string
}) {
  const { data } = await http.post('/api/decision/actions', payload)
  return data
}
