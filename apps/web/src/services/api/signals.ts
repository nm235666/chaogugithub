import { http } from '../http'

export async function fetchInvestmentSignals(params: Record<string, any>) {
  const { data } = await http.get('/api/investment-signals', { params })
  return data
}

export async function fetchSignalTimeline(params: Record<string, any>) {
  const { data } = await http.get('/api/investment-signals/timeline', { params })
  return data
}

export async function fetchThemeHotspots(params: Record<string, any>) {
  const { data } = await http.get('/api/theme-hotspots', { params })
  return data
}

export async function fetchSignalStateTimeline(params: Record<string, any>) {
  const { data } = await http.get('/api/signal-state/timeline', { params })
  return data
}

export async function fetchSignalAudit(params: Record<string, any>) {
  const { data } = await http.get('/api/signal-audit', { params })
  return data
}
