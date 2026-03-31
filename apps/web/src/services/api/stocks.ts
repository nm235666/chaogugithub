import { http } from '../http'

export async function fetchStockFilters() {
  const { data } = await http.get('/api/stocks/filters')
  return data
}

export async function fetchStocks(params: Record<string, any>) {
  const { data } = await http.get('/api/stocks', { params })
  return data
}

export async function fetchStockDetail(params: Record<string, any>) {
  const { data } = await http.get('/api/stock-detail', { params })
  return data
}

export async function fetchStockPrices(params: Record<string, any>) {
  const { data } = await http.get('/api/prices', { params })
  return data
}

export async function fetchStockMinline(params: Record<string, any>) {
  const { data } = await http.get('/api/minline', { params })
  return data
}

export async function fetchStockScores(params: Record<string, any>) {
  const { data } = await http.get('/api/stock-scores', { params })
  return data
}

export async function fetchStockScoreFilters() {
  const { data } = await http.get('/api/stock-scores/filters')
  return data
}

export async function triggerStockNewsFetch(params: Record<string, any>) {
  const { data } = await http.get('/api/stock-news/fetch', { params })
  return data
}

export async function triggerTrendAnalysis(params: Record<string, any>) {
  const { data } = await http.get('/api/llm/trend', {
    params,
    timeout: 180_000,
  })
  return data
}

export async function triggerMultiRoleTask(params: Record<string, any>) {
  const { data } = await http.get('/api/llm/multi-role/start', { params })
  return data
}

export async function fetchMultiRoleTask(params: Record<string, any>) {
  const { data } = await http.get('/api/llm/multi-role/task', { params })
  return data
}
