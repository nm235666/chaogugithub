import { http } from '../http'

export async function fetchNews(params: Record<string, any>) {
  const { data } = await http.get('/api/news', { params })
  return data
}

export async function fetchNewsSources() {
  const { data } = await http.get('/api/news/sources')
  return data
}

export async function fetchStockNews(params: Record<string, any>) {
  const { data } = await http.get('/api/stock-news', { params })
  return data
}

export async function fetchStockNewsSources() {
  const { data } = await http.get('/api/stock-news/sources')
  return data
}

export async function triggerStockNewsScore(params: Record<string, any>) {
  const { data } = await http.get('/api/stock-news/score', { params })
  return data
}

export async function fetchDailySummaries(params: Record<string, any>) {
  const { data } = await http.get('/api/news/daily-summaries', { params })
  return data
}

export async function triggerDailySummaryGenerate(params: Record<string, any>) {
  const { data } = await http.get('/api/news/daily-summaries/generate', { params })
  return data
}

export async function fetchDailySummaryTask(params: Record<string, any>) {
  const { data } = await http.get('/api/news/daily-summaries/task', { params })
  return data
}
