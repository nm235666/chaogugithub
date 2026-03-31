import { http } from '../http'

export async function fetchMacroIndicators() {
  const { data } = await http.get('/api/macro/indicators')
  return data
}

export async function fetchMacroSeries(params: Record<string, any>) {
  const { data } = await http.get('/api/macro', { params })
  return data
}
