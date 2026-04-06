import { http } from '../http'

export async function fetchResearchReports(params: Record<string, any>) {
  const { data } = await http.get('/api/research-reports', { params })
  return data
}

export async function searchAiRetrieval(payload: Record<string, any>) {
  const { data } = await http.post('/api/ai-retrieval/search', payload)
  return data
}
