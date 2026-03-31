import { http } from '../http'

export async function fetchResearchReports(params: Record<string, any>) {
  const { data } = await http.get('/api/research-reports', { params })
  return data
}
