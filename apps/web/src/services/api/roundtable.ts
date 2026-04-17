import { http } from '../http'

export async function createRoundtableJob(payload: {
  ts_code: string
  trigger?: string
  source_job_id?: string
}) {
  const { data } = await http.post('/api/llm/chief-roundtable/jobs', payload)
  return data
}

export async function getRoundtableJob(jobId: string) {
  const { data } = await http.get(`/api/llm/chief-roundtable/jobs/${jobId}`)
  return data
}

export async function listRoundtableJobs(params: Record<string, any> = {}) {
  const { data } = await http.get('/api/llm/chief-roundtable/jobs', { params })
  return data
}
