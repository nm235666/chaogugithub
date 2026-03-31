import { http } from '../http'

export async function fetchSignalQualityConfig() {
  const { data } = await http.get('/api/signal-quality/config')
  return data
}

export async function saveSignalQualityRules(items: Array<Record<string, any>>) {
  const { data } = await http.post('/api/signal-quality/rules/save', { items })
  return data
}

export async function saveSignalQualityBlocklist(items: Array<Record<string, any>>) {
  const { data } = await http.post('/api/signal-quality/blocklist/save', { items })
  return data
}

export async function fetchJobs() {
  const { data } = await http.get('/api/jobs')
  return data
}

export async function fetchJobRuns(params: Record<string, any>) {
  const { data } = await http.get('/api/job-runs', { params })
  return data
}

export async function fetchJobAlerts(params: Record<string, any>) {
  const { data } = await http.get('/api/job-alerts', { params })
  return data
}

export async function triggerJob(job_key: string) {
  const { data } = await http.get('/api/jobs/trigger', { params: { job_key } })
  return data
}

export async function dryRunJob(job_key: string) {
  const { data } = await http.get('/api/jobs/dry-run', { params: { job_key } })
  return data
}
