import { http } from '../http'
import type { DashboardPayload } from '../../shared/types/api'

export async function fetchDashboard() {
  const { data } = await http.get<DashboardPayload>('/api/dashboard')
  return data
}

export async function fetchSourceMonitor() {
  const { data } = await http.get('/api/source-monitor')
  return data
}

export async function fetchDatabaseAudit() {
  const { data } = await http.get('/api/database-audit')
  return data
}

export async function fetchDbHealth() {
  const { data } = await http.get('/api/db-health')
  return data
}
