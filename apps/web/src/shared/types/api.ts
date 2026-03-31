export interface DashboardPayload {
  generated_at: string
  overview: Record<string, number>
  source_monitor?: {
    summary?: Record<string, number | string>
    sources?: Array<Record<string, any>>
  }
  async_jobs?: Record<string, number>
  orchestrator_alerts?: Array<Record<string, any>>
  orchestrator_jobs?: Array<Record<string, any>>
  database_health?: Record<string, any>
  top_scores?: Array<Record<string, any>>
  candidate_pool_top?: Array<Record<string, any>>
  recent_daily_summaries?: Array<Record<string, any>>
  important_news?: Array<Record<string, any>>
}

export interface PaginatedResponse<T = Record<string, any>> {
  items: T[]
  total: number
  page?: number
  page_size?: number
  total_pages?: number
}
