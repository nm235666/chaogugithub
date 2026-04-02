import { http } from '../http'

export type QuantFactorsStartPayload = {
  direction: string
  market_scope?: string
  lookback?: number
  config_profile?: string
  llm_profile?: string
  extra_args?: string[]
}

export async function startQuantMine(payload: QuantFactorsStartPayload) {
  return callQuantFactorsApi('post', '/mine/start', payload)
}

export async function startQuantBacktest(payload: QuantFactorsStartPayload) {
  return callQuantFactorsApi('post', '/backtest/start', payload)
}

export async function fetchQuantTask(task_id: string) {
  return callQuantFactorsApi('get', '/task', undefined, { task_id })
}

export async function fetchQuantResults(params: Record<string, any>) {
  return callQuantFactorsApi('get', '/results', undefined, params)
}

type QuantApiMethod = 'get' | 'post'

function quantApiCandidates(suffix: string): string[] {
  const list = [`/api/quant-factors${suffix}`]
  if (typeof window !== 'undefined') {
    const protocol = window.location.protocol || 'http:'
    const host = window.location.hostname || '127.0.0.1'
    list.push(`${protocol}//${host}:8077/api/quant-factors${suffix}`)
    list.push(`${protocol}//${host}:8002/api/quant-factors${suffix}`)
  }
  return Array.from(new Set(list))
}

async function callQuantFactorsApi(method: QuantApiMethod, suffix: string, payload?: any, params?: Record<string, any>) {
  const urls = quantApiCandidates(suffix)
  let lastError: any = null
  for (const url of urls) {
    try {
      if (method === 'get') {
        const { data } = await http.get(url, { params })
        return data
      }
      const { data } = await http.post(url, payload || {})
      return data
    } catch (error: any) {
      lastError = error
      const status = Number(error?.status || error?.response?.status || 0)
      if (status === 401 || status === 403) throw error
      if (status && status !== 404) throw error
      continue
    }
  }
  throw lastError || new Error('quant-factors API unavailable')
}
