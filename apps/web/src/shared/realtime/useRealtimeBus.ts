import { onMounted, onUnmounted } from 'vue'
import { useRealtimeStore } from '../../stores/realtime'
import { queryClient } from '../query/client'

const EVENT_QUERY_KEYS: Array<[string, readonly unknown[]]> = [
  ['job_run_update', ['dashboard']],
  ['stock_news_update', ['stock-news']],
  ['news_stock_map_update', ['news']],
  ['llm_sentiment_update', ['news']],
  ['chatlog_monitor_update', ['chatrooms']],
  ['chatroom_candidate_pool_update', ['candidate-pool']],
  ['stock_scores_update', ['stock-scores']],
  ['daily_summary_job_update', ['daily-summaries']],
  ['multi_role_job_update', ['dashboard']],
]

export function useRealtimeBus() {
  const realtime = useRealtimeStore()
  let socket: WebSocket | null = null
  let retryTimer = 0

  const wsCandidates = (() => {
    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const host = window.location.hostname || '127.0.0.1'
    return [...new Set([
      `${scheme}://${host}:8077/ws/realtime`,
      `${scheme}://${window.location.host}/ws/realtime`,
    ])]
  })()

  const connect = (index = 0) => {
    const target = wsCandidates[index % wsCandidates.length]
    socket = new WebSocket(target)
    socket.onopen = () => realtime.setConnected(true)
    socket.onclose = () => {
      realtime.setConnected(false)
      window.clearTimeout(retryTimer)
      retryTimer = window.setTimeout(() => connect(index + 1), 3000)
    }
    socket.onerror = () => {}
    socket.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data)
        const eventName = msg?.event || msg?.channel || ''
        const payloadAt = msg?.payload?.created_at || msg?.ts || ''
        realtime.pushEvent(eventName, payloadAt)
        EVENT_QUERY_KEYS.forEach(([evt, key]) => {
          if (evt === eventName) queryClient.invalidateQueries({ queryKey: key })
        })
        if (msg?.channel === 'news' || msg?.channel === 'app') {
          queryClient.invalidateQueries({ queryKey: ['dashboard'] })
          queryClient.invalidateQueries({ queryKey: ['source-monitor'] })
        }
      } catch {
        // ignore malformed ws payloads
      }
    }
  }

  onMounted(() => connect())
  onUnmounted(() => {
    window.clearTimeout(retryTimer)
    socket?.close()
  })
}
