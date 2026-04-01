import axios from 'axios'
import { readAdminToken } from './authToken'

const resolvedBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim() || ''
const bundledAdminToken = import.meta.env.VITE_ADMIN_API_TOKEN?.trim() || ''

export const http = axios.create({
  baseURL: resolvedBaseUrl,
  timeout: 20_000,
})

http.interceptors.request.use((config) => {
  const token = readAdminToken() || bundledAdminToken
  if (token) {
    config.headers = config.headers || {}
    config.headers['X-Admin-Token'] = token
  }
  return config
})

http.interceptors.response.use(
  (response) => response,
  (error) => {
    const data = error?.response?.data || {}
    const code = data?.code ? ` [${data.code}]` : ''
    const hint = data?.hint ? `（${data.hint}）` : ''
    const message = `${data?.error || error?.message || '请求失败'}${code}${hint}`
    return Promise.reject(new Error(message))
  },
)
