import axios from 'axios'

const resolvedBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim() || ''
const bundledAdminToken = import.meta.env.VITE_ADMIN_API_TOKEN?.trim() || ''

function readAdminToken() {
  if (typeof window === 'undefined') {
    return bundledAdminToken
  }
  return (
    window.localStorage.getItem('zanbo_admin_token')?.trim() ||
    window.sessionStorage.getItem('zanbo_admin_token')?.trim() ||
    bundledAdminToken
  )
}

export const http = axios.create({
  baseURL: resolvedBaseUrl,
  timeout: 20_000,
})

http.interceptors.request.use((config) => {
  const token = readAdminToken()
  if (token) {
    config.headers = config.headers || {}
    config.headers['X-Admin-Token'] = token
  }
  return config
})

http.interceptors.response.use(
  (response) => response,
  (error) => {
    const message = error?.response?.data?.error || error?.message || '请求失败'
    return Promise.reject(new Error(message))
  },
)
