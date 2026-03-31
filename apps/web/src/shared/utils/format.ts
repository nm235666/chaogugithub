export function formatNumber(value: unknown, digits = 0): string {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return num.toLocaleString('zh-CN', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

export function formatPercent(value: unknown, digits = 2): string {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${formatNumber(num, digits)}%`
}

export function formatDateTime(value: unknown): string {
  const text = String(value ?? '').trim()
  if (!text) return '-'
  return text.replace('T', ' ').replace('Z', ' UTC')
}

export function formatDate(value: unknown): string {
  const text = String(value ?? '').trim()
  if (!text) return '-'
  if (/^\d{8}$/.test(text)) {
    return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`
  }
  return text.slice(0, 10)
}

export function statusTone(status: string): 'success' | 'warning' | 'danger' | 'info' | 'muted' {
  if (status === 'ok' || status === 'success' || status === '看多') return 'success'
  if (status === 'warn' || status === 'warning' || status === '中' || status === '高') return 'warning'
  if (status === 'error' || status === 'danger' || status === '看空' || status === '极高') return 'danger'
  if (status === 'running' || status === 'brand') return 'info'
  return 'muted'
}

export function listStatusLabel(value: string): string {
  return ({ L: '上市', D: '退市', P: '暂停' } as Record<string, string>)[value] || value || '-'
}
