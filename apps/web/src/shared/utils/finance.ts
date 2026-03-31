const IMPORTANCE_LEVELS = ['极高', '高', '中', '低', '极低'] as const

export function importanceOptions() {
  return [...IMPORTANCE_LEVELS]
}

export function parseJsonObject(raw: unknown): Record<string, any> {
  if (!raw) return {}
  if (typeof raw === 'object' && !Array.isArray(raw)) return raw as Record<string, any>
  try {
    const parsed = JSON.parse(String(raw))
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

export function parseJsonArray(raw: unknown): Array<any> {
  if (!raw) return []
  if (Array.isArray(raw)) return raw as Array<any>
  try {
    const parsed = JSON.parse(String(raw))
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

export function parseImpactTags(raw: unknown) {
  const obj = parseJsonObject(raw)
  const groups = [
    { key: 'macro', label: '宏观' },
    { key: 'markets', label: '市场' },
    { key: 'sectors', label: '板块' },
  ]
  const tags: Array<{ group: string; label: string; direction: string }> = []
  groups.forEach((group) => {
    const items: Array<any> = Array.isArray(obj[group.key]) ? obj[group.key] : []
    items.forEach((item) => {
      if (!item || typeof item !== 'object') return
      const label = String(item.item || item.name || item.target || '').trim()
      if (!label) return
      tags.push({
        group: group.label,
        label,
        direction: String(item.direction || item.sentiment || '中性').trim() || '中性',
      })
    })
  })
  return tags
}

export function parseRelatedStocks(rawNames: unknown, rawCodes: unknown) {
  const names = parseJsonArray(rawNames)
  const codes = parseJsonArray(rawCodes)
  const out: Array<{ ts_code: string; name: string }> = []
  const seen = new Set<string>()
  names.forEach((item: any) => {
    const ts_code = String(item.ts_code || '').trim().toUpperCase()
    const name = String(item.name || item.stock_name || '').trim()
    const key = ts_code || name
    if (!key || seen.has(key)) return
    seen.add(key)
    out.push({ ts_code, name })
  })
  codes.forEach((item: any) => {
    const ts_code = String(item || '').trim().toUpperCase()
    if (!ts_code || seen.has(ts_code)) return
    seen.add(ts_code)
    out.push({ ts_code, name: '' })
  })
  return out
}

export function sourceLabel(value: unknown) {
  const raw = String(value || '').trim()
  const mapped: Record<string, string> = {
    cn_sina_7x24: '新浪7x24',
    cn_eastmoney_fastnews: '东方财富快讯',
    marketscreener_byd_news: 'MarketScreener BYD',
    marketscreener_live_news: 'MarketScreener Live',
    eastmoney_stock_news: '东方财富个股新闻',
  }
  return mapped[raw] || raw || '-'
}
