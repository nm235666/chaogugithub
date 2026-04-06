type QueryValue = string | number | boolean | null | undefined

export function readQueryString(query: Record<string, unknown>, key: string, fallback = ''): string {
  const value = query[key]
  if (value == null) return fallback
  const text = String(Array.isArray(value) ? value[0] : value).trim()
  return text || fallback
}

export function readQueryNumber(query: Record<string, unknown>, key: string, fallback: number): number {
  const raw = readQueryString(query, key, '')
  if (!raw) return fallback
  const parsed = Number(raw)
  return Number.isFinite(parsed) ? parsed : fallback
}

export function buildCleanQuery(input: Record<string, QueryValue>): Record<string, string> {
  const output: Record<string, string> = {}
  Object.entries(input).forEach(([key, value]) => {
    if (value == null) return
    const text = String(value).trim()
    if (!text) return
    output[key] = text
  })
  return output
}
