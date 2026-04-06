export const TASK_SNAPSHOT_TTL_MS = 2 * 60 * 60 * 1000

const TASK_STORAGE_KEY = 'zanbo.web.task-snapshots.v1'
const TASK_STORAGE_PREFIX = 'task:'

export type PersistedTaskSnapshot = {
  scopeKey: string
  jobId: string
  status: string
  stage: string
  progress: number
  requestedModel: string
  usedModel: string
  attempts: Array<Record<string, any>>
  actionMessage: string
  error: string
  updatedAt: string
  expiresAt: number
  data?: Record<string, any>
}

export type PersistedTaskSnapshotInput = Partial<Omit<PersistedTaskSnapshot, 'scopeKey' | 'expiresAt'>> & {
  jobId: string
  expiresAt?: number
}

export type TaskSnapshotLoadResult = {
  snapshot: PersistedTaskSnapshot | null
  expired: boolean
  invalid: boolean
}

function canUseSessionStorage() {
  return typeof window !== 'undefined' && !!window.sessionStorage
}

function safeParseStorage(raw: string | null): Record<string, PersistedTaskSnapshot> {
  if (!raw) return {}
  try {
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {}
    return parsed as Record<string, PersistedTaskSnapshot>
  } catch {
    return {}
  }
}

function readStorageMap() {
  if (!canUseSessionStorage()) return {}
  return safeParseStorage(window.sessionStorage.getItem(TASK_STORAGE_KEY))
}

function writeStorageMap(map: Record<string, PersistedTaskSnapshot>) {
  if (!canUseSessionStorage()) return
  window.sessionStorage.setItem(TASK_STORAGE_KEY, JSON.stringify(map))
}

export function buildTaskScopeKey(routeName: string, businessKey = 'default') {
  const route = String(routeName || '').trim() || 'unknown'
  const biz = String(businessKey || '').trim() || 'default'
  return `${TASK_STORAGE_PREFIX}${route}:${biz}`
}

export function saveTaskSnapshot(scopeKey: string, input: PersistedTaskSnapshotInput) {
  if (!scopeKey || !input?.jobId) return null
  const map = readStorageMap()
  const now = Date.now()
  const snapshot: PersistedTaskSnapshot = {
    scopeKey,
    jobId: String(input.jobId || ''),
    status: String(input.status || ''),
    stage: String(input.stage || ''),
    progress: Number(input.progress || 0),
    requestedModel: String(input.requestedModel || ''),
    usedModel: String(input.usedModel || ''),
    attempts: Array.isArray(input.attempts) ? input.attempts : [],
    actionMessage: String(input.actionMessage || ''),
    error: String(input.error || ''),
    updatedAt: String(input.updatedAt || new Date().toISOString()),
    data: input.data && typeof input.data === 'object' ? input.data : undefined,
    expiresAt: Number(input.expiresAt || now + TASK_SNAPSHOT_TTL_MS),
  }
  map[scopeKey] = snapshot
  writeStorageMap(map)
  return snapshot
}

export function loadTaskSnapshot(scopeKey: string): TaskSnapshotLoadResult {
  if (!scopeKey) return { snapshot: null, expired: false, invalid: false }
  const map = readStorageMap()
  const item = map[scopeKey]
  if (!item) return { snapshot: null, expired: false, invalid: false }
  if (!item.jobId || !item.scopeKey || !item.expiresAt) {
    delete map[scopeKey]
    writeStorageMap(map)
    return { snapshot: null, expired: false, invalid: true }
  }
  if (Date.now() > Number(item.expiresAt || 0)) {
    delete map[scopeKey]
    writeStorageMap(map)
    return { snapshot: null, expired: true, invalid: false }
  }
  return { snapshot: item, expired: false, invalid: false }
}

export function clearTaskSnapshot(scopeKey: string) {
  if (!scopeKey) return
  const map = readStorageMap()
  if (!map[scopeKey]) return
  delete map[scopeKey]
  writeStorageMap(map)
}

export function gcTaskSnapshots() {
  const map = readStorageMap()
  const now = Date.now()
  let changed = false
  Object.keys(map).forEach((key) => {
    const item = map[key]
    if (!item || !item.scopeKey || !item.jobId || now > Number(item.expiresAt || 0)) {
      delete map[key]
      changed = true
    }
  })
  if (changed) writeStorageMap(map)
}

