import { computed, ref } from 'vue'
import {
  clearTaskSnapshot,
  gcTaskSnapshots,
  loadTaskSnapshot,
  saveTaskSnapshot,
  type PersistedTaskSnapshotInput,
} from './taskPersistence'

function formatAgo(ms: number) {
  const delta = Math.max(0, Date.now() - ms)
  const minutes = Math.floor(delta / 60000)
  if (minutes < 1) return '刚刚'
  if (minutes < 60) return `${minutes} 分钟前`
  const hours = Math.floor(minutes / 60)
  const rem = minutes % 60
  if (!rem) return `${hours} 小时前`
  return `${hours} 小时 ${rem} 分钟前`
}

export function usePersistedTaskRunner(scopeKeyGetter: () => string) {
  const restoredAtMs = ref(0)
  const noticeMessage = ref('')

  function restoreTaskSnapshot() {
    gcTaskSnapshots()
    const { snapshot, expired, invalid } = loadTaskSnapshot(scopeKeyGetter())
    if (snapshot) {
      restoredAtMs.value = Date.now()
      noticeMessage.value = ''
      return snapshot
    }
    restoredAtMs.value = 0
    if (expired) {
      noticeMessage.value = '历史任务已过期，请重新发起。'
    } else if (invalid) {
      noticeMessage.value = '历史任务无效，已自动清理。'
    } else {
      noticeMessage.value = ''
    }
    return null
  }

  function persistTaskSnapshot(input: PersistedTaskSnapshotInput) {
    noticeMessage.value = ''
    return saveTaskSnapshot(scopeKeyGetter(), input)
  }

  function clearPersistedTaskSnapshot() {
    clearTaskSnapshot(scopeKeyGetter())
    restoredAtMs.value = 0
    noticeMessage.value = ''
  }

  const restoredHint = computed(() => (restoredAtMs.value ? formatAgo(restoredAtMs.value) : ''))

  return {
    restoredHint,
    noticeMessage,
    restoreTaskSnapshot,
    persistTaskSnapshot,
    clearPersistedTaskSnapshot,
  }
}

