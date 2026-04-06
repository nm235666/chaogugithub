import { openAppDialog } from './dialog'

export async function confirmDangerAction(action: string, target?: string, detail?: string): Promise<boolean> {
  const message = [
    `确认要执行“${action}”吗？`,
    target ? `对象：${target}` : '',
    detail ? `说明：${detail}` : '',
  ]
    .filter(Boolean)
    .join('\n')
  const result = await openAppDialog({
    title: action,
    message,
    confirmText: '确认执行',
    cancelText: '取消',
    tone: 'danger',
  })
  return result.confirmed
}

export async function promptInputAction(title: string, label: string, placeholder = '', detail = ''): Promise<string | null> {
  const result = await openAppDialog({
    title,
    message: detail,
    confirmText: '确认',
    cancelText: '取消',
    tone: 'warning',
    input: {
      label,
      placeholder,
      required: true,
    },
  })
  if (!result.confirmed) return null
  const text = String(result.value || '').trim()
  return text || null
}

export async function infoNoticeAction(title: string, message: string): Promise<void> {
  await openAppDialog({
    title,
    message,
    confirmText: '我知道了',
    cancelText: '关闭',
    tone: 'default',
  })
}
