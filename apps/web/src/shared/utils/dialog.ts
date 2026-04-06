import { reactive } from 'vue'

type DialogTone = 'default' | 'danger' | 'warning'

interface DialogInputConfig {
  label?: string
  placeholder?: string
  defaultValue?: string
  required?: boolean
}

export interface DialogOptions {
  title: string
  message?: string
  confirmText?: string
  cancelText?: string
  tone?: DialogTone
  input?: DialogInputConfig
}

interface DialogResult {
  confirmed: boolean
  value?: string
}

interface DialogState {
  open: boolean
  title: string
  message: string
  confirmText: string
  cancelText: string
  tone: DialogTone
  inputLabel: string
  inputPlaceholder: string
  inputRequired: boolean
  inputEnabled: boolean
  inputValue: string
}

export const appDialogState = reactive<DialogState>({
  open: false,
  title: '',
  message: '',
  confirmText: '确认',
  cancelText: '取消',
  tone: 'default',
  inputLabel: '',
  inputPlaceholder: '',
  inputRequired: false,
  inputEnabled: false,
  inputValue: '',
})

let resolver: ((result: DialogResult) => void) | null = null

function closeDialog(result: DialogResult) {
  if (resolver) {
    resolver(result)
    resolver = null
  }
  appDialogState.open = false
}

export function openAppDialog(options: DialogOptions): Promise<DialogResult> {
  if (resolver) {
    closeDialog({ confirmed: false })
  }
  appDialogState.title = options.title
  appDialogState.message = options.message || ''
  appDialogState.confirmText = options.confirmText || '确认'
  appDialogState.cancelText = options.cancelText || '取消'
  appDialogState.tone = options.tone || 'default'
  appDialogState.inputEnabled = !!options.input
  appDialogState.inputLabel = options.input?.label || '请输入内容'
  appDialogState.inputPlaceholder = options.input?.placeholder || ''
  appDialogState.inputRequired = !!options.input?.required
  appDialogState.inputValue = options.input?.defaultValue || ''
  appDialogState.open = true
  return new Promise<DialogResult>((resolve) => {
    resolver = resolve
  })
}

export function confirmAppDialog() {
  if (appDialogState.inputEnabled) {
    const text = appDialogState.inputValue.trim()
    if (appDialogState.inputRequired && !text) return
    closeDialog({ confirmed: true, value: text })
    return
  }
  closeDialog({ confirmed: true })
}

export function cancelAppDialog() {
  closeDialog({ confirmed: false })
}
