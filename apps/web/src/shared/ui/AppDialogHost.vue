<template>
  <Teleport to="body">
    <div v-if="state.open" class="fixed inset-0 z-[1500] flex items-center justify-center p-4" @keydown="onKeydown">
      <button class="absolute inset-0 bg-[rgba(8,16,22,0.48)]" aria-label="关闭对话框" @click="onCancel" />
      <div
        ref="dialogRef"
        class="relative z-10 w-full max-w-[560px] rounded-[24px] border bg-[linear-gradient(180deg,rgba(255,255,255,0.98)_0%,rgba(238,244,247,0.96)_100%)] p-5 shadow-2xl"
        :class="toneClass"
        role="dialog"
        aria-modal="true"
        :aria-labelledby="titleId"
        tabindex="-1"
      >
        <div :id="titleId" class="text-xl font-extrabold tracking-tight text-[var(--ink)]" style="font-family: var(--font-display)">
          {{ state.title }}
        </div>
        <div v-if="state.message" class="mt-2 whitespace-pre-line text-sm text-[var(--muted)]">{{ state.message }}</div>
        <div v-if="state.inputEnabled" class="mt-4">
          <label class="text-sm font-semibold text-[var(--ink)]">{{ state.inputLabel }}</label>
          <input
            v-model="state.inputValue"
            class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
            :placeholder="state.inputPlaceholder"
            @keyup.enter="onConfirm"
          />
        </div>
        <div class="mt-5 flex flex-wrap justify-end gap-2">
          <button type="button" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-2 font-semibold text-[var(--ink)]" @click="onCancel">
            {{ state.cancelText }}
          </button>
          <button
            type="button"
            class="rounded-2xl px-4 py-2 font-semibold text-white"
            :class="confirmButtonClass"
            :disabled="state.inputEnabled && state.inputRequired && !state.inputValue.trim()"
            @click="onConfirm"
          >
            {{ state.confirmText }}
          </button>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { appDialogState as state, cancelAppDialog, confirmAppDialog } from '../utils/dialog'

const dialogRef = ref<HTMLElement | null>(null)
const titleId = `app-dialog-title-${Math.random().toString(36).slice(2, 10)}`
let lastFocused: HTMLElement | null = null

const toneClass = computed(() => {
  if (state.tone === 'danger') return 'border-[rgba(178,77,84,0.26)]'
  if (state.tone === 'warning') return 'border-[rgba(173,108,34,0.24)]'
  return 'border-[var(--line)]'
})

const confirmButtonClass = computed(() => {
  if (state.tone === 'danger') return 'bg-[var(--danger)]'
  if (state.tone === 'warning') return 'bg-[var(--warning)]'
  return 'bg-[var(--brand)]'
})

function focusFirst() {
  const root = dialogRef.value
  if (!root) return
  const focusables = root.querySelectorAll<HTMLElement>(
    'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
  )
  const target = focusables[0] || root
  target.focus()
}

function onCancel() {
  cancelAppDialog()
}

function onConfirm() {
  confirmAppDialog()
}

function onKeydown(event: KeyboardEvent) {
  if (!state.open) return
  if (event.key === 'Escape') {
    event.preventDefault()
    onCancel()
    return
  }
  if (event.key !== 'Tab') return
  const root = dialogRef.value
  if (!root) return
  const focusables = Array.from(
    root.querySelectorAll<HTMLElement>(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
    ),
  ).filter((el) => !el.hasAttribute('disabled'))
  if (!focusables.length) return
  const first = focusables[0]
  const last = focusables[focusables.length - 1]
  const active = document.activeElement as HTMLElement | null
  if (event.shiftKey && active === first) {
    event.preventDefault()
    last.focus()
  } else if (!event.shiftKey && active === last) {
    event.preventDefault()
    first.focus()
  }
}

watch(
  () => state.open,
  async (open) => {
    if (open) {
      lastFocused = document.activeElement as HTMLElement | null
      await nextTick()
      focusFirst()
      return
    }
    lastFocused?.focus?.()
  },
)
</script>
