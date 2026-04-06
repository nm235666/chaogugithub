<template>
  <Teleport to="body">
    <div v-if="open" class="fixed inset-0 z-50 flex justify-end" @keydown="onKeydown">
      <div class="absolute inset-0 bg-[rgba(8,16,22,0.48)] backdrop-blur-sm" @click="$emit('close')" />
      <aside
        ref="drawerRef"
        class="relative z-10 h-full w-full max-w-[760px] overflow-y-auto border-l border-[var(--line)] bg-[linear-gradient(180deg,rgba(248,251,252,0.98)_0%,rgba(238,244,247,0.96)_100%)] p-5 shadow-2xl"
        role="dialog"
        aria-modal="true"
        :aria-labelledby="titleId"
        tabindex="-1"
      >
        <div class="mb-4 flex items-start justify-between gap-4">
          <div>
            <div v-if="eyebrow" class="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--muted)]">{{ eyebrow }}</div>
            <div :id="titleId" class="mt-1 text-2xl font-extrabold text-[var(--ink)]" style="font-family: var(--font-display)">{{ title }}</div>
            <div v-if="subtitle" class="mt-2 text-sm text-[var(--muted)]">{{ subtitle }}</div>
          </div>
          <button class="rounded-2xl border border-[var(--line)] bg-white px-3 py-2 text-sm font-semibold text-[var(--ink)]" @click="$emit('close')">关闭</button>
        </div>
        <slot />
      </aside>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { nextTick, ref, watch } from 'vue'

const props = defineProps<{
  open: boolean
  title: string
  subtitle?: string
  eyebrow?: string
}>()

const emit = defineEmits<{ close: [] }>()
const drawerRef = ref<HTMLElement | null>(null)
const titleId = `detail-drawer-title-${Math.random().toString(36).slice(2, 10)}`
let lastFocused: HTMLElement | null = null

function focusFirstInDrawer() {
  const drawer = drawerRef.value
  if (!drawer) return
  const focusables = drawer.querySelectorAll<HTMLElement>(
    'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
  )
  const target = focusables[0] || drawer
  target.focus()
}

function onKeydown(event: KeyboardEvent) {
  if (!props.open) return
  if (event.key === 'Escape') {
    event.preventDefault()
    emit('close')
    return
  }
  if (event.key !== 'Tab') return
  const drawer = drawerRef.value
  if (!drawer) return
  const focusables = Array.from(
    drawer.querySelectorAll<HTMLElement>(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
    ),
  ).filter((el) => !el.hasAttribute('disabled'))
  if (!focusables.length) {
    event.preventDefault()
    drawer.focus()
    return
  }
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
  () => props.open,
  async (open) => {
    if (open) {
      lastFocused = document.activeElement as HTMLElement | null
      await nextTick()
      focusFirstInDrawer()
      return
    }
    lastFocused?.focus?.()
  },
  { immediate: true },
)
</script>
