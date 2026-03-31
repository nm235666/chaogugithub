<template>
  <Teleport to="body">
    <div v-if="open" class="fixed inset-0 z-50 flex justify-end">
      <div class="absolute inset-0 bg-[rgba(8,16,22,0.48)] backdrop-blur-sm" @click="$emit('close')" />
      <aside class="relative z-10 h-full w-full max-w-[760px] overflow-y-auto border-l border-[var(--line)] bg-[linear-gradient(180deg,rgba(248,251,252,0.98)_0%,rgba(238,244,247,0.96)_100%)] p-5 shadow-2xl">
        <div class="mb-4 flex items-start justify-between gap-4">
          <div>
            <div v-if="eyebrow" class="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--muted)]">{{ eyebrow }}</div>
            <div class="mt-1 text-2xl font-extrabold text-[var(--ink)]" style="font-family: var(--font-display)">{{ title }}</div>
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
defineProps<{
  open: boolean
  title: string
  subtitle?: string
  eyebrow?: string
}>()

defineEmits<{ close: [] }>()
</script>
