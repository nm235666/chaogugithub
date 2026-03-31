<template>
  <div class="relative overflow-hidden rounded-[22px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.82)_100%)] p-4 shadow-[var(--shadow-soft)] transition hover:-translate-y-0.5 hover:shadow-[var(--shadow)]">
    <div class="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent_0%,rgba(15,97,122,0.2)_35%,rgba(214,134,72,0.16)_75%,transparent_100%)]" />
    <div class="flex items-start justify-between gap-3">
      <div>
        <div class="text-base font-bold text-[var(--ink)]">{{ title }}</div>
        <div v-if="meta" class="mt-1 text-sm text-[var(--muted)]">{{ meta }}</div>
      </div>
      <slot name="badge" />
    </div>
    <div v-if="description" class="mt-3 text-sm leading-7 text-[var(--muted)]">{{ description }}</div>
    <div v-if="$slots.default" class="mt-3"><slot /></div>
  </div>
</template>

<script setup lang="ts">
defineProps<{ title: string; meta?: string; description?: string }>()
</script>
