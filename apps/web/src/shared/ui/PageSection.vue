<template>
  <section class="relative overflow-hidden rounded-[28px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(255,255,255,0.72)_100%)] p-4 shadow-[var(--shadow)] backdrop-blur-xl">
    <div class="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent_0%,rgba(15,97,122,0.26)_32%,rgba(214,134,72,0.22)_68%,transparent_100%)]" />
    <div v-if="title || subtitle || $slots.action" class="mb-4 flex flex-wrap items-end justify-between gap-3">
      <div>
        <div v-if="eyebrow" class="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--muted)]">{{ eyebrow }}</div>
        <h2 v-if="title" class="mt-1 text-xl font-extrabold tracking-tight" style="font-family: var(--font-display)">{{ title }}</h2>
        <p v-if="subtitle" class="mt-1 text-sm text-[var(--muted)]">{{ subtitle }}</p>
      </div>
      <div v-if="$slots.action"><slot name="action" /></div>
    </div>
    <slot />
  </section>
</template>

<script setup lang="ts">
defineProps<{ title?: string; subtitle?: string; eyebrow?: string }>()
</script>
