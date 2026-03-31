<template>
  <div class="relative overflow-hidden rounded-[24px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(238,244,247,0.86)_100%)] p-4 shadow-[var(--shadow-soft)]">
    <div class="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,rgba(15,97,122,0.18)_0%,rgba(214,134,72,0.24)_100%)]" />
    <div class="absolute -right-8 -top-8 h-24 w-24 rounded-full bg-[rgba(15,97,122,0.06)] blur-2xl" />
    <div class="text-xs uppercase tracking-[0.18em] text-[var(--muted)]">{{ title }}</div>
    <div class="mt-2 text-3xl font-extrabold tracking-tight text-[var(--ink)]">{{ value }}</div>
    <div v-if="hint" class="mt-2 text-sm leading-6 text-[var(--muted)]">{{ hint }}</div>
  </div>
</template>

<script setup lang="ts">
defineProps<{ title: string; value: string | number; hint?: string }>()
</script>
