<template>
  <div class="grid gap-3" :class="columnsClass">
    <div
      v-for="item in items"
      :key="`${item.label}-${item.value}`"
      class="rounded-[20px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.96)_0%,rgba(238,244,247,0.86)_100%)] p-4 shadow-[var(--shadow-soft)]"
    >
      <div class="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">{{ item.label }}</div>
      <div class="mt-2 text-lg font-bold text-[var(--ink)]">{{ item.value || '-' }}</div>
      <div v-if="item.hint" class="mt-2 text-sm leading-6 text-[var(--muted)]">{{ item.hint }}</div>
    </div>
    <div
      v-if="!items.length"
      class="rounded-[20px] border border-dashed border-[var(--line)] bg-[rgba(255,255,255,0.52)] px-4 py-8 text-center text-sm text-[var(--muted)]"
    >
      {{ emptyText }}
    </div>
  </div>
</template>

<script setup lang="ts">
export interface MetricGridItem {
  label: string
  value?: string | number | null
  hint?: string
}

withDefaults(defineProps<{
  items: MetricGridItem[]
  emptyText?: string
  columnsClass?: string
}>(), {
  emptyText: '暂无数据',
  columnsClass: 'xl:grid-cols-4 md:grid-cols-2',
})
</script>
