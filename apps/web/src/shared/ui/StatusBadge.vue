<template>
  <span :class="badgeClass">{{ label }}</span>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { statusTone } from '../utils/format'

const props = defineProps<{ value?: string; label?: string }>()

const tone = computed(() => statusTone(props.value || ''))
const label = computed(() => props.label || props.value || '-')
const badgeClass = computed(() => {
  const base = 'inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold'
  return {
    success: `${base} border-[rgba(31,122,100,0.28)] bg-[rgba(31,122,100,0.1)] text-[var(--success)]`,
    warning: `${base} border-[rgba(154,99,34,0.26)] bg-[rgba(154,99,34,0.1)] text-[var(--warning)]`,
    danger: `${base} border-[rgba(160,67,69,0.24)] bg-[rgba(160,67,69,0.1)] text-[var(--danger)]`,
    info: `${base} border-[rgba(47,111,140,0.24)] bg-[rgba(47,111,140,0.1)] text-[var(--info)]`,
    muted: `${base} border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]`,
  }[tone.value]
})
</script>
