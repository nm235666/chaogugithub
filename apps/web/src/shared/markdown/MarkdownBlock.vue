<template>
  <div class="prose max-w-none rounded-[24px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.95)_0%,rgba(238,244,247,0.88)_100%)] p-5 shadow-[var(--shadow-soft)]" v-html="rendered"></div>
</template>

<script setup lang="ts">
import MarkdownIt from 'markdown-it'
import { computed } from 'vue'

const props = defineProps<{ content?: string }>()
const md = new MarkdownIt({ html: false, linkify: true, breaks: true })
const rendered = computed(() => md.render(props.content || ''))
</script>
