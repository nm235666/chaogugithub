<template>
  <RouterView />
</template>

<script setup lang="ts">
import { RouterView } from 'vue-router'
import { useRealtimeBus } from './shared/realtime/useRealtimeBus'

useRealtimeBus()
</script>
