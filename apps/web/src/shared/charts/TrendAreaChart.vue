<template>
  <div ref="chartEl" class="w-full" :style="{ height: `${height}px` }" />
</template>

<script setup lang="ts">
import { useResizeObserver } from '@vueuse/core'
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { LineChart } from 'echarts/charts'
import { GridComponent, GraphicComponent, TooltipComponent } from 'echarts/components'
import { init, use, graphic, type EChartsType } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

use([LineChart, GridComponent, TooltipComponent, GraphicComponent, CanvasRenderer])

export interface TrendChartSeries {
  name: string
  data: Array<number | null>
  color?: string
  area?: boolean
}

const props = withDefaults(defineProps<{
  labels: string[]
  series: TrendChartSeries[]
  height?: number
  emptyText?: string
}>(), {
  height: 320,
  emptyText: '暂无图表数据',
})

const chartEl = ref<HTMLElement | null>(null)
let chart: EChartsType | null = null

function ensureChart() {
  if (!chartEl.value) return null
  if (!chart) {
    chart = init(chartEl.value)
  }
  return chart
}

function render() {
  const instance = ensureChart()
  if (!instance) return

  const hasData = props.labels.length > 0 && props.series.some((item) => item.data.some((value) => value != null))
  if (!hasData) {
    instance.setOption({
      animation: false,
      xAxis: { show: false, type: 'category', data: [] },
      yAxis: { show: false, type: 'value' },
      series: [],
      graphic: {
        type: 'text',
        left: 'center',
        top: 'middle',
        style: {
          text: props.emptyText,
          fill: '#607689',
          fontSize: 13,
          fontFamily: 'MiSans, PingFang SC, Microsoft YaHei, sans-serif',
        },
      },
    }, true)
    return
  }

  instance.setOption({
    animation: false,
    grid: { top: 24, right: 18, bottom: 36, left: 48 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(18, 32, 45, 0.94)',
      borderWidth: 0,
      textStyle: { color: '#f5fbff' },
      valueFormatter: (value: unknown) => {
        const num = Number(value)
        return Number.isFinite(num) ? num.toLocaleString('zh-CN', { maximumFractionDigits: 4 }) : '-'
      },
    },
    xAxis: {
      type: 'category',
      data: props.labels,
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#c8d7df' } },
      axisLabel: { color: '#607689', hideOverlap: true },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      scale: true,
      splitLine: { lineStyle: { color: 'rgba(18, 32, 45, 0.08)' } },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        color: '#607689',
        formatter: (value: number) => value.toLocaleString('zh-CN', { maximumFractionDigits: 2 }),
      },
    },
    series: props.series.map((item) => {
      const color = item.color || '#0f617a'
      return {
        name: item.name,
        type: 'line',
        smooth: true,
        showSymbol: false,
        connectNulls: false,
        data: item.data,
        lineStyle: {
          width: 2,
          color,
        },
        itemStyle: { color },
        areaStyle: item.area ? {
          color: new graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: `${color}55` },
            { offset: 1, color: `${color}08` },
          ]),
        } : undefined,
      }
    }),
  }, true)
}

useResizeObserver(chartEl, () => {
  chart?.resize()
})

watch(
  () => [props.labels, props.series] as const,
  () => render(),
  { deep: true, immediate: true },
)

onMounted(() => {
  render()
})

onBeforeUnmount(() => {
  chart?.dispose()
  chart = null
})
</script>
