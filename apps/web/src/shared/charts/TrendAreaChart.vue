<template>
  <div class="w-full" role="img" :aria-label="ariaLabel" :aria-describedby="summaryId">
    <div ref="chartEl" class="w-full" :style="{ height: `${height}px` }" />
    <p :id="summaryId" class="sr-only">{{ summaryText }}</p>
  </div>
</template>

<script setup lang="ts">
import { useResizeObserver } from '@vueuse/core'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
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
let renderTimer: ReturnType<typeof setTimeout> | null = null
const summaryId = `trend-chart-summary-${Math.random().toString(36).slice(2, 10)}`

function sampleByStep<T>(items: T[], max = 500): T[] {
  if (items.length <= max) return items
  const step = Math.max(1, Math.ceil(items.length / max))
  const sampled: T[] = []
  for (let index = 0; index < items.length; index += step) {
    sampled.push(items[index])
  }
  const last = items[items.length - 1]
  if (sampled[sampled.length - 1] !== last) sampled.push(last)
  return sampled
}

const summaryText = computed(() => {
  const sample = props.labels.length
  const names = props.series.map((item) => item.name).filter(Boolean).join('、') || '趋势序列'
  if (!sample) return `${props.emptyText}。`
  const start = props.labels[0] || '-'
  const end = props.labels[sample - 1] || '-'
  return `图表包含 ${names}，样本 ${sample} 个，范围 ${start} 到 ${end}。`
})
const ariaLabel = computed(() => `${props.series.map((item) => item.name).filter(Boolean).join(' / ') || '趋势图'}图表`)

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
  const sampledLabels = sampleByStep(props.labels, 500)
  const sampledSeries = props.series.map((series) => {
    const sampledData = sampleByStep(series.data, 500)
    return {
      ...series,
      data: sampledData,
    }
  })

  const hasData = sampledLabels.length > 0 && sampledSeries.some((item) => item.data.some((value) => value != null))
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
      data: sampledLabels,
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
    series: sampledSeries.map((item) => {
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

function scheduleRender() {
  if (renderTimer) clearTimeout(renderTimer)
  renderTimer = setTimeout(() => {
    render()
    renderTimer = null
  }, 120)
}

useResizeObserver(chartEl, () => {
  chart?.resize()
})

watch(
  () => [props.labels, props.series] as const,
  () => scheduleRender(),
  { deep: true, immediate: true },
)

onMounted(() => {
  scheduleRender()
})

onBeforeUnmount(() => {
  if (renderTimer) clearTimeout(renderTimer)
  chart?.dispose()
  chart = null
})
</script>
