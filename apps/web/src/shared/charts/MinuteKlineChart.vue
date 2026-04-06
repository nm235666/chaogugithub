<template>
  <div class="w-full" role="img" :aria-label="ariaLabel" :aria-describedby="summaryId">
    <div ref="chartEl" class="w-full" :style="{ height: `${height}px` }" />
    <p :id="summaryId" class="sr-only">{{ summaryText }}</p>
  </div>
</template>

<script setup lang="ts">
import { useResizeObserver } from '@vueuse/core'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { BarChart, CandlestickChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { init, use, type EChartsType } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

use([CandlestickChart, LineChart, BarChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer])

interface MinlineRow {
  trade_date?: string
  minute_time?: string
  price?: number | string | null
  avg_price?: number | string | null
  volume?: number | string | null
}

const props = withDefaults(defineProps<{
  items: MinlineRow[]
  height?: number
  emptyText?: string
}>(), {
  height: 560,
  emptyText: '暂无分钟线数据',
})

const chartEl = ref<HTMLElement | null>(null)
let chart: EChartsType | null = null
let renderTimer: ReturnType<typeof setTimeout> | null = null
const summaryId = `minute-chart-summary-${Math.random().toString(36).slice(2, 10)}`

function toNumberOrNull(value: unknown): number | null {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

const normalized = computed(() => {
  const sorted = [...props.items].sort((a, b) => {
    const ad = String(a.trade_date || '')
    const bd = String(b.trade_date || '')
    if (ad !== bd) return ad.localeCompare(bd)
    return String(a.minute_time || '').localeCompare(String(b.minute_time || ''))
  })
  const rows = sorted.length > 1400
    ? sorted.filter((_, index) => index % Math.ceil(sorted.length / 1400) === 0)
    : sorted
  const categories: string[] = []
  const candles: Array<[number, number, number, number]> = []
  const avgLine: Array<number | null> = []
  const volumeBars: Array<{ value: number; itemStyle: { color: string } }> = []
  let prevClose: number | null = null

  rows.forEach((item) => {
    const close = toNumberOrNull(item.price)
    const avgPrice = toNumberOrNull(item.avg_price)
    const volume = toNumberOrNull(item.volume)
    if (close == null) return
    const open = prevClose == null ? close : prevClose
    const high = Math.max(open, close)
    const low = Math.min(open, close)
    const label = [String(item.trade_date || ''), String(item.minute_time || '')].filter(Boolean).join(' ')
    categories.push(label)
    candles.push([open, close, low, high])
    avgLine.push(avgPrice)
    volumeBars.push({
      value: volume ?? 0,
      itemStyle: { color: close >= open ? 'rgba(23, 122, 98, 0.45)' : 'rgba(178, 77, 84, 0.4)' },
    })
    prevClose = close
  })

  return { categories, candles, avgLine, volumeBars }
})

const summaryText = computed(() => {
  const sample = normalized.value.categories.length
  if (!sample) return `${props.emptyText}。`
  const start = normalized.value.categories[0] || '-'
  const end = normalized.value.categories[sample - 1] || '-'
  return `分钟K线图包含 ${sample} 个时间点，区间 ${start} 到 ${end}。`
})
const ariaLabel = computed(() => '分钟K线与成交量图表')

function ensureChart() {
  if (!chartEl.value) return null
  if (!chart) chart = init(chartEl.value)
  return chart
}

function render() {
  const instance = ensureChart()
  if (!instance) return
  const { categories, candles, avgLine, volumeBars } = normalized.value

  if (!categories.length) {
    instance.setOption({
      animation: false,
      xAxis: { show: false, data: [] },
      yAxis: { show: false },
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
    legend: {
      top: 10,
      textStyle: { color: '#607689' },
      data: ['分钟K线', '均价', '成交量'],
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: 'rgba(18, 32, 45, 0.95)',
      borderWidth: 0,
      textStyle: { color: '#f5fbff' },
    },
    grid: [
      { left: 52, right: 18, top: 44, height: '58%' },
      { left: 52, right: 18, top: '78%', height: '14%' },
    ],
    xAxis: [
      {
        type: 'category',
        data: categories,
        scale: true,
        boundaryGap: true,
        axisLine: { lineStyle: { color: '#c8d7df' } },
        axisLabel: {
          color: '#607689',
          formatter: (value: string) => value.split(' ')[1] || value,
          hideOverlap: true,
        },
        min: 'dataMin',
        max: 'dataMax',
      },
      {
        type: 'category',
        gridIndex: 1,
        data: categories,
        axisLine: { lineStyle: { color: '#c8d7df' } },
        axisLabel: { show: false },
        axisTick: { show: false },
      },
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: false },
        splitLine: { lineStyle: { color: 'rgba(18, 32, 45, 0.08)' } },
        axisLabel: { color: '#607689' },
      },
      {
        gridIndex: 1,
        splitNumber: 2,
        splitLine: { show: false },
        axisLabel: { color: '#607689' },
      },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
      { show: false, xAxisIndex: [0, 1], type: 'slider', start: 0, end: 100 },
    ],
    series: [
      {
        name: '分钟K线',
        type: 'candlestick',
        data: candles,
        itemStyle: {
          color: '#177a62',
          color0: '#b24d54',
          borderColor: '#177a62',
          borderColor0: '#b24d54',
        },
      },
      {
        name: '均价',
        type: 'line',
        data: avgLine,
        showSymbol: false,
        smooth: true,
        lineStyle: { width: 1.8, color: '#d68648' },
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumeBars,
      },
    ],
  }, true)
}

function scheduleRender() {
  if (renderTimer) clearTimeout(renderTimer)
  renderTimer = setTimeout(() => {
    render()
    renderTimer = null
  }, 120)
}

useResizeObserver(chartEl, () => chart?.resize())

watch(
  () => props.items,
  () => scheduleRender(),
  { deep: true, immediate: true },
)

onMounted(() => scheduleRender())

onBeforeUnmount(() => {
  if (renderTimer) clearTimeout(renderTimer)
  chart?.dispose()
  chart = null
})
</script>
