# Zanbo Quant 前端优化优先级文档

> 本文档基于对项目前端代码的全面分析，按照优先级列出所有值得优化或美化的地方。
> 
> 最后更新：2025-04-04

---

## 📋 项目概况

- **技术栈**: Vue 3 + TypeScript + Vite + Tailwind CSS v4
- **UI 组件**: 自定义组件 + ECharts 图表
- **状态管理**: Pinia
- **数据获取**: TanStack Query (Vue Query)
- **构建工具**: Vite 6.x

---

## 🔴 高优先级优化（立即处理）

### 可访问性（A11y）

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 1 | HTML 语言属性错误 | `index.html` 第 2 行 `lang="en"` 与中文界面不符，屏幕阅读器发音错误 | 修改为 `lang="zh-CN"` | 5min |
| 2 | StatusBadge 仅靠颜色 | 色盲用户无法区分 success/warning/danger 状态 | 添加图标前缀：✅ success、⚠️ warning、❌ danger、ℹ️ info，使用 `<span aria-hidden="true">` 隐藏图标 | 30min |
| 3 | 页面标题无动态更新 | 所有页面共用 `title="web"`，无法区分当前位置 | 使用 `vueuse/head` 或路由守卫动态更新：`document.title = \`${pageTitle} | Zanbo Quant\`` | 1h |
| 4 | 焦点管理缺失 | 抽屉/模态框打开后焦点未转移至内部，关闭后未恢复，键盘用户迷失 | 使用 `focus-trap-vue` 管理焦点，打开时聚焦首个可交互元素，关闭时恢复焦点到触发按钮 | 2h |
| 5 | 图表 ARIA 属性待完善 | ECharts 对屏幕阅读器支持有限 | 添加 `role="img"` + `aria-label` 描述图表内容，提供「查看数据表格」替代视图按钮 | 1h |

### 交互体验

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 6 | 缺少全局 Toast 系统 | 操作成功/失败仅通过页面内文本（如 `actionMessage`）展示，不够明显且容易遗漏 | 集成 `vue-sonner` 或自研 Toast 组件，支持成功/错误/警告/加载状态，positioned top-right，支持操作撤销 | 2h |
| 7 | 搜索无自动补全 | 股票代码输入需完整手动输入，无智能提示 | 自建搜索组件，支持股票代码/名称模糊搜索、拼音首字母匹配、历史记录、热门推荐、防抖 300ms | 4h |
| 8 | 缺少虚拟滚动 | 新闻列表、信号列表等可能展示数千条数据，直接渲染导致 DOM 爆炸、卡顿 | 集成 `vue-virtual-scroller`，仅渲染可视区域 + buffer（上下各 5 条），支持动态高度 | 4h |

### 性能优化

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 9 | 路由懒加载粒度粗 | 按页面懒加载，但 `StockDetailPage` 等页面体积过大（40KB+） | 进一步拆分：图表组件独立懒加载、Markdown 渲染独立加载、复杂表单按需加载，使用 `defineAsyncComponent({ loader, loadingComponent, errorComponent })` | 3h |
| 10 | 缺少错误边界 | 组件渲染错误会导致整个应用白屏，用户体验极差 | 添加 Vue 错误边界组件（`onErrorCaptured`），对图表、复杂页面（如 `StockDetailPage`）包裹独立边界，显示友好错误提示 | 2h |

---

## 🟡 中优先级优化（2-4周内完成）

### 视觉设计一致性

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 11 | 圆角变量未完全应用 | 已定义 `--radius-panel: 28px` 和 `--radius-card: 22px`，但代码中仍大量使用硬编码值如 `rounded-[30px]`、`rounded-[20px]` | 统一替换为 CSS 变量：`rounded-[var(--radius-panel)]`、`rounded-[var(--radius-card)]`，新增 `--radius-sm: 12px`、`--radius-lg: 24px` | 2h |
| 12 | 阴影系统不完善 | 仅 `--shadow` 和 `--shadow-soft` 两个级别，缺乏层次感 | 扩展为四级：`--shadow-0: none`、`--shadow-1: 0 2px 8px rgba(15,41,56,0.06)`、`--shadow-2`（原 shadow-soft）、`--shadow-3`（原 shadow） | 1h |
| 13 | 颜色对比度不足 | `--muted: #607689` 在 `--panel` 背景上对比度约 3.2:1，低于 WCAG AA 标准（4.5:1） | 将 `--muted` 调整为 `#4a5d6d`（对比度 4.6:1）或添加 `font-medium` 增强可读性 | 30min |
| 14 | 按钮样式分散 | `styles.css` 使用属性选择器 `[class*='bg-[var(--brand)]']` 匹配按钮样式，易冲突难维护 | 提取为工具类 `.btn-primary`、`.btn-secondary`、`.btn-ghost`，使用 Tailwind `@layer components` 统一定义 | 2h |
| 15 | 图表颜色硬编码 | `MinuteKlineChart.vue` 中 `#177a62`、`#b24d54` 等颜色值写死，不支持主题切换 | 抽取为 CSS 变量：`--chart-up: #177a62`、`--chart-down: #b24d54`、`--chart-line: #d68648`，支持暗黑模式 | 1h |

### 布局优化

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 16 | 侧边栏收起状态信息丢失 | `AppShell.vue` 收起状态仅 `w-[110px]`，导航文字被截断显示不完整 | 改为 `72px` 纯图标模式（配合 Tooltip）或 `200px` 缩略模式，收起时显示图标+首字母 | 2h |
| 17 | 表格缺少固定列/表头 | `DataTable.vue` 大数据量时横向滚动，表头不可见 | 添加 `sticky` 表头（`position: sticky; top: 0`），支持列固定（`fixed: 'left' \| 'right'`），添加水平滚动指示器 | 3h |
| 18 | 筛选区占用空间过大 | 新闻列表等页面筛选表单使用 `grid` 占据整行，在小屏幕上堆叠严重 | 使用折叠面板（Filter Panel）收纳非常用筛选，默认展示核心 2-3 个，添加「更多筛选」展开按钮 | 2h |
| 19 | 图表与表格比例失衡 | `StockDetailPage.vue` 中图表高度固定（`height="320"`），大屏下显得过小 | 使用 `aspect-ratio: 16/9` 或容器查询（Container Queries）使图表自适应容器，大屏下自动扩展 | 2h |
| 20 | 抽屉组件缺少拖拽调整 | `DetailDrawer.vue` 固定宽度 `max-w-[760px]`，大屏下浪费空间，小屏下可能溢出 | 添加拖拽调整宽度功能，或提供「全屏」切换按钮，最大宽度限制为 `90vw`，最小 `320px` | 3h |

### 交互增强

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 21 | 表单输入无实时校验 | `LoginPage.vue` 等表单仅在提交时校验，用户体验差 | 添加 `blur` 时实时校验，使用 `zod` schema 在前端预校验，错误提示即时显示在字段下方（红色边框+文字） | 3h |
| 22 | 分页器过于简陋 | 仅提供上一页/下一页，无法跳转到指定页 | 实现完整的分页器：页码输入框、快速跳转按钮、每页条数选择（20/50/100）、总页数显示 | 2h |
| 23 | 缺少键盘快捷键 | 专业金融终端缺乏键盘导航支持，效率低下 | 添加快捷键系统：`/` 聚焦搜索、`Esc` 关闭抽屉、`Ctrl+K` 打开命令面板、`j/k` 导航列表 | 4h |
| 24 | 图表缺少缩放交互 | `MinuteKlineChart.vue` 仅支持内置 dataZoom，缺少滚轮缩放提示 | 添加鼠标滚轮缩放提示、区间选择刷选（brush）、十字光标（crosshair）数据联动 | 3h |
| 25 | 标签页切换无动画 | `StockDetailPage.vue` 中详情面板切换生硬 | 添加 `fade` 或 `slide` 过渡动画，使用 Vue `<TransitionGroup>` 或 `<Transition mode="out-in">` 实现平滑切换 | 1h |
| 26 | 缺少批量操作功能 | 信号列表、股票列表等仅支持单条操作 | 添加多选复选框，支持批量标记、批量导出 CSV、批量分析等操作 | 4h |

### 响应式设计

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 27 | 移动端导航可进一步完善 | 虽已添加 `mobileNavOpen` 和抽屉导航，但缺少手势和快捷入口 | 添加手势滑动关闭（swipe left）、底部 Tab 栏快捷入口（核心 4-5 个页面） | 3h |
| 28 | 表格在小屏下溢出 | 无横向滚动处理，小屏下内容被截断 | 添加 `overflow-x-auto` 容器，小屏下（`< md`）转换为卡片列表（Card List）视图 | 3h |
| 29 | 字体大小未完全适配小屏 | 标题 `text-[30px]` 在手机上过大，导致布局错乱 | 使用响应式字体：`text-2xl md:text-3xl lg:text-[30px]`，或 CSS `clamp()` 函数 | 1h |
| 30 | Touch 目标需统一 | 部分按钮使用 `px-3 py-2`（约 48x32px），低于 44x44px 推荐值 | 移动端按钮统一 `min-h-[44px] min-w-[44px]`，使用 `touch-manipulation` 防止双击缩放 | 1h |

### 性能优化

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 31 | 图片无懒加载 | 若未来添加新闻配图或头像，无懒加载机制 | 使用 `loading="lazy"` 属性，配合 `IntersectionObserver` 实现渐进加载 | 1h |
| 32 | WebSocket 重连策略简陋 | `useRealtimeBus` 可能存在指数退避缺失，频繁断网时轰炸服务器 | 实现指数退避重连（Exponential Backoff），最大重试间隔 30s，添加连接状态可视化 | 2h |
| 33 | 资源预加载缺失 | 路由切换时按需加载 JS，有明显的加载停顿 | 使用 `<link rel="prefetch">` 预加载下一页资源，对核心页面使用 `preload` | 1h |

### 可访问性进阶

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 34 | 按钮 ARIA 状态缺失 | 加载状态按钮未标记 `aria-busy`，开关按钮无 `aria-pressed` | 添加适当的 ARIA 状态属性，确保屏幕阅读器知晓当前状态 | 1h |
| 35 | 表格缺少语义化 | `DataTable.vue` 未使用 `<th scope="col">` 等语义属性 | 添加 `scope` 属性，复杂表格使用 `aria-describedby` 关联说明文本 | 1h |

---

## 🟢 低优先级优化（长期规划）

### 代码质量与维护性

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 36 | 魔法数字泛滥 | 大量硬编码尺寸如 `h-[320px]`、`max-w-[760px]` | 抽取为设计系统常量，使用 Tailwind 配置 `theme.extend` 定义 `chart-height`、`drawer-width` | 4h |
| 37 | 类型定义分散 | `any` 类型使用较多（如 `row: Record<string, any>`） | 从 API 层开始定义严格的 TypeScript 接口，使用 `zod` 进行运行时校验 | 8h |
| 38 | 缺少单元测试 | 项目无测试框架配置 | 添加 Vitest + Vue Test Utils，优先测试工具函数（`format.ts`、`finance.ts`） | 16h |
| 39 | i18n 未预留 | 所有文本硬编码中文，未来国际化成本高 | 使用 `vue-i18n`，将文本抽取到 `locales/zh-CN.json`，即使短期内不翻译也预留结构 | 8h |

### 性能与构建

| 序号 | 问题 | 原因 | 具体建议 | 工时 |
|:---:|------|------|----------|:--:|
| 40 | CSS 存在大量重复 | 各组件重复书写 `bg-[linear-gradient(...)]` 等复杂样式 | 提取为工具类如 `.card-gradient`、`.panel-glass`，使用 Tailwind `@layer components` | 4h |
| 41 | 构建产物未分析 | `vite.config.ts` 缺少 rollup 分析配置 | 添加 `rollup-plugin-visualizer`，定期分析包体积，设置预算（budget）告警：主包 < 200KB、异步包 < 100KB | 2h |
| 42 | 字体回退不完善 | 仅定义了中文字体回退，缺少等宽字体和数字字体优化 | 添加数字专用字体如 `'DIN Alternate', 'Helvetica Neue'`，代码块使用 `'JetBrains Mono', 'Fira Code'` 回退 | 1h |

---

## 📊 优先级汇总表

| 优先级 | 数量 | 核心关注点 | 预计总工时 |
|:------:|:----:|-----------|:----------:|
| 🔴 高 | 10 | 可访问性（语言、焦点、标题）、交互（Toast、搜索、虚拟滚动）、性能（懒加载细化、错误边界） | 约 20h |
| 🟡 中 | 25 | 视觉一致性（圆角、阴影、颜色）、布局优化（表格、筛选、图表）、键盘导航、响应式适配 | 约 50h |
| 🟢 低 | 7 | 代码质量（常量、类型、测试）、国际化、构建分析 | 约 40h |

---

## ✅ 已完成的优化（值得肯定）

| 改进项 | 实现位置 | 说明 |
|--------|----------|------|
| **ECharts 按需引入** | `MinuteKlineChart.vue` | 使用 `echarts/core` + 注册组件，减少 ~60% 体积 |
| **移动端导航** | `AppShell.vue` | `mobileNavOpen` + 抽屉式导航 + ARIA 属性 |
| **CSS 变量规范化** | `styles.css` | `--radius-panel`、`--radius-card`、`--font-size-compact` |
| **骨架屏** | `styles.css` | `.loading-skeleton` + shimmer 动画 |
| **图表可访问性** | `MinuteKlineChart.vue` | `role="img"` + `aria-label` + `.sr-only` 摘要 |
| **减弱动效支持** | `styles.css` | `prefers-reduced-motion` 媒体查询 |
| **交互卡片** | `InfoCard.vue` | 自动检测 `onClick` 切换为 `<button>` |
| **确认对话框** | `confirm.ts` | `confirmDangerAction()` 基础实现 |
| **表单标签** | `StockDetailPage.vue` | 使用 `<label>` 包裹输入项 |
| **焦点可见性** | `styles.css` | `:focus-visible` 增强 + `outline-offset` |

**当前完成度：约 45%**

---

## 🎯 实施路线图

### 第一阶段（Week 1）- 基础体验
- [ ] 修复 HTML 语言属性
- [ ] StatusBadge 添加图标
- [ ] 页面标题动态更新
- [ ] 集成 Toast 系统
- [ ] 焦点管理实现

### 第二阶段（Week 2）- 交互增强
- [ ] 搜索自动补全组件
- [ ] 虚拟滚动实现
- [ ] 错误边界添加
- [ ] 路由懒加载细化

### 第三阶段（Week 3-4）- 设计系统
- [ ] 圆角/阴影变量统一应用
- [ ] 按钮组件统一
- [ ] 图表颜色变量化
- [ ] 表格组件增强

### 第四阶段（Week 5-6）- 进阶优化
- [ ] 键盘快捷键系统
- [ ] 响应式细节完善
- [ ] 表单实时校验
- [ ] 分页器完善

### 第五阶段（Week 7+）- 代码质量
- [ ] TypeScript 类型严格化
- [ ] 单元测试覆盖
- [ ] i18n 架构搭建
- [ ] 构建优化

---

## 🛠️ 技术实现参考

### StatusBadge 图标增强
```vue
<template>
  <span :class="badgeClass">
    <span v-if="icon" class="mr-1" aria-hidden="true">{{ icon }}</span>
    {{ label }}
  </span>
</template>

<script setup lang="ts">
const iconMap = {
  success: '✅',
  warning: '⚠️',
  danger: '❌',
  info: 'ℹ️',
  muted: '●'
}
const icon = computed(() => iconMap[tone.value])
</script>
```

### 动态页面标题
```typescript
// router.ts
router.beforeEach((to) => {
  const titles: Record<string, string> = {
    '/dashboard': '总控台',
    '/stocks/list': '股票列表',
    '/stocks/detail': '股票详情',
    '/signals/overview': '投资信号',
    '/intelligence/global-news': '国际资讯',
  }
  const title = to.meta.title || titles[to.path] || 'Zanbo Quant'
  document.title = `${title} | Zanbo Quant`
})
```

### Toast 系统集成
```bash
npm install vue-sonner
```

```typescript
// main.ts
import { Toaster } from 'vue-sonner'
app.component('Toaster', Toaster)

// 使用
import { toast } from 'vue-sonner'
toast.success('操作成功')
toast.error('操作失败')
toast.loading('加载中...')
```

### 虚拟滚动
```bash
npm install vue-virtual-scroller
```

```vue
<template>
  <RecycleScroller
    class="scroller"
    :items="items"
    :item-size="80"
    key-field="id"
  >
    <template #default="{ item }">
      <NewsCard :item="item" />
    </template>
  </RecycleScroller>
</template>
```

---

## 📚 参考资源

- [Vue 3 风格指南](https://vuejs.org/style-guide/)
- [Tailwind CSS 最佳实践](https://tailwindcss.com/docs/best-practices)
- [Web 可访问性指南 (WCAG 2.1)](https://www.w3.org/WAI/WCAG21/quickref/)
- [Vite 性能优化](https://vitejs.dev/guide/performance.html)
- [ECharts 按需加载](https://echarts.apache.org/handbook/zh/basics/import/)
- [vue-sonner 文档](https://vue-sonner.vercel.app/)
- [vue-virtual-scroller 文档](https://github.com/Akryum/vue-virtual-scroller)

---

**文档版本**: v1.0  
**创建日期**: 2025-04-04  
**维护者**: Frontend Team
