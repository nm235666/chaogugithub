<template>
  <AppShell title="投研决策板" subtitle="方向验证层主入口：聚焦动作编排、闭环回执与复盘验证。">
    <div class="space-y-4">
      <div class="page-hero-grid">
        <div class="page-hero-card">
          <div class="page-insight-label">Decision Loop</div>
          <div class="page-hero-title">把市场模式、短名单和人工动作放到一张可执行面板里。</div>
          <div class="page-hero-copy">
            这页的目标不是展示更多配置，而是帮助我们更快判断“现在该不该做、该先做哪只、风险在哪里”。聚焦股代码可直接收敛到单票。
          </div>
          <div class="page-action-cluster">
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isBoardFetching" @click="applyFilters">
              {{ isBoardFetching ? '刷新中...' : '刷新决策板' }}
            </button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="isSnapshotPending" @click="runSnapshot">
              {{ isSnapshotPending ? '生成中...' : '生成快照' }}
            </button>
            <RouterLink to="/app/orders" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm font-semibold text-[var(--ink)]">
              执行任务
            </RouterLink>
          </div>
        </div>
        <div class="page-insight-list">
          <div class="page-insight-item">
            <div class="page-insight-label">当前市场模式</div>
            <div class="page-insight-value">{{ marketRegime.label || '数据不足' }}</div>
            <div class="page-insight-note">评分 {{ formatNumber(marketRegime.score, 1) }}，建议先按模式理解仓位语言，再看个股执行。</div>
          </div>
          <div class="page-insight-item">
            <div class="page-insight-label">候选 / 待办</div>
            <div class="page-insight-value">{{ shortlist.length }} 只候选 · {{ todayWatchCount }} 项跟踪</div>
            <div class="page-insight-note">今日候选短名单 {{ shortlist.length }} 只；观察中动作 {{ todayWatchCount }} 项，点击下方列表处理。</div>
          </div>
          <div class="page-insight-item">
            <div class="page-insight-label">风险 / 复盘</div>
            <div class="page-insight-value">{{ riskIndicatorText }}</div>
            <div class="page-insight-note">{{ riskIndicatorNote }}</div>
          </div>
        </div>
      </div>

      <!-- 3.4 命令化阶段协议：决策主链路阶段可视化 -->
      <div class="rounded-[20px] border border-[var(--line)] bg-white/80 px-4 py-3">
        <div class="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">决策链路阶段</div>
        <div class="flex flex-wrap items-center gap-0">
          <div
            v-for="(stage, idx) in DECISION_STAGES"
            :key="stage.key"
            class="flex items-center gap-1"
          >
            <div
              class="rounded-full px-3 py-1.5 text-xs font-semibold transition"
              :class="decisionStageClass(stage.key)"
            >
              {{ stage.label }}
            </div>
            <div v-if="idx < DECISION_STAGES.length - 1" class="mx-1 text-[var(--muted)] opacity-40">→</div>
          </div>
        </div>
        <div class="mt-1.5 text-xs text-[var(--muted)]">当前阶段：<span class="font-semibold text-[var(--ink)]">{{ currentDecisionStageLabel }}</span> · {{ currentDecisionStageHint }}</div>
      </div>

      <div class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div class="text-[var(--muted)]">
            统计证据（宏观评分卡、行业排序、入选理由包）已收敛到
            <RouterLink to="/app/research/scoreboard" class="font-semibold text-[var(--brand)] hover:underline">评分总览</RouterLink>
            。本页只保留动作与验证闭环所需字段。
          </div>
          <RouterLink
            to="/app/research/scoreboard"
            class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
          >
            打开评分总览
          </RouterLink>
        </div>
      </div>

      <PageSection title="决策输入" subtitle="输入单票聚焦代码，或直接刷新全局决策板。">
        <div
          v-if="hasDecisionContext"
          class="mb-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.8)] px-4 py-3 text-sm text-[var(--muted)]"
        >
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="font-semibold text-[var(--ink)]">当前研究上下文</div>
            <button
              type="button"
              class="rounded-full border border-[var(--line)] bg-white px-3 py-1 text-xs font-semibold text-[var(--ink)]"
              @click="clearDecisionContext"
            >
              清空上下文
            </button>
          </div>
          <div class="mt-2 flex flex-wrap gap-2 text-xs">
            <span v-if="decisionContext.from" class="metric-chip font-semibold text-emerald-700">来源 {{ sourceModuleLabel(decisionContext.from) }}</span>
            <span v-if="decisionContext.industry" class="metric-chip">行业 {{ decisionContext.industry }}</span>
            <span v-if="decisionContext.keyword" class="metric-chip">关键词 {{ decisionContext.keyword }}</span>
            <span v-if="decisionContext.score_date" class="metric-chip">评分日期 {{ decisionContext.score_date }}</span>
          </div>
          <div v-if="hasExternalSource && actionTsCodeDraft" class="mt-3">
            <div class="text-xs font-semibold text-[var(--ink)]">快速记录动作：{{ actionTsCodeDraft }}</div>
            <div class="mt-2 flex flex-wrap gap-2">
              <button class="rounded-full border border-emerald-300 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-800 disabled:opacity-60 hover:bg-emerald-100" :disabled="isActionPending" @click="submitManualAction('watch', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">{{ isActionPending ? '记录中...' : '观察' }}</button>
              <button class="rounded-full border border-emerald-300 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-800 disabled:opacity-60 hover:bg-emerald-100" :disabled="isActionPending" @click="submitManualAction('confirm', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">{{ isActionPending ? '记录中...' : '确认' }}</button>
              <button class="rounded-full border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-800 disabled:opacity-60 hover:bg-amber-100" :disabled="isActionPending" @click="submitManualAction('defer', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">{{ isActionPending ? '记录中...' : '暂缓' }}</button>
              <button class="rounded-full border border-red-200 bg-red-50 px-3 py-1.5 text-xs font-semibold text-red-700 disabled:opacity-60 hover:bg-red-100" :disabled="isActionPending" @click="submitManualAction('reject', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">{{ isActionPending ? '记录中...' : '拒绝' }}</button>
            </div>
          </div>
          <div class="mt-2 text-xs text-[var(--muted)]">作用范围：仅用于决策板默认查询条件，不会修改底层评分数据。</div>
        </div>
        <div class="grid gap-3 xl:grid-cols-[1fr_180px_180px_180px] md:grid-cols-2">
          <input v-model.trim="focusTsCode" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="聚焦股票，如 000001.SZ" @keydown.enter="applyFilters" />
          <select v-model.number="pageSize" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3">
            <option :value="8">8 / 页</option>
            <option :value="12">12 / 页</option>
            <option :value="20">20 / 页</option>
          </select>
          <input v-model.trim="killReasonDraft" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="Kill Switch 备注" />
          <div class="flex flex-wrap gap-2">
            <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isBoardFetching" @click="applyFilters">
              {{ isBoardFetching ? '刷新中...' : '刷新决策板' }}
            </button>
            <button class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="isSnapshotPending" @click="runSnapshot">
              {{ isSnapshotPending ? '生成中...' : '生成快照' }}
            </button>
          </div>
        </div>
        <div class="mt-3 flex flex-wrap gap-2">
          <StatusBadge :value="killSwitchTone" :label="killSwitchLabel" />
          <StatusBadge :value="marketRegime.mode || 'muted'" :label="marketRegime.label || '数据不足'" />
          <StatusBadge value="info" :label="`短名单 ${shortlist.length}`" />
          <StatusBadge value="muted" :label="`行业 ${industries.length}`" />
          <StatusBadge
            :value="pipelineHealthStatusTone"
            :label="`链路 ${pipelineHealth.status || 'unknown'}`"
          />
        </div>
        <div class="mt-2 flex flex-wrap gap-2 text-xs text-[var(--muted)]">
          <span class="metric-chip">score_date {{ pipelineHealth.score_date || '-' }}</span>
          <span class="metric-chip">漏斗候选 {{ pipelineHealth.funnel_total ?? 0 }}</span>
          <span class="metric-chip">快照日期 {{ pipelineHealth.latest_snapshot_date || '-' }}</span>
          <span v-for="item in pipelineHealthMissingInputs" :key="item" class="metric-chip text-amber-700">{{ item }}</span>
        </div>
        <div v-if="lastTraceFeedback.action_id || lastTraceFeedback.run_id || lastTraceFeedback.snapshot_id || latestSnapshotId || latestActionId || snapshotDate" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="font-semibold text-[var(--ink)]">最近链路标识</div>
            <button
              v-if="lastActionMarkdown"
              type="button"
              class="rounded-full border border-emerald-300 bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-800 transition hover:bg-emerald-100"
              @click="downloadLastActionMarkdown"
            >
              下载证据摘要 (.md)
            </button>
          </div>
          <div class="mt-2 flex flex-wrap gap-2 text-xs">
            <span v-if="lastTraceFeedback.action_id" class="metric-chip">action_id {{ lastTraceFeedback.action_id }}</span>
            <span v-else-if="latestActionId" class="metric-chip">action_id {{ latestActionId }}</span>
            <span v-if="lastTraceFeedback.run_id" class="metric-chip">run_id {{ lastTraceFeedback.run_id }}</span>
            <span v-if="lastTraceFeedback.snapshot_id" class="metric-chip">snapshot_id {{ lastTraceFeedback.snapshot_id }}</span>
            <span v-else-if="latestSnapshotId" class="metric-chip">snapshot_id {{ latestSnapshotId }}</span>
            <span v-if="snapshotDate" class="metric-chip">snapshot_date {{ snapshotDate }}</span>
          </div>
          <div v-if="lastActionMarkdown" class="mt-2 text-xs text-emerald-600">
            双轨证据产物就绪 — JSON 已存入 decision_actions，Markdown 可点击上方按钮下载。
          </div>
        </div>
        <div v-if="message" class="mt-3 rounded-[18px] border border-[var(--line)] bg-[rgba(255,255,255,0.72)] px-4 py-3 text-sm text-[var(--muted)]">
          {{ message }}
        </div>
        <div
          v-if="snapshotStatus !== 'idle'"
          class="mt-3 rounded-[18px] border px-4 py-3 text-sm"
          :class="snapshotStatus === 'success' ? 'border-emerald-200 bg-emerald-50 text-emerald-800' : snapshotStatus === 'error' ? 'border-red-200 bg-red-50 text-red-700' : 'border-blue-200 bg-blue-50 text-blue-800'"
        >
          <div class="font-semibold">
            {{
              snapshotStatus === 'pending'
                ? '快照生成中...'
                : snapshotStatus === 'success'
                  ? '快照已生成'
                  : '快照生成失败'
            }}
          </div>
          <div class="mt-1">
            {{
              snapshotStatus === 'pending'
                ? '系统正在生成决策快照，请稍候。'
                : snapshotStatusText
            }}
          </div>
        </div>
        <div v-if="boardErrorText" class="mt-3 rounded-[18px] border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ boardErrorText }}
        </div>
      </PageSection>

      <div class="grid gap-4 lg:grid-cols-4 md:grid-cols-2">
        <StatCard title="市场模式" :value="marketRegime.label || '数据不足'" :hint="`评分 ${formatNumber(marketRegime.score, 1)}`" />
        <StatCard title="短名单" :value="String(shortlist.length || 0)" :hint="`最新评分日期 ${snapshotDate || '-'}`" />
        <StatCard title="行业数" :value="String(industries.length || 0)" :hint="`Top 行业 ${topIndustryName || '-'}`" />
        <StatCard title="验证层" :value="validation.status || 'idle'" :hint="`done ${validation.done ?? 0} · error ${validation.error ?? 0}`" />
      </div>

      <PageSection title="明日候选建议卡" subtitle="把短名单直接收口为可执行建议：触发、失效、仓位和风险预算来源。">
        <div v-if="tomorrowCandidates.length === 0" class="rounded-2xl border border-dashed border-[var(--line)] px-4 py-8 text-center text-sm text-[var(--muted)]">
          当前没有可用候选。请先确认评分链路和漏斗状态是否为 ready。
        </div>
        <div v-else class="grid gap-3 lg:grid-cols-3 md:grid-cols-2">
          <div
            v-for="candidate in tomorrowCandidates"
            :key="candidate.ts_code"
            class="rounded-[var(--radius-card)] border border-[var(--line)] bg-white p-4 shadow-[var(--shadow-soft)]"
          >
            <div class="flex items-start justify-between gap-2">
              <div>
                <div class="text-[15px] font-bold text-[var(--ink)]">{{ candidate.name || candidate.ts_code }}</div>
                <div class="mt-0.5 text-xs text-[var(--muted)]">{{ candidate.ts_code || '-' }}</div>
              </div>
              <StatusBadge :value="candidate.position_label || 'muted'" :label="candidate.position_label || '-'" />
            </div>
            <div class="mt-3 space-y-1.5 text-xs leading-5 text-[var(--muted)]">
              <div><span class="font-semibold text-[var(--ink)]">买入触发：</span>{{ candidate.entry_trigger || '-' }}</div>
              <div><span class="font-semibold text-[var(--ink)]">失效条件：</span>{{ candidate.invalidation || '-' }}</div>
              <div><span class="font-semibold text-[var(--ink)]">仓位建议：</span>{{ candidate.position_hint || '-' }}</div>
              <div><span class="font-semibold text-[var(--ink)]">风险预算：</span>{{ candidate.risk_budget_source || '-' }}</div>
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <RouterLink
                :to="`/app/stocks/detail/${candidate.ts_code}`"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
              >
                股票详情
              </RouterLink>
              <button
                class="rounded-full border border-[var(--line)] bg-white px-3 py-1.5 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)] disabled:opacity-60"
                :disabled="isActionPending"
                @click="submitManualAction('confirm', candidate.ts_code, `候选确认：${candidate.name || candidate.ts_code || '-'}`, candidate.name || candidate.ts_code || '')"
              >
                确认入池
              </button>
            </div>
          </div>
        </div>
      </PageSection>

      <PageSection v-if="recentVerdicts.length" title="近期首席裁决" subtitle="来自多角色分析的方向性判断，按时间倒序排列。">
        <div class="flex flex-wrap gap-3">
          <div
            v-for="item in recentVerdicts"
            :key="item.id"
            class="flex min-w-[160px] flex-1 items-center gap-3 rounded-2xl border-2 px-4 py-3"
            :class="item.action_type === 'confirm' ? 'border-emerald-300 bg-emerald-50' : item.action_type === 'reject' ? 'border-red-300 bg-red-50' : 'border-amber-200 bg-amber-50'"
          >
            <div
              class="shrink-0 rounded-full px-2.5 py-1 text-xs font-bold text-white"
              :class="item.action_type === 'confirm' ? 'bg-emerald-700' : item.action_type === 'reject' ? 'bg-red-700' : 'bg-amber-600'"
            >
              {{ actionLabel(item.action_type) }}
            </div>
            <div class="min-w-0 flex-1">
              <div class="flex items-baseline gap-2">
                <RouterLink
                  v-if="item.ts_code"
                  :to="`/app/stocks/detail/${item.ts_code}`"
                  class="block truncate text-sm font-semibold text-[var(--ink)] hover:underline"
                >
                  {{ item.stock_name || item.ts_code }}
                </RouterLink>
                <span v-if="verdictReturn(item).text" class="shrink-0 text-xs" :class="verdictReturn(item).cls">
                  {{ verdictReturn(item).text }}
                </span>
              </div>
              <div class="mt-0.5 truncate text-xs text-[var(--muted)]">{{ item.created_at || '-' }}</div>
              <div class="mt-1 flex items-center gap-1.5">
                <span class="text-xs text-[var(--muted)]">执行：</span>
                <span
                  class="rounded-full px-2 py-0.5 text-xs font-semibold"
                  :class="{
                    'bg-gray-100 text-gray-600': !item.execution_status,
                    'bg-amber-100 text-amber-700': item.execution_status === 'planned',
                    'bg-blue-100 text-blue-700': item.execution_status === 'executing',
                    'bg-emerald-100 text-emerald-700': item.execution_status === 'done',
                    'bg-gray-100 text-gray-500': item.execution_status === 'cancelled',
                  }"
                >{{ { planned: '待执行', executing: '执行中', done: '已完成', cancelled: '已取消' }[item.execution_status as string] || '未关联' }}</span>
                <RouterLink v-if="item.execution_status" :to="`/app/orders?decision_action_id=${item.id}`" class="text-xs text-[var(--brand)] hover:underline">查看执行</RouterLink>
              </div>
              <div v-if="item.note" class="mt-1 line-clamp-2 text-xs text-[var(--muted)]">{{ item.note }}</div>
            </div>
          </div>
        </div>
      </PageSection>

      <PageSection
        v-if="calibrationSummary.total_count > 0 || calibrationQuery.isFetched.value"
        title="裁决精准度"
        subtitle="第三层验证入口：历史看多/看空裁决的收益与命中率，用于评估判断质量。"
      >
        <div class="mb-3 flex flex-wrap gap-2 text-xs">
          <RouterLink to="/app/research/quant-factors" class="metric-chip font-semibold text-[var(--brand)]">
            进入验证与研究层（因子/回测）
          </RouterLink>
        </div>
        <div class="grid gap-3 md:grid-cols-3">
          <StatCard
            title="看多命中率"
            :value="calibrationSummary.confirm_count ? `${(calibrationSummary.confirm_hit_rate_5d * 100).toFixed(0)}%` : '-'"
            :hint="`命中 ${calibrationSummary.confirm_hit_5d} / ${calibrationSummary.confirm_count} 次（5 日为基准）`"
          />
          <StatCard
            title="看空命中率"
            :value="calibrationSummary.reject_count ? `${(calibrationSummary.reject_hit_rate_5d * 100).toFixed(0)}%` : '-'"
            :hint="`命中 ${calibrationSummary.reject_hit_5d} / ${calibrationSummary.reject_count} 次（5 日为基准）`"
          />
          <StatCard
            title="整体准确率"
            :value="calibrationSummary.total_count ? `${(calibrationSummary.total_hit_rate_5d * 100).toFixed(0)}%` : '-'"
            :hint="`共 ${calibrationSummary.total_count} 条有效裁决`"
          />
        </div>
        <DataTable
          v-if="calibrationItems.length"
          class="mt-4"
          :columns="calibrationColumns"
          :rows="calibrationItems"
        >
          <template #cell-action_type="{ row }">
            <StatusBadge :value="actionTone(row.action_type)" :label="actionLabel(row.action_type)" />
          </template>
          <template #cell-return_5d="{ row }">
            <span :class="returnClass(row.return_5d)">{{ formatReturn(row.return_5d) }}</span>
          </template>
          <template #cell-return_20d="{ row }">
            <span :class="returnClass(row.return_20d)">{{ formatReturn(row.return_20d) }}</span>
          </template>
          <template #cell-return_60d="{ row }">
            <span :class="returnClass(row.return_60d)">{{ formatReturn(row.return_60d) }}</span>
          </template>
          <template #cell-hit_5d="{ row }">
            <div class="flex items-center gap-2">
              <span v-if="row.hit_5d === true" class="text-emerald-600 font-semibold">命中</span>
              <span v-else-if="row.hit_5d === false" class="text-red-500">未中</span>
              <span v-else class="text-[var(--muted)]">-</span>
              <RouterLink
                v-if="row.payload?.context?.job_id"
                :to="`/app/research/multi-role?restore_job=${row.payload.context.job_id}`"
                class="rounded-full border border-[var(--line)] bg-white px-2 py-0.5 text-xs text-[var(--brand)] hover:underline"
              >
                原始分析
              </RouterLink>
            </div>
          </template>
        </DataTable>
        <div v-else class="mt-3 text-sm text-[var(--muted)]">{{ verdictEmptyText }}</div>
      </PageSection>

      <div class="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <PageSection title="市场模式与交易计划" subtitle="把宏观评分直接转成仓位语言与执行提醒。">
          <div class="grid gap-3 md:grid-cols-2">
            <InfoCard :title="marketRegime.label || '数据不足'" :meta="`分数 ${formatNumber(marketRegime.score, 1)} · 模式 ${marketRegime.mode || '-'}`" :description="marketRegime.summary || '暂无市场模式摘要。'">
              <div class="mt-3 flex flex-wrap gap-2 text-xs">
                <span v-for="item in marketRegime.factors || []" :key="item" class="metric-chip">{{ item }}</span>
              </div>
            </InfoCard>
            <InfoCard :title="tradePlan.headline || '交易计划'" :meta="`底仓 ${tradePlan.base_position ?? 0}% · 浮动仓 ${tradePlan.floating_position ?? 0}% · 预备仓 ${tradePlan.reserve_position ?? 0}%`" :description="tradePlan.mode || '-'">
              <div class="mt-3 space-y-2 text-sm text-[var(--muted)]">
                <div v-for="item in tradePlan.actions || []" :key="item">{{ item }}</div>
              </div>
            </InfoCard>
          </div>
          <div v-if="focusStock" class="mt-3">
            <InfoCard :title="focusStock.name || focusStock.ts_code || '聚焦股票'" :meta="focusStock.ts_code || '-'" :description="focusStock.reason || focusStock.trade_plan?.suggestion || ''">
              <template #badge>
                <StatusBadge :value="focusStock.score?.position_label || 'muted'" :label="focusStock.score?.position_label || '-'"/>
              </template>
              <div class="mt-3 flex flex-wrap gap-2">
                <StatusBadge value="brand" :label="`总分 ${formatNumber(focusStock.score?.total_score, 1)}`" />
                <StatusBadge value="info" :label="`行业分 ${formatNumber(focusStock.score?.industry_total_score, 1)}`" />
                <StatusBadge value="muted" :label="focusStock.risk || '暂无风险提示'" />
              </div>
              <div class="mt-3 flex flex-wrap gap-2">
                <RouterLink
                  v-if="focusStock.ts_code"
                  :to="`/app/stocks/detail/${focusStock.ts_code}`"
                  class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
                >
                  打开股票详情
                </RouterLink>
              </div>
            </InfoCard>
          </div>
        </PageSection>

        <PageSection title="Kill Switch 与验证层" subtitle="人工确认、快照和验证结果放在一起，便于收口。">
          <InfoCard :title="killSwitchLabel" :meta="killSwitch.updated_at || '-'" :description="killSwitch.reason || '暂无备注'">
            <template #badge>
              <StatusBadge :value="killSwitchTone" :label="killSwitchLabel" />
            </template>
            <div class="mt-3 flex flex-wrap gap-2">
              <button class="rounded-full bg-red-700 px-3 py-2 text-xs font-semibold text-white disabled:opacity-60" :disabled="isTogglePending" @click="pauseTrading">
                {{ isTogglePending ? '切换中...' : '暂停交易' }}
              </button>
              <button class="rounded-full bg-emerald-700 px-3 py-2 text-xs font-semibold text-white disabled:opacity-60" :disabled="isTogglePending" @click="resumeTrading">
                {{ isTogglePending ? '切换中...' : '恢复交易' }}
              </button>
            </div>
          </InfoCard>

          <InfoCard class="mt-3" title="验证层概览" :meta="validation.source || '-'" :description="validationDescription">
            <template #badge>
              <StatusBadge :value="validation.status || 'muted'" :label="validation.status || '-'" />
            </template>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">done <strong>{{ validation.done ?? 0 }}</strong></span>
              <span class="metric-chip">error <strong>{{ validation.error ?? 0 }}</strong></span>
            </div>
            <div v-if="validation.latest" class="mt-3 rounded-[18px] border border-[var(--line)] bg-white/80 px-3 py-3 text-sm text-[var(--muted)]">
              <div class="font-semibold text-[var(--ink)]">{{ validation.latest.task_id || '-' }}</div>
              <div class="mt-1">{{ validation.latest.task_type || '-' }} · {{ validation.latest.status || '-' }}</div>
              <div class="mt-1">{{ validation.latest.error_message || validation.latest.error_code || '暂无错误' }}</div>
            </div>
          </InfoCard>
        </PageSection>
      </div>

      <div class="grid gap-4">
        <PageSection title="股票短名单（动作视角）" subtitle="按执行优先级展示候选。行业统计与评分解释请查看评分总览。">
          <div class="table-lead">
            <div class="table-lead-copy">本表只保留动作相关字段：触发、失效、仓位与动作按钮。统计类明细不在本页重复展示。</div>
            <div class="flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">短名单 {{ shortlist.length }}</span>
              <span class="metric-chip">验证 {{ validation.status || 'idle' }}</span>
              <span class="metric-chip">行业数 {{ industries.length }}</span>
              <span class="metric-chip">Top 行业 {{ topIndustryName || '-' }}</span>
              <RouterLink :to="`/app/research/scoreboard`" class="metric-chip font-semibold text-[var(--brand)]">去看统计总览</RouterLink>
            </div>
          </div>
          <DataTable :columns="stockColumns" :rows="shortlist" row-key="ts_code" empty-text="暂无短名单股票" caption="股票短名单">
            <template #cell-ts_code="{ row }">
              <RouterLink :to="`/app/stocks/detail/${row.ts_code}`" class="font-semibold text-[var(--brand)] hover:underline">
                {{ row.name || row.ts_code || '-' }}
              </RouterLink>
              <div class="mt-1 text-xs text-[var(--muted)]">{{ row.ts_code || '-' }}</div>
            </template>
            <template #cell-total_score="{ row }">{{ formatNumber(row.total_score, 2) }}</template>
            <template #cell-industry_total_score="{ row }">{{ formatNumber(row.industry_total_score, 2) }}</template>
            <template #cell-position_label="{ row }"><StatusBadge :value="row.position_label || 'muted'" :label="row.position_label || '-'"/></template>
            <template #cell-actionable="{ row }">
              <div class="space-y-1 text-xs text-[var(--muted)] leading-5">
                <div><span class="font-semibold text-[var(--ink)]">触发：</span>{{ row.entry_trigger || '-' }}</div>
                <div><span class="font-semibold text-[var(--ink)]">失效：</span>{{ row.invalidation || '-' }}</div>
                <div><span class="font-semibold text-[var(--ink)]">仓位：</span>{{ row.position_hint || '-' }}</div>
              </div>
            </template>
            <template #cell_actions="{ row }">
              <div class="flex flex-wrap gap-2">
                <RouterLink :to="`/app/stocks/detail/${row.ts_code}`" class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]">
                  股票详情
                </RouterLink>
                <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)] disabled:opacity-60" :disabled="isActionPending" @click="submitManualAction('confirm', row.ts_code, `短名单确认：${row.name || row.ts_code || '-'}`, row.name || row.ts_code || '')">
                  确认
                </button>
                <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)] disabled:opacity-60" :disabled="isActionPending" @click="submitManualAction('defer', row.ts_code, `短名单暂缓：${row.name || row.ts_code || '-'}`, row.name || row.ts_code || '')">
                  暂缓
                </button>
                <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)] disabled:opacity-60" :disabled="isActionPending" @click="submitManualAction('reject', row.ts_code, `短名单拒绝：${row.name || row.ts_code || '-'}`, row.name || row.ts_code || '')">
                  拒绝
                </button>
              </div>
            </template>
          </DataTable>
        </PageSection>
      </div>

      <PageSection title="决策快照历史" subtitle="每日快照沉淀，方便回看决策板状态。">
        <DataTable :columns="historyColumns" :rows="historyItems" row-key="id" empty-text="暂无快照历史" caption="决策快照历史">
          <template #cell-snapshot_date="{ row }">{{ row.snapshot_date || '-' }}</template>
          <template #cell-snapshot_type="{ row }">{{ row.snapshot_type || '-' }}</template>
          <template #cell-created_at="{ row }">{{ row.created_at || '-' }}</template>
          <template #cell-summary="{ row }">{{ row.payload?.summary ? `行业 ${row.payload.summary.industry_count || 0} / 短名单 ${row.payload.summary.shortlist_size || 0}` : '-' }}</template>
        </DataTable>
      </PageSection>

      <PageSection title="人工确认记录" subtitle="对短名单标的做确认、暂缓、拒绝和观察留痕，方便回看。">
        <div class="grid gap-3 xl:grid-cols-[1fr_1fr_160px] md:grid-cols-2">
          <input v-model.trim="actionTsCodeDraft" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="动作股票代码，如 000001.SZ" />
          <input v-model.trim="actionNoteDraft" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3" placeholder="动作备注" />
          <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isActionPending || !actionTsCodeDraft.trim()" @click="submitManualAction('review', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">
            {{ isActionPending ? '记录中...' : '记录确认' }}
          </button>
        </div>
        <div class="mt-3 grid gap-3 xl:grid-cols-2">
          <input v-model.trim="actionEvidenceDraft" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm" placeholder="证据来源（可选），如「多角色分析 run_id=xxx」" />
          <input v-model.trim="actionReviewConclusionDraft" class="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm" placeholder="复盘结论（可选），如「5日内涨幅超预期，模式有效」" />
        </div>
        <div class="mt-3">
          <input v-model.trim="actionTriggerReasonDraft" class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm" placeholder="触发原因（可选），如「政策利好 + 主题资金确认 + 行业轮动」" />
        </div>
        <!-- 建议仓位 + 失效条件 + 优先级 (Section 6.2) -->
        <div class="mt-3 grid gap-3 sm:grid-cols-3">
          <label class="text-sm font-semibold text-[var(--ink)]">
            建议仓位（可选）
            <input v-model="actionPositionPct"
              class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm"
              placeholder="如 5-8%（账户仓位区间）" />
            <div v-if="positionSizeHint" class="mt-1 text-xs text-emerald-700">{{ positionSizeHint }}</div>
            <div v-if="positionWarningVisible" class="mt-1 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
              确认动作建议填写账户仓位区间（如 5-8%），否则执行追溯将标记为"仓位未完整"
            </div>
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            失效条件（可选）
            <input v-model="actionExpiryCondition"
              class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm"
              placeholder="如：收盘跌破5日线失效" />
          </label>
          <label class="text-sm font-semibold text-[var(--ink)]">
            优先级
            <select v-model="actionPriority"
              class="mt-1 w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm">
              <option value="high">高</option>
              <option value="medium">中</option>
              <option value="low">低</option>
            </select>
          </label>
        </div>
        <QualityGate
          v-if="gateVisible"
          :result="currentGateResult"
          :warning-acknowledged="gateWarningAcknowledged"
          class="mt-3"
          @acknowledge-warnings="acknowledgeGateWarnings"
        />
        <div class="mt-3 flex flex-wrap gap-2 text-xs">
          <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="isActionPending || !actionTsCodeDraft.trim()" @click="submitManualAction('confirm', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">
            确认
          </button>
          <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="isActionPending || !actionTsCodeDraft.trim()" @click="submitManualAction('defer', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">
            暂缓
          </button>
          <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="isActionPending || !actionTsCodeDraft.trim()" @click="submitManualAction('reject', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">
            拒绝
          </button>
          <button class="rounded-full border border-[var(--line)] bg-white px-3 py-2 font-semibold text-[var(--ink)] disabled:opacity-60" :disabled="isActionPending || !actionTsCodeDraft.trim()" @click="submitManualAction('watch', actionTsCodeDraft, actionNoteDraft, actionTsCodeDraft)">
            观察
          </button>
        </div>
        <div v-if="recentActions.length" class="mt-4 space-y-2">
          <InfoCard
            v-for="item in recentActions"
            :key="item.id"
            :title="joinActionTitle(item)"
            :meta="joinActionMeta(item)"
            :description="item.note || item.payload?.context?.reason || '暂无备注'"
          >
            <template #badge>
              <StatusBadge :value="actionTone(item.action_type)" :label="actionLabel(item.action_type)" />
            </template>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <span class="metric-chip">source {{ actionSource(item) }}</span>
              <span class="metric-chip">状态 {{ actionDisplayStatus(item) }}</span>
              <span v-if="actionTraceId(item)" class="metric-chip">action_id {{ actionTraceId(item) }}</span>
              <span v-if="actionJobId(item)" class="metric-chip">job_id {{ actionJobId(item) }}</span>
            </div>
            <div class="mt-2 text-xs text-[var(--muted)]">
              <span>最近更新 {{ actionDisplayUpdatedAt(item) }}</span>
              <span v-if="actionStageSummary(item)"> · 阶段 {{ actionStageSummary(item) }}</span>
            </div>
            <div v-if="actionEvidenceSources(item).length" class="mt-2 flex flex-wrap gap-1.5 text-xs">
              <span class="font-semibold text-[var(--muted)]">证据：</span>
              <span v-for="(ev, ei) in actionEvidenceSources(item)" :key="ei" class="metric-chip text-[11px]">{{ ev }}</span>
            </div>
            <div class="mt-2 flex flex-wrap gap-1.5 text-xs">
              <span v-if="item?.payload?.position_recommendation || item?.action_payload?.position_recommendation"
                class="metric-chip text-sky-700">
                仓位 {{ item?.payload?.position_recommendation || item?.action_payload?.position_recommendation }}
              </span>
              <span v-if="item?.payload?.position_pct_range || item?.action_payload?.position_pct_range"
                class="metric-chip text-sky-700">
                目标 {{ item?.payload?.position_pct_range || item?.action_payload?.position_pct_range }}
              </span>
              <span v-if="item?.payload?.priority || item?.action_payload?.priority"
                class="metric-chip text-violet-700">
                优先级 {{ actionPriorityLabel(item?.payload?.priority || item?.action_payload?.priority) }}
              </span>
              <span v-if="item?.payload?.expiry_condition || item?.action_payload?.expiry_condition"
                class="metric-chip text-rose-700">
                失效 {{ item?.payload?.expiry_condition || item?.action_payload?.expiry_condition }}
              </span>
              <span v-if="item?.payload?.trigger_reason || item?.action_payload?.trigger_reason"
                class="metric-chip text-amber-700">
                触发原因 {{ item?.payload?.trigger_reason || item?.action_payload?.trigger_reason }}
              </span>
            </div>
            <div v-if="actionReviewConclusion(item)" class="mt-2 rounded-[14px] border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-800">
              <span class="font-semibold">复盘：</span>{{ actionReviewConclusion(item) }}
            </div>
            <div class="mt-3 flex flex-wrap gap-2">
              <RouterLink
                v-if="actionRestoreLink(item)"
                :to="actionRestoreLink(item)"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--brand)] transition hover:underline"
              >
                {{ actionRestoreLabel(item) }}
              </RouterLink>
              <RouterLink
                v-if="item.ts_code"
                :to="`/app/stocks/detail/${item.ts_code}`"
                class="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-semibold text-[var(--ink)] transition hover:border-[var(--brand)] hover:text-[var(--brand)]"
              >
                查看股票详情
              </RouterLink>
            </div>
          </InfoCard>
        </div>
        <div v-else class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
          {{ manualConfirmEmptyText }}
        </div>
      </PageSection>

      <!-- 复盘面板：汇总历史裁决动作，显示价格验证结果与复盘结论，形成策略反馈闭环 -->
      <PageSection title="决策复盘" subtitle="历史裁决的价格验证结果与复盘结论汇总，命中率反哺策略优化。">
        <div class="grid gap-3 md:grid-cols-3 mb-4">
          <StatCard title="确认命中率（5日）" :value="calibrationSummary.confirm_count ? `${(calibrationSummary.confirm_hit_rate_5d * 100).toFixed(0)}%` : '-'" :hint="`命中 ${calibrationSummary.confirm_hit_5d ?? 0} / ${calibrationSummary.confirm_count ?? 0} 次`" />
          <StatCard title="拒绝命中率（5日）" :value="calibrationSummary.reject_count ? `${(calibrationSummary.reject_hit_rate_5d * 100).toFixed(0)}%` : '-'" :hint="`命中 ${calibrationSummary.reject_hit_5d ?? 0} / ${calibrationSummary.reject_count ?? 0} 次`" />
          <StatCard title="综合命中率（5日）" :value="calibrationSummary.total_count ? `${(calibrationSummary.total_hit_rate_5d * 100).toFixed(0)}%` : '-'" :hint="`共 ${calibrationSummary.total_count ?? 0} 条有效裁决`" />
        </div>
        <div v-if="reviewItems.length" class="space-y-2">
          <InfoCard
            v-for="item in reviewItems"
            :key="`review-${item.id}`"
            :title="`${item.stock_name || item.ts_code || '-'} · ${actionLabel(item.action_type)}`"
            :meta="`${item.ts_code || '-'} · ${item.created_at || '-'} · ${item.actor || '-'}`"
            :description="item.note || ''"
          >
            <template #badge>
              <StatusBadge
                :value="item.hit_5d === true ? 'success' : item.hit_5d === false ? 'danger' : 'muted'"
                :label="item.hit_5d === true ? '命中' : item.hit_5d === false ? '未命中' : '待验证'"
              />
            </template>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
              <span v-if="item.price_at_verdict != null" class="metric-chip">裁决时价 <strong>{{ Number(item.price_at_verdict).toFixed(2) }}</strong></span>
              <span v-if="item.return_5d != null" :class="['metric-chip', Number(item.return_5d) > 0 ? 'text-emerald-700' : Number(item.return_5d) < 0 ? 'text-red-600' : '']">5日 <strong>{{ Number(item.return_5d) >= 0 ? '+' : '' }}{{ Number(item.return_5d).toFixed(1) }}%</strong></span>
              <span v-if="item.return_20d != null" :class="['metric-chip', Number(item.return_20d) > 0 ? 'text-emerald-700' : Number(item.return_20d) < 0 ? 'text-red-600' : '']">20日 <strong>{{ Number(item.return_20d) >= 0 ? '+' : '' }}{{ Number(item.return_20d).toFixed(1) }}%</strong></span>
              <span v-if="item.return_60d != null" :class="['metric-chip', Number(item.return_60d) > 0 ? 'text-emerald-700' : Number(item.return_60d) < 0 ? 'text-red-600' : '']">60日 <strong>{{ Number(item.return_60d) >= 0 ? '+' : '' }}{{ Number(item.return_60d).toFixed(1) }}%</strong></span>
            </div>
            <div v-if="actionReviewConclusion(item)" class="mt-2 rounded-[14px] border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-800">
              <span class="font-semibold">复盘结论：</span>{{ actionReviewConclusion(item) }}
            </div>
            <div v-else class="mt-2 text-xs text-[var(--muted)]">尚无复盘结论 — 可在"人工确认记录"中填写"复盘结论"字段后重新提交。</div>
          </InfoCard>
        </div>
        <div v-else class="rounded-[18px] border border-[var(--line)] bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
          暂无已裁决动作可复盘（需先产生确认或拒绝动作）。
        </div>
      </PageSection>
    </div>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useMutation, useQueries, useQuery } from '@tanstack/vue-query'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import AppShell from '../../shared/ui/AppShell.vue'
import PageSection from '../../shared/ui/PageSection.vue'
import DataTable from '../../shared/ui/DataTable.vue'
import InfoCard from '../../shared/ui/InfoCard.vue'
import StatCard from '../../shared/ui/StatCard.vue'
import StatusBadge from '../../shared/ui/StatusBadge.vue'
import QualityGate from '../../shared/ui/QualityGate.vue'
import { fetchDecisionActions, fetchDecisionBoard, fetchDecisionCalibration, fetchDecisionHistory, recordDecisionAction, runDecisionSnapshot, setDecisionKillSwitch } from '../../services/api/decision'
import { fetchLatestAllocation } from '../../services/api/portfolio_allocation'
import type { DecisionTraceReceipt } from '../../services/api/decision'
import { fetchStockPrices } from '../../services/api/stocks'
import { formatNumber } from '../../shared/utils/format'
import { buildCleanQuery, readQueryString } from '../../shared/utils/urlState'
import { evaluateGate, serializeGateAudit } from '../../shared/utils/qualityGate'
import type { GateResult } from '../../shared/utils/qualityGate'

type ActionStatus = 'idle' | 'pending' | 'success' | 'error'
type DecisionContext = {
  from: string
  industry: string
  keyword: string
  score_date: string
}
type SnapshotFeedback = {
  receipt?: DecisionTraceReceipt
  run_id?: string
  snapshot_id?: string
  action_id?: string
  job_id?: string
  trace?: {
    run_id?: string
    snapshot_id?: string
    action_id?: string
  }
}

const route = useRoute()
const router = useRouter()

const focusTsCode = ref('')
const keywordFilter = ref('')
const pageSize = ref(12)
const killReasonDraft = ref('手动切换')
const actionTsCodeDraft = ref('000001.SZ')
const actionNoteDraft = ref('已确认')
const actionEvidenceDraft = ref('')
const actionReviewConclusionDraft = ref('')
const actionTriggerReasonDraft = ref('')
const actionPositionPct = ref('')     // e.g. "5%-8%"
const actionExpiryCondition = ref('') // e.g. "收盘跌破5日线失效"
const actionPriority = ref('medium')  // high/medium/low
const actionPositionPctRange = ref('')    // e.g. "5-8"
const actionTargetPositionPct = ref('')   // e.g. "6.5"
const message = ref('')
const snapshotStatus = ref<ActionStatus>('idle')
const snapshotStatusText = ref('')
const decisionContext = ref<DecisionContext>({ from: '', industry: '', keyword: '', score_date: '' })
const lastTraceFeedback = ref<{ action_id: string; run_id: string; snapshot_id: string }>({ action_id: '', run_id: '', snapshot_id: '' })

// Gate state: tracks pending action type and warning acknowledgment
const pendingGateActionType = ref<'confirm' | 'reject' | 'defer' | 'watch' | 'review' | ''>('')
const gateWarningAcknowledged = ref(false)

// ---- 3.4 Protocolized Workflow Stage Definitions ----
const DECISION_STAGES = [
  { key: 'input', label: '输入', hint: '设置筛选条件或聚焦股票代码' },
  { key: 'evidence', label: '取证', hint: '系统拉取评分、行业数据和市场模式' },
  { key: 'attribution', label: '归因', hint: '短名单关联市场模式、风险检查' },
  { key: 'judgment', label: '判断', hint: '结合上下文做看多/看空/暂缓决策' },
  { key: 'action', label: '动作建议', hint: '记录确认/拒绝/观察人工裁决' },
  { key: 'risk', label: '风险披露', hint: 'Kill Switch + 验证层结果' },
  { key: 'output', label: '输出', hint: '双轨证据产物 (JSON + Markdown)' },
] as const

type DecisionStageKey = typeof DECISION_STAGES[number]['key']

const currentDecisionStage = computed<DecisionStageKey>(() => {
  if (isActionPending.value) return 'action'
  if (lastActionMarkdown.value) return 'output'
  if (pendingGateActionType.value) return 'risk'
  if (isBoardFetching.value) return 'evidence'
  if (board.value && Object.keys(board.value).length > 0) return 'judgment'
  return 'input'
})

const currentDecisionStageLabel = computed(() => {
  const s = DECISION_STAGES.find((st) => st.key === currentDecisionStage.value)
  return s?.label || ''
})

const currentDecisionStageHint = computed(() => {
  const s = DECISION_STAGES.find((st) => st.key === currentDecisionStage.value)
  return s?.hint || ''
})

function decisionStageClass(key: DecisionStageKey): string {
  const stageOrder = DECISION_STAGES.map((s) => s.key)
  const currentIdx = stageOrder.indexOf(currentDecisionStage.value)
  const thisIdx = stageOrder.indexOf(key)
  if (key === currentDecisionStage.value) return 'bg-[var(--brand)] text-white'
  if (thisIdx < currentIdx) return 'border border-emerald-300 bg-emerald-50 text-emerald-700'
  return 'border border-[var(--line)] bg-[var(--panel-soft)] text-[var(--muted)]'
}

const boardQuery = useQuery({
  queryKey: computed(() => ['decision-board', focusTsCode.value, keywordFilter.value, pageSize.value]),
  queryFn: () => fetchDecisionBoard({
    ts_code: focusTsCode.value,
    keyword: keywordFilter.value,
    page: 1,
    page_size: pageSize.value,
  }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 30_000 : 120_000),
})

const historyQuery = useQuery({
  queryKey: ['decision-history'],
  queryFn: () => fetchDecisionHistory({ page: 1, page_size: 10 }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 60_000 : 180_000),
})

const actionsQuery = useQuery({
  queryKey: computed(() => ['decision-actions', focusTsCode.value]),
  queryFn: () => fetchDecisionActions({ page: 1, page_size: 10, ts_code: focusTsCode.value }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 60_000 : 180_000),
})

const calibrationQuery = useQuery({
  queryKey: computed(() => ['decision-calibration', focusTsCode.value]),
  queryFn: () => fetchDecisionCalibration({ page: 1, page_size: 20, ts_code: focusTsCode.value }),
  refetchInterval: () => (document.visibilityState === 'visible' ? 120_000 : 600_000),
})

const { data: allocationData } = useQuery({
  queryKey: ['portfolio-allocation-decision'],
  queryFn: fetchLatestAllocation,
})
const currentAllocation = computed(() => (allocationData.value as any)?.allocation ?? null)
const positionSizeHint = computed(() => {
  const alloc = currentAllocation.value
  if (!alloc) return ''
  return `当前单票上限 ${alloc.max_single_position_pct ?? 8}%，建议区间 ${Math.max(1, Math.floor((alloc.max_single_position_pct ?? 8) / 2))}%–${alloc.max_single_position_pct ?? 8}%`
})

// Section 6.3: Show warning when confirm is pending but no position info is filled
const positionWarningVisible = computed(() =>
  pendingGateActionType.value === 'confirm' &&
  !actionPositionPct.value.trim() &&
  !actionPositionPctRange.value.trim()
)

const toggleKillSwitchMutation = useMutation({
  mutationFn: (allowTrading: boolean) => setDecisionKillSwitch({ allow_trading: allowTrading, reason: killReasonDraft.value }),
  onSuccess: async () => {
    message.value = 'Kill Switch 状态已更新。'
    await refreshAll()
  },
  onError: (error: Error) => {
    message.value = `Kill Switch 切换失败：${error.message}`
  },
})

const runSnapshotMutation = useMutation({
  mutationFn: () => runDecisionSnapshot(),
  onSuccess: async (payload: SnapshotFeedback) => {
    snapshotStatus.value = 'success'
    const ts = new Date().toLocaleString('zh-CN', { hour12: false })
    const trace = {
      action_id: String(payload?.receipt?.trace?.action_id || payload?.trace?.action_id || payload?.action_id || '').trim(),
      run_id: String(payload?.receipt?.trace?.run_id || payload?.trace?.run_id || payload?.run_id || payload?.job_id || '').trim(),
      snapshot_id: String(payload?.receipt?.trace?.snapshot_id || payload?.trace?.snapshot_id || payload?.snapshot_id || '').trim(),
    }
    lastTraceFeedback.value = trace
    const traceText = trace.snapshot_id || trace.run_id || '无（后端未返回）'
    snapshotStatusText.value = `状态 success · 生成时间 ${ts} · 结果标识 ${traceText}`
    message.value = trace.snapshot_id || trace.run_id ? `决策快照已生成（${trace.snapshot_id || trace.run_id}）。` : '决策快照已生成，但后端未返回标识。'
    await refreshAll()
  },
  onError: (error: Error) => {
    snapshotStatus.value = 'error'
    snapshotStatusText.value = error.message || '未知错误'
    message.value = `快照生成失败：${error.message}`
  },
})

const actionMutation = useMutation({
  mutationFn: (payload: { action_type: 'confirm' | 'reject' | 'defer' | 'watch' | 'review'; ts_code: string; stock_name?: string; note?: string; _gate_audit?: string; idempotency_key?: string }) => {
    const evidenceRaw = actionEvidenceDraft.value.trim()
    const evidenceSources = evidenceRaw
      ? evidenceRaw.split(/[,，;；]/).map((s) => ({ label: s.trim() })).filter((s) => s.label)
      : undefined
    const reviewConclusion = actionReviewConclusionDraft.value.trim() || undefined
    return recordDecisionAction({
      action_type: payload.action_type,
      ts_code: payload.ts_code,
      stock_name: payload.stock_name || '',
      note: payload.note || '',
      snapshot_date: snapshotDate.value,
      context: {
        source: decisionContext.value.from || 'decision_board',
        source_module: decisionContext.value.from || 'decision_board',
        market_mode: marketRegime.value.mode || '',
        trade_plan: tradePlan.value.mode || '',
        gate_audit: payload._gate_audit || '',
      },
      evidence_sources: evidenceSources,
      review_conclusion: reviewConclusion,
      position_recommendation: actionPositionPct.value.trim() || undefined,
      position_pct_range: actionPositionPctRange.value.trim() || undefined,
      target_position_pct: actionTargetPositionPct.value ? Number(actionTargetPositionPct.value) : undefined,
      expiry_condition: actionExpiryCondition.value.trim() || undefined,
      priority: (actionPriority.value || 'medium') as 'high' | 'medium' | 'low',
      trigger_reason: actionTriggerReasonDraft.value.trim() || undefined,
    })
  },
  onSuccess: async (payload) => {
    const data = (payload || {}) as Record<string, any>
    const receipt = (data.receipt || {}) as DecisionTraceReceipt
    const trace = {
      action_id: String(receipt.trace?.action_id || (data.trace as Record<string, any> | undefined)?.action_id || data.action_id || '').trim(),
      run_id: String(receipt.trace?.run_id || (data.trace as Record<string, any> | undefined)?.run_id || data.run_id || '').trim(),
      snapshot_id: String(receipt.trace?.snapshot_id || (data.trace as Record<string, any> | undefined)?.snapshot_id || data.snapshot_id || '').trim(),
    }
    lastTraceFeedback.value = trace
    // Generate dual-track Markdown artifact (3.7)
    const gateAudit = String(data.context?.gate_audit || receipt.context?.gate_audit || '').trim()
    lastActionMarkdown.value = generateActionMarkdown(
      String(data.action_type || pendingGateActionType.value || '').trim(),
      actionTsCodeDraft.value,
      actionNoteDraft.value,
      actionEvidenceDraft.value,
      trace,
      gateAudit,
    )
    const messageParts: string[] = [
      trace.action_id ? `动作已记录（${trace.action_id}）` : '动作已记录',
    ]
    const funnelText = formatFunnelSyncFeedback(data?.funnel_sync)
    if (funnelText) messageParts.push(funnelText)
    if (data?.funnel_sync_warning) {
      messageParts.push(String(data.funnel_sync_warning || ''))
    }
    // Show execution task if auto-created
    if (data?.execution_task?.order_id) {
      messageParts.push(`执行任务已自动创建：${data.execution_task.order_id}`)
    } else if (data?.execution_task_warning) {
      messageParts.push(`执行任务提示：${data.execution_task_warning}`)
    }
    message.value = messageParts.join('；')
    actionPositionPctRange.value = ''
    actionTargetPositionPct.value = ''
    await refreshAll()
  },
  onError: (error: Error) => {
    message.value = `人工确认记录失败：${error.message}`
  },
})

const board = computed<Record<string, any>>(() => (boardQuery.data.value || {}) as Record<string, any>)
const isBoardFetching = computed(() => Boolean(boardQuery.isFetching.value))
const boardErrorText = computed(() => {
  const value = boardQuery.error.value
  if (!value) return ''
  return value instanceof Error ? value.message : String(value)
})
const marketRegime = computed<Record<string, any>>(() => (board.value.market_regime || {}) as Record<string, any>)
const industries = computed<Array<Record<string, any>>>(() => (board.value.industries || []) as Array<Record<string, any>>)
const shortlist = computed<Array<Record<string, any>>>(() => (board.value.shortlist || []) as Array<Record<string, any>>)
const tomorrowCandidates = computed<Array<Record<string, any>>>(() =>
  shortlist.value
    .filter((item) => String(item?.position_label || '').trim() === '重点观察')
    .slice(0, 6),
)
const tradePlan = computed<Record<string, any>>(() => (board.value.trade_plan || {}) as Record<string, any>)
const validation = computed<Record<string, any>>(() => (board.value.validation || {}) as Record<string, any>)
const pipelineHealth = computed<Record<string, any>>(() => (board.value.pipeline_health || {}) as Record<string, any>)
const focusStock = computed<Record<string, any> | null>(() => (board.value.focus_stock || null) as Record<string, any> | null)
const killSwitch = computed<Record<string, any>>(() => (board.value.kill_switch || {}) as Record<string, any>)
const historyItems = computed<Array<Record<string, any>>>(() => (historyQuery.data.value?.items || []) as Array<Record<string, any>>)
const recentActions = computed<Array<Record<string, any>>>(() => (actionsQuery.data.value?.items || []) as Array<Record<string, any>>)
const recentVerdicts = computed(() =>
  recentActions.value
    .filter((item) => ['confirm', 'reject', 'defer'].includes(String(item.action_type || '').toLowerCase()))
    .slice(0, 6),
)

// Today's summary for decision hub
const todayWatchCount = computed(() =>
  recentActions.value.filter((item) => String(item.action_type || '').toLowerCase() === 'watch').length,
)
const riskIndicatorText = computed(() => {
  const riskNote = String(tradePlan.value.risk_note || board.value.risk_note || '').trim()
  if (riskNote) return riskNote.slice(0, 20)
  const killAllowed = killSwitch.value.allow_trading !== false
  return killAllowed ? '无紧急风险提示' : '⚠ Kill Switch 已关'
})
const riskIndicatorNote = computed(() => {
  const undecided = recentActions.value.filter((item) =>
    ['watch', 'review'].includes(String(item.action_type || '').toLowerCase()),
  ).length
  return `${undecided} 项待复核；复盘完成后在下方"决策复盘"区域记录结论。`
})

// Price-since-verdict queries — one per verdict card
const verdictPriceQueries = useQueries({
  queries: computed(() =>
    recentVerdicts.value.map((item) => {
      const dateStr = String(item.created_at || '').substring(0, 10).replace(/-/g, '')
      return {
        queryKey: ['verdict-price', String(item.ts_code || ''), dateStr] as const,
        queryFn: () => fetchStockPrices({ ts_code: item.ts_code, start_date: dateStr, page: 1, page_size: 60 }),
        enabled: Boolean(item.ts_code && dateStr.length === 8),
        staleTime: 5 * 60 * 1000,
      }
    }),
  ),
})

const verdictReturnMap = computed(() => {
  const map = new Map<number | string, { pct: number | null; loading: boolean }>()
  const queries = verdictPriceQueries.value
  recentVerdicts.value.forEach((item, idx) => {
    const q = queries[idx]
    if (!q) { map.set(item.id, { pct: null, loading: false }); return }
    if (q.isPending) { map.set(item.id, { pct: null, loading: true }); return }
    const priceItems: Array<Record<string, any>> = (q.data as any)?.items ?? []
    if (priceItems.length < 2) { map.set(item.id, { pct: null, loading: false }); return }
    const latestClose = Number(priceItems[0]?.close ?? 0)
    const verdictClose = Number(priceItems[priceItems.length - 1]?.close ?? 0)
    if (!verdictClose) { map.set(item.id, { pct: null, loading: false }); return }
    map.set(item.id, { pct: (latestClose - verdictClose) / verdictClose * 100, loading: false })
  })
  return map
})

const snapshotDate = computed(() => String(board.value.snapshot_date || '').trim())
const latestSnapshotId = computed(() => {
  const firstHistory = historyItems.value[0]
  return String(firstHistory?.trace?.snapshot_id || '').trim()
})
const latestActionId = computed(() => {
  const firstAction = recentActions.value[0]
  return String(firstAction?.trace?.action_id || '').trim()
})
const topIndustryName = computed(() => String(industries.value[0]?.industry || '').trim())
const killSwitchLabel = computed(() => (Number(killSwitch.value.allow_trading ?? 1) === 1 ? '交易允许' : '交易暂停'))
const killSwitchTone = computed(() => (Number(killSwitch.value.allow_trading ?? 1) === 1 ? 'success' : 'danger'))
const pipelineHealthStatusTone = computed(() => {
  const status = String(pipelineHealth.value.status || '').trim()
  if (status === 'ready') return 'success'
  if (status === 'degraded') return 'warning'
  if (status === 'empty' || status === 'not_initialized') return 'danger'
  return 'muted'
})
const pipelineHealthMissingInputs = computed<string[]>(() => {
  const inputs = pipelineHealth.value.missing_inputs
  return Array.isArray(inputs) ? inputs.slice(0, 5).map((item) => String(item)) : []
})
const isTogglePending = computed(() => Boolean(toggleKillSwitchMutation.isPending.value))
const isSnapshotPending = computed(() => Boolean(runSnapshotMutation.isPending.value))
const isActionPending = computed(() => Boolean(actionMutation.isPending.value))

// Quality Gate evaluation — re-evaluates whenever form state changes
const currentGateResult = computed<GateResult>(() =>
  evaluateGate({
    action_type: pendingGateActionType.value || 'watch',
    ts_code: actionTsCodeDraft.value,
    note: actionNoteDraft.value,
    evidence_sources: actionEvidenceDraft.value,
    position_pct_range: actionPositionPctRange.value,
    position_recommendation: actionPositionPct.value,
    decision_context_from: decisionContext.value.from,
    snapshot_date: snapshotDate.value,
  }),
)
const gateVisible = computed(
  () =>
    ['confirm', 'reject', 'defer', 'watch'].includes(pendingGateActionType.value) &&
    (currentGateResult.value.blockers.length > 0 ||
      currentGateResult.value.warnings.length > 0 ||
      currentGateResult.value.infos.length > 0),
)
const calibrationSummary = computed<Record<string, any>>(() => (calibrationQuery.data.value as any)?.summary ?? {})
const calibrationItems = computed<Array<Record<string, any>>>(() => (calibrationQuery.data.value as any)?.items ?? [])

const calibrationColumns = [
  { key: 'stock_name', label: '股票' },
  { key: 'action_type', label: '裁决' },
  { key: 'created_at', label: '裁决日期' },
  { key: 'price_at_verdict', label: '裁决时价格' },
  { key: 'return_5d', label: '5日收益' },
  { key: 'return_20d', label: '20日收益' },
  { key: 'return_60d', label: '60日收益' },
  { key: 'hit_5d', label: '结果' },
]

const stockColumns = [
  { key: 'ts_code', label: '股票' },
  { key: 'total_score', label: '总分' },
  { key: 'industry_total_score', label: '行业分' },
  { key: 'position_label', label: '位置' },
  { key: 'actionable', label: '执行参数' },
  { key: 'decision_risk', label: '风险' },
  { key: 'actions', label: '动作' },
]

const historyColumns = [
  { key: 'snapshot_date', label: '快照日期' },
  { key: 'snapshot_type', label: '类型' },
  { key: 'summary', label: '摘要' },
  { key: 'created_at', label: '创建时间' },
]

async function refreshAll() {
  await Promise.all([boardQuery.refetch(), historyQuery.refetch(), actionsQuery.refetch()])
}

function applyFilters() {
  refreshAll()
}

function runSnapshot() {
  if (runSnapshotMutation.isPending.value) return
  snapshotStatus.value = 'pending'
  snapshotStatusText.value = ''
  message.value = '正在生成决策快照，请稍候。'
  runSnapshotMutation.mutate()
}

function submitManualAction(actionType: 'confirm' | 'reject' | 'defer' | 'watch' | 'review', tsCode: string, note: string, stockName = '') {
  // Submit lock: prevent duplicate submissions while mutation is in flight
  if (isActionPending.value) return
  const normalizedTsCode = String(tsCode || '').trim().toUpperCase()
  // Set pending action type so gate can evaluate
  pendingGateActionType.value = actionType
  gateWarningAcknowledged.value = false

  if (!normalizedTsCode) return
  actionTsCodeDraft.value = normalizedTsCode
  if (String(note || '').trim()) actionNoteDraft.value = String(note || '').trim()

  // Evaluate gate before submission
  const gateCtx = {
    action_type: actionType,
    ts_code: normalizedTsCode,
    note: String(note || actionNoteDraft.value || '').trim(),
    evidence_sources: actionEvidenceDraft.value,
    position_pct_range: actionPositionPctRange.value,
    position_recommendation: actionPositionPct.value,
    decision_context_from: decisionContext.value.from,
    snapshot_date: snapshotDate.value,
  }
  const gateResult = evaluateGate(gateCtx)

  // Hard block: do not submit if blockers present
  if (gateResult.blockers.length > 0) {
    message.value = `提交被阻断 (${gateResult.blockers.map((v) => v.rule.rule_id).join(',')}): ${gateResult.blockers[0].rule.message}`
    return
  }

  // Soft block: warnings require user acknowledgment (handled via UI flow)
  // For inline shortlist buttons we skip warnings to avoid breaking quick actions
  const gateAudit = serializeGateAudit(gateResult)

  actionMutation.mutate({
    action_type: actionType,
    ts_code: normalizedTsCode,
    note: String(note || actionNoteDraft.value || '').trim(),
    stock_name: String(stockName || normalizedTsCode).trim(),
    _gate_audit: gateAudit,
    idempotency_key: `${actionType}-${normalizedTsCode}-${Date.now()}`,
  })
}

function acknowledgeGateWarnings() {
  gateWarningAcknowledged.value = true
}

// ---- Dual-track evidence: Markdown artifact generation ----
const lastActionMarkdown = ref('')

function generateActionMarkdown(
  actionType: string,
  tsCode: string,
  note: string,
  evidence: string,
  trace: { action_id: string; run_id: string; snapshot_id: string },
  gateAudit: string,
): string {
  const ts = new Date().toLocaleString('zh-CN', { hour12: false })
  const sourceModule = decisionContext.value.from || 'decision_board'
  return [
    `# 决策动作证据摘要`,
    ``,
    `| 字段 | 值 |`,
    `|------|---|`,
    `| 动作类型 | ${actionType} |`,
    `| 股票代码 | ${tsCode} |`,
    `| 记录时间 | ${ts} |`,
    `| 来源模块 | ${sourceModule} |`,
    `| 快照日期 | ${snapshotDate.value || '-'} |`,
    ``,
    `## 决策依据`,
    ``,
    note || '（无备注）',
    ``,
    `## 证据来源`,
    ``,
    evidence || '（无证据来源记录）',
    ``,
    `## 链路标识（Trace IDs）`,
    ``,
    `- action_id: \`${trace.action_id || '-'}\``,
    `- run_id: \`${trace.run_id || '-'}\``,
    `- snapshot_id: \`${trace.snapshot_id || '-'}\``,
    ``,
    `## 质量门禁审计`,
    ``,
    `\`\`\``,
    gateAudit || '（无审计记录）',
    `\`\`\``,
    ``,
    `---`,
    `*由 Zanbo Quant 决策板自动生成 — 可与 decision_actions 表通过 action_id 互查*`,
  ].join('\n')
}

function downloadLastActionMarkdown() {
  if (!lastActionMarkdown.value) return
  const blob = new Blob([lastActionMarkdown.value], { type: 'text/markdown;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  const traceId = lastTraceFeedback.value.action_id || new Date().getTime().toString()
  link.href = url
  link.download = `decision_evidence_${traceId}.md`
  document.body.appendChild(link)
  link.click()
  link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

function pauseTrading() {
  toggleKillSwitchMutation.mutate(false)
}

function resumeTrading() {
  toggleKillSwitchMutation.mutate(true)
}

const ACTION_LABEL_MAP: Record<string, string> = {
  confirm: '确认 / 看多',
  reject: '拒绝 / 看空',
  defer: '暂缓 / 中性',
  watch: '观察',
  review: '复核',
}
const ACTION_TONE_MAP: Record<string, string> = {
  confirm: 'success',
  reject: 'danger',
  defer: 'warning',
  watch: 'info',
  review: 'muted',
}

function actionLabel(type: string) {
  return ACTION_LABEL_MAP[String(type || '').toLowerCase()] || String(type || '-').toUpperCase()
}

function actionTone(type: string) {
  return ACTION_TONE_MAP[String(type || '').toLowerCase()] || 'muted'
}

function actionPriorityLabel(value: unknown): string {
  const key = String(value || '').toLowerCase()
  if (key === 'high') return '高'
  if (key === 'low') return '低'
  if (key === 'medium' || key === 'med' || key === 'normal') return '中'
  return key || '-'
}

function formatFunnelSyncFeedback(syncPayload: unknown): string {
  const sync = (syncPayload || {}) as Record<string, any>
  if (!sync || typeof sync !== 'object') return ''
  if (sync.ok !== true) return ''
  if (sync.skipped) return '漏斗同步已跳过'

  const state = String(sync.state || '').trim()
  const stateLabelMap: Record<string, string> = {
    decision_ready: '待决策',
    confirmed: '已确认入池',
    rejected: '已拒绝',
    deferred: '已暂缓',
  }
  const stateLabel = stateLabelMap[state] || state || '未知状态'
  const created = Boolean(sync.created)
  const transitionCount = Number(sync.transition_count || 0)
  return `漏斗已同步（${stateLabel}${created ? '，新建候选' : ''}${transitionCount > 0 ? `，状态变更 ${transitionCount} 次` : ''}）`
}

function formatReturn(val: number | null | undefined): string {
  if (val === null || val === undefined) return '-'
  const sign = val >= 0 ? '+' : ''
  return `${sign}${val.toFixed(1)}%`
}

function returnClass(val: number | null | undefined): string {
  if (val === null || val === undefined) return 'text-[var(--muted)]'
  if (val > 0.05) return 'text-emerald-600 font-semibold'
  if (val < -0.05) return 'text-red-600 font-semibold'
  return 'text-[var(--muted)]'
}

function verdictReturn(item: Record<string, any>): { text: string; cls: string } {
  const entry = verdictReturnMap.value.get(item.id)
  if (!entry) return { text: '', cls: '' }
  if (entry.loading) return { text: '...', cls: 'text-[var(--muted)]' }
  if (entry.pct === null) return { text: '', cls: '' }
  const sign = entry.pct >= 0 ? '+' : ''
  const cls = entry.pct > 0.05 ? 'text-emerald-600 font-semibold' : entry.pct < -0.05 ? 'text-red-600 font-semibold' : 'text-[var(--muted)]'
  return { text: `${sign}${entry.pct.toFixed(1)}%`, cls }
}

function actionSource(item: Record<string, any>) {
  return String(item.source || item.receipt?.source || item.context?.source || item.payload?.context?.source || 'decision_board').trim() || 'decision_board'
}

function actionTraceId(item: Record<string, any>) {
  return String(item.trace?.action_id || item.receipt?.trace?.action_id || item.action_id || '').trim()
}

function actionJobId(item: Record<string, any>) {
  return String(item.context?.job_id || item.receipt?.context?.job_id || item.payload?.context?.job_id || '').trim()
}

function actionStageSummary(item: Record<string, any>) {
  return String(item.job_trace?.summary || '').trim()
}

function actionExecutionStatus(item: Record<string, any>) {
  return String(item.payload?.execution_status || item.execution_status || '').trim()
}

function actionDisplayStatus(item: Record<string, any>) {
  const execStatus = actionExecutionStatus(item)
  if (execStatus) return execStatus
  const receiptStatus = String(item.receipt?.status || item.status || '').trim()
  if (receiptStatus) return receiptStatus
  return '已记录'
}

function actionDisplayUpdatedAt(item: Record<string, any>) {
  const traceTime = String(item.job_trace?.updated_at || '').trim()
  if (traceTime) return traceTime
  return String(item.created_at || '-').trim()
}

function actionEvidenceSources(item: Record<string, any>): string[] {
  const raw = item.payload?.evidence_sources || item.evidence_sources
  if (!Array.isArray(raw)) return []
  return raw.map((ev: any) => String(ev?.label || ev || '').trim()).filter(Boolean)
}

function actionReviewConclusion(item: Record<string, any>) {
  return String(item.payload?.review_conclusion || item.review_conclusion || '').trim()
}

const reviewItems = computed(() => calibrationItems.value.slice(0, 20))
const validationDescription = computed(() => {
  if (String(validation.value.summary || '').trim()) return String(validation.value.summary)
  if (validation.value.latest?.error_message || validation.value.latest?.error_code) {
    return `最近一次验证未完成：${validation.value.latest.error_message || validation.value.latest.error_code}`
  }
  if ((validation.value.done ?? 0) > 0 || (validation.value.error ?? 0) > 0) {
    return `已回写 done ${validation.value.done ?? 0} / error ${validation.value.error ?? 0}，但还没有结构化验证摘要。`
  }
  return '暂无验证数据，可能是尚未触发验证、验证未回写，或当前快照没有产生可验证对象。'
})
const verdictEmptyText = computed(() => {
  if (recentActions.value.length > 0) return '已有动作记录，但当前还没有 confirm / reject 类型的裁决，说明决策链停在观察或复核阶段。'
  return '暂无裁决记录，说明当前还没有形成真正进入确认/拒绝分支的决策动作。'
})
const manualConfirmEmptyText = computed(() => {
  if (recentActions.value.length > 0) return '当前已有动作，但还没有可回查的人工确认记录，可能尚未确认、未回写，或仍停留在观察/复核状态。'
  return '暂无人工确认记录，因为当前尚未形成需要人工确认的决策动作。'
})

function actionRestoreLink(item: Record<string, any>) {
  const source = actionSource(item)
  const jobId = actionJobId(item)
  if (!jobId) return ''
  if (source === 'multi_role_v3') return `/app/research/multi-role?restore_job=${encodeURIComponent(jobId)}`
  if (source === 'chief_roundtable') return `/app/research/roundtable?job_id=${encodeURIComponent(jobId)}`
  return ''
}

function actionRestoreLabel(item: Record<string, any>) {
  const source = actionSource(item)
  if (source === 'multi_role_v3') return '回到多角色分析'
  if (source === 'chief_roundtable') return '回到首席圆桌'
  return '查看来源页'
}

function joinActionTitle(item: Record<string, any>) {
  return `${item.stock_name || item.ts_code || '-'} · ${actionLabel(item.action_type)}`
}

function joinActionMeta(item: Record<string, any>) {
  return [item.ts_code || '-', item.actor || '-', item.created_at || '-'].filter(Boolean).join(' · ')
}

const EXTERNAL_SOURCE_MODULES = ['news', 'chatroom', 'signal_graph']
const hasExternalSource = computed(() => EXTERNAL_SOURCE_MODULES.includes(decisionContext.value.from))

const hasDecisionContext = computed(() =>
  Boolean(
    decisionContext.value.from ||
    decisionContext.value.industry ||
    decisionContext.value.keyword ||
    decisionContext.value.score_date,
  ),
)

function sourceModuleLabel(from: string): string {
  const MAP: Record<string, string> = {
    news: '新闻',
    chatroom: '群聊',
    signal_graph: '信号图谱',
    stock_detail: '股票详情',
    prices: '价格中心',
    stock_scores: '评分板',
  }
  return MAP[from] || from
}

function looksLikeTsCode(value: string) {
  return /^[0-9]{6}\.(SZ|SH|BJ)$/i.test(String(value || '').trim())
}

function syncContextFromRoute() {
  const q = route.query as Record<string, unknown>
  const next: DecisionContext = {
    from: readQueryString(q, 'from', ''),
    industry: readQueryString(q, 'industry', ''),
    keyword: readQueryString(q, 'keyword', ''),
    score_date: readQueryString(q, 'score_date', ''),
  }

  // Pre-fill action form from structured action template params (news/chatroom bridges)
  const evidenceParam = readQueryString(q, 'evidence', '').trim()
  const noteParam = readQueryString(q, 'note', '').trim()
  const tsCodeParam = readQueryString(q, 'ts_code', '').trim()
  if (evidenceParam) actionEvidenceDraft.value = evidenceParam
  if (noteParam) actionNoteDraft.value = noteParam
  if (tsCodeParam) actionTsCodeDraft.value = tsCodeParam.toUpperCase()

  const changed = JSON.stringify(next) !== JSON.stringify(decisionContext.value)
  decisionContext.value = next
  if (!changed) return
  if (next.keyword) {
    const keyword = next.keyword.trim()
    if (looksLikeTsCode(keyword)) {
      focusTsCode.value = keyword.toUpperCase()
      keywordFilter.value = ''
    } else {
      keywordFilter.value = keyword
      focusTsCode.value = ''
    }
    message.value = '已按评分页上下文同步决策板筛选。'
  } else if (next.industry) {
    keywordFilter.value = next.industry.trim()
    focusTsCode.value = ''
    message.value = '已按行业上下文同步决策板筛选。'
  }
}

function clearDecisionContext() {
  decisionContext.value = { from: '', industry: '', keyword: '', score_date: '' }
  keywordFilter.value = ''
  focusTsCode.value = ''
  message.value = '已清空研究上下文，回到默认决策板视图。'
  const q = route.query as Record<string, unknown>
  router.replace({
    query: buildCleanQuery({
      ...q,
      from: '',
      industry: '',
      keyword: '',
      score_date: '',
    }),
  })
}

watch(
  () => route.query,
  () => {
    syncContextFromRoute()
  },
  { immediate: true },
)
</script>
