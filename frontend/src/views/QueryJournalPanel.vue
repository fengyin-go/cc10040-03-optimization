<template>
  <section class="journal-panel" data-testid="query-journal-panel">
    <header class="journal-head">
      <div>
        <h3>查询统计明细</h3>
        <p class="journal-desc">
          按指标和时间过滤后用同一批已确认执行计算分组数量、慢查询数和平均耗时；翻页沿用同一快照，
          新入账的日志不会改动当前汇总。
        </p>
      </div>
      <span v-if="stats" class="journal-snapshot" data-testid="snapshot-token">
        批次 {{ stats.snapshot.token }} · {{ stats.snapshot.execution_count }} 笔
      </span>
    </header>

    <form class="filter-bar" @submit.prevent="runFresh">
      <label class="filter-item">
        <span>涉及指标</span>
        <input v-model="filter.metrics" data-testid="journal-metrics" placeholder="指标A,指标B" />
      </label>
      <label class="filter-item">
        <span>开始时间</span>
        <input v-model="filter.startTime" data-testid="journal-start" placeholder="2026-09-01 00:00:00" />
      </label>
      <label class="filter-item">
        <span>结束时间</span>
        <input v-model="filter.endTime" data-testid="journal-end" placeholder="2026-09-30 23:59:59" />
      </label>
      <label class="filter-item">
        <span>状态</span>
        <select v-model="filter.status" data-testid="journal-status">
          <option value="">全部</option>
          <option value="正常">正常</option>
          <option value="慢查询">慢查询</option>
          <option value="异常终止">异常终止</option>
        </select>
      </label>
      <button class="btn primary" data-testid="journal-query" type="submit">查询统计</button>
      <button class="btn ghost" data-testid="journal-refresh" type="button" @click="runFresh">
        重新检索
      </button>
      <button class="btn ghost" data-testid="journal-verify" type="button" @click="verify" :disabled="!stats">
        校验明细复算
      </button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>
    <p v-if="notice" class="journal-notice" data-testid="journal-notice">{{ notice }}</p>

    <div v-if="stats" class="stat-row">
      <article class="stat-card">
        <span class="stat-label">分组数量</span>
        <strong class="stat-value" data-testid="journal-group-count">{{ stats.summary.group_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">查询总数</span>
        <strong class="stat-value" data-testid="journal-total">{{ stats.summary.total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">慢查询数</span>
        <strong class="stat-value" data-testid="journal-slow">{{ stats.summary.slow_query_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">平均耗时(ms)</span>
        <strong class="stat-value" data-testid="journal-avg">{{ stats.summary.average_duration_ms }}</strong>
      </article>
    </div>

    <p v-if="stats" class="journal-meta" data-testid="journal-consistency">
      快照明细 {{ stats.snapshot.execution_count }} 笔 · 明细批次
      {{ stats.detail_reused ? `复用 ${stats.reuse_count} 次` : '本次新建' }} ·
      快照后新入账 {{ stats.consistency.new_executions_since_snapshot }} 笔
      <template v-if="stats.consistency.stale">（当前汇总仍绑定这批已确认执行）</template>
    </p>

    <table v-if="stats" class="data-table journal-table">
      <thead>
        <tr>
          <th>执行序号</th>
          <th>执行编号</th>
          <th>查询时间</th>
          <th>涉及指标</th>
          <th>操作人</th>
          <th>执行来源</th>
          <th>入账类型</th>
          <th>耗时(ms)</th>
          <th>状态</th>
          <th>计入本页汇总</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in stats.items"
          :key="item.execution_id"
          :data-testid="`journal-row-${item.execution_id}`"
        >
          <td>#{{ item.sequence }}</td>
          <td>{{ item.execution_id }}</td>
          <td>{{ item['查询时间'] }}</td>
          <td>{{ item['涉及指标'] }}</td>
          <td>{{ item['操作人'] }}</td>
          <td>
            <span class="badge" :class="item.execution_source === 'cache' ? 'badge-cache' : 'badge-computed'">
              {{ item.execution_source_label }}
            </span>
          </td>
          <td>
            <span class="badge" :class="item.is_retry ? 'badge-retry' : 'badge-new'">
              {{ item.entry_kind }}
            </span>
            <small v-if="item.is_retry" class="retry-note">第 {{ item.attempt }} 次 · 接 {{ item.retry_of }}</small>
          </td>
          <td>{{ item.duration_ms }}</td>
          <td>{{ item.status }}</td>
          <td>
            <span>{{ item.counted_in_summary ? '是' : '否' }}</span>
            <button
              class="link locate-link"
              type="button"
              :data-testid="`journal-locate-${item.execution_id}`"
              @click="locate(item.execution_id)"
            >
              定位
            </button>
          </td>
        </tr>
        <tr v-if="!stats.items.length">
          <td colspan="10" class="empty-state">当前过滤条件下没有已确认执行</td>
        </tr>
      </tbody>
    </table>

    <p v-if="located" class="journal-located" data-testid="journal-located">
      执行 {{ located.execution.execution_id }}（序号 #{{ located.execution.sequence }}，
      {{ located.execution.execution_source_label }} / {{ located.execution.entry_kind }}）
      <template v-if="located.latest_snapshot">
        位于批次 {{ located.latest_snapshot.token }} 第 {{ located.latest_snapshot.position }} /
        {{ located.latest_snapshot.execution_count }} 笔，计入平均值
        {{ located.latest_snapshot.average_duration_ms }}ms；
        偏差 {{ located.latest_snapshot.deviation_from_average_ms }}ms，
        占当前总耗时 {{ ((located.latest_snapshot.share_of_total_duration ?? 0) * 100).toFixed(1) }}%。
      </template>
      <template v-else>尚未计入任何已冻结的统计批次，重新检索后会进入汇总。</template>
    </p>

    <footer v-if="stats" class="page-foot">
      <span>第 {{ stats.page }} / {{ Math.max(1, stats.page_count) }} 页，共 {{ stats.total }} 笔明细</span>
      <span class="journal-pager">
        <button class="btn" type="button" :disabled="stats.page <= 1" @click="turnPage(-1)">上一页</button>
        <button
          class="btn"
          type="button"
          :disabled="stats.page >= Math.max(1, stats.page_count)"
          @click="turnPage(1)"
        >
          下一页
        </button>
      </span>
    </footer>

    <details v-if="stats && stats.summary.groups.length" class="journal-groups">
      <summary>按指标分组（{{ stats.summary.group_count }} 组，可由明细复算）</summary>
      <table class="data-table">
        <thead>
          <tr><th>指标</th><th>数量</th><th>慢查询</th><th>平均耗时(ms)</th><th>缓存命中</th><th>重试</th></tr>
        </thead>
        <tbody>
          <tr v-for="group in stats.summary.groups" :key="group.metric">
            <td>{{ group.metric }}</td>
            <td>{{ group.count }}</td>
            <td>{{ group.slow }}</td>
            <td>{{ group.avg_duration_ms }}</td>
            <td>{{ group.cache_hits }}</td>
            <td>{{ group.retries }}</td>
          </tr>
        </tbody>
      </table>
    </details>
  </section>
</template>

<script setup lang="ts">
import { reactive, ref } from 'vue'

import {
  loadQueryStatistics,
  locateExecution,
  verifyQuerySnapshot,
  type QueryExecutionLocation,
  type QueryJournalFilter,
  type QueryJournalStatistics,
} from '@/api/query-journal'

const PAGE_SIZE = 10

const filter = reactive<QueryJournalFilter>({ metrics: '', startTime: '', endTime: '', status: '' })
const stats = ref<QueryJournalStatistics | null>(null)
const located = ref<QueryExecutionLocation | null>(null)
const errorMessage = ref('')
const notice = ref('')

async function fetchPage(page: number, pageToken: string | null, refresh: boolean) {
  errorMessage.value = ''
  try {
    stats.value = await loadQueryStatistics(filter, {
      page,
      size: PAGE_SIZE,
      pageToken,
      refresh,
    })
    notice.value = pageToken && !refresh
      ? `翻页沿用批次 ${stats.value.snapshot.token}，汇总未受新入账记录影响`
      : `已重新检索，绑定批次 ${stats.value.snapshot.token}`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '查询统计读取失败'
  }
}

async function runFresh() {
  await fetchPage(1, null, false)
}

async function turnPage(delta: number) {
  if (!stats.value) return
  const target = stats.value.page + delta
  if (target < 1) return
  await fetchPage(target, stats.value.next_page_token, false)
}

async function verify() {
  if (!stats.value) return
  errorMessage.value = ''
  try {
    const result = await verifyQuerySnapshot(stats.value.snapshot.token)
    notice.value = result.matches_detail
      ? `明细复算一致：${result.detail_count} 笔执行，平均耗时 ${result.summary.average_duration_ms}ms`
      : '明细复算与当前汇总不一致，请重新检索'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '统计快照校验失败'
  }
}

async function locate(executionId: string) {
  errorMessage.value = ''
  try {
    located.value = await locateExecution(executionId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '查询执行定位失败'
  }
}
</script>
