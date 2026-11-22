import { request } from './client'

export type QueryJournalDetail = {
  execution_id: string
  sequence: number
  '查询时间': string
  '操作人': string
  '涉及指标': string
  '查询语句摘要': string
  '返回行数': string
  status: string
  duration_ms: number
  execution_source: 'computed' | 'cache'
  execution_source_label: string
  entry_kind: '新入账' | '重试'
  is_retry: boolean
  retry_of: string | null
  retry_group: string | null
  attempt: number
  commit_version: number | null
  counted_in_summary?: boolean
}

export type QueryJournalGroup = {
  metric: string
  count: number
  slow: number
  avg_duration_ms: number
  total_duration_ms: number
  cache_hits: number
  retries: number
  execution_ids: string[]
}

export type QueryJournalSummary = {
  group_count: number
  total: number
  slow_query_count: number
  average_duration_ms: number
  total_duration_ms: number
  cache_hit_count: number
  computed_count: number
  retry_count: number
  groups: QueryJournalGroup[]
  execution_ids: string[]
  recomputable: boolean
}

export type QueryJournalStatistics = {
  items: QueryJournalDetail[]
  total: number
  page: number
  size: number
  page_count: number
  summary: QueryJournalSummary
  filter: {
    metrics: string[]
    start_time: string | null
    end_time: string | null
    status: string | null
    filter_key: string
  }
  detail_version: number
  snapshot: {
    token: string
    sequence: number
    created_at: string
    detail_version: number
    execution_count: number
    execution_ids: string[]
    same_batch: boolean
  }
  next_page_token: string
  consistency: {
    same_batch: boolean
    snapshot_total: number
    live_detail_version: number
    new_executions_since_snapshot: number
    detail_changed_since_snapshot: boolean
    stale: boolean
  }
  detail_reused: boolean
  reuse_count: number
  summary_recomputed_from_detail: boolean
}

export type QueryJournalFilter = {
  metrics?: string
  startTime?: string
  endTime?: string
  status?: string
}

function buildQuery(
  filter: QueryJournalFilter,
  extras: Record<string, string | number | boolean | undefined> = {},
): string {
  const params = new URLSearchParams()
  if (filter.metrics?.trim()) params.set('metrics', filter.metrics.trim())
  if (filter.startTime?.trim()) params.set('start_time', filter.startTime.trim())
  if (filter.endTime?.trim()) params.set('end_time', filter.endTime.trim())
  if (filter.status?.trim()) params.set('status', filter.status.trim())
  for (const [key, value] of Object.entries(extras)) {
    if (value !== undefined && value !== '') params.set(key, String(value))
  }
  return params.toString()
}

export async function loadQueryStatistics(
  filter: QueryJournalFilter,
  options: {
    page?: number
    size?: number
    pageToken?: string | null
    refresh?: boolean
    includeDetail?: boolean
    slowThresholdMs?: number
  } = {},
): Promise<QueryJournalStatistics> {
  const query = buildQuery(filter, {
    page: options.page ?? 1,
    size: options.size ?? 10,
    page_token: options.pageToken ?? undefined,
    refresh: options.refresh ? 'true' : undefined,
    include_detail: options.includeDetail ? 'true' : undefined,
    slow_threshold_ms: options.slowThresholdMs,
  })
  const response = await request(`/api/cross/query-journal/statistics?${query}`)
  if (!response.ok) throw new Error('查询统计读取失败')
  return (await response.json()) as QueryJournalStatistics
}

export async function verifyQuerySnapshot(token: string): Promise<{
  snapshot_token: string
  detail_count: number
  summary: QueryJournalSummary
  matches_detail: boolean
}> {
  const response = await request(`/api/cross/query-journal/snapshots/${encodeURIComponent(token)}`)
  if (!response.ok) throw new Error('统计快照读取失败')
  return await response.json()
}

export async function loadDetailBatch(
  filter: QueryJournalFilter,
  rebuild = false,
): Promise<{
  detail_count: number
  detail_version: number
  reused: boolean
  reuse_count: number
  details: QueryJournalDetail[]
  summary: QueryJournalSummary
}> {
  const query = buildQuery(filter, { rebuild: rebuild ? 'true' : undefined })
  const response = await request(`/api/cross/query-journal/detail-batches?${query}`)
  if (!response.ok) throw new Error('查询统计明细读取失败')
  return await response.json()
}

export type QueryExecutionLocation = {
  execution: QueryJournalDetail
  counted_in_current_average: boolean
  source_breakdown: {
    execution_source: string
    execution_source_label: string
    entry_kind: string
    is_retry: boolean
    retry_of: string | null
  }
  latest_snapshot: {
    token: string
    created_at: string
    detail_version: number
    execution_count: number
    position: number
    counted_in_summary: boolean
    average_duration_ms: number
    share_of_total_duration: number | null
    deviation_from_average_ms: number
  } | null
  snapshots: {
    token: string
    created_at: string
    detail_version: number
    execution_count: number
    position: number
    counted_in_summary: boolean
    average_duration_ms: number
    share_of_total_duration: number | null
    deviation_from_average_ms: number
  }[]
}

export async function locateExecution(
  executionId: string,
  slowThresholdMs?: number,
): Promise<QueryExecutionLocation> {
  const query = slowThresholdMs != null ? `?slow_threshold_ms=${slowThresholdMs}` : ''
  const response = await request(
    `/api/cross/query-journal/executions/${encodeURIComponent(executionId)}${query}`,
  )
  if (!response.ok) throw new Error('查询执行定位失败')
  return (await response.json()) as QueryExecutionLocation
}
