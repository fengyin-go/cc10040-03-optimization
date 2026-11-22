"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
同时提供跨模块查询和数据流处理能力。
"""
from __future__ import annotations

import copy
import time
import threading
from datetime import datetime
from typing import Any

from app.seed import SEED_ROWS


class Store:
    """进程内数据仓库。

    查询日志同时承担两件事：
      1. ``query_journal`` 列表（页面上的原始日志）；
      2. 查询统计明细的来源（``_query_log`` 与页面列表是同一份数据）。
    因此查询构建器写出的每一次执行都能在查询日志页面看到，统计聚合也只认这一份明细。
    """

    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._id_counters: dict[str, int] = {}
        # Cache layer storage
        self._cache: dict[str, tuple[float, Any]] = {}
        # 查询执行明细：与 query_journal 列表共享同一份对象，避免两处数据漂移。
        self._query_log: list[dict[str, Any]] = self._tables.setdefault("query_journal", [])
        self._query_seq = 0
        self._query_row_seq = 0
        self._query_execution_index: dict[str, dict[str, Any]] = {}
        # 追加明细时递增；改变已有明细（如标记慢查询）时还需要递增 rebuild token。
        self._query_log_version = 0
        self._query_log_rebuild_token = 0
        # 按过滤条件保存“已处理过的统计明细”，重复取数时直接复用。
        self._query_filter_states: dict[str, dict[str, Any]] = {}
        # 冻结快照：翻页和汇总必须落在同一批已确认执行上。
        self._query_snapshots: dict[str, dict[str, Any]] = {}
        self._query_snapshot_seq = 0
        # 同一逻辑查询最近一次执行，用于“重试”归链。
        self._query_dedupe_index: dict[str, str] = {}
        # Cohort snapshots
        self._cohort_snapshots: dict[str, list[dict[str, Any]]] = {}
        # Anomaly records
        self._anomaly_records: list[dict[str, Any]] = []
        # Recalc history
        self._recalc_history: list[dict[str, Any]] = []
        # Lock for thread safety
        self._lock = threading.Lock()

        self._normalize_query_log_rows()

    @property
    def lock(self):
        return self._lock

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        rows = self.rows(module)
        if not rows:
            return 1
        return max(int(row.get("id", 0)) for row in rows) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ── Cross-module query helpers ──

    def find_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        """查找模块中 field 等于 value 的所有记录。"""
        return [row for row in self.rows(module) if row.get(field) == value]

    def find_by_status(self, module: str, status: str) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get("status") == status]

    # ── Query journal ──
    #
    # 一条查询日志 = 一笔“查询统计明细”。查询构建器实时计算、缓存命中、
    # 重试以及人工登记都写进同一份明细，统计接口只从这份明细聚合。

    RETRY_WINDOW_SECONDS = 30.0

    def _normalize_query_log_rows(self) -> None:
        """把历史/种子日志补全成可聚合、可定位的执行明细。"""
        rows = self.rows("query_journal")
        max_id = max((int(row.get("id", 0)) for row in rows), default=0)
        self._query_row_seq = max_id
        committed = 0
        for index, row in enumerate(rows, start=1):
            sequence = int(row.get("sequence") or index)
            self._query_seq = max(self._query_seq, sequence)
            row.setdefault("sequence", sequence)
            row.setdefault("execution_id", f"QE-{sequence:06d}")
            row.setdefault("id", max_id + index)
            row.setdefault("查询时间", time.strftime("%Y-%m-%d %H:%M:%S"))
            text = str(row.get("查询时间") or "")
            if ":" not in text:
                text = f"{text} 00:00:00"
            row["查询时间"] = text
            if row.get("recorded_at") is None:
                row["recorded_at"] = self._parse_epoch(text)
            if row.get("duration_ms") is None:
                status = row.get("status")
                row["duration_ms"] = 320.0 if status == "慢查询" else (
                    800.0 if status == "异常终止" else 45.0)
            row["duration_ms"] = float(row.get("duration_ms") or 0.0)
            source = row.get("execution_source")
            if source not in ("computed", "cache"):
                summary = str(row.get("查询语句摘要", ""))
                source = "cache" if "缓存命中" in summary else "computed"
            row["execution_source"] = source
            row.setdefault("execution_source_label",
                           "缓存命中" if source == "cache" else "实时计算")
            row.setdefault("entry_kind", "新入账")
            row.setdefault("is_retry", False)
            row.setdefault("retry_of", None)
            row.setdefault("retry_group", None)
            row.setdefault("attempt", 1)
            row.setdefault("confirmed", True)
            if row["confirmed"]:
                committed += 1
                row.setdefault("commit_version", committed)
            else:
                row.setdefault("commit_version", None)
            row["pending"] = row.get("status") != "正常"
            row["abnormal"] = row.get("status") in ("慢查询", "异常终止")
            self._query_execution_index[str(row["execution_id"])] = row
        self._query_log_version = committed

    @staticmethod
    def _parse_epoch(text: str) -> float:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).timestamp()
            except ValueError:
                continue
        return time.time()

    @property
    def query_log_version(self) -> int:
        """已确认查询明细的提交版本；每追加/确认一笔就前进一次。"""
        return self._query_log_version

    @property
    def query_log_rebuild_token(self) -> int:
        """明细内容被改写（人工标记慢查询等）时前进，用于识别需要重建的统计。"""
        return self._query_log_rebuild_token

    def record_query_execution(
        self,
        *,
        metric: str,
        operator: str,
        sql_summary: str,
        row_count: int,
        duration_ms: float,
        status: str = "正常",
        execution_source: str = "computed",
        metric_id: int | None = None,
        request_key: str | None = None,
        retry_of: str | None = None,
        recorded_at: datetime | None = None,
        confirmed: bool = True,
    ) -> dict[str, Any]:
        """写一笔查询执行明细，并返回它。

        ``execution_source`` 区分实时计算与缓存命中；``request_key`` 相同的连续
        执行会被归为“重试”，否则就是“新入账”。同一秒内的多笔执行依靠
        ``sequence`` 和微秒级 ``查询时间`` 区分，便于定位谁进了当前平均值。
        """
        with self._lock:
            moment = recorded_at or datetime.now()
            text = moment.strftime("%Y-%m-%d %H:%M:%S.%f")
            epoch = moment.timestamp()
            self._query_seq += 1
            sequence = self._query_seq
            if confirmed:
                self._query_log_version += 1
            commit_version = self._query_log_version if confirmed else None

            source = execution_source if execution_source in ("computed", "cache") else "computed"
            dedupe_key = request_key or "|".join(
                (metric, operator, sql_summary, source))
            prior = self._query_execution_index.get(self._query_dedupe_index.get(dedupe_key, ""))

            is_retry = False
            if retry_of is None and prior is not None and confirmed:
                # 只有“上一次同一请求失败后重新执行”才算重试；
                # 失败后的下一次执行可能是缓存命中的一次重试，也可能是新入账。
                if (
                    prior.get("status") == "异常终止"
                    and epoch - float(prior.get("recorded_at", 0)) <= self.RETRY_WINDOW_SECONDS
                ):
                    retry_of = str(prior.get("execution_id"))
            if retry_of is not None:
                is_retry = True
                prior = prior or self._query_execution_index.get(str(retry_of))
                retry_group = str((prior or {}).get("retry_group") or retry_of)
                attempt = int((prior or {}).get("attempt") or 1) + 1
            else:
                retry_group = None
                attempt = 1

            row = self._query_row_seq + 1
            self._query_row_seq = row
            entry: dict[str, Any] = {
                "id": row,
                "sequence": sequence,
                "execution_id": f"QE-{sequence:06d}",
                "查询时间": text,
                "recorded_at": epoch,
                "操作人": operator,
                "涉及指标": metric,
                "metric_key": metric,
                "metric_id": metric_id,
                "查询语句摘要": sql_summary,
                "返回行数": str(row_count),
                "status": status,
                "duration_ms": round(float(duration_ms), 3),
                "execution_source": source,
                "execution_source_label": "缓存命中" if source == "cache" else "实时计算",
                "entry_kind": "重试" if is_retry else "新入账",
                "is_retry": is_retry,
                "retry_of": retry_of if is_retry else None,
                "retry_group": retry_group,
                "attempt": attempt,
                "request_key": dedupe_key,
                "confirmed": confirmed,
                "commit_version": commit_version,
                "pending": status != "正常",
                "abnormal": status in ("慢查询", "异常终止"),
            }
            self._query_log.append(entry)
            self._query_execution_index[str(entry["execution_id"])] = entry
            if confirmed:
                self._query_dedupe_index[dedupe_key] = str(entry["execution_id"])
            return entry

    def confirm_query_execution(self, execution_id: str) -> dict[str, Any] | None:
        """把一笔已写入但尚未确认的执行纳入统计口径。"""
        entry = self._query_execution_index.get(str(execution_id))
        if entry is None:
            return None
        if entry.get("confirmed"):
            return entry
        with self._lock:
            self._query_log_version += 1
            entry["confirmed"] = True
            entry["commit_version"] = self._query_log_version
        return entry

    def invalidate_query_statistics(self) -> int:
        """已有明细被改写时调用，让复用中的统计明细失效重建。"""
        self._query_log_rebuild_token += 1
        return self._query_log_rebuild_token

    def log_query(self, metric_id: int, operator: str, sql_summary: str,
                  row_count: int, duration_ms: float, status: str = "正常",
                  *, metric_name: str | None = None,
                  execution_source: str = "computed",
                  request_key: str | None = None) -> dict[str, Any]:
        """兼容旧调用：写一笔查询执行明细。"""
        return self.record_query_execution(
            metric=metric_name or f"metric_{metric_id}",
            operator=operator,
            sql_summary=sql_summary,
            row_count=row_count,
            duration_ms=duration_ms,
            status=status,
            execution_source=execution_source,
            metric_id=metric_id,
            request_key=request_key,
        )

    def query_logs(self) -> list[dict[str, Any]]:
        return self._query_log

    def query_execution(self, execution_id: str) -> dict[str, Any] | None:
        return self._query_execution_index.get(str(execution_id))

    # 查询统计明细批次与冻结快照由查询日志服务维护，仓库只负责存。

    def get_filter_state(self, key: str) -> dict[str, Any] | None:
        return self._query_filter_states.get(key)

    def save_filter_state(self, key: str, state: dict[str, Any]) -> None:
        self._query_filter_states[key] = state

    def save_query_snapshot(self, token: str, snapshot: dict[str, Any]) -> None:
        self._query_snapshots[token] = snapshot
        # 同一过滤条件只保留最近 20 个快照，避免长期运行后无限增长。
        siblings = [item for item in self._query_snapshots.values()
                    if item.get("filter_key") == snapshot.get("filter_key")]
        if len(siblings) > 20:
            siblings.sort(key=lambda item: item.get("sequence", 0))
            for stale in siblings[:-20]:
                self._query_snapshots.pop(str(stale.get("token")), None)

    def get_query_snapshot(self, token: str) -> dict[str, Any] | None:
        return self._query_snapshots.get(str(token))

    def query_snapshots(self) -> list[dict[str, Any]]:
        return list(self._query_snapshots.values())

    def latest_query_snapshot(self, filter_key: str) -> dict[str, Any] | None:
        candidates = [item for item in self._query_snapshots.values()
                      if item.get("filter_key") == filter_key]
        return max(candidates, key=lambda item: item.get("sequence", 0), default=None)

    def next_query_snapshot_sequence(self) -> int:
        self._query_snapshot_seq += 1
        return self._query_snapshot_seq

    # ── Cache operations ──

    def cache_get(self, key: str) -> Any | None:
        """获取缓存，过期返回 None。"""
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.time() > expires_at:
            del self._cache[key]
            return None
        return value

    def cache_set(self, key: str, value: Any, ttl: int) -> None:
        """设置缓存，ttl 单位为秒。"""
        self._cache[key] = (time.time() + ttl, value)

    def cache_invalidate(self, key: str) -> bool:
        return self._cache.pop(key, None) is not None

    def cache_invalidate_by_prefix(self, prefix: str) -> int:
        count = 0
        for key in list(self._cache):
            if key.startswith(prefix):
                del self._cache[key]
                count += 1
        return count

    # ── Cohort operations ──

    def save_cohort_snapshot(self, cohort_id: int, members: list[dict[str, Any]]) -> None:
        self._cohort_snapshots[str(cohort_id)] = members

    def get_cohort_snapshot(self, cohort_id: int) -> list[dict[str, Any]] | None:
        return self._cohort_snapshots.get(str(cohort_id))

    # ── Anomaly detection operations ──

    def record_anomaly(self, metric_id: int, severity: str, description: str,
                       value: float, threshold: float) -> dict[str, Any]:
        record = {
            "id": len(self._anomaly_records) + 1,
            "指标ID": metric_id,
            "严重程度": severity,
            "描述": description,
            "当前值": value,
            "阈值": threshold,
            "时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._anomaly_records.append(record)
        return record

    def anomaly_records(self) -> list[dict[str, Any]]:
        return self._anomaly_records

    # ── Recalc operations ──

    def record_recalc(self, metric_id: int, old_version: str, new_version: str,
                      time_range: str, diff_summary: str) -> dict[str, Any]:
        record = {
            "id": len(self._recalc_history) + 1,
            "关联指标": f"metric_{metric_id}",
            "旧口径": old_version,
            "新口径": new_version,
            "历史时间范围": time_range,
            "产出差异": diff_summary,
            "重算时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._recalc_history.append(record)
        return record

    def recalc_history(self) -> list[dict[str, Any]]:
        return self._recalc_history

    # ── Data artifacts for cross-module consumption ──

    def get_metric_data(self, metric_id: int) -> dict[str, Any] | None:
        """Get metric data with its calibers, queries, and services."""
        metric = self.find("metric_definition", metric_id)
        if metric is None:
            return None
        return {
            "metric": metric,
            "calibers": self.find_by_field("caliber_version", "所属指标",
                                           metric.get("指标名称", "")),
            "queries": self.find_by_field("query_builder", "关联指标",
                                          metric.get("指标名称", "")),
            "services": [row for row in self.rows("data_service")
                         if metric.get("指标名称", "") in str(row.get("关联指标或模型", ""))],
            "subscriptions": self.find_by_field("data_subscription", "关联指标",
                                                 metric.get("指标名称", "")),
        }

    def get_dimension_model_data(self, model_id: int) -> dict[str, Any] | None:
        """Get dimension model with its drill-down paths and pivot configs."""
        model = self.find("dimension_model", model_id)
        if model is None:
            return None
        return {
            "model": model,
            "drill_paths": self.find_by_field("drill_down", "起始指标",
                                              model.get("模型名称", "")),
            "pivot_configs": self.find_by_field("pivot_table", "数据源指标",
                                                 model.get("模型名称", "")),
        }

    def get_cohort_analysis(self, cohort_id: int) -> dict[str, Any] | None:
        """Get cohort with experiments and segment comparisons."""
        cohort = self.find("user_cohort", cohort_id)
        if cohort is None:
            return None
        cohort_name = cohort.get("分群名称", "")
        return {
            "cohort": cohort,
            "experiments": [row for row in self.rows("experiment_group")
                            if cohort_name in str(row.get("筛选条件", ""))],
            "comparisons": [row for row in self.rows("segment_compare")
                            if cohort_name in (str(row.get("分群A", "")) + str(row.get("分群B", "")))],
            "anomaly_rules": [row for row in self.rows("anomaly_detector")
                              if cohort_name in str(row.get("目标指标", ""))],
        }

    def get_field_lineage(self, field_name: str) -> dict[str, Any]:
        """Trace a field's complete lineage chain."""
        entries = self.find_by_field("field_lineage", "目标字段", field_name)
        if not entries:
            return {"field": field_name, "lineage": [], "depth": 0}

        lineage = [{
            "source_fields": entry.get("来源字段列表", ""),
            "transform": entry.get("转换逻辑", ""),
            "intermediate_table": entry.get("中间表", ""),
            "depth": entry.get("血缘深度", ""),
            "status": entry.get("status", ""),
        } for entry in entries]

        return {
            "field": field_name,
            "lineage": lineage,
            "depth": len(lineage),
        }


store = Store()