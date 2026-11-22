"""查询日志领域服务：记录和审计所有分析查询的调用链路和性能。

跨模块数据流: query_builder/data_service -> query_journal（记录查询）
              cache_layer -> query_journal（区分缓存与实时查询）

这里的核心不是每次把全量日志重算一遍，而是：

* **统计明细**：一次过滤条件对应一批已确认执行（``execution_id`` 有序）；
* **过滤态复用**：明细的提交版本没变时重复取数直接复用已处理的明细；
* **冻结快照**：同一组聚合和分页绑定同一批明细，翻页期间新写入的日志不会
  偷偷改变当前页、总量、慢查询数和平均值；
* **可复算**：汇总只由明细算出来，明细里带着每笔执行的序号、来源和重试关系，
  分析员可以定位同一秒内到底哪一笔计进了当前平均值。
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from typing import Any, Iterable

from app.store import store
from app.services.base import BaseModuleService

TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
)


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("T", " ")
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _split_metrics(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts: Iterable[str] = (str(item) for item in value)
    else:
        parts = str(value).replace("，", ",").split(",")
    return sorted({part.strip() for part in parts if str(part).strip()})


class QueryJournalService(BaseModuleService):
    MODULE = "query_journal"
    ENTITY = "查询日志"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["查询时间", "操作人", "涉及指标", "查询语句摘要", "返回行数"]
    REQUIRED_FIELDS = ["查询时间", "操作人", "涉及指标"]
    STATUS_ORDER = ["正常", "慢查询", "异常终止"]
    ACTION_RULES = {"标记慢查询": "慢查询", "忽略告警": "正常"}
    NEGATIVE_ACTIONS = ["忽略告警"]
    SLOW_THRESHOLD_MS = 100.0

    # ── 明细与过滤 ──

    @staticmethod
    def _filter_spec(metrics: Any = None, start_time: str | None = None,
                     end_time: str | None = None,
                     status: str | None = None) -> dict[str, Any]:
        start = _parse_time(start_time)
        end = _parse_time(end_time)
        # 只给日期时，结束时间覆盖当天。
        if end is not None and end.hour == end.minute == end.second == 0 and end.microsecond == 0:
            end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        spec = {
            "metrics": _split_metrics(metrics),
            "start_time": start.strftime("%Y-%m-%d %H:%M:%S") if start else None,
            "end_time": end.strftime("%Y-%m-%d %H:%M:%S") if end else None,
            "status": (status or "").strip() or None,
        }
        spec["filter_key"] = QueryJournalService._filter_key(spec)
        return spec

    @staticmethod
    def _filter_key(spec: dict[str, Any]) -> str:
        canonical = json.dumps(
            {key: spec.get(key) for key in ("metrics", "start_time", "end_time", "status")},
            ensure_ascii=False, sort_keys=True,
        )
        return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _matches(row: dict[str, Any], spec: dict[str, Any]) -> bool:
        if spec.get("metrics") and str(row.get("涉及指标", "")) not in spec["metrics"]:
            return False
        if spec.get("status") and row.get("status") != spec["status"]:
            return False
        recorded_at = row.get("recorded_at")
        moment = (datetime.fromtimestamp(float(recorded_at))
                  if recorded_at is not None else _parse_time(str(row.get("查询时间", ""))))
        if moment is None:
            return False
        start = _parse_time(spec.get("start_time"))
        end = _parse_time(spec.get("end_time"))
        if start is not None and moment < start:
            return False
        if end is not None and moment > end:
            return False
        return True

    @staticmethod
    def _detail(row: dict[str, Any]) -> dict[str, Any]:
        """把一条日志收敛成稳定、可复算的统计明细。"""
        return {
            "id": row.get("id"),
            "execution_id": row.get("execution_id"),
            "sequence": int(row.get("sequence") or 0),
            "查询时间": row.get("查询时间"),
            "操作人": row.get("操作人", ""),
            "涉及指标": row.get("涉及指标", ""),
            "查询语句摘要": row.get("查询语句摘要", ""),
            "返回行数": row.get("返回行数", "0"),
            "status": row.get("status", "正常"),
            "duration_ms": round(float(row.get("duration_ms") or 0.0), 3),
            "execution_source": row.get("execution_source", "computed"),
            "execution_source_label": row.get("execution_source_label", "实时计算"),
            "entry_kind": row.get("entry_kind", "新入账"),
            "is_retry": bool(row.get("is_retry")),
            "retry_of": row.get("retry_of"),
            "retry_group": row.get("retry_group"),
            "attempt": int(row.get("attempt") or 1),
            "commit_version": row.get("commit_version"),
        }

    def build_statistics_detail(
        self,
        *,
        metrics: Any = None,
        start_time: str | None = None,
        end_time: str | None = None,
        status: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """生成（或复用）所选指标与时间段的查询统计明细批次。

        同一过滤条件在明细提交版本与重建令牌都没变化时直接复用，避免分析员
        反复筛选、翻页时重扫全量日志。
        """
        spec = self._filter_spec(metrics, start_time, end_time, status)
        version = store.query_log_version
        rebuild_token = store.query_log_rebuild_token
        cached = store.get_filter_state(spec["filter_key"])
        if (
            not force
            and cached is not None
            and cached.get("detail_version") == version
            and cached.get("rebuild_token") == rebuild_token
        ):
            batch = copy.deepcopy(cached)
            batch["reused"] = True
            batch["reuse_count"] = int(cached.get("reuse_count", 0)) + 1
            store.save_filter_state(spec["filter_key"], {
                **cached,
                "reuse_count": batch["reuse_count"],
            })
            return batch

        rows = [
            row for row in store.query_logs()
            if row.get("confirmed", True) and self._matches(row, spec)
        ]
        rows.sort(key=lambda row: (int(row.get("sequence") or 0), int(row.get("id") or 0)))
        details = [self._detail(row) for row in rows]
        batch = {
            **spec,
            "detail_version": version,
            "rebuild_token": rebuild_token,
            "detail_count": len(details),
            "details": details,
            "execution_ids": [item["execution_id"] for item in details],
            "built_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "reused": False,
            "reuse_count": int((cached or {}).get("reuse_count", 0)),
        }
        store.save_filter_state(spec["filter_key"], copy.deepcopy(batch))
        return batch

    @staticmethod
    def summarize(details: list[dict[str, Any]],
                  slow_threshold_ms: float = SLOW_THRESHOLD_MS) -> dict[str, Any]:
        """只依赖明细复算汇总：分组数量、慢查询数、平均耗时。"""
        groups: dict[str, dict[str, Any]] = {}
        total_duration = 0.0
        slow = 0
        cache_hits = 0
        retries = 0
        for item in details:
            duration = float(item.get("duration_ms") or 0.0)
            total_duration += duration
            metric = str(item.get("涉及指标") or "未知")
            group = groups.setdefault(metric, {
                "metric": metric,
                "count": 0,
                "slow": 0,
                "total_duration_ms": 0.0,
                "cache_hits": 0,
                "retries": 0,
                "execution_ids": [],
            })
            group["count"] += 1
            group["total_duration_ms"] += duration
            group["execution_ids"].append(item.get("execution_id"))
            is_slow = item.get("status") == "慢查询" or duration > slow_threshold_ms
            if is_slow:
                slow += 1
                group["slow"] += 1
            if item.get("execution_source") == "cache":
                cache_hits += 1
                group["cache_hits"] += 1
            if item.get("is_retry"):
                retries += 1
                group["retries"] += 1

        total = len(details)
        group_list = []
        for group in groups.values():
            group["total_duration_ms"] = round(group["total_duration_ms"], 2)
            group["avg_duration_ms"] = round(
                group["total_duration_ms"] / max(1, group["count"]), 2)
            group_list.append(group)
        group_list.sort(key=lambda item: item["metric"])

        return {
            "group_count": len(group_list),
            "total": total,
            "slow_query_count": slow,
            "average_duration_ms": round(total_duration / max(1, total), 2),
            "total_duration_ms": round(total_duration, 2),
            "cache_hit_count": cache_hits,
            "computed_count": total - cache_hits,
            "retry_count": retries,
            "groups": group_list,
            "execution_ids": [item.get("execution_id") for item in details],
            "recomputable": True,
        }

    # ── 冻结快照分页 ──

    def _create_snapshot(self, batch: dict[str, Any]) -> dict[str, Any]:
        sequence = store.next_query_snapshot_sequence()
        snapshot = {
            "token": f"QJ-{sequence:06d}",
            "sequence": sequence,
            "filter_key": batch["filter_key"],
            "detail_version": batch["detail_version"],
            "rebuild_token": batch.get("rebuild_token"),
            "details": copy.deepcopy(batch["details"]),
            "execution_ids": list(batch["execution_ids"]),
            # 快照覆盖到的最后一笔执行序号；之后写入的才算“新入账”。
            "last_execution_sequence": max(
                (int(item.get("sequence") or 0) for item in batch["details"]),
                default=0,
            ),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        }
        store.save_query_snapshot(snapshot["token"], snapshot)
        return snapshot

    def query_statistics(
        self,
        *,
        metrics: Any = None,
        start_time: str | None = None,
        end_time: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        page_token: str | None = None,
        refresh: bool = False,
        include_detail: bool = False,
        slow_threshold_ms: float = SLOW_THRESHOLD_MS,
    ) -> dict[str, Any]:
        """按指标和时间取查询统计，并对明细分页。

        ``page_token`` 存在时沿用该快照：翻页期间新入账的执行不会改变总量、
        汇总和已翻过的页。不带 token（或 ``refresh=true``）时按最新已确认
        明细重新取数，因此重新检索可以看到新记录。
        """
        page = max(1, int(page or 1))
        size = max(1, min(int(size or 20), 500))
        spec = self._filter_spec(metrics, start_time, end_time, status)

        snapshot = None
        if page_token and not refresh:
            candidate = store.get_query_snapshot(page_token)
            if candidate is not None and candidate.get("filter_key") == spec["filter_key"]:
                snapshot = candidate

        batch: dict[str, Any] | None = None
        if snapshot is None:
            batch = self.build_statistics_detail(
                metrics=metrics, start_time=start_time, end_time=end_time,
                status=status, force=refresh,
            )
            snapshot = self._create_snapshot(batch)
            detail_reused = bool(batch.get("reused"))
            reuse_count = int(batch.get("reuse_count", 0))
        else:
            # 翻页直接使用冻结快照，不再重扫明细；过滤态只用于报告复用次数。
            state = store.get_filter_state(spec["filter_key"])
            detail_reused = True
            reuse_count = int((state or {}).get("reuse_count", 0))

        details: list[dict[str, Any]] = snapshot["details"]
        summary = self.summarize(details, slow_threshold_ms)
        total = len(details)
        start_index = (page - 1) * size
        page_items = [copy.deepcopy(item) for item in details[start_index:start_index + size]]

        # 标记当前页里每笔执行是否计入了这份汇总。
        execution_id_set = set(summary["execution_ids"])
        for item in page_items:
            item["counted_in_summary"] = item.get("execution_id") in execution_id_set

        last_sequence = int(snapshot.get("last_execution_sequence") or 0)
        newer = sum(
            1 for row in store.query_logs()
            if row.get("confirmed", True)
            and int(row.get("sequence") or 0) > last_sequence
            and self._matches(row, spec)
        )
        effective_version = store.query_log_version
        rebuild_token = store.query_log_rebuild_token

        result: dict[str, Any] = {
            "items": page_items,
            "total": total,
            "page": page,
            "size": size,
            "page_count": (total + size - 1) // size if total else 0,
            "summary": summary,
            # 顶层再展开一份常用汇总，调用方无需关心嵌套结构。
            "group_count": summary["group_count"],
            "slow_query_count": summary["slow_query_count"],
            "avg_duration_ms": summary["average_duration_ms"],
            "average_duration_ms": summary["average_duration_ms"],
            "total_duration_ms": summary["total_duration_ms"],
            "groups": summary["groups"],
            "execution_ids": summary["execution_ids"],
            "filter": {
                "metrics": spec["metrics"],
                "start_time": spec["start_time"],
                "end_time": spec["end_time"],
                "status": spec["status"],
                "filter_key": spec["filter_key"],
            },
            "detail_version": snapshot["detail_version"],
            "snapshot": {
                "token": snapshot["token"],
                "sequence": snapshot["sequence"],
                "created_at": snapshot["created_at"],
                "detail_version": snapshot["detail_version"],
                "execution_count": total,
                "execution_ids": list(snapshot["execution_ids"]),
                "same_batch": True,
            },
            "next_page_token": snapshot["token"],
            "consistency": {
                "same_batch": True,
                "snapshot_total": total,
                "live_detail_version": effective_version,
                "new_executions_since_snapshot": newer,
                "detail_changed_since_snapshot": rebuild_token != snapshot.get("rebuild_token"),
                "stale": newer > 0 or rebuild_token != snapshot.get("rebuild_token"),
            },
            "detail_reused": detail_reused,
            "reuse_count": reuse_count,
            "summary_recomputed_from_detail": True,
        }
        if include_detail:
            result["detail"] = [copy.deepcopy(item) for item in details]
        return result
    def verify_snapshot(self, snapshot_token: str,
                        slow_threshold_ms: float = SLOW_THRESHOLD_MS) -> dict[str, Any] | None:
        """用快照里的明细复算汇总，验证当前返回的统计确实来自同一批执行。"""
        snapshot = store.get_query_snapshot(snapshot_token)
        if snapshot is None:
            return None
        summary = self.summarize(snapshot["details"], slow_threshold_ms)
        return {
            "snapshot_token": snapshot["token"],
            "detail_count": len(snapshot["details"]),
            "summary": summary,
            "matches_detail": summary["total"] == len(snapshot["details"]) and (
                summary["average_duration_ms"] == round(
                    sum(float(item.get("duration_ms") or 0.0)
                        for item in snapshot["details"]) / max(1, len(snapshot["details"])), 2)
            ),
        }

    def locate_execution(self, execution_id: str,
                         slow_threshold_ms: float = SLOW_THRESHOLD_MS) -> dict[str, Any] | None:
        """定位一笔执行：它属于哪些统计批次，是否计入了当前平均值，贡献多少。"""
        entry = store.query_execution(execution_id)
        if entry is None:
            return None
        detail = self._detail(entry)
        snapshots = []
        for snapshot in store.query_snapshots():
            ids = list(snapshot.get("execution_ids", []))
            if execution_id not in ids:
                continue
            summary = self.summarize(snapshot["details"], slow_threshold_ms)
            position = ids.index(execution_id)
            total_duration = float(summary["total_duration_ms"]) or 0.0
            snapshots.append({
                "token": snapshot.get("token"),
                "created_at": snapshot.get("created_at"),
                "detail_version": snapshot.get("detail_version"),
                "execution_count": len(ids),
                "position": position + 1,
                "counted_in_summary": True,
                "average_duration_ms": summary["average_duration_ms"],
                "share_of_total_duration": round(
                    float(detail["duration_ms"]) / total_duration, 4
                ) if total_duration else None,
                "deviation_from_average_ms": round(
                    float(detail["duration_ms"]) - float(summary["average_duration_ms"]), 2),
            })
        snapshots.sort(key=lambda item: item.get("created_at") or "")
        return {
            "execution": detail,
            "counted_in_current_average": bool(snapshots),
            "snapshots": snapshots,
            "latest_snapshot": snapshots[-1] if snapshots else None,
            "source_breakdown": {
                "execution_source": detail["execution_source"],
                "execution_source_label": detail["execution_source_label"],
                "entry_kind": detail["entry_kind"],
                "is_retry": detail["is_retry"],
                "retry_of": detail["retry_of"],
            },
        }

    # ── 兼容旧接口 ──

    def get_slow_queries(self, threshold_ms: float = 100) -> list[dict[str, Any]]:
        """获取慢查询列表。"""
        return [
            self._detail(row) for row in store.query_logs()
            if row.get("confirmed", True) and float(row.get("duration_ms") or 0) > threshold_ms
        ]

    def get_query_stats_by_metric(self) -> dict[str, Any]:
        """按指标统计查询情况。

        跨模块数据流: query_journal -> metric_definition（关联指标分析）
        """
        batch = self.build_statistics_detail()
        summary = self.summarize(batch["details"])
        return {
            group["metric"]: {
                "count": group["count"],
                "slow": group["slow"],
                "avg_duration_ms": group["avg_duration_ms"],
                "cache_hits": group["cache_hits"],
                "retries": group["retries"],
                "execution_ids": group["execution_ids"],
            }
            for group in summary["groups"]
        }

    def get_performance_insights(self) -> dict[str, Any]:
        """获取性能洞察：跨模块查询效率分析。"""
        logs = [row for row in store.query_logs() if row.get("confirmed", True)]
        if not logs:
            return {"message": "暂无查询日志"}

        summary = self.summarize([self._detail(row) for row in logs])
        total = summary["total"]
        slow = summary["slow_query_count"]
        cached = summary["cache_hit_count"]
        errors = sum(1 for log in logs if log.get("status") == "异常终止")

        return {
            "total_queries": total,
            "slow_query_ratio": round(slow / max(1, total), 2),
            "cache_hit_ratio": round(cached / max(1, total), 2),
            "error_count": errors,
            "avg_duration_ms": summary["average_duration_ms"],
            "recomputable": True,
            "recommendations": [
                f"{slow} 条慢查询建议优化口径或增加缓存" if slow > 0 else None,
                f"{cached} 条缓存命中，减少重复计算" if cached > 0 else "暂无缓存命中" if total > 0 else None,
            ]
        }

    # ── 写入与动作：任何明细变化都要让统计失效/前进 ──

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        entry = store.record_query_execution(
            metric=str(values.get("涉及指标") or "未知"),
            operator=str(values.get("操作人") or ""),
            sql_summary=str(values.get("查询语句摘要") or ""),
            row_count=int(str(values.get("返回行数") or 0).strip() or 0)
            if str(values.get("返回行数") or "0").strip().lstrip("-").isdigit() else 0,
            duration_ms=float(values.get("duration_ms") or 0.0),
            status=str(values.get("status") or "正常"),
            execution_source=str(values.get("execution_source") or "computed"),
            recorded_at=_parse_time(str(values.get("查询时间") or "")) or datetime.now(),
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        with store.lock:
            entry["status"] = target
            entry["pending"] = target != self.STATUS_ORDER[-1]
            entry["abnormal"] = target in ("慢查询", "异常终止")
            # 人工标记会改写已有明细，统计明细必须重建而不是继续复用。
            store.invalidate_query_statistics()
        return entry, f"{self.ENTITY}已{action}"
