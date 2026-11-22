"""查询日志统计明细与分页一致性测试。

覆盖：
* 查询构建器实时计算 / 缓存命中都生成可区分的执行明细；
* 按指标和时间过滤，用同一份明细计算分组数量、慢查询数、平均耗时；
* 同一秒内的多笔执行可以逐笔定位（execution_id / sequence / 计入汇总）；
* 同一聚合和分页始终绑定同一批已确认执行，翻页时新入账不改写当前汇总；
* 重试与新入账可区分；
* 重复调度同一过滤条件会复用已处理的统计明细；
* 重新检索可以看到新记录，并可用明细复算当前汇总。
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.services.query_builder import QueryBuilderService
from app.services.query_journal import QueryJournalService
from app.store import store


@pytest.fixture()
def journal_env():
    """准备一个已发布的查询模板和生效口径，返回 (模板ID, 指标名, 操作人)。"""
    journal = QueryJournalService()
    metric = f"统计明细测试指标{len(store.query_logs())}"
    template = {
        "id": store.next_id("query_builder"),
        "模板名称": f"统计明细测试模板{metric}",
        "关联指标": metric,
        "维度组合": "日",
        "过滤条件": "",
        "缓存策略": "短期",
        "status": "已发布",
        "pending": False,
        "abnormal": False,
    }
    store.rows("query_builder").append(template)
    store.rows("caliber_version").append({
        "id": store.next_id("caliber_version"),
        "所属指标": metric,
        "版本号": "v1.0",
        "口径描述": "测试口径",
        "变更类型": "初始创建",
        "生效时间": "2026-11-01",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })
    # 模板关联的指标本身没有指标定义行，仍然能生成空结果并写明细。
    return template["id"], metric, "分析员甲"


def _record(metric: str, operator: str, duration_ms: float,
            status: str = "正常", source: str = "computed",
            recorded_at: datetime | None = None, request_key: str | None = None,
            retry_of: str | None = None) -> dict:
    return store.record_query_execution(
        metric=metric,
        operator=operator,
        sql_summary=f"{source} {operator}",
        row_count=3,
        duration_ms=duration_ms,
        status=status,
        execution_source=source,
        recorded_at=recorded_at,
        request_key=request_key,
        retry_of=retry_of,
    )


class TestQueryExecutionDetail:
    def test_computed_and_cache_produce_distinct_details(self, journal_env):
        template_id, metric, operator = journal_env
        builder = QueryBuilderService()

        cold = builder.execute_query(template_id, operator=operator)
        warm = builder.execute_query(template_id, operator=operator)

        assert cold["source"] == "computed"
        assert warm["source"] == "cache"
        assert cold["execution"]["execution_source"] == "computed"
        assert warm["execution"]["execution_source"] == "cache"
        assert cold["execution"]["execution_id"] != warm["execution"]["execution_id"]
        assert cold["execution"]["sequence"] < warm["execution"]["sequence"]

        details = {
            item["execution_id"]: item
            for item in store.query_logs()
            if item.get("涉及指标") == metric
        }
        assert details[cold["execution"]["execution_id"]]["execution_source_label"] == "实时计算"
        assert details[warm["execution"]["execution_id"]]["execution_source_label"] == "缓存命中"

    def test_failed_execution_then_retry_is_linked(self, journal_env):
        _, metric, operator = journal_env
        failed = store.record_query_execution(
            metric=metric,
            operator=operator,
            sql_summary="口径校验失败",
            row_count=0,
            duration_ms=0,
            status="异常终止",
            request_key="template:retry-case",
        )
        retried = store.record_query_execution(
            metric=metric,
            operator=operator,
            sql_summary="重试计算",
            row_count=3,
            duration_ms=12.5,
            request_key="template:retry-case",
            retry_of=failed["execution_id"],
        )
        fresh = store.record_query_execution(
            metric=metric,
            operator=operator,
            sql_summary="新的查询",
            row_count=3,
            duration_ms=8.0,
            request_key="template:new-case",
        )

        assert retried["is_retry"] is True
        assert retried["entry_kind"] == "重试"
        assert retried["retry_of"] == failed["execution_id"]
        assert retried["attempt"] == 2
        assert retried["retry_group"] == failed["execution_id"]
        assert fresh["is_retry"] is False
        assert fresh["entry_kind"] == "新入账"

    def test_builder_auto_marks_retry_after_caliber_failure(self):
        """查询构建器先因无生效口径失败，补齐口径后再次执行应识别为重试。"""
        metric = f"重试识别指标{len(store.query_logs())}"
        template = {
            "id": store.next_id("query_builder"),
            "模板名称": f"重试识别模板{metric}",
            "关联指标": metric,
            "维度组合": "日",
            "过滤条件": "",
            "缓存策略": "",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
        }
        store.rows("query_builder").append(template)
        builder = QueryBuilderService()

        first = builder.execute_query(template["id"], operator="重试分析员")
        assert "error" in first
        failed_id = first["execution"]["execution_id"]
        assert first["execution"]["status"] == "异常终止"

        store.rows("caliber_version").append({
            "id": store.next_id("caliber_version"),
            "所属指标": metric,
            "版本号": "v1.0",
            "口径描述": "补齐口径",
            "变更类型": "初始创建",
            "生效时间": "2026-11-01",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        second = builder.execute_query(template["id"], operator="重试分析员")
        assert "error" not in second
        assert second["execution"]["is_retry"] is True
        assert second["execution"]["entry_kind"] == "重试"
        assert second["execution"]["retry_of"] == failed_id

        third = builder.execute_query(template["id"], operator="另一位分析员")
        assert third["execution"]["is_retry"] is False
        assert third["execution"]["entry_kind"] == "新入账"


class TestStatisticsDetail:
    def test_unconfirmed_execution_not_counted_until_confirmed(self, journal_env):
        _, metric, operator = journal_env
        pending = store.record_query_execution(
            metric=metric,
            operator=operator,
            sql_summary="尚未确认",
            row_count=1,
            duration_ms=900.0,
            status="慢查询",
            confirmed=False,
        )
        svc = QueryJournalService()
        before = svc.query_statistics(metrics=metric, page=1, size=10)
        assert before["total"] == 0
        assert before["summary"]["slow_query_count"] == 0
        assert before["detail_version"] == store.query_log_version

        store.confirm_query_execution(pending["execution_id"])
        after = svc.query_statistics(metrics=metric, page=1, size=10)
        assert after["total"] == 1
        assert after["summary"]["slow_query_count"] == 1
        assert after["snapshot"]["execution_ids"] == [pending["execution_id"]]
        # 确认后统计明细需要重建，而不是继续复用旧批次。
        assert svc.build_statistics_detail(metrics=metric)["reused"] is True

    def test_filter_by_metric_and_time(self, journal_env):
        _, metric, operator = journal_env
        base = datetime(2026, 10, 1, 9, 0, 0)
        _record(metric, operator, 10.0, recorded_at=base)
        _record(metric, operator, 20.0, recorded_at=base + timedelta(hours=1))
        _record("别的指标", operator, 30.0, recorded_at=base)

        svc = QueryJournalService()
        batch = svc.build_statistics_detail(
            metrics=metric,
            start_time="2026-10-01 08:00:00",
            end_time="2026-10-01 10:00:00",
        )
        assert batch["detail_count"] == 2
        assert all(item["涉及指标"] == metric for item in batch["details"])

        summary = svc.summarize(batch["details"])
        assert summary["group_count"] == 1
        assert summary["total"] == 2
        assert summary["average_duration_ms"] == 15.0
        assert summary["slow_query_count"] == 0

    def test_repeated_filtering_reuses_processed_detail(self, journal_env):
        _, metric, operator = journal_env
        _record(metric, operator, 5.0)
        svc = QueryJournalService()

        first = svc.build_statistics_detail(metrics=metric)
        second = svc.build_statistics_detail(metrics=metric)

        assert first["reused"] is False
        assert second["reused"] is True
        assert second["detail_version"] == first["detail_version"]
        assert second["execution_ids"] == first["execution_ids"]
        assert second["reuse_count"] == 1

    def test_new_execution_advances_detail_version(self, journal_env):
        _, metric, operator = journal_env
        _record(metric, operator, 5.0)
        svc = QueryJournalService()
        first = svc.build_statistics_detail(metrics=metric)

        _record(metric, operator, 50.0)
        second = svc.build_statistics_detail(metrics=metric)

        assert second["reused"] is False
        assert second["detail_version"] > first["detail_version"]
        assert second["detail_count"] == first["detail_count"] + 1


class TestSnapshotPagination:
    def test_cold_and_cache_hit_then_append_slow_query_then_page(self, journal_env):
        """连续查询有冷查询和缓存命中的区间；取总量后追加慢查询再翻页。"""
        template_id, metric, operator = journal_env
        builder = QueryBuilderService()
        cold = builder.execute_query(template_id, operator=operator)
        warm = builder.execute_query(template_id, operator=operator)
        assert {cold["source"], warm["source"]} == {"computed", "cache"}

        svc = QueryJournalService()
        first = svc.query_statistics(metrics=metric, page=1, size=1)
        assert first["total"] == 2
        assert first["summary"]["cache_hit_count"] == 1
        assert first["summary"]["computed_count"] == 1
        token = first["next_page_token"]
        first_ids = first["snapshot"]["execution_ids"]

        # 追加一笔慢查询后继续翻页：仍然绑定原来的已确认执行。
        slow = _record(metric, operator, 480.0, status="慢查询", source="computed")
        second = svc.query_statistics(metrics=metric, page=2, size=1, page_token=token)

        assert second["snapshot"]["token"] == token
        assert second["snapshot"]["execution_ids"] == first_ids
        assert second["total"] == 2
        assert second["summary"]["total"] == 2
        assert second["summary"]["slow_query_count"] == first["summary"]["slow_query_count"]
        assert second["summary"]["average_duration_ms"] == first["summary"]["average_duration_ms"]
        assert second["consistency"]["same_batch"] is True
        assert second["consistency"]["new_executions_since_snapshot"] == 1
        assert second["consistency"]["stale"] is True
        assert slow["execution_id"] not in second["snapshot"]["execution_ids"]
        assert second["items"][0]["execution_id"] == first_ids[1]
        assert second["items"][0]["counted_in_summary"] is True

        # 当前汇总可以由这组明细复算出来。
        verification = svc.verify_snapshot(token)
        assert verification["matches_detail"] is True
        assert verification["detail_count"] == 2
        assert verification["summary"]["average_duration_ms"] == second["summary"]["average_duration_ms"]

        # 重新检索能看到新入账的慢查询。
        refreshed = svc.query_statistics(metrics=metric, page=1, size=10)
        assert refreshed["total"] == 3
        assert refreshed["snapshot"]["token"] != token
        assert slow["execution_id"] in refreshed["snapshot"]["execution_ids"]
        assert refreshed["summary"]["slow_query_count"] == 1

    def test_same_second_executions_are_individually_locatable(self, journal_env):
        _, metric, operator = journal_env
        base = datetime(2026, 10, 2, 12, 0, 0, 120000)
        records = [
            _record(metric, operator, 10.0, recorded_at=base),
            _record(metric, operator, 20.0, recorded_at=base + timedelta(milliseconds=1)),
            _record(metric, operator, 30.0, recorded_at=base + timedelta(milliseconds=2)),
        ]
        assert len({item["execution_id"] for item in records}) == 3
        assert [item["sequence"] for item in records] == sorted(
            item["sequence"] for item in records)

        svc = QueryJournalService()
        result = svc.query_statistics(metrics=metric, page=1, size=10)

        assert result["total"] == 3
        assert result["summary"]["average_duration_ms"] == 20.0
        returned_ids = [item["execution_id"] for item in result["items"]]
        assert returned_ids == [item["execution_id"] for item in records]
        assert all(item["counted_in_summary"] for item in result["items"])
        # 三笔执行落在同一秒：靠微秒时间、序号和执行编号逐笔区分。
        assert {item["查询时间"][:19] for item in result["items"]} == {"2026-10-02 12:00:00"}
        assert len({item["查询时间"] for item in result["items"]}) == 3
        assert len({item["sequence"] for item in result["items"]}) == 3
        assert result["summary"]["execution_ids"] == returned_ids

    def test_same_timestamp_rows_still_locatable_by_sequence(self, journal_env):
        _, metric, operator = journal_env
        moment = datetime(2026, 10, 3, 12, 0, 0, 500000)
        first = _record(metric, operator, 10.0, recorded_at=moment)
        second = _record(metric, operator, 40.0, recorded_at=moment)
        assert first["查询时间"] == second["查询时间"]

        svc = QueryJournalService()
        result = svc.query_statistics(metrics=metric, page=1, size=10)

        sequences = [item["sequence"] for item in result["items"]]
        assert sequences == sorted(sequences)
        assert result["summary"]["execution_ids"] == [first["execution_id"], second["execution_id"]]
        assert result["summary"]["average_duration_ms"] == 25.0
        # 每笔明细带着来源与序号，能反查它是否计入当前平均值。
        by_id = {item["execution_id"]: item for item in result["items"]}
        counted = by_id[first["execution_id"]]
        assert counted["counted_in_summary"] is True
        assert counted["sequence"] + 1 == by_id[second["execution_id"]]["sequence"]

    def test_snapshot_token_must_match_filter(self, journal_env):
        _, metric, operator = journal_env
        _record(metric, operator, 10.0)
        other_metric = f"{metric}-另一组"
        _record(other_metric, operator, 99.0)

        svc = QueryJournalService()
        first = svc.query_statistics(metrics=metric, page=1, size=1)
        # 用不同过滤条件携带旧 token，不能复用快照。
        other = svc.query_statistics(metrics=other_metric, page=1, size=1,
                                     page_token=first["next_page_token"])
        assert other["snapshot"]["token"] != first["next_page_token"]
        assert other["total"] == 1
        assert other["items"][0]["涉及指标"] == other_metric

    def test_marking_slow_query_invalidates_reused_detail(self, journal_env):
        _, metric, operator = journal_env
        record = _record(metric, operator, 10.0)
        svc = QueryJournalService()
        before = svc.build_statistics_detail(metrics=metric)
        assert before["details"][0]["status"] == "正常"

        entry = store.find("query_journal", record["id"])
        entry_result, _ = svc.run_action(record["id"], "标记慢查询")
        assert entry_result["status"] == "慢查询"
        assert entry is record

        after = svc.build_statistics_detail(metrics=metric)
        assert after["reused"] is False
        assert after["rebuild_token"] > before["rebuild_token"]
        assert after["details"][0]["status"] == "慢查询"
        assert svc.summarize(after["details"])["slow_query_count"] == 1


class TestExecutionLocating:
    def test_locate_which_execution_entered_current_average(self, journal_env):
        _, metric, operator = journal_env
        base = datetime(2026, 10, 4, 8, 30, 0)
        first = _record(metric, operator, 10.0, recorded_at=base)
        second = _record(metric, operator, 30.0, status="慢查询",
                         recorded_at=base + timedelta(milliseconds=1))
        svc = QueryJournalService()
        stats = svc.query_statistics(metrics=metric, page=1, size=10)
        assert stats["summary"]["average_duration_ms"] == 20.0

        located = svc.locate_execution(second["execution_id"])
        assert located["counted_in_current_average"] is True
        assert located["execution"]["execution_id"] == second["execution_id"]
        assert located["execution"]["sequence"] > first["sequence"]
        latest = located["latest_snapshot"]
        assert latest["token"] == stats["snapshot"]["token"]
        assert latest["position"] == 2
        assert latest["average_duration_ms"] == 20.0
        assert latest["deviation_from_average_ms"] == 10.0
        assert latest["share_of_total_duration"] == 0.75

        # 快照冻结后追加的新执行不计入这份平均值。
        late = _record(metric, operator, 60.0)
        located_late = svc.locate_execution(late["execution_id"])
        assert located_late["counted_in_current_average"] is False
        still = svc.query_statistics(metrics=metric, page=1, size=10,
                                     page_token=stats["next_page_token"])
        assert still["summary"]["average_duration_ms"] == 20.0
        refreshed = svc.query_statistics(metrics=metric, page=1, size=10)
        assert refreshed["summary"]["average_duration_ms"] == 33.33
        assert svc.locate_execution(late["execution_id"])["counted_in_current_average"] is True
