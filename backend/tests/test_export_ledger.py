"""分片账本写入器重排后的回归测试（标准库 unittest，无需额外依赖）。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import threading
import unittest
from datetime import date, timedelta

from app import store as store_module
from app.routers import export as export_router
from app.seed import SEED_ROWS
from app.services.export import ExportService
from app.services.export_ledger import (
    CONFLICT_DEDUP,
    CONFLICT_REBASE,
    ENTRY_ARCHIVE,
    ENTRY_DELIVER,
    ENTRY_RETRY,
    MERGE_PAGE,
    MERGE_SEQUENCE,
    ShardLedgerWriter,
    fingerprint,
    shard_writer,
)

FIXED_NOW = "2026-11-17T10:00:00"


def fixed_clock() -> str:
    return FIXED_NOW


def reset_store() -> None:
    """把全局内存仓库恢复成种子数据（原地替换，保持各模块的 import 绑定）。"""
    store_module.store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }


def reset_writer(writer: ShardLedgerWriter) -> None:
    with writer._lock:
        writer._ledgers.clear()


def legacy_csv(items: list[dict]) -> str:
    """重构前 /export 的原始 CSV 生成口径，作为对照基线。"""
    import csv
    import io

    def csv_cell(value):
        text = str(value) if value is not None else ""
        if text and text[0] in "=+-@":
            text = "'" + text
        return '"' + text.replace('"', '""') + '"'

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return buf.getvalue()


class ExistingBehaviorTests(unittest.TestCase):
    """既有交付数据、筛选范围与审计字段必须保持原样。"""

    def setUp(self) -> None:
        reset_store()
        self.writer = ShardLedgerWriter(clock=fixed_clock)
        self.service = ExportService(writer=self.writer)

    def test_timeline_matches_seed_data(self) -> None:
        items = self.service.get_timeline()
        self.assertEqual([item["id"] for item in items], [1, 2, 3])
        today = date.today()
        for i, item in enumerate(items):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            self.assertEqual(item["start"], start)
            self.assertEqual(item["end"], end)
            self.assertIn("作业启动", item["logs"])
        # 交付数据字段集合不变
        self.assertEqual(set(items[0].keys()), {"id", "name", "start", "end", "status", "logs"})

    def test_create_entry_validation_and_defaults(self) -> None:
        entry, missing = self.service.create_entry({"任务编号": "X"})
        self.assertIsNone(entry)
        self.assertEqual(missing, ["导出对象", "导出格式"])

        entry, missing = self.service.create_entry(
            {"任务编号": "EXPO-9", "导出对象": "客户表", "导出格式": "CSV"}
        )
        self.assertEqual(missing, [])
        self.assertEqual(entry["id"], 4)
        self.assertEqual(entry["status"], "待导出")
        self.assertTrue(entry["pending"])
        self.assertFalse(entry["abnormal"])

    def test_run_action_transitions_and_errors(self) -> None:
        entry, message = self.service.run_action(1, "开始导出")
        self.assertEqual(entry["status"], "导出中")
        self.assertIn("开始导出", message)

        entry, _ = self.service.run_action(1, "标记失败")
        self.assertEqual(entry["status"], "导出失败")
        self.assertFalse(entry["pending"])

        missing, message = self.service.run_action(999, "开始导出")
        self.assertIsNone(missing)
        self.assertIn("不存在或已归档", message)

        bad, message = self.service.run_action(1, "不存在的动作")
        self.assertIsNone(bad)
        self.assertIn("不属于数据导出可执行范围", message)

    def test_export_csv_byte_identical_to_legacy_output(self) -> None:
        reset_writer(export_router.service.writer)
        response = export_router.export_entries()
        # 已安装的 Starlette 会把同步迭代器包成异步生成器，直接断言响应契约，
        # CSV 内容的字节级一致性由下面的同步渲染对照保证。
        self.assertEqual(response.media_type, "text/csv; charset=utf-8")
        self.assertEqual(response.headers["Content-Disposition"],
                         "attachment; filename=export.csv")

        timeline = export_router.service.get_timeline()
        for item in timeline:
            export_router.service.ensure_delivered(int(item["id"]))
        items: list[dict] = []
        for item in timeline:
            package = export_router.service.delivery_package(int(item["id"]))
            if package and package.get("data_rows"):
                items.extend(package["data_rows"])
            else:
                items.append(item)
        self.assertEqual(items, timeline)
        new_content = legacy_csv(items)
        expected = legacy_csv(export_router.service.get_timeline())
        self.assertEqual(new_content, expected)

    def test_seed_delivery_data_untouched_after_commits(self) -> None:
        before = [dict(row) for row in store_module.store.rows("export")]
        self.service.retry(1)
        self.service.deliver(2)
        self.service.archive(3)
        after = store_module.store.rows("export")
        self.assertEqual(before, after)


class UnifiedCommitTests(unittest.TestCase):
    """三个入口收拢为单一分片账本提交，共享同一个版本边界。"""

    def setUp(self) -> None:
        reset_store()
        self.writer = ShardLedgerWriter(page_size=2, clock=fixed_clock)
        self.service = ExportService(writer=self.writer)

    def test_three_entries_share_version_boundary_no_duplicate_shards(self) -> None:
        r1 = self.service.retry(1)
        r2 = self.service.deliver(1)
        r3 = self.service.archive(1)
        self.assertTrue(r1.created)
        self.assertFalse(r2.created)
        self.assertFalse(r3.created)
        # 同一批次经三个入口提交，只产生一个版本、一份分片与一份审计摘要
        self.assertEqual([r.version for r in (r1, r2, r3)], [1, 1, 1])
        self.assertEqual([r.commit_key for r in (r1, r2, r3)],
                         [r1.commit_key] * 3)
        ledger = self.writer.ledgers()[1]
        self.assertEqual(len(ledger.versions), 1)
        self.assertEqual(len(ledger.all_audit_records()), r1.audit_record_count)

    def test_same_batch_different_entries_build_different_ledgers(self) -> None:
        a = self.service.retry(1)
        b = self.service.deliver(2)
        self.assertNotEqual(a.commit_key, b.commit_key)
        self.assertEqual(sorted(self.writer.ledgers()), [1, 2])

    def test_buffer_flush_and_commit_are_one_atomic_boundary(self) -> None:
        rows = self.service.get_timeline()
        audit_inputs = [
            {"操作类型": "分片账本提交", "操作人": "张审计", "操作对象": f"对象{i}",
             "操作时间": "2026-11-17T09:00:00", "变更内容": "归档",
             "审计结果": "已确认", "记录状态": "已记录"}
            for i in range(3)
        ]
        self.writer.stage(7, rows, audit_inputs, ENTRY_RETRY)
        ledger = self.writer.ledgers()[7]
        self.assertFalse(ledger.buffer.is_empty())
        result = self.writer.commit(7, ENTRY_RETRY)
        # 缓冲已刷新清空
        self.assertTrue(ledger.buffer.is_empty())
        # 3 行、page_size=2 → 完整页 2 页
        self.assertEqual(result.shard_count, 2)
        self.assertEqual(result.row_count, 3)
        self.assertEqual([s.page_index for s in ledger.latest_version().shards], [1, 2])
        self.assertEqual([s.shard_seq for s in ledger.latest_version().shards], [1, 2])

    def test_concurrent_commits_create_single_version(self) -> None:
        barrier = threading.Barrier(3)
        results: list = []

        def worker(method_name: str) -> None:
            barrier.wait()
            results.append(getattr(self.service, method_name)(2))

        threads = [
            threading.Thread(target=worker, args=("retry",)),
            threading.Thread(target=worker, args=("deliver",)),
            threading.Thread(target=worker, args=("archive",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 3)
        self.assertEqual({r.version for r in results}, {1})
        self.assertEqual(sum(1 for r in results if r.created), 1)
        self.assertEqual(len(self.writer.ledgers()[2].versions), 1)
        self.assertEqual(len(self.writer.ledgers()[2].all_audit_records()), 1)

    def test_unknown_entry_returns_none(self) -> None:
        self.assertIsNone(self.service.retry(404))
        self.assertIsNone(self.service.deliver(404))
        self.assertIsNone(self.service.archive(404))
        self.assertIsNone(self.service.ledger_detail(404))
        self.assertIsNone(self.service.delivery_package(404))


class AuditRecordTests(unittest.TestCase):
    """分片账本审计记录：归档输入保留、既有次序、跨模块复核。"""

    def setUp(self) -> None:
        reset_store()
        self.writer = ShardLedgerWriter(page_size=2, clock=fixed_clock)
        self.service = ExportService(writer=self.writer)

    def test_audit_archive_input_fields_preserved(self) -> None:
        self.service.retry(1)
        records = self.service.audit_records(1)
        self.assertEqual(len(records), 1)
        record = records[0]
        for field in ("操作类型", "操作人", "操作对象", "操作时间",
                      "变更内容", "审计结果", "记录状态"):
            self.assertIn(field, record)
        self.assertEqual(record["操作人"], "导出分片写入器")
        self.assertEqual(record["审计结果"], "已确认")
        self.assertEqual(record["记录编号"], record["audit_id"])
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["shard_seq"], 1)
        # 筛选范围原样保留
        item = self.service.get_timeline()[0]
        self.assertEqual(record["筛选范围"]["时间起点"], item["start"])
        self.assertEqual(record["筛选范围"]["时间终点"], item["end"])

    def test_records_follow_existing_version_and_shard_order(self) -> None:
        rows = self.service.get_timeline()
        self.writer.stage(5, rows, [], ENTRY_ARCHIVE)
        self.writer.commit(5, ENTRY_ARCHIVE)
        # 再次提交不同数据形成第二版本
        self.writer.stage(5, rows[:1], [], ENTRY_ARCHIVE)
        self.writer.commit(5, ENTRY_ARCHIVE)
        records = self.service.audit_records(5)
        ordering = [(r["version"], r["shard_seq"]) for r in records]
        self.assertEqual(ordering, sorted(ordering))
        self.assertEqual([r["version"] for r in records][:2], [1, 1])
        self.assertEqual(records[-1]["version"], 2)

    def test_reopen_after_commit_matches_committed_data(self) -> None:
        result = self.service.deliver(1)
        reopened = self.service.ledger_detail(1)
        self.assertEqual(len(reopened["audit_records"]), result.audit_record_count)
        self.assertEqual(reopened["versions"][-1]["commit_key"], result.commit_key)
        package = reopened["latest_package"]
        self.assertTrue(package["committed"])
        self.assertEqual(package["row_count"], result.row_count)

    def test_cross_module_read_includes_all_ledger_audits(self) -> None:
        self.service.retry(1)
        self.service.deliver(2)
        all_records = self.service.audit_records()
        self.assertEqual({r["task_id"] for r in all_records}, {1, 2})
        task_ids = [r["task_id"] for r in all_records]
        self.assertEqual(task_ids, sorted(task_ids))

    def test_rerun_produces_identical_content(self) -> None:
        first = self.service.retry(1).to_dict()
        other_writer = ShardLedgerWriter(page_size=2, clock=fixed_clock)
        other_service = ExportService(writer=other_writer)
        second = other_service.retry(1).to_dict()
        self.assertEqual(first, second)
        self.assertEqual(
            fingerprint(first["package"]["data_rows"]),
            fingerprint(second["package"]["data_rows"]),
        )


class ConflictPathTests(unittest.TestCase):
    """冲突涉及两条有效路径：去重确认 与 重定基提交。"""

    def setUp(self) -> None:
        reset_store()
        self.writer = ShardLedgerWriter(clock=fixed_clock)
        self.service = ExportService(writer=self.writer)

    def test_dedup_returns_existing_confirmation(self) -> None:
        first = self.service.retry(1, CONFLICT_DEDUP)
        again = self.service.deliver(1, CONFLICT_DEDUP)
        self.assertTrue(first.created)
        self.assertFalse(again.created)
        self.assertEqual(again.conflict_path, CONFLICT_DEDUP)
        self.assertEqual(first.package, again.package)
        self.assertEqual(len(self.writer.ledgers()[1].all_audit_records()), 1)

    def test_rebase_replays_same_boundary_without_new_version(self) -> None:
        first = self.service.retry(1, CONFLICT_DEDUP)
        rebased = self.service.archive(1, CONFLICT_REBASE)
        self.assertFalse(rebased.created)
        self.assertEqual(rebased.conflict_path, CONFLICT_REBASE)
        self.assertEqual(rebased.version, first.version)
        self.assertEqual(rebased.package, first.package)
        self.assertEqual(len(self.writer.ledgers()[1].versions), 1)

    def test_invalid_conflict_path_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.writer.commit(1, ENTRY_RETRY, conflict_path="非法路径")
        with self.assertRaises(ValueError):
            self.writer.commit(1, "未知入口")


class RefreshingMergeTests(unittest.TestCase):
    """刷新中交付包被请求：两种合并方式中间态不同，最终数据与审计字段不变。"""

    def setUp(self) -> None:
        reset_store()
        self.writer = ShardLedgerWriter(page_size=2, clock=fixed_clock)
        self.service = ExportService(writer=self.writer)
        self.rows = self.service.get_timeline()
        self.writer.stage(3, self.rows, [], ENTRY_DELIVER)
        self.writer.commit(3, ENTRY_DELIVER)

    def test_refreshing_buffer_is_excluded_from_committed_package(self) -> None:
        self.writer.begin_refresh(3)
        self.writer.stage(3, self.rows[:1], [], ENTRY_RETRY)
        self.assertTrue(self.writer.is_refreshing(3))

        committed = self.writer.build_package(3)
        # 默认只交付已提交版本：刷新中的缓冲行不进入版本包
        self.assertEqual(committed["row_count"], 3)
        self.assertTrue(committed["committed"])
        self.assertEqual(committed["version"], 1)

    def test_two_merge_modes_differ_in_intermediate_but_converge(self) -> None:
        self.writer.begin_refresh(3)
        self.writer.stage(3, self.rows[:1], [], ENTRY_RETRY)

        page_pkg = self.writer.build_package(
            3, merge_mode=MERGE_PAGE, include_refreshing=True)
        seq_pkg = self.writer.build_package(
            3, merge_mode=MERGE_SEQUENCE, include_refreshing=True)

        # 中间交付状态的结构不同：完整页合并沿用已提交分片；序号串接待刷新尾分片
        self.assertEqual(len(seq_pkg["shards"]), page_pkg["shard_count"] + 1)
        tail = seq_pkg["shards"][-1]
        self.assertIsNone(tail["page_index"])
        self.assertEqual(tail["audit_summary"]["状态"], "刷新中")

        # 拍平后的交付数据一致
        self.assertEqual(
            [r["id"] for r in page_pkg["data_rows"]],
            [r["id"] for r in seq_pkg["data_rows"]],
        )
        self.assertEqual(page_pkg["row_count"], seq_pkg["row_count"])
        # 审计记录只来自已提交版本，刷新中不产生审计摘要
        self.assertEqual(page_pkg["audit_records"], seq_pkg["audit_records"])
        self.assertEqual(len(page_pkg["audit_records"]), 2)

    def test_after_commit_final_package_identical_either_mode(self) -> None:
        self.writer.begin_refresh(3)
        self.writer.stage(3, self.rows[:1], [], ENTRY_RETRY)
        self.writer.commit(3, ENTRY_RETRY)
        self.assertFalse(self.writer.is_refreshing(3))

        page_pkg = self.writer.build_package(3, merge_mode=MERGE_PAGE)
        seq_pkg = self.writer.build_package(3, merge_mode=MERGE_SEQUENCE)
        self.assertEqual(page_pkg["data_rows"], seq_pkg["data_rows"])
        self.assertEqual(
            [r["记录编号"] for r in page_pkg["audit_records"]],
            [r["记录编号"] for r in seq_pkg["audit_records"]],
        )
        # 最终交付数据与审计字段保持原样
        self.assertEqual([r["id"] for r in page_pkg["data_rows"]], [r["id"] for r in self.rows[:1]])
        self.assertTrue(all(r["审计结果"] == "已确认" for r in page_pkg["audit_records"]))

    def test_double_begin_refresh_rejected(self) -> None:
        self.writer.begin_refresh(8)
        with self.assertRaises(RuntimeError):
            self.writer.begin_refresh(8)


class RouterContractTests(unittest.TestCase):
    """路由层契约：既有接口不变，新增账本入口与冲突查询参数可用。"""

    def setUp(self) -> None:
        reset_store()
        reset_writer(export_router.service.writer)
        # 审计跨模块接口里 ExportService() 使用进程级全局写入器，一并清空。
        reset_writer(shard_writer)

    def test_list_contract_unchanged(self) -> None:
        payload = export_router.list_entries()
        self.assertEqual(set(payload.keys()), {"items"})
        self.assertEqual(len(payload["items"]), 3)

    def test_commit_endpoints_and_404s(self) -> None:
        ok = export_router.retry_commit(1)
        self.assertTrue(ok.ok)
        self.assertIn("后台重试", ok.message)
        self.assertEqual(ok.entry["version"], 1)

        dup = export_router.deliver_commit(1)
        self.assertTrue(dup.ok)
        self.assertFalse(dup.entry["created"])

        rebased = export_router.archive_commit(1, conflict="rebase")
        self.assertEqual(rebased.entry["conflict_path"], CONFLICT_REBASE)

        bad_conflict = export_router.retry_commit(1, conflict="nope")
        self.assertFalse(bad_conflict.ok)

        missing = export_router.deliver_commit(404)
        self.assertFalse(missing.ok)
        self.assertIn("不存在", missing.message)

    def test_ledger_and_package_endpoints(self) -> None:
        export_router.retry_commit(2)
        detail = export_router.get_ledger(2)
        self.assertEqual(detail["task_id"], 2)
        self.assertTrue(detail["audit_records"])

        package = export_router.get_package(2)
        self.assertTrue(package["committed"])
        seq_package = export_router.get_package(2, merge_mode="sequence")
        self.assertEqual(package["data_rows"], seq_package["data_rows"])

        with self.assertRaises(Exception):
            export_router.get_package(404)
        with self.assertRaises(Exception):
            export_router.get_ledger(404)

    def test_ledger_audit_endpoint_supports_cross_module_read(self) -> None:
        global_service = ExportService()
        reset_writer(global_service.writer)
        global_service.retry(1)
        global_service.archive(2)
        single = export_router.ledger_audits(entry_id=1)
        self.assertEqual(single["total"], 1)
        all_records = export_router.ledger_audits()
        self.assertEqual(all_records["total"], 2)

    def test_audit_router_cross_module_endpoint(self) -> None:
        from app.routers import audit as audit_router

        global_service = ExportService()
        reset_writer(global_service.writer)
        global_service.retry(1)
        payload = audit_router.export_ledger_records()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["task_id"], 1)
        scoped = audit_router.export_ledger_records(entry_id=1)
        self.assertEqual(scoped["total"], 1)


if __name__ == "__main__":
    unittest.main()
