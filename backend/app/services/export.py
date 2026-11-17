"""数据导出 时间线服务：生成作业时间序列和示例数据。

分片缓冲刷新与版本切换的重复写法已收拢到
:mod:`app.services.export_ledger` 的统一分片账本写入器；本服务保留全部既有
时间线/登记/动作行为不变，只额外把后台重试、立即交付、定时归档三个入口委托
给同一份账本提交。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Callable

from app.services.export_ledger import (
    CONFLICT_DEDUP,
    ENTRY_ARCHIVE,
    ENTRY_DELIVER,
    ENTRY_RETRY,
    MERGE_PAGE,
    CommitResult,
    ShardLedgerWriter,
    shard_writer,
)
from app.store import store

MODULE = "export"
REQUIRED_FIELDS = ["任务编号", "导出对象", "导出格式"]
STATUS_ORDER = ["待导出", "导出中", "已完成", "导出失败"]
ACTION_RULES = {"开始导出": "导出中", "重新导出": "待导出", "标记失败": "导出失败"}
NEGATIVE_ACTIONS = []


class ExportService:
    def __init__(self, writer: ShardLedgerWriter | None = None) -> None:
        # 默认使用进程内统一写入器；测试可注入独立写入器避免互相串数据。
        self.writer = writer or shard_writer

    # ------------------------------------------------------------------
    # 既有行为：时间线、明细、登记、动作，输入输出保持不变
    # ------------------------------------------------------------------
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"导出任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据导出可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"导出任务已{action}"

    # ------------------------------------------------------------------
    # 分片账本：解析既有输入，再收拢缓冲刷新并提交分片账本
    # ------------------------------------------------------------------
    def _parse_existing_input(
        self, entry_id: int
    ) -> Callable[[], tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]]:
        """返回“解析已有输入”的闭包：交付数据、审计摘要归档输入、筛选范围。

        只读取既有时间线数据并原样下传，不改动任何交付数据、筛选范围与审计字段。
        """
        def _load() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
            timeline = {item["id"]: item for item in self.get_timeline()}
            item = timeline.get(entry_id)
            if item is None:
                return [], [], {}
            data_rows = [item]
            audit_inputs = [{
                "操作类型": "分片账本提交",
                "操作人": "导出分片写入器",
                "操作对象": item.get("name", f"导出任务{entry_id}"),
                "操作时间": item.get("end", ""),
                "变更内容": f"导出任务{entry_id}分片缓冲刷新并提交账本",
                "审计结果": "已确认",
                "记录状态": "已记录",
            }]
            filter_scope = {
                "任务编号": item.get("name", ""),
                "任务状态": item.get("status", ""),
                "时间起点": item.get("start", ""),
                "时间终点": item.get("end", ""),
            }
            return data_rows, audit_inputs, filter_scope

        return _load

    def _commit(
        self, entry_id: int, source: str, conflict_path: str = CONFLICT_DEDUP
    ) -> CommitResult | None:
        if store.find(MODULE, entry_id) is None:
            return None
        return self.writer.commit(
            entry_id,
            source,
            existing_input=self._parse_existing_input(entry_id),
            conflict_path=conflict_path,
        )

    def retry(self, entry_id: int, conflict_path: str = CONFLICT_DEDUP) -> CommitResult | None:
        """后台重试入口刷新缓冲。"""
        return self._commit(entry_id, ENTRY_RETRY, conflict_path)

    def deliver(self, entry_id: int, conflict_path: str = CONFLICT_DEDUP) -> CommitResult | None:
        """立即交付入口刷新缓冲。"""
        return self._commit(entry_id, ENTRY_DELIVER, conflict_path)

    def archive(self, entry_id: int, conflict_path: str = CONFLICT_DEDUP) -> CommitResult | None:
        """定时归档入口刷新缓冲。"""
        return self._commit(entry_id, ENTRY_ARCHIVE, conflict_path)

    # ------------------------------------------------------------------
    # 账本复核：提交后再打开分片账本审计记录，对照既有数据
    # ------------------------------------------------------------------
    def ledger_detail(self, entry_id: int) -> dict[str, Any] | None:
        """任务详情：展示分片序号和审计摘要（交付页只给已提交版本包）。"""
        if store.find(MODULE, entry_id) is None:
            return None
        ledger = self.writer.ledgers().get(entry_id)
        versions = [version.to_dict() for version in ledger.versions] if ledger else []
        return {
            "task_id": entry_id,
            "refreshing": self.writer.is_refreshing(entry_id),
            "latest_package": self.writer.build_package(entry_id),
            "versions": versions,
            "audit_records": self.writer.task_audit_records(entry_id),
        }

    def audit_records(self, entry_id: int | None = None) -> list[dict[str, Any]]:
        """分片账本审计记录：单任务或跨业务模块全量，按既有次序返回。"""
        if entry_id is None:
            return self.writer.all_audit_records()
        return self.writer.task_audit_records(entry_id)

    def delivery_package(
        self, entry_id: int, *, merge_mode: str = MERGE_PAGE, include_refreshing: bool = False
    ) -> dict[str, Any] | None:
        """交付页：只提供账本已提交的版本包；刷新中可按两种合并方式看中间态。"""
        if store.find(MODULE, entry_id) is None:
            return None
        return self.writer.build_package(
            entry_id, merge_mode=merge_mode, include_refreshing=include_refreshing
        )

    def ensure_delivered(self, entry_id: int) -> CommitResult | None:
        """导出文件接口消费前确保已提交：未提交则走立即交付入口统一提交。"""
        if store.find(MODULE, entry_id) is None:
            return None
        if self.writer.latest_version(entry_id) is None:
            return self.writer.commit(
                entry_id, ENTRY_DELIVER, existing_input=self._parse_existing_input(entry_id)
            )
        return None
