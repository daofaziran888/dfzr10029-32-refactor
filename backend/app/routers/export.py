"""数据导出 时间线接口：返回作业时间序列结构。

分片账本相关接口：
- 三个刷新入口（后台重试 / 立即交付 / 定时归档）都走统一的分片账本提交；
- 导出文件接口消费分片账本审计记录后再按既有口径输出 CSV；
- 任务详情展示分片序号与审计摘要，交付页只提供账本已提交的版本包。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.export import ExportService
from app.services.export_ledger import (
    CONFLICT_DEDUP,
    CONFLICT_REBASE,
    MERGE_PAGE,
    MERGE_SEQUENCE,
)

router = APIRouter(prefix="/api/export", tags=["数据导出"])
service = ExportService()
LIST_FIELDS = ["任务编号", "导出对象", "导出格式", "筛选条件", "数据行数", "导出时间", "文件大小", "任务状态"]
STATUSES = ["待导出", "导出中", "已完成", "导出失败"]

_MERGE_MODES = {"page": MERGE_PAGE, "sequence": MERGE_SEQUENCE}
_CONFLICT_PATHS = {"dedup": CONFLICT_DEDUP, "rebase": CONFLICT_REBASE}


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/ledger-audits")
def ledger_audits(entry_id: int | None = Query(default=None, description="缺省跨业务模块读取全量")) -> dict[str, Any]:
    """分片账本审计记录：单任务按既有次序返回，缺省跨业务模块全量读取。"""
    # 进程内直接调用时 FastAPI 的 Query 占位会原样传入，归一化为缺省全量读取。
    if not isinstance(entry_id, int):
        entry_id = None
    records = service.audit_records(entry_id)
    return {"items": records, "total": len(records)}


@router.get("/export")
def export_entries() -> StreamingResponse:
    # 导出文件接口消费分片账本：先确保每个任务的缓冲已统一提交，再按时间线
    # 既有次序从已提交版本包里取交付数据，CSV 字段、顺序与单元格口径保持不变。
    timeline = service.get_timeline()
    for item in timeline:
        service.ensure_delivered(int(item["id"]))
    items: list[dict[str, Any]] = []
    for item in timeline:
        package = service.delivery_package(int(item["id"]))
        if package and package.get("data_rows"):
            items.extend(package["data_rows"])
        else:
            items.append(item)

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=export.csv"})


@router.get("/{entry_id}/ledger")
def get_ledger(entry_id: int) -> dict[str, Any]:
    """任务详情：展示分片序号和审计摘要，只给账本已提交的版本包。"""
    detail = service.ledger_detail(entry_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"导出任务 {entry_id} 不存在或已归档")
    return detail


def _resolve_merge(merge_mode: str) -> str | None:
    """解析合并方式；未显式传值（FastAPI 的 Query 占位）时回到完整页合并。"""
    if not isinstance(merge_mode, str):
        return MERGE_PAGE
    return _MERGE_MODES.get(merge_mode)


@router.get("/{entry_id}/package")
def get_package(
    entry_id: int,
    merge_mode: str = Query(default="page", description="page=按账本完整页合并；sequence=按分片序号串接"),
    include_refreshing: bool = Query(default=False, description="是否纳入刷新中的中间交付状态"),
) -> dict[str, Any]:
    """交付页：只提供账本已提交的版本包；刷新中两种合并方式中间态不同、最终一致。"""
    mode = _resolve_merge(merge_mode)
    if mode is None:
        raise HTTPException(status_code=400, detail="merge_mode 仅支持 page 或 sequence")
    if not isinstance(include_refreshing, bool):
        include_refreshing = False
    package = service.delivery_package(entry_id, merge_mode=mode,
                                       include_refreshing=include_refreshing)
    if package is None:
        raise HTTPException(status_code=404, detail=f"导出任务 {entry_id} 不存在或已归档")
    return package


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"导出任务 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="导出任务已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


def _commit_response(entry_id: int, result: Any | None, source: str) -> ActionResult:
    if result is None:
        return ActionResult(ok=False, message=f"导出任务 {entry_id} 不存在或已归档")
    return ActionResult(ok=True, message=f"分片账本已通过{source}提交", entry=result.to_dict())


def _resolve_conflict(conflict: str) -> str | None:
    """解析冲突路径；未显式传值（FastAPI 的 Query 占位）时回到默认去重确认。"""
    if not isinstance(conflict, str):
        return CONFLICT_DEDUP
    return _CONFLICT_PATHS.get(conflict)


@router.post("/{entry_id}/retry")
def retry_commit(
    entry_id: int,
    conflict: str = Query(default="dedup", description="dedup=去重确认；rebase=重定基提交"),
) -> ActionResult:
    """后台重试入口：刷新分片缓冲并提交分片账本。"""
    path = _resolve_conflict(conflict)
    if path is None:
        return ActionResult(ok=False, message="conflict 仅支持 dedup 或 rebase")
    return _commit_response(entry_id, service.retry(entry_id, path), "后台重试")


@router.post("/{entry_id}/deliver")
def deliver_commit(
    entry_id: int,
    conflict: str = Query(default="dedup", description="dedup=去重确认；rebase=重定基提交"),
) -> ActionResult:
    """立即交付入口：刷新分片缓冲并提交分片账本。"""
    path = _resolve_conflict(conflict)
    if path is None:
        return ActionResult(ok=False, message="conflict 仅支持 dedup 或 rebase")
    return _commit_response(entry_id, service.deliver(entry_id, path), "立即交付")


@router.post("/{entry_id}/archive")
def archive_commit(
    entry_id: int,
    conflict: str = Query(default="dedup", description="dedup=去重确认；rebase=重定基提交"),
) -> ActionResult:
    """定时归档入口：刷新分片缓冲并提交分片账本。"""
    path = _resolve_conflict(conflict)
    if path is None:
        return ActionResult(ok=False, message="conflict 仅支持 dedup 或 rebase")
    return _commit_response(entry_id, service.archive(entry_id, path), "定时归档")
