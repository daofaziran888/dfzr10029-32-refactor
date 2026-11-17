"""导出分片账本：把分片缓冲刷新与版本切换收拢为单一账本提交。

重构前，后台重试、立即交付、定时归档三个入口各自维护一套“刷新分片缓冲 +
切换导出版本”的写法，版本边界不共享时会出现重复分片或缺失审计摘要。本模块把
这套重复逻辑收拢成唯一的 :meth:`ShardLedgerWriter.commit` 账本提交：

1. 解析既有输入（导出任务的交付数据与筛选范围），先暂存进 :class:`ShardBuffer`；
2. 在同一个版本边界内一次性完成缓冲刷新（按完整页切分片）与版本切换（生成
   下一个已提交版本），并把分片账本审计记录随提交原子落账；
3. 输出统一的分片账本导出版本包，三个入口只负责以不同来源调用同一份写法。

处理范围严格限定在“导出分片缓冲区”与“审计摘要归档业务数据”：这里不改动任何
导出任务的原始交付数据、筛选范围与审计字段，只在账本里保留这些输入的原样副本。
账本审计记录的归档输入（操作类型、操作人、操作对象、操作时间、变更内容、
审计结果、记录状态）原样保留，并额外挂上分片序号、版本号等链路字段，供导出
文件接口与数据审计模块跨业务模块复核。
"""
from __future__ import annotations

import hashlib
import json
import threading
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterable

__all__ = [
    "ENTRY_RETRY",
    "ENTRY_DELIVER",
    "ENTRY_ARCHIVE",
    "ENTRY_SOURCES",
    "CONFLICT_DEDUP",
    "CONFLICT_REBASE",
    "MERGE_PAGE",
    "MERGE_SEQUENCE",
    "ShardBuffer",
    "LedgerShard",
    "LedgerAuditRecord",
    "LedgerVersion",
    "CommitResult",
    "ShardLedger",
    "ShardLedgerWriter",
    "shard_writer",
    "fingerprint",
]

# 三个刷新入口的来源标识；同一批次经不同入口提交，可能落到不同的分片账本。
ENTRY_RETRY = "后台重试"
ENTRY_DELIVER = "立即交付"
ENTRY_ARCHIVE = "定时归档"
ENTRY_SOURCES = (ENTRY_RETRY, ENTRY_DELIVER, ENTRY_ARCHIVE)

# 分片账本审计记录冲突的两条有效路径。
# 去重：同一提交键已落账，直接取回既有确认结果，不再产生第二份确认；
# 重定基：既有提交被刷新时，复用同一版本边界重放，最终数据与审计字段保持不变。
CONFLICT_DEDUP = "去重确认"
CONFLICT_REBASE = "重定基提交"
CONFLICT_PATHS = (CONFLICT_DEDUP, CONFLICT_REBASE)

# 账本正在刷新、交付包已被请求时的两种中间合并方式。
MERGE_PAGE = "按账本完整页合并"
MERGE_SEQUENCE = "按分片序号串接"

DEFAULT_SHARD_PAGE_SIZE = 2
DEFAULT_OPERATOR = "导出分片写入器"


def fingerprint(payload: Any) -> str:
    """对交付数据与筛选范围计算稳定指纹，作为幂等提交键的输入。"""
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _default_clock() -> str:
    """默认操作时间；测试可注入固定时钟，保证重新执行得到相同内容。"""
    return datetime.now().isoformat(timespec="seconds")


@dataclass
class ShardBuffer:
    """导出分片缓冲区：暂存一次刷新解析出的既有输入。

    缓冲区分两类业务数据，且只保存原样副本：
    - ``data_rows``：交付数据（时间线行，字段与既有导出完全一致）；
    - ``audit_inputs``：审计摘要归档业务数据（分片账本审计记录的归档输入）。
    """

    task_id: int
    data_rows: list[dict[str, Any]] = field(default_factory=list)
    audit_inputs: list[dict[str, Any]] = field(default_factory=list)
    source: str | None = None

    def stage(
        self,
        data_rows: Iterable[dict[str, Any]],
        audit_inputs: Iterable[dict[str, Any]],
        source: str,
    ) -> None:
        """解析已有输入后暂存进缓冲区，深拷贝避免外部再改动污染账本。"""
        if source not in ENTRY_SOURCES:
            raise ValueError(f"未知刷新入口：{source}")
        self.source = source
        self.data_rows.extend(deepcopy(row) for row in data_rows)
        self.audit_inputs.extend(deepcopy(row) for row in audit_inputs)

    def drain(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
        """缓冲刷新：取出全部暂存内容并清空，返回（交付数据, 审计归档输入, 来源）。"""
        rows = self.data_rows
        audits = self.audit_inputs
        source = self.source
        self.data_rows = []
        self.audit_inputs = []
        self.source = None
        return rows, audits, source

    def is_empty(self) -> bool:
        return not self.data_rows and not self.audit_inputs


@dataclass
class LedgerShard:
    """一个已提交分片：完整页内的交付数据与该页的审计摘要。"""

    shard_seq: int
    page_index: int
    data_rows: list[dict[str, Any]]
    audit_summary: dict[str, Any]
    audit_inputs: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_seq": self.shard_seq,
            "page_index": self.page_index,
            "row_count": len(self.data_rows),
            "data_rows": deepcopy(self.data_rows),
            "audit_summary": deepcopy(self.audit_summary),
        }


@dataclass
class LedgerAuditRecord:
    """分片账本审计记录。

    归档输入沿用数据审计模块的审计字段口径（操作类型、操作人、操作对象、
    操作时间、变更内容、审计结果、记录状态），再附分片/版本链路字段，供导出
    文件接口消费、跨业务模块复核。
    """

    audit_id: str
    task_id: int
    version: int
    shard_seq: int
    page_index: int
    operation_type: str
    operator: str
    target: str
    operated_at: str
    change_detail: str
    audit_result: str
    record_status: str
    source: str
    filter_scope: dict[str, Any]
    row_count: int
    data_fingerprint: str
    conflict_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "记录编号": self.audit_id,
            "task_id": self.task_id,
            "version": self.version,
            "shard_seq": self.shard_seq,
            "page_index": self.page_index,
            "操作类型": self.operation_type,
            "操作人": self.operator,
            "操作对象": self.target,
            "操作时间": self.operated_at,
            "变更内容": self.change_detail,
            "审计结果": self.audit_result,
            "记录状态": self.record_status,
            "来源": self.source,
            "筛选范围": deepcopy(self.filter_scope),
            "数据行数": self.row_count,
            "数据指纹": self.data_fingerprint,
            "冲突路径": self.conflict_path,
        }


@dataclass
class LedgerVersion:
    """一个已提交的分片账本版本包。"""

    task_id: int
    version: int
    commit_key: str
    source: str
    filter_scope: dict[str, Any]
    shards: list[LedgerShard]
    audit_records: list[LedgerAuditRecord]
    committed_at: str
    conflict_path: str | None = None

    @property
    def shard_count(self) -> int:
        return len(self.shards)

    @property
    def row_count(self) -> int:
        return sum(len(shard.data_rows) for shard in self.shards)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "version": self.version,
            "commit_key": self.commit_key,
            "source": self.source,
            "committed_at": self.committed_at,
            "filter_scope": deepcopy(self.filter_scope),
            "shard_count": self.shard_count,
            "row_count": self.row_count,
            "shards": [shard.to_dict() for shard in self.shards],
            "audit_records": [record.to_dict() for record in self.audit_records],
            "conflict_path": self.conflict_path,
        }


@dataclass
class CommitResult:
    """单次账本提交的确认结果。"""

    task_id: int
    version: int
    commit_key: str
    source: str
    shard_count: int
    row_count: int
    audit_record_count: int
    created: bool
    conflict_path: str | None
    committed_at: str
    package: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "version": self.version,
            "commit_key": self.commit_key,
            "source": self.source,
            "shard_count": self.shard_count,
            "row_count": self.row_count,
            "audit_record_count": self.audit_record_count,
            "created": self.created,
            "conflict_path": self.conflict_path,
            "committed_at": self.committed_at,
            "package": deepcopy(self.package),
        }


class ShardLedger:
    """单个导出任务的分片账本：缓冲区 + 已提交版本序列。"""

    def __init__(self, task_id: int, page_size: int) -> None:
        self.task_id = task_id
        self.page_size = page_size
        self.buffer = ShardBuffer(task_id)
        self._versions: list[LedgerVersion] = []
        self._commit_keys: "OrderedDict[str, LedgerVersion]" = OrderedDict()
        self._next_shard_seq = 1
        # 账本正在刷新（版本边界已打开、尚未提交）时的暂存边界。
        self.refreshing: dict[str, Any] | None = None

    @property
    def versions(self) -> list[LedgerVersion]:
        return list(self._versions)

    def latest_version(self) -> LedgerVersion | None:
        return self._versions[-1] if self._versions else None

    def find_version(self, version: int) -> LedgerVersion | None:
        for current in self._versions:
            if current.version == version:
                return current
        return None

    def find_by_commit_key(self, commit_key: str) -> LedgerVersion | None:
        return self._commit_keys.get(commit_key)

    def all_audit_records(self) -> list[LedgerAuditRecord]:
        """沿用既有次序：按版本号、分片序号依次读出全部审计记录。"""
        records: list[LedgerAuditRecord] = []
        for current in self._versions:
            records.extend(current.audit_records)
        return records

    def append_version(self, version: LedgerVersion) -> None:
        self._versions.append(version)
        self._commit_keys[version.commit_key] = version
        self._next_shard_seq += version.shard_count


class ShardLedgerWriter:
    """统一分片账本写入器。

    三个刷新入口（后台重试 / 立即交付 / 定时归档）都只调用这里的同一份
    :meth:`commit`，在同一把锁、同一个版本边界里完成缓冲刷新与版本切换，
    因此收拢后既不会产生重复分片，也不会缺失审计摘要。
    """

    def __init__(
        self,
        page_size: int = DEFAULT_SHARD_PAGE_SIZE,
        clock: Callable[[], str] | None = None,
    ) -> None:
        self.page_size = page_size
        self._clock = clock or _default_clock
        self._ledgers: dict[int, ShardLedger] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 账本与缓冲的只读视图
    # ------------------------------------------------------------------
    def _ledger(self, task_id: int) -> ShardLedger:
        return self._ledgers.setdefault(task_id, ShardLedger(task_id, self.page_size))

    def ledgers(self) -> dict[int, ShardLedger]:
        with self._lock:
            return dict(self._ledgers)

    def is_refreshing(self, task_id: int) -> bool:
        with self._lock:
            ledger = self._ledgers.get(task_id)
            return bool(ledger and ledger.refreshing is not None)

    def latest_version(self, task_id: int) -> LedgerVersion | None:
        with self._lock:
            ledger = self._ledgers.get(task_id)
            return ledger.latest_version() if ledger else None

    def get_version(self, task_id: int, version: int) -> LedgerVersion | None:
        with self._lock:
            ledger = self._ledgers.get(task_id)
            return ledger.find_version(version) if ledger else None

    def task_audit_records(self, task_id: int) -> list[dict[str, Any]]:
        """提交后再打开分片账本审计记录：对照既有数据，按既有次序返回。"""
        with self._lock:
            ledger = self._ledgers.get(task_id)
            if ledger is None:
                return []
            return [record.to_dict() for record in ledger.all_audit_records()]

    def all_audit_records(self) -> list[dict[str, Any]]:
        """跨业务模块读取：按任务编号、版本号、分片序号的既有次序串接。"""
        with self._lock:
            records: list[LedgerAuditRecord] = []
            for task_id in sorted(self._ledgers):
                records.extend(self._ledgers[task_id].all_audit_records())
            return [record.to_dict() for record in records]

    # ------------------------------------------------------------------
    # 阶段一：解析已有输入，暂存进导出分片缓冲区
    # ------------------------------------------------------------------
    def stage(
        self,
        task_id: int,
        data_rows: Iterable[dict[str, Any]],
        audit_inputs: Iterable[dict[str, Any]],
        source: str,
    ) -> None:
        if source not in ENTRY_SOURCES:
            raise ValueError(f"未知刷新入口：{source}")
        with self._lock:
            self._ledger(task_id).buffer.stage(data_rows, audit_inputs, source)

    def begin_refresh(self, task_id: int) -> None:
        """打开版本边界：标记账本进入“正在刷新”的中间交付状态。"""
        with self._lock:
            ledger = self._ledger(task_id)
            if ledger.refreshing is not None:
                raise RuntimeError(f"导出任务 {task_id} 的分片账本正在刷新")
            ledger.refreshing = {"opened_at": self._clock()}

    # ------------------------------------------------------------------
    # 阶段二+三：缓冲刷新与版本切换收拢成的单一账本提交
    # ------------------------------------------------------------------
    def retry_commit(
        self,
        task_id: int,
        *,
        existing_input: Callable[[], tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]]
        | None = None,
        conflict_path: str = CONFLICT_DEDUP,
    ) -> CommitResult:
        """后台重试入口：刷新缓冲并提交分片账本。"""
        return self.commit(task_id, ENTRY_RETRY, existing_input=existing_input,
                           conflict_path=conflict_path)

    def deliver_commit(
        self,
        task_id: int,
        *,
        existing_input: Callable[[], tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]]
        | None = None,
        conflict_path: str = CONFLICT_DEDUP,
    ) -> CommitResult:
        """立即交付入口：刷新缓冲并提交分片账本。"""
        return self.commit(task_id, ENTRY_DELIVER, existing_input=existing_input,
                           conflict_path=conflict_path)

    def archive_commit(
        self,
        task_id: int,
        *,
        existing_input: Callable[[], tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]]
        | None = None,
        conflict_path: str = CONFLICT_DEDUP,
    ) -> CommitResult:
        """定时归档入口：刷新缓冲并提交分片账本。"""
        return self.commit(task_id, ENTRY_ARCHIVE, existing_input=existing_input,
                           conflict_path=conflict_path)

    def commit(
        self,
        task_id: int,
        source: str,
        *,
        existing_input: Callable[[], tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]]
        | None = None,
        conflict_path: str = CONFLICT_DEDUP,
    ) -> CommitResult:
        """单一分片账本提交：在同一版本边界里完成缓冲刷新 + 版本切换。

        ``existing_input`` 用于“解析已有输入”：返回
        ``(交付数据, 审计摘要归档输入, 筛选范围)``。若缓冲区已有暂存内容，
        以缓冲区内容为准、沿用缓冲时解析的筛选范围；缓冲区为空时才调用它，
        保证空刷新也能解析既有输入而不是静默丢数据。

        冲突的两条有效路径：
        - :data:`CONFLICT_DEDUP`：提交键已存在，直接取回既有确认，不产生第二份；
        - :data:`CONFLICT_REBASE`：在同一版本边界上重放提交，最终数据与审计字段不变。
        """
        if source not in ENTRY_SOURCES:
            raise ValueError(f"未知刷新入口：{source}")
        if conflict_path not in CONFLICT_PATHS:
            raise ValueError(f"未知冲突路径：{conflict_path}")

        with self._lock:
            ledger = self._ledger(task_id)

            # 1. 缓冲刷新：取出暂存数据；缓冲区为空时解析既有输入补位。
            buffered_rows, buffered_audits, buffered_source = ledger.buffer.drain()
            if buffered_rows or buffered_audits:
                data_rows = buffered_rows
                audit_inputs = buffered_audits
                effective_source = buffered_source or source
                filter_scope = self._scope_from_rows(data_rows)
            elif existing_input is not None:
                parsed_rows, parsed_audits, parsed_scope = existing_input()
                data_rows = deepcopy(parsed_rows)
                audit_inputs = deepcopy(parsed_audits)
                filter_scope = deepcopy(parsed_scope)
                effective_source = source
            else:
                # 空刷新且无既有输入：确认结果不产生任何分片与审计摘要。
                latest = ledger.latest_version()
                return self._empty_commit_result(task_id, source, latest)

            commit_key = fingerprint({
                "task_id": task_id,
                "filter_scope": filter_scope,
                "data_rows": data_rows,
                "audit_inputs": audit_inputs,
            })

            # 2. 冲突处理：同一提交键不会多出第二份确认结果。
            existing = ledger.find_by_commit_key(commit_key)
            if existing is not None:
                if conflict_path == CONFLICT_DEDUP:
                    return self._result_from_version(existing, source, created=False,
                                                     conflict_path=CONFLICT_DEDUP)
                # 重定基：复用同一版本边界重放，不新增版本、不新增分片与审计记录。
                return self._result_from_version(existing, source, created=False,
                                                 conflict_path=CONFLICT_REBASE)

            # 3. 版本切换：在同一版本边界内切到下一个已提交版本。
            next_version = (ledger.latest_version().version + 1
                            if ledger.latest_version() is not None else 1)
            committed_at = self._clock()
            version = self._build_version(
                ledger=ledger,
                version_no=next_version,
                commit_key=commit_key,
                source=effective_source,
                filter_scope=filter_scope,
                data_rows=data_rows,
                audit_inputs=audit_inputs,
                committed_at=committed_at,
            )
            ledger.append_version(version)
            ledger.refreshing = None
            return self._result_from_version(version, source, created=True, conflict_path=None)

    # ------------------------------------------------------------------
    # 版本包：交付页只提供账本已提交的版本包
    # ------------------------------------------------------------------
    def build_package(
        self,
        task_id: int,
        *,
        version: int | None = None,
        merge_mode: str = MERGE_PAGE,
        include_refreshing: bool = False,
    ) -> dict[str, Any] | None:
        """输出统一的分片账本导出版本包。

        默认只交付账本已提交的版本；``include_refreshing=True`` 且账本正在刷新
        时，会把刷新中的缓冲数据作为中间交付状态并入。两种合并方式
        （按账本完整页合并 / 按分片序号串接）的中间表示可能不同，但拍平后的
        交付数据与审计字段最终保持一致。
        """
        if merge_mode not in (MERGE_PAGE, MERGE_SEQUENCE):
            raise ValueError(f"未知合并方式：{merge_mode}")
        with self._lock:
            ledger = self._ledgers.get(task_id)
            if ledger is None:
                return None
            target = ledger.find_version(version) if version is not None else ledger.latest_version()
            if target is None and not (include_refreshing and ledger.refreshing is not None):
                return None

            committed_rows: list[dict[str, Any]] = []
            audit_records: list[dict[str, Any]] = []
            shards: list[dict[str, Any]] = []
            filter_scope: dict[str, Any] = {}
            base: LedgerVersion | None = target

            if base is not None:
                filter_scope = deepcopy(base.filter_scope)
                if merge_mode == MERGE_PAGE:
                    # 按账本完整页合并：整页整页地搬运。
                    for shard in base.shards:
                        shards.append(shard.to_dict())
                        committed_rows.extend(deepcopy(shard.data_rows))
                else:
                    # 按分片序号串接：以分片序号为序串成一条连续交付流。
                    for shard in sorted(base.shards, key=lambda item: item.shard_seq):
                        shards.append(shard.to_dict())
                        committed_rows.extend(deepcopy(shard.data_rows))
                audit_records.extend(record.to_dict() for record in base.audit_records)

            refreshing_rows: list[dict[str, Any]] = []
            if include_refreshing and ledger.refreshing is not None and not ledger.buffer.is_empty():
                # 刷新中的数据尚未提交，只作为中间态，绝不进入已提交审计记录。
                refreshing_rows = deepcopy(ledger.buffer.data_rows)
                if merge_mode == MERGE_PAGE:
                    # 完整页合并：缓冲行并入交付流尾部，不新增分片。
                    committed_rows.extend(refreshing_rows)
                else:
                    # 序号串接：缓冲行以占位序号续接在最后一个分片之后。
                    last_seq = base.shards[-1].shard_seq if base is not None else 0
                    shards.append({
                        "shard_seq": last_seq + 1,
                        "page_index": None,
                        "row_count": len(refreshing_rows),
                        "data_rows": deepcopy(refreshing_rows),
                        "audit_summary": {"状态": "刷新中", "已提交": False},
                    })

            # 两种合并方式的分片结构不同，但拍平后的交付数据一致：
            # 完整页合并时缓冲行已在 committed_rows；序号串接时单独并入尾分片。
            if merge_mode == MERGE_PAGE:
                data_rows = committed_rows
            else:
                data_rows = committed_rows + refreshing_rows
            return {
                "task_id": task_id,
                "version": base.version if base is not None else None,
                "merge_mode": merge_mode,
                "committed": base is not None,
                "refreshing": ledger.refreshing is not None,
                "filter_scope": deepcopy(filter_scope),
                "shard_count": len(shards),
                "row_count": len(data_rows),
                "shards": shards,
                "data_rows": data_rows,
                "audit_records": audit_records,
            }

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _scope_from_rows(self, data_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """从交付数据里还原筛选范围（保留原样，不做口径改写）。"""
        if not data_rows:
            return {}
        first = data_rows[0]
        return {
            "任务编号": first.get("name", ""),
            "任务状态": first.get("status", ""),
            "时间起点": first.get("start", ""),
            "时间终点": first.get("end", ""),
        }

    def _build_version(
        self,
        *,
        ledger: ShardLedger,
        version_no: int,
        commit_key: str,
        source: str,
        filter_scope: dict[str, Any],
        data_rows: list[dict[str, Any]],
        audit_inputs: list[dict[str, Any]],
        committed_at: str,
    ) -> LedgerVersion:
        """把缓冲数据按账本完整页切成分片，并生成每个分片的审计摘要与审计记录。"""
        shards: list[LedgerShard] = []
        audit_records: list[LedgerAuditRecord] = []
        total_pages = max(1, (len(data_rows) + self.page_size - 1) // self.page_size)

        for page_index in range(total_pages):
            start = page_index * self.page_size
            page_rows = data_rows[start:start + self.page_size]
            if not page_rows:
                continue
            shard_seq = ledger._next_shard_seq + len(shards)
            page_fingerprint = fingerprint(page_rows)
            audit_input = self._match_audit_input(audit_inputs, page_index, page_rows)

            # 审计摘要：归档业务数据的汇总，字段保持既有审计口径。
            audit_summary = {
                "操作类型": audit_input.get("操作类型", "分片账本提交"),
                "操作人": audit_input.get("操作人", DEFAULT_OPERATOR),
                "审计结果": audit_input.get("审计结果", "已确认"),
                "记录状态": audit_input.get("记录状态", "已记录"),
                "分片序号": shard_seq,
                "版本": version_no,
                "数据行数": len(page_rows),
                "数据指纹": page_fingerprint,
            }
            shards.append(LedgerShard(
                shard_seq=shard_seq,
                page_index=page_index + 1,
                data_rows=deepcopy(page_rows),
                audit_summary=audit_summary,
                audit_inputs=[deepcopy(audit_input)],
            ))

            record = LedgerAuditRecord(
                audit_id=f"EXLA-{ledger.task_id:04d}-V{version_no:02d}-S{shard_seq:03d}",
                task_id=ledger.task_id,
                version=version_no,
                shard_seq=shard_seq,
                page_index=page_index + 1,
                operation_type=audit_summary["操作类型"],
                operator=audit_summary["操作人"],
                target=audit_input.get("操作对象", f"导出任务{ledger.task_id}-分片{shard_seq}"),
                operated_at=audit_input.get("操作时间", committed_at),
                change_detail=audit_input.get("变更内容",
                                               f"{source}：提交分片 {shard_seq}（版本 {version_no}）"),
                audit_result=audit_summary["审计结果"],
                record_status=audit_summary["记录状态"],
                source=source,
                filter_scope=deepcopy(filter_scope),
                row_count=len(page_rows),
                data_fingerprint=page_fingerprint,
            )
            audit_records.append(record)

        return LedgerVersion(
            task_id=ledger.task_id,
            version=version_no,
            commit_key=commit_key,
            source=source,
            filter_scope=deepcopy(filter_scope),
            shards=shards,
            audit_records=audit_records,
            committed_at=committed_at,
        )

    @staticmethod
    def _match_audit_input(
        audit_inputs: list[dict[str, Any]], page_index: int, page_rows: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """沿用既有次序取对应分片的审计摘要归档输入；不足时由数据行兜底生成。"""
        if page_index < len(audit_inputs):
            return audit_inputs[page_index]
        first = page_rows[0] if page_rows else {}
        return {
            "操作类型": "分片账本提交",
            "操作人": DEFAULT_OPERATOR,
            "操作对象": first.get("name", "导出分片"),
            "操作时间": first.get("end", ""),
            "变更内容": "分片缓冲刷新并提交分片账本",
            "审计结果": "已确认",
            "记录状态": "已记录",
        }

    def _result_from_version(
        self,
        version: LedgerVersion,
        source: str,
        *,
        created: bool,
        conflict_path: str | None,
    ) -> CommitResult:
        return CommitResult(
            task_id=version.task_id,
            version=version.version,
            commit_key=version.commit_key,
            source=source,
            shard_count=version.shard_count,
            row_count=version.row_count,
            audit_record_count=len(version.audit_records),
            created=created,
            conflict_path=conflict_path,
            committed_at=version.committed_at,
            package=self._package_from_version(version),
        )

    def _package_from_version(self, version: LedgerVersion) -> dict[str, Any]:
        data_rows: list[dict[str, Any]] = []
        for shard in version.shards:
            data_rows.extend(deepcopy(shard.data_rows))
        return {
            "task_id": version.task_id,
            "version": version.version,
            "merge_mode": MERGE_PAGE,
            "committed": True,
            "refreshing": False,
            "filter_scope": deepcopy(version.filter_scope),
            "shard_count": version.shard_count,
            "row_count": version.row_count,
            "shards": [shard.to_dict() for shard in version.shards],
            "data_rows": data_rows,
            "audit_records": [record.to_dict() for record in version.audit_records],
        }

    def _empty_commit_result(
        self, task_id: int, source: str, latest: LedgerVersion | None
    ) -> CommitResult:
        if latest is None:
            return CommitResult(
                task_id=task_id, version=0, commit_key=fingerprint({"task_id": task_id, "empty": True}),
                source=source, shard_count=0, row_count=0, audit_record_count=0,
                created=False, conflict_path=None, committed_at=self._clock(),
                package={
                    "task_id": task_id, "version": None, "merge_mode": MERGE_PAGE,
                    "committed": False, "refreshing": False, "filter_scope": {},
                    "shard_count": 0, "row_count": 0, "shards": [], "data_rows": [],
                    "audit_records": [],
                },
            )
        return self._result_from_version(latest, source, created=False, conflict_path=None)


# 进程内统一写入器：三个入口共享同一个版本边界。
shard_writer = ShardLedgerWriter()
