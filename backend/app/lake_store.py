"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。

    status 区分文件在提交过程中的位置：
    - live：已进入当前分区清单，可被压实/查询读取
    - staged：归并/压实刚注册、清单尚未换版，不能算作当前分区
    - retired：已被新清单替换，当前分区不再读取，但历史快照仍可按编号回溯
    """
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    status: str = "live"


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 分区文件清单 -----------------------------

def _manifest_checksum(file_ids: list[str]) -> str:
    return hashlib.md5(",".join(sorted(file_ids)).encode()).hexdigest()


@dataclass
class PartitionManifest:
    """分区文件清单：压实唯一可信的输入。

    每个分区同一时刻只有一份 confirmed 清单；归并/压实先登记 draft 清单，
    完成换版（confirm）后旧清单自动 superseded。压实不得读取处于 staged
    状态的文件，也不得把 draft 清单当输入，否则会把"新产物 + 原文件"一起读。
    """
    manifest_id: str
    lake_table: str
    partition_key: str
    version: int
    file_ids: list[str]
    checksum: str
    status: str  # draft | confirmed | superseded
    source: str  # baseline | merge | compact | append
    source_ref: str | None  # 归并号 / 压实号
    created_at: float
    confirmed_at: float | None = None


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 分区文件清单：lake_table -> list[PartitionManifest]（版本序）
        self._manifests: dict[str, list[PartitionManifest]] = {}

        # 归并回执：merge_id -> 首次确认的替换结果（补领回执按它回溯）
        self._merge_receipts: dict[str, dict[str, Any]] = {}

        # 压实工单：ticket_id -> 工单（记录了接单时采用的清单版本）
        self._compact_tickets: dict[str, dict[str, Any]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int,
        manifest: str | None = None, status: str = "live",
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
            status=status,
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        # live 文件即当前分区可读文件：若分区已有确认清单，立即并入新版本，
        # 保证"当前分区可读文件"与"确认清单"始终一致。staged 是中间产物，不并入。
        if status == "live" and self.get_confirmed_manifest(lake_table, partition_key) is not None:
            self.append_to_confirmed_manifest(
                lake_table, partition_key, [file_id], source="append")
        return pf

    def list_files(
        self, lake_table: str, partition_key: str | None = None,
        include_staged: bool = False,
    ) -> list[PartitionFile]:
        """列出分区文件。默认只返回当前分区可见的 live 文件。

        staged 文件是尚未换版的中间产物，压实入口必须把它们排除在外，
        否则归并产物会与原件一同被读入。include_staged=True 供恢复流程
        清点中间产物使用。
        """
        files = list(self._files.get(lake_table, {}).values())
        if not include_staged:
            files = [f for f in files if f.status == "live"]
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def retire_files(self, lake_table: str, file_ids: list[str]) -> int:
        """把文件标记为 retired：当前分区不再读取，历史快照仍可回溯。"""
        table_files = self._files.get(lake_table, {})
        retired = 0
        for fid in file_ids:
            pf = table_files.get(fid)
            if pf is not None and pf.status != "retired":
                pf.status = "retired"
                retired += 1
        return retired

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。

        这里是物理删除，只供彻底作废的场景使用。归并/压实换版请走
        confirm_manifest，让旧文件 retire 而不是消失，历史快照才读得到。
        """
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 分区文件清单（换版协议） ----

    def list_manifests(self, lake_table: str, partition_key: str | None = None) -> list[PartitionManifest]:
        manifests = list(self._manifests.get(lake_table, []))
        if partition_key is not None:
            manifests = [m for m in manifests if m.partition_key == partition_key]
        return manifests

    def get_manifest(self, lake_table: str, manifest_id: str) -> PartitionManifest | None:
        for m in self._manifests.get(lake_table, []):
            if m.manifest_id == manifest_id:
                return m
        return None

    def get_confirmed_manifest(
        self, lake_table: str, partition_key: str
    ) -> PartitionManifest | None:
        """当前分区唯一确认的清单。压实只认这一份。"""
        confirmed = [
            m for m in self._manifests.get(lake_table, [])
            if m.partition_key == partition_key and m.status == "confirmed"
        ]
        if not confirmed:
            return None
        return max(confirmed, key=lambda m: m.version)

    def stage_manifest(
        self, lake_table: str, partition_key: str, file_ids: list[str],
        source: str, source_ref: str | None = None,
    ) -> PartitionManifest:
        """登记一份草稿清单。同分区同来源多次 staging 幂等，返回同一份草稿。"""
        pool = self._manifests.setdefault(lake_table, [])
        if source_ref is not None:
            for m in pool:
                if (
                    m.partition_key == partition_key
                    and m.status == "draft"
                    and m.source == source
                    and m.source_ref == source_ref
                ):
                    m.file_ids = list(file_ids)
                    m.checksum = _manifest_checksum(file_ids)
                    return m
        current = self.get_confirmed_manifest(lake_table, partition_key)
        rank = 1 if current is None else current.version + 1
        for m in pool:
            if m.partition_key == partition_key and m.version >= rank:
                rank = m.version + 1
        manifest = PartitionManifest(
            manifest_id=f"mf-{lake_table}-{partition_key}-v{rank}-{uuid.uuid4().hex[:6]}",
            lake_table=lake_table,
            partition_key=partition_key,
            version=rank,
            file_ids=list(file_ids),
            checksum=_manifest_checksum(file_ids),
            status="draft",
            source=source,
            source_ref=source_ref,
            created_at=time.time(),
        )
        pool.append(manifest)
        return manifest

    def confirm_manifest(self, lake_table: str, manifest_id: str) -> PartitionManifest | None:
        """换版：把草稿清单置为 confirmed，旧 confirmed 清单置为 superseded。

        换版同时把被移出清单的文件 retire（不是删除），把清单内 staged 文件
        置为 live。这样当前分区只读一份清单，历史快照仍能按编号回溯。
        """
        target = self.get_manifest(lake_table, manifest_id)
        if target is None or target.status != "draft":
            return target
        previous = self.get_confirmed_manifest(lake_table, target.partition_key)
        for m in self._manifests.get(lake_table, []):
            if m.partition_key != target.partition_key:
                continue
            if m.status == "confirmed":
                m.status = "superseded"
        target.status = "confirmed"
        target.confirmed_at = time.time()

        table_files = self._files.get(lake_table, {})
        # 新清单里的文件转为 live
        for fid in target.file_ids:
            pf = table_files.get(fid)
            if pf is not None:
                pf.status = "live"
        # 旧清单里被移出的文件 retire：当前分区不再读，历史快照仍可回溯
        if previous is not None and previous.manifest_id != target.manifest_id:
            for fid in previous.file_ids:
                if fid in target.file_ids:
                    continue
                pf = table_files.get(fid)
                if pf is not None:
                    pf.status = "retired"
        return target

    def ensure_confirmed_manifest(
        self, lake_table: str, partition_key: str, source: str = "baseline"
    ) -> PartitionManifest | None:
        """确保分区有一份确认清单。没有时按当前 live 文件补一份基线清单。"""
        current = self.get_confirmed_manifest(lake_table, partition_key)
        if current is not None:
            return current
        live_ids = [f.file_id for f in self.list_files(lake_table, partition_key)]
        if not live_ids:
            return None
        draft = self.stage_manifest(lake_table, partition_key, live_ids, source=source)
        return self.confirm_manifest(lake_table, draft.manifest_id)

    def append_to_confirmed_manifest(
        self, lake_table: str, partition_key: str, new_file_ids: list[str],
        source: str = "append", source_ref: str | None = None,
    ) -> PartitionManifest:
        """在确认清单基础上追加文件并换版：用于压实扫描之后新到的文件。"""
        current = self.ensure_confirmed_manifest(lake_table, partition_key)
        base_ids = list(current.file_ids) if current is not None else []
        merged_ids = list(dict.fromkeys([*base_ids, *new_file_ids]))
        draft = self.stage_manifest(
            lake_table, partition_key, merged_ids, source=source, source_ref=source_ref)
        return self.confirm_manifest(lake_table, draft.manifest_id) or draft

    def open_snapshot_for_manifest(
        self, lake_table: str, manifest: PartitionManifest
    ) -> LakeSnapshot:
        """为一份确认清单生成可用快照，历史回溯按快照编号即可读回文件清单。

        同一份文件清单幂等：已有相同清单的可用快照时直接复用，避免重复开快照。
        """
        wanted = set(manifest.file_ids)
        chain = self.list_snapshots(lake_table)
        for snap in chain:
            if snap.status == "available" and set(snap.file_ids) == wanted:
                return snap
        parent = chain[-1].snapshot_id if chain else None
        rows = sum(f.file_rows for f in self.read_manifest_files(lake_table, manifest))
        snap = self.create_snapshot(
            lake_table, file_ids=list(manifest.file_ids),
            parent_snapshot=parent, added_rows=rows, deleted_rows=0,
        )
        self.mark_snapshot_available(lake_table, snap.snapshot_id)
        return snap

    def read_manifest_files(
        self, lake_table: str, manifest: PartitionManifest
    ) -> list[PartitionFile]:
        """按清单读取文件，包含已 retire 的历史文件，保证快照回溯行数对得上。"""
        table_files = self._files.get(lake_table, {})
        return [table_files[fid] for fid in manifest.file_ids if fid in table_files]

    # ---- 归并回执 ----

    def record_merge_receipt(self, merge_id: str, receipt: dict[str, Any]) -> dict[str, Any]:
        """首次确认的替换结果即回执；重复写入返回同一份，不被后续事件覆盖。"""
        existing = self._merge_receipts.get(merge_id)
        if existing is not None:
            return existing
        stored = dict(receipt)
        stored["merge_id"] = merge_id
        stored["issued_at"] = time.time()
        self._merge_receipts[merge_id] = stored
        return stored

    def get_merge_receipt(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_receipts.get(merge_id)

    # ---- 压实工单 ----

    def issue_compact_ticket(self, ticket: dict[str, Any]) -> dict[str, Any]:
        stored = dict(ticket)
        self._compact_tickets[stored["ticket_id"]] = stored
        return stored

    def get_compact_ticket(self, ticket_id: str) -> dict[str, Any] | None:
        return self._compact_tickets.get(ticket_id)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(
        self, merge_id: str, partial_file_ids: list[str], **extra: Any
    ) -> None:
        """保存归并检查点。

        partial_file_ids 保留为整组原件；extra 记录换版所需的分区、产物、
        草稿清单与阶段，恢复时据此继续而无需重读 staged 残留。
        """
        record: dict[str, Any] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }
        record.update(extra)
        self._merge_checkpoints[merge_id] = record

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()