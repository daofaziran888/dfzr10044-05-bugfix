"""小文件归并领域服务：碎片化分区归并、检查点恢复、换版与回执补领。

关键约束（与压实交接的契约）：
- 归并产物只能以 staged 状态先落盘，换版（confirm）之前不得进入当前分区清单。
- 归并的输入必须取自同一份已确认分区清单，整组原件一次性提交。
- 归并完成换版后签发回执；回执一经签发即幂等，补领只能拿回首次确认的结果。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.lake_store import lake_store, PartitionFile, PartitionManifest


class SmallFileMergeService(BaseModuleService):
    MODULE = "small_file_merge"
    ENTITY = "归并任务"
    OPERATION_TERM = "归并"
    LIST_FIELDS = ["归并编号", "所属湖表", "目标分区", "归并前文件数", "归并后文件数", "目标文件大小", "归并策略", "完成时间"]
    REQUIRED_FIELDS = ["归并编号", "所属湖表", "目标分区"]
    STATUS_ORDER = ["待归并", "归并中", "已归并", "归并失败"]
    ACTION_RULES = {"启动归并": "归并中", "确认归并": "已归并", "标记失败": "归并失败"}
    NEGATIVE_ACTIONS = []

    # ---- 基础能力 ----

    def detect_small_files(self, lake_table: str, partition_key: str,
                           size_threshold: int = 5000) -> list[PartitionFile]:
        """检测指定分区下的碎片文件（字节数小于阈值），只看当前分区可见文件。"""
        files = lake_store.list_files(lake_table, partition_key)
        return [f for f in files if f.file_bytes < size_threshold]

    def plan_merge_targets(self, small_files: list[PartitionFile],
                           target_size: int = 10000) -> list[dict[str, Any]]:
        """规划归并目标文件：按目标大小将小文件分组。"""
        total_rows = sum(f.file_rows for f in small_files)
        total_bytes = sum(f.file_bytes for f in small_files)
        if total_bytes == 0:
            return []
        target_count = max(1, (total_bytes + target_size - 1) // target_size)
        rows_per_target = total_rows // target_count
        targets = []
        for i in range(target_count):
            targets.append({
                "target_file_index": i,
                "estimated_rows": rows_per_target,
                "estimated_bytes": total_bytes // target_count,
            })
        return targets

    # ---- 换版式归并 ----

    def _manifest_snapshot(self, manifest: PartitionManifest | None,
                           lake_table: str) -> dict[str, Any] | None:
        if manifest is None:
            return None
        files = lake_store.read_manifest_files(lake_table, manifest)
        return {
            "manifest_id": manifest.manifest_id,
            "version": manifest.version,
            "status": manifest.status,
            "source": manifest.source,
            "source_ref": manifest.source_ref,
            "checksum": manifest.checksum,
            "file_ids": list(manifest.file_ids),
            "file_count": len(manifest.file_ids),
            "rows": sum(f.file_rows for f in files),
        }

    def stage_batch_merge(self, merge_id: str, lake_table: str, partition_key: str,
                          source_file_ids: list[str]) -> dict[str, Any]:
        """整组提交原件做批量归并：只暂存产物与草稿清单，不换版。

        同一归并号下的重复提交是幂等的：
        - 已换版：直接补领首次确认的回执，不再产生任何新文件；
        - 尚未换版：复用已登记的中断产物与草稿清单，继续等待恢复。
        """
        source_file_ids = list(dict.fromkeys(source_file_ids))
        receipt = lake_store.get_merge_receipt(merge_id)
        if receipt is not None:
            return self._reclaim_result(receipt, source_file_ids)
        checkpoint = lake_store.get_merge_checkpoint(merge_id)
        if checkpoint is not None and checkpoint.get("phase") in ("staged", "confirmed"):
            # 中断恢复场景：同一归并号再次提交整组原件，复用既有中间产物
            return {
                "merge_id": merge_id,
                "lake_table": lake_table,
                "partition_key": partition_key,
                "reused": True,
                "phase": checkpoint.get("phase"),
                "source_file_ids": checkpoint.get("group_file_ids", source_file_ids),
                "product_file_ids": checkpoint.get("product_file_ids", []),
                "draft_manifest_id": checkpoint.get("draft_manifest_id"),
                "checkpoint": checkpoint,
            }
        if not source_file_ids:
            return {"error": "empty_source_group", "merge_id": merge_id}

        input_manifest = lake_store.ensure_confirmed_manifest(lake_table, partition_key)
        if input_manifest is None:
            return {"error": "no_confirmed_manifest", "merge_id": merge_id,
                    "lake_table": lake_table, "partition_key": partition_key}

        # 整组原件必须全部落在同一份已确认清单里，避免跨清单拼输入
        missing = [fid for fid in source_file_ids if fid not in set(input_manifest.file_ids)]
        if missing:
            return {
                "error": "source_not_in_confirmed_manifest",
                "merge_id": merge_id,
                "missing_file_ids": missing,
                "manifest_id": input_manifest.manifest_id,
            }
        source_files = [lake_store.get_file(lake_table, fid) for fid in source_file_ids]
        if any(f is None for f in source_files):
            return {"error": "source_file_not_found", "merge_id": merge_id}

        total_rows = sum(f.file_rows for f in source_files if f is not None)
        total_bytes = sum(f.file_bytes for f in source_files if f is not None)

        # 产物先以 staged 落盘：当前分区清单与压实扫描都看不到它
        product = lake_store.register_file(
            lake_table, partition_key,
            rows=total_rows, bytes_=max(1, total_bytes // 2),
            status="staged",
        )
        draft = lake_store.stage_manifest(
            lake_table, partition_key, [product.file_id],
            source="merge", source_ref=merge_id,
        )
        lake_store.save_merge_checkpoint(
            merge_id,
            source_file_ids,
            lake_table=lake_table,
            partition_key=partition_key,
            group_file_ids=source_file_ids,
            product_file_ids=[product.file_id],
            draft_manifest_id=draft.manifest_id,
            input_manifest_id=input_manifest.manifest_id,
            source_rows=total_rows,
            phase="staged",
        )
        return {
            "merge_id": merge_id,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "source_file_ids": source_file_ids,
            "source_file_count": len(source_file_ids),
            "source_rows": total_rows,
            "product_file_ids": [product.file_id],
            "product_rows": total_rows,
            "draft_manifest_id": draft.manifest_id,
            "input_manifest": self._manifest_snapshot(input_manifest, lake_table),
            "draft_manifest": self._manifest_snapshot(draft, lake_table),
            "phase": "staged",
            "swapped": False,
        }

    def recover_batch_merge(self, merge_id: str, lake_table: str | None = None,
                            confirm_swap: bool = False) -> dict[str, Any] | None:
        """从检查点恢复中断的批量归并。

        恢复只补登记缺失的产物与草稿清单，绝不把 staged 产物并入当前分区。
        confirm_swap=True 时才执行换版；换版后签发（或补领）归并回执。
        """
        checkpoint = lake_store.get_merge_checkpoint(merge_id)
        if checkpoint is None:
            return None
        lk = lake_table or checkpoint.get("lake_table")
        partition_key = checkpoint.get("partition_key")
        if not lk or not partition_key:
            # 旧版检查点只有 partial_file_ids：保持只读恢复语义
            return {
                "merge_id": merge_id,
                "recover_from_partial": len(checkpoint.get("partial_file_ids", [])),
                "recovered": True,
                "phase": checkpoint.get("phase", "legacy"),
            }

        group = list(checkpoint.get("group_file_ids", checkpoint.get("partial_file_ids", [])))
        product_ids = list(checkpoint.get("product_file_ids", []))
        # 产物若在中断中丢失，按整组原件重新推导，不读取任何 staged 残留
        product_files = [lake_store.get_file(lk, fid) for fid in product_ids]
        if not product_ids or any(pf is None for pf in product_files):
            source_files = [lake_store.get_file(lk, fid) for fid in group]
            source_files = [pf for pf in source_files if pf is not None]
            total_rows = sum(f.file_rows for f in source_files)
            total_bytes = sum(f.file_bytes for f in source_files)
            product = lake_store.register_file(
                lk, partition_key, rows=total_rows,
                bytes_=max(1, total_bytes // 2), status="staged",
            )
            product_ids = [product.file_id]

        draft = None
        if checkpoint.get("draft_manifest_id"):
            draft = lake_store.get_manifest(lk, checkpoint["draft_manifest_id"])
        if draft is None or draft.status != "draft":
            draft = lake_store.stage_manifest(
                lk, partition_key, product_ids, source="merge", source_ref=merge_id)

        lake_store.save_merge_checkpoint(
            merge_id, group, lake_table=lk, partition_key=partition_key,
            group_file_ids=group, product_file_ids=product_ids,
            draft_manifest_id=draft.manifest_id,
            input_manifest_id=checkpoint.get("input_manifest_id"),
            source_rows=checkpoint.get("source_rows"),
            phase="staged",
            recovered_at=time.time(),
        )
        detail = self.merge_detail(merge_id, lk) or {
            "merge_id": merge_id, "lake_table": lk, "partition_key": partition_key}
        detail.update({
            "recovered": True,
            "phase": "staged",
            "product_file_ids": product_ids,
            "draft_manifest_id": draft.manifest_id,
        })
        if confirm_swap:
            confirmed = self.confirm_batch_merge(merge_id, lk)
            if confirmed is not None:
                detail["swap"] = confirmed
                detail["phase"] = confirmed.get("phase", "confirmed")
        return detail

    def confirm_batch_merge(self, merge_id: str, lake_table: str | None = None) -> dict[str, Any] | None:
        """归并换版：确认草稿清单，原件 retire，产物进入当前分区，签发回执。"""
        receipt = lake_store.get_merge_receipt(merge_id)
        if receipt is not None:
            return {
                "merge_id": merge_id,
                "phase": "confirmed",
                "already_confirmed": True,
                "receipt": receipt,
                "reclaimed": True,
            }
        checkpoint = lake_store.get_merge_checkpoint(merge_id)
        if checkpoint is None:
            return None
        lk = lake_table or checkpoint.get("lake_table")
        draft_id = checkpoint.get("draft_manifest_id")
        if not lk or not draft_id:
            return None
        draft = lake_store.get_manifest(lk, draft_id)
        if draft is None:
            return None
        if draft.status == "draft":
            # 换版前先做冲突核对：若当前确认清单已被别的作业（如压实）
            # 换掉，且本归并的整组原件已不在其中，说明产物已过期，
            # 不能再把旧产物换回当前分区。
            current = lake_store.get_confirmed_manifest(lk, draft.partition_key)
            group_ids = list(checkpoint.get("group_file_ids", []))
            if current is not None and current.manifest_id != draft_id:
                current_ids = set(current.file_ids)
                if group_ids and not set(group_ids) <= current_ids:
                    return {
                        "merge_id": merge_id,
                        "phase": "stale",
                        "swapped": False,
                        "reason": "input_manifest_superseded",
                        "stale_product_file_ids": list(checkpoint.get("product_file_ids", [])),
                        "current_manifest": self._manifest_snapshot(current, lk),
                        "draft_manifest": self._manifest_snapshot(draft, lk),
                    }
            lake_store.confirm_manifest(lk, draft_id)
        elif draft.status != "confirmed":
            # 草稿被后续换版顶掉：不得把过期产物硬塞回当前分区
            return {
                "merge_id": merge_id,
                "phase": "failed",
                "error": "draft_manifest_superseded",
                "draft_manifest_id": draft_id,
            }
        confirmed_manifest = lake_store.get_confirmed_manifest(lk, draft.partition_key)
        snapshot = (
            lake_store.open_snapshot_for_manifest(lk, confirmed_manifest)
            if confirmed_manifest is not None else None
        )
        product_ids = list(checkpoint.get("product_file_ids", []))
        product_files = [lake_store.get_file(lk, fid) for fid in product_ids]
        product_rows = sum(f.file_rows for f in product_files if f is not None)
        receipt = lake_store.record_merge_receipt(merge_id, {
            "merge_id": merge_id,
            "lake_table": lk,
            "partition_key": draft.partition_key,
            "action": "merged",
            "status": "confirmed",
            "source_file_ids": list(checkpoint.get("group_file_ids", [])),
            "source_file_count": len(checkpoint.get("group_file_ids", [])),
            "source_rows": checkpoint.get("source_rows"),
            "product_file_ids": product_ids,
            "product_rows": product_rows,
            "manifest_id": confirmed_manifest.manifest_id if confirmed_manifest else None,
            "manifest_version": confirmed_manifest.version if confirmed_manifest else None,
            "manifest_checksum": confirmed_manifest.checksum if confirmed_manifest else None,
            "snapshot_id": snapshot.snapshot_id if snapshot else None,
        })
        lake_store.save_merge_checkpoint(
            merge_id,
            list(checkpoint.get("group_file_ids", [])),
            lake_table=lk, partition_key=draft.partition_key,
            group_file_ids=list(checkpoint.get("group_file_ids", [])),
            product_file_ids=product_ids,
            draft_manifest_id=draft_id,
            input_manifest_id=checkpoint.get("input_manifest_id"),
            source_rows=checkpoint.get("source_rows"),
            phase="confirmed", confirmed_at=time.time(),
            manifest_id=confirmed_manifest.manifest_id if confirmed_manifest else None,
            snapshot_id=snapshot.snapshot_id if snapshot else None,
        )
        return {
            "merge_id": merge_id,
            "phase": "confirmed",
            "swapped": True,
            "receipt": receipt,
            "manifest": self._manifest_snapshot(confirmed_manifest, lk),
            "snapshot_id": snapshot.snapshot_id if snapshot else None,
        }

    def reclaim_merge_receipt(self, merge_id: str, lake_table: str | None = None,
                              source_file_ids: list[str] | None = None) -> dict[str, Any] | None:
        """补领回执：只返回首次确认的替换结果，不重新执行归并。"""
        receipt = lake_store.get_merge_receipt(merge_id)
        if receipt is None and lake_table:
            # 回执丢失但检查点已确认：按首次确认的清单重建同一份回执
            checkpoint = lake_store.get_merge_checkpoint(merge_id)
            if checkpoint is not None and checkpoint.get("phase") == "confirmed":
                self.confirm_batch_merge(merge_id, lake_table)
                receipt = lake_store.get_merge_receipt(merge_id)
        if receipt is None:
            return None
        return self._reclaim_result(receipt, source_file_ids or [])

    def _reclaim_result(self, receipt: dict[str, Any], source_file_ids: list[str]) -> dict[str, Any]:
        requested = list(dict.fromkeys(source_file_ids))
        first = list(receipt.get("source_file_ids", []))
        return {
            "merge_id": receipt.get("merge_id"),
            "phase": "confirmed",
            "reclaimed": True,
            "first_confirmed": True,
            "requested_source_file_ids": requested,
            "request_matches_first_confirmation": (
                not requested or set(requested) == set(first)
            ),
            "receipt": receipt,
            "product_file_ids": list(receipt.get("product_file_ids", [])),
            "product_rows": receipt.get("product_rows"),
        }

    def merge_detail(self, merge_id: str, lake_table: str | None = None) -> dict[str, Any] | None:
        """归并详情：中断前后关联的清单、文件与首次确认的回执。"""
        checkpoint = lake_store.get_merge_checkpoint(merge_id)
        receipt = lake_store.get_merge_receipt(merge_id)
        if checkpoint is None and receipt is None:
            return None
        lk = lake_table or (checkpoint or {}).get("lake_table") or (receipt or {}).get("lake_table")
        manifests = []
        if lk:
            for manifest in lake_store.list_manifests(lk):
                if manifest.source_ref == merge_id:
                    manifests.append(self._manifest_snapshot(manifest, lk))
            if checkpoint and checkpoint.get("input_manifest_id"):
                input_manifest = lake_store.get_manifest(lk, checkpoint["input_manifest_id"])
                if input_manifest is not None:
                    manifests.insert(0, self._manifest_snapshot(input_manifest, lk))
        group_ids = list((checkpoint or {}).get("group_file_ids", []))
        product_ids = list((checkpoint or {}).get("product_file_ids", []))
        if receipt is not None:
            group_ids = group_ids or list(receipt.get("source_file_ids", []))
            product_ids = product_ids or list(receipt.get("product_file_ids", []))
        return {
            "merge_id": merge_id,
            "lake_table": lk,
            "phase": (checkpoint or {}).get("phase") or ("confirmed" if receipt else "unknown"),
            "source_file_ids": group_ids,
            "product_file_ids": product_ids,
            "manifests": manifests,
            "receipt": receipt,
            "checkpoint": checkpoint,
        }

    # ---- 兼容旧接口：原子归并 ----

    def execute_merge_atomic(self, lake_table: str, partition_key: str,
                             small_file_ids: list[str],
                             merge_id: str) -> dict[str, Any]:
        """原子执行归并：产物先 staged，换版成功后原件 retire，历史快照仍可读。"""
        staged = self.stage_batch_merge(merge_id, lake_table, partition_key, small_file_ids)
        if "error" in staged:
            return staged
        product_files = [lake_store.get_file(lake_table, fid)
                         for fid in staged.get("product_file_ids", [])]
        product_files = [pf for pf in product_files if pf is not None]
        total_rows = sum(f.file_rows for f in product_files)
        total_bytes = sum(f.file_bytes for f in product_files)
        confirmed = self.confirm_batch_merge(merge_id, lake_table)
        return {
            "merge_id": merge_id,
            "merged_file_count": len(small_file_ids),
            "result_file": product_files[0].file_id if product_files else None,
            "result_rows": total_rows,
            "result_bytes": total_bytes,
            "receipt": (confirmed or {}).get("receipt"),
        }

    def recover_from_checkpoint(self, merge_id: str, lake_table: str) -> dict[str, Any] | None:
        """从检查点恢复：清理部分生成的文件，重做归并。"""
        checkpoint = lake_store.get_merge_checkpoint(merge_id)
        if checkpoint is None:
            return None
        detail = self.recover_batch_merge(merge_id, lake_table)
        if detail is None:
            return None
        result = {
            "merge_id": merge_id,
            "recover_from_partial": len(checkpoint.get("partial_file_ids", [])),
            "recovered": True,
        }
        for key in ("phase", "product_file_ids", "draft_manifest_id", "manifests"):
            if key in detail:
                result[key] = detail[key]
        return result
