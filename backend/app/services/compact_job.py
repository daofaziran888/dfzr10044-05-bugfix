"""压实作业领域服务：按已确认分区文件清单接单、执行与回执。

与归并的交接契约：
- 压实只接收一份已确认的分区文件清单（manifest），不直接扫文件系统。
- 扫描/接单时对清单取快照并与版本绑定；归并换版后旧工单必须能被辨认。
- 交回重排的工单不得申报处理行数；确认完成的工单必须列出实际采用的文件。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.lake_store import lake_store, PartitionManifest


class CompactJobService(BaseModuleService):
    MODULE = "compact_job"
    ENTITY = "压实作业"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["压实编号", "所属湖表", "触发条件", "目标分区", "读取文件数", "输出文件数", "占用资源额度", "压实耗时"]
    REQUIRED_FIELDS = ["压实编号", "所属湖表", "触发条件"]
    STATUS_ORDER = ["待触发", "压实中", "已完成", "压实中断"]
    ACTION_RULES = {"触发压实": "压实中", "确认完成": "已完成", "标记中断": "压实中断"}
    NEGATIVE_ACTIONS = []

    # ---- 触发评估 ----

    def evaluate_compact_trigger(self, lake_table: str) -> dict[str, Any]:
        """评估压实触发条件：基于当前分区可见文件的数量与字节数。"""
        files = lake_store.list_files(lake_table)
        total_files = len(files)
        total_bytes = sum(f.file_bytes for f in files)
        small_files = [f for f in files if f.file_bytes < 3000]
        triggers = {
            "文件过多": total_files > 20,
            "碎片文件超过阈值": len(small_files) > 5,
            "单分区文件密度高": any(
                sum(1 for ff in files if ff.partition_key == f.partition_key) > 8
                for f in files
            ),
        }
        return {
            "total_files": total_files,
            "total_bytes": total_bytes,
            "small_file_count": len(small_files),
            "trigger_conditions": triggers,
            "should_trigger": any(triggers.values()),
        }

    # ---- 接单：绑定一份已确认清单 ----

    def scan_partition(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        """压实扫描：只读取分区当前已确认的清单，绝不包含 staged 产物。"""
        manifest = lake_store.ensure_confirmed_manifest(lake_table, partition_key)
        if manifest is None:
            return {
                "lake_table": lake_table,
                "partition_key": partition_key,
                "manifest": None,
                "file_ids": [],
                "file_count": 0,
                "rows": 0,
                "skipped_staged": [f.file_id for f in lake_store.list_files(
                    lake_table, partition_key, include_staged=True) if f.status != "live"],
            }
        files = lake_store.read_manifest_files(lake_table, manifest)
        # 只读取清单内仍处于 live 的文件；清单里已 retire 的文件属于历史快照
        live_files = [f for f in files if f.status == "live"]
        staged = [f.file_id for f in lake_store.list_files(
            lake_table, partition_key, include_staged=True) if f.status == "staged"]
        return {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "manifest": self._manifest_snapshot(manifest, lake_table),
            "file_ids": [f.file_id for f in live_files],
            "file_count": len(live_files),
            "rows": sum(f.file_rows for f in live_files),
            "skipped_staged": staged,
        }

    def _manifest_snapshot(self, manifest: PartitionManifest, lake_table: str) -> dict[str, Any]:
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

    def open_ticket(self, lake_table: str, partition_key: str,
                    trigger: str = "手动压实") -> dict[str, Any]:
        """接单：扫描出唯一确认清单，生成压实工单并锁定清单版本。"""
        scan = self.scan_partition(lake_table, partition_key)
        if scan["manifest"] is None:
            return {"error": "no_confirmed_manifest", "lake_table": lake_table,
                    "partition_key": partition_key}
        if scan["file_count"] == 0:
            return {"error": "empty_manifest", "lake_table": lake_table,
                    "partition_key": partition_key, "manifest": scan["manifest"]}
        ticket_id = f"CT-{lake_table}-{partition_key}-{uuid.uuid4().hex[:6]}"
        ticket = lake_store.issue_compact_ticket({
            "ticket_id": ticket_id,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "trigger": trigger,
            "manifest_id": scan["manifest"]["manifest_id"],
            "manifest_version": scan["manifest"]["version"],
            "manifest_checksum": scan["manifest"]["checksum"],
            "input_file_ids": scan["file_ids"],
            "input_rows": scan["rows"],
            "skipped_staged": scan["skipped_staged"],
            "status": "open",
            "issued_at": time.time(),
        })
        return {"ticket": ticket, "manifest": scan["manifest"]}

    # ---- 回执判定 ----

    def classify_ticket(self, ticket: dict[str, Any]) -> dict[str, Any]:
        """判定工单是交回重排、仍可执行，还是已对归并产物完成压实。"""
        lake_table = ticket["lake_table"]
        partition_key = ticket["partition_key"]
        current = lake_store.get_confirmed_manifest(lake_table, partition_key)
        current_view = (
            self._manifest_snapshot(current, lake_table) if current is not None else None
        )
        if ticket.get("status") == "completed":
            # 已确认完成的工单先按完成识别，再列出它当时采用的文件
            return {"disposition": "already_completed", "current_manifest": current_view}
        if ticket.get("status") == "returned":
            # 已经交回重排的工单保持该结论，补领回执时不会翻转成完成
            return {
                "disposition": "return_for_reschedule",
                "reason": ticket.get("return_reason") or "manifest_swapped",
                "current_manifest": current_view,
            }
        if current is None:
            return {"disposition": "stale_manifest_missing"}
        current_ids = set(current.file_ids)
        input_ids = list(ticket["input_file_ids"])
        missing = [fid for fid in input_ids if fid not in current_ids]
        if missing:
            # 工单输入已被换版移出当前分区（例如归并换版后原件 retire）：
            # 不能再按旧工单读，必须交回按新清单重排
            return {
                "disposition": "return_for_reschedule",
                "reason": "manifest_swapped",
                "superseded_input_file_ids": missing,
                "current_manifest": current_view,
            }
        # 工单输入仍在当前清单里；扫描后新追加的文件要与压实产物并存
        extras = [fid for fid in current.file_ids if fid not in set(input_ids)]
        return {
            "disposition": "executable",
            "rebase_extra_file_ids": extras,
            "manifest_advanced": current.manifest_id != ticket["manifest_id"],
            "current_manifest": current_view,
        }

    # ---- 执行与回执 ----

    def execute_ticket(self, ticket_id: str) -> dict[str, Any]:
        """按工单执行压实；清单已换版时交回重排，不申报任何处理行数。"""
        ticket = lake_store.get_compact_ticket(ticket_id)
        if ticket is None:
            return {"error": "ticket_not_found", "ticket_id": ticket_id}
        verdict = self.classify_ticket(ticket)
        if verdict["disposition"] != "executable":
            if verdict["disposition"] != "already_completed":
                ticket["status"] = "returned"
                ticket["returned_at"] = time.time()
                ticket["return_reason"] = verdict.get("reason")
            return self._receipt(ticket, verdict, processed=False)

        start_time = time.time()
        lake_table = ticket["lake_table"]
        partition_key = ticket["partition_key"]
        input_ids = list(ticket["input_file_ids"])
        # 扫描之后新到的文件不在工单输入里，但要与压实产物一起留在当前分区
        extras = [fid for fid in verdict.get("rebase_extra_file_ids", [])
                  if fid not in set(input_ids)
                  and (lake_store.get_file(lake_table, fid) is not None)]
        files = [lake_store.get_file(lake_table, fid) for fid in input_ids]
        files = [f for f in files if f is not None]
        total_rows = sum(f.file_rows for f in files)
        product = lake_store.register_file(
            lake_table, partition_key,
            rows=total_rows, bytes_=max(1, sum(f.file_bytes for f in files) // 2),
            status="staged",
        )
        output_ids = [product.file_id, *extras]
        draft = lake_store.stage_manifest(
            lake_table, partition_key, output_ids,
            source="compact", source_ref=ticket_id,
        )
        lake_store.confirm_manifest(lake_table, draft.manifest_id)
        snapshot = lake_store.open_snapshot_for_manifest(lake_table, draft)
        elapsed = time.time() - start_time

        ticket.update({
            "status": "completed",
            "output_file_ids": output_ids,
            "output_rows": total_rows,
            "rebased_file_ids": extras,
            "output_manifest_id": draft.manifest_id,
            "output_manifest_version": draft.version,
            "snapshot_id": snapshot.snapshot_id,
            "executed_at": time.time(),
            "elapsed": elapsed,
        })
        return self._receipt(ticket, {"disposition": "completed"}, processed=True)

    def _receipt(self, ticket: dict[str, Any], verdict: dict[str, Any],
                 processed: bool) -> dict[str, Any]:
        """生成压实回执。

        交回重排：不申报处理行数（processed_rows 必须为 0 或 null），并给出原/现清单。
        确认完成：列出实际采用的文件（输入与输出）与清单版本。
        """
        disposition = verdict["disposition"]
        returned = disposition == "return_for_reschedule"
        completed = disposition in ("completed", "already_completed")
        # 完成工单的"实际采用清单"是产出清单；交回重排的则是它应对齐的当前清单
        adopted_manifest = verdict.get("current_manifest")
        if completed and ticket.get("output_manifest_id"):
            output_manifest = lake_store.get_manifest(
                ticket["lake_table"], ticket["output_manifest_id"])
            if output_manifest is not None:
                adopted_manifest = self._manifest_snapshot(output_manifest, ticket["lake_table"])
        receipt = {
            "ticket_id": ticket["ticket_id"],
            "lake_table": ticket["lake_table"],
            "partition_key": ticket["partition_key"],
            "status": ticket.get("status"),
            "disposition": disposition,
            "reason": verdict.get("reason"),
            "adopted_manifest": adopted_manifest,
            "ticket_manifest_id": ticket["manifest_id"],
            "ticket_manifest_version": ticket["manifest_version"],
            "returned_for_reschedule": returned,
            "completed": completed,
        }
        if returned:
            receipt.update({
                "processed_rows": None,
                "read_files": 0,
                "input_file_ids": [],
                "output_file_ids": [],
            })
        elif completed:
            receipt.update({
                "processed_rows": ticket.get("output_rows", 0),
                "read_files": len(ticket.get("input_file_ids", [])),
                "input_file_ids": list(ticket.get("input_file_ids", [])),
                "output_file_ids": list(ticket.get("output_file_ids", [])),
                "rebased_file_ids": list(ticket.get("rebased_file_ids", [])),
                "output_manifest_version": ticket.get("output_manifest_version"),
                "snapshot_id": ticket.get("snapshot_id"),
            })
        else:
            receipt.update({"processed_rows": None, "read_files": 0})
        return receipt

    def receipt_for_ticket(self, ticket_id: str) -> dict[str, Any] | None:
        """按工单补领回执，并把工单实际采用清单与当前清单并列核对。"""
        ticket = lake_store.get_compact_ticket(ticket_id)
        if ticket is None:
            return None
        verdict = self.classify_ticket(ticket)
        if verdict["disposition"] == "already_completed":
            receipt = self._receipt(ticket, verdict, processed=True)
        elif verdict["disposition"] == "completed":
            receipt = self._receipt(ticket, verdict, processed=True)
        else:
            receipt = self._receipt(ticket, verdict, processed=False)
        receipt["manifest_reconciliation"] = self.reconcile_manifests(ticket_id)
        return receipt

    def reconcile_manifests(self, ticket_id: str) -> dict[str, Any]:
        """核对压实工单与归并详情各自实际采用的清单。"""
        ticket = lake_store.get_compact_ticket(ticket_id)
        if ticket is None:
            return {"error": "ticket_not_found"}
        lake_table = ticket["lake_table"]
        partition_key = ticket["partition_key"]
        ticket_manifest = lake_store.get_manifest(lake_table, ticket["manifest_id"])
        current = lake_store.get_confirmed_manifest(lake_table, partition_key)
        manifests = [
            self._manifest_snapshot(m, lake_table)
            for m in lake_store.list_manifests(lake_table)
            if m.partition_key == partition_key
        ]
        completed = ticket.get("status") == "completed"
        output_manifest = (
            lake_store.get_manifest(lake_table, ticket["output_manifest_id"])
            if ticket.get("output_manifest_id") else None
        )
        # 完成的工单：实际产出清单就是它采用的清单；未完成的工单：采用接单时的输入清单
        effective = output_manifest if (completed and output_manifest is not None) else ticket_manifest
        effective_ids = (
            set(ticket.get("output_file_ids", [])) if completed and output_manifest is not None
            else set(ticket.get("input_file_ids", []))
        )
        current_ids = set(current.file_ids) if current is not None else set()
        aligned = (
            effective is not None
            and current is not None
            and effective.manifest_id == current.manifest_id
            and effective.checksum == current.checksum
            and effective_ids == current_ids
        )
        return {
            "ticket_id": ticket_id,
            "ticket_status": ticket.get("status"),
            "ticket_manifest": (
                self._manifest_snapshot(ticket_manifest, lake_table)
                if ticket_manifest is not None else None
            ),
            "ticket_output_manifest": (
                self._manifest_snapshot(output_manifest, lake_table)
                if output_manifest is not None else None
            ),
            "adopted_manifest": (
                self._manifest_snapshot(effective, lake_table)
                if effective is not None else None
            ),
            "current_manifest": (
                self._manifest_snapshot(current, lake_table) if current is not None else None
            ),
            "manifests_in_partition": manifests,
            "adopted_files_missing_from_current": sorted(effective_ids - current_ids),
            "current_files_not_adopted": sorted(current_ids - effective_ids),
            "aligned": aligned,
        }

    # ---- 兼容旧接口：按分区直接压实 ----

    def execute_compact(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        """执行压实：接单 → 按确认清单执行，返回回执摘要。"""
        opened = self.open_ticket(lake_table, partition_key)
        if "error" in opened:
            return {"error": opened["error"],
                    "lake_table": lake_table, "partition": partition_key,
                    "compact_success": False, "read_files": 0, "output_files": 0}
        receipt = self.execute_ticket(opened["ticket"]["ticket_id"])
        return {
            "lake_table": lake_table,
            "partition": partition_key,
            "read_files": receipt.get("read_files", 0),
            "output_files": len(receipt.get("output_file_ids", [])),
            "total_rows": receipt.get("processed_rows") or 0,
            "elapsed": 0.0,
            "compact_success": receipt.get("completed", False),
            "receipt": receipt,
        }
