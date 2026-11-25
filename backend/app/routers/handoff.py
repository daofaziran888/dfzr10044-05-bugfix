"""归并与压实交接接口：详情核对、回执补领、压实接单与回执查询。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import EntryPayload
from app.services.compact_job import CompactJobService
from app.services.small_file_merge import SmallFileMergeService

router = APIRouter(prefix="/api/handoff", tags=["归并与压实交接"])

merge_svc = SmallFileMergeService()
compact_svc = CompactJobService()


def _merge_or_404(merge_id: str, lake_table: str | None) -> dict[str, Any]:
    detail = merge_svc.merge_detail(merge_id, lake_table)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"归并任务 {merge_id} 不存在或未留检查点")
    return detail


@router.get("/merge/{merge_id}")
def merge_detail(merge_id: str, lake_table: str | None = Query(default=None)) -> dict[str, Any]:
    """归并详情：中断前后关联的清单、产物文件与首次确认的回执。"""
    return _merge_or_404(merge_id, lake_table)


@router.get("/merge/{merge_id}/receipt")
def merge_receipt(merge_id: str, lake_table: str | None = Query(default=None)) -> dict[str, Any]:
    """补领归并回执：同一归并号只返回首次确认的替换结果。"""
    detail = _merge_or_404(merge_id, lake_table)
    reclaimed = merge_svc.reclaim_merge_receipt(merge_id, lake_table or detail.get("lake_table"))
    if reclaimed is None:
        raise HTTPException(status_code=409, detail=f"归并 {merge_id} 尚未完成换版，无可补领回执")
    return reclaimed


@router.post("/merge/{merge_id}/stage")
def stage_merge(merge_id: str, payload: EntryPayload) -> dict[str, Any]:
    """整组提交原件做批量归并：产物 staged，不换版。"""
    values = payload.values
    lake_table = str(values.get("所属湖表") or "")
    partition_key = str(values.get("目标分区") or "")
    group = list(values.get("source_file_ids") or [])
    if not lake_table or not partition_key:
        raise HTTPException(status_code=400, detail="缺少所属湖表或目标分区")
    result = merge_svc.stage_batch_merge(merge_id, lake_table, partition_key, group)
    if "error" in result:
        raise HTTPException(status_code=409, detail=result)
    return result


@router.post("/merge/{merge_id}/recover")
def recover_merge(merge_id: str, payload: EntryPayload | None = None) -> dict[str, Any]:
    """中断恢复：默认只补登记产物与草稿清单；confirm_swap=true 时执行换版。"""
    values = (payload.values if payload else {}) or {}
    lake_table = values.get("所属湖表")
    confirm_swap = bool(values.get("confirm_swap"))
    result = merge_svc.recover_batch_merge(merge_id, lake_table, confirm_swap=confirm_swap)
    if result is None:
        raise HTTPException(status_code=404, detail=f"归并 {merge_id} 没有可恢复的检查点")
    return result


@router.post("/merge/{merge_id}/confirm")
def confirm_merge(merge_id: str, payload: EntryPayload | None = None) -> dict[str, Any]:
    """归并换版并签发（或补领）回执。"""
    values = (payload.values if payload else {}) or {}
    result = merge_svc.confirm_batch_merge(merge_id, values.get("所属湖表"))
    if result is None:
        raise HTTPException(status_code=404, detail=f"归并 {merge_id} 没有可确认的草稿清单")
    return result


@router.get("/compact/scan")
def compact_scan(lake_table: str, partition_key: str) -> dict[str, Any]:
    """压实扫描：只返回分区当前确认清单，列出被跳过的 staged 产物。"""
    return compact_svc.scan_partition(lake_table, partition_key)


@router.post("/compact/tickets")
def open_compact_ticket(payload: EntryPayload) -> dict[str, Any]:
    """压实入口接单：锁定一份确认清单。"""
    values = payload.values
    lake_table = str(values.get("所属湖表") or "")
    partition_key = str(values.get("目标分区") or "")
    trigger = str(values.get("触发条件") or "手动压实")
    if not lake_table or not partition_key:
        raise HTTPException(status_code=400, detail="缺少所属湖表或目标分区")
    opened = compact_svc.open_ticket(lake_table, partition_key, trigger=trigger)
    if "error" in opened:
        raise HTTPException(status_code=409, detail=opened)
    return opened


@router.post("/compact/tickets/{ticket_id}/execute")
def execute_compact_ticket(ticket_id: str) -> dict[str, Any]:
    """执行工单：返回回执。交回重排的工单不申报处理行数。"""
    receipt = compact_svc.execute_ticket(ticket_id)
    if "error" in receipt:
        raise HTTPException(status_code=404, detail=receipt["error"])
    return receipt


@router.get("/compact/tickets/{ticket_id}/receipt")
def compact_receipt(ticket_id: str) -> dict[str, Any]:
    """补领压实回执，并核对工单实际采用清单与当前清单。"""
    receipt = compact_svc.receipt_for_ticket(ticket_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"压实工单 {ticket_id} 不存在")
    return receipt
