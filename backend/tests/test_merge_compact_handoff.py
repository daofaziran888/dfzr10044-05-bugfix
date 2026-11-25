"""归并恢复与压实接单交接协议测试。

覆盖题面场景：
- 压实不得把归并器 staged 产物与原件一并读入（7 行而非 14 行）；
- 整组原件批量归并（3 行 + 4 行）、产物 7 行、换版前停机再唤压实；
- 同一归并号补领回执返回首次确认的替换结果；
- 压实扫描后追加 2 行新文件，原快照仍读 7 行，当前分区 9 行；
- 归并详情与压实回执各自实际采用的清单可核对；
- 恢复先换版而压实仍持旧工单：交回重排不申报行数，完成工单列出实际采用文件。
"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore, lake_store
from app.services.compact_job import CompactJobService
from app.services.small_file_merge import SmallFileMergeService
from app.services.time_travel import TimeTravelService


PARTITION = "20261101"


@pytest.fixture
def lake(monkeypatch) -> LakeStore:
    """每个用例一份干净的内存存储，替换全局单例，服务通过它读写。"""
    fresh = LakeStore()
    import app.lake_store as lake_store_module
    monkeypatch.setattr(lake_store_module, "lake_store", fresh)
    for service_module in (
        "app.services.small_file_merge",
        "app.services.compact_job",
        "app.services.time_travel",
        "app.services.snapshot_mgr",
    ):
        module = __import__(service_module, fromlist=["lake_store"])
        monkeypatch.setattr(module, "lake_store", fresh)
    return fresh


@pytest.fixture
def services():
    return SmallFileMergeService(), CompactJobService(), TimeTravelService()


def _seed_two_originals(lake_store: LakeStore) -> tuple[list[str], str, str]:
    """3 行 + 4 行两个原件，建立确认清单与基线快照。"""
    f1 = lake_store.register_file("tbl", PARTITION, rows=3, bytes_=900)
    f2 = lake_store.register_file("tbl", PARTITION, rows=4, bytes_=1100)
    baseline = lake_store.ensure_confirmed_manifest("tbl", PARTITION)
    snapshot = lake_store.open_snapshot_for_manifest("tbl", baseline)
    return [f1.file_id, f2.file_id], baseline.manifest_id, snapshot.snapshot_id


class TestMergeStagedDoesNotLeakIntoCompact:
    def test_staged_product_not_read_by_compact(self, lake, services):
        merge_svc, compact_svc, _ = services
        group, _, _ = _seed_two_originals(lake)

        staged = merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        assert staged["phase"] == "staged"
        assert staged["product_rows"] == 7

        scan = compact_svc.scan_partition("tbl", PARTITION)
        # 扫描只看到两个原件 7 行，产物被识别为 staged 并跳过
        assert scan["file_count"] == 2
        assert scan["rows"] == 7
        assert staged["product_file_ids"][0] in scan["skipped_staged"]
        assert staged["product_file_ids"][0] not in scan["file_ids"]

    def test_compact_after_interrupt_still_reads_seven_rows(self, lake, services):
        merge_svc, compact_svc, _ = services
        group, _, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)

        ticket = compact_svc.open_ticket("tbl", PARTITION)["ticket"]
        assert ticket["input_rows"] == 7
        receipt = compact_svc.execute_ticket(ticket["ticket_id"])
        assert receipt["disposition"] == "completed"
        assert receipt["processed_rows"] == 7
        assert receipt["read_files"] == 2


class TestBatchMergeRecoveryHandoff:
    def test_stage_then_confirm_receipt_rows(self, lake, services):
        merge_svc, _, _ = services
        group, _, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        confirmed = merge_svc.confirm_batch_merge("merge-1", "tbl")
        receipt = confirmed["receipt"]
        assert receipt["source_file_count"] == 2
        assert receipt["product_rows"] == 7
        assert receipt["manifest_version"] >= 2

    def test_resubmit_same_merge_returns_first_confirmation(self, lake, services):
        merge_svc, _, _ = services
        group, _, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        confirmed = merge_svc.confirm_batch_merge("merge-1", "tbl")
        first_ts = confirmed["receipt"]["issued_at"]

        # 同一归并号整组原件再次提交：不重跑归并，拿回首次确认的回执
        again = merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        assert again["reclaimed"] is True
        assert again["first_confirmed"] is True
        assert again["receipt"]["issued_at"] == first_ts
        assert again["request_matches_first_confirmation"] is True

        # 补领同样返回首次确认结果
        reclaimed = merge_svc.reclaim_merge_receipt("merge-1", "tbl", group)
        assert reclaimed["product_file_ids"] == confirmed["receipt"]["product_file_ids"]
        assert reclaimed["product_rows"] == 7

    def test_recover_reuses_checkpoint_without_new_products(self, lake, services):
        merge_svc, _, _ = services
        group, _, _ = _seed_two_originals(lake)
        staged = merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        recovered = merge_svc.recover_batch_merge("merge-1", "tbl")
        assert recovered["recovered"] is True
        assert recovered["phase"] == "staged"
        # 恢复不重复登记产物，当前分区仍只有原件
        assert recovered["product_file_ids"] == staged["product_file_ids"]
        assert len(lake.list_files("tbl", PARTITION)) == 2

    def test_merge_detail_lists_manifests_around_interrupt(self, lake, services):
        merge_svc, _, _ = services
        group, base_id, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        merge_svc.confirm_batch_merge("merge-1", "tbl")
        detail = merge_svc.merge_detail("merge-1", "tbl")
        versions = {m["version"]: m for m in detail["manifests"]}
        assert base_id in {m["manifest_id"] for m in detail["manifests"]}
        # 基线清单被换版取代，归并清单为 confirmed
        assert any(m["status"] == "confirmed" and m["source"] == "merge"
                   for m in detail["manifests"])
        assert versions  # 至少列出了版本
        assert detail["receipt"]["product_rows"] == 7


class TestLateFileAfterScan:
    def test_new_file_after_scan_survives_compact_and_old_snapshot_readable(
            self, lake, services):
        merge_svc, compact_svc, tt = services
        group, _, baseline_snap = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        merge_svc.confirm_batch_merge("merge-1", "tbl")

        ticket = compact_svc.open_ticket("tbl", PARTITION)["ticket"]
        assert ticket["input_rows"] == 7

        # 压实扫描后提交一个 2 行的新文件并换版
        late = lake.register_file("tbl", PARTITION, rows=2, bytes_=512, status="staged")
        lake.append_to_confirmed_manifest(
            "tbl", PARTITION, [late.file_id], source="append", source_ref="late-batch")

        receipt = compact_svc.execute_ticket(ticket["ticket_id"])
        assert receipt["disposition"] == "completed"
        assert receipt["processed_rows"] == 7
        # 压实的输出里保留了扫描之后到达的新文件
        assert late.file_id in receipt["rebased_file_ids"]
        assert late.file_id in receipt["output_file_ids"]

        # 当前分区：压实产物 + 新文件 = 9 行
        current_rows = sum(f.file_rows for f in lake.list_files("tbl", PARTITION))
        assert current_rows == 9

        # 原快照按编号回溯：两个原文件、7 行
        old = tt.read_at_snapshot("tbl", baseline_snap)
        assert old["success"]
        assert old["rows"] == 7
        assert old["partition_count"] == 1


class TestStaleTicketDisposition:
    def test_old_ticket_returned_for_reschedule_without_rows(self, lake, services):
        merge_svc, compact_svc, _ = services
        group, _, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        old_ticket = compact_svc.open_ticket("tbl", PARTITION)["ticket"]

        # 归并恢复先确认换版，压实仍持有旧工单
        merge_svc.recover_batch_merge("merge-1", "tbl", confirm_swap=True)

        receipt = compact_svc.execute_ticket(old_ticket["ticket_id"])
        assert receipt["disposition"] == "return_for_reschedule"
        assert receipt["returned_for_reschedule"] is True
        assert receipt["completed"] is False
        # 交回重排的工单不得申报处理行数
        assert receipt["processed_rows"] is None
        assert receipt["read_files"] == 0
        assert receipt["input_file_ids"] == []
        # 回执里给出它应当采用的当前清单
        assert receipt["adopted_manifest"]["file_count"] == 1
        assert receipt["adopted_manifest"]["rows"] == 7

        # 补领同一工单的回执仍是交回重排，不会翻成完成
        again = compact_svc.receipt_for_ticket(old_ticket["ticket_id"])
        assert again["disposition"] == "return_for_reschedule"
        assert again["processed_rows"] is None
        assert again["returned_for_reschedule"] is True

    def test_completed_ticket_lists_adopted_files(self, lake, services):
        merge_svc, compact_svc, _ = services
        group, _, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        merge_svc.confirm_batch_merge("merge-1", "tbl")

        ticket = compact_svc.open_ticket("tbl", PARTITION)["ticket"]
        receipt = compact_svc.execute_ticket(ticket["ticket_id"])
        assert receipt["completed"] is True
        assert receipt["disposition"] == "completed"
        assert receipt["processed_rows"] == 7
        # 完成工单列出实际采用的输入与输出文件
        assert len(receipt["input_file_ids"]) == 1
        assert receipt["input_file_ids"] == ticket["input_file_ids"]
        assert len(receipt["output_file_ids"]) == 1
        assert receipt["output_manifest_version"] >= 1

        # 完成工单再补领回执仍按已确认结果返回，不会变成交回重排
        again = compact_svc.receipt_for_ticket(ticket["ticket_id"])
        assert again["disposition"] == "already_completed"
        assert again["completed"] is True
        assert again["processed_rows"] == 7

    def test_ticket_and_merge_detail_manifests_reconcile(self, lake, services):
        merge_svc, compact_svc, _ = services
        group, _, _ = _seed_two_originals(lake)
        merge_svc.stage_batch_merge("merge-1", "tbl", PARTITION, group)
        merge_svc.confirm_batch_merge("merge-1", "tbl")
        ticket = compact_svc.open_ticket("tbl", PARTITION)["ticket"]
        compact_svc.execute_ticket(ticket["ticket_id"])

        reconciliation = compact_svc.reconcile_manifests(ticket["ticket_id"])
        assert reconciliation["aligned"] is True
        assert reconciliation["ticket_output_manifest"]["manifest_id"] == \
            reconciliation["current_manifest"]["manifest_id"]

        # 当工单与当前清单对不上时，核对结果明确列出差异
        extra = lake.register_file("tbl", PARTITION, rows=1, bytes_=256, status="staged")
        lake.append_to_confirmed_manifest("tbl", PARTITION, [extra.file_id])
        drifted = compact_svc.reconcile_manifests(ticket["ticket_id"])
        assert drifted["aligned"] is False
        assert extra.file_id in drifted["current_files_not_adopted"]
