"""交接接口测试：直接驱动 FastAPI 应用，覆盖归并详情、回执补领与压实接单。"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore
from app.main import app
from tests.asgi_client import AsgiClient
from tests.test_merge_compact_handoff import lake, services  # noqa: F401  复用隔离存储夹具


PARTITION = "20261101"


@pytest.fixture
def http(lake):  # noqa: F811  依赖隔离存储夹具
    """以 ASGI 直连方式调用应用，无需额外依赖；服务与路由共用同一份存储。"""
    client = AsgiClient(app)
    return lambda method, path, body=None: client.request(method, path, body)


def _seed(lake_store: LakeStore) -> tuple[str, str, str]:
    f1 = lake_store.register_file("tbl", PARTITION, rows=3, bytes_=900)
    f2 = lake_store.register_file("tbl", PARTITION, rows=4, bytes_=1100)
    base = lake_store.ensure_confirmed_manifest("tbl", PARTITION)
    snap = lake_store.open_snapshot_for_manifest("tbl", base)
    return f1.file_id, f2.file_id, snap.snapshot_id


class TestHandoffApi:
    def test_scan_and_ticket_respect_manifest(self, lake, http):
        group = _seed(lake)
        stage = http("POST", "/api/handoff/merge/m1/stage", {"values": {
            "所属湖表": "tbl", "目标分区": PARTITION,
            "source_file_ids": [group[0], group[1]],
        }})
        assert stage.status_code == 200
        assert stage.json()["phase"] == "staged"

        scan = http("GET", f"/api/handoff/compact/scan?lake_table=tbl&partition_key={PARTITION}")
        assert scan.status_code == 200
        body = scan.json()
        assert body["file_count"] == 2
        assert body["rows"] == 7
        assert len(body["skipped_staged"]) == 1

        ticket = http("POST", "/api/handoff/compact/tickets", {"values": {
            "所属湖表": "tbl", "目标分区": PARTITION,
        }})
        assert ticket.status_code == 200
        ticket_id = ticket.json()["ticket"]["ticket_id"]
        assert ticket.json()["ticket"]["input_rows"] == 7

        # 恢复确认换版后，旧工单交回重排
        recover = http("POST", "/api/handoff/merge/m1/recover", {"values": {
            "所属湖表": "tbl", "confirm_swap": True,
        }})
        assert recover.status_code == 200
        assert recover.json()["phase"] == "confirmed"

        stale = http("POST", f"/api/handoff/compact/tickets/{ticket_id}/execute")
        assert stale.status_code == 200
        assert stale.json()["disposition"] == "return_for_reschedule"
        assert stale.json()["processed_rows"] is None

    def test_merge_receipt_reclaim_returns_first_confirmation(self, lake, http):
        group = _seed(lake)
        http("POST", "/api/handoff/merge/m1/stage", {"values": {
            "所属湖表": "tbl", "目标分区": PARTITION,
            "source_file_ids": [group[0], group[1]],
        }})
        confirm = http("POST", "/api/handoff/merge/m1/confirm", {"values": {"所属湖表": "tbl"}})
        assert confirm.status_code == 200
        first = confirm.json()["receipt"]

        reclaim = http("GET", "/api/handoff/merge/m1/receipt?lake_table=tbl")
        assert reclaim.status_code == 200
        payload = reclaim.json()
        assert payload["first_confirmed"] is True
        assert payload["product_rows"] == 7
        assert payload["receipt"]["product_file_ids"] == first["product_file_ids"]
        assert payload["receipt"]["issued_at"] == first["issued_at"]

    def test_compact_receipt_reports_reconciliation(self, lake, http):
        group = _seed(lake)
        http("POST", "/api/handoff/merge/m1/stage", {"values": {
            "所属湖表": "tbl", "目标分区": PARTITION,
            "source_file_ids": [group[0], group[1]],
        }})
        http("POST", "/api/handoff/merge/m1/confirm", {"values": {"所属湖表": "tbl"}})
        ticket = http("POST", "/api/handoff/compact/tickets", {"values": {
            "所属湖表": "tbl", "目标分区": PARTITION,
        }}).json()["ticket"]
        receipt = http("POST", f"/api/handoff/compact/tickets/{ticket['ticket_id']}/execute")
        assert receipt.status_code == 200
        body = receipt.json()
        assert body["disposition"] == "completed"
        assert body["processed_rows"] == 7
        assert len(body["input_file_ids"]) == 1

        again = http("GET", f"/api/handoff/compact/tickets/{ticket['ticket_id']}/receipt")
        assert again.status_code == 200
        assert again.json()["manifest_reconciliation"]["aligned"] is True

    def test_unknown_merge_detail_404(self, http):
        assert http("GET", "/api/handoff/merge/none?lake_table=tbl").status_code == 404
