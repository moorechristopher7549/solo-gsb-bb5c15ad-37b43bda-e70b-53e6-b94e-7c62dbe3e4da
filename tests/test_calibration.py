import os
import tempfile

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["TRACE_DB_PATH"] = _tmp.name
os.environ["SEED_DEMO"] = "0"

from app import booking as booking_svc  # noqa: E402
from app.database import get_conn_ctx, init_db  # noqa: E402
from app.main import app  # noqa: E402

init_db()


def _chain(client, *, parent_until="2030-01-01T00:00:00",
           leaf_until="2027-01-01T00:00:00",
           leaf_from="2026-01-01T00:00:00",
           issued_leaf="2026-01-02T00:00:00",
           code="W"):
    """提交 基准 G -> 参考 R -> 工作仪器 W 的唯一有效链。

    默认令上级证书有效期长于起点证书，从而在起点证书到期后链本身仍成立，
    可专门验证“预约必须在起点证书到期前结束”的截止规则。证书 id 以仪器
    编码为后缀，保证同一测试里可提交多台仪器而不互相 upsert 覆盖。
    """
    cg, cr, cw = f"CG-{code}", f"CR-{code}", f"CW-{code}"
    instruments = [
        {"code": f"G-{code}", "name": f"国家基准-{code}",
         "is_national_standard": True},
        {"code": f"R-{code}", "name": f"参考标准器-{code}"},
        {"code": code, "name": "工作仪器"},
    ]
    certs = [
        {"id": cg, "instrument_code": f"G-{code}", "cert_number": f"{code}-G1",
         "issued_at": "2024-01-01T00:00:00",
         "valid_from": "2024-01-01T00:00:00", "valid_until": "2031-01-01T00:00:00",
         "uncertainty": 0.0001, "unit": "mm"},
        {"id": cr, "instrument_code": f"R-{code}", "cert_number": f"{code}-R1",
         "issued_at": "2025-06-01T00:00:00",
         "valid_from": "2025-06-01T00:00:00", "valid_until": parent_until,
         "uncertainty": 0.001, "unit": "mm"},
        {"id": cw, "instrument_code": code, "cert_number": f"{code}-W1",
         "issued_at": issued_leaf,
         "valid_from": leaf_from, "valid_until": leaf_until,
         "uncertainty": 0.01, "unit": "mm"},
    ]
    edges = [
        {"child_cert_id": cw, "parent_cert_id": cr},
        {"child_cert_id": cr, "parent_cert_id": cg},
    ]
    r = client.post("/api/submissions",
                    json={"instruments": instruments, "certificates": certs, "edges": edges})
    assert r.status_code == 201, r.text


@pytest.fixture()
def client():
    with TestClient(app) as c:
        c.post("/api/admin/reset")
        yield c


# ---------------------------------------------------------------- GET slots

def test_slot_earliest_when_empty(client):
    _chain(client)
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T00:00:00Z",
        "end": "2026-06-21T00:00:00Z",
        "duration_seconds": 3600,
    })
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["traceability_available"] is True
    assert d["certificate_id"] == "CW-W"
    assert d["certificate_valid_until"] == "2027-01-01T00:00:00Z"
    # 工位空闲：最早时段即窗口起点
    assert d["slot"] == {
        "start": "2026-06-20T00:00:00Z",
        "end": "2026-06-20T01:00:00Z",
        "ends_by_certificate_valid_until": True,
    }
    assert d["busy"] == []
    assert d["reason_code"] is None
    # 附带完整溯源报告以便解释
    assert d["trace_report"]["resolution"] == "unique"


def test_slot_jumps_over_half_open_occupancy(client):
    _chain(client)
    # 占用 [09:00,10:00)
    assert client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 3600,
    }).status_code == 201
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T08:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    # [08,09) 只有 1 小时空档 -> 放得下，最早从 08:00 开始
    assert d["slot"]["start"] == "2026-06-20T08:00:00Z"
    assert d["slot"]["end"] == "2026-06-20T09:00:00Z"
    assert len(d["busy"]) == 1
    # 半开：[08,09) 与占用 [09,10) 不冲突（ends 恰在 starts）
    assert d["busy"][0]["start_at"] == "2026-06-20T09:00:00Z"


def test_slot_back_to_back_boundary_not_overlap(client):
    _chain(client)
    # 占满 [09,11)，从 09:00 查询 1 小时 -> 最早 11:00（可相接）
    assert client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 7200,
    }).status_code == 201
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    assert d["slot"]["start"] == "2026-06-20T11:00:00Z"
    assert d["slot"]["end"] == "2026-06-20T12:00:00Z"


def test_slot_no_free_returns_reason(client):
    _chain(client)
    # 占住 [08:30, 11:30)，窗口 09..12 求 2 小时 -> 剩余空档不足
    assert client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T08:30:00Z", "duration_seconds": 3 * 3600,
    }).status_code == 201
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 7200,
    })
    d = r.json()
    assert d["slot"] is None
    assert d["reason_code"] == "NO_FREE_SLOT_IN_WINDOW"
    assert "单个校准工位" in d["reason"]


def test_slot_search_window_not_pre_clipped_by_certificate_deadline(client):
    """搜索窗不再按查询起点证书截止预裁剪：搜索右端即请求窗终点，
    截止规则改在候选起点逐点重验（本用例起点即放得下）。"""
    _chain(client, leaf_until="2026-06-20T12:00:00Z")
    # 查询窗远超起点证书到期时刻
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T00:00:00Z",
        "end": "2027-06-20T00:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    assert d["search_window_end"] == "2027-06-20T00:00:00Z"
    assert d["slot"]["end"] == "2026-06-20T01:00:00Z"
    assert d["slot"]["ends_by_certificate_valid_until"] is True


def test_slot_deadline_too_early(client):
    _chain(client, leaf_until="2026-06-20T10:30:00Z")
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T10:00:00Z",
        "end": "2026-06-21T00:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    assert d["slot"] is None
    assert d["reason_code"] == "CERTIFICATE_DEADLINE_TOO_EARLY"
    assert "到期" in d["reason"]
    # 搜索窗不再预裁剪，但证书/截止/溯源说明对应最早阻断候选（=查询起点）
    assert d["search_window_end"] == "2026-06-21T00:00:00Z"
    assert d["traceability_at"] == "2026-06-20T10:00:00Z"
    assert d["certificate_id"] == "CW-W"
    assert d["certificate_valid_until"] == "2026-06-20T10:30:00Z"


def test_slot_deadline_clips_otherwise_fitting_window(client):
    """时长本在原窗内放得下，但候选起点证书截止前放不下 -> 归因为截止过早，
    而不是窗口过短或工位占用。"""
    _chain(client, leaf_until="2026-06-20T10:30:00Z")
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T10:00:00Z",
        "end": "2026-06-21T00:00:00Z",   # 原窗 14 小时，足够 2 小时
        "duration_seconds": 7200,
    })
    d = r.json()
    assert d["slot"] is None
    assert d["reason_code"] == "CERTIFICATE_DEADLINE_TOO_EARLY"
    assert d["search_window_end"] == "2026-06-21T00:00:00Z"
    assert d["certificate_valid_until"] == "2026-06-20T10:30:00Z"


def test_slot_exact_fit_to_deadline(client):
    _chain(client, leaf_until="2026-06-20T12:00:00Z")
    # [11:00,12:00) 恰在到期瞬间结束：半开，允许
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T11:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    assert d["slot"] == {
        "start": "2026-06-20T11:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "ends_by_certificate_valid_until": True,
    }


def test_slot_chain_unavailable_explains_reason(client):
    # 只有仪器与一张失效起点证书：无有效证书 -> 链不可开始
    client.post("/api/submissions", json={
        "instruments": [{"code": "W", "name": "工作仪器"}],
        "certificates": [
            {"id": "CW", "instrument_code": "W", "cert_number": "W-1",
             "issued_at": "2025-01-01T00:00:00",
             "valid_from": "2025-01-01T00:00:00",
             "valid_until": "2026-01-01T00:00:00",
             "uncertainty": 0.01, "unit": "mm"},
        ],
        "edges": [],
    })
    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T00:00:00Z",
        "end": "2026-06-21T00:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    assert d["traceability_available"] is False
    assert d["slot"] is None
    assert d["reason_code"] == "TRACEABILITY_CHAIN_UNAVAILABLE"
    assert d["chain_reason_code"] == "NO_ACTIVE_CERTIFICATE"
    assert "没有处于有效区间的校准证书" in d["reason"]
    assert d["trace_report"]["resolution"] == "broken"


def test_slot_ambiguous_chain_explains(client):
    # 同一起点挂两条都到基准的父链 -> 多候选
    client.post("/api/submissions", json={
        "instruments": [
            {"code": "G", "name": "基准", "is_national_standard": True},
            {"code": "RA", "name": "参考A"},
            {"code": "RB", "name": "参考B"},
            {"code": "W", "name": "工作仪器"},
        ],
        "certificates": [
            {"id": "CG", "instrument_code": "G", "cert_number": "G",
             "issued_at": "2024-01-01T00:00:00",
             "valid_from": "2024-01-01T00:00:00", "valid_until": "2031-01-01T00:00:00",
             "uncertainty": 0.0001, "unit": "mm"},
            {"id": "CRA", "instrument_code": "RA", "cert_number": "RA",
             "issued_at": "2025-01-01T00:00:00",
             "valid_from": "2025-01-01T00:00:00", "valid_until": "2030-01-01T00:00:00",
             "uncertainty": 0.001, "unit": "mm"},
            {"id": "CRB", "instrument_code": "RB", "cert_number": "RB",
             "issued_at": "2025-01-01T00:00:00",
             "valid_from": "2025-01-01T00:00:00", "valid_until": "2030-01-01T00:00:00",
             "uncertainty": 0.001, "unit": "mm"},
            {"id": "CW", "instrument_code": "W", "cert_number": "W",
             "issued_at": "2026-01-02T00:00:00",
             "valid_from": "2026-01-01T00:00:00", "valid_until": "2027-01-01T00:00:00",
             "uncertainty": 0.01, "unit": "mm"},
        ],
        "edges": [
            {"child_cert_id": "CW", "parent_cert_id": "CRA"},
            {"child_cert_id": "CW", "parent_cert_id": "CRB"},
            {"child_cert_id": "CRA", "parent_cert_id": "CG"},
            {"child_cert_id": "CRB", "parent_cert_id": "CG"},
        ],
    })
    r = client.get("/api/calibration-slots", params={
        "instrument": "W", "start": "2026-06-20T00:00:00Z",
        "end": "2026-06-21T00:00:00Z", "duration_seconds": 600,
    })
    d = r.json()
    assert d["traceability_available"] is False
    assert d["chain_reason_code"] == "MULTIPLE_VALID_CHAINS"
    assert d["trace_report"]["resolution"] == "ambiguous"


def test_slot_unknown_instrument_404(client):
    r = client.get("/api/calibration-slots", params={
        "instrument": "GHOST",
        "start": "2026-06-20T00:00:00Z", "end": "2026-06-21T00:00:00Z",
        "duration_seconds": 60,
    })
    assert r.status_code == 404


@pytest.mark.parametrize("params", [
    {"start": "2026-06-21T00:00:00Z", "end": "2026-06-20T00:00:00Z",
     "duration_seconds": 60},                       # 起晚于止
    {"start": "2026-06-20T00:00:00Z", "end": "2026-06-20T01:00:00Z",
     "duration_seconds": 7200},                     # 时长超过窗长
    {"start": "2026-06-20T00:00:00Z", "end": "2026-06-20T00:00:00Z",
     "duration_seconds": 60},                       # 空窗（start==end）
])
def test_slot_invalid_window_422(client, params):
    _chain(client)
    r = client.get("/api/calibration-slots",
                   params={"instrument": "W", **params})
    assert r.status_code == 422


def test_slot_nonpositive_duration_422(client):
    _chain(client)
    r = client.get("/api/calibration-slots", params={
        "instrument": "W", "start": "2026-06-20T00:00:00Z",
        "end": "2026-06-21T00:00:00Z", "duration_seconds": 0,
    })
    assert r.status_code == 422


def test_slot_station_shared_across_instruments(client):
    """单工位：另一台仪器的占用同样阻塞本仪器（占用不分仪器）。"""
    _chain(client, code="W")
    _chain(client, code="W2")
    assert client.post("/api/calibration-bookings", json={
        "instrument_code": "W2",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 3600,
    }).status_code == 201
    r = client.get("/api/calibration-slots", params={
        "instrument": "W", "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z", "duration_seconds": 3600,
    })
    d = r.json()
    # [09,10) 被另一台仪器占用 -> 最早 10:00
    assert d["slot"]["start"] == "2026-06-20T10:00:00Z"
    assert d["busy"][0]["instrument_code"] == "W2"


# ---------------------------------------------------- 候选起点逐点重验

def _submit_topology(client, leaf_certs, edges, *, extra_instruments=(),
                     extra_certs=()):
    """提交 基准 G / 参考 R / 工作仪器 W 的自定义证书与边拓扑。"""
    instruments = [
        {"code": "G", "name": "基准", "is_national_standard": True},
        {"code": "R", "name": "参考器"},
        {"code": "W", "name": "工作仪器"},
        *extra_instruments,
    ]
    certs = [
        {"id": "CG", "instrument_code": "G", "cert_number": "G1",
         "issued_at": "2024-01-01T00:00:00",
         "valid_from": "2024-01-01T00:00:00", "valid_until": "2031-01-01T00:00:00",
         "uncertainty": 0.0001, "unit": "mm"},
        {"id": "CR", "instrument_code": "R", "cert_number": "R1",
         "issued_at": "2025-06-01T00:00:00",
         "valid_from": "2025-06-01T00:00:00", "valid_until": "2030-01-01T00:00:00",
         "uncertainty": 0.001, "unit": "mm"},
        *leaf_certs,
        *extra_certs,
    ]
    r = client.post("/api/submissions",
                    json={"instruments": instruments, "certificates": certs,
                          "edges": edges})
    assert r.status_code == 201, r.text


def _leaf(cid, valid_from, valid_until, *, issued_at="2026-01-02T00:00:00",
          instrument="W"):
    return {"id": cid, "instrument_code": instrument, "cert_number": cid,
            "issued_at": issued_at,
            "valid_from": valid_from, "valid_until": valid_until,
            "uncertainty": 0.01, "unit": "mm"}


def _seed_busy(start, end, *, instrument="W", cert="CW1"):
    """绕过写事务门禁直接播种一条占用（历史预约可能在旧证书下确认）。"""
    from datetime import datetime as _dt
    with get_conn_ctx() as conn:
        booking_svc.insert_booking(
            conn,
            instrument_code=instrument,
            start_at=start,
            end_at=end,
            duration_seconds=int(
                (_dt.strptime(end, "%Y-%m-%dT%H:%M:%SZ")
                 - _dt.strptime(start, "%Y-%m-%dT%H:%M:%SZ")).total_seconds()
            ),
            certificate_id=cert,
            certificate_valid_until="2027-01-01T00:00:00Z",
        )


def test_slot_revalidates_chain_after_occupancy_pushes_past_renewal(client):
    """占用把建议起点推过证书闭区间续期交接点后，候选起点须重验链与
    起点证书截止，响应证据对应新起点（修复前会沿用查询起点结论并因
    旧证书截止错误地拒绝/裁剪）。"""
    _submit_topology(
        client,
        [
            _leaf("CW1", "2026-01-01T00:00:00Z", "2026-06-20T10:00:00Z"),
            _leaf("CW2", "2026-06-20T10:00:00Z", "2027-06-01T00:00:00Z",
                  issued_at="2026-06-01T00:00:00"),
        ],
        [
            {"child_cert_id": "CW1", "parent_cert_id": "CR"},
            {"child_cert_id": "CW2", "parent_cert_id": "CR"},
            {"child_cert_id": "CR", "parent_cert_id": "CG"},
        ],
    )
    # 占用 [09:00,10:30) 把候选推到 10:30（落在新证书 CW2 有效期内）
    _seed_busy("2026-06-20T09:00:00Z", "2026-06-20T10:30:00Z", cert="CW1")

    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 1800,
    })
    d = r.json()
    assert d["slot"] == {
        "start": "2026-06-20T10:30:00Z",
        "end": "2026-06-20T11:00:00Z",
        "ends_by_certificate_valid_until": True,
    }
    # 证书/截止/溯源说明均对应实际建议起点（新证书 CW2），不是查询起点 CW1
    assert d["traceability_at"] == "2026-06-20T10:30:00Z"
    assert d["certificate_id"] == "CW2"
    assert d["certificate_valid_until"] == "2027-06-01T00:00:00Z"
    assert d["trace_report"]["at"] == "2026-06-20T10:30:00Z"
    assert d["trace_report"]["starts"] == ["CW2"]
    assert d["search_window_end"] == "2026-06-20T12:00:00Z"
    # 建议时段经 POST 写事务重查同样可确认
    rr = client.post("/api/calibration-bookings", json={
        "instrument_code": "W", "start": "2026-06-20T10:30:00Z",
        "duration_seconds": 1800,
    })
    assert rr.status_code == 201, rr.text
    assert rr.json()["certificate_id"] == "CW2"


def test_slot_closed_interval_handover_point_is_revalidated(client):
    """闭区间首尾相接：交接点本身两证同时有效（起点不唯一），该点候选
    必须重验并跳过，最早可确认时段是交接点之后（10:00:01）。"""
    _submit_topology(
        client,
        [
            _leaf("CW1", "2026-01-01T00:00:00Z", "2026-06-20T10:00:00Z"),
            _leaf("CW2", "2026-06-20T10:00:00Z", "2027-06-01T00:00:00Z",
                  issued_at="2026-06-01T00:00:00"),
        ],
        [
            {"child_cert_id": "CW1", "parent_cert_id": "CR"},
            {"child_cert_id": "CW2", "parent_cert_id": "CR"},
            {"child_cert_id": "CR", "parent_cert_id": "CG"},
        ],
    )
    # 占用恰在交接点 10:00 结束（半开相接）
    _seed_busy("2026-06-20T09:00:00Z", "2026-06-20T10:00:00Z", cert="CW1")

    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 1800,
    })
    d = r.json()
    assert d["slot"]["start"] == "2026-06-20T10:00:01Z"
    assert d["certificate_id"] == "CW2"


def test_slot_edge_expiry_pushed_after_occupancy_reports_unconfirmable(client):
    """溯源边先于起点证书失效：占用把候选推过边有效期后，链在候选起点
    断裂，须沿边界继续搜索；全窗无可确认时段时返回空建议与具体原因，
    证据对应最早阻断候选而非查询起点。"""
    _submit_topology(
        client,
        [_leaf("CW1", "2026-01-01T00:00:00Z", "2026-12-31T00:00:00Z")],
        [
            {"child_cert_id": "CW1", "parent_cert_id": "CR",
             "valid_from": "2026-01-01T00:00:00Z",
             "valid_until": "2026-06-20T10:00:00Z"},
            {"child_cert_id": "CR", "parent_cert_id": "CG"},
        ],
    )
    # 占用把候选推到 10:30：CW1 证书仍有效，但 CW1->CR 边已于 10:00 失效
    _seed_busy("2026-06-20T09:00:00Z", "2026-06-20T10:30:00Z")

    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 1800,
    })
    d = r.json()
    assert d["slot"] is None
    assert d["reason_code"] == "TRACEABILITY_CHAIN_UNAVAILABLE"
    assert d["chain_reason_code"] == "CHAIN_BROKEN"
    assert d["traceability_available"] is False
    assert d["certificate_id"] is None
    # 最早阻断候选 = 占用结束点 10:30，而非查询起点 09:00
    assert d["traceability_at"] == "2026-06-20T10:30:00Z"
    assert d["trace_report"]["at"] == "2026-06-20T10:30:00Z"
    assert "10:30:00" in d["reason"]


def test_slot_deadline_at_pushed_candidate_gives_specific_reason(client):
    """占用把候选推到 10:00，该处起点证书 10:30 到期、放不下 1 小时；
    其后窗内无续期证书 -> 空建议 + CERTIFICATE_DEADLINE_TOO_EARLY，
    证据对应候选起点。"""
    _submit_topology(
        client,
        [_leaf("CW1", "2026-01-01T00:00:00Z", "2026-06-20T10:30:00Z")],
        [
            {"child_cert_id": "CW1", "parent_cert_id": "CR"},
            {"child_cert_id": "CR", "parent_cert_id": "CG"},
        ],
    )
    _seed_busy("2026-06-20T09:00:00Z", "2026-06-20T10:00:00Z")

    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T11:00:00Z",
        "duration_seconds": 3600,
    })
    d = r.json()
    assert d["slot"] is None
    assert d["reason_code"] == "CERTIFICATE_DEADLINE_TOO_EARLY"
    assert d["traceability_at"] == "2026-06-20T10:00:00Z"
    assert d["certificate_id"] == "CW1"
    assert d["certificate_valid_until"] == "2026-06-20T10:30:00Z"
    assert "10:00:00" in d["reason"]


def test_slot_edge_handover_closed_point_rechecked_then_recovers(client):
    """溯源边在整秒点切换（两条边闭区间在交接点同时有效 -> 多候选），
    交接点候选跳过，其后新边唯一时恢复可确认。"""
    _submit_topology(
        client,
        [_leaf("CW", "2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z")],
        [
            {"child_cert_id": "CW", "parent_cert_id": "CR",
             "valid_until": "2026-06-20T10:00:00Z"},
            {"child_cert_id": "CR", "parent_cert_id": "CG"},
        ],
        extra_instruments=[{"code": "R2", "name": "参考B"}],
        extra_certs=[
            {"id": "CR2", "instrument_code": "R2", "cert_number": "R2",
             "issued_at": "2025-01-01T00:00:00",
             "valid_from": "2025-01-01T00:00:00", "valid_until": "2030-01-01T00:00:00",
             "uncertainty": 0.001, "unit": "mm"},
        ],
    )
    client.post("/api/edges", json={
        "child_cert_id": "CW", "parent_cert_id": "CR2",
        "valid_from": "2026-06-20T10:00:00Z",
    })
    assert client.post("/api/edges", json={
        "child_cert_id": "CR2", "parent_cert_id": "CG",
    }).status_code == 201
    _seed_busy("2026-06-20T09:00:00Z", "2026-06-20T10:00:00Z", cert="CW")

    r = client.get("/api/calibration-slots", params={
        "instrument": "W",
        "start": "2026-06-20T09:00:00Z",
        "end": "2026-06-20T12:00:00Z",
        "duration_seconds": 1800,
    })
    d = r.json()
    # 10:00 两条边都有效（多候选）-> 跳过；10:00:01 起仅经 CR2 唯一成链
    assert d["slot"]["start"] == "2026-06-20T10:00:01Z"
    assert d["certificate_id"] == "CW"


# ---------------------------------------------------------------- POST bookings

def test_booking_created_and_persisted(client):
    _chain(client)
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z",
        "duration_seconds": 5400,
        "note": "例行校准",
    })
    assert r.status_code == 201, r.text
    b = r.json()
    assert b["instrument_code"] == "W"
    assert b["start_at"] == "2026-06-20T09:00:00Z"
    assert b["end_at"] == "2026-06-20T10:30:00Z"
    assert b["duration_seconds"] == 5400
    assert b["certificate_id"] == "CW-W"
    assert b["certificate_valid_until"] == "2027-01-01T00:00:00Z"
    assert b["note"] == "例行校准"
    assert b["id"]

    # 持久化到 SQLite：另起请求/连接可见
    with get_conn_ctx() as conn:
        row = conn.execute(
            "SELECT * FROM calibration_bookings WHERE id=?", (b["id"],)
        ).fetchone()
        assert row is not None
        assert row["instrument_code"] == "W"


def test_booking_overlap_returns_409_with_clashes(client):
    _chain(client)
    assert client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 3600,
    }).status_code == 201
    # 另一台仪器在同一时间预约单工位 -> 占用冲突 409
    _chain(client, code="W2")
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W2",
        "start": "2026-06-20T09:30:00Z", "duration_seconds": 3600,
    })
    assert r.status_code == 409
    err = r.json()["detail"]
    assert err["code"] == "STATION_OCCUPIED"
    assert len(err["detail"]["conflicting_bookings"]) == 1
    assert err["detail"]["conflicting_bookings"][0]["start_at"] == \
        "2026-06-20T09:00:00Z"
    # 冲突预约未写入
    rl = client.get("/api/calibration-bookings")
    assert len(rl.json()) == 1


def test_booking_touching_ends_allowed(client):
    """半开：后一段 start 恰等于前一段 end，不冲突。"""
    _chain(client)
    assert client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 3600,
    }).status_code == 201
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T10:00:00Z", "duration_seconds": 1800,
    })
    assert r.status_code == 201, r.text
    assert r.json()["end_at"] == "2026-06-20T10:30:00Z"


def test_booking_ends_after_certificate_expiry_409(client):
    # 上级证书远晚于起点证书：链成立，但预约越过起点证书截止
    _chain(client, leaf_until="2027-01-01T00:00:00Z")
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-12-31T23:00:00Z",
        "duration_seconds": 7200,   # end 2027-01-01T01:00 > 截止 2027-01-01T00:00
    })
    assert r.status_code == 409
    err = r.json()["detail"]
    assert err["code"] == "BOOKING_ENDS_AFTER_CERT_EXPIRY"
    assert err["detail"]["certificate_valid_until"] == "2027-01-01T00:00:00Z"
    assert client.get("/api/calibration-bookings").json() == []


def test_booking_end_exactly_at_expiry_allowed(client):
    _chain(client, leaf_until="2027-01-01T00:00:00Z")
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-12-31T23:00:00Z",
        "duration_seconds": 3600,   # end == valid_until
    })
    assert r.status_code == 201, r.text
    assert r.json()["end_at"] == "2027-01-01T00:00:00Z"


def test_booking_chain_broken_409(client):
    client.post("/api/submissions", json={
        "instruments": [{"code": "W", "name": "工作仪器"}],
        "certificates": [], "edges": [],
    })
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 600,
    })
    assert r.status_code == 409
    err = r.json()["detail"]
    assert err["code"] == "TRACEABILITY_CHAIN_UNAVAILABLE"
    assert err["detail"]["reason_code"] == "NO_ACTIVE_CERTIFICATE"
    # detail 内附完整溯源报告说明原因
    assert err["detail"]["trace_report"]["resolution"] == "broken"


def test_booking_unknown_instrument_409(client):
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "GHOST",
        "start": "2026-06-20T09:00:00Z", "duration_seconds": 60,
    })
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "INSTRUMENT_NOT_FOUND"


def test_booking_bad_body_422(client):
    r = client.post("/api/calibration-bookings", json={
        "instrument_code": "W",
        "start": "2026-06-20T09:00:00Z",
        "duration_seconds": -5,
    })
    assert r.status_code == 422


def test_concurrent_overlapping_bookings_single_winner(client):
    """并发确认同一时段：写事务（BEGIN IMMEDIATE）串行化，恰一个成功，
    另一个在事务内重查到已提交的占用后返回 409（不会出现两条重叠预约）。
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    _chain(client)
    barrier = threading.Barrier(2)
    results = []

    def post(start):
        barrier.wait(timeout=5)
        with TestClient(app) as local:
            r = local.post("/api/calibration-bookings", json={
                "instrument_code": "W",
                "start": start, "duration_seconds": 5400,
            })
            return r.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        for code in pool.map(post, [
            "2026-06-20T09:00:00Z",   # [09:00,10:30)
            "2026-06-20T09:30:00Z",   # [09:30,11:00) 与前者重叠
        ]):
            results.append(code)

    assert sorted(results) == [201, 409]
    rows = client.get("/api/calibration-bookings").json()
    # 恰一条落库（写事务串行化，不会出现两条重叠预约）；赢家不确定，
    # 但必为两笔之一的起点。
    assert len(rows) == 1
    assert rows[0]["start_at"] in (
        "2026-06-20T09:00:00Z", "2026-06-20T09:30:00Z")
    assert rows[0]["duration_seconds"] == 5400


def test_booking_failed_recheck_rolls_back_entire_transaction(client):
    """占用重查失败时，同一写事务内此前的任何写入均随回滚消失。

    替换确认模块内的占用重查函数使其返回一条冲突占用，验证
    confirm 路径抛 BookingConflict 且没有部分写入落库。
    """
    from datetime import datetime

    from app import booking
    from app.database import get_conn_ctx

    _chain(client)

    def fake_clashes(conn, start_s, end_s):
        return [{
            "id": "BK-FAKE", "instrument_code": "OTHER",
            "start_at": "2026-06-20T09:00:00Z", "end_at": "2026-06-20T12:00:00Z",
            "duration_seconds": 10800, "certificate_id": "CW-OTHER",
            "certificate_valid_until": "2030-01-01T00:00:00Z",
            "note": "", "created_at": "2026-06-20T00:00:00Z",
        }]

    mp = pytest.MonkeyPatch()
    mp.setattr(booking, "find_clashes", fake_clashes)
    try:
        with pytest.raises(booking.BookingConflict) as excinfo:
            with get_conn_ctx() as conn:
                conn.execute("BEGIN IMMEDIATE")
                booking.confirm_booking(
                    conn, "W", datetime(2026, 6, 20, 10), 3600)
        assert excinfo.value.code == "STATION_OCCUPIED"
    finally:
        mp.undo()

    # 事务回滚：无任何预约残留
    assert client.get("/api/calibration-bookings").json() == []


# ---------------------------------------------------------------- GET bookings

def test_list_bookings_by_instrument(client):
    _chain(client, code="W")
    _chain(client, code="W2")
    for code, ts in (("W", "09:00:00"), ("W2", "11:00:00"), ("W", "13:00:00")):
        assert client.post("/api/calibration-bookings", json={
            "instrument_code": code,
            "start": f"2026-06-20T{ts}Z", "duration_seconds": 600,
        }).status_code == 201

    all_rows = client.get("/api/calibration-bookings").json()
    assert len(all_rows) == 3
    # 按起点升序
    assert [r["start_at"] for r in all_rows] == [
        "2026-06-20T09:00:00Z", "2026-06-20T11:00:00Z", "2026-06-20T13:00:00Z",
    ]
    w_rows = client.get("/api/calibration-bookings",
                        params={"instrument": "W"}).json()
    assert {r["instrument_code"] for r in w_rows} == {"W"}
    assert len(w_rows) == 2


def test_list_bookings_unknown_instrument_404(client):
    r = client.get("/api/calibration-bookings", params={"instrument": "GHOST"})
    assert r.status_code == 404
