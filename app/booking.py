"""校准工位预约：单工位半开区间占用与预约确认。

业务规则
--------
* 全实验室只有**单个校准工位**：所有仪器的预约共用同一份占用表，
  占用一律按半开区间 ``[start_at, end_at)`` 计算——相接但不重叠的两段
  （如 ``[09:00,10:00)`` 与 ``[10:00,11:00)``）不冲突，可首尾相接预约。
* 预约必须在其起点（GET 为候选时段起点、POST 为预约起点）该仪器
  **唯一有效溯源链的起点证书到期前结束**，即
  ``end_at <= 起点证书 valid_until``。链不唯一 / 断裂 / 校验失败时
  复用溯源引擎给出可解释原因，绝不任取一条链。
* ``GET`` 只读：查询起点链有效时，沿单工位半开占用（预约 + 生效维护
  窗口）贪心推进候选起点，并在每个候选起点**重验**唯一有效链与起点
  证书截止（不沿用查询起点结论）；候选沿证书与溯源边有效期边界
  （闭区间交接点本身也要重验）继续寻找其后最早可确认时段，全窗无
  合格时段时 ``slot=null`` 并给出具体原因。``POST`` 在同一写事务
  （``BEGIN IMMEDIATE``）重查占用与证书截止，冲突返回 409，随后
  原子写入 SQLite。
* 批量改期（``reschedule_bookings``）：一次写事务内核对 2～10 条已有
  预约存在且 ID 不重复，将本批预约的旧占用整体排除后，再按半开区间
  检查新时段彼此及与其他预约的冲突（允许交换时段与首尾相接）；每条
  新起点重新验证唯一有效溯源链，新结束不得晚于起点证书到期；任一失败
  整体回滚，全部通过才更新时段与证书快照（仪器与备注保持不变）。
  时段实际变化的预约，其未补位候补（waiting）在同一事务内标记失效，
  避免旧时段队列悬空。
* 维护封锁：生效中维护窗口（``station_maintenance_windows``，见
  ``maintenance`` 模块）与预约一样按半开区间占用单工位。空档查询把
  维护窗口与预约一并视作占用；确认预约、批量改期在各自写事务内重查
  活跃维护窗口（改期仅排除本批旧预约、不排除维护窗口），重叠返回 409，
  批量改期整体回滚。
* 候补（waitlist）：对指定已确认预约登记候补仪器与备注，候补沿用该
  预约登记时的起止时段，按登记先后（``seq`` 自增）排队。取消预约时
  在同一 ``BEGIN IMMEDIATE`` 写事务内释放占用，依队列顺序逐条重验
  仪器在时段起点的唯一有效溯源链、证书到期时刻及与剩余预约和生效中
  维护窗口的半开冲突；跳过不合格申请（撞维护封锁记
  ``STATION_UNDER_MAINTENANCE``），只将首个合格申请转为已确认预约并
  保存证书快照；预约行已删除，重复取消返回 404，不会重复补位。
* 有效性巡检（``inspect_bookings``）：只读接口。筛选**开始时刻**落在
  UTC 半开查询窗 ``[start, end)`` 内的已确认预约，逐条在其开始时刻
  重建唯一有效溯源链（复用与确认/改期相同的链路门禁），要求链校验
  通过且预约结束不晚于**当前**起点证书到期；再与预约保存的证书 id
  与到期快照比较——链失效或截止不足记 ``invalid``，链仍有效但证书
  id 或到期变化记 ``changed``，其余记 ``valid``；按（开始时刻,
  预约 id）稳定排序并汇总各状态数量，全程不写库。
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from .crud import ValidationError, get_instrument
from .engine import Catalog, relevant_trace_boundaries, unique_chain_gate
from .maintenance import overlapping_windows as _overlapping_maintenance
from .utils import fmt, to_utc_naive, utcnow_str

# 链不可用 / 无空档等原因码
REASON_CHAIN_UNAVAILABLE = "TRACEABILITY_CHAIN_UNAVAILABLE"
REASON_WINDOW_TOO_SHORT = "WINDOW_TOO_SHORT_FOR_DURATION"
REASON_NO_FREE_SLOT = "NO_FREE_SLOT_IN_WINDOW"
REASON_DEADLINE_TOO_EARLY = "CERTIFICATE_DEADLINE_TOO_EARLY"

# POST 写事务冲突原因码
CONFLICT_INSTRUMENT_NOT_FOUND = "INSTRUMENT_NOT_FOUND"
CONFLICT_CHAIN_UNAVAILABLE = "TRACEABILITY_CHAIN_UNAVAILABLE"
CONFLICT_ENDS_AFTER_CERT_EXPIRY = "BOOKING_ENDS_AFTER_CERT_EXPIRY"
CONFLICT_STATION_OCCUPIED = "STATION_OCCUPIED"
# 半开区间与生效中维护封锁窗口重叠（确认/改期/补位重查共用）
CONFLICT_STATION_UNDER_MAINTENANCE = "STATION_UNDER_MAINTENANCE"

# 批量改期新增的冲突原因码（其余复用单条确认的四条）
CONFLICT_DUPLICATE_BOOKING_ID = "DUPLICATE_BOOKING_ID"
CONFLICT_BOOKING_NOT_FOUND = "BOOKING_NOT_FOUND"

_BOOKING_FIELDS = (
    "id",
    "instrument_code",
    "start_at",
    "end_at",
    "duration_seconds",
    "certificate_id",
    "certificate_valid_until",
    "note",
    "created_at",
)

# 候补状态：waiting 候补中 | promoted 已补位 | failed 补位重验不合格 | expired 已失效
WAITLIST_WAITING = "waiting"
WAITLIST_PROMOTED = "promoted"
WAITLIST_FAILED = "failed"
WAITLIST_EXPIRED = "expired"
WAITLIST_STATUSES = (
    WAITLIST_WAITING,
    WAITLIST_PROMOTED,
    WAITLIST_FAILED,
    WAITLIST_EXPIRED,
)

_WAITLIST_FIELDS = (
    "id",
    "booking_id",
    "instrument_code",
    "start_at",
    "end_at",
    "duration_seconds",
    "note",
    "status",
    "status_reason",
    "promoted_booking_id",
    "created_at",
    "processed_at",
)


class BookingConflict(Exception):
    """POST 确认预约在写事务内重查失败（路由层映射为 HTTP 409）。"""

    def __init__(self, code: str, message: str, detail: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}


# ---------------------------------------------------------------- 工具

def _parse(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def _booking_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in _BOOKING_FIELDS}


def insert_booking(
    conn: sqlite3.Connection,
    *,
    instrument_code: str,
    start_at: str,
    end_at: str,
    duration_seconds: int,
    certificate_id: str,
    certificate_valid_until: str,
    note: str = "",
    booking_id: Optional[str] = None,
    created_at: Optional[str] = None,
) -> dict[str, Any]:
    """底层写入（供确认事务与演示播种复用），不做任何占用/溯源校验。"""
    if booking_id is None:
        booking_id = "BK-" + datetime.utcnow().strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:12]
    conn.execute(
        """
        INSERT INTO calibration_bookings(
            id, instrument_code, start_at, end_at, duration_seconds,
            certificate_id, certificate_valid_until, note, created_at)
        VALUES (:id, :instrument_code, :start_at, :end_at, :duration_seconds,
                :certificate_id, :certificate_valid_until, :note, :created_at)
        """,
        {
            "id": booking_id,
            "instrument_code": instrument_code,
            "start_at": start_at,
            "end_at": end_at,
            "duration_seconds": duration_seconds,
            "certificate_id": certificate_id,
            "certificate_valid_until": certificate_valid_until,
            "note": note,
            "created_at": created_at or utcnow_str(),
        },
    )
    row = conn.execute(
        "SELECT * FROM calibration_bookings WHERE id=?", (booking_id,)
    ).fetchone()
    return _booking_dict(row)


def list_bookings(
    conn: sqlite3.Connection, instrument_code: Optional[str] = None
) -> list[dict[str, Any]]:
    if instrument_code is not None:
        rows = conn.execute(
            "SELECT * FROM calibration_bookings WHERE instrument_code=? "
            "ORDER BY start_at, id",
            (instrument_code,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM calibration_bookings ORDER BY start_at, id"
        ).fetchall()
    return [_booking_dict(r) for r in rows]


def _overlapping_bookings(
    conn: sqlite3.Connection, lo_s: str, hi_s: str
) -> list[sqlite3.Row]:
    """与半开查询窗 ``[lo, hi)`` 相交的既有预约（单工位，不分仪器）。

    半开重叠判据 ``b.start < hi AND b.end > lo``：恰在 ``lo`` 结束或
    恰在 ``hi`` 开始的预约不占窗口。
    """
    return conn.execute(
        "SELECT * FROM calibration_bookings WHERE start_at < ? AND end_at > ? "
        "ORDER BY start_at, id",
        (hi_s, lo_s),
    ).fetchall()


def find_clashes(
    conn: sqlite3.Connection, start_s: str, end_s: str
) -> list[dict[str, Any]]:
    """确认事务内的占用重查：与半开区间 ``[start_s, end_s)`` 重叠的预约。"""
    rows = conn.execute(
        "SELECT * FROM calibration_bookings WHERE start_at < ? AND end_at > ? "
        "ORDER BY start_at, id",
        (end_s, start_s),
    ).fetchall()
    return [_booking_dict(r) for r in rows]


def _maintenance_clash_dicts(
    conn: sqlite3.Connection, start_s: str, end_s: str
) -> list[dict[str, Any]]:
    """占用重查：与半开区间 ``[start_s, end_s)`` 重叠的生效中维护窗口。"""
    rows = _overlapping_maintenance(conn, start_s, end_s)
    return [
        {
            "maintenance_window_id": r["id"],
            "start_at": r["start_at"],
            "end_at": r["end_at"],
            "reason": r["reason"],
        }
        for r in rows
    ]


def _raise_if_maintenance(
    conn: sqlite3.Connection, start_s: str, end_s: str, *, what: str
) -> None:
    """半开区间与生效中维护窗口重叠时抛 409（``what`` 描述被封锁对象）。"""
    windows = _maintenance_clash_dicts(conn, start_s, end_s)
    if windows:
        raise BookingConflict(
            CONFLICT_STATION_UNDER_MAINTENANCE,
            f"{what} {start_s} ~ {end_s}（半开区间）与单工位上 "
            f"{len(windows)} 个生效维护窗口重叠，工位维护封锁中",
            {"conflicting_maintenance_windows": windows},
        )


# ---------------------------------------------------------------- 最早空档

def _merge_intervals(
    intervals: list[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    """把可能重叠/相接的半开占用段合并为互不相交、按起点升序的并集段。"""
    if not intervals:
        return []
    ordered = sorted(intervals, key=lambda iv: (iv[0], iv[1]))
    merged: list[list[datetime]] = [list(ordered[0])]
    for s, e in ordered[1:]:
        if s <= merged[-1][1]:  # 半开重叠或相接：并入当前并集段
            if e > merged[-1][1]:
                merged[-1][1] = e
        else:
            merged.append([s, e])
    return [(s, e) for s, e in merged]


def _busy_end_at(
    busy: list[tuple[datetime, datetime]], cursor: datetime
) -> Optional[datetime]:
    """游标落在（或早于）哪个占用段内时，返回该并集段结束点；否则 None。"""
    for b_start, b_end in busy:
        if b_end <= cursor:
            continue
        if cursor < b_start:
            return None  # busy 已按起点升序，后续段更晚
        return b_end
    return None


def find_slots(
    conn: sqlite3.Connection,
    instrument_code: str,
    start: datetime,
    end: datetime,
    duration_seconds: int,
) -> dict[str, Any]:
    """GET /api/calibration-slots 的只读计算。

    查询起点链不可用时直接说明原因；查询起点链有效时，沿单工位半开占用
    （既有预约 + 生效维护窗口）贪心推进候选起点，并在**每个候选起点**
    重新执行与确认预约相同的门禁：该时点唯一有效溯源链仍成立、且
    ``candidate_end <= 该起点证书 valid_until``。证书与溯源边有效期是
    闭区间、状态只在边界点及其后发生切换，故候选除查询起点与占用段
    结束点外，还须评估窗内每张相关证书/每条溯源边的有效期边界
    （边界点本身按闭区间重验，边界之后取 +1s 代表开区间状态）。
    这样被占用推迟的建议起点若已跨过证书截止、证书断档/交接或边失效，
    会继续沿边界寻找其后最早的可确认时段，而不是沿用查询起点的结论。
    """
    if duration_seconds <= 0:
        raise ValidationError("所需时长 duration_seconds 必须为正整数秒")
    start_naive = to_utc_naive(start)
    end_naive = to_utc_naive(end)
    if start_naive >= end_naive:
        raise ValidationError("查询窗起点 start 必须严格早于终点 end")
    duration = timedelta(seconds=duration_seconds)
    if duration > end_naive - start_naive:
        raise ValidationError(
            f"所需时长 {duration_seconds}s 超过查询窗长度 "
            f"{int((end_naive - start_naive).total_seconds())}s"
        )

    lo_s, hi_s = fmt(start_naive), fmt(end_naive)

    if get_instrument(conn, instrument_code) is None:
        raise KeyError(instrument_code)

    cat = Catalog(conn)

    # 1) 查询起点该仪器必须存在唯一且校验通过的溯源链（沿用查询起点门禁：
    #    起点链即不可用时不做后续空档搜索，直接给出链不可用原因）
    first_gate = unique_chain_gate(
        conn, instrument_code, start_naive, cat=cat
    )
    if not first_gate["ok"]:
        return {
            "instrument": instrument_code,
            "window_start": lo_s,
            "window_end": hi_s,
            "duration_seconds": duration_seconds,
            "traceability_at": first_gate["at"],
            "traceability_available": False,
            "reason_code": REASON_CHAIN_UNAVAILABLE,
            "reason": first_gate["reason"],
            "chain_reason_code": first_gate["reason_code"],
            "certificate_id": None,
            "certificate_valid_until": None,
            "search_window_start": lo_s,
            "search_window_end": hi_s,
            "slot": None,
            "busy": [],
            "maintenance_windows": [],
            "trace_report": first_gate["report"],
        }

    # 2) 全窗单工位占用（与半开查询窗相交的既有预约 + 生效维护窗口）
    rows = _overlapping_bookings(conn, lo_s, hi_s)
    mrows = _overlapping_maintenance(conn, lo_s, hi_s)
    busy_intervals = _merge_intervals(
        [(_parse(r["start_at"]), _parse(r["end_at"])) for r in rows]
        + [(_parse(r["start_at"]), _parse(r["end_at"])) for r in mrows]
    )

    # 3) 候选起点集合：查询起点、占用段结束点、窗内相关证书/溯源边的
    #    有效期边界点（闭区间交接点须重验）及其后 +1s（边界之后的开
    #    区间状态）。全部落在查询窗内、升序去重后逐一评估。
    boundary_ts = relevant_trace_boundaries(
        conn, instrument_code, start_naive, end_naive, cat=cat
    )
    candidates: set[datetime] = {start_naive}
    for _b_start, b_end in busy_intervals:
        if start_naive < b_end < end_naive:
            candidates.add(b_end)
    one_second = timedelta(seconds=1)
    for boundary in boundary_ts:
        if start_naive <= boundary < end_naive:
            candidates.add(boundary)
        after = boundary + one_second
        if start_naive < after < end_naive:
            candidates.add(after)
    ordered_candidates = sorted(candidates)

    # 4) 沿候选推进：被占用覆盖则跳到占用段结束（已在候选集中）；
    #    不被占用但放不下整段时长的候选直接跳过；其余候选在候选起点
    #    重验唯一有效链与起点证书截止，最早通过者即为答案。
    blockers: list[dict[str, Any]] = []
    found: Optional[tuple[datetime, datetime, dict[str, Any]]] = None
    saw_fitting_candidate = False  # 是否出现过“不被占用且容得下时长”的候选

    for cand in ordered_candidates:
        busy_end = _busy_end_at(busy_intervals, cand)
        if busy_end is not None:
            continue  # 该候选落在占用段内：跳到其结束点（另一候选）继续
        cand_end = cand + duration
        if cand_end > end_naive:
            continue  # 占用后剩余窗长不足：该候选放不下
        saw_fitting_candidate = True

        gate = unique_chain_gate(conn, instrument_code, cand, cat=cat)
        if not gate["ok"]:
            blockers.append({"kind": "chain", "at": cand, "gate": gate})
            continue
        deadline_s = gate["certificate_valid_until"]
        if cand_end > _parse(deadline_s):
            blockers.append({"kind": "deadline", "at": cand, "gate": gate})
            continue

        found = (cand, cand_end, gate)
        break

    # 5) 组装响应。证书/截止/溯源说明一律对应实际建议起点（无建议时
    #    对应最早阻断候选的门禁；连可放候选都没有则回退查询起点门禁）。
    busy_out = [
        {
            "booking_id": r["id"],
            "instrument_code": r["instrument_code"],
            "start_at": r["start_at"],
            "end_at": r["end_at"],
        }
        for r in rows
    ]
    maintenance_out = [
        {
            "maintenance_window_id": r["id"],
            "start_at": r["start_at"],
            "end_at": r["end_at"],
            "reason": r["reason"],
        }
        for r in mrows
    ]

    if found is not None:
        s_start, s_end, gate = found
        deadline_s = gate["certificate_valid_until"]
        return {
            "instrument": instrument_code,
            "window_start": lo_s,
            "window_end": hi_s,
            "duration_seconds": duration_seconds,
            "traceability_at": gate["at"],
            "traceability_available": True,
            "reason_code": None,
            "reason": None,
            "chain_reason_code": None,
            "certificate_id": gate["start_certificate_id"],
            "certificate_valid_until": deadline_s,
            "search_window_start": lo_s,
            "search_window_end": hi_s,
            "slot": {
                "start": fmt(s_start),
                "end": fmt(s_end),
                "ends_by_certificate_valid_until": fmt(s_end) <= deadline_s,
            },
            "busy": busy_out,
            "maintenance_windows": maintenance_out,
            "trace_report": gate["report"],
        }

    if not saw_fitting_candidate:
        # 窗内每个候选要么落在占用段内、要么其后放不下所需时长：
        # 纯工位占用原因（即便起点证书很早到期，证书也不是阻断因素）
        blocker_gate = blockers[0]["gate"] if blockers else first_gate
        cert_id = (
            blocker_gate.get("start_certificate_id")
            if blocker_gate["ok"]
            else None
        )
        deadline_s = (
            blocker_gate["certificate_valid_until"]
            if blocker_gate["ok"]
            else None
        )
        return {
            "instrument": instrument_code,
            "window_start": lo_s,
            "window_end": hi_s,
            "duration_seconds": duration_seconds,
            "traceability_at": blocker_gate["at"],
            "traceability_available": blocker_gate["ok"],
            "reason_code": REASON_NO_FREE_SLOT,
            "reason": (
                f"单个校准工位在 [{lo_s}, {hi_s}) 内不存在可容纳 "
                f"{duration_seconds}s 的空档（既有预约与生效维护封锁窗口"
                "均按半开区间占用）"
            ),
            "chain_reason_code": (
                None if blocker_gate["ok"] else blocker_gate["reason_code"]
            ),
            "certificate_id": cert_id,
            "certificate_valid_until": deadline_s,
            "search_window_start": lo_s,
            "search_window_end": hi_s,
            "slot": None,
            "busy": busy_out,
            "maintenance_windows": maintenance_out,
            "trace_report": blocker_gate["report"],
        }

    # 有过放得下整段时长且不被占用的候选，但逐一被链不可用/证书截止
    # 拒绝：取最早阻断候选（同时刻链阻断优先于截止阻断说明）作为结论
    blockers.sort(key=lambda b: (b["at"], 0 if b["kind"] == "chain" else 1))
    decisive = blockers[0]
    gate = decisive["gate"]
    at_s = gate["at"]
    if decisive["kind"] == "chain":
        reason_code = REASON_CHAIN_UNAVAILABLE
        reason = (
            f"工位空档候选起点 {at_s} 处，仪器 {instrument_code} 的唯一有效"
            "溯源链不可用（" + gate["reason"] + "）；已沿工位占用与证书/"
            "溯源边有效期边界继续搜索，窗内不存在可确认时段"
        )
        cert_id = None
        deadline_s = None
    else:
        cert_id = gate["start_certificate_id"]
        deadline_s = gate["certificate_valid_until"]
        reason_code = REASON_DEADLINE_TOO_EARLY
        reason = (
            f"工位空档候选起点 {at_s} 处，仪器 {instrument_code} 唯一有效"
            f"溯源链的起点证书 {cert_id} 于 {deadline_s} 到期，"
            f"预约（{duration_seconds}s）无法在到期前结束；已沿工位占用与"
            "证书/溯源边有效期边界继续搜索，窗内不存在可确认时段"
        )
    return {
        "instrument": instrument_code,
        "window_start": lo_s,
        "window_end": hi_s,
        "duration_seconds": duration_seconds,
        "traceability_at": at_s,
        "traceability_available": gate["ok"],
        "reason_code": reason_code,
        "reason": reason,
        "chain_reason_code": (
            gate["reason_code"] if decisive["kind"] == "chain" else None
        ),
        "certificate_id": cert_id,
        "certificate_valid_until": deadline_s,
        "search_window_start": lo_s,
        "search_window_end": hi_s,
        "slot": None,
        "busy": busy_out,
        "maintenance_windows": maintenance_out,
        "trace_report": gate["report"],
    }


# ---------------------------------------------------------------- 预约确认

def confirm_booking(
    conn: sqlite3.Connection,
    instrument_code: str,
    start: datetime,
    duration_seconds: int,
    note: str = "",
) -> dict[str, Any]:
    """在**同一写事务**内重查占用与证书截止并写入预约。

    调用方必须提供已执行 ``BEGIN IMMEDIATE`` 的连接；本函数只做检查与
    插入，COMMIT/ROLLBACK 由调用方（路由层上下文）负责。任一条件不再
    成立即抛 :class:`BookingConflict`（409），事务回滚，绝不写入。
    """
    if duration_seconds <= 0:
        raise ValidationError("所需时长 duration_seconds 必须为正整数秒")
    start_naive = to_utc_naive(start)
    end_naive = start_naive + timedelta(seconds=duration_seconds)
    start_s, end_s = fmt(start_naive), fmt(end_naive)

    # 1) 仪器仍存在
    if get_instrument(conn, instrument_code) is None:
        raise BookingConflict(
            CONFLICT_INSTRUMENT_NOT_FOUND,
            f"仪器不存在: {instrument_code}",
            {"instrument_code": instrument_code},
        )

    # 2) 该预约起点仪器唯一有效链仍成立（重查链，不沿用 GET 时结果）
    gate = unique_chain_gate(conn, instrument_code, start_naive)
    if not gate["ok"]:
        raise BookingConflict(
            CONFLICT_CHAIN_UNAVAILABLE,
            f"预约起点 {start_s} 仪器 {instrument_code} 的唯一有效溯源链不可用："
            + gate["reason"],
            {
                "reason_code": gate["reason_code"],
                "trace_report": gate["report"],
            },
        )

    cert_id = gate["start_certificate_id"]
    deadline_s = gate["certificate_valid_until"]

    # 3) 预约结束不得晚于起点证书到期（半开：end == valid_until 允许）
    if end_s > deadline_s:
        raise BookingConflict(
            CONFLICT_ENDS_AFTER_CERT_EXPIRY,
            f"预约结束 {end_s} 晚于仪器 {instrument_code} 起点证书 {cert_id} "
            f"的到期时刻 {deadline_s}，预约必须在证书到期前结束",
            {
                "certificate_id": cert_id,
                "certificate_valid_until": deadline_s,
                "booking_start": start_s,
                "booking_end": end_s,
            },
        )

    # 4) 单工位占用重查：半开重叠 b.start < end AND b.end > start
    clashes = find_clashes(conn, start_s, end_s)
    if clashes:
        raise BookingConflict(
            CONFLICT_STATION_OCCUPIED,
            f"单个校准工位在 {start_s} ~ {end_s}（半开区间）已被 "
            f"{len(clashes)} 条预约占用，时段冲突",
            {"conflicting_bookings": clashes},
        )

    # 4.1) 维护封锁重查：同一写事务内重查活跃维护窗口（半开重叠即 409）
    _raise_if_maintenance(
        conn, start_s, end_s, what="预约时段"
    )

    # 5) 全部条件成立：原子写入（与上述重查处于同一写事务）
    return insert_booking(
        conn,
        instrument_code=instrument_code,
        start_at=start_s,
        end_at=end_s,
        duration_seconds=duration_seconds,
        certificate_id=cert_id,
        certificate_valid_until=deadline_s,
        note=note,
    )


# ---------------------------------------------------------------- 批量改期

def _get_booking_row(
    conn: sqlite3.Connection, booking_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM calibration_bookings WHERE id=?", (booking_id,)
    ).fetchone()


def _update_booking_schedule(
    conn: sqlite3.Connection,
    *,
    booking_id: str,
    start_at: str,
    end_at: str,
    duration_seconds: int,
    certificate_id: str,
    certificate_valid_until: str,
) -> dict[str, Any]:
    """更新预约时段与证书快照（供批量改期使用）；仪器、备注与创建时间不变。"""
    conn.execute(
        """
        UPDATE calibration_bookings
        SET start_at=:start_at, end_at=:end_at,
            duration_seconds=:duration_seconds,
            certificate_id=:certificate_id,
            certificate_valid_until=:certificate_valid_until
        WHERE id=:id
        """,
        {
            "id": booking_id,
            "start_at": start_at,
            "end_at": end_at,
            "duration_seconds": duration_seconds,
            "certificate_id": certificate_id,
            "certificate_valid_until": certificate_valid_until,
        },
    )
    row = _get_booking_row(conn, booking_id)
    return _booking_dict(row)


def reschedule_bookings(
    conn: sqlite3.Connection,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """批量改期：在**同一写事务**内核验并更新 2～10 条已有预约的时段。

    ``items`` 按请求顺序给出
    ``{"booking_id": str, "start": datetime,
      "duration_seconds": Optional[int]}``；
    ``duration_seconds`` 为 ``None`` 时沿用该预约原时长。调用方必须提供
    已执行 ``BEGIN IMMEDIATE`` 的连接；本函数只做检查与更新，
    COMMIT/ROLLBACK 由调用方（路由层上下文）负责。

    核验顺序（任一失败即抛 :class:`BookingConflict`，事务整体回滚）：

    1. 批次核验：预约 ID 不重复、每条预约均存在；
    2. 逐条核验：仪器仍存在、**新起点**该仪器唯一有效溯源链仍成立
       （重新验证，不沿用旧证书快照）、新结束不晚于起点证书
       ``valid_until``；
    3. 占用核验：本批预约的旧占用**整体排除**后，按半开区间检查
       新时段（a）与其他预约、（b）彼此两两的冲突——因此允许本批
       预约交换时段，也允许首尾相接；
    4. 全部通过才逐条 UPDATE 时段与证书快照（仪器/备注/创建时间不变），
       按请求顺序返回更新后的预约。
    """
    # 1) 批次核验：ID 不重复、预约存在
    seen: set[str] = set()
    originals: dict[str, sqlite3.Row] = {}
    for item in items:
        bid = item["booking_id"]
        if bid in seen:
            raise BookingConflict(
                CONFLICT_DUPLICATE_BOOKING_ID,
                f"预约 ID 重复: {bid}（批量改期要求每条预约 ID 互不重复）",
                {"booking_id": bid},
            )
        seen.add(bid)
        row = _get_booking_row(conn, bid)
        if row is None:
            raise BookingConflict(
                CONFLICT_BOOKING_NOT_FOUND,
                f"预约不存在: {bid}",
                {"booking_id": bid},
            )
        originals[bid] = row

    # 2) 逐条计算新时段，重查仪器、溯源链与证书截止
    targets: list[dict[str, Any]] = []
    for item in items:
        bid = item["booking_id"]
        row = originals[bid]
        instrument_code = row["instrument_code"]
        duration = item["duration_seconds"]
        if duration is None:
            duration = row["duration_seconds"]
        start_naive = to_utc_naive(item["start"])
        end_naive = start_naive + timedelta(seconds=duration)
        start_s, end_s = fmt(start_naive), fmt(end_naive)

        # 仪器仍存在（所属仪器以库内现值为准，改期不更换仪器）
        if get_instrument(conn, instrument_code) is None:
            raise BookingConflict(
                CONFLICT_INSTRUMENT_NOT_FOUND,
                f"预约 {bid} 所属仪器不存在: {instrument_code}",
                {"booking_id": bid, "instrument_code": instrument_code},
            )

        # 新起点该仪器唯一有效链仍成立（重新验证，不沿用旧快照）
        gate = unique_chain_gate(conn, instrument_code, start_naive)
        if not gate["ok"]:
            raise BookingConflict(
                CONFLICT_CHAIN_UNAVAILABLE,
                f"预约 {bid} 新起点 {start_s} 仪器 {instrument_code} 的"
                "唯一有效溯源链不可用：" + gate["reason"],
                {
                    "booking_id": bid,
                    "reason_code": gate["reason_code"],
                    "trace_report": gate["report"],
                },
            )

        cert_id = gate["start_certificate_id"]
        deadline_s = gate["certificate_valid_until"]

        # 新结束不得晚于起点证书到期（半开：end == valid_until 允许）
        if end_s > deadline_s:
            raise BookingConflict(
                CONFLICT_ENDS_AFTER_CERT_EXPIRY,
                f"预约 {bid} 新结束 {end_s} 晚于仪器 {instrument_code} "
                f"起点证书 {cert_id} 的到期时刻 {deadline_s}，"
                "预约必须在证书到期前结束",
                {
                    "booking_id": bid,
                    "certificate_id": cert_id,
                    "certificate_valid_until": deadline_s,
                    "booking_start": start_s,
                    "booking_end": end_s,
                },
            )
        targets.append(
            {
                "booking_id": bid,
                "start_s": start_s,
                "end_s": end_s,
                "duration": duration,
                "certificate_id": cert_id,
                "certificate_valid_until": deadline_s,
            }
        )

    # 3) 占用核验：本批旧占用整体排除后，检查新时段与其他预约及彼此
    batch_ids = set(originals)
    for t in targets:
        clashes = [
            b for b in find_clashes(conn, t["start_s"], t["end_s"])
            if b["id"] not in batch_ids
        ]
        if clashes:
            raise BookingConflict(
                CONFLICT_STATION_OCCUPIED,
                f"预约 {t['booking_id']} 新时段 {t['start_s']} ~ {t['end_s']}"
                f"（半开区间）与单工位上 {len(clashes)} 条其他预约冲突",
                {"booking_id": t["booking_id"], "conflicting_bookings": clashes},
            )
    for i in range(len(targets)):
        for j in range(i + 1, len(targets)):
            a, b = targets[i], targets[j]
            # 半开重叠判据：a.start < b.end AND b.start < a.end（首尾相接不冲突）
            if a["start_s"] < b["end_s"] and b["start_s"] < a["end_s"]:
                raise BookingConflict(
                    CONFLICT_STATION_OCCUPIED,
                    f"批量改期后的新时段彼此冲突：预约 {b['booking_id']} "
                    f"[{b['start_s']}, {b['end_s']}) 与预约 {a['booking_id']} "
                    f"[{a['start_s']}, {a['end_s']}) 在半开区间下重叠",
                    {
                        "booking_id": b["booking_id"],
                        "conflicting_batch_bookings": [
                            {
                                "booking_id": a["booking_id"],
                                "start_at": a["start_s"],
                                "end_at": a["end_s"],
                            },
                            {
                                "booking_id": b["booking_id"],
                                "start_at": b["start_s"],
                                "end_at": b["end_s"],
                            },
                        ],
                    },
                )

    # 3.1) 维护封锁重查：本批预约旧占用可整体排除，但活跃维护窗口不排除
    #      （改期不得移入维护时段）；逐条半开查重，任一重叠整体回滚。
    for t in targets:
        windows = _maintenance_clash_dicts(conn, t["start_s"], t["end_s"])
        if windows:
            raise BookingConflict(
                CONFLICT_STATION_UNDER_MAINTENANCE,
                f"预约 {t['booking_id']} 新时段 {t['start_s']} ~ {t['end_s']}"
                f"（半开区间）与单工位上 {len(windows)} 个生效维护窗口重叠，"
                "工位维护封锁中",
                {
                    "booking_id": t["booking_id"],
                    "conflicting_maintenance_windows": windows,
                },
            )

    # 4) 全部通过：逐条更新时段与证书快照（仪器/备注/创建时间不变）
    updated = [
        _update_booking_schedule(
            conn,
            booking_id=t["booking_id"],
            start_at=t["start_s"],
            end_at=t["end_s"],
            duration_seconds=t["duration"],
            certificate_id=t["certificate_id"],
            certificate_valid_until=t["certificate_valid_until"],
        )
        for t in targets
    ]

    # 5) 时段实际变化的预约：其未补位候补（waiting）随旧时段在同一事务内
    #    失效，避免旧时段队列悬空（候补沿用登记时的起止时段快照）。
    now = utcnow_str()
    for t in targets:
        old = originals[t["booking_id"]]
        if old["start_at"] != t["start_s"] or old["end_at"] != t["end_s"]:
            _expire_waiting_entries(
                conn,
                t["booking_id"],
                reason=(
                    f"目标预约 {t['booking_id']} 已改期，原时段 "
                    f"{old['start_at']} ~ {old['end_at']} 不再占用，"
                    "旧时段候补队列失效"
                ),
                now=now,
            )
    return updated


# ---------------------------------------------------------------- 候补

def _waitlist_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in _WAITLIST_FIELDS}


def insert_waitlist(
    conn: sqlite3.Connection,
    *,
    booking_id: str,
    instrument_code: str,
    start_at: str,
    end_at: str,
    duration_seconds: int,
    note: str = "",
    waitlist_id: Optional[str] = None,
    created_at: Optional[str] = None,
) -> dict[str, Any]:
    """底层写入候补记录（供登记事务与演示播种复用），不做任何校验。"""
    if waitlist_id is None:
        waitlist_id = "WL-" + datetime.utcnow().strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:12]
    conn.execute(
        """
        INSERT INTO calibration_waitlist(
            id, booking_id, instrument_code, start_at, end_at,
            duration_seconds, note, status, created_at)
        VALUES (:id, :booking_id, :instrument_code, :start_at, :end_at,
                :duration_seconds, :note, :status, :created_at)
        """,
        {
            "id": waitlist_id,
            "booking_id": booking_id,
            "instrument_code": instrument_code,
            "start_at": start_at,
            "end_at": end_at,
            "duration_seconds": duration_seconds,
            "note": note,
            "status": WAITLIST_WAITING,
            "created_at": created_at or utcnow_str(),
        },
    )
    row = conn.execute(
        "SELECT * FROM calibration_waitlist WHERE id=?", (waitlist_id,)
    ).fetchone()
    return _waitlist_dict(row)


def _queue_positions(
    conn: sqlite3.Connection, booking_ids: set[str]
) -> dict[str, int]:
    """各预约候补中（waiting）条目按登记先后（seq 升序）的 1 基名次。"""
    positions: dict[str, int] = {}
    for bid in booking_ids:
        rows = conn.execute(
            "SELECT id FROM calibration_waitlist "
            "WHERE booking_id=? AND status='waiting' ORDER BY seq",
            (bid,),
        ).fetchall()
        for i, r in enumerate(rows, 1):
            positions[r["id"]] = i
    return positions


def register_waitlist(
    conn: sqlite3.Connection,
    booking_id: str,
    instrument_code: str,
    note: str = "",
) -> dict[str, Any]:
    """对指定已确认预约登记候补（同一写事务内核对目标预约与仪器存在）。

    候补沿用该预约当前的起止时段快照，按登记先后（seq 自增）排队；
    溯源链 / 证书截止 / 占用冲突不在登记时校验，而在目标预约取消时的
    补位事务内逐条重验。调用方必须提供已执行 ``BEGIN IMMEDIATE`` 的连接。
    """
    row = _get_booking_row(conn, booking_id)
    if row is None:
        raise BookingConflict(
            CONFLICT_BOOKING_NOT_FOUND,
            f"预约不存在: {booking_id}（候补须挂在已确认预约上）",
            {"booking_id": booking_id},
        )
    if get_instrument(conn, instrument_code) is None:
        raise BookingConflict(
            CONFLICT_INSTRUMENT_NOT_FOUND,
            f"仪器不存在: {instrument_code}",
            {"instrument_code": instrument_code},
        )
    entry = insert_waitlist(
        conn,
        booking_id=booking_id,
        instrument_code=instrument_code,
        start_at=row["start_at"],
        end_at=row["end_at"],
        duration_seconds=row["duration_seconds"],
        note=note,
    )
    entry["queue_position"] = _queue_positions(conn, {booking_id})[entry["id"]]
    return entry


def list_waitlist(
    conn: sqlite3.Connection,
    booking_id: Optional[str] = None,
    instrument_code: Optional[str] = None,
    status: Optional[str] = None,
) -> list[dict[str, Any]]:
    """候补状态查询：按目标预约 / 仪器 / 状态筛选，按登记先后（seq）升序。

    ``queue_position`` 仅对候补中（waiting）条目给出其在目标预约队列中
    的 1 基名次；已进入终态（promoted/failed/expired）的条目为 None。
    目标预约可能已取消删除，故 booking_id 筛选不做存在性校验。
    """
    clauses: list[str] = []
    params: list[Any] = []
    if booking_id is not None:
        clauses.append("booking_id=?")
        params.append(booking_id)
    if instrument_code is not None:
        clauses.append("instrument_code=?")
        params.append(instrument_code)
    if status is not None:
        clauses.append("status=?")
        params.append(status)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT * FROM calibration_waitlist{where} ORDER BY seq", params
    ).fetchall()
    entries = [_waitlist_dict(r) for r in rows]
    positions = _queue_positions(conn, {e["booking_id"] for e in entries})
    for e in entries:
        e["queue_position"] = (
            positions.get(e["id"]) if e["status"] == WAITLIST_WAITING else None
        )
    return entries


def _expire_waiting_entries(
    conn: sqlite3.Connection, booking_id: str, *, reason: str, now: str
) -> list[str]:
    """把目标预约全部候补中（waiting）条目标记为 expired，返回其 id。"""
    rows = conn.execute(
        "SELECT id FROM calibration_waitlist "
        "WHERE booking_id=? AND status='waiting' ORDER BY seq",
        (booking_id,),
    ).fetchall()
    for r in rows:
        conn.execute(
            "UPDATE calibration_waitlist "
            "SET status='expired', status_reason=?, processed_at=? WHERE id=?",
            (reason, now, r["id"]),
        )
    return [r["id"] for r in rows]


def _revalidate_for_slot(
    conn: sqlite3.Connection,
    instrument_code: str,
    start_naive: datetime,
    start_s: str,
    end_s: str,
) -> tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str]]:
    """补位重验（取消事务内）：仪器存在、时段起点唯一有效溯源链、
    证书到期时刻覆盖时段终点、与剩余预约及生效维护窗口无半开冲突。

    返回 ``(ok, reason_code, reason, certificate_id, certificate_valid_until)``；
    不合格时给出原因码与中文说明，由调用方跳过该申请，绝不抛出中断队列。
    """
    if get_instrument(conn, instrument_code) is None:
        return (
            False,
            CONFLICT_INSTRUMENT_NOT_FOUND,
            f"仪器不存在: {instrument_code}",
            None,
            None,
        )
    gate = unique_chain_gate(conn, instrument_code, start_naive)
    if not gate["ok"]:
        return (
            False,
            gate["reason_code"],
            f"候补时段起点 {start_s} 仪器 {instrument_code} 的"
            "唯一有效溯源链不可用：" + gate["reason"],
            None,
            None,
        )
    cert_id = gate["start_certificate_id"]
    deadline_s = gate["certificate_valid_until"]
    if end_s > deadline_s:
        return (
            False,
            CONFLICT_ENDS_AFTER_CERT_EXPIRY,
            f"候补时段结束 {end_s} 晚于仪器 {instrument_code} 起点证书 "
            f"{cert_id} 的到期时刻 {deadline_s}，预约必须在证书到期前结束",
            cert_id,
            deadline_s,
        )
    clashes = find_clashes(conn, start_s, end_s)
    if clashes:
        return (
            False,
            CONFLICT_STATION_OCCUPIED,
            f"单个校准工位在 {start_s} ~ {end_s}（半开区间）已被 "
            f"{len(clashes)} 条剩余预约占用，时段冲突",
            cert_id,
            deadline_s,
        )
    windows = _maintenance_clash_dicts(conn, start_s, end_s)
    if windows:
        return (
            False,
            CONFLICT_STATION_UNDER_MAINTENANCE,
            f"候补时段 {start_s} ~ {end_s}（半开区间）与 "
            f"{len(windows)} 个生效维护窗口重叠，工位维护封锁中，"
            "候补补位跳过该申请",
            cert_id,
            deadline_s,
        )
    return True, None, None, cert_id, deadline_s


def cancel_booking(conn: sqlite3.Connection, booking_id: str) -> dict[str, Any]:
    """取消预约并在**同一写事务**内按候补队列补位。

    调用方必须提供已执行 ``BEGIN IMMEDIATE`` 的连接。流程：

    1. 预约不存在（含已取消）即抛 :class:`BookingConflict`
       （``BOOKING_NOT_FOUND``，路由层映射 404）——重复取消不会重复补位；
    2. 删除预约行、释放占用；
    3. 依登记先后（seq 升序）逐条重验候补中条目：仪器在时段起点的
       唯一有效溯源链、证书到期时刻、与剩余预约及生效中维护窗口的
       半开冲突；不合格申请标记 ``failed`` 并跳过（撞维护封锁记
       ``STATION_UNDER_MAINTENANCE``）；
    4. 首个合格申请转为已确认预约（沿用原时段与时长，保存起点证书
       快照与候补备注），该条目标记 ``promoted``；
    5. 排在中签者之后、未及评估的条目（时段已被补位）标记 ``expired``。

    COMMIT/ROLLBACK 由调用方（路由层上下文）负责。
    """
    row = _get_booking_row(conn, booking_id)
    if row is None:
        raise BookingConflict(
            CONFLICT_BOOKING_NOT_FOUND,
            f"预约不存在: {booking_id}（不存在或已取消，不重复补位）",
            {"booking_id": booking_id},
        )
    cancelled = _booking_dict(row)
    start_s, end_s = row["start_at"], row["end_at"]
    duration = row["duration_seconds"]
    start_naive = _parse(start_s)

    # 1) 释放占用（与后续补位处于同一写事务）
    conn.execute("DELETE FROM calibration_bookings WHERE id=?", (booking_id,))

    # 2) 依登记先后逐条重验候补队列
    entries = conn.execute(
        "SELECT * FROM calibration_waitlist "
        "WHERE booking_id=? AND status='waiting' ORDER BY seq",
        (booking_id,),
    ).fetchall()

    now = utcnow_str()
    skipped: list[dict[str, Any]] = []
    promotion: Optional[dict[str, Any]] = None
    for position, e in enumerate(entries, 1):
        ok, reason_code, reason, cert_id, cert_until = _revalidate_for_slot(
            conn, e["instrument_code"], start_naive, start_s, end_s
        )
        if not ok:
            conn.execute(
                "UPDATE calibration_waitlist "
                "SET status='failed', status_reason=?, processed_at=? WHERE id=?",
                (reason, now, e["id"]),
            )
            skipped.append(
                {
                    "waitlist_id": e["id"],
                    "instrument_code": e["instrument_code"],
                    "reason_code": reason_code,
                    "reason": reason,
                }
            )
            continue
        # 3) 首个合格申请：转为已确认预约（沿用原时段，保存证书快照）
        new_booking = insert_booking(
            conn,
            instrument_code=e["instrument_code"],
            start_at=start_s,
            end_at=end_s,
            duration_seconds=duration,
            certificate_id=cert_id,
            certificate_valid_until=cert_until,
            note=e["note"],
        )
        conn.execute(
            "UPDATE calibration_waitlist "
            "SET status='promoted', promoted_booking_id=?, processed_at=? "
            "WHERE id=?",
            (new_booking["id"], now, e["id"]),
        )
        promotion = {
            "waitlist_id": e["id"],
            "instrument_code": e["instrument_code"],
            "queue_position": position,
            "booking": new_booking,
        }
        break

    # 4) 排在中签者之后的候补中条目：时段已被补位，标记失效
    expired_ids: list[str] = []
    if promotion is not None:
        expired_ids = _expire_waiting_entries(
            conn,
            booking_id,
            reason=(
                f"目标预约 {booking_id} 取消时，时段已由队列中更早的合格候补"
                f"补位（新预约 {promotion['booking']['id']}），本候补未获补位而失效"
            ),
            now=now,
        )

    return {
        "cancelled_booking": cancelled,
        "waitlist_processed": len(entries),
        "promoted": promotion is not None,
        "promotion": promotion,
        "skipped": skipped,
        "expired_waitlist_ids": expired_ids,
    }


# ---------------------------------------------------------------- 有效性巡检

# 巡检结论：valid 链有效且快照一致 | changed 链有效但证书 id/到期变化 |
# invalid 链失效或截止不足
INSPECTION_VALID = "valid"
INSPECTION_CHANGED = "changed"
INSPECTION_INVALID = "invalid"

# 巡检特有原因码；invalid 另复用 CONFLICT_INSTRUMENT_NOT_FOUND、
# unique_chain_gate 的溯源链细分原因码与 CONFLICT_ENDS_AFTER_CERT_EXPIRY
INSPECTION_REASON_SNAPSHOT_CHANGED = "CERTIFICATE_SNAPSHOT_CHANGED"


def _inspect_booking_row(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> dict[str, Any]:
    """只读复核单条已确认预约：在其**开始时刻**重建唯一有效溯源链。

    判定规则（与确认/改期写事务共用同一链路门禁与截止比较）：

    1. 仪器已不存在，或开始时刻唯一有效链不成立（无有效证书 / 起点不
       唯一 / 多候选 / 断裂 / 校验失败）-> ``invalid``，原因码取
       ``INSTRUMENT_NOT_FOUND`` 或链细分原因码；
    2. 链仍有效但预约结束晚于**当前**起点证书到期 -> ``invalid``
       （``BOOKING_ENDS_AFTER_CERT_EXPIRY``，半开 ``end == valid_until``
       允许）；
    3. 链仍有效且截止充足，但当前起点证书 id 或到期时刻与确认时保存的
       快照不同 -> ``changed``（``CERTIFICATE_SNAPSHOT_CHANGED``）；
    4. 其余 -> ``valid``。
    """
    start_s, end_s = row["start_at"], row["end_at"]
    instrument_code = row["instrument_code"]
    snap_cert = row["certificate_id"]
    snap_until = row["certificate_valid_until"]
    item: dict[str, Any] = {
        "booking_id": row["id"],
        "instrument_code": instrument_code,
        "start_at": start_s,
        "end_at": end_s,
        "current_certificate_id": None,
        "current_certificate_valid_until": None,
        "snapshot_certificate_id": snap_cert,
        "snapshot_certificate_valid_until": snap_until,
    }

    # 1) 仪器仍存在（预约行不随仪器档案删除，防御性复核）
    if get_instrument(conn, instrument_code) is None:
        item.update(
            status=INSPECTION_INVALID,
            reason_code=CONFLICT_INSTRUMENT_NOT_FOUND,
            reason=f"仪器不存在: {instrument_code}",
        )
        return item

    # 2) 在预约开始时刻重建唯一有效溯源链（复用现有链路门禁，不沿用快照）
    gate = unique_chain_gate(conn, instrument_code, _parse(start_s))
    if not gate["ok"]:
        item.update(
            status=INSPECTION_INVALID,
            reason_code=gate["reason_code"],
            reason=(
                f"预约开始时刻 {start_s} 仪器 {instrument_code} 的"
                "唯一有效溯源链不可用：" + gate["reason"]
            ),
        )
        return item

    cert_id = gate["start_certificate_id"]
    until_s = gate["certificate_valid_until"]
    item["current_certificate_id"] = cert_id
    item["current_certificate_valid_until"] = until_s

    # 3) 预约结束不得晚于当前起点证书到期（半开：end == valid_until 允许）
    if end_s > until_s:
        item.update(
            status=INSPECTION_INVALID,
            reason_code=CONFLICT_ENDS_AFTER_CERT_EXPIRY,
            reason=(
                f"预约结束 {end_s} 晚于当前起点证书 {cert_id} 的到期时刻 "
                f"{until_s}，证书截止不足"
            ),
        )
        return item

    # 4) 链仍有效且截止充足：与确认时保存的证书 id / 到期快照比较
    if cert_id != snap_cert or until_s != snap_until:
        changes: list[str] = []
        if cert_id != snap_cert:
            changes.append(f"起点证书由 {snap_cert} 变为 {cert_id}")
        if until_s != snap_until:
            changes.append(
                f"证书 {cert_id} 到期时刻由 {snap_until} 变为 {until_s}"
            )
        item.update(
            status=INSPECTION_CHANGED,
            reason_code=INSPECTION_REASON_SNAPSHOT_CHANGED,
            reason="；".join(changes),
        )
        return item

    item.update(status=INSPECTION_VALID, reason_code=None, reason=None)
    return item


def inspect_bookings(
    conn: sqlite3.Connection, start: datetime, end: datetime
) -> dict[str, Any]:
    """GET /api/calibration-bookings/inspection 的只读巡检计算。

    筛选**开始时刻**落在 UTC 半开查询窗 ``[start, end)`` 内的已确认预约，
    按（开始时刻, 预约 id）稳定排序逐条复核（见
    :func:`_inspect_booking_row`），并汇总 valid / changed / invalid 数量；
    全程只读，不写库。``start >= end`` 抛 :class:`ValidationError`（422）。
    """
    start_naive = to_utc_naive(start)
    end_naive = to_utc_naive(end)
    if start_naive >= end_naive:
        raise ValidationError("巡检查询窗起点 start 必须严格早于终点 end")
    lo_s, hi_s = fmt(start_naive), fmt(end_naive)

    rows = conn.execute(
        "SELECT * FROM calibration_bookings "
        "WHERE start_at >= ? AND start_at < ? ORDER BY start_at, id",
        (lo_s, hi_s),
    ).fetchall()

    items = [_inspect_booking_row(conn, r) for r in rows]
    summary: dict[str, int] = {
        "total": len(items),
        INSPECTION_VALID: 0,
        INSPECTION_CHANGED: 0,
        INSPECTION_INVALID: 0,
    }
    for it in items:
        summary[it["status"]] += 1
    return {
        "window_start": lo_s,
        "window_end": hi_s,
        "summary": summary,
        "items": items,
    }
