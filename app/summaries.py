"""多级摘要的构建与查询。

摘要块层级：
- level 1：每 BLOCK_SIZE 个原始采样聚合出的最细索引块
- level k>1：由 level k-1 中每 BLOCK_SIZE 个子块合并而成的更粗块

每个块保存：首尾点、压力最高/最低点（并列取最早）、采样点数、
覆盖的原始 rowid 区间与时间区间（end_ts 为最后一点的时间戳）。

查询时优先使用“完整落在查询区间内的最粗块”，只有区间边界切开
level 1 块时，才回退读取该块内的原始采样。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

from . import config

# blocks 表的列 -> Stats 字段
_BLOCK_FIELDS = (
    "sample_count",
    "first_ts",
    "last_ts",
    "first_pressure",
    "last_pressure",
    "max_pressure",
    "max_pressure_ts",
    "min_pressure",
    "min_pressure_ts",
)


@dataclass
class Stats:
    """一段采样（原始行或摘要块）的四点统计结果。"""

    sample_count: int = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    first_pressure: Optional[float] = None
    last_pressure: Optional[float] = None
    max_pressure: Optional[float] = None
    max_pressure_ts: Optional[int] = None
    min_pressure: Optional[float] = None
    min_pressure_ts: Optional[int] = None

    @classmethod
    def from_row(cls, row) -> "Stats":
        return cls(**{f: row[f] for f in _BLOCK_FIELDS})

    def to_tuple(self) -> tuple:
        return tuple(getattr(self, f) for f in _BLOCK_FIELDS)


def stats_from_sample(ts: int, pressure: float) -> Stats:
    """单点统计。"""
    return Stats(
        sample_count=1,
        first_ts=ts,
        last_ts=ts,
        first_pressure=pressure,
        last_pressure=pressure,
        max_pressure=pressure,
        max_pressure_ts=ts,
        min_pressure=pressure,
        min_pressure_ts=ts,
    )


def merge_stats(a: Stats, b: Stats) -> Stats:
    """按时序合并两段统计。极值并列时保留时间戳更早的一个。"""
    if a.sample_count == 0:
        return b
    if b.sample_count == 0:
        return a
    if b.max_pressure > a.max_pressure or (
        b.max_pressure == a.max_pressure and b.max_pressure_ts < a.max_pressure_ts
    ):
        max_p, max_ts = b.max_pressure, b.max_pressure_ts
    else:
        max_p, max_ts = a.max_pressure, a.max_pressure_ts
    if b.min_pressure < a.min_pressure or (
        b.min_pressure == a.min_pressure and b.min_pressure_ts < a.min_pressure_ts
    ):
        min_p, min_ts = b.min_pressure, b.min_pressure_ts
    else:
        min_p, min_ts = a.min_pressure, a.min_pressure_ts
    return Stats(
        sample_count=a.sample_count + b.sample_count,
        first_ts=a.first_ts,
        last_ts=b.last_ts,
        first_pressure=a.first_pressure,
        last_pressure=b.last_pressure,
        max_pressure=max_p,
        max_pressure_ts=max_ts,
        min_pressure=min_p,
        min_pressure_ts=min_ts,
    )


def _insert_block(
    conn: sqlite3.Connection,
    test_id: int,
    level: int,
    block_index: int,
    st: Stats,
    first_rowid: int,
    last_rowid: int,
) -> None:
    conn.execute(
        f"""INSERT INTO blocks (
                test_id, level, block_index,
                start_ts, end_ts,
                {", ".join(_BLOCK_FIELDS)},
                first_rowid, last_rowid
            ) VALUES (
                ?, ?, ?, ?, ?,
                {", ".join("?" for _ in _BLOCK_FIELDS)},
                ?, ?
            )""",
        (
            test_id,
            level,
            block_index,
            st.first_ts,
            st.last_ts,
            *st.to_tuple(),
            first_rowid,
            last_rowid,
        ),
    )


def _update_tail_block(
    conn: sqlite3.Connection,
    test_id: int,
    level: int,
    block_index: int,
    st: Stats,
    last_rowid: int,
) -> None:
    """用合并后的统计原地重写尾部块（start_ts / first_* / first_rowid 不变）。"""
    sets = ", ".join(f"{f} = ?" for f in ("end_ts", *_BLOCK_FIELDS, "last_rowid"))
    conn.execute(
        f"UPDATE blocks SET {sets} "
        "WHERE test_id = ? AND level = ? AND block_index = ?",
        (st.last_ts, *st.to_tuple(), last_rowid, test_id, level, block_index),
    )


def append_summaries(
    conn: sqlite3.Connection,
    test_id: int,
    block_size: int,
) -> int:
    """把已写入 samples 的接续采样并入多级摘要，返回新的最高 level。

    只动各级“受影响的尾部”，绝不重扫旧采样：
    - level 1：先用新采样填满既有尾部半块（统计直接合并），再按 block_size 切新块；
    - level k>1：受影响子块在每层都构成连续后缀，重查这些子块的摘要后，
      原地重写覆盖它们的既有父块，并为超出覆盖范围的子块组新父块；
    - 顶层因此产生多于一个父块时自动加层，直到顶层只剩一块。

    调用方负责事务与“新采样 rowid 均大于旧 rowid”的前提。
    """
    old_top = conn.execute(
        "SELECT COALESCE(MAX(level), 0) FROM blocks WHERE test_id = ?", (test_id,)
    ).fetchone()[0]
    if old_top == 0:
        raise ValueError("试验尚无摘要块，无法接续")

    old_last_rid = conn.execute(
        "SELECT last_rowid FROM blocks WHERE test_id = ? AND level = 1 "
        "ORDER BY block_index DESC LIMIT 1",
        (test_id,),
    ).fetchone()[0]

    new_rows = conn.execute(
        "SELECT rowid AS rid, ts, pressure FROM samples "
        "WHERE test_id = ? AND rowid > ? ORDER BY rowid",
        (test_id, old_last_rid),
    ).fetchall()
    if not new_rows:
        return old_top  # 全部为重叠去重行，没有新采样，摘要不变

    # ---- level 1：填满尾部半块，再切新块 ----
    tail = conn.execute(
        "SELECT * FROM blocks WHERE test_id = ? AND level = 1 "
        "ORDER BY block_index DESC LIMIT 1",
        (test_id,),
    ).fetchone()
    tail_idx = tail["block_index"]
    tail_st = Stats.from_row(tail)
    free = block_size - tail["sample_count"]
    cursor = 0
    if free > 0:
        fill = new_rows[:free]
        for row in fill:
            tail_st = merge_stats(tail_st, stats_from_sample(row["ts"], row["pressure"]))
        _update_tail_block(
            conn, test_id, 1, tail_idx, tail_st, fill[-1]["rid"]
        )
        cursor = len(fill)

    next_idx = tail_idx + 1
    while cursor < len(new_rows):
        chunk = new_rows[cursor : cursor + block_size]
        st = Stats()
        for row in chunk:
            st = merge_stats(st, stats_from_sample(row["ts"], row["pressure"]))
        _insert_block(conn, test_id, 1, next_idx, st, chunk[0]["rid"], chunk[-1]["rid"])
        next_idx += 1
        cursor += len(chunk)

    # 受影响 level-1 后缀的起点：处理前最后一个既有受影响块（被重写的尾部
    # 半块）；若尾部块原本已满，没有旧块被重写，则后缀从第一个新建块开始。
    # 用 aff_prev_end 表示“处理前后缀的最后一个子块下标”，-1 代表后缀全是新块。
    aff_prev_end = tail_idx if tail["sample_count"] < block_size else -1

    # ---- level 2+：逐级向上重写受影响的尾部父块，必要时加层 ----
    # 每层只维护 aff_prev_end：处理前受影响后缀的最后一个子块下标
    # （-1 表示后缀全是新块）。重写从后缀首子块（aff_prev_end+1）所属父槽
    # 起，到当前尾部父槽全部重算，再为超出覆盖的子块新建父槽。
    # 全程只重查受影响子块的摘要块，不触碰原始采样。
    level = 2
    while True:
        tail_idx_row = conn.execute(
            "SELECT MAX(block_index) FROM blocks WHERE test_id = ? AND level = ?",
            (test_id, level),
        ).fetchone()

        if tail_idx_row[0] is None:
            # 本层尚不存在 -> 加层：上一层全部块作为输入（读摘要块，不读采样）
            child_rows = conn.execute(
                "SELECT * FROM blocks WHERE test_id = ? AND level = ? "
                "ORDER BY block_index",
                (test_id, level - 1),
            ).fetchall()
            parent_idx = 0
            for i in range(0, len(child_rows), block_size):
                group = child_rows[i : i + block_size]
                st = Stats()
                for r in group:
                    st = merge_stats(st, Stats.from_row(r))
                _insert_block(
                    conn, test_id, level, parent_idx, st,
                    group[0]["first_rowid"], group[-1]["last_rowid"],
                )
                parent_idx += 1
            # 新层的块全部是新建的，没有“处理前的旧部分”
            aff_prev_end = -1
        else:
            tail_parent_idx = tail_idx_row[0]
            child_count = conn.execute(
                "SELECT COUNT(*) FROM blocks WHERE test_id = ? AND level = ?",
                (test_id, level - 1),
            ).fetchone()[0]
            # 受影响后缀在本层覆盖的第一个父槽。后缀的第一个子块下标为
            # aff_prev_end+1（aff_prev_end=-1 时即 0）：新建子块若落进某个
            # 既有父槽，该槽同样要重算，因此起点必须按“后缀首子块”定位，
            # 不能只按处理前的旧部分末子块定位。
            first_suffix_child = aff_prev_end + 1
            first_existing_to_rewrite = min(
                tail_parent_idx, first_suffix_child // block_size
            )

            # 1) 重写从首个受影响父槽到当前尾部父槽的全部既有父槽
            for p_idx in range(first_existing_to_rewrite, tail_parent_idx + 1):
                slot_first = p_idx * block_size
                kids = conn.execute(
                    "SELECT * FROM blocks WHERE test_id = ? AND level = ? "
                    "AND block_index >= ? AND block_index < ? ORDER BY block_index",
                    (
                        test_id,
                        level - 1,
                        slot_first,
                        slot_first + block_size,
                    ),
                ).fetchall()
                st = Stats()
                for r in kids:
                    st = merge_stats(st, Stats.from_row(r))
                _update_tail_block(
                    conn, test_id, level, p_idx, st, kids[-1]["last_rowid"]
                )

            # 2) 超出既有父槽覆盖范围的子块：按 block_size 组新父槽
            new_parent_idx = tail_parent_idx + 1
            for start in range(new_parent_idx * block_size, child_count, block_size):
                kids = conn.execute(
                    "SELECT * FROM blocks WHERE test_id = ? AND level = ? "
                    "AND block_index >= ? AND block_index < ? ORDER BY block_index",
                    (
                        test_id,
                        level - 1,
                        start,
                        start + block_size,
                    ),
                ).fetchall()
                st = Stats()
                for r in kids:
                    st = merge_stats(st, Stats.from_row(r))
                _insert_block(
                    conn, test_id, level, new_parent_idx, st,
                    kids[0]["first_rowid"], kids[-1]["last_rowid"],
                )
                new_parent_idx += 1

            # 下一层视角的 aff_prev_end：处理前最后一个受影响子块所属的父槽
            # 下标（本层块即下一层的子块）；全部是新父槽时记 -1。
            aff_prev_end = (
                aff_prev_end // block_size if aff_prev_end >= 0 else -1
            )

        total_at_level = conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE test_id = ? AND level = ?",
            (test_id, level),
        ).fetchone()[0]
        if total_at_level == 1:
            return level
        level += 1


def build_summaries(conn: sqlite3.Connection, test_id: int, block_size: int) -> int:
    """为已写入 samples 的试验构建全部摘要层级，返回最高 level。

    调用方负责事务管理；level 1 块直接按 rowid 序号分块聚合，
    之后逐层合并子块，直到只剩一个块。
    """
    cur = conn.execute(
        "SELECT rowid AS rid, ts, pressure FROM samples WHERE test_id = ? ORDER BY rowid",
        (test_id,),
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    level_rows: list[tuple] = []  # (stats, first_rowid, last_rowid)

    def flush(level: int, entries: list[tuple]) -> None:
        for idx, (st, first_rid, last_rid) in enumerate(entries):
            _insert_block(conn, test_id, level, idx, st, first_rid, last_rid)

    # ---- level 1：原始采样分块 ----
    entries: list[tuple] = []
    acc = Stats()
    block_first_rid = rows[0]["rid"]
    for i, row in enumerate(rows):
        st = stats_from_sample(row["ts"], row["pressure"])
        acc = merge_stats(acc, st)
        if (i + 1) % block_size == 0:
            entries.append((acc, block_first_rid, row["rid"]))
            acc = Stats()
            block_first_rid = rows[i + 1]["rid"] if i + 1 < len(rows) else row["rid"]
    if acc.sample_count:
        entries.append((acc, block_first_rid, rows[-1]["rid"]))
    flush(1, entries)
    max_level = 1

    # ---- level 2+：逐层合并上一级块 ----
    prev = entries
    while len(prev) > 1:
        level = max_level + 1
        cur_entries: list[tuple] = []
        for i in range(0, len(prev), block_size):
            group = prev[i : i + block_size]
            st = Stats()
            for child_st, _, _ in group:
                st = merge_stats(st, child_st)
            cur_entries.append((st, group[0][1], group[-1][2]))
        flush(level, cur_entries)
        prev = cur_entries
        max_level = level

    return max_level


def range_summary(
    conn: sqlite3.Connection,
    test_id: int,
    q_start: int,
    q_end: int,
    max_level: int,
    block_size: int | None = None,
    metrics: Optional[dict] = None,
) -> Optional[Stats]:
    """计算半开区间 [q_start, q_end) 内采样的四点统计。

    递归策略：
    1. 取当前最粗层级与区间相交的块；
    2. 块完整落在区间内 -> 直接采用块摘要（命中缓存，不读采样）；
    3. 块被区间边界切开 -> 降到下一层递归；到 level 1 仍被切开
       时，按 rowid 范围读取该块内的原始采样。
    """
    block_size = block_size or config.BLOCK_SIZE
    if metrics is None:
        metrics = {"blocks_used": 0, "blocks_by_level": {}, "raw_rows_read": 0}

    if q_start >= q_end:
        return None

    level = max_level
    overall = Stats()

    while level >= 1:
        rows = conn.execute(
            """SELECT * FROM blocks
               WHERE test_id = ? AND level = ?
                 AND start_ts < ? AND end_ts >= ?
               ORDER BY block_index""",
            (test_id, level, q_end, q_start),
        ).fetchall()

        if not rows:
            # 本层没有相交块（例如查询区间只压在尾部半块之外），继续向下找
            level -= 1
            continue

        for row in rows:
            fully_inside = row["start_ts"] >= q_start and row["end_ts"] < q_end
            if fully_inside:
                overall = merge_stats(overall, Stats.from_row(row))
                metrics["blocks_used"] += 1
                metrics["blocks_by_level"][str(level)] = (
                    metrics["blocks_by_level"].get(str(level), 0) + 1
                )
                continue

            # 块被区间边界切开
            if level == 1:
                # 最细索引块也被切开 -> 唯一需要读原始采样的情形
                raw = conn.execute(
                    """SELECT ts, pressure FROM samples
                       WHERE test_id = ? AND rowid BETWEEN ? AND ?
                         AND ts >= ? AND ts < ?
                       ORDER BY rowid""",
                    (
                        test_id,
                        row["first_rowid"],
                        row["last_rowid"],
                        q_start,
                        q_end,
                    ),
                ).fetchall()
                metrics["raw_rows_read"] += len(raw)
                for r in raw:
                    overall = merge_stats(
                        overall, stats_from_sample(r["ts"], r["pressure"])
                    )
            else:
                # 递归处理该父块在更细一层中的子块，区间裁剪到父块覆盖范围。
                # 右端取 end_ts+1：当父块末点本就落在桶内时，保证最末子块
                # 仍能命中“完整落入”分支；右开排除由 level 1 的 ts < q_end 兜底，
                # 不会把桶外采样带进来。
                sub = range_summary(
                    conn,
                    test_id,
                    max(q_start, row["start_ts"]),
                    min(q_end, row["end_ts"] + 1),
                    level - 1,
                    block_size,
                    metrics,
                )
                if sub is not None:
                    overall = merge_stats(overall, sub)
        return overall if overall.sample_count else None

    return overall if overall.sample_count else None
