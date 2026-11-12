"""接续导入（POST /tests/{id}/append）端到端与摘要增量测试。

覆盖：
- 重叠行同毫秒同压力匹配、不重复入库，只追加晚于末点的采样
- 重叠对不上（缺时刻 / 压力不一致）整份退回，原试验不变
- 坏行 / 空文件 / 未知试验编号的处理
- 增量尾部摘要与全量重扫 build_summaries 完全等价
- 尾部半块补齐、跨层受影响块重建、必要时新增一层
- 接续前后峰谷查询的快照一致性（单事务，查询只见接续前或接续后）
"""

from __future__ import annotations

import io
import os
import tempfile

import pytest

# 独立测试库与小块大小，便于人为制造跨层/加层场景
_tmp = tempfile.TemporaryDirectory()
os.environ["PUMP_DB_PATH"] = os.path.join(_tmp.name, "test_append.db")
os.environ["PUMP_BLOCK_SIZE"] = "4"
os.environ["PUMP_MAX_BUCKETS"] = "1000"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import connect, init_db  # noqa: E402
from app.ingest import parse_csv  # noqa: E402
from app.main import app  # noqa: E402
from app.summaries import build_summaries  # noqa: E402

init_db()
client = TestClient(app)


def _csv(rows, header=("timestamp_ms", "pressure_kpa")) -> bytes:
    buf = io.StringIO()
    buf.write(",".join(header) + "\n")
    for ts, p in rows:
        buf.write(f"{int(ts)},{p}\n")
    return buf.getvalue().encode("utf-8")


def _import(name, rows):
    resp = client.post(
        "/tests/import",
        data={"name": name},
        files={"file": ("f.csv", _csv(rows), "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _append(test_id, rows):
    return client.post(
        f"/tests/{test_id}/append",
        files={"file": ("next.csv", _csv(rows), "text/csv")},
    )


def _snapshot(test_id):
    conn = connect()
    samples = [
        (r["ts"], r["pressure"])
        for r in conn.execute(
            "SELECT ts, pressure FROM samples WHERE test_id = ? ORDER BY rowid",
            (test_id,),
        ).fetchall()
    ]
    blocks = [
        tuple(r[c] for c in r.keys() if c not in ("test_id", "first_rowid", "last_rowid"))
        for r in conn.execute(
            "SELECT level, block_index, start_ts, end_ts, sample_count, "
            "first_ts, last_ts, first_pressure, last_pressure, max_pressure, "
            "max_pressure_ts, min_pressure, min_pressure_ts "
            "FROM blocks WHERE test_id = ? ORDER BY level, block_index",
            (test_id,),
        ).fetchall()
    ]
    test = dict(
        conn.execute("SELECT * FROM tests WHERE id = ?", (test_id,)).fetchone()
    )
    conn.close()
    return samples, blocks, test


# ---------------------------------------------------------------- 基本接续


@pytest.fixture(scope="module")
def base():
    # 10 点，block_size=4：level1=[4,4,2]，level2 1 块
    rows = [
        (1000 + i * 100, float(p))
        for i, p in enumerate([10, 12, 8, 15, 9, 14, 7, 20, 11, 6])
    ]
    return _import("append-base", rows), rows


def test_append_overlap_matched_and_new_points(base):
    body, rows = base
    test_id = body["id"]
    # 末尾 3 点重叠且压力一致，再接 5 个新点
    cont = rows[-3:] + [(2000 + i * 100, float(p)) for i, p in enumerate([13, 18, 5, 22, 9])]
    resp = _append(test_id, cont)
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["appended_count"] == 5
    assert out["overlap_matched"] == 3
    assert out["sample_count"] == 15
    assert out["end_ts"] == 2400
    # 重叠行没有重复入库
    samples, _, _ = _snapshot(test_id)
    assert len(samples) == 15
    assert samples[-1] == (2400, 9.0)


def test_query_after_append_sees_complete_series(base):
    body, _ = base
    resp = client.get(
        f"/tests/{body['id']}/query",
        params={"start_ts": 1000, "end_ts": 2500, "bucket_width_ms": 400},
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert sum(b["sample_count"] for b in out["buckets"]) == 15
    # 极值对拍：全程最高点 22@2300，最低点 5@2200
    all_pts = [pt for b in out["buckets"] for pt in b["points"]]
    max_pt = max(all_pts, key=lambda x: x["pressure"])
    min_pt = min(all_pts, key=lambda x: x["pressure"])
    assert (max_pt["ts"], max_pt["pressure"]) == (2300, 22.0)
    assert (min_pt["ts"], min_pt["pressure"]) == (2200, 5.0)


# ---------------------------------------------------------------- 退回语义


def test_append_pressure_mismatch_rejected_and_unchanged(base):
    body, rows = base
    test_id = body["id"]
    before = _snapshot(test_id)
    # 重叠时刻压力被改写 -> 整份退回
    bad = list(rows[-2:])
    bad[0] = (bad[0][0], 999.0)
    bad += [(3000, 1.0), (3100, 2.0)]
    resp = _append(test_id, bad)
    assert resp.status_code == 400
    assert "对不上" in resp.json()["detail"]
    after = _snapshot(test_id)
    assert after == before  # 采样、摘要、试验元信息全部不变


def test_append_missing_overlap_ts_rejected_and_unchanged(base):
    body, _ = base
    test_id = body["id"]
    before = _snapshot(test_id)
    # 1500 是库内已有时刻但文件给了不同压力；并跳过库内 1800（缺口）也应退回
    resp = _append(test_id, [(1500, 999.0), (4000, 3.0)])
    assert resp.status_code == 400
    assert _snapshot(test_id) == before


def test_append_bad_row_rolls_back(base):
    body, _ = base
    test_id = body["id"]
    before = _snapshot(test_id)
    bad = b"timestamp_ms,pressure_kpa\n2000,1.0\n2100,oops\n2200,3.0\n"
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("bad.csv", bad, "text/csv")},
    )
    assert resp.status_code == 400
    assert _snapshot(test_id) == before


def test_append_non_increasing_file_rejected(base):
    body, _ = base
    test_id = body["id"]
    before = _snapshot(test_id)
    resp = _append(test_id, [(5000, 1.0), (4900, 2.0)])
    assert resp.status_code == 400
    assert _snapshot(test_id) == before


def test_append_unknown_test_404():
    resp = _append(99999, [(1, 1.0), (2, 2.0)])
    assert resp.status_code == 404


def test_append_empty_file_rejected(base):
    body, _ = base
    resp = client.post(
        f"/tests/{body['id']}/append",
        files={"file": ("empty.csv", b"", "text/csv")},
    )
    assert resp.status_code == 400


def test_append_fully_overlapping_is_noop(base):
    body, rows = base
    test_id = body["id"]
    before = _snapshot(test_id)
    resp = _append(test_id, rows[:4])  # 全部不晚于末点且逐点匹配
    assert resp.status_code == 200
    out = resp.json()
    assert out["appended_count"] == 0
    assert out["overlap_matched"] == 4
    assert out["sample_count"] == before[2]["sample_count"]
    assert _snapshot(test_id) == before


# ---------------------------------------------------------------- 增量摘要等价性


def test_incremental_blocks_equal_full_rebuild(base):
    body, rows = base
    test_id = body["id"]
    # 再来一段：制造“尾部半块补齐 + 新块 + 多层重建”
    cont = [(2500 + i * 100, float(p)) for i, p in enumerate([3, 19, 17, 25, 4, 16])]
    resp = _append(test_id, cont)
    assert resp.status_code == 200, resp.text

    inc_samples, inc_blocks, inc_test = _snapshot(test_id)

    # 用同一份全量数据在独立临时库（独立连接，不改全局配置）从零构建，作为对拍基准
    import sqlite3

    ref_db = os.path.join(_tmp.name, f"rebuild_{test_id}.db")
    for suffix in ("", "-wal", "-shm"):
        p = ref_db + suffix
        if os.path.exists(p):
            os.remove(p)
    ref_conn = sqlite3.connect(ref_db)
    ref_conn.row_factory = sqlite3.Row
    rid = 999
    ref_conn.execute(
        "CREATE TABLE tests (id INTEGER PRIMARY KEY, name TEXT, source_filename TEXT, "
        "sample_count INTEGER, start_ts INTEGER, end_ts INTEGER, max_level INTEGER, "
        "imported_at TEXT DEFAULT (datetime('now')))"
    )
    ref_conn.execute(
        "CREATE TABLE samples (test_id INTEGER, ts INTEGER, pressure REAL)"
    )
    ref_conn.execute(
        "CREATE TABLE blocks (test_id INTEGER, level INTEGER, block_index INTEGER, "
        "start_ts INTEGER, end_ts INTEGER, sample_count INTEGER, first_ts INTEGER, "
        "last_ts INTEGER, first_pressure REAL, last_pressure REAL, max_pressure REAL, "
        "max_pressure_ts INTEGER, min_pressure REAL, min_pressure_ts INTEGER, "
        "first_rowid INTEGER, last_rowid INTEGER)"
    )
    ref_conn.execute(
        "INSERT INTO tests (id, name, source_filename, sample_count, start_ts, end_ts, max_level) "
        "VALUES (?, 'ref', 'r.csv', ?, ?, ?, 0)",
        (rid, len(inc_samples), inc_samples[0][0], inc_samples[-1][0]),
    )
    ref_conn.executemany(
        "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
        [(rid, ts, p) for ts, p in inc_samples],
    )
    max_level = build_summaries(ref_conn, rid, 4)
    ref_conn.commit()
    ref_blocks = [
        tuple(r[c] for c in r.keys() if c not in ("test_id", "first_rowid", "last_rowid"))
        for r in ref_conn.execute(
            "SELECT level, block_index, start_ts, end_ts, sample_count, "
            "first_ts, last_ts, first_pressure, last_pressure, max_pressure, "
            "max_pressure_ts, min_pressure, min_pressure_ts "
            "FROM blocks WHERE test_id = ? ORDER BY level, block_index",
            (rid,),
        ).fetchall()
    ]
    ref_conn.close()

    assert inc_blocks == ref_blocks
    assert max_level == inc_test["max_level"]
    assert inc_test["sample_count"] == len(inc_samples)


def test_level_growth_on_append():
    # 4 点恰好 1 个 level1 块、max_level=1；追加后 level1 变 2 块，需要加层
    body = _import("append-grow", [(i, float(i)) for i in range(4)])
    assert body["max_level"] == 1 and body["block_counts"] == {"1": 1}
    resp = _append(body["id"], [(4, 4.0), (5, 5.0), (6, 6.0), (7, 7.0)])
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["appended_count"] == 4
    assert out["max_level"] == 2
    assert out["block_counts"] == {"1": 2, "2": 1}
    # 再加 8 点：level1=4 块 -> level2 恰好 1 块（4 个子块一收），仍是 2 层
    resp2 = _append(body["id"], [(8 + i, float(8 + i)) for i in range(8)])
    out2 = resp2.json()
    assert out2["max_level"] == 2
    assert out2["block_counts"] == {"1": 4, "2": 1}
    # 再补 4 点：level1=5 块 -> level2=2 块 -> 需要 level3 收束
    resp3 = _append(body["id"], [(16 + i, float(16 + i)) for i in range(4)])
    out3 = resp3.json()
    assert out3["max_level"] == 3
    assert out3["block_counts"] == {"1": 5, "2": 2, "3": 1}
    # 全区间峰谷与原始数据一致（共 20 点，ts 0..19）
    q = client.get(
        f"/tests/{body['id']}/query",
        params={"start_ts": 0, "end_ts": 20, "bucket_width_ms": 20},
    ).json()
    pts = q["buckets"][0]["points"]
    assert q["buckets"][0]["sample_count"] == 20
    assert "first" in pts[0]["roles"] and pts[0]["ts"] == 0
    assert "last" in pts[-1]["roles"] and pts[-1]["ts"] == 19
    assert q["metrics"]["raw_rows_read"] == 0  # 整段都能走摘要


def test_tail_partial_block_fill_then_rebuild():
    # 6 点：level1=[4,2]（尾部半块），max_level=2
    body = _import("append-partial", [(i * 10, float(i)) for i in range(6)])
    assert body["block_counts"] == {"1": 2, "2": 1}
    # 追加 2 点：先把半块补成 4，再新增 1 个满块 -> level1=[4,4]
    resp = _append(body["id"], [(60, 6.0), (70, 7.0)])
    out = resp.json()
    assert out["appended_count"] == 2
    assert out["block_counts"] == {"1": 2, "2": 1}
    samples, blocks, _ = _snapshot(body["id"])
    assert [s[0] for s in samples] == [i * 10 for i in range(8)]
    # level1 的 1 号块现在是完整 4 点，区间 40..70
    l1_1 = [b for b in blocks if b[0] == 1 and b[1] == 1][0]
    assert l1_1[4] == 4 and l1_1[2] == 40 and l1_1[3] == 70


def test_randomized_append_ranges_match_bruteforce():
    """接续后对随机区间做摘要查询，结果必须与原始采样扫描一致。"""
    import random

    body = _import(
        "append-random",
        [(100 + i, float((i * 7) % 13 - 6)) for i in range(17)],
    )
    test_id = body["id"]
    _append(test_id, [(100 + 17 + i, float((i * 5) % 11 - 5)) for i in range(23)])
    conn = connect()
    test = conn.execute("SELECT * FROM tests WHERE id = ?", (test_id,)).fetchone()
    raw = [
        (r["ts"], r["pressure"])
        for r in conn.execute(
            "SELECT ts, pressure FROM samples WHERE test_id = ? ORDER BY ts",
            (test_id,),
        ).fetchall()
    ]
    rng = random.Random(20261112)
    lo, hi = raw[0][0], raw[-1][0]
    for _ in range(150):
        qs = rng.randint(lo - 30, hi + 30)
        qe = rng.randint(qs + 1, qs + 40)
        from app.summaries import range_summary

        st = range_summary(conn, test_id, qs, qe, test["max_level"], 4)
        expected = [(ts, p) for ts, p in raw if qs <= ts < qe]
        if not expected:
            assert st is None or st.sample_count == 0
            continue
        assert st.sample_count == len(expected)
        assert (st.first_ts, st.first_pressure) == expected[0]
        assert (st.last_ts, st.last_pressure) == expected[-1]
        mx = max(expected, key=lambda x: (x[1], -x[0]))
        mn = min(expected, key=lambda x: (x[1], x[0]))
        assert (st.max_pressure_ts, st.max_pressure) == mx
        assert (st.min_pressure_ts, st.min_pressure) == mn
    conn.close()


def test_append_and_query_share_transaction_semantics(base):
    """会话级连接单事务：失败回滚后查询仍只见接续前的完整结果。"""
    body, rows = base
    test_id = body["id"]
    current = client.get(f"/tests/{test_id}").json()
    bad = list(rows[-2:])
    bad[0] = (bad[0][0], 42.0)
    bad += [(9000, 1.0)]
    assert _append(test_id, bad).status_code == 400
    resp = client.get(
        f"/tests/{test_id}/query",
        params={"start_ts": 1000, "end_ts": 9100, "bucket_width_ms": 8000},
    )
    out = resp.json()
    # 失败的接续不留任何点，9000 时刻不可见，计数与失败前一致
    assert sum(b["sample_count"] for b in out["buckets"]) == current["sample_count"]
    assert all(pt["ts"] < 9000 for b in out["buckets"] for pt in b["points"])


def test_append_accepts_chinese_header_and_gbk(base):
    body, _ = base
    test_id = body["id"]
    text = "时间戳,压力(kPa)\n5000,12.5\n5100,13.0\n"
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("gbk.csv", text.encode("gbk"), "text/csv")},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["appended_count"] == 2
