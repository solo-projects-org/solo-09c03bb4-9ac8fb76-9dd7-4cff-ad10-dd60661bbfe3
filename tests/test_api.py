"""端到端与单元测试：导入校验、分桶语义、极值并列、缺测、摘要命中策略。"""

from __future__ import annotations

import io
import os
import sqlite3
import tempfile

import pytest

# 测试数据库与块大小必须在导入 app 之前固定
_tmp = tempfile.TemporaryDirectory()
os.environ["PUMP_DB_PATH"] = os.path.join(_tmp.name, "test.db")
os.environ["PUMP_BLOCK_SIZE"] = "4"
os.environ["PUMP_MAX_BUCKETS"] = "1000"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import connect, init_db  # noqa: E402
from app.ingest import CsvValidationError, parse_csv  # noqa: E402
from app.main import app  # noqa: E402
from app.summaries import build_summaries, range_summary  # noqa: E402

init_db()
client = TestClient(app)


def _csv(rows, header=("timestamp_ms", "pressure_kpa")) -> bytes:
    buf = io.StringIO()
    buf.write(",".join(header) + "\n")
    for ts, p in rows:
        buf.write(f"{int(ts)},{p}\n")
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------- 导入校验

def test_parse_rejects_non_strict_increasing():
    with pytest.raises(CsvValidationError, match="严格递增"):
        parse_csv(_csv([(1, 10.0), (2, 11.0), (2, 12.0)]))


def test_parse_rejects_decreasing():
    with pytest.raises(CsvValidationError):
        parse_csv(_csv([(5, 10.0), (4, 11.0)]))


def test_parse_rejects_float_timestamp():
    with pytest.raises(CsvValidationError, match="整数毫秒"):
        parse_csv(b"timestamp_ms,pressure_kpa\n1.5,10.0\n")


def test_parse_rejects_non_finite_pressure():
    with pytest.raises(CsvValidationError, match="有限值"):
        parse_csv(_csv([(1, "nan")]))


def test_parse_rejects_missing_column():
    with pytest.raises(CsvValidationError, match="表头"):
        parse_csv(b"ts,foo\n1,10\n")


def test_parse_accepts_chinese_header():
    samples = parse_csv("时间戳,压力(kPa)\n1,10.5\n2,11\n".encode())
    assert samples == [(1, 10.5), (2, 11.0)]


# ---------------------------------------------------------------- 端到端 API

@pytest.fixture(scope="module")
def imported():
    # 10 个点，block_size=4：level1=[4,4,2]，level2=[4,4,2]
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15, 9, 14, 7, 20, 11, 6])]
    resp = client.post(
        "/tests/import",
        data={"name": "unit-test-1"},
        files={"file": ("u.csv", _csv(rows), "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_import_summary_levels(imported):
    body = imported
    assert body["sample_count"] == 10
    assert body["max_level"] == 2
    assert body["block_counts"] == {"1": 3, "2": 1}
    assert body["start_ts"] == 1000 and body["end_ts"] == 1900


def test_duplicate_name_rejected(imported):
    rows = [(1, 1.0), (2, 2.0)]
    resp = client.post(
        "/tests/import",
        data={"name": "unit-test-1"},
        files={"file": ("u.csv", _csv(rows), "text/csv")},
    )
    assert resp.status_code == 400


def test_bucket_aligned_with_blocks_uses_only_summaries(imported):
    # 桶边界与 level1 块完全对齐：不应读取任何原始采样
    resp = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 1000, "end_ts": 1800, "bucket_width_ms": 400},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["bucket_count"] == 2
    assert body["metrics"]["raw_rows_read"] == 0
    assert body["metrics"]["blocks_used"] >= 1

    b0, b1 = body["buckets"]
    # 桶0 = 点 10,12,8,15
    assert b0["sample_count"] == 4 and b0["missing"] is False
    roles0 = {pt["ts"]: pt["roles"] for pt in b0["points"]}
    assert "first" in roles0[1000] and "last" in roles0[1300]
    assert "max" in roles0[1300] and "min" in roles0[1200]
    # 时间戳 1300 同时是 last 与 max，只列一次、角色合并
    assert roles0[1300].count("max") == 1
    # 点按时序排列
    assert [pt["ts"] for pt in b0["points"]] == sorted(pt["ts"] for pt in b0["points"])
    # 桶1 = 点 9,14,7,20
    max_pt = next(pt for pt in b1["points"] if "max" in pt["roles"])
    min_pt = next(pt for pt in b1["points"] if "min" in pt["roles"])
    assert max_pt["ts"] == 1700 and max_pt["pressure"] == 20.0
    assert min_pt["ts"] == 1600 and min_pt["pressure"] == 7.0


def test_misaligned_bucket_reads_raw_only_at_edges(imported):
    # 桶宽 200ms（2 点/桶），桶边界切开所有 level1 块：仅边界块读原始采样
    resp = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 1050, "end_ts": 1850, "bucket_width_ms": 200},
    )
    body = resp.json()
    assert resp.status_code == 200
    assert body["bucket_count"] == 4
    # [1050,1850) 含 8 点（1000 与 1900 在半开区间外）
    assert sum(b["sample_count"] for b in body["buckets"]) == 8
    # 所有点都落在被切开的 level1 块中 -> 必然回退原始采样
    assert body["metrics"]["raw_rows_read"] == 8

    first = body["buckets"][0]
    # [1050,1250) 只含 ts=1100(12), 1200(8)
    assert first["sample_count"] == 2
    assert first["points"][0]["ts"] == 1100
    assert "min" in first["points"][1]["roles"]


def test_last_bucket_clipped_to_end(imported):
    # 300ms 桶宽：[1800,2100) 末桶截到 end=1950 -> [1800,1950)，仅含 1800 一点
    # （1900 本在区间内；再用 end=1900 验证右开边界排除末点）
    resp = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 1800, "end_ts": 1950, "bucket_width_ms": 300},
    )
    body = resp.json()
    assert body["bucket_count"] == 1
    b = body["buckets"][0]
    assert b["end_ts"] == 1950
    assert b["sample_count"] == 2
    assert [pt["ts"] for pt in b["points"]] == [1800, 1900]

    resp2 = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 1800, "end_ts": 1900, "bucket_width_ms": 300},
    )
    b2 = resp2.json()["buckets"][0]
    assert b2["sample_count"] == 1
    assert [pt["ts"] for pt in b2["points"]] == [1800]


def test_empty_buckets_marked_missing(imported):
    # 数据时间范围 1000..1900，查询前面的空白时段
    resp = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 0, "end_ts": 600, "bucket_width_ms": 200},
    )
    body = resp.json()
    assert body["bucket_count"] == 3
    assert body["missing_bucket_count"] == 3
    assert all(b["missing"] and b["points"] == [] for b in body["buckets"])


def test_mixed_missing_and_data_buckets(imported):
    resp = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 800, "end_ts": 1200, "bucket_width_ms": 200},
    )
    body = resp.json()
    # [800,1000) 空，[1000,1200) 含 ts=1000、1100 两点（1200 属右开边界外）
    assert body["buckets"][0]["missing"] is True
    last = body["buckets"][1]
    assert last["missing"] is False and last["sample_count"] == 2
    first_pt = last["points"][0]
    assert first_pt["ts"] == 1000 and "first" in first_pt["roles"]


def test_single_point_bucket_merges_all_roles(imported):
    # [1000,1100) 只含一个点：首/尾/最高/最低同点，只列一次、角色合并
    resp = client.get(
        f"/tests/{imported['id']}/query",
        params={"start_ts": 1000, "end_ts": 1100, "bucket_width_ms": 100},
    )
    body = resp.json()
    bucket = body["buckets"][0]
    assert bucket["sample_count"] == 1 and len(bucket["points"]) == 1
    assert set(bucket["points"][0]["roles"]) == {"first", "last", "max", "min"}


def test_tie_keeps_earliest_extreme():
    rows = [(i, 5.0) for i in range(8)]  # 压力全部相同
    conn = connect()
    rid = conn.execute(
        "INSERT INTO tests (name, source_filename, sample_count, start_ts, end_ts, max_level) "
        "VALUES ('tie', 't.csv', 8, 0, 7, 0)"
    ).lastrowid
    conn.executemany(
        "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
        [(rid, ts, p) for ts, p in rows],
    )
    build_summaries(conn, rid, 4)
    conn.commit()
    st = range_summary(conn, rid, 0, 8, max_level=2)
    assert st.max_pressure_ts == 0 and st.min_pressure_ts == 0  # 并列取最早
    # 全区间完整覆盖 level1 块，不读原始采样
    st2_metrics = {"blocks_used": 0, "blocks_by_level": {}, "raw_rows_read": 0}
    range_summary(conn, rid, 0, 8, 2, metrics=st2_metrics)
    assert st2_metrics["raw_rows_read"] == 0
    conn.close()


def test_range_summary_matches_raw_bruteforce(imported):
    """随机区间：摘要策略结果必须与直接扫描原始采样一致。"""
    conn = connect()
    test_id = imported["id"]
    max_level = imported["max_level"]
    raw_rows = conn.execute(
        "SELECT ts, pressure FROM samples WHERE test_id = ? ORDER BY ts", (test_id,)
    ).fetchall()

    for q_start, q_end in [(1050, 1450), (999, 1001), (1000, 1900), (1250, 1750), (1500, 1600)]:
        metrics = {"blocks_used": 0, "blocks_by_level": {}, "raw_rows_read": 0}
        st = range_summary(conn, test_id, q_start, q_end, max_level, 4, metrics)
        expected = [
            (r["ts"], r["pressure"])
            for r in raw_rows
            if q_start <= r["ts"] < q_end
        ]
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


def test_validation_errors():
    assert client.get("/tests/999/query", params={
        "start_ts": 0, "end_ts": 10, "bucket_width_ms": 5
    }).status_code == 404

    rid = client.get("/tests").json()[0]["id"]
    bad = client.get(f"/tests/{rid}/query", params={
        "start_ts": 100, "end_ts": 100, "bucket_width_ms": 5
    })
    assert bad.status_code == 400

    bad_width = client.get(f"/tests/{rid}/query", params={
        "start_ts": 0, "end_ts": 10, "bucket_width_ms": 0
    })
    assert bad_width.status_code == 422


def test_randomized_ranges_match_bruteforce(imported):
    """固定种子随机区间对拍：多级摘要递归结果必须始终等于原始扫描。"""
    import random

    conn = connect()
    test_id = imported["id"]
    max_level = imported["max_level"]
    raw_rows = conn.execute(
        "SELECT ts, pressure FROM samples WHERE test_id = ? ORDER BY ts",
        (test_id,),
    ).fetchall()
    rng = random.Random(20261111)
    lo_ts, hi_ts = raw_rows[0]["ts"], raw_rows[-1]["ts"]

    for _ in range(200):
        q_start = rng.randint(lo_ts - 500, hi_ts + 500)
        q_end = rng.randint(q_start + 1, q_start + 1500)
        metrics = {"blocks_used": 0, "blocks_by_level": {}, "raw_rows_read": 0}
        st = range_summary(conn, test_id, q_start, q_end, max_level, 4, metrics)
        expected = [
            (r["ts"], r["pressure"])
            for r in raw_rows
            if q_start <= r["ts"] < q_end
        ]
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


def test_bucket_count_cap():
    resp = client.get(
        f"/tests/{client.get('/tests').json()[0]['id']}/query",
        params={"start_ts": 0, "end_ts": 1_000_000, "bucket_width_ms": 1},
    )
    assert resp.status_code == 400
    assert "上限" in resp.json()["detail"]


# ---------------------------------------------------------------- 接续导入

def _import_rows(name, rows):
    resp = client.post(
        "/tests/import",
        data={"name": name},
        files={"file": ("f.csv", _csv(rows), "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_append_overlap_dedupes_and_appends_only_new_points():
    # 10 点（同 imported fixture 的形状），block_size=4
    pressures = [10, 12, 8, 15, 9, 14, 7, 20, 11, 6]
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate(pressures)]
    body = _import_rows("append-1", rows)
    test_id = body["id"]
    assert body["sample_count"] == 10

    # 接续文件：末 4 点重叠 + 6 个新点
    cont = rows[-4:] + [(2000 + i * 100, float(p)) for i, p in enumerate([13, 18, 5, 22, 9, 17])]
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("part2.csv", _csv(cont), "text/csv")},
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["appended_count"] == 6
    assert out["overlap_count"] == 4
    assert out["sample_count"] == 16
    assert out["start_ts"] == 1000 and out["end_ts"] == 2500

    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM samples WHERE test_id = ?", (test_id,)
    ).fetchone()[0]
    assert n == 16  # 重叠行未重复入库
    # 重叠区仍只有一份原始数据
    dup = conn.execute(
        "SELECT ts, COUNT(*) FROM samples WHERE test_id = ? GROUP BY ts HAVING COUNT(*) > 1",
        (test_id,),
    ).fetchall()
    assert dup == []
    conn.close()

    # 接续后按原编号查询，能看到完整 16 点的峰谷
    q = client.get(
        f"/tests/{test_id}/query",
        params={"start_ts": 1000, "end_ts": 2600, "bucket_width_ms": 200},
    )
    assert q.status_code == 200
    qb = q.json()
    assert sum(b["sample_count"] for b in qb["buckets"]) == 16
    all_pts = [pt for b in qb["buckets"] for pt in b["points"]]
    max_pt = max(all_pts, key=lambda p: p["pressure"])
    min_pt = min(all_pts, key=lambda p: p["pressure"])
    assert max_pt["pressure"] == 22.0 and max_pt["ts"] == 2300
    assert min_pt["pressure"] == 5.0 and min_pt["ts"] == 2200


def test_append_rejects_overlap_pressure_mismatch_and_keeps_test_unchanged():
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15, 9, 14])]
    body = _import_rows("append-2", rows)
    test_id = body["id"]

    # 1400 库内是 9.0，这里给 99.0：同毫秒压力对不上，且 1400 不晚于末点 1500
    bad = [(1300, 15.0), (1400, 99.0), (1700, 5.0), (1800, 3.0)]
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("bad.csv", _csv(bad), "text/csv")},
    )
    assert resp.status_code == 400
    assert "1400" in resp.json()["detail"]

    # 原试验完全不变
    after = client.get(f"/tests/{test_id}").json()
    assert after["sample_count"] == 6 and after["end_ts"] == 1500
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM samples WHERE test_id = ?", (test_id,)
    ).fetchone()[0]
    block_rows = conn.execute(
        "SELECT COUNT(*) FROM blocks WHERE test_id = ?", (test_id,)
    ).fetchone()[0]
    levels = conn.execute(
        "SELECT level, COUNT(*) FROM blocks WHERE test_id = ? GROUP BY level",
        (test_id,),
    ).fetchall()
    conn.close()
    assert n == 6
    # 初始 level1=[4,2]、level2=1；退回后一块不多一块不少
    assert {r[0]: r[1] for r in levels} == {1: 2, 2: 1}


def test_append_rejects_overlap_ts_missing_in_db():
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15, 9, 14])]
    body = _import_rows("append-3", rows)
    # 含一个库内不存在、但又不晚于末点 1500 的时刻
    ghost = [(1400, 9.0), (1450, 99.0), (1600, 5.0)]
    resp = client.post(
        f"/tests/{body['id']}/append",
        files={"file": ("ghost.csv", _csv(ghost), "text/csv")},
    )
    assert resp.status_code == 400
    assert "1450" in resp.json()["detail"]
    assert client.get(f"/tests/{body['id']}").json()["sample_count"] == 6


def test_append_rejects_bad_rows_and_keeps_test_unchanged():
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15])]
    body = _import_rows("append-4", rows)
    test_id = body["id"]

    # 坏行：非整数毫秒时间戳
    bad = b"timestamp_ms,pressure_kpa\n1400,15.0\n1500,9.5\n1.5,3.0\n"
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("bad.csv", bad, "text/csv")},
    )
    assert resp.status_code == 400 and "整数毫秒" in resp.json()["detail"]
    assert client.get(f"/tests/{test_id}").json()["sample_count"] == 4

    # 坏行：非有限压力
    bad2 = b"timestamp_ms,pressure_kpa\n1400,15.0\n1500,nan\n"
    resp2 = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("bad2.csv", bad2, "text/csv")},
    )
    assert resp2.status_code == 400 and "有限值" in resp2.json()["detail"]
    assert client.get(f"/tests/{test_id}").json()["sample_count"] == 4

    # 坏行：文件内时间戳未严格递增
    bad3 = _csv([(1400, 15.0), (1399, 8.0)])
    resp3 = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("bad3.csv", bad3, "text/csv")},
    )
    assert resp3.status_code == 400 and "严格递增" in resp3.json()["detail"]
    assert client.get(f"/tests/{test_id}").json()["sample_count"] == 4


def test_append_without_overlap_starts_right_after_end():
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15])]
    body = _import_rows("append-5", rows)
    cont = [(1500 + i * 100, float(p)) for i, p in enumerate([9, 14, 7])]
    resp = client.post(
        f"/tests/{body['id']}/append",
        files={"file": ("cont.csv", _csv(cont), "text/csv")},
    )
    assert resp.status_code == 200
    out = resp.json()
    assert out["appended_count"] == 3 and out["overlap_count"] == 0
    assert out["sample_count"] == 7 and out["end_ts"] == 1700


def test_append_fully_overlapping_file_adds_nothing():
    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15, 9])]
    body = _import_rows("append-6", rows)
    test_id = body["id"]
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("same.csv", _csv(rows), "text/csv")},
    )
    assert resp.status_code == 200
    out = resp.json()
    assert out["appended_count"] == 0 and out["overlap_count"] == 5
    assert out["sample_count"] == 5 and out["end_ts"] == 1400
    conn = connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM samples WHERE test_id = ?", (test_id,)
    ).fetchone()[0]
    conn.close()
    assert n == 5


def test_append_adds_summary_level_when_top_level_splits():
    # 初始 4 点：level1=1 块，max_level=1；接续 13 点 -> 共 17 点，
    # level1=[4,4,4,4,1] 共 5 块 -> 需要 level2=[4,1] 共 2 块 -> 再加 level3=1
    rows = [(i * 100, float(100 + i)) for i in range(4)]
    body = _import_rows("append-7", rows)
    assert body["max_level"] == 1 and body["block_counts"] == {"1": 1}

    cont = [((4 + i) * 100, float(100 + 4 + i)) for i in range(13)]
    resp = client.post(
        f"/tests/{body['id']}/append",
        files={"file": ("cont.csv", _csv(cont), "text/csv")},
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["appended_count"] == 13 and out["sample_count"] == 17
    assert out["max_level"] == 3
    assert out["block_counts"] == {"1": 5, "2": 2, "3": 1}


def test_append_summary_equivalent_to_full_rebuild():
    """接续后的摘要与“一次性全量导入再建摘要”逐块等价。"""
    first = [(i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15, 9, 14, 7])]
    body = _import_rows("append-8", first)
    test_id = body["id"]
    cont = [
        (700 + i * 100, float(p))
        for i, p in enumerate([20, 11, 6, 13, 18, 5, 22, 9, 17, 4])
    ]
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("cont.csv", _csv(cont), "text/csv")},
    )
    assert resp.status_code == 200, resp.text

    # 对照组：同样 17 点一次性导入
    all_rows = first + cont
    ref = _import_rows("append-8-ref", all_rows)
    ref_id = ref["id"]

    conn = connect()
    max_level = conn.execute(
        "SELECT max_level FROM tests WHERE id = ?", (test_id,)
    ).fetchone()[0]
    ref_level = conn.execute(
        "SELECT max_level FROM tests WHERE id = ?", (ref_id,)
    ).fetchone()[0]
    assert max_level == ref_level
    for level in range(1, max_level + 1):
        got = conn.execute(
            "SELECT block_index, sample_count, start_ts, end_ts, first_ts, last_ts, "
            "first_pressure, last_pressure, max_pressure, max_pressure_ts, "
            "min_pressure, min_pressure_ts, first_rowid, last_rowid "
            "FROM blocks WHERE test_id = ? AND level = ? ORDER BY block_index",
            (test_id, level),
        ).fetchall()
        want = conn.execute(
            "SELECT block_index, sample_count, start_ts, end_ts, first_ts, last_ts, "
            "first_pressure, last_pressure, max_pressure, max_pressure_ts, "
            "min_pressure, min_pressure_ts, first_rowid, last_rowid "
            "FROM blocks WHERE test_id = ? AND level = ? ORDER BY block_index",
            (ref_id, level),
        ).fetchall()
        assert len(got) == len(want)
        for g, w in zip(got, want):
            # rowid 绝对值因两个试验不同而不同，比较跨度；其余字段必须完全一致
            for key in g.keys():
                if key in ("first_rowid", "last_rowid"):
                    continue
                assert g[key] == w[key], (level, key, g[key], w[key])
            assert (g["last_rowid"] - g["first_rowid"]) == (
                w["last_rowid"] - w["first_rowid"]
            )

    # 随机区间对拍：接续试验的 range_summary 与原始扫描一致
    import random

    raw_rows = conn.execute(
        "SELECT ts, pressure FROM samples WHERE test_id = ? ORDER BY ts", (test_id,)
    ).fetchall()
    rng = random.Random(20261112)
    lo_ts, hi_ts = raw_rows[0]["ts"], raw_rows[-1]["ts"]
    for _ in range(100):
        q_start = rng.randint(lo_ts - 300, hi_ts + 300)
        q_end = rng.randint(q_start + 1, q_start + 1200)
        st = range_summary(conn, test_id, q_start, q_end, max_level, 4)
        expected = [
            (r["ts"], r["pressure"])
            for r in raw_rows
            if q_start <= r["ts"] < q_end
        ]
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


def test_append_unknown_test_returns_404():
    resp = client.post(
        "/tests/99999/append",
        files={"file": ("x.csv", _csv([(1, 1.0)]), "text/csv")},
    )
    assert resp.status_code == 404


def test_append_does_not_rescan_old_samples(tmp_path):
    """append_summaries 只按 rowid > 旧末点 读取新采样，不扫描旧采样区间。"""
    from app.summaries import append_summaries, build_summaries

    db_file = tmp_path / "rescan.db"
    c = sqlite3.connect(db_file)
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE tests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
            source_filename TEXT NOT NULL, sample_count INTEGER NOT NULL,
            start_ts INTEGER NOT NULL, end_ts INTEGER NOT NULL,
            max_level INTEGER NOT NULL, imported_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE samples (test_id INTEGER NOT NULL, ts INTEGER NOT NULL, pressure REAL NOT NULL);
        CREATE TABLE blocks (
            test_id INTEGER NOT NULL, level INTEGER NOT NULL, block_index INTEGER NOT NULL,
            start_ts INTEGER NOT NULL, end_ts INTEGER NOT NULL, sample_count INTEGER NOT NULL,
            first_ts INTEGER NOT NULL, last_ts INTEGER NOT NULL, first_pressure REAL NOT NULL,
            last_pressure REAL NOT NULL, max_pressure REAL NOT NULL, max_pressure_ts INTEGER NOT NULL,
            min_pressure REAL NOT NULL, min_pressure_ts INTEGER NOT NULL,
            first_rowid INTEGER NOT NULL, last_rowid INTEGER NOT NULL,
            PRIMARY KEY (test_id, level, block_index)
        );
        """
    )
    c.execute(
        "INSERT INTO tests (name, source_filename, sample_count, start_ts, end_ts, max_level) "
        "VALUES ('r', 'f.csv', 8, 0, 700, 0)"
    )
    tid = c.execute("SELECT id FROM tests").fetchone()[0]
    c.executemany(
        "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
        [(tid, i * 100, float(100 + i)) for i in range(8)],
    )
    build_summaries(c, tid, 4)
    old_last_rid = c.execute(
        "SELECT last_rowid FROM blocks WHERE test_id=? AND level=1 "
        "ORDER BY block_index DESC LIMIT 1",
        (tid,),
    ).fetchone()[0]
    c.executemany(
        "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
        [(tid, (8 + i) * 100, 1.0 + i) for i in range(5)],
    )
    c.commit()

    sample_scans: list[tuple] = []

    class _SpyingConn(sqlite3.Connection):
        def execute(self, sql, params=()):
            if "from samples" in sql.lower():
                sample_scans.append((sql, tuple(params)))
            return super().execute(sql, params)

    spy = sqlite3.connect(db_file, factory=_SpyingConn)
    spy.row_factory = sqlite3.Row
    append_summaries(spy, tid, 4)  # 内部全部走 spy.execute
    spy.commit()
    spy.close()

    # 对 samples 表的访问只有一次，且是 rowid > 旧末点 的新采样查询
    assert len(sample_scans) == 1
    sql, params = sample_scans[0]
    assert "rowid > ?" in sql
    assert params == (tid, old_last_rid)
    c.close()


def test_append_filling_only_the_partial_tail_block_refreshes_parents():
    """新采样恰好填满尾部半块、不产生新 level1 块时，上层父块也必须同步刷新。"""
    first = [(i * 100, float(100 + i)) for i in range(6)]  # level1=[4,2]，level2=1
    body = _import_rows("append-12", first)
    test_id = body["id"]
    cont = [(600, 500.0), (700, 600.0)]  # 恰好 2 点，把尾部 [4,2] 填成 [4,4]
    resp = client.post(
        f"/tests/{test_id}/append",
        files={"file": ("fill.csv", _csv(cont), "text/csv")},
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["appended_count"] == 2 and out["sample_count"] == 8
    assert out["block_counts"] == {"1": 2, "2": 1}  # 没新增块，但 level2 统计已刷新

    # 与 8 点全量导入对拍，level2 块必须反映新极值（500/600 远超原压力）
    ref = _import_rows("append-12-ref", first + cont)
    conn = connect()
    try:
        for level in (1, 2):
            g = conn.execute(
                "SELECT block_index, sample_count, start_ts, end_ts, "
                "max_pressure, max_pressure_ts, min_pressure, min_pressure_ts "
                "FROM blocks WHERE test_id=? AND level=? ORDER BY block_index",
                (test_id, level),
            ).fetchall()
            w = conn.execute(
                "SELECT block_index, sample_count, start_ts, end_ts, "
                "max_pressure, max_pressure_ts, min_pressure, min_pressure_ts "
                "FROM blocks WHERE test_id=? AND level=? ORDER BY block_index",
                (ref["id"], level),
            ).fetchall()
            assert [dict(r) for r in g] == [dict(r) for r in w]
    finally:
        conn.close()


def test_randomized_append_shapes_always_match_full_rebuild(tmp_path):
    """模糊：随机初始长度 + 多轮随机长度接续，增量摘要始终等于全量重建。"""
    import random

    from app.db import _SCHEMA
    from app.summaries import append_summaries, build_summaries

    isolated = sqlite3.connect(tmp_path / "fuzz.db")
    isolated.row_factory = sqlite3.Row
    isolated.executescript(_SCHEMA)
    rng = random.Random(20261113)
    for case in range(200):
        conn = isolated
        n0 = rng.randint(1, 6 * 4)
        seq = [(i * 100, float(rng.randint(-50, 500))) for i in range(n0)]
        tid = conn.execute(
            "INSERT INTO tests (name, source_filename, sample_count, start_ts, end_ts, max_level) "
            "VALUES (?, 'f.csv', ?, ?, ?, 0)",
            (f"fuzz-{case}", n0, seq[0][0], seq[-1][0]),
        ).lastrowid
        conn.executemany(
            "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
            [(tid, ts, p) for ts, p in seq],
        )
        ml = build_summaries(conn, tid, 4)
        conn.execute("UPDATE tests SET max_level=? WHERE id=?", (ml, tid))

        rounds = rng.randint(1, 8)
        cursor = n0
        for _ in range(rounds):
            step = rng.randint(1, 5 * 4)
            new_rows = [
                ((cursor + i) * 100, float(rng.randint(-50, 500)))
                for i in range(step)
            ]
            seq.extend(new_rows)
            conn.executemany(
                "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
                [(tid, ts, p) for ts, p in new_rows],
            )
            ml = append_summaries(conn, tid, 4)
            conn.execute("UPDATE tests SET max_level=? WHERE id=?", (ml, tid))
            cursor += step

        # 对照组：同一序列全量重建
        rid = conn.execute(
            "INSERT INTO tests (name, source_filename, sample_count, start_ts, end_ts, max_level) "
            "VALUES (?, 'f.csv', ?, ?, ?, 0)",
            (f"fuzz-{case}-ref", len(seq), seq[0][0], seq[-1][0]),
        ).lastrowid
        conn.executemany(
            "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)",
            [(rid, ts, p) for ts, p in seq],
        )
        ref_ml = build_summaries(conn, rid, 4)
        assert ml == ref_ml
        for level in range(1, ml + 1):
            g = conn.execute(
                "SELECT block_index, sample_count, start_ts, end_ts, first_ts, last_ts, "
                "first_pressure, last_pressure, max_pressure, max_pressure_ts, "
                "min_pressure, min_pressure_ts, first_rowid, last_rowid "
                "FROM blocks WHERE test_id=? AND level=? ORDER BY block_index",
                (tid, level),
            ).fetchall()
            w = conn.execute(
                "SELECT block_index, sample_count, start_ts, end_ts, first_ts, last_ts, "
                "first_pressure, last_pressure, max_pressure, max_pressure_ts, "
                "min_pressure, min_pressure_ts, first_rowid, last_rowid "
                "FROM blocks WHERE test_id=? AND level=? ORDER BY block_index",
                (rid, level),
            ).fetchall()
            assert len(g) == len(w), (case, level)
            for a, b in zip(g, w):
                for k in a.keys():
                    if k in ("first_rowid", "last_rowid"):
                        continue
                    assert a[k] == b[k], (case, level, k, a[k], b[k])
        conn.rollback()
    isolated.close()


def test_repeated_appends_on_block_boundaries_match_rebuild():
    """每轮都在块边界接续（无尾部半块填充），多轮后摘要仍与全量重建等价。"""
    first = [(i * 100, float(i)) for i in range(8)]  # 两个完整 level1 块
    body = _import_rows("append-11", first)
    test_id = body["id"]
    # 三轮，每轮 4 点（一块），逐步触发新父块与加层
    for round_no in range(3):
        base = 8 + round_no * 4
        cont = [((base + i) * 100, float(base + i)) for i in range(4)]
        resp = client.post(
            f"/tests/{test_id}/append",
            files={"file": (f"r{round_no}.csv", _csv(cont), "text/csv")},
        )
        assert resp.status_code == 200, resp.text

    all_rows = [(i * 100, float(i)) for i in range(20)]
    ref = _import_rows("append-11-ref", all_rows)
    conn = connect()
    try:
        got_level = conn.execute(
            "SELECT max_level FROM tests WHERE id = ?", (test_id,)
        ).fetchone()[0]
        assert got_level == ref["max_level"]
        for level in range(1, got_level + 1):
            g = conn.execute(
                "SELECT block_index, sample_count, start_ts, end_ts, first_ts, last_ts, "
                "first_pressure, last_pressure, max_pressure, max_pressure_ts, "
                "min_pressure, min_pressure_ts FROM blocks "
                "WHERE test_id=? AND level=? ORDER BY block_index",
                (test_id, level),
            ).fetchall()
            w = conn.execute(
                "SELECT block_index, sample_count, start_ts, end_ts, first_ts, last_ts, "
                "first_pressure, last_pressure, max_pressure, max_pressure_ts, "
                "min_pressure, min_pressure_ts FROM blocks "
                "WHERE test_id=? AND level=? ORDER BY block_index",
                (ref["id"], level),
            ).fetchall()
            assert [dict(r) for r in g] == [dict(r) for r in w]
    finally:
        conn.close()


def test_append_query_visibility_is_all_or_nothing():
    """接续事务提交前后，查询只能看到接续前或接续后的完整结果。"""
    import threading

    rows = [(1000 + i * 100, float(p)) for i, p in enumerate([10, 12, 8, 15])]
    body = _import_rows("append-9", rows)
    test_id = body["id"]
    cont = [(1500 + i * 100, float(p)) for i, p in enumerate([9, 14, 7, 20])]
    barrier = threading.Event()
    errors: list = []

    def writer():
        conn = connect()
        try:
            from app.ingest import append_test

            barrier.wait()
            append_test(conn, test_id, "cont.csv", _csv(cont))
            conn.commit()
        except Exception as exc:  # pragma: no cover - 失败时回传断言
            errors.append(exc)
            conn.rollback()
        finally:
            conn.close()

    t = threading.Thread(target=writer)
    t.start()
    barrier.set()
    t.join()
    assert not errors
    # 提交后只可能看到完整的 8 点结果，不可能看到 4..7 的中间态
    seen = client.get(
        f"/tests/{test_id}/query",
        params={"start_ts": 1000, "end_ts": 1900, "bucket_width_ms": 800},
    ).json()
    assert sum(b["sample_count"] for b in seen["buckets"]) == 8
