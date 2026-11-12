#!/usr/bin/env python3
"""生成启泵试验样例 CSV（合成数据，模拟记录仪导出格式）。

运行：
    python3 scripts/generate_sample.py

输出：
    sample_data/pump_start_demo.csv        2 万点、含启泵水锤振荡与尖峰深谷
    sample_data/small_demo.csv             60 点小样本，便于在 Swagger 里肉眼核对
    sample_data/pump_start_part1.csv       接续演示第 1 段（1.2 万点，先导入）
    sample_data/pump_start_part2.csv       接续演示第 2 段（与第 1 段重叠 500 点）
列：timestamp_ms,pressure_kpa；毫秒时间戳严格递增。
"""

from __future__ import annotations

import csv
import math
import os
from pathlib import Path

BASE_TS = 1_731_300_000_000  # 固定基准，保证样例可复现
OUT_DIR = Path(__file__).resolve().parent.parent / "sample_data"


def pressure_at(t_ms: int) -> float:
    """模拟启泵过程：静压 -> 启泵爬升 -> 水锤衰减振荡，含一次低压深谷。"""
    t = t_ms
    if t < 2000:
        # 启泵前管网静压，微小波动
        p = 100.0 + 1.5 * math.sin(2 * math.pi * 0.5 * t / 1000.0)
    elif t < 6000:
        # 启泵后近似线性爬升
        p = 100.0 + 220.0 * (t - 2000) / 4000.0
    else:
        dt = t - 6000
        steady = 320.0
        oscillation = 60.0 * math.exp(-dt / 2500.0) * math.sin(
            2 * math.pi * 2.0 * dt / 1000.0
        )
        # 启泵瞬间约 50ms 处的一次低压水锤深谷
        dip = -45.0 * math.exp(-((dt - 50) / 25.0) ** 2)
        p = steady + oscillation + dip
    return round(p, 3)


def write_csv(path: Path, start_index: int, count: int, interval_ms: int) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "pressure_kpa"])
        for i in range(start_index, start_index + count):
            ts = BASE_TS + i * interval_ms
            writer.writerow([ts, pressure_at(i * interval_ms)])
    print(f"wrote {path} ({count} rows, index {start_index}..{start_index + count - 1})")


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    write_csv(OUT_DIR / "pump_start_demo.csv", start_index=0, count=20_000, interval_ms=10)
    write_csv(OUT_DIR / "small_demo.csv", start_index=0, count=60, interval_ms=100)

    # 接续演示：同一次启泵试验被记录仪切成两段，part2 开头与 part1 末尾
    # 重叠 500 个采样（同毫秒同压力），接续后总点数 24000。
    overlap = 500
    part1_count = 12_000
    part2_count = 12_500
    write_csv(
        OUT_DIR / "pump_start_part1.csv",
        start_index=0,
        count=part1_count,
        interval_ms=10,
    )
    write_csv(
        OUT_DIR / "pump_start_part2.csv",
        start_index=part1_count - overlap,
        count=part2_count,
        interval_ms=10,
    )


if __name__ == "__main__":
    main()
