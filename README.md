# 泵站启泵试验 · 水锤压力峰谷分析服务

面向泵站调试员的启泵试验复盘工具：把记录仪导出的试验 CSV 导入
FastAPI 服务，采样与多级摘要落盘到 **SQLite**；一次试验的压力记录分成
多份 CSV 时，可按试验编号**接续上传**，重叠采样自动核对去重。复盘时给定
起止时刻和桶宽，按桶返回首尾点与压力最高 / 最低点，快速定位水锤造成的
峰谷，空桶自动标记缺测。

## 功能与约定

- **CSV 导入**（`POST /tests/import`，Swagger 可直接上传）
  - 首行表头，自动识别时间戳列与压力列，支持中英文表头
    （`timestamp_ms / ts / 时间戳 …`、`pressure_kpa / pressure / 压力(kPa) …`），
    UTF-8 与 GBK 编码均可
  - 毫秒时间戳必须为整数且**严格递增**，重复或倒序整次拒绝（HTTP 400）
  - 压力为有限数值，单位 kPa；单事务写入，失败不留半成品
- **接续导入**（`POST /tests/{id}/append`，按原试验编号上传同格式 CSV）
  - 文件解析规则与首次导入一致：坏行（列数不符、非整数毫秒、非有限压力、
    时间戳未严格递增等）**整份退回，原试验不变**
  - 时刻**不晚于库内末点 `end_ts`** 的行属于重叠区，必须在库里找到
    **同毫秒、同压力**的采样；匹配的行视为重复，不再入库；
    找不到同时刻采样、或压力对不上，同样整份退回（HTTP 400）
  - 只把严格晚于末点的采样追加入库，响应 `appended_count` 给出新增点数，
    `overlap_count` 给出核对去重的重叠行数；整份文件都在重叠区内时
    `appended_count=0`，可安全重传（幂等）
  - 采样、试验元信息与摘要更新在**同一事务**内提交，峰谷查询只能看到
    接续前或接续后的完整结果，不会读到中间态
- **分桶查询**（`GET /tests/{id}/query`）
  - 从 `start_ts` 起按 `bucket_width_ms` 分桶，**含左端、不含右端**，
    末桶截到 `end_ts`
  - 每桶按时序返回首点、尾点、压力最高点、最低点；**同一点只列一次**，
    角色合并到 `roles`；极值并列时取时间戳最早的点
  - 桶内无采样时 `missing=true`、`points=[]`
- **多级摘要加速**（核心）
  - level 1 为最细索引块（默认每 128 个采样一块），其上逐级合并，
    每块保存首尾点、最高 / 最低点（含时间戳）、采样数与原始 rowid 区间
  - 查询时从最粗层级开始：**完整落在桶内的块直接采用摘要**；
    只有桶边界切开 level 1 最细块时，才按 rowid 区间回读原始采样
  - 响应中的 `metrics` 如实回显：`blocks_by_level`（各级摘要命中数）
    与 `raw_rows_read`（回读原始行数，边界不切块时为 0）
- **接续时的摘要维护**（只更新尾部，不重扫旧采样）
  - 沿用固定块大小 `PUMP_BLOCK_SIZE`：level 1 先用新采样填满既有尾部
    半块（统计直接合并），再按块大小切新块
  - 逐级向上只重写“受影响的尾部父块”，并在顶层被撑出多块时自动加层；
    全过程只读取新增采样与相关摘要块，旧采样一行都不重扫
  - 接续后的摘要与“全量数据一次性导入再建摘要”逐块等价（测试对拍覆盖）

## 目录结构

```
app/
  config.py      环境变量配置（库路径、块大小、上传上限、桶数上限）
  db.py          SQLite 连接、建表（tests / samples / blocks）
  ingest.py      CSV 解析、严格校验、首次导入与接续导入
  summaries.py  多级摘要构建、尾部增量更新与区间递归查询
  service.py     分桶编排、点去重与角色合并
  schemas.py     Pydantic 请求 / 响应模型
  main.py        FastAPI 路由
sample_data/     样例 CSV（由 scripts/generate_sample.py 生成）
scripts/         样例生成脚本
tests/           pytest 测试（33 项，含 200 组接续摘要模糊对拍）
Dockerfile, docker-compose.yml
```

## Docker Compose 启动

```bash
docker compose up --build
```

- 服务：<http://localhost:8000>
- Swagger UI：<http://localhost:8000/docs>
- 健康检查：`GET /health`
- SQLite 数据保存在命名卷 `pump-data`（容器内 `/data/pump_tests.db`）

## 复现样例试验

样例 `sample_data/pump_start_demo.csv` 为 2 万点、10 ms 间隔的合成启泵曲线
（静压 → 启泵爬升 → 衰减振荡，含一次低压深谷），可重新生成：

```bash
python3 scripts/generate_sample.py
```

脚本同时生成**两段有重叠**的记录仪文件，用于演示接续：

| 文件 | 覆盖采样序号 | 点数 | 说明 |
| --- | --- | --- | --- |
| `sample_data/pump_start_part1.csv` | 0..11999 | 12000 | 试验前段 |
| `sample_data/pump_start_part2.csv` | 10000..19999 | 10000 | 接续文件，与 part1 重叠 2000 点（同毫秒同压力） |

在 Swagger（`/docs`）中的操作步骤：

1. `POST /tests/import`：`name` 填 `demo-001`，`file` 选择
   `sample_data/pump_start_part1.csv`，执行。响应返回试验 id、时间范围与各级块数。
2. `POST /tests/{id}/append`：`id` 填上一步返回的试验编号，`file` 选择
   `sample_data/pump_start_part2.csv`，执行。响应中
   `appended_count=8000`、`overlap_count=2000`、`sample_count=20000`，
   与一次性导入完整 `pump_start_demo.csv` 的结果完全一致。
3. `GET /tests`：确认试验已入库，`end_ts` 已延伸到接续后的末点。
4. `GET /tests/{id}/query`：
   - `start_ts=1731300000000`、`end_ts=1731300200000`、`bucket_width_ms=1000`
     → 200 个 1 秒桶，逐桶查看整段试验的峰谷；
   - 改成 `bucket_width_ms=12800`（128 点 × 10 ms，与块边界对齐）再试，
     可见 `metrics.raw_rows_read = 0`，全程只走摘要；
   - 换 `start_ts=1731299990000&end_ts=1731299995000` 可看到全部桶 `missing=true`。
5. 再次用 part2 调一次 `append`：`appended_count=0`、
   `overlap_count=10000`，试验保持不变，说明重复上传是安全的。
6. 若把 part2 中任一重叠行的压力改坏再上传，接口返回 HTTP 400，
   且 `GET /tests/{id}` 显示试验点数与末点均未变化（整份退回）。

小样本 `sample_data/small_demo.csv`（60 点）适合在 Swagger 里肉眼逐桶核对。

### curl 示例

```bash
# 首次导入试验前段
curl -X POST http://localhost:8000/tests/import \
  -F "name=demo-001" \
  -F "file=@sample_data/pump_start_part1.csv;type=text/csv"

# 按原编号接续（重叠 2000 点自动核对去重，只入库 8000 个新点）
curl -X POST http://localhost:8000/tests/1/append \
  -F "file=@sample_data/pump_start_part2.csv;type=text/csv"

# 用原编号查整段峰谷
curl "http://localhost:8000/tests/1/query?start_ts=1731300000000&end_ts=1731300200000&bucket_width_ms=1000"
```

## 本地开发（无 Docker）

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
PUMP_DB_PATH=./data/dev.db python3 -m uvicorn app.main:app --reload --port 8000
```

## 关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PUMP_DB_PATH` | `/data/pump_tests.db` | SQLite 文件路径 |
| `PUMP_BLOCK_SIZE` | `128` | 每级摘要块大小（点数 / 子块数） |
| `PUMP_MAX_UPLOAD_BYTES` | `209715200` | 单次上传字节上限（200 MB） |
| `PUMP_MAX_BUCKETS` | `100000` | 单次查询桶数上限，防止桶宽过小 |
