# 泵站启泵试验 · 水锤压力峰谷分析服务

面向泵站调试员的启泵试验复盘工具：把记录仪导出的**单次试验 CSV** 导入
FastAPI 服务，采样与多级摘要落盘到 **SQLite**；复盘时给定起止时刻和桶宽，
按桶返回首尾点与压力最高 / 最低点，快速定位水锤造成的峰谷，空桶自动标记缺测。
一次试验的压力记录被记录仪切成多份时，可用**接续 API** 按原试验编号把后续
CSV 接到同一试验上，重叠段自动去重，接续后仍用原编号查整段峰谷。

## 功能与约定

- **CSV 导入**（`POST /tests/import`，Swagger 可直接上传）
  - 首行表头，自动识别时间戳列与压力列，支持中英文表头
    （`timestamp_ms / ts / 时间戳 …`、`pressure_kpa / pressure / 压力(kPa) …`），
    UTF-8 与 GBK 编码均可
  - 毫秒时间戳必须为整数且**严格递增**，重复或倒序整次拒绝（HTTP 400）
  - 压力为有限数值，单位 kPa；单事务写入，失败不留半成品
- **CSV 接续**（`POST /tests/{id}/append`，按试验编号上传同格式 CSV）
  - 上传行时刻**不晚于库内末点**的，必须能在库里找到**同毫秒、同压力**的采样；
    匹配上的重叠行视为重复，**不再入库**
  - 重叠对不上（缺该时刻、或压力不一致）或文件含坏行：**整份退回，
    原试验采样 / 元信息 / 摘要全部不变**（HTTP 400，单事务回滚）
  - 只入库严格晚于末点的采样，响应返回新增点数 `appended_count`
    与匹配重叠行数 `overlap_matched`；全文件都在重叠区内且逐点匹配时新增 0
  - 摘要做**尾部增量更新**：沿用固定块大小，只重建各级受影响的尾部块，
    顶层涨成多块时自动加层；不重扫历史采样来重建摘要
  - 采样、试验元信息与摘要在同一事务生效：峰谷查询只能看到接续前或接续后的
    完整结果，不会读到半成品
- **分桶查询**（`GET /tests/{id}/query`，接续后用**原编号**即可查整段）
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
  - 接续时按固定块大小做**尾部增量更新**：level 1 先就地补齐末个未满块、
    再追加新块；以上各层只删除并重建受影响的尾部父块，顶层由 1 块涨成
    多块时自动新增一层收束——全程不重扫历史原始采样
  - 响应中的 `metrics` 如实回显：`blocks_by_level`（各级摘要命中数）
    与 `raw_rows_read`（回读原始行数，边界不切块时为 0）

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
sample_data/     样例 CSV（由 scripts/generate_sample.py 生成，含两段重叠接续样例）
scripts/         样例生成脚本
tests/           pytest 测试（34 项）
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
（静压 → 启泵爬升 → 衰减振荡，含一次低压深谷）；另生成同一条曲线切出的
`pump_start_part1.csv` / `pump_start_part2.csv`（重叠 500 点）供接续演练。
均可重新生成：

```bash
python3 scripts/generate_sample.py
```

在 Swagger（`/docs`）中的操作步骤：

1. `POST /tests/import`：`name` 填 `demo-001`，`file` 选择
   `sample_data/pump_start_demo.csv`，执行。响应返回试验 id、时间范围与各级块数。
2. `GET /tests`：确认试验已入库。
3. `GET /tests/{id}/query`：
   - `start_ts=1731300000000`、`end_ts=1731300019999`、`bucket_width_ms=1000`
     → 200 个 1 秒桶，逐桶查看峰谷；
   - 改成 `bucket_width_ms=1280`（与块边界对齐）再试，
     可见 `metrics.raw_rows_read = 0`，全程只走摘要；
   - 换 `start_ts=1731299990000&end_ts=1731299995000` 可看到全部桶 `missing=true`。

小样本 `sample_data/small_demo.csv`（60 点）适合在 Swagger 里肉眼逐桶核对。

## 接续试验演练（两段重叠记录仪 CSV）

同一次启泵试验被切成两段，`sample_data/pump_start_part1.csv`（1.2 万点）与
`sample_data/pump_start_part2.csv`（1.25 万点）有 **500 个同毫秒同压力的重叠
采样**；接续去重后整段共 24000 点。在 Swagger（`/docs`）中：

1. `POST /tests/import`：`name` 填 `pump-demo`，`file` 选择
   `sample_data/pump_start_part1.csv`，执行，记下返回的试验 id（如 `1`）。
2. `POST /tests/{id}/append`：路径 `id` 填刚才的编号，`file` 选择
   `sample_data/pump_start_part2.csv`，执行。响应示例：

   ```json
   {
     "id": 1,
     "appended_filename": "pump_start_part2.csv",
     "appended_count": 12000,
     "overlap_matched": 500,
     "sample_count": 24000,
     "end_ts": 1731300239990,
     "max_level": 3,
     "block_counts": {"1": 188, "2": 2, "3": 1}
   }
   ```

3. `GET /tests/{id}/query`：**用原编号**查整段峰谷，例如
   `start_ts=1731300000000&end_ts=1731300240000&bucket_width_ms=1000`；
   桶宽取块对齐的 `1280` 时 `metrics.raw_rows_read = 0`，全程只走摘要。
4. 若把接续文件里某个重叠行的压力改掉再上传，会收到 HTTP 400
   「接续文件与库内采样在时刻 … 对不上」，且 `GET /tests/{id}` 显示
   采样数与末点时刻均未变化（整份退回）。

### 接续 curl 示例

```bash
# 第 1 段
curl -X POST http://localhost:8000/tests/import \
  -F "name=pump-demo" \
  -F "file=@sample_data/pump_start_part1.csv;type=text/csv"

# 第 2 段按原编号接续（重叠 500 点自动去重，只追加 12000 点）
curl -X POST http://localhost:8000/tests/1/append \
  -F "file=@sample_data/pump_start_part2.csv;type=text/csv"

# 用原编号查整段峰谷
curl "http://localhost:8000/tests/1/query?start_ts=1731300000000&end_ts=1731300240000&bucket_width_ms=1000"
```

### curl 示例（单段试验）

```bash
curl -X POST http://localhost:8000/tests/import \
  -F "name=demo-001" \
  -F "file=@sample_data/pump_start_demo.csv;type=text/csv"

curl "http://localhost:8000/tests/1/query?start_ts=1731300000000&end_ts=1731300019999&bucket_width_ms=1000"
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
