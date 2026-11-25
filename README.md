# 码头过磅明细按日数据集服务

把挂载目录里一组**同结构** Parquet 过磅明细，按**过磅时间的 UTC 日期**发布成可按日取文件/取数的数据集。
FastAPI + PyArrow 实现，Docker Compose 启动；原始文件只读挂载、绝不改写。

## 设计要点

### 1. 三级处理路径（尽量不读数据行）

对每个输入文件：

1. **页脚快路径 `reuse_footer`**：只读取 Parquet footer 的列统计（min/max/null_count，不读任何数据行）。
   若能证明时间列**非空且全部落在同一个 UTC 日**，直接在清单里登记原文件路径，零数据读取。
2. **读明细确认 `reuse_scan`**：页脚证明不了（如 `write_statistics=False`、字符串统计无法判定）时，
   只读时间列：确认非空且同日 → **仍复用原档**，不重写。
3. **跨日拆写 `split`**：确认一份文件混了多个 UTC 日时，用
   `ParquetFile.iter_batches` **单遍流式扫描全列**——跨日原档的非时间列只解码一遍。
   每个批次按日过滤后立即写成隐藏拆片（`.…partNNNN.parquet.tmp` → 原子改名），
   最后每个有记录的 UTC 日把其拆片按原顺序流式合并成**唯一一份**
   `{stem}__{YYYY-MM-DD}.parquet`（单拆片时直接 `os.replace`，零重复编解码）。
   过滤保持批次内原顺序，因此当天明细（含重复行）与原档顺序完全一致。
   零行文件记为 `empty`，不产生任何按日条目。

### 1.1 固定资源上限（明细装不进内存也能发布）

拆写是流式的，资源占用与输入文件总行数/总大小无关：

- **内存明细行数固定上限**：每批最多 `WEIGH_SPLIT_BATCH_ROWS`（默认 10000）行；
  任意时刻存活的明细容器只有「一个源批次 + 一个刚 filter 出并立即写盘的当日副本」，
  合计 ≤ `2 × 批次行数`（模块常量 `MAX_MEMORY_ROWS`，默认 20000，已含处理中副本）。
  也可直接设 `WEIGH_MAX_MEMORY_ROWS`，会按 `cap // 2` 自动折算批次大小。
- **同时打开文件数 ≤ 8**：暂存拆片与成品 Parquet 合计的打开句柄由
  `_OpenFileSlots` 统一计数，硬上限 `MAX_OPEN_FILES = 8`
  （`WEIGH_MAX_OPEN_FILES` 只能调小不能放大）。写拆片时峰值 1；
  合并时峰值为 1 个成品 writer + 1 个拆片 reader = 2。日期数超过 8 也不受影响：
  拆片用完即关，绝不按日期长期持有 writer。
- `PublishResult.peak_open_files` 返回本次发布实际观测到的句柄峰值，便于审计。

### 2. 空时间与发布失败

- 时间列为 null、空白字符串或无法解析 → 立即中止发布（`PublishError`），API 返回 422。
  页脚统计能提前发现就提前中止；否则流式读到出错批次（哪怕是最后一批）才中止。
- 写盘失败等任何异常同理：整棵 `staged/v<新版本号>/`（本次拆片与所有 `.tmp`
  临时文件）都会被删除；若版本归档已写而换版未完成，也会撤掉该归档，
  **`current.json` 不会被替换**。
- 按日取文件/取数的 API 永远只认 `current.json`，因此失败后读到的仍是上一版清单。

### 3. 原子换版与不重不漏

- 新版本的所有拆写文件全部写完、行数按来源文件核对无误后，才把新清单写入临时文件并 `os.replace`
  原子替换 `current.json`（归档到 `versions/manifest-vXXXXXX.json`）。
- 发布时按 `(date, source_file)` 复核行数，保证每条输入记录只在其实际过磅日出现一次，不重不漏。
- 发布进程间用 `flock` 互斥；输入路径限定在 `input_dir` 内，拒绝绝对路径与 `..`。

### 4. UTC 取日规则

- 带时区 timestamp：先转 UTC 再取日（例：北京 11-24 07:30 → UTC 11-23 23:30，归 11-23）。
- 无时区 timestamp/date：按存储字面值取日（落地统一 UTC 的约定）。
- 字符串：按 `WEIGH_TIME_STRING_FORMAT` 解析，未配置则尝试常见 ISO 格式；解析失败按空时间处理。

## 目录结构

```
app/                  应用代码
  config.py           环境变量配置
  timeutil.py         时间列 → UTC 日期
  manifest.py         清单模型与原子换版存储
  publisher.py        发布核心（页脚判定/扫描/拆写/锁/回滚）
  service.py          按日读取服务
  api.py              FastAPI 路由
  cli.py              命令行入口
docker/               Dockerfile 与 docker-compose.yml
scripts/make_sample_data.py  样例数据生成
tests/                pytest 测试（24 项）
data_in/              原始 Parquet（compose 只读挂载）
data_out/             current.json、versions/、staged/
```

## Docker Compose 启动

```bash
# 1) 准备样例数据（可选，也可直接挂真实目录）
python scripts/make_sample_data.py ./data_in

# 2) 启动
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml logs -f
```

环境变量（compose 里已给默认值）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEIGH_INPUT_DIR` | `/data/input` | 原始 Parquet 目录（容器内只读） |
| `WEIGH_OUTPUT_DIR` | `/data/output` | 清单与拆写产物目录 |
| `WEIGH_TIME_COLUMN` | `weigh_time` | 过磅时间列名 |
| `WEIGH_TIME_STRING_FORMAT` | 空（自动 ISO） | 字符串时间列的 strptime 格式 |
| `WEIGH_LOCK_TIMEOUT` | `10` | 发布锁等待秒数 |
| `WEIGH_SPLIT_BATCH_ROWS` | `10000` | 流式拆写每批最大行数（内存峰值约 2 倍） |
| `WEIGH_MAX_MEMORY_ROWS` | `20000` | 内存明细行数总上限（含过滤副本，按 `//2` 折算批次） |
| `WEIGH_MAX_OPEN_FILES` | `8` | 暂存拆片+成品同时打开数（硬上限 8，只能调小） |

## 可直接试的 API 请求

健康检查：

```bash
curl -s http://localhost:8000/health
```

发布（文件路径相对 `WEIGH_INPUT_DIR`，必须同结构）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":[
    "batch_a_sameday.parquet",
    "batch_b_mixed.parquet",
    "batch_d_nostats.parquet"
  ]}'
```

查看当前清单：

```bash
curl -s http://localhost:8000/manifest | python -m json.tool
```

超过 8 个日期严格交错（样例 `batch_f_many_days.parquet` 含 12 个 UTC 日、
按到货顺序循环交错并有重复行）。批次大小/句柄上限是**服务端**配置，
可在 `docker-compose.yml` 设 `WEIGH_SPLIT_BATCH_ROWS=37` 制造大量拆片来压测：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":["batch_f_many_days.parquet"]}'
# 12 个日期各一份成品，顺序/重复行与原档一致；
# PublishResult.peak_open_files 始终 <= 8（多拆片合并阶段实测峰值 2）。
curl -s 'http://localhost:8000/days/2026-11-15/rows?limit=200' | python -m json.tool
```

按 UTC 日取文件清单（含下载地址、来源 original/split、行数）：

```bash
curl -s http://localhost:8000/days/2026-11-23/files | python -m json.tool
curl -s http://localhost:8000/days/2026-11-24/files | python -m json.tool
```

按 UTC 日直接读记录（多文件自动拼接，`limit` 控制返回行数）：

```bash
curl -s 'http://localhost:8000/days/2026-11-24/rows?limit=100' | python -m json.tool
```

下载当日某个 Parquet（URL 取自 files 接口的 `download_url`）：

```bash
curl -s -o day23-split.parquet \
  "http://localhost:8000/days/2026-11-23/files/split/<base64路径>"
```

失败场景（含空时间的文件，返回 422 且 `retained_version` 指向上一版）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":["batch_a_sameday.parquet","batch_c_null.parquet"]}'
# 再按日取，仍是旧版：
curl -s http://localhost:8000/days/2026-11-23/files
```

交互式文档：启动后访问 `http://localhost:8000/docs`。

## 命令行用法（容器外本地调试）

```bash
python -m app.cli publish batch_a_sameday.parquet batch_b_mixed.parquet
python -m app.cli manifest
python -m app.cli read 2026-11-23
```

## 测试

```bash
python -m pytest tests/ -q
```

共 24 项，覆盖：页脚快路径零数据读取、无统计降级扫描复用、跨日流式拆写不重不漏、
空时间中止、模拟写盘失败保留旧版、成功换版、路径穿越/异结构拒绝、字符串与带时区时间、
多 row group、零行文件、API 全流程与失败后回归旧版；以及新增的资源上限回归
（`tests/test_streaming_split.py`）：

- 14 个 UTC 日严格交错 + 重复行：每日恰好一份成品，id 子序列与原档完全一致；
- 16 个日期交错时存活 `ParquetWriter` 峰值 ≤ 8；
- `WEIGH_MAX_MEMORY_ROWS=300` 时每次写盘负载 ≤ 150 行（批次上限）；
- 跨日原档只发生一次全列 `iter_batches` 扫描（非时间列只解码一遍）；
- 尾批写盘 `OSError`：旧版 retained、`staged/vN` 与 vN 归档清理干净、按日服务仍旧版；
- 尾批/拆写中途无效时间：同样中止并清理全部拆片与临时文件。

### 完工前自测记录（本机 Linux + Python 3.11 + pyarrow 26.0.1）

- `pytest tests/ -q`：**24 passed**。
- 额外资源压测脚本（20 个日期交错、小批次 37，强制多拆片合并）：
  日档内容/顺序/重复行逐行比对通过；`ParquetWriter` 存活峰值 1、
  内部 slot 峰值 2；高频轮询 `/proc/self/fd` 观测到的 staged 目录真实 FD 峰值 2（≤8）。
- 4 万行宽表：Arrow 分配池峰值约为整表 `nbytes` 的 0.4 且不随行数线性增长。
- 尾批失败（让含最后一条记录的写片抛 `OSError`）与「最后一个输入文件」写失败：
  均向上抛原始 `OSError`、整棵 `staged/vN` 删除、无 vN 归档、`current.json` 仍旧版，
  `DatasetService.read_date()` 仍返回上一版数据。
- 用 uvicorn 实起服务 + curl 跑通 `/health`、`/publish`、`/manifest`、
  `/days/.../files`、`/days/.../rows` 与 Parquet 下载；`batch_c_null` 发布返回
  422 且 `retained_version` 指向上一版，随后按日接口版本不变。
- 本机没有 Docker 守护进程/CLI，**`docker compose up --build` 未在本环境实际构建**；
  已校验 compose YAML 与 Dockerfile 内容，容器启动命令等价于上面的 uvicorn 实测。
