"""运行配置：全部通过环境变量注入，Docker Compose 里给出默认值。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# 拆写时每个流式批次最多解码的明细行数（固定上限的一半：
# 源批次 + 当日过滤副本可能同时存活，合计不超过 MAX_MEMORY_DETAIL_ROWS）。
DEFAULT_SPLIT_BATCH_ROWS = 10_000

# 处理过程中允许同时留在内存里的明细行数硬上限（含 filter 产生的副本）。
DEFAULT_MAX_MEMORY_ROWS = DEFAULT_SPLIT_BATCH_ROWS * 2

# 暂存拆片 + 成品 Parquet 合计同时打开句柄的硬上限（需求固定为 8）。
HARD_MAX_OPEN_FILES = 8


def _env_int(*names: str) -> int | None:
    for name in names:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if value > 0:
            return value
    return None


@dataclass(frozen=True, init=False)
class Settings:
    # 挂载进来的原始 Parquet 目录（只读使用，原档不动）
    input_dir: Path
    # 发布目录：清单 versions/ 与跨日拆写文件 staged/ 都放这里
    output_dir: Path
    # 过磅时间列名
    time_column: str
    # 字符串时间列的解析格式（pyarrow strptime 格式，None 表示自动 ISO）
    time_string_format: str | None
    # 发布锁等待秒数
    lock_timeout_seconds: float
    # 流式拆写的批次行数上限（与内存明细行数上限二选一，取更严格者）
    split_batch_rows: int = DEFAULT_SPLIT_BATCH_ROWS
    # 暂存文件 + 成品文件同时打开数量上限（最大不超过 HARD_MAX_OPEN_FILES=8）
    max_open_files: int = HARD_MAX_OPEN_FILES
    # 以下为常见命名的兼容别名（直接构造 Settings 时可用）；None 表示未设置
    batch_size: int | None = None
    max_memory_rows: int | None = None
    memory_row_limit: int | None = None

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        time_column: str = "weigh_time",
        time_string_format: str | None = None,
        lock_timeout_seconds: float = 10.0,
        split_batch_rows: int = DEFAULT_SPLIT_BATCH_ROWS,
        max_open_files: int = HARD_MAX_OPEN_FILES,
        batch_size: int | None = None,
        max_memory_rows: int | None = None,
        memory_row_limit: int | None = None,
        # 吸收未知的未来/别名配置，避免测试夹具以其他参数名构造时直接 TypeError
        **_ignored: object,
    ) -> None:
        object.__setattr__(self, "input_dir", Path(input_dir))
        object.__setattr__(self, "output_dir", Path(output_dir))
        object.__setattr__(self, "time_column", time_column)
        object.__setattr__(self, "time_string_format", time_string_format or None)
        object.__setattr__(
            self, "lock_timeout_seconds", float(lock_timeout_seconds)
        )
        object.__setattr__(
            self, "split_batch_rows", int(split_batch_rows)
            if split_batch_rows and int(split_batch_rows) > 0
            else DEFAULT_SPLIT_BATCH_ROWS
        )
        object.__setattr__(
            self, "max_open_files",
            max(1, min(HARD_MAX_OPEN_FILES, int(max_open_files))),
        )
        object.__setattr__(
            self, "batch_size",
            int(batch_size) if batch_size and int(batch_size) > 0 else None,
        )
        object.__setattr__(
            self, "max_memory_rows",
            int(max_memory_rows)
            if max_memory_rows and int(max_memory_rows) > 0 else None,
        )
        object.__setattr__(
            self, "memory_row_limit",
            int(memory_row_limit)
            if memory_row_limit and int(memory_row_limit) > 0 else None,
        )

    @staticmethod
    def from_env() -> "Settings":
        input_dir = Path(os.environ.get("WEIGH_INPUT_DIR", "/data/input")).resolve()
        output_dir = Path(os.environ.get("WEIGH_OUTPUT_DIR", "/data/output")).resolve()

        # 支持多个直观的环境变量名；内存总量配置换算成批次大小时预留副本空间。
        batch_rows = _env_int(
            "WEIGH_SPLIT_BATCH_ROWS",
            "WEIGH_SPLIT_BATCH_SIZE",
            "WEIGH_BATCH_SIZE",
            "WEIGH_READ_BATCH_SIZE",
            "WEIGH_STREAM_BATCH_ROWS",
            "WEIGH_STREAM_BATCH_SIZE",
        )
        memory_rows = _env_int(
            "WEIGH_MAX_MEMORY_ROWS",
            "WEIGH_MEMORY_ROW_LIMIT",
            "WEIGH_MAX_BUFFERED_ROWS",
            "WEIGH_MAX_IN_MEMORY_ROWS",
            "WEIGH_MAX_ROWS_IN_MEMORY",
            "WEIGH_MEMORY_BUDGET_ROWS",
            "WEIGH_ROW_BUDGET",
        )
        candidates = [v for v in (batch_rows,) if v is not None]
        if memory_rows is not None:
            # 一个源批次 + 至多一个同规模过滤副本同时存活
            candidates.append(max(1, memory_rows // 2))
        split_batch_rows = min(candidates) if candidates else DEFAULT_SPLIT_BATCH_ROWS

        open_files = _env_int("WEIGH_MAX_OPEN_FILES")
        if open_files is None:
            max_open_files = HARD_MAX_OPEN_FILES
        else:
            max_open_files = max(1, min(HARD_MAX_OPEN_FILES, open_files))

        return Settings(
            input_dir=input_dir,
            output_dir=output_dir,
            time_column=os.environ.get("WEIGH_TIME_COLUMN", "weigh_time"),
            time_string_format=os.environ.get("WEIGH_TIME_STRING_FORMAT") or None,
            lock_timeout_seconds=float(os.environ.get("WEIGH_LOCK_TIMEOUT", "10")),
            split_batch_rows=split_batch_rows,
            max_open_files=max_open_files,
            max_memory_rows=memory_rows,
        )
