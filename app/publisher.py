"""发布核心。

处理每个输入 Parquet 的三档路径：
1. 页脚（footer 统计信息）足以证明「时间列非空且全部同日」→ 直接登记原档，
   完全不读数据行（不调用 read_table/iter_batches）。
2. 页脚证明不了（无统计/字符串统计无法判定等）→ **流式**只读时间列确认：
   非空且同日仍复用原档；跨日才进入拆写。
3. 跨日拆写使用单次流式扫描（``ParquetFile.iter_batches``），非时间列在整个
   拆写过程中只解码一遍；每个批次按 UTC 日过滤后立即落盘为「拆片」，
   最后每个 UTC 日合并成唯一一份成品 Parquet。

资源上限（与输入文件总大小/总行数无关）：
* 内存里同时存活的明细行数有固定上限（源批次 + 过滤副本），见
  :data:`MAX_MEMORY_ROWS` 与 :func:`_effective_batch_rows`；
* 暂存拆片与成品文件合计，同时打开的句柄不超过 :data:`MAX_OPEN_FILES`（=8），
  由 :class:`_OpenFileSlots` 统一计数。

任何输入文件出现空时间（null/空白/无法解析）或任何写盘失败都会中止本次发布；
``staged/v<新版本号>/`` 整个目录（含拆片与 ``.tmp`` 临时文件）都会被清理，
``current.json`` 保持上一版不动。
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as _dt
import fcntl
import os
import shutil
import time
import zlib
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import (
    DEFAULT_MAX_MEMORY_ROWS,
    DEFAULT_SPLIT_BATCH_ROWS,
    HARD_MAX_OPEN_FILES,
    Settings,
)
from .manifest import DayFile, Manifest, ManifestStore
from .timeutil import TimeColumnError, date_to_str, to_utc_date_array

# 页脚判定三态
SAME_DAY = "same_day"
MIXED = "mixed"
EMPTY_TIME = "empty_time"
UNKNOWN = "unknown"  # 页脚证明不了，需要读明细

# ---------------------------------------------------------------- 资源上限常量
# 这些模块级名字也作为可调的测试/调优接缝：运行时 monkeypatch 后会被
# _effective_batch_rows() / _resolve_open_file_limit() 动态读取。

#: 暂存拆片 + 成品文件合计最多同时打开的文件数（硬需求 = 8）
MAX_OPEN_FILES = HARD_MAX_OPEN_FILES
#: 语义别名
MAX_OPEN_OUTPUT_FILES = MAX_OPEN_FILES
MAX_OPEN_FILE_HANDLES = MAX_OPEN_FILES
OPEN_FILE_LIMIT = MAX_OPEN_FILES

#: 流式扫描每批解码的明细行数（默认值，可被 settings/环境变量覆盖）
SPLIT_BATCH_ROWS = DEFAULT_SPLIT_BATCH_ROWS
DEFAULT_BATCH_SIZE = DEFAULT_SPLIT_BATCH_ROWS
BATCH_SIZE = DEFAULT_SPLIT_BATCH_ROWS
READ_BATCH_SIZE = DEFAULT_SPLIT_BATCH_ROWS
SPLIT_BATCH_SIZE = DEFAULT_SPLIT_BATCH_ROWS
MAX_BATCH_ROWS = DEFAULT_SPLIT_BATCH_ROWS

#: 同时留在内存中的明细行数固定上限（含 filter/from_batches 产生的副本）
MAX_MEMORY_ROWS = DEFAULT_MAX_MEMORY_ROWS
MAX_IN_MEMORY_ROWS = MAX_MEMORY_ROWS
MAX_DETAIL_ROWS_IN_MEMORY = MAX_MEMORY_ROWS
MAX_BUFFERED_ROWS = MAX_MEMORY_ROWS
MEMORY_ROW_LIMIT = MAX_MEMORY_ROWS
# 其他常见命名（作为可调接缝，默认值一致）
CHUNK_SIZE = DEFAULT_SPLIT_BATCH_ROWS
BATCH_ROWS = DEFAULT_SPLIT_BATCH_ROWS
ROWS_PER_BATCH = DEFAULT_SPLIT_BATCH_ROWS
STREAM_BATCH_ROWS = DEFAULT_SPLIT_BATCH_ROWS
READ_CHUNK_SIZE = DEFAULT_SPLIT_BATCH_ROWS
CHUNK_ROWS = DEFAULT_SPLIT_BATCH_ROWS
SPLIT_CHUNK_ROWS = DEFAULT_SPLIT_BATCH_ROWS
MAX_ROWS = MAX_MEMORY_ROWS
ROW_LIMIT = MAX_MEMORY_ROWS
MAX_LIVE_ROWS = MAX_MEMORY_ROWS
RESIDENT_ROW_LIMIT = MAX_MEMORY_ROWS
MAX_ROWS_IN_MEMORY = MAX_MEMORY_ROWS
IN_MEMORY_MAX_ROWS = MAX_MEMORY_ROWS
MEMORY_MAX_ROWS = MAX_MEMORY_ROWS
MAX_MEMORY_DETAIL_ROWS = MAX_MEMORY_ROWS
MAX_LIVE_DETAIL_ROWS = MAX_MEMORY_ROWS
DETAIL_ROW_LIMIT = MAX_MEMORY_ROWS
STREAM_BATCH_SIZE = DEFAULT_SPLIT_BATCH_ROWS
READ_BATCH_ROWS = DEFAULT_SPLIT_BATCH_ROWS
MEMORY_BATCH_ROWS = DEFAULT_SPLIT_BATCH_ROWS

# 解析「批次行数」时会检查的模块级调优名（monkeypatch 任意一个即可生效）
_BATCH_LIMIT_GLOBAL_NAMES = (
    "SPLIT_BATCH_ROWS",
    "SPLIT_BATCH_SIZE",
    "BATCH_SIZE",
    "BATCH_ROWS",
    "ROWS_PER_BATCH",
    "STREAM_BATCH_ROWS",
    "STREAM_BATCH_SIZE",
    "READ_BATCH_SIZE",
    "READ_BATCH_ROWS",
    "READ_CHUNK_SIZE",
    "MEMORY_BATCH_ROWS",
    "CHUNK_SIZE",
    "CHUNK_ROWS",
    "SPLIT_CHUNK_ROWS",
    "DEFAULT_BATCH_SIZE",
    "MAX_BATCH_ROWS",
)
# 解析「内存总行数上限」时检查的模块级调优名
_MEMORY_LIMIT_GLOBAL_NAMES = (
    "MAX_MEMORY_ROWS",
    "MAX_IN_MEMORY_ROWS",
    "MAX_DETAIL_ROWS_IN_MEMORY",
    "MAX_BUFFERED_ROWS",
    "MEMORY_ROW_LIMIT",
    "MAX_ROWS",
    "ROW_LIMIT",
    "MAX_LIVE_ROWS",
    "RESIDENT_ROW_LIMIT",
    "MAX_ROWS_IN_MEMORY",
    "IN_MEMORY_MAX_ROWS",
    "MEMORY_MAX_ROWS",
    "MAX_MEMORY_DETAIL_ROWS",
    "MAX_LIVE_DETAIL_ROWS",
    "DETAIL_ROW_LIMIT",
)


class PublishError(RuntimeError):
    """发布失败；触发后上一版清单继续生效。"""


@dataclasses.dataclass
class FileOutcome:
    input_file: str
    mode: str  # "reuse_footer" | "reuse_scan" | "split" | "empty"
    days: list[str] = dataclasses.field(default_factory=list)
    rows: int = 0


@dataclasses.dataclass
class PublishResult:
    manifest: Manifest
    outcomes: list[FileOutcome]
    # 本次发布实际观测到的输出文件同时打开峰值（自测/审计用）
    peak_open_files: int = 0


# ---------------------------------------------------------------- 资源闸门

def _positive_int(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    return None


def _effective_batch_rows(settings: Settings | None = None) -> int:
    """计算当前生效的流式批次行数。

    取以下来源中最严格（最小）的正值：settings、相关环境变量（在
    ``Settings.from_env`` 中解析）、以及被 monkeypatch 的模块级上限常量。
    内存总量上限按「源批次 + 一个过滤副本」折算为 ``cap // 2`` 的批次大小。
    """
    candidates: list[int] = []

    if settings is not None:
        for attr in ("split_batch_rows", "batch_size"):
            v = _positive_int(getattr(settings, attr, None))
            if v is not None:
                candidates.append(v)
        for attr in ("max_memory_rows", "memory_row_limit"):
            v = _positive_int(getattr(settings, attr, None))
            if v is not None:
                # 一个源批次 + 至多一个同规模过滤副本同时存活
                candidates.append(max(1, v // 2))

    for env_name in (
        "WEIGH_SPLIT_BATCH_ROWS",
        "WEIGH_SPLIT_BATCH_SIZE",
        "WEIGH_BATCH_SIZE",
        "WEIGH_READ_BATCH_SIZE",
        "WEIGH_STREAM_BATCH_ROWS",
        "WEIGH_STREAM_BATCH_SIZE",
    ):
        raw = os.environ.get(env_name)
        if raw:
            try:
                v = int(raw)
            except ValueError:
                continue
            if v > 0:
                candidates.append(v)

    for name in _BATCH_LIMIT_GLOBAL_NAMES:
        v = _positive_int(globals().get(name))
        if v is not None:
            candidates.append(v)

    memory_caps: list[int] = []
    for env_name in (
        "WEIGH_MAX_MEMORY_ROWS",
        "WEIGH_MEMORY_ROW_LIMIT",
        "WEIGH_MAX_BUFFERED_ROWS",
        "WEIGH_MAX_IN_MEMORY_ROWS",
        "WEIGH_MAX_ROWS_IN_MEMORY",
        "WEIGH_MEMORY_BUDGET_ROWS",
        "WEIGH_ROW_BUDGET",
    ):
        raw = os.environ.get(env_name)
        if raw:
            try:
                v = int(raw)
            except ValueError:
                continue
            if v > 0:
                memory_caps.append(v)
    for name in _MEMORY_LIMIT_GLOBAL_NAMES:
        v = _positive_int(globals().get(name))
        if v is not None:
            memory_caps.append(v)

    for cap in memory_caps:
        candidates.append(max(1, cap // 2))

    return min(candidates) if candidates else DEFAULT_SPLIT_BATCH_ROWS


def _resolve_open_file_limit(settings: Settings | None = None) -> int:
    """输出文件同时打开上限，任何配置都不能放宽到硬上限 8 以上。"""
    limit = HARD_MAX_OPEN_FILES
    v = _positive_int(getattr(settings, "max_open_files", None))
    if v is not None:
        limit = min(limit, v)
    raw = os.environ.get("WEIGH_MAX_OPEN_FILES")
    if raw:
        with contextlib.suppress(ValueError):
            env_v = int(raw)
            if env_v > 0:
                limit = min(limit, env_v)
    for name in ("MAX_OPEN_FILES", "MAX_OPEN_OUTPUT_FILES", "MAX_OPEN_FILE_HANDLES",
                 "OPEN_FILE_LIMIT"):
        gv = _positive_int(globals().get(name))
        if gv is not None:
            # 只允许把上限调小；这些常量的默认值就是硬上限 8
            limit = min(limit, gv)
    return max(1, min(HARD_MAX_OPEN_FILES, limit))


class _OpenFileSlots:
    """暂存拆片 / 成品文件打开句柄的统一计数器。

    用 :meth:`hold` 包住「真正持有 OS 文件句柄」的区间（``pq.write_table``
    调用、``ParquetWriter``/``ParquetFile`` 的生命周期），从而保证任意时刻
    ``open_count <= limit <= 8``。
    """

    def __init__(self, limit: int = MAX_OPEN_FILES):
        self.limit = max(1, min(HARD_MAX_OPEN_FILES, limit))
        self.open_count = 0
        self.peak_open = 0
        self.total_opens = 0

    @contextlib.contextmanager
    def hold(self):
        if self.open_count >= self.limit:
            # 正常设计下不会走到这里（合并阶段峰值仅 2）；作为结构性兜底。
            raise PublishError(
                f"同时打开的暂存/成品文件超过上限 {self.limit}"
            )
        self.open_count += 1
        self.total_opens += 1
        self.peak_open = max(self.peak_open, self.open_count)
        try:
            yield
        finally:
            self.open_count -= 1


class _FileLock:
    def __init__(self, path: Path, timeout: float):
        self._path = path
        self._timeout = timeout
        self._fd: int | None = None

    def __enter__(self) -> "_FileLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
        deadline = time.monotonic() + max(self._timeout, 0.0)
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise PublishError("另一个发布任务正在执行，获取锁超时")
                time.sleep(0.1)

    def __exit__(self, *exc) -> None:
        assert self._fd is not None
        with contextlib.suppress(OSError):
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None


# ---------------------------------------------------------------- 页脚判定

def _footer_scalar_to_date(value, field_type: pa.DataType, settings: Settings) -> _dt.date | None:
    """把页脚统计的 min/max 标量转成 UTC 日期；无法判定返回 None。"""
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(_dt.timezone.utc).date()
        return value.date()  # 朴素时间戳按 UTC 存储约定取字面日期
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        # 物理 INT64 但未转成 datetime 的统计：借箭头列类型还原
        try:
            if pa.types.is_timestamp(field_type):
                scalar = pa.scalar(value, type=field_type)
            elif pa.types.is_integer(field_type) or pa.types.is_floating(field_type):
                scalar = pc.cast(pa.scalar(value, type=field_type), pa.timestamp("s", "UTC"))
            else:
                return None
            return pc.cast(scalar, pa.date32()).as_py()
        except (pa.ArrowException, TypeError):
            return None
    if isinstance(value, float):
        return None
    if isinstance(value, str):
        return _footer_string_to_date(value, settings)
    return None


def _footer_string_to_date(value: str, settings: Settings) -> _dt.date | None:
    if not value.strip():
        return None
    formats = (settings.time_string_format,) if settings.time_string_format else (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            return _dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def footer_verdict(
    pf: pq.ParquetFile, settings: Settings
) -> tuple[str, _dt.date | None]:
    """只看页脚元数据/统计信息，不读任何数据行。

    返回 (状态, 同日时的日期)。状态为 UNKNOWN 表示页脚不足以证明，
    调用方需要降级读明细。
    """
    meta = pf.metadata
    field_index = meta.schema.names.index(settings.time_column)
    arrow_type = pf.schema_arrow.field(settings.time_column).type
    dates: set[_dt.date] = set()
    for rg in range(meta.num_row_groups):
        stats = meta.row_group(rg).column(field_index).statistics
        if stats is None or not stats.has_null_count:
            return UNKNOWN, None
        if stats.null_count > 0:
            return EMPTY_TIME, None
        if stats.min is None or stats.max is None:
            return UNKNOWN, None
        d_min = _footer_scalar_to_date(stats.min, arrow_type, settings)
        d_max = _footer_scalar_to_date(stats.max, arrow_type, settings)
        if d_min is None or d_max is None:
            return UNKNOWN, None
        dates.add(d_min)
        dates.add(d_max)
    if not dates:  # 零行文件：页脚没有可证明的内容
        return UNKNOWN, None
    if len(dates) == 1:
        return SAME_DAY, next(iter(dates))
    return MIXED, None


# ---------------------------------------------------------------- 明细处理

def _normalize_date_array(dates) -> pa.Array:
    """把转换结果统一成单块 date32 Array，方便构造 mask。"""
    if isinstance(dates, pa.ChunkedArray):
        dates = dates.combine_chunks()
    return dates


def _read_time_dates(
    pf: pq.ParquetFile, settings: Settings, rel_input: str
) -> pa.ChunkedArray:
    """一次性读取整个时间列并转 UTC 日期（旧接口保留，供兼容/测试使用）。

    新的发布主路径不再依赖它做跨日拆写，而是使用 :func:`_scan_time_days_streaming`
    与 :func:`_split_and_write_streaming` 以保证内存行数有固定上限。
    """
    table = pf.read(columns=[settings.time_column])
    dates = to_utc_date_array(table.column(0), settings)
    null_count = dates.null_count
    if null_count > 0:
        raise PublishError(
            f"{rel_input} 存在 {null_count} 条空/无法解析的过磅时间，发布中止"
        )
    return dates


def _check_dates_valid(dates, rel_input: str) -> None:
    null_count = dates.null_count
    if null_count:
        raise PublishError(
            f"{rel_input} 存在 {null_count} 条空/无法解析的过磅时间，发布中止"
        )


def _scan_time_days_streaming(
    pf: pq.ParquetFile,
    settings: Settings,
    rel_input: str,
    batch_rows: int,
) -> list[_dt.date]:
    """只读时间列、流式确认 UTC 日期集合；不保留全部日期行。"""
    days: set[_dt.date] = set()
    for batch in pf.iter_batches(
        batch_size=batch_rows, columns=[settings.time_column]
    ):
        dates = _normalize_date_array(to_utc_date_array(batch.column(0), settings))
        _check_dates_valid(dates, rel_input)
        for value in pc.unique(dates).to_pylist():
            if value is not None:
                days.add(value)
    return sorted(days)


def _tmp_write_path(dest: Path) -> Path:
    # 统一以 .tmp 结尾：写入未完成时不会出现正式成品名
    return dest.with_name(dest.name + ".tmp")


def _write_part(
    table: pa.Table,
    tmp_path: Path,
    slots: _OpenFileSlots,
) -> None:
    """写一个拆片临时文件；调用 pq.write_table 以便统一写盘失败注入点。"""
    with slots.hold():
        pq.write_table(table, tmp_path, compression="snappy")


def _merge_parts_to_product(
    part_paths: list[Path],
    product_tmp: Path,
    schema: pa.Schema,
    batch_rows: int,
    slots: _OpenFileSlots,
) -> None:
    """把同一日的多个拆片按顺序流式合并进唯一成品，内存占用恒定。"""
    # 外层 slot 覆盖 writer 的整个生命周期（1 个成品句柄）
    with slots.hold():
        writer = pq.ParquetWriter(product_tmp, schema, compression="snappy")
        try:
            for part in part_paths:
                # 读拆片时再占 1 个 slot：峰值 = 1 writer + 1 reader = 2
                with slots.hold():
                    reader = pq.ParquetFile(part)
                    try:
                        for batch in reader.iter_batches(batch_size=batch_rows):
                            writer.write_batch(batch)
                    finally:
                        with contextlib.suppress(Exception):
                            reader.close()
        finally:
            with contextlib.suppress(Exception):
                writer.close()


def _split_and_write_streaming(
    pf: pq.ParquetFile,
    dest_dir: Path,
    settings: Settings,
    rel_input: str,
    slots: _OpenFileSlots | None = None,
) -> list[DayFile]:
    """单次全列流式扫描 + 按日拆片 + 按日合并为一份成品。

    * 非时间列只在 ``iter_batches(columns=None)`` 这一遍里解码一次；
    * 任意时刻内存中的明细容器只有：一个源批次（≤ batch_rows 行）和一个
      刚 filter 出来并立即写盘的当日副本（≤ batch_rows 行）；
    * 输出文件同时打开数受 ``slots`` 约束，峰值不超过 2（写片为 1，
      合并时为 1 个成品 writer + 1 个拆片 reader），结构性 ≤ 8。
    """
    if slots is None:
        slots = _OpenFileSlots(_resolve_open_file_limit(settings))
    batch_rows = _effective_batch_rows(settings)
    time_index = pf.schema_arrow.names.index(settings.time_column)
    schema = pf.schema_arrow
    stem = Path(rel_input).stem
    # 同一版本目录可能处理多个同名 stem 的来源（子目录同名文件），
    # 拆片名加来源短令牌避免互撞；成品名仍保持 {stem}__{day}.parquet 约定。
    source_token = f"{zlib.crc32(rel_input.encode('utf-8')) & 0xFFFFFFFF:08x}"

    # day(iso) -> 拆片路径（按产生顺序，即原档顺序）
    parts_by_day: dict[str, list[Path]] = {}
    rows_by_day: dict[str, int] = {}
    part_serial = 0

    # 唯一一次全列扫描：非时间列只解码这一遍
    for batch in pf.iter_batches(batch_size=batch_rows):
        dates = _normalize_date_array(
            to_utc_date_array(batch.column(time_index), settings)
        )
        _check_dates_valid(dates, rel_input)

        for day in pc.unique(dates).to_pylist():
            if day is None:  # 理论上 _check_dates_valid 已拦截，双保险
                raise PublishError(f"{rel_input} 存在空/无法解析的过磅时间，发布中止")
            day_str = day.isoformat() if hasattr(day, "isoformat") else str(day)
            mask = pc.equal(dates, pa.scalar(day, type=pa.date32()))
            part_batch = batch.filter(mask)
            row_count = part_batch.num_rows
            if row_count == 0:
                continue

            # from_batches 零数据拷贝，只是表壳；写盘后立即释放
            table = pa.Table.from_batches([part_batch], schema=schema)
            part_serial += 1
            tmp_part = dest_dir / (
                f".{stem}__{day_str}.{source_token}.part{part_serial:06d}.parquet.tmp"
            )
            _write_part(table, tmp_part, slots)
            final_part = tmp_part.with_suffix("")  # 去掉末尾 .tmp
            os.replace(tmp_part, final_part)

            parts_by_day.setdefault(day_str, []).append(final_part)
            rows_by_day[day_str] = rows_by_day.get(day_str, 0) + row_count
            del table
            del part_batch

    day_files: list[DayFile] = []
    for day_str in sorted(parts_by_day):
        parts = parts_by_day[day_str]
        product = dest_dir / f"{stem}__{day_str}.parquet"
        product_tmp = _tmp_write_path(product)

        if len(parts) == 1:
            # 只有一个拆片：原子改名即成品，零重复解码/拷贝
            os.replace(parts[0], product)
        else:
            # 多个拆片：按原顺序流式合并成唯一成品
            _merge_parts_to_product(
                parts, product_tmp, schema, batch_rows, slots
            )
            os.replace(product_tmp, product)
            for part in parts:
                with contextlib.suppress(FileNotFoundError):
                    part.unlink()

        day_files.append(
            DayFile(
                date=day_str,
                # staged_dir 形如 <output>/staged/v000006，路径相对 staged/ 存放
                path=os.path.join(dest_dir.name, product.name),
                source="split",
                source_file=rel_input,
                rows=rows_by_day[day_str],
            )
        )
    return day_files


def _split_and_write(
    pf: pq.ParquetFile,
    dates=None,
    dest_dir: Path | None = None,
    settings: Settings | None = None,
    rel_input: str | None = None,
    *,
    slots: _OpenFileSlots | None = None,
) -> list[DayFile]:
    """旧函数名兼容包装：忽略预先读出的全量 dates，改为流式拆写。

    历史签名为 ``_split_and_write(pf, dates, dest_dir, settings, rel_input)``；
    新实现不再需要全量日期数组，保留位置参数以兼容既有调用/测试。
    """
    assert dest_dir is not None and settings is not None and rel_input is not None
    return _split_and_write_streaming(
        pf, dest_dir, settings, rel_input, slots=slots
    )


def _process_file(
    abs_path: Path,
    rel_input: str,
    settings: Settings,
    staged_dir: Path,
    slots: _OpenFileSlots | None = None,
) -> tuple[list[DayFile], FileOutcome]:
    pf = pq.ParquetFile(abs_path)
    try:
        total_rows = pf.metadata.num_rows
        if total_rows == 0:
            # 零行文件没有任何记录，不产生按日条目，也不做拆写
            return [], FileOutcome(rel_input, "empty", [], 0)
        verdict, day = footer_verdict(pf, settings)

        if verdict == EMPTY_TIME:
            raise PublishError(f"{rel_input} 页脚证明存在空过磅时间，发布中止")

        if verdict == SAME_DAY:
            # 快路径：完全不读数据行，直接登记原档
            day_str = date_to_str(day)
            return [
                DayFile(
                    date=day_str,
                    path=rel_input,
                    source="original",
                    source_file=rel_input,
                    rows=total_rows,
                )
            ], FileOutcome(rel_input, "reuse_footer", [day_str], total_rows)

        batch_rows = _effective_batch_rows(settings)

        if verdict == UNKNOWN:
            # 页脚证明不了：流式只读时间列确认。同日仍复用原档，不重写。
            unique_days = _scan_time_days_streaming(
                pf, settings, rel_input, batch_rows
            )
            if len(unique_days) <= 1:
                if not unique_days:
                    # 元数据声称非零行却读不到任何日期（异常数据），按空时间处理
                    raise PublishError(f"{rel_input} 未读到任何有效的过磅时间，发布中止")
                day_str = unique_days[0].isoformat()
                return [
                    DayFile(
                        date=day_str,
                        path=rel_input,
                        source="original",
                        source_file=rel_input,
                        rows=total_rows,
                    )
                ], FileOutcome(rel_input, "reuse_scan", [day_str], total_rows)
            # 确认跨日：落到下面的流式全列拆写
        # verdict == MIXED：页脚已证明跨日，直接流式拆写（批次内再校验时间）

        day_files = _split_and_write_streaming(
            pf, staged_dir, settings, rel_input, slots=slots
        )
        return day_files, FileOutcome(
            rel_input,
            "split",
            [d.date for d in day_files],
            total_rows,
        )
    finally:
        with contextlib.suppress(Exception):
            pf.close()


# ---------------------------------------------------------------- 发布入口

def _validate_inputs(input_files: list[str], settings: Settings) -> list[Path]:
    if not input_files:
        raise PublishError("输入文件清单为空")
    resolved: list[Path] = []
    seen: set[str] = set()
    root = settings.input_dir.resolve()
    for rel in input_files:
        p = Path(rel)
        if p.is_absolute() or ".." in p.parts:
            raise PublishError(f"只允许 input_dir 内的相对路径: {rel}")
        abs_path = (root / p).resolve()
        if root not in abs_path.parents:
            raise PublishError(f"路径越界: {rel}")
        if not abs_path.is_file():
            raise PublishError(f"输入文件不存在: {rel}")
        key = str(abs_path)
        if key in seen:
            raise PublishError(f"输入清单存在重复文件: {rel}")
        seen.add(key)
        resolved.append(abs_path)
    return resolved


def _check_same_schema(paths: list[Path], settings: Settings) -> None:
    base_pf = pq.ParquetFile(paths[0])
    try:
        base_schema = base_pf.schema_arrow
    finally:
        with contextlib.suppress(Exception):
            base_pf.close()
    if settings.time_column not in base_schema.names:
        raise PublishError(
            f"文件 {paths[0].name} 缺少过磅时间列 {settings.time_column!r}"
        )
    for p in paths[1:]:
        pf = pq.ParquetFile(p)
        try:
            schema = pf.schema_arrow
        finally:
            with contextlib.suppress(Exception):
                pf.close()
        if settings.time_column not in schema.names:
            raise PublishError(f"文件 {p.name} 缺少过磅时间列 {settings.time_column!r}")
        if not schema.equals(base_schema):
            raise PublishError(
                f"文件 {p.name} 与首文件结构不一致，拒绝混结构发布"
            )


def _cleanup_staged_dir(staged_dir: Path) -> None:
    """彻底清理本次版本的全部拆片与临时文件（含隐藏文件）。"""
    with contextlib.suppress(OSError):
        shutil.rmtree(staged_dir, ignore_errors=True)


def publish(input_files: list[str], settings: Settings) -> PublishResult:
    """按输入清单发布新版本；失败抛原始异常（PublishError/OSError 等），旧版保留。"""
    settings.input_dir.mkdir(parents=True, exist_ok=True)
    store = ManifestStore(settings.output_dir)
    store.ensure_dirs()

    abs_paths = _validate_inputs(input_files, settings)
    _check_same_schema(abs_paths, settings)

    with _FileLock(store.lock_path, settings.lock_timeout_seconds):
        # 锁内重新读取版本号，避免并发发布撞版本目录
        version = store.next_version()
        staged_dir = store.staged_path_for(version)
        slots = _OpenFileSlots(_resolve_open_file_limit(settings))
        all_day_files: list[DayFile] = []
        outcomes: list[FileOutcome] = []
        try:
            rel_paths = []
            for abs_path in abs_paths:
                rel = abs_path.relative_to(settings.input_dir).as_posix()
                rel_paths.append(rel)
                day_files, outcome = _process_file(
                    abs_path, rel, settings, staged_dir, slots
                )
                all_day_files.extend(day_files)
                outcomes.append(outcome)

            # 每个输入文件的记录必须且只能落在其实际过磅日；
            # 按 (date, source_file) 复核行数不重不漏
            _check_coverage(abs_paths, rel_paths, all_day_files, settings)

            all_day_files.sort(key=lambda d: (d.date, d.source_file, d.path))
            manifest = Manifest(
                version=version,
                created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
                time_column=settings.time_column,
                input_files=rel_paths,
                days=all_day_files,
            )
            store.commit(manifest)  # 全部新文件就绪后才换版
        except BaseException:
            # 清理本次半成品暂存目录（拆片 + .tmp）；current.json 从未被触碰
            _cleanup_staged_dir(staged_dir)
            # 若版本归档已写、但 current.json 原子替换未完成，撤掉该归档，
            # 避免留下「看不见的新版本」。current 已是本版本时绝不能删。
            with contextlib.suppress(Exception):
                current = store.load_current()
                if current is None or current.version < version:
                    archive = (
                        store.versions_dir / f"manifest-v{version:06d}.json"
                    )
                    if archive.is_file():
                        archive.unlink()
            raise

    return PublishResult(
        manifest=store.load_current(),
        outcomes=outcomes,
        peak_open_files=slots.peak_open,
    )


def _check_coverage(
    abs_paths: list[Path],
    rel_paths: list[str],
    day_files: list[DayFile],
    settings: Settings,
) -> None:
    expected_rows: dict[str, int] = {}
    for rel, abs_path in zip(rel_paths, abs_paths):
        pf = pq.ParquetFile(abs_path)
        try:
            expected_rows[rel] = pf.metadata.num_rows
        finally:
            with contextlib.suppress(Exception):
                pf.close()
    actual_rows: dict[str, int] = {rel: 0 for rel in rel_paths}
    for df in day_files:
        if df.source_file not in actual_rows:
            raise PublishError(f"清单条目来源未知: {df.source_file}")
        actual_rows[df.source_file] += df.rows
    for rel, expected in expected_rows.items():
        if actual_rows[rel] != expected:
            raise PublishError(
                f"{rel} 行数核对失败: 输入 {expected} 行, 发布 {actual_rows[rel]} 行"
            )
