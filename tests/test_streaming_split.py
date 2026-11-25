"""流式拆写资源上限与尾批失败的回归测试（本文件自包含，不依赖 conftest 辅助）。

覆盖任务新增要求：
* 超过 8 个 UTC 日期且严格交错（含重复行）时，日档内容与原档顺序一致；
* 暂存拆片 + 成品文件同时打开数绝不超过 8；
* 内存明细行数有固定上限（批次 + 过滤副本），每次写盘负载都不超过批次上限；
* 跨日原档的非时间列只做一次全列扫描；
* 最后一批写盘失败 / 末尾批次出现无效时间：清理拆片与临时文件并保留旧版。
"""

from __future__ import annotations

import datetime as dt
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.config import Settings
from app.manifest import ManifestStore
from app.publisher import publish

SCHEMA = pa.schema(
    [
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us")),
        pa.field("gross_weight_kg", pa.int64()),
    ]
)


def _utc(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=dt.timezone.utc
    )


def _write(path, rows, *, write_statistics=True):
    pq.write_table(
        pa.table(
            {
                "ticket_id": [r[0] for r in rows],
                "weigh_time": [r[1] for r in rows],
                "gross_weight_kg": [r[2] for r in rows],
            },
            schema=SCHEMA,
        ),
        path,
        compression="snappy",
        write_statistics=write_statistics,
    )


@pytest.fixture()
def settings(tmp_path):
    in_dir = tmp_path / "input"
    in_dir.mkdir()
    return Settings(
        input_dir=in_dir,
        output_dir=tmp_path / "output",
        time_column="weigh_time",
        time_string_format=None,
        lock_timeout_seconds=5,
    )


def _interleaved_rows(ndates=12, cycles=8, duplicates=True):
    """构造按日期严格交错的行：每轮依次穿过所有日期，含重复行。"""
    rows = []
    seq = 0
    for c in range(cycles):
        for d in range(1, ndates + 1):
            repeats = 2 if duplicates and (c + d) % 4 == 0 else 1
            for r in range(repeats):
                seq += 1
                hour = (c * 2 + r) % 24
                rows.append(
                    (
                        f"K{seq:06d}-D{d:02d}",
                        _utc(f"2026-11-{d:02d} {hour:02d}:00:00"),
                        seq,
                    )
                )
    return rows


def test_more_than_eight_dates_interleaved_keeps_order_and_duplicates(settings):
    rows = _interleaved_rows(ndates=14, cycles=10)
    _write(settings.input_dir / "wide.parquet", rows)

    result = publish(["wide.parquet"], settings)
    assert result.outcomes[0].mode == "split"
    assert result.peak_open_files <= 8

    manifest = result.manifest
    assert len({d.date for d in manifest.days}) == 14

    products = list((settings.output_dir / "staged").rglob("wide__2026-*.parquet"))
    assert len(products) == 14
    leftovers = [
        p
        for p in (settings.output_dir / "staged").rglob("*")
        if p.is_file() and (".part" in p.name or p.name.endswith(".tmp"))
    ]
    assert leftovers == []

    expected: dict[str, list[str]] = {}
    for ticket, when, _w in rows:
        # when 为 UTC 瞬间，写入朴素 timestamp 时保留其 UTC 字面值
        day = when.date().isoformat()
        expected.setdefault(day, []).append(ticket)

    store = ManifestStore(settings.output_dir)
    total = 0
    for entry in manifest.days:
        path = store.resolve(entry, settings.input_dir)
        ids = pq.read_table(path).column("ticket_id").to_pylist()
        assert ids == expected[entry.date]
        assert entry.rows == len(expected[entry.date])
        total += len(ids)
    assert total == len(rows)


def test_open_parquet_writers_peak_at_most_eight(settings, monkeypatch):
    monkeypatch.setenv("WEIGH_SPLIT_BATCH_ROWS", "13")
    rows = _interleaved_rows(ndates=16, cycles=4)
    _write(settings.input_dir / "writers.parquet", rows)

    live = 0
    peak = 0
    real_init = pq.ParquetWriter.__init__
    real_close = pq.ParquetWriter.close

    def tracked_init(self, *args, **kwargs):
        nonlocal live, peak
        result = real_init(self, *args, **kwargs)
        live += 1
        peak = max(peak, live)
        return result

    def tracked_close(self, *args, **kwargs):
        nonlocal live
        try:
            return real_close(self, *args, **kwargs)
        finally:
            live -= 1

    monkeypatch.setattr(pq.ParquetWriter, "__init__", tracked_init)
    monkeypatch.setattr(pq.ParquetWriter, "close", tracked_close)

    result = publish(["writers.parquet"], settings)
    assert result.outcomes[0].mode == "split"
    assert peak <= 8, f"同时存活的 ParquetWriter 峰值 {peak} > 8"
    assert result.peak_open_files <= 8


def test_write_payloads_never_exceed_configured_row_cap(settings, monkeypatch):
    """内存上限 300 行（含副本）=> 批次 150；每次写盘负载不得超过批次大小。"""
    monkeypatch.setenv("WEIGH_MAX_MEMORY_ROWS", "300")
    rows = _interleaved_rows(ndates=10, cycles=30)
    _write(settings.input_dir / "cap.parquet", rows)

    sizes: list[int] = []
    real_write = pq.write_table

    def spy(table, where, *args, **kwargs):
        sizes.append(table.num_rows)
        return real_write(table, where, *args, **kwargs)

    monkeypatch.setattr(pq, "write_table", spy)
    publish(["cap.parquet"], settings)

    assert sizes
    assert max(sizes) <= 150, max(sizes)


def test_original_nontime_columns_scanned_once(settings, monkeypatch):
    rows = _interleaved_rows(ndates=11, cycles=3)
    target = settings.input_dir / "scanonce.parquet"
    _write(target, rows)
    orig_path = str(target.resolve())

    calls: list[object] = []
    real_iter = pq.ParquetFile.iter_batches
    real_init = pq.ParquetFile.__init__

    def tagged_init(self, source, *args, **kwargs):
        result = real_init(self, source, *args, **kwargs)
        try:
            self._test_source_path = os.path.abspath(os.fspath(source))
        except Exception:
            self._test_source_path = ""
        return result

    def spy_iter(self, *args, **kwargs):
        if getattr(self, "_test_source_path", "") == orig_path:
            calls.append(kwargs.get("columns", "<all>"))
        return real_iter(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "__init__", tagged_init)
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", spy_iter)

    publish(["scanonce.parquet"], settings)
    assert calls == ["<all>"], calls


def test_tail_batch_write_failure_keeps_previous_version(settings, monkeypatch):
    _write(
        settings.input_dir / "good.parquet",
        [("G1", _utc("2026-11-23 09:00:00"), 1)],
    )
    first = publish(["good.parquet"], settings)
    assert first.manifest.version == 1

    monkeypatch.setenv("WEIGH_SPLIT_BATCH_ROWS", "7")
    rows = _interleaved_rows(ndates=10, cycles=12)
    _write(settings.input_dir / "tail.parquet", rows)
    last_id = rows[-1][0]

    real_write = pq.write_table

    def poison(table, where, *args, **kwargs):
        if last_id in table.column("ticket_id").to_pylist():
            raise OSError("模拟最后一批写盘失败")
        return real_write(table, where, *args, **kwargs)

    monkeypatch.setattr(pq, "write_table", poison)
    with pytest.raises(OSError, match="最后一批"):
        publish(["tail.parquet"], settings)

    store = ManifestStore(settings.output_dir)
    current = store.load_current()
    assert current.version == 1
    assert current.input_files == ["good.parquet"]
    assert not (settings.output_dir / "staged" / "v000002").exists()
    assert not [
        p for p in (settings.output_dir / "staged").rglob("*") if p.is_file()
    ]
    assert not (settings.output_dir / "versions" / "manifest-v000002.json").exists()

    from app.service import DatasetService

    got = DatasetService(settings).read_date("2026-11-23")
    assert got.column("ticket_id").to_pylist() == ["G1"]


def test_invalid_time_in_late_batch_aborts_and_cleans(settings, monkeypatch):
    monkeypatch.setenv("WEIGH_SPLIT_BATCH_ROWS", "25")
    n = 400
    rows = []
    for i in range(n):
        day = 1 + (i % 20)
        rows.append(
            (f"B{i:04d}", _utc(f"2026-11-{day:02d} {i % 24:02d}:00:00"), i)
        )
    rows[-1] = (rows[-1][0], None, rows[-1][2])
    _write(
        settings.input_dir / "latbad.parquet", rows, write_statistics=False
    )

    with pytest.raises(Exception, match="空"):
        publish(["latbad.parquet"], settings)

    assert ManifestStore(settings.output_dir).load_current() is None
    assert not [
        p for p in (settings.output_dir / "staged").rglob("*") if p.is_file()
    ]


def test_invalid_string_found_during_split_cleans_written_parts(
    settings, monkeypatch
):
    str_schema = pa.schema(
        [
            pa.field("ticket_id", pa.string()),
            pa.field("weigh_time", pa.string()),
            pa.field("gross_weight_kg", pa.int64()),
        ]
    )
    monkeypatch.setenv("WEIGH_SPLIT_BATCH_ROWS", "20")
    n = 300
    ids = [f"S{i}" for i in range(n)]
    times = [
        f"2026-11-{1 + (i % 12):02d}T{i % 24:02d}:00:00" for i in range(n)
    ]
    times[-2] = "2026-11-06T25:61:61"  # 词法居中（页脚仍 MIXED），语义无法解析
    pq.write_table(
        pa.table(
            {
                "ticket_id": ids,
                "weigh_time": times,
                "gross_weight_kg": list(range(n)),
            },
            schema=str_schema,
        ),
        settings.input_dir / "badstring.parquet",
    )

    from app.publisher import footer_verdict

    verdict, _ = footer_verdict(
        pq.ParquetFile(settings.input_dir / "badstring.parquet"), settings
    )
    assert verdict == "mixed"

    with pytest.raises(Exception, match="空/无法解析"):
        publish(["badstring.parquet"], settings)

    assert not [
        p for p in (settings.output_dir / "staged").rglob("*") if p.is_file()
    ]
    assert ManifestStore(settings.output_dir).load_current() is None
