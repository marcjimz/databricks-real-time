"""Tests for the pure formatting/aggregation helpers behind slots D/E/F.

Dash layout construction is exercised visually; here we pin only the pure
helpers that decide threshold coloring (stage rail) and shape the chart series
(throughput/latency) — the logic most likely to regress unnoticed.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.components.charts import _annotations, _line, _rgba, _sum_by_second  # noqa: E402
from app.components.stage_rail import _fmt_ms, _fresh_class, _lag_class  # noqa: E402


# -- stage rail formatting / thresholds --------------------------------------
def test_fmt_ms_adaptive_units():
    # Adaptive: sub-second in ms (so a fast bronze hop reads "5 ms", not "0.0s"),
    # ≥1 s in seconds to the tenth.
    assert _fmt_ms(None) == "—"
    assert _fmt_ms(5) == "5 ms"
    assert _fmt_ms(420) == "420 ms"
    assert _fmt_ms(1500) == "1.5s"
    assert _fmt_ms(8000) == "8.0s"


def test_lag_class_thresholds():
    assert _lag_class(None) == ""
    assert _lag_class(3) == ""
    assert _lag_class(10) == "caution"
    assert _lag_class(45) == "warn"


def test_fresh_class_thresholds():
    assert _fresh_class(None) == ""
    assert _fresh_class(2) == ""
    assert _fresh_class(8) == "caution"
    assert _fresh_class(20) == "warn"


# -- chart series shaping ----------------------------------------------------
def test_rgba_from_hex():
    assert _rgba("#8B7FE8", 0.14) == "rgba(139,127,232,0.14)"


def test_line_sorts_and_drops_nulls():
    rows = [
        {"ts": "00:00:03", "v": 3},
        {"ts": "00:00:01", "v": 1},
        {"ts": "00:00:02", "v": None},
        {"ts": "00:00:04", "v": 4},
    ]
    xs, ys = _line(rows, "ts", "v")
    assert xs == ["00:00:01", "00:00:03", "00:00:04"]
    assert ys == [1, 3, 4]


def test_sum_by_second_aggregates_workers():
    # two workers each write a row at the same second → summed
    rows = [
        {"ts": "00:00:01", "sent": 100},
        {"ts": "00:00:01", "sent": 120},
        {"ts": "00:00:02", "sent": 90},
    ]
    xs, ys = _sum_by_second(rows, "ts", "sent")
    assert xs == ["00:00:01", "00:00:02"]
    assert ys == [220, 90]


def test_annotations_only_non_null():
    rows = [
        {"batch_ts": "00:00:01", "annotation": "burst 10×"},
        {"batch_ts": "00:00:02", "annotation": None},
        {"batch_ts": "00:00:03", "annotation": "rate 5/s"},
    ]
    assert _annotations(rows) == [("00:00:01", "burst 10×"), ("00:00:03", "rate 5/s")]
