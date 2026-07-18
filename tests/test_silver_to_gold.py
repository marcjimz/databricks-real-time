"""Tests for the pure helpers in the silver → gold windowing pipeline.

The Spark-bound parts (foreachBatch, MERGE, streaming read) run only on the
cluster, so we keep the local suite to the two pure functions that carry the
windowing and census math — the pieces that must never silently drift.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines.silver_to_gold import _census_counts, _window_start_epoch  # noqa: E402


# -- tumbling-window flooring ------------------------------------------------
def test_window_floors_to_start():
    # any ts inside [1_000_000, 1_000_010) maps to the same 10 s window start
    assert _window_start_epoch(1_000_000.0) == 1_000_000
    assert _window_start_epoch(1_000_009.999) == 1_000_000
    assert _window_start_epoch(1_000_010.0) == 1_000_010


def test_window_respects_custom_width():
    assert _window_start_epoch(1_234_567.0, window_s=60) == 1_234_560


# -- census delta math -------------------------------------------------------
def test_census_counts_admits_discharges_net():
    stream = ["ADT^A01", "ADT^A01", "ADT^A03", "ORU^R01", "ADT^A01"]
    assert _census_counts(stream) == (3, 1, 2)


def test_census_net_can_go_negative():
    assert _census_counts(["ADT^A03", "ADT^A03", "ADT^A01"]) == (1, 2, -1)


def test_census_empty_stream():
    assert _census_counts([]) == (0, 0, 0)
