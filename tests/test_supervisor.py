"""Tests for the generator supervisor (app/generator/supervisor.py).

Lean per the TDD directive: the pure control math (percentiles, live mix
override) is unit-tested directly, and one integration test drives the real
threaded supervisor against a fake in-memory sink to prove records flow and
telemetry rolls up — without touching the network.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import CONFIG  # noqa: E402
from app.generator import supervisor as sup  # noqa: E402
from app.generator.sinks import SendResult, TransportSink  # noqa: E402


class FakeSink(TransportSink):
    path = "zerobus"

    def __init__(self):
        self.batches: list[list[dict]] = []

    async def send(self, records):
        self.batches.append(records)
        return SendResult(ok=True, count=len(records), latency_ms=5.0)

    async def aclose(self):
        pass


# -- pure control math -------------------------------------------------------
def test_percentiles_ordering():
    p50, p95, p99 = sup._percentiles([float(x) for x in range(100)])
    assert p50 <= p95 <= p99
    assert p50 == 50 and p99 == 99


def test_percentiles_empty():
    assert sup._percentiles([]) == (0, 0, 0)


def test_mix_override_replaces_weights():
    base = sup.profile_for("zerobus")
    mixed = sup._with_mix(base, {"ADT^A01": 100.0})
    assert mixed.mix == {"ADT^A01": 100.0}
    assert mixed.facilities == base.facilities  # vocabulary preserved


# -- integration: threaded supervisor drives the sink ------------------------
def test_supervisor_sends_and_rolls_up(monkeypatch):
    fake = FakeSink()
    monkeypatch.setattr(sup, "sink_for", lambda path, cfg: fake)

    rollups: list[dict] = []
    s = sup.Supervisor(CONFIG, on_rollup=lambda rows: rollups.extend(rows))
    s.set_rate(50)
    s.set_workers(2)
    s.start_in_thread()
    s.start()
    try:
        time.sleep(1.4)
    finally:
        s.stop()

    assert s.state.sent_total > 0, "no records were sent"
    assert fake.batches, "sink received no batches"
    # factory records carry the envelope fields the sink will map to bronze
    rec = fake.batches[0][0]
    assert {"event_id", "hl7_raw", "message_type", "ts_generated"} <= rec.keys()
    assert rollups, "no telemetry rollup emitted"
    assert rollups[-1]["source_path"] == "zerobus"
