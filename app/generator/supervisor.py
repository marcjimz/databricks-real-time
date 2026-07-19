"""Asyncio event-generator supervisor (spec section 3).

Owns the worker loop, live traffic controls, backpressure, and telemetry. It is
transport-agnostic: it drives a ``TransportSink`` (Path A Zerobus or Path B
Event Hubs) and never imports a concrete front door. Switching paths drains the
old sink, freezes its summary, and restarts workers on the new one — the visible
"swap the front door" beat.

Design:
  * Up to 16 per-worker tasks each run a strict 1 Hz cadence: every second an
    active worker generates exactly ``rate`` records and fires ONE async Zerobus
    POST, awaiting the durable ack and recording its latency into a ring buffer.
    Output is therefore a steady ``rate``/s per active worker — predictable, not
    bursty — and sends overlap concurrently so a slow ack widens latency rather
    than stalling generation.
  * A 1 Hz rollup task snapshots per-worker send counts + ack percentiles into
    ``rt_gen_metrics`` rows, handed to an injected ``on_rollup`` callback (wired
    to Lakebase by the app; captured in tests). The same numbers live on the
    in-memory ``state`` the dashboard polls each tick.

Runs its own asyncio loop in a daemon thread (``start_in_thread``) so the
synchronous Dash callbacks can drive it with plain method calls.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from config import Config
from .hl7_factory import HL7Factory
from .profiles import Profile, profile_for
from .sinks import SendResult, TransportSink, sink_for

log = logging.getLogger("hl7.generator")

BURST_FACTOR = 10                # burst multiplies rate 10× ...
BURST_SECONDS = 15               # ... for 15 s
DRAIN_TIMEOUT_S = 10             # path-switch drain budget (spec section 2.1)
_RING = 512                      # ack-latency samples retained per rollup window
_POOL_MIN = 1000                 # min pre-generated records per worker to rotate


@dataclass
class Controls:
    """Live, operator-tunable knobs (mutated directly by the UI thread)."""

    rate_per_worker: int = 50        # 1–1000 records/s
    workers: int = 2                 # 1–16
    malformed_pct: float = 0.0       # 0–20
    mix: dict[str, float] | None = None  # None = use profile preset
    running: bool = False
    burst_until: float = 0.0         # monotonic deadline; >now = bursting


@dataclass
class GeneratorState:
    """Snapshot the dashboard polls at 1 Hz (spec section 6, slot B)."""

    path: str = "zerobus"
    running: bool = False
    throttled: bool = False
    switching: bool = False
    workers: int = 0
    rate_per_worker: int = 0
    malformed_pct: float = 0.0
    bursting: bool = False
    sent_total: int = 0
    errors_total: int = 0
    last_error: str = ""      # most recent send failure (surfaced to UI/logs)
    unacked: int = 0
    sent_last_s: int = 0
    ack_p50_ms: int = 0
    ack_p95_ms: int = 0
    ack_p99_ms: int = 0


def _percentiles(samples: list[float]) -> tuple[int, int, int]:
    if not samples:
        return 0, 0, 0
    ordered = sorted(samples)

    def _p(q: float) -> int:
        idx = min(len(ordered) - 1, int(q * len(ordered)))
        return int(ordered[idx])

    return _p(0.50), _p(0.95), _p(0.99)


class Supervisor:
    """Path-agnostic generator engine driving one ``TransportSink`` at a time."""

    def __init__(self, cfg: Config, on_rollup=None):
        self._cfg = cfg
        self._on_rollup = on_rollup      # callable(list[dict]) -> None, optional
        self.controls = Controls()
        self.state = GeneratorState(path=cfg.ingest_path)

        self._sink: TransportSink | None = None
        self._profile: Profile = profile_for(cfg.ingest_path)
        self._tasks: list[asyncio.Task] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

        # telemetry accumulators (single-loop, no locking needed)
        self._sent_since_rollup = 0
        self._acks: deque[float] = deque(maxlen=_RING)
        self._sent_window: deque[int] = deque(maxlen=1)

    # -- thread lifecycle ----------------------------------------------------
    def start_in_thread(self) -> None:
        """Spin the asyncio loop in a daemon thread (for the Dash app)."""
        if self._thread and self._thread.is_alive():
            return
        ready = threading.Event()

        def _run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.call_soon(ready.set)
            self._loop.run_forever()

        self._thread = threading.Thread(target=_run, name="gen-supervisor", daemon=True)
        self._thread.start()
        ready.wait(timeout=5)
        self._submit(self._ensure_sink())
        self._submit_soon(self._rollup_loop())

    def _submit(self, coro):
        """Schedule a coroutine on the loop from the UI thread and wait briefly."""
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=15)

    def _submit_soon(self, coro) -> None:
        assert self._loop is not None
        self._loop.call_soon_threadsafe(lambda: self._tasks.append(self._loop.create_task(coro)))

    # -- controls (called from the UI thread) --------------------------------
    def start(self) -> None:
        self.controls.running = True

    def stop(self) -> None:
        self.controls.running = False

    def set_rate(self, rate: int) -> None:
        self.controls.rate_per_worker = max(1, min(1000, int(rate)))

    def set_workers(self, n: int) -> None:
        self.controls.workers = max(1, min(16, int(n)))

    def set_malformed(self, pct: float) -> None:
        self.controls.malformed_pct = max(0.0, min(20.0, float(pct)))

    def set_mix(self, mix: dict[str, float] | None) -> None:
        self.controls.mix = mix

    def burst(self) -> None:
        self.controls.burst_until = time.monotonic() + BURST_SECONDS

    def reset_metrics(self) -> None:
        """Zero the in-memory counters so the dashboard starts from a clean slate.

        Clears the cumulative totals and telemetry accumulators the header/cards
        display; does not stop generation or touch the serving tables (the app
        pairs this with a Lakebase truncate).
        """
        self.state.sent_total = 0
        self.state.errors_total = 0
        self.state.last_error = ""
        self.state.unacked = 0
        self.state.sent_last_s = 0
        self.state.ack_p50_ms = self.state.ack_p95_ms = self.state.ack_p99_ms = 0
        self._sent_since_rollup = 0
        self._acks.clear()

    def switch_path(self, path: str) -> None:
        """Drain the current sink and restart workers on the new path."""
        if path == self.state.path:
            return
        self._submit(self._switch_path(path))

    # -- sink / path management (loop thread) --------------------------------
    async def _ensure_sink(self) -> None:
        if self._sink is None:
            self._sink = sink_for(self.state.path, self._cfg)
            self._profile = profile_for(self.state.path)
            self._spawn_workers()

    async def _switch_path(self, path: str) -> None:
        # Swapping the front door STOPS generation by default: the two paths are
        # exclusive experiments, so switching ends the current test rather than
        # silently continuing on the new transport. The operator clicks Run to
        # start the new path deliberately (avoids surprise billing on Path B's
        # classic ingest cluster, and keeps each path's metrics clean).
        self.state.switching = True
        self.controls.running = False
        await asyncio.sleep(min(DRAIN_TIMEOUT_S, 1.0))  # let in-flight batches settle
        await self._teardown_workers()
        if self._sink is not None:
            await self._sink.aclose()
        self.state.path = path
        self._sink = sink_for(path, self._cfg)
        self._profile = profile_for(path)
        self._spawn_workers()
        self.controls.running = False  # stay stopped after a swap — click Run to start
        self.state.switching = False

    def _spawn_workers(self) -> None:
        for wid in range(16):
            self._tasks.append(asyncio.create_task(self._worker(wid)))

    async def _teardown_workers(self) -> None:
        for t in self._tasks:
            if t.get_coro().__name__ == "_worker":
                t.cancel()
        self._tasks = [t for t in self._tasks if not t.cancelled()]
        await asyncio.sleep(0)

    # -- worker loop (loop thread) -------------------------------------------
    def _effective_rate(self) -> int:
        rate = self.controls.rate_per_worker
        if time.monotonic() < self.controls.burst_until:
            rate *= BURST_FACTOR
        return rate

    async def _worker(self, wid: int) -> None:
        """One active worker = one steady 1 Hz cadence: each second generate
        exactly ``rate`` records and fire ONE async Zerobus POST.

        This is the whole send path (no producer/queue/sender split): a clean
        per-second batch keeps output at a predictable ``rate``/s per worker and
        the Zerobus REST insert is one async call per second. In-flight sends
        overlap naturally (each second's POST is awaited concurrently across
        workers), so a slow ack doesn't stall generation — it just widens the
        ack-latency the dashboard reports.
        """
        factory = HL7Factory(self._profile, worker_id=f"w{wid}", malformed_pct=0.0)
        last_log = 0.0
        while True:
            await asyncio.sleep(1.0)                     # steady 1 s cadence
            if not self.controls.running or wid >= self.controls.workers:
                continue
            factory.profile = (_with_mix(self._profile, self.controls.mix)
                               if self.controls.mix else self._profile)
            factory.malformed_pct = self.controls.malformed_pct

            n = self._effective_rate()                   # records for this second
            # Fresh records each second: every one carries its own event_id +
            # ts_generated (stamped in make()), so e2e latency and Lakebase PKs
            # stay correct. make() is fast enough at these rates (proven in the
            # standalone bench) that generation adds no meaningful send jitter.
            batch = [factory.make() for _ in range(n)]
            if not batch:
                continue
            self.state.unacked += len(batch)
            assert self._sink is not None
            try:
                res: SendResult = await self._sink.send(batch)
            except Exception as exc:  # transport not ready (e.g. Path B stub)
                res = SendResult(ok=False, count=len(batch), latency_ms=0.0, error=str(exc))
            self.state.unacked = max(0, self.state.unacked - len(batch))
            if res.ok:
                self._sent_since_rollup += res.count
                self.state.sent_total += res.count
                self._acks.append(res.latency_ms / max(1, res.count))
            else:
                self.state.errors_total += len(batch)
                self.state.last_error = res.error or "send failed (no detail)"
                now = time.monotonic()
                if now - last_log >= 5.0:               # throttle /logz spam
                    last_log = now
                    log.error("worker w%d send failed (%d recs): %s",
                              wid, len(batch), self.state.last_error)

    # -- telemetry rollup (loop thread) --------------------------------------
    async def _rollup_loop(self) -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(1.0)
            # sent as a per-SECOND RATE over the ACTUAL elapsed interval, not the
            # raw count this window. The rollup fires every ~1.0 s + its own work,
            # so it drifts against the worker's 1.0 s send cadence — a raw count
            # then reads 100,100,0,100... (two sends land in one window, none in
            # the next). Dividing by real elapsed seconds smooths that to ~100/s.
            now = time.monotonic()
            elapsed = max(now - last, 1e-3)
            last = now
            sent = round(self._sent_since_rollup / elapsed)
            self._sent_since_rollup = 0
            p50, p95, p99 = _percentiles(list(self._acks))
            self._acks.clear()

            self.state.running = self.controls.running
            self.state.workers = self.controls.workers if self.controls.running else 0
            self.state.rate_per_worker = self.controls.rate_per_worker
            self.state.malformed_pct = self.controls.malformed_pct
            self.state.bursting = time.monotonic() < self.controls.burst_until
            self.state.sent_last_s = sent
            self.state.ack_p50_ms, self.state.ack_p95_ms, self.state.ack_p99_ms = p50, p95, p99

            if self._on_rollup and (sent or self.state.running):
                row = {
                    "source_path": self.state.path,
                    "worker_id": "supervisor",
                    "sent": sent,
                    "ack_p50_ms": p50, "ack_p95_ms": p95, "ack_p99_ms": p99,
                    "throttled": int(self.state.throttled),
                }
                try:
                    self._on_rollup([row])
                except Exception:  # metrics sink must never crash generation
                    pass


def _with_mix(profile: Profile, mix: dict[str, float]) -> Profile:
    """Return a copy of ``profile`` with the live mix-slider weights applied."""
    return Profile(
        name=profile.name, facilities=profile.facilities, mrn_prefix=profile.mrn_prefix,
        mix={k: float(v) for k, v in mix.items() if v > 0} or profile.mix,
        obx_min=profile.obx_min, obx_max=profile.obx_max,
    )
