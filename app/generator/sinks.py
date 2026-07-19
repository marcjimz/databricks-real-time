"""Pluggable transport sinks — the swappable "front door" (spec sections 2, 3).

The demo's thesis is *swap the front door, keep the house*: one HL7 factory feeds
one of two transports, and everything downstream (bronze → silver → Lakebase
serving) is identical. This module is that swap point.

A ``TransportSink`` takes factory records and delivers them to bronze:

  * ``ZerobusSink``  — Path A. Wraps the Zerobus Direct Write REST client and
    writes bronze rows directly (no broker, no ingest job). Owns the bronze
    wire-encoding (epoch-microsecond timestamps, explicit ``ts_bronze`` stamp),
    which is the one Zerobus-specific detail proven out in Phase 0.
  * ``KafkaSink``    — Path B. Produces the JSON envelope to the Azure Event
    Hubs Kafka endpoint; a thin ``eventhub_to_bronze`` Spark job lands it in
    bronze. Built in Phase 3 (Event Hubs is provisioned by Terraform then).

Both present the same async ``send`` / ``aclose`` surface so the supervisor is
path-agnostic — it never imports a concrete transport.
"""

from __future__ import annotations

import abc
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from config import Config
from .zerobus_client import ZerobusRestClient

# Real bronze columns; the generator-only underscore hints (_summary,
# _expected_error) never leave the app.
_BRONZE_COLS = (
    "event_id", "source_path", "facility_id", "message_type",
    "hl7_raw", "gen_worker_id",
)


@dataclass
class SendResult:
    """Transport-agnostic outcome of delivering one batch."""

    ok: bool
    count: int
    latency_ms: float
    error: str = ""


def _iso_to_micros(iso: str) -> int:
    """HL7/ISO timestamp → epoch microseconds (Zerobus TIMESTAMP encoding)."""
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1_000_000)


class TransportSink(abc.ABC):
    """One ingestion front door. Stateless w.r.t. the supervisor's worker loop."""

    #: "zerobus" | "eventhub" — stamped onto every record's source_path.
    path: str

    @abc.abstractmethod
    async def send(self, records: list[dict]) -> SendResult:
        """Deliver a batch of factory records to bronze. Returns on durable ack."""

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Release transport resources (connections / producer)."""


class ZerobusSink(TransportSink):
    """Path A: batched HTTP writes straight to ``bronze_hl7_raw`` via Zerobus REST."""

    path = "zerobus"

    def __init__(self, cfg: Config, client: ZerobusRestClient | None = None):
        self._cfg = cfg
        self._client = client or ZerobusRestClient(cfg, table=cfg.table("bronze_hl7_raw"))

    def _to_bronze(self, rec: dict) -> dict:
        """Factory record → bronze wire row (epoch-micros ts, explicit ts_bronze).

        Zerobus forbids column DEFAULTs, so Path A stamps ``ts_bronze`` here,
        just before the durable POST. TIMESTAMP columns encode as epoch
        microseconds (millis/seconds silently land in 1970).
        """
        out = {k: rec.get(k) for k in _BRONZE_COLS}
        out["source_path"] = self.path
        out["ts_generated"] = _iso_to_micros(rec["ts_generated"])
        out["ts_bronze"] = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        return out

    async def send(self, records: list[dict]) -> SendResult:
        wire = [self._to_bronze(r) for r in records]
        res = await self._client.insert(wire)
        return SendResult(ok=res.ok, count=res.count, latency_ms=res.latency_ms, error=res.error)

    async def aclose(self) -> None:
        await self._client.aclose()


#: Envelope fields carried over the wire to Event Hubs. ts_generated travels as
#: an ISO string (the consumer parses it); ts_transport/ts_bronze are stamped on
#: the CONSUMER side (eventhub_to_bronze) so the broker + land hops are real and
#: Path B stays directly comparable to Path A.
_ENVELOPE_COLS = (*_BRONZE_COLS, "ts_generated")


class KafkaSink(TransportSink):
    """Path B: produce the JSON envelope to Azure Event Hubs (Kafka endpoint).

    Transport = confluent-kafka producer, SASL_SSL/PLAIN to ``host:9093`` with
    the Event Hubs connection string as the SASL password (username is the
    literal ``$ConnectionString``). A thin ``eventhub_to_bronze`` Structured
    Streaming job (the Path B front door on the consumer side) deserialises the
    envelope, stamps ``ts_transport`` + ``ts_bronze``, and lands it in the same
    bronze table — so the two paths stay directly comparable.

    The producer is created lazily on the first send (so merely constructing the
    sink is cheap and can't fail), and its ``flush`` runs off the event loop to
    keep the async supervisor non-blocking.
    """

    path = "eventhub"

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._producer = None  # lazily built on first send

    def _ensure_producer(self):
        if self._producer is not None:
            return self._producer
        if not self._cfg.eventhub_bootstrap or not self._cfg.eventhub_connection_string:
            raise RuntimeError(
                "Event Hubs not configured: set EVENTHUB_BOOTSTRAP and "
                "EVENTHUB_CONNECTION_STRING (see resources/eventhub secrets)."
            )
        from confluent_kafka import Producer

        self._producer = Producer({
            "bootstrap.servers": self._cfg.eventhub_bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": self._cfg.eventhub_connection_string,
            # Batch a little for throughput; EH tolerates larger linger well.
            "linger.ms": 20,
            "compression.type": "none",
            "acks": "all",
        })
        return self._producer

    def _to_envelope(self, rec: dict) -> bytes:
        out = {k: rec.get(k) for k in _ENVELOPE_COLS}
        out["source_path"] = self.path
        return json.dumps(out, separators=(",", ":")).encode("utf-8")

    async def send(self, records: list[dict]) -> SendResult:
        """Produce the batch to Event Hubs and await broker acks off-loop."""
        import asyncio

        loop = asyncio.get_running_loop()
        t0 = time.perf_counter()

        def _produce_and_flush() -> tuple[int, str]:
            producer = self._ensure_producer()
            acked = {"n": 0, "err": ""}

            def _cb(err, _msg):
                if err is not None:
                    acked["err"] = str(err)
                else:
                    acked["n"] += 1

            for rec in records:
                producer.produce(self._cfg.eventhub_topic,
                                 value=self._to_envelope(rec), callback=_cb)
            # Block (in the executor thread) until every message is acked.
            pending = producer.flush(30)
            if pending:
                acked["err"] = acked["err"] or f"{pending} message(s) not delivered before timeout"
            return acked["n"], acked["err"]

        try:
            count, err = await loop.run_in_executor(None, _produce_and_flush)
        except Exception as exc:  # noqa: BLE001 — surface transport errors to the UI
            return SendResult(ok=False, count=0,
                              latency_ms=(time.perf_counter() - t0) * 1000, error=str(exc))
        latency_ms = (time.perf_counter() - t0) * 1000
        return SendResult(ok=not err, count=count, latency_ms=latency_ms, error=err)

    async def aclose(self) -> None:
        if self._producer is not None:
            self._producer.flush(10)


def sink_for(path: str, cfg: Config) -> TransportSink:
    """Construct the transport sink for an ingestion path."""
    if path == "zerobus":
        return ZerobusSink(cfg)
    if path == "eventhub":
        return KafkaSink(cfg)
    raise ValueError(f"unknown ingest path {path!r} (expected zerobus | eventhub)")
