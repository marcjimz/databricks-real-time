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
    """Path B: produce the JSON envelope to Azure Event Hubs.

    Transport = the **azure-eventhub SDK over AMQP-on-WebSocket (port 443)**, NOT
    the Kafka protocol (SASL_SSL on 9093). The dashboard runs as a Databricks App,
    whose serverless egress is an HTTPS/443-oriented proxy ("Apps: limited support"
    for network policies) — a raw Kafka TCP connect to :9093 times out from the
    app even though the workspace policy is FULL_ACCESS and Event Hubs is public.
    AMQP-over-WebSocket tunnels the same producer semantics inside a 443 WebSocket,
    so it rides the egress path that already works (the same one that pulls PyPI).

    The class name stays ``KafkaSink`` because it is still the Path-B / Event Hubs
    front door; only the wire transport changed. The CONSUMER
    (``eventhub_to_bronze``) still reads via the Kafka protocol — it runs on a job
    cluster with full VPC egress, so :9093 is fine there. Both sides talk to the
    same Event Hub; producer picks the transport its network allows.

    The producer is built lazily on first send and its blocking send runs off the
    event loop so the async supervisor stays responsive.
    """

    path = "eventhub"

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._producer = None  # lazily built on first send

    def _ensure_producer(self):
        if self._producer is not None:
            return self._producer
        if not self._cfg.eventhub_connection_string:
            raise RuntimeError(
                "Event Hubs not configured: set EVENTHUB_CONNECTION_STRING "
                "(see resources/eventhub secrets)."
            )
        from azure.eventhub import EventHubProducerClient, TransportType

        self._producer = EventHubProducerClient.from_connection_string(
            self._cfg.eventhub_connection_string,
            eventhub_name=self._cfg.eventhub_topic,
            # 443 WebSocket tunnel — the whole point (see class docstring).
            transport_type=TransportType.AmqpOverWebsocket,
        )
        return self._producer

    def _to_envelope(self, rec: dict) -> bytes:
        out = {k: rec.get(k) for k in _ENVELOPE_COLS}
        out["source_path"] = self.path
        return json.dumps(out, separators=(",", ":")).encode("utf-8")

    async def send(self, records: list[dict]) -> SendResult:
        """Produce the batch to Event Hubs (AMQP/WS) and await the send off-loop."""
        import asyncio

        from azure.eventhub import EventData

        loop = asyncio.get_running_loop()
        t0 = time.perf_counter()

        def _send_batch() -> int:
            producer = self._ensure_producer()
            batch = producer.create_batch()
            sent = 0
            for rec in records:
                try:
                    batch.add(EventData(self._to_envelope(rec)))
                    sent += 1
                except ValueError:
                    # Batch full (256 KB EH cap) — flush and start a new one.
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(self._to_envelope(rec)))
                    sent += 1
            producer.send_batch(batch)  # send_batch blocks until the broker acks
            return sent

        try:
            count = await loop.run_in_executor(None, _send_batch)
        except Exception as exc:  # noqa: BLE001 — surface transport errors to the UI
            return SendResult(ok=False, count=0,
                              latency_ms=(time.perf_counter() - t0) * 1000, error=str(exc))
        latency_ms = (time.perf_counter() - t0) * 1000
        return SendResult(ok=True, count=count, latency_ms=latency_ms)

    async def aclose(self) -> None:
        if self._producer is not None:
            try:
                self._producer.close()
            except Exception:
                pass


def sink_for(path: str, cfg: Config) -> TransportSink:
    """Construct the transport sink for an ingestion path."""
    if path == "zerobus":
        return ZerobusSink(cfg)
    if path == "eventhub":
        return KafkaSink(cfg)
    raise ValueError(f"unknown ingest path {path!r} (expected zerobus | eventhub)")
