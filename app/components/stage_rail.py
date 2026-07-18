"""Slot D — the pipeline stage rail (spec 6.2/6.3 D).

One horizontal rail of per-hop latency badges (gen → ack → [broker] → bronze →
silver → lakebase) plus a right-aligned block of batch / lag / workers /
quarantine / freshness figures. The broker hop only appears on Path B (Event
Hubs). Hops come from the newest ``bronze_to_silver`` metric row (per-hop
medians measured in foreachBatch); ack/delivery and worker count come from the
in-process generator state; freshness is the age of the newest landed row.

Threshold coloring per spec: ``lag_s`` amber >5 / red >30, quarantine red >0,
freshness amber >5 s / red >15 s. Everything else is green-neutral.
"""

from __future__ import annotations

from dash import html

from . import PATH_COLOR


def _fmt_ms(v) -> str:
    """Adaptive hop label: sub-second hops in ms (e.g. 5 ms — so a fast bronze
    write doesn't collapse to a meaningless "0.0s"), ≥1 s in seconds to the
    tenth (e.g. 7.5s). Keeps every hop legible at its natural magnitude."""
    if v is None:
        return "—"
    v = float(v)
    return f"{int(round(v))} ms" if v < 1000 else f"{v / 1000:.1f}s"


def _lag_class(lag) -> str:
    if lag is None:
        return ""
    return "warn" if lag > 30 else ("caution" if lag > 5 else "")


def _fresh_class(age) -> str:
    if age is None:
        return ""
    return "warn" if age > 15 else ("caution" if age > 5 else "")


def _hop(label: str, value: str, hop_id: str | None = None,
         extra_style: dict | None = None) -> html.Div:
    kwargs = {"id": hop_id} if hop_id else {}
    return html.Div(
        [html.Span(label, className="k"), html.Span(value, className="v")],
        className="hop", style=extra_style or {}, **kwargs,
    )


def _tick(tick_id: str | None = None, hidden: bool = False) -> html.Span:
    style = {"display": "none"} if hidden else {}
    kwargs = {"id": tick_id} if tick_id else {}
    return html.Span(className="tick", style=style, **kwargs)


def stage_rail(state, snapshot: dict | None, freshness: float | None) -> html.Div:
    snap = snapshot or {}
    path = state.path
    is_b = path == "eventhub"
    ack_label = "delivery" if is_b else "ack"

    lag = snap.get("lag_s")
    lag = float(lag) if lag is not None else None
    quar = int(snap.get("quarantined") or 0)

    hops = [
        _hop("gen", f"{ack_label} {_fmt_ms(state.ack_p95_ms)}"),
        _tick("brokertick", hidden=not is_b),
        _hop("event hubs", f"broker {_fmt_ms(snap.get('broker_ms'))}",
             hop_id="brokerhop", extra_style={} if is_b else {"display": "none"}),
        _tick(),
        _hop("delta", f"bronze {_fmt_ms(snap.get('bronze_ms'))}"),
        _tick(),
        _hop("parse + validate", f"silver {_fmt_ms(snap.get('silver_ms'))}"),
        _tick(),
        _hop("jdbc", f"lakebase {_fmt_ms(snap.get('lakebase_ms'))}"),
    ]

    end = html.Div([
        html.Span(["batch ", html.B(_fmt_ms(snap.get("batch_ms")))]),
        html.Span(["lag ", html.B(f"{lag:.1f} s" if lag is not None else "—",
                                  className=_lag_class(lag))]),
        # "streams" = active generator send-loops (the Streams control), NOT
        # Spark cluster workers — renamed to avoid implying backend compute.
        html.Span(["streams ", html.B(str(state.workers))]),
        html.Span(["quarantine ",
                   html.B(str(quar), className="warn" if quar > 0 else "")]),
        html.Span(["freshness ",
                   html.B(f"{freshness:.1f} s" if freshness is not None else "—",
                          className=_fresh_class(freshness))]),
    ], className="end")

    return html.Div(
        hops + [end], id="stage-rail", className="panel rail",
        style={"borderColor": PATH_COLOR[path], "marginTop": "16px"},
    )
