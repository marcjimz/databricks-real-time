"""Slots B & C — active-path card and frozen previous-run card (spec 6.2).

The active path always lives in slot B and recolors per selection; the other
path's last-run summary always lives in slot C. Switching recolors and swaps
data in place — it never rearranges the layout.
"""

from __future__ import annotations

from dash import html

from . import PATH_COLOR, PATH_LABEL, THEME

_SUBTITLE = {
    "zerobus": "direct write · no broker",
    "eventhub": "Event Hubs · Kafka endpoint",
}


def active_card(state, e2e_p95_ms=None) -> html.Div:
    color = PATH_COLOR[state.path]
    label = PATH_LABEL[state.path]

    if state.throttled:
        status_border, status_note = THEME["red"], "THROTTLED"
    elif state.switching:
        status_border, status_note = THEME["amber"], "switching…"
    else:
        status_border, status_note = color, None

    # Hero = the full serving-layer trip (Zerobus → Lakebase) p95 — the headline
    # the demo is about. Full ts_lakebase − ts_generated (matches the tail's e2e),
    # not the sub-ms in-memory Zerobus ack.
    hero = html.Div([
        html.Strong(f"{e2e_p95_ms / 1000:.1f}" if e2e_p95_ms else "—", className="mono"),
        html.Span(" s", className="unit"),
        html.Em("p95 · ingestion to serving"),
    ], className="hero")

    meta = html.Div([
        html.Span([f"{state.sent_last_s}/s ", html.B("sent")]),
        html.Span(["ack p99 ", html.B(f"{state.ack_p99_ms} ms")]),
        html.Span(["total ", html.B(f"{state.sent_total:,}")]),
        html.Span(["unacked ", html.B(f"{state.unacked:,}")]),
    ], className="meta")

    header = html.Div([
        html.Span(className="swatch", style={"background": color}),
        html.Span(f"● {label} → Serving"),
        html.Span(status_note, className="warn" if state.throttled else "caution",
                  style={"marginLeft": "auto"}) if status_note else None,
    ], className="card-head")

    return html.Div(
        [header, html.Div(_SUBTITLE[state.path], className="sub"), hero, meta],
        id="active-card", className="panel card",
        style={"padding": "18px", "borderColor": status_border},
    )


def previous_card(summary: dict | None, active_path: str) -> html.Div:
    other = "eventhub" if active_path == "zerobus" else "zerobus"
    label = PATH_LABEL[other]
    color = PATH_COLOR[other]

    if not summary:
        body = html.Div("not yet run — switch paths to compare", className="empty")
    else:
        ended = str(summary.get("last_run_ended", ""))[11:16]
        body = html.Div([
            html.Div(f"● {label} · ended {ended}"),
            html.Div(
                f"{summary.get('e2e_p95_ms', 0) / 1000:.1f} s p95 · "
                f"peak {summary.get('peak_rate', 0):,}/s · "
                f"quar {summary.get('quarantined', 0)}",
                className="mono sub",
            ),
        ])

    return html.Div(
        [html.Div([html.Span(className="swatch", style={"background": color}),
                   html.Span("Previous run")], className="card-head"), body],
        id="previous-card", className="panel card prev",
        style={"padding": "18px", "opacity": 0.6},
    )
