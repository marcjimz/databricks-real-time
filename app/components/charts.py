"""Slots E & F — throughput and latency trend charts (spec 6.2/6.3 E/F).

Two Plotly figures on the shared 1 Hz tick, ``uirevision`` pinned so live
updates never reset pan/zoom. Throughput (E) overlays the generator's *sent*
rate (dashed grey) on the pipeline's *landed* rate (area + line, path color);
latency (F) plots E2E p95 (solid) and p50 (faded) from the stage metrics. Both
charts render the same vertical annotation markers — every control change drops
one via ``rt_stage_metrics.annotation`` — so bursts, path switches, stream and
cluster changes line up across both series.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dash import dcc, html

import plotly.graph_objects as go

from . import PATH_COLOR, THEME

WINDOW_SECONDS = 60


def _parse_ts(ts):
    """Coerce a Lakebase timestamp (datetime or ISO string) to aware datetime."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def _yrange(*series, pad=1.15, floor=1.0, cap=None) -> list:
    """Explicit y-range [0, max*pad] computed from the data each rebuild.

    Set explicitly (not via autorange) because uirevision preserves the axis
    range across live figure rebuilds and can defeat autorange=True — so the
    axis would never grow/shrink with the values. Computing it here forces the
    rescale every tick. ``floor`` keeps a sane axis when all values are ~0;
    ``cap`` clamps the top (e.g. 10k events/s) so a spike can't blow out the axis.
    """
    vals = [v for s in series for v in s if v is not None]
    top = max(vals) if vals else 0
    hi = max(floor, top * pad)
    if cap is not None:
        hi = min(hi, cap)
    return [0, hi]


def _window_range(*row_groups) -> tuple:
    """Sliding [now-WINDOW, now] x-range that advances every tick — on the
    WALL CLOCK, not the data clock.

    'now' must track real time so the window keeps scrolling even when the
    generator is stopped (time proceeds; the sent/landed series just taper off).
    But the data is stamped on the Lakebase clock (tz-aware GMT) while the app
    host clock may be skewed/naive — so we anchor to wall-clock UTC and correct
    by the offset between the newest data ts and wall-clock. That keeps the axis
    moving live AND aligned to where the data actually plots. With no data yet,
    the offset is zero (plain wall-clock UTC).
    """
    wall = datetime.now(timezone.utc)
    latest = None
    for rows, key in row_groups:
        for r in rows:
            ts = _parse_ts(r.get(key))
            if ts is not None and (latest is None or ts > latest):
                latest = ts
    # Only correct POSITIVE skew (data clock ahead of the host clock) so the
    # newest points aren't clipped off the right edge. A negative delta means the
    # newest data is simply OLD (generator stopped, or serving lag) — we must NOT
    # shift the window back for that, or it would re-freeze on stop. Floor at 0 so
    # a stale/absent data clock just yields pure wall-clock scrolling. Cap the
    # positive correction so a bogus future timestamp can't fling the axis away.
    offset = (latest - wall) if latest is not None else timedelta(0)
    offset = max(timedelta(0), min(offset, timedelta(seconds=WINDOW_SECONDS)))
    now = wall + offset
    return [now - timedelta(seconds=WINDOW_SECONDS), now]


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _line(rows: list[dict], ts_key: str, val_key: str) -> tuple[list, list]:
    """Ascending (xs, ys) of a metric series, dropping null values (pure)."""
    pts = [(r[ts_key], r[val_key]) for r in rows
           if r.get(ts_key) is not None and r.get(val_key) is not None]
    pts.sort(key=lambda p: str(p[0]))
    return [p[0] for p in pts], [p[1] for p in pts]


def _sum_by_second(rows: list[dict], ts_key: str, val_key: str) -> tuple[list, list]:
    """Ascending (xs, ys) summing values that share a timestamp (pure).

    The generator writes one ``rt_gen_metrics`` row per worker per second, so the
    per-second *sent* rate is the sum across workers at each ``ts``.
    """
    # Bucket by WHOLE SECOND, not exact timestamp: each worker's rollup row has a
    # slightly different sub-second ts, and at high rates the generator can emit
    # several seconds' rows in a cluster — bucketing on the raw ts stacked those
    # into false spikes. Truncating to the second sums all workers for that
    # second into one honest per-second rate.
    acc: dict = {}
    for r in rows:
        ts, val = r.get(ts_key), r.get(val_key)
        if ts is None or val is None:
            continue
        dt = _parse_ts(ts)
        bucket = dt.replace(microsecond=0) if dt is not None else ts
        acc[bucket] = acc.get(bucket, 0) + val
    xs = sorted(acc, key=str)
    return xs, [acc[x] for x in xs]


def _annotations(stage_rows: list[dict]) -> list[tuple]:
    """(ts, label) markers — rows whose annotation is non-null (pure)."""
    return [(r["batch_ts"], r["annotation"]) for r in stage_rows
            if r.get("annotation") and r.get("batch_ts") is not None]


def _base_layout(x_range=None) -> dict:
    # Fix the x-axis to the sliding [now-WINDOW, now] range so the time axis
    # advances every tick (a live, scrolling window) instead of auto-ranging to
    # whatever data happens to exist. autorange=False makes the explicit range
    # authoritative even as points enter/leave the window.
    xaxis = dict(showgrid=False, zeroline=False, color=THEME["faint"],
                 showticklabels=True, tickformat="%H:%M:%S", nticks=4)
    if x_range is not None:
        xaxis.update(range=x_range, autorange=False)
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=8, r=8, t=6, b=20),
        height=150,
        showlegend=False,
        font=dict(color=THEME["muted"], size=10,
                  family="ui-monospace, Menlo, monospace"),
        xaxis=xaxis,
        # uirevision pins y-zoom / interaction across live updates, but the
        # x-range is re-asserted each tick above so the window keeps scrolling.
        uirevision="charts",
        # Smooth scrolling comes from the clientside 100 ms x-range nudge (see
        # app.py); keep the server-rebuild transition short so the 1 s data
        # re-anchor doesn't visibly fight the clientside motion.
        transition=dict(duration=200, easing="linear"),
    )


def _with_annotations(fig: go.Figure, marks: list[tuple]) -> go.Figure:
    for ts, label in marks:
        fig.add_vline(x=ts, line=dict(color=THEME["amber"], width=1, dash="dot"))
        fig.add_annotation(x=ts, yref="paper", y=1.0, text=label, showarrow=False,
                           font=dict(color=THEME["amber"], size=9), yanchor="bottom")
    return fig


def _throughput_fig(gen_rows, stage_rows, color, x_range=None) -> go.Figure:
    sx, sy = _sum_by_second(gen_rows, "ts", "sent")
    # 'landed' must be a per-SECOND rate to compare with 'sent'. rows_written is a
    # per-BATCH count over a multi-second batch, so plotting it raw read ~4-8×
    # sent. Divide by the WALL-CLOCK gap between consecutive batches (the real
    # span those rows accumulated over) — NOT batch_ms, which is only the batch's
    # processing time and would inflate the rate.
    ordered = sorted(stage_rows, key=lambda r: str(r.get("batch_ts")))
    lx, ly = [], []
    prev_ts = None
    for r in ordered:
        ts, rows = _parse_ts(r.get("batch_ts")), r.get("rows_written")
        if ts is None or rows is None:
            continue
        if prev_ts is not None:
            gap = (ts - prev_ts).total_seconds()
            if gap > 0:
                lx.append(ts)
                ly.append(rows / gap)
        prev_ts = ts
    fig = go.Figure(layout=_base_layout(x_range))
    fig.add_trace(go.Scatter(x=lx, y=ly, mode="lines", name="landed", fill="tozeroy",
                             line=dict(color=color, width=1.5),
                             fillcolor=_rgba(color, 0.14)))
    fig.add_trace(go.Scatter(x=sx, y=sy, mode="lines", name="sent",
                             line=dict(color=THEME["muted"], width=1, dash="dot")))
    # Explicit y-range from the data each rebuild so the axis actually rescales
    # to the full spectrum (incl. bursts) — uirevision would otherwise freeze it.
    fig.update_layout(yaxis=dict(showgrid=False, zeroline=False,
                                 color=THEME["faint"],
                                 range=_yrange(sy, ly, floor=10, cap=10_000)))
    return _with_annotations(fig, _annotations(stage_rows))


def _latency_fig(stage_rows, color, x_range=None) -> go.Figure:
    ordered = sorted(stage_rows, key=lambda r: str(r.get("batch_ts")))
    x95, y95 = _line(ordered, "batch_ts", "e2e_p95_ms")
    x50, y50 = _line(ordered, "batch_ts", "e2e_p50_ms")
    # Plot in SECONDS, not ms: the e2e trip is seconds-scale, so "11k" (ms) read
    # confusingly — "11" on a seconds axis (with an "s" suffix) is what a human
    # expects. Divide the series here.
    y95 = [v / 1000 for v in y95]
    y50 = [v / 1000 for v in y50]
    fig = go.Figure(layout=_base_layout(x_range))
    fig.add_trace(go.Scatter(x=x95, y=y95, mode="lines", name="p95",
                             line=dict(color=color, width=1.5)))
    fig.add_trace(go.Scatter(x=x50, y=y50, mode="lines", name="p50",
                             line=dict(color=color, width=1), opacity=0.45))
    # Linear seconds axis with an explicit data-driven range (see _yrange) so it
    # visibly rescales as latency moves; "s" suffix on ticks.
    fig.update_layout(yaxis=dict(showgrid=False, zeroline=False, color=THEME["faint"],
                                 range=_yrange(y95, y50, floor=0.5), ticksuffix="s"))
    return _with_annotations(fig, _annotations(ordered))


def _panel(title: str, window: str, graph_id: str, fig: go.Figure,
           legend: html.Div) -> html.Div:
    return html.Div([
        html.H2([f"{title} ", html.Span(window, style={"color": THEME["faint"]})]),
        dcc.Graph(id=graph_id, figure=fig, config={"displayModeBar": False},
                  style={"height": "150px"}),
        legend,
    ], className="panel chart", style={"padding": "16px 18px"})


def _legend(items: list[tuple]) -> html.Div:
    spans = []
    for color, label in items:
        spans.append(html.Span([
            html.I(style={"background": color, "display": "inline-block",
                          "width": "10px", "height": "10px", "borderRadius": "2px",
                          "marginRight": "5px"}),
            label,
        ], style={"marginRight": "14px"}))
    return html.Div(spans, className="legend",
                    style={"fontSize": "11px", "color": THEME["muted"], "marginTop": "6px"})


def charts(gen_rows: list[dict], stage_rows: list[dict], path: str) -> html.Div:
    color = PATH_COLOR[path]
    stage = [r for r in stage_rows if r.get("pipeline") != "annotation"] or stage_rows
    # One shared sliding window across both charts so their time axes stay locked
    # together and advance every tick, anchored to the newest event seen.
    x_range = _window_range((gen_rows, "ts"), (stage_rows, "batch_ts"))
    thr = _panel(
        "Throughput — events/s", f"{WINDOW_SECONDS} s window", "chart-throughput",
        _throughput_fig(gen_rows, stage_rows, color, x_range),
        _legend([(THEME["muted"], "sent"), (color, "landed"), (THEME["amber"], "annotation")]),
    )
    lat = _panel(
        "Latency — e2e percentiles", f"{WINDOW_SECONDS} s window", "chart-latency",
        _latency_fig(stage, color, x_range),
        # Two distinct callouts (p95 solid, p50 faded) — a single "p95 / p50"
        # entry read as a stray "/" with no p50 swatch.
        _legend([(color, "p95"), (_rgba(color, 0.45), "p50")]),
    )
    return html.Div([thr, lat], className="grid2")
