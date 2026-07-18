"""Slot A — the single header control row (spec section 6.2/6.3).

Path-invariant: one set of traffic controls drives whichever path is active.
Controls never move, duplicate, or reset on switch — only the active-path color
and the profile label change. The picker is disabled during the ≤10 s drain.
"""

from __future__ import annotations

from dash import dcc, html

from . import PATH_LABEL, THEME
from generator.profiles import profile_for


def _seg_button(path: str, active: str, disabled: bool):
    pressed = path == active
    return html.Button(
        PATH_LABEL[path],
        id={"type": "path-pick", "path": path},
        n_clicks=0,
        disabled=disabled,
        className="seg-btn" + (" on" if pressed else ""),
        **{"aria-pressed": "true" if pressed else "false"},
    )


def header(state) -> html.Div:
    running = state.running
    switching = state.switching
    profile = profile_for(state.path).name

    run_toggle = html.Button(
        [html.Span(className="dot"), html.Span("Running" if running else "Stopped")],
        id="run-toggle", n_clicks=0,
        className="run" + ("" if running else " stopped"),
    )

    picker = html.Div(
        [_seg_button("zerobus", state.path, switching),
         _seg_button("eventhub", state.path, switching)],
        className="seg",
    )

    profile_chip = html.Span(
        f"profile: {profile}" + ("  ·  switching…" if switching else ""),
        className="chip", id="profile-chip",
    )

    rate = html.Div([
        html.Label("Rate"),
        # updatemode="mouseup": fire the callback once on release, not on every
        # drag increment — otherwise dragging across the range fires dozens of
        # callbacks (each doing a Lakebase annotation write), which floods the
        # server and makes the slider feel unresponsive/laggy.
        dcc.Slider(id="rate-slider", min=1, max=1000, step=1, value=state.rate_per_worker or 50,
                   marks=None, tooltip={"placement": "bottom"}, updatemode="mouseup"),
        html.Output(f"{state.rate_per_worker or 50}/s", id="rate-out"),
    ], className="ctl")

    streams = html.Div([
        html.Label("Streams"),
        html.Div([
            html.Button("−", id="streams-dec", n_clicks=0, className="step"),
            html.Output(str(state.workers or 2), id="streams-out"),
            html.Button("+", id="streams-inc", n_clicks=0, className="step"),
        ], className="stepper"),
    ], className="ctl")

    burst = html.Button("Burst 10×", id="burst-btn", n_clicks=0,
                        className="burst" + (" on" if state.bursting else ""))

    reset = html.Button("Reset", id="reset-btn", n_clicks=0, className="burst reset")

    malformed = html.Div([
        html.Label("Malformed"),
        dcc.Slider(id="malformed-slider", min=0, max=20, step=1, value=int(state.malformed_pct),
                   marks=None, tooltip={"placement": "bottom"}, updatemode="mouseup"),
        html.Output(f"{int(state.malformed_pct)}%", id="malformed-out"),
    ], className="ctl")

    synthetic = html.Span("⬤ SYNTHETIC DATA", className="syn")

    return html.Div([
        html.Div([run_toggle, picker, profile_chip], className="hdr-row"),
        html.Div([rate, streams, burst, reset, malformed, synthetic], className="hdr-row"),
    ], className="panel header", style={"padding": "16px 18px"})
