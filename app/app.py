"""HL7 Real-Time Intelligence dashboard — Dash app (spec section 6).

The full frame on one 1 Hz tick: the path-invariant header (slot A), the
active-path card (slot B) + frozen previous-run card (slot C), the stage rail
(slot D), the throughput/latency charts (slots E/F), and the live tail (slot G).
Every control change drops a chart annotation. The generator runs in-process
(asyncio supervisor in a daemon thread); Lakebase backs the serving reads and
degrades to empty states locally.
"""

from __future__ import annotations

import logging
import sys

import dash
from dash import ALL, Input, Output, ctx, dcc, html

from components import PATH_LABEL, THEME
from components.charts import charts
from components.header import header
from components.path_cards import active_card, previous_card
from components.stage_rail import stage_rail
from components.tail import live_tail
from config import CONFIG
from data.lakebase import LakebaseClient
from generator.supervisor import Supervisor

# Databricks Apps captures process stdout/stderr into /logz. Configure a
# stream handler on our namespace so generator send failures (which are
# otherwise swallowed inside the async sender) are visible there.
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("hl7").setLevel(logging.INFO)

# --- singletons (one generator + one serving-layer client per app process) ---
LAKEBASE = LakebaseClient(CONFIG)
SUPERVISOR = Supervisor(CONFIG, on_rollup=LAKEBASE.write_gen_metrics)
SUPERVISOR.start_in_thread()

# update_title=None keeps the browser tab title fixed (Dash otherwise flips it
# to "Updating..." on every 1 Hz callback, which flickers constantly here).
app = dash.Dash(__name__, title="HL7 RTI — swap the front door", update_title=None)
server = app.server  # for gunicorn / Databricks Apps

_CSS = """
* { box-sizing: border-box; }
body { background:%(ink)s; color:%(text)s; font-family:%(sans)s; margin:0; padding:20px; }
.panel { background:%(panel)s; border:1px solid %(line)s; border-radius:10px; }
.mono { font-family:%(mono)s; }
.header .hdr-row { display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
.header .hdr-row + .hdr-row { margin-top:12px; }
.run { display:flex; align-items:center; gap:8px; background:none; border:1px solid %(line)s;
       border-radius:8px; padding:7px 14px; cursor:pointer; color:%(text)s; }
.dot { width:8px; height:8px; border-radius:50%%; background:%(pb)s; }
.run.stopped .dot { background:%(faint)s; }
.seg { display:flex; border:1px solid %(line)s; border-radius:8px; overflow:hidden; }
.seg-btn { background:none; border:0; padding:7px 16px; cursor:pointer; color:%(muted)s; }
.seg-btn.on { color:%(text)s; background:%(panel_2)s; box-shadow:inset 0 -2px 0 %(pa)s; }
.seg-btn:disabled { opacity:.5; cursor:default; }
.chip { font-family:%(mono)s; font-size:12px; color:%(muted)s; border:1px solid %(line)s;
        border-radius:999px; padding:3px 10px; }
.ctl { display:flex; align-items:center; gap:8px; font-size:13px; color:%(muted)s; }
.ctl .rc-slider { width:130px; }
.ctl output { font-family:%(mono)s; color:%(text)s; min-width:52px; }
.stepper { display:flex; align-items:center; gap:8px; }
.step { width:26px; height:26px; border:1px solid %(line)s; background:none; border-radius:6px;
        cursor:pointer; color:%(muted)s; }
.burst { background:none; border:1px solid %(line)s; border-radius:8px; padding:7px 14px;
         cursor:pointer; color:%(muted)s; transition:border-color .2s, color .2s; }
.burst.on { border-color:%(amber)s; color:%(amber)s; }
.burst.reset { border-color:%(line)s; color:%(faint)s; }
.burst.reset:hover { border-color:%(red)s; color:%(red)s; }
.syn { margin-left:auto; font-size:11px; letter-spacing:.08em; color:%(amber)s;
       border:1px solid %(amber)s; border-radius:999px; padding:3px 10px; }
.grid2 { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-top:16px; }
.card-head { display:flex; align-items:center; gap:8px; font-size:14px; }
.swatch { width:10px; height:10px; border-radius:3px; display:inline-block; }
.card .sub { font-size:12px; color:%(muted)s; margin:3px 0 14px; }
.hero { display:flex; align-items:baseline; gap:8px; margin-top:6px; }
.hero strong { font-size:38px; font-weight:500; letter-spacing:-.02em; }
.hero .unit { font-size:20px; color:%(muted)s; }
.hero em { font-style:normal; font-size:12px; color:%(muted)s; margin-left:8px; }
.meta { display:flex; gap:18px; margin-top:14px; font-size:12px; color:%(muted)s; flex-wrap:wrap; }
.meta b { font-family:%(mono)s; font-weight:400; color:%(text)s; }
.prev .empty { font-size:12px; color:%(faint)s; margin-top:8px; }
.warn { color:%(red)s; } .caution { color:%(amber)s; }
.chart h2 { font-size:12px; font-weight:500; color:%(muted)s; letter-spacing:.04em; margin-bottom:10px; }
.rail { display:flex; align-items:center; padding:14px 18px; flex-wrap:wrap; row-gap:10px; margin-top:16px; }
.rail .hop { display:flex; flex-direction:column; gap:3px; padding:0 4px; }
.rail .hop .k { font-size:11px; color:%(faint)s; }
.rail .hop .v { font-family:%(mono)s; font-size:13px; background:%(panel_2)s; border:1px solid %(line)s;
                border-radius:6px; padding:4px 10px; min-width:110px; text-align:center; }
.rail .tick { flex:0 0 24px; height:1px; background:%(line)s; position:relative; top:9px; }
.rail .tick::after { content:""; position:absolute; right:0; top:-2.5px; border:3px solid transparent;
                     border-left-color:%(faint)s; }
.rail .end { margin-left:auto; display:flex; gap:16px; font-size:12px; color:%(muted)s; align-items:center; flex-wrap:wrap; }
.rail .end b { font-family:%(mono)s; font-weight:400; color:%(text)s; }
.legend i { vertical-align:middle; }
table.tail { width:100%%; border-collapse:collapse; font-size:13px; }
table.tail th { text-align:left; color:%(faint)s; font-weight:500; font-size:11px;
                padding:4px 8px; border-bottom:1px solid %(line)s; }
table.tail td { padding:6px 8px; border-bottom:1px solid %(panel_2)s; }
table.tail td.empty { color:%(faint)s; text-align:center; padding:24px; }
/* Live tail motion: new rows (freshly keyed by event_id) mount with a
   slide-down + highlight flash; persistent rows keep their DOM node (React
   key) so they do NOT re-animate each tick. Graded per-row opacity is applied
   inline in tail.py so older rows fade out as they slide toward the bottom. */
@keyframes tail-in {
  0%%   { opacity:0; transform:translateY(-8px); background:%(amber_2)s; }
  60%%  { background:%(amber_2)s; }
  100%% { opacity:1; transform:translateY(0); background:transparent; }
}
table.tail tbody tr { animation:tail-in 0.55s ease-out; transition:opacity 0.4s linear; }
table.tail tbody tr td { transition:background 0.6s ease; }
""" % THEME

app.index_string = """<!DOCTYPE html><html><head>{%%metas%%}<title>{%%title%%}</title>
{%%favicon%%}{%%css%%}<style>%s</style></head>
<body>{%%app_entry%%}<footer>{%%config%%}{%%scripts%%}{%%renderer%%}</footer></body></html>""" % _CSS


def _layout() -> html.Div:
    st = SUPERVISOR.state
    return html.Div([
        # Single 1 Hz refresh tick. (A prior 100 ms clientside Plotly.relayout
        # "smooth scroll" wedged the Dash renderer — it thrashed against the 1 s
        # figure rebuild and froze all updates — so it was removed.)
        dcc.Interval(id="tick", interval=1000, n_intervals=0),
        html.Div(header(st), id="header-slot"),
        html.Div([
            html.Div(active_card(st), id="active-slot"),
            html.Div(previous_card(LAKEBASE.path_summary(_other(st.path)), st.path),
                     id="previous-slot"),
        ], className="grid2"),
        html.Div(_rail(st), id="rail-slot"),
        html.Div(_charts(st), id="charts-slot", style={"marginTop": "16px"}),
        html.Div(live_tail(LAKEBASE.latest_transactions(25)), id="tail-slot",
                 style={"marginTop": "16px"}),
    ])


def _rail(st):
    return stage_rail(st, LAKEBASE.stage_snapshot(st.path),
                      LAKEBASE.freshness_seconds(st.path))


def _charts(st):
    return charts(LAKEBASE.recent_gen_metrics(st.path, 60),
                  LAKEBASE.recent_stage_metrics(st.path, 60), st.path)


def _other(path: str) -> str:
    return "eventhub" if path == "zerobus" else "zerobus"


def _annotate(label: str) -> None:
    """Drop a chart marker on the active path for any control change (spec 6.1)."""
    LAKEBASE.write_annotation(SUPERVISOR.state.path, label)


app.layout = _layout


# --- control callbacks (write to the supervisor) ----------------------------
@app.callback(Output("run-toggle", "className"), Output("run-toggle", "children"),
              Input("run-toggle", "n_clicks"), prevent_initial_call=True)
def _toggle_run(_n):
    if SUPERVISOR.controls.running:
        SUPERVISOR.stop()
        _annotate("stop")
    else:
        SUPERVISOR.start()
        _annotate("start")
    running = SUPERVISOR.controls.running
    label = [html.Span(className="dot"), html.Span("Running" if running else "Stopped")]
    return ("run" if running else "run stopped"), label


@app.callback(Output("rate-out", "children"), Input("rate-slider", "value"),
              prevent_initial_call=True)
def _set_rate(v):
    SUPERVISOR.set_rate(v or 1)
    _annotate(f"rate {int(v or 1)}/s")
    return f"{int(v or 1)}/s"


@app.callback(Output("malformed-out", "children"), Input("malformed-slider", "value"),
              prevent_initial_call=True)
def _set_malformed(v):
    SUPERVISOR.set_malformed(v or 0)
    return f"{int(v or 0)}%"


@app.callback(Output("streams-out", "children"),
              Input("streams-inc", "n_clicks"), Input("streams-dec", "n_clicks"),
              prevent_initial_call=True)
def _set_streams(_inc, _dec):
    delta = 1 if ctx.triggered_id == "streams-inc" else -1
    SUPERVISOR.set_workers(SUPERVISOR.controls.workers + delta)
    _annotate(f"streams {SUPERVISOR.controls.workers}")
    return str(SUPERVISOR.controls.workers)


@app.callback(Output("burst-btn", "className"), Input("burst-btn", "n_clicks"),
              prevent_initial_call=True)
def _burst(_n):
    # Momentary action: fire the 15 s surge and leave the button un-highlighted.
    # The surge itself is visible in the throughput chart (10× effective rate),
    # driven by burst_until in the supervisor — not by the button's class.
    SUPERVISOR.burst()
    _annotate("burst 10×")
    return "burst"


@app.callback(
    Output("reset-btn", "className"),
    Output("active-slot", "children", allow_duplicate=True),
    Output("previous-slot", "children", allow_duplicate=True),
    Output("rail-slot", "children", allow_duplicate=True),
    Output("charts-slot", "children", allow_duplicate=True),
    Output("tail-slot", "children", allow_duplicate=True),
    Input("reset-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _reset(_n):
    # Zero the in-memory generator counters + truncate the Lakebase serving
    # tables, THEN re-render every slot immediately from the now-empty state so
    # the tail, hero, rail and charts clear on the click (not a tick later).
    SUPERVISOR.reset_metrics()
    LAKEBASE.reset_serving()
    st = SUPERVISOR.state
    return (
        "burst reset",
        active_card(st, e2e_p95_ms=None),
        previous_card(None, st.path),
        _rail(st),
        _charts(st),
        live_tail([]),
    )


@app.callback(
    Output({"type": "path-pick", "path": ALL}, "className"),
    Output("profile-chip", "children"),
    Input({"type": "path-pick", "path": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _switch_path(_clicks):
    picked = ctx.triggered_id
    if picked and isinstance(picked, dict):
        SUPERVISOR.switch_path(picked["path"])
        _annotate(f"→ {PATH_LABEL.get(picked['path'], picked['path'])}")
    order = [o["id"]["path"] for o in ctx.outputs_list[0]]
    active = SUPERVISOR.state.path
    classes = ["seg-btn on" if p == active else "seg-btn" for p in order]
    from generator.profiles import profile_for
    return classes, f"profile: {profile_for(active).name}"


# --- single 1 Hz refresh (ONE request/tick) --------------------------------
# One consolidated callback doing its Lakebase reads in sequence. Splitting this
# into 5 per-slot callbacks fired 5 simultaneous requests/sec that all contended
# on the single pooled read connection + single dev-server thread — they piled
# up (all "pending") and the whole UI backpressured/froze. One request/sec keeps
# the reads serialized and the page responsive.
@app.callback(
    Output("active-slot", "children"),
    Output("previous-slot", "children"),
    Output("rail-slot", "children"),
    Output("charts-slot", "children"),
    Output("tail-slot", "children"),
    Input("tick", "n_intervals"),
)
def _refresh(_n):
    st = SUPERVISOR.state
    return (
        active_card(st, e2e_p95_ms=LAKEBASE.serving_e2e_p95_ms()),
        previous_card(LAKEBASE.path_summary(_other(st.path)), st.path),
        _rail(st),
        _charts(st),
        live_tail(LAKEBASE.latest_transactions(25)),
    )


if __name__ == "__main__":
    import os

    # threaded=True so the local dev server handles the 1 Hz refresh callbacks
    # concurrently with the generator thread's work — otherwise the single
    # request worker starves and the UI freezes (production uses gunicorn
    # --threads 8, so this only matters for local `python app.py` runs).
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8050")),
            debug=False, threaded=True)
