"""Slot G — live tail of the newest transactions (spec 6.2/6.3).

Reads the newest rows from Lakebase ``rt_latest_transactions``; each row carries
a path-colored chip and its measured E2E latency. Empty state covers both local
dev (Lakebase not configured) and Event Hubs cold start.
"""

from __future__ import annotations

from dash import html

from . import PATH_COLOR, PATH_LABEL

# Per-event E2E trip: generation → bronze (Zerobus) → silver (parse) →
# lakebase (serving). Each hop is one measured delta; e2e is the full trip.
_COLS = ["time", "facility", "type", "summary", "path",
         "→bronze", "→silver", "→lakebase", "e2e"]


def _fmt_time(ts) -> str:
    s = str(ts)
    return s[11:19] if len(s) >= 19 else s


def _ms(v) -> str:
    """Per-hop latency in seconds to the tenth (e.g. 8.0s), matching the rail."""
    return f"{float(v) / 1000:.1f}s" if v is not None else "—"


def _age_opacity(idx: int, total: int) -> float:
    """Fade older rows toward the bottom: newest ~1.0, oldest ~0.35 (linear)."""
    if total <= 1:
        return 1.0
    return round(1.0 - 0.65 * (idx / (total - 1)), 3)


def _row(rec: dict, idx: int, total: int) -> html.Tr:
    path = rec.get("source_path", "zerobus")
    # React key = event_id so a persistent row keeps its DOM node across ticks
    # and does NOT re-fire the mount animation; only genuinely new events flash
    # in. Position-graded opacity makes older rows dim as they slide down.
    return html.Tr([
        html.Td(_fmt_time(rec.get("ts_generated")), className="mono"),
        html.Td(rec.get("facility_id", "")),
        html.Td(rec.get("message_type", ""), className="mono"),
        html.Td(rec.get("summary", "")),
        html.Td(PATH_LABEL.get(path, path), className="chip",
                style={"color": PATH_COLOR.get(path)}),
        html.Td(_ms(rec.get("bronze_ms")), className="mono hop"),
        html.Td(_ms(rec.get("silver_ms")), className="mono hop"),
        html.Td(_ms(rec.get("lakebase_ms")), className="mono hop"),
        html.Td(_ms(rec.get("e2e_ms")), className="mono e2e"),
    ], key=str(rec.get("event_id", idx)), style={"opacity": _age_opacity(idx, total)})


def live_tail(rows: list[dict]) -> html.Div:
    head = html.Tr([html.Th(c) for c in _COLS])
    if rows:
        body = [_row(r, i, len(rows)) for i, r in enumerate(rows)]
    else:
        body = [html.Tr([html.Td("waiting for ingest stream…", colSpan=len(_COLS),
                                  className="empty")])]
    return html.Div([
        html.H2("Live tail — newest 25"),
        html.Table([html.Thead(head), html.Tbody(body, id="tail-body")], className="tail"),
    ], className="panel chart", style={"padding": "16px 18px"})
