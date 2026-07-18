"""Dash UI components for the HL7 RTI dashboard (spec section 6)."""

# Shared palette, lifted verbatim from docs/ui-mockup.html so the built app is
# visually indistinguishable from the acceptance mockup.
THEME = {
    "ink": "#0E141B", "panel": "#161E27", "panel_2": "#1C2632", "line": "#243040",
    "text": "#E6ECF2", "muted": "#8CA0B3", "faint": "#5A6B7C",
    "pa": "#8B7FE8", "pa_dim": "#3A3663",      # Path A — Zerobus (purple)
    "pb": "#2FB48C", "pb_dim": "#1E4A3E",      # Path B — Event Hubs (teal)
    "red": "#E5484D", "amber": "#E8A33D", "amber_2": "rgba(232,163,61,0.16)",
    "mono": "ui-monospace, SFMono-Regular, Menlo, monospace",
    "sans": "Inter, -apple-system, system-ui, sans-serif",
}

PATH_COLOR = {"zerobus": THEME["pa"], "eventhub": THEME["pb"]}
PATH_DIM = {"zerobus": THEME["pa_dim"], "eventhub": THEME["pb_dim"]}
PATH_LABEL = {"zerobus": "Zerobus", "eventhub": "Event Hubs"}
