"""
Page Routes Explorer — consultation paginée côté serveur + détail de trip.

Architecture :
  - dcc.Store "store-routes"  : données de la page courante
  - dcc.Store "store-trip"    : détail d'un trip sélectionné
  - Filtres → Store → Table (pattern découplé, zéro cascade directe)
  - Export CSV via background callback (opération > 1 s)
  - Debounce sur les inputs texte (450 ms, configuré dans filters.py)
"""

import dash
import pandas as pd
from dash import (
    Input, Output, State, callback, clientside_callback,
    dcc, html, no_update, ctx,
)

from dashboard.components.error import empty_state, error_banner, section_loader
from dashboard.components.filters import (
    country_dropdown, filter_row, mode_dropdown,
    page_size_selector, sort_controls,
)
from dashboard.components.tables import obrail_table
from dashboard.services.api_client import client, safe_data, safe_items, error_msg
from dashboard.utils.cache import TTL_SHORT, TTL_MEDIUM
from dashboard.utils.theme import COLORS

dash.register_page(__name__, path="/routes", name="Routes", order=1)


# Ces options de tri suivent les usages les plus fréquents côté exploration métier.
SORT_OPTIONS = [
    {"label": "Distance",        "value": "distance_km"},
    {"label": "CO₂ total",       "value": "emissions_co2"},
    {"label": "CO₂/pkm",         "value": "co2_per_pkm"},
    {"label": "Pays départ",     "value": "departure_country"},
    {"label": "Pays arrivée",    "value": "arrival_country"},
]

_DAYS = ["L", "M", "M", "J", "V", "S", "D"]

def _fmt_days(days_of_week: str | None) -> str:
    """Transforme '1111100' en texte markdown 'L M M J V · ·' (gras si actif)."""
    if not days_of_week:
        return "—"
    d = str(days_of_week).strip().ljust(7, "0")
    return " ".join(f"**{_DAYS[i]}**" if d[i] == "1" else "·" for i in range(7))

def _fmt_time(t: str | None) -> str:
    """Tronque 'HH:MM:SS' → 'HH:MM'."""
    if not t:
        return "—"
    return str(t)[:5]

_COLS = [
    {"name": "Mode",            "id": "mode"},
    {"name": "Départ",          "id": "departure_city"},
    {"name": "Pays dep.",       "id": "departure_country"},
    {"name": "Arrivée",         "id": "arrival_city"},
    {"name": "Pays arr.",       "id": "arrival_country"},
    {"name": "Départ",       "id": "departure_time"},
    {"name": "Arrivée",       "id": "arrival_time"},
    {"name": "Jours",           "id": "days_of_week",  "presentation": "markdown"},
    {"name": "Distance (km)",   "id": "distance_km"},
    {"name": "CO₂ (g)",         "id": "emissions_co2"},
    {"name": "CO₂/pkm",         "id": "co2_per_pkm"},
    {"name": "Nuit",            "id": "is_night_train"},
    {"name": "Opérateur",       "id": "agency_name"},
]

layout = html.Div(
    [
        dcc.Store(id="store-routes", storage_type="memory"),
        dcc.Store(id="store-trip",   storage_type="memory"),
        dcc.Download(id="download-routes-csv"),

        html.Div(
            [
                html.H1("Routes Explorer", className="page-title"),
                html.P("Consultation des trajets ferroviaires et aériens.", className="page-subtitle"),
            ],
            className="page-header",
        ),

        # Les filtres sont découpés par thème pour limiter la charge cognitive.
        filter_row(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("Mode"),
                                mode_dropdown("routes-mode"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px"},
                        ),
                        html.Div(
                            [
                                html.Label("Nuit"),
                                dcc.Dropdown(
                                    id="routes-is-night",
                                    options=[{"label": "Nuit seulement", "value": "true"},
                                             {"label": "Jour seulement",  "value": "false"}],
                                    placeholder="Tous",
                                    clearable=True,
                                    className="obrail-dropdown",
                                    style={"minWidth": "140px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px"},
                        ),
                    ],
                    style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "width": "100%"},
                ),
            ]
        ),

        filter_row(
            [
                html.Div(
                    [
                        html.Label("Pays départ"),
                        country_dropdown("routes-dep-country"),
                        html.Label("Ville départ"),
                        dcc.Input(
                            id="routes-dep-city",
                            type="text",
                            placeholder="Paris, Wien…",
                            debounce=True,
                            className="obrail-input",
                            style={"width": "100%", "minWidth": "0"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "10px",
                        "width": "100%",
                        "flexWrap": "nowrap",
                    },
                ),
            ]
        ),

        filter_row(
            [
                html.Div(
                    [
                        html.Label("Pays arrivée"),
                        country_dropdown("routes-arr-country"),
                        html.Label("Ville arrivée"),
                        dcc.Input(
                            id="routes-arr-city",
                            type="text",
                            placeholder="Berlin, Roma…",
                            debounce=True,
                            className="obrail-input",
                            style={"width": "100%", "minWidth": "0"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "10px",
                        "width": "100%",
                        "flexWrap": "nowrap",
                    },
                ),
            ]
        ),

        filter_row(
            [
                html.Div(
                    [
                        html.Div(),
                        html.Button("Rechercher", id="btn-search-routes", className="btn-primary", n_clicks=0),
                        html.Button(
                            "⬇  Exporter CSV",
                            id="btn-export-routes",
                            className="btn-secondary",
                            style={"justifySelf": "end"},
                        ),
                    ],
                    style={
                        "width": "100%",
                        "display": "grid",
                        "gridTemplateColumns": "1fr auto 1fr",
                        "alignItems": "center",
                        "columnGap": "12px",
                    },
                ),
            ]
        ),

        filter_row(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("Trier"),
                                sort_controls("routes-sort-by", "routes-sort-order", SORT_OPTIONS, "distance_km"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px"},
                        ),
                        html.Div(
                            [
                                html.Label("Par page"),
                                page_size_selector("routes-page-size"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px"},
                        ),
                    ],
                    style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "width": "100%"},
                ),
            ]
        ),

        # La table reste l'élément central; tout le reste sert à la piloter.
        html.Div(id="routes-table-status"),
        html.Div(id="routes-table-title", className="section-title", style={"margin": "12px 0 6px"}),
        section_loader(
            obrail_table(
                table_id="routes-table",
                columns=_COLS,
                data=[],
                page_size=25,
                server_side=True,
                total_count=0,
            )
        ),

        # Le détail est séparé pour ne pas alourdir la table principale.
        html.Hr(className="section-separator"),
        html.Div(id="trip-detail-container"),
    ],
    className="page-container",
)


@callback(
    Output("store-routes", "data"),
    Input("btn-search-routes",   "n_clicks"),
    Input("routes-table",        "page_current"),
    Input("url",                 "pathname"),
    State("routes-mode",         "value"),
    State("routes-dep-country",  "value"),
    State("routes-arr-country",  "value"),
    State("routes-dep-city",     "value"),
    State("routes-arr-city",     "value"),
    State("routes-is-night",     "value"),
    State("routes-sort-by",      "value"),
    State("routes-sort-order",   "value"),
    State("routes-page-size",    "value"),
    prevent_initial_call=False,
)
def load_routes(n_clicks, page_current, pathname,
                mode, dep_country, arr_country, dep_city, arr_city,
                is_night, sort_by, sort_order, page_size):
    if pathname and pathname != "/routes":
        return no_update

    # Repartir en page 1 évite les états incohérents après changement de filtres.
    page = 1 if ctx.triggered_id == "btn-search-routes" else (page_current or 0) + 1

    params = {
        "mode":               mode,
        "departure_country":  dep_country,
        "arrival_country":    arr_country,
        "departure_city":     dep_city,
        "arrival_city":       arr_city,
        "is_night_train":     is_night,
        "sort_by":            sort_by or "distance_km",
        "sort_order":         sort_order or "desc",
        "page":               page,
        "page_size":          page_size or 25,
    }
    return client.get("/routes", params=params, ttl=TTL_SHORT)


@callback(
    Output("routes-table",       "data"),
    Output("routes-table",       "page_count"),
    Output("routes-table",       "style_data_conditional"),
    Output("routes-table-status", "children"),
    Output("routes-table-title",  "children"),
    Input("store-routes", "data"),
)
def render_routes_table(store):
    empty_cond = []
    no_rows: list = []

    if not store:
        return no_rows, 1, empty_cond, html.Div(), ""
    if not store.get("ok"):
        return no_rows, 1, empty_cond, error_banner(error_msg(store), "/routes"), ""

    data_obj  = safe_data(store)
    items     = data_obj.get("data") or data_obj.get("items") or []
    total     = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)

    if not items:
        return no_rows, 1, empty_cond, empty_state("Aucune route ne correspond aux filtres."), ""

    rows = []
    for r in items:
        rows.append({
            "mode":               r.get("mode", "—"),
            "departure_city":     r.get("departure_city") or r.get("departure_station") or "—",
            "departure_country":  (r.get("departure_country") or "").strip(),
            "arrival_city":       r.get("arrival_city") or r.get("arrival_station") or "—",
            "arrival_country":    (r.get("arrival_country") or "").strip(),
            "departure_time":     _fmt_time(r.get("departure_time")),
            "arrival_time":       _fmt_time(r.get("arrival_time")),
            "days_of_week":       _fmt_days(r.get("days_of_week")),
            "distance_km":        f"{r['distance_km']:.0f}" if r.get("distance_km") else "—",
            "emissions_co2":      f"{r['emissions_co2']:.1f}" if r.get("emissions_co2") else "—",
            "co2_per_pkm":        f"{r['co2_per_pkm']:.1f}" if r.get("co2_per_pkm") else "—",
            "is_night_train":     "🌙" if r.get("is_night_train") else "☀",
            "agency_name":        r.get("agency_name", "—"),
        })

    style_cond = [
        {"if": {"filter_query": '{is_night_train} = "🌙"'},
         "backgroundColor": "rgba(57,51,96,0.12)", "color": COLORS["night_light"]},
    ]
    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} routes trouvées".replace(",", "\u202f")
    return rows, page_count, style_cond, html.Div(), title


@callback(
    Output("store-trip", "data"),
    Input("routes-table", "active_cell"),
    State("store-routes", "data"),
    prevent_initial_call=True,
)
def load_trip_detail(active_cell, store):
    if not active_cell or not store:
        return no_update

    raw = safe_data(store)
    items = raw if isinstance(raw, list) else (raw.get("data") or raw.get("items") or [])
    row_idx = active_cell.get("row", 0)
    if row_idx >= len(items):
        return no_update

    row = items[row_idx]
    trip_id = row.get("trip_id")
    if not trip_id:
        return no_update

    return client.get(f"/routes/{trip_id}", ttl=TTL_SHORT)


@callback(Output("trip-detail-container", "children"), Input("store-trip", "data"))
def render_trip_detail(store):
    if not store:
        return html.Div()
    if not store.get("ok"):
        return error_banner(error_msg(store), "trip detail")

    raw = safe_data(store)
    items = raw if isinstance(raw, list) else (raw.get("data") or raw.get("items") or [])
    if not items:
        return empty_state("Aucun segment pour ce trip.")

    df = pd.DataFrame(items)

    rows = []
    for _, seg in df.iterrows():
        rows.append({col: str(seg.get(col, "—")) for col in df.columns})

    cols = [{"name": c.replace("_", " ").capitalize(), "id": c} for c in df.columns[:12]]

    table = obrail_table("trip-seg-table", cols, rows, page_size=50, server_side=False)
    return html.Div(
        [html.H3("Segments du trip", className="section-title"), table],
        className="card",
        style={"marginTop": "0"},
    )


@callback(
    Output("download-routes-csv",  "data"),
    Output("btn-export-routes",    "children"),
    Output("btn-export-routes",    "disabled"),
    Input("btn-export-routes",     "n_clicks"),
    State("routes-table",          "page_current"),
    State("routes-page-size",      "value"),
    State("routes-mode",           "value"),
    State("routes-dep-country",    "value"),
    State("routes-arr-country",    "value"),
    State("routes-dep-city",       "value"),
    State("routes-arr-city",       "value"),
    State("routes-is-night",       "value"),
    State("routes-sort-by",        "value"),
    State("routes-sort-order",     "value"),
    prevent_initial_call=True,
)
def export_routes_csv(n_clicks, page_current, page_size, mode, dep_country, arr_country,
                      dep_city, arr_city, is_night, sort_by, sort_order):
    import io, csv as csv_mod
    if not n_clicks:
        return no_update, "⬇  Exporter CSV", False

    params = {
        "mode":              mode,
        "departure_country": dep_country,
        "arrival_country":   arr_country,
        "departure_city":    dep_city,
        "arrival_city":      arr_city,
        "is_night_train":    is_night,
        "sort_by":           sort_by or "distance_km",
        "sort_order":        sort_order or "desc",
        "page":              (page_current or 0) + 1,
        "page_size":         page_size or 25,
    }

    result = client.get("/routes", params=params, ttl=0)   # L'export doit refléter l'état courant, pas une réponse possiblement stale.
    if not result.get("ok"):
        return no_update, "⬇  Exporter CSV", False

    items = safe_data(result).get("data") or []
    if not items:
        return no_update, "⬇  Exporter CSV", False

    # On découvre les colonnes dynamiquement pour ne perdre aucun champ API.
    fieldnames: list[str] = []
    for row in items:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in items:
        writer.writerow({k: str(r.get(k, "")) for k in fieldnames})

    return (
        dcc.send_string(buf.getvalue(), filename="obrail_routes.csv", type="text/csv"),
        "⬇  Exporter CSV",
        False,
    )
