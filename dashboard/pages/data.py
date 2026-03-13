"""Page Data — exploration tabulaire des données détaillées.

La page regroupe deux tables orientées usage analyste :
1) routes/trajets (`/routes`) avec filtres avancés,
2) comparaisons train/vol (`/compare`) avec tri/pagination serveur.

Les exports CSV reproduisent l'état courant des filtres pour éviter les écarts
entre ce qui est affiché et ce qui est téléchargé.
"""

import dash
import pandas as pd
from dash import Input, Output, State, callback, ctx, dcc, html, no_update

from dashboard.components.error import empty_state, error_banner, section_loader
from dashboard.components.filters import (
    country_dropdown, filter_row, mode_dropdown,
    page_size_selector, sort_controls, text_search,
)
from dashboard.components.tables import obrail_table
from dashboard.services.api_client import client, error_msg, safe_data
from dashboard.utils.cache import TTL_SHORT
from dashboard.utils.theme import COLORS

dash.register_page(__name__, path="/data", name="Data", order=6)

_DAYS = ["L", "M", "M", "J", "V", "S", "D"]

COMPARE_SORT_OPTIONS = [
    {"label": "Pays départ", "value": "departure_country"},
    {"label": "Pays arrivée", "value": "arrival_country"},
    {"label": "Ville départ", "value": "departure_city"},
    {"label": "Ville arrivée", "value": "arrival_city"},
    {"label": "Durée train", "value": "train_duration_min"},
    {"label": "Durée vol", "value": "flight_duration_min"},
    {"label": "Émissions train", "value": "train_emissions_co2"},
    {"label": "Émissions vol", "value": "flight_emissions_co2"},
    {"label": "Mode gagnant", "value": "best_mode"},
]

ROUTES_SORT_OPTIONS = [
    {"label": "Mode", "value": "mode"},
    {"label": "Pays départ", "value": "departure_country"},
    {"label": "Pays arrivée", "value": "arrival_country"},
    {"label": "Ville départ", "value": "departure_city"},
    {"label": "Ville arrivée", "value": "arrival_city"},
    {"label": "Distance", "value": "distance_km"},
    {"label": "Émissions CO₂", "value": "emissions_co2"},
    {"label": "CO₂/pkm", "value": "co2_per_pkm"},
]

ROUTES_COLS = [
    {"name": "Mode", "id": "mode"},
    {"name": "Départ", "id": "departure_city"},
    {"name": "Pays dep.", "id": "departure_country"},
    {"name": "Arrivée", "id": "arrival_city"},
    {"name": "Pays arr.", "id": "arrival_country"},
    {"name": "Opérateur", "id": "agency_name"},
    {"name": "Route", "id": "route_long_name"},
    {"name": "Nuit", "id": "is_night_train"},
    {"name": "Jours", "id": "days_of_week", "presentation": "markdown"},
    {"name": "Départ", "id": "departure_time"},
    {"name": "Arrivée", "id": "arrival_time"},
    {"name": "Distance", "id": "distance_km"},
    {"name": "CO₂", "id": "emissions_co2"},
]

COMPARE_PREFERRED_COLS = [
    "departure_city", "departure_country", "arrival_city", "arrival_country",
    "best_mode", "train_duration_min", "flight_duration_min",
    "train_distance_km", "flight_distance_km",
    "train_emissions_co2", "flight_emissions_co2", "days_of_week",
]


def _fmt_days(days_of_week: str | None) -> str:
    """Transforme le masque binaire des jours en libellé markdown lisible.

    Exemple: `1111100` devient une séquence où les jours actifs sont mis en
    évidence, afin d'accélérer la lecture dans la DataTable.
    """
    if not days_of_week:
        return "—"
    d = str(days_of_week).strip().ljust(7, "0")
    return " ".join(f"**{_DAYS[i]}**" if d[i] == "1" else "·" for i in range(7))


def _fmt_time(value: str | None) -> str:
    """Normalise l'heure en `HH:MM` pour garder des colonnes compactes."""
    if not value:
        return "—"
    return str(value)[:5]


def _csv_from_items(items: list[dict]) -> str:
    """Construit un CSV en mémoire en conservant toutes les clés rencontrées.

    L'ordre des colonnes suit la première apparition des champs pour rester
    stable tout en tolérant des payloads API hétérogènes.
    """
    import csv
    import io

    fieldnames: list[str] = []
    for row in items:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in items:
        writer.writerow({k: str(row.get(k, "")) for k in fieldnames})
    return buffer.getvalue()


layout = html.Div(
    [
        dcc.Store(id="store-data-routes"),
        dcc.Store(id="store-data-compare"),
        dcc.Download(id="download-data-routes-csv"),
        dcc.Download(id="download-data-compare-csv"),

        html.Div(
            [
                html.H1("Data", className="page-title"),
                html.P(
                    "Accès détaillé aux routes jour/nuit et à la table de comparaison train / vol.",
                    className="page-subtitle",
                ),
            ],
            className="page-header",
        ),

        html.Div(
            [
                html.H3("Table des routes et trajets", className="section-title"),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label("Mode"),
                                        mode_dropdown("data-routes-mode"),
                                    ],
                                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                ),
                                html.Div(
                                    [
                                        html.Label("Nuit"),
                                        dcc.Dropdown(
                                            id="data-routes-is-night",
                                            options=[
                                                {"label": "Nuit seulement", "value": "true"},
                                                {"label": "Jour seulement", "value": "false"},
                                            ],
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
                                html.Label("Source"),
                                text_search("data-routes-source", "ÖBB, SNCF…"),
                                html.Label("Opérateur"),
                                text_search("data-routes-agency", "ÖBB, SNCF…"),
                                html.Label("Type route"),
                                dcc.Input(
                                    id="data-routes-route-type",
                                    type="number",
                                    placeholder="2, 100…",
                                    className="obrail-input",
                                    style={"minWidth": "120px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Pays départ"),
                                country_dropdown("data-routes-dep-country"),
                                html.Label("Ville départ"),
                                text_search("data-routes-dep-city", "Paris, Wien…"),
                                html.Label("Gare départ"),
                                text_search("data-routes-dep-station", "Gare du Nord…"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Pays arrivée"),
                                country_dropdown("data-routes-arr-country"),
                                html.Label("Ville arrivée"),
                                text_search("data-routes-arr-city", "Berlin, Roma…"),
                                html.Label("Gare arrivée"),
                                text_search("data-routes-arr-station", "Roma Termini…"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Jours"),
                                dcc.Input(
                                    id="data-routes-days-of-week",
                                    type="text",
                                    placeholder="1111100",
                                    className="obrail-input",
                                    style={"minWidth": "120px"},
                                ),
                                html.Label("Service après"),
                                dcc.DatePickerSingle(
                                    id="data-routes-service-start-after",
                                    display_format="YYYY-MM-DD",
                                    placeholder="YYYY-MM-DD",
                                    className="obrail-datepicker",
                                ),
                                html.Label("Service avant"),
                                dcc.DatePickerSingle(
                                    id="data-routes-service-end-before",
                                    display_format="YYYY-MM-DD",
                                    placeholder="YYYY-MM-DD",
                                    className="obrail-datepicker",
                                ),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Distance min"),
                                dcc.Input(id="data-routes-min-distance", type="number", placeholder="0", className="obrail-input", style={"minWidth": "100px"}),
                                html.Label("Distance max"),
                                dcc.Input(id="data-routes-max-distance", type="number", placeholder="1500", className="obrail-input", style={"minWidth": "100px"}),
                                html.Label("CO₂ min"),
                                dcc.Input(id="data-routes-min-co2", type="number", placeholder="0", className="obrail-input", style={"minWidth": "100px"}),
                                html.Label("CO₂ max"),
                                dcc.Input(id="data-routes-max-co2", type="number", placeholder="500", className="obrail-input", style={"minWidth": "100px"}),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Div(),
                                html.Button("Rechercher", id="btn-data-routes-search", className="btn-primary", n_clicks=0),
                                html.Button("⬇  Exporter CSV", id="btn-export-data-routes", className="btn-secondary", style={"justifySelf": "end"}),
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
                                        sort_controls("data-routes-sort-by", "data-routes-sort-order", ROUTES_SORT_OPTIONS, "distance_km"),
                                    ],
                                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                ),
                                html.Div(
                                    [
                                        html.Label("Par page"),
                                        page_size_selector("data-routes-page-size"),
                                    ],
                                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                ),
                            ],
                            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "width": "100%"},
                        ),
                    ]
                ),
                html.Div(id="data-routes-status"),
                html.Div(id="data-routes-title", className="section-title", style={"margin": "12px 0 6px"}),
                section_loader(
                    obrail_table(
                        table_id="data-routes-table",
                        columns=ROUTES_COLS,
                        data=[],
                        page_size=25,
                        server_side=True,
                        total_count=0,
                    )
                ),
            ],
            className="card",
            style={"marginBottom": "20px"},
        ),

        html.Div(
            [
                html.H3("Table de comparaison train / vol", className="section-title"),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Mode gagnant"),
                                mode_dropdown("data-compare-best-mode"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Ville départ"),
                                text_search("data-compare-dep-city", "Paris, Wien…"),
                                html.Label("Pays départ"),
                                country_dropdown("data-compare-dep-country"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Ville arrivée"),
                                text_search("data-compare-arr-city", "Berlin, Roma…"),
                                html.Label("Pays arrivée"),
                                country_dropdown("data-compare-arr-country"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Div(),
                                html.Button("Rechercher", id="btn-data-compare-search", className="btn-primary", n_clicks=0),
                                html.Button("⬇  Exporter CSV", id="btn-export-data-compare", className="btn-secondary", style={"justifySelf": "end"}),
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
                                        sort_controls("data-compare-sort-by", "data-compare-sort-order", COMPARE_SORT_OPTIONS, "departure_country"),
                                    ],
                                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                ),
                                html.Div(
                                    [
                                        html.Label("Par page"),
                                        page_size_selector("data-compare-page-size"),
                                    ],
                                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                ),
                            ],
                            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "width": "100%"},
                        ),
                    ]
                ),
                html.Div(id="data-compare-status"),
                html.Div(id="data-compare-title", className="section-title", style={"margin": "12px 0 6px"}),
                section_loader(
                    obrail_table(
                        table_id="data-compare-table",
                        columns=[],
                        data=[],
                        page_size=25,
                        server_side=True,
                        total_count=0,
                    )
                ),
            ],
            className="card",
        ),
    ],
    className="page-container",
)


@callback(
    Output("store-data-routes", "data"),
    Input("btn-data-routes-search", "n_clicks"),
    Input("data-routes-table", "page_current"),
    Input("url", "pathname"),
    State("data-routes-mode", "value"),
    State("data-routes-source", "value"),
    State("data-routes-dep-country", "value"),
    State("data-routes-dep-city", "value"),
    State("data-routes-dep-station", "value"),
    State("data-routes-arr-country", "value"),
    State("data-routes-arr-city", "value"),
    State("data-routes-arr-station", "value"),
    State("data-routes-agency", "value"),
    State("data-routes-route-type", "value"),
    State("data-routes-is-night", "value"),
    State("data-routes-days-of-week", "value"),
    State("data-routes-min-distance", "value"),
    State("data-routes-max-distance", "value"),
    State("data-routes-min-co2", "value"),
    State("data-routes-max-co2", "value"),
    State("data-routes-service-start-after", "date"),
    State("data-routes-service-end-before", "date"),
    State("data-routes-sort-by", "value"),
    State("data-routes-sort-order", "value"),
    State("data-routes-page-size", "value"),
    prevent_initial_call=False,
)
def load_data_routes(n_clicks, page_current, pathname,
                     mode, source, dep_country, dep_city, dep_station,
                     arr_country, arr_city, arr_station,
                     agency_name, route_type, is_night, days_of_week,
                     min_distance, max_distance, min_co2, max_co2,
                     service_start_after, service_end_before,
                     sort_by, sort_order, page_size):
    """Charge la page de résultats `/routes` selon l'état courant des filtres.

    Le bouton de recherche force un retour en première page, tandis que la
    pagination conserve les filtres actifs pour assurer une navigation cohérente.
    """
    if pathname and pathname != "/data":
        return no_update

    page = 1 if ctx.triggered_id == "btn-data-routes-search" else (page_current or 0) + 1
    params = {
        "mode": mode,
        "source": source,
        "departure_country": dep_country,
        "departure_city": dep_city,
        "departure_station": dep_station,
        "arrival_country": arr_country,
        "arrival_city": arr_city,
        "arrival_station": arr_station,
        "agency_name": agency_name,
        "route_type": route_type,
        "is_night_train": is_night,
        "days_of_week": days_of_week,
        "min_distance_km": min_distance,
        "max_distance_km": max_distance,
        "min_co2": min_co2,
        "max_co2": max_co2,
        "service_start_after": service_start_after,
        "service_end_before": service_end_before,
        "sort_by": sort_by or "distance_km",
        "sort_order": sort_order or "desc",
        "page": page,
        "page_size": page_size or 25,
    }
    return client.get("/routes", params=params, ttl=TTL_SHORT)


@callback(
    Output("data-routes-table", "data"),
    Output("data-routes-table", "page_count"),
    Output("data-routes-table", "style_data_conditional"),
    Output("data-routes-status", "children"),
    Output("data-routes-title", "children"),
    Input("store-data-routes", "data"),
)
def render_data_routes_table(store):
    """Convertit le payload routes en lignes DataTable prêtes à l'affichage.

    La fonction centralise aussi les états UX (vide/erreur) et la mise en forme
    des champs sensibles à la lisibilité (heures, jours, nuit/jour).
    """
    if not store:
        return [], 1, [], html.Div(), ""
    if not store.get("ok"):
        return [], 1, [], error_banner(error_msg(store), "/routes"), ""

    data_obj = safe_data(store)
    items = data_obj.get("data") or data_obj.get("items") or []
    total = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)
    if not items:
        return [], 1, [], empty_state("Aucune route correspondante."), ""

    rows = []
    for row in items:
        rows.append(
            {
                "mode": row.get("mode") or "—",
                "departure_city": row.get("departure_city") or "—",
                "departure_country": (row.get("departure_country") or "").strip(),
                "arrival_city": row.get("arrival_city") or "—",
                "arrival_country": (row.get("arrival_country") or "").strip(),
                "agency_name": row.get("agency_name") or "—",
                "route_long_name": row.get("route_long_name") or "—",
                "is_night_train": "🌙" if row.get("is_night_train") else "☀",
                "days_of_week": _fmt_days(row.get("days_of_week")),
                "departure_time": _fmt_time(row.get("departure_time")),
                "arrival_time": _fmt_time(row.get("arrival_time")),
                "distance_km": f"{row['distance_km']:.0f}" if row.get("distance_km") else "—",
                "emissions_co2": f"{row['emissions_co2']:.1f}" if row.get("emissions_co2") else "—",
            }
        )

    style_cond = [
        {
            "if": {"filter_query": '{is_night_train} = "🌙"'},
            "backgroundColor": "rgba(57,51,96,0.12)",
            "color": COLORS["night_light"],
        },
    ]
    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} routes trouvées".replace(",", "\u202f")
    return rows, page_count, style_cond, html.Div(), title


@callback(
    Output("download-data-routes-csv", "data"),
    Input("btn-export-data-routes", "n_clicks"),
    State("data-routes-table", "page_current"),
    State("data-routes-page-size", "value"),
    State("data-routes-mode", "value"),
    State("data-routes-source", "value"),
    State("data-routes-dep-country", "value"),
    State("data-routes-dep-city", "value"),
    State("data-routes-dep-station", "value"),
    State("data-routes-arr-country", "value"),
    State("data-routes-arr-city", "value"),
    State("data-routes-arr-station", "value"),
    State("data-routes-agency", "value"),
    State("data-routes-route-type", "value"),
    State("data-routes-is-night", "value"),
    State("data-routes-days-of-week", "value"),
    State("data-routes-min-distance", "value"),
    State("data-routes-max-distance", "value"),
    State("data-routes-min-co2", "value"),
    State("data-routes-max-co2", "value"),
    State("data-routes-service-start-after", "date"),
    State("data-routes-service-end-before", "date"),
    State("data-routes-sort-by", "value"),
    State("data-routes-sort-order", "value"),
    prevent_initial_call=True,
)
def export_data_routes_csv(n_clicks, page_current, page_size,
                           mode, source, dep_country, dep_city, dep_station,
                           arr_country, arr_city, arr_station,
                           agency_name, route_type, is_night, days_of_week,
                           min_distance, max_distance, min_co2, max_co2,
                           service_start_after, service_end_before,
                           sort_by, sort_order):
    """Exporte la page courante des routes avec les filtres effectivement appliqués."""
    if not n_clicks:
        return no_update

    result = client.get(
        "/routes",
        params={
            "mode": mode,
            "source": source,
            "departure_country": dep_country,
            "departure_city": dep_city,
            "departure_station": dep_station,
            "arrival_country": arr_country,
            "arrival_city": arr_city,
            "arrival_station": arr_station,
            "agency_name": agency_name,
            "route_type": route_type,
            "is_night_train": is_night,
            "days_of_week": days_of_week,
            "min_distance_km": min_distance,
            "max_distance_km": max_distance,
            "min_co2": min_co2,
            "max_co2": max_co2,
            "service_start_after": service_start_after,
            "service_end_before": service_end_before,
            "sort_by": sort_by or "distance_km",
            "sort_order": sort_order or "desc",
            "page": (page_current or 0) + 1,
            "page_size": page_size or 25,
        },
        ttl=0,
    )
    if not result.get("ok"):
        return no_update

    items = safe_data(result).get("data") or []
    if not items:
        return no_update

    return dcc.send_string(_csv_from_items(items), filename="obrail_routes.csv", type="text/csv")


@callback(
    Output("store-data-compare", "data"),
    Input("btn-data-compare-search", "n_clicks"),
    Input("data-compare-table", "page_current"),
    Input("url", "pathname"),
    State("data-compare-dep-city", "value"),
    State("data-compare-dep-country", "value"),
    State("data-compare-arr-city", "value"),
    State("data-compare-arr-country", "value"),
    State("data-compare-best-mode", "value"),
    State("data-compare-sort-by", "value"),
    State("data-compare-sort-order", "value"),
    State("data-compare-page-size", "value"),
    prevent_initial_call=False,
)
def load_data_compare(n_clicks, page_current, pathname,
                      dep_city, dep_country, arr_city, arr_country,
                      best_mode, sort_by, sort_order, page_size):
    """Charge les comparaisons train/vol avec tri et pagination côté API."""
    if pathname and pathname != "/data":
        return no_update

    page = 1 if ctx.triggered_id == "btn-data-compare-search" else (page_current or 0) + 1
    params = {
        "departure_city": dep_city,
        "departure_country": dep_country,
        "arrival_city": arr_city,
        "arrival_country": arr_country,
        "best_mode": best_mode,
        "sort_by": sort_by or "departure_country",
        "sort_order": sort_order or "asc",
        "page": page,
        "page_size": page_size or 25,
    }
    return client.get("/compare", params=params, ttl=TTL_SHORT)


@callback(
    Output("data-compare-table", "columns"),
    Output("data-compare-table", "data"),
    Output("data-compare-table", "page_count"),
    Output("data-compare-status", "children"),
    Output("data-compare-title", "children"),
    Input("store-data-compare", "data"),
)
def render_data_compare_table(store):
    """Prépare les colonnes et lignes de la table compare en priorisant les champs métier.

    L'ordre des colonnes met d'abord le contexte O/D et le mode gagnant, puis
    complète avec les attributs retournés par l'API.
    """
    if not store:
        return [], [], 1, html.Div(), ""
    if not store.get("ok"):
        return [], [], 1, error_banner(error_msg(store), "/compare"), ""

    data_obj = safe_data(store)
    items = data_obj.get("data") or data_obj.get("items") or []
    total = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)
    if not items:
        return [], [], 1, empty_state("Aucune comparaison correspondante."), ""

    df = pd.DataFrame(items)
    ordered = [c for c in COMPARE_PREFERRED_COLS if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    final_cols = ordered + remaining

    rows = []
    for row in items:
        formatted = {c: row.get(c, "—") for c in final_cols}
        if "best_mode" in formatted:
            formatted["best_mode"] = "🚆 Train" if formatted["best_mode"] == "train" else "✈️ Vol"
        if "days_of_week" in formatted:
            formatted["days_of_week"] = _fmt_days(formatted["days_of_week"])
        rows.append({k: ("—" if v is None else str(v)) for k, v in formatted.items()})

    columns = []
    for c in final_cols[:12]:
        col = {"name": c.replace("_", " ").capitalize(), "id": c}
        if c == "days_of_week":
            col["presentation"] = "markdown"
        columns.append(col)

    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} comparaisons".replace(",", "\u202f")
    return columns, rows, page_count, html.Div(), title


@callback(
    Output("download-data-compare-csv", "data"),
    Input("btn-export-data-compare", "n_clicks"),
    State("data-compare-table", "page_current"),
    State("data-compare-page-size", "value"),
    State("data-compare-dep-city", "value"),
    State("data-compare-dep-country", "value"),
    State("data-compare-arr-city", "value"),
    State("data-compare-arr-country", "value"),
    State("data-compare-best-mode", "value"),
    State("data-compare-sort-by", "value"),
    State("data-compare-sort-order", "value"),
    prevent_initial_call=True,
)
def export_data_compare_csv(n_clicks, page_current, page_size,
                            dep_city, dep_country, arr_city, arr_country,
                            best_mode, sort_by, sort_order):
    """Exporte la vue compare de la page courante sans réutiliser le cache."""
    if not n_clicks:
        return no_update

    result = client.get(
        "/compare",
        params={
            "departure_city": dep_city,
            "departure_country": dep_country,
            "arrival_city": arr_city,
            "arrival_country": arr_country,
            "best_mode": best_mode,
            "sort_by": sort_by or "departure_country",
            "sort_order": sort_order or "asc",
            "page": (page_current or 0) + 1,
            "page_size": page_size or 25,
        },
        ttl=0,
    )
    if not result.get("ok"):
        return no_update

    items = safe_data(result).get("data") or []
    if not items:
        return no_update

    return dcc.send_string(_csv_from_items(items), filename="obrail_compare.csv", type="text/csv")