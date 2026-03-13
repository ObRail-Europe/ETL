"""
Page Référentiel — villes, gares, aéroports.

Trois onglets avec filtres et tables paginées côté serveur.
Appels à la demande (filtres + pagination), pas de chargement initial lourd.
"""

import dash
import pandas as pd
from dash import Input, Output, State, callback, dcc, html, no_update, ctx

from dashboard.components.error import empty_state, error_banner, section_loader
from dashboard.components.filters import country_dropdown, filter_row, text_search
from dashboard.components.tables import obrail_table
from dashboard.services.api_client import client, safe_data, safe_items, error_msg
from dashboard.utils.cache import TTL_LONG, TTL_MEDIUM
from dashboard.utils.theme import COLORS

dash.register_page(__name__, path="/referential", name="Référentiel", order=5)

layout = html.Div(
    [
        dcc.Store(id="store-ref-cities"),
        dcc.Store(id="store-ref-stations"),
        dcc.Store(id="store-ref-airports"),
        dcc.Download(id="download-ref-cities"),
        dcc.Download(id="download-ref-stations"),
        dcc.Download(id="download-ref-airports"),

        html.Div(
            [
                html.H1("Référentiel", className="page-title"),
                html.P("Villes, gares ferroviaires et aéroports couverts par ObRail.",
                       className="page-subtitle"),
            ],
            className="page-header",
        ),

        # Les onglets évitent de charger simultanément trois référentiels volumineux.
        dcc.Tabs(
            id="ref-tabs",
            value="cities",
            className="ref-tabs",
            children=[
                dcc.Tab(label="Villes",   value="cities",   className="ref-tab"),
                dcc.Tab(label="Gares",    value="stations", className="ref-tab"),
                dcc.Tab(label="Aéroports", value="airports", className="ref-tab"),
            ],
        ),

        # Les villes servent souvent d'entrée d'exploration, on les affiche par défaut.
        html.Div(
            id="panel-cities",
            children=[
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Pays"),
                                country_dropdown("ref-cities-country"),
                                html.Label("Recherche"),
                                text_search("ref-cities-search", "Paris, Berlin…"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label("Gare"),
                                        dcc.Dropdown(
                                            id="ref-cities-has-station",
                                            options=[{"label": "Avec gare",     "value": "true"},
                                                     {"label": "Sans gare",     "value": "false"}],
                                            placeholder="Toutes",
                                            clearable=True,
                                            className="obrail-dropdown",
                                            style={"minWidth": "130px"},
                                        ),
                                    ],
                                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                ),
                                html.Div(
                                    [
                                        html.Label("Aéroport"),
                                        dcc.Dropdown(
                                            id="ref-cities-has-airport",
                                            options=[{"label": "Avec aéroport", "value": "true"},
                                                     {"label": "Sans aéroport", "value": "false"}],
                                            placeholder="Tous",
                                            clearable=True,
                                            className="obrail-dropdown",
                                            style={"minWidth": "150px"},
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
                                html.Div(),
                                html.Button("Rechercher", id="btn-search-cities", className="btn-primary", n_clicks=0),
                                html.Button("⬇  Export CSV", id="btn-export-cities", className="btn-secondary", style={"justifySelf": "end"}),
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
                html.Div(id="ref-cities-status"),
                html.Div(id="ref-cities-title", className="section-title", style={"margin": "12px 0 6px"}),
                section_loader(
                    obrail_table(
                        table_id="ref-cities-table",
                        columns=[
                            {"name": "Ville",              "id": "city_name"},
                            {"name": "Pays",              "id": "country_code"},
                            {"name": "Routes train",      "id": "train_routes"},
                            {"name": "Routes vol",        "id": "flight_routes"},
                            {"name": "Nb gares",          "id": "nb_stations"},
                            {"name": "Routes nocturnes",  "id": "night_routes"},
                            {"name": "Avec gare",         "id": "has_station"},
                            {"name": "Avec aéroport",     "id": "has_airport"},
                        ],
                        data=[],
                        page_size=25,
                        server_side=True,
                        total_count=0,
                    ),
                    keep_visible=False,
                ),
            ],
        ),

        # Le panel gares est masqué tant qu'il n'est pas sélectionné pour limiter le bruit visuel.
        html.Div(
            id="panel-stations",
            style={"display": "none"},
            children=[
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Pays"),
                                country_dropdown("ref-stations-country"),
                                html.Label("Ville"),
                                text_search("ref-stations-city", "Paris, Wien…"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Recherche"),
                                text_search("ref-stations-search", "Gare du Nord…"),
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
                                html.Button("Rechercher", id="btn-search-stations", className="btn-primary", n_clicks=0),
                                html.Button("⬇  Export CSV", id="btn-export-stations", className="btn-secondary", style={"justifySelf": "end"}),
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
                html.Div(id="ref-stations-status"),
                html.Div(id="ref-stations-title", className="section-title", style={"margin": "12px 0 6px"}),
                section_loader(
                    obrail_table(
                        table_id="ref-stations-table",
                        columns=[
                            {"name": "Nom",               "id": "station_name"},
                            {"name": "Ville",            "id": "city_name"},
                            {"name": "Pays",             "id": "country_code"},
                            {"name": "Gare parent",      "id": "parent_station"},
                            {"name": "Départs",          "id": "nb_departures"},
                            {"name": "Destinations",     "id": "destinations_served"},
                            {"name": "Départs nocturnes", "id": "night_departures"},
                        ],
                        data=[],
                        page_size=25,
                        server_side=True,
                        total_count=0,
                    ),
                    keep_visible=False,
                ),
            ],
        ),

        # Même principe pour les aéroports: rendu à la demande selon l'onglet actif.
        html.Div(
            id="panel-airports",
            style={"display": "none"},
            children=[
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Pays"),
                                country_dropdown("ref-airports-country"),
                                html.Label("Ville"),
                                text_search("ref-airports-city", "Paris, London…"),
                            ],
                            style={"display": "flex", "alignItems": "center", "gap": "10px", "width": "100%", "flexWrap": "nowrap"},
                        ),
                    ]
                ),
                filter_row(
                    [
                        html.Div(
                            [
                                html.Label("Recherche"),
                                text_search("ref-airports-search", "CDG, Heathrow…"),
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
                                html.Button("Rechercher", id="btn-search-airports", className="btn-primary", n_clicks=0),
                                html.Button("⬇  Export CSV", id="btn-export-airports", className="btn-secondary", style={"justifySelf": "end"}),
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
                html.Div(id="ref-airports-status"),
                html.Div(id="ref-airports-title", className="section-title", style={"margin": "12px 0 6px"}),
                section_loader(
                    obrail_table(
                        table_id="ref-airports-table",
                        columns=[
                            {"name": "Nom",            "id": "airport_name"},
                            {"name": "Ville",         "id": "city_name"},
                            {"name": "Pays",          "id": "country_code"},
                            {"name": "Nb vols",       "id": "nb_flights"},
                            {"name": "Destinations",  "id": "destinations_served"},
                            {"name": "Pays served",   "id": "countries_served"},
                        ],
                        data=[],
                        page_size=25,
                        server_side=True,
                        total_count=0,
                    ),
                    keep_visible=False,
                ),
            ],
        ),
    ],
    className="page-container",
)


@callback(
    [
        Output("panel-cities",   "style"),
        Output("panel-stations", "style"),
        Output("panel-airports", "style"),
    ],
    Input("ref-tabs", "value"),
)
def toggle_panels(tab):
    show = {"display": "block"}
    hide = {"display": "none"}
    return (
        show if tab == "cities"   else hide,
        show if tab == "stations" else hide,
        show if tab == "airports" else hide,
    )


@callback(
    Output("store-ref-cities", "data"),
    [Input("btn-search-cities", "n_clicks"), Input("ref-cities-table", "page_current")],
    [
        State("ref-cities-country", "value"),
        State("ref-cities-search", "value"),
        State("ref-cities-has-station", "value"),
        State("ref-cities-has-airport", "value"),
    ],
    prevent_initial_call=True,
)
def load_cities(n_clicks, page_current, country, search, has_station, has_airport):
    if not n_clicks and ctx.triggered_id != "ref-cities-table":
        return no_update

    page = 1 if ctx.triggered_id == "btn-search-cities" else (page_current or 0) + 1

    params = {
        "country":     country,
        "search":      search,
        "has_station": has_station,
        "has_airport": has_airport,
        "page":        page,
        "page_size":   25,
    }
    return client.get("/cities", params=params, ttl=TTL_LONG)


@callback(
    Output("ref-cities-table",  "data"),
    Output("ref-cities-table",  "page_count"),
    Output("ref-cities-status", "children"),
    Output("ref-cities-title",  "children"),
    Input("store-ref-cities", "data"),
)
def render_cities_table(store):
    return _render_table_cities(store)


@callback(
    Output("store-ref-stations", "data"),
    [
        Input("ref-tabs", "value"),
        Input("btn-search-stations", "n_clicks"),
        Input("ref-stations-table", "page_current"),
    ],
    [
        State("ref-stations-country", "value"),
        State("ref-stations-city", "value"),
        State("ref-stations-search", "value"),
    ],
    prevent_initial_call=False,
)
def load_stations(active_tab, n_clicks, page_current, country, city, search):
    if active_tab != "stations":
        return no_update

    if ctx.triggered_id == "ref-tabs":
        page = 1
    elif ctx.triggered_id == "btn-search-stations":
        page = 1
    elif ctx.triggered_id == "ref-stations-table":
        page = (page_current or 0) + 1
    else:
        return no_update

    if ctx.triggered_id != "ref-tabs" and not n_clicks and ctx.triggered_id != "ref-stations-table":
        return no_update

    params = {
        "country":   country,
        "city":      city,
        "search":    search,
        "page":      page,
        "page_size": 25,
    }
    return client.get("/stations", params=params, ttl=TTL_LONG)


@callback(
    Output("ref-stations-table",  "data"),
    Output("ref-stations-table",  "page_count"),
    Output("ref-stations-status", "children"),
    Output("ref-stations-title",  "children"),
    Input("store-ref-stations", "data"),
)
def render_stations_table(store):
    return _render_table_stations(store)


@callback(
    Output("store-ref-airports", "data"),
    [
        Input("ref-tabs", "value"),
        Input("btn-search-airports", "n_clicks"),
        Input("ref-airports-table", "page_current"),
    ],
    [
        State("ref-airports-country", "value"),
        State("ref-airports-city", "value"),
        State("ref-airports-search", "value"),
    ],
    prevent_initial_call=False,
)
def load_airports(active_tab, n_clicks, page_current, country, city, search):
    if active_tab != "airports":
        return no_update

    if ctx.triggered_id == "ref-tabs":
        page = 1
    elif ctx.triggered_id == "btn-search-airports":
        page = 1
    elif ctx.triggered_id == "ref-airports-table":
        page = (page_current or 0) + 1
    else:
        return no_update

    if ctx.triggered_id != "ref-tabs" and not n_clicks and ctx.triggered_id != "ref-airports-table":
        return no_update

    params = {
        "country":   country,
        "city":      city,
        "search":    search,
        "page":      page,
        "page_size": 25,
    }
    return client.get("/airports", params=params, ttl=TTL_LONG)


@callback(
    Output("ref-airports-table",  "data"),
    Output("ref-airports-table",  "page_count"),
    Output("ref-airports-status", "children"),
    Output("ref-airports-title",  "children"),
    Input("store-ref-airports", "data"),
)
def render_airports_table(store):
    return _render_table_airports(store)


def _render_table_cities(store: dict):
    """Prépare les lignes de la table villes en préservant le schéma exposé par l'API.

    Le mapping explicite stabilise l'ordre des colonnes affichées et permet de fournir
    des valeurs de repli lisibles quand certains champs sont absents.
    """
    empty: list = []
    if not store:
        return empty, 1, html.Div(), ""
    if not store.get("ok"):
        return empty, 1, error_banner(error_msg(store), "/cities"), ""

    data_obj = safe_data(store)
    items = data_obj.get("data") or data_obj.get("items") or []
    total = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)

    if not items:
        return empty, 1, empty_state("Aucun résultat."), ""

    # Le mapping explicite évite qu'un changement de clé casse silencieusement l'affichage.
    remapped = []
    for row in items:
        remapped.append({
            "city_name": str(row.get("city_name", "—")),
            "country_code": str(row.get("country_code", "—")),
            "train_routes": str(row.get("train_routes", 0)),
            "flight_routes": str(row.get("flight_routes", 0)),
            "nb_stations": str(row.get("nb_stations", 0)),
            "night_routes": str(row.get("night_routes", 0)),
            "has_station": str(row.get("has_station", False)),
            "has_airport": str(row.get("has_airport", False)),
        })

    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} résultats".replace(",", "\u202f")
    return remapped, page_count, html.Div(), title



def _render_table_stations(store: dict):
    """Prépare les lignes de la table gares avec des champs homogènes côté UI.

    Le rendu normalise les valeurs manquantes pour éviter des cellules vides difficiles
    à interpréter lors des comparaisons.
    """
    empty: list = []
    if not store:
        return empty, 1, html.Div(), ""
    if not store.get("ok"):
        return empty, 1, error_banner(error_msg(store), "/stations"), ""

    data_obj = safe_data(store)
    items = data_obj.get("data") or data_obj.get("items") or []
    total = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)

    if not items:
        return empty, 1, empty_state("Aucun résultat."), ""

    # On garde un mapping explicite pour verrouiller l'interface malgré l'évolution du backend.
    remapped = []
    for row in items:
        remapped.append({
            "station_name": str(row.get("station_name", "—")),
            "city_name": str(row.get("city_name", "—")),
            "country_code": str(row.get("country_code", "—")),
            "parent_station": str(row.get("parent_station", "—")),
            "nb_departures": str(row.get("nb_departures", 0)),
            "destinations_served": str(row.get("destinations_served", 0)),
            "night_departures": str(row.get("night_departures", 0)),
        })

    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} résultats".replace(",", "\u202f")
    return remapped, page_count, html.Div(), title



def _render_table_airports(store: dict):
    """Prépare les lignes de la table aéroports avec un format stable pour la lecture.

    Comme pour les autres référentiels, les valeurs sont converties en chaînes afin de
    simplifier le rendu DataTable et l'export ultérieur.
    """
    empty: list = []
    if not store:
        return empty, 1, html.Div(), ""
    if not store.get("ok"):
        return empty, 1, error_banner(error_msg(store), "/airports"), ""

    data_obj = safe_data(store)
    items = data_obj.get("data") or data_obj.get("items") or []
    total = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)

    if not items:
        return empty, 1, empty_state("Aucun résultat."), ""

    # Le mapping explicite conserve un ordre métier cohérent pour les utilisateurs.
    remapped = []
    for row in items:
        remapped.append({
            "airport_name": str(row.get("airport_name", "—")),
            "city_name": str(row.get("city_name", "—")),
            "country_code": str(row.get("country_code", "—")),
            "nb_flights": str(row.get("nb_flights", 0)),
            "destinations_served": str(row.get("destinations_served", 0)),
            "countries_served": str(row.get("countries_served", 0)),
        })

    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} résultats".replace(",", "\u202f")
    return remapped, page_count, html.Div(), title



def _render_table(store: dict, endpoint: str, priority_cols: list):
    """Construit une sortie normalisée pour une réponse paginée générique.

    Cette version factorisée permet de réutiliser la même logique d'état (erreur, vide,
    pagination) quand un endpoint suit le contrat standard data/items/total/page_size.
    """
    empty: list = []
    if not store:
        return empty, 1, html.Div(), ""
    if not store.get("ok"):
        return empty, 1, error_banner(error_msg(store), endpoint), ""

    data_obj  = safe_data(store)
    items     = data_obj.get("data") or data_obj.get("items") or []
    total     = data_obj.get("total", 0)
    page_size = data_obj.get("page_size", 25)

    if not items:
        return empty, 1, empty_state("Aucun résultat."), ""

    df = pd.DataFrame(items)
    ordered   = [c for c in priority_cols if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    final_cols = (ordered + remaining)[:12]

    rows = [{c: str(r.get(c, "—")) for c in final_cols} for r in items]
    page_count = max(1, total // page_size + (1 if total % page_size else 0))
    title = f"{total:,} résultats".replace(",", "\u202f")
    return rows, page_count, html.Div(), title



def _csv_from_items(items: list[dict], fieldnames: list[str] | None = None) -> str:
    import csv
    import io

    if fieldnames is None:
        discovered: list[str] = []
        for row in items:
            for key in row.keys():
                if key not in discovered:
                    discovered.append(key)
        fieldnames = discovered

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in items:
        writer.writerow({k: str(row.get(k, "")) for k in fieldnames})
    return buffer.getvalue()


@callback(
    Output("download-ref-cities", "data"),
    Input("btn-export-cities", "n_clicks"),
    State("ref-cities-country", "value"),
    State("ref-cities-search", "value"),
    State("ref-cities-has-station", "value"),
    State("ref-cities-has-airport", "value"),
    State("ref-cities-table", "page_current"),
    State("ref-cities-table", "page_size"),
    prevent_initial_call=True,
)
def export_cities_csv(n_clicks, country, search, has_station, has_airport, page_current, page_size):
    if not n_clicks:
        return no_update

    params = {
        "country": country,
        "search": search,
        "has_station": has_station,
        "has_airport": has_airport,
        "page": (page_current or 0) + 1,
        "page_size": page_size or 25,
    }
    result = client.get("/cities", params=params, ttl=0)
    if not result.get("ok"):
        return no_update

    items = safe_data(result).get("data") or []
    if not items:
        return no_update

    csv_text = _csv_from_items(items)
    return dcc.send_string(csv_text, filename="obrail_cities_page.csv", type="text/csv")


@callback(
    Output("download-ref-stations", "data"),
    Input("btn-export-stations", "n_clicks"),
    State("ref-stations-country", "value"),
    State("ref-stations-city", "value"),
    State("ref-stations-search", "value"),
    State("ref-stations-table", "page_current"),
    State("ref-stations-table", "page_size"),
    prevent_initial_call=True,
)
def export_stations_csv(n_clicks, country, city, search, page_current, page_size):
    if not n_clicks:
        return no_update

    params = {
        "country": country,
        "city": city,
        "search": search,
        "page": (page_current or 0) + 1,
        "page_size": page_size or 25,
    }
    result = client.get("/stations", params=params, ttl=0)
    if not result.get("ok"):
        return no_update

    items = safe_data(result).get("data") or []
    if not items:
        return no_update

    csv_text = _csv_from_items(items)
    return dcc.send_string(csv_text, filename="obrail_stations_page.csv", type="text/csv")


@callback(
    Output("download-ref-airports", "data"),
    Input("btn-export-airports", "n_clicks"),
    State("ref-airports-country", "value"),
    State("ref-airports-city", "value"),
    State("ref-airports-search", "value"),
    State("ref-airports-table", "page_current"),
    State("ref-airports-table", "page_size"),
    prevent_initial_call=True,
)
def export_airports_csv(n_clicks, country, city, search, page_current, page_size):
    if not n_clicks:
        return no_update

    params = {
        "country": country,
        "city": city,
        "search": search,
        "page": (page_current or 0) + 1,
        "page_size": page_size or 25,
    }
    result = client.get("/airports", params=params, ttl=0)
    if not result.get("ok"):
        return no_update

    items = safe_data(result).get("data") or []
    if not items:
        return no_update

    csv_text = _csv_from_items(items)
    return dcc.send_string(csv_text, filename="obrail_airports_page.csv", type="text/csv")
