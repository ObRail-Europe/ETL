"""
Page Vue d'ensemble — chargement parallèle des métriques globales.

Appels parallèles au chargement :
  - GET /quality/summary         (TTL_LONG)
  - GET /analysis/day-night/summary (TTL_MEDIUM)
  - GET /stats/operators         (TTL_MEDIUM)
  - GET /stats/emissions-by-distance (TTL_LONG)
"""

import dash
import pandas as pd
from dash import Input, Output, callback, dcc, html, no_update

from dashboard.components.charts import (
    bar_chart_h,
    empty_figure,
    scatter_line,
    pie_donut,
)
from dashboard.components.error import error_banner, section_loader
from dashboard.components.kpis import kpi_card, kpi_row
from dashboard.services.api_client import client, safe_data, safe_items, error_msg
from dashboard.utils.cache import TTL_24H
from dashboard.utils.theme import COLORS, fmt_co2, fmt_number, fmt_pct

dash.register_page(__name__, path="/", name="Vue d'ensemble", order=0)

layout = html.Div(
    [
        dcc.Store(id="store-overview", storage_type="session"),

        # Le bandeau d'intro pose le niveau de lecture avant les indicateurs.
        html.Div(
            [
                html.H1("Vue d'ensemble", className="page-title"),
                html.P(
                    "Indicateurs globaux du réseau ferroviaire européen observé.",
                    className="page-subtitle",
                ),
            ],
            className="page-header",
        ),

        # Les KPI arrivent en premier pour donner une synthèse immédiate.
        section_loader(html.Div(id="overview-kpis")),

        html.Hr(className="section-separator"),

        # La grille compare deux angles complémentaires sans changer de contexte visuel.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Top opérateurs", className="section-title"),
                        section_loader(dcc.Graph(id="chart-operators", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Émissions CO₂ par distance", className="section-title"),
                        section_loader(dcc.Graph(id="chart-emissions", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
            ],
            className="grid-2",
            style={"marginBottom": "20px"},
        ),

        # Ce bloc rapproche la répartition jour/nuit et la qualité pour faciliter la lecture croisée.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Répartition Jour / Nuit", className="section-title"),
                        section_loader(dcc.Graph(id="chart-day-night-donut", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(id="overview-quality-panel", className="card"),
            ],
            className="grid-2",
        ),
    ],
    className="page-container",
)


@callback(
    Output("store-overview", "data"),
    Input("url", "pathname"),
    prevent_initial_call=False,
)
def load_overview(pathname: str):
    """Charge les jeux de données nécessaires à la page en un seul lot.

    Le callback est lié à la navigation pour éviter tout rechargement hors contexte.
    Les 4 appels sont lancés en parallèle afin de réduire le temps perçu avant rendu
    des KPI et des graphiques.
    """
    if pathname not in ("/", None):
        return no_update

    results = client.parallel(
        [
            ("/quality/summary",               None, TTL_24H),
            ("/analysis/day-night/summary",    None, TTL_24H),
            ("/stats/operators",               None, TTL_24H),
            ("/stats/emissions-by-distance",   None, TTL_24H),
        ]
    )
    quality_r, dn_r, ops_r, emiss_r = results
    return {
        "quality":   quality_r,
        "day_night": dn_r,
        "operators": ops_r,
        "emissions": emiss_r,
    }


@callback(Output("overview-kpis", "children"), Input("store-overview", "data"))
def render_kpis(store):
    if not store:
        return html.Div()

    quality_result = store.get("quality", {})
    q_raw = safe_data(quality_result)
    q = q_raw.get("data") if isinstance(q_raw, dict) and isinstance(q_raw.get("data"), dict) else q_raw
    dn_items = safe_items(store.get("day_night", {}))

    total_routes_raw = q.get("total_routes") or 0
    total_train_raw = q.get("total_train") or 0
    total_flight_raw = q.get("total_flight") or 0
    countries_raw = max(q.get("countries_departure") or 0, q.get("countries_arrival") or 0)

    completeness_fields = [
        q.get("distance_completeness_pct"),
        q.get("emissions_completeness_pct"),
        q.get("schedule_completeness_pct"),
        q.get("dep_city_completeness_pct"),
        q.get("arr_city_completeness_pct"),
    ]
    completeness_vals = [float(v) for v in completeness_fields if v is not None]
    completeness_raw = (sum(completeness_vals) / len(completeness_vals)) if completeness_vals else 0

    night_routes = 0
    day_routes = 0
    weighted_co2_sum = 0.0
    weighted_count = 0
    for row in dn_items:
        total = row.get("total_routes") or 0
        avg_emissions = row.get("avg_emissions_co2")
        if row.get("is_night_train") is True:
            night_routes += total
        elif row.get("is_night_train") is False:
            day_routes += total
        if avg_emissions is not None and total:
            weighted_co2_sum += float(avg_emissions) * float(total)
            weighted_count += float(total)

    night_pct_raw = (100.0 * night_routes / (night_routes + day_routes)) if (night_routes + day_routes) else 0
    avg_co2_train_raw = (weighted_co2_sum / weighted_count) if weighted_count else 0

    total_routes = fmt_number(total_routes_raw, 0)
    total_trips = fmt_number(total_train_raw + total_flight_raw, 0)
    completeness = fmt_pct(completeness_raw)
    night_pct = fmt_pct(night_pct_raw)
    avg_co2_train = fmt_co2(avg_co2_train_raw)
    countries = fmt_number(countries_raw, 0)

    cards = [
        kpi_card("Routes totales",    total_routes,  icon="⬡",
                 color=COLORS["night_blue"],
                 tooltip="Nombre de paires O/D dans gold_routes"),
        kpi_card("Trips enregistrés", total_trips,   icon="⬡",
                 color=COLORS["night_light"],
                 tooltip="Nombre de trips uniques tous modes confondus"),
        kpi_card("Pays couverts",     countries,     icon="◈",
                 color=COLORS["slate"],
                 tooltip="Pays de départ représentés dans les données"),
        kpi_card("Trains de nuit",    night_pct,     icon="◐",
                 color=COLORS["night_blue"],
                 tooltip="Part des trains de nuit dans l'offre ferroviaire"),
        kpi_card("CO₂ moy. train",   avg_co2_train, icon="♻",
                 color=COLORS["green_co2"],
                 tooltip="Émissions CO₂ moyennes (gCO₂eq/passager) — tous trains"),
        kpi_card("Complétude données", completeness, icon="◉",
                 color=COLORS["amber"],
                 tooltip="Taux moyen de complétude des colonnes gold_routes"),
    ]
    return kpi_row(cards)


@callback(Output("chart-operators", "figure"), Input("store-overview", "data"))
def render_operators(store):
    if not store:
        return empty_figure()

    ops_r = store.get("operators", {})
    if not ops_r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(ops_r)}")

    data = safe_data(ops_r)
    items = data.get("operators") or safe_items(ops_r)
    if not items:
        return empty_figure("Aucun opérateur disponible")

    # On borne l'échantillon pour garder un tri rapide même si l'API renvoie beaucoup d'entrées.
    df = pd.DataFrame(items[:20])
    if df.empty or "agency_name" not in df.columns:
        return empty_figure("Données opérateurs incomplètes")

    count_col = next((c for c in ["total_routes", "trip_count", "route_count", "count"] if c in df.columns), None)
    if not count_col:
        return empty_figure("Colonne count manquante")

    df = df.nlargest(15, count_col)
    return bar_chart_h(
        df, x=count_col, y="agency_name",
        title="Top 15 opérateurs",
        color=COLORS["night_blue"],
        x_label="Routes",
        y_label="",
        tooltip_cols=[c for c in ["countries_served", "dep_cities", "arr_cities", "night_routes"] if c in df.columns],
    )


@callback(Output("chart-emissions", "figure"), Input("store-overview", "data"))
def render_emissions(store):
    if not store:
        return empty_figure()

    emiss_r = store.get("emissions", {})
    if not emiss_r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(emiss_r)}")

    items = safe_items(emiss_r) or safe_data(emiss_r).get("buckets") or []
    if not items:
        return empty_figure("Aucune donnée d'émissions")

    df = pd.DataFrame(items)
    needed = {"mode", "distance_range_start", "distance_range_end"}
    if not needed.issubset(set(df.columns)):
        return empty_figure("Colonnes distance/mode manquantes")

    metric_col = next((c for c in ["avg_total_emissions", "avg_co2_per_pkm"] if c in df.columns), None)
    if not metric_col:
        return empty_figure("Colonne d'émissions manquante")

    df["distance_range_start"] = pd.to_numeric(df["distance_range_start"], errors="coerce")
    df["distance_range_end"] = pd.to_numeric(df["distance_range_end"], errors="coerce")
    df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
    df = df.dropna(subset=["distance_range_start", metric_col])
    if df.empty:
        return empty_figure("Données d'émissions invalides")

    pivot = (
        df.pivot_table(index="distance_range_start", columns="mode", values=metric_col, aggfunc="mean")
        .reset_index()
        .sort_values("distance_range_start")
    )

    y_cols = [c for c in ["train", "flight"] if c in pivot.columns]
    labels = ["Train" if c == "train" else "Vol" for c in y_cols]
    if not y_cols:
        return empty_figure("Modes train/flight absents")

    y_label = "gCO₂eq/pax" if metric_col == "avg_total_emissions" else "gCO₂eq/pkm"
    return scatter_line(
        pivot,
        x="distance_range_start",
        y_cols=y_cols,
        labels=labels,
        colors=[COLORS["amber"], COLORS["slate"]][: len(y_cols)],
        title="CO₂ moyen par tranche de distance",
        x_label="Distance min tranche (km)",
        y_label=y_label,
    )


@callback(Output("chart-day-night-donut", "figure"), Input("store-overview", "data"))
def render_donut(store):
    if not store:
        return empty_figure()

    dn_items = safe_items(store.get("day_night", {}))
    if not dn_items:
        return empty_figure("Données jour/nuit indisponibles")

    night = 0
    day = 0
    for row in dn_items:
        total = row.get("total_routes") or 0
        if row.get("is_night_train") is True:
            night += total
        elif row.get("is_night_train") is False:
            day += total

    if not (night or day):
        return empty_figure("Décompte jour/nuit absent")

    return pie_donut(
        labels=["Train de nuit", "Train de jour"],
        values=[night, day],
        colors=[COLORS["night_blue"], COLORS["amber"]],
        title="Répartition Jour / Nuit",
    )


@callback(Output("overview-quality-panel", "children"), Input("store-overview", "data"))
def render_quality_panel(store):
    if not store:
        return html.Div()

    quality_result = store.get("quality", {})
    q_raw = safe_data(quality_result)
    q = q_raw.get("data") if isinstance(q_raw, dict) and isinstance(q_raw.get("data"), dict) else q_raw

    if not q:
        return error_banner("Qualité indisponible")

    avg_completeness_vals = [
        q.get("distance_completeness_pct"),
        q.get("emissions_completeness_pct"),
        q.get("schedule_completeness_pct"),
        q.get("dep_city_completeness_pct"),
        q.get("arr_city_completeness_pct"),
    ]
    avg_completeness_vals = [float(v) for v in avg_completeness_vals if v is not None]
    avg_completeness = (sum(avg_completeness_vals) / len(avg_completeness_vals)) if avg_completeness_vals else None

    total_routes = q.get("total_routes") or 0
    emissions_pct = q.get("emissions_completeness_pct")
    routes_missing_co2 = None
    if emissions_pct is not None:
        routes_missing_co2 = int(round(float(total_routes) * (100.0 - float(emissions_pct)) / 100.0))

    items = []
    metrics = [
        ("Pays départ",             q.get("countries_departure"),           ""),
        ("Pays arrivée",            q.get("countries_arrival"),             ""),
        ("Complétude moyenne",      avg_completeness,                        "%"),
        ("Routes manquant CO₂",     routes_missing_co2,                      ""),
    ]
    for label, val, unit in metrics:
        if val is None:
            continue
        items.append(html.Div(
            [
                html.Span(label, className="kpi-label"),
                html.Span(
                    f"{val:.1f}{unit}" if isinstance(val, float) else str(val),
                    style={"fontWeight": "700", "color": "#1A2332", "fontSize": "16px"},
                ),
            ],
            style={"display": "flex", "justifyContent": "space-between",
                   "padding": "8px 0", "borderBottom": "1px solid #E3DED5"},
        ))

    return html.Div(
        [html.H3("Qualité globale", className="section-title"), html.Div(items)]
        if items else [error_banner("Métriques qualité non disponibles")]
    )
