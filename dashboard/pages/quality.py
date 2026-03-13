"""
Page Qualité des données.

Appels parallèles :
  - GET /quality/summary
  - GET /quality/completeness
  - GET /quality/completeness/by-country
  - GET /quality/coverage/countries
  - GET /quality/coverage/cities
  - GET /quality/schedules
  - GET /quality/compare-coverage
  - GET /quality/day-night-balance
"""

import dash
import pandas as pd
from dash import Input, Output, callback, dcc, html, no_update

from dashboard.components.charts import (
    bar_chart_h, bar_chart_v, country_heatmap, empty_figure, gauge, grouped_bar,
)
from dashboard.components.error import empty_state, error_banner, section_loader
from dashboard.components.kpis import kpi_card, kpi_row
from dashboard.components.tables import obrail_table
from dashboard.services.api_client import client, safe_data, safe_items, error_msg
from dashboard.utils.cache import TTL_24H
from dashboard.utils.theme import COLORS, fmt_pct, fmt_number

dash.register_page(__name__, path="/quality", name="Qualité", order=4)

layout = html.Div(
    [
        dcc.Store(id="store-quality"),

        html.Div(
            [
                html.H1("Qualité des données", className="page-title"),
                html.P("Indicateurs de complétude, couverture et cohérence de gold_routes.",
                       className="page-subtitle"),
            ],
            className="page-header",
        ),

        # Les KPI donnent le contexte global avant d'entrer dans les diagnostics détaillés.
        section_loader(html.Div(id="quality-kpis")),
        html.Hr(className="section-separator"),

        # On juxtapose vue macro et vue colonne pour relier tendance globale et causes concrètes.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Complétude globale", className="section-title"),
                        section_loader(dcc.Graph(id="chart-quality-gauge", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Complétude par colonne", className="section-title"),
                        section_loader(dcc.Graph(id="chart-completeness-cols", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
            ],
            className="grid-2",
            style={"marginBottom": "20px"},
        ),

        # Cette ligne combine lecture géographique et concentration urbaine.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Couverture pays", className="section-title"),
                        section_loader(dcc.Graph(id="chart-quality-countries", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Top villes desservies", className="section-title"),
                        section_loader(dcc.Graph(id="chart-quality-cities", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
            ],
            className="grid-2",
            style={"marginBottom": "20px"},
        ),

        # La table complète les graphiques quand il faut inspecter les valeurs exactes.
        html.Div(
            [
                html.H3("Complétude par pays", className="section-title"),
                section_loader(html.Div(id="quality-country-table")),
            ],
            className="card",
            style={"marginBottom": "20px"},
        ),

        # Les deux graphiques finaux vérifient la cohérence opérationnelle des services.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Couverture jours de service", className="section-title"),
                        section_loader(dcc.Graph(id="chart-quality-schedules", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Balance Jour / Nuit", className="section-title"),
                        section_loader(dcc.Graph(id="chart-quality-balance", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
            ],
            className="grid-2",
        ),
    ],
    className="page-container",
)


@callback(
    Output("store-quality", "data"),
    Input("url", "pathname"),
    prevent_initial_call=False,
)
def load_quality(pathname):
    if pathname and pathname != "/quality":
        return no_update

    res = client.parallel(
        [
            ("/quality/summary",              None, TTL_24H),
            ("/quality/completeness",         None, TTL_24H),
            ("/quality/completeness/by-country", None, TTL_24H),
            ("/quality/coverage/countries",   None, TTL_24H),
            ("/quality/coverage/cities",      {"top_n": 30}, TTL_24H),
            ("/quality/schedules",            None, TTL_24H),
            ("/quality/compare-coverage",     None, TTL_24H),
            ("/quality/day-night-balance",    None, TTL_24H),
        ]
    )
    keys = ["summary", "completeness", "completeness_by_country",
            "countries", "cities", "schedules", "compare_coverage", "balance"]
    return dict(zip(keys, res))


@callback(Output("quality-kpis", "children"), Input("store-quality", "data"))
def render_quality_kpis(store):
    if not store:
        return html.Div()

    summary_result = store.get("summary", {})
    completeness_result = store.get("completeness", {})
    compare_result = store.get("compare_coverage", {})

    if not summary_result.get("ok"):
        return error_banner(error_msg(summary_result), "/quality/summary")

    s_raw = safe_data(summary_result)
    s = s_raw.get("data") if isinstance(s_raw.get("data"), dict) else s_raw

    c_raw = safe_data(completeness_result) if completeness_result.get("ok") else {}
    c = c_raw.get("data") if isinstance(c_raw.get("data"), dict) else c_raw

    pct_values = [
        float(v) for k, v in (c or {}).items()
        if isinstance(k, str) and k.endswith("_pct") and k != "total_rows" and v is not None
    ]
    avg_completeness = round(sum(pct_values) / len(pct_values), 2) if pct_values else None

    total_routes = s.get("total_routes") or 0
    emissions_pct = c.get("emissions_co2_pct") if isinstance(c, dict) else None
    routes_missing_co2 = None
    if emissions_pct is not None and total_routes:
        routes_missing_co2 = round(float(total_routes) * (100 - float(emissions_pct)) / 100)

    compare_items = safe_items(compare_result) if compare_result.get("ok") else []
    total_train = sum((row.get("total_train_trips") or 0) for row in compare_items)
    total_compared = sum((row.get("compared_trips") or 0) for row in compare_items)
    coverage_pct = (100.0 * total_compared / total_train) if total_train else None

    cards = [
        kpi_card("Routes totales",      fmt_number(total_routes),
                 color=COLORS["night_blue"], icon="⬡"),
        kpi_card("Complétude moy.",     fmt_pct(avg_completeness),
                 color=COLORS["green_co2"], icon="◉",
                 tooltip="Taux moyen de valeurs non-nulles sur gold_routes"),
        kpi_card("Pays couverts",       fmt_number(s.get("countries_departure")),
                 color=COLORS["slate"], icon="◈"),
        kpi_card("Routes sans CO₂",     fmt_number(routes_missing_co2),
                 color=COLORS["danger"], icon="♻",
                 delta_positive=False,
                 tooltip="Routes sans valeur CO₂ calculée"),
        kpi_card("Corridors avec vol",  fmt_pct(coverage_pct),
                 color=COLORS["amber"], icon="✈",
                 tooltip="% corridors ferroviaires ayant un vol équivalent trouvé"),
    ]
    return kpi_row(cards)


@callback(Output("chart-quality-gauge", "figure"), Input("store-quality", "data"))
def render_gauge(store):
    if not store:
        return empty_figure()

    r = store.get("completeness", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    d = safe_data(r)
    payload = d.get("data") if isinstance(d, dict) and isinstance(d.get("data"), dict) else d
    pct_values = [
        float(v) for k, v in (payload or {}).items()
        if isinstance(k, str) and k.endswith("_pct") and k != "total_rows" and v is not None
    ]
    v = round(sum(pct_values) / len(pct_values), 2) if pct_values else 0

    color = COLORS["green_co2"] if v >= 80 else (COLORS["amber"] if v >= 50 else COLORS["danger"])
    return gauge(float(v), title="Complétude globale", color=color)


@callback(Output("chart-completeness-cols", "figure"), Input("store-quality", "data"))
def render_completeness_cols(store):
    if not store:
        return empty_figure()

    r = store.get("completeness", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    d = safe_data(r)
    payload = d.get("data") if isinstance(d, dict) and isinstance(d.get("data"), (dict, list)) else d

    # L'endpoint peut changer de forme selon la version API; on accepte dict ou liste pour rester robuste.
    if isinstance(payload, dict):
        items = [
            {"column": k, "pct": v}
            for k, v in payload.items()
            if isinstance(k, str) and k.endswith("_pct")
        ]
    else:
        items = payload or safe_items(r) or []

    if not items:
        return empty_figure("Données de complétude absentes")

    df = pd.DataFrame(items)
    col_col = next((c for c in ["column", "column_name", "col"] if c in df.columns), None)
    pct_col = next((c for c in ["pct", "completeness_pct", "value"] if c in df.columns), None)

    if not (col_col and pct_col):
        return empty_figure("Structure complétude non reconnue")

    df[pct_col] = pd.to_numeric(df[pct_col], errors="coerce")
    df = df.dropna(subset=[pct_col])
    if df.empty:
        return empty_figure("Données de complétude non numériques")

    # On retire le suffixe technique pour éviter de polluer la lecture métier.
    if col_col == "column":
        df[col_col] = df[col_col].astype(str).str.replace("_pct", "", regex=False)

    df = df.sort_values(pct_col)
    color = COLORS["green_co2"]

    return bar_chart_h(
        df, x=pct_col, y=col_col,
        title="Complétude par colonne (%)",
        color=color, x_label="%", y_label="",
    )


@callback(Output("chart-quality-countries", "figure"), Input("store-quality", "data"))
def render_coverage_countries(store):
    if not store:
        return empty_figure()

    r = store.get("countries", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("countries") or []
    if not items:
        return empty_figure("Aucune couverture pays")

    df = pd.DataFrame(items)
    country_col = next((c for c in ["departure_country", "country_code", "country"] if c in df.columns), None)
    value_col   = next((c for c in ["total_routes", "route_count", "count", "routes"] if c in df.columns), None)

    if not (country_col and value_col):
        return empty_figure("Colonnes pays/count manquantes")

    return country_heatmap(df, country_col, value_col, "Nombre de routes par pays")


@callback(Output("chart-quality-cities", "figure"), Input("store-quality", "data"))
def render_coverage_cities(store):
    if not store:
        return empty_figure()

    r = store.get("cities", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("cities") or []
    if not items:
        return empty_figure("Aucune donnée villes")

    df = pd.DataFrame(items[:20])
    city_col  = next((c for c in ["city", "departure_city", "city_name"] if c in df.columns), None)
    count_col = next((c for c in ["total_routes", "count", "route_count", "total"] if c in df.columns), None)

    if not (city_col and count_col):
        return empty_figure("Colonnes villes/count manquantes")

    return bar_chart_h(
        df.nlargest(15, count_col), x=count_col, y=city_col,
        title="Top 15 villes (départs + arrivées)",
        color=COLORS["night_light"], x_label="Routes", y_label="",
    )


@callback(Output("quality-country-table", "children"), Input("store-quality", "data"))
def render_completeness_countries(store):
    if not store:
        return html.Div()

    r = store.get("completeness_by_country", {})
    if not r.get("ok"):
        return error_banner(error_msg(r), "/quality/completeness/by-country")

    items = safe_items(r) or safe_data(r).get("countries") or []
    if not items:
        return empty_state("Aucune donnée par pays.")

    df   = pd.DataFrame(items)
    cols = [{"name": c.replace("_", " ").capitalize(), "id": c} for c in df.columns[:8]]
    rows = df.to_dict("records")
    return obrail_table("quality-country-tb", cols, rows, page_size=25, server_side=False)


@callback(Output("chart-quality-schedules", "figure"), Input("store-quality", "data"))
def render_schedules(store):
    if not store:
        return empty_figure()

    r = store.get("schedules", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("data") or []
    if not items:
        return empty_figure("Format horaires non reconnu")

    df = pd.DataFrame(items)
    if not {"departure_country", "weekday_only", "weekend_only", "daily"}.issubset(df.columns):
        return empty_figure("Format horaires non reconnu")

    df["_total"] = (
        pd.to_numeric(df["weekday_only"], errors="coerce").fillna(0)
        + pd.to_numeric(df["weekend_only"], errors="coerce").fillna(0)
        + pd.to_numeric(df["daily"], errors="coerce").fillna(0)
    )
    df = df.nlargest(15, "_total")

    return grouped_bar(
        df,
        x="departure_country",
        y_cols=["weekday_only", "weekend_only", "daily"],
        labels=["Semaine", "Week-end", "Quotidien"],
        colors=[COLORS["amber"], COLORS["night_light"], COLORS["night_blue"]],
        title="Couverture jours de service (top 15 pays)",
        barmode="stack",
        x_label="Pays",
        y_label="Routes",
    )


@callback(Output("chart-quality-balance", "figure"), Input("store-quality", "data"))
def render_balance(store):
    if not store:
        return empty_figure()

    r = store.get("balance", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("balance") or []
    if not items:
        return empty_figure("Aucune donnée balance")

    df = pd.DataFrame(items)
    if not {"departure_country", "night_count", "day_count"}.issubset(df.columns):
        return empty_figure("Colonnes balance manquantes")

    # La conversion défensive évite que des valeurs manquantes cassent l'agrégation.
    df["night_count"] = pd.to_numeric(df["night_count"], errors="coerce").fillna(0)
    df["day_count"] = pd.to_numeric(df["day_count"], errors="coerce").fillna(0)

    # On compte départ et arrivée pour refléter la présence réelle d'un pays dans les flux.
    df["departure_country"] = df["departure_country"].astype(str).str.strip().str.upper()
    if "arrival_country" in df.columns:
        df["arrival_country"] = df["arrival_country"].astype(str).str.strip().str.upper()
    else:
        df["arrival_country"] = ""

    dep_side = df[["departure_country", "night_count", "day_count"]].rename(
        columns={"departure_country": "country"}
    )
    arr_side = df[["arrival_country", "night_count", "day_count"]].rename(
        columns={"arrival_country": "country"}
    )
    stacked = pd.concat([dep_side, arr_side], ignore_index=True)
    stacked = stacked[stacked["country"] != ""]

    if stacked.empty:
        return empty_figure("Aucune donnée pays exploitable")

    agg = (
        stacked.groupby("country", as_index=False)[["night_count", "day_count"]]
        .sum()
    )
    agg["_total"] = agg["night_count"] + agg["day_count"]
    target = agg.nlargest(20, "_total")

    return grouped_bar(
        target,
        x="country",
        y_cols=["night_count", "day_count"],
        labels=["Nuit", "Jour"],
        colors=[COLORS["night_blue"], COLORS["amber"]],
        title="Balance Jour / Nuit par pays", barmode="stack",
        y_label="Routes",
    )
