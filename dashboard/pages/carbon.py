"""
Page Carbone — comparaison CO₂ train vs vol.

Sections :
  1. Estimateur O/D (inputs libres → GET /carbon/estimate)
  2. Classement corridors en économie CO₂ (GET /carbon/ranking)
  3. Facteurs d'émission par pays (GET /carbon/factors)

Appels parallèles à l'init : ranking + factors.
L'estimateur est déclenché à la demande (bouton).
"""

import dash
import pandas as pd
from dash import Input, Output, State, callback, dcc, html, no_update

from dashboard.components.charts import (
    bar_chart_h, bar_chart_v, country_heatmap, empty_figure, grouped_bar,
)
from dashboard.components.error import empty_state, error_banner, section_loader
from dashboard.components.filters import country_dropdown, filter_row, text_search
from dashboard.components.kpis import kpi_card, kpi_row
from dashboard.components.tables import obrail_table, table_wrapper
from dashboard.services.api_client import client, safe_data, safe_items, error_msg
from dashboard.utils.cache import TTL_24H, TTL_SHORT
from dashboard.utils.theme import COLORS, fmt_co2, fmt_co2_adaptive, fmt_pct, fmt_km

dash.register_page(__name__, path="/carbon", name="Carbone", order=2)

layout = html.Div(
    [
        dcc.Store(id="store-carbon-bg"),
        dcc.Store(id="store-carbon-estimate"),

        html.Div(
            [
                html.H1("Analyse Carbone", className="page-title"),
                html.P(
                    "Comparaison des émissions CO₂ train vs vol sur les corridors européens.",
                    className="page-subtitle",
                ),
            ],
            className="page-header",
        ),

        # L'estimateur reste manuel pour éviter des appels API à chaque frappe.
        html.Div(
            [
                html.H3("Estimation O/D", className="section-title"),
                filter_row(
                    [
                        html.Label("Origine"),
                        text_search("carbon-origin", "Paris, Wien…", debounce=False),
                        html.Label("Destination"),
                        text_search("carbon-destination", "Berlin, Roma…", debounce=False),
                        html.Div(
                            html.Button("Estimer", id="btn-carbon-estimate", className="btn-primary"),
                            style={"width": "100%", "display": "flex", "justifyContent": "center"},
                        ),
                    ]
                ),
                section_loader(html.Div(id="carbon-estimate-result")),
            ],
            className="card",
            style={"marginBottom": "20px"},
        ),

        html.Hr(className="section-separator"),

        # Les deux vues se complètent et sont chargées ensemble pour garder un temps d'attente homogène.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Classement économie CO₂", className="section-title"),
                        filter_row(
                            [
                                html.Label("Pays départ"),
                                country_dropdown("carbon-ranking-country"),
                            ]
                        ),
                        section_loader(dcc.Graph(id="chart-carbon-ranking", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Facteurs d'émission", className="section-title"),
                        filter_row(
                            [
                                html.Label("Pays"),
                                country_dropdown("carbon-factors-country"),
                            ]
                        ),
                        section_loader(dcc.Graph(id="chart-carbon-factors", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
            ],
            className="grid-2",
            style={"marginBottom": "20px"},
        ),

        # La table garde le détail consultable sans surcharger les graphiques.
        section_loader(html.Div(id="carbon-ranking-table")),
    ],
    className="page-container",
)


@callback(
    Output("store-carbon-bg", "data"),
    [
        Input("url", "pathname"),
        Input("carbon-ranking-country", "value"),
        Input("carbon-factors-country", "value"),
    ],
    prevent_initial_call=False,
)
def load_carbon_bg(pathname, ranking_country, factors_country):
    if pathname and pathname != "/carbon":
        return no_update

    results = client.parallel(
        [
            ("/carbon/ranking", {"departure_country": ranking_country,
                                  "sort_by": "co2_saving_pct", "page_size": 50}, TTL_24H),
            ("/carbon/factors", {"country": factors_country, "mode": "train"}, TTL_24H),
        ]
    )
    return {"ranking": results[0], "factors": results[1]}


@callback(
    Output("store-carbon-estimate", "data"),
    Input("btn-carbon-estimate", "n_clicks"),
    [State("carbon-origin", "value"), State("carbon-destination", "value")],
    prevent_initial_call=True,
)
def load_estimate(n_clicks, origin, destination):
    if not n_clicks or not origin or not destination:
        return no_update
    return client.get(
        "/carbon/estimate",
        params={"origin": origin, "destination": destination},
        ttl=TTL_SHORT,
    )


@callback(Output("carbon-estimate-result", "children"), Input("store-carbon-estimate", "data"))
def render_estimate(store):
    if not store:
        return html.P("Entrez une origine et une destination, puis cliquez sur Estimer.",
                      style={"color": COLORS["text_muted"]})
    if not store.get("ok"):
        return error_banner(error_msg(store), "/carbon/estimate")

    d = safe_data(store)
    best = d.get("best_mode", "")
    saving_pct = d.get("co2_saving_pct")
    corridors = d.get("nb_corridors", 0)

    # La différence absolue complète le pourcentage pour donner un gain tangible.
    avg_train = d.get("avg_train_emissions_co2") or 0
    avg_flight = d.get("avg_flight_emissions_co2") or 0
    saving_abs = avg_flight - avg_train if avg_flight > 0 and avg_train > 0 else 0

    mode_label  = "Train" if best == "train" else "Vol"
    saving_pct_text = f"−{saving_pct:.1f} %" if saving_pct else ""
    saving_abs_text = fmt_co2_adaptive(saving_abs) if saving_abs > 0 else ""

    return html.Div(
        [
            html.Div(
                [
                    kpi_card("Mode gagnant",     mode_label,  color=COLORS["green_co2"]),
                    kpi_card("Économie CO₂ %",   saving_pct_text, color=COLORS["green_co2"]),
                    kpi_card("Économie CO₂ g",   saving_abs_text, color=COLORS["green_co2"]),
                    kpi_card("Corridors matchés",str(corridors), color=COLORS["slate"]),
                    kpi_card("Train min", fmt_co2_adaptive(d.get("min_train_emissions_co2", 0)), color=COLORS["amber"]),
                    kpi_card("Train moy", fmt_co2_adaptive(d.get("avg_train_emissions_co2", 0)), color=COLORS["amber"]),
                    kpi_card("Vol min", fmt_co2_adaptive(d.get("min_flight_emissions_co2", 0)), color=COLORS["slate"]),
                    kpi_card("Vol moy", fmt_co2_adaptive(d.get("avg_flight_emissions_co2", 0)), color=COLORS["slate"]),
                ],
                className="kpi-row",
            ),
        ]
    )


@callback(Output("chart-carbon-ranking", "figure"), Input("store-carbon-bg", "data"))
def render_ranking_chart(store):
    if not store:
        return empty_figure()

    r = store.get("ranking", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("rankings") or []
    if not items:
        return empty_figure("Aucun corridor disponible")

    df = pd.DataFrame(items[:20])
    label_col = next((c for c in ["od_label", "corridor", "label"] if c in df.columns), None)
    if not label_col:
        # On reconstruit un libellé O/D si l'API n'en fournit pas directement.
        if {"departure_city", "arrival_city"}.issubset(df.columns):
            df["_label"] = df["departure_city"] + " → " + df["arrival_city"]
            label_col = "_label"
        else:
            return empty_figure("Colonnes O/D manquantes")

    value_col = next((c for c in ["co2_saving_pct", "saving_pct", "pct_saving"] if c in df.columns), None)
    if not value_col:
        return empty_figure("Colonne économie CO₂ manquante")

    return bar_chart_h(
        df.nlargest(15, value_col), x=value_col, y=label_col,
        title="Top 15 corridors — économie CO₂ train vs vol",
        color=COLORS["green_co2"],
        x_label="Économie (%)", y_label="",
    )


@callback(
    Output("chart-carbon-factors", "figure"), 
    Input("store-carbon-bg", "data"),
    State("carbon-factors-country", "value"),
)
def render_factors_chart(store, factors_country):
    if not store:
        return empty_figure()

    r = store.get("factors", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("factors") or []
    if not items:
        return empty_figure("Aucun facteur disponible")

    df = pd.DataFrame(items)
    
    # Le rendu change selon le niveau de filtre pour rester lisible (vue pays globale ou focus jour/nuit).
    if "country_code" in df.columns and "co2_per_pkm" in df.columns:
        if not factors_country:  # Sans filtre, on compare les pays entre eux.
            group_col = "country_code"
            df["_label"] = df["country_code"]
            agg_df = df.groupby("_label")["co2_per_pkm"].max().reset_index()
            agg_df = agg_df.sort_values("co2_per_pkm")
            title = "Facteurs d'émission ferroviaires par pays"
            y_label = "Pays"
        else:  # Avec filtre pays, on met en avant l'écart jour/nuit.
            df["_label"] = df["is_night_train"].apply(lambda x: "Nuit" if x else "Jour")
            agg_df = df.groupby("_label")["co2_per_pkm"].max().reset_index()
            agg_df = agg_df.sort_values("co2_per_pkm")
            title = f"Facteurs d'émission ferroviaires — {factors_country}"
            y_label = ""
        
        return bar_chart_h(
            agg_df, x="co2_per_pkm", y="_label",
            title=title,
            color=COLORS["amber"],
            x_label="gCO₂eq/pkm", y_label=y_label,
        )
    else:
        return empty_figure("Structure de données facteurs non reconnue")


@callback(Output("carbon-ranking-table", "children"), Input("store-carbon-bg", "data"))
def render_ranking_table(store):
    if not store:
        return html.Div()

    r = store.get("ranking", {})
    if not r.get("ok"):
        return error_banner(error_msg(r), "/carbon/ranking")

    items = safe_items(r) or safe_data(r).get("rankings") or []
    if not items:
        return empty_state("Aucun corridor dans ce classement.")

    df = pd.DataFrame(items)
    cols = [{"name": c.replace("_", " ").capitalize(), "id": c} for c in df.columns[:10]]
    rows = df.to_dict("records")
    table = obrail_table("carbon-table", cols, rows, page_size=25, server_side=False)
    total  = safe_data(r).get("total", len(items))
    return table_wrapper(table, title=f"{total} corridors classés")
