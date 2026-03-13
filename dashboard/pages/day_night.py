"""
Page Analyse Jour / Nuit.

Appels parallèles à l'init :
  - GET /analysis/day-night/summary
  - GET /analysis/day-night/coverage
  - GET /analysis/day-night/emissions

Appels à la demande (filtres) :
  - GET /analysis/day-night/compare (paire O/D)

Les tables détaillées sont exposées dans la page Data.
"""

import dash
import pandas as pd
from dash import Input, Output, State, callback, dcc, html, no_update

from dashboard.components.charts import (
    bar_chart_h, grouped_bar, empty_figure, scatter_line,
)
from dashboard.components.error import empty_state, error_banner, section_loader
from dashboard.components.filters import filter_row, text_search
from dashboard.components.kpis import kpi_card, kpi_row
from dashboard.services.api_client import client, safe_data, safe_items, error_msg
from dashboard.utils.cache import TTL_24H, TTL_SHORT
from dashboard.utils.theme import COLORS, fmt_co2, fmt_number, fmt_pct

dash.register_page(__name__, path="/day-night", name="Jour / Nuit", order=3)

layout = html.Div(
    [
        dcc.Store(id="store-dn-bg"),
        dcc.Store(id="store-dn-compare"),

        html.Div(
            [
                html.H1("Analyse Jour / Nuit", className="page-title"),
                html.P("Distribution de l'offre ferroviaire entre trains de jour et trains de nuit.",
                       className="page-subtitle"),
            ],
            className="page-header",
        ),

        # On affiche d'abord les indicateurs pour cadrer l'analyse avant les graphiques détaillés.
        section_loader(html.Div(id="dn-kpis")),

        html.Hr(className="section-separator"),

        # Les deux graphiques résument l'écart jour/nuit à l'échelle pays.
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Couverture par pays", className="section-title"),
                        section_loader(dcc.Graph(id="chart-dn-coverage", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Émissions CO₂ Jour vs Nuit", className="section-title"),
                        section_loader(dcc.Graph(id="chart-dn-emissions", config={"displayModeBar": False})),
                    ],
                    className="card",
                ),
            ],
            className="grid-2",
            style={"marginBottom": "20px"},
        ),

        html.Hr(className="section-separator"),

        # La comparaison O/D reste volontairement explicite car c'est une action utilisateur ciblée.
        html.Div(
            [
                html.H3("Comparaison Jour / Nuit sur une paire O/D", className="section-title"),
                filter_row(
                    [
                        html.Label("Origine"),
                        text_search("dn-compare-origin", "Paris, Wien…", debounce=False),
                        html.Label("Destination"),
                        text_search("dn-compare-dest", "Berlin, Roma…", debounce=False),
                        html.Div(
                            html.Button("Comparer", id="btn-dn-compare", className="btn-primary"),
                            style={"width": "100%", "display": "flex", "justifyContent": "center"},
                        ),
                    ]
                ),
                section_loader(html.Div(id="dn-compare-result")),
            ],
            className="card",
            style={"marginBottom": "20px"},
        ),
    ],
    className="page-container",
)


@callback(
    Output("store-dn-bg", "data"),
    Input("url", "pathname"),
    prevent_initial_call=False,
)
def load_dn_bg(pathname):
    if pathname and pathname != "/day-night":
        return no_update

    res = client.parallel(
        [
            ("/analysis/day-night/summary",   None, TTL_24H),
            ("/analysis/day-night/coverage",  None, TTL_24H),
            ("/analysis/day-night/emissions", None, TTL_24H),
        ]
    )
    return {"summary": res[0], "coverage": res[1], "emissions": res[2]}


@callback(Output("dn-kpis", "children"), Input("store-dn-bg", "data"))
def render_dn_kpis(store):
    if not store:
        return html.Div()

    result = store.get("summary", {})
    if not result.get("ok"):
        return error_banner(error_msg(result), "/analysis/day-night/summary")

    items = safe_items(result) or []
    if not items:
        return empty_state("Résumé jour/nuit indisponible.")

    night = next((row for row in items if row.get("is_night_train") is True), {})
    day = next((row for row in items if row.get("is_night_train") is False), {})

    cards = [
        kpi_card("Trains de nuit",  fmt_number(night.get("total_routes")),
                 color=COLORS["night_blue"], icon="◐",
                 tooltip="Nombre de routes classifiées trains de nuit"),
        kpi_card("Trains de jour",  fmt_number(day.get("total_routes")),
                 color=COLORS["amber"], icon="☀",
                 tooltip="Nombre de routes classifiées trains de jour"),
        kpi_card("Pays — trains de nuit", fmt_number(night.get("countries_served")),
                 color=COLORS["slate"], icon="◈",
                 tooltip="Pays desservis par au moins un train de nuit"),
        kpi_card("CO₂ moy. nuit",   fmt_co2(night.get("avg_emissions_co2")),
                 color=COLORS["night_light"], icon="♻",
                 tooltip="Émissions moyennes des trains de nuit"),
        kpi_card("CO₂ moy. jour",   fmt_co2(day.get("avg_emissions_co2")),
                 color=COLORS["amber"], icon="♻",
                 tooltip="Émissions moyennes des trains de jour"),
    ]
    return kpi_row(cards)


@callback(Output("chart-dn-coverage", "figure"), Input("store-dn-bg", "data"))
def render_dn_coverage(store):
    if not store:
        return empty_figure()

    r = store.get("coverage", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("coverage") or []
    if not items:
        return empty_figure("Aucune couverture disponible")

    df = pd.DataFrame(items)
    if not {"departure_country", "is_night_train", "nb_routes"}.issubset(df.columns):
        return empty_figure("Structure couverture non reconnue")

    pivot = (
        df.pivot_table(
            index="departure_country",
            columns="is_night_train",
            values="nb_routes",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename(columns={False: "day_routes", True: "night_routes"})
    )

    if "night_routes" not in pivot.columns:
        pivot["night_routes"] = 0
    if "day_routes" not in pivot.columns:
        pivot["day_routes"] = 0

    pivot["_total"] = pivot["day_routes"] + pivot["night_routes"]
    pivot = pivot.nlargest(20, "_total")

    return grouped_bar(
        pivot,
        x="departure_country",
        y_cols=["night_routes", "day_routes"],
        labels=["Nuit", "Jour"],
        colors=[COLORS["night_blue"], COLORS["amber"]],
        title="Trains jour vs nuit — top 20 pays",
        barmode="stack", x_label="Pays", y_label="Nombre de routes",
    )


@callback(Output("chart-dn-emissions", "figure"), Input("store-dn-bg", "data"))
def render_dn_emissions(store):
    if not store:
        return empty_figure()

    r = store.get("emissions", {})
    if not r.get("ok"):
        return empty_figure(f"Erreur : {error_msg(r)}")

    items = safe_items(r) or safe_data(r).get("emissions") or []
    if not items:
        return empty_figure("Aucune donnée d'émissions")

    df = pd.DataFrame(items)
    x_col = next((c for c in ["departure_country", "country"] if c in df.columns), None)
    night = next((c for c in ["avg_total_co2_night", "avg_co2_pkm_night"] if c in df.columns), None)
    day = next((c for c in ["avg_total_co2_day", "avg_co2_pkm_day"] if c in df.columns), None)

    if not (x_col and (night or day)):
        return empty_figure("Structure émissions non reconnue")

    y_cols = [c for c in [night, day] if c]
    labels = [l for c, l in [(night, "Nuit"), (day, "Jour")] if c]
    colors = [COLORS["night_blue"], COLORS["amber"]][: len(y_cols)]

    return grouped_bar(
        df, x=x_col, y_cols=y_cols, labels=labels, colors=colors,
        title="CO₂ moyen Jour vs Nuit par pays",
        barmode="group", x_label="Pays", y_label="gCO₂eq/trajet",
    )


@callback(
    Output("store-dn-compare", "data"),
    Input("btn-dn-compare", "n_clicks"),
    [State("dn-compare-origin", "value"), State("dn-compare-dest", "value")],
    prevent_initial_call=True,
)
def load_dn_compare(n_clicks, origin, dest):
    if not n_clicks or not origin or not dest:
        return no_update
    return client.get(
        "/analysis/day-night/compare",
        params={"origin": origin, "destination": dest},
        ttl=TTL_SHORT,
    )


@callback(Output("dn-compare-result", "children"), Input("store-dn-compare", "data"))
def render_dn_compare(store):
    if not store:
        return html.P("Entrez une paire et cliquez sur Comparer.",
                      style={"color": COLORS["text_muted"]})
    if not store.get("ok"):
        return error_banner(error_msg(store), "/analysis/day-night/compare")

    d = safe_data(store)
    if not d:
        return empty_state("Aucun résultat pour cette paire O/D.")

    cards = []
    for key, label, color in [
        ("night_train", "Train de nuit", COLORS["night_blue"]),
        ("day_train",   "Train de jour", COLORS["amber"]),
    ]:
        seg = d.get(key) or {}
        nb_options = seg.get("nb_options", 0) if seg else 0
        avg_emissions = seg.get("avg_emissions_co2") if seg else None
        delta_text = f"CO₂ moy. {fmt_co2(avg_emissions)}" if avg_emissions is not None else "Aucune option"

        cards.append(kpi_card(
            label=label,
            value=f"{nb_options} option" + ("s" if nb_options != 1 else ""),
            delta=delta_text,
            delta_positive=True,
            color=color,
        ))

    return html.Div(cards, className="kpi-row") if cards else empty_state("Données O/D insuffisantes")


