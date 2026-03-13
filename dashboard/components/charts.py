"""
Helpers graphiques ObRail.

Chaque fonction retourne une figure Plotly prête à l'affichage.
Le template obrail est appliqué globalement (utils/theme.py).

Conventions :
  - bar_chart_h  : barres horizontales (classements, top-N)
  - bar_chart_v  : barres verticales (distributions, histogrammes)
  - grouped_bar  : barres groupées (jour vs nuit, train vs vol)
  - scatter_line : courbe temporelle ou XY
  - pie_donut    : part relative (mode, pays)
  - gauge        : complétude, taux couverture
"""

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.utils.theme import COLORS, COLOR_SEQUENCE, PLOTLY_TEMPLATE, mode_color

_ISO2_TO_ISO3 = {
    "AT": "AUT", "BE": "BEL", "CZ": "CZE", "DE": "DEU", "DK": "DNK",
    "EE": "EST", "ES": "ESP", "FI": "FIN", "FR": "FRA", "GB": "GBR",
    "GR": "GRC", "HR": "HRV", "HU": "HUN", "IE": "IRL", "IT": "ITA",
    "LT": "LTU", "LU": "LUX", "NL": "NLD", "NO": "NOR", "PL": "POL",
    "PT": "PRT", "RO": "ROU", "RS": "SRB", "SE": "SWE", "SI": "SVN",
    "SK": "SVK",
}


# Cette variante est privilégiée pour les classements, car les libellés longs restent lisibles.
def bar_chart_h(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str = "",
    color: str = None,
    color_col: str = None,
    x_label: str = "",
    y_label: str = "",
    tooltip_cols: list = None,
) -> go.Figure:
    """Barres horizontales triées. Idéal pour classements top-N."""
    kw = {}
    if color_col:
        kw["color"] = color_col
    elif color:
        kw["color_discrete_sequence"] = [color]
    else:
        kw["color_discrete_sequence"] = [COLORS["slate"]]

    fig = px.bar(
        df, x=x, y=y, orientation="h",
        title=title, template=PLOTLY_TEMPLATE,
        labels={x: x_label, y: y_label},
        hover_data=tooltip_cols or [],
        **kw,
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    return fig


# La version verticale est utile quand l'axe X représente une progression naturelle.
def bar_chart_v(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str = "",
    color: str = None,
    x_label: str = "",
    y_label: str = "",
    tooltip_cols: list = None,
) -> go.Figure:
    fig = px.bar(
        df, x=x, y=y,
        title=title, template=PLOTLY_TEMPLATE,
        labels={x: x_label, y: y_label},
        color_discrete_sequence=[color or COLORS["amber"]],
        hover_data=tooltip_cols or [],
    )
    return fig


# Les séries sont tracées explicitement pour garder la main sur l'ordre et les couleurs métier.
def grouped_bar(
    df: pd.DataFrame,
    x: str,
    y_cols: list[str],
    labels: list[str],
    colors: list[str] = None,
    title: str = "",
    barmode: str = "group",
    x_label: str = "",
    y_label: str = "",
) -> go.Figure:
    """Barres groupées (ou empilées si barmode='stack') pour comparaisons."""
    palette = colors or COLOR_SEQUENCE
    fig = go.Figure()
    for col, lbl, c in zip(y_cols, labels, palette):
        if col not in df.columns:
            continue
        fig.add_trace(go.Bar(
            name=lbl,
            x=df[x],
            y=df[col],
            marker_color=c,
            hovertemplate=f"<b>%{{x}}</b><br>{lbl}: %{{y:,.1f}}<extra></extra>",
        ))
    fig.update_layout(
        title=title,
        barmode=barmode,
        template=PLOTLY_TEMPLATE,
        xaxis_title=x_label,
        yaxis_title=y_label,
    )
    return fig


# Courbe multi-séries pensée pour les comparaisons de tendance plutôt que de volume brut.
def scatter_line(
    df: pd.DataFrame,
    x: str,
    y_cols: list[str],
    labels: list[str],
    colors: list[str] = None,
    title: str = "",
    x_label: str = "",
    y_label: str = "",
    markers: bool = True,
) -> go.Figure:
    palette = colors or COLOR_SEQUENCE
    fig = go.Figure()
    mode = "lines+markers" if markers else "lines"
    for col, lbl, c in zip(y_cols, labels, palette):
        if col not in df.columns:
            continue
        fig.add_trace(go.Scatter(
            name=lbl, x=df[x], y=df[col],
            mode=mode, line=dict(color=c, width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>%{{x}}</b><br>{lbl}: %{{y:,.1f}}<extra></extra>",
        ))
    fig.update_layout(
        title=title, template=PLOTLY_TEMPLATE,
        xaxis_title=x_label, yaxis_title=y_label,
    )
    return fig


# Le donut facilite la lecture des parts relatives quand il y a peu de catégories.
def pie_donut(
    labels: list,
    values: list,
    title: str = "",
    colors: list = None,
    hole: float = 0.55,
) -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=hole,
        marker=dict(colors=colors or COLOR_SEQUENCE),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} (%{percent})<extra></extra>",
    ))
    fig.update_layout(title=title, template=PLOTLY_TEMPLATE, showlegend=True)
    return fig


# La jauge sert surtout à situer rapidement un score par rapport à un seuil attendu.
def gauge(
    value: float,
    title: str = "",
    max_val: float = 100.0,
    unit: str = "%",
    color: str = None,
) -> go.Figure:
    """Jauge circulaire pour KPI type taux de complétude."""
    c = color or COLORS["green_co2"]
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={"suffix": unit, "font": {"size": 28, "color": COLORS["text_main"]}},
        title={"text": title, "font": {"size": 13, "color": COLORS["text_main"]}},
        gauge={
            "axis": {"range": [0, max_val], "tickcolor": COLORS["text_muted"]},
            "bar": {"color": c},
            "bgcolor": COLORS["border_light"],
            "bordercolor": COLORS["border"],
            "threshold": {
                "line": {"color": COLORS["amber"], "width": 2},
                "thickness": 0.85,
                "value": max_val * 0.8,
            },
        },
    ))
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=220,
        margin=dict(l=24, r=24, t=48, b=16),
    )
    return fig


# On convertit ISO-2 vers ISO-3 pour éviter les trous de rendu dans la carte Plotly.
def country_heatmap(
    df: pd.DataFrame,
    country_col: str,
    value_col: str,
    title: str = "",
    colorscale: str = "Blues",
) -> go.Figure:
    """Carte choroplèthe Europe pour les métriques par pays."""
    plot_df = df.copy()
    country_values = plot_df[country_col].astype(str).str.strip().str.upper()

    if country_values.str.len().eq(2).all():
        plot_df["_country_loc"] = country_values.map(_ISO2_TO_ISO3)
    else:
        plot_df["_country_loc"] = country_values

    plot_df = plot_df.dropna(subset=["_country_loc"])

    fig = px.choropleth(
        plot_df,
        locations="_country_loc",
        locationmode="ISO-3",
        color=value_col,
        title=title,
        template=PLOTLY_TEMPLATE,
        color_continuous_scale=colorscale,
        scope="europe",
        hover_data=[country_col, value_col],
    )
    fig.update_layout(
        geo=dict(
            bgcolor=COLORS["card_bg"],
            lakecolor=COLORS["beige_bg"],
            landcolor=COLORS["border_light"],
            showframe=False,
            showcoastlines=True,
            coastlinecolor=COLORS["border"],
        ),
        coloraxis_colorbar=dict(
            tickfont=dict(color=COLORS["text_main"]),
            title=dict(font=dict(color=COLORS["text_main"])),
        ),
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


# Figure de secours commune pour garder une mise en page stable en cas de vide/erreur.
def empty_figure(message: str = "Aucune donnée") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        annotations=[{
            "text": message, "showarrow": False,
            "font": {"size": 15, "color": COLORS["text_muted"]},
            "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5,
        }],
        xaxis={"visible": False},
        yaxis={"visible": False},
    )
    return fig
