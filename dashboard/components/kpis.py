"""
Composants KPI cards réutilisables.

Hiérarchie visuelle : valeur principale > label > delta optionnel.
"""

from dash import html
from dashboard.utils.theme import COLORS


def kpi_card(
    label: str,
    value: str,
    delta: str = None,
    delta_positive: bool = True,
    icon: str = None,
    color: str = None,
    tooltip: str = None,
) -> html.Div:
    """
    Carte KPI autonome.

    Args:
        label:          Titre de la métrique
        value:          Valeur principale formatée (ex. "3 124", "97.8 %")
        delta:          Variation optionnelle (ex. "+12 vs dernier ETL")
        delta_positive: True → vert, False → rouge
        icon:           Emoji ou caractère Unicode affiché en haut à gauche
        color:          Couleur de la barre d'accent (hex). Défaut : amber
        tooltip:        Texte d'info-bulle (title HTML)
    """
    accent = color or COLORS["amber"]
    delta_color = COLORS["green_co2"] if delta_positive else COLORS["danger"]

    delta_el = (
        html.Span(delta, className="kpi-delta", style={"color": delta_color})
        if delta
        else None
    )

    icon_el = html.Span(icon, className="kpi-icon") if icon else None

    children = [c for c in [icon_el, html.Div(label, className="kpi-label")] if c]
    value_row = [html.Div(value, className="kpi-value")]
    if delta_el:
        value_row.append(delta_el)

    props: dict = {"className": "kpi-card", "style": {"borderTopColor": accent}}
    if tooltip:
        props["title"] = tooltip

    return html.Div(
        [html.Div(children, className="kpi-header"), html.Div(value_row, className="kpi-body")],
        **props,
    )


def kpi_row(cards: list) -> html.Div:
    """Aligne les cartes KPI sur une grille unique pour garder une lecture homogène."""
    return html.Div(cards, className="kpi-row")


def loading_kpi() -> html.Div:
    """Affiche un placeholder visuel pour éviter les sauts de mise en page au chargement."""
    return html.Div([html.Div(className="skeleton skeleton-kpi")], className="kpi-card")
