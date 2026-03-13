"""
Composants d'état : erreurs, vide, chargement.
"""

from dash import html, dcc
from dashboard.utils.theme import COLORS


def error_banner(message: str, endpoint: str = "") -> html.Div:
    """Affiche un message d'échec lisible sans casser la structure de la page.

    L'endpoint est facultatif et sert surtout à accélérer le diagnostic en environnement
    de recette ou de debug.
    """
    detail = html.Span(f" [{endpoint}]", style={"opacity": "0.6"}) if endpoint else None
    return html.Div(
        [
            html.Span("⚠️ ", style={"marginRight": "6px"}),
            html.Span(message, style={"fontWeight": 500}),
            *([] if not detail else [detail]),
        ],
        className="error-banner",
    )


def empty_state(label: str = "Aucun résultat") -> html.Div:
    """Rend un état vide explicite pour éviter une zone blanche ambiguë."""
    return html.Div(
        [
            html.Div("○", style={"fontSize": "2rem", "marginBottom": "8px", "opacity": "0.3"}),
            html.Div(label, className="text-muted"),
        ],
        className="empty-state",
    )


def section_loader(children, keep_visible: bool = True) -> dcc.Loading:
    """Enveloppe children dans un dcc.Loading avec style ObRail.

    keep_visible=True  -> conserve l'ancien contenu visible pendant le fetch
    keep_visible=False -> comportement classique (contenu masqué pendant le fetch)
    """
    kwargs = {}
    if keep_visible:
        kwargs["overlay_style"] = {
            "visibility": "visible",
            "opacity": 0.35,
            "filter": "blur(0.4px)",
        }

    return dcc.Loading(
        children=children,
        type="dot",
        color=COLORS["amber"],
        style={"minHeight": "60px"},
        **kwargs,
    )
