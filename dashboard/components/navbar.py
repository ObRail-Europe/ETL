"""
Barre de navigation principale ObRail.

Pattern Dash multi-pages : chaque page déclare dash.register_page()
et la sidebar pointe sur les paths correspondants.
"""

from dash import html, dcc

# Définition centralisée pour garder l'ordre de navigation stable entre releases.
NAV_ITEMS = [
    {"icon": "◈", "label": "Vue d'ensemble",     "path": "/"},
    {"icon": "⬡", "label": "Routes",              "path": "/routes"},
    {"icon": "♻", "label": "Carbone",             "path": "/carbon"},
    {"icon": "◐", "label": "Jour / Nuit",         "path": "/day-night"},
    {"icon": "◉", "label": "Qualité",             "path": "/quality"},
    {"icon": "⊞", "label": "Référentiel",         "path": "/referential"},
    {"icon": "▤", "label": "Data",                "path": "/data"},
]


def sidebar() -> html.Div:
    """
    Barre latérale permanente.

    Génère des dcc.Link (client-side routing sans rechargement page).
    La classe 'nav-active' est appliquée via CSS :focus-within ou callback léger.
    """
    nav_links = [
        dcc.Link(
            href=item["path"],
            children=[
                html.Span(item["icon"], className="nav-icon"),
                html.Span(item["label"], className="nav-label"),
            ],
            className="nav-item",
            id=f"nav-{item['path'].strip('/') or 'home'}",
        )
        for item in NAV_ITEMS
    ]

    return html.Aside(
        [
            # Le bloc marque reste fixe pour renforcer l'ancrage visuel.
            html.Div(
                [
                    html.Div("ObRail", className="brand-name"),
                    html.Div("Europe", className="brand-sub"),
                ],
                className="brand",
            ),
            # Le séparateur reprend le motif rail utilisé dans le reste du thème.
            html.Div(className="rail-separator"),
            # Les liens sont générés depuis NAV_ITEMS pour éviter les divergences de routes.
            html.Nav(nav_links, className="nav-list"),
            # Le footer regroupe infos techniques et action de maintenance cache.
            html.Div(
                [
                    html.Span("API v1.0.0", className="sidebar-meta"),
                    html.Br(),
                    html.Span("© ObRail Europe", className="sidebar-meta"),
                    html.Div(style={"height": "10px"}),
                    html.Button("Vider le cache", id="btn-clear-cache", className="btn-secondary sidebar-clear-btn", n_clicks=0),
                    html.Div(id="cache-clear-status", className="sidebar-meta", style={"marginTop": "8px"}),
                ],
                className="sidebar-footer",
            ),
        ],
        className="sidebar",
    )
