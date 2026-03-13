"""
Composants de filtres réutilisables.

Tous les dropdowns qui se chargent depuis l'API sont initialisés en lazy
(callback dans la page concernée). Ce module exporte uniquement les éléments
UI, pas la logique de remplissage.
"""

from dash import dcc, html

# Liste statique volontaire: le filtre pays reste instantané même si l'API est indisponible.
EU_COUNTRIES = [
    ("AT", "Autriche"), ("BE", "Belgique"), ("CZ", "Tchéquie"), ("DE", "Allemagne"),
    ("DK", "Danemark"), ("EE", "Estonie"), ("ES", "Espagne"), ("FI", "Finlande"),
    ("FR", "France"), ("GB", "Royaume-Uni"), ("GR", "Grèce"), ("HR", "Croatie"),
    ("HU", "Hongrie"), ("IE", "Irlande"), ("IT", "Italie"), ("LT", "Lituanie"),
    ("LU", "Luxembourg"), ("NL", "Pays-Bas"), ("NO", "Norvège"), ("PL", "Pologne"),
    ("PT", "Portugal"), ("RO", "Roumanie"), ("RS", "Serbie"), ("SE", "Suède"),
    ("SI", "Slovénie"), ("SK", "Slovaquie"),
]

COUNTRY_OPTIONS = [{"label": f"{code} — {name}", "value": code} for code, name in EU_COUNTRIES]


def country_dropdown(
    component_id: str,
    placeholder: str = "Tous les pays",
    multi: bool = False,
    value=None,
) -> dcc.Dropdown:
    return dcc.Dropdown(
        id=component_id,
        options=COUNTRY_OPTIONS,
        value=value,
        placeholder=placeholder,
        multi=multi,
        clearable=True,
        className="obrail-dropdown",
    )


def text_search(
    component_id: str,
    placeholder: str = "Rechercher…",
    debounce: bool = True,
) -> dcc.Input:
    """Champ texte avec debounce pour limiter les rafales de callbacks."""
    return dcc.Input(
        id=component_id,
        type="text",
        placeholder=placeholder,
        debounce=debounce,
        className="obrail-input",
    )


def mode_dropdown(component_id: str, value: str = None) -> dcc.Dropdown:
    return dcc.Dropdown(
        id=component_id,
        options=[
            {"label": "🚆  Train", "value": "train"},
            {"label": "✈️  Vol", "value": "flight"},
        ],
        value=value,
        placeholder="Tous les modes",
        clearable=True,
        className="obrail-dropdown",
    )


def sort_controls(
    sort_by_id: str,
    sort_order_id: str,
    options: list[dict],
    default_by: str = None,
) -> html.Div:
    """Regroupe tri et ordre pour éviter des contrôles dispersés dans le layout."""
    return html.Div(
        [
            dcc.Dropdown(
                id=sort_by_id,
                options=options,
                value=default_by,
                placeholder="Trier par…",
                clearable=True,
                className="obrail-dropdown",
                style={"minWidth": "180px"},
            ),
            dcc.Dropdown(
                id=sort_order_id,
                options=[{"label": "↑ Croissant", "value": "asc"}, {"label": "↓ Décroissant", "value": "desc"}],
                value="asc",
                clearable=False,
                className="obrail-dropdown",
                style={"minWidth": "140px"},
            ),
        ],
        className="sort-controls",
    )


def page_size_selector(component_id: str, default: int = 25) -> dcc.Dropdown:
    return dcc.Dropdown(
        id=component_id,
        options=[{"label": str(n), "value": n} for n in [10, 25, 50, 100]],
        value=default,
        clearable=False,
        className="obrail-dropdown",
        style={"width": "80px"},
    )


def filter_row(children: list) -> html.Div:
    """Encapsule une ligne de filtres avec le spacing commun du design system."""
    return html.Div(children, className="filter-row")
