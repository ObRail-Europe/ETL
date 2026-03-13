"""
Composants de tables paginées ObRail.

Utilise dash_table.DataTable avec le thème sombre ObRail.
La pagination est gérée côté serveur via callbacks.
"""

from typing import Optional

from dash import dash_table, html, dcc


# Style de base partagé pour garantir la même lisibilité sur toutes les tables.
_STYLE_TABLE = {"overflowX": "auto", "borderRadius": "6px"}

_STYLE_HEADER = {
    "backgroundColor": "#F5F0E8",
    "color": "#6B7C8D",
    "fontWeight": "700",
    "fontSize": "12px",
    "textTransform": "uppercase",
    "letterSpacing": "0.04em",
    "border": "none",
    "borderBottom": "2px solid #D5CFC6",
    "padding": "10px 12px",
}

_STYLE_CELL = {
    "backgroundColor": "#FFFFFF",
    "color": "#1A2332",
    "fontSize": "13px",
    "padding": "9px 12px",
    "border": "none",
    "borderBottom": "1px solid #E3DED5",
    "fontFamily": "Inter, system-ui, sans-serif",
    "textAlign": "left",
    "whiteSpace": "normal",
    "height": "auto",
}

_STYLE_CELL_COND = [
    {"if": {"row_index": "odd"}, "backgroundColor": "#FAFAF5"},
    {"if": {"state": "selected"}, "backgroundColor": "rgba(57, 51, 96, 0.08)", "border": "1px solid #393360"},
    {"if": {"state": "active"}, "backgroundColor": "rgba(57, 51, 96, 0.06)"},
]


def obrail_table(
    table_id: str,
    columns: list[dict],
    data: list[dict],
    page_size: int = 25,
    server_side: bool = True,
    total_count: int = 0,
    style_data_conditional: list = None,
    tooltip_data: list = None,
    fixed_columns: int = 0,
) -> dash_table.DataTable:
    """
    DataTable ObRail avec thème sombre.

    Pour la pagination serveur :
      - server_side=True : page_action="custom", le callback doit gérer page_current
      - total_count : nombre total de lignes (pour calculer le nb de pages)
    """
    extra_cond = style_data_conditional or []

    return dash_table.DataTable(
        id=table_id,
        columns=columns,
        data=data,
        # Le mode custom délègue la pagination au backend pour gérer les volumes élevés.
        page_action="custom" if server_side else "native",
        page_current=0,
        page_size=page_size,
        page_count=(total_count // page_size + 1) if server_side else None,
        # Le tri serveur garde la cohérence avec les filtres métier et les résultats paginés.
        sort_action="custom" if server_side else "native",
        sort_mode="single",
        filter_action="none",   # Le filtrage vit dans les contrôles dédiés pour éviter la double logique.
        # Le style est centralisé ici pour éviter les écarts d'une page à l'autre.
        style_table=_STYLE_TABLE,
        style_header=_STYLE_HEADER,
        style_cell=_STYLE_CELL,
        style_data_conditional=_STYLE_CELL_COND + extra_cond,
        # Tooltips instantanés utiles sur les colonnes techniques ou tronquées.
        tooltip_data=tooltip_data,
        tooltip_delay=0,
        tooltip_duration=None,
        # On désactive la sélection native pour privilégier les interactions pilotées par callbacks.
        fixed_columns={"headers": True, "data": fixed_columns} if fixed_columns else {},
        cell_selectable=False,
        row_selectable=False,
    )


def table_wrapper(
    table: dash_table.DataTable,
    title: str = None,
    download_id: str = None,
) -> html.Div:
    """Enveloppe une table avec titre optionnel et bouton de téléchargement."""
    header_parts = []
    if title:
        header_parts.append(html.H3(title, className="section-title"))
    if download_id:
        header_parts.append(
            html.Button(
                "⬇  Exporter CSV",
                id=download_id,
                className="btn-secondary",
                style={"marginLeft": "auto"},
            )
        )
    header = html.Div(header_parts, className="table-header") if header_parts else None
    children = [c for c in [header, table] if c]
    return html.Div(children, className="table-wrapper")
