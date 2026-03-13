"""
ObRail Europe Dashboard — point d'entrée principal.

Lancement :
    cd /path/to/ETL
    python -m dashboard.app                   # développement
    gunicorn dashboard.app:server -b 0.0.0.0:8050   # production

Pattern Dash multi-pages :
  - Chaque fichier pages/*.py déclare dash.register_page()
  - Le routage client-side est géré par dash.page_container
  - Les données sont partagées entre callbacks via dcc.Store (pas de re-fetch)

Background callbacks :
  - Opérations > 1s (export CSV, chargement tables lourdes) utilisent
    DiskcacheManager qui pointe sur la même instance que le cache API.
"""

import dash
from dash import Dash, dcc, html, callback, no_update
from dash import DiskcacheManager

from dashboard.components.navbar import sidebar
from dashboard.utils.cache import get_cache_manager, clear_api_cache

# Le manager de callbacks asynchrones réutilise le même backend cache que les appels API.
background_manager = DiskcacheManager(get_cache_manager())

app = Dash(
    __name__,
    use_pages=True,
    pages_folder="pages",
    background_callback_manager=background_manager,
    suppress_callback_exceptions=True,   # Les callbacks des pages sont chargés à la demande.
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"},
        {"charset": "UTF-8"},
    ],
    title="ObRail Europe",
)

server = app.server   # Point d'entrée WSGI utilisé en déploiement (gunicorn/docker).

# On garde la sidebar hors page_container pour éviter qu'elle soit re-rendue à chaque navigation.
app.layout = html.Div(
    [
        # La route active sert de source unique pour l'état visuel de la navigation.
        dcc.Location(id="url", refresh=False),
        # Ce store sert uniquement d'ancre technique pour le callback clientside de la nav.
        dcc.Store(id="_nav-dummy"),

        # Sidebar persistante pour conserver un repère visuel stable pendant la navigation.
        sidebar(),

        # Le contenu central change selon la route, sans reconstruire le shell global.
        html.Main(
            [
                dcc.Loading(
                    id="page-loading",
                    children=dash.page_container,
                    type="dot",
                    color="#EED679",
                    overlay_style={"visibility": "visible", "opacity": 0.5},
                ),
            ],
            className="main-content",
        ),
    ],
    className="app-shell",
    id="app-shell",
)


from dash import Input, Output, clientside_callback

# Le calcul du lien actif côté navigateur évite un aller-retour Python inutile à chaque changement d'URL.
clientside_callback(
    """
    function(pathname) {
        const items = document.querySelectorAll('.nav-item');
        items.forEach(function(el) {
            const href = el.getAttribute('href');
            // Les sous-routes héritent de l'item parent pour garder un comportement de nav cohérent.
            const isActive = (pathname === href) || (href !== '/' && pathname.startsWith(href));
            el.classList.toggle('nav-active', isActive);
        });
        return null;
    }
    """,
    Output("_nav-dummy", "data"),   # Dash exige une sortie, même si l'effet utile est purement DOM.
    Input("url", "pathname"),
    prevent_initial_call=False,
)


@callback(
    Output("cache-clear-status", "children"),
    Input("btn-clear-cache", "n_clicks"),
    prevent_initial_call=True,
)
def on_clear_cache(n_clicks):
    if not n_clicks:
        return no_update
    deleted = clear_api_cache()
    return f"Cache vidé ({deleted})"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050, dev_tools_hot_reload=True)
