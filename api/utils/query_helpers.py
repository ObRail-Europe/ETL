"""
Utilitaires SQL pour l'API : ORDER BY sécurisé, exécution paginée, construction WHERE.

Toutes les requêtes utilisent des paramètres %s (psycopg2) — jamais de f-string
avec des valeurs utilisateur. Le ORDER BY dynamique est validé contre une whitelist.
"""

from psycopg2.extras import RealDictCursor


# Évite qu'un terme de recherche utilisateur déclenche involontairement des
# wildcards SQL lors des filtres ILIKE.

def escape_like(value: str) -> str:
    """
    Échappe les caractères spéciaux LIKE (\\, %, _) dans une valeur utilisateur.

    Garantit que ILIKE '%%' || %s || '%%' recherche la chaîne littérale
    plutôt qu'un pattern SQL.  PostgreSQL utilise '\\' comme escape LIKE par défaut.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Ces whitelists bornent les colonnes triables pour bloquer l'injection via
# ORDER BY dynamique.

ROUTES_SORTABLE = {
    "departure_country", "arrival_country", "departure_city", "arrival_city",
    "departure_station", "arrival_station", "distance_km", "emissions_co2",
    "co2_per_pkm", "departure_time", "arrival_time", "mode", "agency_name",
    "is_night_train", "route_type",
}

COMPARE_SORTABLE = {
    "departure_country", "arrival_country", "departure_city", "arrival_city",
    "train_duration_min", "train_distance_km", "train_emissions_co2",
    "flight_duration_min", "flight_distance_km", "flight_emissions_co2",
    "best_mode",
}


def safe_order_by(sort_by: str | None, allowed: set[str], default: str) -> str:
    """Retourne un nom de colonne validé pour ORDER BY. Anti-injection SQL."""
    if sort_by and sort_by in allowed:
        return sort_by
    return default


def safe_sort_direction(sort_order: str | None) -> str:
    """Retourne 'ASC' ou 'DESC'. Valeur par défaut : 'ASC'."""
    if sort_order and sort_order.lower() == "desc":
        return "DESC"
    return "ASC"


def execute_paginated(
    conn, query: str, params: list, page: int, page_size: int
) -> dict:
    """
    Exécute une requête paginée et retourne l'enveloppe standard.

    La requête doit contenir un LIMIT %s OFFSET %s à la fin.
    Le total est obtenu via un COUNT(*) wrappant la même requête sans LIMIT/OFFSET.
    """
    offset = (page - 1) * page_size

    # Le comptage est géré via une requête dédiée ; on évite ainsi les
    # manipulations fragiles de SQL texte pour retirer LIMIT/OFFSET.
    data_params = params + [page_size, offset]

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, data_params)
        rows = cur.fetchall()

    return {
        "status": "ok",
        "count": len(rows),
        "page": page,
        "page_size": page_size,
        "data": [dict(r) for r in rows],
    }


def execute_paginated_with_count(
    conn, data_query: str, count_query: str,
    data_params: list, count_params: list,
    page: int, page_size: int,
) -> dict:
    """
    Exécute data_query (avec LIMIT/OFFSET) + count_query (pour total).
    Retourne l'enveloppe {status, count, total, page, page_size, data}.
    """
    offset = (page - 1) * page_size
    full_data_params = data_params + [page_size, offset]

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Le total est calculé avant pagination pour alimenter correctement l'UI.
        cur.execute(count_query, count_params)
        total = cur.fetchone()["total"]

        # On récupère ensuite uniquement la fenêtre demandée.
        cur.execute(data_query, full_data_params)
        rows = cur.fetchall()

    return {
        "status": "ok",
        "count": len(rows),
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": [dict(r) for r in rows],
    }


def execute_query(conn, query: str, params: list | None = None) -> list[dict]:
    """Exécute une requête et retourne une liste de dicts."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params or [])
        return [dict(r) for r in cur.fetchall()]


def execute_single(conn, query: str, params: list | None = None) -> dict | None:
    """Exécute une requête et retourne un seul dict ou None."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params or [])
        row = cur.fetchone()
        return dict(row) if row else None


class WhereBuilder:
    """Constructeur de clause WHERE dynamique avec paramètres psycopg2."""

    def __init__(self):
        self.conditions: list[str] = []
        self.params: list = []

    def add_exact(self, column: str, value, cast: str | None = None) -> "WhereBuilder":
        """Ajoute une condition d'égalité exacte si value n'est pas None."""
        if value is not None:
            col_expr = f"{column}::{cast}" if cast else column
            self.conditions.append(f"{col_expr} = %s")
            self.params.append(value)
        return self

    def add_ilike(self, column: str, value: str | None) -> "WhereBuilder":
        """Ajoute une condition ILIKE %value% si value n'est pas None.
        Les caractères spéciaux LIKE (%, _, \\) sont automatiquement échappés
        pour éviter les wildcards non intentionnels (voir escape_like).
        """
        if value is not None:
            self.conditions.append(f"{column} ILIKE '%%' || %s || '%%'")
            self.params.append(escape_like(value))
        return self

    def add_bool(self, column: str, value: bool | None) -> "WhereBuilder":
        """Ajoute une condition booléenne si value n'est pas None."""
        if value is not None:
            self.conditions.append(f"{column} = %s")
            self.params.append(value)
        return self

    def add_gte(self, column: str, value) -> "WhereBuilder":
        """Ajoute >= si value n'est pas None."""
        if value is not None:
            self.conditions.append(f"{column} >= %s")
            self.params.append(value)
        return self

    def add_lte(self, column: str, value) -> "WhereBuilder":
        """Ajoute <= si value n'est pas None."""
        if value is not None:
            self.conditions.append(f"{column} <= %s")
            self.params.append(value)
        return self

    def add_like(self, column: str, value: str | None) -> "WhereBuilder":
        """Ajoute LIKE (sans %%) si value n'est pas None."""
        if value is not None:
            self.conditions.append(f"{column} LIKE %s")
            self.params.append(value)
        return self

    def add_raw(self, condition: str, params: list | None = None) -> "WhereBuilder":
        """⚠️  Ajoute une condition SQL brute.

        `condition` DOIT être une chaîne STATIQUE (jamais une valeur
        utilisateur directement). Utiliser %s dans condition et passer
        les valeurs via `params`.
        """
        self.conditions.append(condition)
        if params:
            self.params.extend(params)
        return self

    def build(self) -> str:
        """Retourne la clause WHERE complète (sans le mot WHERE)."""
        if not self.conditions:
            return "1=1"
        return " AND ".join(self.conditions)
