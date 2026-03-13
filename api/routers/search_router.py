"""
Endpoints Recherche de Trajets (section 4 de la spec).

2 GET endpoints pour la recherche bidirectionnelle et le détail d'un trajet.
"""

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import get_db
from ..dependencies import pagination_params
from ..utils.query_helpers import execute_query, execute_paginated_with_count, escape_like

router = APIRouter()


@router.get("/routes/search")
def search_routes(
    origin: str = Query(..., min_length=1, max_length=100),
    destination: str = Query(..., min_length=1, max_length=100),
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    day_of_week: int | None = Query(None, ge=1, le=7),
    is_night_train: bool | None = Query(None),
    bidirectional: bool = Query(True),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Recherche de trajets ferroviaires entre deux points, dans les deux sens."""
    # La recherche s'appuie sur du SQL explicite pour gérer proprement le mode
    # bidirectionnel et les filtres optionnels.

    # Ces conditions s'appliquent aux deux sens pour garantir des résultats
    # comparables entre aller et retour.
    conditions = []
    base_params = []

    if date:
        conditions.append("AND (service_start_date <= %s AND service_end_date >= %s)")
        base_params.extend([date, date])
    if day_of_week is not None:
        conditions.append("AND SUBSTRING(days_of_week, %s, 1) = '1'")
        base_params.append(day_of_week)
    if is_night_train is not None:
        conditions.append("AND is_night_train = %s")
        base_params.append(is_night_train)

    extra_where = "\n          ".join(conditions)

    # Requête de base dans le sens origin -> destination.
    outbound_query = f"""
        SELECT *, 'outbound' AS direction
        FROM gold_routes
        WHERE mode = 'train'
          AND (
            departure_city ILIKE '%%' || %s || '%%'
            OR departure_station ILIKE '%%' || %s || '%%'
          )
          AND (
            arrival_city ILIKE '%%' || %s || '%%'
            OR arrival_station ILIKE '%%' || %s || '%%'
          )
          {extra_where}
    """
    # On échappe les caractères LIKE spéciaux pour que les noms de villes
    # soient interprétés littéralement.
    origin_esc = escape_like(origin)
    destination_esc = escape_like(destination)
    outbound_params = [origin_esc, origin_esc, destination_esc, destination_esc] + base_params

    if bidirectional:
        # Même logique dans le sens inverse quand bidirectional=true.
        return_query = f"""
            SELECT *, 'return' AS direction
            FROM gold_routes
            WHERE mode = 'train'
              AND (
                departure_city ILIKE '%%' || %s || '%%'
                OR departure_station ILIKE '%%' || %s || '%%'
              )
              AND (
                arrival_city ILIKE '%%' || %s || '%%'
                OR arrival_station ILIKE '%%' || %s || '%%'
              )
              {extra_where}
        """
        return_params = [destination_esc, destination_esc, origin_esc, origin_esc] + base_params

        full_query = f"""
            ({outbound_query})
            UNION ALL
            ({return_query})
            ORDER BY direction, departure_time
            LIMIT %s OFFSET %s
        """
        full_params = outbound_params + return_params

        count_query = f"""
            SELECT COUNT(*) AS total FROM (
                ({outbound_query})
                UNION ALL
                ({return_query})
            ) combined
        """
        count_params = outbound_params + return_params
    else:
        full_query = f"""
            {outbound_query}
            ORDER BY departure_time
            LIMIT %s OFFSET %s
        """
        full_params = outbound_params

        count_query = f"""
            SELECT COUNT(*) AS total FROM (
                {outbound_query}
            ) sub
        """
        count_params = outbound_params

    return execute_paginated_with_count(
        conn, full_query, count_query,
        full_params, count_params,
        pagination["page"], pagination["page_size"],
    )


@router.get("/routes/{trip_id}")
def get_route_detail(
    trip_id: str,
    source: str | None = Query(None, min_length=1, max_length=100),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    conn=Depends(get_db),
):
    """Liste des segments d'un trajet spécifique (même trip_id peut correspondre à
    plusieurs segments : Paris-Nîmes, Paris-Montpellier, Nîmes-Montpellier)."""
    # On garde un SQL simple et direct ici pour retourner tous les segments
    # attachés au même trip_id.
    conditions = ["trip_id = %s"]
    params = [trip_id]

    if source:
        conditions.append("source = %s")
        params.append(source)
    # Le filtre pays est normalisé pour profiter du partition pruning.
    if departure_country:
        conditions.append("departure_country = %s")
        params.append(departure_country.upper())

    where = " AND ".join(conditions)
    query = f"SELECT * FROM gold_routes WHERE {where} ORDER BY departure_time"

    rows = execute_query(conn, query, params)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
    return rows
