"""
Endpoints Consultation & Téléchargement (section 3 de la spec).

4 GET endpoints : routes paginées, routes CSV, compare paginé, compare CSV.
"""

from fastapi import APIRouter, Depends, Query

from ..database import get_db
from ..dependencies import pagination_params
from ..utils.query_helpers import (
    execute_paginated_with_count, WhereBuilder,
    safe_order_by, safe_sort_direction,
    ROUTES_SORTABLE, COMPARE_SORTABLE,
)
from ..utils.csv_export import stream_csv_response

router = APIRouter()


# Helpers partagés entre endpoints paginés et exports CSV.

def _build_routes_where(
    mode, source, departure_country, arrival_country,
    departure_city, arrival_city, departure_station, arrival_station,
    agency_name, route_type, is_night_train, days_of_week,
    min_distance_km, max_distance_km, min_co2, max_co2,
    service_start_after, service_end_before,
) -> WhereBuilder:
    """Construit la clause WHERE pour gold_routes (sections 3.1 et 3.2)."""
    wb = WhereBuilder()
    wb.add_exact("mode", mode)
    wb.add_exact("source", source)
    # On force le code pays en uppercase pour garder un filtre cohérent et
    # profiter du partition pruning côté table cible.
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())
    if arrival_country:
        wb.add_exact("arrival_country", arrival_country.upper())
    wb.add_ilike("departure_city", departure_city)
    wb.add_ilike("arrival_city", arrival_city)
    wb.add_ilike("departure_station", departure_station)
    wb.add_ilike("arrival_station", arrival_station)
    wb.add_ilike("agency_name", agency_name)
    wb.add_exact("route_type", route_type, cast="text")
    wb.add_bool("is_night_train", is_night_train)
    wb.add_like("days_of_week", days_of_week)
    wb.add_gte("distance_km", min_distance_km)
    wb.add_lte("distance_km", max_distance_km)
    wb.add_gte("emissions_co2", min_co2)
    wb.add_lte("emissions_co2", max_co2)
    wb.add_gte("service_start_date", service_start_after)
    wb.add_lte("service_end_date", service_end_before)
    return wb


def _build_compare_where(
    departure_city, departure_country, arrival_city, arrival_country,
    best_mode, min_train_duration, max_train_duration,
    min_flight_duration, max_flight_duration, days_of_week,
) -> WhereBuilder:
    """Construit la clause WHERE pour gold_compare_best (sections 3.3 et 3.4)."""
    wb = WhereBuilder()
    wb.add_ilike("departure_city", departure_city)
    # Même logique ici : pays normalisé pour rester compatible avec le
    # partition pruning sur les données chargées.
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())
    wb.add_ilike("arrival_city", arrival_city)
    if arrival_country:
        wb.add_exact("arrival_country", arrival_country.upper())
    wb.add_exact("best_mode", best_mode)
    wb.add_gte("train_duration_min", min_train_duration)
    wb.add_lte("train_duration_min", max_train_duration)
    wb.add_gte("flight_duration_min", min_flight_duration)
    wb.add_lte("flight_duration_min", max_flight_duration)
    wb.add_like("days_of_week", days_of_week)
    return wb


@router.get("/routes")
def list_routes(
    mode: str | None = Query(None, pattern="^(train|flight)$"),
    source: str | None = Query(None, max_length=100),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    arrival_country: str | None = Query(None, min_length=2, max_length=2),
    departure_city: str | None = Query(None, max_length=100),
    arrival_city: str | None = Query(None, max_length=100),
    departure_station: str | None = Query(None, max_length=100),
    arrival_station: str | None = Query(None, max_length=100),
    agency_name: str | None = Query(None, max_length=100),
    route_type: int | None = Query(None),
    is_night_train: bool | None = Query(None),
    days_of_week: str | None = Query(None, max_length=7),
    min_distance_km: float | None = Query(None, ge=0),
    max_distance_km: float | None = Query(None, ge=0),
    min_co2: float | None = Query(None, ge=0),
    max_co2: float | None = Query(None, ge=0),
    service_start_after: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    service_end_before: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    sort_by: str | None = Query(None),
    sort_order: str | None = Query(None, pattern="^(asc|desc)$"),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Consultation paginée de gold_routes avec filtrage fin."""
    # Le SQL reste explicite ici pour garder un contrôle fin sur les colonnes,
    # le tri et le coût des requêtes côté PostgreSQL.
    wb = _build_routes_where(
        mode, source, departure_country, arrival_country,
        departure_city, arrival_city, departure_station, arrival_station,
        agency_name, route_type, is_night_train, days_of_week,
        min_distance_km, max_distance_km, min_co2, max_co2,
        service_start_after, service_end_before,
    )

    where = wb.build()
    col = safe_order_by(sort_by, ROUTES_SORTABLE, "departure_country")
    direction = safe_sort_direction(sort_order)

    data_query = f"""
        SELECT * FROM gold_routes
        WHERE {where}
        ORDER BY {col} {direction}
        LIMIT %s OFFSET %s
    """

    count_query = f"SELECT COUNT(*) AS total FROM gold_routes WHERE {where}"

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


@router.get("/routes/download")
def download_routes(
    mode: str | None = Query(None, pattern="^(train|flight)$"),
    source: str | None = Query(None, max_length=100),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    arrival_country: str | None = Query(None, min_length=2, max_length=2),
    departure_city: str | None = Query(None, max_length=100),
    arrival_city: str | None = Query(None, max_length=100),
    departure_station: str | None = Query(None, max_length=100),
    arrival_station: str | None = Query(None, max_length=100),
    agency_name: str | None = Query(None, max_length=100),
    route_type: int | None = Query(None),
    is_night_train: bool | None = Query(None),
    days_of_week: str | None = Query(None, max_length=7),
    min_distance_km: float | None = Query(None, ge=0),
    max_distance_km: float | None = Query(None, ge=0),
    min_co2: float | None = Query(None, ge=0),
    max_co2: float | None = Query(None, ge=0),
    service_start_after: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    service_end_before: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    sort_by: str | None = Query(None),
    sort_order: str | None = Query(None, pattern="^(asc|desc)$"),
    conn=Depends(get_db),
):
    """Téléchargement CSV de gold_routes. Limite 500k lignes."""
    # L'export réutilise exactement les mêmes filtres que la vue paginée pour
    # garantir des résultats cohérents entre consultation et CSV.
    wb = _build_routes_where(
        mode, source, departure_country, arrival_country,
        departure_city, arrival_city, departure_station, arrival_station,
        agency_name, route_type, is_night_train, days_of_week,
        min_distance_km, max_distance_km, min_co2, max_co2,
        service_start_after, service_end_before,
    )

    where = wb.build()
    col = safe_order_by(sort_by, ROUTES_SORTABLE, "departure_country")
    direction = safe_sort_direction(sort_order)

    query = f"""
        SELECT * FROM gold_routes
        WHERE {where}
        ORDER BY {col} {direction}
    """

    count_query = f"SELECT COUNT(*) AS total FROM gold_routes WHERE {where}"

    return stream_csv_response(
        conn, query, wb.params, "routes_export.csv",
        count_query=count_query, count_params=wb.params,
    )


@router.get("/compare")
def list_compare(
    departure_city: str | None = Query(None),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    arrival_city: str | None = Query(None),
    arrival_country: str | None = Query(None, min_length=2, max_length=2),
    best_mode: str | None = Query(None, pattern="^(train|flight)$"),
    min_train_duration: float | None = Query(None, ge=0),
    max_train_duration: float | None = Query(None, ge=0),
    min_flight_duration: float | None = Query(None, ge=0),
    max_flight_duration: float | None = Query(None, ge=0),
    days_of_week: str | None = Query(None, max_length=7),
    sort_by: str | None = Query(None),
    sort_order: str | None = Query(None, pattern="^(asc|desc)$"),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Consultation paginée de gold_compare_best."""
    # Cette requête expose directement la table de comparaison pré-calculée,
    # ce qui évite de refaire le matching train/avion à la volée.
    wb = _build_compare_where(
        departure_city, departure_country, arrival_city, arrival_country,
        best_mode, min_train_duration, max_train_duration,
        min_flight_duration, max_flight_duration, days_of_week,
    )

    where = wb.build()
    col = safe_order_by(sort_by, COMPARE_SORTABLE, "departure_country")
    direction = safe_sort_direction(sort_order)

    data_query = f"""
        SELECT * FROM gold_compare_best
        WHERE {where}
        ORDER BY {col} {direction}
        LIMIT %s OFFSET %s
    """

    count_query = f"SELECT COUNT(*) AS total FROM gold_compare_best WHERE {where}"

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


@router.get("/compare/download")
def download_compare(
    departure_city: str | None = Query(None),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    arrival_city: str | None = Query(None),
    arrival_country: str | None = Query(None, min_length=2, max_length=2),
    best_mode: str | None = Query(None, pattern="^(train|flight)$"),
    min_train_duration: float | None = Query(None, ge=0),
    max_train_duration: float | None = Query(None, ge=0),
    min_flight_duration: float | None = Query(None, ge=0),
    max_flight_duration: float | None = Query(None, ge=0),
    days_of_week: str | None = Query(None, max_length=7),
    sort_by: str | None = Query(None),
    sort_order: str | None = Query(None, pattern="^(asc|desc)$"),
    conn=Depends(get_db),
):
    """Export CSV de gold_compare_best. Limite 500k lignes."""
    # Même principe que /compare avec un flux complet destiné à l'export CSV.
    wb = _build_compare_where(
        departure_city, departure_country, arrival_city, arrival_country,
        best_mode, min_train_duration, max_train_duration,
        min_flight_duration, max_flight_duration, days_of_week,
    )

    where = wb.build()
    col = safe_order_by(sort_by, COMPARE_SORTABLE, "departure_country")
    direction = safe_sort_direction(sort_order)

    query = f"""
        SELECT * FROM gold_compare_best
        WHERE {where}
        ORDER BY {col} {direction}
    """

    count_query = f"SELECT COUNT(*) AS total FROM gold_compare_best WHERE {where}"

    return stream_csv_response(
        conn, query, wb.params, "compare_export.csv",
        count_query=count_query, count_params=wb.params,
    )
