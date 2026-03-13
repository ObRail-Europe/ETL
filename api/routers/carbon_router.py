"""
Endpoints Émissions Carbone (section 5 de la spec).

4 GET endpoints pour le bilan carbone, estimations et facteurs d'émission.
"""

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import get_db
from ..dependencies import pagination_params
from ..utils.query_helpers import (
    execute_query, execute_paginated_with_count, WhereBuilder,
    escape_like,
)

router = APIRouter()


@router.get("/carbon/trip/{trip_id}")
def carbon_trip(
    trip_id: str,
    source: str | None = Query(None, min_length=1, max_length=100),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    conn=Depends(get_db),
):
    """Bilan carbone des segments d'un trajet spécifique (le même trip_id peut
    correspondre à plusieurs segments O/D dans la table)."""
    # On lit directement les segments du trip pour conserver le détail utile
    # à l'analyse carbone fine.
    wb = WhereBuilder()
    wb.add_exact("trip_id", trip_id)
    if source:
        wb.add_exact("source", source)
    # Le filtre pays est normalisé pour limiter le scan si le client le fournit.
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())

    where = wb.build()
    query = f"""
        SELECT source, trip_id, mode, departure_city, arrival_city,
               departure_country, arrival_country,
               distance_km, co2_per_pkm, emissions_co2, is_night_train
        FROM gold_routes
        WHERE {where}
        ORDER BY departure_time
    """
    rows = execute_query(conn, query, wb.params)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
    return rows


@router.get("/carbon/estimate")
def carbon_estimate(
    origin: str = Query(..., min_length=1, max_length=100),
    destination: str = Query(..., min_length=1, max_length=100),
    conn=Depends(get_db),
):
    """Estimation CO₂ pour un trajet, comparaison train vs avion.

    Utilise gold_compare_best qui pré-calcule le matching train/avion par
    paire O/D. Renvoie les stats agrégées pour chaque mode ainsi que le
    meilleur mode écologique (best_mode le plus fréquent sur les corridors
    trouvés).
    """
    # On agrège sur gold_compare_best pour exploiter un matching déjà calculé.
    # MODE() WITHIN GROUP retourne ici le best_mode le plus fréquent.
    query = """
        SELECT
            COUNT(*)                                                AS nb_corridors,

            -- Métriques train (NULL si pas de train sur le corridor)
            COUNT(train_emissions_co2)                             AS nb_train_corridors,
            ROUND(AVG(train_distance_km)::numeric,  1)            AS avg_train_distance_km,
            ROUND(AVG(train_duration_min)::numeric,  1)           AS avg_train_duration_min,
            ROUND(AVG(train_emissions_co2)::numeric, 1)           AS avg_train_emissions_co2,
            ROUND(MIN(train_emissions_co2)::numeric, 1)           AS min_train_emissions_co2,
            ROUND(MAX(train_emissions_co2)::numeric, 1)           AS max_train_emissions_co2,

            -- Métriques avion (NULL si pas de vol sur le corridor)
            COUNT(flight_emissions_co2)                            AS nb_flight_corridors,
            ROUND(AVG(flight_distance_km)::numeric,  1)           AS avg_flight_distance_km,
            ROUND(AVG(flight_duration_min)::numeric,  1)          AS avg_flight_duration_min,
            ROUND(AVG(flight_emissions_co2)::numeric, 1)          AS avg_flight_emissions_co2,
            ROUND(MIN(flight_emissions_co2)::numeric, 1)          AS min_flight_emissions_co2,
            ROUND(MAX(flight_emissions_co2)::numeric, 1)          AS max_flight_emissions_co2,

            -- Mode le plus écologique sur la majorité des corridors
            MODE() WITHIN GROUP (ORDER BY best_mode)              AS best_mode
        FROM gold_compare_best
        WHERE (departure_city ILIKE '%%' || %s || '%%')
          AND (arrival_city   ILIKE '%%' || %s || '%%')
    """
    origin_esc = escape_like(origin)
    destination_esc = escape_like(destination)
    row = execute_query(conn, query, [origin_esc, destination_esc])
    stats = row[0] if row else {}

    result: dict = {
        "origin": origin,
        "destination": destination,
        **stats,
    }

    # Cet indicateur est calculé seulement quand les deux moyennes existent.
    train_co2 = stats.get("avg_train_emissions_co2")
    flight_co2 = stats.get("avg_flight_emissions_co2")
    if train_co2 is not None and flight_co2 is not None and float(flight_co2) > 0:
        result["co2_saving_pct"] = round(
            (1 - float(train_co2) / float(flight_co2)) * 100, 1
        )

    return result


@router.get("/carbon/ranking")
def carbon_ranking(
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    min_distance_km: float | None = Query(None, ge=0),
    sort_by: str = Query("co2_saving_pct", pattern="^(co2_saving_pct|train_emissions_co2|flight_emissions_co2)$"),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Classement des paires O/D par économie de CO₂ du train vs avion."""
    # Ce classement ne garde que les corridors où train et avion sont présents
    # pour comparer des bases homogènes.
    wb = WhereBuilder()
    wb.add_raw("flight_emissions_co2 IS NOT NULL")
    wb.add_raw("train_emissions_co2 IS NOT NULL")
    wb.add_raw("flight_emissions_co2 > 0")
    # Le filtre pays aide à réduire le coût sur les gros volumes.
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())
    wb.add_gte("train_distance_km", min_distance_km)

    where = wb.build()

    # sort_by est déjà borné par Query(..., pattern=...) ; on peut l'injecter
    # sans ouvrir de surface d'injection SQL.
    data_query = f"""
        SELECT
            departure_city,
            departure_country,
            arrival_city,
            arrival_country,
            train_distance_km,
            train_emissions_co2,
            flight_distance_km,
            flight_emissions_co2,
            ROUND(
                            ((1 - train_emissions_co2 / NULLIF(flight_emissions_co2, 0)) * 100)::numeric, 1
            ) AS co2_saving_pct,
            best_mode
        FROM gold_compare_best
        WHERE {where}
        ORDER BY {sort_by} DESC
        LIMIT %s OFFSET %s
    """

    count_query = f"SELECT COUNT(*) AS total FROM gold_compare_best WHERE {where}"

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


@router.get("/carbon/factors")
def carbon_factors(
    country: str | None = Query(None, min_length=2, max_length=2),
    mode: str | None = Query(None, pattern="^(train|flight)$"),
    is_night_train: bool | None = Query(None),
    conn=Depends(get_db),
):
    """Liste les facteurs d'émission utilisés, par pays, avec nb de trajets concernés."""
    # L'endpoint expose les facteurs réellement utilisés dans les trajets,
    # pas seulement un catalogue théorique.
    wb = WhereBuilder()
    wb.add_raw("co2_per_pkm IS NOT NULL")
    # Le filtre pays évite des agrégations inutiles quand la cible est locale.
    if country:
        wb.add_exact("departure_country", country.upper())
    wb.add_exact("mode", mode)
    wb.add_bool("is_night_train", is_night_train)

    where = wb.build()
    query = f"""
        SELECT
            departure_country AS country_code,
            mode,
            is_night_train,
            COUNT(*) AS nb_routes_using_factor,
            ROUND(co2_per_pkm::numeric, 4) AS co2_per_pkm,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT route_type ORDER BY route_type), NULL) AS route_types
        FROM gold_routes
        WHERE {where}
        GROUP BY departure_country, mode, is_night_train, co2_per_pkm
        ORDER BY departure_country, mode, is_night_train, co2_per_pkm
    """
    rows = execute_query(conn, query, wb.params)
    return {"status": "ok", "count": len(rows), "data": rows}
