"""
Endpoint /stats/volumes

Renvoie un tableau de bord synthétique sur les volumes de données :
nombre de trajets, couverture géographique, complétude des données, etc.

C'est l'équivalent du endpoint 7.8 /quality/summary de la spec,
renommé /stats/volumes pour correspondre au brief.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from api.database import get_db

router = APIRouter(tags=["✅ MSPR - Requis"])


@router.get(
    "/stats/volumes",
    summary="Volumes et statistiques globales",
    description="Tableau de bord synthétique : nombre de trajets, couverture géographique, complétude des données.",
)
def get_volumes(db: Session = Depends(get_db)):
    query = text("""
        SELECT
            -- Volume global
            (SELECT COUNT(*) FROM gold_routes)                                    AS total_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train')               AS total_train,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'flight')              AS total_flight,
            (SELECT COUNT(*) FROM gold_compare_best)                              AS total_comparisons,

            -- Couverture géographique
            (SELECT COUNT(DISTINCT departure_country) FROM gold_routes)           AS countries_departure,
            (SELECT COUNT(DISTINCT arrival_country)   FROM gold_routes WHERE arrival_country IS NOT NULL) AS countries_arrival,
            (SELECT COUNT(DISTINCT departure_city)    FROM gold_routes WHERE departure_city  IS NOT NULL) AS unique_dep_cities,
            (SELECT COUNT(DISTINCT arrival_city)      FROM gold_routes WHERE arrival_city    IS NOT NULL) AS unique_arr_cities,
            (SELECT COUNT(DISTINCT agency_name)       FROM gold_routes WHERE agency_name     IS NOT NULL) AS unique_agencies,

            -- Jour / Nuit
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = true)  AS night_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = false) AS day_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train IS NULL) AS unclassified_routes,

            -- Complétude des colonnes critiques (en %)
            (SELECT ROUND(100.0 * COUNT(distance_km)   / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS distance_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(emissions_co2) / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS emissions_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(days_of_week)  / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS schedule_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(departure_city)/ NULLIF(COUNT(*),0), 2) FROM gold_routes) AS dep_city_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(arrival_city)  / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS arr_city_completeness_pct,

            -- Comparaison train vs avion
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'train')  AS train_wins_total,
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'flight') AS flight_wins_total
    """)

    row = db.execute(query).mappings().first()

    return {
        "status": "ok",
        "data": dict(row) if row else {},
    }