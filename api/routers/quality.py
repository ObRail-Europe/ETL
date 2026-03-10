"""
Endpoints /quality — Section 7 de la spec API

Endpoints de monitoring de la qualité des données.
Très utiles pour le dashboard interne et la CI/CD.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from api.database import get_db

router = APIRouter()


@router.get("/quality/summary", summary="Dashboard qualité synthétique — un seul appel")
def quality_summary(db: Session = Depends(get_db)):
    """Tableau de bord synthétique : idéal pour la page d'accueil du dashboard."""
    row = db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM gold_routes)                          AS total_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train')     AS total_train,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'flight')    AS total_flight,
            (SELECT COUNT(*) FROM gold_compare_best)                    AS total_comparisons,
            (SELECT COUNT(DISTINCT departure_country) FROM gold_routes) AS countries_departure,
            (SELECT COUNT(DISTINCT arrival_country)   FROM gold_routes WHERE arrival_country IS NOT NULL) AS countries_arrival,
            (SELECT COUNT(DISTINCT departure_city)    FROM gold_routes WHERE departure_city  IS NOT NULL) AS unique_dep_cities,
            (SELECT COUNT(DISTINCT arrival_city)      FROM gold_routes WHERE arrival_city    IS NOT NULL) AS unique_arr_cities,
            (SELECT COUNT(DISTINCT agency_name)       FROM gold_routes WHERE agency_name     IS NOT NULL) AS unique_agencies,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = true)  AS night_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = false) AS day_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train IS NULL) AS unclassified_routes,
            (SELECT ROUND(100.0 * COUNT(distance_km)   / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS distance_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(emissions_co2) / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS emissions_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(days_of_week)  / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS schedule_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(departure_city)/ NULLIF(COUNT(*),0), 2) FROM gold_routes) AS dep_city_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(arrival_city)  / NULLIF(COUNT(*),0), 2) FROM gold_routes) AS arr_city_completeness_pct,
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'train')  AS train_wins_total,
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'flight') AS flight_wins_total
    """)).mappings().first()

    return {"status": "ok", "data": dict(row) if row else {}}


@router.get("/quality/completeness", summary="Taux de complétude de chaque colonne de gold_routes")
def quality_completeness(db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT
            COUNT(*) AS total_rows,
            ROUND(100.0 * COUNT(trip_id)           / COUNT(*), 2) AS trip_id_pct,
            ROUND(100.0 * COUNT(agency_name)       / COUNT(*), 2) AS agency_name_pct,
            ROUND(100.0 * COUNT(departure_station) / COUNT(*), 2) AS departure_station_pct,
            ROUND(100.0 * COUNT(departure_city)    / COUNT(*), 2) AS departure_city_pct,
            ROUND(100.0 * COUNT(departure_time)    / COUNT(*), 2) AS departure_time_pct,
            ROUND(100.0 * COUNT(arrival_station)   / COUNT(*), 2) AS arrival_station_pct,
            ROUND(100.0 * COUNT(arrival_city)      / COUNT(*), 2) AS arrival_city_pct,
            ROUND(100.0 * COUNT(arrival_country)   / COUNT(*), 2) AS arrival_country_pct,
            ROUND(100.0 * COUNT(arrival_time)      / COUNT(*), 2) AS arrival_time_pct,
            ROUND(100.0 * COUNT(service_start_date)/ COUNT(*), 2) AS service_start_date_pct,
            ROUND(100.0 * COUNT(service_end_date)  / COUNT(*), 2) AS service_end_date_pct,
            ROUND(100.0 * COUNT(days_of_week)      / COUNT(*), 2) AS days_of_week_pct,
            ROUND(100.0 * COUNT(is_night_train)    / COUNT(*), 2) AS is_night_train_pct,
            ROUND(100.0 * COUNT(distance_km)       / COUNT(*), 2) AS distance_km_pct,
            ROUND(100.0 * COUNT(co2_per_pkm)       / COUNT(*), 2) AS co2_per_pkm_pct,
            ROUND(100.0 * COUNT(emissions_co2)     / COUNT(*), 2) AS emissions_co2_pct
        FROM gold_routes
    """)).mappings().first()

    return {"status": "ok", "data": dict(row) if row else {}}


@router.get("/quality/completeness/by-country", summary="Complétude ventilée par pays de départ")
def quality_completeness_by_country(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT departure_country,
               COUNT(*) AS total_rows,
               ROUND(100.0 * COUNT(departure_city)  / COUNT(*), 2) AS departure_city_pct,
               ROUND(100.0 * COUNT(arrival_city)    / COUNT(*), 2) AS arrival_city_pct,
               ROUND(100.0 * COUNT(arrival_country) / COUNT(*), 2) AS arrival_country_pct,
               ROUND(100.0 * COUNT(days_of_week)    / COUNT(*), 2) AS days_of_week_pct,
               ROUND(100.0 * COUNT(distance_km)     / COUNT(*), 2) AS distance_km_pct,
               ROUND(100.0 * COUNT(emissions_co2)   / COUNT(*), 2) AS emissions_co2_pct,
               ROUND(100.0 * COUNT(is_night_train)  / COUNT(*), 2) AS is_night_train_pct
        FROM gold_routes WHERE mode = 'train'
        GROUP BY departure_country ORDER BY departure_country
    """)).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}


@router.get("/quality/coverage/countries", summary="Représentation des pays dans la base")
def quality_coverage_countries(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        WITH s AS (
            SELECT departure_country,
                   COUNT(*) AS total_routes,
                   COUNT(CASE WHEN mode = 'train' THEN 1 END)        AS train_routes,
                   COUNT(CASE WHEN mode = 'flight' THEN 1 END)       AS flight_routes,
                   COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_routes,
                   COUNT(DISTINCT agency_name)                        AS nb_agencies,
                   COUNT(DISTINCT departure_city)                     AS nb_dep_cities,
                   COUNT(DISTINCT arrival_country)                    AS nb_connected_countries
            FROM gold_routes GROUP BY departure_country
        )
        SELECT *, ROUND(100.0 * night_routes / NULLIF(train_routes,0), 1) AS night_pct
        FROM s ORDER BY total_routes DESC
    """)).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}


@router.get("/quality/schedules", summary="Analyse des patterns horaires (jours de service)")
def quality_schedules(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT departure_country,
               COUNT(CASE WHEN days_of_week = '1111100' THEN 1 END) AS weekday_only,
               COUNT(CASE WHEN days_of_week = '0000011' THEN 1 END) AS weekend_only,
               COUNT(CASE WHEN days_of_week = '1111111' THEN 1 END) AS daily,
               COUNT(CASE WHEN days_of_week IS NULL THEN 1 END)     AS no_schedule,
               COUNT(*) AS total,
               ROUND(100.0 * COUNT(CASE WHEN days_of_week IS NULL THEN 1 END) / COUNT(*), 2) AS null_schedule_pct
        FROM gold_routes WHERE mode = 'train'
        GROUP BY departure_country ORDER BY null_schedule_pct DESC
    """)).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}


@router.get("/quality/compare-coverage", summary="Taux de couverture comparaison train/avion")
def quality_compare_coverage(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        WITH train AS (
            SELECT departure_country, COUNT(DISTINCT trip_id) AS total_train_trips
            FROM gold_routes WHERE mode = 'train' GROUP BY departure_country
        ),
        comp AS (
            SELECT departure_country,
                   COUNT(*) AS compared_trips,
                   COUNT(CASE WHEN best_mode = 'train'  THEN 1 END) AS train_wins,
                   COUNT(CASE WHEN best_mode = 'flight' THEN 1 END) AS flight_wins
            FROM gold_compare_best GROUP BY departure_country
        )
        SELECT t.departure_country, t.total_train_trips,
               COALESCE(c.compared_trips, 0) AS compared_trips,
               ROUND(100.0 * COALESCE(c.compared_trips, 0) / NULLIF(t.total_train_trips, 0), 2) AS compare_coverage_pct,
               COALESCE(c.train_wins, 0)  AS train_wins,
               COALESCE(c.flight_wins, 0) AS flight_wins,
               ROUND(100.0 * COALESCE(c.train_wins, 0) / NULLIF(c.compared_trips, 0), 1) AS train_win_pct
        FROM train t LEFT JOIN comp c USING (departure_country)
        ORDER BY t.departure_country
    """)).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}