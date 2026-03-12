# API ObRail Europe — Spécification des Endpoints

## Base URL

```
/api/v1
```

Toutes les réponses JSON suivent le format :
```json
{
  "status": "ok",
  "count": 25,
  "total": 14230,
  "page": 1,
  "page_size": 25,
  "data": [...]
}
```

Pagination par défaut : `page=1`, `page_size=25`, max `page_size=500`.

Les endpoints POST renvoient :
```json
{
  "status": "success",
  "imported": 14230,
  "skipped": 42,
  "errors": [
    { "row": 1507, "message": "Missing departure_country" }
  ]
}
```

---

## 1. Import — Déclenchement du pipeline ETL

Endpoints POST permettant de piloter l'ingestion des données de façon programmatique (cron, orchestrateur, webhook). Chaque endpoint déclenche la phase d'import correspondante et renvoie un compte-rendu d'exécution.

> **Authentification :** Ces endpoints doivent être protégés (API key ou Bearer token) car ils modifient les données en base.


### 1.1 `POST /import/cities`

Import ou mise à jour du référentiel géographique (villes, pays, coordonnées) depuis GeoNames ou les données GTFS stops.

**Body (optionnel) :**
```json
{
  "source": "geonames",
  "file_path": "/data/raw/geonames_cities.csv",
  "overwrite": false
}
```

**Logique :**
```sql
-- Upsert dans la table de référence (si elle existe)
-- Sinon, peuplement dérivé de gold_routes
INSERT INTO ref_cities (city_name, country_code, latitude, longitude)
SELECT DISTINCT
    departure_city,
    departure_country,
    NULL, NULL  -- coordonnées à enrichir depuis GeoNames
FROM gold_routes
WHERE departure_city IS NOT NULL
ON CONFLICT (city_name, country_code) DO NOTHING;
```

> **Note :** Si la table `ref_cities` n'existe pas dans le schéma actuel, cet endpoint peut opérer directement comme une vue matérialisée dérivée de `gold_routes`. Voir section 2 (Référentiel).


### 1.2 `POST /import/stations`

Import du référentiel des gares depuis les GTFS stops filtrés rail.

**Body (optionnel) :**
```json
{
  "source": "gtfs",
  "countries": ["FR", "DE", "AT"],
  "overwrite": false
}
```

**Logique :**
```sql
INSERT INTO ref_stations (station_name, city_name, country_code, parent_station)
SELECT DISTINCT
    departure_station,
    departure_city,
    departure_country,
    departure_parent_station
FROM gold_routes
WHERE departure_station IS NOT NULL
  AND mode = 'train'
ON CONFLICT (station_name, country_code) DO UPDATE
    SET city_name = EXCLUDED.city_name,
        parent_station = EXCLUDED.parent_station;
```


### 1.3 `POST /import/airports`

Import du référentiel aéroports depuis OurAirports.

**Body (optionnel) :**
```json
{
  "source": "ourairports",
  "file_path": "/data/raw/airports.csv"
}
```


### 1.4 `POST /import/routes/train`

Déclenchement de l'import des trajets ferroviaires (GTFS + BOTN). C'est le cœur du pipeline ETL.

**Body :**
```json
{
  "sources": ["gtfs", "botn"],
  "countries": ["FR", "DE"],
  "force_reprocess": false
}
```

**Réponse :**
```json
{
  "status": "success",
  "imported": 245000,
  "skipped": 12400,
  "duplicates_removed": 89000,
  "errors": [],
  "duration_seconds": 342
}
```


### 1.5 `POST /import/routes/flight`

Import des trajets aériens et calcul des distances/émissions.

**Body :**
```json
{
  "source": "ourairports",
  "distance_method": "haversine",
  "emission_source": "defra"
}
```


### 1.6 `POST /import/emissions`

Import ou mise à jour des facteurs d'émission CO₂ (EEA/Ember pour le rail, DEFRA/ICAO pour l'aérien).

**Body :**
```json
{
  "rail_source": "eea_ember",
  "flight_source": "defra",
  "year": 2024
}
```


### 1.7 `POST /import/full`

Pipeline complet : exécute séquentiellement tous les imports (cities → stations → airports → emissions → routes/train → routes/flight → compare).

**Body :**
```json
{
  "force_reprocess": false,
  "skip_stages": []
}
```

**Réponse :**
```json
{
  "status": "success",
  "stages": [
    { "stage": "cities", "imported": 8400, "duration_s": 12 },
    { "stage": "stations", "imported": 45000, "duration_s": 28 },
    { "stage": "airports", "imported": 3200, "duration_s": 8 },
    { "stage": "emissions", "imported": 164, "duration_s": 3 },
    { "stage": "routes_train", "imported": 245000, "duration_s": 342 },
    { "stage": "routes_flight", "imported": 18000, "duration_s": 87 },
    { "stage": "compare", "imported": 14000, "duration_s": 156 }
  ],
  "total_duration_seconds": 636
}
```

---

## 2. Référentiel — Villes, Gares, Aéroports

Endpoints de consultation des données de référence. Indispensables pour l'autocomplétion côté client, la validation des entrées utilisateur, et la navigation dans les données.

> **Implémentation :** Ces endpoints peuvent soit interroger des tables dédiées (`ref_cities`, `ref_stations`, `ref_airports`) si elles existent, soit dériver les données à la volée depuis `gold_routes` avec `SELECT DISTINCT`. La seconde approche est plus simple mais moins performante — à arbitrer selon le besoin de latence.


### 2.1 `GET /cities`

Liste des villes disponibles dans la base.

**Query params (tous optionnels) :**

| Param | Type | Description |
|---|---|---|
| `country` | string | Filtrer par pays (alpha-2) |
| `search` | string | Recherche par nom (`ILIKE`) |
| `has_station` | bool | Uniquement les villes avec au moins une gare |
| `has_airport` | bool | Uniquement les villes avec au moins un aéroport |
| `page` / `page_size` | int | Pagination |

**SQL :**
```sql
WITH city_data AS (
    SELECT
        departure_city AS city_name,
        departure_country AS country_code,
        COUNT(CASE WHEN mode = 'train' THEN 1 END) AS train_routes,
        COUNT(CASE WHEN mode = 'flight' THEN 1 END) AS flight_routes,
        COUNT(DISTINCT departure_station) AS nb_stations,
        COUNT(DISTINCT CASE WHEN mode = 'train' AND is_night_train = true THEN trip_id END) AS night_routes
    FROM gold_routes
    WHERE departure_city IS NOT NULL
    GROUP BY departure_city, departure_country
)
SELECT *,
    (nb_stations > 0) AS has_station,
    (flight_routes > 0) AS has_airport
FROM city_data
WHERE (:country IS NULL OR country_code = :country)
  AND (:search IS NULL  OR city_name ILIKE '%' || :search || '%')
  AND (:has_station IS NULL OR (nb_stations > 0) = :has_station)
  AND (:has_airport IS NULL OR (flight_routes > 0) = :has_airport)
ORDER BY city_name
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 2.2 `GET /cities/{country_code}`

Liste des villes d'un pays spécifique, avec métriques.

**Path param :** `country_code` (alpha-2, ex: `FR`)

**SQL :**
```sql
SELECT
    departure_city AS city_name,
    COUNT(*) AS total_routes,
    COUNT(CASE WHEN mode = 'train' THEN 1 END) AS train_routes,
    COUNT(CASE WHEN mode = 'flight' THEN 1 END) AS flight_routes,
    COUNT(DISTINCT departure_station) AS nb_stations,
    COUNT(DISTINCT arrival_country) AS connected_countries
FROM gold_routes
WHERE departure_country = :country_code
  AND departure_city IS NOT NULL
GROUP BY departure_city
ORDER BY total_routes DESC;
```


### 2.3 `GET /stations`

Liste des gares ferroviaires.

**Query params (tous optionnels) :**

| Param | Type | Description |
|---|---|---|
| `city` | string | Filtrer par ville (`ILIKE`) |
| `country` | string | Filtrer par pays (alpha-2) |
| `search` | string | Recherche par nom de gare (`ILIKE`) |
| `page` / `page_size` | int | Pagination |

**SQL :**
```sql
SELECT
    departure_station AS station_name,
    departure_city AS city_name,
    departure_country AS country_code,
    departure_parent_station AS parent_station,
    COUNT(*) AS nb_departures,
    COUNT(DISTINCT arrival_city) AS destinations_served,
    COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_departures
FROM gold_routes
WHERE mode = 'train'
  AND departure_station IS NOT NULL
  AND (:city IS NULL    OR departure_city ILIKE '%' || :city || '%')
  AND (:country IS NULL OR departure_country = :country)
  AND (:search IS NULL  OR departure_station ILIKE '%' || :search || '%')
GROUP BY departure_station, departure_city, departure_country, departure_parent_station
ORDER BY nb_departures DESC
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 2.4 `GET /stations/{country_code}/{city}`

Toutes les gares d'une ville donnée.

**SQL :**
```sql
SELECT
    departure_station AS station_name,
    departure_parent_station AS parent_station,
    COUNT(*) AS nb_departures,
    COUNT(DISTINCT arrival_city) AS destinations_served,
    COUNT(DISTINCT agency_name) AS operators,
    ARRAY_AGG(DISTINCT agency_name) FILTER (WHERE agency_name IS NOT NULL) AS operator_list,
    COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_departures,
    COUNT(CASE WHEN is_night_train = false THEN 1 END) AS day_departures
FROM gold_routes
WHERE mode = 'train'
  AND departure_country = :country_code
  AND departure_city ILIKE '%' || :city || '%'
  AND departure_station IS NOT NULL
GROUP BY departure_station, departure_parent_station
ORDER BY nb_departures DESC;
```


### 2.5 `GET /airports`

Liste des aéroports disponibles.

**Query params (tous optionnels) :**

| Param | Type | Description |
|---|---|---|
| `city` | string | Filtrer par ville (`ILIKE`) |
| `country` | string | Filtrer par pays (alpha-2) |
| `search` | string | Recherche par nom (`ILIKE`) |
| `page` / `page_size` | int | Pagination |

**SQL :**
```sql
SELECT
    departure_station AS airport_name,
    departure_city AS city_name,
    departure_country AS country_code,
    COUNT(*) AS nb_flights,
    COUNT(DISTINCT arrival_city) AS destinations_served,
    COUNT(DISTINCT arrival_country) AS countries_served
FROM gold_routes
WHERE mode = 'flight'
  AND departure_station IS NOT NULL
  AND (:city IS NULL    OR departure_city ILIKE '%' || :city || '%')
  AND (:country IS NULL OR departure_country = :country)
  AND (:search IS NULL  OR departure_station ILIKE '%' || :search || '%')
GROUP BY departure_station, departure_city, departure_country
ORDER BY nb_flights DESC
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 2.6 `GET /airports/{country_code}/{city}`

Tous les aéroports d'une ville donnée.

**SQL :**
```sql
SELECT
    departure_station AS airport_name,
    COUNT(*) AS nb_flights,
    COUNT(DISTINCT arrival_city) AS destinations_served,
    COUNT(DISTINCT arrival_country) AS countries_served,
    ROUND(AVG(distance_km)::numeric, 1) AS avg_flight_distance_km
FROM gold_routes
WHERE mode = 'flight'
  AND departure_country = :country_code
  AND departure_city ILIKE '%' || :city || '%'
  AND departure_station IS NOT NULL
GROUP BY departure_station
ORDER BY nb_flights DESC;
```

---

## 3. Consultation & Téléchargement

### 3.1 `GET /routes`

Consultation paginée de `gold_routes` avec filtrage fin sur chaque colonne.

**Query params (tous optionnels) :**

| Param | Type | Exemple | Description |
|---|---|---|---|
| `mode` | string | `train`, `flight` | Filtrer par mode de transport |
| `source` | string | `gtfs`, `botn` | Source des données |
| `departure_country` | string | `FR` | Code ISO alpha-2 du pays de départ |
| `arrival_country` | string | `DE` | Code ISO alpha-2 du pays d'arrivée |
| `departure_city` | string | `Paris` | Ville de départ (recherche `ILIKE`) |
| `arrival_city` | string | `Berlin` | Ville d'arrivée (recherche `ILIKE`) |
| `departure_station` | string | `Paris Gare de l'Est` | Gare de départ (recherche `ILIKE`) |
| `arrival_station` | string | `Berlin Hbf` | Gare d'arrivée (recherche `ILIKE`) |
| `agency_name` | string | `SNCF` | Opérateur (recherche `ILIKE`) |
| `route_type` | int | `2` | Code GTFS du type de route |
| `is_night_train` | bool | `true` | Filtrer jour (`false`) / nuit (`true`) |
| `days_of_week` | string | `1111100` | Pattern de jours de service (support du `LIKE`) |
| `min_distance_km` | float | `100` | Distance minimale |
| `max_distance_km` | float | `1500` | Distance maximale |
| `min_co2` | float | `0` | Émissions CO₂ minimales |
| `max_co2` | float | `50` | Émissions CO₂ maximales |
| `service_start_after` | date | `2025-01-01` | Service débutant après cette date |
| `service_end_before` | date | `2025-12-31` | Service terminant avant cette date |
| `sort_by` | string | `distance_km` | Colonne de tri |
| `sort_order` | string | `asc` / `desc` | Ordre du tri (défaut `asc`) |
| `page` | int | `1` | Page courante |
| `page_size` | int | `25` | Taille de la page |

**SQL :**
```sql
SELECT *
FROM gold_routes
WHERE 1=1
  AND (:mode IS NULL         OR mode = :mode)
  AND (:source IS NULL       OR source = :source)
  AND (:dep_country IS NULL  OR departure_country = :dep_country)
  AND (:arr_country IS NULL  OR arrival_country = :arr_country)
  AND (:dep_city IS NULL     OR departure_city ILIKE '%' || :dep_city || '%')
  AND (:arr_city IS NULL     OR arrival_city ILIKE '%' || :arr_city || '%')
  AND (:dep_station IS NULL  OR departure_station ILIKE '%' || :dep_station || '%')
  AND (:arr_station IS NULL  OR arrival_station ILIKE '%' || :arr_station || '%')
  AND (:agency IS NULL       OR agency_name ILIKE '%' || :agency || '%')
  AND (:route_type IS NULL   OR route_type = :route_type::text)
  AND (:is_night IS NULL     OR is_night_train = :is_night)
  AND (:days IS NULL         OR days_of_week LIKE :days)
  AND (:min_dist IS NULL     OR distance_km >= :min_dist)
  AND (:max_dist IS NULL     OR distance_km <= :max_dist)
  AND (:min_co2 IS NULL      OR emissions_co2 >= :min_co2)
  AND (:max_co2 IS NULL      OR emissions_co2 <= :max_co2)
  AND (:start_after IS NULL  OR service_start_date >= :start_after)
  AND (:end_before IS NULL   OR service_end_date <= :end_before)
ORDER BY COALESCE(:sort_by, 'departure_country')
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```

> **Note implémentation :** Le `ORDER BY` dynamique doit être géré côté applicatif (whitelist de colonnes) pour éviter les injections SQL. Ne pas injecter `:sort_by` directement dans la requête.


### 3.2 `GET /routes/download`

Téléchargement CSV de `gold_routes`. Mêmes filtres que `/routes`, sans pagination. Renvoie un `Content-Type: text/csv` avec header `Content-Disposition: attachment; filename="routes_export.csv"`.

**Limite :** max 500 000 lignes par export. Si le résultat dépasse, renvoyer `413` avec un message invitant à affiner les filtres.

**SQL :** Identique à 3.1 sans `LIMIT/OFFSET`, avec `LIMIT 500001` pour détecter le dépassement.


### 3.3 `GET /compare`

Consultation paginée de `gold_compare_best`.

**Query params (tous optionnels) :**

| Param | Type | Description |
|---|---|---|
| `departure_city` | string | Ville de départ (`ILIKE`) |
| `departure_country` | string | Pays de départ (alpha-2) |
| `arrival_city` | string | Ville d'arrivée (`ILIKE`) |
| `arrival_country` | string | Pays d'arrivée (alpha-2) |
| `best_mode` | string | `train` ou `flight` |
| `min_train_duration` | float | Durée train minimale (min) |
| `max_train_duration` | float | Durée train maximale (min) |
| `min_flight_duration` | float | Durée vol minimale (min) |
| `max_flight_duration` | float | Durée vol maximale (min) |
| `days_of_week` | string | Pattern de jours de service |
| `sort_by` | string | Colonne de tri |
| `sort_order` | string | `asc` / `desc` |
| `page` / `page_size` | int | Pagination |

**SQL :**
```sql
SELECT *
FROM gold_compare_best
WHERE 1=1
  AND (:dep_city IS NULL     OR departure_city ILIKE '%' || :dep_city || '%')
  AND (:dep_country IS NULL  OR departure_country = :dep_country)
  AND (:arr_city IS NULL     OR arrival_city ILIKE '%' || :arr_city || '%')
  AND (:arr_country IS NULL  OR arrival_country = :arr_country)
  AND (:best_mode IS NULL    OR best_mode = :best_mode)
  AND (:min_train_dur IS NULL OR train_duration_min >= :min_train_dur)
  AND (:max_train_dur IS NULL OR train_duration_min <= :max_train_dur)
  AND (:min_flt_dur IS NULL  OR flight_duration_min >= :min_flt_dur)
  AND (:max_flt_dur IS NULL  OR flight_duration_min <= :max_flt_dur)
  AND (:days IS NULL         OR days_of_week LIKE :days)
ORDER BY COALESCE(:sort_by, 'departure_country')
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 3.4 `GET /compare/download`

Export CSV de `gold_compare_best`. Mêmes filtres, même limite 500k lignes.

---

## 4. Recherche de Trajets

### 4.1 `GET /routes/search`

Recherche de trajets ferroviaires entre deux points (gares ou villes), dans les deux sens.

**Query params :**

| Param | Type | Requis | Description |
|---|---|---|---|
| `origin` | string | **oui** | Ville ou gare d'origine |
| `destination` | string | **oui** | Ville ou gare de destination |
| `date` | date | non | Date de service (filtre sur `service_start_date` / `service_end_date`) |
| `day_of_week` | int | non | Jour de la semaine (1=lundi, 7=dimanche) — filtre sur la position dans `days_of_week` |
| `is_night_train` | bool | non | Filtrer jour/nuit |
| `bidirectional` | bool | non | `true` (défaut) : recherche dans les deux sens |

**SQL :**
```sql
-- Sens aller
SELECT *, 'outbound' AS direction
FROM gold_routes
WHERE mode = 'train'
  AND (
    departure_city ILIKE '%' || :origin || '%'
    OR departure_station ILIKE '%' || :origin || '%'
  )
  AND (
    arrival_city ILIKE '%' || :destination || '%'
    OR arrival_station ILIKE '%' || :destination || '%'
  )
  AND (:date IS NULL OR (service_start_date <= :date AND service_end_date >= :date))
  AND (:dow IS NULL  OR SUBSTRING(days_of_week, :dow, 1) = '1')
  AND (:is_night IS NULL OR is_night_train = :is_night)

UNION ALL

-- Sens retour (si bidirectional = true)
SELECT *, 'return' AS direction
FROM gold_routes
WHERE mode = 'train'
  AND :bidirectional = true
  AND (
    departure_city ILIKE '%' || :destination || '%'
    OR departure_station ILIKE '%' || :destination || '%'
  )
  AND (
    arrival_city ILIKE '%' || :origin || '%'
    OR arrival_station ILIKE '%' || :origin || '%'
  )
  AND (:date IS NULL OR (service_start_date <= :date AND service_end_date >= :date))
  AND (:dow IS NULL  OR SUBSTRING(days_of_week, :dow, 1) = '1')
  AND (:is_night IS NULL OR is_night_train = :is_night)

ORDER BY direction, departure_time
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 4.2 `GET /routes/{trip_id}`

Détail complet d'un trajet spécifique.

**Path param :** `trip_id` (string)
**Query param optionnel :** `departure_country` (optimise la requête en ciblant la bonne partition)

**SQL :**
```sql
SELECT *
FROM gold_routes
WHERE trip_id = :trip_id
  AND (:dep_country IS NULL OR departure_country = :dep_country);
```

---

## 5. Émissions Carbone

### 5.1 `GET /carbon/trip/{trip_id}`

Bilan carbone d'un trajet spécifique.

**Réponse :**
```json
{
  "trip_id": "FR_IC_CLF_...",
  "mode": "train",
  "departure": "Clermont-Ferrand",
  "arrival": "Paris Bercy",
  "distance_km": 420,
  "co2_per_pkm": 3.2,
  "emissions_co2": 1344,
  "is_night_train": false
}
```

**SQL :**
```sql
SELECT trip_id, mode, departure_city, arrival_city,
       departure_country, arrival_country,
       distance_km, co2_per_pkm, emissions_co2, is_night_train
FROM gold_routes
WHERE trip_id = :trip_id;
```


### 5.2 `GET /carbon/estimate`

Estimation des émissions CO₂ pour un trajet entre deux villes, comparaison train vs avion.

**Query params :**

| Param | Type | Requis | Description |
|---|---|---|---|
| `origin` | string | **oui** | Ville de départ |
| `destination` | string | **oui** | Ville d'arrivée |

**SQL :**
```sql
SELECT
    mode,
    COUNT(*) AS nb_options,
    ROUND(AVG(distance_km)::numeric, 1) AS avg_distance_km,
    ROUND(AVG(co2_per_pkm)::numeric, 2) AS avg_co2_per_pkm,
    ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
    ROUND(MIN(emissions_co2)::numeric, 1) AS min_emissions_co2,
    ROUND(MAX(emissions_co2)::numeric, 1) AS max_emissions_co2
FROM gold_routes
WHERE (departure_city ILIKE '%' || :origin || '%')
  AND (arrival_city ILIKE '%' || :destination || '%')
GROUP BY mode;
```

**Réponse enrichie :**
```json
{
  "origin": "Paris",
  "destination": "Berlin",
  "comparison": [
    {
      "mode": "train",
      "nb_options": 12,
      "avg_distance_km": 1050.3,
      "avg_co2_per_pkm": 4.1,
      "avg_emissions_co2": 4306.2,
      "min_emissions_co2": 3800.0,
      "max_emissions_co2": 5100.0
    },
    {
      "mode": "flight",
      "nb_options": 8,
      "avg_distance_km": 880.0,
      "avg_co2_per_pkm": 145.0,
      "avg_emissions_co2": 127600.0,
      "min_emissions_co2": 118000.0,
      "max_emissions_co2": 140000.0
    }
  ],
  "co2_saving_pct": 96.6
}
```

Le champ `co2_saving_pct` est calculé côté applicatif : `(1 - avg_train / avg_flight) * 100`.


### 5.3 `GET /carbon/ranking`

Classement des paires O/D par économie de CO₂ du train vs avion.

**Query params :**

| Param | Type | Défaut | Description |
|---|---|---|---|
| `departure_country` | string | — | Filtrer par pays de départ |
| `min_distance_km` | float | — | Distance train minimale |
| `sort_by` | string | `co2_saving_pct` | `co2_saving_pct`, `train_emissions_co2`, `flight_emissions_co2` |
| `page` / `page_size` | int | 1 / 25 | Pagination |

**SQL (via `gold_compare_best`) :**
```sql
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
      (1 - train_emissions_co2 / NULLIF(flight_emissions_co2, 0)) * 100, 1
    ) AS co2_saving_pct,
    best_mode
FROM gold_compare_best
WHERE flight_emissions_co2 IS NOT NULL
  AND train_emissions_co2 IS NOT NULL
  AND flight_emissions_co2 > 0
  AND (:dep_country IS NULL OR departure_country = :dep_country)
  AND (:min_dist IS NULL    OR train_distance_km >= :min_dist)
ORDER BY co2_saving_pct DESC
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 5.4 `GET /carbon/factors`

Consultation des facteurs d'émission bruts utilisés dans les calculs. Endpoint de transparence méthodologique : permet à un utilisateur ou une ONG de vérifier les facteurs CO₂/pkm appliqués par pays, par mode et par type de service.

**Query params (tous optionnels) :**

| Param | Type | Description |
|---|---|---|
| `country` | string | Filtrer par pays (alpha-2) |
| `mode` | string | `train` ou `flight` |
| `is_night_train` | bool | Filtrer par type jour/nuit (train uniquement) |

**SQL :**
```sql
SELECT
    departure_country AS country_code,
    mode,
    is_night_train,
    COUNT(*) AS nb_routes_using_factor,
    ROUND(AVG(co2_per_pkm)::numeric, 4) AS avg_co2_per_pkm,
    ROUND(MIN(co2_per_pkm)::numeric, 4) AS min_co2_per_pkm,
    ROUND(MAX(co2_per_pkm)::numeric, 4) AS max_co2_per_pkm,
    ROUND(STDDEV(co2_per_pkm)::numeric, 4) AS stddev_co2_per_pkm,
    COUNT(DISTINCT co2_per_pkm) AS distinct_factors
FROM gold_routes
WHERE co2_per_pkm IS NOT NULL
  AND (:country IS NULL  OR departure_country = :country)
  AND (:mode IS NULL     OR mode = :mode)
  AND (:is_night IS NULL OR is_night_train = :is_night)
GROUP BY departure_country, mode, is_night_train
ORDER BY departure_country, mode, is_night_train;
```

**Réponse :**
```json
{
  "data": [
    {
      "country_code": "FR",
      "mode": "train",
      "is_night_train": false,
      "nb_routes_using_factor": 45200,
      "avg_co2_per_pkm": 3.1200,
      "min_co2_per_pkm": 2.8000,
      "max_co2_per_pkm": 5.4000,
      "stddev_co2_per_pkm": 0.8100,
      "distinct_factors": 3
    }
  ]
}
```

> **Note :** Si une table `co2_reference` dédiée existe dans le pipeline, un endpoint complémentaire `GET /carbon/factors/raw` peut exposer directement ses lignes (gCO₂e/kWh par pays/année, facteur de conversion kWh→pkm, etc.) pour une transparence totale.

---

## 6. Analyse Jour / Nuit

### 6.1 `GET /analysis/day-night/coverage`

Couverture des trains de jour vs nuit par pays.

**Query params optionnels :** `departure_country`

**SQL :**
```sql
SELECT
    departure_country,
    is_night_train,
    COUNT(*)                          AS nb_routes,
    COUNT(DISTINCT agency_name)       AS nb_agencies,
    COUNT(DISTINCT departure_city)    AS nb_dep_cities,
    COUNT(DISTINCT arrival_city)      AS nb_arr_cities,
    COUNT(DISTINCT arrival_country)   AS nb_destination_countries,
    ROUND(AVG(distance_km)::numeric, 1)     AS avg_distance_km,
    ROUND(AVG(emissions_co2)::numeric, 1)   AS avg_emissions_co2
FROM gold_routes
WHERE mode = 'train'
  AND (:dep_country IS NULL OR departure_country = :dep_country)
GROUP BY departure_country, is_night_train
ORDER BY departure_country, is_night_train;
```


### 6.2 `GET /analysis/day-night/emissions`

Comparaison des émissions CO₂ entre trains de jour et trains de nuit, par pays.

**SQL :**
```sql
SELECT
    departure_country,
    ROUND(AVG(CASE WHEN is_night_train = false THEN co2_per_pkm END)::numeric, 2) AS avg_co2_pkm_day,
    ROUND(AVG(CASE WHEN is_night_train = true  THEN co2_per_pkm END)::numeric, 2) AS avg_co2_pkm_night,
    ROUND(AVG(CASE WHEN is_night_train = false THEN emissions_co2 END)::numeric, 1) AS avg_total_co2_day,
    ROUND(AVG(CASE WHEN is_night_train = true  THEN emissions_co2 END)::numeric, 1) AS avg_total_co2_night,
    ROUND(AVG(CASE WHEN is_night_train = false THEN distance_km END)::numeric, 1) AS avg_dist_day,
    ROUND(AVG(CASE WHEN is_night_train = true  THEN distance_km END)::numeric, 1) AS avg_dist_night
FROM gold_routes
WHERE mode = 'train'
  AND (:dep_country IS NULL OR departure_country = :dep_country)
GROUP BY departure_country
HAVING COUNT(CASE WHEN is_night_train = true THEN 1 END) > 0
ORDER BY departure_country;
```


### 6.3 `GET /analysis/day-night/compare`

Comparaison jour vs nuit pour une paire origine/destination spécifique. Répond directement à la question « pour aller de A à B, vaut-il mieux prendre un train de jour ou de nuit en termes de CO₂ ? ».

**Query params :**

| Param | Type | Requis | Description |
|---|---|---|---|
| `origin` | string | **oui** | Ville de départ |
| `destination` | string | **oui** | Ville d'arrivée |

**SQL :**
```sql
SELECT
    is_night_train,
    COUNT(*) AS nb_options,
    ROUND(AVG(distance_km)::numeric, 1) AS avg_distance_km,
    ROUND(AVG(co2_per_pkm)::numeric, 2) AS avg_co2_per_pkm,
    ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
    ROUND(MIN(emissions_co2)::numeric, 1) AS min_emissions_co2,
    ROUND(MAX(emissions_co2)::numeric, 1) AS max_emissions_co2,
    MIN(departure_time) AS earliest_departure,
    MAX(departure_time) AS latest_departure,
    ARRAY_AGG(DISTINCT agency_name) FILTER (WHERE agency_name IS NOT NULL) AS operators
FROM gold_routes
WHERE mode = 'train'
  AND (departure_city ILIKE '%' || :origin || '%')
  AND (arrival_city ILIKE '%' || :destination || '%')
GROUP BY is_night_train;
```

**Réponse :**
```json
{
  "origin": "Paris",
  "destination": "Vienne",
  "day_train": {
    "nb_options": 3,
    "avg_distance_km": 1230.5,
    "avg_co2_per_pkm": 3.8,
    "avg_emissions_co2": 4675.9,
    "operators": ["SNCF", "DB"]
  },
  "night_train": {
    "nb_options": 2,
    "avg_distance_km": 1280.0,
    "avg_co2_per_pkm": 4.2,
    "avg_emissions_co2": 5376.0,
    "operators": ["ÖBB Nightjet"]
  },
  "best": "day"
}
```


### 6.4 `GET /analysis/day-night/routes`

Liste des routes avec leur classification jour/nuit et les métriques associées.

**Query params :** `departure_country`, `arrival_country`, `is_night_train`, `agency_name` (ILIKE), `min_distance_km`, `page`, `page_size`

**SQL :**
```sql
SELECT
    departure_city,
    departure_country,
    arrival_city,
    arrival_country,
    agency_name,
    route_long_name,
    is_night_train,
    days_of_week,
    departure_time,
    arrival_time,
    distance_km,
    co2_per_pkm,
    emissions_co2
FROM gold_routes
WHERE mode = 'train'
  AND (:dep_country IS NULL  OR departure_country = :dep_country)
  AND (:arr_country IS NULL  OR arrival_country = :arr_country)
  AND (:is_night IS NULL     OR is_night_train = :is_night)
  AND (:agency IS NULL       OR agency_name ILIKE '%' || :agency || '%')
  AND (:min_dist IS NULL     OR distance_km >= :min_dist)
ORDER BY departure_country, is_night_train, departure_time
LIMIT :page_size OFFSET (:page - 1) * :page_size;
```


### 6.5 `GET /analysis/day-night/summary`

Résumé agrégé jour/nuit au niveau européen.

**SQL :**
```sql
SELECT
    is_night_train,
    COUNT(*)                                    AS total_routes,
    COUNT(DISTINCT departure_country)           AS countries_served,
    COUNT(DISTINCT agency_name)                 AS operators,
    COUNT(DISTINCT departure_city || '-' || arrival_city) AS unique_od_pairs,
    ROUND(AVG(distance_km)::numeric, 1)         AS avg_distance_km,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY distance_km)::numeric, 1) AS median_distance_km,
    ROUND(AVG(co2_per_pkm)::numeric, 2)         AS avg_co2_per_pkm,
    ROUND(AVG(emissions_co2)::numeric, 1)       AS avg_emissions_co2,
    ROUND(SUM(emissions_co2)::numeric, 0)       AS total_emissions_co2
FROM gold_routes
WHERE mode = 'train'
GROUP BY is_night_train;
```

---

## 7. Qualité des Données

### 7.1 `GET /quality/completeness`

Taux de complétude de chaque colonne de `gold_routes`.

**SQL :**
```sql
SELECT
    COUNT(*) AS total_rows,
    ROUND(100.0 * COUNT(source)               / COUNT(*), 2) AS source_pct,
    ROUND(100.0 * COUNT(trip_id)              / COUNT(*), 2) AS trip_id_pct,
    ROUND(100.0 * COUNT(destination)          / COUNT(*), 2) AS destination_pct,
    ROUND(100.0 * COUNT(trip_short_name)      / COUNT(*), 2) AS trip_short_name_pct,
    ROUND(100.0 * COUNT(agency_name)          / COUNT(*), 2) AS agency_name_pct,
    ROUND(100.0 * COUNT(agency_timezone)      / COUNT(*), 2) AS agency_timezone_pct,
    ROUND(100.0 * COUNT(service_id)           / COUNT(*), 2) AS service_id_pct,
    ROUND(100.0 * COUNT(route_id)             / COUNT(*), 2) AS route_id_pct,
    ROUND(100.0 * COUNT(route_type)           / COUNT(*), 2) AS route_type_pct,
    ROUND(100.0 * COUNT(route_short_name)     / COUNT(*), 2) AS route_short_name_pct,
    ROUND(100.0 * COUNT(route_long_name)      / COUNT(*), 2) AS route_long_name_pct,
    ROUND(100.0 * COUNT(departure_station)    / COUNT(*), 2) AS departure_station_pct,
    ROUND(100.0 * COUNT(departure_city)       / COUNT(*), 2) AS departure_city_pct,
    ROUND(100.0 * COUNT(departure_time)       / COUNT(*), 2) AS departure_time_pct,
    ROUND(100.0 * COUNT(departure_parent_station) / COUNT(*), 2) AS departure_parent_station_pct,
    ROUND(100.0 * COUNT(arrival_station)      / COUNT(*), 2) AS arrival_station_pct,
    ROUND(100.0 * COUNT(arrival_city)         / COUNT(*), 2) AS arrival_city_pct,
    ROUND(100.0 * COUNT(arrival_country)      / COUNT(*), 2) AS arrival_country_pct,
    ROUND(100.0 * COUNT(arrival_time)         / COUNT(*), 2) AS arrival_time_pct,
    ROUND(100.0 * COUNT(arrival_parent_station) / COUNT(*), 2) AS arrival_parent_station_pct,
    ROUND(100.0 * COUNT(service_start_date)   / COUNT(*), 2) AS service_start_date_pct,
    ROUND(100.0 * COUNT(service_end_date)     / COUNT(*), 2) AS service_end_date_pct,
    ROUND(100.0 * COUNT(days_of_week)         / COUNT(*), 2) AS days_of_week_pct,
    ROUND(100.0 * COUNT(is_night_train)       / COUNT(*), 2) AS is_night_train_pct,
    ROUND(100.0 * COUNT(distance_km)          / COUNT(*), 2) AS distance_km_pct,
    ROUND(100.0 * COUNT(co2_per_pkm)          / COUNT(*), 2) AS co2_per_pkm_pct,
    ROUND(100.0 * COUNT(emissions_co2)        / COUNT(*), 2) AS emissions_co2_pct
FROM gold_routes;
```


### 7.2 `GET /quality/completeness/by-country`

Complétude ventilée par pays de départ (identifie les sources GTFS moins riches).

**SQL :**
```sql
SELECT
    departure_country,
    COUNT(*) AS total_rows,
    ROUND(100.0 * COUNT(departure_city)    / COUNT(*), 2) AS departure_city_pct,
    ROUND(100.0 * COUNT(arrival_city)      / COUNT(*), 2) AS arrival_city_pct,
    ROUND(100.0 * COUNT(arrival_country)   / COUNT(*), 2) AS arrival_country_pct,
    ROUND(100.0 * COUNT(days_of_week)      / COUNT(*), 2) AS days_of_week_pct,
    ROUND(100.0 * COUNT(distance_km)       / COUNT(*), 2) AS distance_km_pct,
    ROUND(100.0 * COUNT(emissions_co2)     / COUNT(*), 2) AS emissions_co2_pct,
    ROUND(100.0 * COUNT(is_night_train)    / COUNT(*), 2) AS is_night_train_pct,
    ROUND(100.0 * COUNT(trip_short_name)   / COUNT(*), 2) AS trip_short_name_pct
FROM gold_routes
WHERE mode = 'train'
GROUP BY departure_country
ORDER BY departure_country;
```


### 7.3 `GET /quality/coverage/countries`

Représentation des pays dans les données.

**SQL :**
```sql
WITH country_stats AS (
    SELECT
        departure_country,
        COUNT(*)                                          AS total_routes,
        COUNT(CASE WHEN mode = 'train' THEN 1 END)       AS train_routes,
        COUNT(CASE WHEN mode = 'flight' THEN 1 END)      AS flight_routes,
        COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_routes,
        COUNT(CASE WHEN is_night_train = false AND mode = 'train' THEN 1 END) AS day_routes,
        COUNT(DISTINCT agency_name)                       AS nb_agencies,
        COUNT(DISTINCT departure_city)                    AS nb_dep_cities,
        COUNT(DISTINCT arrival_country)                   AS nb_connected_countries
    FROM gold_routes
    GROUP BY departure_country
)
SELECT *,
    ROUND(100.0 * night_routes / NULLIF(train_routes, 0), 1) AS night_pct,
    ROUND(100.0 * flight_routes / NULLIF(total_routes, 0), 1) AS flight_pct
FROM country_stats
ORDER BY total_routes DESC;
```


### 7.4 `GET /quality/coverage/cities`

Villes les plus présentes dans la base, avec répartition jour/nuit/avion.

**Query params :** `departure_country` (optionnel), `role` = `departure` | `arrival` | `both` (défaut `both`), `top_n` (défaut 50)

**SQL (exemple pour `role=departure`) :**
```sql
SELECT
    departure_city AS city,
    departure_country AS country,
    COUNT(*) AS total_routes,
    COUNT(CASE WHEN mode = 'train' AND is_night_train = false THEN 1 END) AS day_train,
    COUNT(CASE WHEN mode = 'train' AND is_night_train = true  THEN 1 END) AS night_train,
    COUNT(CASE WHEN mode = 'flight' THEN 1 END)                           AS flight,
    COUNT(DISTINCT arrival_country) AS nb_destination_countries
FROM gold_routes
WHERE departure_city IS NOT NULL
  AND (:dep_country IS NULL OR departure_country = :dep_country)
GROUP BY departure_city, departure_country
ORDER BY total_routes DESC
LIMIT :top_n;
```


### 7.5 `GET /quality/schedules`

Analyse de la couverture des jours de service (horaires).

**SQL :**
```sql
SELECT
    departure_country,
    COUNT(CASE WHEN days_of_week = '1111100' THEN 1 END) AS weekday_only,
    COUNT(CASE WHEN days_of_week = '0000011' THEN 1 END) AS weekend_only,
    COUNT(CASE WHEN days_of_week = '1111111' THEN 1 END) AS daily,
    COUNT(CASE WHEN days_of_week IS NULL THEN 1 END)     AS no_schedule,
    COUNT(CASE WHEN days_of_week NOT IN ('1111100','0000011','1111111')
               AND days_of_week IS NOT NULL THEN 1 END)  AS other_pattern,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(CASE WHEN days_of_week IS NULL THEN 1 END) / COUNT(*), 2) AS null_schedule_pct
FROM gold_routes
WHERE mode = 'train'
GROUP BY departure_country
ORDER BY null_schedule_pct DESC;
```


### 7.6 `GET /quality/compare-coverage`

Taux de couverture de la comparaison train/avion : combien de trajets train ont un équivalent avion dans `gold_compare_best`.

**SQL :**
```sql
WITH train_routes AS (
    SELECT
        departure_country,
        COUNT(DISTINCT trip_id) AS total_train_trips
    FROM gold_routes
    WHERE mode = 'train'
    GROUP BY departure_country
),
compared AS (
    SELECT
        departure_country,
        COUNT(*) AS compared_trips,
        COUNT(CASE WHEN best_mode = 'train'  THEN 1 END) AS train_wins,
        COUNT(CASE WHEN best_mode = 'flight' THEN 1 END) AS flight_wins,
        COUNT(CASE WHEN flight_candidate_id IS NOT NULL THEN 1 END) AS with_flight_equiv
    FROM gold_compare_best
    GROUP BY departure_country
)
SELECT
    t.departure_country,
    t.total_train_trips,
    COALESCE(c.compared_trips, 0) AS compared_trips,
    ROUND(100.0 * COALESCE(c.compared_trips, 0) / NULLIF(t.total_train_trips, 0), 2) AS compare_coverage_pct,
    COALESCE(c.with_flight_equiv, 0) AS with_flight_equiv,
    COALESCE(c.train_wins, 0) AS train_wins,
    COALESCE(c.flight_wins, 0) AS flight_wins,
    ROUND(100.0 * COALESCE(c.train_wins, 0) / NULLIF(c.compared_trips, 0), 1) AS train_win_pct
FROM train_routes t
LEFT JOIN compared c USING (departure_country)
ORDER BY t.departure_country;
```


### 7.7 `GET /quality/day-night-balance`

Équilibre entre offre jour et nuit par pays et par corridors principaux.

**SQL :**
```sql
WITH corridor_stats AS (
    SELECT
        departure_country,
        arrival_country,
        COUNT(CASE WHEN is_night_train = false THEN 1 END) AS day_count,
        COUNT(CASE WHEN is_night_train = true  THEN 1 END) AS night_count,
        COUNT(*) AS total
    FROM gold_routes
    WHERE mode = 'train'
      AND arrival_country IS NOT NULL
      AND departure_country != arrival_country
    GROUP BY departure_country, arrival_country
)
SELECT *,
    ROUND(100.0 * night_count / NULLIF(total, 0), 1) AS night_share_pct,
    CASE
        WHEN night_count = 0 THEN 'day_only'
        WHEN day_count = 0   THEN 'night_only'
        ELSE 'mixed'
    END AS service_type
FROM corridor_stats
ORDER BY total DESC;
```


### 7.8 `GET /quality/summary`

Tableau de bord synthétique — un seul appel pour le dashboard qualité.

**SQL :**
```sql
SELECT
    -- Volume global
    (SELECT COUNT(*) FROM gold_routes) AS total_routes,
    (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train') AS total_train,
    (SELECT COUNT(*) FROM gold_routes WHERE mode = 'flight') AS total_flight,
    (SELECT COUNT(*) FROM gold_compare_best) AS total_comparisons,

    -- Couverture géographique
    (SELECT COUNT(DISTINCT departure_country) FROM gold_routes) AS countries_departure,
    (SELECT COUNT(DISTINCT arrival_country) FROM gold_routes WHERE arrival_country IS NOT NULL) AS countries_arrival,
    (SELECT COUNT(DISTINCT departure_city) FROM gold_routes WHERE departure_city IS NOT NULL) AS unique_dep_cities,
    (SELECT COUNT(DISTINCT arrival_city) FROM gold_routes WHERE arrival_city IS NOT NULL) AS unique_arr_cities,
    (SELECT COUNT(DISTINCT agency_name) FROM gold_routes WHERE agency_name IS NOT NULL) AS unique_agencies,

    -- Jour/Nuit
    (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = true) AS night_train_routes,
    (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = false) AS day_train_routes,
    (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train IS NULL) AS unclassified_routes,

    -- Complétude critique
    (SELECT ROUND(100.0 * COUNT(distance_km) / COUNT(*), 2) FROM gold_routes) AS distance_completeness_pct,
    (SELECT ROUND(100.0 * COUNT(emissions_co2) / COUNT(*), 2) FROM gold_routes) AS emissions_completeness_pct,
    (SELECT ROUND(100.0 * COUNT(days_of_week) / COUNT(*), 2) FROM gold_routes) AS schedule_completeness_pct,
    (SELECT ROUND(100.0 * COUNT(departure_city) / COUNT(*), 2) FROM gold_routes) AS dep_city_completeness_pct,
    (SELECT ROUND(100.0 * COUNT(arrival_city) / COUNT(*), 2) FROM gold_routes) AS arr_city_completeness_pct,

    -- Comparaison train/avion
    (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'train') AS train_wins_total,
    (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'flight') AS flight_wins_total;
```

---

## 8. Statistiques & Agrégations

### 8.1 `GET /stats/operators`

Classement des opérateurs ferroviaires par volume et couverture.

**SQL :**
```sql
SELECT
    agency_name,
    COUNT(*) AS total_routes,
    COUNT(DISTINCT departure_country) AS countries_served,
    COUNT(DISTINCT departure_city)    AS dep_cities,
    COUNT(DISTINCT arrival_city)      AS arr_cities,
    COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_routes,
    ROUND(AVG(distance_km)::numeric, 1) AS avg_distance_km,
    ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
    ROUND(MIN(distance_km)::numeric, 1) AS min_distance_km,
    ROUND(MAX(distance_km)::numeric, 1) AS max_distance_km
FROM gold_routes
WHERE mode = 'train'
  AND agency_name IS NOT NULL
GROUP BY agency_name
ORDER BY total_routes DESC;
```


### 8.2 `GET /stats/distances`

Distribution des distances par mode et type jour/nuit (utile pour les histogrammes du dashboard).

**Query params :** `mode` (optionnel), `departure_country` (optionnel), `bucket_size` (défaut 100 km)

**SQL :**
```sql
SELECT
    mode,
    is_night_train,
    FLOOR(distance_km / :bucket_size) * :bucket_size AS distance_bucket,
    COUNT(*) AS nb_routes,
    ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2
FROM gold_routes
WHERE distance_km IS NOT NULL
  AND (:mode IS NULL        OR mode = :mode)
  AND (:dep_country IS NULL OR departure_country = :dep_country)
GROUP BY mode, is_night_train, distance_bucket
ORDER BY mode, is_night_train, distance_bucket;
```


### 8.3 `GET /stats/emissions-by-distance`

Émissions moyennes par tranche de distance — alimente la courbe comparative train vs avion.

**SQL :**
```sql
SELECT
    mode,
    FLOOR(distance_km / 100) * 100 AS distance_range_start,
    FLOOR(distance_km / 100) * 100 + 99 AS distance_range_end,
    COUNT(*) AS nb_routes,
    ROUND(AVG(co2_per_pkm)::numeric, 2) AS avg_co2_per_pkm,
    ROUND(AVG(emissions_co2)::numeric, 1) AS avg_total_emissions
FROM gold_routes
WHERE distance_km IS NOT NULL
  AND emissions_co2 IS NOT NULL
GROUP BY mode, FLOOR(distance_km / 100)
ORDER BY mode, distance_range_start;
```

---

## 9. Index recommandés

Pour rendre ces requêtes performantes sur 93.6M lignes, les index suivants sont critiques :

```sql
-- Le partitionnement LIST sur departure_country couvre déjà les filtres par pays.
-- Les index ci-dessous sont créés sur la table parent et propagés aux partitions.

-- Recherche par ville (endpoints les plus fréquents)
CREATE INDEX idx_routes_dep_city ON gold_routes (departure_city);
CREATE INDEX idx_routes_arr_city ON gold_routes (arrival_city);

-- Filtres combinés fréquents
CREATE INDEX idx_routes_mode_night ON gold_routes (mode, is_night_train);
CREATE INDEX idx_routes_mode_depcountry ON gold_routes (mode, departure_country);

-- Lookup direct par trip_id
CREATE INDEX idx_routes_trip_id ON gold_routes (trip_id);

-- Table de comparaison
CREATE INDEX idx_compare_dep_city ON gold_compare_best (departure_city);
CREATE INDEX idx_compare_arr_city ON gold_compare_best (arrival_city);
CREATE INDEX idx_compare_best_mode ON gold_compare_best (best_mode);
CREATE INDEX idx_compare_trip_id ON gold_compare_best (trip_id);

-- Index GIN trigram pour les recherches ILIKE (autocomplétion, recherche fuzzy)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_routes_dep_city_trgm ON gold_routes USING gin (departure_city gin_trgm_ops);
CREATE INDEX idx_routes_arr_city_trgm ON gold_routes USING gin (arrival_city gin_trgm_ops);
CREATE INDEX idx_routes_dep_station_trgm ON gold_routes USING gin (departure_station gin_trgm_ops);
CREATE INDEX idx_routes_arr_station_trgm ON gold_routes USING gin (arrival_station gin_trgm_ops);
CREATE INDEX idx_routes_agency_trgm ON gold_routes USING gin (agency_name gin_trgm_ops);
```

---

## Récapitulatif des Endpoints

| # | Méthode | Endpoint | Rôle |
|---|---|---|---|
| **Import** | | | |
| 1.1 | POST | `/import/cities` | Import référentiel géographique |
| 1.2 | POST | `/import/stations` | Import référentiel gares |
| 1.3 | POST | `/import/airports` | Import référentiel aéroports |
| 1.4 | POST | `/import/routes/train` | Import trajets ferroviaires (GTFS + BOTN) |
| 1.5 | POST | `/import/routes/flight` | Import trajets aériens |
| 1.6 | POST | `/import/emissions` | Import facteurs d'émission CO₂ |
| 1.7 | POST | `/import/full` | Pipeline ETL complet |
| **Référentiel** | | | |
| 2.1 | GET | `/cities` | Liste des villes |
| 2.2 | GET | `/cities/{country_code}` | Villes d'un pays |
| 2.3 | GET | `/stations` | Liste des gares |
| 2.4 | GET | `/stations/{country_code}/{city}` | Gares d'une ville |
| 2.5 | GET | `/airports` | Liste des aéroports |
| 2.6 | GET | `/airports/{country_code}/{city}` | Aéroports d'une ville |
| **Consultation** | | | |
| 3.1 | GET | `/routes` | Consultation paginée gold_routes |
| 3.2 | GET | `/routes/download` | Export CSV gold_routes |
| 3.3 | GET | `/compare` | Consultation paginée gold_compare_best |
| 3.4 | GET | `/compare/download` | Export CSV gold_compare_best |
| **Recherche** | | | |
| 4.1 | GET | `/routes/search` | Recherche trajet bidirectionnelle |
| 4.2 | GET | `/routes/{trip_id}` | Détail d'un trajet |
| **Carbone** | | | |
| 5.1 | GET | `/carbon/trip/{trip_id}` | Bilan carbone d'un trajet |
| 5.2 | GET | `/carbon/estimate` | Estimation CO₂ pour une paire O/D |
| 5.3 | GET | `/carbon/ranking` | Classement paires par économie CO₂ |
| 5.4 | GET | `/carbon/factors` | Facteurs d'émission (transparence) |
| **Jour / Nuit** | | | |
| 6.1 | GET | `/analysis/day-night/coverage` | Couverture jour/nuit par pays |
| 6.2 | GET | `/analysis/day-night/emissions` | Émissions jour vs nuit par pays |
| 6.3 | GET | `/analysis/day-night/compare` | Comparaison jour/nuit pour une paire O/D |
| 6.4 | GET | `/analysis/day-night/routes` | Liste routes classifiées jour/nuit |
| 6.5 | GET | `/analysis/day-night/summary` | Résumé européen jour/nuit |
| **Qualité** | | | |
| 7.1 | GET | `/quality/completeness` | Complétude globale des colonnes |
| 7.2 | GET | `/quality/completeness/by-country` | Complétude par pays |
| 7.3 | GET | `/quality/coverage/countries` | Représentation des pays |
| 7.4 | GET | `/quality/coverage/cities` | Couverture des villes |
| 7.5 | GET | `/quality/schedules` | Analyse des patterns horaires |
| 7.6 | GET | `/quality/compare-coverage` | Couverture comparaison train/avion |
| 7.7 | GET | `/quality/day-night-balance` | Équilibre jour/nuit par corridor |
| 7.8 | GET | `/quality/summary` | Dashboard qualité synthétique |
| **Statistiques** | | | |
| 8.1 | GET | `/stats/operators` | Classement des opérateurs |
| 8.2 | GET | `/stats/distances` | Distribution des distances |
| 8.3 | GET | `/stats/emissions-by-distance` | Émissions par tranche de distance |
