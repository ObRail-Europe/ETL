# Documentation API — ObRail Europe

**Version** : 1.0.0  
**Base URL** : `https://<host>/api/v1`  
**Format** : JSON  
**Authentification** : Seul l'endpoint `/import` nécessite un Bearer token (voir section [Authentification](#authentification)). Les autres endpoints sont publics avec rate limiting par IP.

---

## Table des matières

- [Documentation API — ObRail Europe](#documentation-api--obrail-europe)
  - [Table des matières](#table-des-matières)
  - [Vue d'ensemble](#vue-densemble)
  - [Authentification](#authentification)
    - [Endpoints protégés](#endpoints-protégés)
    - [Configuration du token](#configuration-du-token)
    - [Utilisation du token](#utilisation-du-token)
    - [Réponses d'authentification](#réponses-dauthentification)
  - [Conventions](#conventions)
  - [Pagination](#pagination)
  - [Codes d'erreur](#codes-derreur)
  - [Référentiel](#référentiel)
    - [GET /cities](#get-cities)
    - [GET /cities/{country\_code}](#get-citiescountry_code)
    - [GET /stations](#get-stations)
    - [GET /stations/{country\_code}/{city}](#get-stationscountry_codecity)
    - [GET /airports](#get-airports)
    - [GET /airports/{country\_code}/{city}](#get-airportscountry_codecity)
  - [Recherche](#recherche)
    - [GET /routes/search](#get-routessearch)
    - [GET /routes/{trip\_id}](#get-routestrip_id)
  - [Consultation](#consultation)
    - [GET /routes](#get-routes)
    - [GET /routes/download](#get-routesdownload)
    - [GET /compare](#get-compare)
    - [GET /compare/download](#get-comparedownload)
  - [Carbone](#carbone)
    - [GET /carbon/trip/{trip\_id}](#get-carbontriptrip_id)
    - [GET /carbon/estimate](#get-carbonestimate)
    - [GET /carbon/ranking](#get-carbonranking)
    - [GET /carbon/factors](#get-carbonfactors)
  - [Analyse Jour/Nuit](#analyse-journuit)
    - [GET /analysis/day-night/coverage](#get-analysisday-nightcoverage)
    - [GET /analysis/day-night/emissions](#get-analysisday-nightemissions)
    - [GET /analysis/day-night/compare](#get-analysisday-nightcompare)
    - [GET /analysis/day-night/routes](#get-analysisday-nightroutes)
    - [GET /analysis/day-night/summary](#get-analysisday-nightsummary)
  - [Qualité](#qualité)
    - [GET /quality/completeness](#get-qualitycompleteness)
    - [GET /quality/completeness/by-country](#get-qualitycompletenessby-country)
    - [GET /quality/coverage/countries](#get-qualitycoveragecountries)
    - [GET /quality/coverage/cities](#get-qualitycoveragecities)
    - [GET /quality/schedules](#get-qualityschedules)
    - [GET /quality/compare-coverage](#get-qualitycompare-coverage)
    - [GET /quality/day-night-balance](#get-qualityday-night-balance)
    - [GET /quality/summary](#get-qualitysummary)
  - [Statistiques](#statistiques)
    - [GET /stats/operators](#get-statsoperators)
    - [GET /stats/distances](#get-statsdistances)
    - [GET /stats/emissions-by-distance](#get-statsemissions-by-distance)
  - [Administration](#administration)
    - [POST /import](#post-import)
    - [GET /health](#get-health)

---

## Vue d'ensemble

L'API ObRail Europe expose les données ferroviaires et aériennes européennes collectées et traitées par le pipeline ETL du projet. Elle permet d'interroger les dessertes ferroviaires, de comparer trains de jour et de nuit, d'estimer les émissions carbone et d'accéder aux indicateurs de qualité des données.

Les données couvrent les pays membres de l'Union Européenne ainsi que des pays voisins, avec une granularité allant du segment de trajet individuel jusqu'aux statistiques agrégées par pays.

---

## Authentification

### Endpoints protégés

Seul l'endpoint `/import` (POST) nécessite une authentification. Les autres endpoints sont publics.

### Configuration du token

Le token d'authentification est défini via la variable d'environnement `API_IMPORT_TOKEN` :

**Dans `.env` :**
```dotenv
API_IMPORT_TOKEN="votre_token_secret_ici"
```

**Dans Docker Compose :**
Le token est automatiquement chargé depuis `.env` :
```yaml
services:
  api:
    env_file: .env
```

### Utilisation du token

Transmettez le token via l'en-tête HTTP standard `Authorization: Bearer <token>` :

```bash
curl -X POST http://localhost:8000/api/v1/import \
  -H "Authorization: Bearer votre_token_secret_ici"
```

### Réponses d'authentification

- **Token valide** → Requête traitée (HTTP 202)
- **Token absent ou invalide** → `HTTP 403 Forbidden`

```json
{
  "detail": "Invalid or missing import token"
}
```

---

## Conventions

- Tous les codes pays sont en **ISO 3166-1 alpha-2** (2 lettres, ex. `FR`, `DE`, `IT`).
- Les dates sont au format **`YYYY-MM-DD`** (ex. `2025-06-01`).
- Les jours de la semaine suivent la numérotation **ISO 8601** : `1` = lundi, `7` = dimanche.
- Les distances sont en **kilomètres**, les émissions en **gCO₂eq/passager**.
- Les durées sont exprimées en **minutes**.

---

## Pagination

Tous les endpoints de liste supportent la pagination via les paramètres suivants :

| Paramètre   | Type    | Défaut | Description                        |
|-------------|---------|--------|------------------------------------|
| `page`      | integer | `1`    | Numéro de page (min. 1)            |
| `page_size` | integer | `25`   | Résultats par page (max. 500)      |

**Exemple de réponse paginée :**

```json
{
  "page": 1,
  "page_size": 25,
  "total": 312,
  "items": [ ... ]
}
```

---

## Codes d'erreur

| Code  | Signification                                           |
|-------|---------------------------------------------------------|
| `200` | Succès                                                  |
| `202` | Accepté (traitement asynchrone en cours)                |
| `422` | Erreur de validation des paramètres (détail dans le corps de la réponse) |
| `429` | Trop de requêtes (rate limiting IP actif)               |

**Exemple d'erreur 422 :**

```json
{
  "detail": [
    {
      "loc": ["query", "country"],
      "msg": "ensure this value has at most 2 characters",
      "type": "value_error.any_str.max_length"
    }
  ]
}
```

---

## Référentiel

### GET /cities

Liste des villes disponibles dans la base.

**Paramètres**

| Paramètre     | Type    | Requis | Description                              |
|---------------|---------|--------|------------------------------------------|
| `country`     | string  | Non    | Filtre par pays (code alpha-2, ex. `FR`) |
| `search`      | string  | Non    | Recherche textuelle sur le nom de ville  |
| `has_station` | boolean | Non    | Filtre les villes ayant une gare         |
| `has_airport` | boolean | Non    | Filtre les villes ayant un aéroport      |
| `page`        | integer | Non    | Page courante (défaut : 1)               |
| `page_size`   | integer | Non    | Taille de la page (défaut : 25, max : 500) |

**Exemples**

```http
# Toutes les villes françaises ayant une gare
GET /api/v1/cities?country=FR&has_station=true

# Recherche de villes contenant "Paris"
GET /api/v1/cities?search=Paris

# Villes avec à la fois une gare et un aéroport
GET /api/v1/cities?has_station=true&has_airport=true&page_size=50
```

---

### GET /cities/{country_code}

Liste des villes d'un pays spécifique avec métriques associées.

**Paramètres de chemin**

| Paramètre      | Type   | Description                   |
|----------------|--------|-------------------------------|
| `country_code` | string | Code pays alpha-2 (ex. `DE`)  |

**Exemple**

```http
# Toutes les villes d'Allemagne
GET /api/v1/cities/DE
```

---

### GET /stations

Liste des gares ferroviaires.

**Paramètres**

| Paramètre   | Type    | Requis | Description                              |
|-------------|---------|--------|------------------------------------------|
| `city`      | string  | Non    | Filtre par ville                         |
| `country`   | string  | Non    | Filtre par pays (code alpha-2)           |
| `search`    | string  | Non    | Recherche textuelle sur le nom de gare   |
| `page`      | integer | Non    | Page courante (défaut : 1)               |
| `page_size` | integer | Non    | Taille de la page (défaut : 25, max : 500) |

**Exemples**

```http
# Gares de Berlin
GET /api/v1/stations?city=Berlin&country=DE

# Recherche de gares contenant "Nord"
GET /api/v1/stations?search=Nord

# Toutes les gares italiennes
GET /api/v1/stations?country=IT&page_size=100
```

---

### GET /stations/{country_code}/{city}

Toutes les gares d'une ville donnée.

**Paramètres de chemin**

| Paramètre      | Type   | Description           |
|----------------|--------|-----------------------|
| `country_code` | string | Code pays alpha-2     |
| `city`         | string | Nom de la ville       |

**Exemple**

```http
# Toutes les gares de Paris
GET /api/v1/stations/FR/Paris
```

---

### GET /airports

Liste des aéroports disponibles.

**Paramètres**

| Paramètre   | Type    | Requis | Description                                |
|-------------|---------|--------|--------------------------------------------|
| `city`      | string  | Non    | Filtre par ville                           |
| `country`   | string  | Non    | Filtre par pays (code alpha-2)             |
| `search`    | string  | Non    | Recherche textuelle sur le nom d'aéroport  |
| `page`      | integer | Non    | Page courante (défaut : 1)                 |
| `page_size` | integer | Non    | Taille de la page (défaut : 25, max : 500) |

**Exemple**

```http
# Aéroports en Espagne
GET /api/v1/airports?country=ES

# Aéroports de Rome
GET /api/v1/airports?city=Rome&country=IT
```

---

### GET /airports/{country_code}/{city}

Tous les aéroports d'une ville donnée.

**Paramètres de chemin**

| Paramètre      | Type   | Description       |
|----------------|--------|-------------------|
| `country_code` | string | Code pays alpha-2 |
| `city`         | string | Nom de la ville   |

**Exemple**

```http
# Aéroports de Londres
GET /api/v1/airports/GB/London
```

---

## Recherche

### GET /routes/search

Recherche de trajets ferroviaires entre deux points, dans les deux sens.

**Paramètres**

| Paramètre       | Type    | Requis | Description                                         |
|-----------------|---------|--------|-----------------------------------------------------|
| `origin`        | string  | **Oui**| Ville ou gare de départ                             |
| `destination`   | string  | **Oui**| Ville ou gare d'arrivée                             |
| `date`          | string  | Non    | Date de voyage (`YYYY-MM-DD`)                       |
| `day_of_week`   | integer | Non    | Jour de la semaine (1=lundi … 7=dimanche)           |
| `is_night_train`| boolean | Non    | `true` = trains de nuit uniquement                  |
| `bidirectional` | boolean | Non    | Inclure les trajets retour (défaut : `true`)        |
| `page`          | integer | Non    | Page courante (défaut : 1)                          |
| `page_size`     | integer | Non    | Taille de la page (défaut : 25, max : 500)          |

**Exemples**

```http
# Trajets Paris → Barcelone
GET /api/v1/routes/search?origin=Paris&destination=Barcelona

# Trains de nuit Paris → Vienne le vendredi
GET /api/v1/routes/search?origin=Paris&destination=Vienna&day_of_week=5&is_night_train=true

# Trajets un jour précis, sens unique
GET /api/v1/routes/search?origin=Brussels&destination=Amsterdam&date=2025-07-15&bidirectional=false
```

---

### GET /routes/{trip_id}

Détail d'un trajet spécifique — liste tous les segments associés à un `trip_id`.

> Un même `trip_id` peut correspondre à plusieurs segments O/D (ex. Paris→Nîmes, Paris→Montpellier, Nîmes→Montpellier sur un même trajet physique).

**Paramètres de chemin**

| Paramètre | Type   | Description        |
|-----------|--------|--------------------|
| `trip_id` | string | Identifiant du trip |

**Paramètres de requête**

| Paramètre          | Type   | Requis | Description                              |
|--------------------|--------|--------|------------------------------------------|
| `source`           | string | Non    | Filtre par source de données (opérateur GTFS) |
| `departure_country`| string | Non    | Filtre par pays de départ (alpha-2)      |

**Exemple**

```http
# Segments du trip SNCF-TGV-8542
GET /api/v1/routes/SNCF-TGV-8542

# Avec filtre source
GET /api/v1/routes/SNCF-TGV-8542?source=SNCF&departure_country=FR
```

---

## Consultation

### GET /routes

Consultation paginée de `gold_routes` avec filtrage fin sur l'ensemble des attributs.

**Paramètres**

| Paramètre            | Type    | Requis | Description                                      |
|----------------------|---------|--------|--------------------------------------------------|
| `mode`               | string  | Non    | `train` ou `flight`                              |
| `source`             | string  | Non    | Opérateur / source de données                    |
| `departure_country`  | string  | Non    | Code pays de départ (alpha-2)                    |
| `arrival_country`    | string  | Non    | Code pays d'arrivée (alpha-2)                    |
| `departure_city`     | string  | Non    | Ville de départ                                  |
| `arrival_city`       | string  | Non    | Ville d'arrivée                                  |
| `departure_station`  | string  | Non    | Gare de départ                                   |
| `arrival_station`    | string  | Non    | Gare d'arrivée                                   |
| `agency_name`        | string  | Non    | Nom de l'agence / opérateur ferroviaire          |
| `route_type`         | integer | Non    | Type de route GTFS (ex. `2` = rail, `100–109`)   |
| `is_night_train`     | boolean | Non    | Filtrer sur les trains de nuit                   |
| `days_of_week`       | string  | Non    | Jours de circulation (chaîne de bits, ex. `1111100` = lun–ven) |
| `min_distance_km`    | number  | Non    | Distance minimale (km)                           |
| `max_distance_km`    | number  | Non    | Distance maximale (km)                           |
| `min_co2`            | number  | Non    | Émissions CO₂ minimales (gCO₂eq/passager)        |
| `max_co2`            | number  | Non    | Émissions CO₂ maximales (gCO₂eq/passager)        |
| `service_start_after`| string  | Non    | Début de service après cette date (`YYYY-MM-DD`) |
| `service_end_before` | string  | Non    | Fin de service avant cette date (`YYYY-MM-DD`)   |
| `sort_by`            | string  | Non    | Colonne de tri                                   |
| `sort_order`         | string  | Non    | `asc` ou `desc`                                  |
| `page`               | integer | Non    | Page courante (défaut : 1)                       |
| `page_size`          | integer | Non    | Taille de la page (défaut : 25, max : 500)       |

**Exemples**

```http
# Tous les trains de nuit au départ d'Autriche, triés par distance
GET /api/v1/routes?mode=train&is_night_train=true&departure_country=AT&sort_by=distance_km&sort_order=desc

# Trajets transfrontaliers France → Italie sur plus de 500 km
GET /api/v1/routes?departure_country=FR&arrival_country=IT&min_distance_km=500

# Trains circulant le week-end (samedi=6, dimanche=7) avec faibles émissions
GET /api/v1/routes?mode=train&days_of_week=0000011&max_co2=50&page_size=100

# Réseau ÖBB Nightjet complet
GET /api/v1/routes?agency_name=ÖBB&is_night_train=true
```

---

### GET /routes/download

Export CSV de `gold_routes`. Supporte les mêmes filtres que `GET /routes`. **Limite : 500 000 lignes.**

**Exemple**

```http
# Télécharger tous les trains de nuit européens en CSV
GET /api/v1/routes/download?mode=train&is_night_train=true

# Export France → Italie trié par CO₂
GET /api/v1/routes/download?departure_country=FR&arrival_country=IT&sort_by=co2&sort_order=asc
```

> La réponse est un fichier CSV (`Content-Type: text/csv`).

---

### GET /compare

Consultation paginée de `gold_compare_best` — comparaison meilleur train / meilleur vol sur chaque paire O/D.

**Paramètres**

| Paramètre             | Type    | Requis | Description                                  |
|-----------------------|---------|--------|----------------------------------------------|
| `departure_city`      | string  | Non    | Ville de départ                              |
| `departure_country`   | string  | Non    | Code pays de départ (alpha-2)                |
| `arrival_city`        | string  | Non    | Ville d'arrivée                              |
| `arrival_country`     | string  | Non    | Code pays d'arrivée (alpha-2)                |
| `best_mode`           | string  | Non    | Filtrer sur le mode le plus écologique : `train` ou `flight` |
| `min_train_duration`  | number  | Non    | Durée minimale du trajet en train (minutes)  |
| `max_train_duration`  | number  | Non    | Durée maximale du trajet en train (minutes)  |
| `min_flight_duration` | number  | Non    | Durée minimale du vol (minutes)              |
| `max_flight_duration` | number  | Non    | Durée maximale du vol (minutes)              |
| `days_of_week`        | string  | Non    | Jours de circulation                         |
| `sort_by`             | string  | Non    | Colonne de tri                               |
| `sort_order`          | string  | Non    | `asc` ou `desc`                              |
| `page`                | integer | Non    | Page courante (défaut : 1)                   |
| `page_size`           | integer | Non    | Taille de la page (défaut : 25, max : 500)   |

**Exemples**

```http
# Paires O/D où le train est le mode le plus écologique
GET /api/v1/compare?best_mode=train

# Comparaisons au départ d'Espagne avec train sous 4h (240 min)
GET /api/v1/compare?departure_country=ES&max_train_duration=240

# Corridors France → Allemagne triés par durée train croissante
GET /api/v1/compare?departure_country=FR&arrival_country=DE&sort_by=train_duration&sort_order=asc
```

---

### GET /compare/download

Export CSV de `gold_compare_best`. Supporte les mêmes filtres que `GET /compare`. **Limite : 500 000 lignes.**

**Exemple**

```http
# Export de toutes les comparaisons où le train gagne
GET /api/v1/compare/download?best_mode=train
```

---

## Carbone

### GET /carbon/trip/{trip_id}

Bilan carbone des segments d'un trajet spécifique.

**Paramètres de chemin**

| Paramètre | Type   | Description         |
|-----------|--------|---------------------|
| `trip_id` | string | Identifiant du trip |

**Paramètres de requête**

| Paramètre           | Type   | Requis | Description                            |
|---------------------|--------|--------|----------------------------------------|
| `source`            | string | Non    | Filtre par source de données           |
| `departure_country` | string | Non    | Filtre par pays de départ (alpha-2)    |

**Exemple**

```http
# Bilan carbone du trajet OBB-NJ-467
GET /api/v1/carbon/trip/OBB-NJ-467

# Avec contexte pays
GET /api/v1/carbon/trip/OBB-NJ-467?departure_country=AT
```

---

### GET /carbon/estimate

Estimation CO₂ pour un trajet, avec comparaison train vs avion.

Utilise `gold_compare_best` pour retrouver les corridors O/D correspondants et retourne les statistiques agrégées par mode ainsi que le mode le plus écologique.

**Paramètres**

| Paramètre     | Type   | Requis  | Description              |
|---------------|--------|---------|--------------------------|
| `origin`      | string | **Oui** | Ville d'origine          |
| `destination` | string | **Oui** | Ville de destination     |

**Exemples**

```http
# Estimation carbone Paris → Rome
GET /api/v1/carbon/estimate?origin=Paris&destination=Rome

# Estimation Amsterdam → Madrid
GET /api/v1/carbon/estimate?origin=Amsterdam&destination=Madrid
```

**Exemple de réponse**

```json
{
  "origin": "Paris",
  "destination": "Rome",
  "corridors_matched": 3,
  "train": {
    "avg_co2_gpp": 4.2,
    "min_co2_gpp": 3.8,
    "max_co2_gpp": 5.1
  },
  "flight": {
    "avg_co2_gpp": 187.4,
    "min_co2_gpp": 174.0,
    "max_co2_gpp": 201.2
  },
  "best_mode": "train",
  "co2_saving_pct": 97.8
}
```

---

### GET /carbon/ranking

Classement des paires O/D par économie de CO₂ du train par rapport à l'avion.

**Paramètres**

| Paramètre           | Type    | Requis | Description                                                             |
|---------------------|---------|--------|-------------------------------------------------------------------------|
| `departure_country` | string  | Non    | Filtre par pays de départ (alpha-2)                                     |
| `min_distance_km`   | number  | Non    | Distance minimale (km)                                                  |
| `sort_by`           | string  | Non    | `co2_saving_pct` (défaut), `train_emissions_co2`, `flight_emissions_co2`|
| `page`              | integer | Non    | Page courante (défaut : 1)                                              |
| `page_size`         | integer | Non    | Taille de la page (défaut : 25, max : 500)                              |

**Exemples**

```http
# Top corridors en économie carbone, toutes distances
GET /api/v1/carbon/ranking?sort_by=co2_saving_pct

# Corridors longs (>800 km) depuis la France
GET /api/v1/carbon/ranking?departure_country=FR&min_distance_km=800

# Classement par émissions train les plus basses
GET /api/v1/carbon/ranking?sort_by=train_emissions_co2
```

---

### GET /carbon/factors

Liste les facteurs d'émission utilisés dans les calculs, par pays.

**Paramètres**

| Paramètre       | Type    | Requis | Description                                |
|-----------------|---------|--------|--------------------------------------------|
| `country`       | string  | Non    | Filtre par pays (code alpha-2)             |
| `mode`          | string  | Non    | `train` ou `flight`                        |
| `is_night_train`| boolean | Non    | Facteurs spécifiques aux trains de nuit    |

**Exemples**

```http
# Tous les facteurs d'émission ferroviaires
GET /api/v1/carbon/factors?mode=train

# Facteurs pour la Suède (réseau très décarboné)
GET /api/v1/carbon/factors?country=SE

# Facteurs trains de nuit uniquement
GET /api/v1/carbon/factors?mode=train&is_night_train=true
```

---

## Analyse Jour/Nuit

### GET /analysis/day-night/coverage

Couverture des trains de jour vs trains de nuit par pays — nombre de routes, de trips, distances moyennes.

**Paramètres**

| Paramètre           | Type   | Requis | Description                         |
|---------------------|--------|--------|-------------------------------------|
| `departure_country` | string | Non    | Filtre par pays de départ (alpha-2) |

**Exemple**

```http
# Couverture jour/nuit en Europe (vue globale)
GET /api/v1/analysis/day-night/coverage

# Couverture pour l'Italie uniquement
GET /api/v1/analysis/day-night/coverage?departure_country=IT
```

---

### GET /analysis/day-night/emissions

Comparaison des émissions CO₂ moyennes entre trains de jour et trains de nuit, par pays.

**Paramètres**

| Paramètre           | Type   | Requis | Description                         |
|---------------------|--------|--------|-------------------------------------|
| `departure_country` | string | Non    | Filtre par pays de départ (alpha-2) |

**Exemple**

```http
# Comparaison émissions jour/nuit pour tous les pays
GET /api/v1/analysis/day-night/emissions

# Focus Autriche (ÖBB)
GET /api/v1/analysis/day-night/emissions?departure_country=AT
```

---

### GET /analysis/day-night/compare

Comparaison jour vs nuit pour une paire O/D spécifique.

**Paramètres**

| Paramètre     | Type   | Requis  | Description          |
|---------------|--------|---------|----------------------|
| `origin`      | string | **Oui** | Ville d'origine      |
| `destination` | string | **Oui** | Ville de destination |

**Exemples**

```http
# Comparaison jour/nuit Paris → Berlin
GET /api/v1/analysis/day-night/compare?origin=Paris&destination=Berlin

# Comparaison Vienne → Bruxelles
GET /api/v1/analysis/day-night/compare?origin=Vienna&destination=Brussels
```

---

### GET /analysis/day-night/routes

Liste des routes avec classification jour/nuit et leurs métadonnées.

**Paramètres**

| Paramètre           | Type    | Requis | Description                                |
|---------------------|---------|--------|--------------------------------------------|
| `departure_country` | string  | Non    | Filtre par pays de départ (alpha-2)        |
| `arrival_country`   | string  | Non    | Filtre par pays d'arrivée (alpha-2)        |
| `is_night_train`    | boolean | Non    | Restreindre aux trains de nuit             |
| `agency_name`       | string  | Non    | Filtrer par opérateur                      |
| `min_distance_km`   | number  | Non    | Distance minimale (km)                     |
| `page`              | integer | Non    | Page courante (défaut : 1)                 |
| `page_size`         | integer | Non    | Taille de la page (défaut : 25, max : 500) |

**Exemples**

```http
# Toutes les routes de nuit transfrontalières depuis l'Autriche
GET /api/v1/analysis/day-night/routes?departure_country=AT&is_night_train=true

# Routes longue distance (>1000 km) avec classification
GET /api/v1/analysis/day-night/routes?min_distance_km=1000

# Réseau Nightjet complet
GET /api/v1/analysis/day-night/routes?agency_name=ÖBB&is_night_train=true&page_size=100
```

---

### GET /analysis/day-night/summary

Résumé agrégé jour/nuit au niveau européen — indicateurs globaux consolidés.

```http
GET /api/v1/analysis/day-night/summary
```

---

## Qualité

Les endpoints de qualité permettent de monitorer la complétude et la cohérence des données chargées dans `gold_routes`.

### GET /quality/completeness

Taux de complétude (% de valeurs non nulles) de chaque colonne de `gold_routes`.

```http
GET /api/v1/quality/completeness
```

---

### GET /quality/completeness/by-country

Complétude ventilée par pays de départ — permet d'identifier les pays avec des données incomplètes.

```http
GET /api/v1/quality/completeness/by-country
```

---

### GET /quality/coverage/countries

Représentation des pays dans les données — nombre de routes par pays, part relative.

```http
GET /api/v1/quality/coverage/countries
```

---

### GET /quality/coverage/cities

Villes les plus présentes dans la base, en tant que départ ou arrivée.

**Paramètres**

| Paramètre           | Type    | Requis | Description                                                 |
|---------------------|---------|--------|-------------------------------------------------------------|
| `departure_country` | string  | Non    | Filtre par pays de départ (alpha-2)                         |
| `role`              | string  | Non    | `departure`, `arrival` ou `both` (défaut : `both`)          |
| `top_n`             | integer | Non    | Nombre de villes à retourner (défaut : 50, max : 500)       |

**Exemples**

```http
# Top 20 villes de départ toutes origines
GET /api/v1/quality/coverage/cities?role=departure&top_n=20

# Top hubs d'arrivée pour les trajets partant d'Allemagne
GET /api/v1/quality/coverage/cities?departure_country=DE&role=arrival&top_n=10
```

---

### GET /quality/schedules

Analyse de la couverture des jours de service — détecte les jours peu couverts ou les patterns calendaires inhabituels.

```http
GET /api/v1/quality/schedules
```

---

### GET /quality/compare-coverage

Taux de couverture de la comparaison train/avion — proportion des corridors ferroviaires pour lesquels un vol équivalent a pu être trouvé.

```http
GET /api/v1/quality/compare-coverage
```

---

### GET /quality/day-night-balance

Équilibre entre offre jour et offre nuit, par pays et par corridors.

```http
GET /api/v1/quality/day-night-balance
```

---

### GET /quality/summary

Tableau de bord synthétique qualité — regroupe les principaux indicateurs en une seule réponse.

```http
GET /api/v1/quality/summary
```

---

## Statistiques

### GET /stats/operators

Classement des opérateurs ferroviaires par volume de trips et couverture géographique.

```http
GET /api/v1/stats/operators
```

---

### GET /stats/distances

Distribution des distances par mode de transport et par type (jour/nuit).

**Paramètres**

| Paramètre           | Type    | Requis | Description                                        |
|---------------------|---------|--------|----------------------------------------------------|
| `mode`              | string  | Non    | `train` ou `flight`                                |
| `departure_country` | string  | Non    | Filtre par pays de départ (alpha-2)                |
| `bucket_size`       | integer | Non    | Largeur des tranches en km (défaut : 100, max : 1000) |

**Exemples**

```http
# Distribution des distances ferroviaires par tranches de 200 km
GET /api/v1/stats/distances?mode=train&bucket_size=200

# Distribution pour les vols depuis la France
GET /api/v1/stats/distances?mode=flight&departure_country=FR
```

---

### GET /stats/emissions-by-distance

Émissions CO₂ moyennes (train et avion) par tranche de distance — permet de visualiser à partir de quelle distance le train devient moins avantageux.

```http
GET /api/v1/stats/emissions-by-distance
```

---

## Administration

### POST /import

Déclenche la pipeline ETL complète (extraction → transformation → chargement) en arrière-plan. La requête est acceptée immédiatement (`HTTP 202`) et le traitement s'exécute de façon asynchrone.

> **Authentification requise :** Bearer token via l'en-tête `Authorization: Bearer <token>`

```http
POST /api/v1/import
Authorization: Bearer default_dev_token_change_in_production
```

**Réponse (HTTP 202)**

```json
{
  "status": "started",
  "message": "Pipeline ETL déclenchée en arrière-plan."
}
```

**Exemple complet**

```bash
# Requête valide
curl -X POST http://localhost:8000/api/v1/import \
  -H "Authorization: Bearer default_dev_token_change_in_production" \
  -H "Content-Type: application/json" \
  -w "\nHTTP Status: %{http_code}\n"

# Réponse attendue : HTTP 202 Accepted
{"status": "started", "message": "Pipeline ETL déclenchée en arrière-plan."}
```

```bash
# Requête sans token
curl -X POST http://localhost:8000/api/v1/import \
  -w "\nHTTP Status: %{http_code}\n"

# Réponse attendue : HTTP 403 Forbidden
{"detail": "Invalid or missing import token"}
```

---

### GET /health

Vérification de l'état de l'API. Cet endpoint est exempté du rate limiting.

```http
GET /api/v1/health
```

**Réponse attendue**

```json
{
  "status": "ok",
  "timestamp": "2025-06-01T12:00:00Z"
}
```

---

*Documentation pour ObRail Europe et utilisateur tier — API v1.0.0*