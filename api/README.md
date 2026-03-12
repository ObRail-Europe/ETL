# ObRail Europe — API REST

API FastAPI exposant les données ferroviaires et aériennes d'ObRail Europe.  
Spec complète : 40 endpoints couvrant import, référentiel, consultation, carbone, analyse jour/nuit, qualité et statistiques.

---

## Démarrage rapide

```bash
# 1. Copier la config
cp .env.example .env

# 2. Adapter les variables si besoin (mot de passe PG, API_KEY)
nano .env

# 3. Lancer API + PostgreSQL
docker compose up --build -d

# 4. Vérifier
curl http://localhost:8000/health
# → {"status":"ok","version":"1.0.0"}

# 5. Documentation interactive
open http://localhost:8000/docs
```

---

## Structure du projet

```
obrail_api/
├── app/
│   ├── main.py                  # Entrypoint FastAPI
│   ├── core/
│   │   ├── config.py            # Settings (pydantic-settings, .env)
│   │   ├── database.py          # Connexion asyncpg / SQLAlchemy async
│   │   ├── schemas.py           # Modèles Pydantic partagés + helpers
│   │   └── security.py          # Auth API Key (POST /import/*)
│   └── routers/
│       ├── import_routes.py     # Section 1  – POST /import/*
│       ├── referentiel.py       # Section 2  – GET /cities, /stations, /airports
│       ├── routes.py            # Sections 3+4 – consultation, download, search
│       ├── carbon.py            # Section 5  – GET /carbon/*
│       ├── analysis.py          # Section 6  – GET /analysis/day-night/*
│       └── quality_stats.py     # Sections 7+8 – qualité + stats
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Endpoints — récapitulatif

| # | Méthode | URL | Description |
|---|---------|-----|-------------|
| **Import** (🔒 X-API-Key) | | | |
| 1.1 | POST | `/api/v1/import/cities` | Import référentiel villes |
| 1.2 | POST | `/api/v1/import/stations` | Import référentiel gares |
| 1.3 | POST | `/api/v1/import/airports` | Import référentiel aéroports |
| 1.4 | POST | `/api/v1/import/routes/train` | Compte-rendu trajets ferroviaires |
| 1.5 | POST | `/api/v1/import/routes/flight` | Compte-rendu trajets aériens |
| 1.6 | POST | `/api/v1/import/emissions` | Compte-rendu facteurs CO₂ |
| 1.7 | POST | `/api/v1/import/full` | Pipeline complète |
| **Référentiel** | | | |
| 2.1 | GET | `/api/v1/cities` | Liste des villes |
| 2.2 | GET | `/api/v1/cities/{country_code}` | Villes d'un pays |
| 2.3 | GET | `/api/v1/stations` | Liste des gares |
| 2.4 | GET | `/api/v1/stations/{country_code}/{city}` | Gares d'une ville |
| 2.5 | GET | `/api/v1/airports` | Liste des aéroports |
| 2.6 | GET | `/api/v1/airports/{country_code}/{city}` | Aéroports d'une ville |
| **Consultation** | | | |
| 3.1 | GET | `/api/v1/routes` | gold_routes paginé + filtres |
| 3.2 | GET | `/api/v1/routes/download` | Export CSV (max 500k lignes) |
| 3.3 | GET | `/api/v1/compare` | gold_compare_best paginé |
| 3.4 | GET | `/api/v1/compare/download` | Export CSV compare |
| **Recherche** | | | |
| 4.1 | GET | `/api/v1/routes/search` | Recherche O/D bidirectionnelle |
| 4.2 | GET | `/api/v1/routes/{trip_id}` | Détail d'un trajet |
| **Carbone** | | | |
| 5.1 | GET | `/api/v1/carbon/trip/{trip_id}` | Bilan CO₂ d'un trajet |
| 5.2 | GET | `/api/v1/carbon/estimate` | Estimation CO₂ train vs avion |
| 5.3 | GET | `/api/v1/carbon/ranking` | Classement paires O/D par économie CO₂ |
| 5.4 | GET | `/api/v1/carbon/factors` | Facteurs d'émission (transparence) |
| **Jour / Nuit** | | | |
| 6.1 | GET | `/api/v1/analysis/day-night/coverage` | Couverture jour/nuit par pays |
| 6.2 | GET | `/api/v1/analysis/day-night/emissions` | Émissions jour vs nuit par pays |
| 6.3 | GET | `/api/v1/analysis/day-night/compare` | Comparaison pour une paire O/D |
| 6.4 | GET | `/api/v1/analysis/day-night/routes` | Routes classifiées jour/nuit |
| 6.5 | GET | `/api/v1/analysis/day-night/summary` | Résumé européen |
| **Qualité** | | | |
| 7.1 | GET | `/api/v1/quality/completeness` | Complétude globale |
| 7.2 | GET | `/api/v1/quality/completeness/by-country` | Complétude par pays |
| 7.3 | GET | `/api/v1/quality/coverage/countries` | Représentation des pays |
| 7.4 | GET | `/api/v1/quality/coverage/cities` | Couverture des villes |
| 7.5 | GET | `/api/v1/quality/schedules` | Patterns horaires |
| 7.6 | GET | `/api/v1/quality/compare-coverage` | Couverture comparaison train/avion |
| 7.7 | GET | `/api/v1/quality/day-night-balance` | Équilibre jour/nuit par corridor |
| 7.8 | GET | `/api/v1/quality/summary` | Dashboard qualité |
| **Statistiques** | | | |
| 8.1 | GET | `/api/v1/stats/operators` | Classement opérateurs |
| 8.2 | GET | `/api/v1/stats/distances` | Distribution des distances |
| 8.3 | GET | `/api/v1/stats/emissions-by-distance` | Émissions par tranche de distance |

---

## Exemples de requêtes

```bash
BASE="http://localhost:8000/api/v1"

# Villes françaises avec au moins une gare
curl "$BASE/cities?country=FR&has_station=true"

# Recherche Paris → Berlin (aller-retour, trains de nuit uniquement)
curl "$BASE/routes/search?origin=Paris&destination=Berlin&is_night_train=true"

# Estimation CO₂ Paris → Vienne
curl "$BASE/carbon/estimate?origin=Paris&destination=Wien"

# Top 10 paires O/D avec la plus grande économie CO₂
curl "$BASE/carbon/ranking?page_size=10"

# Dashboard qualité complet (1 appel)
curl "$BASE/quality/summary"

# Export CSV des trajets train depuis la France
curl "$BASE/routes/download?mode=train&departure_country=FR" -o trains_fr.csv

# Appel sécurisé (import)
curl -X POST "$BASE/import/cities" \
     -H "X-API-Key: changeme-obrail-secret" \
     -H "Content-Type: application/json" \
     -d '{}'
```

---

## Intégration avec la pipeline ETL Spark

La pipeline ETL (phases 1, 2, 3) charge les données dans `gold_routes` et `gold_compare_best` via JDBC.  
L'API lit directement depuis ces tables — **aucune dépendance Spark au runtime**.

Pour connecter le chargement Spark à la même base Docker :

```python
# Dans chargement/config/settings.py
POSTGRES_URL = "jdbc:postgresql://localhost:5432/obrail"
POSTGRES_USER = "obrail"
POSTGRES_PASSWORD = "obrail"
```

---

## Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `POSTGRES_USER` | `obrail` | Utilisateur PostgreSQL |
| `POSTGRES_PASSWORD` | `obrail` | Mot de passe |
| `POSTGRES_HOST` | `db` | Hôte (nom service Docker) |
| `POSTGRES_PORT` | `5432` | Port |
| `POSTGRES_DB` | `obrail` | Nom de la base |
| `API_KEY` | `changeme-obrail-secret` | Clé pour les endpoints POST /import/* |
| `DB_ECHO` | `false` | Log SQL (debug) |
