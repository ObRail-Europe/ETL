# ObRail Europe

ObRail Europe est un projet de collecte, de traitement et d'exposition de données ferroviaires et aériennes européennes. Il permet de consulter des dessertes ferroviaires, d'estimer les émissions CO₂ de chaque trajet, et de comparer le train et l'avion sur les mêmes corridors.

## Table des matieres

- [ObRail Europe](#obrail-europe)
  - [Table des matieres](#table-des-matieres)
  - [1. Architecture](#1-architecture)
  - [2. Prérequis et configuration](#2-prérequis-et-configuration)
  - [3. Lancer le projet](#3-lancer-le-projet)
  - [4. Pipeline ETL](#4-pipeline-etl)
    - [Extraction](#extraction)
    - [Transformation](#transformation)
    - [Chargement](#chargement)
  - [5. Schéma de données Gold](#5-schéma-de-données-gold)
  - [6. Indexation PostgreSQL](#6-indexation-postgresql)
  - [7. Calcul des émissions CO₂](#7-calcul-des-émissions-co)
    - [Facteur ferroviaire](#facteur-ferroviaire)
    - [Facteur aérien](#facteur-aérien)
    - [Choix du meilleur mode](#choix-du-meilleur-mode)
  - [8. API REST](#8-api-rest)
  - [9. Dashboard](#9-dashboard)
  - [10. Points d'attention](#10-points-dattention)
    - [Clés API nécessaires à la collecte](#clés-api-nécessaires-à-la-collecte)
    - [Espace disque](#espace-disque)
    - [Mémoire (Out of Memory)](#mémoire-out-of-memory)

---

## 1. Architecture

Le projet s'articule en trois phases enchaînées, pilotées par `src/pipeline.py`, suivies d'une API de consultation :

```
Extraction  →  Transformation  →  Chargement  →  API FastAPI
(sources      (nettoyage,         (tables Gold    (PostgreSQL)
 brutes)       dimensions,         PostgreSQL +
               CO₂)                index)
```

Il est possible de n'exécuter qu'une partie de la chaîne :

```bash
python src/pipeline.py --phases extraction
python src/pipeline.py --phases transform
python src/pipeline.py --phases load
```

---

## 2. Prérequis et configuration

Le projet nécessite Python 3.13, Java 17 (requis par PySpark) et Docker. Les dépendances Python sont listées dans `requirements.txt`.

Avant de lancer quoi que ce soit, créez un fichier `.env` à la racine du dossier `ETL/` avec les variables de connexion à PostgreSQL :

```dotenv
PG_HOST=postgres
PG_PORT=5432
PG_DB=obrail
PG_USER=obrail
PG_PASSWORD=obrail123
API_IMPORT_TOKEN=votre_token_secret
```

`docker-compose.yml` injecte automatiquement ce fichier dans les conteneurs via `env_file: .env`. 
La variable `API_IMPORT_TOKEN` n'est utilisée que pour protéger l'endpoint `POST /api/v1/import`.

---

## 3. Lancer le projet

La méthode recommandée est Docker Compose. Depuis le dossier `ETL/` :

```bash
docker compose build
docker compose up -d postgres          # démarre PostgreSQL
docker compose run --rm etl            # exécute la pipeline ETL complète
docker compose up -d api               # démarre l'API sur http://localhost:8000
```

Pour ne relancer qu'une phase spécifique (par exemple après une modification des données brutes) :

```bash
docker compose run --rm etl --phases extraction
docker compose run --rm etl --phases transform
docker compose run --rm etl --phases load
```

Pour arrêter et nettoyer l'environnement :

```bash
docker compose down -v
```

---

## 4. Pipeline ETL

### Extraction

L'extraction est gérée par `src/extraction/main_extraction.py`. Les six sources sont téléchargées **en parallèle** via un `ThreadPoolExecutor`, ce qui limite le temps d'attente réseau :

| Source | Contenu |
|---|---|
| `mobility_database` | Feeds GTFS des opérateurs ferroviaires européens |
| `backontrack` | Données trains de nuit |
| `ourairports` | Aéroports et routes aériennes européennes |
| `geonames` | Référentiel géographique des villes |
| `ember` | Intensité carbone du mix électrique par pays |
| `ademe` | Facteurs d'émission aériens (Base Carbone) |

À la fin de l'extraction, un fichier `data/raw/manifest.json` est généré avec les métriques de chaque source (fichiers téléchargés, taille, durée, erreurs éventuelles).

### Transformation

La transformation est pilotée par `src/transformation/main_transformation.py`. Elle est **séquentielle** car les transformateurs ont des dépendances entre eux : ADEME doit être prêt avant OurAirports, et les deux doivent être prêts avant le Merge.

L'ordre d'exécution est le suivant :

1. **MobilityDatabaseTransformer** — normalise les feeds GTFS téléchargés (trajets, arrêts, horaires, agences).
2. **BackOnTrackTransformer** — normalise les données trains de nuit au même format que GTFS.
3. **EmberTransformer** — prépare l'intensité carbone électrique par pays (gCO₂/kWh).
4. **AdemeTransformer** — calcule les facteurs d'émission aériens pondérés depuis la Base Carbone ADEME.
5. **OurAirportsTransformer** — construit les routes aériennes inter-villes avec rattachement géographique et référence d'émission.
6. **MergeTransformer** — fusionne les données ferroviaires, construit le schéma en étoile (`train_trips`, `localite`, `emission`) et calcule les facteurs CO₂ rail par pays et type de train.
7. Enrichissement des vols avec les identifiants de villes, puis sauvegarde de `flight.parquet`.
8. **StopMatchingTransformer** — apparie les gares ferroviaires avec les aéroports proches (dans un rayon de 100 km).

Les sorties sont des fichiers Parquet dans `data/processed/` : `train_trips.parquet`, `flight.parquet`, `localite.parquet`, `emission.parquet`, `stop_matching.parquet`.

### Chargement

Le chargement est géré par `src/chargement/main_chargement.py`. Il se déroule en quatre étapes :

1. Le schéma PostgreSQL est initialisé via `scripts/init.sql` (tables partitionnées par pays, sans index pour ne pas ralentir l'insertion).
2. `GoldAggregator` agrège les données Parquet en tables Gold prêtes pour l'API.
3. Les données sont chargées via JDBC Spark dans des tables de staging, puis basculées en production par un upsert incrémental : seules les sources présentes dans ce run sont remplacées (`DELETE ... WHERE source IN (...)`), les autres sont conservées.
4. Les index et `ANALYZE` sont appliqués via `scripts/post_load.sql`, une fois les données en place.

---

## 5. Schéma de données Gold

Deux tables constituent la couche finale exposée par l'API, toutes deux **partitionnées par `departure_country`** pour confiner les requêtes filtrées par pays à une ou quelques partitions.

**`gold_routes`** contient l'ensemble des trajets (train et avion) avec leurs métriques : distances, horaires, opérateur, indicateur train de nuit, facteur CO₂ au passager-kilomètre (`co2_per_pkm`) et émissions totales du trajet (`emissions_co2`).

**`gold_compare_best`** met en regard, pour chaque paire origine-destination, le meilleur trajet ferroviaire et la meilleure alternative aérienne. La colonne `best_mode` indique le mode le plus écologique selon les émissions CO₂ (la durée sert de critère de départage).

---

## 6. Indexation PostgreSQL

Les index sont **intentionnellement absents pendant le chargement bulk** pour ne pas pénaliser la vitesse d'insertion. Ils sont créés après coup par `scripts/post_load.sql`, de manière idempotente (`IF NOT EXISTS`), et `ANALYZE` est exécuté immédiatement après pour que l'optimiseur PostgreSQL dispose de statistiques à jour.

Sur `gold_routes`, des **index BTREE** couvrent les filtres les plus fréquents de l'API : ville de départ, ville d'arrivée, combinaison mode/nuit, combinaison mode/pays, et la paire `(trip_id, source)` pour les lookups directs. Un index partiel sur `(departure_country, mode, is_night_train, co2_per_pkm)` accélère spécifiquement l'endpoint `/carbon/factors`.

Sur `gold_compare_best`, des index similaires couvrent les villes, le `best_mode` et le `trip_id`.

Pour les **recherches textuelles partielles** (autocomplétion sur les noms de villes, gares et agences), des index **GIN trigram** (`gin_trgm_ops`) sont créés via l'extension `pg_trgm`. Ils rendent les requêtes `ILIKE '%terme%'` aussi rapides qu'un index classique.

---

## 7. Calcul des émissions CO₂

Le calcul repose sur une table de dimension `emission` construite pendant la transformation, qui centralise les facteurs en gCO₂eq par passager-kilomètre pour chaque combinaison pays / type de train, ainsi que pour chaque type de trajet aérien. Dans les tables Gold, l'émission finale d'un trajet est simplement :

```
emissions_co2 = round(distance_km × co2_per_pkm, 3)
```

### Facteur ferroviaire

Le facteur est calculé dans `MergeTransformer` à partir de trois données : l'intensité carbone du réseau électrique national fournie par Ember (gCO₂/kWh), la consommation énergétique propre au type de train (kWh/pkm, issue de la configuration), et le taux d'électrification du réseau du pays.

Pour les types de trains 100 % électriques (TGV, Intercity, etc.) la formule est directe :

```
co2_per_pkm = consommation_kWh_pkm × intensité_carbone_gCO2_kWh
```

Pour les réseaux mixtes (une partie des trains roule au diesel), une moyenne pondérée est appliquée :

```
co2_per_pkm = taux_élec × consommation × intensité  +  (1 − taux_élec) × 65 gCO2/pkm
```

Le facteur diesel fixe de 65 gCO₂/pkm est une constante de configuration. Si un pays est absent des données Ember, la moyenne européenne est utilisée en substitution, et de même pour le taux d'électrification.

### Facteur aérien

Les facteurs aériens proviennent de la **Base Carbone ADEME**. `AdemeTransformer` filtre les entrées valides, classe les avions par taille (20–50 sièges, 51–220, >220) et par distance de vol, puis calcule une moyenne par catégorie. Les facteurs finaux sont des **combinaisons pondérées** de ces catégories de base, définies dans `ADEME_EMISSION_WEIGHTS`, afin de modéliser différents types de trajets (court-courrier petit appareil, moyen-courrier, long-courrier…). Le résultat est converti en gCO₂e/p.km.

### Choix du meilleur mode

Dans `gold_compare_best`, le champ `best_mode` est déterminé en comparant `train_emissions_co2` et `flight_emissions_co2` : le mode avec les émissions les plus faibles est retenu. Si seulement un des deux modes est disponible sur le corridor, c'est lui qui est sélectionné par défaut.

---

## 8. API REST

L'API démarre sur `http://localhost:8000`. Tous les endpoints sont accessibles sous `/api/v1` et retournent du JSON. Un rate limiting par IP est actif (100 requêtes/minute par défaut). La documentation complète des paramètres et des réponses est dans `API_GUIDE.md`.

Les grandes familles d'endpoints sont :

| Groupe | Description |
|---|---|
| **Référentiel** | Lister villes, gares, aéroports avec filtres |
| **Recherche** | Trouver des trajets entre deux villes |
| **Consultation** | Parcourir et exporter `gold_routes` |
| **Carbone** | Émissions d'un trajet, estimations, classements, facteurs |
| **Analyse** | Comparaison trains de jour vs trains de nuit |
| **Qualité** | Indicateurs de complétude et de couverture des données |
| **Statistiques** | Opérateurs, distances, émissions par distance |
| **Import** | Déclencher un rechargement (authentifié) |

Seul l'endpoint `POST /api/v1/import` nécessite une authentification. Il faut passer le token via l'en-tête `Authorization: Bearer <API_IMPORT_TOKEN>`.

---

## 9. Dashboard

Un dashboard interactif développé avec Plotly Dash est disponible pour explorer les données visuellement. Il permet de consulter les indicateurs de qualité, les facteurs d'émission, et de comparer les modes de transport sur une carte.

Pour le lancer :
```bash
docker compose up -d dashboard
```
Le dashboard est ensuite accessible sur `http://localhost:8050` et nécessite que l'API soit également en cours d'exécution.

### Optimisations et Fonctionnalités

Le dashboard intègre plusieurs optimisations pour garantir une expérience fluide :

- **Parallélisation des requêtes** : Les pages nécessitant plusieurs sources de données (par exemple, la page d'accueil) chargent ces dernières en parallèle. Un *pool de threads* est utilisé pour lancer plusieurs appels à l'API simultanément, ce qui réduit considérablement les temps de chargement.

- **Mise en cache agressive** : Les réponses de l'API sont mises en cache sur disque (`diskcache`) pour des durées variables (de 5 à 30 minutes) selon la nature des données. La navigation entre les pages et les rechargements sont quasi-instantanés pour les données déjà consultées.

- **Tâches de fond asynchrones** : Les opérations potentiellement longues (comme l'export d'un grand volume de données en CSV) sont déléguées à un *gestionnaire de tâches de fond*. Cela permet à l'interface de rester réactive, sans risque de *timeout*, pendant que le serveur travaille.

---

## 10. Points d'attention

### Clés API nécessaires à la collecte

Deux sources requièrent une clé avant de lancer l'extraction. Sans elles, la source est simplement ignorée avec un avertissement dans les logs, mais la pipeline continue.

**Mobility Database** fournit les feeds GTFS de tous les opérateurs ferroviaires européens — c'est la source principale de trajets. Il faut créer un compte gratuit sur [mobilitydatabase.org](https://mobilitydatabase.org) pour obtenir un refresh token, puis l'ajouter dans `.env` :

```dotenv
MOBILITY_API_REFRESH_TOKEN=votre_token_ici
```

**Ember** fournit l'intensité carbone du mix électrique par pays, indispensable au calcul CO₂ ferroviaire. La clé est gratuite sur [ember-energy.org](https://ember-energy.org) :

```dotenv
EMBER_API_KEY=votre_clé_ici
```

Les quatre autres sources (Back-on-Track, OurAirports, GeoNames, ADEME Base Carbone) sont publiques et ne nécessitent aucune authentification.

### Espace disque

L'extraction télécharge environ **~1 000 feeds GTFS** depuis la Mobility Database, soit plusieurs dizaines de Go selon les pays sélectionnés. Prévoyez au minimum **50 Go libres** sur le volume monté sur `data/`. Les fichiers Parquet produits par la transformation sont nettement plus compacts, mais les fichiers bruts ne sont pas supprimés automatiquement.

Si les checkpoints Spark sont activés pour soulager la RAM pendant la transformation, les données intermédiaires sont matérialisées sur disque à chaque étape : comptez alors **~500 Go supplémentaires** sur le même volume.

### Mémoire (Out of Memory)

La phase de transformation est la plus gourmande en mémoire : Spark charge et joint plusieurs DataFrames volumineux en même temps, notamment lors du `MergeTransformer`. Si le processus est tué par le système ou si Spark lève une `OutOfMemoryError`, deux ajustements sont possibles :

- Augmenter la mémoire allouée au driver et aux executors via les variables `SPARK_DRIVER_MEMORY` et `SPARK_EXECUTOR_MEMORY` dans `.env` (par défaut `4g`).
- Réduire le nombre de pays chargés en filtrant `MOBILITY_EU_COUNTRIES` dans `src/extraction/config/settings.py` pour traiter les données par batch.
- Activer les **checkpoints Spark** pour matérialiser les DataFrames intermédiaires sur disque plutôt qu'en RAM. Cela soulage significativement le driver, mais génère des fichiers temporaires dans le répertoire de checkpoint.