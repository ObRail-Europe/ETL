# Définir les règles de gestion des fuseaux horaires

## Contexte
Les données collectées proviennent de sources hétérogènes (opérateurs nationaux, open data) couvrant plusieurs pays européens. Les jeux de données actuels présentent des problèmes de fiabilité, notamment des informations manquantes sur les fuseaux horaires et des incohérences dans les formats de date/heure.
Pour garantir des calculs de durée exacts et une comparaison fiable entre les trains (traversant souvent plusieurs fuseaux horaires), une standardisation stricte est nécessaire lors de la phase ETL.

## 1. Fuseau Horaire de Référence (Stockage)

Afin d'assurer l'intégrité des données dans la base PostgreSQL, le format de stockage suivant est imposé :

* **Référentiel Unique :** Toutes les données temporelles (heures de départ, d'arrivée) doivent être converties et stockées en **UTC (Coordinated Universal Time)**.
* **Format de Donnée :** Utilisation du type de données `TIMESTAMP WITH TIME ZONE` (ou équivalent PostgreSQL `TIMESTAMPTZ`) pour conserver l'instant précis indépendamment de la localisation géographique.
* **Justification :** Ce choix permet de neutraliser les différences de fuseaux horaires entre les pays d'origine et de destination (ex: Portugal UTC+0 vs Espagne UTC+1) et facilite le calcul automatisé des durées de trajet.

Pourquoi PostreSQL et pas une autre BDD?
Pour son aspect structuré (bdd relationnel), la qualité, l'intégrité et l'interopérabilité 

## 2. Règles de Conversion (ETL)

Le processus d'extraction et de transformation (ETL) doit appliquer les règles suivantes lors de l'ingestion des données sources :

* **Identification de la Source :** Détection automatique du fuseau horaire local de la gare de départ et de la gare d'arrivée basée sur le code pays ou les coordonnées géographiques.
* **Normalisation :** Conversion systématique des heures locales fournies par les opérateurs (souvent sans offset explicite) vers le format UTC avant insertion en base.
* **Traitement des Erreurs :** Tout enregistrement dont le fuseau horaire ne peut être déterminé avec certitude doit être rejeté ou marqué pour vérification manuelle (ou automatisée) afin de ne pas corrompre les analyses de qualité de service.

## 3. Gestion des Changements d'Heure (Été/Hiver)

La gestion des passages à l'heure d'été (DST - Daylight Saving Time) est critique pour la justesse des calculs de durée et la cohérence des trains de nuit.

* **Gestion Automatique :** Le système de gestion de base de données (PostgreSQL) et les bibliothèques de manipulation de dates (ex: Python Pandas) doivent gérer les décalages DST via les tables de fuseaux horaires IANA (ex: `Europe/Paris`, `Europe/Berlin`).
* **Impact sur les Trains de Nuit :** Pour les trains circulant lors des nuits de changement d'heure, la durée réelle doit être calculée sur la base des timestamps UTC et non sur la différence des heures locales affichées.

## 4. Restitution des Données (API & Dashboard)

Si le stockage est en UTC, la restitution aux utilisateurs doit s'adapter au contexte d'usage :

* **API REST :** Les endpoints doivent exposer les dates au format **ISO 8601** (ex: `2023-10-25T21:00:00Z`) pour garantir l'interopérabilité technique.
* **Tableaux de Bord :** Pour la visualisation "Grand Public", les horaires peuvent être reconvertis dynamiquement en heure locale de l'utilisateur ou de la gare concernée pour assurer la lisibilité.