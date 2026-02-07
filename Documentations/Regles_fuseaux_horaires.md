# Définir les règles de gestion des fuseaux horaires

## Contexte
Les données collectées proviennent de sources hétérogènes (opérateurs nationaux, open data) couvrant plusieurs pays européens. Les jeux de données actuels présentent des problèmes de fiabilité, notamment des informations manquantes sur les fuseaux horaires et des incohérences dans les formats de date/heure.

## Objectif 
Réaliser une standardisation stricte est nécessaire lors de la phase ETL pour garantir les calculs de durée exacts avec une comparaison fiable entre les trains (traversant souvent plusieurs fuseaux horaires).

## 1. Fuseau Horaire de Référence (Stockage)

Afin d'assurer l'intégrité des données dans la base PostgreSQL, le format de stockage suivant est imposé :

* **Référentiel Unique :** Toutes les données temporelles (heures de départ, d'arrivée) doivent être converties et stockées en **UTC (Coordinated Universal Time)**.
* **Format de Donnée :** Utilisation du type de données `TIMESTAMP WITH TIME ZONE` (ou équivalent PostgreSQL `TIMESTAMPTZ`) pour conserver l'instant précis indépendamment de la localisation géographique.
* **Justification :** Ce choix permet de neutraliser les différences de fuseaux horaires entre les pays d'origine et de destination (ex: Portugal UTC+0 vs Espagne UTC+1) et facilite le calcul automatisé des durées de trajet.

Pourquoi PostreSQL et pas une autre BDD ?
Pour son aspect structuré (bdd relationnel), la qualité, l'intégrité et l'interopérabilité 

## 2. Règles de Conversion (ETL)

Le processus d'extraction et de transformation (ETL) doit appliquer les règles suivantes lors de l'extraction des données sources :

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

# Règles de classification jour/nuit des trains

## 1. Critères principaux

**Train de nuit** : départ entre 20h00 et 01h00 (heure locale de départ) ET durée ≥ 5h
**Train de jour** : tous les autres cas (départ entre 01h00 et 20h00 avec n'importe quelle durée)

## 2. Gestion des cas ambigus

### Cas 1 : trains partant dans la fenêtre horaire mais trop courts

**Exemple** : départ 22h00, arrivée 01h30 → durée 3h30
**Classification** : JOUR (durée < 5h, considéré comme un train de soirée standard)
**Justification** : insuffisant pour être qualifié de service nuit

### Cas 2 : trains longs partant avant la fenêtre nuit

**Exemple** : départ 19h00, arrivée 06h00 le lendemain → durée 11h
**Classification** : JOUR (départ avant 20h00 selon la règle stricte)
**Justification** : l'heure de départ n'est pas compris dans le seuil de nuit

### Cas 3 : trains partant après minuit avec longue durée

**Exemple** : départ 01h30, arrivée 09h00 → durée 7h30
**Classification** : JOUR (départ après 01h00, hors fenêtre nuit)
**Justification** : considéré comme un train matinal très tôt plutôt qu'un train de nuit

### Cas 4 : trains partant pile à la limite (01h00 ou 20h00)

**Exemple A** : départ exactement 20h00, durée 6h → NUIT (inclus dans le seuil nuit)
**Exemple B** : départ exactement 01h00, durée 6h → JOUR (exclu de la fenêtre nuit)
**Justification** : intervalle [20h00, 01h00[ (20h00 inclus, 01h00 exclu)

### Cas 5 : trains transfrontaliers avec changement de fuseau

**Exemple** : départ Lisbonne (UTC+0) à 23h00, arrivée Madrid (UTC+1) à 06h00 heure locale Madrid → durée réelle 6h en UTC
**Classification** : NUIT (heure locale de départ = 23h00 ∈compris dans le seuil nuit ET durée UTC = 6h)
**Justification** : l'heure locale de la gare de départ fait foi, la durée est calculée en UTC

### Cas 6 : trains circulant durant le changement d'heure

**Exemple hiver** : départ 22h00, passage à l'heure d'hiver à 03h00 (recul d'1h), arrivée 06h00 affichée → durée UTC réelle = 9h
**Exemple été** : départ 23h00, passage à l'heure d'été à 02h00 (avance d'1h), arrivée 05h00 affichée → durée UTC réelle = 5h
**Classification** : vérifier la durée réelle en UTC. Si ≥ 5h ET départ compris dans le seuil nuit → NUIT
