# Inventorier les sources de données ferroviaires

## Contexte
Le projet nécessite l’identification et l’analyse des différentes sources de données ferroviaires disponibles afin de disposer d’une vision claire de l’écosystème de données existant.

## Objectif 
Recenser, documenter et évaluer l’ensemble des sources de données ferroviaires pertinentes pour sélectionner celles qui répondent le mieux aux besoins du projet.

## 1. ADEME (Open Data)

Les datasets ADEME sur data.ademe.fr sont accessibles avec l’API du portail (moteur CKAN ou équivalent) : requêtes HTTP avec paramètres (filtre, pagination, format JSON/CSV).

Exemple de principe (URL à adapter au dataset choisi) :

    Endpoint type : https://data.ademe.fr/data-fair/api/v1/datasets/{id}/lines avec filtres dans la query string (par ex. q, size, page).

## 2. EEA – Air Quality / Environnement

EEA expose ses données via des services web/API (Air Quality e-Reporting, etc.) ou via des services de téléchargement automatisable.

Exemples de principes :

    Service de téléchargement AQ : endpoints paramétrables (pays, polluant, période) utilisables via HTTP GET.​

    Autres datasets EEA accessibles en CSV/JSON/XML via API Store ou portails d’open data.

## 3. Mobility Database (MobilityData)

    Données : catalogue mondial de flux GTFS/GTFS-RT (horaires, lignes, arrêts) pour de nombreux opérateurs européens (SNCF, DB, ÖBB, Trenitalia, etc.).

    Accès : API REST sécurisée (Bearer token), documentation via Swagger UI.​

    Utilisation script :
        - Endpoint de recherche de feeds (filtre par pays, opérateur).
        - Endpoint pour l’URL de la dernière version du GTFS.
        - Téléchargement du .zip GTFS (requests + stream) pour traitement ultérieur.

## 4. transport.data.gouv.fr (France, mais utile dans ton périmètre Europe)

    Données : jeux de données transport France (SNCF, TER, réseaux locaux), souvent en GTFS ou GTFS-RT, plus d’autres formats (CSV, API propriétaires).​

    Accès :
        - API DCAT (catalogue) pour lister les jeux de données.
        - Pour chaque dataset : URL directe vers le GTFS/CSV ou une API d’opérateur.

    Utilisation script :
        - Appeler l’API catalogue pour trouver les flux ferroviaires.
        - Récupérer les URLs de téléchargement des flux (GTFS, CSV) et les télécharger avec requests.

## 5. Back-on-Track – Night Train Database

    Données : base de données des trains de nuit en Europe (lignes, opérateurs, dessertes jour/nuit, distances, parfois émissions).
    ​
    Accès :
        - Projet publié sous forme de fichiers (CSV/Google Sheets/JSON) dans un dépôt GitHub ou lien de téléchargement.
        - Pas forcément une API REST classique, mais URL de fichier exploitable automatiquement.

    Utilisation script :
        - Télécharger les CSV depuis les URLs du dépôt GitHub (raw) ou Google Sheets export CSV avec requests.

## 6. Transitland / HUSAHUC GTFS France / autres portails GTFS

    Données : catalogue de flux GTFS (y compris rail) à l’échelle internationale ou nationale.​

    Accès :
        - Transitland : API REST pour lister les feeds, opérateurs, lignes.
        - HUSAHUC GTFS France : dépôt GTFS sur GitLab (téléchargement de fichiers).

    Utilisation script :
        - Interroger l’API Transitland pour récupérer des flux rail européens.
        - Télécharger les fichiers GTFS (zip) depuis les URLs renvoyées.

## 7. Eurostat / European Data Portal

    Données : statistiques transports, énergie, émissions par pays, pouvant servir à calibrer ou comparer les émissions ferroviaires à l’échelle européenne.​

    Accès :
        - Eurostat : API REST (SDMX/JSON) pour interroger des ensembles de données par codes (transport, énergie, environnement).
        - European Data Portal : catalogues de jeux de données avec URLs de fichiers ou APIs spécifiques.

    Utilisation script :
        - Appels GET sur l’API Eurostat en spécifiant dataset ID + filtres (geo, time, unit).
        - Téléchargement de CSV/JSON, parsing avec pandas.

## 8. UIC (Union Internationale des Chemins de fer)

    Données : rapports et statistiques sur le rail, souvent au format PDF/Excel, parfois relayés via Eurostat ou ERA.​​

    Accès :
        - Peu de vraie API publique ; plutôt des fichiers téléchargeables.

    Utilisation script :
        - Téléchargement de fichiers (PDF/Excel/CSV) avec requests.
        - Extraction tabulaire (pandas pour Excel/CSV ; pour PDF, éventuel parsing avec bibliothèques spécialisées si tu souhaites aller jusque-là).


## 9. SNCF 

1. Plateforme Open Data SNCF

    Portail : open data SNCF (datasncf.opendatasoft.com / ressources.data.sncf.com).
    Contenu : horaires, gares, régularité, équipements en gare, objets trouvés, etc.

    Accès développeur :
        - API catalogue pour lister les jeux de données --> /api/records/1.0/search/ avec paramètres
        - Pour chaque dataset, API de consultation (filtrage, pagination) en JSON/CSV (format Opendatasoft).

2. API “Théorique et Temps réel” SNCF

    Jeu de données “API théorique et temps réel SNCF” référencé sur data.gouv.fr.

    Fonctionnalités :
        - Horaires planifiés et temps réel de tous les trains SNCF.
        - Calcul d’itinéraires, prochains passages en gare, gares les plus proches.

    Accès :
        - API freemium nécessitant un token (clé) à demander sur le portail développeurs SNCF.

