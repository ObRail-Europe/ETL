# Spécification des indicateurs de performance clés (KPI) pour le Data Warehouse pour les opérateurs Ferroviaire

## Contexte

Dans le cadre de la construction de notre pipeline ETL, il est impératif de définir le dictionnaire de données cible avant de concevoir l'architecture technique. Ce document fait le pont entre les besoins stratégiques des opérateurs ferroviaires (SNCF, Trenitalia, Renfe, etc.) et la modélisation du Data Warehouse. Il sert de référence pour s'assurer que les données extraites et transformées répondent aux questions métier concrètes.

## Objectif

L'objectif est de lister exhaustivement les métriques et dimensions nécessaires pour valider les critères d'acceptation du ticket [DEFINITION] Identifier les besoins des Opérateurs Ferroviaires . Ce document servira de "cahier des charges fonctionnel" pour la conception du schéma en étoile (tables de faits et dimensions).

## 1. Métriques de performance des lignes (Performance Operatoire)

L'objectif est de mesurer l'efficacité et la fiabilité du service sur un axe donné. Le DWH doit permettre de calculer les agrégats suivants :

- Taux de Régularité (On-Time Performance - OTP) :
  - Définition : Pourcentage de trains arrivés à l'heure (ou avec un retard < 5 min ou < 15 min selon le standard).
  - Donnée source nécessaire : Heure arrivée prévue vs Heure arrivée réelle.
  - Source de référence : Normes de l'ART (Autorité de Régulation des Transports).
- Taux d'Annulation :
  - Définition : Ratio entre les trains programmés et les trains ayant réellement circulé.
- Vitesse Commerciale Moyenne :
  - Définition : Distance de la ligne / Durée moyenne du trajet. Permet d'évaluer la compétitivité face à la voiture.
- Taux d'Occupation (Load Factor) :
  - Définition : (Passagers transportés / Capacité totale offerte) x 100.
  - Note : Essentiel pour l'optimisation du Yield Management.

## 2. Indicateurs de couverture réseau

Ces indicateurs permettent de visualiser l'empreinte territoriale de chaque opérateur. Les dimensions géographiques du DWH devront supporter ces analyses.

- Volume de Desserte (Service Frequency) :
  - Définition : Nombre de circulations quotidiennes par gare et par sens.
- Maillage Territorial :
  - Définition : Nombre de gares uniques desservies par région administrative ou département.
- Connectivité Intermodale :
  - Définition : Nombre de connexions possibles avec d'autres modes de transport (identifiés via les codes UIC des gares).
- Kilomètres-Lignes exploités :
  Définition : Somme des longueurs des tronçons sur lesquels l'opérateur propose une offre commerciale.

## 3. Indicateurs écologiques spécifiques

Pour valoriser le train comme alternative durable, le DWH doit intégrer des facteurs d'émission standardisés.

- Empreinte Carbone par Passager-Km (gCO2e/pkm) :
  - Définition : Émissions de GES divisées par le nombre de voyageurs et la distance.
  - Source de calcul : Base Carbone ADEME ou facteurs d'émission UIC (Union Internationale des Chemins de fer).
- Consommation Énergétique à la traction :
  - Définition : kWh consommés par train-km.
- Émissions Évitées :
  - Définition : Différentiel entre le trajet en train et le même trajet en voiture thermique (Moyenne européenne : ~170g CO2/km pour la voiture vs ~5-30g pour le train).

## 4. Comparaisons Inter-Opérateurs (Benchmark)

Le modèle de données doit permettre le partitionnement par operator_id pour permettre des requêtes analytiques de type "Ranking".

- Part de Marché (Offre) :
  - Calcul : (Nombre de trains Opérateur A / Nombre total de trains sur l'axe) sur une période T.
- Différentiel de Prix au Km :
  - Calcul : Prix moyen au km de l'Opérateur A vs Prix moyen au km de l'Opérateur B sur le même axe (nécessite des données tarifaires standardisées).
- Indice de Fiabilité Comparée :
  - Calcul : Comparaison des taux de régularité normalisés sur des lignes à caractéristiques similaires (ex: Grande Vitesse).

## Conclusion

**Les métriques cités sont théoriques et certaines métriques ne sont pas pertinantes par rapport au sujet**

Nous nous focaliserons sur les métriques suivants par soucis de cohérence et de faisabilité des calculs :

- Empreinte Carbone par Passager-Km
- Émissions Évitées (vs Avion)
- Volume de Desserte (Fréquence)
- Vitesse Commerciale Moyenne
- Kilomètres-Lignes exploités
- Part de Marché (en volume d'offre)

Nous écarterons les points suivants :

- Taux de Régularité (On-Time Performance) & Taux d'Annulation : Nécessite un suivi temps réel (Heure prévue vs Heure réelle). Les sources suggérées (GTFS, fichiers Excel, Open Data statique) fournissent des horaires théoriques . Le scraping temps réel est techniquement trop complexe et instable pour ce projet.
- Taux d'Occupation (Load Factor) : Donnée confidentielle et commerciale des opérateurs. Aucun Open Data ne fournit le nombre de billets vendus par train.
- Différentiel de Prix au Km : Scraper des prix est très difficile (tarification dynamique, protection anti-robot) et n'est pas demandé explicitement dans le cahier des charges.
- Consommation Énergétique (kWh) : La donnée disponible est le type de traction (traction: électrique/diesel/mixte), mais pas la consommation exacte en kWh.
