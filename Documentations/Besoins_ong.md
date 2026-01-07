# Spécification des Besoins de Données pour les ONG Environnementales

## Contexte

Ce document synthétise les résultats de la phase de recueil des besoins auprès des parties prenantes (ONG environnementales). Il sert de référence pour l'équipe technique afin de garantir que le Data Warehouse (DWH) contienne les données nécessaires aux campagnes de sensibilisation et de plaidoyer écologique. Il fait le lien entre la vision stratégique et l'implémentation technique.

## Objectif

Ce document vise à valider les critères d'acceptation du ticket [DEFINITION] Identifier les besoins des ONG Environnementales. Il sélectionne les métriques environnementales pertinentes et réalisables pour permettre aux ONG de quantifier les bénéfices du transport ferroviaire.

## Identification des Métriques Prioritaires (KPIs)

Pour construire leurs argumentaires, les ONG ont besoin d'indicateurs fiables et standardisés. Le DWH devra permettre de calculer ou d'agréger les éléments suivants :

- L'Empreinte Carbone (CO2e) :
  - Besoin : Mesure des émissions de Gaz à Effet de Serre (GES) par passager et par kilomètre.
  - Source méthodologique requise : Base Carbone ADEME ou méthodologie Ecotransit.
  - Formule de référence : Emissions=Distance × Facteur_Emission_moyen_transport
  - Spécificité : Distinction nécessaire entre les émissions directes (combustion) et indirectes (amont/fabrication) si les données sources le permettent.
- Le Temps de Trajet (Door-to-Door) :
  - Besoin : Comparer le temps "réel".
  - Train : Temps de trajet en gare.
  - Avion : Temps de vol + temps d'embarquement/débarquement (forfait estimé à +2h pour les vols intra-européens).
- Le Coût Financier :
  - Besoin : Prix moyen du billet pour une date donnée (J-1, J-30, J-90).
  - Objectif du KPI : Mettre en évidence les incohérences fiscales (ex: kérosène non taxé) rendant l'avion moins cher que le train.

## Conclusion

L'emprunte carbonne et les temps de trajets seront conservés. En revanche, les métriques sur le coûts financier ne seront pas indiqué, étant hors périmètre et techniquement complexe.
