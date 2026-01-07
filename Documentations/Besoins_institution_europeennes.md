# Titre
Identifier les besoins des Institutions Européennes

## Contexte
Ce document vise à définir les exigences fonctionnelles et techniques du Data Warehouse (entrepôt de données) pour qu'il serve efficacement les missions d'ObRail Europe auprès des institutions publiques. ObRail collabore avec la Commission européenne et le Parlement européen pour alimenter les politiques publiques, notamment dans le cadre du Green Deal et du programme TEN-T (Trans-European Transport Network). L'objectif est de fournir des données fiables pour soutenir la transition écologique et mesurer la compétitivité du rail face à l'avion sur les trajets intra-européens.

## Objectif
L'objectif de ce document est de structurer le Data Warehouse en traduisant les besoins politiques en spécifications techniques. Il répond aux critères d'acceptation suivants :

- Définition des indicateurs clés (KPIs) environnementaux et de mobilité.

- Identification des niveaux d'agrégation nécessaires pour les analyses transfrontalières.

- Spécification des formats de restitution (API, Dashboard).

- Définition des fréquences de mise à jour pour garantir la fraîcheur des données.

---

## 1. Indicateurs Clés de Performance (KPIs) Attendus

Pour permettre aux institutions d'évaluer l'impact des politiques de mobilité, le Data Warehouse doit être capable de calculer et restituer les indicateurs suivants :

* **Impact Environnemental ($CO_2$) :**
    * Calcul des émissions de $CO_2$ par passager-kilomètre ($pkm$) pour chaque trajet.
    * Comparatif différentiel des émissions entre le train et l'avion pour les mêmes liaisons afin de démontrer l'avantage écologique du rail.
    * *Donnée source exemple :* `emission_gco2e_pkm`, `total_emission_kgco2e`.

* **Performance du Maillage Ferroviaire :**
    * Analyse de la couverture ferroviaire européenne (dessertes existantes).
    * Comparaison de l'offre entre **Trains de Nuit** vs **Trains de Jour** (fréquence, durée, connectivité) pour évaluer leur rôle respectif.
    * Calcul de la durée totale des trajets et de la distance parcourue.

* **Qualité de Service :**
    * Fréquence hebdomadaire des liaisons (ex: nombre de trains par semaine).
    * Type de service (Intercité, Grande Vitesse, Régional, Train de nuit) pour segmenter l'analyse de l'offre.

## 2. Niveaux d'Agrégation Requis

Les institutions européennes ayant besoin d'une vision à la fois macroscopique (politique globale) et microscopique (faisabilité technique), les données doivent être structurées selon les niveaux suivants :

* **Niveau "Corridor / Ligne" (Micro) :**
    * Détail par liaison spécifique (ex: Paris - Berlin) pour comparer directement avec les lignes aériennes équivalentes.
    * Identification précise des gares de départ et d'arrivée et des opérateurs concernés.

* **Niveau "Pays / Transfrontalier" (Meso) :**
    * Agrégation par pays d'origine et de destination pour analyser les flux transfrontaliers.
    * Nécessité d'harmoniser les données pour pallier l'absence actuelle de standardisation transfrontalière et permettre les comparaisons entre pays.

* **Niveau "Réseau Européen" (Macro) :**
    * Vue d'ensemble pour le suivi du programme TEN-T (Trans-European Transport Network).
    * Évaluation globale de la contribution à la neutralité carbone visée par le Green Deal européen.

## 3. Formats de Restitution Documentés

Pour garantir l'accessibilité aux décideurs politiques ainsi qu'aux équipes techniques des institutions, les formats de sortie suivants sont exigés :

* **API REST Documentée :**
    * Mise à disposition des données brutes et agrégées via des endpoints sécurisés pour une intégration dans les outils institutionnels.
    * L'API doit respecter les standards d'accessibilité numérique pour être exploitable par un public varié (data scientists, ONG, institutions).

* **Tableaux de Bord (Data Visualization) :**
    * Création de tableaux de bord accessibles à "tout public" (y compris les décideurs non-techniques).
    * Visualisation de la complétude des données et de la répartition jour/nuit.

* **Compatibilité des Outils :**
    * Les livrables doivent être compatibles avec des environnements standards (PostgreSQL, Docker) pour assurer la pérennité et la réutilisabilité par les services informatiques de l'UE.

## 4. Fréquences de Mise à Jour et Contraintes Temporelles

* **Automatisation du Flux (ETL) :**
    * Le processus doit permettre une mise à jour régulière sans intervention manuelle lourde, garantissant que les politiques se basent sur des données fraîches.

* **Échéance Critique :**
    * Une première version exploitable de l'entrepôt de données doit être livrée