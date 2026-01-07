# Définition des stack techniques diverses

## Contexte

Ce document regroupe l'ensemble des décisions techniques relatives aux choix de technologies, langages et outils pour le projet. Il centralise les analyses comparatives et les justifications de chaque composant de notre architecture.

## Objectif

Documenter les choix technologiques de manière structurée et argumentée, en précisant pour chaque stack :

- Les options évaluées
- Les critères de décision
- Les recommandations finales validées par l'équipe

## 1. Évaluer les technologies pour la transformation ETL

### Décision

**Python** comme langage principal avec **Apache Spark** (PySpark) pour le traitement distribué.

### Évaluation des options

#### Python ✅ Retenu

**Bibliothèques évaluées :**

- `requests` : extraction API simple et efficace
- `SQLAlchemy` : ORM robuste, abstraction multi-SGBD
- PySpark : transformations distribuées

**Avantages :**

- Compétences déjà présentes dans l'équipe
- Écosystème riche (orchestration, cloud, monitoring)
- Syntaxe lisible = maintenabilité élevée
- Intégration native avec Spark (PySpark)

#### Spark : Python vs Java

| Critère                | PySpark               | Spark Java/Scala |
| ---------------------- | --------------------- | ---------------- |
| Courbe d'apprentissage | Faible                | Moyenne/Élevée   |
| Performance            | Légèrement inférieure | Optimale         |
| Productivité           | Élevée                | Moyenne          |
| Écosystème data        | Excellent             | Bon              |
| **Choix**              | ✅ **Retenu**         | ❌               |

**Justification :** La différence de performance est négligeable pour nos volumes. La productivité et la cohérence technologique l'emportent.

#### Alternatives à Spark évaluées

**Dask** ❌

- Similaire à pandas, parallélisation automatique
- Limité pour le processing très distribué
- Conclusion : insuffisant pour nos besoins scalabilité

**Polars** ℹ️

- Performance excellente (Rust), API DataFrame moderne
- Alternative viable pour pipelines non-distribués
- Conclusion : à considérer pour composants spécifiques

#### R (tidyverse) ❌ Non retenu

- Excellente suite pour analyse statistique
- Équipe sans compétences R
- Moins adapté aux pipelines ETL production
- Conclusion : non pertinent pour notre contexte

### Critères de décision

1. **Compétences équipe** : Python maîtrisé
2. **Écosystème** : intégration Airflow, bibliothèques cloud SDK, monitoring
3. **Performance** : Spark garantit la scalabilité horizontale
4. **Maintenabilité** : stack technique homogène Python
5. **Coût** : aucune licence, community active

### Recommandation finale

**Stack ETL :** Python + PySpark

- **PySpark** : toutes les transformations ETL (extraction, transformation, chargement)
- **requests/SQLAlchemy** : connecteurs et interactions API/SGBD
- **Python** : orchestration, scripting utilitaire

## 3. Évaluer les technologies pour l'exposition des données par endpoint API

### Tableaux comparatifs des principaux critères

| Critère                          | FastAPI (Python)                                                                                                                                                                                                                 | Solutions Direct-to-Database (PostgREST / Hasura)                                                                                                                                                     |
| :------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Flexibilité (Logique métier)** | **Élevée.** Permet d'intégrer des transformations complexes, des calculs de CO2 à la volée ou des agrégations spécifiques en Python avant l'envoi[cite: 14]. Idéal pour découpler le modèle de base de données du modèle exposé. | **Faible à Moyenne.** La logique doit souvent résider dans des procédures stockées SQL ou des vues. Moins souple pour des traitements algorithmiques complexes (ex: comparaison jour/nuit dynamique). |
| **Performance**                  | **Très bonne.** Asynchrone par défaut. Très performant pour du Python, suffisant pour les volumes attendus par ObRail.                                                                                                           | **Excellente.** Pas de couche interprétée intermédiaire, requête directe sur la BDD. Souvent plus rapide sur des CRUD simples.                                                                        |
| **Maintenabilité**               | **Élevée.** Code explicite, typé (Pydantic), testable unitairement (Pytest). L'équipe Data Science maîtrise déjà Python[cite: 282], facilitant la reprise du projet.                                                             | **Moyenne.** Dépendance forte au schéma de la base de données. Le versionning de la configuration est parfois plus complexe que le versionning de code.                                               |
| **Documentation (Auto)**         | **Excellente.** Génération native et automatique de Swagger UI / OpenAPI[cite: 293]. Répond parfaitement à l'exigence d'endpoints clairs et documentés[cite: 160].                                                               | **Bonne.** Générée souvent automatiquement, mais la personnalisation des descriptions pour les utilisateurs finaux (ONG, Institutions) peut être plus laborieuse.                                     |
| **Conformité & Sécurité**        | **Granulaire.** Contrôle total sur les headers, le formatage des erreurs et la validation des entrées pour respecter l'accessibilité numérique[cite: 133].                                                                       | **Rigide.** Sécurité basée sur les rôles BDD (Row Level Security). Puissant mais parfois complexe à auditer pour une équipe junior.                                                                   |

### Recommandation Évaluation des solutions

Bien que les solutions _Direct-to-Database_ offrent une mise en place quasi instantanée pour des opérations CRUD (Create, Read, Update, Delete) simples, nous recommandons le maintien du choix de **FastAPI** pour le projet ObRail Europe pour les raisons suivantes :

1.  **Respect des contraintes pédagogiques et techniques :** Le cahier des charges suggère explicitement l'usage de Python et mentionne FastAPI comme framework recommandé[cite: 282, 293].
2.  **Traitement de l'hétérogénéité des données :** Les données proviennent de sources très variées (GTFS, CSV, Scraping)[cite: 68]. FastAPI nous permet de créer une couche d'abstraction (DTOs/Pydantic) pour "lisser" les irrégularités de la base de données et présenter une interface propre et standardisée aux consommateurs de l'API (Institutions, ONG)[cite: 101, 102].
3.  **Alignement des compétences :** Le projet implique une équipe Data Science et le développement futur de modèles IA[cite: 100]. L'usage de Python sur toute la chaîne (ETL + API + IA) garantit une homogénéité technique et facilite la maintenance future.
4.  **Documentation et Accessibilité :** FastAPI génère automatiquement une documentation interactive (Swagger UI). C'est un livrable critique attendu pour permettre la prise en main par des tiers[cite: 160].
