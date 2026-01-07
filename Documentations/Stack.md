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

## Évaluation des options

### Python ✅ Retenu
**Bibliothèques évaluées :**
- `requests` : extraction API simple et efficace
- `SQLAlchemy` : ORM robuste, abstraction multi-SGBD
- PySpark : transformations distribuées

**Avantages :**
- Compétences déjà présentes dans l'équipe
- Écosystème riche (orchestration, cloud, monitoring)
- Syntaxe lisible = maintenabilité élevée
- Intégration native avec Spark (PySpark)

### Spark : Python vs Java
| Critère | PySpark | Spark Java/Scala |
|---------|---------|------------------|
| Courbe d'apprentissage | Faible | Moyenne/Élevée |
| Performance | Légèrement inférieure | Optimale |
| Productivité | Élevée | Moyenne |
| Écosystème data | Excellent | Bon |
| **Choix** | ✅ **Retenu** | ❌ |

**Justification :** La différence de performance est négligeable pour nos volumes. La productivité et la cohérence technologique l'emportent.

### Alternatives à Spark évaluées

**Dask** ❌
- Similaire à pandas, parallélisation automatique
- Limité pour le processing très distribué
- Conclusion : insuffisant pour nos besoins scalabilité

**Polars** ℹ️
- Performance excellente (Rust), API DataFrame moderne
- Alternative viable pour pipelines non-distribués
- Conclusion : à considérer pour composants spécifiques

### R (tidyverse) ❌ Non retenu
- Excellente suite pour analyse statistique
- Équipe sans compétences R
- Moins adapté aux pipelines ETL production
- Conclusion : non pertinent pour notre contexte

## Critères de décision

1. **Compétences équipe** : Python maîtrisé
2. **Écosystème** : intégration Airflow, bibliothèques cloud SDK, monitoring
3. **Performance** : Spark garantit la scalabilité horizontale
4. **Maintenabilité** : stack technique homogène Python
5. **Coût** : aucune licence, community active

## Recommandation finale

**Stack ETL :** Python + PySpark

- **PySpark** : toutes les transformations ETL (extraction, transformation, chargement)
- **requests/SQLAlchemy** : connecteurs et interactions API/SGBD
- **Python** : orchestration, scripting utilitaire