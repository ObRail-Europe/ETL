# Endpoints de l'API REST

## Endpoints de Collecte
**Méthode : POST**

| Endpoint | Description |
|----------|-------------|
| `/import/villes` | Données géographiques |
| `/import/aeroports` | Données aéroports |
| `/import/gares` | Référentiel des gares |
| `/import/trajets/train` | Horaires et distances ferroviaires |
| `/import/trajets/avion` | Vols et distances calculées |

## Endpoints de Consultation
**Méthode : GET**

| Endpoint | Description |
|----------|-------------|
| `/villes` | Liste des villes |
| `/gares/{id_ville}` | Liste toutes les gares de `{id_ville}` |
| `/aeroports/{id_ville}` | Liste tous les aéroports de `{id_ville}` |
| `/trajets/train` | Trajets de train avec filtres (départ, arrivée, est_jour, est_long) |
| `/trajets/avion` | Trajets d'avion avec filtres de distance et d'horaires |

## Endpoints d'Analyse et Comparaison
**Méthode : GET**

| Endpoint | Paramètres | Sortie |
|----------|-----------|--------|
| `/comparaison/impact` | `ville_depart`, `ville_arrivee` | Comparaison CO2 entre meilleur trajet train et vol direct |
| `/statistiques/maillage` | Aucun | Ratio couverture trains jour vs nuit |
| `/statistiques/qualite` | - | Métriques du tableau de bord |

## Endpoints de Référentiel Carbone
**Méthode : GET**

| Endpoint | Description |
|----------|-------------|
| `/emissions` | Liste des facteurs d'émission (code_carbone, emission) |
