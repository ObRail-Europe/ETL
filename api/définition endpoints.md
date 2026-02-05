# Endpoints de l'API REST

## Endpoints de Collecte
**Méthode : POST**

| Endpoint | Description | Format de réponse |
|----------|-------------|-------------------|
| `/import/villes` | Données géographiques | JSON: `{ "status": "success", "imported": number, "errors": [] }` |
| `/import/aeroports` | Données aéroports | JSON: `{ "status": "success", "imported": number, "errors": [] }` |
| `/import/gares` | Référentiel des gares | JSON: `{ "status": "success", "imported": number, "errors": [] }` |
| `/import/trajets/train` | Horaires et distances ferroviaires | JSON: `{ "status": "success", "imported": number, "errors": [] }` |
| `/import/trajets/avion` | Vols et distances calculées | JSON: `{ "status": "success", "imported": number, "errors": [] }` |

## Endpoints de Consultation
**Méthode : GET**

| Endpoint | Paramètres | Description | Format de réponse |
|----------|-----------|-------------|-------------------|
| `/villes` | - | Liste des villes | JSON array, pagination (limit, offset) |
| `/gares/{id_ville}` | `id_ville` | Liste toutes les gares de `{id_ville}` | JSON array, pagination (limit, offset) |
| `/aeroports/{id_ville}` | `id_ville` | Liste tous les aéroports de `{id_ville}` | JSON array, pagination (limit, offset) |
| `/trajets/train` | `ville_depart`, `ville_arrivee`, `est_jour` (booléen), `est_long` (booléen) | Trajets de train avec filtres | JSON array, pagination (limit, offset) |
| `/trajets/avion` | `ville_depart`, `ville_arrivee`, `distance_min`, `distance_max`, `heure_min`, `heure_max` | Trajets d'avion avec filtres | JSON array, pagination (limit, offset) |

## Endpoints d'Analyse et Comparaison
**Méthode : GET**

| Endpoint | Paramètres | Sortie | Format de réponse |
|----------|-----------|--------|-------------------|
| `/comparaison/impact` | `ville_depart`, `ville_arrivee` | Comparaison CO2 entre meilleur trajet train et vol direct | JSON: `{ "train": object, "avion": object, "meilleur": string }` |
| `/statistiques/maillage` | - | Ratio couverture trains jour vs nuit | JSON: `{ "jour": number, "nuit": number, "ratio": number }` |
| `/statistiques/qualite` | - | Métriques du tableau de bord | JSON: `{ "metrics": [] }` |

## Endpoints de Référentiel Carbone
**Méthode : GET**

| Endpoint | Paramètres | Description | Format de réponse |
|----------|-----------|-------------|-------------------|
| `/emissions` | `code_carbone` (optionnel), `type_transport` (optionnel) | Liste des facteurs d'émission filtrés | JSON array, pagination (limit, offset) |

