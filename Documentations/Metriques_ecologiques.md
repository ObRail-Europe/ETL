# Définition des métriques d'impact écologique des transports

## Contexte
Ce document établit les métriques quantifiables pour caractériser l'impact écologique des transports, en particulier pour comparer le train et l'avion sur le plan environnemental.

## Objectif
Harmoniser les calculs d'émissions en s'appuyant sur des sources de données fiables et en intégrant l'ensemble des impacts climatiques, notamment le forçage radiatif pour le transport aérien.

## 1. Sources de données

### Pour la France

Base Empreinte® de l'**ADEME** (Agence de la Transition Écologique)
- **Description** : base de données officielle française des facteurs d'émission de gaz à effet de serre, couvrant plus de 2500 produits et services
- **Couverture** : France métropolitaine et DOM-TOM, tous secteurs d'activité
- **Méthodologie** : analyse du cycle de vie (ACV) selon les normes ISO 14040/14044
- **Mise à jour** : régulière 
- **Accès** : https://base-empreinte.ademe.fr/

### Pour les trajets internationaux
European Environment Agency (**EEA**) (Agence Européenne pour l'Environnement)
- **Description** : agence de l'Union Européenne créée en 1990, fournissant des données environnementales indépendantes et de haute qualité
- **Couverture** : 32 pays membres (27 UE + Islande, Liechtenstein, Norvège, Suisse, Turquie) + 6 pays coopérants (Balkans occidentaux)
- **Méthodologie** : harmonisation des données nationales selon les standards européens (directive 2003/87/CE)
- **Accès** : https://www.eea.europa.eu/fr

UIC (Union Internationale des Chemins de fer)
- **Description** : association mondiale du secteur ferroviaire (fondée en 1922) regroupant 200 membres dans 100 pays
- **Outil principal** : EcoPassenger (https://uic.hafas.cloud/bin/query.exe/fn?L=vs_uic&) - comparaison train/avion/voiture pour passagers en Europe
- **Méthodologie** : approche "well to wheel" (du puits à la roue), validée par l'EEA et la Commission Européenne

## 2. Formules de calcul

### Formule générale

```text
Émissions totales (gCO₂e) = Distance (km) × Facteur d'émission (gCO₂e/passager.km)
```

### Transport ferroviaire

```text
Émissions train = Distance × FE_train
```

Où :

**FE_train** : facteur d'émission du train selon le type (TGV, Intercités, TER)

### Transport aérien

```text
Émissions avion = Distance × FE_avion × Facteur de forçage radiatif
```

Où :

**FE_avion** : facteur d'émission de base selon la classe et le type de vol
**Facteur de forçage radiatif** (RFI) : multiplicateur pour intégrer les traînées de condensation

## 3. Unité de mesure standardisée

gCO₂e/passager.km
- **gCO₂e** : grammes d'équivalent CO₂ (incluant tous les gaz à effet de serre)
- **passager.km** : unité fonctionnelle permettant de comparer différents modes de transport

Cette unité permet une comparaison équitable indépendamment de la distance parcourue