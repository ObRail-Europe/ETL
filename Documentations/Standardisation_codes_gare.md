Standardisation des codes gare

Le contexte est la modernisation du ferroviaire français : avant, chaque entreprise utilisait ses propres codes pour identifier trains, wagons et pièces, ce qui créait des confusions et ralentissait tout.
Les règles GS1 standardisent ces codes avec des étiquettes uniques (code-barres ou RFID) reconnues partout, pour partager facilement les infos entre acteurs comme SNCF ou Alstom.
Cela apporte une traçabilité totale des équipements, une maintenance plus rapide et moins chère, et une meilleure efficacité pour tous, comme un langage commun qui évite les erreurs.

L'objectif est de définir les règles pour uniformiser les codes d'identification des gares ferroviaires (ex. codes UIC à 7 chiffres ou SNCF à 5 chiffres), assurant une identification unique, interopérable et non ambiguë sur le réseau ferré national et européen, facilitant la gestion des horaires, trafics et échanges de données entre opérateurs

En tant que Data Architect, voici les règles de standardisation des identifiants de gares pour garantir unicité et cohérence des références géographiques, basées sur les standards UIC (principal), IATA (pour liaisons aériennes) et codes internes SNCF (trigrammes, TT00020, RESARAIL).

__Standard de codification retenu
- UIC (reférentiel principal) : Code unique international à 7 chiffres (`uic_ref=*`) pour toutes les gares ferroviaires, complété par code UIC8 SNCF (`ref:FR:uic8=*`) pour usage national.

- IATA (secondaire) : Codes à 3 lettres pour gares TGV connectées à l'aviation (ex. XPG pour Paris Gare du Nord, XDT pour CDG 2 TGV).

- Codes internes SNCF : Trigammes (ex. PE pour Paris-Est), TT00020 (ex. SG pour Strasbourg), RESARAIL (ex. FRAEG pour Strasbourg billetique).

 Règles de mapping entre standards
Utilisez une table de correspondance pour mapper les codes (ex. via datasets data.gouv.fr ou data.sncf.com) :
| Standard | Exemple gare | Code     | Mapping source                             |
|----------|--------------|----------|--------------------------------------------|
| UIC7     | Strasbourg   | 8777123  | OpenStreetMap uic_ref, data.gouv UIC codes |
| UIC8 SNCF| Strasbourg   | 00877123 | ref:FR:uic8                                |
| Trigamme | Paris-Est    | PE       | data.gouv trigramme, data.sncf             |
| TT00020  | Strasbourg   | SG       | data.sncf TT00020                          |
| RESARAIL | Strasbourg   | FRAEG    | Trainline opendata                         |
| IATA     | Paris Nord   | XPG      | Wikipedia IATA list                        |

- Implémentation : Stockez tous les codes dans une table unique avec clé primaire UIC7 ; utilisez des champs optionnels pour les autres (ex. `code_iata`, `trigramme_sncf`). Automatisez les jointures via ETL sur sources open data (data.sncf.com, transport.data.gouv.fr).

Gestion des gares sans code standard
- Identification : Utilisez nom + coordonnées géo (Lambert 93/WGS84) + code INSEE commune comme identifiant provisoire (ex. nom="Gare X", code_insee="75056").

- Procédure : 

  - Vérifiez datasets exhaustifs (SNCF liste gares voyageurs/fret ~3000 sites).

  - Attribuez UIC via demande UIC ou SNCF si manquant ; sinon, générez trigramme interne (ex. basé sur nom abrégé).

  - Flaggez comme "à standardiser" dans base de données jusqu'à résolution.
  
- Sources pour complétude : OpenStreetMap pour mapping communautaire, data.sncf pour localisation précise.