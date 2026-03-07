"""
Config de la transformation : constantes métier, mappings, chemins de sortie.

Centralise toutes les constantes spécifiques à la transformation (facteurs géo, mappings d'agences, fuseaux horaires) pour éviter d'avoir tout éparpillé dans le code.
"""

from pathlib import Path

from common.config import BaseConfig


class TransformationConfig(BaseConfig):
    """
    Config de transformation : constantes métier et chemins pour les 6 transformateurs.

    Hérite de BaseConfig pour les configs génériques (spark, chemins data/logs).
    Ajoute les constantes propres aux transfoms GTFS/BOTN/Ember/ADEME/OurAirpot/Merge.
    """

    # chemins d'entrée (données brutes produites par l'extraction)
    RAW_DATA_PATH: Path = BaseConfig.DATA_ROOT / "raw"
    MDB_RAW_PATH: Path = RAW_DATA_PATH / "mobilitydatabase"
    BOTN_RAW_PATH: Path = RAW_DATA_PATH / "back_on_track"
    EMBER_RAW_PATH: Path = RAW_DATA_PATH / "ember"
    GEONAMES_RAW_PATH: Path = RAW_DATA_PATH / "geonames"
    ADEME_RAW_PATH: Path = RAW_DATA_PATH / "ademe"
    OURAIRPORTS_RAW_PATH: Path = RAW_DATA_PATH / "ourairports"

    # chemins de sortie (schéma en étoile)
    PROCESSED_PATH: Path = BaseConfig.DATA_ROOT / "processed"
    TRAIN_TRIP_OUTPUT_PATH: Path = PROCESSED_PATH / "train_trips.parquet"
    LOCALITE_OUTPUT_PATH: Path = PROCESSED_PATH / "localite.parquet"
    EMISSION_OUTPUT_PATH: Path = PROCESSED_PATH / "emission.parquet"
    FLIGHT_OUTPUT_PATH: Path = PROCESSED_PATH / "flight.parquet"

    # paramètres d'écriture parquet
    TRIP_COALESCE_PARTITIONS: int = 40
    FLIGHT_COALESCE_PARTITIONS: int = 40
    DIM_COALESCE_PARTITIONS: int = 1

    # constantes géophysiques
    EARTH_RADIUS_M: float = 6_371_000.0
    RAIL_DETOUR_FACTOR: float = 1.3

    # types de route GTFS ferroviaires valides (115 = funiculaire exclu)
    VALID_ROUTE_TYPES: list[int] = [
        2, 100, 101, 102, 103, 104, 105, 106,
        107, 108, 109, 110, 111, 112, 113, 114, 116, 117
    ]

    # filtres temporels
    END_DATE_THRESHOLD: int = 20240101
    END_DATE_COUNTRY_TRIP_THRESHOLD: int = 500_000

    # geo-bucketing GeoNames
    GEO_BUCKET_SIZE: float = 0.5
    GEO_MAX_DIST_M: float = 15_000.0

    # types de route pour la table emission
    EMISSION_ROUTE_TYPES: list[int] = [2, 100, 101, 102, 103, 105, 106, 109]

    # conso énergétique par type de route (kWh/pkm)
    CONSUMPTION_MAP: dict[int, float] = {
        101: 0.050, 100: 0.040, 102: 0.040, 103: 0.040,
        105: 0.090, 106: 0.080, 109: 0.080, 2: 0.050
    }

    # taux d'électrification par pays (alpha-2)
    ELECTRIFICATION: dict[str, float] = {
        "CH": 1.00, "LI": 1.00, "BE": 0.88, "SE": 0.75, "NL": 0.74,
        "AT": 0.73, "IT": 0.72, "VA": 0.72, "PL": 0.64, "ES": 0.64,
        "AD": 0.64, "DE": 0.61, "FR": 0.60, "MC": 0.60, "GB": 0.38,
        "DK": 0.30, "IE": 0.03, "BA": 0.70, "BG": 0.74, "HR": 0.37,
        "CZ": 0.34, "FI": 0.55, "GR": 0.20, "HU": 0.41, "NO": 0.65,
        "PT": 0.65, "RO": 0.37, "SK": 0.44, "SI": 0.50
    }

    # émission diesel fixe (gCO₂/pkm)
    DIESEL_FIXED_GCO2_PKM: float = 65.0

    # mapping manuel agence MDB (source_key → (agency_id, agency_name, timezone))
    AGENCY_MANUAL_MAPPING: dict[str, tuple[str, str, str]] = {
        "DE/mdb-858":  ("VGN", "VGN (DB Regio Bayern)", "Europe/Berlin"),
        "PL/mdb-1290": ("SKM", "SKM Trójmiasto", "Europe/Warsaw"),
        "PL/tfs-790":  ("SKM", "SKM Trójmiasto", "Europe/Warsaw"),
        "ES/mdb-1064": ("FEVE", "Renfe FEVE", "Europe/Madrid"),
        "ES/mdb-1856": ("FGC", "FGC", "Europe/Madrid"),
        "EE/mdb-2015": ("PV", "Pasažieru Vilciens", "Europe/Riga"),
        "GR/mdb-1161": ("HT", "Hellenic Train", "Europe/Athens"),
        "IT/tld-958":  ("FGC", "Ferrovia Genova-Casella", "Europe/Rome"),
        "IT/mdb-1230": ("FGC", "Ferrovia Genova-Casella", "Europe/Rome"),
        "IT/mdb-2610": ("FGC", "Ferrovia Genova-Casella", "Europe/Rome"),
        "IT/tfs-1011": ("FGC", "Ferrovia Genova-Casella", "Europe/Rome"),
    }

    # mapping timezone par code pays alpha-2
    TZ_MAPPING: dict[str, str] = {
        "FR": "Europe/Paris", "DE": "Europe/Berlin", "AT": "Europe/Vienna",
        "CH": "Europe/Zurich", "IT": "Europe/Rome", "ES": "Europe/Madrid",
        "NL": "Europe/Amsterdam", "BE": "Europe/Brussels", "CZ": "Europe/Prague",
        "PL": "Europe/Warsaw", "HU": "Europe/Budapest", "SK": "Europe/Bratislava",
        "SI": "Europe/Ljubljana", "HR": "Europe/Zagreb", "RS": "Europe/Belgrade",
        "RO": "Europe/Bucharest", "BG": "Europe/Sofia", "GR": "Europe/Athens",
        "SE": "Europe/Stockholm", "NO": "Europe/Oslo", "FI": "Europe/Helsinki",
        "DK": "Europe/Copenhagen", "EE": "Europe/Tallinn", "LT": "Europe/Vilnius",
        "LV": "Europe/Riga", "PT": "Europe/Lisbon", "IE": "Europe/Dublin",
        "GB": "Europe/London", "TR": "Europe/Istanbul", "UA": "Europe/Kyiv",
        "MD": "Europe/Chisinau", "MK": "Europe/Skopje", "ME": "Europe/Podgorica",
        "AL": "Europe/Tirane", "LU": "Europe/Luxembourg",
    }

    # mapping micro-états alpha-3 → alpha-2
    MICRO_STATES_MAP: dict[str, str] = {
        "MCO": "FR",
        "VAT": "IT",
        "LIE": "CH",
    }

    # -- configuration ADEME (émissions aériennes)

    # pondérations pour le mapping trajet_type → émission aérienne
    # format : (emission_id, trajet_type, {clé_base: poids})
    # clé_base = "{seat_category}_{distance_category}" de la table ADEME de base
    ADEME_EMISSION_WEIGHTS: list[tuple[int, str, dict[str, float]]] = [
        (100, "small_small_1000km",     {"small_short": 0.7, "medium_short": 0.3}),
        (101, "small_medium_1000km",    {"small_short": 0.5, "medium_short": 0.5}),
        (102, "small_large_1000km",     {"small_short": 0.4, "medium_short": 0.6}),
        (103, "medium_medium_5000km",   {"small_medium": 0.2, "medium_medium": 0.6, "large_medium": 0.2}),
        (104, "medium_large_unlimited", {"small_medium": 0.1, "medium_long": 0.5, "large_long": 0.4}),
        (105, "large_medium_5000km",    {"small_medium": 0.1, "medium_medium": 0.4, "large_medium": 0.5}),
        (106, "large_large_unlimited",  {"small_medium": 0.1, "medium_long": 0.3, "large_long": 0.6}),
    ]

    # --- configuration OurAirports (trajets aériens)

    # pays européens (44 codes ISO 2-char)
    EUROPEAN_COUNTRIES: list[str] = [
        "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
        "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
        "PL", "PT", "RO", "SK", "SI", "ES", "SE", "CH", "GB", "NO",
        "IS", "RS", "BA", "ME", "MK", "MD", "UA", "BY", "RU", "AL",
        "GE", "AZ", "TR", "AM",
    ]

    #limites géographiques Europe (pour filtrer la Russie asiatique)
    EUROPE_LAT_MIN: float = 35.0
    EUROPE_LAT_MAX: float = 71.0
    EUROPE_LON_MIN: float = -25.0
    EUROPE_LON_MAX: float = 45.0

    # distance minimale pour un trajet aérien (km)
    FLY_MIN_DISTANCE_KM: float = 10.0

    # stop-matching (gare ↔ aéroport)
    STOP_MATCHING_OUTPUT_PATH: Path = PROCESSED_PATH / "stop_matching.parquet"
    STOP_MATCHING_MAX_DIST_M: float = 100_000.0
    STOP_MATCHING_BUCKET_SIZE: float = 1.0

    # colonnes GTFS attendues par table (schéma strict)
    GTFS_COLS_TRIPS: list[str] = [
        "source", "trip_id", "route_id", "service_id",
        "trip_headsign", "trip_short_name"
    ]
    GTFS_COLS_ROUTES: list[str] = [
        "source", "route_id", "route_type", "route_short_name",
        "route_long_name", "agency_id"
    ]
    GTFS_COLS_STOP_TIMES: list[str] = [
        "source", "trip_id", "stop_id", "arrival_time",
        "departure_time", "stop_sequence"
    ]
    GTFS_COLS_STOPS: list[str] = [
        "source", "stop_id", "stop_name", "stop_lat",
        "stop_lon", "parent_station"
    ]
    GTFS_COLS_AGENCY: list[str] = [
        "source", "agency_id", "agency_name", "agency_timezone"
    ]
    GTFS_COLS_CALENDAR: list[str] = [
        "source", "service_id", "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday", "start_date", "end_date"
    ]
    GTFS_COLS_CALENDAR_DATES: list[str] = [
        "source", "service_id", "date", "exception_type"
    ]

    # jours de la semaine (ordre GTFS)
    DAY_COLS: list[str] = [
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday"
    ]

    #mapping jour de la semaine spark (dayofweek : 1=Dim, 2=Lun, ..., 7=Sam)
    DOW_MAP: list[tuple[str, int]] = [
        ("monday", 2), ("tuesday", 3), ("wednesday", 4), ("thursday", 5),
        ("friday", 6), ("saturday", 7), ("sunday", 1),
    ]

    # colonnes string à normaliser dans BOTN pour alignement avec MDB
    BOTN_STRING_COLS: list[str] = [
        "trip_id", "route_id", "service_id", "stop_id", "agency_id",
        "stop_name", "trip_headsign", "trip_short_name",
        "route_short_name", "route_long_name", "parent_station",
        "arrival_time", "departure_time", "start_date", "end_date",
        "agency_name", "agency_timezone", "city", "country"
    ]

    @classmethod
    def validate(cls) -> None:
        """
        Crée les dossiers de sortie pour les tables transformées.

        Notes
        -----
        Appelée automatiquement à l'import. Crée processed/ si absent.
        """
        super().validate()
        cls.PROCESSED_PATH.mkdir(parents=True, exist_ok=True)


# validation automatique à l'import
TransformationConfig.validate()
