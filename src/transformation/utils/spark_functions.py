"""
Fonctions utilitaires PySpark partagées entre tous les transformateurs
Contient les calculs geographiques, la fusion de dataframes, et les fonctions de nettoyage
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


EARTH_RADIUS_M: float = 6_371_000.0


def haversine_dist(
    lat1: F.Column,
    lon1: F.Column,
    lat2: F.Column,
    lon2: F.Column
) -> F.Column:
    """
    Calcule la distance en mètres entre deux points GPS via la formule Haversine

    Parameters
    ----------
    lat1 : Column
        Latitude du point de départ (degrés décimaux)
    lon1 : Column
        Longitude du point de départ (degrés décimaux)
    lat2 : Column
        Latitude du point d'arrivée (degrés décimaux)
    lon2 : Column
        Longitude du point d'arrivée (degrés décimaux)

    Returns
    -------
    Column
        Distance en mètres (expression Catalyst, évaluée par Spark)
    """
    lat1_rad, lat2_rad = F.radians(lat1), F.radians(lat2)
    lon1_rad, lon2_rad = F.radians(lon1), F.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2) ** 2
    c = 2 * F.asin(F.sqrt(a))
    return EARTH_RADIUS_M * c


def tree_union(dfs: list[DataFrame]) -> DataFrame | None:
    """
    Fusionne une liste de dataframes via un arbre binaire (plus rapide que reduce)

    Parameters
    ----------
    dfs : list[DataFrame]
        Liste de DataFrames à fusionner (doivent avoir le même schéma)

    Returns
    -------
    DataFrame ou None
        DataFrame fusionné, ou None si la liste est vide

    Notes
    -----
    La fusion en arbre O(log n) évite la profondeur de plan linéaire
    d'un reduce successif, ce qui accélère la planification Catalyst
    """
    if not dfs:
        return None
    while len(dfs) > 1:
        next_dfs = []
        for i in range(0, len(dfs), 2):
            if i + 1 < len(dfs):
                next_dfs.append(dfs[i].union(dfs[i + 1]))
            else:
                next_dfs.append(dfs[i])
        dfs = next_dfs
    return dfs[0]


def replace_blank_with_nulls(
    df: DataFrame,
    extra_blank_tokens: list[str] | None = None
) -> DataFrame:
    """
    Remplace les chaînes vides et tokens invalides par NULL dans les colonnes string

    Parameters
    ----------
    df : DataFrame
        DataFrame à nettoyer
    extra_blank_tokens : list[str], optional
        Tokens supplémentaires à traiter comme valeurs nulles

    Returns
    -------
    DataFrame
        DataFrame avec les chaînes vides remplacées par NULL
    """
    blank_tokens = ["", "---/---", "-/-"]
    if extra_blank_tokens:
        blank_tokens.extend(extra_blank_tokens)

    str_cols = [c for c, t in df.dtypes if t == "string"]
    for col_name in str_cols:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)).isin(blank_tokens), F.lit(None))
             .otherwise(F.col(col_name))
        )
    return df


def invalid_coord_expr(
    lat_col: str = "stop_lat",
    lon_col: str = "stop_lon"
) -> F.Column:
    """
    Expression Catalyst détectant des coordonnées GPS invalides

    Parameters
    ----------
    lat_col : str, optional
        Nom de la colonne latitude (défaut: 'stop_lat')
    lon_col : str, optional
        Nom de la colonne longitude (défaut: 'stop_lon')

    Returns
    -------
    Column
        Expression booléenne True si les coordonnées sont invalides
        (NULL, hors limites, ou Null Island lat≈0/lon≈0)
    """
    return (
        F.col(lat_col).isNull()
        | F.col(lon_col).isNull()
        | (F.abs(F.col(lat_col)) > 90)
        | (F.abs(F.col(lon_col)) > 180)
        | ((F.abs(F.col(lat_col)) < 1e-9) & (F.abs(F.col(lon_col)) < 1e-9))
    )


def apply_tz_mapping(
    df: DataFrame,
    tz_mapping: dict[str, str],
    country_col: str = "country"
) -> DataFrame:
    """
    Corrige les fuseaux horaires génériques (CET, UTC, NULL) par pays

    Parameters
    ----------
    df : DataFrame
        DataFrame avec colonne agency_timezone
    tz_mapping : dict[str, str]
        Mapping code pays alpha-2 → timezone IANA (ex: {"FR": "Europe/Paris"})
    country_col : str, optional
        Nom de la colonne contenant le code pays alpha-2 (défaut: "country")

    Returns
    -------
    DataFrame
        DataFrame avec agency_timezone corrigé
    """
    tz_map = F.create_map(
        [F.lit(x) for k, v in tz_mapping.items() for x in (k, v)]
    )
    return df.withColumn(
        "agency_timezone",
        F.when(
            F.col("agency_timezone").isin("CET", "UTC") | F.col("agency_timezone").isNull(),
            F.coalesce(tz_map[F.col(country_col)], F.col("agency_timezone"))
        ).otherwise(F.col("agency_timezone"))
    )
