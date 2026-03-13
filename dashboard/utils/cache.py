"""
Stratégie de cache ObRail Dashboard.

Utilise diskcache pour :
1. TTL-based response caching des appels API (clés stables par (endpoint, params))
2. Background callbacks Dash (CacheManager partagé)

TTL classes :
- LONG   (30 min) : référentiel, qualité, stats — données post-ETL stables
- MEDIUM (15 min) : agrégats jour/nuit, carbon factors, operators
- SHORT  ( 5 min) : recherche, routes filtrées, pagination interactive
"""

import hashlib
import json
import os
from typing import Any, Callable, Optional

import diskcache

# Le cache vit dans le module dashboard pour rester local au projet, sans dépendre du système.
_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", ".cache")

cache = diskcache.Cache(
    directory=_CACHE_DIR,
    size_limit=500 * 1024 * 1024,  # Limite volontaire pour éviter une croissance silencieuse en long run.
    eviction_policy="least-recently-used",
)

# TTL calibrés par fréquence de variation: plus la donnée est stable, plus le TTL est long.
TTL_LONG   = 30 * 60   # Référentiels et agrégats stables entre deux cycles ETL.
TTL_MEDIUM = 15 * 60   # Données consultées souvent mais peu volatiles sur une session.
TTL_SHORT  =  5 * 60   # Recherche/pagination interactive: on privilégie la fraîcheur.
TTL_24H    = 24 * 60 * 60  # KPI globaux sans filtre utilisateur, très peu changeants en journée.


def _make_key(endpoint: str, params: Optional[dict]) -> str:
    """Construit une clé déterministe pour un couple endpoint+params.

    Le tri JSON des paramètres garantit qu'un même appel fonctionnel mappe toujours
    vers la même entrée, indépendamment de l'ordre des clés.
    """
    payload = json.dumps({"ep": endpoint, "p": params or {}}, sort_keys=True)
    return "api:" + hashlib.md5(payload.encode()).hexdigest()


def cached_call(
    fn: Callable[[], Any],
    endpoint: str,
    params: Optional[dict] = None,
    ttl: int = TTL_MEDIUM,
) -> Any:
    """
    Appelle fn() (doit retourner les données brutes) uniquement si la clé n'est
    pas en cache ou est expirée. Stocke le résultat avec le TTL donné.

    Usage:
        data = cached_call(lambda: client.get("/cities", params={"country": "FR"}),
                           endpoint="/cities", params={"country": "FR"}, ttl=TTL_LONG)
    """
    key = _make_key(endpoint, params)
    hit = cache.get(key, default=None)
    if hit is not None:
        return hit
    result = fn()
    cache.set(key, result, expire=ttl)
    return result


def invalidate(endpoint: str, params: Optional[dict] = None) -> None:
    """Supprime l'entrée de cache pour un endpoint/params donné."""
    cache.delete(_make_key(endpoint, params))


def clear_api_cache() -> int:
    """Supprime toutes les entrées API (préfixe 'api:') et retourne le nombre supprimé."""
    removed = 0
    for key in list(cache.iterkeys()):
        if isinstance(key, str) and key.startswith("api:"):
            cache.delete(key)
            removed += 1
    return removed


def get_cache_manager() -> diskcache.Cache:
    """Retourne l'instance partagée (utilisé par Dash background callback manager)."""
    return cache
