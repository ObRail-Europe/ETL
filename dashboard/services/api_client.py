"""
Couche service API ObRail Dashboard.

Principes :
- Toutes les communications HTTP passent par ObRailClient (zéro appel httpx direct dans les pages)
- Parallelisation via ThreadPoolExecutor — safe pour les callbacks Dash synchrones
- Chaque réponse retourne un dict normalisé : {"data": ..., "ok": True} ou {"error": ..., "ok": False}
- Wrapping cache transparent via cached_call()
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import httpx

from dashboard.utils.cache import TTL_LONG, TTL_MEDIUM, TTL_SHORT, cached_call

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)  # Niveau verbeux utile pour diagnostiquer les flux API en dev.

# Paramètres centralisés pour éviter des comportements divergents selon les pages.
_BASE_URL = os.getenv("DASHBOARD_API_URL", "http://localhost:8000/api/v1")
_TIMEOUT  = httpx.Timeout(900.0, connect=5.0)   # Lecture longue tolérée, connexion courte pour fail-fast réseau.
_MAX_WORKERS = 8                                 # Borne prudente pour paralléliser sans saturer le service API.


class ObRailClient:
    """
    Client HTTP synchrone centralisé pour l'API ObRail.

    La méthode principale est `get()` (single call, avec cache) et
    `parallel()` (multi-appels indépendants en parallèle).

    Utilise httpx.Client en mode Keep-Alive pour réutiliser les connexions.
    """

    def __init__(self, base_url: str = _BASE_URL):
        self._base = base_url.rstrip("/")
        self._http  = httpx.Client(
            base_url=self._base,
            timeout=_TIMEOUT,
            headers={"Accept": "application/json"},
        )
        logger.info(f"ObRailClient initialized: {self._base}")

    def _raw_get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """
        GET brut sans cache. Retourne toujours un dict normalisé.
        Ne lève jamais d'exception — les erreurs sont capturées et retournées.
        """
        clean_params = {k: v for k, v in (params or {}).items() if v is not None}
        logger.debug(f"API GET {endpoint} with params {clean_params}")
        try:
            r = self._http.get(endpoint, params=clean_params)
            if r.status_code == 200:
                logger.debug(f"API {endpoint} → HTTP 200")
                return {"ok": True, "data": r.json()}
            else:
                logger.warning("API %s %s → HTTP %s", "GET", endpoint, r.status_code)
                return {
                    "ok": False,
                    "error": f"HTTP {r.status_code}",
                    "detail": _safe_detail(r),
                    "status_code": r.status_code,
                }
        except httpx.TimeoutException:
            logger.error("Timeout GET %s", endpoint)
            return {"ok": False, "error": "Timeout — l'API ne répond pas.", "status_code": 0}
        except httpx.RequestError as exc:
            logger.error("Erreur réseau GET %s: %s", endpoint, exc)
            return {"ok": False, "error": f"Erreur réseau : {exc}", "status_code": 0}

    def get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        ttl: int = TTL_MEDIUM,
        no_cache: bool = False,
    ) -> dict:
        """GET avec cache diskcache. ttl=0 ou no_cache=True → cache contourné."""
        if no_cache or ttl == 0:
            return self._raw_get(endpoint, params)
        return cached_call(
            fn=lambda: self._raw_get(endpoint, params),
            endpoint=endpoint,
            params=params,
            ttl=ttl,
        )

    def parallel(self, calls: list[tuple[str, Optional[dict], int]]) -> list[dict]:
        """
        Exécute plusieurs GET en parallèle via ThreadPoolExecutor.

        Args:
            calls: liste de tuples (endpoint, params, ttl)
                   e.g. [("/quality/summary", None, TTL_LONG), ("/stats/operators", None, TTL_MEDIUM)]

        Returns:
            liste de résultats normalisés dans le même ordre que calls.
        """
        futures_map: dict = {}
        results: list[dict] = [None] * len(calls)

        with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(calls))) as pool:
            for i, (ep, params, ttl) in enumerate(calls):
                f = pool.submit(self.get, ep, params, ttl)
                futures_map[f] = i
            for future in as_completed(futures_map):
                idx = futures_map[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # pragma: no cover
                    results[idx] = {"ok": False, "error": str(exc), "status_code": 0}

        return results

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# Instance unique: le pool Keep-Alive reste chaud entre callbacks et limite le coût réseau.
client = ObRailClient()


def _safe_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
        return body.get("detail", str(body))[:200]
    except Exception:
        return response.text[:200]


def safe_items(result: dict) -> list:
    """Extrait la liste d'items d'une réponse, paginée ou directe."""
    if not result.get("ok"):
        return []
    data = result.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or data.get("items") or []
    return []


def safe_data(result: dict) -> dict:
    """Extrait le dict 'data' d'un résultat, ou {} en cas d'erreur."""
    if result.get("ok"):
        return result.get("data") or {}
    return {}


def error_msg(result: dict) -> str:
    """Retourne le message d'erreur humain d'un résultat raté."""
    return result.get("error", "Erreur inconnue")
