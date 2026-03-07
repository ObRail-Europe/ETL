"""
Extracteur pour Mobility Database (principale source de feeds GTFS en Europe).

On télécharge ~1000 feeds pour les 27 pays UE - d'où l'asyncio/aiohttp pour faire ça
en parallèle. Chaque feed = 1 réseau de transport.
"""

import asyncio
import aiohttp
import aiofiles
import zipfile
import shutil
import json
from pathlib import Path
from typing import Any
from datetime import datetime

from .base_extractor import BaseExtractor

ALLOWED_FEEDS: frozenset[str] = frozenset({
    "AT:mdb-1832", "AT:mdb-2138", "AT:mdb-770", "AT:mdb-783", "AT:mdb-900", "AT:mdb-914",
    "CZ:mdb-1082", "CZ:mdb-767", "CZ:mdb-771",
    "DE:mdb-1085", "DE:mdb-1091", "DE:mdb-1092", "DE:mdb-1094", "DE:mdb-1139",
    "DE:mdb-1172", "DE:mdb-1173", "DE:mdb-1224", "DE:mdb-1225", "DE:mdb-1226",
    "DE:mdb-2231", "DE:mdb-2393", "DE:mdb-2651", "DE:mdb-2661", "DE:mdb-2899",
    "DE:mdb-772", "DE:mdb-778", "DE:mdb-780", "DE:mdb-781", "DE:mdb-782",
    "DE:mdb-784", "DE:mdb-785", "DE:mdb-858", "DE:mdb-906",
    "DE:tdg-82199", "DE:tfs-213",
    "DK:mdb-1077",
    "EE:mdb-1095", "EE:mdb-2015", "EE:mdb-2337",
    "ES:mdb-1003", "ES:mdb-1064", "ES:mdb-1065", "ES:mdb-1079", "ES:mdb-1205",
    "ES:mdb-1259", "ES:mdb-1782", "ES:mdb-1856", "ES:mdb-2141", "ES:mdb-2620",
    "ES:mdb-2653", "ES:mdb-2715", "ES:mdb-2717", "ES:mdb-2720", "ES:mdb-2766",
    "ES:mdb-2785", "ES:mdb-2826", "ES:mdb-2827", "ES:mdb-2832", "ES:mdb-790",
    "ES:tfs-655",
    "FI:mdb-865",
    "FR:mdb-1026", "FR:mdb-1069", "FR:mdb-1153", "FR:mdb-1258", "FR:mdb-1260",
    "FR:mdb-1283", "FR:mdb-1291", "FR:mdb-1783", "FR:mdb-1837", "FR:mdb-2144",
    "FR:mdb-845", "FR:tdg-11681", "FR:tdg-67595", "FR:tdg-79818", "FR:tdg-80921",
    "FR:tdg-80931", "FR:tdg-81474", "FR:tdg-81559", "FR:tdg-81942", "FR:tdg-81969",
    "FR:tdg-82285", "FR:tdg-82386", "FR:tdg-83021", "FR:tdg-83448", "FR:tdg-83582",
    "FR:tdg-83620", "FR:tdg-83634", "FR:tdg-83647", "FR:tdg-83675",
    "FR:tfs-413", "FR:tld-725",
    "GB:mdb-2364", "GB:mdb-2431", "GB:tld-5901",
    "GR:mdb-1161",
    "HR:mdb-2920", "HR:mdb-769",
    "HU:mdb-990", "HU:tfs-42", "HU:tfs-625",
    "IE:mdb-2637", "IE:mdb-955",
    "IT:mdb-1031", "IT:mdb-1052", "IT:mdb-1058", "IT:mdb-1075", "IT:mdb-1089",
    "IT:mdb-1142", "IT:mdb-1170", "IT:mdb-1230", "IT:mdb-1266", "IT:mdb-1319",
    "IT:mdb-2610", "IT:mdb-2997", "IT:mdb-840", "IT:mdb-855", "IT:mdb-895",
    "IT:tdg-81653", "IT:tfs-1011", "IT:tfs-542", "IT:tfs-664",
    "IT:tld-4833", "IT:tld-958",
    "LT:mdb-2991",
    "LU:mdb-1108",
    "NL:mdb-1859", "NL:mdb-686", "NL:mdb-768",
    "NO:mdb-1078",
    "PL:mdb-1008", "PL:mdb-1011", "PL:mdb-1290", "PL:mdb-1321", "PL:mdb-1908",
    "PL:mdb-2087", "PL:mdb-2088", "PL:mdb-2091", "PL:mdb-2092", "PL:mdb-853",
    "PL:mdb-863", "PL:tfs-789", "PL:tfs-790", "PL:tld-5701",
    "PT:mdb-1034", "PT:mdb-2057", "PT:tld-715",
    "RO:mdb-1104",
    "RS:mdb-2927",
    "SE:mdb-1292", "SE:mdb-1320",
    "SI:mdb-2940",
    "SK:mdb-2155",
})

class MobilityDatabaseExtractor(BaseExtractor):
    """
    Extracteur pour Mobility Database (principale source de feeds GTFS en Europe).

    Notes
    -----
    Télécharge ~1000 feeds pour les 27 pays UE - d'où l'asyncio/aiohttp
    pour faire ça en parallèle. Chaque feed = 1 réseau de transport.
    """

    def get_source_name(self) -> str:
        """
        Retourne l'identifiant de la source.

        Returns
        -------
        str
            Nom de la source ('mobility_database')
        """
        return "mobility_database"

    def get_default_output_dir(self) -> Path:
        """
        Retourne le répertoire de sortie par défaut.

        Returns
        -------
        Path
            Chemin du répertoire Mobility Database
        """
        return self.config.MOBILITY_OUTPUT_DIR

    def extract(self) -> dict[str, Any]:
        """
        Lance l'extraction complète - wrapper synchrone pour l'extraction async.

        Returns
        -------
        dict[str, Any]
            Stats de téléchargement (nb feeds, taille totale, compression, chemins)

        Raises
        ------
        RuntimeError
            Si l'extraction plante complètement
        """
        # l'API Mobility nécessite un token gratuit mais obligatoire
        if not self.config.MOBILITY_API_REFRESH_TOKEN:
            self.logger.warning("Token API Mobility Database non défini - extraction de la source ignorée")
            return {
                'status': 'SKIPPED',
                'reason': 'Token API non configuré',
                'files_downloaded': 0,
                'total_size_bytes': 0,
                'output_paths': []
            }
        
        self.logger.info("Démarrage de l'extraction des données Mobility Database depuis l'API...")

        try:
            result = asyncio.run(self._async_extract())
            return result
            
        except Exception as e:
            raise RuntimeError(f"Échec extraction asynchrone: {e}")
    
    async def _async_extract(self) -> dict[str, Any]:
        """
        Gère toute l'extraction : auth → liste des feeds → téléchargement parallèle.

        Returns
        -------
        dict[str, Any]
            Stats agrégées (total, succès, échecs, taille, compression)

        Notes
        -----
        Méthode asynchrone - utiliser avec asyncio.run().
        """
        stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0
        }

        # limite le nombre de connexions - sans ça aiohttp ouvre 1000 sockets en même temps
        connector = aiohttp.TCPConnector(
            limit=self.config.MOBILITY_MAX_CONCURRENT * 2,
            limit_per_host=10,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )

        # timeouts généreux pour les téléchargements - certains feeds font 100+MB sur serveurs lents
        download_timeout = aiohttp.ClientTimeout(
            total=1200,      # 20min max par fichier
            connect=60,      # 1min pour établir la connexion
            sock_read=300    # 5min de silence max pendant le téléchargement
        )
        # timeouts plus stricts pour les appels API (métadonnées)
        api_timeout = aiohttp.ClientTimeout(
            total=180,
            connect=30,
            sock_read=150
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=download_timeout,
            raise_for_status=False
        ) as session:

            # étape 1 : authentification OAuth2
            access_token = await self._authenticate(session, api_timeout)

            # étape 2 : liste tous les feeds pour les 27 pays UE
            feeds = await self._fetch_all_feeds(
                session,
                access_token,
                api_timeout
            )

            if not feeds:
                return {
                    'feeds_total': 0,
                    'files_downloaded': 0,
                    'total_size_bytes': 0,
                    'output_paths': []
                }
            

            
            def _get_feed_key(feed: dict[str, Any]) -> str:
                locations: list[dict[str, str]] = feed.get('locations') or []
                country = locations[0].get('country_code', 'XX').upper() if locations else 'XX'
                return f"{country}:{feed.get('id', '')}"

            feeds = [feed for feed in feeds if _get_feed_key(feed) in ALLOWED_FEEDS]

            stats['total'] = len(feeds)
            self.logger.info(f"Téléchargement de {len(feeds)} feeds validé GTFS depuis Mobility Database...")

            # étape 3 : téléchargement en parallèle avec limite de concurrence
            results = await self._download_all_feeds(
                session,
                feeds,
                stats
            )

        # agrégation des résultats
        output_paths = [r['path'] for r in results if r['status'] == 'success']
        total_size = sum(r.get('size', 0) for r in results if r['status'] == 'success')
        total_original_size = sum(r.get('original_size', 0) for r in results if r['status'] == 'success')

        compression_ratio = ((total_original_size - total_size) / total_original_size * 100) if total_original_size > 0 else 0

        # répartition par pays pour le rapport final
        by_country = {}
        for result in results:
            if result['status'] == 'success':
                country = result.get('country', 'UNKNOWN')
                if country not in by_country:
                    by_country[country] = {'count': 0, 'size': 0}
                by_country[country]['count'] += 1
                by_country[country]['size'] += result.get('size', 0)

        return {
            'feeds_total': stats['total'],
            'files_downloaded': stats['success'],
            'files_failed': stats['failed'],
            'files_skipped': stats['skipped'],
            'total_size_bytes': total_size,
            'original_size_bytes': total_original_size,
            'space_saved_bytes': total_original_size - total_size,
            'compression_ratio': f"{compression_ratio:.1f}%",
            'output_paths': output_paths,
            'by_country': by_country
        }
    
    async def _authenticate(
        self,
        session: aiohttp.ClientSession,
        timeout: aiohttp.ClientTimeout
    ) -> str:
        """
        Échange le refresh token contre un access token JWT.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session HTTP async
        timeout : aiohttp.ClientTimeout
            Timeouts pour l'appel API

        Returns
        -------
        str
            Access token valide quelques heures

        Raises
        ------
        RuntimeError
            Si l'API refuse le refresh token
        """
        self.logger.info("Authentification à l'API Mobility Database")

        url = f"{self.config.MOBILITY_API_BASE_URL}/tokens"
        payload = {"refresh_token": self.config.MOBILITY_API_REFRESH_TOKEN}

        try:
            async with session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=timeout
            ) as response:
                response.raise_for_status()
                data = await response.json()
                access_token = data['access_token']

                self.logger.info("Authentification à l'API Mobility Database réussie")
                return access_token

        except Exception as e:
            raise RuntimeError(f"Échec authentification: {e}")
    
    async def _fetch_all_feeds(
        self,
        session: aiohttp.ClientSession,
        access_token: str,
        timeout: aiohttp.ClientTimeout
    ) -> list[dict[str, Any]]:
        """
        Récupère la liste complète des feeds GTFS pour tous les pays UE.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session HTTP async
        access_token : str
            Token JWT d'authentification
        timeout : aiohttp.ClientTimeout
            Timeouts pour les appels API

        Returns
        -------
        list[dict[str, Any]]
            Liste aplatie de tous les feeds (chaque feed = 1 réseau)
        """
        self.logger.info(
            f"Fetch de l'API Mobility Database pour la récupération des feeds pour {len(self.config.MOBILITY_EU_COUNTRIES)} pays..."
        )

        # limite à 5 appels API simultanés pour ne pas se faire rate-limit
        api_semaphore = asyncio.Semaphore(5)

        # lance une requête par pays en parallèle
        tasks = [
            self._fetch_feeds_for_country(
                session,
                access_token,
                country,
                timeout,
                api_semaphore
            )
            for country in self.config.MOBILITY_EU_COUNTRIES
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # aplatit les listes de feeds par pays en une seule liste
        all_feeds: list[dict[str, Any]] = []
        for feeds in results:
            if isinstance(feeds, list):
                all_feeds.extend(feeds)
            elif isinstance(feeds, Exception):
                self.logger.warning(f"Erreur lors d'un fetch de Mobility Database: {feeds}")

        self.logger.info(f"Fetch de l'API Mobility Database terminé - {len(all_feeds)} feeds GTFS récupérés")
        return all_feeds
    
    async def _fetch_feeds_for_country(
        self,
        session: aiohttp.ClientSession,
        access_token: str,
        country: str,
        timeout: aiohttp.ClientTimeout,
        semaphore: asyncio.Semaphore,
        retry: int = 0
    ) -> list[dict[str, Any]]:
        """
        Récupère tous les feeds pour un pays donné avec pagination automatique.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session HTTP async
        access_token : str
            Token JWT
        country : str
            Code pays ISO (ex: 'FR', 'DE')
        timeout : aiohttp.ClientTimeout
            Timeouts pour l'API
        semaphore : asyncio.Semaphore
            Sémaphore pour limiter la concurrence
        retry : int, optional
            Tentative actuelle pour retry exponentiel (défaut: 0)

        Returns
        -------
        list[dict[str, Any]]
            Feeds du pays (peut être vide si aucun feed actif)
        """
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        url = f"{self.config.MOBILITY_API_BASE_URL}/gtfs_feeds"
        all_feeds: list[dict[str, Any]]= []
        offset = 0
        limit = 100

        try:
            # pagination manuelle - l'API renvoie 100 feeds max par page
            while True:
                async with semaphore:
                    await asyncio.sleep(0.5)  # petit délai pour éviter le rate limiting

                    params: dict[str, str | int] = {
                        'country_code': country,
                        'status': 'active',  # ignore les feeds obsolètes
                        'limit': limit,
                        'offset': offset
                    }

                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        timeout=timeout
                    ) as response:
                        response.raise_for_status()
                        data = await response.json()

                        if not data:
                            break

                        all_feeds.extend(data)

                        # si on reçoit moins que le limit, c'est la dernière page
                        if len(data) < limit:
                            break

                        offset += limit

            if all_feeds:
                self.logger.debug(f"{len(all_feeds)} feeds GTFS ont été fetchés depuis Mobility Database pour {country}")

            return all_feeds

        except Exception:
            # retry avec backoff exponentiel (1s, 2s, 4s)
            if retry < 3:
                wait_time = 2 ** retry
                self.logger.warning(
                    f"Erreur du fetch de {country} dans Mobility Database - retry {retry + 1}/3"
                )
                await asyncio.sleep(wait_time)
                return await self._fetch_feeds_for_country(
                    session, access_token, country, timeout, semaphore, retry + 1
                )
            else:
                self.logger.error(f"Échec du fetch de {country} après 3 tentatives - le pays sera ignoré dans l'extraction")
                return []
    
    async def _download_all_feeds(
        self,
        session: aiohttp.ClientSession,
        feeds: list[dict[str, Any]],
        stats: dict[str, int]
    ) -> list[dict[str, Any]]:
        """
        Lance le téléchargement de tous les feeds en parallèle avec limite de concurrence.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session HTTP async
        feeds : list[dict[str, Any]]
            Liste complète des feeds à télécharger
        stats : dict[str, int]
            Dictionnaire de stats (modifié en place)

        Returns
        -------
        list[dict[str, Any]]
            Résultats de chaque téléchargement (success/failed/skipped)
        """
        # limite le nombre de téléchargements simultanés (config par défaut = 20)
        download_semaphore = asyncio.Semaphore(self.config.MOBILITY_MAX_CONCURRENT)

        # set des feeds en cours de téléchargement pour éviter les doublons
        # empêche les race conditions où plusieurs tâches téléchargent le même feed
        feeds_in_progress: set[str] = set()
        # lock pour protéger l'accès au set feeds_in_progress
        progress_lock = asyncio.Lock()

        # crée toutes les tâches async
        tasks = [
            self._download_feed(session, feed, download_semaphore, feeds_in_progress, progress_lock)
            for feed in feeds
        ]

        # exécute et affiche la progression au fur et à mesure
        results: list[dict[str, Any]]= []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)

            # met à jour les stats
            status = result['status']
            if status == 'success':
                stats['success'] += 1
            elif status == 'failed':
                stats['failed'] += 1
            else:
                stats['skipped'] += 1

            # log tous les 50 feeds ou à la fin
            total = stats['success'] + stats['failed'] + stats['skipped']
            if total % 100 == 0 or total == stats['total']:
                self.logger.info(
                    f"Téléchargement Mobility Database: {total}/{stats['total']} "
                    f"(ok: {stats['success']} | fail: {stats['failed']} | skip: {stats['skipped']})"
                )

        return results
    
    async def _download_feed(
        self,
        session: aiohttp.ClientSession,
        feed: dict[str, Any],
        semaphore: asyncio.Semaphore,
        feeds_in_progress: set[str],
        progress_lock: asyncio.Lock,
        retry: int = 0
    ) -> dict[str, Any]:
        """
        Télécharge un feed GTFS et le convertit en Parquet à la volée.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session HTTP async
        feed : dict[str, Any]
            Métadonnées du feed avec URL de téléchargement
        semaphore : asyncio.Semaphore
            Limite le nombre de téléchargements simultanés
        feeds_in_progress : set[str]
            Set des feed_ids en cours de téléchargement
        progress_lock : asyncio.Lock
            Lock pour protéger l'accès au set feeds_in_progress
        retry : int, optional
            Tentative actuelle pour retry exponentiel (défaut: 0)

        Returns
        -------
        dict[str, Any]
            Résultat avec status + métriques de compression Parquet
        """
        feed_id = feed.get('id', 'unknown')
        latest_dataset: dict[str, str] | None = feed.get('latest_dataset')
        download_url = latest_dataset.get('hosted_url') if isinstance(latest_dataset, dict) else None

        # certains feeds n'ont pas d'URL de téléchargement (feeds inactifs ou cassés)
        if not download_url:
            return {
                'status': 'skipped',
                'feed_id': feed_id,
                'reason': 'No download URL'
            }

        # récupère le pays et le provider pour organiser les fichiers
        locations: list[dict[str, Any]] | None = feed.get('locations')
        country = 'XX'
        if locations and len(locations) > 0:
            country = locations[0].get('country_code', 'XX').upper()
        provider = feed.get('provider') or 'unknown'

        # structure de sortie : {country}/{feed_id}/ contient les fichiers Parquet
        country_dir = self.output_dir / country
        parquet_dir = country_dir / str(feed_id)
        country_dir.mkdir(parents=True, exist_ok=True)

        # clé unique pour identifier ce feed (country:feed_id)
        feed_key = f"{country}:{feed_id}"

        # chemin du fichier ZIP temporaire (défini ici pour être accessible dans except)
        temp_zip_path = country_dir / f"{feed_id}_temp.zip"

        # vérifie si ce feed est déjà en cours de téléchargement
        async with progress_lock:
            if feed_key in feeds_in_progress:
                self.logger.debug(
                    f"Feeds {country}:{feed_id} déjà en cours de téléchargement par une autre tâche - skip"
                )
                return {
                    'status': 'skipped',
                    'feed_id': feed_id,
                    'reason': 'Download already in progress'
                }
            # marque ce feed comme en cours
            feeds_in_progress.add(feed_key)

        # télécharge le feed - on s'assure de le retirer du set même en cas d'erreur
        try:
            # skip si déjà converti avec succès - vérifie le statut dans metadata.json
            # retente automatiquement en cas d'échec ou de succès partiel
            metadata_file = parquet_dir / 'metadata.json'
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)

                    # détermine le statut à partir des métriques
                    files_converted = metadata.get('files_converted', 0)
                    files_failed = metadata.get('files_failed', [])

                    # skip seulement si conversion complètement réussie
                    if files_converted > 0 and not files_failed:
                        return {
                            'status': 'skipped',
                            'feed_id': feed_id,
                            'path': str(parquet_dir),
                            'reason': 'Parquet files already exist (success)'
                        }
                    else:
                        # échec ou succès partiel - on nettoie et retente
                        self.logger.debug(
                            f"Feeds {country}:{feed_id} partiellement converti précédemment - retentative de téléchargement et conversion"
                        )
                        shutil.rmtree(parquet_dir, ignore_errors=True)
                        parquet_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    # metadata.json corrompu - on nettoie et retente
                    self.logger.debug(f"Feeds {country}:{feed_id} avec Metadata corrompu - retentative de téléchargement et conversion - {e}")
                    shutil.rmtree(parquet_dir, ignore_errors=True)
                    parquet_dir.mkdir(parents=True, exist_ok=True)

            # téléchargement effectif
            async with semaphore:
                async with session.get(download_url) as response:
                    response.raise_for_status()

                    # écriture en streaming par chunks de 1MB pour ne pas tout charger en RAM
                    async with aiofiles.open(temp_zip_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(
                            self.config.MOBILITY_CHUNK_SIZE
                        ):
                            await f.write(chunk)

                    zip_size = temp_zip_path.stat().st_size

                    # conversion en Parquet dans un thread pool - Spark est synchrone
                    loop = asyncio.get_event_loop()
                    conversion_result = await loop.run_in_executor(
                        None,
                        self._convert_feed_to_parquet,
                        temp_zip_path,
                        feed
                    )

                    if conversion_result['status'] in ('success', 'partial'):
                        return {
                            'status': 'success',
                            'feed_id': feed_id,
                            'country': country,
                            'provider': provider,
                            'path': str(parquet_dir),
                            'size': conversion_result['parquet_size'],
                            'original_size': zip_size,
                            'compression_ratio': conversion_result['compression_ratio'],
                            'files_converted': conversion_result['files_converted']
                        }
                    else:
                        return {
                            'status': 'failed',
                            'feed_id': feed_id,
                            'reason': f"Conversion failed: {conversion_result.get('error', 'Unknown error')}"
                        }

        except Exception as e:
            # nettoie le fichier partiel si le téléchargement a planté
            if temp_zip_path.exists():
                temp_zip_path.unlink()
            # Nettoie aussi le répertoire Parquet partiel
            if parquet_dir.exists():
                shutil.rmtree(parquet_dir, ignore_errors=True)

            # retry automatique avec backoff exponentiel (1s, 2s, 4s)
            if retry < 3:
                await asyncio.sleep(2 ** retry)
                return await self._download_feed(session, feed, semaphore, feeds_in_progress, progress_lock, retry + 1)
            else:
                return {
                    'status': 'failed',
                    'feed_id': feed_id,
                    'reason': str(e)
                }

        finally:
            # retire ce feed du set des téléchargements en cours
            async with progress_lock:
                feeds_in_progress.discard(feed_key)

    def _save_metadata(
        self,
        parquet_dir: Path,
        feed_id: str,
        provider: str,
        country: str,
        files_converted: int,
        files_failed: list[dict[str, str]],
        original_size: int,
        parquet_size: int,
        compression_ratio: float,
        parquet_files: list[str]
    ) -> bool:
        """
        Sauvegarde robuste du metadata.json avec gestion d'erreurs.

        Cette méthode s'assure que le répertoire parent existe et capture
        toutes les exceptions pour éviter qu'une erreur d'écriture ne fasse
        échouer toute la conversion.

        Parameters
        ----------
        parquet_dir : Path
            Répertoire contenant les fichiers Parquet
        feed_id : str
            Identifiant du feed
        provider : str
            Nom du fournisseur
        country : str
            Code pays
        files_converted : int
            Nombre de fichiers convertis avec succès
        files_failed : list[dict[str, str]]
            Liste des fichiers en échec avec leurs erreurs
        original_size : int
            Taille originale du ZIP en bytes
        parquet_size : int
            Taille totale des Parquets en bytes
        compression_ratio : float
            Ratio de compression en pourcentage
        parquet_files : list[str]
            Liste des noms de fichiers Parquet créés

        Returns
        -------
        bool
            True si la sauvegarde a réussi, False sinon
        """
        try:
            # s'assure que le répertoire parent existe
            parquet_dir.mkdir(parents=True, exist_ok=True)

            metadata: dict[str, Any] = {
                'feed_id': str(feed_id),
                'provider': provider,
                'country': country,
                'files_converted': files_converted,
                'files_failed': files_failed,
                'original_size_bytes': original_size,
                'parquet_size_bytes': parquet_size,
                'compression_ratio': f"{compression_ratio:.1f}%",
                'conversion_date': datetime.now().isoformat(),
                'gtfs_files': parquet_files
            }

            metadata_file = parquet_dir / 'metadata.json'
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            self.logger.debug(f"Metadata sauvegardé pour {country}:{feed_id}")
            return True

        except Exception as e:
            self.logger.error(
                f"Échec de la sauvegarde du metadata pour {country}:{feed_id} - {e}"
            )
            return False

    def _convert_feed_to_parquet(
        self,
        zip_path: Path,
        feed: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Extrait le ZIP GTFS et convertit chaque fichier .txt en Parquet avec Spark.

        Les feeds GTFS sont des CSV avec extension .txt (routes.txt, stops.txt, etc.).
        On convertit tout en Parquet pour diviser par 3 la taille et accélérer les traitements.

        Parameters
        ----------
        zip_path : Path
            Chemin du ZIP téléchargé
        feed : dict[str, Any]
            Métadonnées du feed (feed_id, provider, country)

        Returns
        -------
        dict[str, Any]
            Stats de conversion (status, taille, ratio compression, erreurs)

        Raises
        ------
        RuntimeError
            Si le ZIP est corrompu ou vide
        """
        feed_id = feed.get('id', 'unknown')
        locations: list[dict[str, Any]] | None = feed.get('locations')
        country = 'XX'
        if locations and len(locations) > 0:
            country = locations[0].get('country_code', 'XX').upper()

        provider = feed.get('provider', 'unknown')

        # structure de sortie : {country}/{feed_id}/
        country_dir = self.output_dir / country
        parquet_dir = country_dir / str(feed_id)
        parquet_dir.mkdir(parents=True, exist_ok=True)

        temp_extract_dir = parquet_dir / '_temp_extract'
        temp_extract_dir.mkdir(exist_ok=True)

        # initialisation des variables de tracking pour le metadata
        # pour sauvegarder même en cas d'erreur partielle
        files_converted = 0
        files_failed: list[dict[str, str]] = []
        parquet_files: list[str] = []
        original_size = 0

        try:
            # extraction du ZIP
            self.logger.debug(f"Extraction du ZIP GTFS de {country}:{feed_id}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)

            original_size = zip_path.stat().st_size

            # recherche des fichiers .txt - ignore les fichiers cachés macOS
            gtfs_files = [
                f for f in temp_extract_dir.rglob('*.txt')
                if not f.name.startswith('.') and '__MACOSX' not in str(f)
            ]

            if not gtfs_files:
                self.logger.warning(f"Feeds {country}:{feed_id}: Aucun fichier GTFS .txt trouvé dans le ZIP - le feed sera ignoré")
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                shutil.rmtree(parquet_dir, ignore_errors=True)
                if zip_path.exists():
                    zip_path.unlink()
                return {
                    'status': 'failed',
                    'error': 'No GTFS files found in ZIP'
                }

            # conversion de chaque .txt en Parquet
            # (files_converted, files_failed, parquet_files déjà initialisés avant le try)
            for gtfs_file in gtfs_files:
                file_name = gtfs_file.name

                # vérifie que le fichier existe vraiment et n'est pas vide
                if not gtfs_file.exists() or gtfs_file.stat().st_size == 0:
                    self.logger.warning(f"Feeds {country}:{feed_id}:{file_name}: fichier vide ou manquant - fichier ignoré dans le feeds")
                    continue

                # capture la taille originale pour calculer la compression
                original_file_size = gtfs_file.stat().st_size

                try:
                    parquet_file = parquet_dir / gtfs_file.stem

                    # lecture du CSV GTFS avec Spark en mode PERMISSIF
                    # permet de gérer les variations de schéma entre feeds GTFS
                    df = self.spark.read.csv(
                        str(gtfs_file),
                        header=True,
                        inferSchema=True,
                        sep=',',
                        escape='"',
                        quote='"',
                        multiLine=True,  # certains feeds ont des descriptions multi-lignes
                        mode='PERMISSIVE',  # continue même si certaines lignes sont malformées
                        columnNameOfCorruptRecord='_corrupt_record',  # capture les lignes problématiques
                        enforceSchema=False,  # accepte les variations de colonnes
                        emptyValue='',  # valeur par défaut pour les champs vides
                        nullValue='NULL'  # traite "NULL" comme null
                    )

                    # filtre les lignes complètement vides si elles existent
                    if '_corrupt_record' in df.columns:
                        corrupt_count = df.filter(df['_corrupt_record'].isNotNull()).count()
                        if corrupt_count > 0:
                            self.logger.debug(f"Feeds {country}:{feed_id}:{file_name}: {corrupt_count} lignes malformées capturées - elles seront incluses dans le Parquet pour analyse ultérieure")

                    # ne convertit que si le DataFrame contient des données
                    row_count = df.count()
                    if row_count == 0:
                        self.logger.warning(f"Feeds {country}:{feed_id}:{file_name}: aucune donnée valide trouvée - fichier ignoré dans le feed")
                        continue

                    df.write.mode('overwrite').parquet(
                        str(parquet_file),
                        compression=self.config.PARQUET_COMPRESSION
                    )

                    files_converted += 1
                    parquet_files.append(gtfs_file.stem + '.parquet')

                    # calcule la compression pour suivre le pattern de base_extractor
                    parquet_file_size = self._get_file_size(parquet_file)
                    compression_pct = ((original_file_size - parquet_file_size) / original_file_size * 100) if original_file_size > 0 else 0

                    self.logger.debug(
                        f"Conversion en parquet de {country}:{feed_id}:{file_name} terminée: "
                        f"{self._format_size(parquet_file_size)} "
                        f"(compression: ~{compression_pct:.1f}%)"
                    )

                except Exception as e:
                    # capture l'erreur complète mais ne plante pas tout le feed
                    error_msg = str(e)
                    # simplifie les erreurs de chemin pour les logs
                    if 'PATH_NOT_FOUND' in error_msg or 'Path does not exist' in error_msg:
                        self.logger.warning(f"Feeds {country}:{feed_id}:{file_name}: fichier référencé mais absent - ignoré")
                    elif 'CSV header does not conform' in error_msg:
                        self.logger.debug(f"Feeds {country}:{feed_id}:{file_name}: schéma non-standard, conversion tentée")
                        # tente une lecture encore plus permissive sans inférence de schéma
                        try:
                            df = self.spark.read.csv(
                                str(gtfs_file),
                                header=True,
                                inferSchema=False,  # lit tout en string
                                sep=',',
                                escape='"',
                                quote='"',
                                mode='PERMISSIVE'
                            )
                            if df.count() > 0:
                                parquet_file = parquet_dir / gtfs_file.stem
                                df.write.mode('overwrite').parquet(
                                    str(parquet_file),
                                    compression=self.config.PARQUET_COMPRESSION
                                )
                                files_converted += 1
                                parquet_files.append(gtfs_file.stem + '.parquet')

                                # calcule la compression pour suivre le pattern de base_extractor
                                parquet_file_size = self._get_file_size(parquet_file)
                                compression_pct = ((original_file_size - parquet_file_size) / original_file_size * 100) if original_file_size > 0 else 0

                                self.logger.debug(
                                    f"Conversion en parquet de {country}:{feed_id}:{file_name} terminée: "
                                    f"{self._format_size(parquet_file_size)} "
                                    f"(compression: ~{compression_pct:.1f}%)"
                                )
                                continue
                        except Exception as retry_error:
                            self.logger.warning(f"Feeds {country}:{feed_id}:{file_name}: échec même en mode permissif - le fichier sera ignoré dans le feed - {str(retry_error)[:100]}")
                    else:
                        self.logger.warning(f"Feeds {country}:{feed_id}:{file_name}: Echec de conversion - le fichier sera ignoré dans le feed - {str(error_msg)[:100]}")

                    files_failed.append({
                        'file': file_name,
                        'error': error_msg
                    })

            # calcul de la compression ZIP → Parquet
            parquet_size = 0
            for parquet_file_name in parquet_files:
                parquet_path = parquet_dir / parquet_file_name.replace('.parquet', '')
                if parquet_path.exists():
                    parquet_size += self._get_file_size(parquet_path)

            compression_ratio = ((original_size - parquet_size) / original_size * 100) if original_size > 0 else 0

            # nettoyage des fichiers temporaires AVANT la création du metadata.json
            # pour éviter que des erreurs de nettoyage n'empêchent la sauvegarde des métadonnées
            if temp_extract_dir.exists():
                shutil.rmtree(temp_extract_dir, ignore_errors=True)

            # sauvegarde robuste des métadonnées dans metadata.json
            # cette fonction s'assure que le répertoire existe et gère les erreurs
            self._save_metadata(
                parquet_dir=parquet_dir,
                feed_id=feed_id,
                provider=provider,
                country=country,
                files_converted=files_converted,
                files_failed=files_failed,
                original_size=original_size,
                parquet_size=parquet_size,
                compression_ratio=compression_ratio,
                parquet_files=parquet_files
            )

            # supprime le ZIP pour économiser l'espace (divise par ~3 grâce à Parquet)
            if zip_path.exists():
                zip_path.unlink()
                self.logger.debug(f"ZIP {country}:{feed_id} supprimé après conversion")

            status = 'success' if not files_failed else 'partial'

            self.logger.debug(
                f"Conversion en parquet de {country}:{feed_id} terminée - {files_converted} fichiers : "
                f"{self._format_size(parquet_size)} (compression: ~{compression_ratio:.1f}%)"
            )

            return {
                'status': status,
                'parquet_dir': str(parquet_dir),
                'files_converted': files_converted,
                'files_failed': files_failed,
                'original_size': original_size,
                'parquet_size': parquet_size,
                'compression_ratio': compression_ratio
            }

        except zipfile.BadZipFile as e:
            # ZIP corrompu - nettoie tout et abandonne
            self.logger.error(f"Feeds {country}:{feed_id}: ZIP corrompu - Le feed est supprimé - {e}")
            if temp_extract_dir.exists():
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
            if parquet_dir.exists():
                shutil.rmtree(parquet_dir, ignore_errors=True)
            if zip_path.exists():
                zip_path.unlink()
            return {
                'status': 'failed',
                'error': f"Corrupted ZIP file: {e}"
            }

        except Exception as e:
            self.logger.error(f"{country}:{feed_id}: Erreur de conversion - {e}")

            # tentative de sauvegarde du metadata avec les données partielles
            # permet de ne pas perdre les conversions partielles réussies
            parquet_size = 0
            try:
                for parquet_file_name in parquet_files:
                    parquet_path = parquet_dir / parquet_file_name.replace('.parquet', '')
                    if parquet_path.exists():
                        parquet_size += self._get_file_size(parquet_path)
            except Exception:
                pass  # ignore les erreurs de calcul de taille

            compression_ratio = ((original_size - parquet_size) / original_size * 100) if original_size > 0 else 0

            # ajoute l'erreur fatale à la liste des erreurs
            files_failed.append({
                'file': '_conversion_error',
                'error': f"Fatal conversion error: {str(e)}"
            })

            # sauvegarde robuste du metadata
            metadata_saved = self._save_metadata(
                parquet_dir=parquet_dir,
                feed_id=feed_id,
                provider=provider,
                country=country,
                files_converted=files_converted,
                files_failed=files_failed,
                original_size=original_size,
                parquet_size=parquet_size,
                compression_ratio=compression_ratio,
                parquet_files=parquet_files
            )

            # nettoyage des fichiers temporaires
            if temp_extract_dir.exists():
                shutil.rmtree(temp_extract_dir, ignore_errors=True)

            # si le metadata a été sauvegardé, on garde le parquet_dir pour analyse/retry
            # sinon, on supprime tout
            if not metadata_saved:
                self.logger.warning(
                    f"{country}:{feed_id}: Metadata non sauvegardé - suppression du feed complet"
                )
                if parquet_dir.exists():
                    shutil.rmtree(parquet_dir, ignore_errors=True)
            else:
                self.logger.info(
                    f"{country}:{feed_id}: Metadata sauvegardé avec {files_converted} fichiers - "
                    f"le feed sera retenté au prochain run"
                )

            return {
                'status': 'failed',
                'error': str(e),
                'files_converted': files_converted,
                'metadata_saved': metadata_saved
            }