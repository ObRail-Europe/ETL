#!/usr/bin/env python3
"""
Script ETL - Extraction Mobility Database
Télécharge les fichiers GTFS via API (async optimisé)
Auteur: Équipe MSPR ObRail Europe
"""

import os
import sys
import json
import logging
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Pays de l'Union européenne (codes ISO 3166-1 alpha-2)
EU_COUNTRIES = [
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR',
    'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL',
    'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'
]

# Configuration API
API_BASE_URL = os.getenv("MOBILITY_API_BASE_URL")
DEFAULT_RAW_PATH = os.getenv("MOBILITY_RAW_PATH")


class MobilityDatabaseDownloader:
    """Téléchargeur GTFS via l'API Mobility Database (async)."""

    def __init__(
        self,
        output_dir: str = None,
        refresh_token: Optional[str] = None,
        max_concurrent: int = 20,
        timeout: int = 2400,
        chunk_size: int = 1024 * 1024,
        max_retries: int = 3
    ):
        """
        Initialise le téléchargeur.

        Args:
            output_dir: Répertoire de sortie
            refresh_token: Token API Mobility Database
            max_concurrent: Nombre de téléchargements simultanés
            timeout: Timeout par téléchargement (secondes)
            chunk_size: Taille des chunks pour l'écriture (bytes)
            max_retries: Nombre de tentatives en cas d'échec
        """
        self.output_dir = Path(output_dir or DEFAULT_RAW_PATH)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.refresh_token = refresh_token or os.environ.get('MOBILITY_API_REFRESH_TOKEN')
        if not self.refresh_token:
            raise ValueError("Token API requis (MOBILITY_API_REFRESH_TOKEN)")

        self.access_token = None
        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(total=timeout, connect=60, sock_read=300)
        self.api_timeout = aiohttp.ClientTimeout(total=180, connect=30, sock_read=150)
        self.chunk_size = chunk_size
        self.max_retries = max_retries

        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0
        }

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._api_semaphore = asyncio.Semaphore(5)

    async def authenticate(self, session: aiohttp.ClientSession) -> str:
        """Obtient un token d'accès à l'API."""
        try:
            async with session.post(
                f"{API_BASE_URL}/tokens",
                json={"refresh_token": self.refresh_token},
                headers={"Content-Type": "application/json"},
                timeout=self.api_timeout
            ) as response:
                response.raise_for_status()
                data = await response.json()
                self.access_token = data['access_token']
                logger.info("Authentification réussie")
                return self.access_token
        except Exception as e:
            logger.error(f"Échec authentification: {e}")
            raise

    async def fetch_feeds_for_country(
        self,
        session: aiohttp.ClientSession,
        country: str,
        retry: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Récupère les feeds pour un pays avec pagination et retry.
        
        Args:
            session: Session aiohttp
            country: Code pays ISO
            retry: Numéro de tentative
        
        Returns:
            Liste des feeds
        """
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        url = f"{API_BASE_URL}/gtfs_feeds"
        all_feeds = []
        offset = 0
        limit = 100

        try:
            while True:
                async with self._api_semaphore:
                    await asyncio.sleep(0.5)

                    params = {
                        'country_code': country,
                        'status': 'active',
                        'limit': limit,
                        'offset': offset
                    }

                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        timeout=self.api_timeout
                    ) as response:
                        response.raise_for_status()
                        data = await response.json()

                        if not data:
                            break

                        all_feeds.extend(data)

                        if len(data) < limit:
                            break

                        offset += limit
                        logger.debug(f"  {country}: {len(all_feeds)} feeds récupérés...")

            logger.info(f"  {country}: {len(all_feeds)} feeds")
            return all_feeds

        except asyncio.TimeoutError as e:
            if retry < self.max_retries:
                wait_time = 2 ** retry
                logger.warning(f"  {country}: timeout, retry {retry + 1}/{self.max_retries}")
                await asyncio.sleep(wait_time)
                return await self.fetch_feeds_for_country(session, country, retry + 1)
            else:
                logger.error(f"  {country}: échec après {self.max_retries} tentatives")
                return []
        except aiohttp.ClientResponseError as e:
            if e.status in (503, 429) and retry < self.max_retries:
                wait_time = 5 * (2 ** retry)
                logger.warning(f"  {country}: rate limit ({e.status}), retry {retry + 1}/{self.max_retries}")
                await asyncio.sleep(wait_time)
                return await self.fetch_feeds_for_country(session, country, retry + 1)
            elif retry < self.max_retries:
                wait_time = 2 ** retry
                logger.warning(f"  {country}: erreur HTTP {e.status}, retry {retry + 1}/{self.max_retries}")
                await asyncio.sleep(wait_time)
                return await self.fetch_feeds_for_country(session, country, retry + 1)
            else:
                logger.error(f"  {country}: échec après {self.max_retries} tentatives")
                return []
        except Exception as e:
            if retry < self.max_retries:
                wait_time = 2 ** retry
                logger.warning(f"  {country}: erreur, retry {retry + 1}/{self.max_retries}")
                await asyncio.sleep(wait_time)
                return await self.fetch_feeds_for_country(session, country, retry + 1)
            else:
                logger.error(f"  {country}: échec après {self.max_retries} tentatives")
                return []

    async def fetch_feeds(
        self,
        session: aiohttp.ClientSession,
        country_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Récupère les feeds GTFS depuis l'API (en parallèle par pays).

        Args:
            session: Session aiohttp
            country_codes: Liste de codes pays (défaut: EU_COUNTRIES)

        Returns:
            Liste des feeds
        """
        if not self.access_token:
            await self.authenticate(session)

        country_codes = country_codes or EU_COUNTRIES
        logger.info(f"Récupération des feeds pour {len(country_codes)} pays (en parallèle)...")

        # Récupération parallèle pour tous les pays
        tasks = [
            self.fetch_feeds_for_country(session, country)
            for country in country_codes
        ]
        results = await asyncio.gather(*tasks)

        # Aplatir les résultats
        all_feeds = []
        for feeds in results:
            all_feeds.extend(feeds)

        logger.info(f"Total: {len(all_feeds)} feeds récupérés")
        return all_feeds

    async def download_feed_with_retry(
        self,
        session: aiohttp.ClientSession,
        feed: Dict[str, Any],
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Télécharge un feed GTFS avec retry automatique.

        Args:
            session: Session aiohttp
            feed: Informations du feed
            force: Forcer le téléchargement si existant

        Returns:
            Résultat du téléchargement
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return await self.download_feed(session, feed, force)
            except asyncio.TimeoutError as e:
                last_error = f'Timeout après {attempt + 1} tentative(s)'
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Backoff exponentiel
            except Exception as e:
                last_error = str(e)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        # Si on arrive ici, toutes les tentatives ont échoué
        return {
            'feed_id': feed.get('id', 'unknown'),
            'status': 'failed',
            'reason': last_error or 'Échec après plusieurs tentatives'
        }

    async def download_feed(
        self,
        session: aiohttp.ClientSession,
        feed: Dict[str, Any],
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Télécharge un feed GTFS (async).

        Args:
            session: Session aiohttp
            feed: Informations du feed
            force: Forcer le téléchargement si existant

        Returns:
            Résultat du téléchargement
        """
        async with self._semaphore:  # Limite les téléchargements concurrents
            feed_id = feed.get('id', 'unknown')

            # Extraire le code pays depuis locations (l'API retourne une liste)
            country = 'XX'
            if 'locations' in feed and feed['locations'] and len(feed['locations']) > 0:
                country = feed['locations'][0].get('country_code', 'XX').upper()

            provider = feed.get('provider', 'unknown')

            # Récupérer l'URL de téléchargement
            # Structure de l'API Mobility Database v1:
            # - feed['latest_dataset']['hosted_url'] : URL hébergée par MobilityDatabase
            # - feed['source_info']['producer_url'] : URL d'origine du producteur
            download_url = None

            # Priorité 1: URL hébergée (plus fiable)
            if 'latest_dataset' in feed and feed['latest_dataset']:
                latest = feed['latest_dataset']
                if isinstance(latest, dict) and 'hosted_url' in latest:
                    download_url = latest['hosted_url']

          
            if not download_url:
                return {
                    'feed_id': feed_id,
                    'status': 'skipped',
                    'reason': 'Pas d\'URL de téléchargement'
                }

            # Préparer le chemin de sortie dans data/raw/mobility
            country_dir = self.output_dir / country
            country_dir.mkdir(exist_ok=True)

            provider_clean = "".join(c if c.isalnum() or c in '-_' else '_' for c in provider)[:50]
            filename = f"{country}_{feed_id}_{provider_clean}.zip"
            output_path = country_dir / filename

            # Vérifier l'existence
            if output_path.exists() and not force:
                return {
                    'feed_id': feed_id,
                    'status': 'skipped',
                    'reason': 'Fichier existant'
                }

            try:
                logger.info(f"Téléchargement: {provider} ({country})")

                async with session.get(
                    download_url,
                    timeout=self.timeout,
                    allow_redirects=True
                ) as response:
                    response.raise_for_status()

                    # Vérifier le type de contenu
                    content_type = response.headers.get('Content-Type', '')
                    if 'text/html' in content_type:
                        return {
                            'feed_id': feed_id,
                            'status': 'failed',
                            'reason': 'Contenu HTML reçu au lieu de ZIP'
                        }

                    # Télécharger avec chunks optimisés
                    total_size = 0
                    async with aiofiles.open(output_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(self.chunk_size):
                            if chunk:
                                await f.write(chunk)
                                total_size += len(chunk)

                # Vérifier la taille
                if total_size < 100:
                    output_path.unlink()
                    return {
                        'feed_id': feed_id,
                        'status': 'failed',
                        'reason': 'Fichier trop petit'
                    }

                logger.info(f"  ✓ {filename} ({total_size / 1024:.1f} KB)")

                return {
                    'feed_id': feed_id,
                    'status': 'success',
                    'path': str(output_path),
                    'size': total_size,
                    'country': country,
                    'provider': provider
                }

            except asyncio.TimeoutError:
                raise  # Laisse le retry handler gérer
            except Exception as e:
                logger.warning(f"  ✗ Erreur {feed_id}: {e}")
                raise  # Laisse le retry handler gérer

    async def download_all(
        self,
        session: aiohttp.ClientSession,
        feeds: List[Dict[str, Any]],
        force: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Télécharge tous les feeds en parallèle (async).

        Args:
            session: Session aiohttp
            feeds: Liste des feeds
            force: Forcer le téléchargement
            limit: Limite de téléchargements (pour tests)

        Returns:
            Liste des résultats
        """
        if limit:
            feeds = feeds[:limit]

        self.stats['total'] = len(feeds)

        logger.info(
            f"Démarrage du téléchargement de {len(feeds)} feeds "
            f"(max {self.max_concurrent} concurrents)..."
        )

        # Créer toutes les tâches de téléchargement
        tasks = [
            self.download_feed_with_retry(session, feed, force)
            for feed in feeds
        ]

        # Exécuter avec callback pour la progression
        results = []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)

            # Statistiques
            status = result['status']
            if status == 'success':
                self.stats['success'] += 1
            elif status == 'failed':
                self.stats['failed'] += 1
            else:
                self.stats['skipped'] += 1

            # Progression
            total = sum(self.stats[k] for k in ['success', 'failed', 'skipped'])
            if total % 10 == 0 or total == self.stats['total']:
                logger.info(
                    f"Progression: {total}/{self.stats['total']} "
                    f"(✓{self.stats['success']} ✗{self.stats['failed']} ⊘{self.stats['skipped']})"
                )

        return results

    def generate_report(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Génère un rapport de téléchargement."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': self.stats.copy(),
            'by_country': {},
            'failed': [],
            'downloaded': []
        }

        for result in results:
            if result['status'] == 'success':
                country = result['country']
                if country not in report['by_country']:
                    report['by_country'][country] = {'count': 0, 'size': 0}

                report['by_country'][country]['count'] += 1
                report['by_country'][country]['size'] += result['size']

                report['downloaded'].append({
                    'feed_id': result['feed_id'],
                    'country': country,
                    'provider': result['provider'],
                    'path': result['path']
                })

            elif result['status'] == 'failed':
                report['failed'].append({
                    'feed_id': result['feed_id'],
                    'reason': result['reason']
                })

        return report

    def save_report(self, report: Dict[str, Any]):
        """Sauvegarde le rapport JSON."""
        report_path = self.output_dir / 'download_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Rapport: {report_path}")

    async def run(
        self,
        country_codes: Optional[List[str]] = None,
        force: bool = False,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Exécute le processus complet.

        Args:
            country_codes: Codes pays (défaut: UE)
            force: Forcer le téléchargement
            limit: Limite de téléchargements

        Returns:
            Rapport
        """
        logger.info("="*60)
        logger.info("Extraction Mobility Database - GTFS (async)")
        logger.info("="*60)

        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,
            limit_per_host=10,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers={'User-Agent': 'ObRail-Europe-ETL/1.0'},
            raise_for_status=False
        ) as session:
            feeds = await self.fetch_feeds(session, country_codes)
            if not feeds:
                logger.error("Aucun feed trouvé")
                return {'error': 'Aucun feed'}

            results = await self.download_all(session, feeds, force, limit)

        report = self.generate_report(results)
        self.save_report(report)

        logger.info("\n" + "="*60)
        logger.info("RÉSUMÉ")
        logger.info("="*60)
        logger.info(f"Total: {self.stats['total']}")
        logger.info(f"Succès: {self.stats['success']}")
        logger.info(f"Échecs: {self.stats['failed']}")
        logger.info(f"Ignorés: {self.stats['skipped']}")

        return report


def extract_mobilitydatabase(
    output_dir: str = None,
    limit: Optional[int] = None
) -> bool:
    """
    Fonction wrapper synchrone pour l'orchestrateur ETL.
    
    Args:
        output_dir: Répertoire de sortie
        limit: Limite de téléchargements (pour tests)
    
    Returns:
        True si extraction réussie, False sinon
    """
    try:
        downloader = MobilityDatabaseDownloader(output_dir=output_dir or DEFAULT_RAW_PATH)
        report = asyncio.run(downloader.run(limit=limit))
        
        if 'error' in report:
            return False
        
        return report['summary']['success'] > 0
        
    except Exception as e:
        logger.error(f"Erreur: {e}", exc_info=True)
        return False


async def async_main():
    """Point d'entrée asynchrone CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Télécharge les GTFS via l'API Mobility Database"
    )
    parser.add_argument('-o', '--output', default=DEFAULT_RAW_PATH,
                        help='Répertoire de sortie')
    parser.add_argument('-t', '--token',
                        help='Token API')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Forcer le re-téléchargement')
    parser.add_argument('-l', '--limit', type=int,
                        help='Limiter le nombre de téléchargements')
    parser.add_argument('-c', '--concurrent', type=int, default=20,
                        help='Téléchargements concurrents')
    parser.add_argument('--timeout', type=int, default=300,
                        help='Timeout (secondes)')
    parser.add_argument('--chunk-size', type=int, default=1048576,
                        help='Taille des chunks')
    parser.add_argument('--retries', type=int, default=3,
                        help='Nombre de tentatives')

    args = parser.parse_args()

    try:
        downloader = MobilityDatabaseDownloader(
            output_dir=args.output,
            refresh_token=args.token,
            max_concurrent=args.concurrent,
            timeout=args.timeout,
            chunk_size=args.chunk_size,
            max_retries=args.retries
        )

        report = await downloader.run(force=args.force, limit=args.limit)

        if 'error' in report:
            sys.exit(1)
        elif report['summary']['failed'] > report['summary']['success']:
            sys.exit(2)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"Erreur: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Point d'entrée."""
    asyncio.run(async_main())


if __name__ == '__main__':
    main()