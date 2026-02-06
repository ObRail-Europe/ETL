"""
Orchestrateur ETL - ObRail Europe
Exécute l'ensemble des scripts d'extraction de données
Auteur: Équipe MSPR ObRail Europe
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Callable

# Import des fonctions d'extraction
from extract_backontrack import extract_backontrack
from extract_eea import extract_eea
from extract_ourairports import extract_ourairports
from extract_mobilitydatabase import extract_mobilitydatabase

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Orchestrateur de l'ETL pour l'extraction de toutes les sources."""
    
    def __init__(self, base_output_dir: str = "data/raw"):
        """
        Initialise l'orchestrateur.
        
        Args:
            base_output_dir: Répertoire de base pour les extractions
        """
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Configuration des sources avec leurs fonctions d'extraction
        self.sources: List[Dict[str, any]] = [
            {
                'name': 'Back-on-Track',
                'function': extract_backontrack,
                'enabled': True
            },
            {
                'name': 'EEA CO2 Intensity',
                'function': extract_eea,
                'enabled': True
            },
            {
                'name': 'OurAirports',
                'function': extract_ourairports,
                'enabled': True
            },
            {
                'name': 'Mobility Database',
                'function': extract_mobilitydatabase,
                'enabled': True,
                'options': {'limit': None}  # Mettre une limite pour tests
            }
        ]
        
        self.results = {}
    
    def run_extraction(self, source: Dict) -> bool:
        """
        Exécute l'extraction pour une source.
        
        Args:
            source: Configuration de la source
        
        Returns:
            True si réussi, False sinon
        """
        source_name = source['name']
        logger.info(f"{'='*60}")
        logger.info(f"EXTRACTION: {source_name}")
        logger.info(f"{'='*60}")
        
        try:
            start_time = datetime.now()
            
            # Exécution de la fonction d'extraction
            if 'options' in source:
                success = source['function'](**source['options'])
            else:
                success = source['function']()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if success:
                logger.info(f"✓ {source_name} terminé avec succès ({duration:.1f}s)")
                self.results[source_name] = {
                    'status': 'success',
                    'duration': duration,
                    'timestamp': end_time.isoformat()
                }
                return True
            else:
                logger.error(f"✗ {source_name} a échoué ({duration:.1f}s)")
                self.results[source_name] = {
                    'status': 'failed',
                    'duration': duration,
                    'timestamp': end_time.isoformat()
                }
                return False
                
        except Exception as e:
            logger.error(f"✗ Erreur lors de l'extraction {source_name}: {e}", exc_info=True)
            self.results[source_name] = {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            return False
    
    def run_all(self) -> bool:
        """
        Exécute toutes les extractions.
        
        Returns:
            True si au moins une extraction a réussi, False sinon
        """
        logger.info("\n" + "="*60)
        logger.info("ORCHESTRATEUR ETL - ObRail Europe")
        logger.info(f"Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60 + "\n")
        
        enabled_sources = [s for s in self.sources if s.get('enabled', True)]
        logger.info(f"Sources à extraire: {len(enabled_sources)}")
        
        success_count = 0
        failed_count = 0
        
        # Exécution séquentielle de toutes les sources
        for source in enabled_sources:
            if self.run_extraction(source):
                success_count += 1
            else:
                failed_count += 1
            logger.info("")  # Saut de ligne entre les sources
        
        # Résumé final
        logger.info("\n" + "="*60)
        logger.info("RÉSUMÉ FINAL ETL")
        logger.info("="*60)
        logger.info(f"Total: {len(enabled_sources)} sources")
        logger.info(f"Succès: {success_count}")
        logger.info(f"Échecs: {failed_count}")
        logger.info("="*60)
        
        # Détails par source
        for source_name, result in self.results.items():
            status_symbol = "✓" if result['status'] == 'success' else "✗"
            duration_str = f"{result.get('duration', 0):.1f}s" if 'duration' in result else "N/A"
            logger.info(f"{status_symbol} {source_name}: {result['status']} ({duration_str})")
        
        logger.info(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60 + "\n")
        
        return success_count > 0


def main():
    """Point d'entrée principal."""
    try:
        orchestrator = ETLOrchestrator()
        success = orchestrator.run_all()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Erreur critique: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
