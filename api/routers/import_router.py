"""
Endpoint Import — Déclenchement de la pipeline ETL complète (section 1 de la spec).
"""

import os
import sys
import subprocess
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends

from ..dependencies import verify_import_token

router = APIRouter()

_PIPELINE_SCRIPT = Path(__file__).parents[2] / "src" / "pipeline.py"


def _run_etl_pipeline() -> None:
    """Exécute la pipeline ETL complète en sous-processus isolé."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_PIPELINE_SCRIPT.parent)
    subprocess.run(
        [sys.executable, str(_PIPELINE_SCRIPT)],
        env=env,
        check=False,
    )


@router.post("/import", status_code=202)
async def trigger_import(
    background_tasks: BackgroundTasks,
    token_valid: None = Depends(verify_import_token),
):
    """Déclenche la pipeline ETL complète (extraction → transformation → chargement) en arrière-plan.
    
    **Authentification requise :** Bearer token via header Authorization.
    """
    background_tasks.add_task(_run_etl_pipeline)
    return {
        "status": "started",
        "message": "Pipeline ETL déclenchée en arrière-plan.",
    }
