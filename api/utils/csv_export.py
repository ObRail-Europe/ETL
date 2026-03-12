"""
Export CSV streaming pour les endpoints /download.

Utilise un curseur nommé (server-side) psycopg2 pour streamer les résultats
sans charger toutes les lignes en mémoire. Applique la limite de 500k lignes.
"""

import csv
import io

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor

from ..config import ApiConfig


def stream_csv_response(
    conn,
    query: str,
    params: list,
    filename: str,
    count_query: str | None = None,
    count_params: list | None = None,
) -> StreamingResponse:
    """
    Exécute la requête et retourne un StreamingResponse CSV.

    Si count_query est fourni, vérifie d'abord que le nombre de lignes
    ne dépasse pas CSV_MAX_ROWS (500k). Sinon, retourne HTTP 413.
    """
    max_rows = ApiConfig.CSV_MAX_ROWS

    if count_query:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(count_query, count_params or params)
            total = cur.fetchone()["total"]
            if total > max_rows:
                raise HTTPException(
                    status_code=413,
                    detail=(
                        f"Le résultat contient {total} lignes, "
                        f"dépassant la limite de {max_rows}. "
                        "Veuillez affiner vos filtres."
                    ),
                )

    def generate():
        with conn.cursor(name="csv_export", cursor_factory=RealDictCursor) as cur:
            cur.itersize = 10_000
            cur.execute(query, params)

            header_written = False
            for row in cur:
                row_dict = dict(row)
                if not header_written:
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=row_dict.keys())
                    writer.writeheader()
                    yield output.getvalue()
                    output.close()
                    header_written = True

                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=row_dict.keys())
                writer.writerow(row_dict)
                yield output.getvalue()
                output.close()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )
