"""Ingestion des diagnostics DPE (API data-fair ADEME) dans Elasticsearch.

Contrairement à DVF (CSV téléchargé en une fois), la source DPE est une API REST
paginée (`data-fair`) : l'ingestion consomme page par page jusqu'à obtenir une
page vide.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

_PAGE_SIZE = 1000


async def fetch_dpe_pages(source_url: str) -> AsyncIterator[list[dict[str, Any]]]:
    """Itère les pages de résultats de l'API data-fair ADEME (pagination par page)."""
    async with httpx.AsyncClient(timeout=60.0) as http_client:
        page = 1
        while True:
            response = await http_client.get(
                f"{source_url}/lines", params={"page": page, "size": _PAGE_SIZE}
            )
            response.raise_for_status()
            results = response.json().get("results", [])
            if not results:
                return
            yield results
            page += 1


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "_id": make_document_id(row["numero_dpe"]),
        "numero_dpe": row["numero_dpe"],
        "date_etablissement": row.get("date_etablissement_dpe"),
        "etiquette_dpe": row.get("etiquette_dpe"),
        "etiquette_ges": row.get("etiquette_ges"),
        "commune": row.get("nom_commune_ban"),
        "code_postal": row.get("code_postal_ban"),
        "surface_habitable": row.get("surface_habitable_logement"),
    }


async def ingest_dpe(
    client: AsyncElasticsearch, index_alias: str, source_url: str
) -> tuple[int, int]:
    """Télécharge, page par page, et indexe les diagnostics DPE depuis `source_url`."""
    total_success = 0
    total_errors = 0
    async for page in fetch_dpe_pages(source_url):
        documents = (_row_to_document(row) for row in page)
        success, errors = await bulk_index(client, index_alias, documents)
        total_success += success
        total_errors += errors

    logger.info("dpe_ingestion_completed", success=total_success, errors=total_errors)
    return total_success, total_errors
