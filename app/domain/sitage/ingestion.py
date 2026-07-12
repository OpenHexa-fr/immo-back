"""Ingestion des permis de construire Sit@del2 dans Elasticsearch.

Le mapping des colonnes sources est une hypothèse de travail (voir schemas.py) :
à valider avec la structure réelle du CSV publié par le SDES avant mise en
production.
"""

from __future__ import annotations

from typing import Any

import httpx
import polars as pl
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

_SITADEL_COLUMNS = [
    "numero_permis",
    "date_autorisation",
    "type_permis",
    "commune",
    "code_postal",
    "nombre_logements",
    "surface_plancher",
    "longitude",
    "latitude",
]


async def fetch_sitadel_csv(source_url: str) -> bytes:
    """Télécharge le fichier CSV Sit@del2 depuis `source_url`."""
    async with httpx.AsyncClient(timeout=60.0) as http_client:
        response = await http_client.get(source_url)
        response.raise_for_status()
        return response.content


def parse_sitadel_csv(raw_csv: bytes) -> pl.DataFrame:
    """Parse le CSV Sit@del2 en DataFrame Polars, en ne conservant que les colonnes utiles."""
    return pl.read_csv(
        raw_csv, columns=_SITADEL_COLUMNS, infer_schema_length=10_000, ignore_errors=True
    )


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    location = None
    if row.get("latitude") is not None and row.get("longitude") is not None:
        location = {"lat": row["latitude"], "lon": row["longitude"]}

    return {
        "_id": make_document_id(row["numero_permis"]),
        "numero_permis": row["numero_permis"],
        "date_autorisation": row["date_autorisation"],
        "type_permis": row["type_permis"],
        "commune": row["commune"],
        "code_postal": row["code_postal"],
        "nombre_logements": row.get("nombre_logements"),
        "surface_plancher": row.get("surface_plancher"),
        "location": location,
    }


async def ingest_sitadel(
    client: AsyncElasticsearch, index_alias: str, source_url: str
) -> tuple[int, int]:
    """Télécharge, parse et indexe les permis de construire depuis `source_url`."""
    raw_csv = await fetch_sitadel_csv(source_url)
    dataframe = parse_sitadel_csv(raw_csv)
    documents = (_row_to_document(row) for row in dataframe.iter_rows(named=True))

    success, errors = await bulk_index(client, index_alias, documents)
    logger.info("sitadel_ingestion_completed", success=success, errors=errors)
    return success, errors
