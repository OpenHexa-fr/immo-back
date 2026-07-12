"""Ingestion des données DVF (Données de Valeurs Foncières) dans Elasticsearch."""

from __future__ import annotations

from typing import Any

import httpx
import polars as pl
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

_DVF_COLUMNS = [
    "id_mutation",
    "date_mutation",
    "valeur_fonciere",
    "surface_reelle_bati",
    "type_local",
    "nom_commune",
    "code_postal",
    "numero_lot",
    "longitude",
    "latitude",
]


async def fetch_dvf_csv(source_url: str) -> bytes:
    """Télécharge le fichier CSV DVF depuis `source_url`."""
    async with httpx.AsyncClient(timeout=60.0) as http_client:
        response = await http_client.get(source_url)
        response.raise_for_status()
        return response.content


def parse_dvf_csv(raw_csv: bytes) -> pl.DataFrame:
    """Parse le CSV DVF en DataFrame Polars, en ne conservant que les colonnes utiles."""
    return pl.read_csv(
        raw_csv, columns=_DVF_COLUMNS, infer_schema_length=10_000, ignore_errors=True
    )


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    location = None
    if row.get("latitude") is not None and row.get("longitude") is not None:
        location = {"lat": row["latitude"], "lon": row["longitude"]}

    return {
        "_id": make_document_id(
            row["id_mutation"], row["date_mutation"], row.get("numero_lot") or ""
        ),
        "id_mutation": row["id_mutation"],
        "date_mutation": row["date_mutation"],
        "valeur_fonciere": row["valeur_fonciere"],
        "surface_reelle_bati": row.get("surface_reelle_bati"),
        "type_local": row.get("type_local"),
        "commune": row["nom_commune"],
        "code_postal": row["code_postal"],
        "location": location,
    }


async def ingest_dvf(
    client: AsyncElasticsearch, index_alias: str, source_url: str
) -> tuple[int, int]:
    """Télécharge, parse et indexe les transactions DVF depuis `source_url`."""
    raw_csv = await fetch_dvf_csv(source_url)
    dataframe = parse_dvf_csv(raw_csv)
    documents = (_row_to_document(row) for row in dataframe.iter_rows(named=True))

    success, errors = await bulk_index(client, index_alias, documents)
    logger.info("dvf_ingestion_completed", success=success, errors=errors)
    return success, errors
