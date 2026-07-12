"""Ingestion des données DVF (Données de Valeurs Foncières) dans Elasticsearch.

Source réelle : `https://files.data.gouv.fr/geo-dvf/latest/csv/<annee>/full.csv.gz`
(fichier national unique, compressé gzip). Il n'existe pas de colonne `numero_lot`
unique (hypothèse initiale invalidée face à l'export réel) : le CSV distingue les
lots via `numero_disposition` et jusqu'à 5 colonnes `lotN_numero`.

Une mutation (`id_mutation`) peut en outre porter sur plusieurs parcelles
cadastrales et plusieurs natures de local (ex : une maison + sa dépendance sur
deux parcelles adjacentes) sans que `lot1_numero` ne varie pour autant : deux
lignes peuvent alors partager le même couple (id_mutation, numero_disposition,
lot1_numero) tout en décrivant des biens distincts. Vérifié sur l'export 2025
réel (dédoublonnage sans `id_parcelle`/`code_type_local` : 43% d'ids uniques
seulement sur un échantillon de 50k lignes). L'identifiant de document doit donc
aussi intégrer `id_parcelle` et `code_type_local`.

Même avec ces cinq colonnes, certains groupes de lignes restent strictement
identiques dans le CSV source (ex : plusieurs caves/emplacements de parking
indissociables vendus dans la même mutation, sans lot ni surface renseignés :
vérifié sur l'export 2025 réel, groupe de 2996 lignes dont seulement 528
distinctes sur l'ensemble des colonnes). Le CSV ne fournit alors aucune colonne
capable de les distinguer : un index d'occurrence, calculé par position dans le
fichier au sein de chaque groupe de clé identique, sert de dernier recours pour
garantir des `_id` uniques. Il rend l'ingestion idempotente d'un run à l'autre
tant que l'ordre des lignes du fichier source ne change pas (stable en pratique
pour un millésime déjà publié).
"""

from __future__ import annotations

import gzip
import io
from typing import Any

import httpx
import polars as pl
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

_DVF_COLUMNS = [
    "id_mutation",
    "numero_disposition",
    "id_parcelle",
    "code_type_local",
    "date_mutation",
    "valeur_fonciere",
    "surface_reelle_bati",
    "type_local",
    "nom_commune",
    "code_postal",
    "lot1_numero",
    "longitude",
    "latitude",
]

_DEDUP_KEY_COLUMNS = [
    "id_mutation",
    "numero_disposition",
    "id_parcelle",
    "code_type_local",
    "lot1_numero",
]


async def fetch_dvf_csv(source_url: str) -> bytes:
    """Télécharge le fichier CSV DVF (gzip) depuis `source_url` et le décompresse."""
    # files.data.gouv.fr redirige (302) vers un bucket S3 OVH : httpx ne suit
    # pas les redirections par défaut, contrairement à curl/aux navigateurs
    # (vérifié en conditions réelles : la source échoue systématiquement sans
    # `follow_redirects`).
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as http_client:
        response = await http_client.get(source_url)
        response.raise_for_status()
        return response.content


def parse_dvf_csv(raw_csv: bytes) -> pl.DataFrame:
    """Parse le CSV DVF en DataFrame Polars, en ne conservant que les colonnes utiles.

    `raw_csv` peut être du CSV brut ou du CSV compressé gzip (cas du fichier national
    réel `full.csv.gz`) : la compression est détectée via la signature magique.
    """
    if raw_csv[:2] == b"\x1f\x8b":
        raw_csv = gzip.decompress(raw_csv)
    dataframe = pl.read_csv(
        io.BytesIO(raw_csv), columns=_DVF_COLUMNS, infer_schema_length=10_000, ignore_errors=True
    )
    return dataframe.with_columns(
        pl.int_range(pl.len()).over(_DEDUP_KEY_COLUMNS).alias("_occurrence")
    )


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    location = None
    if row.get("latitude") is not None and row.get("longitude") is not None:
        location = {"lat": row["latitude"], "lon": row["longitude"]}

    return {
        "_id": make_document_id(
            row["id_mutation"],
            row.get("numero_disposition") or "",
            row.get("id_parcelle") or "",
            row.get("code_type_local") or "",
            row.get("lot1_numero") or "",
            row.get("_occurrence") or 0,
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
