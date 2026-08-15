"""Ingestion des permis de construire (base Sitadel/SDES) dans Elasticsearch.

Source réelle : export CSV DiDo du SDES, "Liste des autorisations d'urbanisme
créant des logements" (Sitadel2 est arrêté depuis mars 2026, remplacé par
Sitadel3 ; le jeu de données Sitadel "brut" par téléchargement direct
`files.data.gouv.fr/sitadel/...` supposé dans une version antérieure de ce code
n'existe pas — la donnée est distribuée via la plateforme DiDo).

Le CSV réel est délimité par `;`, encodé en colonnes MAJUSCULES, et ne contient
aucune coordonnée géographique (ni latitude/longitude, ni adresse structurée
géocodée) : seul un code commune INSEE (`COMM`) et un intitulé de localité
(`ADR_LOCALITE_TER`) sont disponibles. Le champ `location` du document normalisé
reste donc toujours `None` ; une géolocalisation par commune nécessiterait une
table de correspondance code INSEE -> centroïde, hors périmètre de cette ingestion.

`NUM_DAU` seul n'est pas une clé unique : un même numéro peut être partagé par
une déclaration préalable (DP) suivie d'un permis de construire (PC) sur le
même projet (vérifié sur l'export réel). Le couple (NUM_DAU, TYPE_DAU) lève
cette ambiguïté.
"""

from __future__ import annotations

import asyncio
import io
from typing import Any

import httpx
import polars as pl
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

# Le fichier national ("logements") pèse plusieurs centaines de Mo une fois
# décompressé par le serveur DiDo : la source est plus lente que DVF/DPE.
_FETCH_TIMEOUT_SECONDS = 300.0

_SITADEL_COLUMNS = [
    "NUM_DAU",
    "DATE_REELLE_AUTORISATION",
    "TYPE_DAU",
    "COMM",
    "ADR_LOCALITE_TER",
    "ADR_CODPOST_TER",
    "NB_LGT_TOT_CREES",
    "SURF_HAB_CREEE",
]


async def fetch_sitadel_csv(source_url: str) -> bytes:
    """Télécharge le fichier CSV des autorisations d'urbanisme depuis `source_url`."""
    async with httpx.AsyncClient(
        timeout=_FETCH_TIMEOUT_SECONDS, follow_redirects=True
    ) as http_client:
        response = await http_client.get(source_url)
        response.raise_for_status()
        return response.content


def parse_sitadel_csv(raw_csv: bytes) -> pl.DataFrame:
    """Parse le CSV Sitadel (délimiteur `;`) en ne conservant que les colonnes utiles."""
    return pl.read_csv(
        io.BytesIO(raw_csv),
        columns=_SITADEL_COLUMNS,
        separator=";",
        # COMM (code INSEE) et ADR_CODPOST_TER sont numériques dans le CSV réel :
        # sans forcer Utf8, polars les infère en entier, ce qui fait échouer la
        # validation Pydantic de l'API (`code_postal`/`commune`: str) et
        # tronquerait le zéro initial des codes commençant par 0 (même bug que
        # sur DVF, voir `dvf/ingestion.py`).
        schema_overrides={"COMM": pl.Utf8, "ADR_CODPOST_TER": pl.Utf8},
        infer_schema_length=10_000,
        ignore_errors=True,
    )


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "_id": make_document_id(row["NUM_DAU"], row["TYPE_DAU"]),
        "numero_permis": row["NUM_DAU"],
        "date_autorisation": row["DATE_REELLE_AUTORISATION"],
        "type_permis": row["TYPE_DAU"],
        "commune": row.get("ADR_LOCALITE_TER") or row["COMM"],
        "code_postal": row.get("ADR_CODPOST_TER"),
        "nombre_logements": row.get("NB_LGT_TOT_CREES"),
        "surface_plancher": row.get("SURF_HAB_CREEE"),
        "location": None,
    }


async def ingest_sitadel(
    client: AsyncElasticsearch, index_alias: str, source_url: str
) -> tuple[int, int]:
    """Télécharge, parse et indexe les permis de construire depuis `source_url`."""
    raw_csv = await fetch_sitadel_csv(source_url)
    # Parsing Polars CPU-bound synchrone (fichier national, plusieurs centaines
    # de Mo) : voir le commentaire équivalent dans dvf/ingestion.py::ingest_dvf.
    dataframe = await asyncio.to_thread(parse_sitadel_csv, raw_csv)
    documents = (_row_to_document(row) for row in dataframe.iter_rows(named=True))

    success, errors = await bulk_index(client, index_alias, documents)
    logger.info("sitadel_ingestion_completed", success=success, errors=errors)
    return success, errors
