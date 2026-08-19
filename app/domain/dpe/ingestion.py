"""Ingestion des diagnostics DPE (API data-fair ADEME) dans Elasticsearch.

Contrairement à DVF (CSV téléchargé en une fois), la source DPE est une API REST
paginée (`data-fair`) : l'ingestion consomme page par page jusqu'à obtenir une
page vide.

La pagination par numéro de page (`page`/`size`) est plafonnée par data-fair à
10 000 résultats (`size + skip` ne peut pas dépasser 10 000, limite du result
window Elasticsearch sous-jacent) — inutilisable pour ce dataset qui compte plus
de 15 millions de lignes. L'ingestion suit donc le lien `next` fourni par
l'API, qui encode un curseur `after` stable au-delà de cette limite.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date, timedelta
from typing import Any

import httpx
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

_PAGE_SIZE = 1000


async def fetch_dpe_pages(
    source_url: str, depuis: date | None = None
) -> AsyncIterator[list[dict[str, Any]]]:
    """Itère les pages de résultats de l'API data-fair ADEME via le curseur `next`.

    `depuis` restreint la moisson aux diagnostics reçus à partir de cette date.
    C'est ce qui rend une réingestion hebdomadaire tenable : le dataset complet
    dépasse 15 millions de lignes, quand une semaine en apporte quelques
    milliers.
    """
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as http_client:
        next_url: str | None = f"{source_url}/lines"
        params: dict[str, Any] | None = {"size": _PAGE_SIZE}
        if depuis is not None and params is not None:
            params["qs"] = f"date_reception_dpe:[{depuis.isoformat()} TO *]"
        while next_url:
            response = await http_client.get(next_url, params=params)
            response.raise_for_status()
            body = response.json()
            results = body.get("results", [])
            if not results:
                return
            yield results
            # Le lien `next` porte déjà le filtre et le curseur : les
            # `params` ne doivent pas être réappliqués par-dessus.
            next_url = body.get("next")
            params = None


def _parse_geopoint(raw: object) -> dict[str, float] | None:
    """`_geopoint` arrive sous la forme "lat,lon" et non en objet structuré."""
    if not isinstance(raw, str) or "," not in raw:
        return None
    lat, _, lon = raw.partition(",")
    try:
        return {"lat": float(lat), "lon": float(lon)}
    except ValueError:
        return None


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "_id": make_document_id(row["numero_dpe"]),
        "numero_dpe": row["numero_dpe"],
        "date_etablissement": row.get("date_etablissement_dpe"),
        "date_reception": row.get("date_reception_dpe"),
        "etiquette_dpe": row.get("etiquette_dpe"),
        "etiquette_ges": row.get("etiquette_ges"),
        "commune": row.get("nom_commune_ban"),
        "code_postal": row.get("code_postal_ban"),
        "surface_habitable": row.get("surface_habitable_logement"),
        "identifiant_ban": row.get("identifiant_ban"),
        "adresse_ban": row.get("adresse_ban"),
        "score_ban": row.get("score_ban"),
        "type_batiment": row.get("type_batiment"),
        "location": _parse_geopoint(row.get("_geopoint")),
    }


# Marge de recouvrement appliquée à la borne incrémentale. Une ligne peut
# apparaître dans le dataset après coup avec une date de réception antérieure ;
# la reprendre est sans risque, les `_id` étant déterministes.
_RECOUVREMENT_JOURS = 7


async def derniere_reception(client: AsyncElasticsearch, index_alias: str) -> date | None:
    """Date de réception la plus récente déjà indexée, ou `None` si l'index est vide.

    Sert de point de reprise à l'ingestion incrémentale. Renvoie `None` en cas
    d'échec de lecture : mieux vaut une moisson complète, coûteuse mais
    correcte, qu'un trou silencieux dans les données.
    """
    try:
        response = await client.search(
            index=index_alias,
            size=1,
            source_includes=["date_reception"],
            sort=[{"date_reception": "desc"}],
            query={"exists": {"field": "date_reception"}},
        )
    except Exception:  # noqa: BLE001 - index absent ou champ pas encore peuplé
        return None

    hits = response["hits"]["hits"]
    if not hits:
        return None
    brut = hits[0]["_source"].get("date_reception")
    if not brut:
        return None
    try:
        return date.fromisoformat(str(brut)[:10])
    except ValueError:
        return None


async def ingest_dpe(
    client: AsyncElasticsearch, index_alias: str, source_url: str, *, complet: bool = False
) -> tuple[int, int]:
    """Indexe les diagnostics DPE depuis `source_url`, en incrémental par défaut.

    Le dataset ADEME dépasse 15 millions de lignes : le moissonner intégralement
    prend des heures et dépassait le budget du job planifié. On ne reprend donc
    que ce qui a été reçu depuis la dernière ingestion, avec une marge de
    recouvrement. `complet=True` force la moisson totale — nécessaire au premier
    remplissage, ou après un élargissement des champs conservés, les documents
    déjà indexés n'étant jamais rétro-complétés.
    """
    depuis: date | None = None
    if not complet:
        derniere = await derniere_reception(client, index_alias)
        if derniere is not None:
            depuis = derniere - timedelta(days=_RECOUVREMENT_JOURS)

    logger.info("dpe_ingestion_started", depuis=depuis.isoformat() if depuis else "complet")

    total_success = 0
    total_errors = 0
    async for page in fetch_dpe_pages(source_url, depuis=depuis):
        documents = (_row_to_document(row) for row in page)
        success, errors = await bulk_index(client, index_alias, documents)
        total_success += success
        total_errors += errors

    logger.info("dpe_ingestion_completed", success=total_success, errors=total_errors)
    return total_success, total_errors
