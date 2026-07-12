"""Requêtes de recherche Elasticsearch pour le domaine DVF."""

from __future__ import annotations

from typing import Any

from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.search import build_filters, paginate

from app.domain.dvf.schemas import DVFSearchParams


def _build_dvf_query(params: DVFSearchParams) -> dict[str, Any]:
    filters = build_filters(
        commune=params.commune,
        code_postal=params.code_postal,
        type_local=params.type_local,
    )

    range_clause: dict[str, Any] = {}
    if params.valeur_fonciere_min is not None:
        range_clause["gte"] = params.valeur_fonciere_min
    if params.valeur_fonciere_max is not None:
        range_clause["lte"] = params.valeur_fonciere_max
    if range_clause:
        filters.append({"range": {"valeur_fonciere": range_clause}})

    if not filters:
        return {"match_all": {}}
    return {"bool": {"filter": filters}}


async def search_dvf(
    client: AsyncElasticsearch,
    index: str,
    params: DVFSearchParams,
    search_after: list[Any] | None = None,
    size: int = 20,
) -> dict[str, Any]:
    """Recherche des transactions DVF selon `params`, paginée par `search_after`."""
    query = _build_dvf_query(params)
    sort = [{"date_mutation": "desc"}, {"_id": "asc"}]
    return await paginate(
        client, index=index, query=query, sort=sort, search_after=search_after, size=size
    )


async def get_dvf_by_mutation(
    client: AsyncElasticsearch, index: str, id_mutation: str
) -> list[dict[str, Any]]:
    """Retourne tous les lots (documents) associés à une mutation DVF donnée.

    Une mutation peut porter sur plusieurs lots, donc plusieurs documents
    partagent le même `id_mutation` sans partager le même `_id`.
    """
    query = {"term": {"id_mutation": id_mutation}}
    response = await client.search(index=index, query=query, size=50)
    return list(response["hits"]["hits"])
