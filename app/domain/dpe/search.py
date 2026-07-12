"""Requêtes de recherche Elasticsearch pour le domaine DPE."""

from __future__ import annotations

from typing import Any

from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.search import build_filters, paginate

from app.domain.dpe.schemas import DPESearchParams


def _build_dpe_query(params: DPESearchParams) -> dict[str, Any]:
    filters = build_filters(
        commune=params.commune,
        code_postal=params.code_postal,
        etiquette_dpe=params.etiquette_dpe,
    )
    if not filters:
        return {"match_all": {}}
    return {"bool": {"filter": filters}}


async def search_dpe(
    client: AsyncElasticsearch,
    index: str,
    params: DPESearchParams,
    search_after: list[Any] | None = None,
    size: int = 20,
) -> dict[str, Any]:
    """Recherche des diagnostics DPE selon `params`, paginée par `search_after`."""
    query = _build_dpe_query(params)
    sort = [{"date_etablissement": "desc"}, {"_seq_no": "asc"}]
    return await paginate(
        client, index=index, query=query, sort=sort, search_after=search_after, size=size
    )
