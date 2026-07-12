"""Requêtes de recherche Elasticsearch pour le domaine Sit@del2."""

from __future__ import annotations

from typing import Any

from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.search import build_filters, paginate

from app.domain.sitage.schemas import SitadelSearchParams


def _build_sitadel_query(params: SitadelSearchParams) -> dict[str, Any]:
    filters = build_filters(
        commune=params.commune,
        code_postal=params.code_postal,
        type_permis=params.type_permis,
    )
    if not filters:
        return {"match_all": {}}
    return {"bool": {"filter": filters}}


async def search_sitadel(
    client: AsyncElasticsearch,
    index: str,
    params: SitadelSearchParams,
    search_after: list[Any] | None = None,
    size: int = 20,
) -> dict[str, Any]:
    """Recherche des permis de construire selon `params`, paginée par `search_after`."""
    query = _build_sitadel_query(params)
    sort = [{"date_autorisation": "desc"}, {"_seq_no": "asc"}]
    return await paginate(
        client, index=index, query=query, sort=sort, search_after=search_after, size=size
    )
