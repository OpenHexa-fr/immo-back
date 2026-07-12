"""Routes Sit@del2 (permis de construire)."""

from __future__ import annotations

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Query
from openhexa_core.elasticsearch.client import get_client

from app.config import Settings, get_settings
from app.domain.sitage.schemas import (
    PermisConstruire,
    SitadelSearchParams,
    SitadelSearchResponse,
)
from app.domain.sitage.search import search_sitadel

router = APIRouter(prefix="/sitadel", tags=["sitadel"])


async def _es_client() -> AsyncElasticsearch:
    return await get_client()


@router.get("/search", response_model=SitadelSearchResponse)
async def search(
    commune: str | None = None,
    code_postal: str | None = None,
    type_permis: list[str] | None = Query(None),
    search_after: list[str] | None = Query(None),
    size: int = 20,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> SitadelSearchResponse:
    """Recherche des permis de construire Sit@del2."""
    params = SitadelSearchParams(
        commune=commune, code_postal=code_postal, type_permis=type_permis
    )
    index = f"{settings.es_index_prefix}-sitage"
    page = await search_sitadel(client, index, params, search_after=search_after, size=size)

    items = [PermisConstruire.model_validate(hit["_source"]) for hit in page["hits"]]
    return SitadelSearchResponse(
        items=items, total=page["total"], next_search_after=page["next_search_after"]
    )
