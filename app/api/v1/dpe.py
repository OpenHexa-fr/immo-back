"""Routes DPE (Diagnostic de Performance Énergétique)."""

from __future__ import annotations

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Query
from openhexa_core.elasticsearch.client import get_client

from app.config import Settings, get_settings
from app.domain.dpe.schemas import DPEDiagnostic, DPESearchParams, DPESearchResponse
from app.domain.dpe.search import search_dpe

router = APIRouter(prefix="/dpe", tags=["dpe"])


async def _es_client() -> AsyncElasticsearch:
    return await get_client()


@router.get("/search", response_model=DPESearchResponse)
async def search(
    commune: str | None = None,
    code_postal: str | None = None,
    etiquette_dpe: list[str] | None = Query(None),
    search_after: list[str] | None = Query(None),
    size: int = 20,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> DPESearchResponse:
    """Recherche des diagnostics DPE."""
    params = DPESearchParams(
        commune=commune, code_postal=code_postal, etiquette_dpe=etiquette_dpe
    )
    index = f"{settings.es_index_prefix}-dpe"
    page = await search_dpe(client, index, params, search_after=search_after, size=size)

    items = [DPEDiagnostic.model_validate(hit["_source"]) for hit in page["hits"]]
    return DPESearchResponse(
        items=items, total=page["total"], next_search_after=page["next_search_after"]
    )
