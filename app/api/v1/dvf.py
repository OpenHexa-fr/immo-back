"""Routes DVF (Données de Valeurs Foncières)."""

from __future__ import annotations

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Query
from openhexa_core.elasticsearch.client import get_client

from app.config import Settings, get_settings
from app.domain.dvf.schemas import DVFSearchParams, DVFSearchResponse, DVFTransaction
from app.domain.dvf.search import get_dvf_by_mutation, search_dvf

router = APIRouter(prefix="/dvf", tags=["dvf"])


async def _es_client() -> AsyncElasticsearch:
    return await get_client()


@router.get("/search", response_model=DVFSearchResponse)
async def search(
    commune: str | None = None,
    code_postal: str | None = None,
    type_local: list[str] | None = Query(None),
    valeur_fonciere_min: float | None = None,
    valeur_fonciere_max: float | None = None,
    search_after: list[str] | None = Query(None),
    size: int = 20,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> DVFSearchResponse:
    """Recherche des transactions DVF."""
    params = DVFSearchParams(
        commune=commune,
        code_postal=code_postal,
        type_local=type_local,
        valeur_fonciere_min=valeur_fonciere_min,
        valeur_fonciere_max=valeur_fonciere_max,
    )
    index = f"{settings.es_index_prefix}-dvf"
    page = await search_dvf(client, index, params, search_after=search_after, size=size)

    items = [DVFTransaction.model_validate(hit["_source"]) for hit in page["hits"]]
    return DVFSearchResponse(
        items=items, total=page["total"], next_search_after=page["next_search_after"]
    )


@router.get("/{id_mutation}", response_model=list[DVFTransaction])
async def get_by_mutation(
    id_mutation: str,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> list[DVFTransaction]:
    """Retourne les lots d'une mutation DVF donnée (fiche bien)."""
    index = f"{settings.es_index_prefix}-dvf"
    hits = await get_dvf_by_mutation(client, index, id_mutation)
    return [DVFTransaction.model_validate(hit["_source"]) for hit in hits]
