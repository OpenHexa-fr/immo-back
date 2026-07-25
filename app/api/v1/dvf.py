"""Routes DVF (Données de Valeurs Foncières)."""

from __future__ import annotations

from typing import Literal

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query
from openhexa_core.elasticsearch.client import get_client

from app.config import Settings, get_settings
from app.domain.dvf.schemas import (
    DVFSearchParams,
    DVFSearchResponse,
    DVFTransaction,
    PrixCarteBucket,
    PrixCarteResponse,
)
from app.domain.dvf.search import aggregate_prix_carte, get_dvf_by_mutation, search_dvf

router = APIRouter(prefix="/dvf", tags=["dvf"])


async def _es_client() -> AsyncElasticsearch:
    return await get_client()


@router.get("/search", response_model=DVFSearchResponse)
async def search(
    commune: str | None = None,
    code_postal: str | None = None,
    id_parcelle: str | None = None,
    type_local: list[str] | None = Query(None),
    valeur_fonciere_min: float | None = None,
    valeur_fonciere_max: float | None = None,
    surface_min: int | None = None,
    surface_max: int | None = None,
    etiquette_dpe: list[str] | None = Query(None),
    date_mutation_min: str | None = None,
    date_mutation_max: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_km: float = 10.0,
    tri: str | None = None,
    search_after: list[str] | None = Query(None),
    size: int = 20,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> DVFSearchResponse:
    """Recherche des transactions DVF."""
    params = DVFSearchParams(
        commune=commune,
        code_postal=code_postal,
        id_parcelle=id_parcelle,
        type_local=type_local,
        valeur_fonciere_min=valeur_fonciere_min,
        valeur_fonciere_max=valeur_fonciere_max,
        surface_min=surface_min,
        surface_max=surface_max,
        etiquette_dpe=etiquette_dpe,
        date_mutation_min=date_mutation_min,
        date_mutation_max=date_mutation_max,
        lat=lat,
        lon=lon,
        radius_km=radius_km,
        tri=tri,
    )
    index = f"{settings.es_index_prefix}-dvf"
    page = await search_dvf(client, index, params, search_after=search_after, size=size)

    items = [DVFTransaction.model_validate(hit["_source"]) for hit in page["hits"]]
    return DVFSearchResponse(
        items=items, total=page["total"], next_search_after=page["next_search_after"]
    )


@router.get("/prix-carte", response_model=PrixCarteResponse)
async def prix_carte(
    niveau: Literal["departement", "commune", "section"],
    code_departement: str | None = None,
    code_commune: str | None = None,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> PrixCarteResponse:
    """Prix médian au m² agrégé par zone, pour la choroplèthe de la carte.

    Chaque niveau de zoom doit être filtré par le niveau parent pour éviter
    d'agréger la France entière en une seule requête.
    """
    if niveau == "commune" and not code_departement:
        raise HTTPException(400, "code_departement est requis pour niveau=commune")
    if niveau == "section" and not code_commune:
        raise HTTPException(400, "code_commune est requis pour niveau=section")

    index = f"{settings.es_index_prefix}-dvf"
    buckets = await aggregate_prix_carte(
        client, index, niveau, code_departement=code_departement, code_commune=code_commune
    )
    return PrixCarteResponse(
        niveau=niveau, buckets=[PrixCarteBucket.model_validate(bucket) for bucket in buckets]
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
