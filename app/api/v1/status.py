"""Route d'état de disponibilité des données.

Utilisée par le frontend pour masquer un bandeau "synchronisation en cours"
une fois que chaque domaine a reçu au moins un document.
"""

from __future__ import annotations

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends
from openhexa_core.elasticsearch.client import get_client
from openhexa_core.elasticsearch.search import count
from pydantic import BaseModel

from app.config import Settings, get_settings

router = APIRouter(tags=["status"])


class DomainStatus(BaseModel):
    dvf: bool
    dpe: bool
    sitage: bool


async def _es_client() -> AsyncElasticsearch:
    return await get_client()


@router.get("/status", response_model=DomainStatus)
async def status(
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> DomainStatus:
    """Indique, pour chaque domaine, si au moins un document a été ingéré."""
    prefix = settings.es_index_prefix
    dvf_count = await count(client, f"{prefix}-dvf")
    dpe_count = await count(client, f"{prefix}-dpe")
    sitage_count = await count(client, f"{prefix}-sitage")
    return DomainStatus(dvf=dvf_count > 0, dpe=dpe_count > 0, sitage=sitage_count > 0)
