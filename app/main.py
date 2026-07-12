"""Point d'entrée de l'API OpenHexa Immo."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from openhexa_core.elasticsearch.client import close_client, get_client
from openhexa_core.elasticsearch.index import create_index, ensure_alias

from app.api.v1 import dpe, dvf, sitage
from app.config import get_settings
from app.domain.dpe.mappings import DPE_MAPPING
from app.domain.dvf.mappings import DVF_MAPPING
from app.domain.sitage.mappings import SITADEL_MAPPING

logger = structlog.get_logger(__name__)

_DOMAIN_MAPPINGS = {
    "dvf": DVF_MAPPING,
    "dpe": DPE_MAPPING,
    "sitage": SITADEL_MAPPING,
}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    client = await get_client(settings)

    for domain, mapping in _DOMAIN_MAPPINGS.items():
        alias = f"{settings.es_index_prefix}-{domain}"
        index_name = f"{alias}-000001"
        await create_index(client, index_name, mapping)
        await ensure_alias(client, alias, index_name)

    logger.info("immo_api_started")
    yield
    await close_client()
    logger.info("immo_api_stopped")


app = FastAPI(title="OpenHexa Immo API", lifespan=lifespan)
app.include_router(dvf.router, prefix="/api/v1")
app.include_router(dpe.router, prefix="/api/v1")
app.include_router(sitage.router, prefix="/api/v1")
