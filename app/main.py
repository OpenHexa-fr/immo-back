"""Point d'entrée de l'API OpenHexa Immo."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import structlog
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from openhexa_core.elasticsearch.client import close_client, get_client
from openhexa_core.elasticsearch.index import create_index, ensure_alias

from app.api.v1 import dpe, dvf, sitage
from app.config import Settings, get_settings
from app.domain.dpe.ingestion import ingest_dpe
from app.domain.dpe.mappings import DPE_MAPPING
from app.domain.dvf.ingestion import ingest_dvf
from app.domain.dvf.mappings import DVF_MAPPING
from app.domain.sitage.ingestion import ingest_sitadel
from app.domain.sitage.mappings import SITADEL_MAPPING

logger = structlog.get_logger(__name__)

_DOMAIN_MAPPINGS = {
    "dvf": DVF_MAPPING,
    "dpe": DPE_MAPPING,
    "sitage": SITADEL_MAPPING,
}


async def _polling_loop(
    name: str,
    ingest: Callable[[], Awaitable[tuple[int, int]]],
    interval_seconds: int,
) -> None:
    """Interroge périodiquement une source et réindexe, sans jamais s'arrêter sur erreur."""
    while True:
        try:
            await ingest()
        except Exception:  # noqa: BLE001 - le polling ne doit jamais s'arrêter sur une erreur réseau
            logger.exception(f"{name}_polling_failed")
        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    client = await get_client(settings)

    for domain, mapping in _DOMAIN_MAPPINGS.items():
        alias = f"{settings.es_index_prefix}-{domain}"
        index_name = f"{alias}-000001"
        await create_index(client, index_name, mapping)
        await ensure_alias(client, alias, index_name)

    polling_tasks = _start_polling_tasks(client, settings)

    logger.info("immo_api_started")
    yield

    for task in polling_tasks:
        task.cancel()
    for task in polling_tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await close_client()
    logger.info("immo_api_stopped")


def _start_polling_tasks(
    client: AsyncElasticsearch, settings: Settings
) -> list[asyncio.Task[None]]:
    prefix = settings.es_index_prefix
    return [
        asyncio.create_task(
            _polling_loop(
                "dvf",
                lambda: ingest_dvf(client, f"{prefix}-dvf", settings.resolved_dvf_data_url()),
                settings.dvf_polling_interval_seconds,
            )
        ),
        asyncio.create_task(
            _polling_loop(
                "dpe",
                lambda: ingest_dpe(client, f"{prefix}-dpe", settings.dpe_data_url),
                settings.dpe_polling_interval_seconds,
            )
        ),
        asyncio.create_task(
            _polling_loop(
                "sitadel",
                lambda: ingest_sitadel(client, f"{prefix}-sitage", settings.sitadel_data_url),
                settings.sitadel_polling_interval_seconds,
            )
        ),
    ]


app = FastAPI(title="OpenHexa Immo API", lifespan=lifespan)
app.include_router(dvf.router, prefix="/api/v1")
app.include_router(dpe.router, prefix="/api/v1")
app.include_router(sitage.router, prefix="/api/v1")
