"""Point d'entrée de l'API OpenHexa Immo."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import structlog
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from openhexa_core.elasticsearch.client import close_client, get_client
from openhexa_core.elasticsearch.index import create_index, ensure_alias
from openhexa_core.elasticsearch.search import count

from app.api.v1 import dpe, dvf, sitage, status
from app.config import Settings, get_settings
from app.domain.dpe.ingestion import ingest_dpe
from app.domain.dpe.mappings import DPE_MAPPING
from app.domain.dvf.ingestion import ingest_dvf_years
from app.domain.dvf.mappings import DVF_MAPPING
from app.domain.sitage.ingestion import ingest_sitadel
from app.domain.sitage.mappings import SITADEL_MAPPING

logger = structlog.get_logger(__name__)

_DOMAIN_MAPPINGS = {
    "dvf": DVF_MAPPING,
    "dpe": DPE_MAPPING,
    "sitage": SITADEL_MAPPING,
}


async def _index_has_data(client: AsyncElasticsearch, index_alias: str) -> bool:
    """True si `index_alias` contient déjà au moins un document.

    Avec `min-replicas: 0` (scale-to-zero), le process redémarre à chaque cold
    start : sans ce check, l'ingestion initiale (téléchargement + parsing d'un
    fichier national potentiellement volumineux) repartirait de zéro à chaque
    fois alors que les données sont déjà indexées. Si le comptage échoue
    (cluster injoignable, alias pas encore créé), on retente l'ingestion par
    prudence plutôt que de la sauter à tort.
    """
    try:
        return await count(client, index_alias) > 0
    except Exception:  # noqa: BLE001 - le comptage ne doit jamais bloquer/casser le polling
        return False


async def _polling_loop(
    name: str,
    ingest: Callable[[], Awaitable[tuple[int, int]]],
    interval_seconds: int,
    *,
    skip_initial_run: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    """Interroge périodiquement une source et réindexe, sans jamais s'arrêter sur erreur.

    Si `skip_initial_run` est fourni et retourne True (typiquement : l'index a
    déjà des données), le premier run immédiat est sauté au profit du prochain
    intervalle — voir `_index_has_data`.
    """
    if skip_initial_run is not None and await skip_initial_run():
        logger.info(f"{name}_polling_initial_run_skipped", reason="index_already_populated")
    else:
        try:
            await ingest()
        except Exception:  # noqa: BLE001 - le polling ne doit jamais s'arrêter sur une erreur réseau
            logger.exception(f"{name}_polling_failed")

    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await ingest()
        except Exception:  # noqa: BLE001 - le polling ne doit jamais s'arrêter sur une erreur réseau
            logger.exception(f"{name}_polling_failed")


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
    dvf_alias, dpe_alias, sitadel_alias = f"{prefix}-dvf", f"{prefix}-dpe", f"{prefix}-sitage"
    return [
        asyncio.create_task(
            _polling_loop(
                "dvf",
                lambda: ingest_dvf_years(client, dvf_alias, settings.resolved_dvf_data_urls()),
                settings.dvf_polling_interval_seconds,
                skip_initial_run=lambda: _index_has_data(client, dvf_alias),
            )
        ),
        asyncio.create_task(
            _polling_loop(
                "dpe",
                lambda: ingest_dpe(client, dpe_alias, settings.dpe_data_url),
                settings.dpe_polling_interval_seconds,
                skip_initial_run=lambda: _index_has_data(client, dpe_alias),
            )
        ),
        asyncio.create_task(
            _polling_loop(
                "sitadel",
                lambda: ingest_sitadel(client, sitadel_alias, settings.sitadel_data_url),
                settings.sitadel_polling_interval_seconds,
                skip_initial_run=lambda: _index_has_data(client, sitadel_alias),
            )
        ),
    ]


app = FastAPI(title="OpenHexa Immo API", lifespan=lifespan)
# API publique en lecture seule (données ouvertes, pas de cookies/session) :
# CORS permissif nécessaire puisque le frontend est servi sur une origine distincte.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.include_router(dvf.router, prefix="/api/v1")
app.include_router(dpe.router, prefix="/api/v1")
app.include_router(sitage.router, prefix="/api/v1")
app.include_router(status.router, prefix="/api/v1")
