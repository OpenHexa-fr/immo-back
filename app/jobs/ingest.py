"""Ingestion exécutée hors du process web.

Les boucles de polling tournaient jusqu'ici dans le process qui sert l'API :
un parsing Polars d'un CSV national ou un crawl d'agrégation sur des centaines
de milliers de sections cadastrales entrait en concurrence directe, CPU et
mémoire, avec le trafic HTTP — au point d'avoir déjà provoqué un OOM sur le
conteneur web. Ce module fournit le même travail sous forme de commande unique,
destinée à être planifiée séparément (Azure Container Apps Job) :

    python -m app.jobs.ingest --source all
    python -m app.jobs.ingest --source dvf --source zones

Le code de retour vaut 0 si toutes les sources demandées ont abouti, 1 sinon :
c'est ce qui permet à l'ordonnanceur de signaler un cycle en échec.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.client import close_client, get_client

from app.config import Settings, get_settings
from app.domain.dpe.ingestion import ingest_dpe
from app.domain.dvf.ingestion import ingest_dvf_years
from app.domain.dvf.jointure import joindre_dpe
from app.domain.dvf.zones import compute_zones
from app.domain.sitage.ingestion import ingest_sitadel
from app.indices import alias_for, ensure_indices

logger = structlog.get_logger(__name__)

# L'ordre vaut pour `--source all` : `jointure` a besoin de DVF et DPE
# fraîchement ingérés, `zones` dérive de DVF.
SOURCES = ("dvf", "dpe", "sitadel", "jointure", "zones")


async def _run_source(
    client: AsyncElasticsearch, settings: Settings, source: str
) -> tuple[int, int]:
    if source == "dvf":
        return await ingest_dvf_years(
            client, alias_for(settings, "dvf"), settings.resolved_dvf_data_urls()
        )
    if source == "dpe":
        return await ingest_dpe(client, alias_for(settings, "dpe"), settings.dpe_data_url)
    if source == "dpe-complet":
        return await ingest_dpe(
            client, alias_for(settings, "dpe"), settings.dpe_data_url, complet=True
        )
    if source == "jointure":
        return await joindre_dpe(
            client, alias_for(settings, "dvf"), alias_for(settings, "dpe")
        )
    if source == "sitadel":
        return await ingest_sitadel(
            client, alias_for(settings, "sitage"), settings.sitadel_data_url
        )
    if source == "zones":
        return await compute_zones(
            client, alias_for(settings, "dvf"), alias_for(settings, "dvf-zones")
        )
    raise ValueError(f"source inconnue : {source}")


async def run(sources: list[str]) -> int:
    """Exécute les sources demandées dans l'ordre reçu, et retourne un code de sortie.

    `zones` dérivant de `dvf`, l'ordre de la ligne de commande est respecté tel
    quel : c'est à l'appelant de demander `--source dvf --source zones` dans cet
    ordre s'il veut recalculer les agrégats sur des données fraîches.
    """
    settings = get_settings()
    client = await get_client(settings)
    failed: list[str] = []

    try:
        await ensure_indices(client, settings)
        for source in sources:
            try:
                success, errors = await _run_source(client, settings, source)
            except Exception:  # noqa: BLE001 - une source en échec ne doit pas priver les autres
                logger.exception("ingestion_job_source_failed", source=source)
                failed.append(source)
                continue
            logger.info("ingestion_job_source_done", source=source, success=success, errors=errors)
            if errors:
                failed.append(source)
    finally:
        await close_client()

    if failed:
        logger.error("ingestion_job_failed", sources=failed)
        return 1
    logger.info("ingestion_job_completed", sources=sources)
    return 0


def _parse_args(argv: list[str] | None = None) -> list[str]:
    parser = argparse.ArgumentParser(description="Ingestion des sources immobilières OpenHexa.")
    parser.add_argument(
        "--source",
        action="append",
        choices=[*SOURCES, "dpe-complet", "all"],
        required=True,
        help="Source à ingérer ; répétable. `all` équivaut à toutes les sources, "
        "dans un ordre qui respecte leurs dépendances. `dpe-complet` force une "
        "moisson intégrale du dataset ADEME, là où `dpe` ne reprend que les "
        "diagnostics reçus depuis la dernière ingestion.",
    )
    args = parser.parse_args(argv)
    sources: list[str] = args.source
    if "all" in sources:
        return list(SOURCES)
    return sources


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(_parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
