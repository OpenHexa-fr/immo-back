"""Index et alias Elasticsearch du domaine immobilier.

Partagé par le serveur web (`app.main`) et le job d'ingestion
(`app.jobs.ingest`) : les deux doivent garantir la même topologie d'index, et
l'un comme l'autre peut être le premier à démarrer sur un cluster vierge.
"""

from __future__ import annotations

import asyncio
from typing import Any

from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.index import create_index, ensure_alias

from app.config import Settings
from app.domain.dpe.mappings import DPE_MAPPING
from app.domain.dvf.mappings import DVF_MAPPING, DVF_ZONES_MAPPING

DOMAIN_MAPPINGS = {
    "dvf": DVF_MAPPING,
    "dpe": DPE_MAPPING,
    # Pas une source ingérée mais un index dérivé de `dvf`, alimenté par
    # `compute_zones` : il suit néanmoins la même convention index+alias.
    "dvf-zones": DVF_ZONES_MAPPING,
}


def alias_for(settings: Settings, domain: str) -> str:
    return f"{settings.es_index_prefix}-{domain}"


async def _ensure_domain_index(
    client: AsyncElasticsearch, settings: Settings, domain: str, mapping: dict[str, Any]
) -> None:
    alias = alias_for(settings, domain)
    index_name = f"{alias}-000001"
    await create_index(client, index_name, mapping)
    await ensure_alias(client, alias, index_name)


async def ensure_indices(client: AsyncElasticsearch, settings: Settings) -> None:
    """Crée les index manquants et leurs alias ; met à jour les mappings existants.

    Les domaines sont indépendants (index/alias distincts) : les traiter en
    parallèle plutôt qu'en séquence évite d'accumuler 3×2 aller-retours
    Elasticsearch sur le chemin critique du cold start (scale-to-zero), avant
    que le serveur ne commence à répondre.
    """
    await asyncio.gather(
        *(
            _ensure_domain_index(client, settings, domain, mapping)
            for domain, mapping in DOMAIN_MAPPINGS.items()
        )
    )
