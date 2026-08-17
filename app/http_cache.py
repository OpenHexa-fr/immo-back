"""Validation conditionnelle des réponses DVF (ETag).

Le `Cache-Control` posé sur les réponses évite l'aller-retour tant qu'elles sont
fraîches ; l'ETag traite ce qui se passe ensuite. Quand le client revalide, on
peut répondre 304 **sans exécuter la requête Elasticsearch**, parce que
l'empreinte ne dépend que de deux choses connues d'avance : l'URL demandée et la
version des données. Une revalidation coûte alors une lecture de version mise en
cache, au lieu d'une recherche complète.

La version est la date du dernier calcul des zones (`calcule_le`), qui suit
chaque ingestion DVF. Elle n'est donc valable que pour les routes DVF : une
ingestion DPE ne la fait pas bouger, et servir un 304 sur `/dpe/search` à ce
titre renverrait une réponse périmée.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from app.domain.dvf.zones import latest_calcule_le

# Une version périmée de quelques dizaines de secondes est sans conséquence :
# l'ingestion qui la fait changer dure des minutes, et le pire cas est de
# revalider une fois de trop.
_VERSION_TTL_SECONDS = 60.0


class DataVersion:
    """Cache en mémoire de la version des données, pour ne pas interroger ES à chaque requête."""

    def __init__(self, ttl_seconds: float = _VERSION_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
        self._value: str | None = None
        self._fetched_at: float | None = None

    def invalidate(self) -> None:
        self._value = None
        self._fetched_at = None

    async def get(self, client: Any, zones_index: str) -> str | None:
        now = time.monotonic()
        if self._fetched_at is not None and now - self._fetched_at < self._ttl:
            return self._value

        try:
            self._value = await latest_calcule_le(client, zones_index)
        except Exception:  # noqa: BLE001 - une version indisponible désactive l'ETag, sans plus
            return None
        self._fetched_at = now
        return self._value


def build_etag(version: str, path: str, query: str) -> str:
    """Empreinte faible d'une réponse : même version + même URL = même contenu.

    Faible (`W/`) parce qu'elle ne porte pas sur les octets rendus — la
    compression ou l'ordre des clés JSON peuvent varier sans que la réponse
    change de sens.
    """
    digest = hashlib.sha256(f"{version}|{path}?{query}".encode()).hexdigest()[:32]
    return f'W/"{digest}"'
