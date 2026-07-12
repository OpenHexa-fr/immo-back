"""Modèles Pydantic du domaine Sitadel (permis de construire).

Schéma validé face à l'export CSV réel du SDES (plateforme DiDo, "Liste des
autorisations d'urbanisme créant des logements") : voir le commentaire en tête
de `ingestion.py` pour le détail des colonnes sources et leurs limites (pas de
coordonnées géographiques dans la source).
"""

from __future__ import annotations

from openhexa_core.models import BaseDocument, BasePaginatedResponse
from pydantic import BaseModel


class GeoPoint(BaseModel):
    lat: float
    lon: float


class PermisConstruire(BaseDocument):
    numero_permis: str
    date_autorisation: str
    type_permis: str
    commune: str | None = None
    code_postal: str | None = None
    nombre_logements: int | None = None
    surface_plancher: float | None = None
    location: GeoPoint | None = None


class SitadelSearchParams(BaseModel):
    commune: str | None = None
    code_postal: str | None = None
    type_permis: list[str] | None = None


class SitadelSearchResponse(BasePaginatedResponse[PermisConstruire]):
    pass
