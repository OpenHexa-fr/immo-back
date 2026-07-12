"""Modèles Pydantic du domaine Sit@del2 (permis de construire).

Schéma défini par hypothèse : Sit@del2 n'est pas détaillé dans le CLAUDE.md du
projet (contrairement à DVF et DPE). À valider face à la structure réelle des
fichiers publiés par le SDES.
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
    commune: str
    code_postal: str
    nombre_logements: int | None = None
    surface_plancher: float | None = None
    location: GeoPoint | None = None


class SitadelSearchParams(BaseModel):
    commune: str | None = None
    code_postal: str | None = None
    type_permis: list[str] | None = None


class SitadelSearchResponse(BasePaginatedResponse[PermisConstruire]):
    pass
