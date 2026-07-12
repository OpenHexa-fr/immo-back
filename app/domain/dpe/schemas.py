"""Modèles Pydantic du domaine DPE (Diagnostic de Performance Énergétique)."""

from __future__ import annotations

from openhexa_core.models import BaseDocument, BasePaginatedResponse
from pydantic import BaseModel


class DPEDiagnostic(BaseDocument):
    numero_dpe: str
    date_etablissement: str | None = None
    etiquette_dpe: str | None = None
    etiquette_ges: str | None = None
    commune: str | None = None
    code_postal: str | None = None
    surface_habitable: float | None = None


class DPESearchParams(BaseModel):
    commune: str | None = None
    code_postal: str | None = None
    etiquette_dpe: list[str] | None = None


class DPESearchResponse(BasePaginatedResponse[DPEDiagnostic]):
    pass
