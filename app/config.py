"""Configuration de l'API Immo : paramètres Elasticsearch (via core) + sources de données.

URLs validées face aux exports réels (voir les docstrings de chaque module
`ingestion.py`) :

- DVF : un fichier national annuel `full.csv.gz` par millésime. Le mirroir
  `geo-dvf/latest` ne conserve que les cinq derniers millésimes glissants (à ce
  jour : 2021 à 2025, vérifié via l'index `https://files.data.gouv.fr/geo-dvf/
  latest/csv/` — les années antérieures y renvoient 404) ; l'année N n'est pas
  encore disponible en cours d'année N, d'où un défaut sur le dernier
  millésime complet.
- DPE : l'identifiant de dataset ADEME data-fair n'est pas un slug stable
  ("dpe-v2-logements-existants" renvoie 404) mais un identifiant opaque propre
  au catalogue ; il doit être re-vérifié périodiquement via
  `GET https://data.ademe.fr/data-fair/api/v1/datasets?q=DPE` si l'ingestion
  se met à échouer.
- Sitadel : distribué via la plateforme DiDo du SDES, pas par téléchargement
  direct de fichiers statiques.
"""

from __future__ import annotations

from functools import lru_cache

from openhexa_core.config import ESSettings
from pydantic_settings import SettingsConfigDict


class Settings(ESSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    dvf_year_start: int = 2021
    dvf_year_end: int = 2025
    dvf_data_url: str = ""
    dpe_data_url: str = (
        "https://data.ademe.fr/data-fair/api/v1/datasets/meg-83tjwtg8dyz4vv7h1dqe"
    )
    sitadel_data_url: str = (
        "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1/"
        "datafiles/8b35affb-55fc-4c1f-915b-7750f974446a/csv"
    )

    # Sources volumineuses et peu volatiles (mises à jour mensuelles côté
    # producteurs) : polling nettement moins fréquent que les prix carburants.
    dvf_polling_interval_seconds: int = 7 * 24 * 3600
    dpe_polling_interval_seconds: int = 24 * 3600
    sitadel_polling_interval_seconds: int = 7 * 24 * 3600

    def resolved_dvf_data_urls(self) -> list[str]:
        """URLs des fichiers DVF nationaux pour chaque millésime de `dvf_year_start` à
        `dvf_year_end` inclus, sauf override explicite (une seule URL)."""
        if self.dvf_data_url:
            return [self.dvf_data_url]
        return [
            f"https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/full.csv.gz"
            for year in range(self.dvf_year_start, self.dvf_year_end + 1)
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
