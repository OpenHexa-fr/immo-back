"""Mapping Elasticsearch explicite du domaine DPE."""

from typing import Any

from openhexa_core.elasticsearch.mappings import DATE, FLOAT, GEO_POINT, KEYWORD

DPE_MAPPING: dict[str, Any] = {
    "properties": {
        "numero_dpe": KEYWORD,
        "date_etablissement": DATE,
        # Date de réception par l'ADEME : c'est elle qui pilote l'apparition
        # d'une ligne dans le dataset, donc elle qui sert de borne à
        # l'ingestion incrémentale (et non la date d'établissement, qui peut
        # être antérieure de plusieurs semaines).
        "date_reception": DATE,
        "etiquette_dpe": KEYWORD,
        "etiquette_ges": KEYWORD,
        "commune": KEYWORD,
        "code_postal": KEYWORD,
        "surface_habitable": FLOAT,
        # Clé de rapprochement avec DVF : identifiant d'adresse de la Base
        # Adresse Nationale, au format `{code_insee}_{code_voie}_{numero}`.
        # Reconstructible depuis DVF sans appeler le service de géocodage.
        "identifiant_ban": KEYWORD,
        "adresse_ban": KEYWORD,
        # Qualité du géocodage BAN : sert à écarter les rapprochements douteux.
        "score_ban": FLOAT,
        "type_batiment": KEYWORD,
        "location": GEO_POINT,
    }
}
