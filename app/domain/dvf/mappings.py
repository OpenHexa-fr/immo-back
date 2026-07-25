"""Mapping Elasticsearch explicite du domaine DVF."""

from typing import Any

from openhexa_core.elasticsearch.mappings import DATE, FLOAT, GEO_POINT, INTEGER, KEYWORD, TEXT

DVF_MAPPING: dict[str, Any] = {
    "properties": {
        "id_mutation": KEYWORD,
        "date_mutation": DATE,
        "valeur_fonciere": FLOAT,
        "surface_reelle_bati": INTEGER,
        "surface_terrain": INTEGER,
        "nombre_pieces_principales": INTEGER,
        "type_local": KEYWORD,
        "commune": KEYWORD,
        "code_postal": KEYWORD,
        "code_departement": KEYWORD,
        "code_commune": KEYWORD,
        "code_section": KEYWORD,
        "id_parcelle": KEYWORD,
        "adresse": TEXT,
        "prix_m2": FLOAT,
        "location": GEO_POINT,
        "etiquette_dpe": KEYWORD,
    }
}
