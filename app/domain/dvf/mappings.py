"""Mapping Elasticsearch explicite du domaine DVF."""

from typing import Any

from openhexa_core.elasticsearch.mappings import (
    BOOLEAN,
    DATE,
    FLOAT,
    GEO_POINT,
    INTEGER,
    KEYWORD,
    TEXT,
)

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
        # Clé de rapprochement avec les DPE (cf. dvf/ingestion.py).
        "identifiant_ban": KEYWORD,
        "prix_m2": FLOAT,
        "location": GEO_POINT,
        "etiquette_dpe": KEYWORD,
        # Absent = DPE antérieur ou égal à la vente (cas fiable) ; True = DPE
        # postérieur accepté dans une fenêtre courte (cf. dvf/jointure.py) —
        # l'interface doit alors signaler la réserve.
        "etiquette_dpe_apres_vente": BOOLEAN,
    }
}

# Agrégats de prix par zone, recalculés à l'issue de chaque ingestion DVF. La
# choroplèthe de la carte n'a aucune raison de refaire à chaque affichage un
# calcul dont le résultat ne change qu'entre deux ingestions.
DVF_ZONES_MAPPING: dict[str, Any] = {
    "properties": {
        "niveau": KEYWORD,  # departement | commune | section
        "code": KEYWORD,
        # Millésime de la mutation. Absent sur les documents « tous millésimes
        # confondus » qui alimentent la choroplèthe : c'est ce qui distingue la
        # vue d'ensemble des points d'une série temporelle.
        "annee": INTEGER,
        # Département d'une commune, commune d'une section : c'est ce champ que
        # filtre la carte pour ne charger que les zones du niveau parent visible.
        "code_parent": KEYWORD,
        "label": KEYWORD,
        # Bâti et terrain nu sont deux marchés distincts, agrégés séparément :
        # au m², un appartement et un champ ne se comparent pas.
        "prix_m2_median_bati": FLOAT,
        "prix_m2_p25_bati": FLOAT,
        "prix_m2_p75_bati": FLOAT,
        "nb_mutations_bati": INTEGER,
        "prix_m2_median_terrain": FLOAT,
        "prix_m2_p25_terrain": FLOAT,
        "prix_m2_p75_terrain": FLOAT,
        "nb_mutations_terrain": INTEGER,
        "nb_mutations": INTEGER,
        "calcule_le": DATE,
    }
}
