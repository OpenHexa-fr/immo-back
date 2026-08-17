"""Tests du domaine DVF."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.domain.dvf.ingestion import (
    _compute_adresse,
    _row_to_document,
    ingest_dvf_years,
    parse_dvf_csv,
)
from app.domain.dvf.schemas import BBox, DVFSearchParams, DVFTransaction
from app.domain.dvf.search import (
    CARTE_SOURCE_FIELDS,
    _build_dvf_query,
    _build_dvf_sort,
    _build_prix_carte_query,
    aggregate_prix_carte,
    get_dvf_by_mutation,
    get_parcelle_mutations,
    search_dvf,
)


def test_row_to_document_builds_deterministic_id() -> None:
    row = {
        "id_mutation": "2024-1",
        "date_mutation": "2024-01-15",
        "numero_disposition": "000001",
        "id_parcelle": "13001000AB0042",
        "lot1_numero": "001",
        "valeur_fonciere": 250000.0,
        "surface_reelle_bati": 80,
        "type_local": "Appartement",
        "nom_commune": "Marseille",
        "code_postal": "13001",
        "code_departement": "13",
        "code_commune": "13001",
        "latitude": 43.29,
        "longitude": 5.37,
    }

    document = _row_to_document(row)

    assert document["_id"] == _row_to_document(row)["_id"]
    assert len(document["_id"]) == 16
    assert document["commune"] == "Marseille"
    assert document["location"] == {"lat": 43.29, "lon": 5.37}
    assert document["code_departement"] == "13"
    assert document["code_commune"] == "13001"
    assert document["code_section"] == "13001000AB"
    assert document["id_parcelle"] == "13001000AB0042"
    assert document["prix_m2"] == 3125.0


def test_row_to_document_falls_back_to_surface_terrain_for_prix_m2() -> None:
    row = {
        "id_mutation": "2024-3",
        "date_mutation": "2024-01-15",
        "numero_disposition": "000001",
        "id_parcelle": None,
        "lot1_numero": None,
        "valeur_fonciere": 100000.0,
        "surface_reelle_bati": None,
        "surface_terrain": 1000,
        "type_local": None,
        "nom_commune": "Vannes",
        "code_postal": "56000",
        "latitude": None,
        "longitude": None,
    }

    document = _row_to_document(row)

    assert document["prix_m2"] == 100.0
    assert document["code_section"] is None


def test_compute_adresse_combines_numero_suffixe_et_voie() -> None:
    row = {"adresse_numero": 28, "adresse_suffixe": None, "adresse_nom_voie": "RUE DE LA POTERNE"}

    assert _compute_adresse(row) == "28 RUE DE LA POTERNE"


def test_compute_adresse_includes_suffixe_when_present() -> None:
    row = {"adresse_numero": 12, "adresse_suffixe": "B", "adresse_nom_voie": "RUE DE TUNIS"}

    assert _compute_adresse(row) == "12B RUE DE TUNIS"


def test_compute_adresse_returns_none_without_voie() -> None:
    row = {"adresse_numero": 12, "adresse_suffixe": None, "adresse_nom_voie": None}

    assert _compute_adresse(row) is None


def test_row_to_document_builds_adresse_from_row() -> None:
    row = {
        "id_mutation": "2024-4",
        "date_mutation": "2024-01-15",
        "numero_disposition": "000001",
        "id_parcelle": "59380000AC0052",
        "lot1_numero": None,
        "valeur_fonciere": 70000.0,
        "surface_reelle_bati": 58,
        "nom_commune": "Bergues",
        "code_postal": "59380",
        "adresse_numero": 28,
        "adresse_suffixe": None,
        "adresse_nom_voie": "RUE DE LA POTERNE",
        "latitude": None,
        "longitude": None,
    }

    document = _row_to_document(row)

    assert document["adresse"] == "28 RUE DE LA POTERNE"


def test_row_to_document_handles_missing_location() -> None:
    row = {
        "id_mutation": "2024-2",
        "date_mutation": "2024-01-15",
        "numero_disposition": "000001",
        "lot1_numero": None,
        "valeur_fonciere": 100000.0,
        "surface_reelle_bati": None,
        "type_local": None,
        "nom_commune": "Lyon",
        "code_postal": "69001",
        "latitude": None,
        "longitude": None,
    }

    document = _row_to_document(row)

    assert document["location"] is None


_RAW_CSV_HEADER = (
    b"id_mutation,numero_disposition,id_parcelle,code_type_local,date_mutation,"
    b"valeur_fonciere,surface_reelle_bati,surface_terrain,nombre_pieces_principales,"
    b"type_local,nom_commune,code_postal,code_departement,code_commune,"
    b"adresse_numero,adresse_suffixe,adresse_nom_voie,lot1_numero,longitude,latitude"
)


def test_parse_dvf_csv_keeps_only_known_columns() -> None:
    raw_csv = (
        _RAW_CSV_HEADER
        + b",extra_column\n"
        + b"2024-1,000001,010760000B0514,1,2024-01-15,250000.0,80,,3,Appartement,Marseille,"
        b"13001,13,13055,28,,RUE DE LA POTERNE,001,5.37,43.29,ignored\n"
    )

    dataframe = parse_dvf_csv(raw_csv)

    assert "extra_column" not in dataframe.columns
    assert dataframe.shape[0] == 1


def test_parse_dvf_csv_keeps_code_postal_as_string() -> None:
    # Vu en conditions réelles : code_postal est numérique dans le CSV source
    # ("59170"), donc inféré en entier par polars sans un schema_overrides
    # explicite. Une chaîne à zéro initial (départment de l'Ain) prouve à la
    # fois que le type est bien str ET que le zéro n'est pas tronqué.
    raw_csv = (
        _RAW_CSV_HEADER
        + b"\n"
        + b"2024-1,000001,010760000B0514,1,2024-01-15,250000.0,80,,3,Appartement,Bourg,"
        b"01000,01,01053,28,,RUE DE LA POTERNE,001,5.37,43.29\n"
    )

    dataframe = parse_dvf_csv(raw_csv)
    row = next(iter(dataframe.iter_rows(named=True)))

    assert row["code_postal"] == "01000"
    assert isinstance(row["code_postal"], str)
    assert row["code_departement"] == "01"
    assert isinstance(row["code_departement"], str)
    assert row["code_commune"] == "01053"
    assert isinstance(row["code_commune"], str)


def test_parse_dvf_csv_decompresses_gzip_input() -> None:
    import gzip

    raw_csv = (
        _RAW_CSV_HEADER + b"\n"
        b"2024-1,000001,010760000B0514,1,2024-01-15,250000.0,80,,3,Appartement,Marseille,"
        b"13001,13,13055,28,,RUE DE LA POTERNE,001,5.37,43.29\n"
    )

    dataframe = parse_dvf_csv(gzip.compress(raw_csv))

    assert dataframe.shape[0] == 1


def test_parse_dvf_csv_drops_rows_with_missing_valeur_fonciere() -> None:
    raw_csv = (
        _RAW_CSV_HEADER
        + b"\n"
        + b"2024-1,000001,010760000B0514,1,2024-01-15,,80,,3,Appartement,Marseille,"
        b"13001,13,13055,28,,RUE DE LA POTERNE,001,5.37,43.29\n"
        b"2024-2,000001,010760000B0515,1,2024-01-15,250000.0,80,,3,Appartement,Marseille,"
        b"13001,13,13055,28,,RUE DE LA POTERNE,001,5.37,43.29\n"
    )

    dataframe = parse_dvf_csv(raw_csv)

    assert dataframe.shape[0] == 1
    assert dataframe["id_mutation"][0] == "2024-2"


def test_parse_dvf_csv_disambiguates_identical_rows_via_occurrence_index() -> None:
    row = (
        b"2025-1,1,33193000AD0001,1,2025-01-07,243596.0,,,,,Bordeaux,33000,33,33063,,,,,-0.567,44.84\n"
    )
    raw_csv = _RAW_CSV_HEADER + b"\n" + row * 3

    dataframe = parse_dvf_csv(raw_csv)
    documents = [_row_to_document(r) for r in dataframe.iter_rows(named=True)]
    ids = [d["_id"] for d in documents]

    assert len(set(ids)) == 3


async def test_ingest_dvf_years_aggregates_success_and_errors_across_years() -> None:
    client = AsyncMock()
    with patch(
        "app.domain.dvf.ingestion.ingest_dvf", AsyncMock(side_effect=[(100, 1), (50, 0)])
    ) as mocked:
        success, errors = await ingest_dvf_years(client, "openhexa-dvf", ["url-2024", "url-2025"])

    assert (success, errors) == (150, 1)
    assert mocked.call_count == 2


async def test_ingest_dvf_years_continues_past_a_failing_year() -> None:
    client = AsyncMock()
    with patch(
        "app.domain.dvf.ingestion.ingest_dvf",
        AsyncMock(side_effect=[ConnectionError("boom"), (50, 0)]),
    ) as mocked:
        success, errors = await ingest_dvf_years(client, "openhexa-dvf", ["url-2024", "url-2025"])

    assert (success, errors) == (50, 0)
    assert mocked.call_count == 2


def test_build_dvf_query_excludes_mutations_without_valeur_fonciere_by_default() -> None:
    assert _build_dvf_query(DVFSearchParams()) == {
        "bool": {"filter": [{"exists": {"field": "valeur_fonciere"}}]}
    }


def test_build_dvf_query_combines_filters_and_range() -> None:
    params = DVFSearchParams(
        commune="Marseille", valeur_fonciere_min=100000, valeur_fonciere_max=300000
    )

    query = _build_dvf_query(params)

    assert {"term": {"commune": "Marseille"}} in query["bool"]["filter"]
    assert {"range": {"valeur_fonciere": {"gte": 100000, "lte": 300000}}} in query["bool"][
        "filter"
    ]


def test_build_dvf_query_filters_by_id_parcelle() -> None:
    params = DVFSearchParams(id_parcelle="59350000AB0112")

    query = _build_dvf_query(params)

    assert {"term": {"id_parcelle": "59350000AB0112"}} in query["bool"]["filter"]


def test_build_dvf_query_filters_by_date_mutation_range() -> None:
    params = DVFSearchParams(date_mutation_min="2020-01-01", date_mutation_max="2020-12-31")

    query = _build_dvf_query(params)

    assert {"range": {"date_mutation": {"gte": "2020-01-01", "lte": "2020-12-31"}}} in query[
        "bool"
    ]["filter"]


def test_build_dvf_query_uses_geo_bounding_box_for_bbox() -> None:
    params = DVFSearchParams(bbox=BBox(min_lon=3.0, min_lat=50.7, max_lon=3.2, max_lat=50.8))

    query = _build_dvf_query(params)

    assert {
        "geo_bounding_box": {
            "location": {
                "top_left": {"lat": 50.8, "lon": 3.0},
                "bottom_right": {"lat": 50.7, "lon": 3.2},
            }
        }
    } in query["bool"]["filter"]


def test_build_dvf_query_prefers_bbox_over_radius_but_keeps_center_for_sorting() -> None:
    params = DVFSearchParams(
        lat=50.75,
        lon=3.1,
        radius_km=5,
        bbox=BBox(min_lon=3.0, min_lat=50.7, max_lon=3.2, max_lat=50.8),
        tri="distance",
    )

    query = _build_dvf_query(params)

    assert not any("geo_distance" in clause for clause in query["bool"]["filter"])
    assert _build_dvf_sort(params)[0]["_geo_distance"]["location"] == {"lat": 50.75, "lon": 3.1}


def test_build_dvf_query_keeps_geo_distance_without_bbox() -> None:
    params = DVFSearchParams(lat=50.75, lon=3.1, radius_km=5)

    query = _build_dvf_query(params)

    assert {
        "geo_distance": {"distance": "5.0km", "location": {"lat": 50.75, "lon": 3.1}}
    } in query["bool"]["filter"]


def test_bbox_parses_geojson_order() -> None:
    bbox = BBox.parse("3.0,50.7,3.2,50.8")

    assert (bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat) == (3.0, 50.7, 3.2, 50.8)


@pytest.mark.parametrize(
    "raw",
    [
        "3.0,50.7,3.2",  # trop peu de valeurs
        "3.0,50.7,3.2,50.8,1",  # trop de valeurs
        "3.0,50.7,3.2,nord",  # non numérique
        "3.0,50.8,3.2,50.7",  # min_lat au-dessus de max_lat
        "3.0,-91,3.2,50.8",  # latitude hors bornes
    ],
)
def test_bbox_rejects_malformed_input(raw: str) -> None:
    with pytest.raises(ValueError):
        BBox.parse(raw)


def test_bbox_accepts_antimeridian_crossing() -> None:
    """Une emprise qui franchit l'antiméridien a `min_lon > max_lon` — ES le gère."""
    bbox = BBox.parse("179.5,-16.0,-179.5,-15.0")

    assert bbox.min_lon > bbox.max_lon


async def test_search_dvf_forwards_source_filtering() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [], "total": {"value": 0}}}

    await search_dvf(client, "openhexa-dvf", DVFSearchParams(), source=CARTE_SOURCE_FIELDS)

    assert client.search.call_args.kwargs["source"] == CARTE_SOURCE_FIELDS


def test_carte_source_fields_cover_required_transaction_fields() -> None:
    """`champs=carte` doit rapatrier de quoi valider un `DVFTransaction`.

    Sinon la réponse casserait à la validation Pydantic au lieu de simplement
    être plus légère.
    """
    required = {
        name for name, field in DVFTransaction.model_fields.items() if field.is_required()
    }

    assert required <= set(CARTE_SOURCE_FIELDS)


async def test_get_parcelle_mutations_groups_lots_by_mutation() -> None:
    """Le regroupement est fait par Elasticsearch, pas reconstitué côté client."""
    client = AsyncMock()
    client.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {"id_mutation": "2024-1", "date_mutation": "2024-03-02"},
                    "inner_hits": {
                        "lots": {
                            "hits": {
                                "hits": [
                                    {"_source": {"id_mutation": "2024-1", "type_local": "Maison"}},
                                    {"_source": {"id_mutation": "2024-1", "type_local": "Garage"}},
                                ]
                            }
                        }
                    },
                }
            ]
        }
    }

    mutations = await get_parcelle_mutations(client, "openhexa-dvf", "59350000AB0112")

    assert len(mutations) == 1
    assert mutations[0]["id_mutation"] == "2024-1"
    assert [lot["type_local"] for lot in mutations[0]["lots"]] == ["Maison", "Garage"]

    kwargs = client.search.call_args.kwargs
    assert kwargs["collapse"]["field"] == "id_mutation"
    assert {"term": {"id_parcelle": "59350000AB0112"}} in kwargs["query"]["bool"]["filter"]


async def test_get_parcelle_mutations_excludes_mutations_without_valeur_fonciere() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}

    await get_parcelle_mutations(client, "openhexa-dvf", "59350000AB0112")

    filters = client.search.call_args.kwargs["query"]["bool"]["filter"]
    assert {"exists": {"field": "valeur_fonciere"}} in filters


async def test_search_dvf_calls_paginate_with_built_query() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [], "total": {"value": 0}}}

    result = await search_dvf(client, "openhexa-dvf", DVFSearchParams(commune="Marseille"))

    assert result["total"] == 0
    client.search.assert_called_once()


def test_build_prix_carte_query_always_requires_prix_m2() -> None:
    query = _build_prix_carte_query(None, None)

    assert query == {"bool": {"filter": [{"exists": {"field": "prix_m2"}}]}}


def test_build_prix_carte_query_scopes_by_parent_zone() -> None:
    query = _build_prix_carte_query("59", None)

    assert {"term": {"code_departement": "59"}} in query["bool"]["filter"]


async def test_aggregate_prix_carte_computes_median_and_drops_empty_buckets() -> None:
    client = AsyncMock()
    client.search.return_value = {
        "aggregations": {
            "by_zone": {
                "buckets": [
                    {
                        "key": "59",
                        "doc_count": 42,
                        "prix_median": {"values": {"50.0": 2500.123}},
                    },
                    {
                        "key": "60",
                        "doc_count": 0,
                        "prix_median": {"values": {"50.0": None}},
                    },
                ]
            }
        }
    }

    results = await aggregate_prix_carte(client, "openhexa-dvf", "departement")

    assert results == [
        {"code": "59", "label": "59", "prix_m2_median": 2500.12, "nb_mutations": 42}
    ]


async def test_aggregate_prix_carte_uses_top_hit_as_commune_label() -> None:
    client = AsyncMock()
    client.search.return_value = {
        "aggregations": {
            "by_zone": {
                "buckets": [
                    {
                        "key": "59055",
                        "doc_count": 10,
                        "prix_median": {"values": {"50.0": 2022.0}},
                        "label": {"hits": {"hits": [{"_source": {"commune": "Bailleul"}}]}},
                    }
                ]
            }
        }
    }

    results = await aggregate_prix_carte(client, "openhexa-dvf", "commune", code_departement="59")

    assert results[0]["label"] == "Bailleul"


async def test_get_dvf_by_mutation_filters_on_term_query() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [{"_source": {"id_mutation": "2024-1"}}]}}

    hits = await get_dvf_by_mutation(client, "openhexa-dvf", "2024-1")

    assert len(hits) == 1
    client.search.assert_called_once_with(
        index="openhexa-dvf", query={"term": {"id_mutation": "2024-1"}}, size=50
    )
