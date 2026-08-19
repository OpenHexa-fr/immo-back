"""Ingestion des données DVF (Données de Valeurs Foncières) dans Elasticsearch.

Source réelle : `https://files.data.gouv.fr/geo-dvf/latest/csv/<annee>/full.csv.gz`
(fichier national unique, compressé gzip). Il n'existe pas de colonne `numero_lot`
unique (hypothèse initiale invalidée face à l'export réel) : le CSV distingue les
lots via `numero_disposition` et jusqu'à 5 colonnes `lotN_numero`.

Une mutation (`id_mutation`) peut en outre porter sur plusieurs parcelles
cadastrales et plusieurs natures de local (ex : une maison + sa dépendance sur
deux parcelles adjacentes) sans que `lot1_numero` ne varie pour autant : deux
lignes peuvent alors partager le même couple (id_mutation, numero_disposition,
lot1_numero) tout en décrivant des biens distincts. Vérifié sur l'export 2025
réel (dédoublonnage sans `id_parcelle`/`code_type_local` : 43% d'ids uniques
seulement sur un échantillon de 50k lignes). L'identifiant de document doit donc
aussi intégrer `id_parcelle` et `code_type_local`.

Même avec ces cinq colonnes, certains groupes de lignes restent strictement
identiques dans le CSV source (ex : plusieurs caves/emplacements de parking
indissociables vendus dans la même mutation, sans lot ni surface renseignés :
vérifié sur l'export 2025 réel, groupe de 2996 lignes dont seulement 528
distinctes sur l'ensemble des colonnes). Le CSV ne fournit alors aucune colonne
capable de les distinguer : un index d'occurrence, calculé par position dans le
fichier au sein de chaque groupe de clé identique, sert de dernier recours pour
garantir des `_id` uniques. Il rend l'ingestion idempotente d'un run à l'autre
tant que l'ordre des lignes du fichier source ne change pas (stable en pratique
pour un millésime déjà publié).
"""

from __future__ import annotations

import asyncio
import gzip
import io
from typing import Any

import httpx
import polars as pl
import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index, make_document_id

logger = structlog.get_logger(__name__)

_DVF_COLUMNS = [
    "id_mutation",
    "numero_disposition",
    "id_parcelle",
    "code_type_local",
    "date_mutation",
    "valeur_fonciere",
    "surface_reelle_bati",
    "surface_terrain",
    "nombre_pieces_principales",
    "type_local",
    "nom_commune",
    "code_postal",
    "code_departement",
    "code_commune",
    "adresse_numero",
    "adresse_suffixe",
    "adresse_nom_voie",
    # Code FANTOIR de la voie : deuxième segment de l'identifiant BAN, donc
    # indispensable au rapprochement avec les DPE (cf. `_compute_identifiant_ban`).
    "adresse_code_voie",
    "lot1_numero",
    "longitude",
    "latitude",
]

# Longueur du préfixe d'`id_parcelle` identifiant la section cadastrale (commune
# INSEE sur 5 caractères + préfixe de commune associée sur 3 caractères + lettres
# de section sur 2 caractères), le numéro de parcelle (4 derniers caractères) en
# étant exclu. Permet de regrouper les mutations par section sans dépendre d'une
# colonne dédiée, absente du CSV source.
_SECTION_PREFIX_LENGTH = 10

_DEDUP_KEY_COLUMNS = [
    "id_mutation",
    "numero_disposition",
    "id_parcelle",
    "code_type_local",
    "lot1_numero",
]


async def fetch_dvf_csv(source_url: str) -> bytes:
    """Télécharge le fichier CSV DVF (gzip) depuis `source_url` et le décompresse."""
    # files.data.gouv.fr redirige (302) vers un bucket S3 OVH : httpx ne suit
    # pas les redirections par défaut, contrairement à curl/aux navigateurs
    # (vérifié en conditions réelles : la source échoue systématiquement sans
    # `follow_redirects`).
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as http_client:
        response = await http_client.get(source_url)
        response.raise_for_status()
        return response.content


def parse_dvf_csv(raw_csv: bytes) -> pl.DataFrame:
    """Parse le CSV DVF en DataFrame Polars, en ne conservant que les colonnes utiles.

    `raw_csv` peut être du CSV brut ou du CSV compressé gzip (cas du fichier national
    réel `full.csv.gz`) : la compression est détectée via la signature magique.
    """
    if raw_csv[:2] == b"\x1f\x8b":
        raw_csv = gzip.decompress(raw_csv)
    dataframe = pl.read_csv(
        io.BytesIO(raw_csv),
        columns=_DVF_COLUMNS,
        # `code_postal`, `code_commune` et `code_departement` ont l'air numériques
        # dans le CSV réel (ex: "59170", "59350", "01") : sans forcer Utf8, polars
        # les infère en entier, ce qui (a) fait échouer la validation Pydantic de
        # l'API (`str`, 500 vérifié en conditions réelles) et (b) tronquerait le
        # zéro initial des codes commençant par 0 (ex: département de l'Ain,
        # "01180" -> 1180, "01053" -> 1053).
        schema_overrides={
            "code_postal": pl.Utf8,
            "code_departement": pl.Utf8,
            "code_commune": pl.Utf8,
            # Les codes FANTOIR sont alphanumériques ("B078") mais certains sont
            # purement numériques ("0550") : sans forçage, polars infère un
            # entier sur les fichiers où seule la seconde forme apparaît, et le
            # zéro initial saute.
            "adresse_code_voie": pl.Utf8,
        },
        infer_schema_length=10_000,
        ignore_errors=True,
    )
    # `valeur_fonciere` est nominalement obligatoire dans le CSV source, mais
    # certaines lignes réelles la laissent vide : sans ce filtre, ces
    # documents indexés avec `valeur_fonciere: null` font échouer la
    # validation Pydantic (`float` non optionnel) au moment de la recherche.
    dataframe = dataframe.filter(pl.col("valeur_fonciere").is_not_null())
    return dataframe.with_columns(
        pl.int_range(pl.len()).over(_DEDUP_KEY_COLUMNS).alias("_occurrence")
    )


def _compute_prix_m2(
    valeur_fonciere: float | None, surface_bati: int | None, surface_terrain: int | None
) -> float | None:
    """Prix au m² : surface bâtie si disponible, sinon surface du terrain.

    Le repli sur la surface du terrain couvre les ventes de terrains nus.

    `valeur_fonciere` est nominalement obligatoire mais certaines lignes du CSV
    source réel la laissent vide (vu en conditions réelles : `TypeError` sur la
    division sans ce garde), d'où le typage optionnel malgré le schéma Pydantic.
    """
    if not valeur_fonciere:
        return None
    surface = surface_bati if surface_bati else surface_terrain
    if not surface:
        return None
    return round(valeur_fonciere / surface, 2)


def _compute_adresse(row: dict[str, Any]) -> str | None:
    """Adresse composée à partir des colonnes numéro/suffixe/voie du CSV source."""
    nom_voie = row.get("adresse_nom_voie")
    if not nom_voie:
        return None
    numero = row.get("adresse_numero")
    suffixe = row.get("adresse_suffixe") or ""
    prefix = f"{int(numero)}{suffixe} " if numero else ""
    return f"{prefix}{nom_voie}"


# Correspondance des suffixes d'adresse DVF vers leur forme dans l'identifiant
# BAN. DVF code le suffixe sur une lettre, la BAN l'écrit en toutes lettres pour
# les rangs ("bis", "ter") mais garde la lettre pour les indices de bâtiment. La
# lettre seule étant ambiguë, on ne traduit que les rangs usuels et on renonce
# au rapprochement pour le reste — un faux positif serait pire qu'une absence.
_SUFFIXES_BAN = {"B": "bis", "T": "ter", "Q": "quater"}


def _compute_identifiant_ban(row: dict[str, Any]) -> str | None:
    """Reconstruit l'identifiant BAN `{code_insee}_{code_voie}_{numero}`.

    C'est la clé de jointure avec les diagnostics DPE, que l'ADEME publie déjà
    sous cette forme. La reconstruire ici évite d'appeler le service de
    géocodage de la BAN sur des millions de mutations.

    Renvoie `None` dès qu'un segment manque — typiquement les ventes de terrains
    nus, dépourvues de numéro de voie, et qui n'ont de toute façon aucun DPE.
    """
    code_commune = row.get("code_commune")
    code_voie = row.get("adresse_code_voie")
    numero = row.get("adresse_numero")
    if not code_commune or not code_voie or numero is None:
        return None

    try:
        numero_formate = f"{int(float(numero)):05d}"
    except (TypeError, ValueError):
        return None

    identifiant = f"{code_commune}_{code_voie}_{numero_formate}"

    suffixe = (row.get("adresse_suffixe") or "").strip().upper()
    if suffixe:
        rang = _SUFFIXES_BAN.get(suffixe)
        if rang is None:
            return None
        identifiant = f"{identifiant}_{rang}"
    return identifiant


def _row_to_document(row: dict[str, Any]) -> dict[str, Any]:
    location = None
    if row.get("latitude") is not None and row.get("longitude") is not None:
        location = {"lat": row["latitude"], "lon": row["longitude"]}

    id_parcelle = row.get("id_parcelle") or None
    code_section = id_parcelle[:_SECTION_PREFIX_LENGTH] if id_parcelle else None

    return {
        "_id": make_document_id(
            row["id_mutation"],
            row.get("numero_disposition") or "",
            row.get("id_parcelle") or "",
            row.get("code_type_local") or "",
            row.get("lot1_numero") or "",
            row.get("_occurrence") or 0,
        ),
        "id_mutation": row["id_mutation"],
        "date_mutation": row["date_mutation"],
        "valeur_fonciere": row["valeur_fonciere"],
        "surface_reelle_bati": row.get("surface_reelle_bati"),
        "surface_terrain": row.get("surface_terrain"),
        "nombre_pieces_principales": row.get("nombre_pieces_principales"),
        "type_local": row.get("type_local"),
        "commune": row["nom_commune"],
        "code_postal": row["code_postal"],
        "code_departement": row.get("code_departement"),
        "code_commune": row.get("code_commune"),
        "code_section": code_section,
        "id_parcelle": id_parcelle,
        "adresse": _compute_adresse(row),
        "identifiant_ban": _compute_identifiant_ban(row),
        "prix_m2": _compute_prix_m2(
            row["valeur_fonciere"], row.get("surface_reelle_bati"), row.get("surface_terrain")
        ),
        "location": location,
    }


async def ingest_dvf(
    client: AsyncElasticsearch, index_alias: str, source_url: str
) -> tuple[int, int]:
    """Télécharge, parse et indexe les transactions DVF depuis `source_url` (un millésime)."""
    raw_csv = await fetch_dvf_csv(source_url)
    # `parse_dvf_csv` est un parsing Polars CPU-bound synchrone (fichier national,
    # potentiellement plusieurs centaines de Mo) : exécuté tel quel dans une
    # coroutine, il gèle tout le event loop (donc toutes les requêtes HTTP en
    # cours) pendant toute sa durée. `to_thread` le sort du thread principal.
    dataframe = await asyncio.to_thread(parse_dvf_csv, raw_csv)
    documents = (_row_to_document(row) for row in dataframe.iter_rows(named=True))

    success, errors = await bulk_index(client, index_alias, documents)
    logger.info("dvf_ingestion_completed", source_url=source_url, success=success, errors=errors)
    return success, errors


async def ingest_dvf_years(
    client: AsyncElasticsearch, index_alias: str, source_urls: list[str]
) -> tuple[int, int]:
    """Ingère séquentiellement plusieurs millésimes DVF (un par URL).

    L'échec d'un millésime (ex. fichier pas encore publié, incident réseau
    ponctuel) n'interrompt pas les autres : chaque téléchargement pouvant
    dépasser une centaine de Mo, une erreur transitoire sur l'un d'eux ne doit
    pas priver les autres millésimes, déjà disponibles, de mise à jour.
    """
    total_success = 0
    total_errors = 0
    for source_url in source_urls:
        try:
            success, errors = await ingest_dvf(client, index_alias, source_url)
        except Exception:  # noqa: BLE001 - un millésime en échec ne doit pas bloquer les autres
            logger.exception("dvf_year_ingestion_failed", source_url=source_url)
            continue
        total_success += success
        total_errors += errors
    return total_success, total_errors
