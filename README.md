# openhexa-immo-back

API du domaine immobilier de l'écosystème OpenHexa, en Go (réécriture de l'API FastAPI).

Ingestion et exposition de deux sources de données publiques :

- **DVF** (Données de Valeurs Foncières) : un fichier national `full.csv.gz` par
  millésime sur `files.data.gouv.fr` (`DVF_YEAR_START` à `DVF_YEAR_END`). Le
  fichier est lu **en flux** (téléchargement, décompression, parsing et
  indexation avancent ensemble) : la mémoire ne dépend pas de sa taille.
- **DPE** (Diagnostic de Performance Énergétique) : API data-fair de l'ADEME,
  dataset "DPE Logements existants (depuis juillet 2021)". L'identifiant de
  dataset est opaque : à re-vérifier via
  `GET https://data.ademe.fr/data-fair/api/v1/datasets?q=DPE` si l'ingestion
  échoue avec un 404.

Deux traitements dérivés :

- **Jointure DVF ↔ DPE** sur l'identifiant BAN (reconstruit depuis DVF,
  publié par l'ADEME) : renseigne `etiquette_dpe` sur les ventes, en
  préférant le DPE antérieur le plus récent, à défaut un DPE postérieur de
  moins de 18 mois (marqué `etiquette_dpe_apres_vente`).
- **Zones** : prix médian et quartiles au m² par département, commune et
  section cadastrale (bâti et terrain séparés), part de passoires
  énergétiques, et séries annuelles. Pré-calculés dans l'index `-dvf-zones`
  que lit la carte.

## Index Elasticsearch

Tout commence par `ES_INDEX_PREFIX` (sur la stack partagée du NAS :
`openhexa-immo`) :

| Alias | Contenu |
|---|---|
| `<préfixe>-dvf` | une transaction (lot de mutation) par document |
| `<préfixe>-dpe` | un diagnostic par document |
| `<préfixe>-dvf-zones` | agrégats par zone, et par zone et millésime |

## Contrat d'API

Identique à la version Python (mêmes routes, paramètres et champs JSON, `null`
explicites inclus) : la réécriture est transparente pour `immo-front`.

| Route | Description |
|---|---|
| `GET /api/v1/dvf/search` | filtres (`commune`, `code_postal`, `id_parcelle`, `type_local`*, `etiquette_dpe`*, intervalles `_min`/`_max`, `date_mutation_min/max`), position (`lat`, `lon`, `radius_km` ou `bbox`), `tri`, `champs` (`complet`\|`carte`), `cursor`, `size` |
| `GET /api/v1/dvf/{id_mutation}` | lots d'une mutation |
| `GET /api/v1/dvf/parcelle/{id_parcelle}` | ventes d'une parcelle, groupées par mutation |
| `GET /api/v1/dvf/parcelle/{id_parcelle}/dpe` | tous les DPE connus aux adresses de la parcelle |
| `GET /api/v1/dvf/prix-carte` | choroplèthe : `niveau`, `code_departement`/`code_commune`, `categorie` |
| `GET /api/v1/dvf/prix-serie` | série annuelle d'une zone |
| `GET /api/v1/dvf/zone` | agrégat global d'une zone |
| `GET /api/v1/dpe/search` | `commune`, `code_postal`, `etiquette_dpe`*, `search_after`*, `size` |
| `GET /api/v1/status` | `{"dvf": bool, "dpe": bool}` |

\* paramètre répétable.

Réponses de lecture : `Cache-Control: public, max-age=…, stale-while-revalidate=…`
(sauf `/status`, `no-store`), ETag sur les routes DVF (version = date du dernier
calcul des zones) avec réponse 304 sans requête Elasticsearch, compression gzip
au-delà de 1 000 octets, CORS permissif en lecture seule. Erreurs au format
FastAPI (`{"detail": ...}`).

### Différences avec la version Python

- **Identifiants de documents DVF** : les colonnes de la clé (`numero_disposition`,
  `code_type_local`, `lot1_numero`) sont prises telles qu'écrites dans le CSV
  (`000001`), là où Polars les convertissait en entiers (`1`) et perdait les
  numéros de lot non numériques. Une base remplie par la version Python serait
  donc dupliquée par une réingestion : partir d'index vides.
- **Jointure en mode `in_process`** : le cycle DVF enchaîne désormais ingestion,
  jointure DPE et calcul des zones. La version Python ne lançait la jointure que
  depuis le job, si bien qu'`etiquette_dpe` restait vide en mode `in_process`.
- `/dpe/search` renvoie aussi `total_relation` et `next_cursor`, en plus de
  `next_search_after`.
- Une ligne DPE sans `numero_dpe` est ignorée (la version Python échouait).
  `surface_habitable` et `score_ban` sont convertis en nombres même si l'API
  les publie en chaînes.
- `size` hors de `[1, 1000]` répond 422 (500 en Python).
- Pas de `/docs` ni de `/openapi.json`.

## Configuration

Variables d'environnement (voir `.env.example`, chargé automatiquement s'il existe) :

| Variable | Défaut |
|---|---|
| `ES_URL` | — (obligatoire) |
| `ES_API_KEY` ou `ES_USER` + `ES_PASSWORD` | — |
| `ES_INDEX_PREFIX` | `openhexa` (NAS : `openhexa-immo`) |
| `ELASTIC_CLIENT_APIVERSIONING` | à `true` face à un Elasticsearch 9.x |
| `PORT` | `8000` |
| `INGESTION_MODE` | `in_process` (ou `job`) |
| `DVF_YEAR_START` / `DVF_YEAR_END` | `2021` / `2025` |
| `DVF_DATA_URL` | vide (sinon : une seule URL, ignore les millésimes) |
| `DPE_DATA_URL` | dataset ADEME DPE logements existants |
| `DVF_POLLING_INTERVAL_SECONDS` | `604800` (7 jours) |
| `DPE_POLLING_INTERVAL_SECONDS` | `86400` |
| `HTTP_CACHE_MAX_AGE_SECONDS` | `3600` |
| `HTTP_CACHE_STALE_WHILE_REVALIDATE_SECONDS` | `86400` |

## Ingestion

**`in_process`** (défaut) : le serveur ingère en tâche de fond.

- Cycle DVF (tous les 7 jours) : millésimes, puis jointure DPE, puis zones.
- Cycle DPE (quotidien, incrémental depuis la dernière réception moins 7 jours).
- Au démarrage, un index déjà peuplé saute le premier passage, et les zones
  manquantes sont recalculées.

**`job`** : le serveur n'ingère rien ; un ordonnanceur lance

```bash
immo-back ingest --source all                       # dvf, dpe, jointure, zones
immo-back ingest --source dvf --source zones        # ordre respecté tel quel
immo-back ingest --source dpe-complet               # moisson DPE intégrale, par département
```

Code de retour 0 si tout a abouti, 1 sinon.

Sur un index DPE vide, la première ingestion moissonne tout le dataset (plus
de 15 millions de lignes, plusieurs heures) ; les suivantes sont incrémentales.

## Développement

La dépendance à [`core`](https://github.com/OpenHexa-fr/core) passe par un `replace`
vers `../core` dans `go.mod` : cloner les deux repos côte à côte.

```bash
cp .env.example .env
go run ./cmd/immo-back
```

## Tests / qualité

```bash
gofmt -l .
go vet ./...
go test -race ./...
```

Les tests n'ont besoin d'aucun Elasticsearch : `core/estest` simule le cluster.

## Docker

```bash
# Depuis la racine du monorepo (core/ et immo-back/ côte à côte)
docker build -f immo-back/Dockerfile -t openhexa-immo-back .
```

L'image finale est `distroless/static` (binaire statique, sans shell).
