# openhexa-immo-back

API FastAPI du domaine immobilier de l'écosystème OpenHexa.

Ingestion et exposition de trois sources de données publiques, chacune interrogée
en polling (comme `essence-back`) via une boucle démarrée au lifespan de l'app :

- **DVF** (Données de Valeurs Foncières) — fichier national annuel
  `full.csv.gz` sur `files.data.gouv.fr` (voir `DVF_YEAR` dans `.env.example`).
- **DPE** (Diagnostic de Performance Énergétique) — API data-fair de l'ADEME,
  dataset "DPE Logements existants (depuis juillet 2021)". L'identifiant de
  dataset est un id opaque du catalogue (pas un slug stable) : à re-vérifier
  via `GET https://data.ademe.fr/data-fair/api/v1/datasets?q=DPE` si
  l'ingestion échoue avec un 404.
- **Sitadel** (permis de construire) — export CSV de la plateforme DiDo du
  SDES, "Liste des autorisations d'urbanisme créant des logements". Sitadel2
  est arrêté depuis mars 2026 (remplacé par Sitadel3) ; il n'existe pas de
  téléchargement direct de fichiers statiques pour ce jeu de données, d'où le
  passage par l'API DiDo. La source ne fournit aucune coordonnée géographique :
  le champ `location` de `PermisConstruire` reste toujours `null`.

Les trois URLs et schémas ont été validés face aux exports réels (voir les
docstrings de chaque module `ingestion.py` pour le détail des colonnes sources
et des pièges rencontrés, notamment sur le dédoublonnage DVF).

## Installation (développement)

```bash
pip install -e ../core
pip install -e ".[dev]"
cp .env.example .env
```

## Lancer l'API

```bash
uvicorn app.main:app --reload --port 8000
```

## Tests / qualité

```bash
pytest
ruff check .
mypy .
```
