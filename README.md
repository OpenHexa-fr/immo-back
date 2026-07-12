# openhexa-immo-back

API FastAPI du domaine immobilier de l'écosystème OpenHexa.

Ingestion et exposition de trois sources de données publiques :

- **DVF** (Données de Valeurs Foncières) — transactions immobilières
- **DPE** (Diagnostic de Performance Énergétique)
- **Sit@del2** — permis de construire

> Le schéma Sit@del2 n'étant pas spécifié dans le CLAUDE.md du projet, il a été
> défini par hypothèse (`numero_permis`, `date_autorisation`, `type_permis`,
> `nombre_logements`, `surface_plancher`, `location`) et devra être validé face
> à la structure réelle des fichiers SDES.

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
