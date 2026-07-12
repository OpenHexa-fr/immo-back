# openhexa-core n'est pas encore publié sur PyPI : ce Dockerfile doit être buildé
# depuis la racine du monorepo pour pouvoir installer core/ localement.
#   docker build -f immo-back/Dockerfile -t openhexa-immo-back .
FROM python:3.11-slim
WORKDIR /app

COPY core/ /core/
RUN pip install --no-cache-dir /core

COPY immo-back/pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY immo-back/app/ app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
