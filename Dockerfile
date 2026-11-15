# Image de l'API Immo. Le contexte de build est la racine du monorepo (core/ et
# immo-back/ côte à côte), car core n'est pas encore publié comme module Go
# versionné (voir le `replace` de go.mod).
#   docker build -f immo-back/Dockerfile -t openhexa-immo-back .
FROM golang:1.24 AS build
WORKDIR /src
COPY core/ core/
COPY immo-back/ immo-back/
WORKDIR /src/immo-back
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/immo-back ./cmd/immo-back

# distroless/static embarque les certificats CA (HTTPS vers data.gouv.fr et
# l'ADEME) et rien d'autre : pas de shell, pas de gestionnaire de paquets.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/immo-back /immo-back
EXPOSE 8000
ENTRYPOINT ["/immo-back"]
