# syntax=docker/dockerfile:1
# Build depuis la racine du dépôt : docker build -f src/Batch.Host/Dockerfile .

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG TARGETARCH
WORKDIR /src

# Restauration en couche séparée : ne se rejoue que si les dépendances changent.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/Batch.Abstractions/Batch.Abstractions.csproj src/Batch.Abstractions/
COPY src/Batch.Core/Batch.Core.csproj src/Batch.Core/
COPY src/Batch.Infrastructure.PostgreSql/Batch.Infrastructure.PostgreSql.csproj src/Batch.Infrastructure.PostgreSql/
COPY src/Batch.Hosting.Quartz/Batch.Hosting.Quartz.csproj src/Batch.Hosting.Quartz/
COPY src/Batch.Jobs/Batch.Jobs.csproj src/Batch.Jobs/
COPY src/Batch.Host/Batch.Host.csproj src/Batch.Host/
RUN dotnet restore src/Batch.Host/Batch.Host.csproj -a $TARGETARCH

COPY src/ src/
RUN dotnet publish src/Batch.Host/Batch.Host.csproj -c Release -a $TARGETARCH --no-restore -o /app

# Image chiseled « extra » : pas de shell, utilisateur non-root, mais tzdata présent (fuseaux Quartz).
FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled-extra AS runtime
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "batch.dll"]
CMD ["list"]
