# Stage 1: Build + AoT
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

WORKDIR /app

# 🔧 Voraussetzungen für AoT-Build installieren
RUN apt-get update && \
    apt-get install -y --no-install-recommends clang zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Projektdateien kopieren
COPY ./Paper-By-Country-Consumer ./

# AoT Publish (z. B. für linux-x64)
RUN dotnet publish -c Release -r linux-x64 --self-contained true -o /app/publish

# Stage 2: Minimaler Runtime-Container
FROM debian:bookworm AS final

# Benötigte Tools für native Binarys
RUN apt-get update && apt-get install -y \
    libicu-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Nur das AoT-Binary übernehmen
COPY --from=build /app/publish ./

# AoT-Binary ist bereits ausführbar
ENTRYPOINT ["./Write-To-DB-Consumer"]