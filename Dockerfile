FROM python:3.13-slim

# Java 17 est requis par PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# default-java est un symlink portable (fonctionne sur amd64 et arm64)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Dépendances Python d'abord (layer mis en cache tant que requirements.txt n'change pas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code source
COPY src/ ./src/

# Répertoires créés ici pour les runs sans volume monté (dev/test)
# En production ils sont remplacés par les volumes docker-compose
RUN mkdir -p /app/data /app/logs

# src/ est la racine des modules Python (common, extraction, ...)
ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "src/pipeline.py"]
