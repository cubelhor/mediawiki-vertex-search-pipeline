# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    SOURCE_GCP_PROJECT="prj-c9ec-ing-ds-dev-ec" \
    WIKI_GCS_BUCKET="mediawiki-data-nov-25" \
    WIKI_GCS_PREFIX="" \
    BQ_GCP_PROJECT="prj-c9ec-data-dom-dev-ec" \
    BQ_LOCATION="EU" \
    BQ_SOURCE_DATASET="wiki_raw_internal" \
    BQ_AUTHORIZED_VIEW_DATASET="wiki_vertex_authorized" \
    TARGET_VERTEX_PROJECT="prj-7efd-everydai-dev-4f9a" \
    TARGET_VERTEX_PROJECT_NUMBER="7016082038" \
    VERTEX_LOCATION="eu"

# System CA certificates (Airbus internal root CA is mounted at runtime via Secret Manager -> MEDIAWIKI_CA_BUNDLE)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run as non-root user for least-privilege container execution
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy only the pipeline scripts (wikimedia-data/, tests/, and secrets are excluded by .dockerignore)
COPY wiki_utils.py \
     0_mediawiki_download.py \
     1_fetch_templates.py \
     1b_build_raw_with_templates_jsonl.py \
     2_clean_wikitext.py \
     3_chunk_pages.py \
     4_generate_vertex_jsonl.py \
     5_vertex_import_jsonl.py \
     run_pipeline.py \
     /app/

# Writable working data directory backed by Cloud Run in-memory tmpfs
RUN mkdir -p /app/wikimedia-data && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "/app/run_pipeline.py"]
