# Airbus MediaWiki → Vertex AI Search Dual-Datastore Pipeline

End-to-end ingestion, preprocessing, manual chunking, and **GCS → BigQuery (Authorized View) → Cross-Project Vertex AI Search** pipeline for comparing two retrieval architectures against a Golden Dataset:

1. **Option 1 (`gemini-enterprise-wiki-raw`)**: Full raw MediaWiki `wikitext` with inlined `referenced_templates` definitions per page + standalone `Template:*` documents + extracted `categories` and redirect `aliases`.
2. **Option 2 (`gemini-enterprise-wiki-chunked`)**: Manual preprocessing pipeline expanding MediaWiki templates (`Actuator`, `ECM`, `DMS`, `GlossaryEntry`, `Infobox Equipment`, `EquipmentBox`, `A3Report`, callouts, etc.), converting wiki/HTML tables to Markdown pipe tables, preserving filenames/IDs/links, splitting by hierarchical section headings, and chunking at ~400 tokens with contextual headers (`Page / Aliases / Section / Categories`).

---

## Pipeline Overview

| Script | Stage | Description |
| :--- | :--- | :--- |
| `wiki_utils.py` | Shared Core | Batch/template loader, explicit + template-inherited `categories` parser, redirect `aliases` resolver, RFC-1034 ID generator, and path containment helper. |
| `0_mediawiki_download.py` | Step 0 (API) | Discovers **all knowledge namespaces** (`0` Main, `4` Project, `12` Help, `14` Category hubs, and custom namespaces `100+`) and downloads 50-page batches (`gaplimit=50`) with full `revisions`, `categories`, and canonical URLs. |
| `1_fetch_templates.py` | Step 1 (API) | Downloads all `Template:*` definitions from Namespace `10` into `wikimedia-data/templates/*.json`. |
| `1b_build_raw_with_templates_jsonl.py` | **Option 1** | Builds `raw_with_templates_bq.jsonl` (BigQuery flat schema) and `rawapi-for-vertexai.jsonl` (Vertex GCS schema). |
| `2_clean_wikitext.py` | **Option 2a** | Expands templates, converts `{| wikitable |}` and HTML `<table>` infoboxes to Markdown tables, preserves code/doc filenames & URLs, converts `==` headings to `##`, and writes structured Markdown to `wikimedia-data/processed/pages/`. |
| `3_chunk_pages.py` | **Option 2b** | Splits pages by hierarchical `##`/`###` sections, deduplicates `#REDIRECT` chains into target-page `aliases`, chunks at ~400 tokens with rolling overlap, and prepends `Page / Aliases / Section / Categories` headers. |
| `4_generate_vertex_jsonl.py` | **Option 2c** | Exports Option 2 chunks to `chunks_bq.jsonl` (BigQuery flat schema) and `chunks-for-vertexai.jsonl` (Vertex GCS schema). |
| `5_vertex_import_jsonl.py` | **Cloud Deploy** | Uploads Option 1 & Option 2 JSONLs to GCS, loads **native BigQuery tables**, creates & authorizes **BigQuery Authorized Views** (plus optional materialized export tables), and imports into cross-project Vertex AI Search datastores. |
| `run_pipeline.py` | **Orchestrator** | In-process Cloud Run Job & local orchestrator supporting `--skip-crawl`, daily `recentchanges` short-circuit, and `>= 85%` drop-guard. |
| `tests/test_pipeline.py` | **Tests** | Unit and dataset invariant test suite (`python3 -m unittest discover -s tests`). |

---

## Quick Start

### 1. Install Dependencies & Run Unit Tests
```bash
python3 -m venv .venv-mac
source .venv-mac/bin/activate
pip install -r requirements.txt
python3 -m unittest discover -s tests
```

### 2. (On Airbus Corporate Machine / VPN) Refresh Full MediaWiki Instance Across All Namespaces
```bash
# Standard run (auto-discovers all active namespaces, stages atomically, retries maxlag/5xx/429s)
python3 0_mediawiki_download.py
python3 1_fetch_templates.py

# If an external internet HTTP_PROXY/HTTPS_PROXY intercepts internal .airbus.corp hosts:
python3 0_mediawiki_download.py --bypass-proxy
python3 1_fetch_templates.py --bypass-proxy

# If Airbus SSO authentication is required (no anonymous API read):
#   Option A: Pass a Netscape cookies.txt exported from your authenticated browser session:
python3 0_mediawiki_download.py --bypass-proxy --cookie-file cookies.txt
python3 1_fetch_templates.py --bypass-proxy --cookie-file cookies.txt
#   Option B: Or export MEDIAWIKI_COOKIE="aow_session=..." or MEDIAWIKI_BOT_USER / MEDIAWIKI_BOT_PASSWORD.
```

### 3. Build Both Option 1 (Raw + Templates) and Option 2 (Manual Chunks) JSONLs Locally
```bash
# Option 1: Raw wikitext + inlined referenced templates + standalone templates + categories
python3 1b_build_raw_with_templates_jsonl.py

# Option 2: Clean + expand templates/tables + hierarchical section chunking + categories
python3 2_clean_wikitext.py
python3 3_chunk_pages.py
python3 4_generate_vertex_jsonl.py
```

### 4. Upload to GCS → Create BigQuery Native Tables & Authorized Views → Import to Vertex AI Search
```bash
# Preview all GCS URIs, BigQuery DDL, IAM checklist, and Vertex requests first:
python3 5_vertex_import_jsonl.py --dry-run --materialize-export-table

# Execute GCS upload + BigQuery native table load + Authorized View setup in Project A:
python3 5_vertex_import_jsonl.py \
  --steps upload-gcs setup-bq \
  --source-project "prj-7efd-everydai-dev-4f9a" \
  --gcs-bucket "gemini-enterprise-wiki-staging-20261013-7efd" \
  --bq-source-dataset "wiki_raw_internal" \
  --bq-view-dataset "wiki_vertex_authorized" \
  --materialize-export-table

# Trigger cross-project Vertex AI Search datastore imports into Project B:
python3 5_vertex_import_jsonl.py \
  --steps import-vertex \
  --import-source bq \
  --materialize-export-table \
  --source-project "prj-7efd-everydai-dev-4f9a" \
  --bq-view-dataset "wiki_vertex_authorized" \
  --vertex-project "<TARGET_VERTEX_PROJECT_ID>" \
  --vertex-location "eu"
```

---

## Cross-Project IAM Requirements (Project A BigQuery → Project B Vertex AI Search)

To allow Vertex AI Search in **Project B** to ingest from BigQuery in **Project A**, grant the following roles to `service-<PROJECT_B_NUMBER>@gcp-sa-discoveryengine.iam.gserviceaccount.com`:
1. `roles/bigquery.dataViewer` on dataset `Project_A.wiki_vertex_authorized`
2. `roles/bigquery.jobUser` on `Project_A`

> **Note on Authorized Views vs. Materialized Export Tables**: Discovery Engine bulk ingestion (`BigQuerySource`) prohibits External Tables over GCS and can reject logical SQL Views during table extract jobs. Running `5_vertex_import_jsonl.py` with `--materialize-export-table` creates **both** the Authorized Views (`v_wiki_option1_raw_templates`, `v_wiki_option2_manual_chunks`) and physical export tables (`t_wiki_option1_raw_templates_export`, `t_wiki_option2_manual_chunks_export`) inside the shared dataset.

---

## Deploying as a GCP Cloud Run Job (`run_pipeline.py` + `Dockerfile`)

All standalone scripts (`0_`–`5_`) remain untouched for local runs, while `run_pipeline.py`, `Dockerfile`, `.dockerignore`, and `requirements.txt` package the pipeline for **Cloud Run Jobs**:

### 1. Test Locally Before Deploying
```bash
# Preview `--skip-crawl` (GCS -> BigQuery Authorized Views, no MediaWiki network needed):
python3 run_pipeline.py --skip-crawl --dry-run \
  --source-project "prj-c9ec-data-dom-dev-ec" \
  --gcs-bucket "mediawiki-data-nov-25" \
  --gcs-prefix "" \
  --bq-source-dataset "<YOUR_SOURCE_DATASET>" \
  --bq-view-dataset "wiki_vertex_authorized"
```

### 2. Build Container & Create Cloud Run Job in `--skip-crawl` Mode (Before OpenWiki Firewall Opens)
```bash
# Build container via Cloud Build (.dockerignore ensures wikimedia-data/ & secrets are never uploaded):
gcloud artifacts repositories create wiki-pipeline \
  --repository-format=docker --location=europe-west1 --project=prj-c9ec-data-dom-dev-ec

gcloud builds submit \
  --tag europe-west1-docker.pkg.dev/prj-c9ec-data-dom-dev-ec/wiki-pipeline/mediawiki-sync:latest \
  --project=prj-c9ec-data-dom-dev-ec

# Create Cloud Run Job running in `--skip-crawl` mode first (GCS -> BigQuery):
gcloud run jobs create mediawiki-vertex-sync \
  --image=europe-west1-docker.pkg.dev/prj-c9ec-data-dom-dev-ec/wiki-pipeline/mediawiki-sync:latest \
  --region=europe-west1 \
  --project=prj-c9ec-data-dom-dev-ec \
  --service-account="sa-wiki-sync-job@prj-c9ec-data-dom-dev-ec.iam.gserviceaccount.com" \
  --memory=4Gi --cpu=2 --task-timeout=3600s --max-retries=1 \
  --set-env-vars="SKIP_CRAWL=true,SOURCE_GCP_PROJECT=prj-c9ec-data-dom-dev-ec,WIKI_GCS_BUCKET=mediawiki-data-nov-25,WIKI_GCS_PREFIX=,BQ_LOCATION=EU,BQ_SOURCE_DATASET=<YOUR_SOURCE_DATASET>,BQ_AUTHORIZED_VIEW_DATASET=wiki_vertex_authorized,MATERIALIZE_EXPORT_TABLE=true,RUN_VERTEX_IMPORT=false"

# Execute a test run right now (reads from gs://mediawiki-data-nov-25/ -> loads BigQuery + Authorized Views):
gcloud run jobs execute mediawiki-vertex-sync \
  --region=europe-west1 --project=prj-c9ec-data-dom-dev-ec --wait
```

### 3. Switch to Full Daily Sync Once the OpenWiki Firewall Opens
Update the Cloud Run Job with Direct VPC Egress, Secret Manager CA mount, and `SKIP_CRAWL=false` (daily `list=recentchanges` quiet-day skip + 85% drop-guard + `FULL` Vertex reconciliation):
```bash
gcloud run jobs update mediawiki-vertex-sync \
  --region=europe-west1 --project=prj-c9ec-data-dom-dev-ec \
  --network="<AIRBUS_VPC>" --subnet="<AIRBUS_SUBNET>" --vpc-egress=private-ranges-only \
  --update-env-vars="SKIP_CRAWL=false,RUN_VERTEX_IMPORT=true,TARGET_VERTEX_PROJECT=prj-7efd-everydai-dev-4f9a,MEDIAWIKI_CA_BUNDLE=/secrets/airbus-ca.crt" \
  --set-secrets="/secrets/airbus-ca.crt=airbus-internal-ca:latest"
```

