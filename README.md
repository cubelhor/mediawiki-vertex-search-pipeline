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

### 4. Upload to GCS (`ing-dev`) → BigQuery Native Tables & Authorized Views (`data-dom`) → Vertex AI Search (`everyday-dev`)
```bash
# Preview all GCS URIs, BigQuery DDL, IAM checklist, and Vertex requests (uses built-in 3-project defaults):
python3 5_vertex_import_jsonl.py --dry-run --materialize-export-table

# Execute GCS upload (ing-dev) + BigQuery native table load & Authorized Views (data-dom):
python3 5_vertex_import_jsonl.py \
  --steps upload-gcs setup-bq \
  --source-project "prj-c9ec-ing-ds-dev-ec" \
  --bq-project "prj-c9ec-data-dom-dev-ec" \
  --gcs-bucket "mediawiki-data-nov-25" \
  --bq-source-dataset "aow_nov_26" \
  --bq-view-dataset "wiki_vertex_authorized" \
  --materialize-export-table

# Trigger cross-project Vertex AI Search datastore imports into everyday-dev (prj-7efd-everydai-dev-4f9a):
python3 5_vertex_import_jsonl.py \
  --steps import-vertex \
  --import-source bq \
  --materialize-export-table \
  --bq-project "prj-c9ec-data-dom-dev-ec" \
  --bq-view-dataset "wiki_vertex_authorized" \
  --vertex-project "prj-7efd-everydai-dev-4f9a" \
  --vertex-location "eu"
```

---

## 3-Project Architecture & Cross-Project IAM

| Layer | GCP Project | Resources |
| :--- | :--- | :--- |
| **1. Ingestion (`ing-dev`)** | `prj-c9ec-ing-ds-dev-ec` (`#21248470869`) | Cloud Run Job (`mediawiki-vertex-sync`, `europe-west4`), GCS bucket (`gs://mediawiki-data-nov-25`), Secret Manager, Cloud Scheduler |
| **2. Data Domain (`data-dom`)** | `prj-c9ec-data-dom-dev-ec` | BigQuery (`EU`) base tables (`aow_nov_26`) & Authorized Views + export tables (`wiki_vertex_authorized`) |
| **3. Consumer (`everyday-dev`)** | `prj-7efd-everydai-dev-4f9a` (`#7016082038`) | Vertex AI Search datastores (`gemini-enterprise-wiki-raw`, `gemini-enterprise-wiki-chunked`, `eu`) |

- **`project-service-account@prj-c9ec-ing-ds-dev-ec.iam.gserviceaccount.com`** needs:
  - On `prj-c9ec-ing-ds-dev-ec`: `roles/storage.objectAdmin` (`gs://mediawiki-data-nov-25`), `roles/run.invoker`, `roles/secretmanager.secretAccessor`
  - On `prj-c9ec-data-dom-dev-ec`: `roles/bigquery.dataOwner`, `roles/bigquery.jobUser`
  - On `prj-7efd-everydai-dev-4f9a`: `roles/discoveryengine.editor` *(or `roles/discoveryengine.admin`)*
- **`service-7016082038@gcp-sa-discoveryengine.iam.gserviceaccount.com`** needs:
  - On `prj-c9ec-data-dom-dev-ec`: `roles/bigquery.dataViewer` (**dataset `wiki_vertex_authorized` ONLY**), `roles/bigquery.jobUser`, `roles/bigquery.readSessionUser`

> **Cybersecurity Review Package**: High-resolution network/firewall and authentication diagrams plus security control tables are in `cybersecurity_diagrams/` (`CYBERSECURITY_ARCHITECTURE.md`, `diagram_1_network_and_data_flows.png/.svg`, `diagram_2_auth_and_security_sequence.png/.svg`).

> **Note on Authorized Views vs. Materialized Export Tables**: Discovery Engine bulk ingestion (`BigQuerySource`) prohibits External Tables over GCS and can reject logical SQL Views during table extract jobs. Running with `--materialize-export-table` (enabled by default in `run_pipeline.py`) creates **both** the Authorized Views (`v_wiki_option1_raw_templates`, `v_wiki_option2_manual_chunks`) and physical export tables (`t_wiki_option1_raw_templates_export`, `t_wiki_option2_manual_chunks_export`) inside `prj-c9ec-data-dom-dev-ec.wiki_vertex_authorized`.

---

## Deploying in `prj-c9ec-ing-ds-dev-ec` (`europe-west4` + Shared VPC)

All standalone scripts (`0_`–`5_`) remain available for local runs, while `run_pipeline.py`, `Dockerfile`, `.dockerignore`, and `requirements.txt` package the pipeline as a **Cloud Run Job**:

### 1. Test Locally Before Deploying
```bash
# Preview `--skip-crawl` (gs://mediawiki-data-nov-25 in ing-dev -> BigQuery tables + Authorized Views in data-dom):
python3 run_pipeline.py --skip-crawl --dry-run
```

### 2. Build Container (EU Staging) & Deploy Cloud Run Job (`europe-west4` + Shared VPC `all-traffic`)
```bash
# Create Artifact Registry repository (once):
gcloud artifacts repositories create wiki-pipeline \
  --repository-format=docker --location=europe-west1 --project=prj-c9ec-ing-ds-dev-ec

# Build container strictly in EU using your existing EU GCS bucket for staging/logs:
gcloud builds submit \
  --region=europe-west1 \
  --service-account=projects/prj-c9ec-ing-ds-dev-ec/serviceAccounts/project-service-account@prj-c9ec-ing-ds-dev-ec.iam.gserviceaccount.com \
  --gcs-source-staging-dir=gs://mediawiki-data-nov-25/_cloudbuild/source \
  --gcs-log-dir=gs://mediawiki-data-nov-25/_cloudbuild/logs \
  --tag=europe-west1-docker.pkg.dev/prj-c9ec-ing-ds-dev-ec/wiki-pipeline/mediawiki-sync:latest \
  --project=prj-c9ec-ing-ds-dev-ec

# Deploy Cloud Run Job in europe-west4 attached to Airbus Shared VPC (100.90.29.64/26):
gcloud run jobs deploy mediawiki-vertex-sync \
  --image=europe-west1-docker.pkg.dev/prj-c9ec-ing-ds-dev-ec/wiki-pipeline/mediawiki-sync:latest \
  --region=europe-west4 \
  --project=prj-c9ec-ing-ds-dev-ec \
  --service-account=project-service-account@prj-c9ec-ing-ds-dev-ec.iam.gserviceaccount.com \
  --network=projects/prj-ebb5-devapp-pro-2f88/global/networks/svpc-ebb5-spoke-devapp-prod \
  --subnet=projects/prj-ebb5-devapp-pro-2f88/regions/europe-west4/subnetworks/main-ing-ds-c9ec-ec-ew4-devapp-dev \
  --vpc-egress=all-traffic \
  --memory=4Gi --cpu=2 --task-timeout=3600s --max-retries=1 \
  --set-env-vars="SKIP_CRAWL=true,RUN_VERTEX_IMPORT=false"

# Execute a test run (reads ing-dev GCS -> loads data-dom BigQuery + Authorized Views):
gcloud run jobs execute mediawiki-vertex-sync \
  --region=europe-west4 --project=prj-c9ec-ing-ds-dev-ec --wait
```

### 3. Enable Cross-Project Vertex Import & Daily Full Sync
```bash
# Once IAM is granted on data-dom & everyday-dev, enable Vertex AI Search sync:
gcloud run jobs update mediawiki-vertex-sync \
  --region=europe-west4 --project=prj-c9ec-ing-ds-dev-ec \
  --update-env-vars="RUN_VERTEX_IMPORT=true"

# Once OpenWiki firewall opens (100.90.29.64/26 -> aow.eu.airbus.corp:443), mount CA secret & flip SKIP_CRAWL=false:
gcloud run jobs update mediawiki-vertex-sync \
  --region=europe-west4 --project=prj-c9ec-ing-ds-dev-ec \
  --update-env-vars="SKIP_CRAWL=false,RUN_VERTEX_IMPORT=true,MEDIAWIKI_CA_BUNDLE=/secrets/airbus-ca.crt" \
  --set-secrets="/secrets/airbus-ca.crt=airbus-internal-ca:latest"
```



