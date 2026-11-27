# Airbus MediaWiki → Vertex AI Search Dual-Datastore Pipeline

End-to-end ingestion, preprocessing, and **GCS / BigQuery (Authorized View) → Cross-Project Vertex AI Search** pipeline comparing two retrieval architectures against a Golden Dataset.

## Unified `action=parse` HTML Pipeline *(Recommended — Branch `feat/action-parse-html`)*

Uses MediaWiki's native **`action=parse`** renderer once so MediaWiki expands every template (`GlossaryEntry`, `Infobox Equipment`, `DMS`, `ECM`, `{{#switch}}`, tables) natively, feeding **both** datastores cleanly from one crawl:

1. **Option 1 (`html_native` → `gemini-enterprise-wiki-html-native`):** Standalone rendered `.html` files + `metadata.jsonl` in GCS → Vertex AI Search **`CONTENT_REQUIRED` + Layout Parser + 500-token `LayoutBasedChunkingConfig` (`include_ancestor_headings=True`)**. Defaults to `INCREMENTAL` reconciliation (with `metadata_incremental.jsonl`) to avoid full daily Layout Parser re-parse fees.
2. **Option 2 (`html_chunked` → `gemini-enterprise-wiki-html-chunked`):** Lightweight stdlib `html.parser` converts the exact same `.html` files to clean Markdown (~450-token chunks, `Page / Aliases / Section / Categories` headers, exact `#section` deep links, clean single-separator tables) → **BigQuery Authorized View (`data-dom` → `everyday-dev`)**.

| Script | Stage | Description |
| :--- | :--- | :--- |
| `crawl_parse_html.py` | **Stage 1 (Crawl + Option 1)** | Fast `action=query` pass for page list/redirects (`aliases`) + polite concurrent `action=parse` HTML crawl with `revid` caching, active-HTML sanitization, and Option 1 GCS manifests (`metadata.jsonl` + `metadata_incremental.jsonl`). |
| `html_to_bq_chunks.py` | **Stage 2 (Option 2)** | Zero-dependency stdlib HTML → Markdown section chunker emitting 18-column BigQuery JSONL (`chunks_bq.jsonl`) with `#section` anchor URIs. |
| `run_parse_pipeline.py` | **Orchestrator** | Runs Stages 1–2 + drop-guard + `5_vertex_import_jsonl.py` for `html_native` and/or `html_chunked`. |
| `tests/test_parse_pipeline.py` | **Tests** | Offline unit tests for HTML sanitization, incremental caching, table/breadcrumb chunking, and Vertex `CONTENT_REQUIRED` Layout Parser protobuf config. |

### Quick Start (`action=parse` Pipeline — Windows PowerShell Safe)
```powershell
# 0. One-time GCS IAM grant so Vertex AI Search (everyday-dev) can read Option 1 HTML files from ing-dev:
gcloud storage buckets add-iam-policy-binding gs://mediawiki-data-nov-25 --member="serviceAccount:service-7016082038@gcp-sa-discoveryengine.iam.gserviceaccount.com" --role="roles/storage.objectViewer" --project=prj-c9ec-ing-ds-dev-ec

# 1. Fast 2-page smoke test (Abbreviations 3687 + Finance abbreviations 8159) — dry run first:
python run_parse_pipeline.py --bypass-proxy --page-ids 3687,8159 --run-vertex-import --materialize-export-table --dry-run

# 2. Live 25-page smoke test into both new datastores (gemini-enterprise-wiki-html-native & gemini-enterprise-wiki-html-chunked):
python run_parse_pipeline.py --bypass-proxy --limit 25 --run-vertex-import --materialize-export-table

# 3. Full wiki crawl (~4-6 min with 4 polite workers) + deploy both Option 1 (GCS) and Option 2 (BQ):
python run_parse_pipeline.py --bypass-proxy --workers 4 --run-vertex-import --materialize-export-table

# 4. Daily incremental refresh (only re-parses pages with new revid and only sends changed pages to Layout Parser):
python run_parse_pipeline.py --bypass-proxy --workers 4 --run-vertex-import --use-incremental-manifest --materialize-export-table
```

---

## Legacy `action=query` Wikitext Baseline *(Kept for A/B Comparison)*

1. **Option 1 (`gemini-enterprise-wiki-raw`)**: Full raw MediaWiki `wikitext` with inlined `referenced_templates` definitions per page + standalone `Template:*` documents + extracted `categories` and redirect `aliases`.
2. **Option 2 (`gemini-enterprise-wiki-chunked`)**: Manual preprocessing pipeline expanding MediaWiki templates (`Actuator`, `ECM`, `DMS`, `GlossaryEntry`, `Infobox Equipment`, `EquipmentBox`, `A3Report`, callouts, etc.), converting wiki/HTML tables to Markdown pipe tables, preserving filenames/IDs/links, splitting by hierarchical section headings, and chunking at ~400 tokens with contextual headers (`Page / Aliases / Section / Categories`).

| Script | Stage | Description |
| :--- | :--- | :--- |
| `wiki_utils.py` | Shared Core | Batch/template loader, explicit + template-inherited `categories` parser, redirect `aliases` resolver, RFC-1034 ID generator, and path containment helper. |
| `0_mediawiki_download.py` | Step 0 (API) | Discovers **all knowledge namespaces** (`0` Main, `4` Project, `12` Help, `14` Category hubs, and custom namespaces `100+`) and downloads 50-page batches (`gaplimit=50`) with full `revisions`, `categories`, and canonical URLs. |
| `1_fetch_templates.py` | Step 1 (API) | Downloads all `Template:*` definitions from Namespace `10` into `wikimedia-data/templates/*.json`. |
| `1b_build_raw_with_templates_jsonl.py` | **Option 1** | Builds `raw_with_templates_bq.jsonl` (BigQuery flat schema) and `rawapi-for-vertexai.jsonl` (Vertex GCS schema). |
| `2_clean_wikitext.py` | **Option 2a** | Expands templates, converts `{| wikitable |}` and HTML `<table>` infoboxes to Markdown tables, preserves code/doc filenames & URLs, converts `==` headings to `##`, and writes structured Markdown to `wikimedia-data/processed/pages/`. |
| `3_chunk_pages.py` | **Option 2b** | Splits pages by hierarchical `##`/`###` sections, deduplicates `#REDIRECT` chains into target-page `aliases`, chunks at ~400 tokens with rolling overlap, and prepends `Page / Aliases / Section / Categories` headers. |
| `4_generate_vertex_jsonl.py` | **Option 2c** | Exports Option 2 chunks to `chunks_bq.jsonl` (BigQuery flat schema) and `chunks-for-vertexai.jsonl` (Vertex GCS schema). |
| `5_vertex_import_jsonl.py` | **Cloud Deploy** | Uploads Option 1 & Option 2 datasets to GCS, loads **native BigQuery tables**, creates & authorizes **BigQuery Authorized Views** (plus optional materialized export tables), and imports into cross-project Vertex AI Search datastores (including `html_native` Unstructured HTML + Layout Parser). |
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



