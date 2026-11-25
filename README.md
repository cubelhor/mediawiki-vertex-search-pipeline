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
| `1b_build_raw_with_templates_jsonl.py` | **Option 1** | Builds complete `wikimedia-data/raw_jsonl/*` files plus `raw_with_templates_bq.jsonl` (BigQuery flat schema) and `rawapi-for-vertexai.jsonl` (Vertex GCS schema). |
| `2_clean_wikitext.py` | **Option 2a** | Expands templates, converts `{| wikitable |}` and HTML `<table>` infoboxes to Markdown tables, preserves code/doc filenames & URLs, converts `==` headings to `##`, and writes structured Markdown to `wikimedia-data/processed/pages/`. |
| `3_chunk_pages.py` | **Option 2b** | Splits pages by hierarchical `##`/`###` sections, deduplicates `#REDIRECT` stubs into target-page `aliases`, chunks at ~400 tokens with rolling overlap, and prepends `Page / Aliases / Section / Categories` headers. |
| `4_generate_vertex_jsonl.py` | **Option 2c** | Exports Option 2 chunks to `chunks_bq.jsonl` (BigQuery flat schema) and `chunks-for-vertexai.jsonl` / `vertex_search_documents.jsonl` (Vertex GCS schema). |
| `5_vertex_import_jsonl.py` | **Cloud Deploy** | Uploads Option 1 & Option 2 JSONLs to GCS, loads **native BigQuery tables**, creates & authorizes **BigQuery Authorized Views** (plus optional materialized export tables), and imports into cross-project Vertex AI Search datastores. |

---

## Quick Start

### 1. Install Dependencies
```bash
python3 -m venv .venv-mac
source .venv-mac/bin/activate
pip install -r .requirements.txt
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
