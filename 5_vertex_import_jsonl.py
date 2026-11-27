"""Upload Option 1 & Option 2 JSONLs to GCS, load BigQuery tables + Authorized Views, and import into Vertex AI Search."""

from __future__ import annotations

import argparse
import concurrent.futures
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

from wiki_utils import (
    DEFAULT_BQ_LOCATION,
    DEFAULT_BQ_PROJECT,
    DEFAULT_BQ_SOURCE_DATASET,
    DEFAULT_BQ_VIEW_DATASET,
    DEFAULT_CHUNKED_DATASTORE_ID,
    DEFAULT_GCS_BUCKET,
    DEFAULT_GCS_PREFIX,
    DEFAULT_HTML_CHUNKED_DATASTORE_ID,
    DEFAULT_HTML_NATIVE_DATASTORE_ID,
    DEFAULT_RAW_DATASTORE_ID,
    DEFAULT_SOURCE_PROJECT,
    DEFAULT_VERTEX_LOCATION,
    DEFAULT_VERTEX_PROJECT,
    DEFAULT_VERTEX_PROJECT_NUMBER,
    get_logger,
)

logger = get_logger("5_vertex_import")
ROOT = Path(__file__).resolve().parent

_GCP_PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,29}$")
_BQ_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
_GCS_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")


@dataclass(frozen=True)
class PipelineOptionSpec:
    key: str
    label: str
    local_bq_jsonl: Path
    local_gcs_doc_jsonl: Path
    gcs_bq_blob: str
    gcs_doc_blob: str
    bq_source_table: str
    bq_authorized_view: str
    bq_export_table: str
    datastore_id: str
    datastore_display_name: str
    is_unstructured_html: bool = False
    local_html_pages_dir: Optional[Path] = None
    local_incremental_doc_jsonl: Optional[Path] = None
    gcs_html_pages_prefix: str = ""
    gcs_incremental_doc_blob: str = ""


def validate_gcp_project_id(project_id: str) -> str:
    if not _GCP_PROJECT_RE.match(project_id or ""):
        raise ValueError(f"Invalid GCP project ID: {project_id!r}")
    return project_id


def validate_bq_identifier(identifier: str) -> str:
    if not _BQ_IDENT_RE.match(identifier or ""):
        raise ValueError(f"Invalid BigQuery identifier (allow-list [A-Za-z0-9_]): {identifier!r}")
    return identifier


def validate_gcs_bucket(bucket_name: str) -> str:
    cleaned = (bucket_name or "").replace("gs://", "").strip("/")
    if not _GCS_BUCKET_RE.match(cleaned):
        raise ValueError(f"Invalid GCS bucket name: {bucket_name!r}")
    return cleaned


def build_option_specs(
    gcs_prefix: str,
    raw_datastore_id: str,
    chunked_datastore_id: str,
    html_native_datastore_id: str = DEFAULT_HTML_NATIVE_DATASTORE_ID,
    html_chunked_datastore_id: str = DEFAULT_HTML_CHUNKED_DATASTORE_ID,
    parsed_html_dir: Optional[Path] = None,
) -> List[PipelineOptionSpec]:
    prefix = gcs_prefix.strip("/")
    prefix_part = f"{prefix}/" if prefix else ""
    html_root = parsed_html_dir or (ROOT / "wikimedia-data" / "parsed_html")
    return [
        PipelineOptionSpec(
            key="raw",
            label="Option 1 (Raw Wikitext + Templates + Categories)",
            local_bq_jsonl=ROOT / "wikimedia-data" / "jsonl-for-vertex" / "rawapi" / "raw_with_templates_bq.jsonl",
            local_gcs_doc_jsonl=ROOT / "wikimedia-data" / "jsonl-for-vertex" / "rawapi" / "rawapi-for-vertexai.jsonl",
            gcs_bq_blob=f"{prefix_part}option1_raw/raw_with_templates_bq.jsonl",
            gcs_doc_blob=f"{prefix_part}option1_raw/rawapi-for-vertexai.jsonl",
            bq_source_table="wiki_option1_raw_templates",
            bq_authorized_view="v_wiki_option1_raw_templates",
            bq_export_table="t_wiki_option1_raw_templates_export",
            datastore_id=raw_datastore_id,
            datastore_display_name="Airbus Wiki - Option 1 (Raw + Templates)",
        ),
        PipelineOptionSpec(
            key="chunked",
            label="Option 2 (Manual Clean + Template Expansion + Manual Chunks)",
            local_bq_jsonl=ROOT / "wikimedia-data" / "jsonl-for-vertex" / "chunks" / "chunks_bq.jsonl",
            local_gcs_doc_jsonl=ROOT / "wikimedia-data" / "jsonl-for-vertex" / "chunks" / "chunks-for-vertexai.jsonl",
            gcs_bq_blob=f"{prefix_part}option2_chunked/chunks_bq.jsonl",
            gcs_doc_blob=f"{prefix_part}option2_chunked/chunks-for-vertexai.jsonl",
            bq_source_table="wiki_option2_manual_chunks",
            bq_authorized_view="v_wiki_option2_manual_chunks",
            bq_export_table="t_wiki_option2_manual_chunks_export",
            datastore_id=chunked_datastore_id,
            datastore_display_name="Airbus Wiki - Option 2 (Manual Chunks)",
        ),
        PipelineOptionSpec(
            key="html_native",
            label="Option 1 HTML (action=parse Rendered HTML + Vertex Layout Parser & Auto-Chunker via GCS)",
            local_bq_jsonl=html_root / "metadata.jsonl",
            local_gcs_doc_jsonl=html_root / "metadata.jsonl",
            gcs_bq_blob=f"{prefix_part}option1_html/metadata.jsonl",
            gcs_doc_blob=f"{prefix_part}option1_html/metadata.jsonl",
            bq_source_table="",
            bq_authorized_view="",
            bq_export_table="",
            datastore_id=html_native_datastore_id,
            datastore_display_name="Airbus Wiki - Option 1 HTML (Vertex Layout Parser)",
            is_unstructured_html=True,
            local_html_pages_dir=html_root / "pages",
            local_incremental_doc_jsonl=html_root / "metadata_incremental.jsonl",
            gcs_html_pages_prefix=f"{prefix_part}option1_html/pages",
            gcs_incremental_doc_blob=f"{prefix_part}option1_html/metadata_incremental.jsonl",
        ),
        PipelineOptionSpec(
            key="html_chunked",
            label="Option 2 HTML (action=parse HTML -> Clean Markdown Manual Chunks via BigQuery)",
            local_bq_jsonl=html_root / "chunks_bq.jsonl",
            local_gcs_doc_jsonl=html_root / "chunks-for-vertexai.jsonl",
            gcs_bq_blob=f"{prefix_part}option2_html_chunked/chunks_bq.jsonl",
            gcs_doc_blob=f"{prefix_part}option2_html_chunked/chunks-for-vertexai.jsonl",
            bq_source_table="wiki_option2_html_chunks",
            bq_authorized_view="v_wiki_option2_html_chunks",
            bq_export_table="t_wiki_option2_html_chunks_export",
            datastore_id=html_chunked_datastore_id,
            datastore_display_name="Airbus Wiki - Option 2 HTML (Manual Chunks)",
        ),
    ]


def get_bq_schema_fields():
    from google.cloud import bigquery

    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="RFC-1034 compliant document ID"),
        bigquery.SchemaField("doc_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ns", "INT64", mode="NULLABLE", description="MediaWiki namespace ID"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("page_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("section_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("chunk_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
        bigquery.SchemaField("referenced_templates", "STRING", mode="REPEATED"),
        bigquery.SchemaField("aliases", "STRING", mode="REPEATED"),
        bigquery.SchemaField("is_redirect", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("redirect_target", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uri", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_file", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("token_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "STRING", mode="NULLABLE"),
    ]


def build_authorized_view_sql(project_id: str, source_dataset: str, source_table: str) -> str:
    proj = validate_gcp_project_id(project_id)
    ds = validate_bq_identifier(source_dataset)
    tbl = validate_bq_identifier(source_table)
    return (
        f"SELECT\n"
        f"  id, doc_type, ns, title, content, page_id, page_title, section_title,\n"
        f"  chunk_type, categories, referenced_templates, aliases, is_redirect,\n"
        f"  redirect_target, uri, source_file, token_count, updated_at\n"
        f"FROM `{proj}.{ds}.{tbl}`\n"
        f"WHERE content IS NOT NULL AND LENGTH(TRIM(content)) > 0"
    )


def upload_option_files_to_gcs(
    source_project: str,
    bucket_name: str,
    specs: Sequence[PipelineOptionSpec],
    import_source: str = "bq",
    upload_all_variants: bool = False,
    dry_run: bool = False,
    upload_workers: int = 8,
) -> None:
    proj = validate_gcp_project_id(source_project)
    bucket_clean = validate_gcs_bucket(bucket_name)

    for spec in specs:
        if spec.is_unstructured_html:
            pages_dir = spec.local_html_pages_dir
            if pages_dir is None or not pages_dir.is_dir():
                raise FileNotFoundError(f"Missing parsed HTML pages directory: {pages_dir}")
            if not spec.local_gcs_doc_jsonl.is_file():
                raise FileNotFoundError(f"Missing Option 1 HTML metadata JSONL: {spec.local_gcs_doc_jsonl}")

            html_files = sorted(pages_dir.glob("*.html"))
            manifest_targets = [(spec.local_gcs_doc_jsonl, spec.gcs_doc_blob)]
            if (
                spec.local_incremental_doc_jsonl
                and spec.local_incremental_doc_jsonl.is_file()
                and spec.gcs_incremental_doc_blob
            ):
                manifest_targets.append((spec.local_incremental_doc_jsonl, spec.gcs_incremental_doc_blob))

            if dry_run:
                logger.info(
                    "[DRY-RUN] Would upload %d .html file(s) -> gs://%s/%s/ and %d manifest JSONL(s)",
                    len(html_files),
                    bucket_clean,
                    spec.gcs_html_pages_prefix,
                    len(manifest_targets),
                )
                continue

            from google.cloud import storage

            client = storage.Client(project=proj)
            bucket = client.bucket(bucket_clean)
            safe_workers = max(1, min(int(upload_workers), 16))
            pages_prefix = spec.gcs_html_pages_prefix.strip("/")

            def _upload_one_html(local_html: Path) -> None:
                blob = bucket.blob(f"{pages_prefix}/{local_html.name}")
                blob.upload_from_filename(str(local_html), content_type="text/html; charset=utf-8")

            with concurrent.futures.ThreadPoolExecutor(max_workers=safe_workers) as pool:
                list(pool.map(_upload_one_html, html_files))

            for local_manifest, blob_name in manifest_targets:
                bucket.blob(blob_name).upload_from_filename(
                    str(local_manifest), content_type="application/jsonl; charset=utf-8"
                )
                logger.info("Uploaded %s -> gs://%s/%s", local_manifest.name, bucket_clean, blob_name)
            logger.info(
                "Uploaded %d HTML page(s) -> gs://%s/%s/",
                len(html_files),
                bucket_clean,
                pages_prefix,
            )
            continue

        targets = [(spec.local_bq_jsonl, spec.gcs_bq_blob)]
        if import_source == "gcs" or upload_all_variants:
            targets.append((spec.local_gcs_doc_jsonl, spec.gcs_doc_blob))

        for local_path, blob_name in targets:
            if not local_path.exists():
                raise FileNotFoundError(f"Missing local export file: {local_path}")
            gcs_uri = f"gs://{bucket_clean}/{blob_name}"
            if dry_run:
                logger.info("[DRY-RUN] Would upload %s (%d bytes) -> %s", local_path, local_path.stat().st_size, gcs_uri)
                continue

            from google.cloud import storage

            client = storage.Client(project=proj)
            blob = client.bucket(bucket_clean).blob(blob_name)
            blob.upload_from_filename(str(local_path), content_type="application/jsonl; charset=utf-8")
            logger.info("Uploaded %s -> %s", local_path.name, gcs_uri)


def setup_bigquery_tables_and_authorized_views(
    bq_project: str,
    bucket_name: str,
    bq_location: str,
    source_dataset_id: str,
    view_dataset_id: str,
    specs: Sequence[PipelineOptionSpec],
    materialize_export_table: bool = False,
    dry_run: bool = False,
) -> None:
    proj = validate_gcp_project_id(bq_project)
    bucket_clean = validate_gcs_bucket(bucket_name)
    src_ds_name = validate_bq_identifier(source_dataset_id)
    view_ds_name = validate_bq_identifier(view_dataset_id)

    for spec in specs:
        if spec.is_unstructured_html:
            logger.info(
                "Skipping BigQuery setup for %s (unstructured GCS CONTENT_REQUIRED datastore).",
                spec.key,
            )
            continue
        src_tbl = validate_bq_identifier(spec.bq_source_table)
        view_name = validate_bq_identifier(spec.bq_authorized_view)
        exp_tbl = validate_bq_identifier(spec.bq_export_table)
        gcs_bq_uri = f"gs://{bucket_clean}/{spec.gcs_bq_blob}"
        view_sql = build_authorized_view_sql(proj, src_ds_name, src_tbl)

        if dry_run:
            logger.info("[DRY-RUN] BigQuery setup for %s:", spec.label)
            logger.info("  1. Load native table `%s.%s.%s` from %s", proj, src_ds_name, src_tbl, gcs_bq_uri)
            logger.info("  2. Create & authorize view `%s.%s.%s`", proj, view_ds_name, view_name)
            if materialize_export_table:
                logger.info("  3. Materialize physical export table `%s.%s.%s`", proj, view_ds_name, exp_tbl)
            continue

        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        bq = bigquery.Client(project=proj, location=bq_location)

        # 1. Ensure both datasets exist (only call create_dataset on HTTP 404 NotFound!)
        for ds_id in (src_ds_name, view_ds_name):
            ds_full = f"{proj}.{ds_id}"
            try:
                bq.get_dataset(ds_full)
            except NotFound:
                ds_obj = bigquery.Dataset(ds_full)
                ds_obj.location = bq_location
                bq.create_dataset(ds_obj, exists_ok=True)
                logger.info("Created BigQuery dataset `%s` (%s)", ds_full, bq_location)

        # 2. Load GCS JSONL into a NATIVE BigQuery table (NOT an External Table!)
        src_table_ref = f"{proj}.{src_ds_name}.{src_tbl}"
        load_config = bigquery.LoadJobConfig(
            schema=get_bq_schema_fields(),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = bq.load_table_from_uri(gcs_bq_uri, src_table_ref, job_config=load_config)
        load_job.result()
        loaded_table = bq.get_table(src_table_ref)
        logger.info("Loaded %d rows into native table `%s`", loaded_table.num_rows, src_table_ref)

        # 3. Create or update the View in-place (only call create_table on HTTP 404 NotFound!)
        view_ref_str = f"{proj}.{view_ds_name}.{view_name}"
        try:
            existing_view = bq.get_table(view_ref_str)
            existing_view.view_query = view_sql
            created_view = bq.update_table(existing_view, ["view_query"])
            logger.info("Updated view `%s` in place", view_ref_str)
        except NotFound:
            view_obj = bigquery.Table(view_ref_str)
            view_obj.view_query = view_sql
            created_view = bq.create_table(view_obj)
            logger.info("Created view `%s`", view_ref_str)

        # 4. Authorize the View against the restricted source dataset (only needed when datasets differ)
        if src_ds_name != view_ds_name:
            source_dataset = bq.get_dataset(f"{proj}.{src_ds_name}")
            access_entries = list(source_dataset.access_entries)
            auth_entry = bigquery.AccessEntry(
                role=None,
                entity_type="view",
                entity_id=created_view.reference.to_api_repr(),
            )
            if auth_entry not in access_entries:
                access_entries.append(auth_entry)
                source_dataset.access_entries = access_entries
                bq.update_dataset(source_dataset, ["access_entries"])
                logger.info("Authorized view `%s` on source dataset `%s.%s`", view_ref_str, proj, src_ds_name)

        # 5. Optional materialized physical table in shared dataset
        if materialize_export_table:
            exp_ref_str = f"{proj}.{view_ds_name}.{exp_tbl}"
            ctas_sql = (
                f"CREATE OR REPLACE TABLE `{proj}.{view_ds_name}.{exp_tbl}` AS\n"
                f"SELECT * FROM `{proj}.{view_ds_name}.{view_name}`"
            )
            bq.query(ctas_sql).result()
            logger.info("Materialized physical export table `%s`", exp_ref_str)


def _get_discovery_client_options(location: str):
    from google.api_core.client_options import ClientOptions

    if location and location != "global":
        return ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
    return None


def build_unstructured_html_datastore_config(
    display_name: str,
    chunk_size: int = 500,
    include_ancestor_headings: bool = True,
):
    """Build a Discovery Engine `DataStore` proto for Unstructured HTML (`CONTENT_REQUIRED` + Layout Parser + Auto-Chunking)."""
    from google.cloud import discoveryengine_v1 as v1

    bounded_chunk_size = max(100, min(int(chunk_size), 500))
    return v1.DataStore(
        display_name=display_name,
        industry_vertical=v1.IndustryVertical.GENERIC,
        solution_types=[v1.SolutionType.SOLUTION_TYPE_SEARCH],
        content_config=v1.DataStore.ContentConfig.CONTENT_REQUIRED,
        document_processing_config=v1.DocumentProcessingConfig(
            default_parsing_config=v1.DocumentProcessingConfig.ParsingConfig(
                layout_parsing_config=v1.DocumentProcessingConfig.ParsingConfig.LayoutParsingConfig(
                    exclude_html_classes=["navbox", "navbox-inner", "mw-editsection", "toc", "noprint", "catlinks"],
                    exclude_html_elements=["script", "style", "noscript"],
                )
            ),
            chunking_config=v1.DocumentProcessingConfig.ChunkingConfig(
                layout_based_chunking_config=v1.DocumentProcessingConfig.ChunkingConfig.LayoutBasedChunkingConfig(
                    chunk_size=bounded_chunk_size,
                    include_ancestor_headings=bool(include_ancestor_headings),
                )
            ),
        ),
    )


def ensure_datastore(
    vertex_project: str,
    vertex_location: str,
    datastore_id: str,
    display_name: str,
    is_unstructured_html: bool = False,
    chunk_size: int = 500,
) -> str:
    from google.api_core.exceptions import NotFound
    from google.cloud import discoveryengine_v1 as v1

    client = v1.DataStoreServiceClient(client_options=_get_discovery_client_options(vertex_location))
    parent = f"projects/{vertex_project}/locations/{vertex_location}/collections/default_collection"
    datastore_name = f"{parent}/dataStores/{datastore_id}"

    try:
        client.get_data_store(name=datastore_name)
        logger.info("Datastore already exists: %s", datastore_name)
        return datastore_name
    except NotFound:
        pass

    if is_unstructured_html:
        datastore = build_unstructured_html_datastore_config(
            display_name=display_name,
            chunk_size=chunk_size,
            include_ancestor_headings=True,
        )
    else:
        datastore = v1.DataStore(
            display_name=display_name,
            industry_vertical=v1.IndustryVertical.GENERIC,
            solution_types=[v1.SolutionType.SOLUTION_TYPE_SEARCH],
        )

    request = v1.CreateDataStoreRequest(
        parent=parent,
        data_store_id=datastore_id,
        data_store=datastore,
    )
    operation = client.create_data_store(request=request)
    result = operation.result(timeout=600)
    logger.info("Created datastore: %s (unstructured_html=%s)", result.name, is_unstructured_html)
    return result.name


def import_into_vertex_datastores(
    bq_project: str,
    bucket_name: str,
    view_dataset_id: str,
    vertex_project: str,
    vertex_location: str,
    specs: Sequence[PipelineOptionSpec],
    import_source: str = "bq",
    use_materialized_table: bool = False,
    reconciliation_mode: str = "auto",
    use_incremental_manifest: bool = False,
    html_chunk_size: int = 500,
    dry_run: bool = False,
) -> None:
    src_proj = validate_gcp_project_id(bq_project)
    dst_proj = validate_gcp_project_id(vertex_project)
    bucket_clean = validate_gcs_bucket(bucket_name)
    view_ds_name = validate_bq_identifier(view_dataset_id)

    for spec in specs:
        effective_mode = reconciliation_mode.lower()
        if effective_mode == "auto":
            # Default to INCREMENTAL on Layout Parser HTML datastores to prevent daily full re-parse fees!
            effective_mode = "incremental" if spec.is_unstructured_html else "full"

        if spec.is_unstructured_html:
            if (
                use_incremental_manifest
                and effective_mode == "incremental"
                and spec.local_incremental_doc_jsonl
                and spec.local_incremental_doc_jsonl.is_file()
                and spec.gcs_incremental_doc_blob
            ):
                if spec.local_incremental_doc_jsonl.stat().st_size == 0:
                    logger.info(
                        "0 changed HTML documents in %s; skipping Vertex Layout Parser import for %s.",
                        spec.local_incremental_doc_jsonl.name,
                        spec.datastore_id,
                    )
                    continue
                manifest_blob = spec.gcs_incremental_doc_blob
            else:
                manifest_blob = spec.gcs_doc_blob
            gcs_doc_uri = f"gs://{bucket_clean}/{manifest_blob}"
            if dry_run:
                logger.info("[DRY-RUN] Vertex AI Search Unstructured HTML import for %s:", spec.label)
                logger.info(
                    "  Target datastore: projects/%s/locations/%s/collections/default_collection/dataStores/%s "
                    "(CONTENT_REQUIRED + LayoutParser + LayoutBasedChunkingConfig(chunk_size=%d, include_ancestor_headings=True))",
                    dst_proj,
                    vertex_location,
                    spec.datastore_id,
                    html_chunk_size,
                )
                logger.info(
                    "  Source: GcsSource(input_uris=[%r], data_schema='document'), mode=%s",
                    gcs_doc_uri,
                    effective_mode.upper(),
                )
                continue

            from google.cloud import discoveryengine_v1 as v1

            ensure_datastore(
                vertex_project=dst_proj,
                vertex_location=vertex_location,
                datastore_id=spec.datastore_id,
                display_name=spec.datastore_display_name,
                is_unstructured_html=True,
                chunk_size=html_chunk_size,
            )
            doc_client = v1.DocumentServiceClient(client_options=_get_discovery_client_options(vertex_location))
            branch_parent = doc_client.branch_path(
                project=dst_proj,
                location=vertex_location,
                data_store=spec.datastore_id,
                branch="0",
            )
            rec_enum = (
                v1.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
                if effective_mode == "incremental"
                else v1.ImportDocumentsRequest.ReconciliationMode.FULL
            )
            req = v1.ImportDocumentsRequest(
                parent=branch_parent,
                gcs_source=v1.GcsSource(input_uris=[gcs_doc_uri], data_schema="document"),
                reconciliation_mode=rec_enum,
            )
            operation = doc_client.import_documents(request=req)
            logger.info("Started Vertex HTML import operation for %s: %s", spec.datastore_id, operation.operation.name)
            result = operation.result(timeout=1800)
            logger.info("Completed Vertex HTML import for %s: %s", spec.datastore_id, result)
            continue

        bq_target_obj = validate_bq_identifier(
            spec.bq_export_table if use_materialized_table else spec.bq_authorized_view
        )
        gcs_doc_uri = f"gs://{bucket_clean}/{spec.gcs_doc_blob}"

        if dry_run:
            logger.info("[DRY-RUN] Vertex AI Search import for %s:", spec.label)
            logger.info(
                "  Target datastore: projects/%s/locations/%s/collections/default_collection/dataStores/%s",
                dst_proj,
                vertex_location,
                spec.datastore_id,
            )
            if import_source == "bq":
                logger.info(
                    "  Source: BigQuerySource(project_id=%r, dataset_id=%r, table_id=%r, data_schema='custom'), id_field='id', mode=%s",
                    src_proj,
                    view_ds_name,
                    bq_target_obj,
                    effective_mode.upper(),
                )
            else:
                logger.info(
                    "  Source: GcsSource(input_uris=[%r], data_schema='document'), mode=%s",
                    gcs_doc_uri,
                    effective_mode.upper(),
                )
            continue

        from google.cloud import discoveryengine_v1 as v1

        ensure_datastore(
            vertex_project=dst_proj,
            vertex_location=vertex_location,
            datastore_id=spec.datastore_id,
            display_name=spec.datastore_display_name,
            is_unstructured_html=False,
        )
        doc_client = v1.DocumentServiceClient(client_options=_get_discovery_client_options(vertex_location))
        branch_parent = doc_client.branch_path(
            project=dst_proj,
            location=vertex_location,
            data_store=spec.datastore_id,
            branch="0",
        )
        rec_enum = (
            v1.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
            if effective_mode == "incremental"
            else v1.ImportDocumentsRequest.ReconciliationMode.FULL
        )

        if import_source == "bq":
            req = v1.ImportDocumentsRequest(
                parent=branch_parent,
                bigquery_source=v1.BigQuerySource(
                    project_id=src_proj,
                    dataset_id=view_ds_name,
                    table_id=bq_target_obj,
                    data_schema="custom",
                ),
                id_field="id",
                auto_generate_ids=False,
                reconciliation_mode=rec_enum,
            )
        else:
            req = v1.ImportDocumentsRequest(
                parent=branch_parent,
                gcs_source=v1.GcsSource(input_uris=[gcs_doc_uri], data_schema="document"),
                reconciliation_mode=rec_enum,
            )

        operation = doc_client.import_documents(request=req)
        logger.info("Started Vertex import operation for %s: %s", spec.datastore_id, operation.operation.name)
        result = operation.result(timeout=1800)
        logger.info("Completed Vertex import for %s: %s", spec.datastore_id, result)


def print_cross_project_iam_hint(
    bq_project: str,
    view_dataset_id: str,
    target_vertex_project_number: str,
    gcs_bucket: str = DEFAULT_GCS_BUCKET,
    include_gcs_hint: bool = False,
) -> None:
    proj_num = target_vertex_project_number or "<TARGET_VERTEX_PROJECT_NUMBER>"
    sa = f"service-{proj_num}@gcp-sa-discoveryengine.iam.gserviceaccount.com"
    logger.info(
        "Cross-Project IAM Checklist (Discovery Engine SA `%s`): "
        "(1) `roles/bigquery.dataViewer` on `%s.%s`, (2) `roles/bigquery.jobUser` + `roles/bigquery.readSessionUser` on `%s`.",
        sa,
        bq_project,
        view_dataset_id,
        bq_project,
    )
    if include_gcs_hint:
        logger.info(
            "Cross-Project GCS IAM Checklist (for Option 1 Unstructured HTML): "
            "grant `roles/storage.objectViewer` on `gs://%s` to `%s`.",
            gcs_bucket,
            sa,
        )


def add_common_cloud_cli_args(parser: argparse.ArgumentParser) -> None:
    """Attach shared GCS / BigQuery / Vertex AI Search flags (used by both `5_vertex_import_jsonl.py` and `run_pipeline.py`)."""
    parser.add_argument(
        "--import-source",
        choices=["bq", "gcs"],
        default="bq",
        help="Source for Vertex AI Search datastore import (`bq` or `gcs`).",
    )
    parser.add_argument(
        "--upload-all-variants",
        action="store_true",
        help="Upload both BigQuery (`*_bq.jsonl`) and Vertex GCS (`*-for-vertexai.jsonl`) files even when `--import-source bq`.",
    )
    parser.add_argument(
        "--source-project",
        default=DEFAULT_SOURCE_PROJECT,
        help="GCP project hosting Cloud Run Job and GCS staging bucket (default: %(default)s).",
    )
    parser.add_argument(
        "--bq-project",
        default=DEFAULT_BQ_PROJECT,
        help="GCP project hosting BigQuery base tables and Authorized Views (default: %(default)s).",
    )
    parser.add_argument("--gcs-bucket", default=DEFAULT_GCS_BUCKET, help="GCS bucket name (default: %(default)s).")
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX, help="Optional GCS object prefix (default: '%(default)s').")
    parser.add_argument("--bq-location", default=DEFAULT_BQ_LOCATION, help="BigQuery dataset location (default: %(default)s).")
    parser.add_argument(
        "--bq-source-dataset",
        default=DEFAULT_BQ_SOURCE_DATASET,
        help="Restricted BigQuery dataset in bq-project for native tables (default: %(default)s).",
    )
    parser.add_argument(
        "--bq-view-dataset",
        default=DEFAULT_BQ_VIEW_DATASET,
        help="Authorized View & export dataset in bq-project shared with Vertex AI Search (default: %(default)s).",
    )
    parser.add_argument(
        "--vertex-project",
        default=DEFAULT_VERTEX_PROJECT,
        help="Target GCP project hosting Vertex AI Search datastores (default: %(default)s).",
    )
    parser.add_argument("--vertex-project-number", default=DEFAULT_VERTEX_PROJECT_NUMBER)
    parser.add_argument("--vertex-location", default=DEFAULT_VERTEX_LOCATION, help="Vertex AI Search location (default: %(default)s).")
    parser.add_argument("--raw-datastore-id", default=DEFAULT_RAW_DATASTORE_ID)
    parser.add_argument("--chunked-datastore-id", default=DEFAULT_CHUNKED_DATASTORE_ID)
    parser.add_argument("--html-native-datastore-id", default=DEFAULT_HTML_NATIVE_DATASTORE_ID)
    parser.add_argument("--html-chunked-datastore-id", default=DEFAULT_HTML_CHUNKED_DATASTORE_ID)
    parser.add_argument(
        "--reconciliation-mode",
        choices=["auto", "incremental", "full"],
        default="auto",
        help="Vertex import reconciliation mode (`auto` = INCREMENTAL for html_native Layout Parser, FULL for BQ tables).",
    )
    parser.add_argument(
        "--use-incremental-manifest",
        action="store_true",
        help="For `html_native` incremental imports, use `metadata_incremental.jsonl` (only changed pages) when non-empty.",
    )
    parser.add_argument(
        "--html-chunk-size",
        type=int,
        default=500,
        help="Target token chunk size (100..500) for Vertex LayoutBasedChunkingConfig on `html_native` (default: 500).",
    )


def run_cloud_deployment(
    options: Sequence[str],
    steps: Sequence[str],
    source_project: str = DEFAULT_SOURCE_PROJECT,
    bq_project: str = DEFAULT_BQ_PROJECT,
    gcs_bucket: str = DEFAULT_GCS_BUCKET,
    gcs_prefix: str = DEFAULT_GCS_PREFIX,
    bq_location: str = DEFAULT_BQ_LOCATION,
    bq_source_dataset: str = DEFAULT_BQ_SOURCE_DATASET,
    bq_view_dataset: str = DEFAULT_BQ_VIEW_DATASET,
    vertex_project: str = DEFAULT_VERTEX_PROJECT,
    vertex_project_number: str = DEFAULT_VERTEX_PROJECT_NUMBER,
    vertex_location: str = DEFAULT_VERTEX_LOCATION,
    raw_datastore_id: str = DEFAULT_RAW_DATASTORE_ID,
    chunked_datastore_id: str = DEFAULT_CHUNKED_DATASTORE_ID,
    html_native_datastore_id: str = DEFAULT_HTML_NATIVE_DATASTORE_ID,
    html_chunked_datastore_id: str = DEFAULT_HTML_CHUNKED_DATASTORE_ID,
    parsed_html_dir: Optional[Path] = None,
    import_source: str = "bq",
    materialize_export_table: bool = False,
    upload_all_variants: bool = False,
    reconciliation_mode: str = "auto",
    use_incremental_manifest: bool = False,
    html_chunk_size: int = 500,
    dry_run: bool = False,
) -> None:
    """Execute GCS upload (ing-dev), BigQuery tables + Authorized Views (data-dom), and Vertex imports (everyday-dev)."""
    all_specs = build_option_specs(
        gcs_prefix=gcs_prefix,
        raw_datastore_id=raw_datastore_id,
        chunked_datastore_id=chunked_datastore_id,
        html_native_datastore_id=html_native_datastore_id,
        html_chunked_datastore_id=html_chunked_datastore_id,
        parsed_html_dir=parsed_html_dir,
    )
    selected_specs = [s for s in all_specs if s.key in set(options)]
    has_html_native = any(s.is_unstructured_html for s in selected_specs)

    if "upload-gcs" in steps:
        upload_option_files_to_gcs(
            source_project=source_project,
            bucket_name=gcs_bucket,
            specs=selected_specs,
            import_source=import_source,
            upload_all_variants=upload_all_variants,
            dry_run=dry_run,
        )

    if "setup-bq" in steps:
        setup_bigquery_tables_and_authorized_views(
            bq_project=bq_project,
            bucket_name=gcs_bucket,
            bq_location=bq_location,
            source_dataset_id=bq_source_dataset,
            view_dataset_id=bq_view_dataset,
            specs=selected_specs,
            materialize_export_table=materialize_export_table,
            dry_run=dry_run,
        )
        print_cross_project_iam_hint(
            bq_project=bq_project,
            view_dataset_id=bq_view_dataset,
            target_vertex_project_number=vertex_project_number,
            gcs_bucket=gcs_bucket,
            include_gcs_hint=has_html_native,
        )

    if "import-vertex" in steps:
        import_into_vertex_datastores(
            bq_project=bq_project,
            bucket_name=gcs_bucket,
            view_dataset_id=bq_view_dataset,
            vertex_project=vertex_project,
            vertex_location=vertex_location,
            specs=selected_specs,
            import_source=import_source,
            use_materialized_table=materialize_export_table,
            reconciliation_mode=reconciliation_mode,
            use_incremental_manifest=use_incremental_manifest,
            html_chunk_size=html_chunk_size,
            dry_run=dry_run,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload Option 1 & Option 2 datasets to GCS, build BigQuery Authorized Views, and import into Vertex Search."
    )
    parser.add_argument(
        "--options",
        nargs="+",
        choices=["raw", "chunked", "html_native", "html_chunked"],
        default=["raw", "chunked"],
        help="Which pipeline dataset(s) to deploy (`raw`, `chunked`, `html_native`, `html_chunked`).",
    )
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=["upload-gcs", "setup-bq", "import-vertex"],
        default=["upload-gcs", "setup-bq", "import-vertex"],
        help="Pipeline stages to run.",
    )
    parser.add_argument(
        "--parsed-html-dir",
        type=Path,
        default=None,
        help="Local directory containing `pages/`, `metadata.jsonl`, and `chunks_bq.jsonl` for action=parse options.",
    )
    parser.add_argument(
        "--materialize-export-table",
        action="store_true",
        help="Also materialize physical export tables in the shared BigQuery dataset and import from them.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate files and print planned operations only.")
    add_common_cloud_cli_args(parser)
    args = parser.parse_args()

    run_cloud_deployment(
        options=args.options,
        steps=args.steps,
        source_project=args.source_project,
        bq_project=args.bq_project,
        gcs_bucket=args.gcs_bucket,
        gcs_prefix=args.gcs_prefix,
        bq_location=args.bq_location,
        bq_source_dataset=args.bq_source_dataset,
        bq_view_dataset=args.bq_view_dataset,
        vertex_project=args.vertex_project,
        vertex_project_number=args.vertex_project_number,
        vertex_location=args.vertex_location,
        raw_datastore_id=args.raw_datastore_id,
        chunked_datastore_id=args.chunked_datastore_id,
        html_native_datastore_id=args.html_native_datastore_id,
        html_chunked_datastore_id=args.html_chunked_datastore_id,
        parsed_html_dir=args.parsed_html_dir,
        import_source=args.import_source,
        materialize_export_table=args.materialize_export_table,
        upload_all_variants=args.upload_all_variants,
        reconciliation_mode=args.reconciliation_mode,
        use_incremental_manifest=args.use_incremental_manifest,
        html_chunk_size=args.html_chunk_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
