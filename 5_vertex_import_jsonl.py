"""Upload Option 1 & Option 2 JSONLs to GCS, load BigQuery tables + Authorized Views, and import into Vertex AI Search.

Supports your full cross-project enterprise workflow:
  Local JSONL -> GCS Bucket (Project A)
              -> Native BigQuery Table in `BQ_SOURCE_DATASET` (Project A)
              -> Authorized View (and optional materialized export table) in `BQ_VIEW_DATASET` (Project A)
              -> Vertex AI Search Datastore 1 (Raw+Templates) & Datastore 2 (Manual Chunks) in Project B

Examples:
  # Preview all GCS paths, BigQuery schemas/views, IAM bindings, and Vertex import requests without cloud calls:
  python3 5_vertex_import_jsonl.py --dry-run

  # Upload JSONLs to GCS and create BigQuery native tables + Authorized Views in Project A:
  python3 5_vertex_import_jsonl.py --steps upload-gcs setup-bq

  # Also trigger cross-project Vertex AI Search imports into Project B (for both Option 1 and Option 2):
  python3 5_vertex_import_jsonl.py --steps upload-gcs setup-bq import-vertex --import-source bq
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

ROOT = Path(__file__).resolve().parent

# Source Data / Staging Project (Project A: GCS + BigQuery)
DEFAULT_SOURCE_PROJECT = os.environ.get("SOURCE_GCP_PROJECT", "prj-7efd-everydai-dev-4f9a")
DEFAULT_GCS_BUCKET = os.environ.get("WIKI_GCS_BUCKET", "gemini-enterprise-wiki-staging-20261013-7efd")
DEFAULT_GCS_PREFIX = os.environ.get("WIKI_GCS_PREFIX", "wiki/vertex")
DEFAULT_BQ_LOCATION = os.environ.get("BQ_LOCATION", "EU")
DEFAULT_BQ_SOURCE_DATASET = os.environ.get("BQ_SOURCE_DATASET", "wiki_raw_internal")
DEFAULT_BQ_VIEW_DATASET = os.environ.get("BQ_AUTHORIZED_VIEW_DATASET", "wiki_vertex_authorized")

# Target Vertex AI Search / Gemini Enterprise Project (Project B)
DEFAULT_VERTEX_PROJECT = os.environ.get("TARGET_VERTEX_PROJECT", DEFAULT_SOURCE_PROJECT)
DEFAULT_VERTEX_PROJECT_NUMBER = os.environ.get("TARGET_VERTEX_PROJECT_NUMBER", "")
DEFAULT_VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION", "eu")

DEFAULT_RAW_DATASTORE_ID = os.environ.get("VERTEX_RAW_DATASTORE_ID", "gemini-enterprise-wiki-raw")
DEFAULT_CHUNKED_DATASTORE_ID = os.environ.get("VERTEX_CHUNKED_DATASTORE_ID", "gemini-enterprise-wiki-chunked")

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
) -> List[PipelineOptionSpec]:
    prefix = gcs_prefix.strip("/")
    return [
        PipelineOptionSpec(
            key="raw",
            label="Option 1 (Raw Wikitext + Templates + Categories)",
            local_bq_jsonl=ROOT / "wikimedia-data" / "jsonl-for-vertex" / "rawapi" / "raw_with_templates_bq.jsonl",
            local_gcs_doc_jsonl=ROOT / "wikimedia-data" / "jsonl-for-vertex" / "rawapi" / "rawapi-for-vertexai.jsonl",
            gcs_bq_blob=f"{prefix}/option1_raw/raw_with_templates_bq.jsonl",
            gcs_doc_blob=f"{prefix}/option1_raw/rawapi-for-vertexai.jsonl",
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
            gcs_bq_blob=f"{prefix}/option2_chunked/chunks_bq.jsonl",
            gcs_doc_blob=f"{prefix}/option2_chunked/chunks-for-vertexai.jsonl",
            bq_source_table="wiki_option2_manual_chunks",
            bq_authorized_view="v_wiki_option2_manual_chunks",
            bq_export_table="t_wiki_option2_manual_chunks_export",
            datastore_id=chunked_datastore_id,
            datastore_display_name="Airbus Wiki - Option 2 (Manual Chunks)",
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
        f"  id,\n"
        f"  doc_type,\n"
        f"  ns,\n"
        f"  title,\n"
        f"  content,\n"
        f"  page_id,\n"
        f"  page_title,\n"
        f"  section_title,\n"
        f"  chunk_type,\n"
        f"  categories,\n"
        f"  referenced_templates,\n"
        f"  aliases,\n"
        f"  is_redirect,\n"
        f"  redirect_target,\n"
        f"  uri,\n"
        f"  source_file,\n"
        f"  token_count,\n"
        f"  updated_at\n"
        f"FROM `{proj}.{ds}.{tbl}`\n"
        f"WHERE content IS NOT NULL AND LENGTH(TRIM(content)) > 0"
    )


def upload_option_files_to_gcs(
    source_project: str,
    bucket_name: str,
    specs: Sequence[PipelineOptionSpec],
    dry_run: bool = False,
) -> None:
    proj = validate_gcp_project_id(source_project)
    bucket_clean = validate_gcs_bucket(bucket_name)

    for spec in specs:
        for local_path, blob_name in [
            (spec.local_bq_jsonl, spec.gcs_bq_blob),
            (spec.local_gcs_doc_jsonl, spec.gcs_doc_blob),
        ]:
            if not local_path.exists():
                raise FileNotFoundError(f"Missing local export file: {local_path}")
            gcs_uri = f"gs://{bucket_clean}/{blob_name}"
            if dry_run:
                print(f"[DRY-RUN] Would upload {local_path} ({local_path.stat().st_size:,} bytes) -> {gcs_uri}")
                continue

            from google.cloud import storage

            client = storage.Client(project=proj)
            blob = client.bucket(bucket_clean).blob(blob_name)
            blob.upload_from_filename(str(local_path), content_type="application/jsonl; charset=utf-8")
            print(f"Uploaded {local_path.name} -> {gcs_uri}")


def setup_bigquery_tables_and_authorized_views(
    source_project: str,
    bucket_name: str,
    bq_location: str,
    source_dataset_id: str,
    view_dataset_id: str,
    specs: Sequence[PipelineOptionSpec],
    materialize_export_table: bool = False,
    dry_run: bool = False,
) -> None:
    proj = validate_gcp_project_id(source_project)
    bucket_clean = validate_gcs_bucket(bucket_name)
    src_ds_name = validate_bq_identifier(source_dataset_id)
    view_ds_name = validate_bq_identifier(view_dataset_id)

    for spec in specs:
        src_tbl = validate_bq_identifier(spec.bq_source_table)
        view_name = validate_bq_identifier(spec.bq_authorized_view)
        exp_tbl = validate_bq_identifier(spec.bq_export_table)
        gcs_bq_uri = f"gs://{bucket_clean}/{spec.gcs_bq_blob}"
        view_sql = build_authorized_view_sql(proj, src_ds_name, src_tbl)

        if dry_run:
            print(f"\n[DRY-RUN] BigQuery setup for {spec.label}:")
            print(f"  1. Load native table `{proj}.{src_ds_name}.{src_tbl}` from {gcs_bq_uri}")
            print(f"  2. Create & authorize view `{proj}.{view_ds_name}.{view_name}`:\n{view_sql}")
            if materialize_export_table:
                print(
                    f"  3. Materialize physical export table `{proj}.{view_ds_name}.{exp_tbl}` "
                    f"(recommended fallback if Discovery Engine BigQuerySource rejects logical views)"
                )
            continue

        from google.cloud import bigquery

        bq = bigquery.Client(project=proj, location=bq_location)

        # 1. Ensure both datasets exist
        for ds_id in (src_ds_name, view_ds_name):
            ds_obj = bigquery.Dataset(f"{proj}.{ds_id}")
            ds_obj.location = bq_location
            bq.create_dataset(ds_obj, exists_ok=True)

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
        print(f"Loaded {loaded_table.num_rows} rows into native table `{src_table_ref}`")

        # 3. Create or update the View in the separate shared dataset
        view_ref_str = f"{proj}.{view_ds_name}.{view_name}"
        view_obj = bigquery.Table(view_ref_str)
        view_obj.view_query = view_sql
        try:
            bq.delete_table(view_ref_str, not_found_ok=True)
        except Exception:
            pass
        created_view = bq.create_table(view_obj)
        print(f"Created view `{view_ref_str}`")

        # 4. Authorize the View against the restricted source dataset
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
            print(f"Authorized view `{view_ref_str}` on source dataset `{proj}.{src_ds_name}`")

        # 5. Optional materialized physical table in shared dataset (for Discovery Engine bulk extract compatibility)
        if materialize_export_table:
            exp_ref_str = f"{proj}.{view_ds_name}.{exp_tbl}"
            ctas_sql = (
                f"CREATE OR REPLACE TABLE `{proj}.{view_ds_name}.{exp_tbl}` AS\n"
                f"SELECT * FROM `{proj}.{view_ds_name}.{view_name}`"
            )
            bq.query(ctas_sql).result()
            print(f"Materialized physical export table `{exp_ref_str}`")


def _get_discovery_client_options(location: str):
    from google.api_core.client_options import ClientOptions

    if location and location != "global":
        return ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
    return None


def ensure_datastore(
    vertex_project: str,
    vertex_location: str,
    datastore_id: str,
    display_name: str,
) -> str:
    from google.cloud import discoveryengine_v1 as v1

    client = v1.DataStoreServiceClient(client_options=_get_discovery_client_options(vertex_location))
    parent = f"projects/{vertex_project}/locations/{vertex_location}/collections/default_collection"
    datastore_name = f"{parent}/dataStores/{datastore_id}"

    try:
        client.get_data_store(name=datastore_name)
        print(f"Datastore already exists: {datastore_name}")
        return datastore_name
    except Exception:
        pass

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
    print(f"Created datastore: {result.name}")
    return result.name


def import_into_vertex_datastores(
    source_project: str,
    bucket_name: str,
    view_dataset_id: str,
    vertex_project: str,
    vertex_location: str,
    specs: Sequence[PipelineOptionSpec],
    import_source: str = "bq",
    use_materialized_table: bool = False,
    dry_run: bool = False,
) -> None:
    src_proj = validate_gcp_project_id(source_project)
    dst_proj = validate_gcp_project_id(vertex_project)
    bucket_clean = validate_gcs_bucket(bucket_name)
    view_ds_name = validate_bq_identifier(view_dataset_id)

    for spec in specs:
        bq_target_obj = validate_bq_identifier(
            spec.bq_export_table if use_materialized_table else spec.bq_authorized_view
        )
        gcs_doc_uri = f"gs://{bucket_clean}/{spec.gcs_doc_blob}"

        if dry_run:
            print(f"\n[DRY-RUN] Vertex AI Search import for {spec.label}:")
            print(
                f"  Target datastore: projects/{dst_proj}/locations/{vertex_location}"
                f"/collections/default_collection/dataStores/{spec.datastore_id}"
            )
            if import_source == "bq":
                print(
                    f"  Source: BigQuerySource(project_id={src_proj!r}, dataset_id={view_ds_name!r}, "
                    f"table_id={bq_target_obj!r}, data_schema='custom'), id_field='id', mode=FULL"
                )
            else:
                print(f"  Source: GcsSource(input_uris=[{gcs_doc_uri!r}], data_schema='document'), mode=FULL")
            continue

        from google.cloud import discoveryengine_v1 as v1

        ensure_datastore(
            vertex_project=dst_proj,
            vertex_location=vertex_location,
            datastore_id=spec.datastore_id,
            display_name=spec.datastore_display_name,
        )
        doc_client = v1.DocumentServiceClient(client_options=_get_discovery_client_options(vertex_location))
        branch_parent = doc_client.branch_path(
            project=dst_proj,
            location=vertex_location,
            data_store=spec.datastore_id,
            branch="0",
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
                reconciliation_mode=v1.ImportDocumentsRequest.ReconciliationMode.FULL,
            )
        else:
            req = v1.ImportDocumentsRequest(
                parent=branch_parent,
                gcs_source=v1.GcsSource(input_uris=[gcs_doc_uri], data_schema="document"),
                reconciliation_mode=v1.ImportDocumentsRequest.ReconciliationMode.FULL,
            )

        operation = doc_client.import_documents(request=req)
        print(f"Started Vertex import operation for {spec.datastore_id}: {operation.operation.name}")
        result = operation.result(timeout=1800)
        print(f"Completed Vertex import for {spec.datastore_id}: {result}")


def print_cross_project_iam_hint(
    source_project: str,
    view_dataset_id: str,
    target_vertex_project_number: str,
) -> None:
    proj_num = target_vertex_project_number or "<TARGET_VERTEX_PROJECT_NUMBER>"
    sa = f"service-{proj_num}@gcp-sa-discoveryengine.iam.gserviceaccount.com"
    print("\nCross-Project IAM Checklist (for Project B reading from Project A BigQuery):")
    print(f"  Discovery Engine Service Agent: {sa}")
    print(f"  1. On dataset `{source_project}.{view_dataset_id}` -> grant `roles/bigquery.dataViewer`")
    print(f"  2. On project `{source_project}` -> grant `roles/bigquery.jobUser`")
    print(
        "  (Note: If Discovery Engine bulk import rejects the logical Authorized View, re-run with "
        "`--materialize-export-table` to populate physical export tables in the same shared dataset.)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload Option 1 & Option 2 JSONLs to GCS, build BigQuery Authorized Views, and import into Vertex Search."
    )
    parser.add_argument(
        "--options",
        nargs="+",
        choices=["raw", "chunked"],
        default=["raw", "chunked"],
        help="Which pipeline dataset(s) to deploy (default: both raw and chunked).",
    )
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=["upload-gcs", "setup-bq", "import-vertex"],
        default=["upload-gcs", "setup-bq", "import-vertex"],
        help="Pipeline stages to run.",
    )
    parser.add_argument(
        "--import-source",
        choices=["bq", "gcs"],
        default="bq",
        help="Source for Vertex AI Search datastore import (`bq` or `gcs`).",
    )
    parser.add_argument(
        "--materialize-export-table",
        action="store_true",
        help="Also materialize physical export tables in the shared BigQuery dataset and import from them.",
    )
    parser.add_argument("--source-project", default=DEFAULT_SOURCE_PROJECT)
    parser.add_argument("--gcs-bucket", default=DEFAULT_GCS_BUCKET)
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--bq-location", default=DEFAULT_BQ_LOCATION)
    parser.add_argument("--bq-source-dataset", default=DEFAULT_BQ_SOURCE_DATASET)
    parser.add_argument("--bq-view-dataset", default=DEFAULT_BQ_VIEW_DATASET)
    parser.add_argument("--vertex-project", default=DEFAULT_VERTEX_PROJECT)
    parser.add_argument("--vertex-project-number", default=DEFAULT_VERTEX_PROJECT_NUMBER)
    parser.add_argument("--vertex-location", default=DEFAULT_VERTEX_LOCATION)
    parser.add_argument("--raw-datastore-id", default=DEFAULT_RAW_DATASTORE_ID)
    parser.add_argument("--chunked-datastore-id", default=DEFAULT_CHUNKED_DATASTORE_ID)
    parser.add_argument("--dry-run", action="store_true", help="Validate files and print planned operations only.")
    args = parser.parse_args()

    all_specs = build_option_specs(
        gcs_prefix=args.gcs_prefix,
        raw_datastore_id=args.raw_datastore_id,
        chunked_datastore_id=args.chunked_datastore_id,
    )
    selected_specs = [s for s in all_specs if s.key in set(args.options)]

    if "upload-gcs" in args.steps:
        upload_option_files_to_gcs(
            source_project=args.source_project,
            bucket_name=args.gcs_bucket,
            specs=selected_specs,
            dry_run=args.dry_run,
        )

    if "setup-bq" in args.steps:
        setup_bigquery_tables_and_authorized_views(
            source_project=args.source_project,
            bucket_name=args.gcs_bucket,
            bq_location=args.bq_location,
            source_dataset_id=args.bq_source_dataset,
            view_dataset_id=args.bq_view_dataset,
            specs=selected_specs,
            materialize_export_table=args.materialize_export_table,
            dry_run=args.dry_run,
        )
        print_cross_project_iam_hint(
            source_project=args.source_project,
            view_dataset_id=args.bq_view_dataset,
            target_vertex_project_number=args.vertex_project_number,
        )

    if "import-vertex" in args.steps:
        import_into_vertex_datastores(
            source_project=args.source_project,
            bucket_name=args.gcs_bucket,
            view_dataset_id=args.bq_view_dataset,
            vertex_project=args.vertex_project,
            vertex_location=args.vertex_location,
            specs=selected_specs,
            import_source=args.import_source,
            use_materialized_table=args.materialize_export_table,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
