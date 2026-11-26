"""Cloud Run Job & Local Orchestrator for the Airbus MediaWiki → GCS → BigQuery → Vertex AI Search Pipeline.

Executes all stages in-process in Python (sharing loaded MediaWiki pages, templates, and chunks in memory
to avoid duplicate disk I/O and child Python subprocess spawns) while keeping scripts `0_`–`5_` standalone.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from wiki_utils import (
    DEFAULT_API_URL,
    create_mediawiki_session,
    get_logger,
    load_all_raw_pages,
    load_all_template_records,
    load_template_registry,
    maybe_login_mediawiki,
    mw_api_request,
)

# Import numbered pipeline stages cleanly in-process (zero subprocess spawns or CLI flag duplication)
_step0 = importlib.import_module("0_mediawiki_download")
_step1 = importlib.import_module("1_fetch_templates")
_step1b = importlib.import_module("1b_build_raw_with_templates_jsonl")
_step2 = importlib.import_module("2_clean_wikitext")
_step3 = importlib.import_module("3_chunk_pages")
_step4 = importlib.import_module("4_generate_vertex_jsonl")
_step5 = importlib.import_module("5_vertex_import_jsonl")

logger = get_logger("run_pipeline")
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "wikimedia-data"
STATE_BLOB_PATH = "_state/last_sync.json"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_list(name: str, default: List[str]) -> List[str]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return list(default)
    return [part.strip() for part in raw.replace(",", " ").split() if part.strip()]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _load_sync_state(source_project: str, bucket_name: str, dry_run: bool = False) -> Dict[str, Any]:
    if dry_run:
        return {}
    try:
        from google.cloud import storage

        client = storage.Client(project=source_project)
        blob = client.bucket(bucket_name).blob(STATE_BLOB_PATH)
        if not blob.exists():
            return {}
        return json.loads(blob.download_as_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("Could not read prior sync state gs://%s/%s (%s); assuming first run.", bucket_name, STATE_BLOB_PATH, exc)
        return {}


def _save_sync_state(source_project: str, bucket_name: str, state: Dict[str, Any], dry_run: bool = False) -> None:
    if dry_run:
        logger.info("[DRY-RUN] Would persist sync state to gs://%s/%s: %s", bucket_name, STATE_BLOB_PATH, json.dumps(state))
        return
    from google.cloud import storage

    client = storage.Client(project=source_project)
    blob = client.bucket(bucket_name).blob(STATE_BLOB_PATH)
    blob.upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2),
        content_type="application/json; charset=utf-8",
    )
    logger.info("Saved sync state to gs://%s/%s", bucket_name, STATE_BLOB_PATH)


def has_mediawiki_changes_since(
    api_url: str,
    since_iso_utc: str,
    bypass_proxy: bool = False,
    cookie_file: Optional[str] = None,
) -> Tuple[bool, str]:
    """Check lightweight MediaWiki `recentchanges` and delete/move `logevents` since `since_iso_utc`."""
    session = create_mediawiki_session(bypass_proxy=bypass_proxy, cookie_file=cookie_file)
    maybe_login_mediawiki(session, api_url, timeout=30)

    rc_data = mw_api_request(
        session,
        api_url,
        params={
            "action": "query",
            "list": "recentchanges",
            "rcend": since_iso_utc,
            "rclimit": "5",
            "rcprop": "title|timestamp|type",
            "format": "json",
        },
        method="GET",
        timeout=30,
        context="MediaWiki recentchanges check",
    )
    rc_items = rc_data.get("query", {}).get("recentchanges", [])
    if rc_items:
        return True, f"detected {len(rc_items)}+ recent change(s) since {since_iso_utc}"

    for log_type in ("delete", "move"):
        le_data = mw_api_request(
            session,
            api_url,
            params={
                "action": "query",
                "list": "logevents",
                "letype": log_type,
                "leend": since_iso_utc,
                "lelimit": "5",
                "format": "json",
            },
            method="GET",
            timeout=30,
            context=f"MediaWiki {log_type} logevents check",
        )
        le_items = le_data.get("query", {}).get("logevents", [])
        if le_items:
            return True, f"detected {len(le_items)}+ `{log_type}` event(s) since {since_iso_utc}"

    return False, f"0 edits/deletes/moves since {since_iso_utc}"


def verify_drop_guard(
    prev_state: Dict[str, Any],
    min_page_floor: int = 500,
    min_page_ratio: float = 0.85,
) -> Dict[str, Any]:
    """Validate newly built Option 1 & Option 2 JSONLs before overwriting GCS/BigQuery/Vertex."""
    opt1_bq = DATA_DIR / "jsonl-for-vertex" / "rawapi" / "raw_with_templates_bq.jsonl"
    opt2_bq = DATA_DIR / "jsonl-for-vertex" / "chunks" / "chunks_bq.jsonl"

    if not opt1_bq.is_file() or not opt2_bq.is_file():
        raise RuntimeError(f"Expected generated JSONL files missing: {opt1_bq}, {opt2_bq}")

    opt1_pages = 0
    opt1_templates = 0
    with opt1_bq.open("r", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if row.get("doc_type") == "raw_page":
                opt1_pages += 1
            elif row.get("doc_type") == "raw_template":
                opt1_templates += 1

    opt2_chunks = 0
    opt2_pids = set()
    with opt2_bq.open("r", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            opt2_chunks += 1
            opt2_pids.add(row.get("page_id"))

    if opt1_pages < min_page_floor:
        raise RuntimeError(
            f"Drop-guard triggered: crawled only {opt1_pages} pages (minimum floor={min_page_floor}). "
            "Aborting BEFORE modifying GCS/BigQuery/Vertex."
        )

    prev_pages = int(prev_state.get("page_count") or 0)
    if prev_pages > 0:
        min_expected = int(prev_pages * min_page_ratio)
        if opt1_pages < min_expected:
            raise RuntimeError(
                f"Drop-guard triggered: page count dropped from {prev_pages} -> {opt1_pages} "
                f"(< {min_page_ratio:.0%} threshold of {min_expected}). Aborting BEFORE cloud writes."
            )

    if opt2_chunks == 0:
        raise RuntimeError("Drop-guard triggered: Option 2 generated 0 chunks. Aborting BEFORE cloud writes.")

    metrics = {
        "page_count": opt1_pages,
        "template_count": opt1_templates,
        "option1_records": opt1_pages + opt1_templates,
        "option2_chunks": opt2_chunks,
        "option2_unique_pages": len(opt2_pids),
        "option1_sha256": _sha256_file(opt1_bq),
        "option2_sha256": _sha256_file(opt2_bq),
    }
    logger.info(
        "Drop-guard PASSED: pages=%d, templates=%d, option2_chunks=%d (unique_pages=%d)",
        opt1_pages,
        opt1_templates,
        opt2_chunks,
        len(opt2_pids),
    )
    return metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cloud Run Job & local orchestrator for Airbus MediaWiki -> GCS -> BigQuery -> Vertex AI Search."
    )
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        default=_env_bool("SKIP_CRAWL", False),
        help="Skip Steps 0-4 (MediaWiki crawl & local build) and run Step 5 directly on existing GCS JSONLs.",
    )
    parser.add_argument(
        "--force-sync",
        action="store_true",
        default=_env_bool("FORCE_SYNC", False),
        help="Bypass the lightweight MediaWiki recentchanges check and always run a full crawl/build.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=_env_bool("DRY_RUN", False),
        help="Preview planned cloud operations without modifying GCS, BigQuery, or Vertex AI Search.",
    )
    parser.add_argument(
        "--options",
        nargs="+",
        choices=["raw", "chunked"],
        default=_env_list("PIPELINE_OPTIONS", ["raw", "chunked"]),
        help="Which option datasets to deploy (`raw`, `chunked`, or both).",
    )
    parser.add_argument(
        "--cloud-steps",
        nargs="+",
        choices=["upload-gcs", "setup-bq", "import-vertex"],
        default=_env_list("CLOUD_STEPS", []),
        help="Override Step 5 cloud actions.",
    )
    parser.add_argument(
        "--run-vertex-import",
        action="store_true",
        default=_env_bool("RUN_VERTEX_IMPORT", False),
        help="Append `import-vertex` to the default Step 5 cloud steps.",
    )
    parser.add_argument(
        "--materialize-export-table",
        action="store_true",
        default=_env_bool("MATERIALIZE_EXPORT_TABLE", True),
        help="Materialize physical BigQuery export tables alongside Authorized Views (default: True).",
    )
    parser.add_argument(
        "--no-materialize-export-table",
        dest="materialize_export_table",
        action="store_false",
        help="Disable physical BigQuery export table materialization.",
    )
    parser.add_argument(
        "--bypass-proxy",
        action="store_true",
        default=_env_bool("MEDIAWIKI_BYPASS_PROXY", False),
    )
    parser.add_argument(
        "--cookie-file",
        default=os.environ.get("MEDIAWIKI_COOKIE_FILE", "") or None,
    )
    parser.add_argument(
        "--min-page-floor",
        type=int,
        default=int(os.environ.get("MIN_PAGE_FLOOR", "500")),
    )
    parser.add_argument(
        "--min-page-ratio",
        type=float,
        default=float(os.environ.get("MIN_PAGE_RATIO", "0.85")),
    )
    _step5.add_common_cloud_cli_args(parser)
    parser.set_defaults(import_source=os.environ.get("IMPORT_SOURCE", "bq"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_started_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    api_url = DEFAULT_API_URL

    if args.cloud_steps:
        steps = list(args.cloud_steps)
    else:
        steps = ["setup-bq"] if args.skip_crawl else ["upload-gcs", "setup-bq"]
        if args.run_vertex_import and "import-vertex" not in steps:
            steps.append("import-vertex")

    if args.skip_crawl and "upload-gcs" in steps:
        raise ValueError("`--skip-crawl` cannot be combined with `upload-gcs` (no local crawl files exist in container).")

    logger.info(
        "MediaWiki Pipeline Start (%s): mode=%s, options=%s, cloud_steps=%s, gcs_project=%s (gs://%s/%s), "
        "bq=%s.%s->%s (materialize=%s), vertex=%s (%s), dry_run=%s",
        run_started_utc,
        "skip-crawl" if args.skip_crawl else "full-sync",
        args.options,
        steps,
        args.source_project,
        args.gcs_bucket,
        args.gcs_prefix.strip("/"),
        args.bq_project,
        args.bq_source_dataset,
        args.bq_view_dataset,
        args.materialize_export_table,
        args.vertex_project,
        args.vertex_location,
        args.dry_run,
    )

    prev_state = _load_sync_state(args.source_project, args.gcs_bucket, dry_run=args.dry_run)
    crawl_metrics: Dict[str, Any] = {}

    if not args.skip_crawl:
        last_ts = prev_state.get("last_sync_timestamp")
        if last_ts and not args.force_sync:
            changed, reason = has_mediawiki_changes_since(
                api_url=api_url,
                since_iso_utc=str(last_ts),
                bypass_proxy=args.bypass_proxy,
                cookie_file=args.cookie_file,
            )
            logger.info("Change check: %s", reason)
            if not changed:
                logger.info("No source changes detected since last successful sync; exiting cleanly.")
                return

        # 1. Crawl all accessible MediaWiki namespaces + Namespace-10 templates sharing one session
        shared_session = create_mediawiki_session(bypass_proxy=args.bypass_proxy, cookie_file=args.cookie_file)
        maybe_login_mediawiki(shared_session, api_url, timeout=30)
        _step0.fetch_wiki_batches(
            api_url=api_url,
            bypass_proxy=args.bypass_proxy,
            cookie_file=args.cookie_file,
            session=shared_session,
        )
        _step1.fetch_all_templates(
            api_url=api_url,
            bypass_proxy=args.bypass_proxy,
            cookie_file=args.cookie_file,
            session=shared_session,
        )

        # 2. Load templates & raw pages ONCE in memory and build Option 1 & Option 2
        need_gcs_doc = args.import_source == "gcs" or args.upload_all_variants
        all_templates = load_all_template_records(DATA_DIR / "templates")
        template_registry = load_template_registry(records=all_templates)
        pages = load_all_raw_pages(DATA_DIR / "raw_api", template_registry=template_registry)

        _step1b.build_option1_raw_datasets(
            all_templates=all_templates,
            template_registry=template_registry,
            pages=pages,
            write_gcs_doc_jsonl=need_gcs_doc,
        )
        _step2.process_raw_wikitext(template_registry=template_registry, pages=pages)
        chunks = _step3.process_chunks()
        _step4.generate_option2_jsonl(chunks=chunks, write_gcs_doc_jsonl=need_gcs_doc)

        # 3. Drop-guard verification BEFORE any GCS overwrite / BigQuery truncate / Vertex FULL import
        crawl_metrics = verify_drop_guard(
            prev_state=prev_state,
            min_page_floor=args.min_page_floor,
            min_page_ratio=args.min_page_ratio,
        )

        if (
            not args.force_sync
            and prev_state.get("option1_sha256") == crawl_metrics["option1_sha256"]
            and prev_state.get("option2_sha256") == crawl_metrics["option2_sha256"]
        ):
            logger.info("Generated Option 1 & Option 2 JSONLs are byte-identical to previous sync; skipping cloud import.")
            return

    # 4. Run Step 5 in-process
    _step5.run_cloud_deployment(
        options=args.options,
        steps=steps,
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
        import_source=args.import_source,
        materialize_export_table=args.materialize_export_table,
        upload_all_variants=args.upload_all_variants,
        dry_run=args.dry_run,
    )

    if not args.skip_crawl and crawl_metrics:
        new_state = {
            **crawl_metrics,
            "last_sync_timestamp": run_started_utc,
            "options_deployed": args.options,
            "cloud_steps_executed": steps,
        }
        _save_sync_state(args.source_project, args.gcs_bucket, new_state, dry_run=args.dry_run)

    logger.info("MediaWiki Pipeline Completed Successfully.")


if __name__ == "__main__":
    main()
