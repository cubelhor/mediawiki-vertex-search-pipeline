"""Single-command orchestrator for the unified MediaWiki `action=parse` HTML pipeline (Option 1 GCS + Option 2 BQ).

Runs in-process across 3 clean stages:
  1. `crawl_parse_html.py`   — Pass 1 (`action=query` metadata + redirects) + Pass 2 (`action=parse` rendered HTML)
                               -> `pages/*.html`, `metadata.jsonl`, `metadata_incremental.jsonl` (Option 1 ready)
  2. `html_to_bq_chunks.py`  — Clean stdlib HTML -> Markdown section chunks + `#section` anchor URIs
                               -> `chunks_bq.jsonl` (Option 2 ready)
  3. `5_vertex_import_jsonl.py` — Deploys:
                               • `html_native`  -> GCS + Vertex `CONTENT_REQUIRED` + Layout Parser + 500-token chunking
                               • `html_chunked` -> GCS + BigQuery Authorized View + Vertex `NO_CONTENT`
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import crawl_parse_html
import html_to_bq_chunks
from wiki_utils import DEFAULT_API_URL, add_common_mediawiki_cli_args, get_logger

_step5 = importlib.import_module("5_vertex_import_jsonl")

logger = get_logger("run_parse_pipeline")
ROOT = Path(__file__).resolve().parent
DEFAULT_PARSED_DIR = ROOT / "wikimedia-data" / "parsed_html"


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


def verify_parse_outputs(
    parsed_dir: Path,
    selected_options: List[str],
    min_page_floor: int = 500,
    is_partial_test: bool = False,
) -> Dict[str, Any]:
    """Validate Option 1 HTML manifests & Option 2 BQ chunks before any cloud writes."""
    manifest_path = parsed_dir / "pages_manifest.json"
    meta_path = parsed_dir / "metadata.jsonl"
    chunks_path = parsed_dir / "chunks_bq.jsonl"

    if not manifest_path.is_file() or not meta_path.is_file():
        raise RuntimeError(f"Expected parsed HTML outputs missing in {parsed_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    page_count = int(manifest.get("total_pages") or 0)
    if page_count == 0:
        raise RuntimeError("Drop-guard triggered: 0 parsed HTML pages available. Aborting BEFORE cloud writes.")

    if not is_partial_test and page_count < min_page_floor:
        raise RuntimeError(
            f"Drop-guard triggered: crawled only {page_count} pages (minimum floor={min_page_floor}). "
            "Pass --min-page-floor or --limit/--page-ids for partial smoke tests."
        )

    chunk_count = 0
    if "html_chunked" in selected_options:
        if not chunks_path.is_file():
            raise RuntimeError(f"Missing Option 2 HTML chunks JSONL: {chunks_path}")
        with chunks_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    chunk_count += 1
        if chunk_count == 0:
            raise RuntimeError("Drop-guard triggered: Option 2 HTML chunker produced 0 chunks.")

    metrics = {
        "page_count": page_count,
        "parsed_count": int(manifest.get("parsed_count") or 0),
        "reused_count": int(manifest.get("reused_count") or 0),
        "failed_count": len(manifest.get("failed_ids") or []),
        "html_chunk_count": chunk_count,
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    logger.info(
        "Drop-guard PASSED: pages=%d (newly_parsed=%d, cached=%d, failed=%d), option2_html_chunks=%d",
        metrics["page_count"],
        metrics["parsed_count"],
        metrics["reused_count"],
        metrics["failed_count"],
        metrics["html_chunk_count"],
    )
    return metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified MediaWiki `action=parse` HTML pipeline for Option 1 (GCS Layout Parser) and Option 2 (BigQuery Chunks)."
    )
    add_common_mediawiki_cli_args(parser)
    _step5.add_common_cloud_cli_args(parser)

    parser.add_argument("--parsed-dir", type=Path, default=DEFAULT_PARSED_DIR)
    parser.add_argument(
        "--options",
        nargs="+",
        choices=["html_native", "html_chunked"],
        default=_env_list("PARSE_PIPELINE_OPTIONS", ["html_native", "html_chunked"]),
        help="Which action=parse option(s) to build and deploy (default: both html_native and html_chunked).",
    )
    parser.add_argument(
        "--cloud-steps",
        nargs="+",
        choices=["upload-gcs", "setup-bq", "import-vertex"],
        default=_env_list("CLOUD_STEPS", []),
        help="Explicit Step 3 cloud actions. Defaults to upload-gcs + setup-bq (+ import-vertex when --run-vertex-import).",
    )
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        default=_env_bool("SKIP_CRAWL", False),
        help="Skip MediaWiki crawl and reuse existing local `parsed_html/` files.",
    )
    parser.add_argument(
        "--skip-cloud",
        action="store_true",
        default=_env_bool("SKIP_CLOUD", False),
        help="Run local crawl + HTML chunking only without touching GCS, BigQuery, or Vertex AI Search.",
    )
    parser.add_argument(
        "--run-vertex-import",
        action="store_true",
        default=_env_bool("RUN_VERTEX_IMPORT", False),
        help="Also create/import Vertex AI Search datastores in target consumer project.",
    )
    parser.add_argument(
        "--materialize-export-table",
        action="store_true",
        default=_env_bool("MATERIALIZE_EXPORT_TABLE", False),
        help="Materialize physical BigQuery export table for Option 2 (`html_chunked`).",
    )
    parser.add_argument(
        "--page-ids",
        nargs="+",
        default=None,
        help="Optional page IDs for fast smoke tests (e.g. --page-ids 3687,8159).",
    )
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="MediaWiki api.php HTTPS URL (default: %(default)s).")
    parser.add_argument("--limit", type=int, default=None, help="Optional max pages to crawl for smoke testing.")
    parser.add_argument("--workers", type=int, default=int(os.environ.get("PARSE_WORKERS", "4")))
    parser.add_argument("--force-reparse", action="store_true", default=_env_bool("FORCE_REPARSE", False))
    parser.add_argument("--max-tokens", type=int, default=int(os.environ.get("CHUNK_MAX_TOKENS", "450")))
    parser.add_argument("--overlap-tokens", type=int, default=int(os.environ.get("CHUNK_OVERLAP_TOKENS", "50")))
    parser.add_argument("--min-page-floor", type=int, default=int(os.environ.get("MIN_PAGE_FLOOR", "500")))
    parser.add_argument("--dry-run", action="store_true", default=_env_bool("DRY_RUN", False))
    return parser


def main(argv: Optional[List[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    page_ids = crawl_parse_html._parse_page_ids_arg(args.page_ids)
    is_partial_test = bool(page_ids or args.limit)

    if not args.skip_crawl:
        crawl_parse_html.crawl_parsed_html_wiki(
            output_dir=args.parsed_dir,
            api_url=args.api_url,
            gcs_bucket=args.gcs_bucket,
            gcs_prefix=args.gcs_prefix,
            page_ids=page_ids,
            limit=args.limit,
            workers=args.workers,
            maxlag=args.maxlag,
            force_reparse=args.force_reparse,
            bypass_proxy=args.bypass_proxy,
            cookie_file=args.cookie_file,
        )

    if "html_chunked" in args.options:
        html_to_bq_chunks.build_html_chunks_dataset(
            parsed_dir=args.parsed_dir,
            max_tokens=args.max_tokens,
            overlap_tokens=args.overlap_tokens,
        )

    verify_parse_outputs(
        parsed_dir=args.parsed_dir,
        selected_options=args.options,
        min_page_floor=args.min_page_floor,
        is_partial_test=is_partial_test,
    )

    if args.skip_cloud:
        logger.info("--skip-cloud enabled; local action=parse build complete.")
        return

    steps = list(args.cloud_steps) if args.cloud_steps else ["upload-gcs", "setup-bq"]
    if args.run_vertex_import and "import-vertex" not in steps:
        steps.append("import-vertex")

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
        html_native_datastore_id=args.html_native_datastore_id,
        html_chunked_datastore_id=args.html_chunked_datastore_id,
        parsed_html_dir=args.parsed_dir,
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
