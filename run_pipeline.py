"""Cloud Run Job & Local Orchestrator for the Airbus MediaWiki → GCS → BigQuery → Vertex AI Search Pipeline.

Keeps all existing scripts (`0_` through `5_`) 100% standalone for local testing while adding a clean,
container-ready entrypoint supporting both CLI flags and Cloud Run environment variables.

Operating Modes:
1. `--skip-crawl` (or `SKIP_CRAWL=true`) — Recommended BEFORE the OpenWiki firewall opens:
   Skips MediaWiki crawling (`0_`, `1_`) and local JSONL generation (`1b_`–`4_`), and runs
   `5_vertex_import_jsonl.py` directly against the already-uploaded JSONLs in GCS
   (e.g., `gs://mediawiki-data-nov-25/option1_raw/` & `option2_chunked/`).
2. Full Daily Sync (default once the OpenWiki firewall opens):
   - Fast quiet-day check via MediaWiki `list=recentchanges` + `list=logevents` against
     `gs://<bucket>/_state/last_sync.json` (exits 0 in ~2s if nothing changed, unless `--force-sync`).
   - Runs `0_mediawiki_download.py` -> `1_fetch_templates.py` -> `1b_build_raw_with_templates_jsonl.py`
     -> `2_clean_wikitext.py` -> `3_chunk_pages.py` -> `4_generate_vertex_jsonl.py`.
   - Enforces a Drop-Guard (`>= 85%` of previous page count and `>= MIN_PAGE_FLOOR`) BEFORE any
     GCS overwrite, BigQuery truncate, or Vertex AI Search `FULL` reconciliation!
   - Runs `5_vertex_import_jsonl.py` and saves updated sync state to GCS on full success.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

from wiki_utils import (
    create_mediawiki_session,
    maybe_login_mediawiki,
    mw_api_request,
)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "wikimedia-data"
STATE_BLOB_PATH = "_state/last_sync.json"

# Hardcoded allow-list of local pipeline scripts (never executed via shell=True)
_ALLOWED_SCRIPTS = {
    "0_mediawiki_download.py",
    "1_fetch_templates.py",
    "1b_build_raw_with_templates_jsonl.py",
    "2_clean_wikitext.py",
    "3_chunk_pages.py",
    "4_generate_vertex_jsonl.py",
    "5_vertex_import_jsonl.py",
}


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


def _run_stage_script(script_name: str, extra_args: Optional[List[str]] = None) -> None:
    """Execute an allow-listed local pipeline script safely without shell interpolation."""
    if script_name not in _ALLOWED_SCRIPTS:
        raise ValueError(f"Refusing to execute non-allowlisted script: {script_name!r}")
    script_path = (ROOT / script_name).resolve()
    if not script_path.is_relative_to(ROOT.resolve()) or not script_path.is_file():
        raise FileNotFoundError(f"Pipeline script not found: {script_path}")

    cmd = [sys.executable, str(script_path), *(extra_args or [])]
    print(f"\n>>> Running {script_name} {' '.join(extra_args or [])}".rstrip())
    subprocess.run(cmd, cwd=str(ROOT), check=True)


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
        print(f"  [INFO] Could not read prior sync state gs://{bucket_name}/{STATE_BLOB_PATH} ({exc}); assuming first run.")
        return {}


def _save_sync_state(source_project: str, bucket_name: str, state: Dict[str, Any], dry_run: bool = False) -> None:
    if dry_run:
        print(f"[DRY-RUN] Would persist sync state to gs://{bucket_name}/{STATE_BLOB_PATH}: {json.dumps(state)}")
        return
    from google.cloud import storage

    client = storage.Client(project=source_project)
    blob = client.bucket(bucket_name).blob(STATE_BLOB_PATH)
    blob.upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2),
        content_type="application/json; charset=utf-8",
    )
    print(f"Saved sync state to gs://{bucket_name}/{STATE_BLOB_PATH}")


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
    print(
        f"Drop-guard PASSED: pages={opt1_pages}, templates={opt1_templates}, "
        f"option2_chunks={opt2_chunks} (unique_pages={len(opt2_pids)})"
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
        help=(
            "Override Step 5 cloud actions. Default: `setup-bq` when `--skip-crawl` is set "
            "(add `import-vertex` when ready), or `upload-gcs setup-bq` in full crawl mode."
        ),
    )
    parser.add_argument(
        "--run-vertex-import",
        action="store_true",
        default=_env_bool("RUN_VERTEX_IMPORT", False),
        help="Convenience flag to append `import-vertex` to the default Step 5 cloud steps.",
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
        "--import-source",
        choices=["bq", "gcs"],
        default=os.environ.get("IMPORT_SOURCE", "bq"),
    )
    parser.add_argument(
        "--source-project",
        default=os.environ.get("SOURCE_GCP_PROJECT", "prj-c9ec-data-dom-dev-ec"),
    )
    parser.add_argument(
        "--gcs-bucket",
        default=os.environ.get("WIKI_GCS_BUCKET", "mediawiki-data-nov-25"),
    )
    parser.add_argument(
        "--gcs-prefix",
        default=os.environ.get("WIKI_GCS_PREFIX", ""),
    )
    parser.add_argument(
        "--bq-location",
        default=os.environ.get("BQ_LOCATION", "EU"),
    )
    parser.add_argument(
        "--bq-source-dataset",
        default=os.environ.get("BQ_SOURCE_DATASET", "wiki_raw_internal"),
    )
    parser.add_argument(
        "--bq-view-dataset",
        default=os.environ.get("BQ_AUTHORIZED_VIEW_DATASET", "wiki_vertex_authorized"),
    )
    parser.add_argument(
        "--vertex-project",
        default=os.environ.get("TARGET_VERTEX_PROJECT", "prj-7efd-everydai-dev-4f9a"),
    )
    parser.add_argument(
        "--vertex-project-number",
        default=os.environ.get("TARGET_VERTEX_PROJECT_NUMBER", ""),
    )
    parser.add_argument(
        "--vertex-location",
        default=os.environ.get("VERTEX_LOCATION", "eu"),
    )
    parser.add_argument(
        "--raw-datastore-id",
        default=os.environ.get("VERTEX_RAW_DATASTORE_ID", "gemini-enterprise-wiki-raw"),
    )
    parser.add_argument(
        "--chunked-datastore-id",
        default=os.environ.get("VERTEX_CHUNKED_DATASTORE_ID", "gemini-enterprise-wiki-chunked"),
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
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_started_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    api_url = os.environ.get("MEDIAWIKI_API_URL", "https://aow.eu.airbus.corp/api.php")

    # Resolve Step 5 cloud steps cleanly based on mode
    if args.cloud_steps:
        steps = list(args.cloud_steps)
    else:
        steps = ["setup-bq"] if args.skip_crawl else ["upload-gcs", "setup-bq"]
        if args.run_vertex_import and "import-vertex" not in steps:
            steps.append("import-vertex")

    if args.skip_crawl and "upload-gcs" in steps:
        raise ValueError("`--skip-crawl` cannot be combined with `upload-gcs` (no local crawl files exist in container).")

    print(
        f"=== MediaWiki Cloud Run Pipeline Start ({run_started_utc}) ===\n"
        f"  mode={'skip-crawl (GCS -> Cloud)' if args.skip_crawl else 'full-sync (MediaWiki -> GCS -> Cloud)'}\n"
        f"  options={args.options}\n"
        f"  cloud_steps={steps}\n"
        f"  source_project={args.source_project}, gcs=gs://{args.gcs_bucket}/{args.gcs_prefix.strip('/')}\n"
        f"  bq={args.bq_source_dataset} -> {args.bq_view_dataset} (materialize={args.materialize_export_table})\n"
        f"  vertex_project={args.vertex_project} ({args.vertex_location}), dry_run={args.dry_run}"
    )

    prev_state = _load_sync_state(args.source_project, args.gcs_bucket, dry_run=args.dry_run)
    crawl_metrics: Dict[str, Any] = {}

    if not args.skip_crawl:
        # 1. Fast daily quiet-day check via MediaWiki recentchanges + logevents
        last_ts = prev_state.get("last_sync_timestamp")
        if last_ts and not args.force_sync:
            changed, reason = has_mediawiki_changes_since(
                api_url=api_url,
                since_iso_utc=str(last_ts),
                bypass_proxy=args.bypass_proxy,
                cookie_file=args.cookie_file,
            )
            print(f"Change check: {reason}")
            if not changed:
                print("No source changes detected since last successful sync; skipping crawl/BQ/Vertex run.")
                return

        # 2. Crawl all accessible MediaWiki namespaces + Namespace-10 templates
        crawl_flags: List[str] = []
        if args.bypass_proxy:
            crawl_flags.append("--bypass-proxy")
        if args.cookie_file:
            crawl_flags.extend(["--cookie-file", args.cookie_file])

        _run_stage_script("0_mediawiki_download.py", crawl_flags)
        _run_stage_script("1_fetch_templates.py", crawl_flags)

        # 3. Build Option 1 & Option 2 JSONLs locally inside container tmpfs
        _run_stage_script("1b_build_raw_with_templates_jsonl.py")
        _run_stage_script("2_clean_wikitext.py")
        _run_stage_script("3_chunk_pages.py")
        _run_stage_script("4_generate_vertex_jsonl.py")

        # 4. Drop-guard verification BEFORE any GCS overwrite / BigQuery truncate / Vertex FULL import
        crawl_metrics = verify_drop_guard(
            prev_state=prev_state,
            min_page_floor=args.min_page_floor,
            min_page_ratio=args.min_page_ratio,
        )

        # Skip cloud writes if generated JSONL content hashes are byte-identical to last sync
        if (
            not args.force_sync
            and prev_state.get("option1_sha256") == crawl_metrics["option1_sha256"]
            and prev_state.get("option2_sha256") == crawl_metrics["option2_sha256"]
        ):
            print("Generated Option 1 & Option 2 JSONLs are byte-identical to previous sync; skipping cloud import.")
            return

    # 5. Run Step 5 (GCS upload / BigQuery native tables + Authorized Views / Vertex AI Search import)
    step5_args: List[str] = [
        "--options",
        *args.options,
        "--steps",
        *steps,
        "--import-source",
        args.import_source,
        "--source-project",
        args.source_project,
        "--gcs-bucket",
        args.gcs_bucket,
        "--gcs-prefix",
        args.gcs_prefix,
        "--bq-location",
        args.bq_location,
        "--bq-source-dataset",
        args.bq_source_dataset,
        "--bq-view-dataset",
        args.bq_view_dataset,
        "--vertex-project",
        args.vertex_project,
        "--vertex-location",
        args.vertex_location,
        "--raw-datastore-id",
        args.raw_datastore_id,
        "--chunked-datastore-id",
        args.chunked_datastore_id,
    ]
    if args.vertex_project_number:
        step5_args.extend(["--vertex-project-number", args.vertex_project_number])
    if args.materialize_export_table:
        step5_args.append("--materialize-export-table")
    if args.dry_run:
        step5_args.append("--dry-run")

    _run_stage_script("5_vertex_import_jsonl.py", step5_args)

    # 6. Persist updated sync manifest only after a full crawl succeeded
    if not args.skip_crawl and crawl_metrics:
        new_state = {
            **crawl_metrics,
            "last_sync_timestamp": run_started_utc,
            "options_deployed": args.options,
            "cloud_steps_executed": steps,
        }
        _save_sync_state(args.source_project, args.gcs_bucket, new_state, dry_run=args.dry_run)

    print("\n=== MediaWiki Cloud Run Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()
