"""Download all pages across all active MediaWiki namespaces (with full wikitext, categories, and URLs)."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from wiki_utils import (
    DEFAULT_API_URL,
    REQUEST_TIMEOUT_SECONDS,
    add_common_mediawiki_cli_args,
    backfill_missing_revisions,
    commit_staged_files,
    create_mediawiki_session,
    get_logger,
    is_generator_advancing,
    is_revision_texthidden,
    maybe_login_mediawiki,
    merge_mediawiki_page_payload,
    mw_api_request,
    reset_staging_dir,
    safe_child_path,
    validate_https_url,
)

logger = get_logger("0_mediawiki_download")
ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "wikimedia-data" / "raw_api"

# System / non-article even namespaces excluded by default (10=Template is fetched by 1_fetch_templates.py)
DEFAULT_EXCLUDED_EVEN_NAMESPACES = {
    2,    # User
    6,    # File / Image
    8,    # MediaWiki UI messages
    10,   # Template (handled separately by 1_fetch_templates.py)
    828,  # Scribunto Lua Module
}


def discover_all_knowledge_namespaces(
    session: requests.Session,
    api_url: str,
    include_talk: bool = False,
    include_user: bool = False,
) -> List[Tuple[int, str]]:
    """Query `meta=siteinfo&siprop=namespaces` and return all article/project/custom namespaces."""
    payload = mw_api_request(
        session,
        api_url,
        params={
            "action": "query",
            "meta": "siteinfo",
            "siprop": "namespaces",
            "format": "json",
        },
        method="GET",
        timeout=REQUEST_TIMEOUT_SECONDS,
        context="MediaWiki siteinfo discovery",
    )
    ns_map = payload.get("query", {}).get("namespaces", {})

    excluded = set(DEFAULT_EXCLUDED_EVEN_NAMESPACES)
    if include_user:
        excluded.discard(2)

    selected: List[Tuple[int, str]] = []
    for ns_info in ns_map.values():
        ns_id = int(ns_info.get("id", -1))
        if ns_id < 0 or (not include_talk and ns_id % 2 == 1) or ns_id in excluded:
            continue
        ns_name = str(ns_info.get("*") or ns_info.get("canonical") or "Main")
        selected.append((ns_id, ns_name))

    selected.sort(key=lambda item: item[0])
    return selected or [(0, "Main")]


def fetch_wiki_batches(
    target_pages: Optional[int] = None,
    gaplimit: int = 50,
    namespaces: Optional[Iterable[Tuple[int, str]]] = None,
    api_url: str = DEFAULT_API_URL,
    clean_existing_batches: bool = True,
    bypass_proxy: bool = False,
    cookie_file: Optional[str] = None,
    maxlag: int = 15,
    session: Optional[requests.Session] = None,
) -> int:
    if gaplimit < 1 or gaplimit > 50:
        raise ValueError(
            f"gaplimit={gaplimit} exceeds MediaWiki's 50-revision per-request limit for non-bot accounts; use <= 50."
        )

    base_url = validate_https_url(api_url)
    staging_dir = reset_staging_dir(RAW_DIR, ".staging_download")

    sess = session or create_mediawiki_session(bypass_proxy=bypass_proxy, cookie_file=cookie_file)
    maybe_login_mediawiki(sess, base_url, timeout=REQUEST_TIMEOUT_SECONDS)

    try:
        ns_list = list(namespaces) if namespaces is not None else discover_all_knowledge_namespaces(sess, base_url)
        logger.info(
            "Discovered namespaces to download: %s",
            ", ".join(f"{ns_id}:{ns_name}" for ns_id, ns_name in ns_list),
        )

        batch_num = 1
        total_downloaded = 0
        per_ns_counts: Dict[str, int] = {}

        for ns_id, ns_name in ns_list:
            base_params: Dict[str, str] = {
                "action": "query",
                "generator": "allpages",
                "gapnamespace": str(ns_id),
                "gaplimit": str(gaplimit),
                "prop": "revisions|categories|info",
                "rvprop": "ids|timestamp|user|sha1|content",
                "rvslots": "main",
                "cllimit": "max",
                "inprop": "url",
                "continue": "",
                "maxlag": str(maxlag),
                "format": "json",
            }
            active_gapcontinue: Optional[str] = None
            params: Dict[str, str] = dict(base_params)
            seen_request_signatures = set()
            window_pages: Dict[str, Dict[str, Any]] = {}
            ns_count = 0

            logger.info("Scanning namespace %d (%s)...", ns_id, ns_name)

            def flush_window() -> bool:
                nonlocal batch_num, total_downloaded, ns_count, window_pages
                if not window_pages:
                    return False
                valid_pages = {
                    pid: p
                    for pid, p in window_pages.items()
                    if int(p.get("pageid", pid)) > 0 and "missing" not in p
                }
                if not valid_pages:
                    window_pages = {}
                    return False

                initial_missing = sum(1 for p in valid_pages.values() if not p.get("revisions"))
                if initial_missing > 0:
                    recovered = backfill_missing_revisions(
                        sess,
                        base_url,
                        valid_pages,
                        timeout=REQUEST_TIMEOUT_SECONDS,
                        context=f"Backfill missing revisions (ns={ns_id}, batch={batch_num:04d})",
                    )
                    logger.info(
                        "Backfilled %d/%d truncated page revision(s) in batch_%04d.",
                        recovered,
                        initial_missing,
                        batch_num,
                    )

                page_count = len(valid_pages)
                missing_revs = sum(
                    1
                    for p in valid_pages.values()
                    if not p.get("revisions")
                    or (
                        not is_revision_texthidden(p["revisions"][0])
                        and "slots" not in p["revisions"][0]
                        and "*" not in p["revisions"][0]
                        and "content" not in p["revisions"][0]
                    )
                )
                if missing_revs > 0:
                    logger.warning(
                        "%d/%d pages in batch_%04d still lacked revision content!",
                        missing_revs,
                        page_count,
                        batch_num,
                    )

                payload = {
                    "batchcomplete": "",
                    "namespace": {"id": ns_id, "name": ns_name},
                    "query": {"pages": valid_pages},
                }

                batch_file = safe_child_path(staging_dir, f"batch_{batch_num:04d}.json")
                batch_file.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                total_downloaded += page_count
                ns_count += page_count
                logger.info(
                    "Staged %s (ns=%d:%s, pages=%d, missing_revisions=%d, total=%d)",
                    batch_file.name,
                    ns_id,
                    ns_name,
                    page_count,
                    missing_revs,
                    total_downloaded,
                )
                batch_num += 1
                window_pages = {}
                return target_pages is not None and total_downloaded >= target_pages

            reached_global_target = False
            while True:
                req_sig = tuple(sorted(params.items()))
                if req_sig in seen_request_signatures:
                    flush_window()
                    logger.warning("Detected repeated request state; advancing to next namespace.")
                    break
                seen_request_signatures.add(req_sig)

                try:
                    data = mw_api_request(
                        sess,
                        base_url,
                        params=params,
                        method="GET",
                        timeout=REQUEST_TIMEOUT_SECONDS,
                        context=f"MediaWiki query (ns={ns_id})",
                    )
                except RuntimeError as api_err:
                    err_low = str(api_err).lower()
                    is_acl_denial = any(
                        token in err_low
                        for token in ("readapidenied", "permissiondenied", "http 403", "access denied", "not allowed")
                    )
                    if ns_id != 0 and is_acl_denial:
                        logger.warning(
                            "[SKIPPED] Namespace %d (%s) denied by MediaWiki ACL; continuing.",
                            ns_id,
                            ns_name,
                        )
                        window_pages = {}
                        break
                    raise

                pages = data.get("query", {}).get("pages", {})
                if not pages and not window_pages:
                    logger.info("Namespace %d (%s) is empty; skipping.", ns_id, ns_name)
                    break

                for pid, pdata in pages.items():
                    if pid not in window_pages:
                        window_pages[pid] = dict(pdata)
                    else:
                        merge_mediawiki_page_payload(window_pages[pid], pdata)

                continue_token = data.get("continue")
                if not continue_token:
                    reached_global_target = flush_window()
                    break

                if is_generator_advancing(continue_token):
                    reached_global_target = flush_window()
                    if reached_global_target:
                        break
                    active_gapcontinue = str(continue_token["gapcontinue"])
                    params = dict(base_params)
                    params["gapcontinue"] = active_gapcontinue
                    if "continue" in continue_token:
                        params["continue"] = str(continue_token["continue"])
                else:
                    params = dict(base_params)
                    if active_gapcontinue is not None:
                        params["gapcontinue"] = active_gapcontinue
                    params.update({str(k): str(v) for k, v in continue_token.items()})

            per_ns_counts[f"{ns_id}:{ns_name}"] = ns_count
            if reached_global_target:
                logger.info("Reached target page count %d; stopping.", target_pages)
                break

        commit_staged_files(staging_dir, RAW_DIR, "batch_*.json", clean_existing=clean_existing_batches)
        logger.info("Download complete and atomically committed to %s (total=%d pages).", RAW_DIR, total_downloaded)
        for ns_label, cnt in per_ns_counts.items():
            if cnt > 0:
                logger.info("  Namespace %s: %d pages", ns_label, cnt)
        return total_downloaded

    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download full MediaWiki pages across all active namespaces in 50-page batches."
    )
    parser.add_argument("--target-pages", type=int, default=None, help="Optional max pages to download.")
    parser.add_argument("--gaplimit", type=int, default=50, help="Pages per batch (max 50 for full revisions).")
    parser.add_argument("--main-only", action="store_true", help="Only download Namespace 0 (Main).")
    parser.add_argument("--include-talk", action="store_true", help="Also include Talk discussion namespaces.")
    parser.add_argument("--include-user", action="store_true", help="Also include User (ns=2) pages.")
    add_common_mediawiki_cli_args(parser)
    args = parser.parse_args()

    shared_session = create_mediawiki_session(bypass_proxy=args.bypass_proxy, cookie_file=args.cookie_file)
    api_endpoint = validate_https_url(DEFAULT_API_URL)

    if args.main_only:
        ns_targets = [(0, "Main")]
    else:
        maybe_login_mediawiki(shared_session, api_endpoint, timeout=REQUEST_TIMEOUT_SECONDS)
        ns_targets = discover_all_knowledge_namespaces(
            shared_session,
            api_endpoint,
            include_talk=args.include_talk,
            include_user=args.include_user,
        )

    fetch_wiki_batches(
        target_pages=args.target_pages,
        gaplimit=args.gaplimit,
        namespaces=ns_targets,
        api_url=api_endpoint,
        bypass_proxy=args.bypass_proxy,
        cookie_file=args.cookie_file,
        maxlag=args.maxlag,
        session=shared_session,
    )