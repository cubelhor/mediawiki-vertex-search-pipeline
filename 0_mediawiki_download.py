"""Download all pages across all active MediaWiki namespaces (with full wikitext, categories, and URLs).

Hardened against real-world MediaWiki API & corporate network edge cases:
1. Iterates across **all knowledge namespaces** by default (`0` Main, `4` Project, `12` Help,
   `14` Category hubs, and all custom namespaces `100+` like `LGSIM:`, `LG Test:`, etc.).
2. Uses `gaplimit=50` to match MediaWiki's 50-revision per-request cap for non-bot accounts,
   and automatically backfills individual `pageids=<pid>` if any batch hits `$wgAPIMaxResultSize`.
3. Preserves `active_gapcontinue` across `clcontinue`/`rvcontinue` sub-continuations so Window 2+
   never resets back to the first page of a namespace when >500 categories exist in a batch.
4. Handles HTTP-200 `maxlag`/`ratelimited` backoff (`Retry-After`), SSO HTML login redirect detection,
   and `readapidenied` diagnostics via `mw_api_request`.
5. Stages batches in `wikimedia-data/raw_api/.staging_download/` and atomically swaps into `raw_api/`
   only after the full download completes—never wiping existing batches on a mid-run VPN/proxy drop.
6. Supports Airbus internal PKI (`MEDIAWIKI_CA_BUNDLE` / OS `truststore`), `--bypass-proxy` for
   internal `.corp` hosts, SSO cookies (`MEDIAWIKI_COOKIE` / `--cookie-file`), and BotPasswords.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from wiki_utils import (
    backfill_missing_revisions,
    create_mediawiki_session,
    is_revision_texthidden,
    maybe_login_mediawiki,
    merge_mediawiki_page_payload,
    mw_api_request,
    safe_child_path,
)

ROOT = Path(__file__).resolve().parent
BASE_DIR = ROOT / "wikimedia-data"
RAW_DIR = BASE_DIR / "raw_api"
DEFAULT_API_URL = os.environ.get("MEDIAWIKI_API_URL", "https://aow.eu.airbus.corp/api.php")
REQUEST_TIMEOUT_SECONDS = 30

# System / non-article even namespaces excluded by default (10=Template is fetched by 1_fetch_templates.py)
DEFAULT_EXCLUDED_EVEN_NAMESPACES = {
    2,    # User
    6,    # File / Image
    8,    # MediaWiki UI messages
    10,   # Template (handled separately by 1_fetch_templates.py)
    828,  # Scribunto Lua Module
}


def _validate_https_url(url: str) -> str:
    if not url.lower().startswith("https://"):
        raise ValueError(f"Refusing insecure non-HTTPS MediaWiki API URL: {url!r}")
    return url


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
        if ns_id < 0:  # Skip Special (-1) and Media (-2)
            continue
        if not include_talk and (ns_id % 2 == 1):  # Odd IDs in MediaWiki are Talk namespaces
            continue
        if ns_id in excluded:
            continue
        ns_name = str(ns_info.get("*") or ns_info.get("canonical") or "Main")
        selected.append((ns_id, ns_name))

    selected.sort(key=lambda item: item[0])
    return selected or [(0, "Main")]


def merge_page_payload(target: Dict[str, Any], incoming: Dict[str, Any]) -> None:
    """Merge multi-continuation MediaWiki `prop` payloads (`revisions`, `categories`, `fullurl`)."""
    merge_mediawiki_page_payload(target, incoming)


def _is_generator_advancing(continue_token: Dict[str, Any]) -> bool:
    """Return True only when all sub-property continuations (`rvcontinue`, `clcontinue`) are finished."""
    if "gapcontinue" not in continue_token:
        return False
    cont_str = str(continue_token.get("continue") or "")
    # In MediaWiki's `<gen>|<list>|<prop>` token, leading `||` means generator is paused for sub-props
    if cont_str.startswith("||"):
        return False
    sub_keys = {k for k in continue_token if k not in {"gapcontinue", "continue"}}
    return len(sub_keys) == 0


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

    base_url = _validate_https_url(api_url)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    staging_dir = RAW_DIR / ".staging_download"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    sess = session or create_mediawiki_session(bypass_proxy=bypass_proxy, cookie_file=cookie_file)
    maybe_login_mediawiki(sess, base_url, timeout=REQUEST_TIMEOUT_SECONDS)

    try:
        if namespaces is None:
            ns_list = discover_all_knowledge_namespaces(sess, base_url)
        else:
            ns_list = list(namespaces)

        print(
            "Discovered namespaces to download: "
            + ", ".join(f"{ns_id}:{ns_name}" for ns_id, ns_name in ns_list)
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

            print(f"\nScanning namespace {ns_id} ({ns_name})...")

            def flush_window() -> bool:
                nonlocal batch_num, total_downloaded, ns_count, window_pages
                if not window_pages:
                    return False
                # Ignore negative page IDs (`missing` pages)
                valid_pages = {
                    pid: p
                    for pid, p in window_pages.items()
                    if int(p.get("pageid", pid)) > 0 and "missing" not in p
                }
                if not valid_pages:
                    window_pages = {}
                    return False

                # If any page in the 50-page window omitted revisions (e.g. >8MB `$wgAPIMaxResultSize` cap),
                # automatically backfill each missing page ID individually before writing the batch!
                initial_missing = sum(1 for p in valid_pages.values() if not p.get("revisions"))
                if initial_missing > 0:
                    recovered = backfill_missing_revisions(
                        sess,
                        base_url,
                        valid_pages,
                        timeout=REQUEST_TIMEOUT_SECONDS,
                        context=f"Backfill missing revisions (ns={ns_id}, batch={batch_num:04d})",
                    )
                    print(
                        f"  Backfilled {recovered}/{initial_missing} truncated page revision(s) in batch_{batch_num:04d}."
                    )

                page_count = len(valid_pages)
                missing_revs = sum(
                    1
                    for p in valid_pages.values()
                    if not p.get("revisions")
                    or (
                        p.get("revisions")
                        and not is_revision_texthidden(p["revisions"][0])
                        and "slots" not in p["revisions"][0]
                        and "*" not in p["revisions"][0]
                        and "content" not in p["revisions"][0]
                    )
                )
                if missing_revs > 0:
                    print(
                        f"  [WARNING] {missing_revs}/{page_count} pages in batch_{batch_num:04d} still lacked revision content!"
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
                print(
                    f"  Staged {batch_file.name} (ns={ns_id}:{ns_name}, pages={page_count}, "
                    f"missing_revisions={missing_revs}, total={total_downloaded})"
                )
                batch_num += 1
                window_pages = {}
                return target_pages is not None and total_downloaded >= target_pages

            reached_global_target = False
            while True:
                req_sig = tuple(sorted(params.items()))
                if req_sig in seen_request_signatures:
                    flush_window()
                    print("  Detected repeated request state; advancing to next namespace.")
                    break
                seen_request_signatures.add(req_sig)

                data = mw_api_request(
                    sess,
                    base_url,
                    params=params,
                    method="GET",
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    context=f"MediaWiki query (ns={ns_id})",
                )

                pages = data.get("query", {}).get("pages", {})
                if not pages and not window_pages:
                    print(f"  Namespace {ns_id} ({ns_name}) is empty; skipping.")
                    break

                for pid, pdata in pages.items():
                    if pid not in window_pages:
                        window_pages[pid] = dict(pdata)
                    else:
                        merge_page_payload(window_pages[pid], pdata)

                continue_token = data.get("continue")
                if not continue_token:
                    reached_global_target = flush_window()
                    break

                if _is_generator_advancing(continue_token):
                    reached_global_target = flush_window()
                    if reached_global_target:
                        break
                    active_gapcontinue = str(continue_token["gapcontinue"])
                    params = dict(base_params)
                    params["gapcontinue"] = active_gapcontinue
                    if "continue" in continue_token:
                        params["continue"] = str(continue_token["continue"])
                else:
                    # Sub-property continuation (`clcontinue` / `rvcontinue`) on the SAME 50-page window:
                    # CRITICAL: preserve `active_gapcontinue` so Window 2+ never resets back to page 1!
                    params = dict(base_params)
                    if active_gapcontinue is not None:
                        params["gapcontinue"] = active_gapcontinue
                    params.update({str(k): str(v) for k, v in continue_token.items()})

            per_ns_counts[f"{ns_id}:{ns_name}"] = ns_count
            if reached_global_target:
                print(f"Reached target page count {target_pages}; stopping.")
                break

        # Atomic swap: only replace RAW_DIR/batch_*.json once the full run succeeded!
        staged_files = sorted(staging_dir.glob("batch_*.json"))
        if not staged_files:
            raise RuntimeError("No pages were downloaded across any namespace; leaving existing raw_api/ intact.")

        if clean_existing_batches:
            for old_batch in RAW_DIR.glob("batch_*.json"):
                old_batch.unlink()

        for staged_file in staged_files:
            dest = safe_child_path(RAW_DIR, staged_file.name)
            shutil.move(str(staged_file), str(dest))

        shutil.rmtree(staging_dir, ignore_errors=True)

        print("\nDownload complete and atomically committed to wikimedia-data/raw_api/!")
        for ns_label, cnt in per_ns_counts.items():
            if cnt > 0:
                print(f"  Namespace {ns_label}: {cnt} pages")
        print(f"Total pages captured: {total_downloaded}")
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
    parser.add_argument(
        "--main-only",
        action="store_true",
        help="Only download Namespace 0 (Main) instead of iterating all namespaces.",
    )
    parser.add_argument("--include-talk", action="store_true", help="Also include Talk discussion namespaces.")
    parser.add_argument("--include-user", action="store_true", help="Also include User (ns=2) pages.")
    parser.add_argument(
        "--bypass-proxy",
        action="store_true",
        help="Ignore HTTP_PROXY/HTTPS_PROXY environment variables for direct internal .airbus.corp access.",
    )
    parser.add_argument(
        "--cookie-file",
        default=None,
        help="Optional Netscape cookies.txt file for corporate SSO authentication (or set MEDIAWIKI_COOKIE).",
    )
    parser.add_argument(
        "--maxlag",
        type=int,
        default=15,
        help="MediaWiki maxlag threshold in seconds (automatically retries with backoff if exceeded).",
    )
    args = parser.parse_args()

    shared_session = create_mediawiki_session(
        bypass_proxy=args.bypass_proxy,
        cookie_file=args.cookie_file,
    )
    api_endpoint = _validate_https_url(DEFAULT_API_URL)

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