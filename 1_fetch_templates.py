"""Download all MediaWiki Template definitions (Namespace 10) safely into wikimedia-data/templates/."""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from wiki_utils import (
    DEFAULT_API_URL,
    REQUEST_TIMEOUT_SECONDS,
    add_common_mediawiki_cli_args,
    backfill_missing_revisions,
    canonical_wiki_uri,
    clean_category_name,
    commit_staged_files,
    create_mediawiki_session,
    extract_explicit_categories,
    extract_revision_wikitext,
    get_logger,
    is_generator_advancing,
    maybe_login_mediawiki,
    merge_mediawiki_page_payload,
    merge_unique_strings,
    mw_api_request,
    reset_staging_dir,
    safe_child_path,
    validate_https_url,
)

logger = get_logger("1_fetch_templates")
ROOT = Path(__file__).resolve().parent
TEMPLATE_DIR = ROOT / "wikimedia-data" / "templates"


def fetch_all_templates(
    api_url: str = DEFAULT_API_URL,
    bypass_proxy: bool = False,
    cookie_file: Optional[str] = None,
    maxlag: int = 15,
    clean_existing: bool = True,
    session: Optional[requests.Session] = None,
) -> int:
    base_url = validate_https_url(api_url)
    staging_dir = reset_staging_dir(TEMPLATE_DIR, ".staging_templates")

    base_params: Dict[str, str] = {
        "action": "query",
        "generator": "allpages",
        "gapnamespace": "10",  # MediaWiki Template Namespace
        "gaplimit": "50",
        "prop": "revisions|categories|info",
        "rvprop": "ids|timestamp|content",
        "rvslots": "main",
        "cllimit": "max",
        "inprop": "url",
        "continue": "",
        "maxlag": str(maxlag),
        "format": "json",
    }
    active_gapcontinue: Optional[str] = None
    params: Dict[str, str] = dict(base_params)
    templates_by_id: Dict[str, Dict[str, Any]] = {}
    seen_request_signatures = set()

    sess = session or create_mediawiki_session(bypass_proxy=bypass_proxy, cookie_file=cookie_file)
    maybe_login_mediawiki(sess, base_url, timeout=REQUEST_TIMEOUT_SECONDS)
    logger.info("Fetching templates from Namespace 10 (%s)...", base_url)

    try:
        while True:
            req_sig = tuple(sorted(params.items()))
            if req_sig in seen_request_signatures:
                break
            seen_request_signatures.add(req_sig)

            data = mw_api_request(
                sess,
                base_url,
                params=params,
                method="GET",
                timeout=REQUEST_TIMEOUT_SECONDS,
                context="MediaWiki template query (ns=10)",
            )

            pages = data.get("query", {}).get("pages", {})
            for page_id, page_data in pages.items():
                if int(page_data.get("pageid", page_id)) <= 0 or "missing" in page_data:
                    continue
                if page_id not in templates_by_id:
                    templates_by_id[page_id] = dict(page_data)
                else:
                    merge_mediawiki_page_payload(templates_by_id[page_id], page_data)

            continue_token = data.get("continue")
            if not continue_token:
                break

            if is_generator_advancing(continue_token):
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

        missing_tpl_revs = sum(1 for p in templates_by_id.values() if not p.get("revisions"))
        if missing_tpl_revs > 0:
            recovered = backfill_missing_revisions(
                sess,
                base_url,
                templates_by_id,
                timeout=REQUEST_TIMEOUT_SECONDS,
                context="Backfill missing template revisions (ns=10)",
            )
            logger.info("Backfilled %d/%d truncated template revision(s).", recovered, missing_tpl_revs)

        saved = 0
        for page_id, page_data in sorted(templates_by_id.items(), key=lambda kv: int(kv[0])):
            title = str(page_data.get("title") or "")
            revisions = page_data.get("revisions") or []
            if not revisions:
                logger.warning("Template page_id=%s (%r) had no revisions after backfill; skipping.", page_id, title)
                continue
            rev0 = revisions[0]
            content = extract_revision_wikitext(rev0)
            short_name = re.sub(r"^[^:]+:\s*", "", title, count=1).strip() or title.strip()

            api_cats = [
                clean_category_name(c.get("title", ""))
                for c in (page_data.get("categories") or [])
                if isinstance(c, dict) and c.get("title")
            ]
            merged_cats = merge_unique_strings(api_cats, extract_explicit_categories(content))

            staged_path = safe_child_path(staging_dir, f"{page_id}_{short_name}.json")
            template_payload = {
                "page_id": str(page_id),
                "title": title,
                "wikitext": content,
                "categories": merged_cats,
                "uri": page_data.get("fullurl") or canonical_wiki_uri(title),
                "updated_at": rev0.get("timestamp"),
            }
            staged_path.write_text(
                json.dumps(template_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            saved += 1

        commit_staged_files(staging_dir, TEMPLATE_DIR, "*.json", clean_existing=clean_existing)
        logger.info("Finished downloading and atomically committing %d templates.", saved)
        return saved

    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download all MediaWiki Namespace-10 templates.")
    add_common_mediawiki_cli_args(parser)
    args = parser.parse_args()
    fetch_all_templates(
        bypass_proxy=args.bypass_proxy,
        cookie_file=args.cookie_file,
        maxlag=args.maxlag,
    )