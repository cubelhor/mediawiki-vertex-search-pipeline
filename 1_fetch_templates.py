"""Download all MediaWiki Template definitions (Namespace 10) safely into wikimedia-data/templates/.

Hardened against raw API & Windows/macOS filesystem edge cases:
1. Preserves `active_gapcontinue` across `clcontinue`/`rvcontinue` sub-continuations.
2. Merges multi-continuation `categories` lists instead of overwriting them.
3. Uses `mw_api_request` for HTTP-200 `maxlag`/`ratelimited` backoff, Airbus PKI `SSLError` guidance,
   corporate SSO HTML redirect detection, and `MEDIAWIKI_COOKIE` / `--cookie-file` auth.
4. Automatically backfills individual `pageids=<pid>` if any template in a 50-page batch omitted `revisions`
   (preventing silent dropped templates).
5. Prefixes saved template files with `{page_id}_{short_name}.json` inside an atomic staging folder
   so case-only variants (`Template:Cite Web` vs `Template:Cite web`, `TOClimit` vs `Toclimit`) and
   Windows reserved names (`CON`, `AUX`, `PRN`) never collide or wipe existing templates on failure.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from wiki_utils import (
    backfill_missing_revisions,
    canonical_wiki_uri,
    clean_category_name,
    create_mediawiki_session,
    extract_explicit_categories,
    extract_revision_wikitext,
    maybe_login_mediawiki,
    merge_mediawiki_page_payload,
    merge_unique_strings,
    mw_api_request,
    safe_child_path,
)

ROOT = Path(__file__).resolve().parent
BASE_DIR = ROOT / "wikimedia-data"
TEMPLATE_DIR = BASE_DIR / "templates"
DEFAULT_API_URL = os.environ.get("MEDIAWIKI_API_URL", "https://aow.eu.airbus.corp/api.php")
REQUEST_TIMEOUT_SECONDS = 30


def _merge_template_payload(target: Dict[str, Any], incoming: Dict[str, Any]) -> None:
    merge_mediawiki_page_payload(target, incoming)


def _is_generator_advancing(continue_token: Dict[str, Any]) -> bool:
    if "gapcontinue" not in continue_token:
        return False
    cont_str = str(continue_token.get("continue") or "")
    if cont_str.startswith("||"):
        return False
    sub_keys = {k for k in continue_token if k not in {"gapcontinue", "continue"}}
    return len(sub_keys) == 0


def fetch_all_templates(
    api_url: str = DEFAULT_API_URL,
    bypass_proxy: bool = False,
    cookie_file: Optional[str] = None,
    maxlag: int = 15,
    clean_existing: bool = True,
    session: Optional[requests.Session] = None,
) -> int:
    if not api_url.lower().startswith("https://"):
        raise ValueError(f"Refusing insecure non-HTTPS MediaWiki API URL: {api_url!r}")

    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    staging_dir = TEMPLATE_DIR / ".staging_templates"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

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
    maybe_login_mediawiki(sess, api_url, timeout=REQUEST_TIMEOUT_SECONDS)
    print(f"Fetching templates from Namespace 10 ({api_url})...")

    try:
        while True:
            req_sig = tuple(sorted(params.items()))
            if req_sig in seen_request_signatures:
                break
            seen_request_signatures.add(req_sig)

            data = mw_api_request(
                sess,
                api_url,
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
                    _merge_template_payload(templates_by_id[page_id], page_data)

            continue_token = data.get("continue")
            if not continue_token:
                break

            if _is_generator_advancing(continue_token):
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
                api_url,
                templates_by_id,
                timeout=REQUEST_TIMEOUT_SECONDS,
                context="Backfill missing template revisions (ns=10)",
            )
            print(f"  Backfilled {recovered}/{missing_tpl_revs} truncated template revision(s).")

        saved = 0
        for page_id, page_data in sorted(templates_by_id.items(), key=lambda kv: int(kv[0])):
            title = str(page_data.get("title") or "")
            revisions = page_data.get("revisions") or []
            if not revisions:
                print(f"  [WARNING] Template page_id={page_id} ({title!r}) had no revisions even after backfill; skipping.")
                continue
            rev0 = revisions[0]
            content = extract_revision_wikitext(rev0)
            # Strip leading `Template:` (or localized namespace prefix like `Modèle:`) safely
            short_name = re.sub(r"^[^:]+:\s*", "", title, count=1).strip() or title.strip()

            api_cats = [
                clean_category_name(c.get("title", ""))
                for c in (page_data.get("categories") or [])
                if isinstance(c, dict) and c.get("title")
            ]
            merged_cats = merge_unique_strings(api_cats, extract_explicit_categories(content))

            # Prefix with page_id so case-insensitive OSes (Windows/macOS) never overwrite collisions
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

        if saved == 0:
            raise RuntimeError("0 templates returned by API; leaving existing templates/ intact.")

        if clean_existing:
            for old_json in TEMPLATE_DIR.glob("*.json"):
                old_json.unlink()

        for staged_file in sorted(staging_dir.glob("*.json")):
            dest = safe_child_path(TEMPLATE_DIR, staged_file.name)
            shutil.move(str(staged_file), str(dest))

        shutil.rmtree(staging_dir, ignore_errors=True)
        print(f"Finished downloading and atomically committing {saved} templates.")
        return saved

    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download all MediaWiki Namespace-10 templates.")
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
    fetch_all_templates(
        bypass_proxy=args.bypass_proxy,
        cookie_file=args.cookie_file,
        maxlag=args.maxlag,
    )