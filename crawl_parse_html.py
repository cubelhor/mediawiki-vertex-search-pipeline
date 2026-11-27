"""Crawl Airbus MediaWiki using `action=parse` (rendered HTML) for both Option 1 (GCS) and Option 2 (BQ).

Pass 1 (`action=query`, lightweight metadata only, ~10s):
  - Discovers active knowledge namespaces (skipping User/File/MediaWiki/Template/Module & talk namespaces).
  - Lists all canonical non-redirect pages (pageid, title, ns, url, categories, lastrevid, updated_at).
  - Resolves redirect pages via `gapfilterredir=redirects&redirects=1` to build the canonical `aliases` map.

Pass 2 (`action=parse`, polite concurrency + incremental `revid` cache):
  - Fetches rendered HTML with all MediaWiki templates expanded natively by MediaWiki (`disableeditsection=1&disabletoc=1`).
  - Strips active HTML content (`<script>`, `<iframe>`, `<object>`, `<embed>`, inline `on*` handlers, `javascript:` URIs).
  - Wraps each page as a clean UTF-8 standalone `.html` document with `<h1>` title + `Aliases` / `Categories` header.
  - Writes `metadata.jsonl` (full Option 1 GCS Vertex AI Search manifest) and `metadata_incremental.jsonl` (changed pages only).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import importlib
import json
import re
import threading
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests

from wiki_utils import (
    DEFAULT_API_URL,
    DEFAULT_GCS_BUCKET,
    DEFAULT_GCS_PREFIX,
    REQUEST_TIMEOUT_SECONDS,
    add_common_mediawiki_cli_args,
    canonical_wiki_uri,
    clean_category_name,
    create_mediawiki_session,
    get_logger,
    is_generator_advancing,
    make_rfc1034_id,
    maybe_login_mediawiki,
    merge_unique_strings,
    mw_api_request,
    mw_canonical_title_key,
    safe_child_path,
    strip_invisible_chars,
    validate_https_url,
)

_step0 = importlib.import_module("0_mediawiki_download")

logger = get_logger("crawl_parse_html")
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "wikimedia-data" / "parsed_html"
DEFAULT_HTML_GCS_SUBDIR = "option1_html"

MAX_HTML_BYTES = 5 * 1024 * 1024  # 5 MB safety ceiling per parsed HTML page
MAX_CONCURRENT_WORKERS = 8

_DROP_CONTENT_TAGS = frozenset({"script", "iframe", "object", "embed", "applet", "form"})
_VOID_HTML_TAGS = frozenset(
    {"area", "base", "br", "col", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)
_URI_ATTRS = frozenset({"href", "src", "action", "formaction", "xlink:href", "data"})
_DANGEROUS_URI_RE = re.compile(r"^\s*(?:javascript|vbscript|data\s*:\s*text/html)\s*:", re.IGNORECASE)
_GCS_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")


class _SafeHtmlSanitizer(HTMLParser):
    """Strip active executable elements (`<script>`, `<iframe>`, `on*` handlers, `javascript:` URIs) from HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self._out: List[str] = []
        self._drop_stack: List[str] = []

    def _clean_attrs(self, attrs: List[Tuple[str, Optional[str]]]) -> str:
        parts: List[str] = []
        for raw_name, raw_val in attrs:
            name = (raw_name or "").strip().lower()
            if not name or name.startswith("on"):
                continue
            if raw_val is None:
                parts.append(f" {name}")
                continue
            if name in _URI_ATTRS and _DANGEROUS_URI_RE.match(raw_val):
                continue
            parts.append(f' {name}="{html.escape(raw_val, quote=True)}"')
        return "".join(parts)

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        lower = tag.lower()
        if lower in _DROP_CONTENT_TAGS:
            if lower not in _VOID_HTML_TAGS:
                self._drop_stack.append(lower)
            return
        if self._drop_stack:
            return
        self._out.append(f"<{lower}{self._clean_attrs(attrs)}>")

    def handle_startendtag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        lower = tag.lower()
        if lower in _DROP_CONTENT_TAGS or self._drop_stack:
            return
        self._out.append(f"<{lower}{self._clean_attrs(attrs)} />")

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()
        if lower in _DROP_CONTENT_TAGS:
            if self._drop_stack and self._drop_stack[-1] == lower:
                self._drop_stack.pop()
            return
        if self._drop_stack or lower in _VOID_HTML_TAGS:
            return
        self._out.append(f"</{lower}>")

    def handle_data(self, data: str) -> None:
        if not self._drop_stack:
            self._out.append(data)

    def handle_entityref(self, name: str) -> None:
        if not self._drop_stack:
            self._out.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if not self._drop_stack:
            self._out.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        # Drop HTML comments (MediaWiki parser report timing comments add noise)
        return

    def get_html(self) -> str:
        return "".join(self._out)


def sanitize_rendered_html(raw_html: str) -> str:
    """Remove executable/active tags and attributes while preserving all MediaWiki semantic markup."""
    sanitizer = _SafeHtmlSanitizer()
    sanitizer.feed(raw_html or "")
    sanitizer.close()
    return sanitizer.get_html()


def build_standalone_html_document(
    title: str,
    body_html: str,
    aliases: Sequence[str] = (),
    categories: Sequence[str] = (),
) -> str:
    """Wrap MediaWiki `action=parse` HTML in a clean standalone HTML5 document with escaped metadata header."""
    safe_title = html.escape(strip_invisible_chars(title).strip() or "Untitled", quote=True)
    meta_parts: List[str] = []
    clean_aliases = [strip_invisible_chars(a).strip() for a in aliases if strip_invisible_chars(a).strip()]
    clean_cats = [strip_invisible_chars(c).strip() for c in categories if strip_invisible_chars(c).strip()]
    if clean_aliases:
        meta_parts.append(f"Aliases: {', '.join(clean_aliases)}")
    if clean_cats:
        meta_parts.append(f"Categories: {', '.join(clean_cats)}")

    meta_block = ""
    if meta_parts:
        safe_meta = html.escape(" | ".join(meta_parts), quote=True)
        meta_block = f'  <p class="wiki-meta">{safe_meta}</p>\n'

    sanitized_body = sanitize_rendered_html(body_html).strip()
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '  <meta charset="utf-8">\n'
        f"  <title>{safe_title}</title>\n"
        "</head>\n"
        "<body>\n"
        f"  <h1>{safe_title}</h1>\n"
        f"{meta_block}"
        f"{sanitized_body}\n"
        "</body>\n"
        "</html>\n"
    )


def _validate_gcs_bucket(bucket_name: str) -> str:
    cleaned = (bucket_name or "").replace("gs://", "").strip("/")
    if not _GCS_BUCKET_RE.match(cleaned):
        raise ValueError(f"Invalid GCS bucket name: {bucket_name!r}")
    return cleaned


def _extract_categories_from_query_page(page_obj: Dict[str, Any]) -> List[str]:
    cats: List[str] = []
    for item in page_obj.get("categories") or []:
        if isinstance(item, dict):
            raw = item.get("title") or item.get("category") or item.get("*") or ""
            if item.get("hidden"):
                continue
        else:
            raw = str(item)
        cleaned = clean_category_name(str(raw))
        if cleaned:
            cats.append(cleaned)
    return merge_unique_strings(cats)


def _extract_parse_text_and_categories(parse_payload: Dict[str, Any]) -> Tuple[str, List[str]]:
    parse_obj = parse_payload.get("parse")
    if not isinstance(parse_obj, dict):
        raise RuntimeError(f"Unexpected MediaWiki action=parse payload: keys={list(parse_payload.keys())}")

    raw_text = parse_obj.get("text", "")
    if isinstance(raw_text, dict):
        html_body = str(raw_text.get("*") or "")
    else:
        html_body = str(raw_text or "")

    cats: List[str] = []
    for item in parse_obj.get("categories") or []:
        if not isinstance(item, dict) or item.get("hidden"):
            continue
        raw_cat = str(item.get("category") or item.get("*") or "").replace("_", " ")
        cleaned = clean_category_name(raw_cat)
        if cleaned:
            cats.append(cleaned)
    return html_body, merge_unique_strings(cats)


def _discover_redirect_aliases(
    session: requests.Session,
    api_url: str,
    ns_list: Sequence[Tuple[int, str]],
    maxlag: int = 15,
) -> Dict[str, List[str]]:
    """List redirect pages via `list=allpages` and resolve their canonical targets in 50-title batches (`redirects=1`)."""
    redirect_titles: List[str] = []
    for ns_id, ns_name in ns_list:
        base_params: Dict[str, str] = {
            "action": "query",
            "list": "allpages",
            "apnamespace": str(ns_id),
            "apfilterredir": "redirects",
            "aplimit": "500",
            "continue": "",
            "maxlag": str(maxlag),
            "format": "json",
            "formatversion": "2",
        }
        params = dict(base_params)
        seen_sigs: Set[Tuple[Tuple[str, str], ...]] = set()
        while True:
            sig = tuple(sorted(params.items()))
            if sig in seen_sigs:
                break
            seen_sigs.add(sig)
            try:
                data = mw_api_request(
                    session,
                    api_url,
                    params=params,
                    method="GET",
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    context=f"MediaWiki redirect list (ns={ns_id}:{ns_name})",
                )
            except RuntimeError as exc:
                if ns_id != 0 and any(
                    tok in str(exc).lower() for tok in ("readapidenied", "permissiondenied", "http 403")
                ):
                    break
                raise

            for item in data.get("query", {}).get("allpages", []):
                title = strip_invisible_chars(str(item.get("title") or "")).strip()
                if title:
                    redirect_titles.append(title)

            cont = data.get("continue")
            if not cont:
                break
            params = {**base_params, **{k: str(v) for k, v in cont.items()}}

    # Resolve redirect targets in batches of 50 via `titles=...&redirects=1` (never combined with `generator=allpages`)
    unique_redirs = merge_unique_strings(redirect_titles)
    aliases_by_target: Dict[str, List[str]] = {}
    for start in range(0, len(unique_redirs), 50):
        batch = unique_redirs[start : start + 50]
        data = mw_api_request(
            session,
            api_url,
            params={
                "action": "query",
                "titles": "|".join(batch),
                "redirects": "1",
                "maxlag": str(maxlag),
                "format": "json",
                "formatversion": "2",
            },
            method="GET",
            timeout=REQUEST_TIMEOUT_SECONDS,
            context=f"MediaWiki redirect resolution (batch {start // 50 + 1})",
        )
        for redir in data.get("query", {}).get("redirects", []):
            src = strip_invisible_chars(str(redir.get("from") or "")).strip()
            dst = strip_invisible_chars(str(redir.get("to") or "")).strip()
            if not src or not dst:
                continue
            src_key = mw_canonical_title_key(src)
            dst_key = mw_canonical_title_key(dst)
            if src_key and dst_key and src_key != dst_key:
                aliases_by_target.setdefault(dst_key, []).append(src)

    return {k: merge_unique_strings(v) for k, v in aliases_by_target.items()}


def discover_pages_metadata(
    session: requests.Session,
    api_url: str,
    namespaces: Optional[Sequence[Tuple[int, str]]] = None,
    page_ids: Optional[Sequence[int]] = None,
    limit: Optional[int] = None,
    maxlag: int = 15,
) -> List[Dict[str, Any]]:
    """Fast `action=query` metadata pass returning canonical non-redirect pages with redirect `aliases`."""
    base_url = validate_https_url(api_url)
    pages_by_id: Dict[int, Dict[str, Any]] = {}

    if page_ids:
        clean_ids = sorted({int(pid) for pid in page_ids if int(pid) > 0})
        for start in range(0, len(clean_ids), 50):
            chunk_ids = clean_ids[start : start + 50]
            params: Dict[str, str] = {
                "action": "query",
                "pageids": "|".join(str(i) for i in chunk_ids),
                "prop": "info|categories|revisions|linkshere",
                "inprop": "url",
                "rvprop": "ids|timestamp",
                "cllimit": "max",
                "lhprop": "title|redirect",
                "lhshow": "redirect",
                "lhlimit": "max",
                "continue": "",
                "maxlag": str(maxlag),
                "format": "json",
                "formatversion": "2",
            }
            seen_sigs: Set[Tuple[Tuple[str, str], ...]] = set()
            while True:
                sig = tuple(sorted(params.items()))
                if sig in seen_sigs:
                    break
                seen_sigs.add(sig)
                data = mw_api_request(
                    session,
                    base_url,
                    params=params,
                    method="GET",
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    context="MediaWiki page-id metadata lookup",
                )
                for p in data.get("query", {}).get("pages", []):
                    if not isinstance(p, dict) or p.get("missing") or p.get("redirect"):
                        continue
                    pid = int(p.get("pageid", 0))
                    if pid <= 0:
                        continue
                    entry = pages_by_id.setdefault(
                        pid,
                        {
                            "page_id": pid,
                            "ns": int(p.get("ns", 0)),
                            "title": strip_invisible_chars(str(p.get("title") or "")).strip(),
                            "uri": str(p.get("fullurl") or ""),
                            "revid": int(p.get("lastrevid") or 0),
                            "updated_at": p.get("touched"),
                            "categories": [],
                            "aliases": [],
                        },
                    )
                    revs = p.get("revisions") or []
                    if revs and isinstance(revs[0], dict):
                        entry["revid"] = int(revs[0].get("revid") or entry["revid"] or 0)
                        if revs[0].get("timestamp"):
                            entry["updated_at"] = str(revs[0]["timestamp"])
                    entry["categories"] = merge_unique_strings(
                        entry["categories"], _extract_categories_from_query_page(p)
                    )
                    lh_aliases = [
                        strip_invisible_chars(str(lh.get("title") or "")).strip()
                        for lh in (p.get("linkshere") or [])
                        if isinstance(lh, dict) and lh.get("redirect") and lh.get("title")
                    ]
                    entry["aliases"] = merge_unique_strings(entry["aliases"], lh_aliases)

                cont = data.get("continue")
                if not cont:
                    break
                params = {**params, **{k: str(v) for k, v in cont.items()}}

        result = sorted(pages_by_id.values(), key=lambda r: r["page_id"])
        for row in result:
            if not row["uri"]:
                row["uri"] = canonical_wiki_uri(row["title"])
        return result[:limit] if limit else result

    ns_list = list(namespaces) if namespaces is not None else _step0.discover_all_knowledge_namespaces(session, base_url)
    logger.info(
        "Pass 1 metadata discovery across namespaces: %s",
        ", ".join(f"{ns_id}:{ns_name}" for ns_id, ns_name in ns_list),
    )

    aliases_by_target = _discover_redirect_aliases(session, base_url, ns_list, maxlag=maxlag)
    logger.info("Discovered redirect aliases for %d canonical target page(s).", len(aliases_by_target))

    for ns_id, ns_name in ns_list:
        base_params: Dict[str, str] = {
            "action": "query",
            "generator": "allpages",
            "gapnamespace": str(ns_id),
            "gapfilterredir": "nonredirects",
            "gaplimit": "500",
            "prop": "info|categories",
            "inprop": "url",
            "cllimit": "max",
            "continue": "",
            "maxlag": str(maxlag),
            "format": "json",
            "formatversion": "2",
        }
        params = dict(base_params)
        seen_sigs = set()

        while True:
            sig = tuple(sorted(params.items()))
            if sig in seen_sigs:
                break
            seen_sigs.add(sig)
            try:
                data = mw_api_request(
                    session,
                    base_url,
                    params=params,
                    method="GET",
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    context=f"MediaWiki page list (ns={ns_id}:{ns_name})",
                )
            except RuntimeError as exc:
                if ns_id != 0 and any(
                    tok in str(exc).lower() for tok in ("readapidenied", "permissiondenied", "http 403")
                ):
                    logger.warning("[SKIPPED] Namespace %d (%s) denied by MediaWiki ACL.", ns_id, ns_name)
                    break
                raise

            for p in data.get("query", {}).get("pages", []):
                if not isinstance(p, dict) or p.get("missing") or p.get("redirect"):
                    continue
                pid = int(p.get("pageid", 0))
                if pid <= 0:
                    continue
                title = strip_invisible_chars(str(p.get("title") or "")).strip()
                entry = pages_by_id.setdefault(
                    pid,
                    {
                        "page_id": pid,
                        "ns": int(p.get("ns", ns_id)),
                        "title": title,
                        "uri": str(p.get("fullurl") or canonical_wiki_uri(title)),
                        "revid": int(p.get("lastrevid") or 0),
                        "updated_at": p.get("touched"),
                        "categories": [],
                        "aliases": aliases_by_target.get(mw_canonical_title_key(title), []),
                    },
                )
                entry["categories"] = merge_unique_strings(
                    entry["categories"], _extract_categories_from_query_page(p)
                )

            cont = data.get("continue")
            if limit and len(pages_by_id) >= limit and (not cont or is_generator_advancing(cont)):
                break
            if not cont:
                break
            params = {**base_params, **{k: str(v) for k, v in cont.items()}}

        if limit and len(pages_by_id) >= limit:
            break

    result = sorted(pages_by_id.values(), key=lambda r: r["page_id"])
    return result[:limit] if limit else result


def _load_existing_manifest(manifest_path: Path) -> Dict[int, Dict[str, Any]]:
    if not manifest_path.is_file():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        pages = payload.get("pages", []) if isinstance(payload, dict) else []
        return {int(p["page_id"]): p for p in pages if isinstance(p, dict) and p.get("page_id")}
    except Exception as exc:
        logger.warning("Could not load existing manifest %s (%s); performing full parse.", manifest_path, exc)
        return {}


def _clone_worker_session(
    primary_session: requests.Session,
    bypass_proxy: bool,
    cookie_file: Optional[str],
) -> requests.Session:
    """Create a thread-local requests.Session inheriting cookies/proxy/TLS settings from the authenticated session."""
    worker_sess = create_mediawiki_session(bypass_proxy=bypass_proxy, cookie_file=cookie_file)
    for cookie in primary_session.cookies:
        worker_sess.cookies.set_cookie(cookie)
    worker_sess.verify = primary_session.verify
    worker_sess.headers.update(primary_session.headers)
    return worker_sess


def crawl_parsed_html_wiki(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    api_url: str = DEFAULT_API_URL,
    gcs_bucket: str = DEFAULT_GCS_BUCKET,
    gcs_prefix: str = DEFAULT_GCS_PREFIX,
    html_gcs_subdir: str = DEFAULT_HTML_GCS_SUBDIR,
    page_ids: Optional[Sequence[int]] = None,
    limit: Optional[int] = None,
    workers: int = 4,
    maxlag: int = 15,
    force_reparse: bool = False,
    bypass_proxy: bool = False,
    cookie_file: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Run Pass 1 (metadata + redirects) + Pass 2 (`action=parse` HTML) and write Option 1 GCS manifests."""
    base_url = validate_https_url(api_url)
    bucket_clean = _validate_gcs_bucket(gcs_bucket)
    safe_workers = max(1, min(int(workers), MAX_CONCURRENT_WORKERS))

    output_dir.mkdir(parents=True, exist_ok=True)
    pages_dir = output_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "pages_manifest.json"

    prev_by_id = {} if force_reparse else _load_existing_manifest(manifest_path)

    primary_sess = session or create_mediawiki_session(bypass_proxy=bypass_proxy, cookie_file=cookie_file)
    maybe_login_mediawiki(primary_sess, base_url, timeout=REQUEST_TIMEOUT_SECONDS)
    worker_sessions: List[requests.Session] = []
    worker_sessions_lock = threading.Lock()

    try:
        discovered = discover_pages_metadata(
            session=primary_sess,
            api_url=base_url,
            page_ids=page_ids,
            limit=limit,
            maxlag=maxlag,
        )
        logger.info("Pass 1 complete: %d canonical non-redirect page(s) to process.", len(discovered))

        to_parse: List[Dict[str, Any]] = []
        reused_pages: Dict[int, Dict[str, Any]] = {}

        for meta in discovered:
            pid = int(meta["page_id"])
            doc_id = make_rfc1034_id(f"page-{pid}", fallback=f"page-{pid}")
            html_file = safe_child_path(pages_dir, f"{doc_id}.html")
            prev = prev_by_id.get(pid)
            if (
                not force_reparse
                and prev is not None
                and int(prev.get("revid") or 0) > 0
                and int(prev.get("revid") or 0) == int(meta.get("revid") or 0)
                and prev.get("aliases") == meta.get("aliases")
                and html_file.is_file()
                and html_file.stat().st_size > 0
            ):
                merged = dict(meta)
                merged["id"] = doc_id
                merged["html_file"] = html_file.name
                merged["categories"] = merge_unique_strings(prev.get("categories", []), meta.get("categories", []))
                merged["bytes"] = html_file.stat().st_size
                reused_pages[pid] = merged
            else:
                meta["id"] = doc_id
                to_parse.append(meta)

        logger.info(
            "Pass 2 (`action=parse`): parsing %d new/changed page(s), reusing %d cached unchanged page(s) (workers=%d).",
            len(to_parse),
            len(reused_pages),
            safe_workers,
        )

        thread_local = threading.local()

        def _get_thread_session() -> requests.Session:
            if session is not None and safe_workers == 1:
                return primary_sess
            s = getattr(thread_local, "session", None)
            if s is None:
                s = _clone_worker_session(primary_sess, bypass_proxy=bypass_proxy, cookie_file=cookie_file)
                thread_local.session = s
                with worker_sessions_lock:
                    worker_sessions.append(s)
            return s

        def _parse_one(meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            pid = int(meta["page_id"])
            doc_id = meta["id"]
            sess = _get_thread_session()
            payload = mw_api_request(
                sess,
                base_url,
                params={
                    "action": "parse",
                    "pageid": str(pid),
                    "prop": "text|displaytitle|categories",
                    "disableeditsection": "1",
                    "disabletoc": "1",
                    "maxlag": str(maxlag),
                    "format": "json",
                    "formatversion": "2",
                },
                method="GET",
                timeout=REQUEST_TIMEOUT_SECONDS,
                context=f"MediaWiki action=parse (page_id={pid})",
            )
            body_html, parse_cats = _extract_parse_text_and_categories(payload)
            if not body_html.strip():
                logger.warning("Skipping empty parsed HTML for page_id=%d (%s)", pid, meta.get("title"))
                return None

            all_cats = merge_unique_strings(meta.get("categories", []), parse_cats)
            standalone_html = build_standalone_html_document(
                title=meta["title"],
                body_html=body_html,
                aliases=meta.get("aliases", []),
                categories=all_cats,
            )
            encoded = standalone_html.encode("utf-8")
            if len(encoded) > MAX_HTML_BYTES:
                logger.warning(
                    "Truncating oversized HTML page_id=%d (%d bytes > %d ceiling).",
                    pid,
                    len(encoded),
                    MAX_HTML_BYTES,
                )
                encoded = encoded[:MAX_HTML_BYTES]

            html_file = safe_child_path(pages_dir, f"{doc_id}.html")
            html_file.write_bytes(encoded)

            result_meta = dict(meta)
            result_meta["categories"] = all_cats
            result_meta["html_file"] = html_file.name
            result_meta["bytes"] = len(encoded)
            return result_meta

        parsed_pages: Dict[int, Dict[str, Any]] = {}
        failed_ids: List[int] = []

        if safe_workers == 1:
            for idx, item in enumerate(to_parse, start=1):
                try:
                    res = _parse_one(item)
                    if res is not None:
                        parsed_pages[int(res["page_id"])] = res
                except Exception as exc:
                    logger.warning("Failed to parse page_id=%s (%s): %s", item.get("page_id"), item.get("title"), exc)
                    failed_ids.append(int(item["page_id"]))
                if idx % 100 == 0 or idx == len(to_parse):
                    logger.info("Parsed %d/%d page(s)...", idx, len(to_parse))
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=safe_workers) as pool:
                future_map = {pool.submit(_parse_one, item): item for item in to_parse}
                done_count = 0
                for fut in concurrent.futures.as_completed(future_map):
                    item = future_map[fut]
                    done_count += 1
                    try:
                        res = fut.result()
                        if res is not None:
                            parsed_pages[int(res["page_id"])] = res
                    except Exception as exc:
                        logger.warning(
                            "Failed to parse page_id=%s (%s): %s", item.get("page_id"), item.get("title"), exc
                        )
                        failed_ids.append(int(item["page_id"]))
                    if done_count % 100 == 0 or done_count == len(to_parse):
                        logger.info("Parsed %d/%d page(s)...", done_count, len(to_parse))

        all_pages_map: Dict[int, Dict[str, Any]] = {**reused_pages, **parsed_pages}
        all_pages = sorted(all_pages_map.values(), key=lambda r: int(r["page_id"]))
        changed_ids = sorted(parsed_pages.keys())
        deleted_ids = sorted(set(prev_by_id.keys()) - set(all_pages_map.keys())) if not page_ids and not limit else []

        prefix_part = f"{gcs_prefix.strip('/')}/" if gcs_prefix.strip("/") else ""
        subdir_clean = html_gcs_subdir.strip("/")

        full_meta_path = output_dir / "metadata.jsonl"
        inc_meta_path = output_dir / "metadata_incremental.jsonl"

        def _build_vertex_doc(row: Dict[str, Any]) -> Dict[str, Any]:
            struct_data: Dict[str, Any] = {
                "doc_type": "parsed_html_page",
                "ns": int(row.get("ns", 0)),
                "title": row["title"],
                "page_id": int(row["page_id"]),
                "page_title": row["title"],
                "uri": row["uri"],
                "categories": row.get("categories") or [],
                "aliases": row.get("aliases") or [],
                "is_redirect": False,
                "revid": int(row.get("revid") or 0),
                "updated_at": row.get("updated_at") or "",
            }
            return {
                "id": row["id"],
                "structData": struct_data,
                "content": {
                    "mimeType": "text/html",
                    "uri": f"gs://{bucket_clean}/{prefix_part}{subdir_clean}/pages/{row['html_file']}",
                },
            }

        changed_set = set(changed_ids)
        with (
            full_meta_path.open("w", encoding="utf-8") as f_full,
            inc_meta_path.open("w", encoding="utf-8") as f_inc,
        ):
            for row in all_pages:
                line = json.dumps(_build_vertex_doc(row), ensure_ascii=False) + "\n"
                f_full.write(line)
                if int(row["page_id"]) in changed_set:
                    f_inc.write(line)

        manifest = {
            "api_url": base_url,
            "gcs_bucket": bucket_clean,
            "gcs_prefix": gcs_prefix.strip("/"),
            "html_gcs_subdir": subdir_clean,
            "total_pages": len(all_pages),
            "parsed_count": len(parsed_pages),
            "reused_count": len(reused_pages),
            "failed_ids": sorted(failed_ids),
            "changed_ids": changed_ids,
            "deleted_ids": deleted_ids,
            "pages": all_pages,
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        logger.info(
            "Saved %d parsed HTML pages (%d newly parsed, %d cached, %d failed) -> %s",
            len(all_pages),
            len(parsed_pages),
            len(reused_pages),
            len(failed_ids),
            output_dir,
        )
        return manifest
    finally:
        for ws in worker_sessions:
            try:
                ws.close()
            except Exception:
                pass
        if session is None:
            primary_sess.close()


def _parse_page_ids_arg(raw_values: Optional[Sequence[str]]) -> Optional[List[int]]:
    if not raw_values:
        return None
    ids: List[int] = []
    for raw in raw_values:
        for part in str(raw).replace(",", " ").split():
            part = part.strip()
            if part:
                ids.append(int(part))
    return ids or None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl MediaWiki via action=parse (rendered HTML) and emit Option 1 GCS metadata manifests."
    )
    add_common_mediawiki_cli_args(parser)
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="MediaWiki api.php HTTPS URL (default: %(default)s).")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--gcs-bucket", default=DEFAULT_GCS_BUCKET)
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--html-gcs-subdir", default=DEFAULT_HTML_GCS_SUBDIR)
    parser.add_argument(
        "--page-ids",
        nargs="+",
        default=None,
        help="Optional comma/space-separated page IDs for fast smoke tests (e.g. --page-ids 3687,8159).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max pages to crawl (e.g. --limit 25).")
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help=f"Polite concurrent workers for action=parse (1..{MAX_CONCURRENT_WORKERS}, default: 4).",
    )
    parser.add_argument(
        "--force-reparse",
        action="store_true",
        help="Ignore cached revids and re-fetch every page via action=parse.",
    )
    args = parser.parse_args()

    crawl_parsed_html_wiki(
        output_dir=args.output_dir,
        api_url=args.api_url,
        gcs_bucket=args.gcs_bucket,
        gcs_prefix=args.gcs_prefix,
        html_gcs_subdir=args.html_gcs_subdir,
        page_ids=_parse_page_ids_arg(args.page_ids),
        limit=args.limit,
        workers=args.workers,
        maxlag=args.maxlag,
        force_reparse=args.force_reparse,
        bypass_proxy=args.bypass_proxy,
        cookie_file=args.cookie_file,
    )


if __name__ == "__main__":
    main()
