"""Shared MediaWiki API client, title/redirect/category/template resolution, and security utilities."""

from __future__ import annotations

import hashlib
import http.cookiejar
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parent

# Prevent Windows cp1252 console crashes when printing Unicode wiki titles/symbols
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

# Fallback to local .venv pure-Python packages if running with system python
for _venv_candidate in (
    ROOT_DIR / ".venv" / "lib" / "site-packages",
    ROOT_DIR / ".venv" / "Lib" / "site-packages",
):
    if _venv_candidate.exists() and str(_venv_candidate) not in sys.path:
        sys.path.append(str(_venv_candidate))

import mwparserfromhell  # noqa: E402

WIKI_BASE_ARTICLE_URL = "https://aow.eu.airbus.corp/wiki/"

# Invisible directional / zero-width characters occasionally present in MediaWiki titles/categories
_INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\ufeff]")

# Matches [[Category:Foo]] or [[Category:Foo|SortKey]], excluding inline [[:Category:Foo]] links
_CATEGORY_TAG_RE = re.compile(
    r"\[\[\s*(?!:)(?:Category|category)\s*:\s*([^\[\]]+?)\s*\]\]"
)

# Matches MediaWiki redirect directive at start of page: #REDIRECT [[Target Page#Section|Label]]
_REDIRECT_RE = re.compile(
    r"^\s*#redirect\s*:?\s*\[\[\s*:?\s*([^\[\]|#]+)(?:#[^\[\]|]*)?(?:\|[^\[\]]*)?\s*\]\]",
    re.IGNORECASE,
)

# Transient HTTP-200 MediaWiki API error codes that should back off and retry instead of aborting
_RETRYABLE_MW_ERROR_CODES = {
    "maxlag",
    "ratelimited",
    "internal_api_error_DBConnectionError",
    "internal_api_error_DBQueryError",
    "internal_api_error_DBQueryTimeoutError",
    "readonly",
}

# Magic words / parser variables that look like 0-arg templates ({{PAGENAME}}, {{NUMBEROFARTICLES}}, etc.)
_MEDIAWIKI_MAGIC_WORDS = {
    "pagename",
    "pagenamee",
    "fullpagename",
    "fullpagenamee",
    "basepagename",
    "subpagename",
    "rootpagename",
    "talkpagename",
    "subjectpagename",
    "namespace",
    "namespacenumber",
    "talkspace",
    "subjectspace",
    "servername",
    "server",
    "scriptpath",
    "stylepath",
    "sitename",
    "currentyear",
    "currentmonth",
    "currentmonthname",
    "currentday",
    "currentday2",
    "currentdayname",
    "currenttime",
    "currenttimestamp",
    "localyear",
    "localmonth",
    "localday",
    "localtime",
    "localtimestamp",
    "revisionid",
    "revisionday",
    "revisionmonth",
    "revisionyear",
    "revisiontimestamp",
    "revisionuser",
    "numberofpages",
    "numberofarticles",
    "numberoffiles",
    "numberofedits",
    "numberofusers",
    "numberofadmins",
    "numberofactiveusers",
    "!",
    "=",
}

# Template maintenance categories that should not propagate onto content pages
_EXCLUDED_TEMPLATE_CATEGORIES = {
    "templates",
    "documentation assistance templates",
    "documentation see also templates",
    "template documentation",
    "template-related templates",
    "templates not safe for use in citation templates",
    "internal category-link templates",
    "wikipedia citation administration",
    "wikipedia help navigation",
    "container categories",
    "mbox templates",
}


@dataclass
class TemplateRecord:
    page_id: int
    title: str
    name: str
    norm_key: str
    raw_wikitext: str
    transcludable_wikitext: str
    own_categories: List[str] = field(default_factory=list)
    inherited_categories: List[str] = field(default_factory=list)
    uri: str = ""
    source_file: str = ""
    updated_at: Optional[str] = None
    is_redirect: bool = False
    redirect_target: Optional[str] = None


@dataclass
class RawPageRecord:
    page_id: int
    ns: int
    title: str
    wikitext: str
    batch_file: str
    revid: Optional[int] = None
    updated_at: Optional[str] = None
    content_model: str = "wikitext"
    uri: str = ""
    is_redirect: bool = False
    redirect_target: Optional[str] = None
    categories: List[str] = field(default_factory=list)
    referenced_template_names: List[str] = field(default_factory=list)
    referenced_page_titles: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)


def strip_invisible_chars(value: str) -> str:
    return _INVISIBLE_CHARS_RE.sub("", value or "")


_WINDOWS_RESERVED_STEMS = {
    "con", "prn", "aux", "nul",
    *(f"com{i}" for i in range(1, 10)),
    *(f"lpt{i}" for i in range(1, 10)),
}


def safe_child_path(base_dir: Path, unsafe_filename: str, max_stem_len: int = 100) -> Path:
    """Sanitize an untrusted filename (safe on Windows MAX_PATH & macOS/Linux) and enforce directory containment."""
    resolved_base = base_dir.resolve()
    cleaned = strip_invisible_chars(unsafe_filename)
    cleaned = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", cleaned).strip(" .")
    if not cleaned:
        cleaned = "unnamed"

    p = Path(cleaned)
    suffix = p.suffix
    stem = p.stem[:max_stem_len].rstrip(" ._") or "unnamed"
    if stem.lower() in _WINDOWS_RESERVED_STEMS:
        stem = f"mw_{stem}"
    candidate = (resolved_base / f"{stem}{suffix}").resolve()

    if not candidate.is_relative_to(resolved_base):
        raise ValueError(f"Path traversal blocked for filename: {unsafe_filename!r}")
    return candidate


def extract_revision_wikitext(rev_obj: Dict[str, object]) -> str:
    """Extract wikitext from MediaWiki MCR slots (`slots.main.*` / `content`) or legacy `rev['*']`."""
    if not isinstance(rev_obj, dict):
        return ""
    slots = rev_obj.get("slots")
    if isinstance(slots, dict):
        main_slot = slots.get("main")
        if isinstance(main_slot, dict):
            wt = main_slot.get("*") if "*" in main_slot else main_slot.get("content")
            if isinstance(wt, str):
                return wt
    legacy = rev_obj.get("*") if "*" in rev_obj else rev_obj.get("content")
    return legacy if isinstance(legacy, str) else ""


def extract_revision_content_model(rev_obj: Dict[str, object]) -> str:
    """Return the MediaWiki slot `contentmodel` (e.g. `wikitext`, `css`, `javascript`, `json`)."""
    if not isinstance(rev_obj, dict):
        return "wikitext"
    slots = rev_obj.get("slots")
    if isinstance(slots, dict):
        main_slot = slots.get("main")
        if isinstance(main_slot, dict) and main_slot.get("contentmodel"):
            return str(main_slot["contentmodel"]).strip().lower()
    if rev_obj.get("contentmodel"):
        return str(rev_obj["contentmodel"]).strip().lower()
    return "wikitext"


def is_revision_texthidden(rev_obj: Dict[str, object]) -> bool:
    """Return True if an admin suppressed/hid the revision text (`texthidden` flag)."""
    if not isinstance(rev_obj, dict):
        return False
    if "texthidden" in rev_obj:
        return True
    slots = rev_obj.get("slots")
    if isinstance(slots, dict):
        main_slot = slots.get("main")
        if isinstance(main_slot, dict) and "texthidden" in main_slot:
            return True
    return False


def _load_cookie_jar(cookie_file_path: str) -> http.cookiejar.CookieJar:
    """Load cookies from a local Netscape `cookies.txt` or LWP cookie file safely."""
    resolved = Path(cookie_file_path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Cookie file not found: {resolved}")
    if resolved.stat().st_size > 2 * 1024 * 1024:
        raise ValueError(f"Cookie file unreasonably large (>2MB): {resolved}")

    for jar_cls in (http.cookiejar.MozillaCookieJar, http.cookiejar.LWPCookieJar):
        try:
            jar = jar_cls(str(resolved))
            jar.load(ignore_discard=True, ignore_expires=True)
            return jar
        except Exception:
            continue
    raise ValueError(
        f"Could not parse cookie file {resolved} (expected Netscape cookies.txt or LWP format)."
    )


def create_mediawiki_session(
    bypass_proxy: bool = False,
    cookie_file: Optional[str] = None,
):
    """Build a hardened `requests.Session` with retries, Airbus PKI CA support, proxy bypass, and optional SSO cookies."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # Use OS native trust store on Windows/macOS if `truststore` is installed (helps with Airbus internal PKI)
    ca_bundle = (
        os.environ.get("MEDIAWIKI_CA_BUNDLE")
        or os.environ.get("REQUESTS_CA_BUNDLE")
        or os.environ.get("SSL_CERT_FILE")
    )
    if not ca_bundle:
        try:
            import truststore  # type: ignore

            truststore.inject_into_ssl()
        except Exception:
            pass

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "AirbusWikiVertexSync/1.0 (Python requests; internal RAG ingestion)",
            "Accept": "application/json",
        }
    )
    if ca_bundle:
        session.verify = ca_bundle
    if bypass_proxy:
        # Prevent external internet HTTP_PROXY from intercepting internal *.airbus.corp calls
        session.trust_env = False

    # Support corporate SSO session cookies via file or MEDIAWIKI_COOKIE env var (never logged)
    effective_cookie_file = cookie_file or os.environ.get("MEDIAWIKI_COOKIE_FILE", "").strip()
    if effective_cookie_file:
        session.cookies.update(_load_cookie_jar(effective_cookie_file))

    raw_cookie_header = os.environ.get("MEDIAWIKI_COOKIE", "").strip()
    if raw_cookie_header:
        session.headers["Cookie"] = raw_cookie_header

    retry = Retry(
        total=5,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def check_mediawiki_api_payload(data: Dict[str, object], context: str = "MediaWiki API") -> None:
    """Raise RuntimeError if MediaWiki returned an HTTP-200 application-level `error` object."""
    if not isinstance(data, dict):
        raise RuntimeError(f"{context} returned non-JSON object: {type(data)!r}")
    err = data.get("error")
    if err:
        code = err.get("code", "") if isinstance(err, dict) else ""
        if code == "readapidenied":
            raise RuntimeError(
                f"{context} returned `readapidenied` (authentication required to read this wiki). "
                "Either export MEDIAWIKI_BOT_USER & MEDIAWIKI_BOT_PASSWORD (from Special:BotPasswords), "
                "or export MEDIAWIKI_COOKIE / pass `--cookie-file cookies.txt` from an authenticated browser session."
            )
        raise RuntimeError(f"{context} error: {json.dumps(err, ensure_ascii=False)}")


def mw_api_request(
    session,
    api_url: str,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, str]] = None,
    method: str = "GET",
    timeout: int = 30,
    context: str = "MediaWiki API",
    max_app_retries: int = 5,
) -> Dict[str, Any]:
    """Execute a MediaWiki API call with TLS/proxy/SSO HTML diagnostics and `maxlag` HTTP-200 backoff."""
    import requests

    if not api_url.lower().startswith("https://"):
        raise ValueError(f"Refusing insecure non-HTTPS MediaWiki API URL: {api_url!r}")

    method_upper = method.upper()
    if method_upper not in {"GET", "POST"}:
        raise ValueError(f"Unsupported HTTP method for MediaWiki API: {method!r}")

    for attempt in range(1, max_app_retries + 1):
        try:
            if method_upper == "POST":
                resp = session.post(api_url, params=params, data=data, timeout=timeout)
            else:
                resp = session.get(api_url, params=params, timeout=timeout)
        except requests.exceptions.SSLError as ssl_err:
            raise RuntimeError(
                f"{context}: SSL certificate verification failed against Airbus internal PKI. "
                "Export MEDIAWIKI_CA_BUNDLE=/path/to/airbus-ca.crt (from https://github.airbus.corp/connectivity/airbus-ca) "
                "or run `pip install truststore` on your Airbus corporate machine."
            ) from ssl_err
        except requests.exceptions.ProxyError as proxy_err:
            raise RuntimeError(
                f"{context}: Corporate HTTP_PROXY/HTTPS_PROXY blocked access to internal host {api_url}. "
                "Re-run with `--bypass-proxy`."
            ) from proxy_err

        if resp.status_code in {401, 403, 407}:
            proxy_hint = (
                " If an external internet HTTPS_PROXY is intercepting `.airbus.corp`, re-run with `--bypass-proxy`."
                if getattr(session, "trust_env", True)
                else ""
            )
            raise RuntimeError(
                f"{context}: HTTP {resp.status_code} from {api_url}.{proxy_hint} "
                "If SSO authentication is required, set MEDIAWIKI_COOKIE / `--cookie-file` or MEDIAWIKI_BOT_USER."
            )

        resp.raise_for_status()

        content_type = (resp.headers.get("Content-Type") or "").lower()
        try:
            payload = resp.json()
        except ValueError as json_err:
            if "text/html" in content_type or resp.text.lstrip().startswith("<"):
                raise RuntimeError(
                    f"{context}: Received HTML instead of JSON from {api_url} (Content-Type={content_type!r}). "
                    "This usually means an Airbus SSO gateway redirected the unauthenticated request to a login page. "
                    "Export MEDIAWIKI_COOKIE / pass `--cookie-file cookies.txt` or configure MEDIAWIKI_BOT_USER & MEDIAWIKI_BOT_PASSWORD."
                ) from json_err
            raise RuntimeError(
                f"{context}: Invalid non-JSON response from {api_url} (Content-Type={content_type!r})."
            ) from json_err

        if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
            err_code = str(payload["error"].get("code") or "")
            if err_code in _RETRYABLE_MW_ERROR_CODES and attempt < max_app_retries:
                retry_hdr = resp.headers.get("Retry-After", "")
                try:
                    wait_secs = min(max(float(retry_hdr), 1.0), 30.0)
                except ValueError:
                    wait_secs = min(2.0 ** attempt, 30.0)
                print(
                    f"  [{context}] Transient MediaWiki `{err_code}` (attempt {attempt}/{max_app_retries}); "
                    f"backing off {wait_secs:.1f}s..."
                )
                time.sleep(wait_secs)
                continue

        check_mediawiki_api_payload(payload, context=context)
        return payload

    raise RuntimeError(f"{context}: Exhausted {max_app_retries} retries against {api_url}.")


def maybe_login_mediawiki(session, api_url: str, timeout: int = 30) -> None:
    """Authenticate once per session via MediaWiki BotPassword if MEDIAWIKI_BOT_USER & MEDIAWIKI_BOT_PASSWORD are set."""
    if getattr(session, "_mw_logged_in", False):
        return

    bot_user = os.environ.get("MEDIAWIKI_BOT_USER", "").strip()
    bot_password = os.environ.get("MEDIAWIKI_BOT_PASSWORD", "").strip()
    if not bot_user or not bot_password:
        return

    tok_data = mw_api_request(
        session,
        api_url,
        params={"action": "query", "meta": "tokens", "type": "login", "format": "json"},
        method="GET",
        timeout=timeout,
        context="MediaWiki login token fetch",
    )
    login_token = tok_data.get("query", {}).get("tokens", {}).get("logintoken")
    if not login_token:
        raise RuntimeError("Failed to retrieve MediaWiki logintoken.")

    login_data = mw_api_request(
        session,
        api_url,
        data={
            "action": "login",
            "lgname": bot_user,
            "lgpassword": bot_password,
            "lgtoken": login_token,
            "format": "json",
        },
        method="POST",
        timeout=timeout,
        context="MediaWiki login",
    )
    result = login_data.get("login", {}).get("result")
    if result != "Success":
        reason = login_data.get("login", {}).get("reason", "unknown reason")
        raise RuntimeError(f"MediaWiki login failed (result={result!r}): {reason}")

    setattr(session, "_mw_logged_in", True)
    print(f"Authenticated to MediaWiki API as {bot_user}")


def merge_mediawiki_page_payload(target: Dict[str, Any], incoming: Dict[str, Any]) -> None:
    """Merge multi-continuation MediaWiki `prop` payloads (`revisions`, `categories`, `fullurl`) cleanly."""
    for key, value in incoming.items():
        if key == "revisions":
            if value:
                target["revisions"] = value
        elif key == "categories":
            existing_cats = target.setdefault("categories", [])
            seen_titles = {c.get("title") for c in existing_cats if isinstance(c, dict)}
            for cat in value or []:
                if isinstance(cat, dict) and cat.get("title") not in seen_titles:
                    existing_cats.append(cat)
                    seen_titles.add(cat.get("title"))
        else:
            target[key] = value


def backfill_missing_revisions(
    session,
    api_url: str,
    pages_dict: Dict[str, Dict[str, Any]],
    timeout: int = 30,
    context: str = "MediaWiki single-page revision backfill",
) -> int:
    """Fetch individual `pageids=<pid>` for any valid page whose revision was omitted/truncated in a 50-page batch."""
    missing_pids = [
        str(pid)
        for pid, pdata in pages_dict.items()
        if int(pdata.get("pageid", pid)) > 0
        and "missing" not in pdata
        and not pdata.get("revisions")
    ]
    if not missing_pids:
        return 0

    recovered = 0
    for pid in missing_pids:
        active_clcontinue: Optional[str] = None
        while True:
            req_params: Dict[str, str] = {
                "action": "query",
                "pageids": pid,
                "prop": "revisions|categories|info",
                "rvprop": "ids|timestamp|user|sha1|content",
                "rvslots": "main",
                "cllimit": "max",
                "inprop": "url",
                "continue": "",
                "format": "json",
            }
            if active_clcontinue:
                req_params["clcontinue"] = active_clcontinue

            data = mw_api_request(
                session,
                api_url,
                params=req_params,
                method="GET",
                timeout=timeout,
                context=f"{context} (pageid={pid})",
            )
            fetched_page = data.get("query", {}).get("pages", {}).get(pid)
            if isinstance(fetched_page, dict):
                merge_mediawiki_page_payload(pages_dict[pid], fetched_page)

            cont = data.get("continue") or {}
            if "clcontinue" in cont:
                active_clcontinue = str(cont["clcontinue"])
            else:
                break

        if pages_dict[pid].get("revisions"):
            recovered += 1

    return recovered


def make_rfc1034_id(raw_value: object, fallback: str = "doc-0") -> str:
    """Build a deterministic RFC-1034-compliant document ID (1-63 chars, [a-z0-9-] only).

    Required by Vertex AI Search BigQuerySource (`idField`) and compatible with GcsSource.
    """
    raw_str = strip_invisible_chars(str(raw_value or fallback)).strip().lower()
    slug = re.sub(r"[^a-z0-9-]+", "-", raw_str)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    if not slug:
        slug = fallback.lower()
    if not slug[0].isalnum():
        slug = f"d-{slug}".strip("-")
    if len(slug) > 63:
        digest = hashlib.sha256(raw_str.encode("utf-8")).hexdigest()[:8]
        slug = f"{slug[:54].rstrip('-')}-{digest}"
    return slug


def estimate_tokens(text: str) -> int:
    """Approximate LLM/embedding token count (~1.3x whitespace-delimited tokens)."""
    return int(len(re.findall(r"\S+", text or "")) * 1.3)


def canonical_wiki_uri(title: str, base_url: str = WIKI_BASE_ARTICLE_URL) -> str:
    clean_title = strip_invisible_chars(title).strip().replace(" ", "_")
    return f"{base_url}{quote(clean_title, safe=':_/-().')}"


def normalize_title_key(title: str) -> str:
    """Case-insensitive normalized title key (used as secondary fallback when exact MediaWiki casing doesn't match)."""
    cleaned = strip_invisible_chars(title or "").strip().lstrip(":").replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.lower()


def mw_canonical_title_key(title: str) -> str:
    """MediaWiki `first-letter` canonical title key: case-insensitive namespace + capitalized first title letter,
    preserving case on remaining characters so `Main Page` (id=1) and `Main page` (id=10002) stay distinct!
    """
    cleaned = strip_invisible_chars(title or "").strip().lstrip(":").replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        return ""

    if ":" in cleaned:
        ns_part, rest = cleaned.split(":", 1)
        ns_norm = ns_part.strip().lower()
        rest = rest.strip()
        if rest:
            rest_canon = rest[0].upper() + rest[1:]
            return f"{ns_norm}:{rest_canon}"
        return ns_norm

    return cleaned[0].upper() + cleaned[1:]


def normalize_template_key(name: str) -> str:
    cleaned = strip_invisible_chars(name or "").strip()
    if cleaned.startswith(":"):
        cleaned = cleaned[1:].strip()
    if cleaned.lower().startswith("template:"):
        cleaned = cleaned[len("template:") :].strip()
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.lower()


def clean_category_name(raw_cat: str) -> str:
    part = raw_cat.split("|", 1)[0]
    part = strip_invisible_chars(part)
    part = re.sub(r"^(?i:category)\s*:\s*", "", part)
    part = part.replace("_", " ")
    part = re.sub(r"\s+", " ", part).strip()
    return part


def _strip_comments_and_nowiki(wikitext: str) -> str:
    text = re.sub(r"<!--.*?-->", "", wikitext, flags=re.DOTALL)
    text = re.sub(r"<nowiki\b[^>]*>.*?</nowiki>", "", text, flags=re.IGNORECASE | re.DOTALL)
    return text


def extract_transcludable_wikitext(template_wikitext: str) -> Tuple[str, str]:
    """Return (transcludable_body, non_noinclude_text) following MediaWiki transclusion rules."""
    if not template_wikitext:
        return "", ""

    non_noinclude = re.sub(
        r"<noinclude\b[^>]*>.*?</noinclude>",
        "",
        template_wikitext,
        flags=re.IGNORECASE | re.DOTALL,
    )
    onlyinclude_blocks = re.findall(
        r"<onlyinclude\b[^>]*>(.*?)</onlyinclude>",
        template_wikitext,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if onlyinclude_blocks:
        active = "\n".join(block.strip() for block in onlyinclude_blocks if block.strip())
    else:
        active = non_noinclude

    active = re.sub(
        r"</?includeonly\b[^>]*>",
        "",
        active,
        flags=re.IGNORECASE,
    )
    non_noinclude = re.sub(
        r"</?(?:includeonly|onlyinclude)\b[^>]*>",
        "",
        non_noinclude,
        flags=re.IGNORECASE,
    )
    return active.strip(), non_noinclude.strip()


def extract_explicit_categories(wikitext: str) -> List[str]:
    """Extract ordered, deduplicated [[Category:...]] names (ignoring [[:Category:...]] links)."""
    scannable = _strip_comments_and_nowiki(wikitext or "")
    scannable = re.sub(
        r"<noinclude\b[^>]*>.*?</noinclude>",
        "",
        scannable,
        flags=re.IGNORECASE | re.DOTALL,
    )
    seen = set()
    categories: List[str] = []
    for match in _CATEGORY_TAG_RE.finditer(scannable):
        cat = clean_category_name(match.group(1))
        if not cat:
            continue
        key = cat.lower()
        if key not in seen:
            seen.add(key)
            categories.append(cat)
    return categories


def strip_category_tags(wikitext: str) -> str:
    """Remove [[Category:...]] assignment tags while preserving [[:Category:...]] inline links."""
    return _CATEGORY_TAG_RE.sub("", wikitext or "")


def detect_redirect(wikitext: str) -> Tuple[bool, Optional[str]]:
    match = _REDIRECT_RE.match(wikitext or "")
    if not match:
        return False, None
    target = strip_invisible_chars(match.group(1)).replace("_", " ").strip()
    target = re.sub(r"\s+", " ", target)
    return (True, target) if target else (False, None)


def extract_transclusion_references(wikitext: str) -> Tuple[List[str], List[str]]:
    """Extract `(template_norm_keys, explicit_page_transclusions)` referenced in a page's wikitext."""
    if not wikitext:
        return [], []
    try:
        parsed = mwparserfromhell.parse(wikitext)
    except Exception:
        return [], []

    seen_tpls = set()
    ordered_tpls: List[str] = []
    seen_pages = set()
    ordered_pages: List[str] = []

    for tpl in parsed.filter_templates(recursive=True):
        raw_name = strip_invisible_chars(str(tpl.name)).strip()
        if not raw_name or raw_name.startswith("#"):
            continue

        # Leading `:` always transcludes a Main-namespace (or specified namespace) page, e.g. `{{:SA MLG Gears}}`
        if raw_name.startswith(":"):
            target_title = raw_name[1:].strip()
            if target_title:
                p_key = mw_canonical_title_key(target_title)
                if p_key not in seen_pages:
                    seen_pages.add(p_key)
                    ordered_pages.append(target_title)
            continue

        # Skip built-in 0-arg magic words ({{PAGENAME}}, {{NUMBEROFARTICLES}}, etc.)
        if not tpl.params and raw_name.lower() in _MEDIAWIKI_MAGIC_WORDS:
            continue

        # Cross-namespace transclusion (`{{Category:CPIOM}}`, `{{LGSIM:Header}}`) vs `{{Template:Foo}}`
        if ":" in raw_name and not raw_name.lower().startswith("template:"):
            ns_prefix = raw_name.split(":", 1)[0].strip().lower()
            if ns_prefix != "special":
                p_key = mw_canonical_title_key(raw_name)
                if p_key not in seen_pages:
                    seen_pages.add(p_key)
                    ordered_pages.append(raw_name)
                continue

        key = normalize_template_key(raw_name)
        if not key or key in seen_tpls:
            continue
        seen_tpls.add(key)
        ordered_tpls.append(key)

    return ordered_tpls, ordered_pages


def extract_referenced_template_keys(wikitext: str) -> List[str]:
    """Extract normalized Namespace-10 template names referenced in a page's wikitext."""
    tpl_keys, _ = extract_transclusion_references(wikitext)
    return tpl_keys


def merge_unique_strings(*iterables: Iterable[str]) -> List[str]:
    seen = set()
    merged: List[str] = []
    for group in iterables:
        for item in group:
            cleaned = strip_invisible_chars(item or "").strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                merged.append(cleaned)
    return merged


def load_all_template_records(template_dir: Path) -> List[TemplateRecord]:
    """Load every unique `Template:*` record from `wikimedia-data/templates/*.json` (without case-folding collisions)
    and resolve `#REDIRECT [[Template:...]]` chains so redirect templates inherit target body & categories.
    """
    if not template_dir.exists():
        return []

    resolved_dir = template_dir.resolve()
    by_unique_key: Dict[str, TemplateRecord] = {}

    for json_file in sorted(resolved_dir.glob("*.json")):
        if not json_file.is_file() or json_file.stat().st_size == 0:
            continue
        try:
            payload = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        title = strip_invisible_chars(str(payload.get("title") or "")).strip()
        if not title:
            continue
        raw_wt = str(payload.get("wikitext") or "")
        page_id_raw = payload.get("page_id") or 0
        try:
            page_id = int(page_id_raw)
        except (TypeError, ValueError):
            page_id = 0

        short_name = re.sub(r"^(?i:template)\s*:\s*", "", title).strip()
        norm_key = normalize_template_key(short_name)
        is_redir, redir_target = detect_redirect(raw_wt)
        transcludable, non_noinclude = extract_transcludable_wikitext(raw_wt)

        api_tpl_cats = [
            clean_category_name(c if isinstance(c, str) else c.get("title", ""))
            for c in (payload.get("categories") or [])
            if (isinstance(c, str) and c) or (isinstance(c, dict) and c.get("title"))
        ]
        all_own_cats = merge_unique_strings(api_tpl_cats, extract_explicit_categories(raw_wt))
        active_cats = (
            []
            if is_redir
            else [
                c
                for c in extract_explicit_categories(non_noinclude)
                if c.lower() not in _EXCLUDED_TEMPLATE_CATEGORIES
            ]
        )

        rec = TemplateRecord(
            page_id=page_id,
            title=title,
            name=short_name,
            norm_key=norm_key,
            raw_wikitext=raw_wt,
            transcludable_wikitext=transcludable,
            own_categories=all_own_cats,
            inherited_categories=active_cats,
            uri=str(payload.get("uri") or canonical_wiki_uri(title)),
            source_file=json_file.name,
            updated_at=payload.get("updated_at") or None,
            is_redirect=is_redir,
            redirect_target=redir_target,
        )
        uniq = f"id:{page_id}" if page_id > 0 else f"title:{mw_canonical_title_key(title)}"
        by_unique_key[uniq] = rec

    records = sorted(by_unique_key.values(), key=lambda t: (t.page_id, t.title))

    # Build canonical & lowercase lookup maps (preferring non-redirect templates) to resolve `#REDIRECT` chains
    by_canon: Dict[str, TemplateRecord] = {mw_canonical_title_key(r.title): r for r in records}
    by_norm: Dict[str, TemplateRecord] = {}
    for r in records:
        existing = by_norm.get(r.norm_key)
        if existing is None or (existing.is_redirect and not r.is_redirect):
            by_norm[r.norm_key] = r

    for r in records:
        if not r.is_redirect or not r.redirect_target:
            continue
        # Note: Some MediaWiki templates (e.g. `Template:Docmaster`) have BOTH `#REDIRECT` AND `<onlyinclude>` body!
        # Only overwrite transcludable_wikitext if the redirect stub has no `<onlyinclude>` body of its own.
        has_own_onlyinclude = "<onlyinclude" in r.raw_wikitext.lower()
        curr = r
        visited = {mw_canonical_title_key(r.title)}
        for _ in range(5):
            if not curr.is_redirect or not curr.redirect_target:
                break
            tgt_raw = curr.redirect_target
            if not tgt_raw.lower().startswith("template:") and ":" not in tgt_raw:
                tgt_full = f"Template:{tgt_raw}"
            else:
                tgt_full = tgt_raw
            c_key = mw_canonical_title_key(tgt_full)
            nxt = by_canon.get(c_key) or by_norm.get(normalize_template_key(tgt_raw))
            if nxt is None or mw_canonical_title_key(nxt.title) in visited:
                break
            visited.add(mw_canonical_title_key(nxt.title))
            curr = nxt

        if curr is not r and not curr.is_redirect:
            if not has_own_onlyinclude:
                r.transcludable_wikitext = curr.transcludable_wikitext
            r.inherited_categories = merge_unique_strings(
                r.inherited_categories, curr.inherited_categories
            )

    return records


def load_template_registry(template_dir: Path) -> Dict[str, TemplateRecord]:
    """Return normalized template lookup dictionary (preferring real templates over case-variant redirect stubs)."""
    registry: Dict[str, TemplateRecord] = {}
    for rec in load_all_template_records(template_dir):
        existing = registry.get(rec.norm_key)
        if existing is None or (existing.is_redirect and not rec.is_redirect):
            registry[rec.norm_key] = rec
        # Also index exact MediaWiki canonical short name (`first-letter` case)
        canon_short = mw_canonical_title_key(rec.name)
        if canon_short and canon_short not in registry:
            registry[canon_short] = rec
    return registry


def build_page_title_index(pages: Iterable[RawPageRecord]) -> Dict[str, RawPageRecord]:
    """Index pages by BOTH exact MediaWiki canonical title key AND lowercase fallback (preferring non-redirects)."""
    index: Dict[str, RawPageRecord] = {}
    for p in pages:
        c_key = mw_canonical_title_key(p.title)
        if c_key:
            existing_c = index.get(c_key)
            if existing_c is None or (existing_c.is_redirect and not p.is_redirect):
                index[c_key] = p

        l_key = normalize_title_key(p.title)
        if l_key:
            existing_l = index.get(l_key)
            if existing_l is None or (existing_l.is_redirect and not p.is_redirect):
                index[l_key] = p
    return index


def resolve_page_by_title(
    title: str,
    title_index: Dict[str, RawPageRecord],
    follow_redirects: bool = True,
) -> Optional[RawPageRecord]:
    """Look up a page by MediaWiki canonical title first, then case-insensitive fallback, following redirect chains."""
    if not title:
        return None
    curr = title_index.get(mw_canonical_title_key(title)) or title_index.get(normalize_title_key(title))
    if curr is None or not follow_redirects:
        return curr

    visited = {curr.page_id}
    for _ in range(5):
        if not curr.is_redirect or not curr.redirect_target:
            break
        nxt = (
            title_index.get(mw_canonical_title_key(curr.redirect_target))
            or title_index.get(normalize_title_key(curr.redirect_target))
        )
        if nxt is None or nxt.page_id in visited:
            break
        visited.add(nxt.page_id)
        curr = nxt
    return curr


def load_all_raw_pages(
    raw_dir: Path,
    template_registry: Optional[Dict[str, TemplateRecord]] = None,
) -> List[RawPageRecord]:
    """Load and enrich all raw MediaWiki pages across batch_*.json files (supporting multi-namespace & redirect chains)."""
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw MediaWiki directory not found: {raw_dir}")

    template_registry = template_registry or {}
    pages_by_id: Dict[int, RawPageRecord] = {}

    for batch_file in sorted(raw_dir.glob("batch_*.json")):
        payload = json.loads(batch_file.read_text(encoding="utf-8"))
        pages = payload.get("query", {}).get("pages", {})
        for raw_pid, page_data in pages.items():
            try:
                pid = int(page_data.get("pageid") or raw_pid)
            except (TypeError, ValueError):
                continue
            if pid <= 0 or "missing" in page_data:
                continue

            title = strip_invisible_chars(str(page_data.get("title") or pid)).strip()
            ns = int(page_data.get("ns", 0))
            revisions = page_data.get("revisions") or []
            wikitext = ""
            revid = None
            updated_at = None
            content_model = "wikitext"
            if revisions:
                rev0 = revisions[0]
                revid = rev0.get("revid")
                updated_at = rev0.get("timestamp")
                content_model = extract_revision_content_model(rev0)
                wikitext = extract_revision_wikitext(rev0)

            # Prefer existing record if it already has non-empty wikitext and new stub is empty
            existing = pages_by_id.get(pid)
            if existing and existing.wikitext and not wikitext:
                continue

            api_cats = [
                clean_category_name(c.get("title", ""))
                for c in (page_data.get("categories") or [])
                if isinstance(c, dict) and c.get("title")
            ]
            explicit_cats = extract_explicit_categories(wikitext) if content_model == "wikitext" else []
            tpl_keys, transcluded_pages = (
                extract_transclusion_references(wikitext) if content_model == "wikitext" else ([], [])
            )

            matched_tpl_names: List[str] = []
            inherited_cats: List[str] = []
            for key in tpl_keys:
                tpl_rec = template_registry.get(key)
                if tpl_rec:
                    matched_tpl_names.append(tpl_rec.name)
                    inherited_cats.extend(tpl_rec.inherited_categories)

            categories = merge_unique_strings(api_cats, explicit_cats, inherited_cats)
            is_redirect, redirect_target = (
                detect_redirect(wikitext) if content_model == "wikitext" else (False, None)
            )
            full_uri = page_data.get("fullurl") or canonical_wiki_uri(title)

            pages_by_id[pid] = RawPageRecord(
                page_id=pid,
                ns=ns,
                title=title,
                wikitext=wikitext,
                batch_file=batch_file.name,
                revid=revid,
                updated_at=updated_at,
                content_model=content_model,
                uri=full_uri,
                is_redirect=is_redirect,
                redirect_target=redirect_target,
                categories=categories,
                referenced_template_names=matched_tpl_names,
                referenced_page_titles=transcluded_pages,
            )

    ordered_pages = [pages_by_id[pid] for pid in sorted(pages_by_id)]
    title_index = build_page_title_index(ordered_pages)

    # Resolve multi-hop redirect chains (`A -> B -> C`) onto the final non-redirect target page
    # without self-aliasing case-variant stubs (`Main page` -> `Main Page`), and propagate redirect categories!
    aliases_by_pid: Dict[int, List[str]] = {}
    redirect_cats_by_pid: Dict[int, List[str]] = {}

    for rec in ordered_pages:
        if not rec.is_redirect or not rec.redirect_target:
            continue
        target_rec = resolve_page_by_title(rec.redirect_target, title_index, follow_redirects=True)
        if target_rec is None or target_rec.page_id == rec.page_id or target_rec.is_redirect:
            continue
        if rec.title != target_rec.title:
            aliases_by_pid.setdefault(target_rec.page_id, []).append(rec.title)
        if rec.categories:
            redirect_cats_by_pid.setdefault(target_rec.page_id, []).extend(rec.categories)

    for rec in ordered_pages:
        if rec.page_id in aliases_by_pid:
            rec.aliases = sorted(set(aliases_by_pid[rec.page_id]))
        if rec.page_id in redirect_cats_by_pid:
            rec.categories = merge_unique_strings(rec.categories, redirect_cats_by_pid[rec.page_id])

    return ordered_pages
