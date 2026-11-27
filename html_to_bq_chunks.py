"""Convert `action=parse` rendered MediaWiki HTML pages into clean ~450-token Markdown chunks for BigQuery (Option 2).

Reads the exact `pages/*.html` and `pages_manifest.json` produced by `crawl_parse_html.py` (no second MediaWiki crawl),
parses HTML using Python's standard-library `html.parser` (zero extra dependencies), strips navboxes/TOC/CSS clutter,
preserves clean Markdown tables & heading breadcrumbs (`H2 > H3`) with exact `#section` anchor URIs, and emits the
standard 18-column BigQuery JSONL schema (`chunks_bq.jsonl`) + Vertex struct JSONL (`chunks-for-vertexai.jsonl`).
"""

from __future__ import annotations

import argparse
import importlib
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

from wiki_utils import (
    estimate_tokens,
    get_logger,
    make_rfc1034_id,
    safe_child_path,
    strip_invisible_chars,
    to_vertex_struct_doc,
)

_step3 = importlib.import_module("3_chunk_pages")

logger = get_logger("html_to_bq_chunks")
ROOT = Path(__file__).resolve().parent
DEFAULT_PARSED_DIR = ROOT / "wikimedia-data" / "parsed_html"

_SKIP_TAGS = frozenset({"head", "script", "style", "noscript", "svg", "iframe", "object", "embed"})
_SKIP_CLASSES = frozenset(
    {
        "navbox",
        "navbox-inner",
        "mw-editsection",
        "toc",
        "noprint",
        "catlinks",
        "wiki-meta",
        "printfooter",
        "mw-empty-elt",
        "magnify",
    }
)
_HEADING_TAGS = frozenset({"h2", "h3", "h4", "h5", "h6"})
_VOID_TAGS = frozenset(
    {"area", "base", "br", "col", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)
_EXTERNAL_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")


@dataclass
class HtmlSection:
    breadcrumb: str
    anchor_id: str
    level: int
    markdown_body: str


class _TableBuilder:
    """Collects rows and cells for a single HTML `<table>` and renders clean Markdown."""

    def __init__(self) -> None:
        self.rows: List[List[Tuple[bool, str]]] = []
        self._cur_row: Optional[List[Tuple[bool, str]]] = None
        self._cur_cell_is_header: bool = False
        self._cur_cell_parts: Optional[List[str]] = None

    def start_row(self) -> None:
        self._cur_row = []

    def end_row(self) -> None:
        if self._cur_cell_parts is not None:
            self.end_cell()
        if self._cur_row is not None and any(cell_txt for _, cell_txt in self._cur_row):
            self.rows.append(self._cur_row)
        self._cur_row = None

    def start_cell(self, is_header: bool) -> None:
        if self._cur_row is None:
            self.start_row()
        self._cur_cell_is_header = is_header
        self._cur_cell_parts = []

    def append_text(self, text: str) -> None:
        if self._cur_cell_parts is not None:
            self._cur_cell_parts.append(text)

    def end_cell(self) -> None:
        if self._cur_cell_parts is None:
            return
        raw = "".join(self._cur_cell_parts)
        cleaned = re.sub(r"\s+", " ", raw).strip()
        if self._cur_row is not None:
            self._cur_row.append((self._cur_cell_is_header, cleaned))
        self._cur_cell_parts = None

    def render_markdown(self) -> str:
        if not self.rows:
            return ""

        # Case 1: Single-row 2-column MediaWiki GlossaryEntry / definition table -> compact bullet
        if len(self.rows) == 1 and len(self.rows[0]) == 2:
            left = self.rows[0][0][1].strip("* ")
            right = self.rows[0][1][1]
            if left and right:
                return f"\n- **{left}**: {right}\n"
            return f"\n{left or right}\n"

        # Case 2: Single-column layout wrapper table -> unwrap cleanly as paragraphs
        max_cols = max(len(r) for r in self.rows)
        if max_cols <= 1:
            lines = [r[0][1] for r in self.rows if r and r[0][1]]
            return "\n\n" + "\n\n".join(lines) + "\n\n" if lines else ""

        # Case 3: Multi-row 2-column key-value table with NO <th> headers -> clean bullet list
        has_any_th = any(is_h for r in self.rows for is_h, _ in r)
        if max_cols == 2 and not has_any_th and all(len(r) == 2 for r in self.rows):
            bullets = []
            for (_, k), (_, v) in self.rows:
                k_clean = k.strip("* ")
                if k_clean and v:
                    bullets.append(f"- **{k_clean}**: {v}")
                elif k_clean or v:
                    bullets.append(f"- {k_clean or v}")
            return "\n" + "\n".join(bullets) + "\n"

        # Case 4: Standard multi-row data table -> ONE header row + ONE `| --- |` separator + data rows
        def _esc(cell: str) -> str:
            return cell.replace("|", "\\|").strip()

        normalized = []
        for r in self.rows:
            texts = [_esc(txt) for _, txt in r]
            while len(texts) < max_cols:
                texts.append("")
            normalized.append(texts)

        header = normalized[0]
        sep = ["---"] * max_cols
        body_rows = normalized[1:]
        md_lines = [
            "| " + " | ".join(header) + " |",
            "| " + " | ".join(sep) + " |",
            *("| " + " | ".join(r) + " |" for r in body_rows),
        ]
        return "\n\n" + "\n".join(md_lines) + "\n\n"


class _MediaWikiHtmlToSectionsParser(HTMLParser):
    """Stream-parse MediaWiki rendered HTML into hierarchical Markdown sections with exact `#anchor` IDs."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.sections: List[HtmlSection] = []
        self._heading_stack: List[Tuple[int, str, str]] = []  # [(level, text, anchor_id)]
        self._cur_parts: List[str] = []
        self._cur_level: int = 1
        self._cur_breadcrumb: str = "Overview"
        self._cur_anchor: str = ""

        # Depth trackers
        self._skip_stack: List[str] = []
        self._in_h1: bool = False
        self._in_heading_level: Optional[int] = None
        self._heading_parts: List[str] = []
        self._heading_anchor: str = ""
        self._in_pre: bool = False
        self._pre_parts: List[str] = []
        self._list_stack: List[Tuple[str, int]] = []  # [("ul"|"ol", counter)]
        self._link_stack: List[Optional[str]] = []  # external href or None
        self._link_text_parts: List[List[str]] = []
        self._table_stack: List[_TableBuilder] = []

    def _emit(self, text: str) -> None:
        if not text:
            return
        if self._in_heading_level is not None:
            self._heading_parts.append(text)
        elif self._in_pre:
            self._pre_parts.append(text)
        elif self._link_text_parts:
            self._link_text_parts[-1].append(text)
        elif self._table_stack:
            self._table_stack[-1].append_text(text)
        else:
            self._cur_parts.append(text)

    def _flush_section(self) -> None:
        raw = "".join(self._cur_parts)
        cleaned = self._normalize_markdown(raw)
        if cleaned:
            self.sections.append(
                HtmlSection(
                    breadcrumb=self._cur_breadcrumb,
                    anchor_id=self._cur_anchor,
                    level=self._cur_level,
                    markdown_body=cleaned,
                )
            )
        self._cur_parts = []

    @staticmethod
    def _normalize_markdown(raw: str) -> str:
        lines = [strip_invisible_chars(ln).rstrip() for ln in raw.splitlines()]
        out_lines: List[str] = []
        in_fence = False
        for ln in lines:
            if ln.strip().startswith("```"):
                in_fence = not in_fence
                out_lines.append(ln)
                continue
            if in_fence:
                out_lines.append(ln)
            else:
                out_lines.append(_MULTI_SPACE_RE.sub(" ", ln).strip())
        joined = "\n".join(out_lines)
        joined = _MULTI_NEWLINE_RE.sub("\n\n", joined)
        return joined.strip()

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        lower = tag.lower()
        attr_map = {k.lower(): (v or "") for k, v in attrs}
        classes = set(attr_map.get("class", "").split())

        if self._skip_stack:
            if lower not in _VOID_TAGS:
                self._skip_stack.append(lower)
            return

        if lower in _SKIP_TAGS or (classes & _SKIP_CLASSES):
            if lower not in _VOID_TAGS:
                self._skip_stack.append(lower)
            return

        if lower == "h1":
            self._in_h1 = True
            return
        if self._in_h1:
            return

        if lower in _HEADING_TAGS:
            self._flush_section()
            self._in_heading_level = int(lower[1])
            self._heading_parts = []
            self._heading_anchor = attr_map.get("id", "").strip()
            return

        if self._in_heading_level is not None:
            if not self._heading_anchor and attr_map.get("id"):
                self._heading_anchor = attr_map["id"].strip()
            return

        if lower == "pre":
            self._in_pre = True
            self._pre_parts = []
            return

        if lower == "table":
            self._table_stack.append(_TableBuilder())
            return
        if self._table_stack:
            if lower == "tr":
                self._table_stack[-1].start_row()
                return
            if lower in ("th", "td"):
                self._table_stack[-1].start_cell(is_header=(lower == "th"))
                return

        if lower == "a":
            href = attr_map.get("href", "").strip()
            ext_href = href if _EXTERNAL_URL_RE.match(href) else None
            self._link_stack.append(ext_href)
            self._link_text_parts.append([])
            return

        if lower in ("strong", "b"):
            self._emit("**")
        elif lower in ("em", "i"):
            self._emit("*")
        elif lower == "code" and not self._in_pre:
            self._emit("`")
        elif lower == "br":
            self._emit("\n" if not self._table_stack else " ")
        elif lower in ("p", "div", "blockquote") and not self._table_stack:
            self._emit("\n\n")
        elif lower in ("ul", "ol") and not self._table_stack:
            self._list_stack.append((lower, 0))
            self._emit("\n")
        elif lower == "li" and not self._table_stack:
            indent = "  " * max(0, len(self._list_stack) - 1)
            if self._list_stack and self._list_stack[-1][0] == "ol":
                kind, num = self._list_stack[-1]
                self._list_stack[-1] = (kind, num + 1)
                self._emit(f"\n{indent}{num + 1}. ")
            else:
                self._emit(f"\n{indent}- ")
        elif lower == "dt" and not self._table_stack:
            self._emit("\n- **")
        elif lower == "dd" and not self._table_stack:
            self._emit(": ")
        elif lower == "hr" and not self._table_stack:
            self._emit("\n\n---\n\n")

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()

        if self._skip_stack:
            if lower in self._skip_stack:
                while self._skip_stack:
                    popped = self._skip_stack.pop()
                    if popped == lower:
                        break
            return

        if lower == "h1":
            self._in_h1 = False
            return
        if self._in_h1:
            return

        if self._in_heading_level is not None and lower == f"h{self._in_heading_level}":
            level = self._in_heading_level
            heading_text = re.sub(r"\s+", " ", strip_invisible_chars("".join(self._heading_parts))).strip()
            anchor = self._heading_anchor or heading_text.replace(" ", "_")
            self._in_heading_level = None
            self._heading_parts = []
            self._heading_anchor = ""

            if heading_text:
                while self._heading_stack and self._heading_stack[-1][0] >= level:
                    self._heading_stack.pop()
                self._heading_stack.append((level, heading_text, anchor))
                self._cur_level = level
                self._cur_breadcrumb = " > ".join(item[1] for item in self._heading_stack)
                self._cur_anchor = anchor
            return

        if lower == "pre" and self._in_pre:
            code_body = "".join(self._pre_parts).strip("\n")
            self._in_pre = False
            self._pre_parts = []
            if code_body.strip():
                self._emit(f"\n\n```\n{code_body}\n```\n\n")
            return

        if lower == "a" and self._link_stack:
            ext_href = self._link_stack.pop()
            link_text = re.sub(r"\s+", " ", "".join(self._link_text_parts.pop())).strip()
            if ext_href:
                if link_text and link_text != ext_href:
                    self._emit(f"{link_text} ({ext_href})")
                else:
                    self._emit(ext_href)
            elif link_text:
                self._emit(link_text)
            return

        if self._table_stack:
            if lower in ("th", "td"):
                self._table_stack[-1].end_cell()
                return
            if lower == "tr":
                self._table_stack[-1].end_row()
                return
            if lower == "table":
                finished = self._table_stack.pop()
                rendered = finished.render_markdown()
                if rendered:
                    self._emit(rendered)
                return

        if lower in ("strong", "b"):
            self._emit("**")
        elif lower in ("em", "i"):
            self._emit("*")
        elif lower == "code" and not self._in_pre:
            self._emit("`")
        elif lower == "dt" and not self._table_stack:
            self._emit("**")
        elif lower in ("ul", "ol") and self._list_stack:
            self._list_stack.pop()
            self._emit("\n")
        elif lower in ("p", "div", "blockquote") and not self._table_stack:
            self._emit("\n\n")

    def handle_data(self, data: str) -> None:
        if self._skip_stack or self._in_h1:
            return
        if self._in_pre:
            self._pre_parts.append(data)
        else:
            self._emit(data)

    def finalize(self) -> List[HtmlSection]:
        self._flush_section()
        return self.sections


def parse_html_into_sections(html_text: str) -> List[HtmlSection]:
    """Parse rendered MediaWiki HTML into ordered hierarchical Markdown sections with `#anchor` IDs."""
    parser = _MediaWikiHtmlToSectionsParser()
    parser.feed(html_text or "")
    parser.close()
    return parser.finalize()


def _build_section_uri(base_uri: str, anchor_id: str) -> str:
    clean_base = (base_uri or "").split("#", 1)[0]
    clean_anchor = strip_invisible_chars(anchor_id or "").strip()
    if not clean_base or not clean_anchor:
        return clean_base
    return f"{clean_base}#{quote(clean_anchor, safe='-_.():/')}"


def _build_chunk_header(
    page_title: str,
    section_breadcrumb: str,
    aliases: Sequence[str],
    categories: Sequence[str],
) -> str:
    lines = [f"Page: {page_title}"]
    if aliases:
        lines.append(f"Aliases: {', '.join(aliases)}")
    lines.append(f"Section: {section_breadcrumb}")
    if categories:
        lines.append(f"Categories: {', '.join(categories)}")
    return "\n".join(lines)


def chunk_parsed_html_page(
    page_meta: Dict[str, Any],
    html_text: str,
    max_tokens: int = 450,
    overlap_tokens: int = 50,
    min_merge_tokens: int = 90,
) -> List[Dict[str, Any]]:
    """Convert one parsed HTML page into ~450-token BigQuery chunk rows matching the 18-column schema."""
    page_id = int(page_meta["page_id"])
    page_title = strip_invisible_chars(str(page_meta.get("title") or "")).strip()
    aliases = list(page_meta.get("aliases") or [])
    categories = list(page_meta.get("categories") or [])
    base_uri = str(page_meta.get("uri") or "")
    ns = int(page_meta.get("ns", 0))
    updated_at = page_meta.get("updated_at")
    source_file = str(page_meta.get("html_file") or f"page-{page_id}.html")

    sections = parse_html_into_sections(html_text)
    if not sections:
        return []

    # Merge tiny consecutive sub-sections under a reasonable ceiling without swallowing large sections
    merged_sections: List[HtmlSection] = []
    for sec in sections:
        if (
            merged_sections
            and estimate_tokens(merged_sections[-1].markdown_body) < min_merge_tokens
            and estimate_tokens(merged_sections[-1].markdown_body) + estimate_tokens(sec.markdown_body) <= max_tokens
            and merged_sections[-1].level <= sec.level
        ):
            prev = merged_sections[-1]
            sub_hdr = f"### {sec.breadcrumb.split(' > ')[-1]}\n\n" if sec.breadcrumb != prev.breadcrumb else ""
            combined_bc = prev.breadcrumb if "(+ subsections)" in prev.breadcrumb else f"{prev.breadcrumb} (+ subsections)"
            merged_sections[-1] = HtmlSection(
                breadcrumb=combined_bc,
                anchor_id=prev.anchor_id or sec.anchor_id,
                level=prev.level,
                markdown_body=f"{prev.markdown_body}\n\n{sub_hdr}{sec.markdown_body}".strip(),
            )
        else:
            merged_sections.append(sec)

    rows: List[Dict[str, Any]] = []
    chunk_idx = 0

    for sec in merged_sections:
        header = _build_chunk_header(page_title, sec.breadcrumb, aliases, categories)
        header_tokens = estimate_tokens(header) + 4
        body_budget = max(120, max_tokens - header_tokens)
        windows = _step3.split_section_into_windows(
            sec.markdown_body,
            max_tokens=body_budget,
        )
        section_uri = _build_section_uri(base_uri, sec.anchor_id)

        for win_num, window_text in enumerate(windows, start=1):
            clean_win = window_text.strip()
            if not clean_win:
                continue
            chunk_idx += 1
            part_suffix = f" (part {win_num}/{len(windows)})" if len(windows) > 1 else ""
            full_content = f"{header}\n\n{clean_win}"
            chunk_id = make_rfc1034_id(f"html-chunk-{page_id}-{chunk_idx:03d}")
            rows.append(
                {
                    "id": chunk_id,
                    "doc_type": "html_manual_chunk",
                    "ns": ns,
                    "title": f"{page_title} — {sec.breadcrumb}{part_suffix}",
                    "content": full_content,
                    "page_id": page_id,
                    "page_title": page_title,
                    "section_title": sec.breadcrumb,
                    "chunk_type": _step3.classify_chunk(clean_win),
                    "categories": categories,
                    "referenced_templates": [],
                    "aliases": aliases,
                    "is_redirect": False,
                    "redirect_target": None,
                    "uri": section_uri,
                    "source_file": source_file,
                    "token_count": estimate_tokens(full_content),
                    "updated_at": updated_at,
                }
            )

    return rows


def build_html_chunks_dataset(
    parsed_dir: Path = DEFAULT_PARSED_DIR,
    max_tokens: int = 450,
    overlap_tokens: int = 50,
) -> Dict[str, Any]:
    """Convert all crawled HTML pages in `parsed_dir` into `chunks_bq.jsonl` and `chunks-for-vertexai.jsonl`."""
    manifest_path = parsed_dir / "pages_manifest.json"
    pages_dir = parsed_dir / "pages"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing parsed HTML manifest: {manifest_path} (run crawl_parse_html.py first)")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pages = manifest.get("pages", [])

    bq_out_path = parsed_dir / "chunks_bq.jsonl"
    vertex_out_path = parsed_dir / "chunks-for-vertexai.jsonl"

    total_chunks = 0
    pages_chunked = 0
    total_tokens = 0

    with (
        bq_out_path.open("w", encoding="utf-8") as f_bq,
        vertex_out_path.open("w", encoding="utf-8") as f_vtx,
    ):
        for page_meta in pages:
            html_name = str(page_meta.get("html_file") or f"page-{page_meta['page_id']}.html")
            html_path = safe_child_path(pages_dir, html_name)
            if not html_path.is_file():
                logger.warning("Missing HTML file %s for page_id=%s; skipping.", html_path, page_meta.get("page_id"))
                continue

            html_text = html_path.read_text(encoding="utf-8", errors="replace")
            page_chunks = chunk_parsed_html_page(
                page_meta=page_meta,
                html_text=html_text,
                max_tokens=max_tokens,
                overlap_tokens=overlap_tokens,
            )
            if page_chunks:
                pages_chunked += 1
            for row in page_chunks:
                f_bq.write(json.dumps(row, ensure_ascii=False) + "\n")
                f_vtx.write(json.dumps(to_vertex_struct_doc(row), ensure_ascii=False) + "\n")
                total_chunks += 1
                total_tokens += int(row["token_count"])

    avg_tokens = round(total_tokens / total_chunks, 1) if total_chunks else 0.0
    summary = {
        "pages_chunked": pages_chunked,
        "total_chunks": total_chunks,
        "avg_tokens": avg_tokens,
        "chunks_bq_path": str(bq_out_path),
        "chunks_vertex_path": str(vertex_out_path),
    }
    logger.info(
        "Built Option 2 HTML->Markdown chunks: %d chunks across %d page(s) (avg %.1f tokens/chunk) -> %s",
        total_chunks,
        pages_chunked,
        avg_tokens,
        bq_out_path,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert action=parse MediaWiki HTML files into clean ~450-token BigQuery Markdown chunks (Option 2)."
    )
    parser.add_argument("--parsed-dir", type=Path, default=DEFAULT_PARSED_DIR)
    parser.add_argument("--max-tokens", type=int, default=450, help="Target max tokens per chunk (default: 450).")
    parser.add_argument("--overlap-tokens", type=int, default=50, help="Overlap tokens when splitting long sections.")
    args = parser.parse_args()

    build_html_chunks_dataset(
        parsed_dir=args.parsed_dir,
        max_tokens=args.max_tokens,
        overlap_tokens=args.overlap_tokens,
    )


if __name__ == "__main__":
    main()
