"""Option 2 Step 2: Clean & expand MediaWiki wikitext into structured Markdown (preserving templates, tables, headings, filenames, and categories).

Key improvements over original `2_clean_wikitext.py`:
1. Expands MediaWiki templates (`Actuator`, `ECM`, `DMS`, `GlossaryEntry`, `Infobox Equipment`,
   `EquipmentBox`, `A380FuelEquip`, `A3Report`, `IconBox`, `Note`, `ColouredBox`, `TechRequest`,
   `Docmaster`, `Normmaster`, `LgSimFramework`, main/custom-namespace transclusions, 0-arg content
   templates, and generic key-value templates) — INCLUDING templates nested inside `<ref>`, `<table>`,
   and `<div>` HTML tags while leaving `<math>` and `<syntaxhighlight>` blocks untouched!
2. Preserves all filenames, script names (`.m`, `.c`, `.bat`, `.pdf`), document IDs, and URLs so
   Option 1 and Option 2 share identical factual identifiers for a fair Golden Dataset comparison.
3. Converts MediaWiki `{| ... |}` tables (including unclosed `{|` layout tables) and HTML `<table>`
   infoboxes into clean Markdown pipe tables.
4. Converts MediaWiki `#` numbered lists BEFORE converting `== Heading ==` into Markdown `## Heading`
   so section hierarchy survives intact for `3_chunk_pages.py` (and wraps CSS/JS/JSON content models
   in code blocks instead of mangling `#selector` lines).
5. Extracts explicit + template-inherited `categories` and multi-hop redirect `aliases` into structured JSON frontmatter.
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from wiki_utils import (
    RawPageRecord,
    TemplateRecord,
    build_page_title_index,
    load_all_raw_pages,
    load_template_registry,
    mwparserfromhell,
    normalize_template_key,
    resolve_page_by_title,
    safe_child_path,
    strip_category_tags,
    strip_invisible_chars,
)

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "wikimedia-data" / "raw_api"
TEMPLATE_DIR = ROOT / "wikimedia-data" / "templates"
PAGES_DIR = ROOT / "wikimedia-data" / "processed" / "pages"

# Pure layout / formatting / citation-list / redirect-category templates with no standalone semantic text
_DECORATIVE_TEMPLATES = {
    "clear",
    "-",
    "toc limit",
    "toclimit",
    "alphatoc",
    "reflist",
    "notelist",
    "!",
    "!!",
    "!-",
    "(!",
    "!)",
    "=",
    "pipe",
    "braces",
    "brackets",
    "pad",
    "col-begin",
    "col-begin-small",
    "col-break",
    "col-end",
    "col-float",
    "col-float-break",
    "col-float-end",
    "col-2",
    "col-3",
    "col-4",
    "col-5",
    "columns-start",
    "columns-end",
    "div col",
    "div col end",
    "hidden begin",
    "hidden end",
    "hide category pages",
    "citation needed",
    "fix",
    "elucidate",
    "unc",
    "numberofusers",
    "numberofarticles",
    "pagename",
    "fullpagename",
    "subpagename",
    "basepagename",
    "namespace",
    "currentyear",
    "currentmonth",
    "currentday",
    "redirect category shell",
    "rcat shell",
    "rcatsh",
    "r from move",
    "r from template shortcut",
    "r related",
    "r tsh",
    "r msp",
    "documentation",
}

# Tags whose inner text must NOT be parsed for MediaWiki templates (e.g. LaTeX math `^{2}}` or MATLAB cell `{{a}}`)
_OPAQUE_CODE_MATH_TAGS = {
    "math",
    "chem",
    "nowiki",
    "pre",
    "syntaxhighlight",
    "source",
    "code",
}

_IMAGE_OPTIONS_RE = re.compile(
    r"^(?:thumb|thumbnail|frameless|frame|framed|border|right|left|center|centre|none|baseline|middle|sub|super|text-top|text-bottom|top|bottom|upright(?:\s*=\s*[0-9.]+)?|\d*x?\d+\s*px|alt\s*=.*|link\s*=.*|page\s*=.*|class\s*=.*)$",
    re.IGNORECASE,
)


def _get_param(tpl: mwparserfromhell.nodes.Template, *names: str, default: str = "") -> str:
    """Case-insensitively fetch the first matching non-empty parameter from a template."""
    for target in names:
        target_low = target.lower()
        for param in tpl.params:
            p_name = str(param.name).strip().lower()
            if p_name == target_low:
                val = str(param.value).strip()
                if val:
                    return val
    return default


def _clean_inline_wikitext(
    text: str,
    pages_by_title: Dict[str, RawPageRecord],
    template_registry: Dict[str, TemplateRecord],
    depth: int = 0,
) -> str:
    """Clean nested inline wikitext inside template parameters or table cells."""
    if not text:
        return ""
    expanded = expand_templates_in_wikitext(text, pages_by_title, template_registry, depth=depth + 1)
    expanded = _convert_file_tags(expanded)
    expanded = _convert_links_and_media(expanded)
    expanded = re.sub(r"'{2,5}", "", expanded)
    expanded = re.sub(r"</?br\s*/?>", "; ", expanded, flags=re.IGNORECASE)
    expanded = re.sub(r"</?[a-zA-Z][^>]*>", " ", expanded)
    expanded = html.unescape(expanded)
    expanded = re.sub(r"[ \t]{2,}", " ", expanded)
    return expanded.strip()


def _render_single_template(
    tpl: mwparserfromhell.nodes.Template,
    pages_by_title: Dict[str, RawPageRecord],
    template_registry: Dict[str, TemplateRecord],
    depth: int,
) -> str:
    raw_name = strip_invisible_chars(str(tpl.name)).strip()
    if not raw_name:
        return ""

    # Skip Special:* magic inclusions like {{Special:AllPages}}
    if raw_name.lower().startswith("special:"):
        return ""

    # Handle explicit page transclusion: `{{:Main Article}}` OR cross-namespace `{{LGSIM:Subpage}}` / `{{Category:CPIOM}}`
    is_colon_transclusion = raw_name.startswith(":")
    is_ns_page_transclusion = (
        not is_colon_transclusion
        and ":" in raw_name
        and not raw_name.lower().startswith("template:")
        and resolve_page_by_title(raw_name, pages_by_title, follow_redirects=True) is not None
    )
    if is_colon_transclusion or is_ns_page_transclusion:
        target_title = raw_name[1:].strip() if is_colon_transclusion else raw_name.strip()
        target_page = resolve_page_by_title(target_title, pages_by_title, follow_redirects=True)
        if target_page and not target_page.is_redirect and depth < 2:
            cleaned_sub = clean_wikitext_body(
                target_page.wikitext,
                pages_by_title=pages_by_title,
                template_registry=template_registry,
                depth=depth + 1,
                content_model=target_page.content_model,
            )
            return f"\n{cleaned_sub}\n"
        return f"See article: {target_title}"

    key = normalize_template_key(raw_name)
    if key in _DECORATIVE_TEMPLATES:
        return ""
    if raw_name.startswith("#"):
        # Basic support for inline {{#if: cond | then | else }} when left in page wikitext
        low_pf = raw_name.lower()
        if low_pf.startswith("#if:") and tpl.params:
            cond = raw_name[4:].strip()
            branch = str(tpl.params[0].value).strip() if cond else (str(tpl.params[1].value).strip() if len(tpl.params) > 1 else "")
            return _clean_inline_wikitext(branch, pages_by_title, template_registry, depth=depth) if branch else ""
        return ""

    def clean_val(v: str) -> str:
        return _clean_inline_wikitext(v, pages_by_title, template_registry, depth=depth)

    # 1. Document Reference Templates: ECM, DMS, Docmaster, Normmaster, TechRequest, IShare, ITKM
    if key in {"ecm", "dms", "docmaster", "normmaster", "techrequest", "ishare", "itkm"}:
        doc_id = clean_val(_get_param(tpl, "1", "id", "ref"))
        label = clean_val(_get_param(tpl, "2", "title", "text"))
        issue = clean_val(_get_param(tpl, "Issue", "issue"))
        system_label = {
            "ecm": "ECM",
            "dms": "DMS",
            "docmaster": "DocMaster",
            "normmaster": "NormMaster",
            "techrequest": "TechRequest",
            "ishare": "iShare",
            "itkm": "ITKM",
        }[key]
        issue_suffix = f", Issue {issue}" if issue else ""
        if label and doc_id and doc_id.lower() not in label.lower():
            return f"{label} ({system_label}: {doc_id}{issue_suffix})"
        if label:
            return f"{label} ({system_label}{issue_suffix})" if issue_suffix else label
        if doc_id:
            return f"{system_label} {doc_id}{issue_suffix}"
        return ""

    # 2. FileLink / DefinedLink / GlossaryLink / Main / See also / Further / Cl / Category link
    if key in {
        "filelink",
        "definedlink",
        "glossarylink",
        "main",
        "see also",
        "see",
        "further",
        "further information",
        "distinguish",
        "confused",
        "cl",
        "category link",
    }:
        parts = [clean_val(str(p.value)) for p in tpl.params if str(p.value).strip()]
        if not parts:
            return ""
        prefix = "Related: " if key in {"main", "see also", "see", "further", "further information"} else ""
        return f"{prefix}{', '.join(p for p in parts if p)}"

    # 3. GlossaryEntry (1,354 uses across the wiki!)
    if key == "glossaryentry":
        entry = clean_val(_get_param(tpl, "Entry", "entry", "1"))
        text = clean_val(_get_param(tpl, "Text", "text", "2"))
        if entry and text:
            return f"\n- **{entry}**: {text}\n"
        return entry or text

    # 4. Callouts & Framed Content Boxes: Note, Info, Exclamation, Caution, Notice, MsgBox, IconBox, ColouredBox, LgSimFramework, WindowTile
    if key in {
        "note",
        "info",
        "exclamation",
        "caution",
        "notice",
        "msgbox",
        "iconbox",
        "colouredbox",
        "colouredboxsimplified",
        "lgsimframework",
        "windowtile",
        "side box",
        "right side box",
    }:
        title = clean_val(_get_param(tpl, "title", "Title", "1"))
        link = clean_val(_get_param(tpl, "link", "Link"))
        raw_body = _get_param(tpl, "content", "Content", "text", "Text", "comment", "2")
        body = (
            clean_wikitext_body(raw_body, pages_by_title, template_registry, depth=depth + 1)
            if ("\n" in raw_body or "{|" in raw_body or "{{" in raw_body)
            else clean_val(raw_body)
        )
        header = f"{title} ({link})" if (title and link) else (title or link)
        if header and body:
            return f"\n**{header}**\n{body}\n"
        return f"\n{header or body}\n" if (header or body) else ""

    # 5. Wrapper / Inline Text Formatting Templates
    if key in {
        "columns-list",
        "plainlist",
        "flatlist",
        "unbulleted list",
        "hlist",
        "highlight",
        "highlighttext",
        "code",
        "mono",
        "small",
        "smaller",
        "smallcaps",
        "sm",
        "larger",
        "em",
        "sup",
        "quotation",
        "cquote",
        "color",
        "colour",
        "abbr",
        "define",
        "tl",
        "tlx",
        "tlc",
        "tc",
        "tn",
    }:
        unnamed_vals = [str(p.value).strip() for p in tpl.params if not p.showkey and str(p.value).strip()]
        target_raw = unnamed_vals[-1] if unnamed_vals else _get_param(tpl, "text", "content", "1")
        if "\n" in target_raw:
            return "\n" + clean_wikitext_body(target_raw, pages_by_title, template_registry, depth=depth + 1) + "\n"
        return clean_val(target_raw)

    # 6. Actuator Template (handles both `equipped_aircraft` and `equipped_aircrafts` parameter variants!)
    if key == "actuator":
        pn = clean_val(_get_param(tpl, "pn", "PN"))
        supplier = clean_val(_get_param(tpl, "supplier"))
        surface = clean_val(_get_param(tpl, "surface"))
        program = clean_val(_get_param(tpl, "program"))
        aircraft = clean_val(_get_param(tpl, "equipped_aircraft", "equipped_aircrafts"))
        fin = clean_val(_get_param(tpl, "FIN", "fin"))
        length = clean_val(_get_param(tpl, "length"))

        lines = []
        intro_parts = [f"The actuator {pn or 'unit'} is a Primary Flight Control Actuator (PFCA)"]
        if supplier:
            intro_parts.append(f"supplied by {supplier}")
        if surface or aircraft:
            surf_str = f"on {surface} surfaces" if surface else ""
            ac_str = f"of {aircraft} aircraft" if aircraft else ""
            intro_parts.append(" ".join(x for x in ["and is mounted", surf_str, ac_str] if x))
        lines.append(" ".join(intro_parts).strip() + ".")
        lines.append("\n=== Main Characteristics ===")
        for label, val in [
            ("Part Number (P/N)", pn),
            ("Supplier", supplier),
            ("Program", program),
            ("Equipped Aircraft", aircraft),
            ("FIN", fin),
            ("Length between fittings at 0 deg (mm)", length),
            ("Extension Side", clean_val(_get_param(tpl, "extension_side"))),
            ("Retraction Side", clean_val(_get_param(tpl, "retraction_side"))),
            ("Functional Stroke Extension (mm)", clean_val(_get_param(tpl, "stroke_func_ext_mm"))),
            ("Functional Stroke Extension (deg)", clean_val(_get_param(tpl, "stroke_func_ext_deg"))),
            ("Functional Stroke Retraction (mm)", clean_val(_get_param(tpl, "stroke_func_ret_mm"))),
            ("Functional Stroke Retraction (deg)", clean_val(_get_param(tpl, "stroke_func_ret_deg"))),
            ("Max Stroke to Stop Extension (mm)", clean_val(_get_param(tpl, "stroke_max_ext_mm"))),
            ("Max Stroke to Stop Extension (deg)", clean_val(_get_param(tpl, "stroke_max_ext_deg"))),
            ("Max Stroke to Stop Retraction (mm)", clean_val(_get_param(tpl, "stroke_max_ret_mm"))),
            ("Max Stroke to Stop Retraction (deg)", clean_val(_get_param(tpl, "stroke_max_ret_deg"))),
        ]:
            if val:
                lines.append(f"- **{label}**: {val}")
        return "\n" + "\n".join(lines) + "\n"

    # 7. Citation Templates (`Cite web`, `Cite report`, `Cite book`, `Cite journal`, etc.)
    if key.startswith("cite ") or key == "citation":
        pieces = []
        for field_name in ("author", "last", "title", "work", "publisher", "institution", "number", "id", "date", "year", "url"):
            val = clean_val(_get_param(tpl, field_name))
            if val:
                pieces.append(val)
        return "; ".join(pieces)

    # 8. Zero-parameter static content templates in `template_registry` (e.g. reusable banners/snippets)
    if not tpl.params and depth < 3:
        tpl_rec = template_registry.get(key)
        if tpl_rec and tpl_rec.transcludable_wikitext:
            tbody = tpl_rec.transcludable_wikitext
            if "{{#invoke:" not in tbody.lower() and "{{{" not in tbody:
                return clean_wikitext_body(tbody, pages_by_title, template_registry, depth=depth + 1)

    # 9. Generic Structured Infobox / Report / Key-Value Renderer (`Infobox Equipment`, `EquipmentBox`, `A380FuelEquip`, `A3Report`, `RTProject`, etc.)
    kv_lines: List[str] = []
    unnamed_items: List[str] = []
    for param in tpl.params:
        p_val_raw = str(param.value).strip()
        if not p_val_raw:
            continue
        p_val = clean_val(p_val_raw)
        if not p_val or (p_val in {"*", "TBD", "N/A"} and len(tpl.params) > 5):
            if p_val == "*":
                continue
        if param.showkey:
            p_key = clean_val(str(param.name).strip())
            if p_key.lower() in {"image", "imagefilename", "width", "height", "colour", "color", "style", "border"}:
                continue
            if p_key and p_val:
                kv_lines.append(f"- **{p_key}**: {p_val}")
        else:
            if p_val:
                unnamed_items.append(p_val)

    if kv_lines:
        header_title = raw_name.replace("_", " ").strip()
        return f"\n**{header_title}**\n" + "\n".join(kv_lines) + "\n"
    if unnamed_items:
        return " — ".join(unnamed_items)
    return ""


def _expand_wikicode_ast(
    wikicode: mwparserfromhell.wikicode.Wikicode,
    pages_by_title: Dict[str, RawPageRecord],
    template_registry: Dict[str, TemplateRecord],
    depth: int,
) -> None:
    """Expand top-level templates AND recurse into non-code/non-math HTML `Tag`, `Wikilink`, and `ExternalLink` nodes."""
    for tpl in list(wikicode.filter_templates(recursive=False)):
        replacement = _render_single_template(tpl, pages_by_title, template_registry, depth=depth)
        try:
            wikicode.replace(tpl, replacement, recursive=False)
        except ValueError:
            pass

    for tag_node in list(wikicode.filter_tags(recursive=False)):
        tag_name = str(tag_node.tag).strip().lower()
        if tag_name in _OPAQUE_CODE_MATH_TAGS or tag_node.contents is None:
            continue
        if "{{" in str(tag_node.contents):
            _expand_wikicode_ast(tag_node.contents, pages_by_title, template_registry, depth=depth)

    for h_node in list(wikicode.filter_headings(recursive=False)):
        if h_node.title is not None and "{{" in str(h_node.title):
            _expand_wikicode_ast(h_node.title, pages_by_title, template_registry, depth=depth)

    for wl in list(wikicode.filter_wikilinks(recursive=False)):
        if wl.text is not None and "{{" in str(wl.text):
            _expand_wikicode_ast(wl.text, pages_by_title, template_registry, depth=depth)

    for el in list(wikicode.filter_external_links(recursive=False)):
        if el.title is not None and "{{" in str(el.title):
            _expand_wikicode_ast(el.title, pages_by_title, template_registry, depth=depth)


def expand_templates_in_wikitext(
    wikitext: str,
    pages_by_title: Dict[str, RawPageRecord],
    template_registry: Dict[str, TemplateRecord],
    depth: int = 0,
) -> str:
    if not wikitext or "{{" not in wikitext or depth > 4:
        return wikitext

    try:
        parsed = mwparserfromhell.parse(wikitext)
    except Exception:
        return wikitext

    _expand_wikicode_ast(parsed, pages_by_title, template_registry, depth=depth)
    return str(parsed)


def _convert_file_tags(text: str) -> str:
    """Convert Airbus Wiki `<file>path|label</file>` tags without redacting paths or filenames."""

    def repl(match: re.Match) -> str:
        inner = match.group(1).strip()
        if "|" in inner:
            path_part, label_part = inner.split("|", 1)
            path_part = path_part.strip()
            label_part = label_part.strip()
            if label_part and path_part:
                return f"{label_part} ({path_part})"
            return label_part or path_part
        return inner

    return re.sub(r"<file\b[^>]*>(.*?)</file>", repl, text, flags=re.IGNORECASE | re.DOTALL)


def _convert_ref_tags(text: str) -> str:
    """Preserve inline `<ref>...</ref>` citations next to the claim they support."""
    text = re.sub(r"<ref\b[^>/]*/\s*>", "", text, flags=re.IGNORECASE)

    def repl(match: re.Match) -> str:
        inner = re.sub(r"\s+", " ", match.group(1)).strip()
        if not inner:
            return ""
        return f" [Ref: {inner}]"

    return re.sub(r"<ref\b[^>]*>(.*?)</ref>", repl, text, flags=re.IGNORECASE | re.DOTALL)


def _strip_cell_attrs(cell: str) -> str:
    """Strip MediaWiki cell style/colspan/rowspan attributes (`style="..." | value` -> `value`)."""
    cell = cell.strip()
    if "|" in cell and not cell.startswith("[[") and not cell.startswith("{{"):
        attr_candidate, rest = cell.split("|", 1)
        if re.search(
            r"\b(?:style|class|width|height|align|valign|colspan|rowspan|bgcolor|scope|border)\s*=",
            attr_candidate,
            flags=re.IGNORECASE,
        ):
            return rest.strip()
    return cell


def _format_markdown_table(rows: List[Tuple[bool, List[str]]]) -> str:
    """Render parsed table rows `(is_header_row, cells)` as a clean Markdown pipe table."""
    cleaned_rows: List[Tuple[bool, List[str]]] = []
    max_cols = 0
    for is_hdr, cells in rows:
        norm_cells = [re.sub(r"\s+", " ", c).replace("|", "/").strip() for c in cells]
        if not any(c and c != "*" for c in norm_cells):
            continue
        max_cols = max(max_cols, len(norm_cells))
        cleaned_rows.append((is_hdr, norm_cells))

    if not cleaned_rows or max_cols == 0:
        return ""

    out_lines: List[str] = []
    _, first_cells = cleaned_rows[0]
    padded_first = first_cells + [""] * (max_cols - len(first_cells))
    out_lines.append("| " + " | ".join(padded_first) + " |")
    out_lines.append("| " + " | ".join(["---"] * max_cols) + " |")

    for _, cells in cleaned_rows[1:]:
        padded = cells + [""] * (max_cols - len(cells))
        out_lines.append("| " + " | ".join(padded) + " |")

    return "\n" + "\n".join(out_lines) + "\n"


def _convert_single_wikitable(table_block: str) -> str:
    raw_lines = table_block.splitlines()
    open_attrs = raw_lines[0].lstrip()[2:].strip() if (raw_lines and raw_lines[0].lstrip().startswith("{|")) else ""
    lines = raw_lines[1:] if (raw_lines and raw_lines[0].lstrip().startswith("{|")) else list(raw_lines)
    if lines and lines[-1].lstrip().startswith("|}"):
        lines = lines[:-1]

    has_headings_inside = any(re.match(r"^\s*=+.+=+\s*$", ln) for ln in lines)
    is_borderless_layout = bool(
        re.search(r"(?:border\s*=\s*[\"']?0|background\s*:\s*none)", open_attrs, flags=re.IGNORECASE)
    )

    caption = ""
    rows: List[Tuple[bool, List[str]]] = []
    raw_cell_blocks: List[List[str]] = []
    current_cells: List[str] = []
    current_is_header = False
    has_any_header_cell = False

    def flush_row() -> None:
        nonlocal current_cells, current_is_header
        if current_cells:
            rows.append((current_is_header, current_cells))
            current_cells = []
            current_is_header = False

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line == "|}":
            if raw_cell_blocks:
                raw_cell_blocks[-1].append("")
            continue
        if line.startswith("|+"):
            caption = _strip_cell_attrs(line[2:].strip())
            continue
        if line.startswith("|-"):
            flush_row()
            continue
        if line.startswith("!"):
            current_is_header = True
            has_any_header_cell = True
            header_body = line[1:].lstrip("|").strip()
            parts = re.split(r"\s*(?:!!|\|\|)\s*", header_body)
            for p in parts:
                cleaned_p = _strip_cell_attrs(p)
                current_cells.append(cleaned_p)
                raw_cell_blocks.append([cleaned_p])
            continue
        if line.startswith("|"):
            cell_body = line[1:].strip()
            parts = re.split(r"\s*\|\|\s*", cell_body)
            for p in parts:
                cleaned_p = _strip_cell_attrs(p)
                current_cells.append(cleaned_p)
                raw_cell_blocks.append([cleaned_p])
            continue

        # Non-pipe line inside table: NEVER drop it when `current_cells` is empty (e.g. layout `{| width=100%`)!
        if current_cells:
            current_cells[-1] = f"{current_cells[-1]} {line}".strip()
            if raw_cell_blocks:
                raw_cell_blocks[-1].append(raw_line)
        else:
            current_cells.append(line)
            raw_cell_blocks.append([raw_line])

    flush_row()

    # Unwrap pure layout wrappers (no `!` headers + contains `== Section ==` headings or border=0 layout boxes)
    # so headings, bullet lists, and multi-paragraph callouts aren't squashed into a single horizontal pipe row!
    if not has_any_header_cell and (has_headings_inside or (is_borderless_layout and len(rows) <= 3)):
        unwrapped_parts = ["\n".join(block).strip() for block in raw_cell_blocks if "\n".join(block).strip()]
        if caption:
            unwrapped_parts.insert(0, f"**{caption}**")
        return "\n\n" + "\n\n".join(unwrapped_parts) + "\n\n" if unwrapped_parts else ""

    md_table = _format_markdown_table(rows)
    if caption and md_table:
        return f"\n**{caption}**{md_table}"
    return md_table


def _convert_wikitables(text: str) -> str:
    """Iteratively convert `{| ... |}` MediaWiki tables AND strip unclosed layout `{|` table wrappers."""
    pattern = re.compile(r"\{\|(?:(?!\{\|).)*?\|\}", re.DOTALL)
    for _ in range(6):
        if "{|" not in text:
            break
        new_text, count = pattern.subn(lambda m: _convert_single_wikitable(m.group(0)), text)
        text = new_text
        if count == 0:
            break

    # Handle pages where an author opened a layout `{| ...` wrapper without a closing `|}`
    if "{|" in text or "|}" in text:
        cleaned_lines: List[str] = []
        for ln in text.splitlines():
            s = ln.strip()
            if s.startswith("{|") or s == "|}" or s.startswith("|-"):
                continue
            cleaned_lines.append(ln)
        text = "\n".join(cleaned_lines)

    return text


def _convert_html_tables(text: str) -> str:
    """Convert HTML `<table>...</table>` infoboxes into clean Markdown tables."""

    def repl_table(match: re.Match) -> str:
        html_block = match.group(0)
        rows: List[Tuple[bool, List[str]]] = []
        for tr_match in re.finditer(r"<tr\b[^>]*>(.*?)</tr>", html_block, flags=re.IGNORECASE | re.DOTALL):
            tr_inner = tr_match.group(1)
            cells: List[str] = []
            has_td = False
            has_th = False
            for cell_match in re.finditer(
                r"<(th|td)\b[^>]*>(.*?)</\1>", tr_inner, flags=re.IGNORECASE | re.DOTALL
            ):
                tag = cell_match.group(1).lower()
                if tag == "th":
                    has_th = True
                else:
                    has_td = True
                cell_val = re.sub(r"<br\s*/?>", "; ", cell_match.group(2), flags=re.IGNORECASE)
                cell_val = re.sub(r"</?[a-zA-Z][^>]*>", " ", cell_val)
                cell_val = re.sub(r"\s+", " ", html.unescape(cell_val)).strip()
                cells.append(cell_val)
            if cells:
                rows.append((has_th and not has_td, cells))
        return _format_markdown_table(rows)

    pattern = re.compile(r"<table\b[^>]*>(?:(?!<table\b).)*?</table>", re.IGNORECASE | re.DOTALL)
    for _ in range(4):
        if "<table" not in text.lower():
            break
        text, count = pattern.subn(repl_table, text)
        if count == 0:
            break
    return text


def _convert_links_and_media(text: str) -> str:
    """Convert `[[File:...]]`, `[[Target|Label]]`, and `[https://... Label]` cleanly without losing URLs."""

    def repl_media(match: re.Match) -> str:
        filename = match.group(1).strip()
        rest = match.group(2) or ""
        captions = []
        for opt in rest.split("|"):
            opt_clean = opt.strip()
            if not opt_clean or _IMAGE_OPTIONS_RE.match(opt_clean):
                continue
            captions.append(opt_clean)
        caption_str = " — ".join(captions).strip()
        if caption_str:
            return f"[Image: {filename} — {caption_str}]"
        return f"[Image: {filename}]"

    text = re.sub(
        r"\[\[\s*(?:File|Image)\s*:\s*([^\[\]|]+)(?:\|([^\[\]]*))?\]\]",
        repl_media,
        text,
        flags=re.IGNORECASE,
    )

    def repl_wikilink(match: re.Match) -> str:
        target = match.group(1).lstrip(":").strip()
        label = (match.group(2) or "").strip()
        if label:
            return label
        return target

    for _ in range(3):
        next_text = re.sub(r"\[\[([^\[\]|]+)(?:\|([^\[\]]*))?\]\]", repl_wikilink, text)
        if next_text == text:
            break
        text = next_text

    def repl_extlink(match: re.Match) -> str:
        url = match.group(1).strip()
        label = (match.group(2) or "").strip()
        if url.lower().startswith("mailto:"):
            addr = url[7:].strip()
            return label or addr
        if label and label != url:
            return f"{label} ({url})"
        return url

    text = re.sub(
        r"\[((?:https?|ftp|file|mailto):[^\s\[\]]+)(?:\s+([^\[\]]+))?\]",
        repl_extlink,
        text,
        flags=re.IGNORECASE,
    )
    return text


def _convert_lists_and_headings(text: str) -> str:
    """Convert MediaWiki `#` / `*` lists FIRST, then `== Heading ==` to Markdown `## Heading`."""
    out_lines: List[str] = []
    in_code_fence = False

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code_fence = not in_code_fence
            out_lines.append(line)
            continue
        if in_code_fence:
            out_lines.append(line)
            continue

        # MediaWiki heading: = H1 =, == H2 ==, === H3 ===
        h_match = re.match(r"^(=+)\s*(.*?)\s*\1\s*$", stripped)
        if h_match:
            eq_count = len(h_match.group(1))
            md_level = max(2, min(6, eq_count if eq_count >= 2 else 2))
            heading_title = re.sub(r"'{2,5}", "", h_match.group(2)).strip()
            if heading_title:
                out_lines.append(f"\n{'#' * md_level} {heading_title}\n")
            continue

        # MediaWiki numbered list (`# item`, `## subitem`) — avoid `#REDIRECT`
        if re.match(r"^#+\s*(?!redirect\b)\S", line.lstrip(), flags=re.IGNORECASE):
            num_match = re.match(r"^\s*(#+)\s*(.*)$", line)
            if num_match:
                indent = "  " * (len(num_match.group(1)) - 1)
                out_lines.append(f"{indent}1. {num_match.group(2).strip()}")
                continue

        # MediaWiki bullet list (`* item`, `** subitem`) — ignore already-rendered Markdown `**Bold**` lines!
        if re.match(r"^\*+\s*\S", stripped) and not re.match(r"^\*\*[^*].*\*\*$", stripped):
            bul_match = re.match(r"^\s*(\*+)\s*(.*)$", line)
            if bul_match:
                indent = "  " * (len(bul_match.group(1)) - 1)
                out_lines.append(f"{indent}- {bul_match.group(2).strip()}")
                continue

        # MediaWiki indented bullet (`:* item` or `:# item`)
        indent_list_m = re.match(r"^(:+)(\*+|#+)\s*(.+)$", stripped)
        if indent_list_m:
            depth_indent = "  " * len(indent_list_m.group(1))
            marker = "1." if indent_list_m.group(2).startswith("#") else "-"
            out_lines.append(f"{depth_indent}{marker} {indent_list_m.group(3).strip()}")
            continue

        # MediaWiki colon-indented prose (`:Welcome...` or `::Note...`)
        if re.match(r"^:+\s*\S", stripped):
            out_lines.append(re.sub(r"^:+\s*", "", stripped))
            continue

        # MediaWiki definition term (`; Term : Def` or `; Term`)
        if stripped.startswith(";"):
            term_body = stripped[1:].strip()
            if ":" in term_body:
                t_part, d_part = term_body.split(":", 1)
                out_lines.append(f"**{t_part.strip()}**: {d_part.strip()}")
            else:
                out_lines.append(f"**{term_body}**")
            continue

        out_lines.append(line)

    return "\n".join(out_lines)


def clean_wikitext_body(
    wikitext: str,
    pages_by_title: Dict[str, RawPageRecord],
    template_registry: Dict[str, TemplateRecord],
    depth: int = 0,
    content_model: str = "wikitext",
) -> str:
    """Transform raw MediaWiki wikitext into clean, structure-preserving Markdown."""
    if not wikitext:
        return ""

    # Non-wikitext slots (e.g., CSS/JS/JSON subpages in custom namespaces) should stay intact as code blocks
    model = (content_model or "wikitext").lower()
    if model in {"css", "sanitized-css", "javascript", "json"}:
        lang = "css" if "css" in model else model
        return f"```{lang}\n{wikitext.strip()}\n```"

    text = strip_invisible_chars(wikitext)
    # Remove HTML comments and category assignment tags (already extracted into metadata)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = strip_category_tags(text)

    # Expand MediaWiki templates (including inside <ref>, <table>, <div>) before converting tables/links/headings
    text = expand_templates_in_wikitext(text, pages_by_title, template_registry, depth=depth)

    # Convert <syntaxhighlight>, <source>, and <pre> blocks into fenced Markdown code blocks
    def _repl_codeblock(m: re.Match) -> str:
        attrs = m.group(1) or ""
        body_code = html.unescape(m.group(2) or "").strip("\r\n")
        lang_m = re.search(r'lang\s*=\s*["\']?([A-Za-z0-9_+-]+)', attrs, flags=re.IGNORECASE)
        lang = lang_m.group(1) if lang_m else ""
        return f"\n```{lang}\n{body_code}\n```\n"

    text = re.sub(
        r"<(?:syntaxhighlight|source|pre)\b([^>]*)>(.*?)</(?:syntaxhighlight|source|pre)>",
        _repl_codeblock,
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Preserve <nowiki>...</nowiki> literal wiki syntax examples safely as inline code (never emitting ``` fences inside tables)
    def _repl_nowiki(m: re.Match) -> str:
        inner = re.sub(r"\s+", " ", html.unescape(m.group(1) or "")).strip()
        if not inner:
            return ""
        if inner in {",", ";", "|", "*", "#"}:
            return inner
        return f"`{inner}`"

    text = re.sub(r"<nowiki\b[^>]*>(.*?)</nowiki>", _repl_nowiki, text, flags=re.IGNORECASE | re.DOTALL)

    # Remove MediaWiki magic behavior switches (__NOTOC__, __TOC__, __NOEDITSECTION__, etc.)
    text = re.sub(
        r"__(?:NOTOC|TOC|FORCETOC|NOEDITSECTION|NEWSECTIONLINK|NONEWSECTIONLINK|HIDDENCAT|INDEX|NOINDEX|STATICREDIRECT)__",
        "",
        text,
        flags=re.IGNORECASE,
    )
    # Resolve any unexpanded template parameter placeholders `{{{param|default}}}` -> `default` (or empty)
    text = re.sub(r"\{\{\{[^{}|]+\|([^{}]*)\}\}\}", r"\1", text)
    text = re.sub(r"\{\{\{[^{}]+\}\}\}", "", text)

    # Convert <file>, <ref>, <gallery>, AND convert [[Target|Label]] / [[File:...|...]] BEFORE tables
    # so single pipes `|` inside wikilinks/media are never turned into `/` by table cell normalization!
    text = _convert_file_tags(text)
    text = _convert_ref_tags(text)
    text = re.sub(r"</?gallery\b[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = _convert_links_and_media(text)
    text = _convert_html_tables(text)
    text = _convert_wikitables(text)

    # Convert lists and headings (respecting fenced code blocks!)
    text = _convert_lists_and_headings(text)

    # Bold / italic wiki quotes -> Markdown
    text = re.sub(r"'{5}(.+?)'{5}", r"***\1***", text)
    text = re.sub(r"'{3}(.+?)'{3}", r"**\1**", text)
    text = re.sub(r"'{2}(.+?)'{2}", r"*\1*", text)

    # Strip residual presentational HTML tags while preserving line breaks (including `</br>` author typos)
    text = re.sub(r"</?br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<references\b[^>]*/?>|</references>", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"</?(?:div|span|p|u|big|small|font|center|tt|code|nowiki|includeonly|onlyinclude|noinclude|sub|sup|hr)\b[^>]*>",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = html.unescape(text)

    # Normalize horizontal whitespace & excessive blank lines while keeping Markdown line structure!
    cleaned_lines = [re.sub(r"[ \t]{2,}", " ", ln).rstrip() for ln in text.splitlines()]
    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def process_raw_wikitext() -> Dict[str, int]:
    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    for old_md in PAGES_DIR.glob("*.md"):
        old_md.unlink()

    template_registry = load_template_registry(TEMPLATE_DIR)
    pages = load_all_raw_pages(RAW_DIR, template_registry=template_registry)
    pages_by_title = build_page_title_index(pages)

    processed = 0
    redirects = 0
    for page in pages:
        if page.is_redirect:
            redirects += 1
        cleaned_text = clean_wikitext_body(
            page.wikitext,
            pages_by_title=pages_by_title,
            template_registry=template_registry,
            content_model=page.content_model,
        )

        frontmatter_meta = {
            "page_id": page.page_id,
            "ns": page.ns,
            "title": page.title,
            "uri": page.uri,
            "updated_at": page.updated_at,
            "content_model": page.content_model,
            "source_batch": page.batch_file,
            "is_redirect": page.is_redirect,
            "redirect_target": page.redirect_target,
            "categories": page.categories,
            "referenced_templates": page.referenced_template_names,
            "aliases": page.aliases,
        }
        md_content = (
            "---\n"
            + json.dumps(frontmatter_meta, ensure_ascii=False, indent=2)
            + "\n---\n\n"
            + cleaned_text
            + "\n"
        )

        out_file = safe_child_path(PAGES_DIR, f"{page.page_id}_{page.title}.md")
        out_file.write_text(md_content, encoding="utf-8")
        processed += 1

    print(
        f"Finished cleaning {processed} pages ({redirects} redirects, "
        f"{processed - redirects} content articles) into {PAGES_DIR}"
    )
    return {"pages_cleaned": processed, "redirects": redirects}


if __name__ == "__main__":
    process_raw_wikitext()