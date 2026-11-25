"""Option 2 Step 3: Section-aware manual chunker with hierarchical headings, rolling overlap, and category/alias headers.

Key fixes over original `3_chunk_pages.py`:
1. Parses structured JSON frontmatter cleanly between opening and closing `---` markers (zero YAML leakage).
2. Parses hierarchical `##` / `###` / `####` Markdown headings into breadcrumb paths
   (e.g. `Development Process > Model Testing > Model compilation`).
3. Handles `#REDIRECT` pages smartly: skips duplicate 5-token redirect stubs whose target page
   already exists in the corpus (since target pages now carry `aliases`), while keeping orphaned redirects.
4. Assigns globally unique, RFC-1034-compliant IDs (`chunk-{page_id}-{page_chunk_idx:03d}`) across all
   sections of a page so section 2+ chunks never collide with section 1.
5. Prepends a rich contextual header (`Title`, `Aliases`, `Section`, `Categories`) to each chunk's
   `content` while also preserving `raw_text` and full metadata (`categories`, `uri`, `page_id`).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from wiki_utils import (
    estimate_tokens,
    make_rfc1034_id,
    mw_canonical_title_key,
    normalize_title_key,
    safe_child_path,
)

ROOT = Path(__file__).resolve().parent
PAGES_DIR = ROOT / "wikimedia-data" / "processed" / "pages"
CHUNKS_DIR = ROOT / "wikimedia-data" / "processed" / "chunks"
CHUNKS_FILE = CHUNKS_DIR / "chunks.json"

PURE_BOILERPLATE_SECTIONS = {
    "references",
    "notes",
    "bibliography",
    "citations",
    "see also",
}


def classify_chunk(text: str) -> str:
    operational_keywords = {
        "run", "build", "compile", "copy", "check", "update", "execute", "install",
        "open", "rename", "generate", "test", "debug", "create", "load", "follow",
        "select", "initialise", "configure", "set",
    }
    reference_keywords = {
        "part number", "program", "supplier", "surface", "aircraft", "equipped",
        "component specification", "document", "model", "logic", "version", "fin",
    }

    lowered = text.lower()
    if any(keyword in lowered for keyword in operational_keywords):
        return "operational"
    if any(keyword in lowered for keyword in reference_keywords):
        return "reference"
    return "conceptual"


def parse_markdown_frontmatter(raw_md: str) -> Tuple[Dict[str, Any], str]:
    """Extract `(metadata_dict, body_markdown)` without leaking frontmatter into body text."""
    match = re.match(r"^---\r?\n(.*?)\r?\n---\r?\n*(.*)$", raw_md, flags=re.DOTALL)
    if not match:
        return {}, raw_md.strip()

    header_str, body = match.group(1).strip(), match.group(2).strip()
    try:
        meta = json.loads(header_str)
        if isinstance(meta, dict):
            return meta, body
    except json.JSONDecodeError:
        pass

    # Fallback simple key: value YAML parser for legacy files
    meta = {}
    for ln in header_str.splitlines():
        if ":" in ln:
            k, v = ln.split(":", 1)
            meta[k.strip()] = v.strip()
    return meta, body


def split_hierarchical_sections(markdown_text: str) -> List[Tuple[str, str]]:
    """Split Markdown by `##`..`######` headings (ignoring `#` comments inside ``` code fences!)."""
    lines = markdown_text.splitlines()
    sections: List[Tuple[str, str]] = []
    heading_stack: Dict[int, str] = {}
    current_breadcrumb = "Overview"
    current_lines: List[str] = []
    in_code_fence = False

    def flush() -> None:
        nonlocal in_code_fence
        if in_code_fence:
            current_lines.append("```")
            in_code_fence = False
        body = "\n".join(current_lines).strip()
        if body:
            sections.append((current_breadcrumb, body))

    for line in lines:
        if line.strip().startswith("```"):
            in_code_fence = not in_code_fence
            current_lines.append(line)
            continue

        if not in_code_fence:
            h_match = re.match(r"^(#{2,6})\s+(.+?)\s*$", line)
            if h_match:
                flush()
                level = len(h_match.group(1))
                title = h_match.group(2).strip()
                for k in [lvl for lvl in heading_stack if lvl >= level]:
                    del heading_stack[k]
                heading_stack[level] = title
                ordered_titles = [heading_stack[lvl] for lvl in sorted(heading_stack)]
                current_breadcrumb = " > ".join(ordered_titles) if ordered_titles else "Overview"
                current_lines = []
                continue

        current_lines.append(line)

    flush()
    return sections


def _is_empty_or_boilerplate_section(breadcrumb: str, section_text: str) -> bool:
    leaf = breadcrumb.split(" > ")[-1].strip().lower()
    if leaf in PURE_BOILERPLATE_SECTIONS:
        # Keep if it actually has substantial prose (> 80 tokens), otherwise skip pure link/empty lists
        if estimate_tokens(section_text) < 80:
            return True
    if not section_text.strip():
        return True
    return False


def merge_tiny_sections(
    sections: List[Tuple[str, str]],
    min_tokens: int = 55,
    max_tokens: int = 400,
) -> List[Tuple[str, str]]:
    """Merge very short consecutive sections so we don't produce fragmented 10-token chunks."""
    merged: List[Tuple[str, str]] = []
    for heading, body in sections:
        if not merged:
            merged.append((heading, body))
            continue

        prev_heading, prev_body = merged[-1]
        prev_tokens = estimate_tokens(prev_body)
        curr_tokens = estimate_tokens(body)

        if (prev_tokens < min_tokens or curr_tokens < min_tokens) and (prev_tokens + curr_tokens <= max_tokens):
            leaf = heading.split(" > ")[-1]
            if prev_heading == heading:
                combined_heading = prev_heading
            elif prev_heading.endswith("(+ subsections)") or prev_heading.count(" / ") >= 2 or len(prev_heading) + len(leaf) > 90:
                base_h = prev_heading.replace(" (+ subsections)", "")
                combined_heading = f"{base_h} (+ subsections)"
            else:
                combined_heading = f"{prev_heading} / {leaf}"
            combined_body = f"### {heading}\n{body}" if heading != "Overview" else body
            if prev_heading != "Overview" and not prev_body.startswith("### "):
                prev_body = f"### {prev_heading}\n{prev_body}"
            merged[-1] = (combined_heading, f"{prev_body}\n\n{combined_body}".strip())
        else:
            merged.append((heading, body))
    return merged


def _split_into_atomic_blocks(section_text: str) -> List[str]:
    """Split section Markdown on blank lines OUTSIDE ``` code fences so code blocks stay intact."""
    blocks: List[str] = []
    buf: List[str] = []
    in_fence = False

    for line in section_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_fence and buf and any(x.strip() for x in buf):
                blocks.append("\n".join(buf).strip())
                buf = [line]
                in_fence = True
            elif in_fence:
                buf.append(line)
                blocks.append("\n".join(buf).strip())
                buf = []
                in_fence = False
            else:
                buf = [line]
                in_fence = True
            continue

        if in_fence:
            buf.append(line)
            continue

        if not stripped:
            if buf and any(x.strip() for x in buf):
                blocks.append("\n".join(buf).strip())
                buf = []
        else:
            buf.append(line)

    if buf and any(x.strip() for x in buf):
        if in_fence:
            buf.append("```")
        blocks.append("\n".join(buf).strip())

    return [b for b in blocks if b]


def _split_oversized_prose_or_line(text: str, max_tokens: int) -> List[str]:
    """Split a long single line or paragraph by sentence boundaries, with word-level fallback."""
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s]
    out: List[str] = []
    acc: List[str] = []
    acc_tokens = 0

    for sentence in sentences:
        st = estimate_tokens(sentence)
        if st > max_tokens:
            if acc:
                out.append(" ".join(acc))
                acc = []
                acc_tokens = 0
            words = sentence.split()
            w_group: List[str] = []
            w_tokens = 0
            for w in words:
                wt = max(1, int(1.3))
                if w_group and estimate_tokens(" ".join(w_group + [w])) > max_tokens:
                    out.append(" ".join(w_group))
                    w_group = [w]
                    w_tokens = wt
                else:
                    w_group.append(w)
                    w_tokens += wt
            if w_group:
                out.append(" ".join(w_group))
            continue

        if acc and acc_tokens + st > max_tokens:
            out.append(" ".join(acc))
            acc = [sentence]
            acc_tokens = st
        else:
            acc.append(sentence)
            acc_tokens += st

    if acc:
        out.append(" ".join(acc))
    return out


def _coalesce_small_windows(windows: List[str], max_tokens: int, min_tokens: int = 35) -> List[str]:
    """Merge tiny lead-in or trailing micro-chunks (`< min_tokens`) into an adjacent chunk in the same section."""
    if len(windows) <= 1:
        return windows

    items = list(windows)
    idx = 0
    while idx < len(items):
        tok = estimate_tokens(items[idx])
        if tok >= min_tokens:
            idx += 1
            continue

        stripped = items[idx].rstrip()
        looks_like_lead_in = stripped.endswith(":") or (stripped.startswith("**") and stripped.endswith("**"))

        # Prefer merging forward if it's a lead-in heading/prompt (`...:` or `**Subheading**`)
        if looks_like_lead_in and idx + 1 < len(items):
            if tok + estimate_tokens(items[idx + 1]) <= max_tokens + 45:
                items[idx + 1] = f"{items[idx]}\n\n{items[idx + 1]}"
                items.pop(idx)
                continue

        # Otherwise merge backward into previous chunk if it fits, else forward into next chunk
        if idx > 0 and estimate_tokens(items[idx - 1]) + tok <= max_tokens + 45:
            items[idx - 1] = f"{items[idx - 1]}\n\n{items[idx]}"
            items.pop(idx)
            continue

        if idx + 1 < len(items) and tok + estimate_tokens(items[idx + 1]) <= max_tokens + 45:
            items[idx + 1] = f"{items[idx]}\n\n{items[idx + 1]}"
            items.pop(idx)
            continue

        idx += 1

    return items


def split_section_into_windows(section_text: str, max_tokens: int = 350) -> List[str]:
    """Split a section's Markdown text into <= `max_tokens` chunks (preserving code fences, tables & lead-ins)."""
    paragraphs = _split_into_atomic_blocks(section_text)
    chunks: List[str] = []
    current_paragraphs: List[str] = []
    current_tokens = 0

    for paragraph in paragraphs:
        p_tokens = estimate_tokens(paragraph)

        # Oversized single block (large Markdown table, fenced code block, or giant list/paragraph)
        if p_tokens > max_tokens:
            lead_in_prefix = ""
            lead_in_tokens = 0
            if current_paragraphs:
                joined_curr = "\n\n".join(current_paragraphs)
                # Keep short lead-in sentences (`<= 90 tokens`) attached to the first slice of the table/list/code!
                if current_tokens <= 90:
                    lead_in_prefix = joined_curr
                    lead_in_tokens = current_tokens
                else:
                    chunks.append(joined_curr)
                current_paragraphs = []
                current_tokens = 0

            p_lines = paragraph.splitlines()
            is_code_fence = (
                len(p_lines) >= 2
                and p_lines[0].strip().startswith("```")
                and p_lines[-1].strip().startswith("```")
            )

            # 1. Oversized fenced code block: split interior lines and re-wrap EVERY chunk in matching ``` fences!
            if is_code_fence:
                open_fence = p_lines[0].strip()
                inner_lines = p_lines[1:-1]
                fence_overhead = 4
                sub_chunks: List[str] = []
                group: List[str] = []
                first_budget = max(80, max_tokens - lead_in_tokens - fence_overhead)
                active_budget = first_budget
                g_tokens = 0

                for ln in inner_lines:
                    ln_tok = estimate_tokens(ln)
                    if ln_tok > active_budget:
                        if group:
                            sub_chunks.append(f"{open_fence}\n" + "\n".join(group) + "\n```")
                            group = []
                            g_tokens = 0
                            active_budget = max(80, max_tokens - fence_overhead)
                        for piece in _split_oversized_prose_or_line(ln, active_budget):
                            sub_chunks.append(f"{open_fence}\n{piece}\n```")
                        continue

                    if group and g_tokens + ln_tok > active_budget:
                        sub_chunks.append(f"{open_fence}\n" + "\n".join(group) + "\n```")
                        group = [ln]
                        g_tokens = ln_tok
                        active_budget = max(80, max_tokens - fence_overhead)
                    else:
                        group.append(ln)
                        g_tokens += ln_tok

                if group:
                    sub_chunks.append(f"{open_fence}\n" + "\n".join(group) + "\n```")
                if sub_chunks and lead_in_prefix:
                    sub_chunks[0] = f"{lead_in_prefix}\n\n{sub_chunks[0]}"
                chunks.extend(sub_chunks)
                continue

            # 2. Oversized Markdown table or multi-line list: keep table header on every slice!
            if len(p_lines) > 2:
                is_md_table = p_lines[0].startswith("| ") and p_lines[1].startswith("| ---")
                table_header = p_lines[:2] if is_md_table else []
                body_lines = p_lines[2:] if is_md_table else p_lines
                header_tokens = estimate_tokens("\n".join(table_header))

                sub_chunks = []
                group = []
                active_budget = max(100, max_tokens - lead_in_tokens)
                g_tokens = header_tokens

                for ln in body_lines:
                    ln_tokens = estimate_tokens(ln)
                    if header_tokens + ln_tokens > active_budget:
                        if group:
                            sub_chunks.append("\n".join(table_header + group))
                            group = []
                            g_tokens = header_tokens
                            active_budget = max_tokens
                        line_budget = max(80, active_budget - header_tokens)
                        for piece in _split_oversized_prose_or_line(ln, line_budget):
                            row_piece = f"| {piece} |" if (is_md_table and not piece.startswith("|")) else piece
                            sub_chunks.append("\n".join(table_header + [row_piece]))
                        active_budget = max_tokens
                        continue

                    if group and g_tokens + ln_tokens > active_budget:
                        sub_chunks.append("\n".join(table_header + group))
                        group = [ln]
                        g_tokens = header_tokens + ln_tokens
                        active_budget = max_tokens
                    else:
                        group.append(ln)
                        g_tokens += ln_tokens

                if group:
                    sub_chunks.append("\n".join(table_header + group))
                if sub_chunks and lead_in_prefix:
                    sub_chunks[0] = f"{lead_in_prefix}\n\n{sub_chunks[0]}"
                chunks.extend(sub_chunks)
                continue

            # 3. Oversized prose paragraph: split by sentence/word boundaries
            sub_chunks = _split_oversized_prose_or_line(
                f"{lead_in_prefix}\n\n{paragraph}" if lead_in_prefix else paragraph,
                max_tokens,
            )
            chunks.extend(sub_chunks)
            continue

        if current_paragraphs and current_tokens + p_tokens > max_tokens:
            chunks.append("\n\n".join(current_paragraphs))
            last_p = current_paragraphs[-1]
            last_is_fence = last_p.lstrip().startswith("```")
            # Keep 1-paragraph overlap only for short prose paragraphs (not fenced code blocks or tables)
            if not last_is_fence and not last_p.lstrip().startswith("| ") and estimate_tokens(last_p) <= 100:
                current_paragraphs = [last_p]
                current_tokens = estimate_tokens(last_p)
            else:
                current_paragraphs = []
                current_tokens = 0

        current_paragraphs.append(paragraph)
        current_tokens += p_tokens

    if current_paragraphs:
        chunks.append("\n\n".join(current_paragraphs))

    cleaned_windows = [c.strip() for c in chunks if c.strip()]
    return _coalesce_small_windows(cleaned_windows, max_tokens=max_tokens, min_tokens=35)


def build_contextual_header(
    page_title: str,
    section_title: str,
    categories: List[str],
    aliases: List[str],
) -> str:
    header_lines = [f"Page: {page_title}"]
    if aliases:
        header_lines.append(f"Aliases: {', '.join(aliases)}")
    if section_title and section_title != page_title:
        header_lines.append(f"Section: {section_title}")
    if categories:
        header_lines.append(f"Categories: {', '.join(categories)}")
    return "\n".join(header_lines)


def build_contextual_chunk_text(
    page_title: str,
    section_title: str,
    categories: List[str],
    aliases: List[str],
    body_text: str,
) -> str:
    """Prepend concise document/section/category metadata so embeddings retain full page context."""
    header = build_contextual_header(page_title, section_title, categories, aliases)
    return header + "\n\n" + body_text.strip()


def process_chunks(max_tokens: int = 400) -> List[Dict[str, Any]]:
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    # Clear old per-page chunk files so stale IDs don't linger
    for old_chunk_file in CHUNKS_DIR.glob("*_chunks.json"):
        old_chunk_file.unlink()

    if not PAGES_DIR.exists():
        raise FileNotFoundError(f"Directory {PAGES_DIR} does not exist. Run 2_clean_wikitext.py first.")

    md_files = sorted(PAGES_DIR.glob("*.md"))
    parsed_pages: List[Tuple[Path, Dict[str, Any], str]] = []
    non_redirect_canon = set()
    non_redirect_lower = set()
    redirect_map: Dict[str, str] = {}

    for md_file in md_files:
        raw_md = md_file.read_text(encoding="utf-8")
        meta, body = parse_markdown_frontmatter(raw_md)
        title = str(meta.get("title") or md_file.stem)
        c_key = mw_canonical_title_key(title)
        l_key = normalize_title_key(title)
        if not meta.get("is_redirect"):
            if c_key:
                non_redirect_canon.add(c_key)
            if l_key:
                non_redirect_lower.add(l_key)
        elif meta.get("redirect_target"):
            tgt = str(meta["redirect_target"])
            if c_key:
                redirect_map[c_key] = tgt
            if l_key and l_key not in redirect_map:
                redirect_map[l_key] = tgt
        parsed_pages.append((md_file, meta, body))

    def _resolve_final_redirect_target(raw_target: str) -> Tuple[str, bool]:
        """Follow redirect chains up to 5 hops; return `(final_target_title, exists_as_content_page)`."""
        curr_t = raw_target
        visited_keys = set()
        for _ in range(5):
            ck = mw_canonical_title_key(curr_t)
            lk = normalize_title_key(curr_t)
            if ck in non_redirect_canon or lk in non_redirect_lower:
                return curr_t, True
            nxt = redirect_map.get(ck) or redirect_map.get(lk)
            if not nxt or ck in visited_keys:
                break
            visited_keys.add(ck)
            curr_t = nxt
        ck = mw_canonical_title_key(curr_t)
        lk = normalize_title_key(curr_t)
        return curr_t, (ck in non_redirect_canon or lk in non_redirect_lower)

    all_chunks: List[Dict[str, Any]] = []
    stats = {
        "pages_seen": 0,
        "redirects_merged_as_aliases": 0,
        "orphan_redirects_kept": 0,
        "empty_pages_skipped": 0,
        "sections_seen": 0,
        "boilerplate_sections_skipped": 0,
        "chunks_generated": 0,
    }

    for md_file, meta, body in parsed_pages:
        stats["pages_seen"] += 1
        page_id = int(meta.get("page_id") or 0)
        ns = int(meta.get("ns") or 0)
        page_title = str(meta.get("title") or md_file.stem)
        categories: List[str] = list(meta.get("categories") or [])
        aliases: List[str] = list(meta.get("aliases") or [])
        referenced_templates: List[str] = list(meta.get("referenced_templates") or [])
        is_redirect = bool(meta.get("is_redirect"))
        redirect_target: Optional[str] = meta.get("redirect_target")
        uri = str(meta.get("uri") or "")
        updated_at = meta.get("updated_at")

        if is_redirect and redirect_target:
            final_target, target_exists = _resolve_final_redirect_target(redirect_target)
            if target_exists:
                stats["redirects_merged_as_aliases"] += 1
                continue
            stats["orphan_redirects_kept"] += 1
            redirect_target = final_target
            body = f"{page_title} redirects to {final_target}."

        if not body.strip():
            stats["empty_pages_skipped"] += 1
            continue

        raw_sections = split_hierarchical_sections(body)
        filtered_sections: List[Tuple[str, str]] = []
        for sec_heading, sec_body in raw_sections:
            stats["sections_seen"] += 1
            if _is_empty_or_boilerplate_section(sec_heading, sec_body):
                stats["boilerplate_sections_skipped"] += 1
                continue
            filtered_sections.append((sec_heading, sec_body))

        if not filtered_sections:
            # Fallback if page only had short links/notes
            filtered_sections = [("Overview", body.strip())]

        base_header_tokens = estimate_tokens(
            build_contextual_header(page_title, "Overview", categories, aliases)
        )
        section_merge_budget = max(180, max_tokens - base_header_tokens)
        merged_sections = merge_tiny_sections(filtered_sections, max_tokens=section_merge_budget)

        page_chunks: List[Dict[str, Any]] = []
        page_chunk_idx = 0

        for sec_heading, sec_body in merged_sections:
            sec_header_tokens = estimate_tokens(
                build_contextual_header(page_title, sec_heading, categories, aliases)
            )
            body_budget = max(180, max_tokens - sec_header_tokens)
            windows = split_section_into_windows(sec_body, max_tokens=body_budget)
            for window_text in windows:
                page_chunk_idx += 1
                chunk_id = make_rfc1034_id(f"chunk-{page_id}-{page_chunk_idx:03d}")
                contextual_text = build_contextual_chunk_text(
                    page_title=page_title,
                    section_title=sec_heading,
                    categories=categories,
                    aliases=aliases,
                    body_text=window_text,
                )
                chunk_record = {
                    "id": chunk_id,
                    "ns": ns,
                    "page_id": page_id,
                    "page_title": page_title,
                    "section_title": sec_heading,
                    "chunk_type": classify_chunk(window_text),
                    "categories": categories,
                    "referenced_templates": referenced_templates,
                    "aliases": aliases,
                    "is_redirect": is_redirect,
                    "redirect_target": redirect_target,
                    "uri": uri,
                    "text": contextual_text,
                    "raw_text": window_text,
                    "token_count": estimate_tokens(contextual_text),
                    "source_file": md_file.name,
                    "updated_at": updated_at,
                }
                page_chunks.append(chunk_record)

        if page_chunks:
            page_file = safe_child_path(CHUNKS_DIR, f"{page_id}_{page_title}_chunks.json")
            page_file.write_text(json.dumps(page_chunks, ensure_ascii=False, indent=2), encoding="utf-8")
            all_chunks.extend(page_chunks)
            stats["chunks_generated"] += len(page_chunks)

    CHUNKS_FILE.write_text(json.dumps(all_chunks, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Generated {stats['chunks_generated']} chunks successfully at {CHUNKS_FILE}")
    for k, v in stats.items():
        print(f"  {k}={v}")
    return all_chunks


if __name__ == "__main__":
    process_chunks()