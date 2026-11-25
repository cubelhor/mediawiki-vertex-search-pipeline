"""Option 1 Pipeline: Build complete Raw + Templates datasets (with categories, aliases & namespaces)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from wiki_utils import (
    RawPageRecord,
    TemplateRecord,
    build_page_title_index,
    estimate_tokens,
    get_logger,
    load_all_raw_pages,
    load_all_template_records,
    load_template_registry,
    make_rfc1034_id,
    merge_unique_strings,
    normalize_template_key,
    resolve_page_by_title,
    to_vertex_struct_doc,
)

logger = get_logger("1b_build_raw_with_templates")
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "wikimedia-data"
RAW_API_DIR = DATA_DIR / "raw_api"
TEMPLATE_DIR = DATA_DIR / "templates"
RAW_JSONL_DIR = DATA_DIR / "raw_jsonl"
VERTEX_RAW_DIR = DATA_DIR / "jsonl-for-vertex" / "rawapi"


def build_inlined_raw_content(
    page: RawPageRecord,
    template_registry: Dict[str, TemplateRecord],
    page_title_index: Dict[str, RawPageRecord],
) -> str:
    """Combine a page's full raw wikitext with all templates and subpages it transcludes."""
    sections: List[str] = [page.wikitext.strip()]

    if page.aliases:
        sections.insert(0, f"<!-- Redirect Aliases: {', '.join(page.aliases)} -->")

    inlined_blocks: List[str] = []
    for tpl_name in page.referenced_template_names:
        tpl = template_registry.get(normalize_template_key(tpl_name))
        if not tpl:
            continue
        body = tpl.transcludable_wikitext or tpl.raw_wikitext.strip()
        if body:
            inlined_blocks.append(f"--- Template:{tpl.name} (page_id={tpl.page_id}) ---\n{body}")

    inlined_subpages: List[str] = []
    seen_sub_pids = {page.page_id}
    for sub_title in page.referenced_page_titles:
        sub_rec = resolve_page_by_title(sub_title, page_title_index, follow_redirects=True)
        if not sub_rec or sub_rec.page_id in seen_sub_pids or sub_rec.is_redirect:
            continue
        sub_body = sub_rec.wikitext.strip()
        if sub_body:
            seen_sub_pids.add(sub_rec.page_id)
            inlined_subpages.append(f"--- Transcluded Page:{sub_rec.title} (page_id={sub_rec.page_id}) ---\n{sub_body}")

    if inlined_subpages:
        sections.append("=== Transcluded Wiki Subpages ===\n" + "\n\n".join(inlined_subpages))
    if inlined_blocks:
        sections.append("=== Referenced MediaWiki Templates ===\n" + "\n\n".join(inlined_blocks))

    return "\n\n".join(part for part in sections if part).strip()


def build_option1_raw_datasets(
    all_templates: Optional[List[TemplateRecord]] = None,
    template_registry: Optional[Dict[str, TemplateRecord]] = None,
    pages: Optional[List[RawPageRecord]] = None,
    write_debug_raw_jsonl: bool = False,
    write_gcs_doc_jsonl: bool = True,
) -> Dict[str, int]:
    VERTEX_RAW_DIR.mkdir(parents=True, exist_ok=True)
    if write_debug_raw_jsonl:
        RAW_JSONL_DIR.mkdir(parents=True, exist_ok=True)

    if all_templates is None:
        all_templates = load_all_template_records(TEMPLATE_DIR)
    if template_registry is None:
        template_registry = load_template_registry(records=all_templates)
    if pages is None:
        pages = load_all_raw_pages(RAW_API_DIR, template_registry=template_registry)

    page_title_index = build_page_title_index(pages)

    bq_jsonl_out = VERTEX_RAW_DIR / "raw_with_templates_bq.jsonl"
    gcs_jsonl_out = VERTEX_RAW_DIR / "rawapi-for-vertexai.jsonl"

    bq_rows: List[Dict[str, Any]] = []
    raw_pages_debug: List[Dict[str, Any]] = []
    raw_pages_tpl_debug: List[Dict[str, Any]] = []
    raw_templates_debug: List[Dict[str, Any]] = []

    for page in pages:
        resolved_subpage_titles = []
        for sub_title in page.referenced_page_titles:
            sub_rec = resolve_page_by_title(sub_title, page_title_index, follow_redirects=True)
            if sub_rec and not sub_rec.is_redirect and sub_rec.page_id != page.page_id:
                resolved_subpage_titles.append(f":{sub_rec.title}")

        all_transclusion_refs = merge_unique_strings(
            page.referenced_template_names, resolved_subpage_titles
        )

        if write_debug_raw_jsonl:
            base_record = {
                "id": str(page.page_id),
                "page_id": page.page_id,
                "ns": page.ns,
                "title": page.title,
                "uri": page.uri,
                "batch_file": page.batch_file,
                "revid": page.revid,
                "updated_at": page.updated_at,
                "content_model": page.content_model,
                "is_redirect": page.is_redirect,
                "redirect_target": page.redirect_target,
                "categories": page.categories,
                "aliases": page.aliases,
                "wikitext": page.wikitext,
            }
            raw_pages_debug.append(base_record)
            referenced_tpl_objs = [
                {
                    "name": tpl.name,
                    "title": tpl.title,
                    "page_id": str(tpl.page_id),
                    "wikitext": tpl.raw_wikitext,
                    "transcludable_wikitext": tpl.transcludable_wikitext,
                }
                for tpl_name in page.referenced_template_names
                if (tpl := template_registry.get(normalize_template_key(tpl_name))) is not None
            ]
            raw_pages_tpl_debug.append(
                {
                    **base_record,
                    "referenced_template_names": all_transclusion_refs,
                    "referenced_templates": referenced_tpl_objs,
                }
            )

        combined_content = build_inlined_raw_content(page, template_registry, page_title_index)
        if not combined_content:
            continue

        bq_rows.append(
            {
                "id": make_rfc1034_id(f"page-{page.page_id}"),
                "doc_type": "raw_page",
                "ns": page.ns,
                "title": page.title,
                "content": combined_content,
                "page_id": page.page_id,
                "page_title": page.title,
                "section_title": page.title,
                "chunk_type": "raw_redirect" if page.is_redirect else "raw_page",
                "categories": page.categories,
                "referenced_templates": all_transclusion_refs,
                "aliases": page.aliases,
                "is_redirect": page.is_redirect,
                "redirect_target": page.redirect_target,
                "uri": page.uri,
                "source_file": page.batch_file,
                "token_count": estimate_tokens(combined_content),
                "updated_at": page.updated_at,
            }
        )

    for tpl in all_templates:
        if write_debug_raw_jsonl:
            raw_templates_debug.append(
                {
                    "id": str(tpl.page_id),
                    "page_id": str(tpl.page_id),
                    "ns": 10,
                    "title": tpl.title,
                    "name": tpl.name,
                    "uri": tpl.uri,
                    "updated_at": tpl.updated_at,
                    "is_redirect": tpl.is_redirect,
                    "redirect_target": tpl.redirect_target,
                    "categories": tpl.own_categories,
                    "inherited_categories": tpl.inherited_categories,
                    "wikitext": tpl.raw_wikitext,
                    "transcludable_wikitext": tpl.transcludable_wikitext,
                }
            )

        tpl_content = tpl.raw_wikitext.strip()
        if not tpl_content:
            continue

        doc_id = (
            make_rfc1034_id(f"template-{tpl.page_id}")
            if tpl.page_id
            else make_rfc1034_id(f"template-{tpl.name}")
        )
        bq_rows.append(
            {
                "id": doc_id,
                "doc_type": "raw_template",
                "ns": 10,
                "title": tpl.title,
                "content": tpl_content,
                "page_id": tpl.page_id,
                "page_title": tpl.title,
                "section_title": tpl.title,
                "chunk_type": "raw_template_redirect" if tpl.is_redirect else "raw_template",
                "categories": tpl.own_categories,
                "referenced_templates": [],
                "aliases": [],
                "is_redirect": tpl.is_redirect,
                "redirect_target": tpl.redirect_target,
                "uri": tpl.uri,
                "source_file": tpl.source_file,
                "token_count": estimate_tokens(tpl_content),
                "updated_at": tpl.updated_at,
            }
        )

    if write_debug_raw_jsonl:
        for out_path, rows in [
            (RAW_JSONL_DIR / "mediawiki_raw_pages.jsonl", raw_pages_debug),
            (RAW_JSONL_DIR / "mediawiki_raw_pages_with_templates.jsonl", raw_pages_tpl_debug),
            (RAW_JSONL_DIR / "mediawiki_raw_templates.jsonl", raw_templates_debug),
        ]:
            with out_path.open("w", encoding="utf-8") as fh:
                for rec in rows:
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    seen_ids = set()
    f_gcs = gcs_jsonl_out.open("w", encoding="utf-8") if write_gcs_doc_jsonl else None
    try:
        with bq_jsonl_out.open("w", encoding="utf-8") as f_bq:
            for row in bq_rows:
                if row["id"] in seen_ids:
                    raise ValueError(f"Duplicate Option 1 document ID generated: {row['id']}")
                seen_ids.add(row["id"])
                f_bq.write(json.dumps(row, ensure_ascii=False) + "\n")
                if f_gcs is not None:
                    f_gcs.write(json.dumps(to_vertex_struct_doc(row), ensure_ascii=False) + "\n")
    finally:
        if f_gcs is not None:
            f_gcs.close()

    stats = {
        "raw_pages": len(pages),
        "empty_wikitext_pages": sum(1 for p in pages if not p.wikitext.strip()),
        "pages_with_categories": sum(1 for p in pages if p.categories),
        "pages_with_referenced_templates_or_subpages": sum(
            1 for p in pages if p.referenced_template_names or p.referenced_page_titles
        ),
        "raw_templates": len(all_templates),
        "total_option1_records": len(bq_rows),
    }
    logger.info(
        "Option 1 summary: total_records=%d (raw_pages=%d, raw_templates=%d, with_cats=%d, bq_jsonl=%s)",
        stats["total_option1_records"],
        stats["raw_pages"],
        stats["raw_templates"],
        stats["pages_with_categories"],
        bq_jsonl_out,
    )
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Option 1 (Raw + Templates) JSONL datasets.")
    parser.add_argument(
        "--write-debug-raw-jsonl",
        action="store_true",
        help="Also write intermediate debug dumps under wikimedia-data/raw_jsonl/.",
    )
    args = parser.parse_args()
    build_option1_raw_datasets(write_debug_raw_jsonl=args.write_debug_raw_jsonl)
