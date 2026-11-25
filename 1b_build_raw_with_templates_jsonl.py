"""Option 1 Pipeline: Build complete Raw + Templates datasets (with categories, aliases & namespaces).

Generates:
1. Refreshed `wikimedia-data/raw_jsonl/`:
   - `mediawiki_raw_pages.jsonl` (all pages across all namespaces, 100% populated wikitext + categories + aliases)
   - `mediawiki_raw_pages_with_templates.jsonl` (pages + referenced template & transcluded subpage definitions)
   - `mediawiki_raw_templates.jsonl` (all unique templates + categories + redirect metadata)
2. Datastore 1 exports in `wikimedia-data/jsonl-for-vertex/rawapi/`:
   - `raw_with_templates_bq.jsonl`: Flat BigQuery-ready JSONL (`dataSchema="custom"`, `idField="id"`)
     containing all raw pages (with referenced templates & transcluded subpages appended inline) PLUS all
     standalone `Template:*` documents.
   - `rawapi-for-vertexai.jsonl`: Vertex GCS `document`-schema JSONL (`{"id": ..., "structData": ...}`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from wiki_utils import (
    RawPageRecord,
    TemplateRecord,
    build_page_title_index,
    estimate_tokens,
    load_all_raw_pages,
    load_all_template_records,
    load_template_registry,
    make_rfc1034_id,
    merge_unique_strings,
    normalize_template_key,
    resolve_page_by_title,
)

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
        if not body:
            continue
        inlined_blocks.append(f"--- Template:{tpl.name} (page_id={tpl.page_id}) ---\n{body}")

    inlined_subpages: List[str] = []
    seen_sub_pids = {page.page_id}
    for sub_title in page.referenced_page_titles:
        sub_rec = resolve_page_by_title(sub_title, page_title_index, follow_redirects=True)
        if not sub_rec or sub_rec.page_id in seen_sub_pids or sub_rec.is_redirect:
            continue
        sub_body = sub_rec.wikitext.strip()
        if not sub_body:
            continue
        seen_sub_pids.add(sub_rec.page_id)
        inlined_subpages.append(f"--- Transcluded Page:{sub_rec.title} (page_id={sub_rec.page_id}) ---\n{sub_body}")

    if inlined_subpages:
        sections.append("=== Transcluded Wiki Subpages ===\n" + "\n\n".join(inlined_subpages))

    if inlined_blocks:
        sections.append(
            "=== Referenced MediaWiki Templates ===\n" + "\n\n".join(inlined_blocks)
        )

    return "\n\n".join(part for part in sections if part).strip()


def _to_vertex_struct_doc(flat_row: Dict[str, Any]) -> Dict[str, Any]:
    struct_data = {k: v for k, v in flat_row.items() if k != "id"}
    return {
        "id": flat_row["id"],
        "structData": struct_data,
    }


def build_option1_raw_datasets() -> Dict[str, int]:
    RAW_JSONL_DIR.mkdir(parents=True, exist_ok=True)
    VERTEX_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Remove stale 0-byte artifact in templates/ if present
    stale_fdy = TEMPLATE_DIR / "FDy"
    if stale_fdy.is_file() and stale_fdy.stat().st_size == 0:
        stale_fdy.unlink()

    all_templates = load_all_template_records(TEMPLATE_DIR)
    template_registry = load_template_registry(TEMPLATE_DIR)
    pages = load_all_raw_pages(RAW_API_DIR, template_registry=template_registry)
    page_title_index = build_page_title_index(pages)

    raw_pages_out = RAW_JSONL_DIR / "mediawiki_raw_pages.jsonl"
    raw_pages_tpl_out = RAW_JSONL_DIR / "mediawiki_raw_pages_with_templates.jsonl"
    raw_templates_out = RAW_JSONL_DIR / "mediawiki_raw_templates.jsonl"

    bq_jsonl_out = VERTEX_RAW_DIR / "raw_with_templates_bq.jsonl"
    gcs_jsonl_out = VERTEX_RAW_DIR / "rawapi-for-vertexai.jsonl"

    bq_rows: List[Dict[str, Any]] = []

    with (
        raw_pages_out.open("w", encoding="utf-8") as f_pages,
        raw_pages_tpl_out.open("w", encoding="utf-8") as f_pages_tpl,
    ):
        for page in pages:
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
            f_pages.write(json.dumps(base_record, ensure_ascii=False) + "\n")

            referenced_tpl_objs = []
            for tpl_name in page.referenced_template_names:
                tpl = template_registry.get(normalize_template_key(tpl_name))
                if tpl:
                    referenced_tpl_objs.append(
                        {
                            "name": tpl.name,
                            "title": tpl.title,
                            "page_id": str(tpl.page_id),
                            "wikitext": tpl.raw_wikitext,
                            "transcludable_wikitext": tpl.transcludable_wikitext,
                        }
                    )

            resolved_subpage_titles = []
            for sub_title in page.referenced_page_titles:
                sub_rec = resolve_page_by_title(sub_title, page_title_index, follow_redirects=True)
                if sub_rec and not sub_rec.is_redirect and sub_rec.page_id != page.page_id:
                    resolved_subpage_titles.append(f":{sub_rec.title}")

            all_transclusion_refs = merge_unique_strings(
                page.referenced_template_names, resolved_subpage_titles
            )

            with_tpl_record = {
                **base_record,
                "referenced_template_names": all_transclusion_refs,
                "referenced_templates": referenced_tpl_objs,
            }
            f_pages_tpl.write(json.dumps(with_tpl_record, ensure_ascii=False) + "\n")

            combined_content = build_inlined_raw_content(
                page, template_registry, page_title_index
            )
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

    with raw_templates_out.open("w", encoding="utf-8") as f_tpls:
        for tpl in all_templates:
            tpl_record = {
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
            f_tpls.write(json.dumps(tpl_record, ensure_ascii=False) + "\n")

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

    seen_ids = set()
    with (
        bq_jsonl_out.open("w", encoding="utf-8") as f_bq,
        gcs_jsonl_out.open("w", encoding="utf-8") as f_gcs,
    ):
        for row in bq_rows:
            if row["id"] in seen_ids:
                raise ValueError(f"Duplicate Option 1 document ID generated: {row['id']}")
            seen_ids.add(row["id"])
            f_bq.write(json.dumps(row, ensure_ascii=False) + "\n")
            f_gcs.write(json.dumps(_to_vertex_struct_doc(row), ensure_ascii=False) + "\n")

    empty_pages = sum(1 for p in pages if not p.wikitext.strip())
    pages_with_cats = sum(1 for p in pages if p.categories)
    pages_with_tpls = sum(
        1 for p in pages if p.referenced_template_names or p.referenced_page_titles
    )

    stats = {
        "raw_pages": len(pages),
        "empty_wikitext_pages": empty_pages,
        "pages_with_categories": pages_with_cats,
        "pages_with_referenced_templates_or_subpages": pages_with_tpls,
        "raw_templates": len(all_templates),
        "total_option1_records": len(bq_rows),
    }
    print("Option 1 (Raw + Templates) build summary:")
    for k, v in stats.items():
        print(f"  {k}={v}")
    print(f"  bq_jsonl={bq_jsonl_out}")
    print(f"  vertex_gcs_jsonl={gcs_jsonl_out}")
    return stats


if __name__ == "__main__":
    build_option1_raw_datasets()
