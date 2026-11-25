"""Option 2 Step 4: Export manually processed chunks into BigQuery-ready flat JSONL and Vertex GCS JSONL."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from wiki_utils import estimate_tokens, get_logger, make_rfc1034_id, to_vertex_struct_doc

logger = get_logger("4_generate_vertex_jsonl")
ROOT = Path(__file__).resolve().parent
CHUNKS_DIR = ROOT / "wikimedia-data" / "processed" / "chunks"
AGGREGATE_CHUNKS_FILE = CHUNKS_DIR / "chunks.json"

VERTEX_CHUNKS_DIR = ROOT / "wikimedia-data" / "jsonl-for-vertex" / "chunks"
BQ_CHUNKS_OUT = VERTEX_CHUNKS_DIR / "chunks_bq.jsonl"
GCS_CHUNKS_OUT = VERTEX_CHUNKS_DIR / "chunks-for-vertexai.jsonl"


def generate_option2_jsonl(
    chunks: Optional[List[Dict[str, Any]]] = None,
    write_gcs_doc_jsonl: bool = True,
) -> Dict[str, Any]:
    VERTEX_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

    # Remove legacy duplicate file if left over from an earlier build
    legacy_dup = CHUNKS_DIR / "vertex_search_documents.jsonl"
    if legacy_dup.is_file():
        legacy_dup.unlink()

    if chunks is not None:
        items = chunks
    elif AGGREGATE_CHUNKS_FILE.exists():
        items = json.loads(AGGREGATE_CHUNKS_FILE.read_text(encoding="utf-8"))
    else:
        raise FileNotFoundError(
            f"Missing {AGGREGATE_CHUNKS_FILE}. Run 2_clean_wikitext.py and 3_chunk_pages.py first."
        )

    flat_rows: List[Dict[str, Any]] = []
    seen_ids = set()

    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue

        page_id = int(item.get("page_id") or 0)
        page_title = str(item.get("page_title") or "Untitled").strip()
        section_title = str(item.get("section_title") or "Overview").strip()

        display_title = (
            page_title
            if (not section_title or section_title == "Overview" or section_title == page_title)
            else f"{page_title} — {section_title}"
        )

        record_id = make_rfc1034_id(item.get("id") or f"chunk-{page_id}-{idx:03d}", fallback=f"chunk-{idx}")
        if record_id in seen_ids:
            raise ValueError(f"Duplicate chunk ID encountered: {record_id}")
        seen_ids.add(record_id)

        flat_rows.append(
            {
                "id": record_id,
                "doc_type": "manual_chunk",
                "ns": int(item.get("ns") or 0),
                "title": display_title,
                "content": text,
                "page_id": page_id,
                "page_title": page_title,
                "section_title": section_title,
                "chunk_type": str(item.get("chunk_type") or "unknown"),
                "categories": list(item.get("categories") or []),
                "referenced_templates": list(item.get("referenced_templates") or []),
                "aliases": list(item.get("aliases") or []),
                "is_redirect": bool(item.get("is_redirect", False)),
                "redirect_target": item.get("redirect_target"),
                "uri": str(item.get("uri") or ""),
                "source_file": str(item.get("source_file") or ""),
                "token_count": int(item.get("token_count") or estimate_tokens(text)),
                "updated_at": item.get("updated_at"),
            }
        )

    if not flat_rows:
        raise RuntimeError("No chunks found to export. Run 2_clean_wikitext.py and 3_chunk_pages.py first.")

    f_gcs = GCS_CHUNKS_OUT.open("w", encoding="utf-8") if write_gcs_doc_jsonl else None
    try:
        with BQ_CHUNKS_OUT.open("w", encoding="utf-8") as f_bq:
            for row in flat_rows:
                f_bq.write(json.dumps(row, ensure_ascii=False) + "\n")
                if f_gcs is not None:
                    f_gcs.write(json.dumps(to_vertex_struct_doc(row), ensure_ascii=False) + "\n")
    finally:
        if f_gcs is not None:
            f_gcs.close()

    chunks_with_cats = sum(1 for r in flat_rows if r["categories"])
    non_overview = sum(1 for r in flat_rows if r["section_title"] != "Overview")
    logger.info(
        "Option 2 JSONL summary: rows=%d, with_categories=%d, named_sections=%d, bq_jsonl=%s",
        len(flat_rows),
        chunks_with_cats,
        non_overview,
        BQ_CHUNKS_OUT,
    )
    return {
        "rows": len(flat_rows),
        "chunks_with_categories": chunks_with_cats,
        "chunks_with_named_sections": non_overview,
    }


if __name__ == "__main__":
    generate_option2_jsonl()
