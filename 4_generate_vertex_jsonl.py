"""Option 2 Step 4: Export manually processed chunks into both BigQuery-ready flat JSONL and Vertex GCS JSONL.

Outputs:
1. `wikimedia-data/jsonl-for-vertex/chunks/chunks_bq.jsonl`:
   Flat BigQuery-compatible JSONL (`dataSchema="custom"`, `idField="id"`) matching Option 1's exact column schema.
2. `wikimedia-data/jsonl-for-vertex/chunks/chunks-for-vertexai.jsonl` AND
   `wikimedia-data/processed/chunks/vertex_search_documents.jsonl`:
   Vertex GCS `document`-schema JSONL (`{"id": ..., "structData": {...}}`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from wiki_utils import estimate_tokens, make_rfc1034_id

ROOT = Path(__file__).resolve().parent
CHUNKS_DIR = ROOT / "wikimedia-data" / "processed" / "chunks"
AGGREGATE_CHUNKS_FILE = CHUNKS_DIR / "chunks.json"

LEGACY_VERTEX_DOCS_OUT = CHUNKS_DIR / "vertex_search_documents.jsonl"
VERTEX_CHUNKS_DIR = ROOT / "wikimedia-data" / "jsonl-for-vertex" / "chunks"
BQ_CHUNKS_OUT = VERTEX_CHUNKS_DIR / "chunks_bq.jsonl"
GCS_CHUNKS_OUT = VERTEX_CHUNKS_DIR / "chunks-for-vertexai.jsonl"


def generate_option2_jsonl() -> Dict[str, Any]:
    VERTEX_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

    if AGGREGATE_CHUNKS_FILE.exists():
        items = json.loads(AGGREGATE_CHUNKS_FILE.read_text(encoding="utf-8"))
    else:
        items = []
        for chunk_file in sorted(CHUNKS_DIR.glob("*_chunks.json")):
            data = json.loads(chunk_file.read_text(encoding="utf-8"))
            if isinstance(data, list):
                items.extend(data)

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

        flat_row: Dict[str, Any] = {
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
        flat_rows.append(flat_row)

    with (
        BQ_CHUNKS_OUT.open("w", encoding="utf-8") as f_bq,
        GCS_CHUNKS_OUT.open("w", encoding="utf-8") as f_gcs,
        LEGACY_VERTEX_DOCS_OUT.open("w", encoding="utf-8") as f_legacy,
    ):
        for row in flat_rows:
            f_bq.write(json.dumps(row, ensure_ascii=False) + "\n")
            vertex_doc = {
                "id": row["id"],
                "structData": {k: v for k, v in row.items() if k != "id"},
            }
            doc_line = json.dumps(vertex_doc, ensure_ascii=False) + "\n"
            f_gcs.write(doc_line)
            f_legacy.write(doc_line)

    if not flat_rows:
        raise RuntimeError("No chunks found to export. Run 2_clean_wikitext.py and 3_chunk_pages.py first.")

    chunks_with_cats = sum(1 for r in flat_rows if r["categories"])
    non_overview = sum(1 for r in flat_rows if r["section_title"] != "Overview")
    print("Option 2 (Manual Clean + Chunk) JSONL summary:")
    print(f"  created_jsonl={len(flat_rows)}")
    print(f"  chunks_with_categories={chunks_with_cats}")
    print(f"  chunks_with_named_sections={non_overview}")
    print(f"  bq_jsonl={BQ_CHUNKS_OUT}")
    print(f"  vertex_gcs_jsonl={GCS_CHUNKS_OUT}")
    print(f"  first_id={flat_rows[0]['id']}")
    print(f"  first_title={flat_rows[0]['title']}")
    return {
        "rows": len(flat_rows),
        "chunks_with_categories": chunks_with_cats,
        "chunks_with_named_sections": non_overview,
    }


if __name__ == "__main__":
    generate_option2_jsonl()
