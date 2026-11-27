"""Unit tests for the unified MediaWiki `action=parse` HTML pipeline (Option 1 GCS + Option 2 BigQuery)."""

from __future__ import annotations

import importlib
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import crawl_parse_html  # noqa: E402
import html_to_bq_chunks  # noqa: E402
import run_parse_pipeline  # noqa: E402

_step5 = importlib.import_module("5_vertex_import_jsonl")


def _mock_resp(json_data, status_code: int = 200):
    r = MagicMock()
    r.status_code = status_code
    r.headers = {"Content-Type": "application/json; charset=utf-8"}
    r.text = json.dumps(json_data)
    r.raise_for_status = MagicMock()
    r.json = MagicMock(return_value=json_data)
    return r


SAMPLE_ABBREVIATIONS_HTML = """
<div class="mw-parser-output">
  <div id="toc" class="toc"><h2>Contents</h2><ul><li>A</li><li>P</li></ul></div>
  <p>Official abbreviations reference page.</p>
  <h2><span class="mw-headline" id="P">P</span><span class="mw-editsection">[edit]</span></h2>
  <p><a href="https://lexinet-b4c2-p.apps.ocp02.airbus.corp/results/ABBREVIATION/P">Lexinet letter P</a></p>
  <table class="wikitable" width="100%">
    <tbody><tr>
      <td><b><div id="PCP">PCP</div></b></td>
      <td>Public Cloud Platform (<a href="/wiki/Category:IT_and_Software">Category:IT and Software</a>)</td>
    </tr></tbody>
  </table>
  <table class="wikitable">
    <tbody>
      <tr><th>Code</th><th>Meaning</th><th>Domain</th></tr>
      <tr><td>OP</td><td>Operative Planning</td><td>Finance</td></tr>
      <tr><td>P2P</td><td>Procure to Pay</td><td>Finance</td></tr>
    </tbody>
  </table>
  <h3><span class="mw-headline" id="Sub_Notes">Sub Notes</span></h3>
  <p>Run the following helper:</p>
  <pre>python run_parse_pipeline.py --page-ids 3687</pre>
  <div class="navbox">Should be stripped from Option 2 chunks!</div>
</div>
"""


class ActionParsePipelineTests(unittest.TestCase):
    def test_html_sanitizer_strips_active_content_and_escapes_headers(self) -> None:
        unsafe_body = (
            '<div onclick="alert(1)" class="ok">'
            '<script>evil()</script>'
            '<iframe src="https://evil.example"></iframe>'
            '<a href="javascript:alert(1)">BadLink</a>'
            '<a href="https://example.com/doc">GoodLink</a>'
            "<p>Safe content</p>"
            "</div>"
        )
        doc = crawl_parse_html.build_standalone_html_document(
            title='Title <img src=x onerror="alert(1)">',
            body_html=unsafe_body,
            aliases=["Alias<script>x</script>"],
            categories=["Cat & Co"],
        )
        self.assertNotIn("<script>", doc)
        self.assertNotIn("<iframe", doc)
        self.assertNotIn("onclick=", doc)
        self.assertNotIn("javascript:", doc)
        self.assertIn("&lt;img src=x onerror=&quot;alert(1)&quot;&gt;", doc)
        self.assertIn("Aliases: Alias&lt;script&gt;x&lt;/script&gt;", doc)
        self.assertIn("Categories: Cat &amp; Co", doc)
        self.assertIn('href="https://example.com/doc"', doc)
        self.assertIn("Safe content", doc)

    def test_crawl_parse_incremental_cache_and_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "parsed_html"

            # First run: Pass 1 (page-id lookup) + Pass 2 (action=parse)
            sess1 = MagicMock()
            sess1.trust_env = True
            sess1._mw_logged_in = True
            sess1.cookies = []
            sess1.headers = {}
            sess1.verify = True
            sess1.get.side_effect = [
                _mock_resp(
                    {
                        "query": {
                            "pages": [
                                {
                                    "pageid": 3687,
                                    "ns": 0,
                                    "title": "Abbreviations",
                                    "fullurl": "https://aow.eu.airbus.corp/wiki/Abbreviations",
                                    "lastrevid": 9001,
                                    "touched": "2026-11-10T12:34:27Z",
                                    "categories": [{"title": "Category:Abbreviations"}],
                                    "revisions": [{"revid": 9001, "timestamp": "2026-11-10T12:34:27Z"}],
                                    "linkshere": [{"title": "Acronyms", "redirect": True}],
                                }
                            ]
                        }
                    }
                ),
                _mock_resp(
                    {
                        "parse": {
                            "title": "Abbreviations",
                            "pageid": 3687,
                            "text": SAMPLE_ABBREVIATIONS_HTML,
                            "categories": [{"category": "Documentation", "hidden": False}],
                        }
                    }
                ),
            ]

            manifest1 = crawl_parse_html.crawl_parsed_html_wiki(
                output_dir=out_dir,
                gcs_bucket="mediawiki-data-nov-25",
                page_ids=[3687],
                workers=1,
                session=sess1,
            )
            self.assertEqual(manifest1["total_pages"], 1)
            self.assertEqual(manifest1["parsed_count"], 1)
            self.assertEqual(manifest1["reused_count"], 0)
            self.assertTrue((out_dir / "pages" / "page-3687.html").is_file())

            meta_lines = (out_dir / "metadata.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(meta_lines), 1)
            doc_rec = json.loads(meta_lines[0])
            self.assertEqual(doc_rec["id"], "page-3687")
            self.assertEqual(doc_rec["content"]["mimeType"], "text/html")
            self.assertEqual(
                doc_rec["content"]["uri"],
                "gs://mediawiki-data-nov-25/option1_html/pages/page-3687.html",
            )
            self.assertEqual(doc_rec["structData"]["aliases"], ["Acronyms"])
            self.assertEqual(doc_rec["structData"]["categories"], ["Abbreviations", "Documentation"])
            self.assertEqual(len((out_dir / "metadata_incremental.jsonl").read_text(encoding="utf-8").splitlines()), 1)

            # Second run with SAME revid=9001 -> must reuse cached HTML (0 action=parse calls!)
            sess2 = MagicMock()
            sess2.trust_env = True
            sess2._mw_logged_in = True
            sess2.cookies = []
            sess2.headers = {}
            sess2.verify = True
            sess2.get.side_effect = [
                _mock_resp(
                    {
                        "query": {
                            "pages": [
                                {
                                    "pageid": 3687,
                                    "ns": 0,
                                    "title": "Abbreviations",
                                    "fullurl": "https://aow.eu.airbus.corp/wiki/Abbreviations",
                                    "lastrevid": 9001,
                                    "touched": "2026-11-10T12:34:27Z",
                                    "categories": [{"title": "Category:Abbreviations"}],
                                    "revisions": [{"revid": 9001, "timestamp": "2026-11-10T12:34:27Z"}],
                                    "linkshere": [{"title": "Acronyms", "redirect": True}],
                                }
                            ]
                        }
                    }
                ),
            ]
            manifest2 = crawl_parse_html.crawl_parsed_html_wiki(
                output_dir=out_dir,
                gcs_bucket="mediawiki-data-nov-25",
                page_ids=[3687],
                workers=1,
                session=sess2,
            )
            self.assertEqual(manifest2["parsed_count"], 0)
            self.assertEqual(manifest2["reused_count"], 1)
            self.assertEqual(sess2.get.call_count, 1)  # Only Pass 1 metadata call!
            self.assertEqual((out_dir / "metadata_incremental.jsonl").stat().st_size, 0)

    def test_html_to_bq_chunks_tables_breadcrumbs_anchors_and_schema(self) -> None:
        standalone = crawl_parse_html.build_standalone_html_document(
            title="Abbreviations",
            body_html=SAMPLE_ABBREVIATIONS_HTML,
            aliases=["Acronyms"],
            categories=["Abbreviations", "Documentation"],
        )
        page_meta = {
            "page_id": 3687,
            "ns": 0,
            "title": "Abbreviations",
            "uri": "https://aow.eu.airbus.corp/wiki/Abbreviations",
            "aliases": ["Acronyms"],
            "categories": ["Abbreviations", "Documentation"],
            "updated_at": "2026-11-10T12:34:27Z",
            "html_file": "page-3687.html",
        }
        chunks = html_to_bq_chunks.chunk_parsed_html_page(
            page_meta=page_meta,
            html_text=standalone,
            max_tokens=450,
            min_merge_tokens=1,  # Keep sections separate to verify exact anchors and breadcrumbs
        )
        self.assertGreaterEqual(len(chunks), 3)

        # Verify navbox / editsection / toc are stripped
        combined = "\n".join(c["content"] for c in chunks)
        self.assertNotIn("Should be stripped from Option 2 chunks!", combined)
        self.assertNotIn("[edit]", combined)
        self.assertIn("- **PCP**: Public Cloud Platform", combined)

        # Verify multi-row data table has EXACTLY ONE separator line (not repeated per row!)
        p_chunk = next(c for c in chunks if c["section_title"] == "P")
        self.assertEqual(p_chunk["uri"], "https://aow.eu.airbus.corp/wiki/Abbreviations#P")
        self.assertEqual(p_chunk["content"].count("| --- | --- | --- |"), 1)
        self.assertIn("| Code | Meaning | Domain |", p_chunk["content"])
        self.assertIn("| P2P | Procure to Pay | Finance |", p_chunk["content"])

        # Verify nested H2 -> H3 breadcrumb & deep-link anchor & fenced code block
        sub_chunk = next(c for c in chunks if c["section_title"] == "P > Sub Notes")
        self.assertEqual(sub_chunk["uri"], "https://aow.eu.airbus.corp/wiki/Abbreviations#Sub_Notes")
        self.assertIn("```\npython run_parse_pipeline.py --page-ids 3687\n```", sub_chunk["content"])
        self.assertIn("Aliases: Acronyms", sub_chunk["content"])

        # Verify exact 18-column BigQuery schema parity & RFC-1034 IDs
        expected_bq_cols = {f.name for f in _step5.get_bq_schema_fields()}
        rfc_re = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
        for row in chunks:
            self.assertEqual(set(row.keys()), expected_bq_cols)
            self.assertTrue(rfc_re.match(row["id"]), f"Non-RFC-1034 id: {row['id']}")

    def test_vertex_unstructured_html_datastore_config_and_zero_change_skip(self) -> None:
        from google.cloud import discoveryengine_v1 as v1

        ds = _step5.build_unstructured_html_datastore_config(
            display_name="Airbus Wiki - Option 1 HTML (Vertex Layout Parser)",
            chunk_size=500,
            include_ancestor_headings=True,
        )
        self.assertEqual(ds.content_config, v1.DataStore.ContentConfig.CONTENT_REQUIRED)
        self.assertEqual(
            ds.document_processing_config.chunking_config.layout_based_chunking_config.chunk_size,
            500,
        )
        self.assertTrue(
            ds.document_processing_config.chunking_config.layout_based_chunking_config.include_ancestor_headings
        )
        self.assertIn(
            "navbox",
            list(ds.document_processing_config.default_parsing_config.layout_parsing_config.exclude_html_classes),
        )

        # Verify empty metadata_incremental.jsonl skips Vertex import completely when use_incremental_manifest=True
        with tempfile.TemporaryDirectory() as tmp:
            parsed_dir = Path(tmp) / "parsed_html"
            (parsed_dir / "pages").mkdir(parents=True)
            (parsed_dir / "metadata.jsonl").write_text('{"id":"page-1"}\n', encoding="utf-8")
            (parsed_dir / "metadata_incremental.jsonl").write_text("", encoding="utf-8")

            specs = _step5.build_option_specs(
                gcs_prefix="",
                raw_datastore_id="raw",
                chunked_datastore_id="chunked",
                parsed_html_dir=parsed_dir,
            )
            html_native_spec = [s for s in specs if s.key == "html_native"]
            with patch.object(_step5, "ensure_datastore") as mock_ensure:
                _step5.import_into_vertex_datastores(
                    bq_project="prj-c9ec-data-dom-dev-ec",
                    bucket_name="mediawiki-data-nov-25",
                    view_dataset_id="wiki_vertex_authorized",
                    vertex_project="prj-7efd-everydai-dev-4f9a",
                    vertex_location="eu",
                    specs=html_native_spec,
                    reconciliation_mode="auto",
                    use_incremental_manifest=True,
                    dry_run=False,
                )
                mock_ensure.assert_not_called()

    def test_run_parse_pipeline_end_to_end_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parsed_dir = Path(tmp) / "parsed_html"
            pages_dir = parsed_dir / "pages"
            pages_dir.mkdir(parents=True)
            standalone = crawl_parse_html.build_standalone_html_document(
                title="Abbreviations",
                body_html=SAMPLE_ABBREVIATIONS_HTML,
                aliases=["Acronyms"],
                categories=["Abbreviations"],
            )
            (pages_dir / "page-3687.html").write_text(standalone, encoding="utf-8")
            (parsed_dir / "metadata.jsonl").write_text('{"id":"page-3687"}\n', encoding="utf-8")
            (parsed_dir / "metadata_incremental.jsonl").write_text('{"id":"page-3687"}\n', encoding="utf-8")
            (parsed_dir / "pages_manifest.json").write_text(
                json.dumps(
                    {
                        "total_pages": 1,
                        "parsed_count": 1,
                        "reused_count": 0,
                        "failed_ids": [],
                        "pages": [
                            {
                                "id": "page-3687",
                                "page_id": 3687,
                                "ns": 0,
                                "title": "Abbreviations",
                                "uri": "https://aow.eu.airbus.corp/wiki/Abbreviations",
                                "aliases": ["Acronyms"],
                                "categories": ["Abbreviations"],
                                "updated_at": "2026-11-10T12:34:27Z",
                                "html_file": "page-3687.html",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            run_parse_pipeline.main(
                [
                    "--skip-crawl",
                    "--parsed-dir",
                    str(parsed_dir),
                    "--limit",
                    "1",
                    "--run-vertex-import",
                    "--dry-run",
                ]
            )
            self.assertTrue((parsed_dir / "chunks_bq.jsonl").is_file())
            self.assertGreater((parsed_dir / "chunks_bq.jsonl").stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
