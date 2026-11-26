"""Unit and integration invariant tests for the MediaWiki -> BigQuery -> Vertex AI Search pipeline."""

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

import run_pipeline  # noqa: E402
from wiki_utils import (  # noqa: E402
    DEFAULT_BQ_PROJECT,
    DEFAULT_BQ_SOURCE_DATASET,
    DEFAULT_BQ_VIEW_DATASET,
    DEFAULT_GCS_BUCKET,
    DEFAULT_SOURCE_PROJECT,
    DEFAULT_VERTEX_PROJECT,
    DEFAULT_VERTEX_PROJECT_NUMBER,
    backfill_missing_revisions,
    build_page_title_index,
    create_mediawiki_session,
    is_generator_advancing,
    load_all_raw_pages,
    load_all_template_records,
    load_template_registry,
    make_rfc1034_id,
    maybe_login_mediawiki,
    mw_api_request,
    resolve_page_by_title,
    safe_child_path,
)

_step3 = importlib.import_module("3_chunk_pages")
_step5 = importlib.import_module("5_vertex_import_jsonl")


def _mock_resp(status_code: int = 200, json_data=None, text: str = "", headers=None):
    r = MagicMock()
    r.status_code = status_code
    r.headers = headers or {"Content-Type": "application/json; charset=utf-8"}
    r.text = text
    r.raise_for_status = MagicMock()
    if status_code >= 400:
        import requests

        r.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")
    if json_data is not None:
        r.json = MagicMock(return_value=json_data)
    else:
        r.json = MagicMock(side_effect=ValueError("No JSON object could be decoded"))
    return r


class MediaWikiPipelineTests(unittest.TestCase):
    def test_maxlag_backoff_and_sso_diagnostics(self) -> None:
        sess = MagicMock()
        sess.trust_env = True
        sess.get.side_effect = [
            _mock_resp(
                200,
                json_data={"error": {"code": "maxlag", "info": "7s lagged"}},
                headers={"Content-Type": "application/json", "Retry-After": "1"},
            ),
            _mock_resp(200, json_data={"batchcomplete": "", "query": {"pages": {}}}),
        ]
        with patch("time.sleep") as mock_sleep:
            res = mw_api_request(sess, "https://aow.eu.airbus.corp/api.php", params={"action": "query"})
            self.assertIn("query", res)
            mock_sleep.assert_called_once_with(1.0)

        sess_sso = MagicMock()
        sess_sso.trust_env = True
        sess_sso.get.return_value = _mock_resp(
            200,
            json_data=None,
            text="<html><body>Airbus SSO</body></html>",
            headers={"Content-Type": "text/html"},
        )
        with self.assertRaisesRegex(RuntimeError, "SSO"):
            mw_api_request(sess_sso, "https://aow.eu.airbus.corp/api.php", params={"action": "query"})

    def test_idempotent_login_and_cookie_jar(self) -> None:
        sess = MagicMock()
        sess._mw_logged_in = False
        sess.trust_env = True
        sess.get.return_value = _mock_resp(200, json_data={"query": {"tokens": {"logintoken": "tok+\\"}}})
        sess.post.return_value = _mock_resp(200, json_data={"login": {"result": "Success"}})
        with patch.dict("os.environ", {"MEDIAWIKI_BOT_USER": "bot@sync", "MEDIAWIKI_BOT_PASSWORD": "secret"}):
            maybe_login_mediawiki(sess, "https://aow.eu.airbus.corp/api.php")
            maybe_login_mediawiki(sess, "https://aow.eu.airbus.corp/api.php")
        self.assertEqual(sess.post.call_count, 1)

        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as tf:
            tf.write("# Netscape HTTP Cookie File\n")
            tf.write("aow.eu.airbus.corp\tFALSE\t/\tTRUE\t0\taow_session\tabc123xyz\n")
            cookie_path = tf.name
        try:
            cookie_sess = create_mediawiki_session(bypass_proxy=True, cookie_file=cookie_path)
            self.assertEqual(cookie_sess.cookies.get("aow_session"), "abc123xyz")
            self.assertFalse(cookie_sess.trust_env)
        finally:
            Path(cookie_path).unlink(missing_ok=True)

    def test_revision_backfill_and_continuation_state(self) -> None:
        self.assertFalse(is_generator_advancing({"clcontinue": "12|Foo", "continue": "||revisions|info"}))
        self.assertTrue(is_generator_advancing({"gapcontinue": "NextPage", "continue": "gapcontinue||"}))

        sess_bf = MagicMock()
        sess_bf.trust_env = True
        sess_bf.get.return_value = _mock_resp(
            200,
            json_data={
                "query": {
                    "pages": {
                        "42": {
                            "pageid": 42,
                            "ns": 100,
                            "title": "LGSIM:Huge",
                            "revisions": [{"slots": {"main": {"contentmodel": "wikitext", "*": "Body!"}}}],
                        }
                    }
                }
            },
        )
        batch = {"42": {"pageid": 42, "ns": 100, "title": "LGSIM:Huge"}}
        self.assertEqual(backfill_missing_revisions(sess_bf, "https://aow.eu.airbus.corp/api.php", batch), 1)
        self.assertEqual(batch["42"]["revisions"][0]["slots"]["main"]["*"], "Body!")

    def test_chunk_classifier_whole_word_boundaries(self) -> None:
        # Substrings like "dataset" (`set`) or "runway" (`run`) must NOT falsely trigger "operational"
        self.assertEqual(_step3.classify_chunk("The dataset describes the runway overview."), "conceptual")
        self.assertEqual(_step3.classify_chunk("Part number 31073 supplied for A320 aircraft."), "reference")
        self.assertEqual(_step3.classify_chunk("Run SetupToolbox.m to compile the code."), "operational")

    def test_security_validators_and_fence_preservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p_trav = safe_child_path(Path(tmp), "../../evil.json")
            self.assertTrue(p_trav.is_relative_to(Path(tmp).resolve()))
            p_win = safe_child_path(Path(tmp), "CON.json")
            self.assertEqual(p_win.name, "mw_CON.json")

        self.assertEqual(make_rfc1034_id("chunk_10000_001"), "chunk-10000-001")
        with self.assertRaises(ValueError):
            _step5.validate_bq_identifier("bad-table; DROP TABLE x")

        md_with_code = "## Intro\n\n```python\n## comment inside code\nx = 1\n\ny = 2\n```\n"
        secs = _step3.split_hierarchical_sections(md_with_code)
        self.assertEqual(len(secs), 1)
        wins = _step3.split_section_into_windows(secs[0][1], max_tokens=200)
        self.assertEqual(len(wins), 1)
        self.assertEqual(wins[0].count("```"), 2)

    def test_orchestrator_recentchanges_and_empty_gcs_prefix(self) -> None:
        self.assertEqual(DEFAULT_SOURCE_PROJECT, "prj-c9ec-ing-ds-dev-ec")
        self.assertEqual(DEFAULT_BQ_PROJECT, "prj-c9ec-data-dom-dev-ec")
        self.assertEqual(DEFAULT_GCS_BUCKET, "mediawiki-data-nov-25")
        self.assertEqual(DEFAULT_BQ_SOURCE_DATASET, "aow_nov_26")
        self.assertEqual(DEFAULT_BQ_VIEW_DATASET, "wiki_vertex_authorized")
        self.assertEqual(DEFAULT_VERTEX_PROJECT, "prj-7efd-everydai-dev-4f9a")
        self.assertEqual(DEFAULT_VERTEX_PROJECT_NUMBER, "7016082038")
        parsed = run_pipeline.build_parser().parse_args(["--skip-crawl", "--dry-run"])
        self.assertEqual(parsed.source_project, "prj-c9ec-ing-ds-dev-ec")
        self.assertEqual(parsed.bq_project, "prj-c9ec-data-dom-dev-ec")
        self.assertEqual(parsed.vertex_project, "prj-7efd-everydai-dev-4f9a")
        view_sql = _step5.build_authorized_view_sql(
            parsed.bq_project, parsed.bq_source_dataset, "wiki_option1_raw_templates"
        )
        self.assertIn("`prj-c9ec-data-dom-dev-ec.aow_nov_26.wiki_option1_raw_templates`", view_sql)

        specs = _step5.build_option_specs("", "raw-ds", "chunk-ds")
        self.assertEqual(specs[0].gcs_bq_blob, "option1_raw/raw_with_templates_bq.jsonl")
        self.assertEqual(specs[1].gcs_bq_blob, "option2_chunked/chunks_bq.jsonl")

        with (
            patch("run_pipeline.create_mediawiki_session"),
            patch("run_pipeline.maybe_login_mediawiki"),
            patch("run_pipeline.mw_api_request") as mreq,
        ):
            mreq.side_effect = [
                {"query": {"recentchanges": []}},
                {"query": {"logevents": []}},
                {"query": {"logevents": []}},
            ]
            changed, _ = run_pipeline.has_mediawiki_changes_since(
                "https://aow.eu.airbus.corp/api.php", "2026-11-24T00:00:00Z"
            )
            self.assertFalse(changed)

    def test_local_generated_datasets_if_present(self) -> None:
        opt1_path = REPO / "wikimedia-data/jsonl-for-vertex/rawapi/raw_with_templates_bq.jsonl"
        opt2_path = REPO / "wikimedia-data/jsonl-for-vertex/chunks/chunks_bq.jsonl"
        if not opt1_path.exists() or not opt2_path.exists():
            self.skipTest("Local wikimedia-data/ not present (expected in clean CI / Cloud Build).")

        all_tpls = load_all_template_records(REPO / "wikimedia-data/templates")
        tpls = load_template_registry(records=all_tpls)
        pages = load_all_raw_pages(REPO / "wikimedia-data/raw_api", template_registry=tpls)
        idx = build_page_title_index(pages)

        dqv = resolve_page_by_title("DAT", idx, follow_redirects=True)
        self.assertIsNotNone(dqv)
        self.assertEqual(dqv.title, "Data Quick View")
        self.assertIn("DAT", dqv.aliases)

        rfc_re = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
        opt1_rows = [json.loads(ln) for ln in opt1_path.read_text(encoding="utf-8").splitlines()]
        opt2_rows = [json.loads(ln) for ln in opt2_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(set(opt1_rows[0].keys()), set(opt2_rows[0].keys()))
        self.assertTrue(all(rfc_re.match(r["id"]) and r["content"].strip() for r in opt1_rows))
        self.assertTrue(all(rfc_re.match(r["id"]) and r["content"].strip() for r in opt2_rows))


if __name__ == "__main__":
    unittest.main()
