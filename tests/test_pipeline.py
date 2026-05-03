from __future__ import annotations

import json
import shutil
import unittest
from datetime import datetime, timezone
from pathlib import Path

from argus.pipeline import default_state_path, read_source_config, run_pipeline

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "argus"
FEEDS = FIXTURE_ROOT / "feeds"
NOW = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)


def read_json(path: Path):
    return json.loads(path.read_text())


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class PipelineTests(unittest.TestCase):
    def test_source_config_validation_and_pipeline(self):
        sources = read_source_config(FIXTURE_ROOT / "test-sources.yaml")
        self.assertEqual(len(sources), 11)
        with self.subTest("pipeline"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmpdir:
                exit_code, summary = run_pipeline(
                    FIXTURE_ROOT / "test-sources.yaml",
                    Path(tmpdir),
                    now=NOW,
                    fixture_dir=FEEDS,
                )
                self.assertEqual(exit_code, 0)
                self.assertEqual(summary["exit_status"], "partial_failure")
                self.assertEqual(summary["counts"]["failed_sources"], 1)
                self.assertEqual(summary["counts"]["source_local_duplicates"], 2)
                for name in [
                    "run-summary.json",
                    "source-health.json",
                    "normalized.jsonl",
                    "clusters.jsonl",
                    "publish-candidates.jsonl",
                ]:
                    self.assertTrue((Path(tmpdir) / name).exists(), name)
                self.assertEqual(summary["state"]["path"], str(default_state_path(Path(tmpdir))))
                self.assertTrue(summary["state"]["write_performed"])

    def test_artifacts_preserve_provenance_and_community_labeling(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            run_pipeline(FIXTURE_ROOT / "test-sources.yaml", Path(tmpdir), now=NOW, fixture_dir=FEEDS)
            health = read_json(Path(tmpdir) / "source-health.json")
            normalized = read_jsonl(Path(tmpdir) / "normalized.jsonl")
            candidates = read_jsonl(Path(tmpdir) / "publish-candidates.jsonl")
            health_by_id = {row["source_id"]: row for row in health}
            normalized_by_id = {row["report_id"]: row for row in normalized}

            self.assertEqual(len(health), 11)
            self.assertEqual(health_by_id["malformed_source"]["status"], "failed")
            self.assertEqual(health_by_id["empty_source"]["status"], "empty")

            reddit = next(row for row in normalized if row["source_id"] == "reddit_localllama")
            self.assertEqual(reddit["source_class"], "community")
            self.assertIn("Community thread", reddit["raw_summary"])

            for row in candidates:
                self.assertIn(row["report_id"], normalized_by_id)
                self.assertEqual(
                    row["provenance"]["source_class"],
                    normalized_by_id[row["report_id"]]["source_class"],
                )
                self.assertTrue(row["metadata"]["embedding_text"])

    def test_source_local_dedupe_and_cross_source_preservation(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            run_pipeline(FIXTURE_ROOT / "test-sources.yaml", Path(tmpdir), now=NOW, fixture_dir=FEEDS)
            clusters = read_jsonl(Path(tmpdir) / "clusters.jsonl")
            candidates = read_jsonl(Path(tmpdir) / "publish-candidates.jsonl")

            dup_cluster = next(row for row in clusters if row["source_id"] == "dup_source")
            self.assertEqual(dup_cluster["dedupe_reasons"], ["canonical_url_exact"])
            self.assertEqual(len(dup_cluster["report_ids"]), 2)

            title_cluster = next(row for row in clusters if row["source_id"] == "dup_title_source")
            self.assertEqual(title_cluster["dedupe_reasons"], ["normalized_title_date_bucket"])
            self.assertEqual(len(title_cluster["report_ids"]), 2)

            shared_candidates = [row for row in candidates if row["canonical_url"] == "https://shared.example/story"]
            self.assertEqual(len(shared_candidates), 2)
            self.assertEqual({row["source_id"] for row in shared_candidates}, {"cross_source_one", "cross_source_two"})

    def test_all_source_failure_exits_nonzero(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            exit_code, summary = run_pipeline(
                FIXTURE_ROOT / "all-fail-sources.yaml",
                Path(tmpdir),
                now=NOW,
                fixture_dir=FEEDS,
            )
            self.assertEqual(exit_code, 1)
            self.assertEqual(summary["exit_status"], "failed")

    def test_state_filters_repeated_items_and_emits_only_new_additions(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            state_path = Path(tmpdir) / "argus-state.json"
            out1 = Path(tmpdir) / "run1"
            out2 = Path(tmpdir) / "run2"
            out3 = Path(tmpdir) / "run3"

            _, summary1 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", out1, now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary1["counts"]["publish_candidates"], 2)
            self.assertTrue(state_path.exists())

            _, summary2 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", out2, now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary2["counts"]["publish_candidates"], 0)
            health2 = read_json(out2 / "source-health.json")
            self.assertEqual(health2[0]["skipped_previously_seen_count"], 2)

            xml_path = fixture_dir / "stateful_source.xml"
            xml_text = xml_path.read_text()
            xml_path.write_text(
                xml_text.replace(
                    "</channel>\n</rss>",
                    """    <item>\n      <guid>stateful-guid-3</guid>\n      <title>Third story</title>\n      <link>https://stateful.example/story-3?utm_source=rss</link>\n      <pubDate>Wed, 30 Apr 2026 10:00:00 +0000</pubDate>\n      <description>Brand new story.</description>\n    </item>\n  </channel>\n</rss>""",
                )
            )
            _, summary3 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", out3, now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary3["counts"]["publish_candidates"], 1)
            candidates3 = read_jsonl(out3 / "publish-candidates.jsonl")
            self.assertEqual([row["title"] for row in candidates3], ["Third story"])

    def test_state_supports_http_304_without_re_emitting(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            meta_path = fixture_dir / "stateful_source.meta.json"
            meta_path.write_text(json.dumps({"etag": "\"stateful-etag-v1\"", "last_modified": "Wed, 30 Apr 2026 09:00:00 GMT"}))
            state_path = Path(tmpdir) / "argus-state.json"

            run_pipeline(Path(tmpdir) / "stateful-sources.yaml", Path(tmpdir) / "run1", now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            meta_path.write_text(
                json.dumps(
                    {
                        "status_code": 304,
                        "etag": "\"stateful-etag-v1\"",
                        "last_modified": "Wed, 30 Apr 2026 09:00:00 GMT",
                        "if_none_match": "\"stateful-etag-v1\"",
                    }
                )
            )
            _, summary = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", Path(tmpdir) / "run2", now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary["counts"]["publish_candidates"], 0)
            health = read_json(Path(tmpdir) / "run2" / "source-health.json")
            self.assertEqual(health[0]["status"], "not_modified")
            self.assertEqual(health[0]["http_status"], 304)

    def test_dry_run_reads_state_but_does_not_write_it(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            state_path = Path(tmpdir) / "argus-state.json"

            _, summary = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "run1",
                now=NOW,
                fixture_dir=fixture_dir,
                state_path=state_path,
                dry_run=True,
                state_write=False,
            )
            self.assertFalse(state_path.exists())
            self.assertFalse(summary["state"]["write_enabled"])
            self.assertFalse(summary["state"]["write_performed"])

    def test_prime_advances_state_without_emitting_publish_candidates(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            state_path = Path(tmpdir) / "argus-state.json"

            _, summary1 = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "prime",
                now=NOW,
                fixture_dir=fixture_dir,
                state_path=state_path,
                prime=True,
            )
            self.assertTrue(state_path.exists())
            self.assertTrue(summary1["prime"])
            self.assertTrue(summary1["state"]["write_performed"])
            self.assertEqual(summary1["counts"]["publish_candidates"], 0)
            self.assertEqual(summary1["counts"]["primed_candidates"], 2)
            self.assertFalse((Path(tmpdir) / "prime" / "publish-candidates.jsonl").exists())

            _, summary2 = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "after",
                now=NOW,
                fixture_dir=fixture_dir,
                state_path=state_path,
            )
            self.assertEqual(summary2["counts"]["publish_candidates"], 0)


if __name__ == "__main__":
    unittest.main()
