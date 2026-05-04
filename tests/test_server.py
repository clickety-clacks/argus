from __future__ import annotations

import json
import shutil
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

import argus.server as server_module
from argus.server import ArgusServer, FakeClock, load_runtime_config

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "argus"
FEEDS = FIXTURE_ROOT / "feeds"
NOW = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)


def write_config(root: Path, *, mode: str = "interval", interval: str = "1h", publish: dict | None = None, embedding: dict | bool | None = None) -> Path:
    fixture_dir = root / "feeds"
    fixture_dir.mkdir(exist_ok=True)
    shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful-source.xml")
    config = {
        "runtime": {
            "database_path": str(root / "argus.sqlite3"),
            "output_dir": str(root / "out"),
            "fixture_dir": str(fixture_dir),
        },
        "schedule": {
            "mode": mode,
            "interval": interval,
            "jitter_seconds": 0,
            "run_on_startup_if_due": True,
            "missed_tick_policy": "coalesce_one",
        },
        "publish": publish or {"state": "inactive"},
        "embedding": (
            {
                "backend": "cli",
                "command": str(write_fake_embedder(root)),
                "provider": "fake",
                "model": "argus-local-v0",
                "dimensions": 1,
                "space_id": "argus-local-v0",
            }
            if embedding is None
            else ({} if embedding is False else embedding)
        ),
        "sources": [
            {
                "id": "stateful-source",
                "display_name": "Stateful Source",
                "source_class": "editorial",
                "feed_url": "https://fixture.invalid/stateful-source.xml",
                "site_url": "https://stateful.example/",
                "adapter": "rss",
                "enabled": True,
            }
        ],
    }
    path = root / "argus.yaml"
    path.write_text(yaml.safe_dump(config))
    return path


def rows(db_path: Path, table: str):
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
    finally:
        connection.close()


def write_fake_embedder(root: Path) -> Path:
    script = root / "fake_embedder.py"
    script.write_text(
        """#!/usr/bin/env python3
import json
import sys

request = json.loads(sys.stdin.read())
vector_seed = sum(ord(ch) for ch in request["request_id"])
print(json.dumps({
    "request_id": request["request_id"],
    "space_id": request["space_id"],
    "provider": request["provider"],
    "model": request["model"],
    "dimensions": request["dimensions"],
    "vector": [float(vector_seed % 17)],
    "backend_request_id": "fake-" + request["request_id"][-8:],
}))
"""
    )
    script.chmod(0o755)
    return script


def write_failing_embedder(root: Path) -> Path:
    script = root / "failing_embedder.py"
    script.write_text(
        """#!/usr/bin/env python3
import sys

sys.stderr.write("embedding failed\\n")
sys.exit(2)
"""
    )
    script.chmod(0o755)
    return script


def write_json_failing_embedder(root: Path) -> Path:
    script = root / "json_failing_embedder.py"
    script.write_text(
        """#!/usr/bin/env python3
import json
import sys

print(json.dumps({"error_class": "embed_invalid_response", "message": "bad vector", "retry_eligible": True}))
sys.exit(2)
"""
    )
    script.chmod(0o755)
    return script


class ServerTests(unittest.TestCase):
    def test_default_scheduler_is_interval_one_hour_inactive(self):
        with TemporaryDirectory() as tmpdir:
            path = write_config(Path(tmpdir), mode="interval", publish={"state": "inactive"})
            config = load_runtime_config(path)
            self.assertEqual(config.scheduler.mode, "interval")
            self.assertEqual(config.scheduler.interval_seconds, 3600)
            self.assertEqual(config.publish.state, "inactive")

    def test_empty_db_inactive_startup_runs_without_prime_or_publish(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                result = server.tick()
            finally:
                server.close()
            self.assertIsNotNone(result)
            self.assertEqual(len(rows(root / "argus.sqlite3", "prime_events")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)
            self.assertEqual(len(list((root / "out" / "runs").glob("20260429T120000Z-scheduled-*/publish-candidates.jsonl"))), 1)

    def test_manual_mode_does_not_run_until_manual_cycle(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                self.assertIsNone(server.tick())
                clock.advance(7200)
                self.assertIsNone(server.tick())
                exit_code, summary = server.manual_cycle()
            finally:
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["run_kind"], "manual")
            self.assertEqual(len(rows(root / "argus.sqlite3", "runs")), 1)
            self.assertEqual(len(rows(root / "argus.sqlite3", "prime_events")), 0)

    def test_manual_to_interval_reload_waits_one_interval_without_backfill(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                config = yaml.safe_load(path.read_text())
                config["schedule"]["mode"] = "interval"
                config["schedule"]["interval"] = "1h"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                state = dict(server.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone())
                self.assertEqual(state["next_due_at"], "2026-04-29T13:00:00Z")
                self.assertIsNone(server.tick())
                clock.advance(3600)
                self.assertIsNotNone(server.tick())
            finally:
                server.close()

    def test_prime_creates_baseline_only_no_packages_or_attempts(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                exit_code, summary = server.prime()
            finally:
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertTrue(summary["prime"])
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-prime-*"))
            self.assertTrue((run_dir / "prime-baseline.json").exists())
            self.assertFalse((run_dir / "publish-candidates.jsonl").exists())
            self.assertFalse((run_dir / "package-candidates.jsonl").exists())
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_blocked_active_reload_and_approved_active_forward_only(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture_dir = root / "feeds"
            fixture_dir.mkdir(exist_ok=True)
            (fixture_dir / "stateful-source.xml").write_text((FEEDS / "stateful_source.xml").read_text())
            config = yaml.safe_load(path.read_text())
            config["runtime"]["fixture_dir"] = str(fixture_dir)
            path.write_text(yaml.safe_dump(config))

            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

                config["publish"] = {"state": "active", "live_approval": False, "subspace_endpoint": "https://subspace.invalid"}
                path.write_text(yaml.safe_dump(config))
                server.reload()
                snapshot = json.loads(rows(root / "argus.sqlite3", "runtime_config_snapshots")[-1]["snapshot_json"])
                self.assertEqual(snapshot["effective_mode"], "blocked")

                config["publish"] = {
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                }
                path.write_text(yaml.safe_dump(config))
                server.reload()
                clock.advance(3600)
                (fixture_dir / "stateful-source.xml").write_text(
                    (FEEDS / "stateful_source.xml").read_text().replace(
                        "  </channel>\n</rss>",
                        """    <item>\n      <guid>stateful-guid-3</guid>\n      <title>Third story</title>\n      <link>https://stateful.example/story-3</link>\n      <pubDate>Wed, 30 Apr 2026 10:00:00 +0000</pubDate>\n      <description>Brand new story.</description>\n    </item>\n  </channel>\n</rss>""",
                    )
                )
                server.tick()
            finally:
                server.close()
            packages = rows(root / "argus.sqlite3", "packages")
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual(sum(row["active_eligible"] for row in packages), 1)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "pending")

    def test_invalid_reload_fails_closed_from_active(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding=False,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                path.write_text("publish: [")
                server.reload()
                self.assertNotEqual(server.status()["publish"]["effective_mode"], "active")
            finally:
                server.close()

    def test_no_overlap_decision_records_skip(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.connection.execute("UPDATE scheduler_state SET running_run_id = ?", ("in-flight",))
                server.connection.commit()
                self.assertIsNone(server.tick())
            finally:
                server.close()
            events = rows(root / "argus.sqlite3", "scheduler_events")
            self.assertIn("cycle_skipped_already_running", [row["event_type"] for row in events])

    def test_manual_and_prime_obey_no_overlap(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.connection.execute("UPDATE scheduler_state SET running_run_id = ?", ("in-flight",))
                server.connection.commit()
                _, manual_summary = server.manual_cycle()
                _, prime_summary = server.prime()
            finally:
                server.close()
            self.assertEqual(manual_summary["skip_reason"], "cycle_already_running")
            self.assertEqual(prime_summary["skip_reason"], "cycle_already_running")
            self.assertEqual(len(rows(root / "argus.sqlite3", "runs")), 0)

    def test_restart_recovers_stale_running_marker(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            first = ArgusServer(path, clock=FakeClock(NOW))
            try:
                first.connection.execute("UPDATE scheduler_state SET running_run_id = ?", ("stale-run",))
                first.connection.commit()
            finally:
                first.close()

            second = ArgusServer(path, clock=FakeClock(NOW))
            try:
                state = dict(second.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone())
            finally:
                second.close()
            self.assertIsNone(state["running_run_id"])
            events = rows(root / "argus.sqlite3", "scheduler_events")
            self.assertIn("cycle_recovered_after_restart", [row["event_type"] for row in events])

    def test_same_second_manual_runs_get_distinct_run_ids(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                first = server.manual_cycle()[1]["run_id"]
                second = server.manual_cycle()[1]["run_id"]
            finally:
                server.close()
            self.assertNotEqual(first, second)

    def test_require_embeddings_does_not_queue_unembedded_publish_attempts(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding=False,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            with self.assertRaisesRegex(Exception, "missing_embedding_config"):
                ArgusServer(path, clock=FakeClock(NOW))

    def test_configured_embedding_backend_does_not_imply_candidate_embeddings(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            config = yaml.safe_load(path.read_text())
            config["embedding"] = {
                "backend": "cli",
                "command": "/definitely/not/invoked",
                "provider": "fake",
                "model": "text-embedding-3-small",
                "dimensions": 1536,
                "space_id": "openai:text-embedding-3-small:1536:v1",
            }
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_cli_embedding_requires_provider(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            config = yaml.safe_load(path.read_text())
            del config["embedding"]["provider"]
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "missing_embedding_config"):
                ArgusServer(path, clock=FakeClock(NOW))

    def test_embedding_failure_records_failed_embedding_and_artifact(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "cli",
                    "command": str(write_failing_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"failed"})
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-scheduled-*"))
            failures = json.loads((run_dir / "embedding-failures.json").read_text())
            self.assertEqual(len(failures), 2)
            self.assertEqual({failure["class"] for failure in failures}, {"embed_backend_unavailable"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)

    def test_embedding_failure_retries_on_later_cycle(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "cli",
                    "command": str(write_failing_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                config = yaml.safe_load(path.read_text())
                config["embedding"]["command"] = str(write_fake_embedder(root))
                path.write_text(yaml.safe_dump(config))
                server.reload()
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"embedded"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_embedding_failure_honors_backend_failure_json(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "cli",
                    "command": str(write_json_failing_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            failure = json.loads(rows(root / "argus.sqlite3", "embeddings")[0]["failure_json"])
            self.assertEqual(failure["class"], "embed_invalid_response")
            self.assertEqual(failure["message"], "bad vector")

    def test_active_no_backfill_even_if_historical_report_reprocessed(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                inactive_package_ids = {row["package_id"] for row in rows(root / "argus.sqlite3", "packages")}
                config = yaml.safe_load(path.read_text())
                config["publish"] = {
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                }
                path.write_text(yaml.safe_dump(config))
                server.reload()
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            packages = rows(root / "argus.sqlite3", "packages")
            self.assertEqual({row["package_id"] for row in packages}, inactive_package_ids)
            self.assertEqual(sum(row["active_eligible"] for row in packages), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_invalid_scheduler_reload_keeps_publish_snapshot_when_publish_inputs_unchanged(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                config = yaml.safe_load(path.read_text())
                for invalid_interval in ("1m", "bogus"):
                    config["schedule"]["interval"] = invalid_interval
                    path.write_text(yaml.safe_dump(config))
                    server.reload()
                    self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                config["schedule"]["interval"] = "1h"
                config["schedule"]["jitter_seconds"] = "bogus"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
            finally:
                server.close()
            self.assertIn("scheduler_reload_failed", [row["event_type"] for row in rows(root / "argus.sqlite3", "scheduler_events")])

    def test_invalid_scheduler_reload_with_publish_change_fails_inactive(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                config = yaml.safe_load(path.read_text())
                config["publish"] = {"state": "inactive"}
                config["schedule"]["interval"] = "1m"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                self.assertEqual(server.status()["publish"]["effective_mode"], "inactive")
            finally:
                server.close()

    def test_server_config_rejects_invalid_source_id(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            config = yaml.safe_load(path.read_text())
            config["sources"][0]["id"] = "stateful_source"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "Invalid source id"):
                load_runtime_config(path)

    def test_manual_mode_ignores_invalid_interval_value(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual", interval="bogus")
            config = load_runtime_config(path)
            self.assertEqual(config.scheduler.mode, "manual")

    def test_inactive_run_writes_review_artifacts_and_sqlite_state(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-scheduled-*"))
            for artifact in [
                "digest.md",
                "digest.json",
                "normalized-items.jsonl",
                "dedupe-decisions.json",
                "skipped-items.json",
                "package-candidates.jsonl",
            ]:
                self.assertTrue((run_dir / artifact).exists(), artifact)
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "report_seen_runs")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_decisions")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "embeddings")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)
            self.assertEqual(len(json.loads((run_dir / "dedupe-decisions.json").read_text())), 2)
            self.assertEqual(json.loads((run_dir / "skipped-items.json").read_text()), [])

    def test_server_ignores_legacy_json_seen_state_for_sqlite_authority(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            legacy_state = root / "out" / "runs" / "state.json"
            legacy_state.parent.mkdir(parents=True)
            legacy_state.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "sources": {
                            "stateful-source": {
                                "validators": {},
                                "seen_identities": [
                                    "feed_entry_id|stateful-guid-1",
                                    "feed_entry_id|stateful-guid-2",
                                ],
                            }
                        },
                    }
                )
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_post_pipeline_artifact_failure_records_failed_run(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_after_pipeline = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_after_pipeline["enabled"] and self.name == "run-summary.json":
                    raise OSError("simulated artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_after_pipeline["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated artifact failure"):
                    server.tick()
            finally:
                Path.write_text = original_write_text
                server.close()
            run_rows = rows(root / "argus.sqlite3", "runs")
            self.assertEqual(len(run_rows), 1)
            self.assertEqual(run_rows[0]["status"], "failed")
            self.assertIn("post_pipeline_error", json.loads(run_rows[0]["summary_json"]))

    def test_prime_artifact_failure_marks_prime_event_failed(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_prime_artifact = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_prime_artifact["enabled"] and self.name == "prime-baseline.md":
                    raise OSError("simulated prime artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_prime_artifact["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated prime artifact failure"):
                    server.prime()
            finally:
                Path.write_text = original_write_text
                server.close()
            event = rows(root / "argus.sqlite3", "prime_events")[0]
            self.assertEqual(event["status"], "failed")
            self.assertIsNotNone(event["completed_at"])
            self.assertIsNotNone(event["run_id"])
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "failed")

    def test_prime_persists_baseline_dedupe_state_without_packages(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.prime()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_stale_items_are_not_inserted_as_accepted_reports(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            config = yaml.safe_load(path.read_text())
            config["sources"][0]["freshness_window_hours"] = 1
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertGreaterEqual(len(rows(root / "argus.sqlite3", "skipped_items")), 1)

    def test_same_canonical_url_different_feed_ids_are_distinct_candidates(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture = root / "feeds" / "stateful-source.xml"
            fixture.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <guid>guid-a</guid>
      <title>First item</title>
      <link>https://stateful.example/shared</link>
      <pubDate>Wed, 29 Apr 2026 10:00:00 +0000</pubDate>
      <description>First item.</description>
    </item>
    <item>
      <guid>guid-b</guid>
      <title>Second item</title>
      <link>https://stateful.example/shared</link>
      <pubDate>Wed, 29 Apr 2026 10:05:00 +0000</pubDate>
      <description>Second item.</description>
    </item>
  </channel>
</rss>"""
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_feed_identity_rerun_updates_seen_report_not_duplicate_report(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture = root / "feeds" / "stateful-source.xml"
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                fixture.write_text(
                    fixture.read_text().replace("<title>First story</title>", "<title>First story revised</title>")
                )
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "report_seen_runs")), 4)
            self.assertIn("seen_existing_report", {row["decision"] for row in rows(root / "argus.sqlite3", "dedupe_decisions")})

    def test_package_json_uses_canonical_shape_and_embeddings(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            config = yaml.safe_load(path.read_text())
            embedder = write_fake_embedder(root)
            config["embedding"] = {
                "backend": "cli",
                "command": str(embedder),
                "provider": "fake",
                "model": "argus-local-v0",
                "dimensions": 1,
                "space_id": "argus-local-v0",
            }
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            package = json.loads(rows(root / "argus.sqlite3", "packages")[0]["package_json"])
            self.assertEqual(package["schema"], "swarm.channel.news.report.v0")
            self.assertTrue(package["package_id"].startswith("sha256:"))
            self.assertIsInstance(package["supplied_embeddings"], list)
            self.assertEqual(package["supplied_embeddings"][0]["space_id"], "argus-local-v0")
            self.assertNotIn("suppliedEmbeddings", package)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 2)

    def test_spec_named_cli_backend_invokes_embedding_command(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "subspace-embedding-cli",
                    "command": str(write_fake_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"embedded"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_source_health_command_reads_sqlite_state(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            health = server_module.run_source_health(root / "argus.sqlite3")
            self.assertEqual(health[0]["source_id"], "stateful-source")
            self.assertEqual(health[0]["last_run_id"], rows(root / "argus.sqlite3", "runs")[0]["run_id"])
            self.assertEqual(len(rows(root / "argus.sqlite3", "source_run_status")), 1)

    def test_run_status_records_source_errors(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            config = yaml.safe_load(path.read_text())
            config["sources"].append(
                {
                    "id": "missing-source",
                    "display_name": "Missing Source",
                    "source_class": "editorial",
                    "feed_url": "https://fixture.invalid/missing-source.xml",
                    "site_url": "https://missing.example/",
                    "adapter": "rss",
                    "enabled": True,
                }
            )
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                exit_code, _ = server.tick()
            finally:
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "succeeded_with_source_errors")
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "source_run_status")}, {"ok", "failed"})

    def test_explain_skip_includes_exact_duplicate_details_from_sqlite(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            (root / "feeds" / "stateful-source.xml").write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <title>Canonical duplicate story</title>
      <link>https://stateful.example/story?utm_source=rss</link>
      <pubDate>Tue, 29 Apr 2026 07:00:00 +0000</pubDate>
      <description>First copy.</description>
    </item>
    <item>
      <title>Canonical duplicate story</title>
      <link>https://stateful.example/story?utm_medium=email</link>
      <pubDate>Tue, 29 Apr 2026 07:00:00 +0000</pubDate>
      <description>Second copy.</description>
    </item>
  </channel>
</rss>
"""
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                _, summary = server.tick()
            finally:
                server.close()
            explanation = server_module.explain_skip(root / "argus.sqlite3", summary["run_id"])
            self.assertEqual(len(explanation["exact_duplicates"]), 1)
            duplicate = explanation["exact_duplicates"][0]
            self.assertEqual(duplicate["selected_key_type"], "canonical_url")
            self.assertEqual(duplicate["candidate"]["canonical_url"], "https://stateful.example/story")
            self.assertIsNotNone(duplicate["duplicate_of_report_id"])
            self.assertIn("cross-source", duplicate["cross_source_note"])

    def test_missing_identity_key_is_skipped_before_storage_and_package(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            (root / "feeds" / "stateful-source.xml").write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <description>No identity fields.</description>
    </item>
  </channel>
</rss>
"""
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            skipped = rows(root / "argus.sqlite3", "skipped_items")
            self.assertEqual(skipped[0]["reason_code"], "missing_identity_key")
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-scheduled-*"))
            artifact = json.loads((run_dir / "skipped-items.json").read_text())
            self.assertEqual(artifact[0]["reason_code"], "missing_identity_key")

    def test_activation_reload_during_cycle_does_not_backfill_cycle_started_inactive(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            config = yaml.safe_load(path.read_text())
            original_run_pipeline = server_module.run_pipeline_for_sources
            server = ArgusServer(path, clock=FakeClock(NOW))

            def activate_during_fetch(*args, **kwargs):
                result = original_run_pipeline(*args, **kwargs)
                config["publish"] = {
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                }
                path.write_text(yaml.safe_dump(config))
                server.reload_requested = True
                return result

            server_module.run_pipeline_for_sources = activate_during_fetch
            try:
                server.tick()
            finally:
                server_module.run_pipeline_for_sources = original_run_pipeline
                server.close()
            self.assertEqual(rows(root / "argus.sqlite3", "packages")[0]["active_eligible"], 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)


if __name__ == "__main__":
    unittest.main()
