from __future__ import annotations

import json
import shutil
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from argus.server import ArgusServer, FakeClock, load_runtime_config

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "argus"
FEEDS = FIXTURE_ROOT / "feeds"
NOW = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)


def write_config(root: Path, *, mode: str = "interval", interval: str = "1h", publish: dict | None = None) -> Path:
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
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
                self.assertEqual(server.status()["publish"]["effective_mode"], "blocked")
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

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

    def test_active_no_backfill_even_if_historical_report_reprocessed(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                inactive_package_ids = {row["package_id"] for row in rows(root / "argus.sqlite3", "packages")}
                (root / "out" / "state.json").unlink()
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

    def test_invalid_scheduler_reload_keeps_publish_snapshot(self):
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


if __name__ == "__main__":
    unittest.main()
