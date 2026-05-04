from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import signal
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from .pipeline import PipelineError, SourceConfig, date_bucket, iso_z, normalize_feed_entry_id, normalize_title, parse_now, run_pipeline_for_sources, utc_now


MIN_INTERVAL_SECONDS = 15 * 60
MAX_INTERVAL_SECONDS = 24 * 60 * 60
DEFAULT_INTERVAL_SECONDS = 60 * 60
SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


@dataclasses.dataclass(frozen=True)
class SchedulerConfig:
    mode: str = "interval"
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS
    jitter_seconds: int = 0
    run_on_startup_if_due: bool = True
    missed_tick_policy: str = "coalesce_one"


@dataclasses.dataclass(frozen=True)
class PublishConfig:
    state: str = "inactive"
    live_approval: bool = False
    subspace_endpoint: Optional[str] = None
    require_embeddings: bool = True
    allow_non_embedded_fallback: bool = False


@dataclasses.dataclass(frozen=True)
class EmbeddingConfig:
    backend: Optional[str] = None
    command: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    dimensions: Optional[int] = None
    space_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class RuntimeConfig:
    database_path: Path
    output_dir: Path
    sources: List[SourceConfig]
    scheduler: SchedulerConfig
    publish: PublishConfig
    embedding: EmbeddingConfig
    fixture_dir: Optional[Path] = None
    source_config_path: str = "<config>"
    config_hash: str = ""


@dataclasses.dataclass(frozen=True)
class ServiceRegistration:
    pid: int
    database_path: str
    config_path: str


class Clock:
    def now(self) -> datetime:
        return utc_now()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class FakeClock(Clock):
    def __init__(self, now: datetime) -> None:
        self._now = now
        self.slept: List[float] = []

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += timedelta(seconds=seconds)

    def advance(self, seconds: int) -> None:
        self._now += timedelta(seconds=seconds)


def parse_duration_seconds(value: Any) -> int:
    try:
        if isinstance(value, int):
            return value
        text = str(value).strip()
        if text.endswith("m"):
            return int(text[:-1]) * 60
        if text.endswith("h"):
            return int(text[:-1]) * 60 * 60
        if text.endswith("s"):
            return int(text[:-1])
        return int(text)
    except (TypeError, ValueError):
        raise PipelineError("Invalid schedule.interval: {}".format(value))


def config_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def event_id(prefix: str, observed_at: datetime, detail: str) -> str:
    basis = "{}\n{}\n{}".format(prefix, iso_z(observed_at), detail)
    return "{}:{}".format(prefix, hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24])


def validate_scheduler(payload: Dict[str, Any]) -> SchedulerConfig:
    mode = str(payload.get("mode", "interval"))
    if mode not in {"interval", "manual"}:
        raise PipelineError("Invalid schedule.mode: {}".format(mode))
    interval_seconds = DEFAULT_INTERVAL_SECONDS if mode == "manual" else parse_duration_seconds(payload.get("interval", "1h"))
    try:
        jitter_seconds = int(payload.get("jitter_seconds", 0))
    except (TypeError, ValueError):
        raise PipelineError("Invalid schedule.jitter_seconds: {}".format(payload.get("jitter_seconds")))
    missed_tick_policy = str(payload.get("missed_tick_policy", "coalesce_one"))
    if missed_tick_policy != "coalesce_one":
        raise PipelineError("Invalid schedule.missed_tick_policy: {}".format(missed_tick_policy))
    if jitter_seconds < 0 or jitter_seconds > 3600:
        raise PipelineError("Invalid schedule.jitter_seconds: {}".format(jitter_seconds))
    if mode == "interval":
        if interval_seconds < MIN_INTERVAL_SECONDS or interval_seconds > MAX_INTERVAL_SECONDS:
            raise PipelineError("Invalid schedule.interval: {}".format(interval_seconds))
        if jitter_seconds >= interval_seconds / 2:
            raise PipelineError("schedule.jitter_seconds must be less than half of interval")
    return SchedulerConfig(
        mode=mode,
        interval_seconds=interval_seconds,
        jitter_seconds=jitter_seconds,
        run_on_startup_if_due=bool(payload.get("run_on_startup_if_due", True)),
        missed_tick_policy=missed_tick_policy,
    )


def source_from_runtime(row: Dict[str, Any]) -> SourceConfig:
    source_id = str(row["id"])
    if not SOURCE_ID_RE.match(source_id):
        raise PipelineError("Invalid source id: {}".format(source_id))
    feed_url = row.get("feed_url") or row.get("api_url")
    adapter = str(row.get("adapter", "rss"))
    feed_type = {"rss": "rss", "atom": "atom", "arxiv_atom": "arxiv_atom", "generic_rss": "rss"}.get(adapter, adapter)
    return SourceConfig(
        id=source_id,
        name=str(row.get("display_name") or row.get("name") or source_id),
        source_class=str(row["source_class"]),
        source_category=str(row.get("source_category") or row.get("source_class")),
        feed_type=feed_type,
        feed_url=str(feed_url),
        site_url=str(row.get("site_url") or feed_url),
        enabled=bool(row.get("enabled", True)),
        tier=int(row.get("tier", 1)),
        freshness_window_hours=(int(row["freshness_window_hours"]) if row.get("freshness_window_hours") is not None else None),
        adapter=adapter,
        request_headers={str(k): str(v) for k, v in (row.get("headers") or row.get("request_headers") or {}).items()},
        notes=str(row["notes"]) if row.get("notes") is not None else None,
    )


def load_runtime_config(path: Path) -> RuntimeConfig:
    payload = yaml.safe_load(path.read_text())
    if not isinstance(payload, dict):
        raise PipelineError("Invalid Argus config: {}".format(path))
    runtime = payload.get("runtime") or {}
    if not isinstance(runtime, dict):
        raise PipelineError("Invalid runtime config")
    database_path = runtime.get("database_path") or payload.get("database_path")
    output_dir = runtime.get("output_dir") or payload.get("output_dir")
    if not database_path or not output_dir:
        raise PipelineError("runtime.database_path and runtime.output_dir are required")
    sources_payload = payload.get("sources")
    if not isinstance(sources_payload, list):
        raise PipelineError("sources must be a list")
    seen_source_ids = set()
    sources = []
    for row in sources_payload:
        if not isinstance(row, dict):
            continue
        source = source_from_runtime(row)
        if source.id in seen_source_ids:
            raise PipelineError("Duplicate source id: {}".format(source.id))
        seen_source_ids.add(source.id)
        sources.append(source)
    if not any(source.enabled for source in sources):
        raise PipelineError("at least one enabled source is required")
    publish_payload = payload.get("publish") or {}
    embedding_payload = payload.get("embedding") or {}
    publish = PublishConfig(
        state=str(publish_payload.get("state", "inactive")),
        live_approval=bool(publish_payload.get("live_approval", False)),
        subspace_endpoint=(str(publish_payload["subspace_endpoint"]) if publish_payload.get("subspace_endpoint") else None),
        require_embeddings=bool(publish_payload.get("require_embeddings", True)),
        allow_non_embedded_fallback=bool(publish_payload.get("allow_non_embedded_fallback", False)),
    )
    if publish.state not in {"inactive", "active"}:
        raise PipelineError("Invalid publish.state: {}".format(publish.state))
    embedding = EmbeddingConfig(
        backend=(str(embedding_payload["backend"]) if embedding_payload.get("backend") else None),
        command=(str(embedding_payload["command"]) if embedding_payload.get("command") else None),
        provider=(str(embedding_payload["provider"]) if embedding_payload.get("provider") else None),
        model=(str(embedding_payload["model"]) if embedding_payload.get("model") else None),
        dimensions=(int(embedding_payload["dimensions"]) if embedding_payload.get("dimensions") is not None else None),
        space_id=(str(embedding_payload["space_id"]) if embedding_payload.get("space_id") else None),
    )
    if publish.require_embeddings and not publish.allow_non_embedded_fallback and not embedding_config_valid(embedding):
        raise PipelineError("missing_embedding_config")
    return RuntimeConfig(
        database_path=Path(database_path),
        output_dir=Path(output_dir),
        sources=sources,
        scheduler=validate_scheduler(payload.get("schedule") or {}),
        publish=publish,
        embedding=embedding,
        fixture_dir=(Path(runtime["fixture_dir"]) if runtime.get("fixture_dir") else None),
        source_config_path=str(path),
        config_hash=config_hash(payload),
    )


def connect_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    apply_migrations(connection)
    return connection


def apply_migrations(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          run_kind TEXT NOT NULL,
          started_at TEXT NOT NULL,
          completed_at TEXT,
          status TEXT NOT NULL,
          output_dir TEXT NOT NULL,
          effective_snapshot_id TEXT,
          summary_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS packages (
          package_id TEXT PRIMARY KEY,
          report_id TEXT NOT NULL,
          schema_version TEXT NOT NULL,
          embedding_space_id TEXT,
          embedding_vector_hash TEXT,
          active_eligible INTEGER NOT NULL,
          package_json TEXT NOT NULL,
          created_run_id TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE (report_id, schema_version, embedding_space_id, embedding_vector_hash)
        );
        CREATE TABLE IF NOT EXISTS accepted_reports (
          report_id TEXT PRIMARY KEY,
          first_accepted_run_id TEXT NOT NULL,
          first_effective_mode TEXT NOT NULL,
          first_snapshot_id TEXT NOT NULL,
          first_accepted_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS normalized_reports (
          report_id TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          first_seen_run_id TEXT NOT NULL,
          latest_seen_run_id TEXT NOT NULL,
          report_id_input_type TEXT NOT NULL,
          report_id_input_hash TEXT NOT NULL,
          feed_guid TEXT,
          raw_url TEXT,
          canonical_url TEXT,
          title_normalized TEXT NOT NULL,
          published_at TEXT,
          fetched_at TEXT NOT NULL,
          report_json TEXT NOT NULL,
          status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS report_seen_runs (
          report_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          seen_at TEXT NOT NULL,
          PRIMARY KEY (report_id, run_id)
        );
        CREATE TABLE IF NOT EXISTS dedupe_keys (
          key_scope TEXT NOT NULL,
          key_type TEXT NOT NULL,
          key_hash TEXT NOT NULL,
          report_id TEXT NOT NULL,
          normalized_key_value TEXT NOT NULL,
          PRIMARY KEY (key_type, key_scope, key_hash)
        );
        CREATE TABLE IF NOT EXISTS dedupe_decisions (
          run_id TEXT NOT NULL,
          report_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          decision TEXT NOT NULL,
          duplicate_of_report_id TEXT,
          duplicate_key_type TEXT,
          duplicate_key_scope TEXT,
          duplicate_key_hash TEXT,
          duplicate_key_precedence INTEGER,
          duplicate_key_value_preview TEXT,
          reason TEXT NOT NULL,
          detail_json TEXT NOT NULL,
          PRIMARY KEY (run_id, report_id)
        );
        CREATE TABLE IF NOT EXISTS skipped_items (
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          report_id TEXT,
          reason_code TEXT NOT NULL,
          detail_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_health (
          source_id TEXT PRIMARY KEY,
          last_status TEXT NOT NULL,
          last_run_id TEXT NOT NULL,
          last_checked_at TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          parse_status TEXT,
          last_successful_fetch_at TEXT,
          latest_item_published_at TEXT,
          consecutive_failures INTEGER NOT NULL DEFAULT 0,
          total_successes INTEGER NOT NULL DEFAULT 0,
          total_failures INTEGER NOT NULL DEFAULT 0,
          last_error_class TEXT,
          last_error_message TEXT,
          last_error TEXT,
          health_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_run_status (
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          status TEXT NOT NULL,
          fetched_at TEXT,
          http_status INTEGER,
          failure_reason TEXT,
          detail_json TEXT NOT NULL,
          PRIMARY KEY (run_id, source_id)
        );
        CREATE TABLE IF NOT EXISTS embeddings (
          report_id TEXT NOT NULL,
          embedding_space_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          model TEXT NOT NULL,
          dimensions INTEGER NOT NULL,
          embedded_text_hash TEXT NOT NULL,
          embedding_vector_hash TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          backend_request_id TEXT,
          status TEXT NOT NULL,
          failure_json TEXT,
          embedded_at TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (report_id, embedding_space_id, embedded_text_hash)
        );
        CREATE TABLE IF NOT EXISTS publish_attempts (
          attempt_id TEXT PRIMARY KEY,
          package_id TEXT NOT NULL,
          publish_target_key TEXT NOT NULL,
          publish_idempotency_key TEXT NOT NULL,
          effective_publish_snapshot_id TEXT NOT NULL,
          attempt_number INTEGER NOT NULL,
          mode TEXT NOT NULL,
          status TEXT NOT NULL,
          attempted_at TEXT NOT NULL,
          completed_at TEXT,
          response_json TEXT,
          error_class TEXT,
          error_message TEXT,
          UNIQUE (publish_idempotency_key, attempt_number)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_success_once ON publish_attempts(publish_idempotency_key) WHERE status = 'succeeded';
        CREATE TABLE IF NOT EXISTS runtime_config_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          config_hash TEXT NOT NULL,
          observed_at TEXT NOT NULL,
          requested_mode TEXT NOT NULL,
          effective_mode TEXT NOT NULL,
          activation_observed_at TEXT,
          live_approval_observed INTEGER NOT NULL,
          require_embeddings INTEGER NOT NULL,
          allow_non_embedded_fallback INTEGER NOT NULL,
          blocked_reason TEXT,
          snapshot_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_events (
          event_id TEXT PRIMARY KEY,
          observed_at TEXT NOT NULL,
          event_type TEXT NOT NULL,
          config_hash TEXT,
          snapshot_id TEXT,
          status TEXT NOT NULL,
          error_class TEXT,
          error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS scheduler_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          mode TEXT NOT NULL,
          interval_seconds INTEGER,
          jitter_seconds INTEGER NOT NULL,
          run_on_startup_if_due INTEGER NOT NULL,
          missed_tick_policy TEXT NOT NULL,
          config_hash TEXT NOT NULL,
          last_decision_at TEXT,
          last_started_run_id TEXT,
          last_completed_run_id TEXT,
          last_completed_at TEXT,
          running_run_id TEXT,
          next_due_at TEXT,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scheduler_events (
          event_id TEXT PRIMARY KEY,
          observed_at TEXT NOT NULL,
          event_type TEXT NOT NULL,
          decision_id TEXT,
          run_id TEXT,
          status TEXT NOT NULL,
          config_hash TEXT,
          detail_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prime_events (
          event_id TEXT PRIMARY KEY,
          requested_at TEXT NOT NULL,
          completed_at TEXT,
          requested_by TEXT,
          run_id TEXT,
          prime_watermark_run_id TEXT,
          status TEXT NOT NULL,
          error_class TEXT,
          error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS service_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          pid INTEGER NOT NULL,
          config_path TEXT NOT NULL,
          started_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    connection.commit()


def build_publish_snapshot(config: RuntimeConfig, observed_at: datetime, force_inactive: bool = False, error: Optional[str] = None) -> Dict[str, Any]:
    requested = "inactive" if force_inactive else config.publish.state
    effective = "inactive"
    blocked_reason = None
    activation_observed_at = None
    if error:
        blocked_reason = error
    elif config.publish.state == "active":
        if not config.publish.live_approval:
            effective = "blocked"
            blocked_reason = "missing_live_approval"
        elif not config.publish.subspace_endpoint:
            effective = "blocked"
            blocked_reason = "missing_subspace_endpoint"
        elif config.publish.require_embeddings and not config.publish.allow_non_embedded_fallback and not embedding_config_valid(config.embedding):
            effective = "blocked"
            blocked_reason = "missing_embedding_config"
        else:
            effective = "active"
            activation_observed_at = iso_z(observed_at)
    snapshot = {
        "snapshot_id": "sha256:{}:{}".format(config.config_hash, iso_z(observed_at)),
        "config_hash": config.config_hash,
        "observed_at": iso_z(observed_at),
        "requested_mode": requested,
        "effective_mode": effective,
        "activation_observed_at": activation_observed_at,
        "live_approval_observed": bool(config.publish.live_approval),
        "require_embeddings": bool(config.publish.require_embeddings),
        "allow_non_embedded_fallback": bool(config.publish.allow_non_embedded_fallback),
        "embedding_space_id": config.embedding.space_id,
        "embedding_backend": config.embedding.backend,
        "blocked_reason": blocked_reason,
    }
    return snapshot


def embedding_config_valid(config: EmbeddingConfig) -> bool:
    if config.backend == "disabled":
        return False
    if not config.backend or not config.provider or not config.model or not config.dimensions or not config.space_id:
        return False
    if not config.command:
        return False
    return True


def store_runtime_snapshot(connection: sqlite3.Connection, snapshot: Dict[str, Any], event_type: str, status: str, error: Optional[str] = None) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO runtime_config_snapshots
        (snapshot_id, config_hash, observed_at, requested_mode, effective_mode, activation_observed_at,
         live_approval_observed, require_embeddings, allow_non_embedded_fallback, blocked_reason, snapshot_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["snapshot_id"],
            snapshot["config_hash"],
            snapshot["observed_at"],
            snapshot["requested_mode"],
            snapshot["effective_mode"],
            snapshot["activation_observed_at"],
            int(snapshot["live_approval_observed"]),
            int(snapshot["require_embeddings"]),
            int(snapshot["allow_non_embedded_fallback"]),
            snapshot["blocked_reason"],
            json.dumps(snapshot, sort_keys=True),
        ),
    )
    connection.execute(
        "INSERT OR REPLACE INTO runtime_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id(event_type, parse_now(snapshot["observed_at"]), status + (error or "")),
            snapshot["observed_at"],
            event_type,
            snapshot["config_hash"],
            snapshot["snapshot_id"],
            status,
            "config" if error else None,
            error,
        ),
    )
    connection.commit()


def latest_snapshot(connection: sqlite3.Connection) -> Dict[str, Any]:
    row = connection.execute("SELECT snapshot_json FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1").fetchone()
    if row is None:
        raise PipelineError("No runtime publish snapshot")
    return json.loads(row["snapshot_json"])


def supplied_embedding_for(candidate: Dict[str, Any]) -> Dict[str, Any]:
    supplied = candidate.get("supplied_embeddings") or []
    if isinstance(supplied, list):
        return supplied[0] if supplied else {}
    if isinstance(supplied, dict):
        return supplied
    return {}


def package_id_for(candidate: Dict[str, Any]) -> str:
    supplied_embeddings = supplied_embedding_for(candidate)
    vector_hash = supplied_embeddings.get("vector_hash") or hashlib.sha256(
        json.dumps(supplied_embeddings, sort_keys=True).encode("utf-8")
    ).hexdigest()
    basis = "package:swarm.channel.news.report.v0\n{}\n{}\n{}\n{}".format(
        candidate["report_id"],
        candidate.get("schema_version", 1),
        supplied_embeddings.get("space_id", ""),
        vector_hash,
    )
    return "sha256:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()


def embedding_matches_snapshot(snapshot: Dict[str, Any], supplied_embeddings: Dict[str, Any]) -> bool:
    return bool(supplied_embeddings) and supplied_embeddings.get("space_id") == snapshot.get("embedding_space_id")


def is_scheduler_reload_error(exc: Exception) -> bool:
    text = str(exc)
    return text.startswith("Invalid schedule.") or text.startswith("schedule.")


def selected_dedupe_key_for_report(report: Dict[str, Any]) -> Tuple[str, str, str, str]:
    if report.get("feed_entry_id"):
        key_type = "feed_entry_id"
        normalized_key_value = normalize_feed_entry_id(report["feed_entry_id"])
    elif report.get("canonical_url"):
        key_type = "canonical_url"
        normalized_key_value = report["canonical_url"]
    elif not normalize_title(report["title"]):
        key_type = "missing_identity_key"
        normalized_key_value = ""
    else:
        key_type = "normalized_title_date"
        normalized_key_value = "{}\n{}".format(normalize_title(report["title"]), date_bucket(report.get("published_at") or report.get("fetched_at")))
    key_scope = "source:{}".format(report["source_id"])
    key_hash = "sha256:" + hashlib.sha256("dedupe:v0\n{}\n{}\n{}".format(key_scope, key_type, normalized_key_value).encode("utf-8")).hexdigest()
    return key_type, key_scope, normalized_key_value, key_hash


def canonical_embedding_record(candidate: Dict[str, Any]) -> Dict[str, Any]:
    supplied = supplied_embedding_for(candidate)
    return {
        "space_id": supplied.get("space_id"),
        "provider": supplied.get("provider") or supplied.get("backend"),
        "model": supplied.get("model"),
        "dimensions": supplied.get("dimensions"),
        "input_hash": supplied.get("input_hash") or ("sha256:" + hashlib.sha256((candidate.get("metadata", {}).get("embedding_text") or "").encode("utf-8")).hexdigest()),
        "vector_hash": supplied.get("vector_hash"),
        "vector": supplied.get("vector"),
    }


def embedded_text_hash(candidate: Dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256((candidate.get("metadata", {}).get("embedding_text") or "").encode("utf-8")).hexdigest()


def package_payload_for(candidate: Dict[str, Any], package_id: str) -> Dict[str, Any]:
    selected_key_type = candidate.get("dedupe_identity", {}).get("primary")
    selected_key_value = candidate.get("dedupe_identity", {}).get("value")
    selected_key_scope = "source:{}".format(candidate["source_id"])
    selected_key_hash = None
    if selected_key_type and selected_key_value is not None:
        selected_key_hash = "sha256:" + hashlib.sha256("dedupe:v0\n{}\n{}\n{}".format(selected_key_scope, selected_key_type, selected_key_value).encode("utf-8")).hexdigest()
    return {
        "schema": "swarm.channel.news.report.v0",
        "package_id": package_id,
        "report_id": candidate["report_id"],
        "body": {
            "title": candidate["title"],
            "summary": candidate["clean_summary"],
            "url": candidate["canonical_url"],
            "source_name": candidate["source_name"],
            "published_at": candidate["published_at"],
        },
        "provenance": {
            "source_id": candidate["source_id"],
            "source_class": candidate["source_class"],
            "feed_guid": candidate.get("feed_entry_id") if selected_key_type == "feed_entry_id" else None,
            "raw_url": candidate.get("raw_url"),
            "canonical_url": candidate["canonical_url"],
            "fetched_at": candidate["fetched_at"],
        },
        "dedupe": {
            "selected_key_type": selected_key_type,
            "selected_key_scope": selected_key_scope,
            "selected_key_hash": selected_key_hash,
        },
        "supplied_embeddings": [canonical_embedding_record(candidate)] if supplied_embedding_for(candidate) else [],
    }


class ArgusServer:
    def __init__(self, config_path: Path, clock: Optional[Clock] = None, register_service: bool = True) -> None:
        self.config_path = config_path
        self.clock = clock or Clock()
        self.register_service = register_service
        self.config = load_runtime_config(config_path)
        self.connection = connect_database(self.config.database_path)
        self.running = False
        self.reload_requested = False
        self._cycle_running = False
        if self.register_service:
            self.start()
        else:
            self._ensure_control_state()

    def _ensure_control_state(self) -> None:
        now = self.clock.now()
        store_runtime_snapshot(self.connection, build_publish_snapshot(self.config, now), "control_start", "ok")
        if self.connection.execute("SELECT 1 FROM scheduler_state WHERE id = 1").fetchone() is None:
            self._record_scheduler_config(self.config.scheduler, now, recompute_next=True)

    def start(self) -> None:
        now = self.clock.now()
        snapshot = build_publish_snapshot(self.config, now)
        store_runtime_snapshot(self.connection, snapshot, "service_start" if self.register_service else "control_start", "ok")
        if self.register_service:
            self.connection.execute(
                "INSERT OR REPLACE INTO service_state VALUES (1, ?, ?, ?, ?)",
                (os.getpid(), str(self.config_path), iso_z(now), iso_z(now)),
            )
            self._write_service_registration()
            stale = self.connection.execute("SELECT running_run_id FROM scheduler_state WHERE id = 1").fetchone()
            if stale and stale["running_run_id"]:
                self.connection.execute(
                    "UPDATE scheduler_state SET running_run_id = NULL, updated_at = ? WHERE id = 1",
                    (iso_z(now),),
                )
                self.connection.commit()
                self._record_scheduler_event("cycle_recovered_after_restart", stale["running_run_id"], "failed", {}, now)
        self._record_scheduler_config(self.config.scheduler, now, recompute_next=True)
        self._record_scheduler_event("scheduler_ready", None, "ok", {"mode": self.config.scheduler.mode}, now)

    def _write_service_registration(self) -> None:
        registration = {
            "pid": os.getpid(),
            "database_path": str(self.config.database_path),
            "config_path": str(self.config_path),
        }
        payload = json.dumps(registration, sort_keys=True) + "\n"
        self._runtime_registration_path().write_text(payload)
        self._temp_registration_path().write_text(payload)
        try:
            self._config_registration_path().write_text(payload)
        except OSError:
            pass

    def _config_registration_path(self) -> Path:
        return self.config_path.with_name(self.config_path.name + ".service.json")

    def _runtime_registration_path(self) -> Path:
        return self.config.database_path.with_name("argus.service.json")

    def _temp_registration_path(self) -> Path:
        name = "argus-{}.service.json".format(hashlib.sha256(str(self.config_path).encode("utf-8")).hexdigest()[:24])
        return Path(tempfile.gettempdir()) / name

    def close(self) -> None:
        self.connection.close()

    def request_reload(self, *_args: Any) -> None:
        self.reload_requested = True

    def reload(self) -> None:
        now = self.clock.now()
        try:
            new_config = load_runtime_config(self.config_path)
        except Exception as exc:
            if is_scheduler_reload_error(exc) and self._reload_publish_inputs_unchanged():
                self._record_scheduler_event("scheduler_reload_failed", None, "failed", {"error": str(exc)}, now)
                return
            snapshot = build_publish_snapshot(self.config, now, force_inactive=True, error=str(exc))
            store_runtime_snapshot(self.connection, snapshot, "reload_failed", "failed", str(exc))
            self._record_scheduler_event("scheduler_reload_failed", None, "failed", {"error": str(exc)}, now)
            return
        self.config = new_config
        snapshot = build_publish_snapshot(new_config, now)
        store_runtime_snapshot(self.connection, snapshot, "reload", "ok")
        self._record_scheduler_config(new_config.scheduler, now, recompute_next=True)
        self._record_scheduler_event("scheduler_reload", None, "ok", {"mode": new_config.scheduler.mode}, now)

    def _reload_publish_inputs_unchanged(self) -> bool:
        try:
            payload = yaml.safe_load(self.config_path.read_text())
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        publish_payload = payload.get("publish") or {}
        embedding_payload = payload.get("embedding") or {}
        publish = PublishConfig(
            state=str(publish_payload.get("state", "inactive")),
            live_approval=bool(publish_payload.get("live_approval", False)),
            subspace_endpoint=(str(publish_payload["subspace_endpoint"]) if publish_payload.get("subspace_endpoint") else None),
            require_embeddings=bool(publish_payload.get("require_embeddings", True)),
            allow_non_embedded_fallback=bool(publish_payload.get("allow_non_embedded_fallback", False)),
        )
        embedding = EmbeddingConfig(
            backend=(str(embedding_payload["backend"]) if embedding_payload.get("backend") else None),
            command=(str(embedding_payload["command"]) if embedding_payload.get("command") else None),
            provider=(str(embedding_payload["provider"]) if embedding_payload.get("provider") else None),
            model=(str(embedding_payload["model"]) if embedding_payload.get("model") else None),
            dimensions=(int(embedding_payload["dimensions"]) if embedding_payload.get("dimensions") is not None else None),
            space_id=(str(embedding_payload["space_id"]) if embedding_payload.get("space_id") else None),
        )
        return publish == self.config.publish and embedding == self.config.embedding

    def set_publish_state(self, state: str) -> Dict[str, Any]:
        if state not in {"inactive", "active"}:
            raise PipelineError("Invalid publish state: {}".format(state))
        self.config = dataclasses.replace(self.config, publish=dataclasses.replace(self.config.publish, state=state))
        snapshot = build_publish_snapshot(self.config, self.clock.now())
        store_runtime_snapshot(self.connection, snapshot, "set_publish_state", "ok")
        return snapshot

    def prime(self, requested_by: str = "cli") -> Tuple[int, Dict[str, Any]]:
        now = self.clock.now()
        state = self._scheduler_state()
        if self._cycle_running or state["running_run_id"]:
            return self._run_cycle("prime", now, prime=True)
        event = event_id("prime", now, requested_by)
        self.connection.execute("INSERT OR REPLACE INTO prime_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (event, iso_z(now), None, requested_by, None, None, "running", None, None))
        self.connection.commit()
        try:
            exit_code, summary = self._run_cycle("prime", now, prime=True)
            completed_at = iso_z(self.clock.now())
            self.connection.execute(
                "UPDATE prime_events SET completed_at = ?, run_id = ?, prime_watermark_run_id = ?, status = ? WHERE event_id = ?",
                (completed_at, summary["run_id"], summary["run_id"], "succeeded" if exit_code == 0 else "failed", event),
            )
            self.connection.commit()
            return exit_code, summary
        except Exception as exc:
            row = self.connection.execute("SELECT run_id FROM runs WHERE run_kind = 'prime' ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
            run_id = row["run_id"] if row else None
            self.connection.execute(
                "UPDATE prime_events SET completed_at = ?, run_id = ?, prime_watermark_run_id = NULL, status = ?, error_class = ?, error_message = ? WHERE event_id = ?",
                (iso_z(self.clock.now()), run_id, "failed", exc.__class__.__name__, str(exc), event),
            )
            self.connection.commit()
            raise

    def manual_cycle(self) -> Tuple[int, Dict[str, Any]]:
        return self._run_cycle("manual", self.clock.now(), prime=False)

    def tick(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        if self.reload_requested:
            self.reload_requested = False
            self.reload()
        now = self.clock.now()
        state = self._scheduler_state()
        if state["mode"] == "manual":
            self._record_scheduler_event("decision_manual_idle", None, "skipped", {}, now)
            return None
        if self._cycle_running or state["running_run_id"]:
            self._record_scheduler_event("cycle_skipped_already_running", None, "skipped", {}, now)
            return None
        next_due_at = parse_now(state["next_due_at"]) if state["next_due_at"] else now
        if now < next_due_at:
            self._record_scheduler_event("decision_not_due", None, "skipped", {"next_due_at": state["next_due_at"]}, now)
            return None
        return self._run_cycle("scheduled", now, prime=False)

    def serve_forever(self) -> int:
        self.running = True
        signal.signal(signal.SIGHUP, self.request_reload)
        while self.running:
            result = self.tick()
            if result is None:
                state = self._scheduler_state()
                next_due_at = parse_now(state["next_due_at"]) if state["next_due_at"] else self.clock.now() + timedelta(seconds=60)
                delay = max(1.0, min(60.0, (next_due_at - self.clock.now()).total_seconds()))
                self.clock.sleep(delay)
        return 0

    def status(self) -> Dict[str, Any]:
        state = self._scheduler_state()
        return {
            "database_path": str(self.config.database_path),
            "output_dir": str(self.config.output_dir),
            "publish": latest_snapshot(self.connection),
            "scheduler": dict(state),
            "last_run": self._last_run(),
        }

    def _run_cycle(self, run_kind: str, now: datetime, prime: bool = False) -> Tuple[int, Dict[str, Any]]:
        state = self._scheduler_state()
        if self._cycle_running or state["running_run_id"]:
            self._record_scheduler_event("cycle_skipped_already_running", state["running_run_id"], "skipped", {"requested_run_kind": run_kind}, now)
            return 0, {
                "run_id": state["running_run_id"],
                "run_kind": run_kind,
                "exit_status": "skipped",
                "skip_reason": "cycle_already_running",
            }
        self._cycle_running = True
        run_id = "{}-{}-{}".format(
            now.strftime("%Y%m%dT%H%M%SZ"),
            run_kind,
            hashlib.sha256("{}:{}:{}".format(run_kind, iso_z(now), time.monotonic_ns()).encode("utf-8")).hexdigest()[:8],
        )
        cycle_snapshot = latest_snapshot(self.connection)
        output_dir = self.config.output_dir / "runs" / run_id
        self.connection.execute(
            "UPDATE scheduler_state SET running_run_id = ?, last_started_run_id = ?, updated_at = ? WHERE id = 1",
            (run_id, run_id, iso_z(now)),
        )
        self.connection.commit()
        try:
            exit_code, summary = run_pipeline_for_sources(
                self.config.sources,
                self.config.source_config_path,
                output_dir,
                now,
                fixture_dir=self.config.fixture_dir,
                prime=prime,
                state_path=None,
                state_read=False,
                state_write=False,
                dedupe_in_pipeline=False,
                package_candidates_artifact=".pre-package-candidates.jsonl",
                publish_candidates_artifact=".pre-publish-candidates.jsonl",
            )
            summary["run_id"] = run_id
            summary["run_kind"] = run_kind
            if self.reload_requested:
                self.reload_requested = False
                self.reload()
            publish_snapshot = latest_snapshot(self.connection)
            summary["effective_publish_snapshot"] = cycle_snapshot
            self._store_run(run_id, run_kind, now, exit_code, output_dir, cycle_snapshot, summary)
            (output_dir / "run-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            self._store_source_health(run_id, now, output_dir)
            if prime:
                self._store_normalized_storage(run_id, now, output_dir, None)
                self._write_decision_artifacts(run_id, output_dir)
                self._write_prime_artifacts(output_dir)
            else:
                package_candidates_path = output_dir / ".pre-package-candidates.jsonl"
                package_candidate_rows = [json.loads(line) for line in package_candidates_path.read_text().splitlines() if line.strip()]
                candidate_report_ids = {row["report_id"] for row in package_candidate_rows}
                accepted_report_ids = self._store_normalized_storage(run_id, now, output_dir, candidate_report_ids)
                accepted_candidate_rows = []
                seen_candidate_report_ids = set()
                for row in package_candidate_rows:
                    if row["report_id"] in accepted_report_ids and row["report_id"] not in seen_candidate_report_ids:
                        accepted_candidate_rows.append(row)
                        seen_candidate_report_ids.add(row["report_id"])
                package_candidates_path.write_text("".join(json.dumps(row) + "\n" for row in accepted_candidate_rows))
                package_payloads = self._store_packages_and_publish(run_id, now, output_dir, package_candidates_path, cycle_snapshot, publish_snapshot)
                self._write_decision_artifacts(run_id, output_dir)
                (output_dir / "package-candidates.jsonl").write_text("".join(json.dumps(row) + "\n" for row in package_payloads))
                (output_dir / "publish-candidates.jsonl").write_text("".join(json.dumps(row) + "\n" for row in package_payloads))
            completed_at = self.clock.now()
            self._record_scheduler_completion(run_id, completed_at)
            self._record_scheduler_event("cycle_completed", run_id, "ok" if exit_code == 0 else "failed", {"run_kind": run_kind}, completed_at)
            return exit_code, summary
        except Exception as exc:
            self._mark_run_failed(run_id, run_kind, now, output_dir, cycle_snapshot, exc)
            self._record_scheduler_event("cycle_completed", run_id, "failed", {"run_kind": run_kind, "error": str(exc)}, self.clock.now())
            raise
        finally:
            self._cycle_running = False
            self.connection.execute(
                "UPDATE scheduler_state SET running_run_id = NULL, updated_at = ? WHERE running_run_id = ?",
                (iso_z(self.clock.now()), run_id),
            )
            self.connection.commit()

    def _write_prime_artifacts(self, output_dir: Path) -> None:
        (output_dir / "prime-baseline.json").write_text((output_dir / "run-summary.json").read_text())
        (output_dir / "prime-source-health.json").write_text((output_dir / "source-health.json").read_text())
        (output_dir / "prime-normalized-items.jsonl").write_text((output_dir / "normalized-items.jsonl").read_text())
        (output_dir / "prime-baseline.md").write_text("# Argus Prime Baseline\n\nNo live publish work was created.\n")

    def _write_decision_artifacts(self, run_id: str, output_dir: Path) -> None:
        dedupe_rows = [
            {
                **dict(row),
                "detail": json.loads(row["detail_json"]),
            }
            for row in self.connection.execute("SELECT * FROM dedupe_decisions WHERE run_id = ? ORDER BY source_id, report_id", (run_id,))
        ]
        skipped_rows = [
            {
                **dict(row),
                "detail": json.loads(row["detail_json"]),
            }
            for row in self.connection.execute("SELECT * FROM skipped_items WHERE run_id = ? ORDER BY source_id, report_id, reason_code", (run_id,))
        ]
        (output_dir / "dedupe-decisions.json").write_text(json.dumps(dedupe_rows, indent=2, sort_keys=True) + "\n")
        (output_dir / "skipped-items.json").write_text(json.dumps(skipped_rows, indent=2, sort_keys=True) + "\n")

    def _store_run(self, run_id: str, run_kind: str, now: datetime, exit_code: int, output_dir: Path, snapshot: Dict[str, Any], summary: Dict[str, Any]) -> None:
        status = "failed" if exit_code != 0 else "succeeded_with_source_errors" if summary.get("counts", {}).get("failed_sources", 0) else "succeeded"
        self.connection.execute(
            "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, run_kind, iso_z(now), iso_z(self.clock.now()), status, str(output_dir), snapshot["snapshot_id"], json.dumps(summary, sort_keys=True)),
        )
        self.connection.commit()

    def _mark_run_failed(self, run_id: str, run_kind: str, now: datetime, output_dir: Path, snapshot: Dict[str, Any], error: Exception) -> None:
        row = self.connection.execute("SELECT summary_json FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        summary = json.loads(row["summary_json"]) if row else {"run_id": run_id, "run_kind": run_kind}
        summary["post_pipeline_error"] = {"class": error.__class__.__name__, "message": str(error)}
        if row is None:
            self.connection.execute(
                "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, run_kind, iso_z(now), iso_z(self.clock.now()), "failed", str(output_dir), snapshot["snapshot_id"], json.dumps(summary, sort_keys=True)),
            )
        else:
            self.connection.execute(
                "UPDATE runs SET status = ?, completed_at = ?, summary_json = ? WHERE run_id = ?",
                ("failed", iso_z(self.clock.now()), json.dumps(summary, sort_keys=True), run_id),
            )
        self.connection.commit()

    def _store_source_health(self, run_id: str, now: datetime, output_dir: Path) -> None:
        path = output_dir / "source-health.json"
        health_rows = json.loads(path.read_text()) if path.exists() else []
        for item in health_rows:
            previous = self.connection.execute("SELECT * FROM source_health WHERE source_id = ?", (item["source_id"],)).fetchone()
            failed = item.get("status") == "failed"
            consecutive_failures = (int(previous["consecutive_failures"]) if previous else 0) + 1 if failed else 0
            total_successes = int(previous["total_successes"]) if previous else 0
            total_failures = int(previous["total_failures"]) if previous else 0
            if failed:
                total_failures += 1
            else:
                total_successes += 1
            last_error = item.get("last_error") or {}
            self.connection.execute(
                """
                INSERT INTO source_health VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                  last_status=excluded.last_status,
                  last_run_id=excluded.last_run_id,
                  last_checked_at=excluded.last_checked_at,
                  enabled=excluded.enabled,
                  parse_status=excluded.parse_status,
                  last_successful_fetch_at=excluded.last_successful_fetch_at,
                  latest_item_published_at=excluded.latest_item_published_at,
                  consecutive_failures=excluded.consecutive_failures,
                  total_successes=excluded.total_successes,
                  total_failures=excluded.total_failures,
                  last_error_class=excluded.last_error_class,
                  last_error_message=excluded.last_error_message,
                  last_error=excluded.last_error,
                  health_json=excluded.health_json
                """,
                (
                    item["source_id"],
                    item.get("status") or "unknown",
                    run_id,
                    item.get("fetched_at") or iso_z(now),
                    int(bool(item.get("enabled", True))),
                    item.get("parse_status"),
                    item.get("last_successful_fetch_at"),
                    item.get("latest_item_published_at"),
                    consecutive_failures,
                    total_successes,
                    total_failures,
                    last_error.get("class"),
                    last_error.get("message"),
                    item.get("failure_reason"),
                    json.dumps(item, sort_keys=True),
                ),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO source_run_status VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    item["source_id"],
                    item.get("status") or "unknown",
                    item.get("fetched_at"),
                    item.get("http_status"),
                    item.get("failure_reason"),
                    json.dumps(item, sort_keys=True),
                ),
            )
        self.connection.commit()

    def _embedding_for_candidate(self, candidate: Dict[str, Any], now: datetime) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if self.config.embedding.command:
            input_hash = embedded_text_hash(candidate)
            request_id = "sha256:" + hashlib.sha256(
                "embed-request:v0\n{}\n{}\n{}".format(candidate["report_id"], self.config.embedding.space_id, input_hash).encode("utf-8")
            ).hexdigest()
            request = {
                "request_id": request_id,
                "space_id": self.config.embedding.space_id,
                "provider": self.config.embedding.provider,
                "model": self.config.embedding.model,
                "dimensions": self.config.embedding.dimensions,
                "text": candidate.get("metadata", {}).get("embedding_text") or "",
            }
            try:
                completed = subprocess.run(
                    [self.config.embedding.command, "--input-json", "-"],
                    input=json.dumps(request),
                    capture_output=True,
                    check=False,
                    text=True,
                )
                if completed.returncode != 0:
                    failure_payload = None
                    for stream in (completed.stdout, completed.stderr):
                        try:
                            failure_payload = json.loads(stream) if stream.strip() else None
                        except ValueError:
                            failure_payload = None
                        if isinstance(failure_payload, dict):
                            break
                    if isinstance(failure_payload, dict):
                        return None, {
                            "class": str(failure_payload.get("error_class") or failure_payload.get("class") or "embed_backend_unavailable"),
                            "message": str(failure_payload.get("message") or failure_payload.get("error") or "embedding backend exited non-zero"),
                            "retry_eligible": bool(failure_payload.get("retry_eligible", True)),
                        }
                    return None, {
                        "class": "embed_backend_unavailable",
                        "message": completed.stderr.strip() or "embedding backend exited non-zero",
                        "retry_eligible": True,
                    }
                try:
                    response = json.loads(completed.stdout)
                except ValueError as exc:
                    return None, {"class": "embed_invalid_response", "message": str(exc), "retry_eligible": True}
            except OSError as exc:
                return None, {"class": "embed_backend_unavailable", "message": str(exc), "retry_eligible": True}
            if (
                response.get("space_id") != self.config.embedding.space_id
                or response.get("provider") != self.config.embedding.provider
                or response.get("model") != self.config.embedding.model
                or int(response.get("dimensions", -1)) != int(self.config.embedding.dimensions or -2)
            ):
                return None, {"class": "embed_invalid_response", "message": "embedding metadata mismatch", "retry_eligible": True}
            vector = response.get("vector")
            if not isinstance(vector, list):
                return None, {"class": "embed_invalid_response", "message": "embedding vector is not a list", "retry_eligible": True}
            vector_hash = "sha256:" + hashlib.sha256(json.dumps(vector, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()
            return {
                "space_id": self.config.embedding.space_id,
                "provider": response.get("provider") or self.config.embedding.provider or "cli",
                "model": self.config.embedding.model,
                "dimensions": self.config.embedding.dimensions,
                "input_hash": input_hash,
                "vector_hash": vector_hash,
                "vector": vector,
                "backend_request_id": response.get("backend_request_id"),
                "embedded_at": iso_z(now),
            }, None
        supplied = supplied_embedding_for(candidate)
        if supplied:
            return supplied, None
        return None, {"class": "embed_backend_unavailable", "message": "no supplied embedding", "retry_eligible": True}

    def _record_embedding_failure(self, run_id: str, now: datetime, candidate: Dict[str, Any], failure: Dict[str, Any]) -> Dict[str, Any]:
        failure = {
            **failure,
            "run_id": run_id,
            "source_id": candidate["source_id"],
            "report_id": candidate["report_id"],
        }
        self.connection.execute(
            "INSERT OR REPLACE INTO embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                candidate["report_id"],
                self.config.embedding.space_id or "",
                self.config.embedding.provider or "",
                self.config.embedding.model or "",
                int(self.config.embedding.dimensions or 0),
                embedded_text_hash(candidate),
                "",
                "[]",
                None,
                "failed",
                json.dumps(failure, sort_keys=True),
                None,
                iso_z(now),
            ),
        )
        return failure

    def _store_packages_and_publish(self, run_id: str, now: datetime, output_dir: Path, candidates_path: Path, cycle_snapshot: Dict[str, Any], publish_snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
        package_payloads: List[Dict[str, Any]] = []
        embedding_failures: List[Dict[str, Any]] = []
        rows = [json.loads(line) for line in candidates_path.read_text().splitlines() if line.strip()]
        for candidate in rows:
            if self.reload_requested:
                self.reload_requested = False
                self.reload()
                publish_snapshot = latest_snapshot(self.connection)
            self.connection.execute(
                "INSERT OR IGNORE INTO accepted_reports VALUES (?, ?, ?, ?, ?)",
                (
                    candidate["report_id"],
                    run_id,
                    cycle_snapshot["effective_mode"],
                    cycle_snapshot["snapshot_id"],
                    iso_z(now),
                ),
            )
            first = self.connection.execute(
                "SELECT first_effective_mode FROM accepted_reports WHERE report_id = ?",
                (candidate["report_id"],),
            ).fetchone()
            supplied_embeddings, embedding_failure = self._embedding_for_candidate(candidate, now)
            if supplied_embeddings is None and cycle_snapshot["require_embeddings"] and not cycle_snapshot["allow_non_embedded_fallback"]:
                failure = embedding_failure or {"class": "embed_backend_unavailable", "message": "embedding unavailable", "retry_eligible": True}
                embedding_failures.append(self._record_embedding_failure(run_id, now, candidate, failure))
                self.connection.execute(
                    "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        candidate["source_id"],
                        candidate["report_id"],
                        failure["class"],
                        json.dumps({**failure, "report_id": candidate["report_id"], "source_id": candidate["source_id"]}, sort_keys=True),
                        iso_z(now),
                    ),
                )
                continue
            if supplied_embeddings is None:
                supplied_embeddings = {}
            candidate = {**candidate, "supplied_embeddings": [supplied_embeddings] if supplied_embeddings else []}
            package_id = package_id_for(candidate)
            if supplied_embeddings:
                self.connection.execute(
                    "INSERT OR REPLACE INTO embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        candidate["report_id"],
                        supplied_embeddings.get("space_id", ""),
                        supplied_embeddings.get("provider", ""),
                        supplied_embeddings.get("model", ""),
                        int(supplied_embeddings.get("dimensions") or 0),
                        supplied_embeddings.get("input_hash") or embedded_text_hash(candidate),
                        supplied_embeddings.get("vector_hash", ""),
                        json.dumps(supplied_embeddings.get("vector") or [], sort_keys=True),
                        supplied_embeddings.get("backend_request_id"),
                        "embedded",
                        None,
                        supplied_embeddings.get("embedded_at") or iso_z(now),
                        iso_z(now),
                    ),
                )
            first_acceptance_allows_publish = first["first_effective_mode"] == "active" and (
                not cycle_snapshot["require_embeddings"] or cycle_snapshot["allow_non_embedded_fallback"] or embedding_matches_snapshot(cycle_snapshot, supplied_embeddings)
            )
            publish_allowed = first_acceptance_allows_publish and publish_snapshot["effective_mode"] == "active" and (
                not publish_snapshot["require_embeddings"] or publish_snapshot["allow_non_embedded_fallback"] or embedding_matches_snapshot(publish_snapshot, supplied_embeddings)
            )
            package_payload = package_payload_for(candidate, package_id)
            package_payloads.append(package_payload)
            self.connection.execute(
                "INSERT OR IGNORE INTO packages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    package_id,
                    candidate["report_id"],
                    str(candidate.get("schema_version", 1)),
                    supplied_embeddings.get("space_id"),
                    supplied_embeddings.get("vector_hash"),
                    int(first_acceptance_allows_publish),
                    json.dumps(package_payload, sort_keys=True),
                    run_id,
                    iso_z(now),
                ),
            )
            if publish_allowed:
                target = "sha256:" + hashlib.sha256(("publish-target:v0\n" + str(self.config.publish.subspace_endpoint)).encode("utf-8")).hexdigest()
                key = "sha256:" + hashlib.sha256(("publish:v0\n{}\n{}".format(package_id, target)).encode("utf-8")).hexdigest()
                if self.connection.execute("SELECT 1 FROM publish_attempts WHERE publish_idempotency_key = ? AND status = 'succeeded'", (key,)).fetchone():
                    continue
                attempt = "sha256:" + hashlib.sha256(("publish-attempt:v0\n{}\n1".format(key)).encode("utf-8")).hexdigest()
                self.connection.execute(
                    "INSERT OR IGNORE INTO publish_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (attempt, package_id, target, key, publish_snapshot["snapshot_id"], 1, publish_snapshot["effective_mode"], "pending", iso_z(now), None, None, None, None),
                )
        (output_dir / "embedding-failures.json").write_text(json.dumps(embedding_failures, indent=2, sort_keys=True) + "\n")
        self.connection.commit()
        return package_payloads

    def _store_normalized_storage(self, run_id: str, now: datetime, output_dir: Path, accepted_report_ids: Optional[set[str]]) -> set[str]:
        sqlite_accepted_report_ids: set[str] = set()
        rows = [json.loads(line) for line in (output_dir / "normalized-items.jsonl").read_text().splitlines() if line.strip()]
        rows.sort(key=lambda row: (row["source_id"], row.get("published_at") is None, row.get("published_at") or row.get("fetched_at") or "", row["report_id"]))
        for report in rows:
            key_type, key_scope, normalized_key_value, key_hash = selected_dedupe_key_for_report(report)
            existing_report = self.connection.execute("SELECT report_id FROM normalized_reports WHERE report_id = ?", (report["report_id"],)).fetchone()
            existing_key = self.connection.execute(
                "SELECT report_id FROM dedupe_keys WHERE key_type = ? AND key_scope = ? AND key_hash = ?",
                (key_type, key_scope, key_hash),
            ).fetchone()
            existing_seen_in_run = self.connection.execute(
                "SELECT 1 FROM report_seen_runs WHERE report_id = ? AND run_id = ?",
                (report["report_id"], run_id),
            ).fetchone()
            is_accepted = accepted_report_ids is None or report["report_id"] in accepted_report_ids
            retry_failed_embedding = bool(
                existing_report
                and is_accepted
                and self.connection.execute(
                    "SELECT 1 FROM embeddings WHERE report_id = ? AND status = 'failed'",
                    (report["report_id"],),
                ).fetchone()
                and not self.connection.execute(
                    "SELECT 1 FROM packages WHERE report_id = ?",
                    (report["report_id"],),
                ).fetchone()
            )
            if key_type == "missing_identity_key":
                decision = "missing_identity_key"
            elif retry_failed_embedding:
                decision = "seen_existing_report"
                sqlite_accepted_report_ids.add(report["report_id"])
            elif existing_key and existing_seen_in_run:
                decision = "exact_duplicate"
            elif existing_report:
                decision = "seen_existing_report"
            elif existing_key:
                decision = "exact_duplicate"
            elif is_accepted:
                decision = "accepted"
                sqlite_accepted_report_ids.add(report["report_id"])
            else:
                decision = "skipped_not_package_candidate"
            if decision in {"exact_duplicate", "skipped_not_package_candidate", "missing_identity_key"}:
                detail = {
                    "source_id": report["source_id"],
                    "report_id": report["report_id"],
                    "duplicate_of_report_id": existing_key["report_id"] if existing_key else None,
                    "duplicate_key_type": None if decision == "missing_identity_key" else key_type,
                    "duplicate_key_scope": key_scope,
                    "duplicate_key_hash": None if decision == "missing_identity_key" else key_hash,
                    "duplicate_key_precedence": 1 if key_type == "feed_entry_id" else 2 if key_type == "canonical_url" else 3 if key_type == "normalized_title_date" else None,
                    "reason": decision,
                    "title": report["title"],
                    "canonical_url": report["canonical_url"],
                    "feed_entry_id": report.get("feed_entry_id"),
                    "published_at": report.get("published_at"),
                }
                self.connection.execute(
                    "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, report["source_id"], report["report_id"], decision, json.dumps(detail, sort_keys=True), iso_z(now)),
                )
                self.connection.execute(
                    "INSERT OR REPLACE INTO dedupe_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        report["report_id"],
                        report["source_id"],
                        decision,
                        detail["duplicate_of_report_id"],
                        detail["duplicate_key_type"],
                        key_scope,
                        detail["duplicate_key_hash"],
                        detail["duplicate_key_precedence"],
                        normalized_key_value.replace("\n", " ")[:160],
                        decision,
                        json.dumps(detail, sort_keys=True),
                    ),
                )
                continue
            report_id_input_hash = "sha256:" + hashlib.sha256(normalized_key_value.encode("utf-8")).hexdigest()
            self.connection.execute(
                """
                INSERT INTO normalized_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(report_id) DO UPDATE SET latest_seen_run_id = excluded.latest_seen_run_id, report_json = excluded.report_json, status = excluded.status
                """,
                (
                    report["report_id"],
                    report["source_id"],
                    run_id,
                    run_id,
                    key_type,
                    report_id_input_hash,
                    report.get("feed_entry_id"),
                    report.get("raw_url"),
                    report.get("canonical_url"),
                    normalize_title(report["title"]),
                    report.get("published_at"),
                    report["fetched_at"],
                    json.dumps(report, sort_keys=True),
                    "accepted",
                ),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO report_seen_runs VALUES (?, ?, ?, ?)",
                (report["report_id"], run_id, report["source_id"], iso_z(now)),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO dedupe_keys VALUES (?, ?, ?, ?, ?)",
                (key_scope, key_type, key_hash, report["report_id"], normalized_key_value),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO dedupe_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    report["report_id"],
                    report["source_id"],
                    decision,
                    None,
                    key_type,
                    key_scope,
                    key_hash,
                    1 if key_type == "feed_entry_id" else 2 if key_type == "canonical_url" else 3,
                    normalized_key_value.replace("\n", " ")[:160],
                    decision,
                    json.dumps({"key_hash": key_hash, "key_type": key_type}, sort_keys=True),
                ),
            )
        skipped_path = output_dir / "skipped-items.json"
        for item in json.loads(skipped_path.read_text()) if skipped_path.exists() else []:
            reason_code = item.get("reason_code") or "source_skipped_counts"
            self.connection.execute(
                "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    item["source_id"],
                    item.get("report_id"),
                    reason_code,
                    json.dumps(item.get("detail") or item, sort_keys=True),
                    iso_z(now),
                ),
            )
        self.connection.commit()
        return sqlite_accepted_report_ids

    def _record_scheduler_config(self, scheduler: SchedulerConfig, now: datetime, recompute_next: bool) -> None:
        existing = self.connection.execute("SELECT mode, last_completed_at FROM scheduler_state WHERE id = 1").fetchone()
        last_completed_at = existing["last_completed_at"] if existing else None
        previous_mode = existing["mode"] if existing else None
        if scheduler.mode == "manual":
            next_due_at = None
        elif not last_completed_at and recompute_next and previous_mode == "manual":
            next_due_at = iso_z(now + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:{}".format(iso_z(now), scheduler.interval_seconds))))
        elif not last_completed_at and scheduler.run_on_startup_if_due:
            next_due_at = iso_z(now)
        elif last_completed_at:
            base = now if recompute_next and previous_mode == "manual" else parse_now(last_completed_at)
            next_due_at = iso_z(base + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:{}".format(iso_z(now), scheduler.interval_seconds))))
        else:
            next_due_at = iso_z(now + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:startup".format(iso_z(now)))))
        self.connection.execute(
            """
            INSERT INTO scheduler_state
            (id, mode, interval_seconds, jitter_seconds, run_on_startup_if_due, missed_tick_policy, config_hash,
             last_decision_at, last_started_run_id, last_completed_run_id, last_completed_at, running_run_id, next_due_at, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              mode=excluded.mode,
              interval_seconds=excluded.interval_seconds,
              jitter_seconds=excluded.jitter_seconds,
              run_on_startup_if_due=excluded.run_on_startup_if_due,
              missed_tick_policy=excluded.missed_tick_policy,
              config_hash=excluded.config_hash,
              next_due_at=excluded.next_due_at,
              updated_at=excluded.updated_at
            """,
            (
                scheduler.mode,
                scheduler.interval_seconds,
                scheduler.jitter_seconds,
                int(scheduler.run_on_startup_if_due),
                scheduler.missed_tick_policy,
                self.config.config_hash,
                last_completed_at,
                next_due_at,
                iso_z(now),
            ),
        )
        self.connection.commit()

    def _record_scheduler_completion(self, run_id: str, now: datetime) -> None:
        next_due_at = None
        if self.config.scheduler.mode == "interval":
            next_due_at = iso_z(now + timedelta(seconds=self.config.scheduler.interval_seconds + self._jitter_offset(run_id)))
        self.connection.execute(
            """
            UPDATE scheduler_state
            SET last_completed_run_id = ?, last_completed_at = ?, running_run_id = NULL, next_due_at = ?, updated_at = ?
            WHERE id = 1
            """,
            (run_id, iso_z(now), next_due_at, iso_z(now)),
        )
        self.connection.commit()

    def _jitter_offset(self, basis: str) -> int:
        jitter = self.config.scheduler.jitter_seconds
        if jitter <= 0:
            return 0
        return int(hashlib.sha256(basis.encode("utf-8")).hexdigest()[:8], 16) % (jitter + 1)

    def _record_scheduler_event(self, event_type: str, run_id: Optional[str], status: str, detail: Dict[str, Any], now: datetime) -> None:
        decision = event_id("decision", now, event_type + (run_id or ""))
        self.connection.execute(
            "INSERT OR REPLACE INTO scheduler_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id("scheduler", now, event_type + (run_id or "")),
                iso_z(now),
                event_type,
                decision,
                run_id,
                status,
                self.config.config_hash,
                json.dumps(detail, sort_keys=True),
            ),
        )
        self.connection.execute("UPDATE scheduler_state SET last_decision_at = ?, updated_at = ? WHERE id = 1", (iso_z(now), iso_z(now)))
        self.connection.commit()

    def _scheduler_state(self) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone()
        if row is None:
            raise PipelineError("Scheduler state is not initialized")
        return row

    def _last_run(self) -> Optional[Dict[str, Any]]:
        row = self.connection.execute("SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def run_status(db_path: Path) -> Dict[str, Any]:
    connection = connect_database(db_path)
    try:
        snapshot_row = connection.execute("SELECT snapshot_json FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1").fetchone()
        scheduler_row = connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone()
        run_row = connection.execute("SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
        return {
            "publish": json.loads(snapshot_row["snapshot_json"]) if snapshot_row else None,
            "scheduler": dict(scheduler_row) if scheduler_row else None,
            "last_run": dict(run_row) if run_row else None,
        }
    finally:
        connection.close()


def request_process_reload(config_path: Path) -> Dict[str, Any]:
    temp_registration_path = Path(tempfile.gettempdir()) / "argus-{}.service.json".format(hashlib.sha256(str(config_path).encode("utf-8")).hexdigest()[:24])
    registration_path = config_path.with_name(config_path.name + ".service.json")
    effective_registration_path = registration_path if registration_path.exists() else temp_registration_path
    if effective_registration_path.exists():
        registration = json.loads(effective_registration_path.read_text())
        pid = int(registration["pid"])
        if not _pid_matches_config(pid, config_path):
            raise PipelineError("Recorded Argus service is not running for {}".format(config_path))
        os.kill(pid, signal.SIGHUP)
        return {"pid": pid, "signal": "SIGHUP", "config_path": str(config_path)}
    config = load_runtime_config(config_path)
    connection = connect_database(config.database_path)
    try:
        row = connection.execute("SELECT * FROM service_state WHERE id = 1").fetchone()
        if row is None:
            raise PipelineError("No running Argus service is recorded")
        if not _pid_matches_config(int(row["pid"]), config_path):
            raise PipelineError("Recorded Argus service is not running for {}".format(config_path))
        os.kill(int(row["pid"]), signal.SIGHUP)
        return {"pid": int(row["pid"]), "signal": "SIGHUP", "config_path": str(config_path)}
    finally:
        connection.close()


def _pid_matches_config(pid: int, config_path: Path) -> bool:
    try:
        completed = subprocess.run(["ps", "-p", str(pid), "-o", "command="], check=False, capture_output=True, text=True)
    except OSError:
        return False
    command = completed.stdout.strip()
    return completed.returncode == 0 and str(config_path) in command and "argus" in command


def run_source_health(db_path: Path) -> List[Dict[str, Any]]:
    connection = connect_database(db_path)
    try:
        return [
            {
                **json.loads(row["health_json"]),
                "last_run_id": row["last_run_id"],
                "consecutive_failures": row["consecutive_failures"],
                "total_successes": row["total_successes"],
                "total_failures": row["total_failures"],
            }
            for row in connection.execute("SELECT * FROM source_health ORDER BY source_id")
        ]
    finally:
        connection.close()


def explain_skip(db_path: Path, run_id: str) -> Dict[str, Any]:
    connection = connect_database(db_path)
    try:
        row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise PipelineError("Unknown run: {}".format(run_id))
        output_dir = Path(row["output_dir"])
        summary = json.loads((output_dir / "run-summary.json").read_text())
        skipped_rows = connection.execute(
            "SELECT * FROM skipped_items WHERE run_id = ? ORDER BY source_id, report_id, reason_code",
            (run_id,),
        ).fetchall()
        duplicate_rows = connection.execute(
            "SELECT * FROM dedupe_decisions WHERE run_id = ? AND decision = 'exact_duplicate' ORDER BY source_id, report_id",
            (run_id,),
        ).fetchall()
        return {
            "run": dict(row),
            "summary": summary,
            "skipped_items": [
                {
                    **dict(skipped),
                    "detail": json.loads(skipped["detail_json"]),
                }
                for skipped in skipped_rows
            ],
            "exact_duplicates": [
                {
                    "source_id": duplicate["source_id"],
                    "report_id": duplicate["report_id"],
                    "duplicate_of_report_id": duplicate["duplicate_of_report_id"],
                    "selected_key_type": duplicate["duplicate_key_type"],
                    "selected_key_scope": duplicate["duplicate_key_scope"],
                    "selected_key_hash": duplicate["duplicate_key_hash"],
                    "selected_key_precedence": duplicate["duplicate_key_precedence"],
                    "normalized_value_preview": duplicate["duplicate_key_value_preview"],
                    "reason": duplicate["reason"],
                    "candidate": json.loads(duplicate["detail_json"]),
                    "cross_source_note": "source-local exact dedupe only; cross-source overlap is not suppressed",
                }
                for duplicate in duplicate_rows
            ],
        }
    finally:
        connection.close()
