# Argus server-mode deploy and Racter readiness runbook

Argus production target is a long-running server process:

```bash
argus serve --config /etc/argus/argus.yaml
```

The host supervisor may start this process on Racter reboot. It must not schedule ingestion. Fetch cadence is owned by Argus' internal scheduler from `schedule:` config; the default interval is `1h`.

This runbook is a readiness/install guide only. Do not deploy from review agents unless Flynn explicitly asks for that exact deployment.

## Racter filesystem layout

```text
/opt/argus/                    # checkout or release tree
/usr/local/bin/argus           # wrapper/symlink to the installed CLI
/etc/argus/argus.yaml          # server-mode runtime config
/var/lib/argus/argus.sqlite3   # SQLite state
/var/lib/argus/runs/           # per-cycle artifacts
/var/log/argus/                # supervisor stdout/stderr logs
```

## Install/update steps

```bash
cd /opt/argus
git pull
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -e .
sudo mkdir -p /etc/argus /var/lib/argus/runs /var/log/argus
sudo cp config/argus.example.yaml /etc/argus/argus.yaml
sudo ln -sf /opt/argus/bin/argus /usr/local/bin/argus
```

Edit `/etc/argus/argus.yaml` for host-local paths and the embedding command. Default config keeps `publish.state: inactive` and `publish.live_approval: false`; that is the safety boundary.

## Runtime commands

Start the long-running process manually for readiness checks:

```bash
argus serve --config /etc/argus/argus.yaml
```

Deterministic one-decision smoke check without deploying a supervisor:

```bash
argus serve --config /etc/argus/argus.yaml --once
```

Operator controls:

```bash
argus prime --config /etc/argus/argus.yaml
argus run-cycle --config /etc/argus/argus.yaml --reason manual
argus run-cycle --config /etc/argus/argus-e2e-canary.yaml --reason e2e-shrdlu --max-live-publishes 1
argus reload --config /etc/argus/argus.yaml
argus set-publish-state --config /etc/argus/argus.yaml --state inactive
argus set-publish-state --config /etc/argus/argus.yaml --state active
argus status --db /var/lib/argus/argus.sqlite3
argus source-health --db /var/lib/argus/argus.sqlite3
argus explain-skip --db /var/lib/argus/argus.sqlite3 --run <run_id>
```

`prime` is optional/manual baseline tooling only. Normal scheduled/manual cycles do not require prime and do not use prime as an active/inactive gate.

## Racter readiness checks

Before declaring Racter ready, verify:

```bash
argus serve --config /etc/argus/argus.yaml --once
argus status --db /var/lib/argus/argus.sqlite3
argus source-health --db /var/lib/argus/argus.sqlite3
```

Then inspect the newest run directory under `/var/lib/argus/runs/` and confirm required inactive artifacts exist:

```bash
test -s /var/lib/argus/runs/<run-id>/run-summary.json
test -s /var/lib/argus/runs/<run-id>/source-health.json
test -s /var/lib/argus/runs/<run-id>/digest.md
test -s /var/lib/argus/runs/<run-id>/digest.json
test -s /var/lib/argus/runs/<run-id>/normalized-items.jsonl
test -s /var/lib/argus/runs/<run-id>/dedupe-decisions.json
test -s /var/lib/argus/runs/<run-id>/skipped-items.json
test -s /var/lib/argus/runs/<run-id>/package-candidates.jsonl
```

A healthy inactive deployment has no `publish_attempts` rows unless `publish.state: active`, live approval, and Subspace endpoint config are all present.

## Controlled Subetha/Subspace E2E canary

Do not run a live E2E send without explicit Flynn approval. Active publish is externally visible.

Prepare a separate one-item canary config from `config/argus.e2e-canary.example.yaml`; keep the production `/etc/argus/argus.yaml` inactive while testing. Required live values after approval:

```yaml
publish:
  state: active
  live_approval: true
  subspace_endpoint: https://subspace.swarm.channel
  subspace_daemon_socket: ~/.openclaw/subspace-daemon/daemon.sock
  subspace_daemon_api_path: /v1/messages
  require_embeddings: true
```

Use a canary-local database/output directory such as `/var/lib/argus-e2e/`, and verify the canary source list still has exactly one enabled source before activation:

```bash
grep -A80 '^sources:' /etc/argus/argus-e2e-canary.yaml
argus run-cycle --config /etc/argus/argus-e2e-canary.yaml --reason e2e-shrdlu --max-live-publishes 1
argus status --db /var/lib/argus-e2e/argus.sqlite3
sqlite3 /var/lib/argus-e2e/argus.sqlite3 'select status, subspace_message_id, response_json from publish_attempts;'
```

The checked-in canary config is fixture-backed by design so it emits exactly one operator-controlled package rather than live feed contents. The `--max-live-publishes 1` guard fails the cycle before any daemon POST if more than one active-eligible live send would be emitted. shrdlu-side receipt is verified outside Argus by observing the expected Subspace inbound message with the recorded `subspace_message_id` and package `package_id`.

Rollback is to set the canary config back to `publish.state: inactive` and `publish.live_approval: false`, then rerun status checks. Do not delete the canary SQLite file before capturing the `publish_attempts` evidence.

## Supervisor boundary

A supervisor may keep `argus serve --config /etc/argus/argus.yaml` running after Racter reboot. It must not be configured as a timer that periodically invokes one-shot ingestion. No cron, systemd timer, launchd timer, or equivalent external schedule is part of the production model.
