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

## Supervisor boundary

A supervisor may keep `argus serve --config /etc/argus/argus.yaml` running after Racter reboot. It must not be configured as a timer that periodically invokes one-shot ingestion. No cron, systemd timer, launchd timer, or equivalent external schedule is part of the production model.
