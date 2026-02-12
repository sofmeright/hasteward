# HASteward

**H**igh **A**vailability **Steward** — Go CLI and Kubernetes operator for database cluster triage, repair, backup, and restore. Backups use [restic](https://restic.net/) for block-level dedup, encryption, and compression.

> For the legacy Ansible playbook, see [ansible/README.md](ansible/README.md).

## Supported Engines

| Engine | Database | Operator |
|--------|----------|----------|
| `cnpg` | PostgreSQL | [CloudNativePG](https://cloudnative-pg.io/) |
| `galera` | MariaDB | [mariadb-operator](https://github.com/mariadb-operator/mariadb-operator) |

## Subcommands

| Command | Description |
|---------|-------------|
| `triage` | Read-only diagnostics for a database cluster |
| `repair` | Heal unhealthy database instances (with pre-repair backup) |
| `backup` | Back up a database cluster to a restic repository |
| `restore` | Restore a database cluster from a restic snapshot |
| `get backups` | List restic backup snapshots |
| `get policies` | List BackupPolicy resources |
| `get repositories` | List BackupRepository resources |
| `get status` | Show triage status of managed database clusters |
| `export` | Extract a backup snapshot to a local `.sql.gz` file |
| `prune` | Apply retention policy and remove old snapshots |
| `serve` | Run the operator (controller + scheduler) |

## Global Flags

| Flag | Short | Env | Description |
|------|-------|-----|-------------|
| `--engine` | `-e` | `HASTEWARD_ENGINE` | Database engine: `cnpg` or `galera` |
| `--cluster` | `-c` | `HASTEWARD_CLUSTER` | Database cluster CR name |
| `--namespace` | `-n` | `HASTEWARD_NAMESPACE` | Kubernetes namespace |
| `--backups-path` | | `HASTEWARD_BACKUPS_PATH` | Restic repository path or URL |
| `--restic-password` | | `RESTIC_PASSWORD` | Restic repository encryption password |
| `--instance` | `-i` | `HASTEWARD_INSTANCE` | Target specific instance number |
| `--force` | `-f` | `HASTEWARD_FORCE` | Override safety checks (targeted repair only) |
| `--no-escrow` | | `HASTEWARD_NO_ESCROW` | Skip pre-repair backup |
| `--method` | `-m` | `HASTEWARD_BACKUP_METHOD` | Backup method: `dump` (default) or `native` |
| `--snapshot` | | `HASTEWARD_SNAPSHOT` | Restic snapshot ID or `latest` (for restore) |
| `--heal-timeout` | | `HASTEWARD_HEAL_TIMEOUT` | Heal wait timeout in seconds (default: 600) |
| `--delete-timeout` | | `HASTEWARD_DELETE_TIMEOUT` | Delete wait timeout in seconds (default: 300) |
| `--verbose` | `-v` | `HASTEWARD_VERBOSE` | Debug logging |

## Backup Model

Backups are stored in restic repositories with content-defined chunking (CDC) for block-level deduplication. All backups go through `restic backup --stdin` with a virtual filename in the snapshot.

### Snapshot Tags

Every snapshot is tagged with:

| Tag | Values | Description |
|-----|--------|-------------|
| `engine` | `cnpg`, `galera` | Database engine |
| `cluster` | cluster name | Database cluster CR name |
| `namespace` | namespace | Kubernetes namespace |
| `type` | `backup`, `diverged` | Snapshot type (see below) |
| `job` | `20060102T150405Z` | Groups diverged snapshots from the same repair (diverged only) |

### Snapshot Types

| Type | When | Virtual Path | Description |
|------|------|-------------|-------------|
| `backup` | Normal backup or pre-repair escrow | `<ns>/<cluster>/pgdumpall.sql` | Standard database dump. Escrow backups before repair are also `type=backup` and follow normal retention. |
| `diverged` | Split-brain detected during repair | `<ns>/<cluster>/<ordinal>-pgdumpall.sql` | Per-instance capture of each diverged replica. Shared `job` tag groups them. Forensic record for admin review. |

Engine-specific filenames: CNPG uses `pgdumpall.sql`, Galera uses `mysqldump.sql`.

### Snapshot Timestamps

All snapshots use `--time <job-start>` so the restic timestamp reflects when the operation was initiated, not when the dump completed. This ensures all snapshots from the same job (escrow + diverged captures) share a consistent timestamp.

## Retention / Prune

```bash
hasteward prune -e cnpg -c my-postgres -n my-ns --backups-path /backups
```

| Flag | Default | Description |
|------|---------|-------------|
| `--keep-last` | 7 | Keep the last N snapshots (or jobs for diverged) |
| `--keep-daily` | 30 | Keep N daily snapshots (or jobs for diverged) |
| `--keep-weekly` | 12 | Keep N weekly snapshots (or jobs for diverged) |
| `--keep-monthly` | 24 | Keep N monthly snapshots (or jobs for diverged) |
| `-t` / `--type` | `backup` | Snapshot type to prune: `backup`, `diverged`, or `all` |

**Group-aware prune for diverged snapshots**: Retention policies apply to job groups, not individual snapshots. A repair job that captured 3 diverged instances counts as 1 unit for `--keep-last`. Snapshots sharing the same `job` tag are kept or removed together.

## Examples

### Triage

```bash
hasteward triage -e cnpg -c zitadel-postgres -n zeldas-lullaby
```

### Repair All Unhealthy Replicas

```bash
hasteward repair -e galera -c osticket-mariadb -n hyrule-castle \
  --backups-path /backups
```

### Repair a Specific Instance

```bash
hasteward repair -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  -i 3 --backups-path /backups
```

### Force Repair During Split-Brain

```bash
hasteward repair -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  -i 2 --force --backups-path /backups
```

### Backup (Dump)

```bash
hasteward backup -e cnpg -c grafana-postgres -n gossip-stone \
  --backups-path /backups
```

### Backup (Native S3 — CNPG Only)

```bash
hasteward backup -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  --method native
```

### Restore from Latest Snapshot

```bash
hasteward restore -e galera -c osticket-mariadb -n hyrule-castle \
  --backups-path /backups
```

### Restore a Specific Snapshot

```bash
hasteward restore -e cnpg -c grafana-postgres -n gossip-stone \
  --backups-path /backups --snapshot abc123
```

### Restore from a Diverged Snapshot

```bash
hasteward restore -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  --backups-path /backups --snapshot abc123 -i 2
```

### List Backups

```bash
hasteward get backups                              # all types
hasteward get backups -t diverged                  # diverged only
hasteward get backups -n zeldas-lullaby            # filter by namespace
hasteward get backups -c zitadel-postgres          # filter by cluster
```

### Export a Snapshot to File

```bash
hasteward export -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  --backups-path /backups --snapshot latest -o dump.sql.gz

# Export a diverged instance
hasteward export -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  --backups-path /backups --snapshot abc123 -i 2 -o instance2.sql.gz
```

### Prune Old Snapshots

```bash
hasteward prune -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  --backups-path /backups --keep-last 7 --keep-daily 30

# Prune diverged snapshots (group-aware: keeps last 3 repair jobs)
hasteward prune -e cnpg -c zitadel-postgres -n zeldas-lullaby \
  --backups-path /backups -t diverged --keep-last 3
```

## Safety Gates

### Repair Mode

| Scenario | Untargeted | Targeted |
|----------|-----------|----------|
| Split-brain detected | **HARD STOP** (no override) | Fail (override with `--force`) |
| Target is primary | N/A | **HARD STOP** (no override) |
| Target is healthy | Skipped automatically | Skip (override with `--force`) |
| No healthy donor | **ABORT** | **ABORT** |

### Pre-Repair Backup Behavior

| Configuration | Before Heal | Split-Brain |
|---------------|-------------|-------------|
| Default (`--backups-path`) | Escrow saved as `type=backup` | + Per-instance `type=diverged` with `job` tag |
| `--no-escrow` | Warning, no backup | No diverged captures |
| Heal fails | Escrow persists (normal backup retention) | Diverged snapshots persist for admin review |

## Operator Mode

```bash
hasteward serve
```

The operator watches CNPG Cluster and MariaDB CRs for `clinic.hasteward.prplanit.com/policy` annotations and runs scheduled backups and triage/repair operations.

### CRDs

**BackupPolicy** (cluster-scoped) — defines global defaults:

```yaml
apiVersion: clinic.hasteward.prplanit.com/v1alpha1
kind: BackupPolicy
metadata:
  name: default
spec:
  backupSchedule: "0 2 * * *"
  triageSchedule: "*/15 * * * *"
  mode: repair
  repositories:
    - local-backups
  retention:
    keepLast: 7
    keepDaily: 30
    keepWeekly: 12
    keepMonthly: 24
```

**BackupRepository** (cluster-scoped) — defines restic repo connection:

```yaml
apiVersion: clinic.hasteward.prplanit.com/v1alpha1
kind: BackupRepository
metadata:
  name: local-backups
spec:
  restic:
    repository: /backups/restic/local
    passwordSecretRef:
      name: restic-password
      namespace: fairy-bottle
      key: password
    envSecretRef:              # optional, for S3
      name: s3-credentials
      namespace: fairy-bottle
```

### Database CR Opt-In

Add annotations to CNPG Cluster or MariaDB CRs:

```yaml
metadata:
  annotations:
    clinic.hasteward.prplanit.com/policy: "default"
    # Optional overrides:
    clinic.hasteward.prplanit.com/backup-schedule: "0 3 * * *"
    clinic.hasteward.prplanit.com/mode: "triage"
    clinic.hasteward.prplanit.com/exclude: "true"
```

### Operator Endpoints

| Endpoint | Description |
|----------|-------------|
| `:8080/metrics` | Prometheus metrics |
| `:8081/healthz` | Liveness probe |
| `:8081/readyz` | Readiness probe |

## How It Works

### CNPG Repair Flow

1. **Triage** — Collect `pg_controldata` from all instances, query replication status, check disk space
2. **Safety gate** — Verify primary is running, check for split-brain via timeline analysis
3. **Escrow** — Stream `pg_dumpall` from primary through `restic backup --stdin` (`type=backup`)
4. **Diverged** — If split-brain: dump each running instance individually (`type=diverged`, ordinal-prefixed, shared `job` tag)
5. **Heal** — Fence instance, clear pgdata on existing PVC, `pg_basebackup` from primary, unfence
6. **Re-triage** — Verify cluster health post-repair

### Galera Repair Flow

1. **Triage** — Read `grastate.dat` from all nodes, query `wsrep` status, check disk space
2. **Safety gate** — Verify healthy donor exists, check for split-brain via UUID/seqno comparison
3. **Escrow** — Stream `mysqldump` from healthy donor through `restic backup --stdin` (`type=backup`)
4. **Diverged** — If split-brain: dump each running instance individually (`type=diverged`, ordinal-prefixed, shared `job` tag)
5. **Heal** — Suspend CR, scale down, preserve and reset `grastate.dat`/`galera.cache`, scale up, resume
6. **Re-triage** — Verify cluster health post-repair

### Backup Streaming

Backups use Kubernetes exec API to pipe database dump output directly through `restic backup --stdin`. No intermediate storage or temporary files on the database pods. Restic handles chunking, dedup, encryption, and compression.

## Running in Kubernetes

A Job template is provided in the [dungeon repo](https://gitlab.prplanit.com/precisionplanit/dungeon) at `ansible/k8s/recovery/hasteward-job.yaml`:

```bash
# One-time setup: ServiceAccount + RBAC
kubectl apply -f hasteward-job.yaml -l app.kubernetes.io/component=rbac

# One-time setup: Backups PVC
kubectl apply -f hasteward-job.yaml -l app.kubernetes.io/component=storage

# Per-run: Edit the Job env vars + args[0], then apply
kubectl apply -f hasteward-job.yaml -l app.kubernetes.io/component=job
```

### Container Image

```bash
docker.io/prplanit/hasteward:latest
```

Single static binary with embedded restic. No runtime dependencies.

## License

MIT
