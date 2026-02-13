# TODO

Planned features and improvements, ordered by priority.

## P0 — Critical

### Drop cluster-admin to least-privilege ClusterRole

Replace `cluster-admin` with the scoped ClusterRole in `deploy/rbac/clusterrole.yaml`.

Missing permissions to add:
- `pods/log` subresource (heal pod log streaming)
- `statefulsets/scale` subresource (Galera node heal: scale down/up)
- `pods` create verb (helper pods for CNPG pg_basebackup, Galera recovery)

See `docs/security.md` for full threat model.

### Vault engine

Backup Vault's Raft storage via `vault operator raft snapshot save -` (streams to
stdout). Fits the restic stdin model. Restore via `vault operator raft snapshot
restore -`. Triage via `vault status` + `vault operator raft list-peers`.

### S3 immutable backups (object lock)

Configure S3 targets with object lock for ransomware-resistant backups.

- Write-only IAM policy (PutObject only, no DeleteObject)
- Separate admin credentials for out-of-band pruning
- See `docs/security.md` for architecture

## P1 — High

### Standalone engine

Backup/restore for bare database containers not managed by CNPG or MariaDB Operator.
Supports postgres (`pg_dumpall`/`psql`), mysql/mariadb (`mysqldump`/`mysql`), and
mongo (`mongodump --archive`/`mongorestore --archive`).

Parameterized by `--db-type`, `--container`, `--secret-name`, `--secret-key`,
`--db-user`. Operator mode uses a `StandaloneDatabase` CRD for discovery and
scheduling.

No triage/repair — standalone databases have no operator to fence/unfence and no
replication to analyze. Backup and restore only.

### Per-cluster restic passwords

Reduce blast radius by using separate passwords per BackupRepository. The CRD
already supports this via `passwordSecretRef`.

### Restic copy / sync between repos

Transfer snapshots between repositories (filesystem to S3, local to offsite).
`restic copy` deduplicates during transfer. Enables tiered backup architecture
and disaster recovery. Could be a `hasteward sync` subcommand or operator-scheduled.

## P2 — Medium

### Scheduled restic check

Run `restic check` on a schedule to verify repository integrity. Emit Prometheus
metric on failure.

### vmbackup orchestration wrapper

Trigger VictoriaMetrics' native `vmbackup` tool on schedule. Orchestration only —
vmbackup handles its own incremental backup format. No restic wrapping.

### Automated restore verification

Periodically test-restore a backup into a temporary pod and verify it loads.

## P3 — Low / Out of Scope

| Service | Reason |
|---------|--------|
| Wazuh Indexer | OpenSearch native snapshot API to S3. Not a dump target. |
| Loki | Data lives in object storage. Backup the storage backend. |
| Gitaly | GitLab-native backup tooling. Already replicated via Praefect. |
| Redis | Cache/queue only. Authoritative data lives in SQL databases behind them. |
