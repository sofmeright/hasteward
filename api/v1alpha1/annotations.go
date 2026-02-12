package v1alpha1

import "strconv"

// Annotation keys for database CR opt-in and overrides.
const (
	// AnnotationPolicy is the BackupPolicy name to use (required for opt-in).
	AnnotationPolicy = "clinic.hasteward.prplanit.com/policy"

	// AnnotationBackupSchedule overrides the backup cron schedule.
	AnnotationBackupSchedule = "clinic.hasteward.prplanit.com/backup-schedule"

	// AnnotationTriageSchedule overrides the triage cron schedule.
	AnnotationTriageSchedule = "clinic.hasteward.prplanit.com/triage-schedule"

	// AnnotationMode overrides the operation mode (triage, repair, disabled).
	AnnotationMode = "clinic.hasteward.prplanit.com/mode"

	// AnnotationRepositories overrides target repositories (comma-separated).
	AnnotationRepositories = "clinic.hasteward.prplanit.com/repositories"

	// AnnotationRetentionKeepLast overrides retention keep-last.
	AnnotationRetentionKeepLast = "clinic.hasteward.prplanit.com/retention-keep-last"

	// AnnotationRetentionKeepDaily overrides retention keep-daily.
	AnnotationRetentionKeepDaily = "clinic.hasteward.prplanit.com/retention-keep-daily"

	// AnnotationRetentionKeepWeekly overrides retention keep-weekly.
	AnnotationRetentionKeepWeekly = "clinic.hasteward.prplanit.com/retention-keep-weekly"

	// AnnotationRetentionKeepMonthly overrides retention keep-monthly.
	AnnotationRetentionKeepMonthly = "clinic.hasteward.prplanit.com/retention-keep-monthly"

	// AnnotationExclude excludes the database from all operations.
	AnnotationExclude = "clinic.hasteward.prplanit.com/exclude"

	// AnnotationHealTimeout overrides the heal timeout in seconds.
	AnnotationHealTimeout = "clinic.hasteward.prplanit.com/heal-timeout"

	// AnnotationDeleteTimeout overrides the delete timeout in seconds.
	AnnotationDeleteTimeout = "clinic.hasteward.prplanit.com/delete-timeout"
)

// Status annotations written by hasteward (read-only for users).
const (
	AnnotationLastBackup         = "clinic.hasteward.prplanit.com/last-backup"
	AnnotationLastBackupDuration = "clinic.hasteward.prplanit.com/last-backup-duration"
	AnnotationLastTriage         = "clinic.hasteward.prplanit.com/last-triage"
	AnnotationLastTriageResult   = "clinic.hasteward.prplanit.com/last-triage-result"
	AnnotationLastRepair         = "clinic.hasteward.prplanit.com/last-repair"
	AnnotationManaged            = "clinic.hasteward.prplanit.com/managed"
)

// EffectiveConfig is the resolved configuration for a managed database,
// merging BackupPolicy defaults with per-CR annotation overrides.
type EffectiveConfig struct {
	PolicyName     string
	BackupSchedule string
	TriageSchedule string
	Mode           string
	Repositories   []string
	Retention      RetentionPolicy
	HealTimeout    int
	DeleteTimeout  int
	Excluded       bool
}

// ParseAnnotations resolves the effective configuration for a database CR
// by starting from the BackupPolicy defaults and applying annotation overrides.
func ParseAnnotations(annotations map[string]string, policy *BackupPolicySpec) *EffectiveConfig {
	cfg := &EffectiveConfig{
		PolicyName: annotations[AnnotationPolicy],
	}

	if policy != nil {
		cfg.BackupSchedule = policy.BackupSchedule
		cfg.TriageSchedule = policy.TriageSchedule
		cfg.Mode = policy.Mode
		cfg.Repositories = policy.Repositories
		cfg.Retention = policy.Retention
		cfg.HealTimeout = policy.HealTimeout
		cfg.DeleteTimeout = policy.DeleteTimeout
	}

	// Apply annotation overrides
	if v, ok := annotations[AnnotationBackupSchedule]; ok {
		cfg.BackupSchedule = v
	}
	if v, ok := annotations[AnnotationTriageSchedule]; ok {
		cfg.TriageSchedule = v
	}
	if v, ok := annotations[AnnotationMode]; ok {
		cfg.Mode = v
	}
	if v, ok := annotations[AnnotationRepositories]; ok {
		cfg.Repositories = splitCSV(v)
	}
	if v, ok := annotations[AnnotationRetentionKeepLast]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Retention.KeepLast = n
		}
	}
	if v, ok := annotations[AnnotationRetentionKeepDaily]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Retention.KeepDaily = n
		}
	}
	if v, ok := annotations[AnnotationRetentionKeepWeekly]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Retention.KeepWeekly = n
		}
	}
	if v, ok := annotations[AnnotationRetentionKeepMonthly]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Retention.KeepMonthly = n
		}
	}
	if v, ok := annotations[AnnotationHealTimeout]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HealTimeout = n
		}
	}
	if v, ok := annotations[AnnotationDeleteTimeout]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.DeleteTimeout = n
		}
	}
	if v, ok := annotations[AnnotationExclude]; ok {
		cfg.Excluded = v == "true"
	}

	return cfg
}

// splitCSV splits a comma-separated string into trimmed non-empty parts.
func splitCSV(s string) []string {
	var parts []string
	for _, p := range splitString(s, ',') {
		p = trimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// splitString splits s by sep without importing strings.
func splitString(s string, sep byte) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// trimSpace trims leading/trailing whitespace without importing strings.
func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
