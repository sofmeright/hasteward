package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// BackupPolicy defines global defaults for backup and triage operations.
// Cluster-scoped — individual database CRs override via annotations.
type BackupPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec BackupPolicySpec `json:"spec,omitempty"`
}

// BackupPolicySpec defines the desired backup and triage configuration.
type BackupPolicySpec struct {
	// BackupSchedule is a cron expression for backup frequency.
	BackupSchedule string `json:"backupSchedule,omitempty"`

	// TriageSchedule is a cron expression for health-check frequency.
	TriageSchedule string `json:"triageSchedule,omitempty"`

	// Mode controls the triage behavior: "triage" (read-only), "repair" (auto-heal), or "disabled".
	Mode string `json:"mode,omitempty"`

	// Retention defines the restic snapshot retention policy.
	Retention RetentionPolicy `json:"retention,omitempty"`

	// Repositories lists BackupRepository names to target.
	Repositories []string `json:"repositories,omitempty"`

	// HealTimeout is the heal operation timeout in seconds.
	HealTimeout int `json:"healTimeout,omitempty"`

	// DeleteTimeout is the delete operation timeout in seconds.
	DeleteTimeout int `json:"deleteTimeout,omitempty"`
}

// RetentionPolicy defines how many restic snapshots to keep.
type RetentionPolicy struct {
	KeepLast    int `json:"keepLast,omitempty"`
	KeepDaily   int `json:"keepDaily,omitempty"`
	KeepWeekly  int `json:"keepWeekly,omitempty"`
	KeepMonthly int `json:"keepMonthly,omitempty"`
}

// BackupPolicyList contains a list of BackupPolicy resources.
type BackupPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupPolicy `json:"items"`
}
