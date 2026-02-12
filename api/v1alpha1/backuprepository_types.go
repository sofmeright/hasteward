package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// BackupRepository defines a restic repository connection.
// Cluster-scoped — multiple repos supported (3-2-1 rule).
type BackupRepository struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupRepositorySpec   `json:"spec,omitempty"`
	Status BackupRepositoryStatus `json:"status,omitempty"`
}

// BackupRepositorySpec defines the desired state of a backup repository.
type BackupRepositorySpec struct {
	Restic ResticSpec `json:"restic"`
}

// ResticSpec defines the restic repository connection details.
type ResticSpec struct {
	// Repository is the restic repo URL or local path.
	// Examples: "/backups/restic/local", "s3:http://ceph-rgw.gorons-bracelet.svc/hasteward"
	Repository string `json:"repository"`

	// PasswordSecretRef references the Secret containing the repo encryption password.
	PasswordSecretRef SecretKeyRef `json:"passwordSecretRef"`

	// EnvSecretRef optionally references a Secret whose keys are injected as env vars.
	// Used for S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
	EnvSecretRef *SecretRef `json:"envSecretRef,omitempty"`
}

// SecretKeyRef references a specific key within a Secret.
type SecretKeyRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

// SecretRef references an entire Secret (all keys become env vars).
type SecretRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// BackupRepositoryStatus defines the observed state of a backup repository.
type BackupRepositoryStatus struct {
	Ready             bool        `json:"ready"`
	LastCheck         metav1.Time `json:"lastCheck,omitempty"`
	SnapshotCount     int         `json:"snapshotCount,omitempty"`
	TotalSize         string      `json:"totalSize,omitempty"`
	DeduplicatedSize  string      `json:"deduplicatedSize,omitempty"`
	LastError         string      `json:"lastError,omitempty"`
}

// BackupRepositoryList contains a list of BackupRepository resources.
type BackupRepositoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupRepository `json:"items"`
}
