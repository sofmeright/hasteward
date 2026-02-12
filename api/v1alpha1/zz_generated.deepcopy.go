package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

// --- BackupRepository ---

func (in *BackupRepository) DeepCopyInto(out *BackupRepository) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *BackupRepository) DeepCopy() *BackupRepository {
	if in == nil {
		return nil
	}
	out := new(BackupRepository)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupRepository) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// --- BackupRepositorySpec ---

func (in *BackupRepositorySpec) DeepCopyInto(out *BackupRepositorySpec) {
	*out = *in
	in.Restic.DeepCopyInto(&out.Restic)
}

// --- ResticSpec ---

func (in *ResticSpec) DeepCopyInto(out *ResticSpec) {
	*out = *in
	out.PasswordSecretRef = in.PasswordSecretRef
	if in.EnvSecretRef != nil {
		out.EnvSecretRef = new(SecretRef)
		*out.EnvSecretRef = *in.EnvSecretRef
	}
}

// --- BackupRepositoryStatus ---

func (in *BackupRepositoryStatus) DeepCopyInto(out *BackupRepositoryStatus) {
	*out = *in
	in.LastCheck.DeepCopyInto(&out.LastCheck)
}

// --- BackupRepositoryList ---

func (in *BackupRepositoryList) DeepCopyInto(out *BackupRepositoryList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]BackupRepository, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *BackupRepositoryList) DeepCopy() *BackupRepositoryList {
	if in == nil {
		return nil
	}
	out := new(BackupRepositoryList)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupRepositoryList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// --- BackupPolicy ---

func (in *BackupPolicy) DeepCopyInto(out *BackupPolicy) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

func (in *BackupPolicy) DeepCopy() *BackupPolicy {
	if in == nil {
		return nil
	}
	out := new(BackupPolicy)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupPolicy) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// --- BackupPolicySpec ---

func (in *BackupPolicySpec) DeepCopyInto(out *BackupPolicySpec) {
	*out = *in
	out.Retention = in.Retention
	if in.Repositories != nil {
		out.Repositories = make([]string, len(in.Repositories))
		copy(out.Repositories, in.Repositories)
	}
}

// --- BackupPolicyList ---

func (in *BackupPolicyList) DeepCopyInto(out *BackupPolicyList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]BackupPolicy, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *BackupPolicyList) DeepCopy() *BackupPolicyList {
	if in == nil {
		return nil
	}
	out := new(BackupPolicyList)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupPolicyList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}
