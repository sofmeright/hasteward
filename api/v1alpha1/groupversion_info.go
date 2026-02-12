package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version for hasteward CRDs.
	GroupVersion = schema.GroupVersion{Group: "clinic.hasteward.prplanit.com", Version: "v1alpha1"}

	// SchemeBuilder is used to add types to the scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds hasteward types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&BackupRepository{},
		&BackupRepositoryList{},
		&BackupPolicy{},
		&BackupPolicyList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
