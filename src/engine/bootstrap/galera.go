package bootstrap

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/PrPlanIT/HASteward/src/common"
	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
	"github.com/PrPlanIT/HASteward/src/engine/triage"
	"github.com/PrPlanIT/HASteward/src/k8s"
	"github.com/PrPlanIT/HASteward/src/output"
	"github.com/PrPlanIT/HASteward/src/output/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func init() {
	Register("galera", func(ep provider.EngineProvider) (Bootstrapper, error) {
		p, ok := ep.(*provider.GaleraProvider)
		if !ok {
			return nil, fmt.Errorf("galera bootstrapper requires *provider.GaleraProvider, got %T", ep)
		}
		t, err := triage.Get(p)
		if err != nil {
			return nil, fmt.Errorf("galera bootstrapper: %w", err)
		}
		return &galeraBootstrap{p: p, triager: t}, nil
	})
}

// galeraBootstrap implements the Bootstrapper interface for MariaDB Galera clusters.
type galeraBootstrap struct {
	p       *provider.GaleraProvider
	triager triage.Triager
}

func (b *galeraBootstrap) Name() string { return "galera" }

// Bootstrap performs a full Galera cluster bootstrap when all nodes are down.
//
// Flow:
//  1. Triage to assess cluster state
//  2. Paranoid safety gates (refuse if healthy nodes exist, refuse if ambiguous seqno unless --force)
//  3. Identify best bootstrap candidate (highest effective seqno)
//  4. If dryRun: return decision + planned actions without mutation
//  5. Execute: suspend CR -> scale to 0 -> set safe_to_bootstrap=1 on candidate PVC ->
//     patch CR forceClusterBootstrapInPod -> scale up -> resume -> wait ready
//  6. Re-triage and return typed BootstrapResult
func (b *galeraBootstrap) Bootstrap(ctx context.Context, dryRun bool) (*model.BootstrapResult, error) {
	cfg := b.p.Config()
	ns := cfg.Namespace
	clusterRef := model.ObjectRef{
		APIVersion: "k8s.mariadb.com/v1alpha1",
		Kind:       "MariaDB",
		Namespace:  ns,
		Name:       cfg.ClusterName,
	}
	result := &model.BootstrapResult{
		Engine:  "galera",
		Cluster: clusterRef,
	}

	// Phase 1: Triage
	output.Section("Bootstrap Phase 1: Triage")
	triageResult, err := triage.Run(ctx, b.triager, engine.NopSink{})
	if err != nil {
		return nil, fmt.Errorf("triage failed: %w", err)
	}

	// Phase 2: Safety gates
	output.Section("Bootstrap Phase 2: Safety Gates")

	// Gate 1: Must have all nodes down
	if !triageResult.AllNodesDown {
		healthyCount := 0
		for _, a := range triageResult.Assessments {
			if a.IsRunning && a.IsReady {
				healthyCount++
			}
		}
		if healthyCount > 0 {
			result.Decision = model.BootstrapDecision{
				Eligible: false,
				Reason:   fmt.Sprintf("cluster has %d healthy node(s) — bootstrap is only for full cluster down", healthyCount),
			}
			return result, fmt.Errorf("ABORT: Cluster has %d healthy node(s). Bootstrap is only for when ALL nodes are down. Use 'repair' instead", healthyCount)
		}
	}

	// Gate 2: Must have a best candidate
	if triageResult.BestSeqnoNode == nil {
		result.Decision = model.BootstrapDecision{
			Eligible: false,
			Reason:   "no bootstrap candidate found — could not determine best seqno node",
		}
		return result, fmt.Errorf("ABORT: Could not determine best seqno node. Cannot bootstrap")
	}

	candidate := triageResult.BestSeqnoNode
	candidateSeqno := candidate.EffectiveSeqno

	// Gate 3: Check for ambiguity (multiple nodes at same seqno)
	var competitors []string
	for _, a := range triageResult.Assessments {
		if a.Pod != candidate.Pod && a.EffectiveSeqno == candidateSeqno && candidateSeqno > 0 {
			competitors = append(competitors, a.Pod)
		}
	}
	ambiguous := len(competitors) > 0

	// Gate 4: Split-brain detection
	safeToHeal := triageResult.DataComparison.SafeToHeal

	forceRequired := ambiguous || !safeToHeal
	safeToProceed := !forceRequired || cfg.Force

	result.Decision = model.BootstrapDecision{
		Eligible:          true,
		Reason:            "all nodes down, bootstrap candidate identified",
		CandidatePod:      candidate.Pod,
		CandidateSeqno:    candidateSeqno,
		CandidateUUID:     candidate.UUID,
		AmbiguityDetected: ambiguous,
		ForceRequired:     forceRequired,
		SafeToProceed:     safeToProceed,
		Competitors:       competitors,
	}

	if ambiguous {
		common.WarnLog("AMBIGUITY: Multiple nodes at seqno %d: %s and %v",
			candidateSeqno, candidate.Pod, competitors)
		if !cfg.Force {
			result.Decision.Reason = fmt.Sprintf("ambiguous: %s and %v all at seqno %d — use --force to pick %s",
				candidate.Pod, competitors, candidateSeqno, candidate.Pod)
			return result, fmt.Errorf("ABORT: Ambiguous bootstrap candidate. Multiple nodes at seqno %d. Re-run with --force to select %s",
				candidateSeqno, candidate.Pod)
		}
		common.WarnLog("force=true — proceeding with %s despite ambiguity", candidate.Pod)
	}

	if !safeToHeal && !cfg.Force {
		result.Decision.Reason = "split-brain detected — use --force to override"
		return result, fmt.Errorf("ABORT: Split-brain detected. Re-run with --force to override")
	}

	output.Success("Bootstrap candidate: %s (seqno: %d, uuid: %s)", candidate.Pod, candidateSeqno, candidate.UUID)

	// Build planned actions
	stsRef := model.ObjectRef{Kind: "StatefulSet", Namespace: ns, Name: cfg.ClusterName}
	crRef := clusterRef

	result.ActionsPlanned = []model.BootstrapAction{
		{Phase: model.PhaseSuspend, Description: "Suspend MariaDB CR", Resource: &crRef},
		{Phase: model.PhaseScaleDown, Description: "Scale StatefulSet to 0", Resource: &stsRef},
		{Phase: model.PhaseBootstrapMark, Description: fmt.Sprintf("Set safe_to_bootstrap=1 on %s PVC", candidate.Pod)},
		{Phase: model.PhaseClusterPatch, Description: "Patch CR forceClusterBootstrapInPod=" + candidate.Pod, Resource: &crRef},
		{Phase: model.PhaseScaleUp, Description: fmt.Sprintf("Scale StatefulSet to %d", b.p.Replicas()), Resource: &stsRef},
		{Phase: model.PhaseWaitReady, Description: "Wait for all pods Ready"},
		{Phase: model.PhaseCleanup, Description: "Resume CR and clean up recovery pods", Resource: &crRef},
		{Phase: model.PhaseVerify, Description: "Re-triage to verify cluster health"},
	}

	// Dry run: return plan without executing
	if dryRun {
		output.Info("DRY RUN — returning planned actions without executing")
		return result, nil
	}

	// Phase 3: Execute
	output.Section("Bootstrap Phase 3: Execute")
	if err := b.executeBootstrap(ctx, candidate.Pod, result); err != nil {
		return result, err
	}

	return result, nil
}

func (b *galeraBootstrap) executeBootstrap(ctx context.Context, candidatePod string, result *model.BootstrapResult) error {
	cfg := b.p.Config()
	ns := cfg.Namespace
	c := k8s.GetClients()
	originalReplicas := int32(b.p.Replicas())

	// Capture SA before pods are deleted
	sa := "default"
	pods, err := c.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/instance=%s", cfg.ClusterName),
	})
	if err == nil {
		sa = k8s.ServiceAccountFromPods(pods.Items)
	}

	suspended := false
	scaledDown := false

	rescue := func() {
		if scaledDown {
			_ = b.scaleStatefulSet(ctx, originalReplicas)
		}
		if suspended {
			_ = b.resumeCR(ctx)
		}
		if suspended || scaledDown {
			common.WarnLog("BOOTSTRAP FAILED. CR resumed and scale restored.")
		}
	}

	markAction := func(phase string) {
		for i := range result.ActionsTaken {
			if result.ActionsTaken[i].Phase == phase {
				result.ActionsTaken[i].Completed = true
				return
			}
		}
	}

	// Copy planned to taken
	result.ActionsTaken = make([]model.BootstrapAction, len(result.ActionsPlanned))
	copy(result.ActionsTaken, result.ActionsPlanned)

	// STEP 1: Suspend CR
	common.InfoLog("STEP 1: Suspending MariaDB CR")
	if err := b.suspendCR(ctx); err != nil {
		return fmt.Errorf("failed to suspend CR: %w", err)
	}
	suspended = true
	markAction(model.PhaseSuspend)
	time.Sleep(3 * time.Second)

	// STEP 2: Scale to 0
	common.InfoLog("STEP 2: Scaling StatefulSet to 0")
	if err := b.scaleStatefulSet(ctx, 0); err != nil {
		rescue()
		return fmt.Errorf("failed to scale StatefulSet to 0: %w", err)
	}
	scaledDown = true
	markAction(model.PhaseScaleDown)

	// Wait for all pods to terminate
	deleteTimeout := cfg.DeleteTimeout
	if deleteTimeout <= 0 {
		deleteTimeout = 300
	}
	for i := 0; i < deleteTimeout/5; i++ {
		pods, err := c.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app.kubernetes.io/instance=%s", cfg.ClusterName),
		})
		if err != nil || len(pods.Items) == 0 {
			common.InfoLog("All pods terminated")
			break
		}
		time.Sleep(5 * time.Second)
	}

	// STEP 3: Set safe_to_bootstrap=1 on candidate PVC
	common.InfoLog("STEP 3: Setting safe_to_bootstrap=1 on %s", candidatePod)
	storagePVC := fmt.Sprintf("storage-%s", candidatePod)
	helperName := fmt.Sprintf("%s-bootstrap-%d", cfg.ClusterName, time.Now().Unix())

	bootstrapScript := `set -e
echo "=== Current grastate.dat ==="
cat /var/lib/mysql/grastate.dat 2>/dev/null || echo "not found"
echo ""
echo "=== Preserving grastate.dat ==="
cp /var/lib/mysql/grastate.dat /var/lib/mysql/grastate.dat.pre-bootstrap 2>/dev/null || echo "nothing to preserve"
echo "=== Setting safe_to_bootstrap: 1 ==="
sed -i 's/safe_to_bootstrap: 0/safe_to_bootstrap: 1/' /var/lib/mysql/grastate.dat
echo "=== Updated grastate.dat ==="
cat /var/lib/mysql/grastate.dat
echo "=== Done ==="
`
	if err := b.runHelperPod(ctx, helperName, storagePVC, "/var/lib/mysql", bootstrapScript, sa); err != nil {
		rescue()
		return fmt.Errorf("failed to set safe_to_bootstrap: %w", err)
	}
	markAction(model.PhaseBootstrapMark)

	// STEP 4: Patch CR with forceClusterBootstrapInPod
	common.InfoLog("STEP 4: Patching CR forceClusterBootstrapInPod=%s", candidatePod)
	patchJSON := fmt.Sprintf(`{"spec":{"galera":{"recovery":{"forceClusterBootstrapInPod":"%s"}}}}`, candidatePod)
	_, err = c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(ns).Patch(
		ctx, cfg.ClusterName, types.MergePatchType, []byte(patchJSON), metav1.PatchOptions{})
	if err != nil {
		rescue()
		return fmt.Errorf("failed to patch CR: %w", err)
	}
	markAction(model.PhaseClusterPatch)

	// STEP 5: Scale back up
	common.InfoLog("STEP 5: Scaling StatefulSet to %d", originalReplicas)
	b.deleteRecoveryPods(ctx)
	time.Sleep(2 * time.Second)

	if err := b.scaleStatefulSet(ctx, originalReplicas); err != nil {
		rescue()
		return fmt.Errorf("failed to scale StatefulSet back up: %w", err)
	}
	scaledDown = false
	markAction(model.PhaseScaleUp)

	// STEP 6: Resume CR
	common.InfoLog("STEP 6: Resuming MariaDB CR")
	if err := b.resumeCR(ctx); err != nil {
		rescue()
		return fmt.Errorf("failed to resume CR: %w", err)
	}
	suspended = false

	// Wait for all pods ready
	common.InfoLog("STEP 7: Waiting for all pods to become ready")
	b.waitForAllReady(ctx)
	markAction(model.PhaseWaitReady)

	// Cleanup: clear forceClusterBootstrapInPod
	common.InfoLog("STEP 8: Cleaning up forceClusterBootstrapInPod")
	clearPatch := `{"spec":{"galera":{"recovery":{"forceClusterBootstrapInPod":""}}}}`
	_, _ = c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(ns).Patch(
		ctx, cfg.ClusterName, types.MergePatchType, []byte(clearPatch), metav1.PatchOptions{})
	markAction(model.PhaseCleanup)

	// STEP 9: Re-triage
	output.Section("Bootstrap Phase 4: Verify")
	obj, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(ns).Get(ctx, cfg.ClusterName, metav1.GetOptions{})
	if err == nil {
		b.p.SetMariaDB(obj)
	}

	postTriage, _ := triage.Run(ctx, b.triager, engine.NopSink{})
	markAction(model.PhaseVerify)

	if postTriage != nil {
		healthy := postTriage.ReadyCount == postTriage.TotalCount
		result.FinalHealth = &model.ClusterHealthSummary{
			ReadyCount: postTriage.ReadyCount,
			TotalCount: postTriage.TotalCount,
			Healthy:    healthy,
		}
		if healthy {
			output.Success("Bootstrap complete — cluster is healthy (%d/%d ready)", postTriage.ReadyCount, postTriage.TotalCount)
		} else {
			output.Warn("Bootstrap complete — cluster may need time (%d/%d ready)", postTriage.ReadyCount, postTriage.TotalCount)
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// K8s helper methods (duplicated from galera engine — bootstrap is independent
// of repair and needs its own copy of these operations)
// ---------------------------------------------------------------------------

// suspendCR patches the MariaDB CR to set spec.suspend=true.
func (b *galeraBootstrap) suspendCR(ctx context.Context) error {
	c := k8s.GetClients()
	cfg := b.p.Config()
	patch := `{"spec":{"suspend":true}}`
	_, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(cfg.Namespace).Patch(
		ctx, cfg.ClusterName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// resumeCR patches the MariaDB CR to set spec.suspend=false.
func (b *galeraBootstrap) resumeCR(ctx context.Context) error {
	c := k8s.GetClients()
	cfg := b.p.Config()
	patch := `{"spec":{"suspend":false}}`
	_, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(cfg.Namespace).Patch(
		ctx, cfg.ClusterName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// scaleStatefulSet scales the StatefulSet to the desired replica count.
func (b *galeraBootstrap) scaleStatefulSet(ctx context.Context, replicas int32) error {
	c := k8s.GetClients()
	cfg := b.p.Config()
	scale, err := c.Clientset.AppsV1().StatefulSets(cfg.Namespace).GetScale(
		ctx, cfg.ClusterName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	scale.Spec.Replicas = replicas
	_, err = c.Clientset.AppsV1().StatefulSets(cfg.Namespace).UpdateScale(
		ctx, cfg.ClusterName, scale, metav1.UpdateOptions{})
	return err
}

// runHelperPod creates a busybox pod that mounts a PVC and runs a script,
// waits for completion, fetches logs, and cleans up.
func (b *galeraBootstrap) runHelperPod(ctx context.Context, name, pvcName, mountPath, script, sa string) error {
	cfg := b.p.Config()
	ns := cfg.Namespace
	c := k8s.GetClients()

	rootUser := int64(0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"hasteward": "heal-helper"},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyNever,
			ServiceAccountName: sa,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: &rootUser,
			},
			Containers: []corev1.Container{{
				Name:    "healer",
				Image:   "docker.io/library/busybox:latest",
				Command: []string{"sh", "-c", script},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "data", MountPath: mountPath},
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			}},
		},
	}

	_, err := c.Clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create helper pod %s: %w", name, err)
	}

	// Wait for completion
	for i := 0; i < 30; i++ {
		time.Sleep(5 * time.Second)
		p, pErr := c.Clientset.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if pErr != nil {
			continue
		}
		phase := string(p.Status.Phase)
		if phase == "Succeeded" {
			b.logHelperPodOutput(ctx, name)
			_ = c.Clientset.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{
				GracePeriodSeconds: ptr(int64(0)),
			})
			time.Sleep(2 * time.Second)
			return nil
		}
		if phase == "Failed" {
			b.logHelperPodOutput(ctx, name)
			_ = c.Clientset.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{
				GracePeriodSeconds: ptr(int64(0)),
			})
			return fmt.Errorf("helper pod %s failed", name)
		}
	}

	_ = c.Clientset.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr(int64(0)),
	})
	return fmt.Errorf("helper pod %s timed out", name)
}

// logHelperPodOutput fetches and displays logs from a helper pod.
func (b *galeraBootstrap) logHelperPodOutput(ctx context.Context, podName string) {
	c := k8s.GetClients()
	cfg := b.p.Config()
	req := c.Clientset.CoreV1().Pods(cfg.Namespace).GetLogs(podName, &corev1.PodLogOptions{})
	stream, err := req.Stream(ctx)
	if err != nil {
		common.DebugLog("Failed to get helper pod logs: %v", err)
		return
	}
	defer stream.Close()
	data, _ := io.ReadAll(stream)
	if len(data) > 0 {
		common.DebugLog("Helper pod output:\n%s", string(data))
	}
}

// deleteRecoveryPods removes stale mariadb-operator recovery pods.
func (b *galeraBootstrap) deleteRecoveryPods(ctx context.Context) {
	c := k8s.GetClients()
	cfg := b.p.Config()
	pods, err := c.Clientset.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/instance=" + cfg.ClusterName + ",k8s.mariadb.com/recovery=true",
	})
	if err != nil {
		return
	}
	for _, p := range pods.Items {
		_ = c.Clientset.CoreV1().Pods(cfg.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
			GracePeriodSeconds: ptr(int64(0)),
		})
	}
}

// waitForAllReady waits for all StatefulSet pods to become Running and Ready.
func (b *galeraBootstrap) waitForAllReady(ctx context.Context) {
	c := k8s.GetClients()
	cfg := b.p.Config()
	expected := int(b.p.Replicas())

	for i := 0; i < 30; i++ {
		pods, err := c.Clientset.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/instance=" + cfg.ClusterName,
		})
		if err == nil {
			ready := 0
			for _, p := range pods.Items {
				if p.Status.Phase == "Running" && len(p.Status.ContainerStatuses) > 0 && p.Status.ContainerStatuses[0].Ready {
					ready++
				}
			}
			if ready == expected {
				common.InfoLog("All %d pods are Running and Ready", expected)
				return
			}
			common.DebugLog("Ready: %d/%d", ready, expected)
		}
		time.Sleep(10 * time.Second)
	}
	common.WarnLog("Not all pods became ready within timeout")
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T { return &v }
