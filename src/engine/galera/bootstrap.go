package galera

import (
	"context"
	"fmt"
	"time"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/k8s"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Bootstrap performs a full Galera cluster bootstrap when all nodes are down.
//
// Flow:
//  1. Triage to assess cluster state
//  2. Paranoid safety gates (refuse if healthy nodes exist, refuse if ambiguous seqno unless --force)
//  3. Identify best bootstrap candidate (highest effective seqno)
//  4. If dryRun: return decision + planned actions without mutation
//  5. Execute: suspend CR → scale to 0 → set safe_to_bootstrap=1 on candidate PVC →
//     patch CR forceClusterBootstrapInPod → scale up → resume → wait ready
//  6. Re-triage and return typed BootstrapResult
func (e *Engine) Bootstrap(ctx context.Context, dryRun bool) (*model.BootstrapResult, error) {
	ns := e.cfg.Namespace
	clusterRef := model.ObjectRef{
		APIVersion: "k8s.mariadb.com/v1alpha1",
		Kind:       "MariaDB",
		Namespace:  ns,
		Name:       e.cfg.ClusterName,
	}
	result := &model.BootstrapResult{
		Engine:  "galera",
		Cluster: clusterRef,
	}

	// Phase 1: Triage
	output.Section("Bootstrap Phase 1: Triage")
	triageResult, err := e.Triage(ctx)
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
	safeToProceed := !forceRequired || e.cfg.Force

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
		if !e.cfg.Force {
			result.Decision.Reason = fmt.Sprintf("ambiguous: %s and %v all at seqno %d — use --force to pick %s",
				candidate.Pod, competitors, candidateSeqno, candidate.Pod)
			return result, fmt.Errorf("ABORT: Ambiguous bootstrap candidate. Multiple nodes at seqno %d. Re-run with --force to select %s",
				candidateSeqno, candidate.Pod)
		}
		common.WarnLog("force=true — proceeding with %s despite ambiguity", candidate.Pod)
	}

	if !safeToHeal && !e.cfg.Force {
		result.Decision.Reason = "split-brain detected — use --force to override"
		return result, fmt.Errorf("ABORT: Split-brain detected. Re-run with --force to override")
	}

	output.Success("Bootstrap candidate: %s (seqno: %d, uuid: %s)", candidate.Pod, candidateSeqno, candidate.UUID)

	// Build planned actions
	stsRef := model.ObjectRef{Kind: "StatefulSet", Namespace: ns, Name: e.cfg.ClusterName}
	crRef := clusterRef

	result.ActionsPlanned = []model.BootstrapAction{
		{Phase: model.PhaseSuspend, Description: "Suspend MariaDB CR", Resource: &crRef},
		{Phase: model.PhaseScaleDown, Description: "Scale StatefulSet to 0", Resource: &stsRef},
		{Phase: model.PhaseBootstrapMark, Description: fmt.Sprintf("Set safe_to_bootstrap=1 on %s PVC", candidate.Pod)},
		{Phase: model.PhaseClusterPatch, Description: "Patch CR forceClusterBootstrapInPod=" + candidate.Pod, Resource: &crRef},
		{Phase: model.PhaseScaleUp, Description: fmt.Sprintf("Scale StatefulSet to %d", e.replicas), Resource: &stsRef},
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
	if err := e.executeBootstrap(ctx, candidate.Pod, result); err != nil {
		return result, err
	}

	return result, nil
}

func (e *Engine) executeBootstrap(ctx context.Context, candidatePod string, result *model.BootstrapResult) error {
	ns := e.cfg.Namespace
	c := k8s.GetClients()
	originalReplicas := int32(e.replicas)

	// Capture SA before pods are deleted
	sa := "default"
	pods, err := c.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/instance=%s", e.cfg.ClusterName),
	})
	if err == nil {
		sa = k8s.ServiceAccountFromPods(pods.Items)
	}

	suspended := false
	scaledDown := false

	rescue := func() {
		if scaledDown {
			_ = e.scaleStatefulSet(ctx, originalReplicas)
		}
		if suspended {
			_ = e.resumeCR(ctx)
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
	if err := e.suspendCR(ctx); err != nil {
		return fmt.Errorf("failed to suspend CR: %w", err)
	}
	suspended = true
	markAction(model.PhaseSuspend)
	time.Sleep(3 * time.Second)

	// STEP 2: Scale to 0
	common.InfoLog("STEP 2: Scaling StatefulSet to 0")
	if err := e.scaleStatefulSet(ctx, 0); err != nil {
		rescue()
		return fmt.Errorf("failed to scale StatefulSet to 0: %w", err)
	}
	scaledDown = true
	markAction(model.PhaseScaleDown)

	// Wait for all pods to terminate
	deleteTimeout := e.cfg.DeleteTimeout
	if deleteTimeout <= 0 {
		deleteTimeout = 300
	}
	for i := 0; i < deleteTimeout/5; i++ {
		pods, err := c.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app.kubernetes.io/instance=%s", e.cfg.ClusterName),
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
	helperName := fmt.Sprintf("%s-bootstrap-%d", e.cfg.ClusterName, time.Now().Unix())

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
	if err := e.runHelperPod(ctx, helperName, storagePVC, "/var/lib/mysql", bootstrapScript, sa); err != nil {
		rescue()
		return fmt.Errorf("failed to set safe_to_bootstrap: %w", err)
	}
	markAction(model.PhaseBootstrapMark)

	// STEP 4: Patch CR with forceClusterBootstrapInPod
	common.InfoLog("STEP 4: Patching CR forceClusterBootstrapInPod=%s", candidatePod)
	patchJSON := fmt.Sprintf(`{"spec":{"galera":{"recovery":{"forceClusterBootstrapInPod":"%s"}}}}`, candidatePod)
	_, err = c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(ns).Patch(
		ctx, e.cfg.ClusterName, types.MergePatchType, []byte(patchJSON), metav1.PatchOptions{})
	if err != nil {
		rescue()
		return fmt.Errorf("failed to patch CR: %w", err)
	}
	markAction(model.PhaseClusterPatch)

	// STEP 5: Scale back up
	common.InfoLog("STEP 5: Scaling StatefulSet to %d", originalReplicas)
	e.deleteRecoveryPods(ctx)
	time.Sleep(2 * time.Second)

	if err := e.scaleStatefulSet(ctx, originalReplicas); err != nil {
		rescue()
		return fmt.Errorf("failed to scale StatefulSet back up: %w", err)
	}
	scaledDown = false
	markAction(model.PhaseScaleUp)

	// STEP 6: Resume CR
	common.InfoLog("STEP 6: Resuming MariaDB CR")
	if err := e.resumeCR(ctx); err != nil {
		rescue()
		return fmt.Errorf("failed to resume CR: %w", err)
	}
	suspended = false

	// Wait for all pods ready
	common.InfoLog("STEP 7: Waiting for all pods to become ready")
	e.waitForAllReady(ctx)
	markAction(model.PhaseWaitReady)

	// Cleanup: clear forceClusterBootstrapInPod
	common.InfoLog("STEP 8: Cleaning up forceClusterBootstrapInPod")
	clearPatch := `{"spec":{"galera":{"recovery":{"forceClusterBootstrapInPod":""}}}}`
	_, _ = c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(ns).Patch(
		ctx, e.cfg.ClusterName, types.MergePatchType, []byte(clearPatch), metav1.PatchOptions{})
	markAction(model.PhaseCleanup)

	// STEP 9: Re-triage
	output.Section("Bootstrap Phase 4: Verify")
	obj, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(ns).Get(ctx, e.cfg.ClusterName, metav1.GetOptions{})
	if err == nil {
		e.mariadb = obj
		e.mariadbSpec = k8s.GetNestedMap(obj, "spec")
		e.mariadbStatus = k8s.GetNestedMap(obj, "status")
		e.readyCondition = findCondition(e.mariadbStatus, "Ready")
		e.galeraCondition = findCondition(e.mariadbStatus, "GaleraReady")
		e.galeraRecovery = k8s.GetNestedMap(obj, "status", "galeraRecovery")
		e.isSuspended = k8s.GetNestedBool(obj, "spec", "suspend")
	}

	postTriage, _ := e.Triage(ctx)
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
