package repair

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/PrPlanIT/HASteward/src/common"
	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/backup"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
	"github.com/PrPlanIT/HASteward/src/engine/triage"
	"github.com/PrPlanIT/HASteward/src/k8s"
	"github.com/PrPlanIT/HASteward/src/output"
	"github.com/PrPlanIT/HASteward/src/output/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const galeraDumpFilename = "mysqldump.sql"

func init() {
	Register("galera", func(p provider.EngineProvider) (Repairer, error) {
		gp, ok := p.(*provider.GaleraProvider)
		if !ok {
			return nil, fmt.Errorf("galera repair: expected *provider.GaleraProvider, got %T", p)
		}
		t, err := triage.Get(p)
		if err != nil {
			return nil, fmt.Errorf("galera repair: triage init: %w", err)
		}
		b, err := backup.Get(p)
		if err != nil {
			return nil, fmt.Errorf("galera repair: backup init: %w", err)
		}
		return &galeraRepair{p: gp, triager: t, backuper: b}, nil
	})
}

// galeraRepair implements Repairer for MariaDB Galera clusters.
type galeraRepair struct {
	p              *provider.GaleraProvider
	triager        triage.Triager
	backuper       backup.Backer
	donorSelection *DonorSelection // Resolved once in SafetyGate, immutable for the run
}

func (g *galeraRepair) Name() string { return "galera" }

// Assess runs a full triage of the Galera cluster.
func (g *galeraRepair) Assess(ctx context.Context) (*model.TriageResult, error) {
	output.Section("Phase 1: Triage")
	return triage.Run(ctx, g.triager, engine.NopSink{})
}

// SafetyGate resolves the donor and verifies it is suitable for SST.
// The resolved donor is cached on g.donorSelection for use by all downstream steps.
func (g *galeraRepair) SafetyGate(ctx context.Context, result *model.TriageResult) error {
	output.Section("Phase 2: Donor Resolution")
	ds, err := g.resolveRepairDonor(ctx, result)
	if err != nil {
		return err
	}
	g.donorSelection = ds
	displayDonorSelection(ds)
	return nil
}

// Escrow performs the pre-repair escrow backup and diverged per-instance backups.
func (g *galeraRepair) Escrow(ctx context.Context, result *model.TriageResult) error {
	cfg := g.p.Config()
	start := time.Now()

	if !cfg.NoEscrow {
		if cfg.BackupsPath == "" || cfg.ResticPassword == "" {
			return fmt.Errorf("repair requires --backups-path and RESTIC_PASSWORD for escrow (or --no-escrow to skip)")
		}

		if g.donorSelection != nil {
			donor := g.donorSelection.Pod
			ns := cfg.Namespace
			stdinFilename := fmt.Sprintf("%s/%s/%s", ns, cfg.ClusterName, galeraDumpFilename)
			escrowResult, err := g.backuper.BackupDump(ctx, "backup", donor, stdinFilename, start, nil)
			if err != nil {
				return fmt.Errorf("pre-repair backup failed: %w", err)
			}
			common.InfoLog("Pre-repair backup from %s: %s", donor, escrowResult.SnapshotID)
		} else {
			common.WarnLog("No donor resolved for pre-repair backup. Skipping.")
		}
	} else {
		common.WarnLog("no_escrow=true — proceeding without pre-repair backup")
	}

	// Diverged per-instance backups (when split-brain detected)
	if !result.DataComparison.SafeToHeal && !cfg.NoEscrow {
		jobID := start.UTC().Format("20060102T150405Z")
		common.WarnLog("Split-brain detected — capturing per-instance diverged backups (job=%s)", jobID)
		ns := cfg.Namespace
		for _, a := range result.Assessments {
			if !a.IsRunning || !a.IsReady {
				common.WarnLog("Skipping diverged backup for %s (not running/ready)", a.Pod)
				continue
			}
			stdinFilename := fmt.Sprintf("%s/%s/%d-%s", ns, cfg.ClusterName, a.Instance, galeraDumpFilename)
			extraTags := map[string]string{"job": jobID}
			divResult, err := g.backuper.BackupDump(ctx, "diverged", a.Pod, stdinFilename, start, extraTags)
			if err != nil {
				common.WarnLog("Failed diverged backup for %s: %v", a.Pod, err)
				continue
			}
			common.InfoLog("Diverged backup %s: %s", a.Pod, divResult.SnapshotID)
		}
	}

	return nil
}

// PlanTargets determines which instances need healing.
func (g *galeraRepair) PlanTargets(ctx context.Context, result *model.TriageResult) ([]HealTarget, error) {
	cfg := g.p.Config()

	if cfg.InstanceNumber != nil {
		return g.planTargeted(ctx, result)
	}
	return g.planUntargeted(ctx, result)
}

func (g *galeraRepair) planTargeted(ctx context.Context, result *model.TriageResult) ([]HealTarget, error) {
	cfg := g.p.Config()
	targetPod := fmt.Sprintf("%s-%d", cfg.ClusterName, *cfg.InstanceNumber)

	// Find target assessment
	var targetAssessment *model.InstanceAssessment
	for i := range result.Assessments {
		if result.Assessments[i].Pod == targetPod {
			targetAssessment = &result.Assessments[i]
			break
		}
	}
	if targetAssessment == nil {
		return nil, fmt.Errorf("ABORT: %s not found in instance assessments. Check cluster_name and instance_number", targetPod)
	}

	// Safety gate: target is active primary member and healthy
	if targetAssessment.IsInPrimary && !targetAssessment.NeedsHeal && !cfg.Force {
		common.WarnLog("%s is an active member of the Primary component. Healing will destroy its data and force SST rejoin.", targetPod)
	}

	// Safety gate: split-brain -> fail unless force
	if !result.DataComparison.SafeToHeal && !cfg.Force {
		return nil, fmt.Errorf("ABORT: Split-brain detected. Healing %s may cause DATA LOSS. Re-run with --force to override", targetPod)
	}
	if !result.DataComparison.SafeToHeal && cfg.Force {
		common.WarnLog("force=true - proceeding despite split-brain detection. Data on %s will be DESTROYED", targetPod)
	}

	// Safety gate: target is healthy -> skip unless force
	if !targetAssessment.NeedsHeal && !cfg.Force {
		output.Info("Node %s is healthy and does not need healing. Nothing to do.", targetPod)
		return nil, nil
	}
	if !targetAssessment.NeedsHeal && cfg.Force {
		common.WarnLog("force=true - healing %s even though it appears healthy", targetPod)
	}

	// Verify storage PVC exists
	c := k8s.GetClients()
	storagePVC := fmt.Sprintf("storage-%s", targetPod)
	_, err := c.Clientset.CoreV1().PersistentVolumeClaims(cfg.Namespace).Get(ctx, storagePVC, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage PVC %s not found: %w", storagePVC, err)
	}

	reason := "needs heal"
	if len(targetAssessment.Notes) > 0 {
		reason = strings.Join(targetAssessment.Notes, ", ")
	}
	return []HealTarget{{
		Pod:         targetPod,
		InstanceNum: *cfg.InstanceNumber,
		Reason:      reason,
	}}, nil
}

func (g *galeraRepair) planUntargeted(ctx context.Context, result *model.TriageResult) ([]HealTarget, error) {
	// Safety gate: split-brain -> HARD STOP (no override for untargeted)
	if !result.DataComparison.SafeToHeal {
		return nil, fmt.Errorf("HARD STOP: Split-brain detected. Cannot auto-heal all nodes. " +
			"Admin must review triage output, then use targeted repair: --instance <N>")
	}

	var targets []HealTarget
	for _, a := range result.Assessments {
		if a.NeedsHeal {
			reason := "needs heal"
			if len(a.Notes) > 0 {
				reason = strings.Join(a.Notes, ", ")
			}
			targets = append(targets, HealTarget{
				Pod:         a.Pod,
				InstanceNum: a.Instance,
				Reason:      reason,
			})
		}
	}

	if len(targets) == 0 {
		output.Info("All nodes are healthy. Nothing to heal.")
		return nil, nil
	}

	// Display plan
	output.Section("Repair Plan")
	for _, t := range targets {
		output.Bullet(0, "%s (%s)", t.Pod, t.Reason)
	}

	return targets, nil
}

// Heal heals a single Galera node via suspend/scale/wipe/resume.
func (g *galeraRepair) Heal(ctx context.Context, target HealTarget) error {
	return g.healNode(ctx, target.Pod, target.InstanceNum)
}

// Stabilize waits for the operator to reconcile and all pods to become ready.
func (g *galeraRepair) Stabilize(ctx context.Context) error {
	output.Section("Post-Repair Stabilization")
	common.InfoLog("Waiting 30s for MariaDB operator to reconcile...")
	time.Sleep(30 * time.Second)
	g.waitForAllReady(ctx)
	return nil
}

// Reassess re-fetches the MariaDB CR state and runs triage again.
func (g *galeraRepair) Reassess(ctx context.Context) (*model.TriageResult, error) {
	output.Section("Post-Repair Re-Triage")
	cfg := g.p.Config()
	obj, err := k8s.GetClients().Dynamic.Resource(k8s.MariaDBGVR).Namespace(cfg.Namespace).Get(
		ctx, cfg.ClusterName, metav1.GetOptions{})
	if err == nil {
		g.p.SetMariaDB(obj)
	}
	return triage.Run(ctx, g.triager, engine.NopSink{})
}

// ---------------------------------------------------------------------------
// Private heal methods (from galera/heal.go)
// ---------------------------------------------------------------------------

// healNode heals a single Galera node via suspend/scale/wipe/resume.
func (g *galeraRepair) healNode(ctx context.Context, targetPod string, instanceNum int) error {
	cfg := g.p.Config()
	ns := cfg.Namespace
	c := k8s.GetClients()

	// Capture SA from target pod before it gets deleted during scale-down
	sa := "default"
	if targetPodObj, err := c.Clientset.CoreV1().Pods(ns).Get(ctx, targetPod, metav1.GetOptions{}); err == nil {
		if targetPodObj.Spec.ServiceAccountName != "" {
			sa = targetPodObj.Spec.ServiceAccountName
		}
	}

	storagePVC := fmt.Sprintf("storage-%s", targetPod)
	galeraPVC := fmt.Sprintf("galera-%s", targetPod)
	storageHelper := fmt.Sprintf("%s-heal-storage-%d-%d", cfg.ClusterName, instanceNum, time.Now().Unix())
	galeraHelper := fmt.Sprintf("%s-heal-galera-%d-%d", cfg.ClusterName, instanceNum, time.Now().Unix())

	suspended := false
	scaledDown := false
	originalReplicas := int32(g.p.Replicas())

	// Determine scale strategy:
	// If target is the last ordinal (replicas-1), can do partial scale down to that ordinal.
	// Otherwise, must scale to 0 because StatefulSets scale down from highest ordinal.
	scaleTarget := int32(0)
	if instanceNum == int(g.p.Replicas())-1 {
		scaleTarget = int32(instanceNum)
	}
	strategy := "full"
	if scaleTarget > 0 {
		strategy = "partial"
	}

	// Check if galera config PVC exists
	_, galeraErr := c.Clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, galeraPVC, metav1.GetOptions{})
	hasGaleraPVC := galeraErr == nil

	output.Section("Healing " + targetPod)
	output.Bullet(0, "Strategy: %s (scale to %d)", strategy, scaleTarget)
	output.Bullet(0, "1. Suspend MariaDB CR (operator stops reconciling)")
	output.Bullet(0, "2. Scale down StatefulSet to %d (release PVCs)", scaleTarget)
	if cfg.WipeDatadir {
		output.Bullet(0, "3. WIPE ENTIRE DATADIR on storage PVC (full SST reseed)")
	} else {
		output.Bullet(0, "3. Wipe grastate.dat + galera.cache on storage PVC")
	}
	if hasGaleraPVC {
		output.Bullet(0, "4. Remove bootstrap config from galera PVC")
	} else {
		output.Bullet(0, "4. (no galera PVC)")
	}
	output.Bullet(0, "5. Scale back up, resume CR (node rejoins via SST)")

	// Rescue cleanup function
	rescue := func() {
		_ = c.Clientset.CoreV1().Pods(ns).Delete(ctx, storageHelper, metav1.DeleteOptions{
			GracePeriodSeconds: ptr(int64(0)),
		})
		if hasGaleraPVC {
			_ = c.Clientset.CoreV1().Pods(ns).Delete(ctx, galeraHelper, metav1.DeleteOptions{
				GracePeriodSeconds: ptr(int64(0)),
			})
		}
		if scaledDown {
			g.scaleStatefulSet(ctx, originalReplicas)
		}
		if suspended {
			g.resumeCR(ctx)
		}
		if suspended || scaledDown {
			common.WarnLog("HEAL FAILED for %s. CR resumed and scale restored.", targetPod)
		}
	}

	// STEP 1: Suspend MariaDB CR
	common.InfoLog("STEP 1: Suspending MariaDB CR")
	if err := g.suspendCR(ctx); err != nil {
		return fmt.Errorf("failed to suspend CR: %w", err)
	}
	suspended = true
	time.Sleep(3 * time.Second)

	// STEP 2: Scale down StatefulSet
	common.InfoLog("STEP 2: Scaling StatefulSet to %d", scaleTarget)
	if err := g.scaleStatefulSet(ctx, scaleTarget); err != nil {
		rescue()
		return fmt.Errorf("failed to scale StatefulSet: %w", err)
	}
	scaledDown = true

	// Wait for target pod to terminate
	deleteTimeout := cfg.DeleteTimeout
	if deleteTimeout <= 0 {
		deleteTimeout = 300
	}

	for i := 0; i < deleteTimeout/5; i++ {
		_, err := c.Clientset.CoreV1().Pods(ns).Get(ctx, targetPod, metav1.GetOptions{})
		if err != nil {
			common.InfoLog("Pod %s terminated, PVCs released", targetPod)
			break
		}
		time.Sleep(5 * time.Second)
	}

	// STEP 3: Wipe storage PVC
	var storageScript string
	if cfg.WipeDatadir {
		common.WarnLog("STEP 3: WIPING ENTIRE DATADIR on %s (--wipe-datadir)", targetPod)
		storageScript = `set -e
echo "=== FULL DATADIR WIPE ==="
echo "Contents before wipe:"
ls -la /var/lib/mysql/ 2>/dev/null || echo "  (empty or not mounted)"
echo ""
echo "=== Verifying mount ==="
if [ ! -d /var/lib/mysql ]; then
  echo "ERROR: /var/lib/mysql does not exist"
  exit 1
fi
mountpoint -q /var/lib/mysql || echo "WARNING: /var/lib/mysql is not a mountpoint"
echo "=== Wiping all data ==="
rm -rf /var/lib/mysql/*
rm -rf /var/lib/mysql/.*  2>/dev/null || true
echo "=== Datadir wiped ==="
ls -la /var/lib/mysql/ 2>/dev/null || echo "  (empty)"
echo "=== Done! Node will require full SST from donor ==="
`
	} else {
		common.InfoLog("STEP 3: Wiping grastate.dat + galera.cache")
		storageScript = `set -e
echo "=== Current grastate.dat ==="
cat /var/lib/mysql/grastate.dat 2>/dev/null || echo "not found"
echo ""
echo "=== Preserving grastate.dat ==="
cp /var/lib/mysql/grastate.dat /var/lib/mysql/grastate.dat.pre-heal 2>/dev/null || echo "nothing to preserve"
echo "=== Wiping grastate.dat ==="
printf '%s\n' \
  '# GALERA saved state' \
  'version: 2.1' \
  'uuid:    00000000-0000-0000-0000-000000000000' \
  'seqno:   -1' \
  'safe_to_bootstrap: 0' \
  > /var/lib/mysql/grastate.dat
echo "New grastate.dat:"
cat /var/lib/mysql/grastate.dat
echo ""
echo "=== Preserving galera.cache ==="
mv /var/lib/mysql/galera.cache /var/lib/mysql/galera.cache.pre-heal 2>/dev/null || echo "no galera.cache to preserve"
echo "=== Done! ==="
`
	}
	if err := g.runHelperPod(ctx, storageHelper, storagePVC, "/var/lib/mysql", storageScript, sa); err != nil {
		rescue()
		return fmt.Errorf("storage helper failed: %w", err)
	}

	// STEP 4: Remove bootstrap config from galera PVC (if exists)
	if hasGaleraPVC {
		common.InfoLog("STEP 4: Removing bootstrap config from galera PVC")
		galeraScript := `set -e
echo "=== Current galera config ==="
ls -la /galera/
echo ""
if [ -f /galera/1-bootstrap.cnf ]; then
  echo "=== Found bootstrap config ==="
  cat /galera/1-bootstrap.cnf
  echo ""
  echo "=== Removing 1-bootstrap.cnf ==="
  rm -f /galera/1-bootstrap.cnf
  echo "Bootstrap config removed!"
else
  echo "No 1-bootstrap.cnf found (OK)"
fi
echo ""
echo "=== Final galera config ==="
ls -la /galera/
echo "=== Done! ==="
`
		if err := g.runHelperPod(ctx, galeraHelper, galeraPVC, "/galera", galeraScript, sa); err != nil {
			rescue()
			return fmt.Errorf("galera config helper failed: %w", err)
		}
	}

	// STEP 5: Scale back up and resume CR
	common.InfoLog("STEP 5: Scaling back up and resuming CR")

	// Clear stale recovery pods
	g.deleteRecoveryPods(ctx)
	time.Sleep(2 * time.Second)

	// Scale back up
	if err := g.scaleStatefulSet(ctx, originalReplicas); err != nil {
		rescue()
		return fmt.Errorf("failed to scale StatefulSet back up: %w", err)
	}
	scaledDown = false

	// Resume CR
	if err := g.resumeCR(ctx); err != nil {
		rescue()
		return fmt.Errorf("failed to resume CR: %w", err)
	}
	suspended = false

	// Wait for pod to come back online
	common.InfoLog("Waiting for %s to come back online", targetPod)
	healTimeout := cfg.HealTimeout
	if healTimeout <= 0 {
		healTimeout = 600
	}

	ready := false
	for i := 0; i < healTimeout/10; i++ {
		time.Sleep(10 * time.Second)
		pod, err := c.Clientset.CoreV1().Pods(ns).Get(ctx, targetPod, metav1.GetOptions{})
		if err == nil && pod.Status.Phase == "Running" &&
			len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].Ready {
			ready = true
			break
		}
	}

	if ready {
		output.Success("Node %s has been healed!", targetPod)
	} else {
		common.WarnLog("%s did not become ready within timeout. SST may still be in progress.", targetPod)
	}

	return nil
}

// suspendCR patches the MariaDB CR to set spec.suspend=true.
func (g *galeraRepair) suspendCR(ctx context.Context) error {
	cfg := g.p.Config()
	c := k8s.GetClients()
	patch := `{"spec":{"suspend":true}}`
	_, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(cfg.Namespace).Patch(
		ctx, cfg.ClusterName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// resumeCR patches the MariaDB CR to set spec.suspend=false.
func (g *galeraRepair) resumeCR(ctx context.Context) error {
	cfg := g.p.Config()
	c := k8s.GetClients()
	patch := `{"spec":{"suspend":false}}`
	_, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(cfg.Namespace).Patch(
		ctx, cfg.ClusterName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// scaleStatefulSet scales the StatefulSet to the desired replica count.
func (g *galeraRepair) scaleStatefulSet(ctx context.Context, replicas int32) error {
	cfg := g.p.Config()
	c := k8s.GetClients()
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
func (g *galeraRepair) runHelperPod(ctx context.Context, name, pvcName, mountPath, script, sa string) error {
	cfg := g.p.Config()
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
			g.logHelperPodOutput(ctx, name)
			_ = c.Clientset.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{
				GracePeriodSeconds: ptr(int64(0)),
			})
			time.Sleep(2 * time.Second)
			return nil
		}
		if phase == "Failed" {
			g.logHelperPodOutput(ctx, name)
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
func (g *galeraRepair) logHelperPodOutput(ctx context.Context, podName string) {
	cfg := g.p.Config()
	c := k8s.GetClients()
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
func (g *galeraRepair) deleteRecoveryPods(ctx context.Context) {
	cfg := g.p.Config()
	c := k8s.GetClients()
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

// displayFinalStatus shows the current cluster state after healing.
func (g *galeraRepair) displayFinalStatus(ctx context.Context) {
	cfg := g.p.Config()
	c := k8s.GetClients()
	obj, err := c.Dynamic.Resource(k8s.MariaDBGVR).Namespace(cfg.Namespace).Get(
		ctx, cfg.ClusterName, metav1.GetOptions{})
	if err != nil {
		return
	}

	status := k8s.GetNestedMap(obj, "status")
	readyCond := provider.FindCondition(status, "Ready")
	galeraCond := provider.FindCondition(status, "GaleraReady")

	output.Section("Final Status")
	if readyCond != nil {
		output.Field("Ready", fmt.Sprintf("%v", readyCond["status"]))
	}
	if galeraCond != nil {
		output.Field("GaleraReady", fmt.Sprintf("%v", galeraCond["status"]))
	}

	pods, err := c.Clientset.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/instance=" + cfg.ClusterName,
	})
	if err == nil {
		for _, p := range pods.Items {
			podReady := false
			if len(p.Status.ContainerStatuses) > 0 {
				podReady = p.Status.ContainerStatuses[0].Ready
			}
			output.Bullet(0, "%s: %s ready=%v", p.Name, p.Status.Phase, podReady)
		}
	}
}

// waitForAllReady polls until all expected replicas are Running and Ready.
func (g *galeraRepair) waitForAllReady(ctx context.Context) {
	cfg := g.p.Config()
	c := k8s.GetClients()
	expected := int(g.p.Replicas())

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

func ptr[T any](v T) *T { return &v }
