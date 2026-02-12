package main

import (
	"fmt"
	"os"

	"gitlab.prplanit.com/precisionplanit/hasteward/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/restic"

	"github.com/spf13/cobra"
)

var pruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Apply retention policy and remove old snapshots",
	Long: `Prunes old backup snapshots from restic repositories according to the
configured retention policy (keep-last, keep-daily, keep-weekly, keep-monthly).

By default, only type=backup snapshots are pruned. Use -t diverged to prune
only diverged snapshots, or -t all to prune both types.

For diverged snapshots, retention is group-aware: snapshots sharing the same
job tag (from one repair operation) are kept or removed as a unit. So
--keep-last 3 means "keep the 3 most recent repair jobs" regardless of how
many instances each job captured.

Examples:
  hasteward prune -e cnpg -c zitadel-postgres -n zeldas-lullaby --backups-path /backups
  hasteward prune -e cnpg -c zitadel-postgres -n zeldas-lullaby --backups-path /backups \
    --keep-last 7 --keep-daily 30 --keep-weekly 12 --keep-monthly 24
  hasteward prune -e cnpg -c zitadel-postgres -n zeldas-lullaby --backups-path /backups \
    -t diverged --keep-last 3`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.BackupsPath == "" {
			return fmt.Errorf("prune requires --backups-path")
		}
		if cfg.ResticPassword == "" {
			return fmt.Errorf("prune requires RESTIC_PASSWORD env var")
		}
		if cfg.Engine == "" {
			return fmt.Errorf("prune requires --engine/-e")
		}
		if cfg.ClusterName == "" {
			return fmt.Errorf("prune requires --cluster/-c")
		}
		if cfg.Namespace == "" {
			return fmt.Errorf("prune requires --namespace/-n")
		}

		switch pruneType {
		case "backup", "diverged", "all":
		default:
			return fmt.Errorf("--type must be backup, diverged, or all (got %q)", pruneType)
		}

		if cfg.Verbose {
			os.Setenv(common.EnvPrefix+"LOG_LEVEL", "debug")
			common.InitLogging(false)
		}
		if cfg.ResticPassword != "" {
			common.RegisterSecret(cfg.ResticPassword)
		}

		rc := restic.NewClient(cfg.BackupsPath, cfg.ResticPassword)

		baseTags := map[string]string{
			"engine":    cfg.Engine,
			"cluster":   cfg.ClusterName,
			"namespace": cfg.Namespace,
		}

		policy := restic.RetentionPolicy{
			KeepLast:    pruneKeepLast,
			KeepDaily:   pruneKeepDaily,
			KeepWeekly:  pruneKeepWeekly,
			KeepMonthly: pruneKeepMonthly,
		}

		common.InfoLog("Applying retention policy (type=%s): keep-last=%d keep-daily=%d keep-weekly=%d keep-monthly=%d",
			pruneType, policy.KeepLast, policy.KeepDaily, policy.KeepWeekly, policy.KeepMonthly)

		totalKeep := 0
		totalRemove := 0

		// Backup snapshots: standard restic forget
		if pruneType == "backup" || pruneType == "all" {
			tags := copyTags(baseTags)
			tags["type"] = "backup"
			results, err := rc.Forget(cmd.Context(), tags, policy, pruneType == "backup")
			if err != nil {
				return fmt.Errorf("prune (backup) failed: %w", err)
			}
			for _, r := range results {
				totalKeep += len(r.Keep)
				totalRemove += len(r.Remove)
			}
		}

		// Diverged snapshots: group-aware forget (by job tag)
		if pruneType == "diverged" || pruneType == "all" {
			tags := copyTags(baseTags)
			tags["type"] = "diverged"
			kept, removed, err := rc.ForgetGrouped(cmd.Context(), tags, policy, true)
			if err != nil {
				return fmt.Errorf("prune (diverged) failed: %w", err)
			}
			totalKeep += kept
			totalRemove += removed
		}

		fmt.Printf("Pruned %d snapshots, kept %d\n", totalRemove, totalKeep)
		return nil
	},
}

var (
	pruneKeepLast    int
	pruneKeepDaily   int
	pruneKeepWeekly  int
	pruneKeepMonthly int
	pruneType        string
)

func copyTags(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func init() {
	pruneCmd.Flags().IntVar(&pruneKeepLast, "keep-last", 7, "Keep the last N snapshots (or jobs for diverged)")
	pruneCmd.Flags().IntVar(&pruneKeepDaily, "keep-daily", 30, "Keep N daily snapshots (or jobs for diverged)")
	pruneCmd.Flags().IntVar(&pruneKeepWeekly, "keep-weekly", 12, "Keep N weekly snapshots (or jobs for diverged)")
	pruneCmd.Flags().IntVar(&pruneKeepMonthly, "keep-monthly", 24, "Keep N monthly snapshots (or jobs for diverged)")
	pruneCmd.Flags().StringVarP(&pruneType, "type", "t", "backup", "Snapshot type to prune: backup, diverged, or all")
}
