package cmd

import (
	"fmt"

	"github.com/PrPlanIT/HASteward/src/engine/bootstrap"
	"github.com/PrPlanIT/HASteward/src/output"
	"github.com/PrPlanIT/HASteward/src/output/printer"

	"github.com/spf13/cobra"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Bootstrap a fully-down Galera cluster from the best candidate",
	Long: `Performs a full Galera cluster bootstrap when ALL nodes are down.

This is a DANGEROUS operation. It identifies the node with the highest
sequence number (most recent data), sets safe_to_bootstrap=1, patches
the MariaDB CR with forceClusterBootstrapInPod, and brings the cluster
back from total failure.

Safety gates:
  - Refuses if any healthy nodes exist (use 'repair' instead)
  - Refuses if seqno is ambiguous across nodes (unless --force)
  - Refuses if split-brain is detected (unless --force)
  - Supports --dry-run to preview the plan without mutation

Use --dry-run --output json for automation to inspect the decision
before approving execution.

Examples:
  hasteward bootstrap -e galera -c kimai-mariadb -n hyrule-castle
  hasteward bootstrap -e galera -c kimai-mariadb -n hyrule-castle --dry-run
  hasteward bootstrap -e galera -c kimai-mariadb -n hyrule-castle --dry-run --output json
  hasteward bootstrap -e galera -c kimai-mariadb -n hyrule-castle --force`,
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := InitPrinter("bootstrap")
		if err != nil {
			return err
		}

		prov, err := PreRun(cmd, "bootstrap")
		if err != nil {
			return err
		}

		bootstrapper, err := bootstrap.Get(prov)
		if err != nil {
			return err
		}

		result, err := bootstrap.Run(cmd.Context(), bootstrapper, IsDryRun(), newSink(p))
		if err != nil {
			if !p.IsHuman() && result != nil {
				// Return the partial result (includes decision) even on error
				printer.PrintResult(p, result, nil, err)
			}
			return err
		}

		if p.IsHuman() {
			if IsDryRun() {
				output.Banner("DRY RUN — Bootstrap Plan")
				output.Field("Candidate", result.Decision.CandidatePod)
				output.Field("Seqno", fmt.Sprintf("%d", result.Decision.CandidateSeqno))
				output.Field("UUID", result.Decision.CandidateUUID)
				if result.Decision.AmbiguityDetected {
					output.Warn("Ambiguity detected: competitors %v", result.Decision.Competitors)
				}
				output.Section("Planned Actions")
				for _, action := range result.ActionsPlanned {
					output.Bullet(0, "[%s] %s", action.Phase, action.Description)
				}
				output.Info("Re-run without --dry-run to execute")
			} else {
				output.Complete("Bootstrap complete")
			}
		} else {
			printer.PrintResult(p, result, nil, nil)
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(bootstrapCmd)
}
