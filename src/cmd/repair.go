package cmd

import (
	"fmt"
	"time"

	"github.com/PrPlanIT/HASteward/src/engine/repair"
	"github.com/PrPlanIT/HASteward/src/output"
	"github.com/PrPlanIT/HASteward/src/output/model"
	"github.com/PrPlanIT/HASteward/src/output/printer"

	"github.com/spf13/cobra"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Heal unhealthy database instances",
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := InitPrinter("repair")
		if err != nil {
			return err
		}

		if !Cfg.NoEscrow {
			if Cfg.BackupsPath == "" {
				return fmt.Errorf("repair requires --backups-path for escrow (or --no-escrow to skip)")
			}
			if Cfg.ResticPassword == "" {
				return fmt.Errorf("repair requires RESTIC_PASSWORD for escrow (or --no-escrow to skip)")
			}
		}

		prov, err := PreRun(cmd, "repair")
		if err != nil {
			return err
		}

		repairer, err := repair.Get(prov)
		if err != nil {
			return err
		}

		result, err := repair.Run(cmd.Context(), repairer, newSink(p))
		if err != nil {
			if !p.IsHuman() {
				printer.PrintResult(p, (*model.RepairResult)(nil), nil, err)
			}
			return err
		}

		if p.IsHuman() {
			summary := fmt.Sprintf("Repair complete — healed: %d, skipped: %d (%s)",
				len(result.HealedInstances), len(result.SkippedInstances), result.Duration.Truncate(time.Second))
			output.Complete(summary)
		} else {
			printer.PrintResult(p, result, nil, nil)
		}
		return nil
	},
}
