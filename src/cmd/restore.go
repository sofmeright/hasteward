package cmd

import (
	"fmt"
	"time"

	"github.com/PrPlanIT/HASteward/src/engine/restore"
	"github.com/PrPlanIT/HASteward/src/output"
	"github.com/PrPlanIT/HASteward/src/output/model"
	"github.com/PrPlanIT/HASteward/src/output/printer"

	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a database cluster from a restic snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := InitPrinter("restore")
		if err != nil {
			return err
		}

		if Cfg.BackupsPath == "" {
			return fmt.Errorf("restore requires --backups-path")
		}
		if Cfg.ResticPassword == "" {
			return fmt.Errorf("restore requires RESTIC_PASSWORD env var")
		}

		prov, err := PreRun(cmd, "restore")
		if err != nil {
			return err
		}

		restorer, err := restore.Get(prov)
		if err != nil {
			return err
		}

		result, err := restore.Run(cmd.Context(), restorer, newSink(p))
		if err != nil {
			if !p.IsHuman() {
				printer.PrintResult(p, (*model.RestoreResult)(nil), nil, err)
			}
			return err
		}

		if p.IsHuman() {
			output.Complete(fmt.Sprintf("Restore complete — snapshot %s (%s)", result.SnapshotID, result.Duration.Truncate(time.Second)))
		} else {
			printer.PrintResult(p, result, nil, nil)
		}
		return nil
	},
}
