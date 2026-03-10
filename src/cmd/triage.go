package cmd

import (
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine/triage"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/printer"

	"github.com/spf13/cobra"
)

var triageCmd = &cobra.Command{
	Use:   "triage",
	Short: "Read-only diagnostics for a database cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := InitPrinter("triage")
		if err != nil {
			return err
		}

		prov, err := PreRun(cmd, "triage")
		if err != nil {
			return err
		}

		triager, err := triage.Get(prov)
		if err != nil {
			return err
		}

		result, err := triage.Run(cmd.Context(), triager, engine.NopSink{})
		if err != nil {
			if !p.IsHuman() {
				printer.PrintResult(p, (*model.TriageResult)(nil), nil, err)
			}
			return err
		}

		if p.IsHuman() {
			output.Complete("Triage complete")
		} else {
			printer.PrintResult(p, result, nil, nil)
		}
		return nil
	},
}
