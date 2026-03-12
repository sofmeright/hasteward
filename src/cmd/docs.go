package cmd

import (
	"fmt"
	"os"

	"github.com/PrPlanIT/HASteward/internal/docsgen"
	"github.com/PrPlanIT/HASteward/src/output"

	"github.com/spf13/cobra"
)

var docsOutputPath string

var docsCmd = &cobra.Command{
	Use:   "docs",
	Short: "Documentation generation commands",
}

var docsGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate CLI reference documentation",
	RunE: func(cmd *cobra.Command, args []string) error {
		if docsOutputPath == "" {
			return docsgen.GenerateCLIReference(RootCmd, os.Stdout)
		}

		f, err := os.Create(docsOutputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer f.Close()

		if err := docsgen.GenerateCLIReference(RootCmd, f); err != nil {
			return err
		}

		output.Stderr("Generated: %s", docsOutputPath)
		return nil
	},
}

func init() {
	docsGenerateCmd.Flags().StringVarP(&docsOutputPath, "output", "o", "", "Output file path (default: stdout)")
	docsCmd.AddCommand(docsGenerateCmd)
	RootCmd.AddCommand(docsCmd)
}
