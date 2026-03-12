package main

import (
	"os"

	"github.com/PrPlanIT/HASteward/src/cmd"
	"github.com/PrPlanIT/HASteward/src/common"
	"github.com/PrPlanIT/HASteward/src/output"
)

func main() {
	common.InitLogging(false)
	if err := cmd.RootCmd.Execute(); err != nil {
		output.Stderr("Error: %v", err)
		os.Exit(1)
	}
}
