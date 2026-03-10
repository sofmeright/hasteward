package main

import (
	"os"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/cmd"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output"
)

func main() {
	common.InitLogging(false)
	if err := cmd.RootCmd.Execute(); err != nil {
		output.Stderr("Error: %v", err)
		os.Exit(1)
	}
}
