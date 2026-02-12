package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"strconv"

	"gitlab.prplanit.com/precisionplanit/hasteward/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/restic"

	"github.com/spf13/cobra"
)

var exportOutput string

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Extract a backup snapshot to a local .sql.gz file",
	Long: `Exports a database dump from a restic snapshot to a local gzipped SQL file.

For diverged snapshots, use -i to specify the instance ordinal.

Examples:
  hasteward export -e cnpg -c zitadel-postgres -n zeldas-lullaby --snapshot latest -o dump.sql.gz
  hasteward export -e cnpg -c zitadel-postgres -n zeldas-lullaby --snapshot abc123 -i 2 -o instance2.sql.gz`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.BackupsPath == "" {
			return fmt.Errorf("export requires --backups-path")
		}
		if cfg.ResticPassword == "" {
			return fmt.Errorf("export requires RESTIC_PASSWORD env var")
		}
		if exportOutput == "" {
			return fmt.Errorf("export requires --output/-o")
		}
		if cfg.Engine == "" {
			return fmt.Errorf("export requires --engine/-e")
		}
		if cfg.ClusterName == "" {
			return fmt.Errorf("export requires --cluster/-c")
		}
		if cfg.Namespace == "" {
			return fmt.Errorf("export requires --namespace/-n")
		}

		if cfg.Verbose {
			os.Setenv(common.EnvPrefix+"LOG_LEVEL", "debug")
			common.InitLogging(false)
		}
		if cfg.ResticPassword != "" {
			common.RegisterSecret(cfg.ResticPassword)
		}

		// Resolve instance for diverged exports
		if err := resolveInstance(cmd); err != nil {
			return err
		}

		rc := restic.NewClient(cfg.BackupsPath, cfg.ResticPassword)

		var dumpFile string
		switch cfg.Engine {
		case "cnpg":
			dumpFile = "pgdumpall.sql"
		case "galera":
			dumpFile = "mysqldump.sql"
		default:
			return fmt.Errorf("unknown engine %q", cfg.Engine)
		}

		// Diverged snapshots have ordinal-prefixed filenames
		if cfg.InstanceNumber != nil {
			dumpFile = strconv.Itoa(*cfg.InstanceNumber) + "-" + dumpFile
		}
		snapshotPath := cfg.Namespace + "/" + cfg.ClusterName + "/" + dumpFile

		tags := map[string]string{
			"engine":    cfg.Engine,
			"cluster":   cfg.ClusterName,
			"namespace": cfg.Namespace,
		}

		// Create output file
		f, err := os.Create(exportOutput)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer f.Close()

		// Wrap in gzip writer
		gz := gzip.NewWriter(f)
		defer gz.Close()

		snapshot := cfg.Snapshot
		if snapshot == "" {
			snapshot = "latest"
		}

		common.InfoLog("Exporting snapshot %s path %s to %s", snapshot, snapshotPath, exportOutput)
		if err := rc.Dump(cmd.Context(), snapshot, snapshotPath, gz, tags); err != nil {
			// Clean up partial file on error
			gz.Close()
			f.Close()
			os.Remove(exportOutput)
			return fmt.Errorf("export failed: %w", err)
		}

		common.InfoLog("Export complete: %s", exportOutput)
		return nil
	},
}

func init() {
	exportCmd.Flags().StringVarP(&exportOutput, "output", "o", "", "Output file path (e.g., dump.sql.gz)")
}
