package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	v1alpha1 "gitlab.prplanit.com/precisionplanit/hasteward/api/v1alpha1"
	"gitlab.prplanit.com/precisionplanit/hasteward/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/controller"
	"gitlab.prplanit.com/precisionplanit/hasteward/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/k8s"
	"gitlab.prplanit.com/precisionplanit/hasteward/output"

	// Register engines via init()
	_ "gitlab.prplanit.com/precisionplanit/hasteward/engine/cnpg"
	_ "gitlab.prplanit.com/precisionplanit/hasteward/engine/galera"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// Shared flags bound to the root command
var cfg common.Config

func main() {
	common.InitLogging(false)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "hasteward",
	Short: "HASteward - High Availability Steward for database clusters",
	Long: `HASteward safely triages, repairs, backs up, and restores
database clusters managed by CNPG (PostgreSQL) and MariaDB Operator (Galera).

Backups are stored in restic repositories with block-level dedup,
encryption, and compression.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	// Persistent flags available to all subcommands
	pf := rootCmd.PersistentFlags()
	pf.StringVarP(&cfg.Engine, "engine", "e", common.Env("ENGINE", ""), "Database engine: cnpg or galera")
	pf.StringVarP(&cfg.ClusterName, "cluster", "c", common.Env("CLUSTER", ""), "Database cluster CR name")
	pf.StringVarP(&cfg.Namespace, "namespace", "n", common.Env("NAMESPACE", ""), "Kubernetes namespace")
	pf.BoolVarP(&cfg.Force, "force", "f", common.EnvBool("FORCE", false), "Override safety checks (targeted repair only)")
	pf.StringVar(&cfg.BackupsPath, "backups-path", common.Env("BACKUPS_PATH", ""), "Restic repository path or URL")
	pf.StringVar(&cfg.ResticPassword, "restic-password", common.EnvRaw("RESTIC_PASSWORD", common.Env("RESTIC_PASSWORD", "")), "Restic repository encryption password")
	pf.BoolVar(&cfg.NoEscrow, "no-escrow", common.EnvBool("NO_ESCROW", false), "Skip pre-repair escrow backup")
	pf.StringVarP(&cfg.BackupMethod, "method", "m", common.Env("BACKUP_METHOD", "dump"), "Backup method: dump or native")
	pf.StringVar(&cfg.Snapshot, "snapshot", common.Env("SNAPSHOT", "latest"), "Restic snapshot ID or 'latest' (for restore)")
	pf.IntVar(&cfg.HealTimeout, "heal-timeout", common.EnvInt("HEAL_TIMEOUT", 600), "Heal wait timeout in seconds")
	pf.IntVar(&cfg.DeleteTimeout, "delete-timeout", common.EnvInt("DELETE_TIMEOUT", 300), "Delete wait timeout in seconds")
	pf.StringVar(&cfg.Kubeconfig, "kubeconfig", common.EnvRaw("KUBECONFIG", ""), "Path to kubeconfig file")
	pf.BoolVarP(&cfg.Verbose, "verbose", "v", common.EnvBool("VERBOSE", false), "Verbose output (debug logging)")

	// Instance flag needs special handling for optional int
	pf.StringP("instance", "i", common.Env("INSTANCE", ""), "Target specific instance number")

	// Subcommands
	rootCmd.AddCommand(serveCmd, triageCmd, repairCmd, backupCmd, restoreCmd, getCmd, exportCmd, pruneCmd)
}

// schemeWithCRDs builds a runtime.Scheme with core types + hasteward CRDs.
func schemeWithCRDs() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// resolveInstance parses the --instance flag into cfg.InstanceNumber.
func resolveInstance(cmd *cobra.Command) error {
	raw, _ := cmd.Flags().GetString("instance")
	if raw == "" {
		cfg.InstanceNumber = nil
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("--instance must be an integer, got %q", raw)
	}
	cfg.InstanceNumber = &n
	return nil
}

// preRun validates required flags, initializes K8s clients, and resolves the engine.
func preRun(cmd *cobra.Command, mode string) (engine.Engine, error) {
	cfg.Mode = mode

	// Re-init logging with debug level if verbose
	if cfg.Verbose {
		os.Setenv(common.EnvPrefix+"LOG_LEVEL", "debug")
		common.InitLogging(false)
	}

	// Register restic password as a secret so it's redacted from logs
	if cfg.ResticPassword != "" {
		common.RegisterSecret(cfg.ResticPassword)
	}

	// Validate required flags
	var missing []string
	if cfg.Engine == "" {
		missing = append(missing, "--engine/-e")
	}
	if cfg.ClusterName == "" {
		missing = append(missing, "--cluster/-c")
	}
	if cfg.Namespace == "" {
		missing = append(missing, "--namespace/-n")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("required flags: %s", strings.Join(missing, ", "))
	}

	// Resolve instance
	if err := resolveInstance(cmd); err != nil {
		return nil, err
	}

	// Initialize K8s clients
	if _, err := k8s.Init(cfg.Kubeconfig); err != nil {
		return nil, fmt.Errorf("kubernetes init failed: %w", err)
	}

	// Get engine
	eng, err := engine.Get(cfg.Engine)
	if err != nil {
		return nil, err
	}

	// Validate engine (fetch CR, set state)
	ctx := cmd.Context()
	output.Header(eng.Name(), mode, cfg.ClusterName, cfg.Namespace)
	if err := eng.Validate(ctx, &cfg); err != nil {
		return nil, err
	}

	return eng, nil
}

var triageCmd = &cobra.Command{
	Use:   "triage",
	Short: "Read-only diagnostics for a database cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		eng, err := preRun(cmd, "triage")
		if err != nil {
			return err
		}

		result, err := eng.Triage(cmd.Context())
		if err != nil {
			return err
		}

		_ = result // Display is handled inside the engine triage
		output.Complete("Triage complete")
		return nil
	},
}

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Heal unhealthy database instances",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Validate restic config for repair (unless no_escrow)
		if !cfg.NoEscrow {
			if cfg.BackupsPath == "" {
				return fmt.Errorf("repair requires --backups-path for escrow (or --no-escrow to skip)")
			}
			if cfg.ResticPassword == "" {
				return fmt.Errorf("repair requires RESTIC_PASSWORD for escrow (or --no-escrow to skip)")
			}
		}

		eng, err := preRun(cmd, "repair")
		if err != nil {
			return err
		}

		result, err := eng.Repair(cmd.Context())
		if err != nil {
			return err
		}

		summary := fmt.Sprintf("Repair complete — healed: %d, skipped: %d (%s)",
			len(result.HealedInstances), len(result.SkippedInstances), result.Duration.Truncate(time.Second))
		output.Complete(summary)
		return nil
	},
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Back up a database cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Validate restic config for dump backups
		if cfg.BackupMethod != "native" {
			if cfg.BackupsPath == "" {
				return fmt.Errorf("backup requires --backups-path (or --method native for CNPG S3)")
			}
			if cfg.ResticPassword == "" {
				return fmt.Errorf("backup requires RESTIC_PASSWORD env var")
			}
		}

		eng, err := preRun(cmd, "backup")
		if err != nil {
			return err
		}

		result, err := eng.Backup(cmd.Context())
		if err != nil {
			return err
		}

		output.Complete(fmt.Sprintf("Backup complete — snapshot %s (%s)", result.SnapshotID, result.Duration.Truncate(time.Second)))
		return nil
	},
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a database cluster from a restic snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.BackupsPath == "" {
			return fmt.Errorf("restore requires --backups-path")
		}
		if cfg.ResticPassword == "" {
			return fmt.Errorf("restore requires RESTIC_PASSWORD env var")
		}

		eng, err := preRun(cmd, "restore")
		if err != nil {
			return err
		}

		result, err := eng.Restore(cmd.Context())
		if err != nil {
			return err
		}

		output.Complete(fmt.Sprintf("Restore complete — snapshot %s (%s)", result.SnapshotID, result.Duration.Truncate(time.Second)))
		return nil
	},
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the hasteward operator (controller + scheduler)",
	Long: `Starts the hasteward operator which watches CNPG Cluster and MariaDB CRs
for clinic.hasteward.prplanit.com/policy annotations and automatically runs scheduled backups
and triage/repair operations based on BackupPolicy configuration.

Endpoints:
  :8080/metrics   Prometheus metrics
  :8081/healthz   Liveness probe
  :8081/readyz    Readiness probe`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Switch to JSON logging for operator mode
		if cfg.Verbose {
			os.Setenv(common.EnvPrefix+"LOG_LEVEL", "debug")
		}
		common.InitLogging(true)

		return controller.Run(cmd.Context(), cfg.Kubeconfig)
	},
}
