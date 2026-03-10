package retention

import (
	"context"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
)

// Retainer is the engine-specific hook contract for backup retention operations.
type Retainer interface {
	Name() string
	// Prune applies the retention policy and removes old snapshots.
	Prune(ctx context.Context, opts PruneOptions) (*model.PruneResult, error)
}

// PruneOptions holds the configuration for a prune operation.
type PruneOptions struct {
	Type        string // "backup", "diverged", or "all"
	KeepLast    int
	KeepDaily   int
	KeepWeekly  int
	KeepMonthly int
}
