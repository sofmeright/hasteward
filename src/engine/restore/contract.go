package restore

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Restorer is the engine-specific hook contract for restore operations.
type Restorer interface {
	Name() string
	Restore(ctx context.Context) (*model.RestoreResult, error)
}
