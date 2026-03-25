package reconfigure

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Reconfigurer defines cluster-scoped authority correction operations.
// Unlike repair (instance-scoped), reconfigure operations intentionally
// stop all pods and restart the cluster with corrected metadata.
type Reconfigurer interface {
	Name() string
	Assess(ctx context.Context) (*model.TriageResult, error)
	Validate(ctx context.Context, result *model.TriageResult) error
	PrintPlan(ctx context.Context, result *model.TriageResult)
	Execute(ctx context.Context, result *model.TriageResult) error
}
