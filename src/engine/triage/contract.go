package triage

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Triager is the engine-specific hook contract for triage operations.
type Triager interface {
	Name() string
	Collect(ctx context.Context) error
	Analyze(ctx context.Context) (*model.TriageResult, error)
}
