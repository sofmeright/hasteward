package retention

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Run is the shared retention lifecycle.
func Run(ctx context.Context, r Retainer, opts PruneOptions, sink engine.StepSink) (*model.PruneResult, error) {
	sink.Step("retention", "running")
	result, err := r.Prune(ctx, opts)
	if err != nil {
		return nil, err
	}
	sink.Step("retention", "done")
	return result, nil
}
