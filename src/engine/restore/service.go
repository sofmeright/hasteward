package restore

import (
	"context"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
)

// Run is the shared restore lifecycle.
func Run(ctx context.Context, r Restorer, sink engine.StepSink) (*model.RestoreResult, error) {
	sink.Step("restore", "running")
	result, err := r.Restore(ctx)
	if err != nil {
		return nil, err
	}
	sink.Step("restore", "done")
	return result, nil
}
