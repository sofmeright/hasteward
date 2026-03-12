package backup

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Run is the shared backup lifecycle.
func Run(ctx context.Context, b Backer, sink engine.StepSink) (*model.BackupResult, error) {
	sink.Step("backup", "running")
	result, err := b.Backup(ctx)
	if err != nil {
		return nil, err
	}
	sink.Step("backup", "done")
	return result, nil
}
