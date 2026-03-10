package triage

import (
	"context"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
)

// Run is the shared triage lifecycle. All engines go through this flow.
func Run(ctx context.Context, t Triager, sink engine.StepSink) (*model.TriageResult, error) {
	sink.Step("collect", "running")
	if err := t.Collect(ctx); err != nil {
		return nil, err
	}
	sink.Step("collect", "done")

	sink.Step("analyze", "running")
	result, err := t.Analyze(ctx)
	if err != nil {
		return nil, err
	}
	sink.Step("analyze", "done")

	return result, nil
}
