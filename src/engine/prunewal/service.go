package prunewal

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Pruner is the engine-specific hook contract for WAL pruning operations.
type Pruner interface {
	Name() string
	PruneWAL(ctx context.Context) (*model.PruneWALResult, error)
}

// Constructor creates a Pruner for a given provider.
type Constructor func(provider.EngineProvider) (Pruner, error)

var registry = map[string]Constructor{}

// Register adds a pruner constructor for an engine.
func Register(eng string, ctor Constructor) {
	registry[eng] = ctor
}

// Get returns a Pruner for the given provider's engine.
func Get(p provider.EngineProvider) (Pruner, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("prune-wal", p.Name())
	}
	return ctor(p)
}

// Run is the shared prunewal lifecycle.
func Run(ctx context.Context, pr Pruner, sink engine.StepSink) (*model.PruneWALResult, error) {
	sink.Step("prune-wal", "running")
	result, err := pr.PruneWAL(ctx)
	if err != nil {
		return nil, err
	}
	sink.Step("prune-wal", "done")
	return result, nil
}
