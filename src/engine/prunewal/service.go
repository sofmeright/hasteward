package prunewal

import (
	"context"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine/provider"
)

// Pruner is the engine-specific hook contract for WAL pruning operations.
type Pruner interface {
	Name() string
	PruneWAL(ctx context.Context) error
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
func Run(ctx context.Context, pr Pruner, sink engine.StepSink) error {
	sink.Step("prune-wal", "running")
	if err := pr.PruneWAL(ctx); err != nil {
		return err
	}
	sink.Step("prune-wal", "done")
	return nil
}
