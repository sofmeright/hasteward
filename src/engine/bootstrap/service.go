package bootstrap

import (
	"context"

	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Bootstrapper is the engine-specific hook contract for bootstrap operations.
type Bootstrapper interface {
	Name() string
	Bootstrap(ctx context.Context, dryRun bool) (*model.BootstrapResult, error)
}

// Constructor creates a Bootstrapper for a given provider.
type Constructor func(provider.EngineProvider) (Bootstrapper, error)

var registry = map[string]Constructor{}

// Register adds a bootstrapper constructor for an engine.
func Register(eng string, ctor Constructor) {
	registry[eng] = ctor
}

// Get returns a Bootstrapper for the given provider's engine.
func Get(p provider.EngineProvider) (Bootstrapper, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("bootstrap", p.Name())
	}
	return ctor(p)
}

// Run is the shared bootstrap lifecycle.
func Run(ctx context.Context, b Bootstrapper, dryRun bool, sink engine.StepSink) (*model.BootstrapResult, error) {
	sink.Step("bootstrap", "running")
	result, err := b.Bootstrap(ctx, dryRun)
	if err != nil {
		return result, err
	}
	sink.Step("bootstrap", "done")
	return result, nil
}
