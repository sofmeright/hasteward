package repair

import (
	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
)

// Constructor creates a Repairer from a validated EngineProvider.
type Constructor func(provider.EngineProvider) (Repairer, error)

var registry = map[string]Constructor{}

// Register adds a repair constructor for an engine.
func Register(engine string, ctor Constructor) {
	registry[engine] = ctor
}

// Get returns a Repairer for the given provider's engine.
func Get(p provider.EngineProvider) (Repairer, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("repair", p.Name())
	}
	return ctor(p)
}
