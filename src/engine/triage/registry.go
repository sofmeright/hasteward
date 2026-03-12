package triage

import (
	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
)

// Constructor creates a Triager for a given provider.
type Constructor func(provider.EngineProvider) (Triager, error)

var registry = map[string]Constructor{}

// Register adds a triager constructor for an engine.
func Register(engine string, ctor Constructor) {
	registry[engine] = ctor
}

// Get returns a Triager for the given provider's engine.
func Get(p provider.EngineProvider) (Triager, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("triage", p.Name())
	}
	return ctor(p)
}
