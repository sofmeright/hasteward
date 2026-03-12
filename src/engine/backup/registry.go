package backup

import (
	"github.com/PrPlanIT/HASteward/src/engine"
	"github.com/PrPlanIT/HASteward/src/engine/provider"
)

// Constructor creates a Backer from a validated EngineProvider.
type Constructor func(provider.EngineProvider) (Backer, error)

var registry = map[string]Constructor{}

// Register adds an engine backup constructor to the registry.
func Register(engine string, ctor Constructor) {
	registry[engine] = ctor
}

// Get returns a Backer for the given provider, or an error if unsupported.
func Get(p provider.EngineProvider) (Backer, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("backup", p.Name())
	}
	return ctor(p)
}
