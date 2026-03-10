package restore

import (
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine/provider"
)

// Constructor creates a Restorer from a validated EngineProvider.
type Constructor func(provider.EngineProvider) (Restorer, error)

var registry = map[string]Constructor{}

// Register adds an engine restore constructor to the registry.
func Register(engine string, ctor Constructor) {
	registry[engine] = ctor
}

// Get returns a Restorer for the given provider, or an error if unsupported.
func Get(p provider.EngineProvider) (Restorer, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("restore", p.Name())
	}
	return ctor(p)
}
