package retention

import (
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine/provider"
)

// Constructor creates a Retainer for a given provider.
type Constructor func(provider.EngineProvider) (Retainer, error)

var registry = map[string]Constructor{}

// Register adds a retainer constructor for an engine.
func Register(eng string, ctor Constructor) {
	registry[eng] = ctor
}

// Get returns a Retainer for the given provider's engine.
func Get(p provider.EngineProvider) (Retainer, error) {
	ctor, ok := registry[p.Name()]
	if !ok {
		return nil, engine.Unsupported("retention", p.Name())
	}
	return ctor(p)
}
