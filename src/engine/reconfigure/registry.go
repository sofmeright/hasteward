package reconfigure

import (
	"fmt"

	"github.com/PrPlanIT/HASteward/src/engine/provider"
)

type factory func(p provider.EngineProvider) (Reconfigurer, error)

var registry = map[string]factory{}

// Register adds a reconfigure engine factory.
func Register(engine string, f factory) {
	registry[engine] = f
}

// Get returns a Reconfigurer for the given engine provider.
func Get(p provider.EngineProvider) (Reconfigurer, error) {
	f, ok := registry[p.Name()]
	if !ok {
		return nil, fmt.Errorf("no reconfigure engine registered for %q", p.Name())
	}
	return f(p)
}
