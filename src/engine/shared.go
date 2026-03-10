package engine

import (
	"errors"
	"fmt"
)

// ErrNotSupported indicates an operation is not available for a given engine.
var ErrNotSupported = errors.New("operation not supported")

// Unsupported returns a consistent error for unsupported operation+engine combinations.
// All operation registries use this so error shape is predictable.
func Unsupported(op, eng string) error {
	return fmt.Errorf("%w: %s for %s", ErrNotSupported, op, eng)
}

// StepSink receives lifecycle step events from operation services.
type StepSink interface {
	Step(name, status string)
}

// NopSink discards all step events.
type NopSink struct{}

func (NopSink) Step(string, string) {}
