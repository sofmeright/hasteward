package repair

import (
	"context"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
)

// Repairer is the engine-specific hook contract for repair operations.
type Repairer interface {
	Name() string
	Assess(ctx context.Context) (*model.TriageResult, error)
	SafetyGate(ctx context.Context, triage *model.TriageResult) error
	Escrow(ctx context.Context, triage *model.TriageResult) error
	PlanTargets(ctx context.Context, triage *model.TriageResult) ([]HealTarget, error)
	Heal(ctx context.Context, target HealTarget) error
	Stabilize(ctx context.Context) error
	Reassess(ctx context.Context) (*model.TriageResult, error)
}

// HealTarget identifies a single instance to heal.
type HealTarget struct {
	Pod         string
	InstanceNum int
	Reason      string
}
