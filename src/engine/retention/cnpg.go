package retention

import (
	"context"
	"fmt"

	"gitlab.prplanit.com/precisionplanit/hasteward/src/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/engine/provider"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/restic"
)

func init() {
	Register("cnpg", func(p provider.EngineProvider) (Retainer, error) {
		cp, ok := p.(*provider.CNPGProvider)
		if !ok {
			return nil, fmt.Errorf("expected *provider.CNPGProvider, got %T", p)
		}
		return &cnpgRetainer{p: cp}, nil
	})
}

type cnpgRetainer struct {
	p *provider.CNPGProvider
}

func (r *cnpgRetainer) Name() string { return "cnpg" }

func (r *cnpgRetainer) Prune(ctx context.Context, opts PruneOptions) (*model.PruneResult, error) {
	cfg := r.p.Config()
	rc := restic.NewClient(cfg.BackupsPath, cfg.ResticPassword)

	baseTags := map[string]string{
		"engine":    "cnpg",
		"cluster":   cfg.ClusterName,
		"namespace": cfg.Namespace,
	}

	policy := restic.RetentionPolicy{
		KeepLast:    opts.KeepLast,
		KeepDaily:   opts.KeepDaily,
		KeepWeekly:  opts.KeepWeekly,
		KeepMonthly: opts.KeepMonthly,
	}

	common.InfoLog("Applying retention policy (type=%s): keep-last=%d keep-daily=%d keep-weekly=%d keep-monthly=%d",
		opts.Type, policy.KeepLast, policy.KeepDaily, policy.KeepWeekly, policy.KeepMonthly)

	totalKeep := 0
	totalRemove := 0

	if opts.Type == "backup" || opts.Type == "all" {
		tags := make(map[string]string, len(baseTags)+1)
		for k, v := range baseTags {
			tags[k] = v
		}
		tags["type"] = "backup"
		results, err := rc.Forget(ctx, tags, policy, opts.Type == "backup")
		if err != nil {
			return nil, fmt.Errorf("prune (backup) failed: %w", err)
		}
		for _, r := range results {
			totalKeep += len(r.Keep)
			totalRemove += len(r.Remove)
		}
	}

	if opts.Type == "diverged" || opts.Type == "all" {
		tags := make(map[string]string, len(baseTags)+1)
		for k, v := range baseTags {
			tags[k] = v
		}
		tags["type"] = "diverged"
		kept, removed, err := rc.ForgetGrouped(ctx, tags, policy, true)
		if err != nil {
			return nil, fmt.Errorf("prune (diverged) failed: %w", err)
		}
		totalKeep += kept
		totalRemove += removed
	}

	return &model.PruneResult{
		TotalKept:    totalKeep,
		TotalRemoved: totalRemove,
	}, nil
}
