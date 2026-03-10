package cmd

import (
	"gitlab.prplanit.com/precisionplanit/hasteward/src/common"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/model"
	"gitlab.prplanit.com/precisionplanit/hasteward/src/output/printer"

	"github.com/spf13/cobra"
)

var triageCmd = &cobra.Command{
	Use:   "triage",
	Short: "Read-only diagnostics for a database cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := InitPrinter("triage")
		if err != nil {
			return err
		}

		eng, err := PreRun(cmd, "triage")
		if err != nil {
			return err
		}

		result, err := eng.Triage(cmd.Context())
		if err != nil {
			if !p.IsHuman() {
				printer.PrintResult(p, (*model.TriageResult)(nil), nil, err)
			}
			return err
		}

		if p.IsHuman() {
			output.Complete("Triage complete")
		} else {
			triageResult := convertTriageResult(result)
			printer.PrintResult(p, triageResult, nil, nil)
		}
		return nil
	},
}

// convertTriageResult converts the legacy common.TriageResult to model.TriageResult.
func convertTriageResult(r *common.TriageResult) *model.TriageResult {
	if r == nil {
		return nil
	}
	result := &model.TriageResult{
		Engine: Cfg.Engine,
		Cluster: model.ObjectRef{
			Namespace: Cfg.Namespace,
			Name:      Cfg.ClusterName,
		},
		ClusterPhase: r.ClusterPhase,
		ReadyCount:   r.ReadyCount,
		TotalCount:   r.TotalCount,
		AllNodesDown: r.AllNodesDown,
	}

	for _, a := range r.Assessments {
		result.Assessments = append(result.Assessments, convertAssessment(a))
	}

	result.DataComparison = model.DataComparison{
		MostAdvanced:       r.DataComparison.MostAdvanced,
		MostAdvancedValue:  r.DataComparison.MostAdvancedValue,
		SafeToHeal:         r.DataComparison.SafeToHeal,
		Warnings:           r.DataComparison.Warnings,
		SplitBrainDetails:  r.DataComparison.SplitBrainDetails,
		CheckpointLocation: r.DataComparison.CheckpointLocation,
		PrimaryMembers:     r.DataComparison.PrimaryMembers,
		BestPrimarySeqno:   r.DataComparison.BestPrimarySeqno,
	}

	if r.BestSeqnoNode != nil {
		conv := convertAssessment(*r.BestSeqnoNode)
		result.BestSeqnoNode = &conv
	}

	return result
}

func convertAssessment(a common.InstanceAssessment) model.InstanceAssessment {
	return model.InstanceAssessment{
		Pod:                a.Pod,
		Instance:           a.Instance,
		IsRunning:          a.IsRunning,
		IsReady:            a.IsReady,
		NeedsHeal:          a.NeedsHeal,
		Notes:              a.Notes,
		Recommendation:     a.Recommendation,
		IsPrimary:          a.IsPrimary,
		Timeline:           a.Timeline,
		LSN:                a.LSN,
		IsInPrimary:        a.IsInPrimary,
		Seqno:              a.Seqno,
		EffectiveSeqno:     a.EffectiveSeqno,
		SeqnoSource:        a.SeqnoSource,
		SeqnoLag:           a.SeqnoLag,
		UUID:               a.UUID,
		SafeToBootstrap:    a.SafeToBootstrap,
		WsrepState:         a.WsrepState,
		WsrepStateComment:  a.WsrepStateComment,
		WsrepConnected:     a.WsrepConnected,
		WsrepReady:         a.WsrepReady,
		WsrepClusterStatus: a.WsrepClusterStatus,
		CrashReason:        a.CrashReason,
		DiskPct:            a.DiskPct,
	}
}
