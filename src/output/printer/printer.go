package printer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PrPlanIT/HASteward/src/output/model"
	"github.com/PrPlanIT/HASteward/src/output/render"
)

// Printer is the central coordinator for all output.
// It dispatches to the appropriate renderer based on the configured output mode.
type Printer struct {
	Mode    OutputMode
	Stdout  io.Writer
	Stderr  io.Writer
	RunID   string
	Command string
	Start   time.Time

	human *render.Human
	json  *render.JSON
	jsonl *render.JSONL
}

// New creates a Printer with the resolved output mode.
func New(mode OutputMode, command string) *Printer {
	resolved := mode.Resolve()
	p := &Printer{
		Mode:    resolved,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		RunID:   generateRunID(),
		Command: command,
		Start:   time.Now(),
	}
	switch resolved {
	case OutputHuman:
		p.human = &render.Human{Out: os.Stdout}
	case OutputJSON:
		p.json = &render.JSON{Out: os.Stdout}
	case OutputJSONL:
		p.jsonl = &render.JSONL{Out: os.Stdout}
	}
	return p
}

// IsHuman returns true if the printer is in human mode.
func (p *Printer) IsHuman() bool {
	return p.Mode == OutputHuman
}

// IsJSON returns true if the printer is in JSON mode.
func (p *Printer) IsJSON() bool {
	return p.Mode == OutputJSON
}

// IsJSONL returns true if the printer is in JSONL mode.
func (p *Printer) IsJSONL() bool {
	return p.Mode == OutputJSONL
}

// Human returns the human renderer, or nil if not in human mode.
func (p *Printer) Human() *render.Human {
	return p.human
}

// EmitEvent emits a progress event (JSONL mode only, no-op for others).
func (p *Printer) EmitEvent(evt model.Event) {
	if p.jsonl != nil {
		_ = p.jsonl.Emit(evt)
	}
}

// EmitRunStarted emits the initial run.started event.
func (p *Printer) EmitRunStarted() {
	p.EmitEvent(model.NewEvent(model.EventRunStarted, p.Command, p.RunID))
}

// EmitPhaseStarted emits a phase.started event.
func (p *Printer) EmitPhaseStarted(phase, message string) {
	p.EmitEvent(model.NewEvent(model.EventPhaseStarted, p.Command, p.RunID).
		WithPhase(phase).WithMessage(message))
}

// EmitPhaseCompleted emits a phase.completed event.
func (p *Printer) EmitPhaseCompleted(phase string, success bool) {
	p.EmitEvent(model.NewEvent(model.EventPhaseComplete, p.Command, p.RunID).
		WithPhase(phase).WithSuccess(success))
}

// PrintResult outputs the final result in the configured format.
// For JSON mode: wraps in an envelope and renders as a single JSON document.
// For JSONL mode: wraps in an envelope, renders as compact JSON, then emits run.completed.
// For human mode: no-op (human output is emitted progressively by the caller).
func PrintResult[T any](p *Printer, data T, warnings []model.Warning, err error) int {
	duration := time.Since(p.Start)

	var envelope model.Envelope[T]
	if err != nil {
		exitCode := model.ExitGenericFailure
		sErr := model.NewError("command.failed", model.CategoryInternal, err.Error())
		envelope = model.NewEnvelope(p.Command, p.RunID, data)
		envelope.WithDuration(duration)
		envelope.WithWarnings(warnings...)
		envelope.WithErrors(exitCode, sErr)
	} else {
		envelope = model.NewEnvelope(p.Command, p.RunID, data)
		envelope.WithDuration(duration)
		envelope.WithWarnings(warnings...)
	}

	switch p.Mode {
	case OutputJSON:
		_ = p.json.Render(envelope)
	case OutputJSONL:
		_ = p.jsonl.Emit(envelope)
		p.EmitEvent(model.CompletionEvent(
			p.Command, p.RunID,
			envelope.Success, envelope.Partial,
			envelope.ExitCode, envelope.Warnings, envelope.Errors,
		))
	case OutputHuman:
		if err != nil && p.human != nil {
			p.human.Fail("%v", err)
		}
	}

	return envelope.ExitCode
}

// Diagnostic writes a diagnostic message to stderr (all modes).
func (p *Printer) Diagnostic(format string, args ...any) {
	fmt.Fprintf(p.Stderr, format+"\n", args...)
}

func generateRunID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
