package printer

import (
	"fmt"
	"os"
	"strings"

	"github.com/PrPlanIT/HASteward/src/output/style"
)

// OutputMode defines the output format.
type OutputMode int

const (
	OutputAuto  OutputMode = iota // TTY → human, non-TTY → json
	OutputHuman                   // Box-drawing, color, status icons
	OutputJSON                    // Single JSON document to stdout
	OutputJSONL                   // Newline-delimited JSON events
)

// String returns the string representation of an OutputMode.
func (m OutputMode) String() string {
	switch m {
	case OutputAuto:
		return "auto"
	case OutputHuman:
		return "human"
	case OutputJSON:
		return "json"
	case OutputJSONL:
		return "jsonl"
	default:
		return "unknown"
	}
}

// ParseOutputMode parses a string into an OutputMode.
func ParseOutputMode(s string) (OutputMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "auto", "":
		return OutputAuto, nil
	case "human":
		return OutputHuman, nil
	case "json":
		return OutputJSON, nil
	case "jsonl":
		return OutputJSONL, nil
	default:
		return OutputAuto, fmt.Errorf("invalid output mode %q (valid: auto, human, json, jsonl)", s)
	}
}

// Resolve resolves OutputAuto to a concrete mode based on TTY detection.
func (m OutputMode) Resolve() OutputMode {
	if m != OutputAuto {
		return m
	}
	if style.IsTTY(os.Stdout) {
		return OutputHuman
	}
	return OutputJSON
}
