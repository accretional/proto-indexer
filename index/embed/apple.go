package embed

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

type appleProvider struct {
	binaryPath string
}

// NewApple returns a Provider that calls the macos-vision CLI to produce
// sentence embeddings via Apple's NaturalLanguage framework.
// binaryPath is the path to the macos-vision binary; if empty, "macos-vision"
// is looked up on $PATH.
func NewApple(binaryPath string) Provider {
	if binaryPath == "" {
		binaryPath = "macos-vision"
	}
	return &appleProvider{binaryPath: binaryPath}
}

func (p *appleProvider) Name() string  { return "apple" }
func (p *appleProvider) Model() string { return "sentence" }

// Dimension returns 0 until the first successful Embed call establishes the
// dimension. Callers that need the dimension before embedding should call
// Embed with a single sample text.
func (p *appleProvider) Dimension() int { return 0 }

// nlResult is the subset of the macos-vision JSON envelope we care about.
type nlResult struct {
	Result struct {
		Vector    []float32 `json:"vector"`
		Dimension int       `json:"dimension"`
	} `json:"result"`
}

func (p *appleProvider) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		if text == "" {
			continue
		}
		vec, err := p.embedOne(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("apple embed text[%d]: %w", i, err)
		}
		out[i] = vec
	}
	return out, nil
}

func (p *appleProvider) embedOne(ctx context.Context, text string) ([]float32, error) {
	cmd := exec.CommandContext(ctx, p.binaryPath, "nl",
		"--operation", "embed",
		"--text", text,
	)
	raw, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("macos-vision: %w", err)
	}
	var res nlResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("parse output: %w", err)
	}
	if len(res.Result.Vector) == 0 {
		return nil, fmt.Errorf("empty vector in response")
	}
	return res.Result.Vector, nil
}
